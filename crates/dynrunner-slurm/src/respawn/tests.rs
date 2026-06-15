//! Trait-contract tests for the SLURM spawner.
//!
//! Strategy: drive `spawn()` against a recording `Gateway` that
//! captures the `sbatch ...` invocation (so we can assert the new
//! secondary id is propagated) and against a fail-on-sbatch
//! gateway (so we can assert `SpawnError` surfaces). The tunnel
//! step is mocked via a `RecordingTunnelEstablisher` that counts
//! calls and records the id it was invoked with — no real ssh /
//! sbatch / podman is touched.
//!
//! Respawn is FIRE-AND-FORGET: there is no revoke surface, and the
//! happy path NEVER issues `scancel`. The tests below pin both
//! invariants — over-allocation is structurally tolerated, never
//! cancelled (rule 1, #543: respawning must NEVER cancel a job).
use super::{SlurmSecondarySpawner, TunnelEstablisher, WrapperScriptGenerator};
use crate::config::SlurmConfig;
use crate::job_manager::SlurmJobManager;
use crate::preparation::PrepError;
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};
use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawnSpec, SecondarySpawner, SpawnError,
};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Recording gateway: every `execute_command` is captured, sbatch
/// is answered with a canned job ID and (optionally) configurable
/// failure. Routing by command-prefix matches `job_manager.rs`'s
/// `SubmitRecordingGateway` shape so future `submit_job` setup
/// commands don't break these tests.
#[derive(Default)]
struct RecordingGateway {
    commands: StdMutex<Vec<String>>,
    sbatch_fails: bool,
    /// Node name a `squeue -j … -o '%N'` probe answers with (the
    /// SLURM-vocabulary node the respawn resolves for `--exclude`).
    /// Empty → squeue reports no node (the resolution falls through to
    /// sacct, which this gateway also answers empty).
    squeue_node: String,
}

impl RecordingGateway {
    fn commands(&self) -> Vec<String> {
        self.commands.lock().unwrap().clone()
    }
}

impl Gateway for RecordingGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn execute_command(
        &self,
        cmd: &str,
        _cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        self.commands.lock().unwrap().push(cmd.to_string());
        // Respawn node-resolution probes. `node_from_squeue` issues
        // `squeue -j … -h -o '%N'` (distinct from cancel-verify's
        // `-o '%T|%N|%r'`); `node_from_sacct` issues `sacct … NodeList`.
        // The squeue probe answers with the configured node; sacct
        // answers empty (the squeue branch covers the resolved case, and
        // an empty squeue node falls through to an empty sacct → None).
        if cmd.contains("squeue") && cmd.contains("-o '%N'") {
            return Ok(CommandResult {
                return_code: 0,
                stdout: self.squeue_node.clone(),
                stderr: String::new(),
            });
        }
        if cmd.contains("sacct") && cmd.contains("NodeList") {
            return Ok(CommandResult {
                return_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            });
        }
        // `submit_job` pipes the wrapper body to sbatch over STDIN
        // (`printf '%s' '<body>' | sbatch --parsable …`), so the
        // recorded command CONTAINS `| sbatch ` rather than starting
        // with `sbatch `. Match that shape so the canned job-id stdout
        // still flows back.
        let is_sbatch = cmd.contains("| sbatch ");
        if is_sbatch && self.sbatch_fails {
            return Ok(CommandResult {
                return_code: 1,
                stdout: String::new(),
                stderr: "sbatch: error: simulated failure".into(),
            });
        }
        let stdout = if is_sbatch {
            "67890".to_string()
        } else {
            String::new()
        };
        Ok(CommandResult {
            return_code: 0,
            stdout,
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn create_directory(&self, _remote: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
        Ok(false)
    }
    fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
        Ok(())
    }
}

/// `TunnelEstablisher` stub: records every call and returns either
/// `Ok(())` or a canned `PrepError` so we can exercise the
/// spawner's success and failure branches without touching ssh.
///
/// A `gate` oneshot is held by tests that want to suspend the
/// `establish_one_tunnel` future deterministically (so they can
/// abort the outer spawn() future and then complete the tunnel
/// step to observe the post-abort side-effects on `job_manager`).
/// When `gate` is `None`, the call returns immediately.
struct RecordingTunnelEstablisher {
    calls: AtomicUsize,
    last_id: StdMutex<Option<String>>,
    fail_with: StdMutex<Option<PrepError>>,
    gate: StdMutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl RecordingTunnelEstablisher {
    fn ok() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            last_id: StdMutex::new(None),
            fail_with: StdMutex::new(None),
            gate: StdMutex::new(None),
        }
    }

    /// Variant that suspends `establish_one_tunnel` on the
    /// supplied gate. The test holds the matching `Sender` and
    /// completes the tunnel step deterministically (or drops the
    /// sender to observe a wedged-tunnel scenario).
    fn gated(gate: tokio::sync::oneshot::Receiver<()>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            last_id: StdMutex::new(None),
            fail_with: StdMutex::new(None),
            gate: StdMutex::new(Some(gate)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn last_id(&self) -> Option<String> {
        self.last_id.lock().unwrap().clone()
    }
}

impl TunnelEstablisher for RecordingTunnelEstablisher {
    fn establish_one_tunnel<'a>(
        &'a self,
        secondary_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PrepError>> + 'a>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last_id.lock().unwrap() = Some(secondary_id.to_owned());
        let maybe_err = self.fail_with.lock().unwrap().take();
        let maybe_gate = self.gate.lock().unwrap().take();
        Box::pin(async move {
            if let Some(gate) = maybe_gate {
                // Awaiting a dropped sender returns Err; treat
                // both Ok and Err as "the test released us".
                let _ = gate.await;
            }
            match maybe_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }
}

fn make_spec(id: &str) -> SecondarySpawnSpec {
    SecondarySpawnSpec {
        new_secondary_id: id.to_owned(),
        primary_endpoint: "primary.test.invalid:9001".to_owned(),
        primary_pubkey_pem: "-----BEGIN PUBLIC KEY-----\nstub\n".to_owned(),
        dead_member_id: None,
    }
}

/// Build a spec that names a dead member, so the spawner's
/// resolve-then-`--exclude` propagation can be asserted.
fn make_spec_with_dead_member(id: &str, dead_member_id: &str) -> SecondarySpawnSpec {
    SecondarySpawnSpec {
        dead_member_id: Some(dead_member_id.to_owned()),
        ..make_spec(id)
    }
}

/// `spawn()` must thread `spec.new_secondary_id` into both the
/// wrapper-script generator (so the script's secondary id matches)
/// AND the `--job-name=` argument of the sbatch invocation when
/// no consumer prefix is set. This locks down both propagation paths
/// in a single test — the same id must reach both places or the
/// respawn is broken. All `spawn()` tests run under a `LocalSet`:
/// the production implementation uses `tokio::task::spawn_local` to
/// detach the (sbatch + job_ids.push + tunnel) inner task from the
/// coordinator's `respawn_tasks` JoinSet so a JoinSet-level abort
/// can't orphan a submitted job. `spawn_local` requires a `LocalSet`
/// context — providing it in the test scaffold matches the
/// production caller (the operational loop's `run_until`).
#[tokio::test]
async fn slurm_spawner_submit_job_called_with_new_id() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway::default();
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));
            let tunnel = Arc::new(RecordingTunnelEstablisher::ok());

            let captured_id: Arc<StdMutex<Option<String>>> = Arc::new(StdMutex::new(None));
            let captured_id_for_closure = Arc::clone(&captured_id);
            let wrap_gen: WrapperScriptGenerator = Arc::new(move |spec: &SecondarySpawnSpec| {
                *captured_id_for_closure.lock().unwrap() = Some(spec.new_secondary_id.clone());
                Ok(format!(
                    "#!/bin/sh\n# wrapper for {}\n",
                    spec.new_secondary_id
                ))
            });

            let spawner = SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                None,
            );

            spawner
                .spawn(make_spec("sec-replacement-7"))
                .await
                .expect("spawn must succeed");

            // (a) wrapper generator received the new id.
            assert_eq!(
                captured_id.lock().unwrap().as_deref(),
                Some("sec-replacement-7"),
            );

            // (b) sbatch was invoked with --job-name=<new_id> (no prefix).
            let mgr_locked = mgr.lock().await;
            let cmds = mgr_locked.gateway().commands();
            let sbatch = cmds
                .iter()
                .find(|c| c.contains("| sbatch "))
                .expect("sbatch command must have been issued");
            assert!(
                sbatch.contains("--job-name=sec-replacement-7"),
                "new id must propagate into sbatch --job-name when no \
                 prefix is set; got: {sbatch}",
            );

            // (c) `--nodes=1` per spec (1-node allocation for one
            // secondary), matches the brief's "1-node sbatch" contract.
            assert!(
                sbatch.contains("--nodes=1"),
                "respawn must request exactly 1 node; got: {sbatch}",
            );

            // (d) `--no-requeue` on every framework sbatch — a requeued
            // respawn job would return under the SAME id and be refused
            // re-admission, squatting a node to its give-up.
            assert!(
                sbatch.contains("--no-requeue"),
                "respawn sbatch must carry --no-requeue; got: {sbatch}",
            );

            // (e) No `--exclude` when the spec carries no dead node — the
            // common no-node-known case must not emit a blank flag (which
            // hard-errors sbatch).
            assert!(
                !sbatch.contains("--exclude"),
                "no --exclude when exclude_node is None; got: {sbatch}",
            );
        })
        .await;
}

/// A respawn whose spec NAMES a dead member must resolve that member's
/// SLURM node from SLURM's own vocabulary (job id → squeue `%N`) and put
/// it on the replacement's sbatch as `--exclude=<resolved-node>` — NOT a
/// mesh-advertised hostname. The resolved value (`krater04`) differs
/// from the dead member's id (`secondary-0`), so the test proves the
/// argv carries the SLURM name, not the member id. Pairs with the
/// omit-when-None assertion in `slurm_spawner_submit_job_called_with_new_id`.
#[tokio::test]
async fn slurm_spawner_resolves_and_passes_exclude_node_to_sbatch() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway {
                squeue_node: "krater04".into(),
                ..RecordingGateway::default()
            };
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));
            let tunnel = Arc::new(RecordingTunnelEstablisher::ok());
            let wrap_gen: WrapperScriptGenerator =
                Arc::new(|spec: &SecondarySpawnSpec| Ok(format!("#!/bin/sh\n# {}\n", spec.new_secondary_id)));

            // Register the dead member's job so the resolver can map
            // `secondary-0 → job id → squeue %N`, exactly as the initial
            // cohort submit-loop would have.
            mgr.lock()
                .await
                .submit_job(
                    "#!/bin/sh\n# dead\n",
                    "asm-secondary-0",
                    "secondary-0",
                    1,
                    "/srv/slurm-test/log/run-1",
                    None,
                )
                .await
                .expect("seeding the dead member's job must succeed");

            let spawner = SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                None,
            );

            spawner
                .spawn(make_spec_with_dead_member("sec-replacement-9", "secondary-0"))
                .await
                .expect("spawn must succeed");

            let mgr_locked = mgr.lock().await;
            let cmds = mgr_locked.gateway().commands();
            // The replacement's sbatch is the LAST `| sbatch ` (the dead
            // member's seed-submit is the first).
            let sbatch = cmds
                .iter()
                .rev()
                .find(|c| c.contains("| sbatch "))
                .expect("sbatch command must have been issued");
            assert!(
                sbatch.contains("--exclude=krater04"),
                "resolved SLURM node must propagate into sbatch --exclude; got: {sbatch}",
            );
            assert!(
                !sbatch.contains("secondary-0") || !sbatch.contains("--exclude=secondary-0"),
                "the dead member id must not be used as the exclude node; got: {sbatch}",
            );
        })
        .await;
}

/// A respawn whose dead member cannot be resolved to a SLURM node (no
/// `secondary_jobs` entry → no job id → no squeue/sacct node) must NOT
/// emit `--exclude` and the spawn must still proceed: the exclusion is a
/// best-effort hint, never a spawn prerequisite.
#[tokio::test]
async fn slurm_spawner_omits_exclude_when_dead_member_unresolvable() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway::default();
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));
            let tunnel = Arc::new(RecordingTunnelEstablisher::ok());
            let wrap_gen: WrapperScriptGenerator =
                Arc::new(|spec: &SecondarySpawnSpec| Ok(format!("#!/bin/sh\n# {}\n", spec.new_secondary_id)));

            let spawner = SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                None,
            );

            // Name a dead member the manager never submitted a job for.
            spawner
                .spawn(make_spec_with_dead_member("sec-replacement-9", "secondary-99"))
                .await
                .expect("spawn must succeed even when the node is unresolvable");

            let mgr_locked = mgr.lock().await;
            let cmds = mgr_locked.gateway().commands();
            let sbatch = cmds
                .iter()
                .find(|c| c.contains("| sbatch "))
                .expect("sbatch command must have been issued");
            assert!(
                !sbatch.contains("--exclude"),
                "an unresolvable dead member must omit --exclude; got: {sbatch}",
            );
        })
        .await;
}

/// When sbatch fails (non-zero rc), `spawn()` must surface the
/// failure as `SpawnError::Other` carrying the manager's error
/// rendering. Two things matter:
///
///   (a) the error is `SpawnError::Other` (the
///       provider-unavailable / timeout variants are reserved for
///       structurally different failure modes — sbatch returning a
///       non-zero rc is "other"); and
///   (b) the tunnel was NOT attempted (no point bringing up a
///       reverse tunnel for a job that never made it past
///       submission). This second invariant is what keeps the
///       respawn flow's failure-budget arithmetic correct upstream.
#[tokio::test]
async fn slurm_spawner_returns_spawn_error_on_sbatch_failure() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway {
                sbatch_fails: true,
                ..RecordingGateway::default()
            };
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));
            let tunnel = Arc::new(RecordingTunnelEstablisher::ok());

            let wrap_gen: WrapperScriptGenerator =
                Arc::new(|_spec: &SecondarySpawnSpec| Ok("#!/bin/sh\necho hi\n".to_string()));

            let spawner = SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                None,
            );

            let err = spawner
                .spawn(make_spec("sec-replacement-8"))
                .await
                .expect_err("sbatch failure must surface");
            match err {
                SpawnError::Other(msg) => {
                    assert!(
                        msg.starts_with("sbatch: "),
                        "sbatch failure must be tagged with the 'sbatch:' prefix; got: {msg}",
                    );
                    assert!(
                        msg.contains("simulated failure"),
                        "underlying sbatch stderr must be preserved; got: {msg}",
                    );
                }
                other => panic!("expected SpawnError::Other, got {other:?}"),
            }

            // Tunnel was NOT attempted — the recorder counts every entry,
            // so zero calls proves the failure short-circuited before the
            // tunnel step.
            assert_eq!(
                tunnel.calls(),
                0,
                "tunnel establishment must not run when sbatch failed",
            );

            // Rule 1 (#543): respawning must NEVER cancel a job — not on
            // the happy path AND not when sbatch fails.
            let cmds = mgr.lock().await.gateway().commands();
            assert!(
                cmds.iter().all(|c| !c.starts_with("scancel ")),
                "respawn must NEVER scancel a job (rule 1); got: {cmds:?}",
            );
        })
        .await;
}

/// On the happy path, the tunnel port must be invoked AFTER
/// `submit_job` returns Ok. We assert this by:
///
///   (a) the recorder logged exactly 1 call (proves the
///       tunnel-establishment path ran);
///   (b) the recorded id is `spec.new_secondary_id` (proves
///       the tunnel was for the right secondary, not a stale id
///       from a prior call); and
///   (c) the sbatch command was in the gateway's command log
///       (cross-check that submission did happen).
#[tokio::test]
async fn slurm_spawner_invokes_establish_one_tunnel_after_submit() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway::default();
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));
            let tunnel = Arc::new(RecordingTunnelEstablisher::ok());

            let wrap_gen: WrapperScriptGenerator =
                Arc::new(|_spec: &SecondarySpawnSpec| Ok("#!/bin/sh\necho hi\n".to_string()));

            let spawner = SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                None,
            );

            spawner
                .spawn(make_spec("sec-replacement-9"))
                .await
                .expect("spawn must succeed");

            // (a) tunnel-establishment path ran exactly once.
            assert_eq!(
                tunnel.calls(),
                1,
                "establish_one_tunnel must be invoked exactly once per spawn",
            );

            // (b) the recorded id is the spec's new_secondary_id.
            assert_eq!(
                tunnel.last_id().as_deref(),
                Some("sec-replacement-9"),
                "tunnel must target the new secondary's id",
            );

            // (c) submission happened (sbatch line in the command log).
            let mgr_locked = mgr.lock().await;
            let cmds = mgr_locked.gateway().commands();
            assert!(
                cmds.iter().any(|c| c.contains("| sbatch ")),
                "submit_job must have issued an sbatch command",
            );

            // Rule 1 (#543): happy-path respawn issues ZERO scancel
            // commands. The over-allocation case is structurally
            // tolerated (see [[feedback_at_least_once_execution_deliberate]]),
            // never resolved by killing a job.
            assert!(
                cmds.iter().all(|c| !c.starts_with("scancel ")),
                "respawn happy path must NEVER scancel a job (rule 1); got: {cmds:?}",
            );
        })
        .await;
}

/// Rule 3 (#543): a respawned secondary's `--job-name` is composed from
/// the consumer-set prefix captured at startup plus the new secondary
/// id. Operators eyeballing `squeue` see consistent naming across
/// initial and respawned cohorts.
#[tokio::test]
async fn slurm_spawner_uses_consumer_job_name_prefix_when_set() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway::default();
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));
            let tunnel = Arc::new(RecordingTunnelEstablisher::ok());
            let wrap_gen: WrapperScriptGenerator =
                Arc::new(|_spec: &SecondarySpawnSpec| Ok("#!/bin/sh\necho hi\n".to_string()));

            let spawner = SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                Some("asm-2026".into()),
            );

            spawner
                .spawn(make_spec("sec-replacement-7"))
                .await
                .expect("spawn must succeed");

            let mgr_locked = mgr.lock().await;
            let cmds = mgr_locked.gateway().commands();
            let sbatch = cmds
                .iter()
                .find(|c| c.contains("| sbatch "))
                .expect("sbatch command must have been issued");
            assert!(
                sbatch.contains("--job-name=asm-2026-sec-replacement-7"),
                "consumer prefix must compose with the new id; got: {sbatch}",
            );
        })
        .await;
}

/// Orphan-safety contract: after `spawn()` has submitted sbatch
/// (the inner task pushed the job_id onto `job_manager.job_ids`),
/// dropping the outer future mid-tunnel-setup must NOT lose the
/// job_id from the manager — the coordinator's later `cleanup()`
/// scancels the orphan exactly because the id is recorded there.
#[tokio::test]
async fn slurm_spawner_orphan_sbatch_recorded_in_job_ids_after_shutdown_abort() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let gw = RecordingGateway::default();
            let cfg = SlurmConfig {
                root_folder: "/srv/slurm-test".into(),
                ..SlurmConfig::default()
            };
            let mgr = Arc::new(Mutex::new(SlurmJobManager::new(cfg, gw)));

            // Gate the tunnel step so we can observe the post-sbatch /
            // pre-tunnel window. The sender is dropped by the test at
            // the right moment to release the inner task.
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            let tunnel = Arc::new(RecordingTunnelEstablisher::gated(gate_rx));

            let wrap_gen: WrapperScriptGenerator =
                Arc::new(|_spec: &SecondarySpawnSpec| Ok("#!/bin/sh\necho hi\n".to_string()));

            let spawner = Arc::new(SlurmSecondarySpawner::new(
                Arc::clone(&mgr),
                Arc::clone(&tunnel),
                wrap_gen,
                "/srv/slurm-test/log/run-1".into(),
                None,
            ));

            // Mirror the operational-loop shape: the outer `spawn()`
            // future lives on a separate spawn_local (the JoinSet's
            // task). When we abort that JoinHandle, only the outer
            // future is cancelled — the inner spawn_local the
            // production code spawned during `spawn()` survives.
            let spawner_for_task = Arc::clone(&spawner);
            let outer_handle = tokio::task::spawn_local(async move {
                spawner_for_task.spawn(make_spec("sec-orphan-test")).await
            });

            // Yield until sbatch has run.
            let sbatch_seen = async {
                loop {
                    {
                        let mgr_locked = mgr.lock().await;
                        if mgr_locked
                            .gateway()
                            .commands()
                            .iter()
                            .any(|c| c.contains("| sbatch "))
                        {
                            break;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            };
            sbatch_seen.await;

            // Confirm the orphan-safety pre-condition: the job_id is
            // already on `job_ids` immediately after `submit_job`
            // returns. This pins the invariant the production code
            // relies on for the post-abort scancel path.
            {
                let mgr_locked = mgr.lock().await;
                assert_eq!(
                    mgr_locked.job_ids(),
                    &["67890".to_string()],
                    "submit_job must push the id onto job_ids before \
                 yielding control back to the spawner",
                );
            }

            // Abort the outer future (mirrors `JoinSet::shutdown`).
            outer_handle.abort();
            tokio::task::yield_now().await;

            // Release the gate so the inner task can finish its tunnel step.
            let _ = gate_tx.send(());

            // Yield a few times so the inner task drains.
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }

            // The orphan-safety contract: the id is still on
            // `job_ids` so the coordinator's `cleanup()` can scancel
            // it on drop. Plus: the tunnel establishment ran (the
            // inner task wasn't aborted along with the outer).
            let mgr_locked = mgr.lock().await;
            assert_eq!(
                mgr_locked.job_ids(),
                &["67890".to_string()],
                "post-abort, job_id must remain on job_ids so \
             cleanup() can scancel the orphan",
            );
            assert_eq!(
                tunnel.calls(),
                1,
                "the inner task must keep running after outer abort \
             (proves the spawn_local detach)",
            );
        })
        .await;
}
