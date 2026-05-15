//! SLURM provider implementation of [`SecondarySpawner`].
//!
//! Single concern: turn a [`SecondarySpawnSpec`] from the
//! `dynrunner-manager-distributed` operational loop into the SLURM
//! provider's triple of operations:
//!
//!   1. **Wrapper-script synthesis** for the new secondary id (delegated
//!      to a caller-supplied closure, because rendering a
//!      [`WrapperScriptConfig`](crate::wrapper_script::WrapperScriptConfig)
//!      requires deployment-specific context — image paths, mount
//!      sources, dispatcher argv — that this module intentionally does
//!      not own).
//!
//!   2. **sbatch submission** via [`SlurmJobManager::submit_job`] on a
//!      1-node allocation, using `spec.new_secondary_id` as the SLURM
//!      job name so operators eyeballing `squeue` see the same id the
//!      framework's respawn-event ring carries.
//!
//!   3. **Reverse-tunnel establishment** via the [`TunnelEstablisher`]
//!      port (production-bound to
//!      [`SlurmPreparation::establish_one_tunnel`]). The port keeps the
//!      spawner from depending on the concrete `SlurmPreparation`
//!      struct, so trait-contract tests can drive `spawn()` against a
//!      no-op tunnel without spinning up a real `ssh -N -R`. The pool
//!      / rate-limiter / retry-budget invariants are still shared with
//!      the initial `setup_ssh_tunnels` loop because production wires
//!      the SAME `Arc<SlurmPreparation>` into the port.
//!
//! API boundary crossing: this file implements the
//! [`SecondarySpawner`] trait from
//! [`dynrunner_manager_distributed::primary::respawn`]. Callers
//! upstream (the primary coordinator) hold a `dyn SecondarySpawner` and
//! never see any of the SLURM-specific types listed above.
//!
//! Why a caller-supplied wrapper-script closure (option (a) from the
//! design sketch) rather than a direct call to
//! [`generate_wrapper_script`](crate::wrapper_script::generate_wrapper_script)
//! (option (b)): a [`WrapperScriptConfig`] has ~20 deployment-specific
//! fields (image path, container command, cores spec, mount sources,
//! forwarded argv, …). Capturing the constant portion in the closure
//! at wire-up time lets `spawn()` stay parameterised purely over the
//! per-respawn id, with no special-casing for "which fields change per
//! respawn vs. stay constant across the run". The closure crosses the
//! boundary cleanly: `Fn(&SecondarySpawnSpec) -> Result<String, _>`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use dynrunner_gateway::traits::Gateway;
use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawnSpec, SecondarySpawner, SpawnError,
};
use tokio::sync::Mutex;

use crate::job_manager::SlurmJobManager;
use crate::preparation::{InfoFileReader, PrepError, SlurmPreparation};

/// Closure that synthesises a SLURM wrapper-script body for a given
/// respawn spec. Returns the script content (not a path); the
/// [`SlurmJobManager::submit_job`] call below writes it to the gateway
/// at `<root_folder>/job_<job_name>.sh` and submits via
/// `sbatch --parsable`.
///
/// `Send + Sync` because the spawner is shared via `Arc<dyn
/// SecondarySpawner>` and the trait method takes `&self`.
pub type WrapperScriptGenerator =
    Arc<dyn Fn(&SecondarySpawnSpec) -> Result<String, String> + Send + Sync>;

/// Port that brings up the reverse SSH tunnel for a just-spawned
/// secondary. Production wires this to
/// [`SlurmPreparation::establish_one_tunnel`] via the blanket impl in
/// this module; tests pass an in-memory stub so the spawner contract
/// can be exercised without `ssh -N -R`.
///
/// Single concern: "given a new secondary id, ensure the reverse
/// tunnel is up". The spawner does not need to know about the
/// connection-info polling, the `Semaphore` rate-limiter, or the
/// shared `ssh_tunnels` cleanup Vec — those are owned by the
/// production implementation.
///
/// `&self` (not `&mut self`) matches the underlying
/// `establish_one_tunnel` shape, which already runs under `&self`
/// thanks to the `Arc<StdMutex<...>>` shared state inside
/// `SlurmPreparation`.
pub trait TunnelEstablisher: Send + Sync {
    fn establish_one_tunnel<'a>(
        &'a self,
        secondary_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PrepError>> + 'a>>;
}

/// Production binding of [`TunnelEstablisher`] to
/// [`SlurmPreparation::establish_one_tunnel`]. Captures the
/// `InfoFileReader` once (the trait method's signature does not carry
/// the reader, so it has to live on the binding) and re-uses the
/// shared `Arc<SlurmPreparation>` so respawn tunnels join the same
/// cleanup set as the initial cohort.
pub struct SlurmPreparationTunnelEstablisher<R: InfoFileReader + Send + Sync> {
    preparation: Arc<SlurmPreparation>,
    info_reader: R,
}

impl<R: InfoFileReader + Send + Sync> SlurmPreparationTunnelEstablisher<R> {
    pub fn new(preparation: Arc<SlurmPreparation>, info_reader: R) -> Self {
        Self {
            preparation,
            info_reader,
        }
    }
}

impl<R: InfoFileReader + Send + Sync> TunnelEstablisher
    for SlurmPreparationTunnelEstablisher<R>
{
    fn establish_one_tunnel<'a>(
        &'a self,
        secondary_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PrepError>> + 'a>> {
        let reader = self.info_reader.clone();
        Box::pin(async move {
            self.preparation
                .establish_one_tunnel(secondary_id, reader)
                .await
        })
    }
}

/// SLURM provider implementation of [`SecondarySpawner`].
///
/// Composes the existing SLURM collaborators (`SlurmJobManager`,
/// the `TunnelEstablisher` port, and the wrapper-script generator)
/// into the trait surface that `dynrunner-manager-distributed`
/// consumes.
pub struct SlurmSecondarySpawner<G: Gateway, T: TunnelEstablisher> {
    /// `SlurmJobManager` shared with whatever else drives sbatch on
    /// this run (typically the pipeline's initial-setup path). Wrapped
    /// in `tokio::sync::Mutex` because `submit_job` takes `&mut self`
    /// (it pushes onto `job_ids`); the lock is held across an await.
    job_manager: Arc<Mutex<SlurmJobManager<G>>>,
    /// Reverse-tunnel port. Production binds to `SlurmPreparation`;
    /// tests bind to an in-memory recorder. The spawner sees only
    /// "give me a tunnel for this id".
    tunnel_establisher: Arc<T>,
    /// Per-spec wrapper-script body generator. Bound at wire-up time
    /// so `spawn()` stays parameterised purely over the spec.
    wrapper_script_generator: WrapperScriptGenerator,
    /// `run_log_dir` forwarded into `submit_job` for the
    /// `--output=`/`--error=` paths. Same shape the initial-setup path
    /// uses; captured here so the per-respawn call site doesn't have
    /// to re-derive it.
    run_log_dir: String,
}

impl<G: Gateway, T: TunnelEstablisher> SlurmSecondarySpawner<G, T> {
    /// Construct a SLURM spawner. In production, all collaborators
    /// must already be initialised — in particular, the
    /// `TunnelEstablisher` must wrap a `SlurmPreparation` that has
    /// had `setup_ssh_tunnels` called at least once so its
    /// `primary_quic_port` is captured (see
    /// [`SlurmPreparation::establish_one_tunnel`] for why).
    pub fn new(
        job_manager: Arc<Mutex<SlurmJobManager<G>>>,
        tunnel_establisher: Arc<T>,
        wrapper_script_generator: WrapperScriptGenerator,
        run_log_dir: String,
    ) -> Self {
        Self {
            job_manager,
            tunnel_establisher,
            wrapper_script_generator,
            run_log_dir,
        }
    }
}

#[async_trait(?Send)]
impl<G, T> SecondarySpawner for SlurmSecondarySpawner<G, T>
where
    G: Gateway + Send + Sync + 'static,
    T: TunnelEstablisher + 'static,
{
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError> {
        // (1) Synthesise the wrapper script body for this respawn id.
        // The generator owns the deployment-context capture; any
        // failure (template-render, missing config, …) lands here as
        // an opaque string so the trait's `SpawnError` variants stay
        // provider-agnostic.
        let wrapper_script = (self.wrapper_script_generator)(&spec)
            .map_err(|e| SpawnError::Other(format!("wrapper-gen: {e}")))?;

        // (2) Submit a 1-node sbatch. `submit_job` owns the script-
        // write + sbatch invocation; we just hand it the script body
        // and the new secondary id (used as both the SLURM job-name
        // and the wrapper-script filename suffix). The lock is held
        // across the entire submission so concurrent respawn callers
        // serialise their sbatch invocations through the same manager
        // — matches the existing pipeline contract (the manager owns
        // `job_ids` mutation under `&mut self`).
        let job_id = {
            let mut mgr = self.job_manager.lock().await;
            mgr.submit_job(&wrapper_script, &spec.new_secondary_id, 1, &self.run_log_dir)
                .await
                .map_err(|e| SpawnError::Other(format!("sbatch: {e}")))?
        };

        tracing::info!(
            new_secondary_id = %spec.new_secondary_id,
            job_id = %job_id,
            "respawn sbatch submitted; awaiting tunnel",
        );

        // (3) Bring up the reverse tunnel for the new secondary via
        // the `TunnelEstablisher` port. Production binds this to
        // `SlurmPreparation::establish_one_tunnel`, which polls the
        // connection-info file the compute node writes and spawns the
        // verified `ssh -N -R` into the shared `ssh_tunnels` Vec.
        self.tunnel_establisher
            .establish_one_tunnel(&spec.new_secondary_id)
            .await
            .map_err(|e| SpawnError::Other(format!("tunnel: {e}")))?;

        tracing::info!(
            new_secondary_id = %spec.new_secondary_id,
            job_id = %job_id,
            "respawn tunnel established",
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Trait-contract tests for the SLURM spawner.
    //!
    //! Strategy: drive `spawn()` against a recording `Gateway` that
    //! captures the `sbatch ...` invocation (so we can assert the new
    //! secondary id is propagated) and against a fail-on-sbatch
    //! gateway (so we can assert `SpawnError` surfaces). The tunnel
    //! step is mocked via a `RecordingTunnelEstablisher` that counts
    //! calls and records the id it was invoked with — no real ssh /
    //! sbatch / podman is touched.
    use super::*;
    use crate::config::SlurmConfig;
    use crate::preparation::PrepError;
    use dynrunner_gateway::traits::{CommandResult, GatewayError};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    /// Recording gateway: every `execute_command` is captured, sbatch
    /// is answered with a canned job ID and (optionally) configurable
    /// failure. Routing by command-prefix matches `job_manager.rs`'s
    /// `SubmitRecordingGateway` shape so future `submit_job` setup
    /// commands don't break these tests.
    #[derive(Default)]
    struct RecordingGateway {
        commands: StdMutex<Vec<String>>,
        sbatch_fails: bool,
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
            let is_sbatch = cmd.starts_with("sbatch ");
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
    struct RecordingTunnelEstablisher {
        calls: AtomicUsize,
        last_id: StdMutex<Option<String>>,
        fail_with: StdMutex<Option<PrepError>>,
    }

    impl RecordingTunnelEstablisher {
        fn ok() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                last_id: StdMutex::new(None),
                fail_with: StdMutex::new(None),
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
            Box::pin(async move {
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
        }
    }

    /// `spawn()` must thread `spec.new_secondary_id` into both the
    /// wrapper-script generator (so the script's secondary id matches)
    /// AND the `--job-name=` argument of the sbatch invocation (so
    /// operators eyeballing `squeue` see the same id as the
    /// respawn-event ring). This locks down both propagation paths in
    /// a single test rather than splitting them — the same id must
    /// reach both places or the respawn is broken.
    #[tokio::test]
    async fn slurm_spawner_submit_job_called_with_new_id() {
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
            Ok(format!("#!/bin/sh\n# wrapper for {}\n", spec.new_secondary_id))
        });

        let spawner = SlurmSecondarySpawner::new(
            Arc::clone(&mgr),
            Arc::clone(&tunnel),
            wrap_gen,
            "/srv/slurm-test/log/run-1".into(),
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

        // (b) sbatch was invoked with --job-name=<new_id>.
        let mgr_locked = mgr.lock().await;
        let cmds = mgr_locked.gateway().commands();
        let sbatch = cmds
            .iter()
            .find(|c| c.starts_with("sbatch "))
            .expect("sbatch command must have been issued");
        assert!(
            sbatch.contains("--job-name=sec-replacement-7"),
            "new id must propagate into sbatch --job-name; got: {sbatch}",
        );

        // (c) `--nodes=1` per spec (1-node allocation for one
        // secondary), matches the brief's "1-node sbatch" contract.
        assert!(
            sbatch.contains("--nodes=1"),
            "respawn must request exactly 1 node; got: {sbatch}",
        );
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
    ///
    /// Ordering between (a) and (c) is implicit: `spawn()`'s code
    /// path runs them sequentially under a single `await` chain, so
    /// "both happened" is equivalent to "submit_job ran first" given
    /// the sbatch-failure test above pinned the failure-short-circuit
    /// branch.
    #[tokio::test]
    async fn slurm_spawner_invokes_establish_one_tunnel_after_submit() {
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
            cmds.iter().any(|c| c.starts_with("sbatch ")),
            "submit_job must have issued an sbatch command",
        );
    }
}
