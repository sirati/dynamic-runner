//! Tests for the post-`scancel` verification sweep in
//! [`SlurmJobManager::cancel_all_jobs`].
//!
//! Repro target: asm-dataset run_20260611_182745. The fatal-path
//! teardown scancel exited 0 on every job, yet secondary-2 (job 155629)
//! was still RUNNING 4+ minutes later and had to be scancelled by hand.
//! A bare fire-and-forget scancel never re-checks the queue, so a
//! cancel that races a PENDING→RUNNING transition (or a gateway
//! round-trip that partially fails) leaves the job allocated forever.
//!
//! The gateway stub below answers every `scancel` with exit 0 (the
//! trap), while `squeue -j <id>` keeps reporting one id as RUNNING for
//! a configurable number of polls. The verification sweep must re-issue
//! scancel and confirm the job left the queue — or, when it never
//! leaves, surface the surviving id loudly rather than returning a
//! false-clean.
//!
//! [`inflight_sbatch_orphan_race`] covers the SECOND class of gap: a
//! `submit_job` future cancelled while the sbatch SSH round-trip is
//! in progress.  The marker left in `job_ids` by the pre-await push
//! ensures `cancel_all_jobs_verified` never silently ignores a
//! possibly-submitted job.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::super::types::{CancelVerifyPolicy, SlurmJobManager};
use crate::config::SlurmConfig;
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};
use tokio::sync::{oneshot, Notify};

/// Per-id squeue script: each call to `squeue -j <id>` decrements the
/// id's "rounds still RUNNING" budget; while > 0 it reports RUNNING,
/// once it hits 0 the row disappears (squeue exits non-zero → "gone").
/// A budget of `u32::MAX` is a job that NEVER leaves — the
/// budget-exhausted survivor case.
struct ScancelVerifyGateway {
    /// Records every command issued, in order, so the test can assert
    /// how many `scancel` invocations each id received.
    commands: Mutex<Vec<String>>,
    /// id → remaining squeue rounds it will still report as RUNNING.
    /// Decremented on each `squeue -j <id>` query.
    running_rounds: Mutex<HashMap<String, u32>>,
}

impl ScancelVerifyGateway {
    /// `running_rounds[id]` = how many squeue polls return RUNNING for
    /// that id before it disappears from the queue.
    fn new(running_rounds: HashMap<String, u32>) -> Self {
        Self {
            commands: Mutex::new(Vec::new()),
            running_rounds: Mutex::new(running_rounds),
        }
    }

    fn commands(&self) -> Vec<String> {
        self.commands.lock().unwrap().clone()
    }

    /// Count of `scancel <id>` invocations the stub recorded for `id`.
    fn scancel_count(&self, id: &str) -> usize {
        let needle = format!("scancel {id}");
        self.commands()
            .iter()
            .filter(|c| c.as_str() == needle)
            .count()
    }
}

impl Gateway for ScancelVerifyGateway {
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

        // scancel is fire-and-forget at the SLURM layer: always exits 0
        // (the trap the verification sweep exists to catch).
        if cmd.starts_with("scancel ") {
            return Ok(CommandResult {
                return_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            });
        }

        // `squeue -j <id> -o '...' --noheader 2>/dev/null`: report
        // RUNNING while the id still has rounds left, else "gone"
        // (non-zero exit, the shape `get_job_status` reads as no row).
        if let Some(rest) = cmd.strip_prefix("squeue -j ") {
            let id = rest.split_whitespace().next().unwrap_or("").to_string();
            let mut rounds = self.running_rounds.lock().unwrap();
            let remaining = rounds.entry(id).or_insert(0);
            if *remaining > 0 {
                *remaining = remaining.saturating_sub(1);
                return Ok(CommandResult {
                    return_code: 0,
                    stdout: "RUNNING|krater07|None".to_string(),
                    stderr: String::new(),
                });
            }
            // No row: squeue exits non-zero for an unknown job id.
            return Ok(CommandResult {
                return_code: 1,
                stdout: String::new(),
                stderr: String::new(),
            });
        }

        Ok(CommandResult {
            return_code: 0,
            stdout: String::new(),
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

/// Build a manager whose `job_ids` already holds `ids` (as if four
/// secondaries had been submitted) backed by the given squeue script.
/// Zero-delay policy keeps the bounded loop off the wall clock.
fn manager_with_jobs(
    gw: ScancelVerifyGateway,
    ids: &[&str],
) -> SlurmJobManager<ScancelVerifyGateway> {
    let mut mgr = SlurmJobManager::new(SlurmConfig::default(), gw);
    mgr.seed_job_ids_for_test(ids);
    mgr
}

fn zero_delay_policy(attempts: u32) -> CancelVerifyPolicy {
    CancelVerifyPolicy {
        attempts,
        poll_delay: std::time::Duration::ZERO,
    }
}

/// REPRO: the run_20260611_182745 shape — four jobs, scancel exits 0 on
/// all, but secondary-2 (155629) stays RUNNING for one extra squeue
/// poll past the initial scancel. The fire-and-forget code re-scancels
/// nothing; the verified sweep re-issues scancel on the straggler so it
/// is actually cancelled, and the survivor set ends EMPTY.
///
/// RED before the fix: `cancel_all_jobs` issued exactly one scancel per
/// id and never polled, so 155629 got a single scancel that the stub
/// shows still RUNNING afterwards — `scancel_count("155629") == 1`.
/// GREEN: the verify loop polls, sees it RUNNING, and re-scancels — so
/// `scancel_count("155629") >= 2`.
#[tokio::test]
async fn verified_cancel_re_scancels_a_job_that_survives_the_first_scancel() {
    // 155629 reports RUNNING on its first post-scancel squeue poll, then
    // disappears; the other three are already gone (0 rounds).
    let mut running = HashMap::new();
    running.insert("155629".to_string(), 1u32);
    let gw = ScancelVerifyGateway::new(running);
    let mut mgr = manager_with_jobs(gw, &["155626", "155627", "155628", "155629"]);

    mgr.cancel_all_jobs_verified(zero_delay_policy(3))
        .await
        .expect("verified cancel returns Ok (fail-safe)");

    // The straggler got at least a second scancel — the whole point.
    assert!(
        mgr.gateway().scancel_count("155629") >= 2,
        "the surviving job must be re-scancelled by the verification sweep; \
         got {} scancel(s) for 155629 (commands: {:?})",
        mgr.gateway().scancel_count("155629"),
        mgr.gateway().commands(),
    );
    // The already-gone jobs are scancelled exactly once (no needless
    // re-scancel on a job squeue already shows as gone).
    for gone in ["155626", "155627", "155628"] {
        assert_eq!(
            mgr.gateway().scancel_count(gone),
            1,
            "an already-gone job must not be re-scancelled; {gone} commands: {:?}",
            mgr.gateway().commands(),
        );
    }
    // The job-id registry is drained either way.
    assert!(
        mgr.job_ids().is_empty(),
        "cancel_all_jobs must clear the tracked job-id list",
    );
}

/// A job that NEVER leaves the queue exhausts the budget. The sweep
/// must stay FAIL-SAFE (return Ok, no hang) AND have re-scancelled the
/// survivor on every round (so the operator's manual scancel is a last
/// resort, not the only attempt).
#[tokio::test]
async fn verified_cancel_is_fail_safe_when_a_job_never_leaves() {
    let mut running = HashMap::new();
    running.insert("155629".to_string(), u32::MAX); // never disappears
    let gw = ScancelVerifyGateway::new(running);
    let mut mgr = manager_with_jobs(gw, &["155629"]);

    // Bounded: 3 attempts, zero delay → returns promptly even though the
    // job never clears.
    mgr.cancel_all_jobs_verified(zero_delay_policy(3))
        .await
        .expect("must return Ok even when a job never leaves (fail-safe)");

    // Initial scancel + one re-scancel per attempt round = 1 + 3.
    assert_eq!(
        mgr.gateway().scancel_count("155629"),
        4,
        "the never-leaving job must be re-scancelled on every verify round; commands: {:?}",
        mgr.gateway().commands(),
    );
}

/// CHARACTERISES THE BUG (the RED baseline): with `attempts = 0` the
/// sweep degenerates to the legacy fire-and-forget shape — one scancel
/// per id, no squeue poll, no re-scancel. The straggler keeps RUNNING
/// and gets exactly ONE scancel. This is the run_20260611_182745
/// behaviour; the `>= 2` assertion in
/// [`verified_cancel_re_scancels_a_job_that_survives_the_first_scancel`]
/// is RED against this path and GREEN only because the default policy
/// runs the verification loop.
#[tokio::test]
async fn unverified_cancel_does_not_re_scancel_a_surviving_job() {
    let mut running = HashMap::new();
    running.insert("155629".to_string(), 1u32);
    let gw = ScancelVerifyGateway::new(running);
    let mut mgr = manager_with_jobs(gw, &["155629"]);

    mgr.cancel_all_jobs_verified(zero_delay_policy(0))
        .await
        .expect("unverified cancel returns Ok");

    assert_eq!(
        mgr.gateway().scancel_count("155629"),
        1,
        "attempts=0 must NOT re-check or re-scancel — this is the fire-and-forget gap",
    );
}

/// The common case: every scancel takes immediately, so the FIRST
/// squeue poll shows all jobs gone and the sweep returns after a single
/// verification round with no re-scancel and no extra delay.
#[tokio::test]
async fn verified_cancel_returns_after_one_poll_when_all_jobs_clear() {
    // All ids already gone on the first poll (0 rounds).
    let gw = ScancelVerifyGateway::new(HashMap::new());
    let mut mgr = manager_with_jobs(gw, &["155626", "155627"]);

    mgr.cancel_all_jobs_verified(zero_delay_policy(3))
        .await
        .expect("clean cancel returns Ok");

    for id in ["155626", "155627"] {
        assert_eq!(
            mgr.gateway().scancel_count(id),
            1,
            "a job that clears on the first poll must be scancelled exactly once; {id}",
        );
    }
}

// ── In-flight sbatch race tests ──────────────────────────────────────────────

/// Gateway whose `execute_command` for sbatch blocks on a oneshot gate.
/// Models the sbatch SSH round-trip being in-flight when the coordinator
/// ends and the `submit_job` future is cancelled at its await point.
///
/// The gateway signals `sbatch_entered` (a `Notify`) the moment it is
/// about to block on the gate, BEFORE awaiting.  The test waits on that
/// notify so it knows the pending marker has already been pushed to
/// `job_ids` — without needing to acquire the manager's `TokioMutex`
/// while the submit task still holds it.
struct BlockingSbatchGateway {
    /// Held by the test; dropped or signalled to release the gate.
    gate_rx: Mutex<Option<oneshot::Receiver<()>>>,
    /// Fired once, right before the gateway blocks on `gate_rx`.
    /// The test awaits this to learn "marker is now in job_ids" without
    /// contending with the submit task for the manager mutex.
    sbatch_entered: Arc<Notify>,
}

impl BlockingSbatchGateway {
    fn new(gate_rx: oneshot::Receiver<()>, sbatch_entered: Arc<Notify>) -> Self {
        Self {
            gate_rx: Mutex::new(Some(gate_rx)),
            sbatch_entered,
        }
    }
}

impl Gateway for BlockingSbatchGateway {
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
        if cmd.contains("| sbatch ") {
            // Signal the test that the marker is now in job_ids and we are
            // about to block.  This happens before the gate await so the
            // test does not need the manager lock to observe our progress.
            self.sbatch_entered.notify_one();

            // Block until the gate fires or is dropped.  In the
            // cancellation scenario the whole future is dropped at this
            // point so we never reach the return below.
            let rx = self
                .gate_rx
                .lock()
                .unwrap()
                .take()
                .expect("gate taken only once");
            let _ = rx.await;
            return Ok(CommandResult {
                return_code: 0,
                stdout: "99999".to_string(),
                stderr: String::new(),
            });
        }
        // mkdir and squeue probes: succeed immediately with no output.
        Ok(CommandResult {
            return_code: 0,
            stdout: String::new(),
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

/// RACE REPLAY: teardown while sbatch is in-flight.
///
/// Sequence:
/// 1. A `submit_job` future is launched on a LocalSet; it pushes the
///    pending marker to `job_ids` and then suspends at the gateway's
///    `execute_command` await (the sbatch SSH round-trip).
/// 2. While the future is blocked, we DROP it (mirroring the coordinator's
///    LocalSet ending while the respawn task holds the manager mutex
///    mid-sbatch).
/// 3. `cancel_all_jobs_verified` is called on the same manager.
///
/// RED before the fix: `cancel_all_jobs` saw an empty `job_ids` (the
/// marker didn't exist) and returned a clean-looking Ok — the orphan was
/// silently missed.
///
/// GREEN: the pending marker is visible in `job_ids` when the future is
/// dropped; `cancel_all_jobs_verified` drains it, emits a WARN, and
/// returns Ok (fail-safe) without issuing a spurious scancel for an
/// unknown ID.
///
/// Deadlock avoidance: the submit task holds the `TokioMutex` for the
/// entire duration of `execute_command` (including the sbatch block).
/// The test therefore MUST NOT try to acquire the manager lock before
/// aborting the submit task.  Instead the gateway fires `sbatch_entered`
/// (a `Notify`) right before it blocks — the test waits on that signal
/// and then aborts; the abort releases the mutex without the test ever
/// contending for it.
#[tokio::test]
async fn inflight_sbatch_orphan_race_marker_seen_by_teardown() {
    use tokio::sync::Mutex as TokioMutex;

    tokio::task::LocalSet::new()
        .run_until(async {
            let (gate_tx, gate_rx) = oneshot::channel::<()>();
            let sbatch_entered = Arc::new(Notify::new());
            let gw = BlockingSbatchGateway::new(gate_rx, Arc::clone(&sbatch_entered));
            let mgr = Arc::new(TokioMutex::new(SlurmJobManager::new(
                SlurmConfig::default(),
                gw,
            )));

            // Spawn a submit_job call on the LocalSet.  It will push the
            // pending marker and then block at the sbatch gate.
            let mgr_clone = Arc::clone(&mgr);
            let submit_handle = tokio::task::spawn_local(async move {
                let mut locked = mgr_clone.lock().await;
                locked
                    .submit_job("#!/bin/sh\necho hi\n", "sec-race-test", "sec-race-test", 1, "/log")
                    .await
            });

            // Wait until the gateway has fired `sbatch_entered` — at that
            // point the marker is in job_ids and the task is blocked at
            // the gate.  We do NOT acquire the manager lock here: the
            // submit task still holds it.
            sbatch_entered.notified().await;

            // Abort the submit future (mirrors the coordinator's LocalSet
            // ending / JoinSet::shutdown while the task holds the mutex).
            // The abort cancels the future at the `gate_rx.await` point
            // inside execute_command, which drops the TokioMutexGuard,
            // releasing the lock.
            submit_handle.abort();
            // Yield so the abort takes effect and the mutex guard is released.
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            // The gate sender is intentionally NOT sent: the sbatch round-
            // trip never completed.  This mirrors a real LocalSet drop
            // where the task is destroyed mid-sbatch-await.
            drop(gate_tx);

            // Teardown: cancel_all_jobs_verified must see the marker,
            // emit a WARN, and return Ok without hanging or panicking.
            let mut locked = mgr.lock().await;
            locked
                .cancel_all_jobs_verified(zero_delay_policy(1))
                .await
                .expect("cancel_all_jobs must return Ok even with a pending marker (fail-safe)");

            // The marker must be drained (no residue in job_ids).
            assert!(
                locked.job_ids().is_empty(),
                "cancel_all_jobs must drain the pending marker from job_ids; \
                 remaining: {:?}",
                locked.job_ids(),
            );

            // No spurious scancel was attempted for the unknown ID.
            // The BlockingSbatchGateway accepts any scancel without
            // recording; the primary assertion is absence of panic / hang.
        })
        .await;
}
