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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use super::super::types::{CancelVerifyPolicy, SlurmJobManager};
use crate::config::SlurmConfig;
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

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
