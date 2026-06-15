//! Respawn whose SPAWN SYSCALL fails must stay under backed-off restart
//! management — never hot-loop, never go permanently dead.
//!
//! Production shape (asm-dataset, run_20260611_115429): secondaries
//! s6/7/8/13 flapped leave→rejoin; replacement wrapper jobs gutted the
//! still-live containers' rootfs (the preflight orphan-sweep defect,
//! fixed in `slurm-wrapper`), after which every per-type respawn's
//! `Command::spawn` failed with "failed to exec worker N: No such file
//! or directory (os error 2)". Each affected slot logged the ERROR a
//! couple of times — once from the dispatch edge, once from the restart
//! tail — and then went PERMANENTLY dead: the restart-tail's `Err` arm
//! dropped the slot from `pending_worker_restarts` without
//! rescheduling, and a spawn-syscall failure never fed the
//! startup-crash streak (no child generation was ever created), so any
//! reschedule that DID happen ran at zero delay (the surviving slot
//! handle still read `ever_ready=true` from the healthy pre-failure
//! subprocess).
//!
//! The contract pinned here, replaying that trace through the REAL
//! `process_tasks` loop with a factory whose typed spawn returns the
//! verbatim exec-ENOENT error:
//!   1. the loop stays bounded (no zero-delay respawn hot-loop),
//!   2. spawn ATTEMPTS are bounded (the startup-crash backoff brakes
//!      exec-failures exactly like dead-after-spawn children), and
//!   3. the slot REMAINS scheduled for restart at window end (a broken
//!      exec context heals — e.g. a remounted storage root — and the
//!      slot must heal with it, on the calm #370 cadence).

#![cfg(test)]

use super::super::test_helpers::{TestId, make_secondary_recording};
use super::super::*;
use dynrunner_core::{MessageReceiver, MessageSender, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_protocol_primary_secondary::{DistributedBinaryInfo, DistributedMessage};
use dynrunner_transport_channel::{ChannelManagerEnd, channel_pair};
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

/// Single-worker production-shaped config (mirrors firstbind_orphan.rs)
/// with a fast keepalive so the healthy-loop iteration budget is
/// dominated by the timer cadence, not the assignment.
fn one_worker_config(secondary_id: &str) -> SecondaryConfig {
    SecondaryConfig {
        secondary_id: secondary_id.into(),
        num_workers: 1,
        max_resources: dynrunner_core::ResourceMap::from([(
            dynrunner_core::ResourceKind::memory(),
            1024 * 1024 * 1024,
        )]),
        hostname: "test-host".into(),
        keepalive_interval: Duration::from_millis(50),
        src_network: None,
        src_tmp: None,
        peer_timeout: Duration::from_secs(120),
        keepalive_miss_threshold: 3,
        retry_max_passes: 1,
        oom_retry_max_passes: 1,
        primary_link_failure_threshold: 5,
        primary_link_failure_window: Duration::from_secs(30),
        primary_silence_backstop: Duration::from_secs(120),
        unconfigured_deadline: Duration::from_secs(600),
        can_be_primary: false,
        resource_check_interval: Duration::from_millis(100),
        log_oom_watcher: false,
        phase_status_log_intervals: vec![Duration::from_secs(60)],
        promoted_primary_quiesce_grace: Duration::from_millis(100),
        unfulfillable_reinject_max_per_task: None,
        mem_manager_reserved_bytes: None,
        output_dir: None,
        memuse_log_path: None,
        forwarded_argv: Vec::new(),
    }
}

/// Build a wire `TaskAssignment` for `binary`, keyed by `file_hash`,
/// targeting `worker_id` on `sec_id` (same shape as firstbind_orphan.rs).
fn task_assignment(
    sender_id: &str,
    sec_id: &str,
    worker_id: u32,
    binary: &dynrunner_core::TaskInfo<TestId>,
    file_hash: &str,
) -> DistributedMessage<TestId> {
    DistributedMessage::TaskAssignment {
        target: None,
        sender_id: sender_id.into(),
        timestamp: 0.0,
        secondary_id: sec_id.into(),
        worker_id,
        zip_file: None,
        binary_info: DistributedBinaryInfo::from_task_info(binary),
        local_path: binary.path.to_string_lossy().into_owned(),
        file_hash: file_hash.into(),
        predecessor_outputs: std::collections::BTreeMap::new(),
        supplanted_holder: None,
    }
}

/// Minimal task with the default `TypeId` (the slot starts with
/// `loaded_type_id == None`, so the first assignment is a FIRST-BIND
/// and engages the type-shift respawn).
fn make_binary(name: &str) -> dynrunner_core::TaskInfo<TestId> {
    dynrunner_core::TaskInfo {
        // Absolute path so the dispatch's unresolvable-task guard
        // (no src_network + relative path → fail-loud reject) does not
        // eat the assignment before the first-bind respawn engages.
        path: std::path::PathBuf::from(format!("/tmp/{name}")),
        size: 50,
        identifier: TestId(name.into()),
        phase_id: dynrunner_core::PhaseId::from("default"),
        type_id: dynrunner_core::TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: Vec::new(),
        preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
        resolved_path: None,
    }
}

/// Factory replaying the production trace: the INITIAL pool init is
/// healthy — the setup-spawned subprocess Readies and idles, exactly
/// like the workers that imported fine on the affected nodes — and
/// then the exec context is GUTTED (`gutted` flipped by the test,
/// modelling the replacement-job preflight ripping the rootfs out from
/// under the live secondary): from that point EVERY spawn, typed or
/// untyped, fails AT THE SPAWN SYSCALL with the verbatim
/// run_20260611_115429 error shape — no child is ever created, the
/// factory returns `Err`. This is a DIFFERENT edge from
/// `dead_worker_spin.rs`'s EOF-before-Ready child — there a generation
/// exists and its startup death feeds the streak; here the failure
/// happens before any generation exists. `exec_attempts` counts the
/// failed exec attempts the respawn machinery drives.
struct GuttedContextFactory {
    gutted: Rc<Cell<bool>>,
    exec_attempts: Rc<Cell<u32>>,
}

impl GuttedContextFactory {
    fn fail(&self, worker_id: WorkerId) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        self.exec_attempts.set(self.exec_attempts.get() + 1);
        // The verbatim production failure: the spawn syscall itself
        // fails (exec context gutted) — no child generation exists.
        Err(format!(
            "failed to exec worker {worker_id}: No such file or directory (os error 2)"
        ))
    }
}

impl WorkerFactory<ChannelManagerEnd> for GuttedContextFactory {
    fn spawn_worker(
        &mut self,
        worker_id: WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        if self.gutted.get() {
            return self.fail(worker_id);
        }
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(async move {
            let mut runner = runner_end;
            let _ = runner.send(Response::Ready).await;
            loop {
                match MessageReceiver::<Command>::recv(&mut runner).await {
                    Some(Command::Stop) => break,
                    Some(Command::Custom { .. }) => {}
                    Some(Command::ProcessTask { .. }) => {
                        let _ = runner.send(Response::Done { result_data: None }).await;
                    }
                    None => break,
                }
            }
        });
        Ok((manager_end, None))
    }

    fn spawn_worker_for_type(
        &mut self,
        worker_id: WorkerId,
        _type_id: &dynrunner_core::TypeId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        self.fail(worker_id)
    }
}

/// THE repro. Drive the REAL `process_tasks` select loop for 2 seconds
/// of wall clock with (1) a first assignment whose first-bind respawn
/// FAILS AT THE SPAWN SYSCALL (the production exec-ENOENT), and (2)
/// nothing else. Assert the loop stays BOUNDED (no zero-delay respawn
/// hot-loop), the exec attempts are a small backed-off number, AND the
/// slot is still under restart management at window end (not
/// permanently dead — the broken context heals, the slot must too).
#[tokio::test(flavor = "current_thread")]
async fn exec_failing_respawn_stays_backed_off_and_scheduled() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let gutted = Rc::new(Cell::new(false));
            let exec_attempts = Rc::new(Cell::new(0u32));
            let mut factory = GuttedContextFactory {
                gutted: gutted.clone(),
                exec_attempts: exec_attempts.clone(),
            };

            // Healthy generic pool init (gen 0 Readies normally — the
            // pre-flap state on the production nodes).
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // The leave→rejoin replacement job guts the exec context out
            // from under the live secondary: every spawn now ENOENTs.
            gutted.set(true);

            // First assignment: first-bind → typed respawn → exec ENOENT.
            let binary = make_binary("consumer-task");
            let assignment = task_assignment("setup", "sec-2", 0, &binary, "deadbeef");
            assert!(
                secondary.deliver_to_inbox(assignment),
                "assignment must reach the slot inbox"
            );

            // Run the REAL operational loop for 2s of wall clock. It must
            // still be running when the window closes (the dead worker is
            // not a run terminal).
            let outcome = tokio::time::timeout(
                Duration::from_secs(2),
                secondary.process_tasks(&mut factory),
            )
            .await;
            assert!(
                outcome.is_err(),
                "the loop must keep running across the window; got {outcome:?}"
            );

            let stats = secondary
                .op_loop_arm_stats
                .as_ref()
                .expect("process_tasks publishes its arm stats")
                .snapshot();
            let spawns = exec_attempts.get();
            eprintln!(
                "DIAG respawn_exec_failure: iter={} spawns={} counts={:?}",
                stats.iter, spawns, stats.counts
            );

            // (1) No hot-loop: the loop's iteration count must look like
            // the timer cadence (~1e2 in 2s), not a zero-delay
            // schedule→fail→schedule spin (1e4+).
            assert!(
                stats.iter < 1_000,
                "operational loop spun: {} select iterations in 2s with an \
                 exec-failing respawn (attempts: {spawns}) — a spawn-syscall \
                 failure must be retried on a bounded backoff, not at \
                 runtime speed",
                stats.iter,
            );
            // (2) The exec attempts themselves are backed off: a 2s window
            // sees only the first few (dispatch edge + at most one due
            // restart-tail retry), never a churn.
            assert!(
                spawns < 10,
                "typed worker exec attempted {spawns} times in 2s — a \
                 spawn-syscall failure must feed the startup-crash backoff, \
                 not retry at loop speed",
            );
            // (3) NOT permanently dead: the slot must still be under
            // restart management (a pending backed-off entry) so a healed
            // exec context (e.g. a remounted storage root) heals the slot.
            // Pre-fix the restart tail's Err arm removed the entry and
            // never rescheduled — the slot silently dropped out of the
            // pool forever (run_20260611_115429: 5 workers lost per
            // gutted secondary).
            let pending = &secondary
                .op_ref()
                .expect("loop exits Operational state intact")
                .pending_worker_restarts;
            assert!(
                pending.contains_key(&0),
                "slot 0 must remain scheduled for a backed-off restart \
                 after its respawn failed at the spawn syscall; \
                 pending_worker_restarts = {pending:?}",
            );
        })
        .await;
}
