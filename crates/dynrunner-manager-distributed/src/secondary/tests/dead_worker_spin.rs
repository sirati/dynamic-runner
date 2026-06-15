//! Dead-at-spawn worker must not spin the operational loop (#370 RCA).
//!
//! Production shape (asm-tokenizer test-env, head 64d627e6,
//! run_20260611_005927): the consumer's worker module had a startup
//! error, so every TYPED worker subprocess died immediately at spawn
//! (manager-side transport EOF before `Response::Ready`). The first
//! assignment's first-bind type-shift respawn then entered a
//! respawn-crash cycle at runtime speed — spawn, instant EOF,
//! `Disconnected`, restart, spawn, … — burning 100% of the
//! single-threaded runtime's CPU (procfs: utime +93 ticks/s, wchan 0),
//! starving every timer arm AND the QUIC driver (peers observed
//! idle-timeout drops; a `PrimaryChanged` announcement landing in the
//! window was never processed), and leaving the dead child an unreaped
//! zombie.
//!
//! The contract pinned here: a worker that dies BEFORE Ready, over and
//! over, must be retried on a BOUNDED schedule (backoff), never at loop
//! speed. The observable is the operational loop's own iteration
//! counter (`OpLoopArmStats`): driven for 2 wall-clock seconds with a
//! dead-at-spawn typed worker, a healthy loop runs its timer cadence
//! (~hundreds of iterations); the pre-fix respawn-crash loop runs
//! orders of magnitude more.

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

/// Factory replaying the production consumer failure: the INITIAL
/// (untyped) worker is healthy — it Readies and idles, exactly like the
/// generic pool-init subprocess — but every TYPED spawn (the per-type
/// respawn carrying the consumer's worker module) dies instantly at
/// startup: the runner end of the channel pair is dropped before
/// anything is sent, so the manager observes EOF before `Ready`
/// (`WaitReadyResult::Disconnected`). `typed_spawns` counts the
/// kill+spawn cycles the respawn machinery drives.
struct DeadTypedFactory {
    typed_spawns: Rc<Cell<u32>>,
}

impl WorkerFactory<ChannelManagerEnd> for DeadTypedFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
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
        _worker_id: WorkerId,
        _type_id: &dynrunner_core::TypeId,
        _subcgroup: Option<&dynrunner_manager_local::cgroup::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        self.typed_spawns.set(self.typed_spawns.get() + 1);
        let (manager_end, runner_end) = channel_pair();
        // Child dies at startup: EOF before Ready.
        drop(runner_end);
        Ok((manager_end, None))
    }
}

/// THE repro. Drive the REAL `process_tasks` select loop for 2 seconds
/// of wall clock with (1) a first assignment whose first-bind respawn
/// produces a dead-at-spawn typed worker, and (2) nothing else. Assert
/// the loop stays BOUNDED: its iteration count must look like the timer
/// cadence, not a spin, and the kill+spawn cycle count must be a small
/// backed-off number, not thousands.
#[tokio::test(flavor = "current_thread")]
async fn dead_at_spawn_worker_must_not_spin_the_operational_loop() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut secondary, _log) = make_secondary_recording(one_worker_config("sec-2"), 1);
            secondary.set_bootstrap_primary_id("setup".to_string());

            let typed_spawns = Rc::new(Cell::new(0u32));
            let mut factory = DeadTypedFactory {
                typed_spawns: typed_spawns.clone(),
            };

            // Healthy generic pool init (gen 0 Readies normally).
            let pool = secondary.initialize_workers(&mut factory).await.unwrap();
            secondary.enter_operational_for_test();
            *secondary.pool_mut() = pool;

            // First assignment: first-bind → typed respawn → instant EOF.
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
            let spawns = typed_spawns.get();
            eprintln!(
                "DIAG dead_worker_spin: iter={} spawns={} counts={:?}",
                stats.iter, spawns, stats.counts
            );

            // Starvation half of the contract (the production wedge
            // starved EVERY timer + the inbound path on the
            // single-threaded runtime): across the 2s window the 50ms
            // keepalive arm must keep winning on cadence. Pre-fix the
            // loop still iterated, but in production the per-cycle
            // fork/exec + event churn starved the QUIC driver and the
            // inbox (procfs: 100% utime, peers hit idle-timeout drops,
            // the PrimaryChanged broadcast was never processed); the
            // iteration bound below is the in-process observable for
            // the same defect.
            let keepalives = stats
                .counts
                .iter()
                .find(|(name, _)| *name == "keepalive")
                .map(|(_, c)| *c)
                .unwrap_or(0);
            assert!(
                keepalives >= 20,
                "keepalive arm starved: only {keepalives} ticks won in 2s \
                 at a 50ms cadence — the loop is not servicing its timers \
                 while a dead-at-spawn worker churns",
            );

            // Healthy bound: the 50ms keepalive + 50ms OOM sample + 100ms
            // decision cadence yields on the order of 1e2 iterations in 2s.
            // The pre-fix respawn-crash loop yields 1e4–1e6. The bound is
            // two orders above healthy and orders below the spin.
            assert!(
                stats.iter < 1_000,
                "operational loop spun: {} select iterations in 2s with a \
                 dead-at-spawn worker (typed spawns: {spawns}) — a worker \
                 that dies before Ready must be respawned on a bounded \
                 backoff, not at runtime speed",
                stats.iter,
            );
            // And the kill+spawn cycle itself must be backed off: at loop
            // speed this is thousands; with a sane backoff a 2s window
            // sees only the first few attempts.
            assert!(
                spawns < 10,
                "typed worker respawned {spawns} times in 2s — the \
                 dead-at-spawn worker must be retried on a backoff, not \
                 respawn-crash-looped",
            );
        })
        .await;
}
