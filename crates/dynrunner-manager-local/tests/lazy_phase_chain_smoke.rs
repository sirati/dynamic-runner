//! End-to-end smoke test for `PrimaryCommand::SpawnTasks` driven
//! from `on_phase_end` against `LocalManager`.
//!
//! Single concern: prove that the local backend's command-channel
//! ingress fires the lazy-phase-chain idiom asm-tokenizer relies on
//! (the motivating bug for the LocalManager-PrimaryHandle work).
//!
//! Scenario:
//!   * Run a single-phase batch (phase = "p1") with two binaries.
//!   * From `on_phase_end("p1", ...)`, fire-and-forget a
//!     `PrimaryCommand::SpawnTasks` carrying one phase-2 binary
//!     (phase = "p2") via the manager's `command_sender()`.
//!   * Assert the outer-loop restart picks up the post-phase-1
//!     spawned task and dispatches+completes it. Stats.completed
//!     == 3 (two phase-1 + one phase-2).
//!
//! What this pins:
//!   * `LocalManager::command_sender()` returns a sender wired to the
//!     same receiver `process_binaries` consumes.
//!   * The worker-loop `select!` arm drains commands queued during
//!     the in-flight tick.
//!   * The outer-loop restart on `task_by_hash` growth runs the
//!     5-phase pipeline a second time so a phase that only had
//!     tasks injected after phase-1's drain still gets coverage.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dynrunner_core::{
    AffinityId, MessageReceiver, MessageSender, PhaseId, PrimaryCommand, ResourceKind, ResourceMap,
    SoftPreferredSecondaries, TaskInfo, TypeId, WorkerId,
};
use dynrunner_manager_local::{LocalManager, LocalManagerConfig, WorkerFactory};
use dynrunner_protocol_manager_worker::{Command, Response};
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_scheduler_api::ResourceEstimator;
use dynrunner_transport_channel::{ChannelManagerEnd, ChannelRunnerEnd, channel_pair};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

struct FixedEstimator;

impl ResourceEstimator<TestId> for FixedEstimator {
    fn estimate(&self, _task: &TaskInfo<TestId>) -> ResourceMap {
        ResourceMap::from([(ResourceKind::memory(), 100)])
    }
}

fn make_binary(name: &str, phase: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(name),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: Some(AffinityId::from(name)),
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// Auto-succeeding fake worker — copy of
/// `manager::test_helpers::FakeWorkerFactory::AlwaysSucceed`.
/// Reproduced here because integration tests can't reach the
/// private `test_helpers` module.
struct FakeWorkerFactory;

impl WorkerFactory<ChannelManagerEnd> for FakeWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(fake_worker_loop(runner_end));
        Ok((manager_end, None))
    }
}

async fn fake_worker_loop(mut runner: ChannelRunnerEnd) {
    let _ = runner.send(Response::Ready).await;
    loop {
        match MessageReceiver::<Command>::recv(&mut runner).await {
            Some(Command::Stop) | None => break,
            Some(Command::ProcessTask { .. }) => {
                let _ = runner.send(Response::Done { result_data: None }).await;
            }
        }
    }
}

fn test_config(num_workers: u32) -> LocalManagerConfig {
    LocalManagerConfig {
        num_workers,
        max_resources: ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]),
        reuse_workers: true,
        restart_predicate: None,
        retry_max_attempts: 1,
        print_pid: false,
        memuse_log_path: None,
        stage_timeouts: HashMap::new(),
        low_resource_thresholds: ResourceMap::from([(ResourceKind::memory(), 300 * 1024 * 1024)]),
        resource_check_interval: std::time::Duration::from_millis(100),
        phase_status_log_intervals: Vec::new(),
        log_oom_watcher: false,
        output_dir: None,
        unfulfillable_reinject_max_per_task: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn on_phase_end_spawn_tasks_runs_post_phase_via_outer_loop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let config = test_config(1);
            let mut manager: LocalManager<ChannelManagerEnd, _, _, TestId> =
                LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator);

            // Grab the command sender BEFORE process_binaries.
            // `on_phase_end` closes over the sender and fires a
            // `SpawnTasks` once phase `p1` drains.
            let sender = manager.command_sender();
            // Track whether we've already fired the spawn so a
            // second `p1` drain (e.g. cascade) doesn't re-spawn.
            let already_spawned = Arc::new(Mutex::new(false));
            let already_spawned_for_cb = Arc::clone(&already_spawned);

            // Phase deps: p2 depends on p1 (the lazy-chain shape;
            // p2 stays blocked until p1 completes — but here we
            // SPAWN p2's tasks dynamically from `on_phase_end("p1")`).
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("p2"), vec![PhaseId::from("p1")]);

            // Two initial p1 binaries; no p2 binaries at run start.
            let binaries = vec![make_binary("p1a", "p1"), make_binary("p1b", "p1")];

            let on_phase_end = move |phase_id: &PhaseId,
                                     _completed: u32,
                                     _failed: u32,
                                     _outputs: &std::collections::BTreeMap<
                String,
                dynrunner_core::TaskOutputs,
            >| {
                if phase_id.as_str() != "p1" {
                    return;
                }
                let mut guard = already_spawned_for_cb.lock().expect("mutex");
                if *guard {
                    return;
                }
                *guard = true;
                // Mint a reply oneshot but drop the receiver
                // immediately — the lazy-chain idiom is fire-and-
                // forget. The handler still applies the spawn; only
                // the per-task SpawnError vec is silently discarded.
                let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel::<
                    Result<Vec<(usize, dynrunner_core::SpawnError)>, String>,
                >();
                let cmd = PrimaryCommand::SpawnTasks {
                    tasks: vec![make_binary("p2a", "p2")],
                    reply: reply_tx,
                };
                // `try_send` is sync; the command lands in the
                // bounded channel and the worker loop's `select!`
                // (or the outer-loop tail drain) picks it up.
                let _ = sender.try_send(cmd);
            };

            let mut factory = FakeWorkerFactory;
            manager
                .process_binaries(
                    binaries,
                    phase_deps,
                    |_phase| {},
                    on_phase_end,
                    &mut factory,
                )
                .await
                .expect("process_binaries");

            // 2 phase-1 + 1 phase-2 = 3 completed.
            assert_eq!(
                manager.stats().completed,
                3,
                "phase-2 task spawned from on_phase_end must run"
            );
            assert!(manager.failed_tasks().is_empty(), "no failures expected");
        })
        .await;
}
