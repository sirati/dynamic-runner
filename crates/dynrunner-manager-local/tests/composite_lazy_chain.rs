//! Composite-run repro tests for the single-process (`_dispatch_local` →
//! `LocalManager`) mode, mirroring the asm-tokenizer `FullPipelineTask`
//! shape that the consumer reported broken:
//!
//!   * THREE chained phases (p1 → p2 → p3) with a DISTINCT `type_id`
//!     per phase (the composite's per-phase worker modules).
//!   * Only p1 has tasks at run start (40 of them); p2 and p3 are
//!     declared in `phase_deps` but EMPTY — their items are injected
//!     lazily from `on_phase_end` via `PrimaryCommand::SpawnTasks`
//!     (the `PrimaryHandle.spawn_tasks` fire-and-forget shape).
//!   * Worker count EXCEEDS the later phases' task counts (the
//!     consumer's `--cores 0` = all-cores default), and
//!     `reuse_workers=false` (the production default since the
//!     worker-lifecycle inversion).
//!
//! Face 1 of the bug report is FALSE SUCCESS — `stats.completed`
//! says everything succeeded while the task bodies never executed —
//! so every assertion here checks the EFFECT (the fake worker records
//! each task path it actually processed) and not just the status
//! counters.

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

fn make_task(name: &str, phase: &str, type_id: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(name),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from(type_id),
        affinity_id: Some(AffinityId::from(name)),
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        resolved_path: None,
    }
}

/// The shared body-ran ledger: every task path a fake worker actually
/// processed. THE face-1 effect assertion reads this — a task that is
/// "completed" without an entry here is the false-success bug.
type Executed = Arc<Mutex<Vec<String>>>;

/// Auto-succeeding fake worker factory that RECORDS each processed
/// task path into the shared `Executed` ledger before replying Done.
struct RecordingWorkerFactory {
    executed: Executed,
}

impl WorkerFactory<ChannelManagerEnd> for RecordingWorkerFactory {
    fn spawn_worker(
        &mut self,
        _worker_id: WorkerId,
        _subcgroup: Option<&dynrunner_manager_local::SubcgroupHandle>,
    ) -> Result<(ChannelManagerEnd, Option<u32>), String> {
        let (manager_end, runner_end) = channel_pair();
        tokio::task::spawn_local(recording_worker_loop(runner_end, self.executed.clone()));
        Ok((manager_end, None))
    }
}

async fn recording_worker_loop(mut runner: ChannelRunnerEnd, executed: Executed) {
    let _ = runner.send(Response::Ready).await;
    loop {
        match MessageReceiver::<Command>::recv(&mut runner).await {
            Some(Command::Stop) | None => break,
            Some(Command::Custom { .. }) => {}
            Some(Command::ProcessTask { relative_path, .. }) => {
                executed
                    .lock()
                    .expect("executed ledger")
                    .push(relative_path);
                let _ = runner.send(Response::Done { result_data: None }).await;
            }
        }
    }
}

fn test_config(num_workers: u32, reuse_workers: bool) -> LocalManagerConfig {
    LocalManagerConfig {
        num_workers,
        max_resources: ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024)]),
        reuse_workers,
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

/// Fire-and-forget `SpawnTasks` exactly like `PrimaryHandle.spawn_tasks`
/// does from inside a lifecycle hook (in-runtime `try_send`, reply
/// receiver dropped).
fn fire_and_forget_spawn(
    sender: &tokio::sync::mpsc::Sender<PrimaryCommand<TestId>>,
    tasks: Vec<TaskInfo<TestId>>,
) {
    let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel::<
        Result<Vec<(usize, dynrunner_core::SpawnError)>, String>,
    >();
    let cmd = PrimaryCommand::SpawnTasks {
        tasks,
        reply: reply_tx,
    };
    let _ = sender.try_send(cmd);
}

/// THE consumer-shape repro (faces 1 + 2 in one topology): 40 p1 tasks,
/// p2/p3 lazily injected from `on_phase_end`, MORE workers than later-
/// phase tasks, `reuse_workers = false`. Asserts the EFFECT (bodies ran)
/// for all 42 tasks, not just the status counters.
#[tokio::test(flavor = "current_thread")]
async fn composite_three_phase_lazy_chain_runs_every_body() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const NUM_P1: usize = 40;
            const NUM_WORKERS: u32 = 16;

            let config = test_config(NUM_WORKERS, false);
            let mut manager: LocalManager<ChannelManagerEnd, _, _, TestId> =
                LocalManager::new(config, ResourceStealingScheduler::memory(), FixedEstimator);

            let sender = manager.command_sender();
            let spawned_p2 = Arc::new(Mutex::new(false));
            let spawned_p3 = Arc::new(Mutex::new(false));
            let spawned_p2_cb = Arc::clone(&spawned_p2);
            let spawned_p3_cb = Arc::clone(&spawned_p3);
            let sender_cb = sender.clone();

            // Consumer phase graph: p2 depends on p1, p3 depends on p2.
            let mut phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
            phase_deps.insert(PhaseId::from("p2"), vec![PhaseId::from("p1")]);
            phase_deps.insert(PhaseId::from("p3"), vec![PhaseId::from("p2")]);

            let binaries: Vec<TaskInfo<TestId>> = (0..NUM_P1)
                .map(|i| make_task(&format!("p1-{i}"), "p1", "type-p1"))
                .collect();

            let on_phase_end = move |phase_id: &PhaseId,
                                     _completed: u32,
                                     _failed: u32,
                                     _outputs: &std::collections::BTreeMap<
                String,
                dynrunner_core::TaskOutputs,
            >| {
                match phase_id.as_str() {
                    "p1" => {
                        let mut guard = spawned_p2_cb.lock().expect("mutex");
                        if *guard {
                            return;
                        }
                        *guard = true;
                        // One aggregate p2 task (the unify-vocab shape).
                        fire_and_forget_spawn(
                            &sender_cb,
                            vec![make_task("p2-agg", "p2", "type-p2")],
                        );
                    }
                    "p2" => {
                        let mut guard = spawned_p3_cb.lock().expect("mutex");
                        if *guard {
                            return;
                        }
                        *guard = true;
                        fire_and_forget_spawn(
                            &sender_cb,
                            vec![make_task("p3-agg", "p3", "type-p3")],
                        );
                    }
                    _ => {}
                }
            };

            let executed: Executed = Arc::new(Mutex::new(Vec::new()));
            let mut factory = RecordingWorkerFactory {
                executed: Arc::clone(&executed),
            };
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

            let executed = executed.lock().expect("executed ledger").clone();
            let p1_ran = executed.iter().filter(|p| p.starts_with("p1-")).count();
            let p2_ran = executed.iter().filter(|p| p.starts_with("p2-")).count();
            let p3_ran = executed.iter().filter(|p| p.starts_with("p3-")).count();

            // EFFECT assertions first (face 1: status without execution
            // is the bug class under test).
            assert_eq!(p1_ran, NUM_P1, "every p1 body must actually run");
            assert_eq!(p2_ran, 1, "the lazily-spawned p2 body must actually run");
            assert_eq!(p3_ran, 1, "the lazily-spawned p3 body must actually run");

            assert_eq!(
                manager.stats().completed,
                (NUM_P1 + 2) as u32,
                "all three phases' tasks complete"
            );
            assert!(manager.failed_tasks().is_empty(), "no failures expected");
        })
        .await;
}
