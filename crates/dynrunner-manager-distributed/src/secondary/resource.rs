use dynrunner_core::{
    ErrorType, Identifier, MessageReceiver, MessageSender, ResourceKind, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::oom::OomWatcher;
use dynrunner_manager_local::pool::ResourcePressureResult;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

/// Wire marker used when a secondary's worker is killed by a no-fault
/// resource-stealing preempt (`KillReason::is_no_fault()`). The primary
/// recognises this string in [`PrimaryCoordinator::handle_task_failed`]
/// as a backpressure-shaped TaskFailed — re-queue the task at the
/// pool front WITHOUT consuming retry budget. Same shape as the
/// pre-existing `"No idle worker available"` and `"worker pipe broken;
/// respawning"` markers. The string is the public contract between
/// secondary and primary; do not change it without updating the
/// primary's `is_backpressure` predicate in the same commit.
pub const NO_FAULT_PREEMPT_WIRE_MESSAGE: &str =
    "worker no-fault preempt; resource stealing";


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Route the resource-pressure decision tick through the OOM
    /// watcher (mirrors `LocalManager::check_resource_pressure_via_watcher`).
    /// The watcher invokes `WorkerPool::check_resource_pressure`
    /// internally so it can record kill events for the structured-log
    /// trigger; the secondary-specific kill-outcome handling
    /// (TaskFailed mesh broadcast + worker restart + request new
    /// task) stays here.
    pub(super) async fn check_resource_pressure_via_watcher(
        &mut self,
        watcher: &mut OomWatcher,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let max = self.max_resources();
        let result = watcher.on_decision(&mut self.pool, &self.scheduler, &max, false);
        self.handle_resource_pressure_result(result, factory).await;
    }

    /// Secondary-specific outcome handler. Pulled out of the prior
    /// `check_resource_pressure` body so both the watcher-driven path
    /// and any future direct caller share the same TaskFailed-broadcast
    /// + restart + request rules.
    ///
    /// Routing is keyed on [`KillReason`]:
    ///
    ///   * No-fault preempt (memory stealing or under-budget) →
    ///     broadcast a backpressure-shaped `TaskFailed` carrying
    ///     [`NO_FAULT_PREEMPT_WIRE_MESSAGE`]. The primary's
    ///     `handle_task_failed` recognises this marker, requeues the
    ///     task at the pool front, and skips the `failed_tasks`
    ///     insert — retry budget is preserved.
    ///   * At-fault OOM (over budget / last resort) → today's path:
    ///     broadcast `TaskFailed { ErrorType::ResourceExhausted(memory) }`.
    ///     Consumes one retry attempt and surfaces in
    ///     `resource_pressure_tasks` for the OOM retry pass.
    ///
    /// Worker restart + new-task request runs in both arms — the
    /// killed worker is gone either way, so the slot needs a fresh
    /// subprocess and a new assignment from the primary.
    async fn handle_resource_pressure_result(
        &mut self,
        result: ResourcePressureResult<I>,
        factory: &mut impl WorkerFactory<M>,
    ) {
        match result {
            ResourcePressureResult::Killed {
                worker_id,
                reason,
                ..
            } => {
                // Find and report the task as failed
                let file_hash = self
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.active_tasks.remove(&hash);

                    let (error_type, error_message) = if reason.is_no_fault() {
                        (ErrorType::Recoverable, NO_FAULT_PREEMPT_WIRE_MESSAGE.into())
                    } else {
                        (
                            ErrorType::ResourceExhausted(ResourceKind::memory()),
                            reason.as_str().into(),
                        )
                    };

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type,
                        error_message,
                    };
                    let _ = self.send_to_current_primary(msg.clone()).await;
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                // Restart the worker and request a new task
                if let Err(e) = self.pool.restart_worker(worker_id, factory, false).await {
                    tracing::error!(worker_id, error = %e, "secondary OOM-restart failed");
                    return;
                }
                let _ = self.request_task_for_worker(worker_id, factory).await;
            }
            ResourcePressureResult::NoAction => {}
        }
    }

    /// Handle a worker event (completion, disconnection, etc.)
    ///
    /// Returns `Some(worker_id)` if the worker needs to be restarted (e.g.
    /// after disconnect). The caller is responsible for calling
    pub(super) async fn request_task_for_worker(
        &mut self,
        worker_id: WorkerId,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        if !self.primary_link.should_request_now(worker_id) {
            return Ok(());
        }

        let available_memory = if (worker_id as usize) < self.pool.workers.len() {
            self.pool.workers[worker_id as usize].reserved_budgets.get(&dynrunner_core::ResourceKind::memory())
        } else {
            self.config.max_resources.get(&dynrunner_core::ResourceKind::memory()) / self.config.num_workers as u64
        };

        let msg = DistributedMessage::TaskRequest {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            available_resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: available_memory,
            }],
        };
        self.primary_link.note_request_sent(worker_id);

        self.send_to_current_primary(msg).await
    }

    /// Periodic safety-net wakeup: walk every idle worker and call
    /// `request_task_for_worker`. The per-worker exponential backoff
    /// (held by `primary_link`, doubling from 1s to a 60s cap) suppresses
    /// requests within the backoff window, so the only fan-out cost is
    /// the in-budget polls — which is precisely the work the kickstart
    /// pattern would have done anyway.
    ///
    /// Only meaningful for the primary failover path (peer
    /// secondaries' workers don't get kickstarted by the primary
    /// when a phase activates) and edge cases on the live-primary path
    /// (a worker that got "no work" between two other workers'
    /// completions and the primary's kickstart targeted only one of
    /// them). Regular live-primary runs see most polls suppressed by
    /// the backoff because the kickstart already covers the path.
    pub(super) async fn repoll_idle_workers(
        &mut self,
        factory: &mut impl WorkerFactory<M>,
    ) {
        let n = self.pool.workers.len();
        for wid in 0..n {
            if self.pool.workers[wid].is_idle_state() {
                let _ = self.request_task_for_worker(wid as WorkerId, factory).await;
            }
        }
    }
}
