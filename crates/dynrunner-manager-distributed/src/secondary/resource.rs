use dynrunner_core::{ErrorType, Identifier, ResourceKind, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::oom::OomWatcher;
use dynrunner_manager_local::pool::ResourcePressureResult;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    Address, DistributedMessage, PeerTransport, Role,
};
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

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Send an operational frame to whoever currently holds the
    /// primary role, feeding the failover-health probe on a no-route
    /// result.
    ///
    /// This is the single chokepoint for every primary-bound
    /// operational send (TaskRequest, terminal TaskComplete/TaskFailed,
    /// Keepalive, MeshReady). Routing is fully opaque: the unified
    /// transport resolves `Address::Role(Role::Primary)` to loopback /
    /// uplink / a promoted peer; the manager never inspects which.
    ///
    /// # Failover-health probe (the fast path)
    ///
    /// A clean `Err` from `send` means "no route to the primary":
    /// the bootstrap uplink has closed AND no peer holds `Role::Primary`
    /// (cache cold). That is the fast-failover signal — it arms the
    /// count-axis of `PrimaryLink` immediately, well before the
    /// keepalive time-axis would. The probe is transport-AGNOSTIC: the
    /// manager reacts only to a send RESULT, never to `peer_count()` or
    /// a recv-None branch or any locality inspection. A successful send
    /// (loopback, healthy uplink, or a reachable promoted peer) resets
    /// the health window via the normal `record_primary_message`
    /// path when the primary's reply / keepalive arrives.
    ///
    /// On a breach the same arming the deleted recv-None branch used is
    /// applied: backdate `primary_last_seen` so the next
    /// `run_election_tick` enters Suspecting.
    ///
    /// Note the deliberate name: this carries NO locality logic (unlike
    /// the removed `send_to_current_primary` router, which branched
    /// loopback-vs-wire on the old `PrimaryLink.current_primary`). It
    /// just sends to the primary role opaquely and notes a
    /// failover-health breach if the role is unreachable.
    pub(in crate::secondary) async fn send_to_primary(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        let result = self
            .transport
            .send(Address::Role(Role::Primary), msg)
            .await;
        if let Err(ref e) = result {
            // No route to the primary — feed the failover-health
            // probe. `record_recv_failure` anchors the failure window
            // on the first breach and returns true once the count- or
            // time-axis threshold is crossed.
            let armed = self.primary_link.record_recv_failure();
            if armed {
                tracing::warn!(
                    error = %e,
                    "no route to primary (uplink closed, no promoted peer); \
                     failover-health threshold breached — arming election"
                );
                let backdate = self
                    .config
                    .keepalive_interval
                    .saturating_mul(self.config.keepalive_miss_threshold + 1);
                self.primary_last_seen = Some(
                    std::time::Instant::now()
                        .checked_sub(backdate)
                        .unwrap_or_else(std::time::Instant::now),
                );
            } else {
                tracing::debug!(
                    error = %e,
                    "no route to primary; recording failover-health probe \
                     (threshold not yet breached)"
                );
            }
        }
        result
    }

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
                    // Report to the primary role only. The AUTHORITY
                    // originates the terminal CRDT mutation and
                    // broadcasts it to the mesh, so every peer/observer
                    // mirror converges — the reporting secondary must
                    // NOT broadcast itself (a second CRDT originator
                    // would break the authority's apply-before-dispatch
                    // ordering).
                    let _ = self.send_to_primary(msg).await;
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

        self.send_to_primary(msg).await
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
