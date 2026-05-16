use dynrunner_core::{
    ErrorType, Identifier, MessageReceiver, MessageSender, ResourceKind, WorkerId,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::oom::OomWatcher;
use dynrunner_manager_local::pool::ResourcePressureResult;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


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

                    let msg = DistributedMessage::TaskFailed {
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: ErrorType::ResourceExhausted(ResourceKind::memory()),
                        error_message: reason,
                    };
                    let _ = self.send_to_current_primary(msg.clone()).await;
                    let _ = self.peer_transport.broadcast(msg).await;
                }

                // Restart the worker and request a new task
                if let Err(e) = self.pool.restart_worker(worker_id, factory, false).await {
                    tracing::error!(worker_id, error = %e, "secondary OOM-restart failed");
                    return;
                }
                let _ = self.request_task_for_worker(worker_id).await;
            }
            ResourcePressureResult::NoAction => {}
        }
    }

    /// Handle a worker event (completion, disconnection, etc.)
    ///
    /// Returns `Some(worker_id)` if the worker needs to be restarted (e.g.
    /// after disconnect). The caller is responsible for calling
    pub(super) async fn request_task_for_worker(&mut self, worker_id: WorkerId) -> Result<(), String> {
        // When primary, handle task requests locally
        if self.is_primary && !self.primary_pending_is_empty() {
            let available_memory = if (worker_id as usize) < self.pool.workers.len() {
                self.pool.workers[worker_id as usize].reserved_budgets.get(&dynrunner_core::ResourceKind::memory())
            } else {
                self.config.max_resources.get(&dynrunner_core::ResourceKind::memory()) / self.config.num_workers as u64
            };
            return self
                .handle_primary_task_request(
                    self.config.secondary_id.clone(),
                    worker_id,
                    available_memory,
                )
                .await;
        }

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

    /// Send `msg` to the node currently holding primary authority,
    /// whichever it is.
    ///
    /// One concern: hide the routing decision behind a single boundary
    /// so every "send to primary" call site (TaskComplete, TaskFailed,
    /// Keepalive, TaskRequest, OOM report) follows the same rule. The
    /// rule is dynamic: at run start the local node is primary and we
    /// route via `primary_transport`; after `PromotePrimary` (and
    /// after election) `primary_link.current_primary()` names the current
    /// primary and we route via `peer_transport.send_to_peer` instead.
    ///
    /// Pre-extraction this routing logic existed inline in exactly one
    /// place (`request_task_for_worker`); every other operational
    /// "send to primary" went directly to `primary_transport.send`,
    /// which after promotion points at a peer that is no longer the
    /// authoritative primary. That mismatch is the
    /// `primary_connection-points-at-local` class of bug — completion
    /// reports, failure reports, and keepalives all routed to the
    /// wrong endpoint after promotion. Centralising the decision
    /// makes the right routing automatic at every call site.
    ///
    /// Setup-phase messages (welcome, cert exchange) deliberately
    /// keep using `primary_transport` directly: at that point there
    /// IS no other primary candidate, and the original transport is
    /// the only path that exists. Once setup completes and a
    /// secondary may be promoted, all operational messages route
    /// through here.
    pub(super) async fn send_to_current_primary(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        if let Some(current_primary) = self.primary_link.current_primary() {
            if current_primary != self.config.secondary_id.as_str() {
                let peer = current_primary.to_string();
                return self.peer_transport.send_to_peer(&peer, msg).await;
            }
            // Self-addressed (we ARE the primary). Keep the loopback
            // through `primary_transport` so the demoted observer can
            // still tick its completion counter and run the
            // run-done-termination check (see lifecycle.rs's
            // observer-mode contract: forwarded outcomes still drive
            // the local primary's terminal counters, only re-injection
            // is suppressed).
            //
            // Tolerate transport errors on this loopback. Post-
            // promotion the demoted primary process is allowed to
            // exit cleanly — when it does, its transport-writer task
            // closes and subsequent sends fail (QUIC: "transport
            // writer task exited"; channel test fixture: "channel
            // closed"). That failure is benign here: every
            // operational call site has already done the local
            // bookkeeping directly before invoking this helper, and
            // we ARE the authoritative primary. Without the swallow,
            // lone-promoted-primary runs (no peer to relay through,
            // local primary exited) propagated the error fatally
            // through `?` operators at processing.rs / dispatch.rs
            // and crashed the secondary.
            let _ = self.primary_transport.send(msg).await;
            return Ok(());
        }
        self.primary_transport.send(msg).await
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
    pub(super) async fn repoll_idle_workers(&mut self) {
        let n = self.pool.workers.len();
        for wid in 0..n {
            if self.pool.workers[wid].is_idle_state() {
                let _ = self.request_task_for_worker(wid as WorkerId).await;
            }
        }
    }
}
