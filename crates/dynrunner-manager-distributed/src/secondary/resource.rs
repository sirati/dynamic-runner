use dynrunner_core::{Identifier, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_manager_local::pool::ResourcePressureResult;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport, PrimaryTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};


use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: PrimaryTransport<I>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    pub(super) async fn check_resource_pressure(&mut self, factory: &mut impl WorkerFactory<M>) {
        let max = self.max_resources();
        match self.pool.check_resource_pressure(&self.scheduler, &max, false) {
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
                        error_type: "OutOfMemory".into(),
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
            // We are the current primary — message addressed to ourselves.
            // The dispatch handlers expect to consume messages off the
            // transport receivers (primary_transport.recv on secondaries,
            // peer.recv_peer on peers) rather than self-deliver. The
            // primary's own self-dispatch path doesn't go through
            // this helper (`handle_primary_task_request` is called
            // directly from `request_task_for_worker` when
            // `is_primary && !primary_pending_is_empty`), so this
            // branch is hit only by the few odd code paths that don't
            // know whether the primary is local. Falling through to
            // `primary_transport.send` is the historical behaviour and
            // a no-op when the peer is the same node — primary_transport
            // is local-loopback in that case anyway.
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
