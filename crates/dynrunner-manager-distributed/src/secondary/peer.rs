
use dynrunner_core::{ErrorType, Identifier};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
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
    pub(super) async fn handle_peer_message(&mut self, msg: DistributedMessage<I>) {
        match msg {
            DistributedMessage::Keepalive {
                secondary_id,
                timestamp,
                active_workers,
                ..
            } => {
                self.peer_keepalives.insert(secondary_id.clone(), timestamp);
                tracing::trace!(
                    peer = %secondary_id,
                    active_workers,
                    "peer keepalive received"
                );
            }
            DistributedMessage::TaskComplete {
                secondary_id,
                task_hash,
                ..
            } => {
                // Track peer's completed task to avoid duplicate processing
                self.completed_tasks.insert(task_hash.clone());
                // A successful TaskComplete from this peer proves it's
                // healthy — clear any primary backpressure
                // backoff so the next dispatch cycle can re-target it.
                // Mirrors regular primary's TaskComplete handler.
                self.clear_primary_peer_backpressure(&secondary_id);
                // Drive the primary's phase machine: if this
                // node dispatched the task as primary, the
                // peer's completion message is the only signal the
                // pool gets that the item is no longer in flight.
                self.note_primary_item_completed(&task_hash);
                tracing::debug!(
                    peer = %secondary_id,
                    task_hash,
                    "peer task complete"
                );
            }
            DistributedMessage::TaskFailed {
                secondary_id,
                task_hash,
                error_type,
                error_message,
                ..
            } => {
                // Two TaskFailed shapes arrive on the primary
                // path:
                //   1. Backpressure rejection — peer's dispatch.rs
                //      sends `Recoverable / "No idle worker
                //      available"` when its worker pool can't accept
                //      the assignment. The task NEVER ran; the
                //      binary must be returned to the pool, the
                //      peer marked backpressured. Drives
                //      `handle_primary_peer_rejection` (re-queue +
                //      backoff). Skipping it would leak the binary
                //      from `primary_in_flight` and stall the
                //      per-phase in_flight counter.
                //   2. Terminal failure — peer's worker actually ran
                //      the binary and reported failure (Recoverable
                //      from the worker, NonRecoverable, OutOfMemory,
                //      etc.). The phase machine just needs the
                //      in-flight counter decremented.
                let is_backpressure = matches!(error_type, ErrorType::Recoverable)
                    && error_message == "No idle worker available";
                if is_backpressure {
                    if let Some(peer) = self.handle_primary_peer_rejection(&task_hash) {
                        tracing::debug!(
                            peer = %peer,
                            task_hash,
                            "peer rejected primary assignment; re-queued + backpressure backoff applied"
                        );
                    }
                } else {
                    // Route through the failure-aware decrementer:
                    // Recoverable failures land in
                    // `primary_failed` for the retry pass,
                    // others just decrement in-flight as before.
                    self.note_primary_item_failed(&task_hash, &error_type);
                    // Synchronous drain-check: if THIS failure was
                    // the last in-flight item AND the pool is
                    // empty, immediately re-inject the failed-task
                    // ledger and re-poll our own workers. Without
                    // this synchronous trigger the retry waits
                    // until the next keepalive tick (up to the
                    // keepalive interval), which races the live
                    // primary's `operational_loop` exit (which
                    // fires on `completed + failed >= total` and
                    // doesn't wait). No-op when there's still
                    // pending work or no Recoverable failures
                    // logged.
                    self.primary_drain_check_and_retry().await;
                    tracing::debug!(
                        peer = %secondary_id,
                        task_hash,
                        error_type = ?error_type,
                        "peer task failed"
                    );
                }
            }
            DistributedMessage::TimeoutDetected {
                timed_out_secondary_id,
                last_seen,
                ..
            } => {
                tracing::warn!(
                    timed_out = %timed_out_secondary_id,
                    last_seen,
                    "peer timeout detected by another secondary"
                );
            }
            DistributedMessage::TimeoutQuery {
                query_node_id,
                sender_id,
                ..
            } => {
                // Respond with our last known keepalive for the queried node.
                let last_keepalive = self.peer_keepalives.get(&query_node_id).copied();
                let response: DistributedMessage<I> = DistributedMessage::TimeoutResponse {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    query_node_id,
                    last_keepalive,
                };
                tracing::debug!(peer = %sender_id, "timeout query received, queueing response");
                // Queue for async send — will be flushed in the main loop
                self.pending_peer_messages.push((sender_id, response));
            }
            DistributedMessage::TimeoutResponse {
                sender_id,
                query_node_id: _,
                last_keepalive,
                ..
            } => {
                self.record_timeout_response(sender_id, last_keepalive);
            }
            DistributedMessage::PromotionVote {
                sender_id,
                candidate_id,
                vote_round,
                ..
            } => {
                if let Some(reply) = self.record_promotion_vote(candidate_id, vote_round) {
                    self.pending_peer_messages.push((sender_id, reply));
                }
            }
            DistributedMessage::PromotionConfirm {
                sender_id,
                new_primary_id,
                vote_round,
                ..
            } => {
                self.record_promotion_confirm(sender_id, new_primary_id, vote_round);
            }
            DistributedMessage::TaskRequest {
                secondary_id,
                worker_id,
                available_resources,
                ..
            } if self.is_primary => {
                // Peer routed this to us because we won the election. Same
                // dispatch path that the live-primary case uses, just
                // arriving over peer_transport instead of primary_transport.
                let available_memory = available_resources
                    .iter()
                    .find(|r| r.kind == dynrunner_core::ResourceKind::memory())
                    .map(|r| r.amount)
                    .unwrap_or(0);
                if let Err(e) = self
                    .handle_primary_task_request(secondary_id, worker_id, available_memory)
                    .await
                {
                    tracing::warn!(error = %e, "post-promotion peer TaskRequest dispatch failed");
                }
            }
            _ => {
                tracing::debug!(msg_type = ?msg.msg_type(), "unhandled peer message");
            }
        }
    }

    /// One-shot watchdog: 30s after `connect_to_peers` fired with a
    /// non-empty peer list, decide whether the peer mesh formed.
    /// Self-healing if the mesh forms before the deadline
    /// (`peer_count() > 0` suppresses the fault) or partially forms
    /// after the deadline (any incoming peer connection clears
    /// `peer_mesh_check_at`, no fault).
    ///
    /// On confirmed full-mesh failure (deadline elapsed, zero peers)
    /// the run enters DEGRADED mode rather than dying:
    ///   1. `peer_mesh_degraded` is latched true so callers that
    ///      need the mesh (failover election, peer-broadcast
    ///      keepalives) can fail loud or skip — the contract is
    ///      owned by those callers, not by this watchdog.
    ///   2. `MeshReady` is sent with `peer_count=0` so the primary's
    ///      `wait_for_mesh_ready` step releases `PromotePrimary` and
    ///      operational dispatch (over WSS, not the peer mesh) can
    ///      flow. Without this the whole run blocks on the missing
    ///      mesh signal.
    ///   3. `peer_mesh_check_at = None` so the watchdog is one-shot.
    ///
    /// Why not fatal? Operational dispatch primary→secondary uses
    /// WSS, not the QUIC peer mesh. If no failover is ever needed
    /// the run can complete cleanly even with zero peers. The old
    /// fatal behaviour (send `SecondaryFatalError`, set
    /// `fatal_exit`) stranded every remaining task whenever the
    /// peer mesh genuinely couldn't form — see
    /// asm-tokenizer's `--jobs 2` regression where 474 of 484 tasks
    /// were lost ~30s into the run because the watchdog fired even
    /// though primary→secondary dispatch was healthy.
    ///
    /// `peer_count()` already calls `drain_new_connections` so this
    /// reads the freshest state.
    pub(super) async fn check_peer_mesh_watchdog(&mut self) {
        let deadline = match self.peer_mesh_check_at {
            Some(d) => d,
            None => return,
        };
        // peer_count drains new connections internally; calling it
        // BEFORE the deadline check lets a fresh connection clear
        // the watchdog without firing the fault.
        let connected = self.peer_transport.peer_count();
        if connected > 0 {
            self.peer_mesh_check_at = None;
            // Mesh formed for the first time — tell the primary so
            // it can release `PromotePrimary`. Idempotent via
            // `mesh_ready_sent`.
            self.report_mesh_ready_if_needed().await;
            return;
        }
        if std::time::Instant::now() < deadline {
            return;
        }
        // Latch the watchdog off first so it never re-fires.
        self.peer_mesh_check_at = None;
        self.peer_mesh_degraded = true;

        tracing::warn!(
            attempted = self.peer_dial_count,
            connected = 0,
            "peer mesh did not form — failover and inter-secondary \
             keepalive paths are unavailable; run will continue but \
             is fragile (tasks dispatched via primary→secondary WSS \
             still flow)"
        );

        // Report mesh-ready (with peer_count=0) so the primary's
        // `wait_for_mesh_ready` step releases `PromotePrimary`
        // instead of blocking the full mesh-ready timeout on a
        // secondary that will never see peers. Idempotent via
        // `mesh_ready_sent`.
        self.report_mesh_ready_if_needed().await;
    }

    /// Single source of truth for "have we told the primary the
    /// peer-mesh is settled?". Idempotent: the first call that
    /// observes a settled state (mesh formed, watchdog elapsed, or
    /// no peers were ever expected — i.e. single-secondary) emits
    /// `MeshReady` and flips the one-shot guard so subsequent calls
    /// are no-ops.
    ///
    /// Concern owned here, not at call sites: callers (the keepalive
    /// tick's `check_peer_mesh_watchdog` and the operational-loop
    /// entry hook) shouldn't have to know the rules — they just say
    /// "now's a moment the mesh state may have changed; report if
    /// anything to report". This keeps the modular boundary clean
    /// (peer.rs owns peer-mesh status; processing.rs just calls).
    pub(super) async fn report_mesh_ready_if_needed(&mut self) {
        if self.mesh_ready_sent {
            return;
        }
        // Three reportable states, all coalesced by this single
        // helper:
        //   - peer_dial_count == 0: no peers were expected (single-
        //     secondary run, or empty PeerInfo). Mesh is trivially
        //     "ready" the moment we reach the operational loop.
        //   - peer_count > 0: at least one dial landed; mesh has
        //     formed (further peers may keep arriving but the
        //     primary just needs the first non-empty signal).
        //   - peer_mesh_check_at is None AND peer_dial_count > 0:
        //     the watchdog has already cleared the deadline (either
        //     mesh formed, in which case the previous branch fired,
        //     or it elapsed with zero peers). The fully-failed case
        //     still reports so the primary doesn't wait the full
        //     mesh-ready timeout for nothing.
        let connected = self.peer_transport.peer_count() as u32;
        let no_peers_expected = self.peer_dial_count == 0;
        let mesh_formed = connected > 0;
        let watchdog_done =
            self.peer_dial_count > 0 && self.peer_mesh_check_at.is_none();
        if !(no_peers_expected || mesh_formed || watchdog_done) {
            return;
        }
        let msg: DistributedMessage<I> = DistributedMessage::MeshReady {
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            peer_count: connected,
        };
        if let Err(e) = self.send_to_current_primary(msg).await {
            // Best-effort: log and flip the flag anyway so we
            // don't busy-retry on every keepalive tick. The
            // primary's wait step will time out (warning, not a
            // hard error — see lifecycle.rs `wait_for_mesh_ready`)
            // and the run continues.
            tracing::warn!(
                error = %e,
                "failed to send MeshReady to primary; primary will fall back to \
                 mesh-ready timeout before promoting primary"
            );
        } else {
            tracing::debug!(
                connected,
                "MeshReady sent to primary"
            );
        }
        self.mesh_ready_sent = true;
    }

    /// Check for peer timeouts based on keepalive tracking. When this
    /// secondary is the primary, a peer-timeout ALSO recovers
    /// any in-flight tasks dispatched to that peer back into the
    /// pool — without this, the primary_in_flight ledger leaks the
    /// binary forever (the peer will never report TaskComplete /
    /// TaskFailed because it's gone) and the per-phase in_flight
    /// counter stays positive, blocking phase progression.
    /// Non-primary peers don't have a primary_in_flight ledger
    /// to recover, so the recovery path is a no-op for them.
    pub(super) fn check_peer_timeouts(&mut self) {
        // Degraded-mesh skip: with no peer mesh, there's no peer
        // keepalive to time out and no in-flight peer-targeted work
        // to recover. The walk below is a no-op against an empty
        // `peer_keepalives` map anyway — short-circuiting here
        // documents the contract so a future change that buffers
        // peer state pre-connection doesn't accidentally make this
        // matter. See `peer_mesh_degraded` field doc on the
        // SecondaryCoordinator for the full set of guarded paths.
        if self.peer_mesh_degraded {
            return;
        }
        let now = timestamp_now();
        let timeout_secs = self.config.peer_timeout.as_secs_f64();
        let mut timed_out = Vec::new();

        for (peer_id, last_seen) in &self.peer_keepalives {
            if now - last_seen > timeout_secs {
                timed_out.push(peer_id.clone());
            }
        }

        for peer_id in timed_out {
            let last_seen = self.peer_keepalives.remove(&peer_id).unwrap_or(0.0);
            // Recover any tasks the primary dispatched to this
            // peer. Walk primary_in_flight, collect hashes whose target
            // matches, then call `recover_in_flight_to_pool` for each
            // (which requeues the binary, decrements in_flight, and
            // clears the ledger entry).
            let recovered: Vec<String> = self
                .primary_in_flight
                .iter()
                .filter(|(_, item)| item.target_secondary_id == peer_id)
                .map(|(hash, _)| hash.clone())
                .collect();
            let recovered_count = recovered.len();
            for hash in recovered {
                self.recover_in_flight_to_pool(&hash);
            }
            // Drop the peer's backpressure entry too — once it's
            // declared dead the backoff is meaningless.
            self.backpressured_secondaries.remove(&peer_id);
            tracing::warn!(
                peer = %peer_id,
                last_seen,
                elapsed = now - last_seen,
                recovered_in_flight = recovered_count,
                "peer timeout detected; recovered in-flight tasks dispatched to it"
            );
        }
    }
}
