use dynrunner_core::{ErrorType, Identifier, ResourceKind, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_manager_local::oom::OomWatcher;
use dynrunner_manager_local::pool::ResourcePressureResult;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, SendTarget, resolve_destination,
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
pub const NO_FAULT_PREEMPT_WIRE_MESSAGE: &str = "worker no-fault preempt; resource stealing";

use super::SecondaryCoordinator;
use super::wire::timestamp_now;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// THE egress edge: resolve the role-bearing [`Destination`] this
    /// coordinator owns the facts for, stamp it on the frame (the C3 routing
    /// field the RECEIVER's mesh-pump demuxes by), and queue the frame onto
    /// the one mesh through this coordinator's [`crate::process::MeshClient`].
    /// The coordinator never names a transport and never branches on
    /// locality.
    ///
    /// `resolve_destination` stays AT this coordinator (clarification H1):
    /// its role-specific bootstrap fallback (`current_primary()` warm after
    /// a `PrimaryChanged`, the bootstrap-primary id as the cold-cache
    /// fallback) is the GATE that produces the honest "no route to the
    /// primary" `Err` the failover-health probe in [`Self::send_to_primary`]
    /// keys on. The probe fires in TWO cases, both surfaced here before the
    /// frame is queued:
    ///   - `resolve_destination` returns `None` — no current primary AND no
    ///     bootstrap link, so nothing resolves at all.
    ///   - it resolves to a concrete remote [`SendTarget::Peer`] that is NOT
    ///     a connected mesh member (`!self.client.has_peer(id)`). This is the
    ///     one-mesh analogue of the deleted transport-level
    ///     `send_to_peer(id) -> NoRoute Err`: because
    ///     [`crate::process::MeshClient::send`] is QUEUED (it returns `Ok`
    ///     the moment it enqueues, never observing the eventual wire result),
    ///     the no-route signal must be read from the pump-published
    ///     membership view at egress, not awaited from the send. The view is
    ///     ≤1-cycle stale + monotone-toward-truth, which is SAFE for the
    ///     probe: it never declares death (the probe only feeds a thresholded
    ///     health window that a successful keepalive resets), and a stale-high
    ///     `has_peer` merely delays the probe by one cycle — the keepalive
    ///     time-axis backstop covers that window.
    ///
    /// # Two `Destination`s: the routing send-target vs the C3 stamp
    ///
    /// The mesh-pump's `dispatch` routes the queued frame by the
    /// `MeshClient::send` `target` (loopback-vs-remote by id); the RECEIVER's
    /// pump demuxes it to a local slot by the frame's STAMPED `target()`.
    /// They are the same for all but the REMOTE-primary case, because
    /// [`Destination::Primary`] is id-less and the mesh cannot route it by
    /// host (the documented C3-seam `Mesh::dispatch` leaves open). So the
    /// egress resolves `Destination::Primary` to its concrete host BEFORE
    /// dispatch (per the `Mesh::dispatch` Primary-arm contract):
    ///   - `SendTarget::Loopback` (a promoted self): send `dst` itself — the
    ///     mesh loopbacks to the local role slot via `deliver_local`. Stamp
    ///     `dst`.
    ///   - `SendTarget::Peer(id)` from `Destination::Primary` (a REMOTE
    ///     primary): send an id-bearing target carrying `id` so the mesh
    ///     routes it by-id over the wire, but STAMP `Destination::Primary` so
    ///     the receiving pump delivers to that host's PRIMARY slot.
    ///   - `SendTarget::Peer(id)` from `Secondary`/`Observer`, and
    ///     `SendTarget::Broadcast`: send `dst` itself; stamp `dst`.
    ///
    /// Nothing is dropped: a self-addressed `Destination::Primary` loopbacks
    /// to the local primary slot; a remote one routes by its resolved host.
    pub(in crate::secondary) async fn send_to(
        &mut self,
        dst: Destination,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        // No-route GATE — the failover-health probe substrate. Resolve the
        // role facts this coordinator owns; `None` is "no primary resolvable
        // at all".
        let target = resolve_destination(
            dst.clone(),
            self.cluster_state.current_primary(),
            self.bootstrap_primary_id.as_deref(),
            &self.config.secondary_id,
        )
        .ok_or_else(|| {
            "Destination::Primary unresolvable: no current primary in the role table and no \
             bootstrap primary link — no route to the primary"
                .to_string()
        })?;
        // A resolved remote host that is NOT a connected member is the
        // queued-mesh analogue of the old transport-level NoRoute — surface
        // it as the probe `Err`. `Loopback` (a promoted self) and `Broadcast`
        // never no-route.
        if let SendTarget::Peer(id) = &target
            && !self.client.has_peer(id)
        {
            return Err(format!(
                "no route to {id}: resolved host is not a connected mesh member \
                 (queued-mesh no-route — failover-health probe)"
            ));
        }
        // The C3 stamp is ALWAYS the role-bearing intent `dst` — it is what
        // the receiver demuxes to a slot. The routing send-target carries the
        // resolved host ONLY for a remote `Destination::Primary` (id-less, so
        // the mesh can't route it by host without the resolution done here).
        let send_target = match (&dst, &target) {
            (Destination::Primary, SendTarget::Peer(id)) => Destination::Secondary(id.clone()),
            _ => dst.clone(),
        };
        // Queue it. `MeshClient::send` is QUEUED (M4): the pump drains it and
        // routes loopback-or-remote against the live slots by `send_target`,
        // and the receiving pump demuxes by the stamped `dst`.
        self.client.send(send_target, msg.with_target(dst))
    }

    /// Send an operational frame to whoever currently holds the
    /// primary role, feeding the failover-health probe on a no-route
    /// result.
    ///
    /// This is the single chokepoint for every primary-bound
    /// operational send (TaskRequest, terminal TaskComplete/TaskFailed,
    /// Keepalive, MeshReady). It addresses [`Destination::Primary`] and
    /// the edge resolver ([`Self::send_to`]) picks the concrete peer —
    /// the current primary, the bootstrap primary while cold, or
    /// loopback for a promoted self; the manager never inspects which.
    ///
    /// # Failover-health probe (the fast path)
    ///
    /// A clean `Err` from the send means "no route to the primary": the
    /// role table has no current primary AND no bootstrap link resolves.
    /// That is the fast-failover signal — it arms the count-axis of
    /// `PrimaryLink` immediately, well before the keepalive time-axis
    /// would. The probe is transport-AGNOSTIC: the manager reacts only
    /// to a send RESULT, never to `peer_count()` or a recv-None branch
    /// or any locality inspection. A successful send resets the health
    /// window via the normal `record_primary_message` path when the
    /// primary's reply / keepalive arrives.
    ///
    /// On a breach `primary_last_seen` is backdated. This is NOT what
    /// trips the local election any more — `run_election_tick`'s fast leg
    /// (A) reads `primary_link.should_arm_failover()` directly. The
    /// backdate is RETAINED for the peer-side confirmation gates that
    /// still key on the `keepalive_interval × keepalive_miss_threshold`
    /// deadline (`record_promotion_vote`'s `primary_silent` + a peer's
    /// own Suspecting quorum tally): on a busy genuine death the link
    /// arms fast, and funnelling the no-route signal into
    /// `primary_last_seen` lets those gates agree immediately rather than
    /// stalling the full ~15s deadline. The backdate (≈20s) is far below
    /// `primary_silence_backstop` (≈120s), so it never trips the
    /// election's patient leg (B).
    pub(in crate::secondary) async fn send_to_primary(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        let result = self.send_to(Destination::Primary, msg).await;
        if let Err(ref e) = result {
            // No route to the primary — feed the failover-health
            // probe. `record_recv_failure` anchors the failure window
            // on the first breach and returns true once the count- or
            // time-axis threshold is crossed.
            let armed = self.op_mut().primary_link.record_recv_failure();
            if armed {
                tracing::warn!(
                    error = %e,
                    "no route to primary (resolved primary peer unreachable / no primary \
                     resolvable); failover-health threshold breached — arming election"
                );
                let backdate = self
                    .config
                    .keepalive_interval
                    .saturating_mul(self.config.keepalive_miss_threshold + 1);
                self.op_mut().primary_last_seen = Some(
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
            // FAILOVER-B: a no-route is NOT a run-fatal error — it is a
            // failover SIGNAL, fully recorded above into the primary-link
            // health window. Returning the no-route `Err` here would
            // `?`-propagate up every operational caller
            // (`request_task_for_worker`, the TaskComplete/TaskFailed
            // reports in `worker_event`/`dispatch`) and ABORT the run loop
            // — deliberately killing a VOTER on primary-loss instead of
            // letting `run_election_tick` enter `Suspecting`. A primary
            // death MUST recover via election, never abort. So we ABSORB
            // the no-route into `Ok(())` and let the loop continue so the
            // election (already armed) runs; the secondary holds no
            // authority and owns no requeue.
            //
            // ACCOUNTING HONESTY: the absorbed terminal is genuinely LOST,
            // NOT recovered. The post-failover authority's recovery paths
            // (`recover_inflight_for_dead_secondary`,
            // `maybe_requeue_silent_held_work`) fire ONLY for DEAD / SILENT
            // holders — a holder the new primary believes is gone. This
            // secondary SURVIVES the failover gap, so the new primary keeps
            // its in-flight slot live and waits for a `TaskComplete` that was
            // dropped here and will never come; the completed task is then
            // STRANDED by the primary's final accounting. That is an
            // honest-PESSIMISTIC over-strand (the work really did finish, but
            // the run reports it as un-terminal), NOT a false-green — the
            // buffered-terminal-replay that would let a surviving holder re-
            // deliver across the gap is the proper fix (owner-deferred).
            //
            // This is the NO-ROUTE abort — DISTINCT from the
            // `mesh_degraded` split-brain guard in `run_election_tick`,
            // which is preserved: a genuinely-lone (zero-peer) secondary
            // still bails there rather than self-promoting on `quorum=1`.
            //
            // `send_to(Destination::Primary, …)` errors ONLY on no-route
            // (the two branches in `send_to`'s no-route gate; the queued
            // `MeshClient::send` never surfaces a wire-level error here),
            // so absorbing the `Err` discards no other error class. The
            // `Result` return is retained for a future genuinely-fatal
            // primary-bound send class, should one ever exist.
            return Ok(());
        }
        result
    }

    /// Report a respawn-HOLD-deferred task whose worker died before it
    /// could run (the worker disconnected between `RespawnInProgress`
    /// and the expected `Ready`, or `assign_task` failed at the
    /// post-Ready dispatch). The task NEVER ran, so it must be requeued
    /// at the authority — not counted as a failure. A backpressure-
    /// shaped `TaskFailed` (`Recoverable` + the `"worker pipe broken;
    /// respawning"` marker the authority's `is_backpressure` predicate
    /// recognises) is the wire contract that drives the requeue +
    /// re-dispatch.
    ///
    /// CLASS-1 own-worker report: the secondary is never the authority,
    /// so this is the SOLE recovery for a lost deferred task — there is
    /// no local pool to requeue into.
    pub(in crate::secondary) async fn report_deferred_task_lost(
        &mut self,
        worker_id: WorkerId,
        file_hash: &str,
    ) -> Result<(), String> {
        let msg = DistributedMessage::TaskFailed {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            task_hash: file_hash.to_string(),
            error_type: ErrorType::Recoverable,
            error_message: "worker pipe broken; respawning".into(),
        };
        self.send_to_primary(msg).await
    }

    /// Sweep any `active_tasks` entry bound to `worker_id` into the
    /// reinject path before the slot's subprocess is REPLACED.
    ///
    /// A worker-replacement edge installs a fresh subprocess (a new
    /// generation) into the slot. If the slot still carries an
    /// `active_tasks` entry, the bound task would otherwise be silently
    /// abandoned — assigned-never-terminal — and wedge the phase
    /// barrier. This sweeps that entry through the SAME backpressure-
    /// shaped reinject contract `report_deferred_task_lost` already
    /// uses (the authority requeues + re-dispatches without consuming
    /// retry budget). No-op when the slot has no bound task (the common
    /// case: replacement of an idle or already-swept slot).
    ///
    /// The paths that report a SPECIFIC terminal disposition before
    /// replacing the worker (OOM-kill → `ResourceExhausted`; explicit
    /// wire-failure → `NonRecoverable`) sweep `active_tasks` themselves
    /// with that classification and do NOT call this — this is the
    /// generic reinject for a replacement that carries no other failure
    /// signal (the type-shift respawn of a slot that drifted to holding
    /// a stale `active_tasks` entry).
    pub(in crate::secondary) async fn sweep_replaced_worker_task(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(), String> {
        let file_hash = self
            .op_mut()
            .active_tasks
            .iter()
            .find(|&(_, &wid)| wid == worker_id)
            .map(|(hash, _)| hash.clone());
        if let Some(hash) = file_hash {
            self.op_mut().active_tasks.remove(&hash);
            tracing::warn!(
                worker_id,
                task_hash = %hash,
                "worker slot replaced while still bound to a task; \
                 sweeping it into reinject (backpressure) so the \
                 replaced generation cannot strand it"
            );
            self.report_deferred_task_lost(worker_id, &hash).await?;
        }
        Ok(())
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
        // Clone the scheduler before borrowing the operational pool: the
        // pool now lives inside `OperationalState` (reached via
        // `op_mut()`, a full `&mut self` borrow), so a simultaneous
        // `&self.scheduler` shared borrow would conflict. The scheduler
        // is `Clone`-bounded in this impl and cheap to clone (a
        // config-shaped value); cloning once per decision tick keeps the
        // disjoint borrows clean without a manual struct destructure.
        let scheduler = self.scheduler.clone();
        let result = watcher.on_decision(&mut self.op_mut().pool, &scheduler, &max, false);
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
                worker_id, reason, ..
            } => {
                // Find and report the task as failed
                let op = self.op_mut();
                let file_hash = op
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.op_mut().active_tasks.remove(&hash);

                    let (error_type, error_message) = if reason.is_no_fault() {
                        (ErrorType::Recoverable, NO_FAULT_PREEMPT_WIRE_MESSAGE.into())
                    } else {
                        (
                            ErrorType::ResourceExhausted(ResourceKind::memory()),
                            reason.as_str().into(),
                        )
                    };

                    let msg = DistributedMessage::TaskFailed {
                        target: None,
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

                // Restart the worker NON-BLOCKINGLY. This handler runs
                // inside the operational `select!` (the OOM-decision arm),
                // so it must not inline-wait for the new subprocess's
                // Ready — that would hold the `select!` open for the whole
                // slow-worker startup window and starve the keepalive,
                // exactly the wedge `restart_worker_async` exists to
                // avoid. The `WorkerEvent::Ready` arm reclaims the slot
                // and re-issues its `TaskRequest` once the replacement
                // reports `Response::Ready`, so the post-restart repoll
                // rides that arm rather than a (premature) call here.
                if let Err(e) = self
                    .op_mut()
                    .pool
                    .restart_worker_async(worker_id, factory, false)
                    .await
                {
                    tracing::error!(worker_id, error = %e, "secondary OOM-restart failed");
                }
            }
            ResourcePressureResult::NoAction => {}
        }
    }

    /// Send a `TaskRequest` for one idle worker to the current primary
    /// role.
    ///
    /// A pure capacity hint: rate-limited per worker by `primary_link.
    /// should_request_now`, then dispatched through `send_to_primary`
    /// (the [`Destination::Primary`] egress edge resolves the concrete
    /// primary peer — current or bootstrap — and the manager never
    /// branches on locality). Since the P2 transport collapse this no
    /// longer needs a `WorkerFactory`: the request never spawns or
    /// restarts a worker, it only advertises the worker's available
    /// capacity to the authority.
    pub(super) async fn request_task_for_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> Result<(), String> {
        if !self.op_mut().primary_link.should_request_now(worker_id) {
            return Ok(());
        }

        let available_memory = if (worker_id as usize) < self.op_mut().pool.workers.len() {
            self.op_mut().pool.workers[worker_id as usize]
                .reserved_budgets
                .get(&dynrunner_core::ResourceKind::memory())
        } else {
            self.config
                .max_resources
                .get(&dynrunner_core::ResourceKind::memory())
                / self.config.num_workers as u64
        };

        let msg = DistributedMessage::TaskRequest {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            worker_id,
            available_resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: available_memory,
            }],
        };
        self.op_mut().primary_link.note_request_sent(worker_id);

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
    pub(super) async fn repoll_idle_workers(&mut self) {
        let n = self.op_mut().pool.workers.len();
        for wid in 0..n {
            // Re-borrow per iteration: the idle-state read (an `op_mut`
            // borrow) must end before the `request_task_for_worker`
            // await (which re-borrows `op_mut` internally for the
            // rate-limiter + capacity read).
            if self.op_mut().pool.workers[wid].is_idle_state() {
                let _ = self.request_task_for_worker(wid as WorkerId).await;
            }
        }
    }
}
