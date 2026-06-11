//! Worker-event handler: convert one inbound `WorkerEvent` (task
//! completed, task failed, worker exited unexpectedly, etc.) into local
//! per-pool bookkeeping plus a CLASS-1 terminal report to the primary
//! role.
//!
//! Single concern: the secondary's OWN-worker → primary bridge. The
//! secondary holds NO authority — it originates no CRDT mutation and
//! drives no phase machine. Each terminal worker outcome is reported to
//! whoever currently holds the primary role via `send_to_primary`
//! (`Destination::Primary`, resolved at the egress edge); the
//! authoritative `PrimaryCoordinator` owns the `ClusterMutation`
//! origination and the mesh broadcast.

use std::time::Duration;

use dynrunner_core::{ErrorType, Identifier, WorkerId};
use dynrunner_manager_local::oom::{OomWatcher, classify_disconnect};
use dynrunner_manager_local::worker::WorkerEvent;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

/// Same window as the LocalManager path — 500ms covers ~10 samples
/// worth of correlation tolerance at the production 50ms sample
/// cadence between kernel `oom_kill` counter increment and the
/// worker pipe-EOF observation.
const KERNEL_OOM_CORRELATION_WINDOW: Duration = Duration::from_millis(500);

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Single concern: the secondary's OWN-worker → cluster bridge.
    /// One inbound `WorkerEvent` from this node's worker pool becomes
    /// (1) local pool bookkeeping (slot clear, sampler flush, respawn
    /// queueing) and (2) a CLASS-1 terminal report to the primary role
    /// (`send_to_primary`). The secondary is never the authority: it
    /// originates NO CRDT mutation and drives NO phase machine — the
    /// same-peer `PrimaryCoordinator` owns authoritative accounting,
    /// reached via the `send_to_primary` loopback.
    pub(in crate::secondary) async fn handle_worker_event(
        &mut self,
        event: WorkerEvent<I>,
        oom_watcher: &OomWatcher,
    ) -> Result<Option<WorkerId>, String> {
        // Generation gate (root fix for the type-shift-respawn wedge).
        //
        // A worker-replacement edge (type-shift respawn, OOM/disconnect
        // restart) bumps the slot's generation and installs a fresh
        // subprocess. The prior subprocess's poll task may have buffered
        // a TERMINAL event the pool's `abort_poll_task` could not retract
        // (the poll loop resolves `poll_status` and `tx.send`s the
        // terminal with no await in between). That stale event carries
        // the OLD generation. Without this gate the secondary's terminal
        // arms resolve the task by scanning `active_tasks` for the
        // event's `worker_id` — so the stale terminal CONSUMES / mis-
        // attributes the entry of whatever task the fresh subprocess was
        // since bound to, leaving the real in-flight task orphaned
        // (assigned-never-terminal) and wedging the phase barrier.
        //
        // The check (and its WARN) is owned by the pool, which owns
        // worker identity + the per-slot generation — see
        // [`dynrunner_manager_local::WorkerPool::is_stale_event`].
        if self.op_mut().pool.is_stale_event(&event) {
            return Ok(None);
        }

        match event {
            WorkerEvent::TaskCompleted {
                worker_id,
                result,
                result_data,
                binary,
                ..
            } => {
                // Test-only: track tasks run on this secondary's own
                // workers. See `local_tasks_run` doc on
                // `SecondaryCoordinator`.
                #[cfg(test)]
                {
                    self.local_tasks_run += 1;
                }
                // Capture the per-worker estimated + actual usage
                // BEFORE `clear_task` zeroes the slot. Mirrors
                // `LocalManager::handle_task_completed`'s ordering;
                // the snapshots feed the shared memuse writer so
                // every secondary's `memuse.log` row carries the
                // same shape as the LocalManager's.
                let estimated_for_memuse = self.op_mut().pool.workers[worker_id as usize]
                    .estimated_resources
                    .clone();
                let actual_for_memuse = self.op_mut().pool.workers[worker_id as usize]
                    .actual_usage
                    .clone();
                if let Some(log_path) = self.config.memuse_log_path.as_deref() {
                    dynrunner_manager_local::memuse::log_resource_usage(
                        log_path,
                        binary.as_ref(),
                        &estimated_for_memuse,
                        &actual_for_memuse,
                        !result.success,
                    );
                }
                // Reclaim protocol state from the spawned poll task
                self.op_mut().pool.workers[worker_id as usize]
                    .reclaim_protocol()
                    .await;
                self.op_mut().pool.workers[worker_id as usize].clear_task();

                // Flush the per-task memprofile writer (if any).
                // Sampler hook is a fire-and-forget mpsc-send;
                // ordering relative to the cluster-state apply +
                // wire broadcasts below does not matter for
                // correctness — the sampler's command queue
                // serialises events.
                if let Some(b) = binary.as_ref() {
                    // `task_id` is non-optional by the framework's
                    // boundary contract; clone the verbatim value.
                    self.notify_sampler_completed(b.task_id.clone());
                }

                // Find the file hash for this worker's task
                let file_hash = self
                    .op_mut()
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());
                let log_task_hash = file_hash.clone();

                if let Some(hash) = file_hash {
                    self.op_mut().active_tasks.remove(&hash);
                    // No per-node terminal set: the authority owns the
                    // CRDT terminal mutation and every replica's
                    // `cluster_state` mirror converges to it. The
                    // secondary just clears its OWN `active_tasks` slot
                    // (above) and reports its own worker's outcome to
                    // the primary role (below).

                    if result.success {
                        // Report completion to the current primary
                        // (whichever node currently holds authority).
                        // CLASS-1 own-worker report: the authority
                        // (`PrimaryCoordinator`, reached via the
                        // `send_to_primary` loopback) owns the
                        // `ClusterMutation::TaskCompleted` origination,
                        // the keyed-outputs apply, and the phase-machine
                        // advance. The secondary never holds authority,
                        // so it neither applies the completion locally
                        // nor drives a phase machine.
                        let msg = DistributedMessage::TaskComplete {
                            target: None,
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            result_data,
                            // Stamped at the send_to_primary chokepoint (#352).
                            delivery_seq: None,
                            // Stamped at the send_to_primary chokepoint (ordering gate).
                            msgs_posted_through: None,
                        };
                        // Report to the primary role only. The AUTHORITY
                        // originates the terminal CRDT mutation
                        // (`apply_and_broadcast_cluster_mutations`) and
                        // broadcasts it to the mesh, so every peer /
                        // observer mirror converges and run-complete
                        // cues stay intact — the reporting secondary
                        // must NOT broadcast itself (a second CRDT
                        // originator would break the authority's
                        // apply-before-dispatch ordering).
                        self.send_to_primary(msg).await?;
                    } else {
                        // Reuse the upstream classification from the
                        // worker protocol; default to NonRecoverable
                        // only when the worker layer didn't tag the
                        // result (genuine "unknown" — distinct from
                        // the disconnect path which now reports
                        // Recoverable explicitly via worker.rs /
                        // protocol-manager-worker).
                        let error_type = result
                            .error_type
                            .clone()
                            .unwrap_or(ErrorType::NonRecoverable);
                        // Re-poll our OWN idle workers (own worker
                        // management — not authority). A retry the
                        // authority reinjects and re-dispatches to this
                        // node reaches an idle worker on the next tick
                        // rather than waiting a full keepalive interval.
                        self.repoll_idle_workers().await;
                        // Report error to the current primary. CLASS-1
                        // own-worker report: the authority owns the
                        // failure accounting, the retry-bucket cascade,
                        // and the terminal CRDT mutation.
                        let msg = DistributedMessage::TaskFailed {
                            target: None,
                            sender_id: self.config.secondary_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: self.config.secondary_id.clone(),
                            worker_id,
                            task_hash: hash.clone(),
                            error_type,
                            error_message: result
                                .error_message
                                .unwrap_or_else(|| "Unknown error".into()),
                            // Stamped at the send_to_primary chokepoint (#352).
                            delivery_seq: None,
                            // Stamped at the send_to_primary chokepoint (ordering gate).
                            msgs_posted_through: None,
                        };
                        // Report to the primary role only; the authority
                        // originates + broadcasts the terminal CRDT
                        // mutation (see the TaskComplete arm above).
                        self.send_to_primary(msg).await?;
                    }

                    // Request next task for this worker
                    self.request_task_for_worker(worker_id).await?;
                }

                // Operator-facing INFO: did the worker finish a
                // task, and did it succeed? The `secondary_id` is
                // redundant in per-secondary slurm_*.out files
                // (the file IS the secondary). Per-task identity
                // (task_id / phase / task_type) moves to the
                // sibling DEBUG so the operator log stays terse
                // while diagnostic context is preserved.
                tracing::info!(
                    worker_id,
                    task_hash = ?log_task_hash,
                    success = result.success,
                    "task done"
                );
                tracing::debug!(
                    secondary = %self.config.secondary_id,
                    worker_id,
                    task_id = ?binary.as_ref().map(|b| b.task_id.as_str()),
                    phase = ?binary.as_ref().map(|b| b.phase_id.to_string()),
                    task_type = ?binary.as_ref().map(|b| b.type_id.to_string()),
                    task_hash = ?log_task_hash,
                    success = result.success,
                    "task done: identity"
                );

                Ok(None)
            }
            WorkerEvent::Disconnected {
                worker_id,
                generation,
                result,
                binary,
            } => {
                // Reap the subprocess BEFORE reclaim_protocol so the
                // exit status rides the same log line as the
                // disconnect. `try_reap_exit` is non-blocking; `None`
                // means PID was untracked, kernel hasn't reaped yet
                // (SIGCHLD race), or the child was already reaped by
                // another path. See WorkerHandle::try_reap_exit for
                // the full set of None conditions.
                let exit_status = self.op_mut().pool.workers[worker_id as usize].try_reap_exit();
                if exit_status.is_none() {
                    // SIGCHLD race: the child is dead (pipe EOF observed)
                    // but the non-blocking reap missed it. Hand it to the
                    // bounded detached reaper so it cannot linger as a
                    // zombie (no other path will ever wait this pid).
                    self.op_mut().pool.workers[worker_id as usize].reap_detached_fallback();
                }

                // Reclaim protocol state from the spawned poll task
                self.op_mut().pool.workers[worker_id as usize]
                    .reclaim_protocol()
                    .await;
                self.op_mut().pool.workers[worker_id as usize].clear_task();

                tracing::warn!(
                    worker_id,
                    error = ?result.error_message,
                    exit_status = exit_status.as_ref().map(|s| s.to_string()),
                    "worker disconnected"
                );

                // Recover any pending first-bind binary first: the
                // worker died between `RespawnInProgress` and the
                // expected `Ready`, so the task held in
                // `pending_first_bind` (deferred per the respawn-HOLD
                // contract) never ran. The disconnect is itself a
                // slot-replacement trigger (the restart loop respawns
                // the slot afterward), so it drains the deferred stash
                // through the SAME `reinject_pending_first_bind`
                // chokepoint every replacement edge uses — popping it
                // HERE means the later restart-loop sweep finds nothing
                // (no double-report). The secondary is never the
                // authority, so this is the sole own-worker recovery.
                // `drained_deferred` records whether this drain resolved a
                // deferred task, so the no-active-task WARN below stays
                // silent when the deferred drain is what handled the
                // disconnect (a swept stash is recovered, not lost).
                let drained_deferred = self.reinject_pending_first_bind(worker_id).await?;

                // Find and report the task as failed
                let file_hash = self
                    .op_mut()
                    .active_tasks
                    .iter()
                    .find(|&(_, &wid)| wid == worker_id)
                    .map(|(hash, _)| hash.clone());

                if let Some(hash) = file_hash {
                    self.op_mut().active_tasks.remove(&hash);

                    // Discriminate two Disconnected-event shapes:
                    //
                    //   A. Worker explicitly reported a real
                    //      task failure on the wire — Response::
                    //      Error(NonRecoverable, msg). The
                    //      communication SUCCEEDED; the message
                    //      WAS delivered. The task ran (or at
                    //      least the worker attempted it) and
                    //      reported a non-recoverable failure.
                    //      → forward as TaskFailed(NonRecoverable)
                    //        so the primary records the real
                    //        failure (consumes retry budget if
                    //        applicable; surfaces in fail_final
                    //        per the outcome-class breakdown).
                    //
                    //   B. Pure transport-level disconnect — no
                    //      wire-level error response, just an
                    //      EOF on the manager's read end (the
                    //      protocol layer at `state.rs:152`
                    //      synthesises `Recoverable +
                    //      "transport disconnected"` for this
                    //      case; `worker.rs:154` synthesises
                    //      `Recoverable + "Disconnected before
                    //      Ready"` for the pre-Ready variant).
                    //      The communication FAILED; the task
                    //      may not have run at all.
                    //      → forward as backpressure-shaped
                    //        TaskFailed (Recoverable + the
                    //        marker the primary's
                    //        `is_backpressure` predicate
                    //        recognises) so the primary REQUEUES
                    //        the task at the pool without
                    //        consuming retry budget. The next
                    //        TaskRequest from the respawned
                    //        worker (or another peer) picks it
                    //        back up.
                    //
                    // Discriminator is `result.error_type`:
                    // NonRecoverable → real failure (A); anything
                    // else (Recoverable + transport-disconnected
                    // synthesis) → comm failure (B).
                    //
                    // Before discrimination, reclassify using the
                    // exit-status + OOM watcher: a SIGKILL with a
                    // recent kernel `oom_kill` increment upgrades
                    // the synthesised `Recoverable` to
                    // `ResourceExhausted(memory)`; SIGSEGV / SIGABRT
                    // / SIGBUS / SIGFPE / SIGILL downgrade to
                    // `NonRecoverable`. See [`classify_disconnect`].
                    let kernel_oom_recent =
                        oom_watcher.kernel_oom_recent(KERNEL_OOM_CORRELATION_WINDOW);
                    let error_type = classify_disconnect(
                        result.error_type.clone().unwrap_or(ErrorType::Recoverable),
                        exit_status.as_ref(),
                        kernel_oom_recent,
                    );
                    let is_comm_failure = matches!(error_type, ErrorType::Recoverable);

                    let (wire_error_type, wire_error_message) = if is_comm_failure {
                        (
                            ErrorType::Recoverable,
                            "worker pipe broken; respawning".to_string(),
                        )
                    } else {
                        (
                            error_type,
                            result
                                .error_message
                                .unwrap_or_else(|| "Worker disconnected".into()),
                        )
                    };

                    // Found → reporting: make the report attempt visible
                    // end-to-end so a strand investigation can see the
                    // terminal was at least ORIGINATED here (the wire-side
                    // outcome — delivered vs no-route-absorbed-and-retained —
                    // is logged inside `send_to_primary`).
                    tracing::info!(
                        worker_id,
                        task_hash = %hash,
                        error_type = ?wire_error_type,
                        "reporting disconnect terminal for worker's active task"
                    );
                    let msg = DistributedMessage::TaskFailed {
                        target: None,
                        sender_id: self.config.secondary_id.clone(),
                        timestamp: timestamp_now(),
                        secondary_id: self.config.secondary_id.clone(),
                        worker_id,
                        task_hash: hash,
                        error_type: wire_error_type,
                        error_message: wire_error_message,
                        // Stamped at the send_to_primary chokepoint (#352).
                        delivery_seq: None,
                        // Stamped at the send_to_primary chokepoint (ordering gate).
                        msgs_posted_through: None,
                    };
                    // Report to the primary role only; the authority
                    // originates + broadcasts the terminal CRDT mutation.
                    let _ = self.send_to_primary(msg).await;
                } else if !drained_deferred
                    && (result.error_type.is_some() || result.error_message.is_some())
                {
                    // The `active_tasks` scan found NO task for a worker that
                    // disconnected WITH AN ERROR, AND the deferred-first-bind
                    // drain above did NOT resolve it either. This must NEVER be
                    // silent: a disconnect carrying a fault that resolves to no
                    // in-flight task is either a prior-terminal/idle slot (a
                    // benign no-op) OR a tracking gap that strands work. Pre-fix
                    // the whole `if let Some(hash)` block was skipped with no
                    // trace, hiding the latter. WARN with the worker +
                    // generation + error so a strand investigation can see the
                    // disconnect-with-error reached a no-task resolution. (The
                    // `drained_deferred` guard suppresses the routine
                    // worker-died-before-Ready case, which the reinject path
                    // already recovered + logged.)
                    tracing::warn!(
                        worker_id,
                        generation,
                        error = ?result.error_message,
                        error_type = ?result.error_type,
                        "worker disconnected WITH ERROR but no active task bound to \
                         it and no deferred stash drained (prior-terminal/idle slot, \
                         or a tracking gap) — nothing to report"
                    );
                }

                let _ = binary; // binary info already reported

                // Flush any per-task memprofile writers attached to
                // this worker BEFORE we return — the outer loop's
                // `pool.restart_worker(wid, ...)` Drops the prior
                // `WorkerHandle`, whose `SubcgroupHandle::Drop`
                // best-effort rmdirs the per-worker cgroup leaf.
                // A sampler tick that races past the rmdir would
                // read `memory.current` against a now-empty
                // directory and the last frame silently drops. No
                // matching `TaskCompleted` will arrive on a
                // transport disconnect; this is the only flush
                // opportunity. Mirrors the same ordering invariant
                // in `LocalManager::handle_event`'s Disconnected
                // arm.
                self.notify_sampler_disconnected(worker_id);

                // Signal that this worker needs restart
                Ok(Some(worker_id))
            }
            WorkerEvent::PhaseUpdate {
                worker_id,
                phase_name,
                ..
            } => {
                tracing::debug!(worker_id, phase = %phase_name, "phase update");
                Ok(None)
            }
            WorkerEvent::CustomMessage {
                worker_id,
                topic,
                data,
                ..
            } => {
                // Consumer custom message from this node's own worker
                // (NON-TERMINAL — task attribution untouched). The
                // stale-generation gate above already dropped frames
                // from replaced subprocesses. Resolve the sending
                // task's `type_id` (the consumer's routing context),
                // then ENQUEUE for the worker-message dispatcher —
                // the consumer's `worker_message_listener` is Python
                // and must never run on this operational loop (the
                // CCD-9-shaped boundary; see
                // `crate::worker_messages`). Worker↔secondary customs
                // are deliberately node-local: no CRDT mutation, no
                // primary report — the consumer relays to the primary
                // explicitly via `SecondaryHandle.send_to_primary`
                // (feature 5) when it wants replication.
                let type_id = self.op_mut().pool.workers[worker_id as usize]
                    .current_binary
                    .as_ref()
                    .map(|b| b.type_id.to_string())
                    .unwrap_or_default();
                tracing::debug!(
                    worker_id,
                    topic = %topic,
                    bytes = data.len(),
                    task_type = %type_id,
                    "worker custom message received; dispatching to listeners"
                );
                let _ = self
                    .worker_message_tx
                    .send(crate::worker_messages::WorkerCustomMessage {
                        worker_id,
                        type_id,
                        topic,
                        data,
                    });
                Ok(None)
            }
            WorkerEvent::Keepalive { worker_id, .. } => {
                tracing::trace!(worker_id, "worker keepalive");
                Ok(None)
            }
            WorkerEvent::Ready { worker_id, .. } => {
                // Reclaim the protocol state from the background
                // `wait_ready` watcher task that either
                // `WorkerPool::ensure_worker_for_type_async` (type-shift
                // respawn) or `restart_worker_async` (worker restart)
                // spawned. The
                // watcher already wrote the Ready event (this
                // arm) to the channel before returning the new
                // `RunnerProtocolState::Idle`; `reclaim_protocol`
                // installs that state synchronously so the slot
                // observably transitions Transitioning → Idle and
                // is_idle_state returns true on the very next
                // assignment attempt.
                //
                // Idempotent on slots that didn't go through
                // spawn_ready_watcher: `reclaim_protocol` only
                // takes / awaits the JoinHandle if one is set,
                // otherwise it's a no-op. The
                // initial-pool-init Ready events (from
                // `pool.initialize`'s `wait_for_all_ready` path)
                // go through `poll_ready` directly without a
                // background task, so they hit this arm with
                // `poll_task = None` and pass through cleanly.
                self.op_mut().pool.workers[worker_id as usize]
                    .reclaim_protocol()
                    .await;
                // Drop the per-worker `TaskRequest` backoff window.
                // The slot just transitioned out of the
                // type-shift Transitioning state — any backoff
                // accrued against the PRIOR Ready cycle (e.g. the
                // TaskRequest the setup loop fired before the
                // respawn happened) is stale; gating the
                // post-Ready repoll on it would freeze the
                // bounced-task pickup for up to `MAX_BACKOFF`
                // seconds. Mirrors the `reset_backoff` call in
                // the assign_task success path and the
                // `on_primary_changed` reset on
                // primary-identity flips — the rule is "any
                // observable transition that revives the slot's
                // pull semantic resets the rate-limiter".
                self.op_mut().primary_link.reset_backoff(worker_id);
                // Pending first-bind binary (if any): the dispatch
                // arm in `dispatch/router.rs` (peer-assigned) or
                // `primary/task_request.rs` (self-assigned)
                // stashed the binary in `pending_first_bind` when
                // it hit `EnsureWorkerOutcome::RespawnInProgress`.
                // The slot is now Idle with the correct
                // `loaded_type_id`; the same-type fast path inside
                // `ensure_worker_for_type_async` would be a no-op,
                // so we go straight to `assign_task`. Stashing-then-
                // assign avoids the bounce-as-backpressure wire
                // round-trip that the original type-shift fix used
                // and preserves the dispatch-arm's worker-target
                // choice (matters for the
                // `setup_promote_multi_secondary_distributes_to_idle_peers_on_promote`
                // distribution-fairness contract).
                if let Some(pending) = self.op_mut().pending_first_bind.remove(&worker_id) {
                    let super::super::PendingFirstBind {
                        binary,
                        file_hash,
                        estimated,
                        predecessor_outputs,
                    } = pending;
                    let log_task_hash = file_hash.clone();
                    // Run-terminal gate (asm-dataset run_20260611_112116,
                    // secondary-6's zombie): once the replicated `RunAborted`
                    // verdict is latched, this continuation must NOT bind the
                    // deferred task ("pending first-bind assigned post-Ready"
                    // 3+ minutes post-abort). The stash is DROPPED — not
                    // reinjected to the authority (the authority exits on the
                    // same verdict; there is no run left to requeue into) —
                    // and this loop's own tail exits on the same latch within
                    // this iteration, tearing every worker down.
                    if let Some(reason) = self.cluster_state.run_aborted() {
                        tracing::info!(
                            worker_id,
                            task_hash = %log_task_hash,
                            reason = %reason,
                            "pending first-bind NOT assigned post-Ready: the \
                             replicated run-terminal verdict is latched; \
                             dropping the deferred task (this node exits on \
                             the same latch)"
                        );
                        return Ok(None);
                    }
                    // #360 gate: this continuation is a DISPATCH-shaped
                    // bind (it puts work on a worker without the
                    // authority's per-tick gate seeing it again), so it
                    // re-consults the member-side half of the pairwise
                    // dispatch-readiness predicate before binding. The
                    // production bypass: the stash was taken while this
                    // member's mesh leg to the current primary was
                    // unconfirmed (or the primary identity changed across
                    // the respawn, re-arming the confirmation), the
                    // primary's own gate was actively withholding work
                    // ("member remains unassignable until its mesh leg
                    // confirms") — and this arm assigned anyway ("pending
                    // first-bind assigned post-Ready"), running a task
                    // whose terminal then swallowed on the half-formed
                    // egress leg. An unconfirmed leg at Ready-time routes
                    // the deferred task through the SAME reinject contract
                    // every other deferred-loss edge uses
                    // (`report_deferred_task_lost`, backpressure-shaped):
                    // the authority requeues it for a confirmed member.
                    // The worker is healthy (it reached Ready) and stays
                    // idle — the periodic idle-repoll pulls fresh work
                    // once the leg settles, and a re-dispatch of the same
                    // type binds via the same-type fast path.
                    if !self.mesh_leg_confirmed_for_bind().await {
                        tracing::warn!(
                            worker_id,
                            task_hash = %log_task_hash,
                            "pending first-bind NOT assigned post-Ready: this \
                             member's mesh leg to the current primary is \
                             unconfirmed, so a bound task's terminal could \
                             swallow on the half-formed egress leg; reinjecting \
                             the deferred task to the authority instead"
                        );
                        self.report_deferred_task_lost(worker_id, &log_task_hash)
                            .await?;
                        return Ok(None);
                    }
                    let task_id_log = binary.task_id.clone();
                    let phase_log = binary.phase_id.clone();
                    let type_log = binary.type_id.clone();
                    let estimated_mb =
                        estimated.get(&dynrunner_core::ResourceKind::memory()) / (1024 * 1024);
                    // Snapshot for the sampler hook before the
                    // move into `assign_task`. Same pattern as
                    // the other secondary assign sites — see
                    // `notify_sampler_assigned` doc.
                    let binary_for_hook = binary.clone();
                    match self.op_mut().pool.workers[worker_id as usize]
                        .assign_task(binary, estimated, false, predecessor_outputs)
                        .await
                    {
                        Ok(()) => {
                            self.notify_sampler_assigned(worker_id, &binary_for_hook);
                            self.op_mut().active_tasks.insert(file_hash, worker_id);
                            tracing::info!(
                                worker_id,
                                task_id = ?task_id_log,
                                phase = %phase_log,
                                task_type = %type_log,
                                task_hash = %log_task_hash,
                                estimated_mb,
                                "pending first-bind assigned post-Ready"
                            );
                        }
                        Err(e) => {
                            // The freshly-spawned worker died
                            // between Ready and the assign_task
                            // write. Reap so the log shows the
                            // actual signal/code rather than the
                            // pipe-level error string. Recovery
                            // shape mirrors the same-arm
                            // [Disconnected] path: the deferred task
                            // never ran, so report it back to the
                            // authority as backpressure so it requeues
                            // + re-dispatches.
                            let exit_status =
                                self.op_mut().pool.workers[worker_id as usize].try_reap_exit();
                            tracing::warn!(
                                worker_id,
                                error = %e,
                                exit_status = exit_status.as_ref().map(|s| s.to_string()),
                                task_hash = %log_task_hash,
                                "pending first-bind assign_task failed; reporting \
                                 deferred task back to the authority"
                            );
                            self.schedule_worker_restart(worker_id);
                            self.report_deferred_task_lost(worker_id, &log_task_hash)
                                .await?;
                        }
                    }
                    return Ok(None);
                }
                // Repoll for a task now that the slot is Idle
                // again. Mirrors the post-TaskCompleted repoll
                // (line 165 above) so the type-shift respawn
                // path picks up its bounced task on the next
                // primary round-trip without waiting for the
                // periodic `repoll_idle_workers` keepalive tick.
                // Errors are propagated through `?` since a
                // send-to-primary failure here is the same wire
                // error class as elsewhere in this handler.
                self.request_task_for_worker(worker_id).await?;
                tracing::debug!(worker_id, "worker ready (post-respawn reclaim)");
                Ok(None)
            }
        }
    }

    /// Schedule a worker-slot restart: store the EARLIEST instant the
    /// restart may execute, derived ONCE here from the pool's
    /// startup-crash backoff ([`dynrunner_manager_local::pool::WorkerPool::
    /// restart_backoff_delay`] — zero for a healthy worker that died
    /// mid-task, exponential for a subprocess that died before Ready).
    /// The operational loop's restart tail executes due entries; its
    /// wake arm parks on [`Self::next_worker_restart_due`]. Storing the
    /// deadline at schedule time (never deriving it at the arm) is the
    /// persistent-deadline law: sibling arms firing cannot push a
    /// backed-off respawn out. Re-scheduling an already-pending slot
    /// overwrites the due instant (the inputs are identical, so the
    /// value is too — last-writer-wins is a no-op).
    pub(in crate::secondary) fn schedule_worker_restart(&mut self, worker_id: WorkerId) {
        let delay = self.op_mut().pool.restart_backoff_delay(worker_id);
        if !delay.is_zero() {
            // Operator-visible brake engagement: the slot's subprocess
            // died before Ready (broken worker image / module import
            // error is the canonical cause), so the respawn waits out a
            // backoff slot instead of crash-looping at runtime speed.
            tracing::warn!(
                worker_id,
                delay_secs = delay.as_secs_f64(),
                "worker subprocess died before Ready; backing off its \
                 respawn (startup-crash backoff — a broken worker image \
                 would otherwise respawn-crash-loop at runtime speed)"
            );
        }
        let due = std::time::Instant::now() + delay;
        self.op_mut().pending_worker_restarts.insert(worker_id, due);
    }

    /// The operational loop's worker-restart wake deadline: the
    /// EARLIEST stored due instant across the scheduled restarts,
    /// `None` when nothing is pending (the wake arm parks).
    pub(in crate::secondary) fn next_worker_restart_due(&self) -> Option<std::time::Instant> {
        self.op_ref()?
            .pending_worker_restarts
            .values()
            .min()
            .copied()
    }
}
