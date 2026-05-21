//! Primary-side worker-pull dispatch: respond to a peer's
//! `TaskRequest` by picking a suitable binary from the local pool and
//! sending back a `TaskAssignment`.
//!
//! Single concern: turn one peer's "I have capacity" signal into one
//! `TaskAssignment` (or a no-op when the pool can't satisfy it).
//! Worker assignment, in-flight ledger book-keeping, broadcast of the
//! `TaskAssigned` cluster mutation, and backpressure / dead-peer
//! recovery all flow through this method.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, WorkerId};
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    DistributedBinaryInfo, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::wire::timestamp_now;
use super::super::{PrimaryInFlightItem, SecondaryCoordinator};
use super::task_file_hash;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Handle a task request from a peer when acting as primary.
    /// Finds a suitable task and sends a TaskAssignment back.
    pub(in crate::secondary) async fn handle_primary_task_request(
        &mut self,
        requesting_secondary_id: String,
        worker_id: WorkerId,
        available_memory: u64,
        factory: &mut impl WorkerFactory<M>,
    ) -> Result<(), String> {
        if self.primary_pending_is_empty() {
            tracing::debug!(
                secondary = %requesting_secondary_id,
                worker_id,
                "no pending tasks for primary assignment"
            );
            return Ok(());
        }

        // Drop tasks completed elsewhere since population. The hash is
        // computed from path+identifier exactly the way the dispatch
        // path does so the same key space matches both sides.
        let completed_tasks = self.completed_tasks.clone();
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.retain(|item| !completed_tasks.contains(&task_file_hash(item)));
        }

        if self.primary_pending_is_empty() {
            return Ok(());
        }

        // Per-peer backpressure backoff (mirrors regular primary's
        // `is_backpressured`): if this peer recently bounced an
        // assignment with "No idle worker available", skip the
        // dispatch entirely. The binary stays in the pool — another
        // peer's TaskRequest can pick it up in the meantime.
        // Self-assignments bypass the check because the local
        // `is_idle_state()` test below is the source of truth for
        // our own workers; the peer-side backpressure ledger is
        // populated only by remote rejections.
        if requesting_secondary_id != self.config.secondary_id
            && self.is_primary_peer_backpressured(&requesting_secondary_id)
        {
            tracing::trace!(
                secondary = %requesting_secondary_id,
                worker_id,
                "skipping primary dispatch: peer is in backpressure backoff"
            );
            return Ok(());
        }

        // Find a task that fits the available memory; remove it from
        // the pool so it isn't handed out twice. `take_first_match`
        // walks bucket-key order, FIFO inside each bucket — same
        // ordering the original Vec produced after the size-DESC sort.
        let estimator = self.estimator.clone();
        let kind_memory = dynrunner_core::ResourceKind::memory();
        let assigned = self
            .primary_pending
            .as_mut()
            .and_then(|pool| {
                pool.take_first_match(|item| {
                    let estimated = estimator.estimate(item);
                    estimated.get(&kind_memory) <= available_memory
                })
            });

        if let Some(binary) = assigned {
            let file_hash = task_file_hash(&binary);
            let log_task_hash = file_hash.clone();
            // The pool's `take_first_match` is a removal-only primitive
            // — it does not bump in-flight. Pair the dispatch with an
            // explicit `mark_in_flight` so the phase machine treats
            // the item as still belonging to the phase until the
            // cluster reports it finished. `primary_in_flight` mirrors
            // the same fact at the per-item level so we can call
            // `on_item_finished(phase_id)` when TaskComplete /
            // TaskFailed arrives later, AND retains the binary +
            // target so the rejection-recovery path
            // (`handle_primary_peer_rejection`) can `pool.requeue` it.
            let dispatched_phase = binary.phase_id.clone();
            if let Some(pool) = self.primary_pending.as_mut() {
                pool.mark_in_flight(&dispatched_phase);
            }
            self.primary_in_flight.insert(
                file_hash.clone(),
                PrimaryInFlightItem {
                    phase_id: dispatched_phase,
                    target_secondary_id: requesting_secondary_id.clone(),
                    binary: binary.clone(),
                },
            );

            if requesting_secondary_id == self.config.secondary_id {
                // Assign directly to local worker (avoid recursive
                // dispatch_message cycle). Route through the
                // three-mode helper so pre-staged + uses-not-file-
                // based modes don't bypass the resolver and ship
                // unresolvable paths to the worker.
                //
                // Resolution-miss policy mirrors the historical
                // primary self-assign behaviour: in
                // file-based + non-pre-staged mode the absolute
                // path is the worker's filesystem view (in-process
                // SLURM secondaries share the gateway's FS), so a
                // None resolution falls through to passthrough. In
                // pre-staged mode the absolute path is the
                // gateway-host view that the container can't see;
                // None there is a configuration error worth failing
                // loudly to avoid the misleading worker-level
                // "Not a valid binary file" buried in angr / ghidra.
                let resolution_path = binary.path.to_string_lossy().into_owned();
                let resolved =
                    self.resolve_for_dispatch(None, &resolution_path, &file_hash);
                let actual_binary = match resolved {
                    Some(path) => {
                        // Surface the locally-resolved on-disk path
                        // via `resolved_path`; keep `binary.path` as
                        // the wire-supplied identifier so the worker
                        // sees the consumer's relative-under-source
                        // string in `task.relative_path` for output
                        // mirroring.
                        let mut b = binary.clone();
                        b.resolved_path = Some(path);
                        b
                    }
                    None if self.pre_staged_mode() => {
                        tracing::error!(
                            worker_id,
                            file_hash = %file_hash,
                            path = %resolution_path,
                            "primary self-assign in pre-staged mode: \
                             binary path unresolvable via src_network; \
                             dropping (likely pre-staged mode misconfiguration \
                             or stale wire path)"
                        );
                        return Ok(());
                    }
                    None => binary.clone(),
                };
                let estimated = self.estimator.estimate(&actual_binary);
                let wid = worker_id.min(self.pool.workers.len() as u32 - 1);
                if self.pool.workers[wid as usize].is_idle_state() {
                    // Per-type subprocess dispatch via the async-event
                    // flow for both first-bind AND true type-shift.
                    // Same wedge prevention as `dispatch/router.rs`'s
                    // TaskAssignment arm; see that arm for the
                    // full rationale. The pre-fix `is_true_type_shift`
                    // discriminator left first-bind on the sync
                    // `ensure_worker_for_type` path, which blocked
                    // the secondary's `select!` for the full Python
                    // worker import time on the asm-tokenizer LMU
                    // dispatch.
                    //
                    // Pending-binary stash: keeping the binary on
                    // this secondary's `pending_first_bind` map
                    // (rather than bouncing as backpressure via
                    // `recover_in_flight_to_pool`) preserves
                    // distribution fairness on the promote-and-
                    // distribute workloads — peer secondaries pay
                    // one wire round-trip per first-bind, the
                    // promoted-primary's self-assigns would reach
                    // the same-type fast path sub-ms after Ready,
                    // and the gap let the promoted node burn
                    // through the entire pool. Stashing keeps the
                    // binary pinned to the worker the dispatch arm
                    // chose; the Disconnected handler recovers via
                    // `recover_in_flight_to_pool` if the worker
                    // dies before Ready (the `primary_in_flight`
                    // entry was inserted upstream of this match,
                    // outside this if-else, so it stays alive
                    // across the pending stash).
                    let ensure_result: Result<(), String> = match self
                        .pool
                        .ensure_worker_for_type_async(wid, &actual_binary.type_id, factory, false)
                        .await
                    {
                        Ok(dynrunner_manager_local::EnsureWorkerOutcome::AlreadyLoaded) => Ok(()),
                        Ok(dynrunner_manager_local::EnsureWorkerOutcome::RespawnInProgress) => {
                            tracing::debug!(
                                worker_id = wid,
                                type_id = %actual_binary.type_id,
                                file_hash = %file_hash,
                                "primary self-assign: type-bind respawn issued; \
                                 stashing binary in pending_first_bind until Ready arrives"
                            );
                            self.pending_first_bind.insert(
                                wid,
                                super::super::PendingFirstBind {
                                    binary: actual_binary,
                                    file_hash,
                                    estimated,
                                    source: super::super::BindSource::PrimarySelfAssign,
                                },
                            );
                            return Ok(());
                        }
                        Err(e) => Err(e),
                    };
                    if let Err(e) = ensure_result {
                        tracing::warn!(
                            worker_id = wid,
                            error = %e,
                            type_id = %actual_binary.type_id,
                            "primary self-assign: ensure_worker_for_type failed; re-queuing binary"
                        );
                        self.pending_worker_restarts.insert(wid);
                        self.recover_in_flight_to_pool(&file_hash);
                        return Ok(());
                    }
                    match self.pool.workers[wid as usize]
                        .assign_task(actual_binary, estimated, false)
                        .await
                    {
                        Ok(()) => {
                            self.active_tasks.insert(file_hash, wid);
                            self.primary_link.reset_backoff(wid);
                        }
                        Err(e) => {
                            // `assign_task` failed AFTER we already
                            // moved the binary out of the pool +
                            // bumped in_flight + recorded
                            // `primary_in_flight`. Walk every step
                            // back so the binary isn't silently
                            // dropped: `recover_in_flight_to_pool`
                            // pulls the binary out of `primary_in_flight`
                            // and `pool.requeue`s it (decrements
                            // in_flight + pushes to front of bucket).
                            // Mirrors the recovery shape of
                            // `handle_primary_peer_rejection` below.
                            //
                            // SendFailed (the typical `Err` shape
                            // here) means the worker subprocess died
                            // between tasks — the pipe is broken.
                            // Reap so the log carries the actual
                            // signal/code, not just "Broken pipe".
                            // No `Disconnected` event will fire on
                            // this path (poll_loop is only spawned
                            // for an Assigned task), so this is the
                            // only log line the operator sees for a
                            // self-assign-time death.
                            //
                            // Bug B: queue this worker for respawn
                            // so the next `process_tasks` tick
                            // brings a fresh subprocess up. Without
                            // this, the docstring contract on
                            // `NonRecoverableError` ("worker process
                            // is restarted on the next assignment")
                            // was unmet on the SLURM-secondary path:
                            // the dead pipe stayed dead and every
                            // subsequent assignment to this slot
                            // failed the same way.
                            //
                            // Bug C: the binary is recovered back to
                            // the local pool (via
                            // `recover_in_flight_to_pool`), NOT
                            // marked as failed. The task hasn't
                            // been attempted by a worker — the
                            // pipe-write never landed. Once the
                            // worker respawns, its first
                            // TaskRequest re-picks the binary from
                            // the pool. Task retried, not lost.
                            let exit_status =
                                self.pool.workers[wid as usize].try_reap_exit();
                            tracing::warn!(
                                worker_id = wid,
                                error = %e,
                                exit_status = exit_status.as_ref().map(|s| s.to_string()),
                                "primary self-assign failed; queuing worker respawn + re-queuing binary"
                            );
                            self.pending_worker_restarts.insert(wid);
                            self.recover_in_flight_to_pool(&file_hash);
                        }
                    }
                } else {
                    // `is_idle_state()` flipped between the worker's
                    // TaskRequest and this dispatch (race after a
                    // recent kickstart-assignment landed first). Pre-
                    // fix this branch was missing entirely — the
                    // binary stayed `primary_in_flight`-tracked but no
                    // worker was processing it and no completion
                    // would ever arrive ⇒ silent task loss + stuck
                    // phase counter. Recover by undoing the take +
                    // mark_in_flight via `recover_in_flight_to_pool`.
                    tracing::warn!(
                        worker_id = wid,
                        "primary self-assign skipped: worker not idle (race with kickstart); re-queuing binary"
                    );
                    self.recover_in_flight_to_pool(&file_hash);
                }
            } else {
                // Send TaskAssignment to peer
                let msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.secondary_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: requesting_secondary_id.clone(),
                    worker_id,
                    zip_file: None,
                    binary_info: DistributedBinaryInfo::from_task_info(&binary),
                    local_path: binary.path.to_string_lossy().into_owned(),
                    file_hash,
                    // Phase 4 will populate from the cluster-state
                    // task-outputs cache. Empty here keeps wire bytes
                    // identical to pre-feature on no-dep tasks.
                    predecessor_outputs: std::collections::BTreeMap::new(),
                };
                let _ = self
                    .peer_transport
                    .send_to_peer(&requesting_secondary_id, msg)
                    .await;
            }

            tracing::info!(
                secondary = %requesting_secondary_id,
                worker_id,
                task_id = ?binary.task_id,
                phase = %binary.phase_id,
                task_type = %binary.type_id,
                task_hash = %log_task_hash,
                remaining = self.primary_pending_len(),
                "primary assigned task"
            );
        }

        Ok(())
    }
}
