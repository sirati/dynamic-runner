
use std::time::Instant;

use dynrunner_core::{TaskInfo, Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    Address, ClusterMutation, DistributedMessage, PeerTransport, Role,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};


use super::PrimaryCoordinator;
use super::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {
    pub(super) async fn handle_task_request(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        if let DistributedMessage::TaskRequest {
            ref secondary_id,
            worker_id,
            ref available_resources,
            ..
        } = msg
        {
            let available_res: ResourceMap = available_resources.iter()
                .map(|r| (r.kind.clone(), r.amount))
                .collect();
            // Find matching worker
            let mut target_idx = None;
            let mut local_idx: u32 = 0;
            for (idx, w) in self.workers.iter().enumerate() {
                if w.secondary_id == *secondary_id {
                    if local_idx == worker_id {
                        target_idx = Some(idx);
                        break;
                    }
                    local_idx += 1;
                }
            }

            let mut assigned = false;

            // Demoted observer mode: the promoted primary is
            // the sole authority for assignment. Skip the local-
            // assign branch entirely so the request always falls
            // through to the primary relay below — that way
            // only one primary's pool ever decides what runs where.
            // Without this skip, the local primary would race the
            // primary by assigning from its own (post-handoff
            // stale) pool view. See `demoted` doc on
            // `PrimaryCoordinator`.
            if let Some(idx) = target_idx
                && !self.demoted
            {
                // Stale TaskRequest guard: if primary's view says this
                // worker is already mid-dispatch (current_task =
                // Some(_)), the kickstart in `handle_task_complete` /
                // `handle_task_failed` has just sent a TaskAssignment
                // to the same worker. The TaskRequest in our hand was
                // sent by the secondary BEFORE that kickstart-
                // assignment arrived. Honouring it would dispatch a
                // SECOND assignment to a worker that's about to be
                // busy with the first, secondary then bounces the
                // second with "No idle worker available" — every such
                // bounce becomes a Recoverable failure that consumes
                // a retry budget. Skip silently; the worker will
                // process the kickstart-assignment and send a fresh
                // TaskRequest after that one terminates.
                if self.workers[idx].current_task.is_some() {
                    tracing::trace!(
                        secondary = %secondary_id,
                        worker_id,
                        "stale TaskRequest after kickstart-dispatch; skipping"
                    );
                    return Ok(());
                }
                // Backpressure guard: if the secondary is in backoff
                // (recently returned "No idle worker available"),
                // skip dispatch to its workers. The TaskRequest may
                // be for a worker that's actually idle, but since
                // we're not 100% confident the secondary's pool will
                // accept new work right now, defer until the backoff
                // window expires (cleared on the next successful
                // TaskComplete from this secondary, or when the
                // 500ms backoff naturally elapses).
                if self.is_backpressured(secondary_id) {
                    return Ok(());
                }
                // Mark worker idle
                self.workers[idx].current_task = None;
                self.workers[idx].estimated_resources = ResourceMap::new();
                self.workers[idx].is_idle = true;
                if !available_res.is_empty() {
                    self.workers[idx].resource_budgets = available_res.clone();
                }

                // Try to assign from local pending. The pool's
                // `view_for_worker` returns the soft-pin priority order
                // for this worker; the scheduler picks the index, the
                // pool commits the take.
                let global_wid = self.workers[idx].worker_id;
                let view = self.cap_filter_view(self.pool().view_for_worker(global_wid));
                if !view.is_empty() {
                    let worker_info = self.workers[idx].budget_info();
                    let all_infos: Vec<WorkerBudgetInfo<I>> =
                        self.workers.iter().map(|w| w.budget_info()).collect();
                    let max_res = self.workers[idx].resource_budgets.clone();

                    let decision = self.scheduler.assign_normal(
                        &worker_info,
                        &all_infos,
                        view.as_slice(),
                        &max_res,
                        &self.estimator,
                        false,
                    );

                    if let AssignmentDecision::Assign {
                        binary_index,
                        estimated_usage,
                        ..
                    } = decision
                    {
                        let binary = self.pool_mut().take_from_view(view, binary_index);
                        self.reserve_type_slot(&binary.type_id);
                        let sec_id = self.workers[idx].secondary_id.clone();
                        self.workers[idx].current_task = Some(binary.clone());
                        self.workers[idx].estimated_resources = estimated_usage.clone();
                        self.workers[idx].is_idle = false;

                        let task_hash = compute_task_hash(&binary);
                        let assignment_msg = DistributedMessage::TaskAssignment {
                            sender_id: self.config.node_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: sec_id.clone(),
                            worker_id,
                            zip_file: None,
                            binary_info: binary_to_distributed(&binary),
                            local_path: self.config.wire_local_path(&binary),
                            file_hash: task_hash.clone(),
                        };

                        // Same partial-commit-leak rollback as
                        // `dispatch_to_idle_workers`: a send_to
                        // failure here pre-fix left the worker's
                        // current_task set + is_idle=false + pool
                        // in_flight bumped. dispatch_order then
                        // skipped the slot forever; the leaked
                        // in_flight never decremented because no
                        // TaskComplete/TaskFailed could arrive for
                        // a task that wasn't sent. asm-tokenizer's
                        // 33-in_flight/active=0 jam at 84f669c is
                        // the operator-facing symptom of cumulative
                        // leaks from this and the sibling path.
                        if let Err(send_err) =
                            self.transport.send_to(&sec_id, assignment_msg).await
                        {
                            tracing::warn!(
                                secondary = %sec_id,
                                worker_id,
                                task_hash = %task_hash,
                                error = %send_err,
                                "task-assignment send failed; rolling back worker state and requeuing binary"
                            );
                            self.workers[idx].current_task = None;
                            self.workers[idx].estimated_resources =
                                ResourceMap::new();
                            self.workers[idx].is_idle = true;
                            self.release_type_slot(&binary.type_id);
                            self.pool_mut().requeue(binary);
                            // Return early without setting
                            // `assigned`: the binary is back in
                            // the pool, the slot is open again,
                            // and the requesting secondary will
                            // retry the TaskRequest on its next
                            // tick. Falling through to the
                            // relay-to-primary arm would re-send
                            // the same TaskRequest we just failed
                            // to handle, looping work back.
                            return Ok(());
                        }

                        // Operator-facing INFO: which secondary/
                        // worker just took the task. Per-task
                        // identity (task_id / phase / type) →
                        // DEBUG sibling.
                        tracing::info!(
                            secondary = %sec_id,
                            worker_id,
                            task_hash = %task_hash,
                            "task assigned"
                        );
                        tracing::debug!(
                            secondary = %sec_id,
                            worker_id,
                            task_id = ?binary.task_id,
                            phase = %binary.phase_id,
                            task_type = %binary.type_id,
                            task_hash = %task_hash,
                            "task assigned: identity"
                        );
                        assigned = true;
                    }
                }
            }

            // If no local assignment was made, relay to whoever
            // currently holds the primary role. Pre-Step-5 this branch
            // dispatched via `self.transport.send_to(&self.primary_id,
            // msg)` — but `self.primary_id` is the post-promotion
            // PROMOTED-PEER's id while the writer-task on the other
            // side of that per-secondary channel exits the moment it
            // observes `PromotePrimary`. The pre-Step-5 hotfix
            // (commit 7845851) guarded this branch with
            // `!self.demoted` to drop the relay outright after
            // demotion — benign but lossy: the requesting secondary
            // re-issues on its next backoff tick, but until then the
            // request is silently dropped.
            //
            // Step 5 collapses the guard structurally: addressing by
            // role (`Address::Role(Role::Primary)`) resolves through
            // the `peer_transport`'s write-through `RoleTable` cache,
            // which `cluster_state` updates on every `PrimaryChanged`
            // apply (post-promotion the cache points at the promoted
            // peer's id; pre-promotion it's cold and `send` returns
            // Err, which we silently swallow — same observable
            // behaviour as the pre-Step-5 `self.primary_id.is_none()`
            // skip). The wire frame is `RoleAddressed { intended_role:
            // Primary, payload: msg, .. }`; the receiver's Step-4
            // relay-and-hint absorbs the rare case where THIS sender's
            // cache is stale relative to the receiver's view.
            //
            // The demoted-primary path stays correct without a
            // `self.demoted` special case: role addressing routes via
            // the mesh-level peer link (still alive across promotion
            // per `feedback_mesh_independent_of_role_and_membership.md`)
            // regardless of who's authoritative.
            if !assigned
                && let Err(e) = self
                    .peer_transport
                    .send(Address::Role(Role::Primary), msg)
                    .await
            {
                tracing::debug!(
                    error = %e,
                    "primary-bound relay via Address::Role(Primary) dropped; \
                     secondary will retry on its next backoff tick"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn handle_task_complete(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskComplete {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } = &msg
        {
            // Dedup gate (#50 peer-forwarding redundancy):
            // peers forward observed-via-peer-mesh TaskCompletes
            // to the primary so wire-loss on the originator's
            // direct send is covered by N-1 redundant paths.
            // First receipt does all side-effects (counter
            // updates, type-slot release, phase cascade, kickstart
            // dispatch, secondary forward); subsequent receipts
            // are no-ops. Without this gate, type_slot would
            // double-decrement, `note_item_completed` would
            // double-fire on the phase machine, and the log line
            // would emit N times per task.
            //
            // `completed_tasks` is the dedup key: it's only
            // populated below (and from `mirror_mutation_to_accounting`
            // on cluster_state broadcasts the primary RECEIVES,
            // which the live primary doesn't normally — see
            // `peer.rs` comment chain). Idempotent via HashSet.
            if self.completed_tasks.contains(task_hash) {
                return;
            }
            let secondary_id = secondary_id.clone();
            let worker_id = *worker_id;
            // A TaskComplete supersedes a prior TaskFailed for the
            // same hash. Without this, a forwarded retry-success
            // from the primary would leave the local primary's
            // `failed_tasks` populated with a hash that was actually
            // retried OK, inflating `failed_count()` reports.
            // Pre-demotion, `run_retry_passes` cleared the entire
            // `failed_tasks` set at the start of a retry pass and
            // added back only the tasks that failed AGAIN; once the
            // local primary stops running retry passes, the per-hash
            // remove here keeps the same invariant: a hash sits in
            // exactly one of {completed, failed}.
            self.failed_tasks.remove(task_hash);
            self.completed_tasks.insert(task_hash.clone());
            // Replicated-ledger update: every node mirrors the
            // post-completion state by applying this mutation. CRDT
            // semantics make duplicate applies (e.g. on a re-broadcast
            // for a task already terminal locally) a NoOp.
            self.apply_and_broadcast_cluster_mutations(vec![
                ClusterMutation::TaskCompleted {
                    hash: task_hash.clone(),
                },
            ])
            .await;

            // A successful TaskComplete from this secondary proves
            // it's healthy — clear any backpressure backoff. The
            // backoff window is short (500ms by default) so this
            // matters mostly for high-throughput runs where one
            // completion lands while a previous backpressure-window
            // is still active.
            self.backpressured_secondaries.remove(&secondary_id);

            // Mark the specific worker idle using secondary_id + local worker_id.
            // Capture the phase + type of the just-finished item so we
            // can fold it into per-phase counters, release the
            // per-type concurrency slot, and run the phase lifecycle
            // cascade.
            let mut completed_meta: Option<(
                dynrunner_core::PhaseId,
                dynrunner_core::TypeId,
                Option<String>,
            )> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        if let Some(task) = w.current_task.take() {
                            completed_meta = Some((
                                task.phase_id.clone(),
                                task.type_id.clone(),
                                task.task_id.clone(),
                            ));
                        }
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            // Operator-facing INFO: enough to grep "did task X
            // complete and how are aggregate counts moving?". The
            // per-task identity fields (task_id / phase / task_type)
            // are diagnostic noise on the routine path — they go to
            // the DEBUG sibling below so debugging keeps the info
            // but the operator log stays terse.
            let outcome = self.outcome_summary();
            tracing::info!(
                secondary = %secondary_id,
                worker_id,
                task_hash = %task_hash,
                succeeded = outcome.succeeded,
                fail_retry = outcome.fail_retry,
                fail_oom = outcome.fail_oom,
                fail_final = outcome.fail_final,
                "task complete"
            );
            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                task_id = ?completed_meta.as_ref().and_then(|(_, _, t)| t.as_deref()),
                phase = ?completed_meta.as_ref().map(|(p, _, _)| p.to_string()),
                task_type = ?completed_meta.as_ref().map(|(_, t, _)| t.to_string()),
                task_hash = %task_hash,
                "task complete: identity"
            );

            if let Some((phase, type_id, task_id)) = completed_meta {
                self.release_type_slot(&type_id);
                self.note_item_completed(&phase, task_id.as_deref());
            }

            // Kickstart dispatch to every idle worker. After
            // `note_item_completed` runs the phase-lifecycle cascade,
            // a previously-Blocked phase may have just transitioned
            // to Active. Workers that have been idle since startup
            // (because their initial TaskRequest got "no work" when
            // the new phase wasn't yet active) won't re-poll on their
            // own — they sent their last TaskRequest already, got
            // nothing, and are waiting for an unsolicited
            // TaskAssignment. Without this kickstart, a 2-phase task
            // graph where phase-N has 1 item and phase-(N+1) has the
            // rest would stall after the phase-N item finishes —
            // the originating secondary's worker DOES re-request via
            // its `request_task_for_worker(0)` in
            // `processing.rs:193`, but every OTHER secondary's
            // workers don't. Same kickstart pattern as
            // `run_retry_passes` uses after re-injection.
            //
            // Idempotent: if no phase advanced (the common case for
            // mid-phase completions where the phase still has queued
            // work), `dispatch_to_idle_workers` finds the soft-pin
            // soft-pin order returns the originating worker first and
            // the kickstart no-ops by definition. If multiple phases
            // cascaded done in one tick (chain of empty phases →
            // first populated phase), every newly-active phase's
            // items are seen.
            self.dispatch_to_idle_workers().await.ok();

            // Belt-and-suspenders: forward to every other secondary
            // so each one's `completed_tasks` cache stays current.
            // The originating secondary already broadcasts
            // peer-to-peer (processing.rs), but that's best-effort;
            // a primary-side forward closes the gap if a peer
            // broadcast was lost. Without this, on local-death-then-
            // failover, a secondary missing the peer broadcast
            // would re-dispatch the already-completed task. (Same
            // failover-survivability invariant the FullTaskList
            // broadcast used to enforce in 04d9012, now achieved
            // continuously via per-completion ClusterMutation
            // broadcasts.)
            self.forward_completion_to_secondaries(&msg, &secondary_id)
                .await;
        }
    }

    /// Send `msg` to every connected secondary except the one that
    /// originated it. Per-secondary failures are logged and continue
    /// — a missed completion forward just risks a re-dispatch on
    /// failover, not a run-wide failure.
    async fn forward_completion_to_secondaries(
        &mut self,
        msg: &DistributedMessage<I>,
        origin_secondary_id: &str,
    ) {
        let recipients: Vec<String> = self
            .secondaries
            .keys()
            .filter(|id| id.as_str() != origin_secondary_id)
            .cloned()
            .collect();
        for secondary_id in &recipients {
            if let Err(e) = self
                .transport
                .send_to(secondary_id, msg.clone())
                .await
            {
                tracing::debug!(
                    secondary = %secondary_id,
                    error = %e,
                    "failed to forward task completion; that secondary may re-dispatch on failover"
                );
            }
        }
    }

    pub(super) async fn handle_task_failed(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskFailed {
            secondary_id,
            worker_id,
            task_hash,
            error_type,
            error_message,
            ..
        } = &msg
        {
            // Dedup gate (#50 peer-forwarding redundancy):
            // Same shape as `handle_task_complete`'s dedup —
            // peers forward observed peer-TaskFailed events to
            // the primary; the first receipt does all the
            // bookkeeping, subsequent ones are no-ops. Without
            // this, `note_item_failed` would double-fire (phase
            // counter goes wrong) and `type_slot` would double-
            // decrement.
            //
            // `failed_tasks` is the dedup key for non-superseded
            // failures. The `completed_tasks` check covers the
            // separate case "TaskFailed arrives late after the
            // task already TaskComplete'd" (the existing
            // line-538 invariant: never regress
            // completed → failed). Backpressure-shaped
            // TaskFailed bypasses both checks since it's a re-
            // queue signal, not a terminal state — handled
            // below at the `is_backpressure` arm where the
            // re-queue is idempotent (the binary either gets
            // requeued or has already been requeued by an
            // earlier landing).
            let dedup_is_backpressure = error_message == "No idle worker available"
                || error_message == "worker pipe broken; respawning";
            if !dedup_is_backpressure
                && (self.completed_tasks.contains(task_hash)
                    || self.failed_tasks.contains_key(task_hash))
            {
                return;
            }
            let secondary_id = secondary_id.clone();
            let worker_id = *worker_id;
            let task_hash = task_hash.clone();
            let error_type = error_type.clone();
            let error_message = error_message.clone();
            // Find the specific worker and recover the binary if it's a
            // recoverable error so it can be re-assigned to another worker.
            let mut recovered_binary: Option<TaskInfo<I>> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        recovered_binary = w.current_task.take();
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            // Backpressure detection: secondary's dispatch.rs sends
            // this exact error_message when its `is_idle_state()`
            // check finds every worker non-idle for an inbound
            // TaskAssignment. That's NOT a task failure — the worker
            // never ran the binary; the secondary just couldn't place
            // it. Treat it as a transient backoff signal:
            //   1. Re-queue the binary into the pool (don't lose it).
            //   2. Skip the failed_tasks insert — preserves the
            //      retry budget for actual failures.
            //   3. Mark the secondary as backpressured for a short
            //      window — the kickstart and TaskRequest paths
            //      both consult `is_backpressured()` and skip
            //      dispatch to that secondary's workers until the
            //      window expires or a successful TaskComplete from
            //      that secondary clears the flag.
            //   4. Skip the kickstart at the bottom of this function
            //      — the kickstart's whole point is "another worker
            //      may now be idle"; on backpressure, the failing
            //      secondary's workers are precisely what we DON'T
            //      want to re-target.
            // Without this: a single broken secondary (every
            // assignment to it bouncing as backpressure) consumes
            // the entire retry budget while the kickstart amplifier
            // keeps cycling work back to it. Tokenizer hit this on
            // a 1791-task cohort: 3128 errors all on one secondary,
            // 1511 permanent failures.
            // Two backpressure shapes — both mean "task DIDN'T
            // actually run; requeue at the pool, do not consume
            // retry budget":
            //
            //   1. "No idle worker available" — peer's worker
            //      pool was full at dispatch time.
            //   2. "worker pipe broken; respawning" — peer's
            //      target worker subprocess died between tasks;
            //      pipe-write failed; the peer is respawning.
            //      The not-yet-attempted task comes back with
            //      this marker so the primary requeues into the
            //      pool and re-dispatches once a peer signals
            //      capacity. Without this case Bug C produced
            //      silent task loss on every Broken-pipe assign
            //      attempt at a peer secondary (#46 secondary-
            //      side fix needed this primary-side companion
            //      to actually requeue rather than mark-as-failed).
            let is_backpressure = error_message == "No idle worker available"
                || error_message == "worker pipe broken; respawning";
            if is_backpressure {
                let backoff_ms = 500;
                self.backpressured_secondaries.insert(
                    secondary_id.clone(),
                    Instant::now() + std::time::Duration::from_millis(backoff_ms),
                );
                tracing::debug!(
                    secondary = %secondary_id,
                    worker_id,
                    backoff_ms,
                    "secondary returned backpressure; re-queuing task and applying backoff"
                );
                if let Some(binary) = recovered_binary {
                    self.release_type_slot(&binary.type_id);
                    self.pool_mut().requeue(binary);
                }
                return;
            }

            // Failure budget: one per task per pass. Recoverable
            // and NonRecoverable both terminate the dispatch slot
            // and add to `failed_tasks`. The `run()` pipeline calls
            // `retry_failed_tasks_pass` after the main operational
            // loop drains, which re-injects everything in
            // `failed_tasks` (clearing the set) and runs the loop
            // again. Up to `config.retry_max_passes` retry passes
            // (default 1) before failures are permanent.
            //
            // Critically NOT counted as a failure: secondary
            // disconnect → `requeue_dead_secondary` puts the
            // in-flight task back into the pool via
            // `pool.requeue` (NOT through this function). The task
            // never reached `failed_tasks`, so its retry budget
            // stays untouched.
            self.failed_tasks
                .insert(task_hash.clone(), error_type.clone());
            // Replicated-ledger update: every node mirrors the
            // post-failure state. The wire `error_type` is now the
            // typed `ErrorType` enum (Phase D), so the CRDT mutation
            // takes it verbatim — no string-token mapping.
            self.apply_and_broadcast_cluster_mutations(vec![
                ClusterMutation::TaskFailed {
                    hash: task_hash.clone(),
                    kind: error_type.clone(),
                    error: error_message.clone(),
                },
            ])
            .await;

            // Operator-facing WARN: per-class for retry/policy
            // decisions (error_type discriminates retry/oom/final);
            // the error message itself is essential for debugging
            // and stays on WARN. Per-task identity (task_id / phase
            // / task_type) is diagnostic noise on this path — it
            // moves to the DEBUG sibling below.
            tracing::warn!(
                secondary = %secondary_id,
                worker_id,
                task_hash = %task_hash,
                error_type = ?error_type,
                error = %error_message,
                "task failed"
            );
            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                task_id = ?recovered_binary.as_ref().and_then(|b| b.task_id.as_deref()),
                phase = ?recovered_binary.as_ref().map(|b| b.phase_id.to_string()),
                task_type = ?recovered_binary.as_ref().map(|b| b.type_id.to_string()),
                task_hash = %task_hash,
                "task failed: identity"
            );

            if let Some(binary) = recovered_binary {
                self.release_type_slot(&binary.type_id);
                self.note_item_failed(&binary.phase_id, binary.task_id.as_deref());
            }

            // Same kickstart rationale as `handle_task_complete`:
            // `note_item_failed` may have just cascaded a phase
            // through Drained → Done and activated a dependent
            // phase; idle workers across other secondaries won't
            // re-poll on their own. Idempotent.
            self.dispatch_to_idle_workers().await.ok();

            // Forward task-terminal outcomes to peer secondaries so
            // their `failed_tasks` / `completed_tasks` caches stay
            // current — required for promoted-primary handoff
            // not to re-dispatch a task we just gave up on. Both
            // Recoverable and NonRecoverable are terminal in the
            // pass-based retry model: the retry pass re-injects into
            // the pool by re-running the operational loop, which is
            // the new "second chance"; an immediate requeue would
            // recreate the busy-loop bug.
            self.forward_completion_to_secondaries(&msg, &secondary_id)
                .await;
        }
    }

    /// Apply a replicated `DistributedMessage::ClusterMutation` batch.
    ///
    /// Single concern: keep the demoted primary's CRDT mirror — and the
    /// accounting sets the operational loop's exit-counter check
    /// reads — converged with the cluster's view, even when the live
    /// primary authority has handed off to a promoted secondary.
    ///
    /// Pre-fix the primary's `dispatch_message` had no arm for
    /// `MessageType::ClusterMutation`: any mutation broadcast addressed
    /// at the demoted primary fell through the catch-all. The
    /// operational loop's `completed + failed >= total` exit check
    /// reads `self.completed_tasks` / `self.failed_tasks`, which on a
    /// demoted primary are fed only by direct `TaskComplete` /
    /// `TaskFailed` forwards reaching the local primary's transport.
    /// Cross-secondary completions on the new primary's pool never
    /// arrived as direct forwards (the new primary doesn't loopback
    /// peer-observed completions to the demoted primary's transport),
    /// so the counter never reached the total and the run loop sat
    /// forever — the asm-dataset-nix R2 / T3 hang.
    ///
    /// Mirrors `secondary::dispatch::apply_cluster_mutations` in shape
    /// (idempotent fan-out over a `Vec<ClusterMutation>`); diverges in
    /// that the primary additionally maintains `completed_tasks` /
    /// `failed_tasks` because those are the sets the lifecycle
    /// exit-counter reads. The CRDT idempotency on `cluster_state`
    /// makes repeated apply safe; `HashSet::insert` is idempotent on
    /// the accounting side.
    pub(super) async fn handle_cluster_mutation(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            // Note whether any TaskAdded rides in this batch BEFORE
            // moving the mutations into apply. Only `TaskAdded` grows
            // the cluster ledger; refreshing `total_tasks` for any
            // other variant would be a wasted read on the hot terminal-
            // mutation path (TaskCompleted / TaskFailed).
            let has_task_added = mutations
                .iter()
                .any(|m| matches!(m, ClusterMutation::TaskAdded { .. }));
            for m in mutations {
                self.mirror_mutation_to_accounting(&m);
                self.cluster_state.apply(m);
            }
            if has_task_added {
                // Refresh from the post-apply CRDT view. In setup-defer
                // mode the demoted submitter starts with
                // `total_tasks = 0` (no local `all_binaries`, no
                // `seed_cluster_state` ever ran on this node) and only
                // learns the run's task count from the promoted
                // secondary's broadcast `TaskAdded` batch; without
                // this refresh the operational-loop exit check
                // (`completed + failed >= total_tasks`) trips at
                // `0 + 0 >= 0` the moment the demoted primary enters
                // the loop — before the chosen secondary has
                // broadcast its first TaskAdded — and the local
                // primary exits prematurely, taking the whole run
                // down with it.
                //
                // Legacy bootstrap path: `total_tasks` was set in
                // `run()` from `binaries.len()` before any mutation is
                // mirrored. The local `seed_cluster_state` batch
                // applies to the same count, so `task_count()`
                // converges to the same value the field already
                // holds — the refresh is a no-op write. Idempotent
                // against duplicate-Add via the CRDT's presence
                // semantics: re-applying a TaskAdded for a hash
                // already in the ledger leaves `task_count`
                // unchanged.
                self.total_tasks = self.cluster_state.task_count();
            }
        }
    }

    /// Update `completed_tasks` / `failed_tasks` (the sets the
    /// operational loop reads for its exit-counter check) from a
    /// single `ClusterMutation`. Non-terminal mutations
    /// (`TaskAdded`, `TaskAssigned`, `PrimaryChanged`, `PhaseDepsSet`,
    /// `RunComplete`) leave both sets untouched: `TaskAdded` /
    /// `TaskAssigned` describe non-terminal lifecycle states the
    /// counter check ignores, `PrimaryChanged` / `PhaseDepsSet` are
    /// orthogonal, and `RunComplete` flows through `cluster_state`'s
    /// own `run_complete` flag which the loop reads separately.
    /// (`total_tasks` is refreshed by the caller — see
    /// `handle_cluster_mutation` — after the batch applies, not per
    /// mutation here.)
    ///
    /// Terminal mutations preserve the same single-bucket invariant
    /// `handle_task_complete` / `handle_task_failed` enforce: a hash
    /// sits in exactly one of {completed, failed} at a time. A late
    /// `TaskCompleted` after a `TaskFailed` removes from `failed_tasks`
    /// and inserts into `completed_tasks` (success supersedes
    /// recoverable failure, mirroring the live-primary behaviour).
    /// `TaskFailed` for a hash already in `completed_tasks` does NOT
    /// regress — the cluster_state apply will NoOp the mutation by
    /// terminal-locked-out semantics, and we mirror that here by
    /// skipping the failed-set insert when the hash is already
    /// completed.
    fn mirror_mutation_to_accounting(&mut self, m: &ClusterMutation<I>) {
        match m {
            ClusterMutation::TaskCompleted { hash } => {
                self.failed_tasks.remove(hash);
                self.completed_tasks.insert(hash.clone());
            }
            ClusterMutation::TaskFailed { hash, kind, .. } => {
                if !self.completed_tasks.contains(hash) {
                    self.failed_tasks.insert(hash.clone(), kind.clone());
                }
            }
            // `TaskAdded` proves the chosen secondary has run discovery
            // and seeded at least one task; `RunComplete` proves it
            // legitimately finished (including the zero-items case).
            // Either is the signal that the operational-loop exit-check
            // can safely re-enable on a demoted setup-promote submitter
            // — see the `setup_pending` field doc on
            // `PrimaryCoordinator`. Idempotent flip (a no-op on the
            // legacy path where the field is already `false`).
            ClusterMutation::TaskAdded { .. } | ClusterMutation::RunComplete => {
                self.setup_pending = false;
            }
            ClusterMutation::TaskReinjected { hash } => {
                // External-control reinject moves the entry off
                // `Failed` in the CRDT; the per-pass `failed_tasks`
                // ledger must mirror so the operational-loop exit
                // check (`completed + failed >= total`) doesn't trip
                // on a hash that's been re-armed for dispatch.
                self.failed_tasks.remove(hash);
            }
            ClusterMutation::TaskAssigned { .. }
            | ClusterMutation::PrimaryChanged { .. }
            | ClusterMutation::PhaseDepsSet { .. }
            | ClusterMutation::TaskPreferredSecondariesUpdated { .. } => {
                // Routing/role hints with no impact on terminal-state
                // accounting.
            }
        }
    }
}
