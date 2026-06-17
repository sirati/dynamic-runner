use std::sync::Arc;

use dynrunner_core::{Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;
use crate::primary::task::predecessor_outputs::gather_predecessor_outputs;
use crate::primary::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

use super::dispatch_order;

/// The outcome of [`PrimaryCoordinator::dispatch_one_assignment`] — which the
/// caller uses to dispose of the `binary` it supplied. The helper owns the
/// in-flight-bookkeeping triple (slot / type-slot / ledger via
/// `commit_assignment`) and its symmetric rollback; it does NOT own where a
/// failed binary goes BACK to, because the source differs per caller (the
/// global `PendingPool` for the pool-fed sites, the per-secondary affine queue
/// for the affine-fed site). So a non-`Committed` outcome hands the binary
/// back for the caller to requeue at its source.
pub(crate) enum DispatchOutcome<I: Identifier> {
    /// The assignment committed AND the `TaskAssignment` was sent + the
    /// `TaskAssigned` CRDT transition originated. The binary is now in-flight;
    /// the caller does nothing further with it.
    Committed,
    /// `commit_assignment` refused (the slot was not idle — the #517 enforced
    /// idle-guard backstop). Nothing was reserved; the binary is handed back
    /// untouched for the caller to requeue at its source.
    CommitRefused(Arc<TaskInfo<I>>),
    /// The `TaskAssignment` send failed; the `commit_assignment` triple was
    /// rolled back (slot re-idled, type-slot released, ledger entry dropped),
    /// so no terminal will ever arrive for this hash. The binary is handed
    /// back for the caller to requeue at its source.
    SendFailed(Arc<TaskInfo<I>>),
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// THE single-task dispatch transaction: commit the assignment of `binary`
    /// to the idle worker at `worker_idx`, gather its predecessor outputs,
    /// build + send the `TaskAssignment`, and — only on a successful send —
    /// originate the `Pending → InFlight` CRDT transition. The previously
    /// DUPLICATED core of the pool-fed `dispatch_to_idle_workers` and the
    /// request-fed `handle_task_request`, now also the affine-fed
    /// per-secondary-queue dispatch site — ONE seam for every per-worker
    /// single dispatch regardless of which source produced the `binary`.
    ///
    /// Single concern: the commit → send → originate transaction for ONE task
    /// to ONE worker, with the in-flight-bookkeeping triple (slot / type-slot /
    /// ledger) and its rollback owned here. The binary's SOURCE disposition on
    /// failure is the caller's concern (see [`DispatchOutcome`]): the helper
    /// hands a non-committed binary back rather than reaching into the pool,
    /// because the per-secondary affine source is not the pool.
    ///
    /// The local worker id is derived from `worker_idx` via
    /// [`Self::local_worker_id_in_secondary`] (the inverse of the
    /// `worker_idx_for` resolution every caller already did), so the wire
    /// `worker_id` and the `commit_assignment` holder key stay in lockstep at
    /// the one seam — exactly the value the request-fed path passed inline.
    pub(crate) async fn dispatch_one_assignment(
        &mut self,
        worker_idx: usize,
        binary: Arc<TaskInfo<I>>,
        estimated_usage: ResourceMap,
    ) -> DispatchOutcome<I> {
        let sec_id = self.workers[worker_idx].secondary_id.clone();
        let local_worker_id = self.local_worker_id_in_secondary(worker_idx);
        let task_hash = compute_task_hash(&binary);

        // Type-slot reserve + slot `Idle -> Assigned{task_hash}` + ledger
        // insert, committed together so the three pieces of in-flight
        // bookkeeping can never diverge. The slot is idle by construction at
        // every caller (each gates on an idle worker); the enforced idle-guard
        // (#517) refuses only if a bug ever broke that invariant — hand the
        // binary back so the caller requeues it at its source rather than
        // dispatch a task the model can't track (the silent-overwrite
        // backstop).
        if !self.commit_assignment(
            worker_idx,
            binary.clone(),
            task_hash.clone(),
            estimated_usage,
        ) {
            return DispatchOutcome::CommitRefused(binary);
        }

        // Resolve the per-edge predecessor-output map from the replicated
        // `cluster_state.task_outputs` cache. The helper handles both the
        // direct-dep present-but-empty contract and the `inherit_outputs`
        // transitive walk; an empty map results when the task has no deps.
        let predecessor_outputs = gather_predecessor_outputs(&self.cluster_state, &binary);
        // Pre-start fences (#530):
        //   A) supplanted_holder — Some IFF this hash is a dead-secondary-
        //      requeue redirect; left in place across the assignment-failure
        //      rollback so a re-dispatch stays fenced.
        //   B) secondary_id_member_gen — always Some, the addressee's current
        //      `peer_member_gen` per this coordinator's CRDT view.
        let supplanted_holder = self.supplanted_holders.get(&task_hash).cloned();
        let secondary_id_member_gen = Some(self.cluster_state.peer_member_gen(&sec_id));
        let assignment_msg = DistributedMessage::TaskAssignment {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: sec_id.clone(),
            worker_id: local_worker_id,
            zip_file: None,
            binary_info: binary_to_distributed(&binary),
            local_path: self.config.wire_local_path(&binary),
            file_hash: task_hash.clone(),
            predecessor_outputs,
            supplanted_holder,
            secondary_id_member_gen,
        };

        // Transport-send failure rollback: an `await?` returned Err left the
        // worker `Assigned` and the pool/type-slot/ledger bumped, but the task
        // never reached the peer — no TaskComplete / TaskFailed will ever
        // arrive. Undo the `commit_assignment` triple and hand the binary back
        // for the caller to requeue at its source. WARN so an operator
        // grepping the in-flight-leak jam sees the proximate cause.
        if let Err(send_err) = self
            .send_to(
                Destination::Secondary(PeerId::from(sec_id.clone())),
                assignment_msg,
            )
            .await
        {
            tracing::warn!(
                secondary = %sec_id,
                worker_id = local_worker_id,
                task_hash = %task_hash,
                error = %send_err,
                "task-assignment send failed; rolling back worker state and requeuing binary"
            );
            self.rollback_assignment(worker_idx, &task_hash, &binary.type_id);
            return DispatchOutcome::SendFailed(binary);
        }

        // Send succeeded: originate the CRDT `Pending → InFlight` transition
        // (the single origination point). After the send so a failure needs no
        // CRDT compensation (the rollback above runs before we reach here).
        self.originate_task_assigned(task_hash.clone(), sec_id.clone(), local_worker_id)
            .await;

        tracing::info!(
            secondary = %sec_id,
            worker_id = local_worker_id,
            task_hash = %task_hash,
            "task assigned"
        );
        tracing::debug!(
            secondary = %sec_id,
            worker_id = local_worker_id,
            task_id = ?binary.task_id,
            phase = %binary.phase_id,
            task_type = %binary.type_id,
            task_hash = %task_hash,
            "task assigned: identity"
        );
        DispatchOutcome::Committed
    }
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Iterate every free worker and dispatch a task from the pool if
    /// one fits. This is worker management's dispatch RECHECK: the
    /// operational loop's worker-management `select!` arm calls it on a
    /// drained [`crate::worker_signal::WorkerSignalBatch`] carrying a
    /// `TasksAdded` (decoupled from the phase/task code that emitted
    /// the signal — see the dispatch-decoupling law). Workers won't
    /// send a fresh `TaskRequest` on their own after a phase boundary
    /// or a re-injection, so the recheck re-evaluates EVERY free worker
    /// and dispatches what now fits. Mirrors the per-worker logic in
    /// `handle_task_request` minus the primary relay (which is
    /// irrelevant for the non-promoted-primary at this stage).
    ///
    /// `bypass_backpressure` lifts the per-secondary backoff for this
    /// recheck (NOT the OOM single-worker mask). The worker-management
    /// arm passes `true` when reacting to a genuine `TasksAdded`:
    /// circumstances changed, so a freed slot on a recently-
    /// backpressured secondary is a valid target again. See
    /// [`PrimaryCoordinator::should_skip_worker_for_dispatch`].
    pub(crate) async fn dispatch_to_idle_workers(
        &mut self,
        bypass_backpressure: bool,
    ) -> Result<(), String> {
        // Chunked-yield outer loop (#547). The dispatch recheck visits
        // every free worker; on a large idle fleet (e.g. 96 workers ×
        // a 46 k pool) the per-worker view rebuild + scheduler scan is
        // O(P) per worker, so the burst is O(M × P) of contiguous CPU
        // on the coordinator's single-thread runtime. Each chunk's
        // `yield_now()` releases the runtime to sibling `spawn_local`
        // tasks on the LocalSet (respawn watchers,
        // task_completed_dispatcher, etc.) — this prevents starving
        // them on long bursts. NOTE: this does NOT return to the
        // parent `select!`; sibling select! arms (ARM_INBOX,
        // ARM_HEARTBEAT) cannot fire until
        // react_to_worker_signal_batch's ARM_WORKER_MGMT body fully
        // returns. For a 96-worker × 46k-pool burst the cumulative
        // wall-clock is hundreds of ms, well below the
        // dispatch-starvation thresholds; if a future workload needs
        // ARM_INBOX/ARM_HEARTBEAT to re-fire mid-batch, the fix is a
        // PumpDispatchContinuation kick analogous to #547's
        // PumpSpawnContinuation. The next chunk re-derives `order`
        // and `all_infos` from the current worker roster, so a worker
        // that became busy between chunks is naturally dropped.
        let mut visited: std::collections::HashSet<usize> = std::collections::HashSet::new();
        loop {
            let progressed = self
                .dispatch_to_idle_workers_chunk(bypass_backpressure, &mut visited)
                .await;
            if !progressed {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    /// One chunk of the dispatch recheck (#547). Re-derives `order` AND
    /// `all_infos` from the CURRENT roster + the per-worker
    /// idle/backpressure/cap state, then dispatches up to
    /// [`DISPATCH_CHUNK_WORKERS`] workers' worth of assignments before
    /// returning. Returns `true` iff a worker was visited (caller's loop
    /// re-iterates), `false` once the order is exhausted (the outer loop
    /// terminates).
    ///
    /// `visited` accumulates worker indices already attempted in this
    /// recheck so a worker that COULDN'T fit a task in one chunk (no view
    /// match for its budget) is not retried in a later chunk — the
    /// scheduler's view-construction is deterministic given the pool +
    /// roster, so a no-fit verdict at chunk N would re-fire at chunk N+1
    /// against the same inputs. Skipping already-visited indices keeps
    /// the outer loop O(M).
    async fn dispatch_to_idle_workers_chunk(
        &mut self,
        bypass_backpressure: bool,
        visited: &mut std::collections::HashSet<usize>,
    ) -> bool {
        // Re-derive the order from the CURRENT roster every chunk: a
        // worker freed by a completion landing via the inbox arm between
        // chunks shows up in the fresh order, and a worker that just
        // committed an assignment drops out. `dispatch_order` filters to
        // idle on the authoritative `held_task().is_none()` predicate.
        let order = dispatch_order(&self.workers);
        // Fleet-wide budget snapshot — see the parent function's note on
        // why this is built ONCE per chunk (vs per-worker inside the
        // loop). The chunked outer loop re-builds it per chunk so a
        // mid-recheck completion's effect on the snapshot lands by the
        // next chunk.
        let mut all_infos: Vec<dynrunner_scheduler_api::WorkerBudgetInfo<I>> =
            self.workers.iter().map(|w| w.budget_info()).collect();
        let mut visited_this_chunk = 0usize;
        let mut any_progress = false;
        for worker_idx in order {
            if visited.contains(&worker_idx) {
                continue;
            }
            any_progress = true;
            visited.insert(worker_idx);
            visited_this_chunk += 1;
            // Composed dispatch-shape gate: backpressure backoff +
            // OOM-bucket single-worker masking. The predicate lives
            // on `PrimaryCoordinator` so this call site stays
            // agnostic to either policy. See
            // `should_skip_worker_for_dispatch` for the per-reason
            // documentation. Re-checked inline (not pre-filtered in
            // `dispatch_order`) so a backpressure window or
            // single-worker-mode flip mid-tick takes effect
            // immediately.
            if self.should_skip_worker_for_dispatch(worker_idx, bypass_backpressure) {
                continue;
            }
            // #519 per-decision bias: only a worker that reaches
            // view-construction makes a real dispatch DECISION, so this runs
            // AFTER the skip gate (a backpressured / OOM-masked worker is not
            // a decision and must not advance the counter or consume a toggle
            // flip — that would desync the deterministic alternation). The
            // call folds the decision-count bump, the every-W gate re-eval,
            // and the toggle flip; it returns `false` whenever the cached
            // gate verdict is disarmed (pre-#519 view).
            let prefer_dependency = self.prefer_dependency_for_decision();
            // Per-secondary-FIRST affine source (ADDITIVE): pop this worker's
            // secondary's affine queue BEFORE building the global-pool view, so
            // the design's "per-secondary queue first, then the global queue"
            // ordering holds. A committed affine dispatch refreshes the budget
            // snapshot and advances to the next worker. When the queue is empty
            // this is a no-op and the unchanged global path below runs (the
            // 1536 baseline is preserved).
            if self.try_affine_pop_for_worker(worker_idx).await {
                all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                continue;
            }
            // Dispatch-shape view pipeline: pool view → soft
            // preferred-secondaries tie-break → strict
            // preferred-secondaries gate (OOM bucket only) → cap
            // filter. The full pipeline lives behind a single
            // accessor so OOM-bucket policy never leaks here.
            let view = self.dispatch_view_for_worker(worker_idx, prefer_dependency);
            if view.is_empty() {
                // Idle-steal trigger: BOTH the global pool view AND this
                // worker's per-secondary queue are empty — steal a whole
                // schedulable unit from the longest-queue donor and dispatch
                // it. Drop the (empty) view borrow first so the steal can take
                // `&mut self`.
                drop(view);
                if self.try_affine_steal_for_worker(worker_idx).await {
                    all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                    continue;
                }
                // LAST-resort eager-prep idle filler (#638): the worker has
                // NOTHING else — empty pool view, empty affine queue, and no
                // steal donor — so speculatively run one eager-prep task on its
                // secondary. This is the LOWEST dispatch precedence by
                // construction (it runs only after every other source declined),
                // so it never displaces real work and never blocks a phase
                // transition (eager-prep is phase-agnostic + uncounted). A no-op
                // when no eager-prep cell is non-terminal here.
                if self.try_eager_prep_fill_for_worker(worker_idx).await {
                    all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                }
                continue;
            }
            let max_res = self.workers[worker_idx].resource_budgets.clone();

            let decision = self.scheduler.assign_normal(
                &all_infos[worker_idx],
                &all_infos,
                view.as_slice(),
                &max_res,
                &self.estimator,
                false,
            );

            if let dynrunner_scheduler_api::AssignmentDecision::Assign {
                binary_index,
                estimated_usage,
                ..
            } = decision
            {
                // Extract the owned consumption ticket — the view's last
                // use, releasing the pool borrow for the take below.
                let selection = view.select(binary_index);
                let binary = self.pool_mut().take_selected(selection);
                // The single-task dispatch transaction (commit → gather →
                // build → send → originate, with the in-flight-bookkeeping
                // triple + rollback) lives in `dispatch_one_assignment`,
                // shared with the request-fed + affine-fed sites. A
                // non-committed outcome hands the binary back: this is the
                // POOL-fed source, so a refused/failed binary requeues to the
                // FRONT of its pool bucket (matches
                // `handle_primary_peer_rejection`); either way we refresh the
                // budget snapshot so later workers' scheduler calls see the
                // slot's current busy/idle state, and `continue` the loop so
                // other idle workers still get a chance this tick.
                match self
                    .dispatch_one_assignment(worker_idx, binary, estimated_usage.clone())
                    .await
                {
                    DispatchOutcome::Committed => {}
                    DispatchOutcome::CommitRefused(binary)
                    | DispatchOutcome::SendFailed(binary) => {
                        self.pool_mut().requeue(binary);
                        all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                        continue;
                    }
                }
                all_infos[worker_idx] = self.workers[worker_idx].budget_info();
            }
            // Chunk break: `yield_now()` releases the runtime to sibling
            // `spawn_local` tasks on the LocalSet (respawn watchers,
            // task_completed_dispatcher, etc.) between chunks. NOTE: this
            // does NOT return to the parent `select!`; sibling select! arms
            // (ARM_INBOX, ARM_HEARTBEAT) cannot fire until
            // react_to_worker_signal_batch's ARM_WORKER_MGMT body fully
            // returns. The outer `dispatch_to_idle_workers` re-enters this
            // function on the next chunk; `visited` carries forward so the
            // new chunk's `order` skips already-visited workers.
            if visited_this_chunk >= Self::DISPATCH_CHUNK_WORKERS {
                break;
            }
        }
        any_progress
    }
}
