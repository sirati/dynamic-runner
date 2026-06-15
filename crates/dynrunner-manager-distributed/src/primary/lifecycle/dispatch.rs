use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::PrimaryCoordinator;
use crate::primary::task::predecessor_outputs::gather_predecessor_outputs;
use crate::primary::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

use super::dispatch_order;

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
        // on the coordinator's single-thread runtime. The worker-mgmt
        // arm is itself a select! arm body, so other arms can't
        // re-fire until this returns — but unlike the SpawnTasks
        // wedge (per-batch 150 s), a single recheck burst is
        // bounded enough (~hundreds of ms) that yielding INSIDE the
        // arm body is sufficient: each chunk of K workers releases
        // the runtime to sibling spawn_local tasks (the lifecycle /
        // task_completed dispatchers, etc.) and the next chunk
        // re-derives `order` and `all_infos` from the current worker
        // roster, so a worker that became busy mid-recheck (a
        // completion landed via the OTHER select! arm running before
        // we return) is naturally dropped.
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
            if self.should_skip_worker_for_dispatch(worker_idx, bypass_backpressure, false) {
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
            // Dispatch-shape view pipeline: pool view → soft
            // preferred-secondaries tie-break → strict
            // preferred-secondaries gate (OOM bucket only) → cap
            // filter. The full pipeline lives behind a single
            // accessor so OOM-bucket policy never leaks here.
            let view = self.dispatch_view_for_worker(worker_idx, prefer_dependency);
            if view.is_empty() {
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
                let sec_id = self.workers[worker_idx].secondary_id.clone();
                let local_worker_id = self.local_worker_id_in_secondary(worker_idx);

                let task_hash = compute_task_hash(&binary);
                // Type-slot reserve + slot `Idle -> Assigned{task_hash}`
                // + ledger insert, committed together so the three
                // pieces of in-flight bookkeeping can never diverge. The
                // slot is idle by construction here (`dispatch_order`
                // filters to idle workers), so the enforced idle-guard
                // (#517) refuses only if a bug ever broke that invariant:
                // requeue the taken binary + refresh the snapshot + skip
                // the send rather than dispatch a task the model can't
                // track (the silent-overwrite backstop).
                if !self.commit_assignment(
                    worker_idx,
                    binary.clone(),
                    task_hash.clone(),
                    estimated_usage.clone(),
                ) {
                    self.pool_mut().requeue(binary);
                    all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                    continue;
                }
                // Keep the hoisted budget snapshot coherent: the commit
                // just made this slot busy, and later workers' scheduler
                // calls must see it that way (idle-rank shifts under a
                // per-worker rebuild would have).
                all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                // Resolve the per-edge predecessor-output map from the
                // replicated `cluster_state.task_outputs` cache. The
                // helper handles both the direct-dep present-but-empty
                // contract and the `inherit_outputs` transitive walk;
                // an empty map results when the task has no deps. The
                // same helper is consumed by the sibling dispatch site
                // in `primary/task/request.rs` so the wire shape is
                // identical regardless of which path fires.
                let predecessor_outputs = gather_predecessor_outputs(&self.cluster_state, &binary);
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
                };

                // Transport-send failure rollback: pre-fix the
                // `await?` returned Err with the worker's
                // `current_task` set, `is_idle = false`, and the
                // pool's `in_flight_per_phase` bumped (via the
                // earlier `take_selected` call) — but the task
                // itself never reached the peer. The primary's
                // view permanently believed the slot was busy,
                // `dispatch_order` skipped it forever, and the
                // leaked in_flight slot never decremented (no
                // TaskComplete / TaskFailed will ever arrive for
                // a task that wasn't sent). Cumulative leaks
                // explain asm-tokenizer's "33 in_flight with
                // active=0" jam at 84f669c.
                //
                // Rollback symmetry: revert worker state,
                // requeue the binary back to the FRONT of its
                // bucket (matches `handle_primary_peer_rejection`),
                // release the type slot, and `continue` the
                // dispatch loop so other idle workers still get a
                // chance this tick. WARN so an operator grepping
                // for the jam symptom sees the proximate cause.
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
                    // Undo the `commit_assignment` triple (type slot +
                    // slot state + ledger) for the task whose send never
                    // made it real — no terminal will ever arrive for
                    // this hash — then requeue the binary.
                    self.rollback_assignment(worker_idx, &task_hash, &binary.type_id);
                    self.pool_mut().requeue(binary);
                    // The rollback re-idled the slot; refresh its
                    // snapshot entry so later workers see it free again.
                    all_infos[worker_idx] = self.workers[worker_idx].budget_info();
                    continue;
                }

                // Send succeeded: originate the CRDT `Pending → InFlight`
                // transition (the single origination point). After the
                // send so a failure needs no CRDT compensation (the
                // rollback above runs before we reach here).
                self.originate_task_assigned(task_hash.clone(), sec_id.clone(), local_worker_id)
                    .await;

                // Operator-facing INFO: which secondary/worker just
                // took the task. Per-task identity (task_id /
                // phase / type) → DEBUG sibling.
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
            }
            // Chunk break: yield after `DISPATCH_CHUNK_WORKERS` workers'
            // worth of processing so sibling LocalSet tasks (the lifecycle
            // / task_completed dispatchers) get an opportunity to run
            // between chunks. The outer `dispatch_to_idle_workers` re-
            // enters this function on the next chunk; `visited` carries
            // forward so the new chunk's `order` skips already-visited
            // workers.
            if visited_this_chunk >= Self::DISPATCH_CHUNK_WORKERS {
                break;
            }
        }
        any_progress
    }
}
