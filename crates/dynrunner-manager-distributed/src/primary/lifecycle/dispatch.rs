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
        // Visit free workers in load-aware order so a secondary with
        // many in-flight tasks doesn't keep winning tail-of-phase
        // dispatches against an idler peer. `dispatch_order` selects on
        // the authoritative free predicate (`held_task().is_none()`)
        // and sorts by the advisory busy-load count.
        let order = dispatch_order(&self.workers);
        for worker_idx in order {
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
            // Dispatch-shape view pipeline: pool view → soft
            // preferred-secondaries tie-break → strict
            // preferred-secondaries gate (OOM bucket only) → cap
            // filter. The full pipeline lives behind a single
            // accessor so OOM-bucket policy never leaks here.
            let view = self.dispatch_view_for_worker(worker_idx);
            if view.is_empty() {
                continue;
            }
            let worker_info = self.workers[worker_idx].budget_info();
            let all_infos: Vec<dynrunner_scheduler_api::WorkerBudgetInfo<I>> =
                self.workers.iter().map(|w| w.budget_info()).collect();
            let max_res = self.workers[worker_idx].resource_budgets.clone();

            let decision = self.scheduler.assign_normal(
                &worker_info,
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
                let binary = self.pool_mut().take_from_view(view, binary_index);
                let sec_id = self.workers[worker_idx].secondary_id.clone();
                let local_worker_id = self.local_worker_id_in_secondary(worker_idx);

                let task_hash = compute_task_hash(&binary);
                // Type-slot reserve + slot `Idle -> Assigned{task_hash}`
                // + ledger insert, committed together so the three
                // pieces of in-flight bookkeeping can never diverge. The
                // slot is idle by construction here (`dispatch_order`
                // filters to idle workers).
                self.commit_assignment(
                    worker_idx,
                    binary.clone(),
                    task_hash.clone(),
                    estimated_usage.clone(),
                );
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
                // earlier `take_from_view` call) — but the task
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
        }
        Ok(())
    }
}
