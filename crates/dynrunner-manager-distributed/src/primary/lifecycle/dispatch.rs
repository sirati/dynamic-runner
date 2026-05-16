
use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

use super::dispatch_order;


impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    /// Iterate every idle worker and dispatch a task from the pool
    /// if one fits. Used by `run_retry_passes` to kickstart dispatch
    /// after re-injection (workers won't send a fresh TaskRequest on
    /// their own — see the run_retry_passes comment). Mirrors the
    /// per-worker logic in `handle_task_request` minus the
    /// primary relay (which is irrelevant for the
    /// non-promoted-primary at this stage).
    pub(crate) async fn dispatch_to_idle_workers(&mut self) -> Result<(), String> {
        // Demoted observer mode: the promoted primary is the
        // sole authority for dispatch. Returning here covers every
        // call site (kickstart from handle_task_complete /
        // handle_task_failed, plus the retry-pass kickstart) without
        // sprinkling `if !self.demoted` across the message-handling
        // code. See `demoted` doc on `PrimaryCoordinator`.
        if self.demoted {
            return Ok(());
        }
        // Visit idle workers in load-aware order so a secondary with
        // many in-flight tasks doesn't keep winning tail-of-phase
        // dispatches against an idler peer. `dispatch_order` filters
        // to idle and sorts by (busy-workers-on-secondary, worker_id).
        let order = dispatch_order(&self.workers);
        for worker_idx in order {
            // Skip workers belonging to a secondary that's currently
            // in backpressure backoff — see
            // `backpressured_secondaries` doc on `PrimaryCoordinator`.
            // Without this, the kickstart would re-target the same
            // unresponsive secondary in a tight loop, which is
            // exactly the failure storm 07ae301-followup is
            // designed to break. Re-checked inline (not pre-filtered
            // in `dispatch_order`) so a backpressure window that
            // opens mid-tick takes effect immediately.
            if self.is_backpressured(&self.workers[worker_idx].secondary_id) {
                continue;
            }
            let global_wid = self.workers[worker_idx].worker_id;
            // Soft preference tie-break: tasks whose
            // `preferred_secondaries` lists this worker's secondary
            // sort first within their priority class. Applied AFTER
            // `cap_filter_view` so caps remain hard. See
            // `primary::preferred_secondaries`.
            let dispatch_secondary_id =
                self.workers[worker_idx].secondary_id.clone();
            let preference_predicate =
                crate::primary::preferred_secondaries::apply_preferred_secondaries_predicate::<I>(
                    &dispatch_secondary_id,
                );
            let view = self.cap_filter_view(
                self.pool()
                    .view_for_worker(global_wid, Some(&preference_predicate)),
            );
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
                self.reserve_type_slot(&binary.type_id);
                let sec_id = self.workers[worker_idx].secondary_id.clone();
                let local_worker_id = self.workers[..worker_idx + 1]
                    .iter()
                    .filter(|w| w.secondary_id == sec_id)
                    .count() as u32
                    - 1;
                self.workers[worker_idx].current_task = Some(binary.clone());
                self.workers[worker_idx].estimated_resources = estimated_usage.clone();
                self.workers[worker_idx].is_idle = false;

                let task_hash = compute_task_hash(&binary);
                let assignment_msg = DistributedMessage::TaskAssignment {
                    sender_id: self.config.node_id.clone(),
                    timestamp: timestamp_now(),
                    secondary_id: sec_id.clone(),
                    worker_id: local_worker_id,
                    zip_file: None,
                    binary_info: binary_to_distributed(&binary),
                    local_path: self.config.wire_local_path(&binary),
                    file_hash: task_hash.clone(),
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
                if let Err(send_err) =
                    self.transport.send_to(&sec_id, assignment_msg).await
                {
                    tracing::warn!(
                        secondary = %sec_id,
                        worker_id = local_worker_id,
                        task_hash = %task_hash,
                        error = %send_err,
                        "task-assignment send failed; rolling back worker state and requeuing binary"
                    );
                    self.workers[worker_idx].current_task = None;
                    self.workers[worker_idx].estimated_resources =
                        ResourceMap::new();
                    self.workers[worker_idx].is_idle = true;
                    self.release_type_slot(&binary.type_id);
                    self.pool_mut().requeue(binary);
                    continue;
                }

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
