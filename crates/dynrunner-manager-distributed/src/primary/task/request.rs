
use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    Address, DistributedMessage, PeerTransport, Role,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo,
};

use crate::primary::PrimaryCoordinator;
use crate::primary::wire::{binary_to_distributed, compute_task_hash, timestamp_now};


impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    pub(crate) async fn handle_task_request(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
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
                // Composed dispatch-shape gate: backpressure backoff
                // + OOM-bucket single-worker masking. See
                // `should_skip_worker_for_dispatch` for the
                // per-reason documentation. Replaces the historical
                // bare backpressure check so the OOM bucket's "only
                // worker 0 of each secondary may serve a retry"
                // shape applies here too; without that the
                // TaskRequest path would happily hand a memory-
                // pressed retry to worker N>0 and defeat the bucket.
                if self.should_skip_worker_for_dispatch(idx) {
                    return Ok(());
                }
                // Mark worker idle
                self.workers[idx].current_task = None;
                self.workers[idx].estimated_resources = ResourceMap::new();
                self.workers[idx].is_idle = true;
                if !available_res.is_empty() {
                    self.workers[idx].resource_budgets = available_res.clone();
                }

                // Try to assign from local pending. The dispatch-
                // shape view pipeline lives behind a single accessor
                // on the coordinator so this site stays agnostic to
                // soft/strict preferred-secondaries and per-type
                // caps. See `dispatch_view_for_worker`.
                let view = self.dispatch_view_for_worker(idx);
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

}
