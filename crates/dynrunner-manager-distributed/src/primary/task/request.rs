use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    Destination, DistributedMessage, PeerId, PeerTransport,
};
use dynrunner_scheduler_api::{AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo};

use crate::primary::PrimaryCoordinator;
use crate::primary::task::predecessor_outputs::gather_predecessor_outputs;
use crate::primary::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<Tr: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier>
    PrimaryCoordinator<Tr, S, E, I>
{
    pub(crate) async fn handle_task_request(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        if let DistributedMessage::TaskRequest {
            ref secondary_id,
            worker_id,
            ref available_resources,
            ..
        } = msg
        {
            let available_res: ResourceMap = available_resources
                .iter()
                .map(|r| (r.kind.clone(), r.amount))
                .collect();
            // Find matching worker by its stable secondary-local id.
            let target_idx = self.worker_idx_for(secondary_id, worker_id);

            let mut assigned = false;

            // R1: `TaskRequest` is a pure capacity hint that NEVER
            // frees a slot. The removed free-on-request block
            // (`current_task = None; is_idle = true`) and the removed
            // stale-request guard let a bare request mutate slot state;
            // R1's `SlotState` typestate makes assignment reachable
            // ONLY from `Idle` (via `commit_assignment`'s
            // `assign`/`debug_assert`) and frees a slot ONLY on a
            // terminal outcome via `free_slot_on_terminal`. The
            // `demoted` short-circuit is gone too — there is no
            // demoted-primary self-assign race to guard against.
            //
            // Capacity-hint contract: if the addressed slot is already
            // `Assigned`, the request is a no-op on slot state (a
            // delayed/duplicate request for a worker that's still
            // running the task it last took) — we fall through to the
            // primary-relay arm without touching the slot or the
            // ledger. Only an `Idle` slot refreshes its budget and
            // attempts one assignment.
            if let Some(idx) = target_idx
                && self.workers[idx].is_idle()
            {
                // Composed dispatch-shape gate: backpressure backoff
                // + OOM-bucket single-worker masking. See
                // `should_skip_worker_for_dispatch` for the
                // per-reason documentation. `false`: a secondary's own
                // `TaskRequest` does NOT bypass its backoff — the
                // backoff exists precisely to stop a secondary that
                // just said "no idle worker" from re-hammering us on
                // its request-retry tick.
                if self.should_skip_worker_for_dispatch(idx, false) {
                    return Ok(());
                }
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
                        let sec_id = self.workers[idx].secondary_id.clone();

                        let task_hash = compute_task_hash(&binary);
                        // Type-slot reserve + slot `Idle ->
                        // Assigned{task_hash}` + ledger insert, committed
                        // together. The slot is idle here: the outer arm
                        // gated on `is_idle()`, so assignment is reachable
                        // only from an idle slot.
                        self.commit_assignment(
                            idx,
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
                        // in `primary/lifecycle/dispatch.rs` so the wire shape
                        // is identical regardless of which path fires.
                        let predecessor_outputs =
                            gather_predecessor_outputs(&self.cluster_state, &binary);
                        let assignment_msg = DistributedMessage::TaskAssignment {
                            sender_id: self.config.node_id.clone(),
                            timestamp: timestamp_now(),
                            secondary_id: sec_id.clone(),
                            worker_id,
                            zip_file: None,
                            binary_info: binary_to_distributed(&binary),
                            local_path: self.config.wire_local_path(&binary),
                            file_hash: task_hash.clone(),
                            predecessor_outputs,
                        };

                        // Same partial-commit-leak rollback as
                        // `dispatch_to_idle_workers`: a send_to
                        // failure here pre-fix left the slot Assigned +
                        // pool in_flight bumped. dispatch_order then
                        // skipped the slot forever; the leaked
                        // in_flight never decremented because no
                        // TaskComplete/TaskFailed could arrive for
                        // a task that wasn't sent. asm-tokenizer's
                        // 33-in_flight/active=0 jam at 84f669c is
                        // the operator-facing symptom of cumulative
                        // leaks from this and the sibling path.
                        if let Err(send_err) = self
                            .send_to(Destination::Secondary(PeerId::from(sec_id.clone())), assignment_msg)
                            .await
                        {
                            tracing::warn!(
                                secondary = %sec_id,
                                worker_id,
                                task_hash = %task_hash,
                                error = %send_err,
                                "task-assignment send failed; rolling back worker state and requeuing binary"
                            );
                            // Undo the `commit_assignment` triple (type
                            // slot + slot state + ledger) for the unsent
                            // task, then requeue the binary.
                            self.rollback_assignment(idx, &task_hash, &binary.type_id);
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

                        // Send succeeded: originate the CRDT `Pending →
                        // InFlight` transition (the single origination
                        // point). After the send so a failure needs no
                        // CRDT compensation (the rollback above runs
                        // before we reach here).
                        self.originate_task_assigned(task_hash.clone(), sec_id.clone(), worker_id)
                            .await;

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

            // No local assignment was made. The `Destination::Primary`
            // relay exists ONLY for the DEMOTED-primary case: a node that
            // used to be primary but whose authority has moved must
            // forward the request on to the REAL (remote) current primary.
            // Addressing `Destination::Primary` resolves at the egress
            // edge (`Self::send_to`) through
            // `cluster_state.current_primary()` — which every
            // `PrimaryChanged` apply updates — and rides the mesh-level
            // peer link, alive across promotion per
            // `feedback_mesh_independent_of_role_and_membership.md`.
            //
            // But when this node IS the current primary, that same
            // `Destination::Primary` resolves to SELF
            // (`SendTarget::Loopback`), and on a co-located host the
            // loopback delivers a LIVE frame to the own-secondary's
            // inbound, which demuxes it straight back into this primary's
            // inbound — an unthrottled self-feeding `TaskRequest` cycle
            // (it bypasses the secondary's per-worker origination backoff
            // because it never re-enters via `request_task_for_worker`).
            // Relaying to self is therefore NOT a no-op; it is the bug.
            //
            // Gate: relay ONLY when the current primary is a remote peer
            // (`!current_primary_is_self()`). When self IS the primary,
            // PARK the request locally — there is no remaining action. The
            // worker re-attempts on its next backoff-throttled
            // `request_task_for_worker` tick, and `dispatch_to_idle_workers`
            // re-nudges idle workers when work actually arrives
            // (`WorkerMgmtSignal::TasksAdded` / a completion), so parking
            // strands nothing.
            if !assigned
                && !self.current_primary_is_self()
                && let Err(e) = self.send_to(Destination::Primary, msg).await
            {
                tracing::debug!(
                    error = %e,
                    "primary-bound relay via Destination::Primary dropped; \
                     secondary will retry on its next backoff tick"
                );
            }
        }
        Ok(())
    }
}
