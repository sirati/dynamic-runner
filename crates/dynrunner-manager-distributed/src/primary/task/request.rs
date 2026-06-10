use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};
use dynrunner_scheduler_api::{AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo};

use crate::primary::PrimaryCoordinator;
use crate::primary::task::predecessor_outputs::gather_predecessor_outputs;
use crate::primary::wire::{binary_to_distributed, compute_task_hash, timestamp_now};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    pub(crate) async fn handle_task_request(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        if let DistributedMessage::TaskRequest {
            target: None,
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

            // Failover-resume occupancy reconciliation. A promoted primary
            // reconstructs each `TaskState::InFlight` slot `Assigned` from
            // the replicated ledger, but that occupancy is a STALE GUESS:
            // a survivor worker whose pre-kill task COMPLETED during the
            // primary-less election window has its completion LOST (no
            // primary was up to receive it), so the CRDT still says
            // `InFlight` while the worker is idle. The worker's own
            // post-`PrimaryChanged` `TaskRequest` (its secondary's
            // `repoll_idle_workers`, gated on the worker being idle) is the
            // ground-truth re-confirmation. So a request landing on an
            // INHERITED (unconfirmed) slot reconciles it: free the slot,
            // requeue the task (`InFlight → Pending`, broadcast for replica
            // coherence), and fall through to the idle-assignment path
            // below. Specific to the promoted-takeover: a live `Dispatched`
            // slot is NEVER reconciled (R1 holds — see below), so the
            // relocated/normal/rc-G2 cases where preserving committed
            // in_flight IS correct are untouched. Without this the 6
            // survivor slots stay phantom-busy forever and dispatch never
            // fires (the LMU-gating deadlock).
            if let Some(idx) = target_idx
                && let Some(requeue) = self.reconcile_inherited_slot(idx)
            {
                // Broadcast the `InFlight → Pending` transition in lockstep
                // with the local pool requeue just done inside the
                // reconcile, so a stale replicated `InFlight` cannot survive
                // and re-strand the task on a later failover. The slot is
                // now `Idle`, so the assignment block below dispatches the
                // requeued (and any other ready) work to it.
                self.apply_and_broadcast_cluster_mutations(vec![requeue])
                    .await;
            }

            // R1: `TaskRequest` is a pure capacity hint that NEVER
            // frees a LIVE-dispatched slot. The removed free-on-request
            // block (`current_task = None; is_idle = true`) and the removed
            // stale-request guard let a bare request mutate slot state;
            // R1's `SlotState` typestate makes assignment reachable
            // ONLY from `Idle` (via `commit_assignment`'s
            // `assign`/`debug_assert`) and frees a slot ONLY on a
            // terminal outcome via `free_slot_on_terminal` (or, for an
            // UNCONFIRMED inherited slot, the reconciliation just above).
            // The `demoted` short-circuit is gone too — there is no
            // demoted-primary self-assign race to guard against.
            //
            // Capacity-hint contract: if the addressed slot is a live
            // `Assigned { Dispatched }`, the request is a no-op on slot
            // state (a delayed/duplicate request for a worker that's still
            // running the task it last took) — we fall through to the
            // primary-relay arm without touching the slot or the
            // ledger. Only an `Idle` slot (including one the reconciliation
            // above just freed) refreshes its budget and attempts one
            // assignment.
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
                if self.should_skip_worker_for_dispatch(idx, false, true) {
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
                            target: None,
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
                            .send_to(
                                Destination::Secondary(PeerId::from(sec_id.clone())),
                                assignment_msg,
                            )
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
                            // tick.
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

            // No local assignment was made: DROP the request. A
            // `TaskRequest` is a pure capacity hint (R1) — the requesting
            // worker re-polls on its own backoff tick
            // (`request_task_for_worker` / `repoll_idle_workers`), and
            // `dispatch_to_idle_workers` re-nudges idle workers when work
            // actually arrives (`WorkerMgmtSignal::TasksAdded` / a
            // completion) — so nothing strands.
            //
            // A "relay to the real primary" arm used to live here
            // (`send_to(Destination::Primary, msg)` on `!assigned`),
            // claiming to forward a demoted node's requests to the new
            // authority. It was wrong twice over. (1) A node running this
            // handler IS the authoritative primary by construction (the
            // operational loop's authority invariant; a demoted primary is
            // torn down into an observer handoff, it never keeps serving
            // requests). (2) The primary's egress stamps `Destination::
            // Primary` unresolved and `Mesh::dispatch`'s `Primary` arm is
            // LOOPBACK-ONLY, so the relayed frame landed back in this
            // coordinator's OWN inbox — `RoleSlot::deliver` clears the
            // routing stamp, the frame re-matched `target: None`, was
            // still unassignable, and was relayed again: a self-sustaining
            // memory-speed inbox cycle (the run_20260610_121427 ingest
            // wedge — ~600K inbox-arm wins/s — and the lifetime ~97% CPU
            // heat on every relocated primary in its milder small-cycle
            // form). With ≥2 frames circulating, the mesh-pump's biased
            // select (egress before ingress) never drained the egress
            // queue empty, so WIRE ingress — the fleet's completions —
            // starved and ingest froze. `PrimaryCoordinator::send_to`
            // now rejects `Destination::Primary` outright as the
            // structural backstop; this site simply has nothing to send.
            if !assigned {
                tracing::debug!(
                    secondary = %secondary_id,
                    worker_id,
                    "TaskRequest not assignable (no roster slot / no \
                     dispatchable work); dropped — the worker re-polls on \
                     its backoff tick"
                );
            }
        }
        Ok(())
    }
}
