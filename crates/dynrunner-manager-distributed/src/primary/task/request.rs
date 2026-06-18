use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{AssignmentDecision, ResourceEstimator, Scheduler, WorkerBudgetInfo};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::primary::coordinator::InheritedSlotReconcile;
use crate::primary::lifecycle::dispatch::DispatchOutcome;

/// Minimum spacing between two rolled-up unassignable-PARK operator lines
/// (`coordinator::unassignable_park_warn`). At scale every idle worker
/// re-requests once per backoff tick while a phase is drained, so the
/// per-event DEBUG line spammed; one rollup per interval names the parked
/// count + the suppressed re-requests instead.
pub(in crate::primary) const UNASSIGNABLE_PARK_WARN_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the terminal-veto settle cascade (its
    /// `note_item_*` → `process_phase_lifecycle` may run consumer
    /// `on_phase_end` callbacks that queue `spawn_tasks`). Pre-loop /
    /// post-loop callers pass `&mut None`, same as the terminal handlers.
    pub(crate) async fn handle_task_request(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
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
            if let Some(idx) = target_idx {
                match self.reconcile_inherited_slot(idx) {
                    InheritedSlotReconcile::Requeued(requeue) => {
                        // Broadcast the `InFlight → Pending` transition in
                        // lockstep with the local pool requeue just done
                        // inside the reconcile, so a stale replicated
                        // `InFlight` cannot survive and re-strand the task
                        // on a later failover. The slot is now `Idle`, so
                        // the assignment block below dispatches the
                        // requeued (and any other ready) work to it.
                        self.apply_and_broadcast_cluster_mutations(vec![*requeue])
                            .await;
                    }
                    InheritedSlotReconcile::VetoedByTerminal { task_hash } => {
                        // The replicated ledger already records a terminal
                        // for the held hash (the requeue-vs-complete race,
                        // run_20260610_221140): settle the slot / ledger /
                        // pool residue through the ONE CRDT-terminal settle
                        // path instead of re-queueing completed work. The
                        // slot comes back `Idle`, so the assignment block
                        // below can hand this worker FRESH work.
                        if !self
                            .settle_local_state_on_crdt_terminal(&task_hash, command_rx)
                            .await
                        {
                            // Inherited slot ⟺ inherited ledger entry is a
                            // seed-time invariant (`seed_inflight` +
                            // `reconstruct_workers_from_cluster_state` are
                            // 1:1); a veto that found nothing to settle
                            // would leave the slot phantom-busy.
                            tracing::warn!(
                                task_hash = %task_hash,
                                "terminal-vetoed inherited slot had no \
                                 settleable residue (ledger entry / queued \
                                 copy missing) — slot left untouched"
                            );
                        }
                    }
                    InheritedSlotReconcile::NotInherited => {}
                }
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
                if self.should_skip_worker_for_dispatch(idx, false) {
                    return Ok(());
                }
                if !available_res.is_empty() {
                    self.workers[idx].resource_budgets = available_res.clone();
                }

                // Per-secondary-FIRST affine source (ADDITIVE): pop this
                // worker's secondary's affine queue BEFORE the global-pool
                // decision, mirroring the proactive `dispatch_to_idle_workers`
                // path so the per-secondary import→work ordering holds on the
                // REACTIVE request path too (a worker requesting work runs its
                // secondary's queued import THEN its dependent build, in order).
                // A committed affine dispatch satisfies this request. When the
                // queue is empty this is a no-op and the unchanged global path
                // below runs (baseline-preserved).
                if self.try_affine_pop_for_worker(idx).await {
                    return Ok(());
                }

                // #519 dispatch-bias: a single `TaskRequest` serves ONE
                // worker = ONE dispatch decision, so the per-decision
                // primitive runs exactly once here (it folds the decision
                // count, the every-W gate re-eval, and the toggle flip).
                // Returns `false` while the cached gate verdict is disarmed
                // (pre-#519 view).
                let prefer_dependency = self.prefer_dependency_for_decision();

                // Try to assign from local pending. The dispatch-
                // shape view pipeline lives behind a single accessor
                // on the coordinator so this site stays agnostic to
                // soft/strict preferred-secondaries and per-type
                // caps. See `dispatch_view_for_worker`.
                let view = self.dispatch_view_for_worker(idx, prefer_dependency);
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
                        // Owned consumption ticket — the view's last use,
                        // releasing the pool borrow for the take below.
                        let selection = view.select(binary_index);
                        let binary = self.pool_mut().take_selected(selection);
                        // The single-task dispatch transaction (commit →
                        // gather → build → send → originate, with the
                        // in-flight-bookkeeping triple + rollback) lives in
                        // `dispatch_one_assignment`, shared with the pool-fed
                        // + affine-fed sites so the wire shape + leak-safe
                        // rollback are identical regardless of which path
                        // fires. A non-committed outcome hands the binary
                        // back: this is the POOL-fed source, so it requeues to
                        // the pool and returns (the requester re-polls on its
                        // backoff tick — the slot is open again).
                        match self
                            .dispatch_one_assignment(idx, binary, estimated_usage.clone())
                            .await
                        {
                            DispatchOutcome::Committed => {
                                assigned = true;
                            }
                            DispatchOutcome::CommitRefused(binary)
                            | DispatchOutcome::SendFailed(binary) => {
                                self.pool_mut().requeue(binary);
                                return Ok(());
                            }
                        }
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
            //
            // Split by sub-cause:
            //   * `target_idx == None` — the worker addressed an
            //     unknown/dead secondary slot (a membership/liveness
            //     condition, rare and orthogonal to park churn). Keep an
            //     INDIVIDUAL line.
            //   * `target_idx == Some` + unassigned — the worker holds a
            //     known roster slot but no work fit it: it is PARKED
            //     awaiting the push. At scale every idle worker re-requests
            //     once per backoff tick, so this is the spam case — roll it
            //     up through `unassignable_park_warn` (one line per
            //     interval naming the parked count + suppressed
            //     re-requests). The parked-worker count is computed LAZILY
            //     only on the permitted tick (an O(workers) sweep).
            if !assigned {
                match target_idx {
                    None => {
                        tracing::debug!(
                            secondary = %secondary_id,
                            worker_id,
                            "TaskRequest names no roster slot (unknown / dead \
                             secondary); dropped — the worker re-polls on its \
                             backoff tick"
                        );
                    }
                    Some(_) => {
                        if let Some(suppressed) = self.unassignable_park_warn.permit() {
                            let parked = self
                                .workers
                                .iter()
                                .filter(|w| w.held_task().is_none())
                                .count();
                            // Affine-dep-work STRAND signature (the deepest
                            // unassignable-trap layer): a work task withheld
                            // from the global view by `has_affine_dep` but
                            // recorded PLACED yet sitting in no affine queue is
                            // permanently unassignable until its `placed_work`
                            // guard clears (which the requeue-recovery now does).
                            // Naming the count here turns a silent strand into a
                            // one-line greppable signal: a non-zero
                            // `affine_dep_strand_candidates` while workers park
                            // pins THIS layer immediately. Upper bound (a
                            // momentarily popped-but-not-redispatched unit
                            // counts), so it is diagnostic only.
                            let affine_dep_strand_candidates =
                                self.affine_scheduler.placed_but_unqueued_count();
                            tracing::debug!(
                                parked_workers = parked,
                                suppressed_re_requests = suppressed,
                                affine_dep_strand_candidates,
                                "idle workers parked awaiting work; \
                                 unassignable re-requests suppressed in the \
                                 last {:?} — each worker is assigned by the \
                                 dispatch push as soon as work fits \
                                 (affine_dep_strand_candidates names work hashes \
                                 placed but absent from every affine queue)",
                                UNASSIGNABLE_PARK_WARN_INTERVAL
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
