//! Illegal-assignment bounce handling (#517) — the authority half of
//! the secondary's honor-or-bounce.
//!
//! A secondary that receives a `TaskAssignment` for a worker slot that
//! is NOT idle HONORS the assigned `worker_id` (it never re-picks
//! another worker — the dispatch-decoupling law) and bounces a typed
//! [`dynrunner_protocol_primary_secondary::DistributedMessage::IllegallyAssignedToNonidleWorker`]
//! (see the emitter in `secondary/dispatch/helpers.rs`). That frame is
//! an occupancy-DIVERGENCE report, NOT a `TaskFailed`: it never routes
//! through the terminal gate / `handle_task_failed`, so it can never be
//! accounted as a failure (no `failed_tasks`, no retry-budget burn).
//!
//! The divergence it surfaces: the primary keys occupancy by
//! `(secondary, worker_id)` and committed the assigned task onto its
//! MODEL of that slot, but physically the slot is busy with an
//! incumbent. Left unreconciled the primary keeps re-assigning the
//! physically-busy slot (the fleet-wide assign→bounce loop). This
//! handler BREAKS the loop: it requeues the bounced task AND reconciles
//! the slot to reflect the incumbent the secondary reported, so the
//! primary stops believing the worker is idle.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::primary::{PrimaryCoordinator, SlotProvenance};
use crate::worker_signal::WorkerMgmtSignal;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// React to a secondary's illegal-assignment bounce: reconcile this
    /// primary's `(secondary, worker_id)` occupancy model + REQUEUE the
    /// bounced task. Never a failure (no terminal accounting).
    ///
    /// Two coupled steps, both idempotent:
    ///
    /// 1. REQUEUE the assigned (bounced) task. If it is still in the
    ///    in-flight ledger this primary committed it (optimistically, onto
    ///    a slot the secondary refused), so free that hold and return the
    ///    binary to `Pending` (`InFlight → Pending`, broadcast in lockstep
    ///    — the SAME `TaskRequeued` origination the dead-secondary /
    ///    backpressure paths emit). A hash already absent from the ledger
    ///    (a raced terminal / a prior requeue) is a safe no-op.
    ///
    /// 2. RECONCILE the slot to the incumbent the secondary named, so the
    ///    primary's model agrees with physical reality and STOPS
    ///    re-assigning the busy slot (the loop-breaker). The incumbent is
    ///    marked [`SlotProvenance::Inherited`] — its live occupancy is the
    ///    secondary's report, not a fresh dispatch this primary originated,
    ///    so the incumbent's own terminal (or, if it already finished, the
    ///    holder's next `TaskRequest`) reconciles the slot through the
    ///    normal paths. When the bounce carried NO incumbent
    ///    (out-of-range `worker_id` / 0-worker pool) there is nothing to
    ///    reconcile onto a slot — only the requeue runs.
    ///
    /// Emits one `TasksAdded` so the dispatch recheck re-places the
    /// requeued task on a GENUINELY-idle worker (decoupled, never a direct
    /// dispatch call — the dispatch-decoupling law).
    pub(crate) async fn handle_illegally_assigned(&mut self, msg: DistributedMessage<I>) {
        let DistributedMessage::IllegallyAssignedToNonidleWorker {
            secondary_id,
            worker_id,
            assigned,
            incumbent,
            ..
        } = msg
        else {
            return;
        };

        tracing::error!(
            secondary = %secondary_id,
            worker_id,
            assigned_hash = %assigned.hash,
            incumbent_hash = incumbent.as_ref().map(|i| i.hash.as_str()),
            "secondary bounced an ILLEGAL assignment: the assigned worker was \
             not idle. The per-(secondary, worker_id) occupancy model diverged \
             from physical reality; reconciling the slot to the incumbent and \
             requeuing the bounced task (NOT failing it)"
        );

        // (1) REQUEUE the bounced task. `free_slot_on_terminal` resolves the
        // holder slot from the LEDGER entry (by hash) — independent of the
        // wire `worker_id` — frees that slot, drops the ledger entry, and
        // releases the type slot, returning the binary. A hash not in the
        // ledger yields `None` (already requeued / terminal): safe no-op.
        if let Some(entry) = self.free_slot_on_terminal(&secondary_id, worker_id, &assigned.hash) {
            self.pool_mut().requeue(entry.task);
            self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskRequeued {
                hash: assigned.hash.clone(),
                // Stamped at the origination choke point.
                version: Default::default(),
            }])
            .await;
        }

        // (2) RECONCILE the slot to the incumbent. The secondary reported
        // that `worker_id` is running `incumbent`; make this primary's slot
        // reflect that so it stops dispatching to the busy worker. Resolve
        // the stable `(secondary, worker_id)` to the live Vec index, and —
        // only if that slot is currently Idle in the model (the diverged
        // belief) — mark it holding the incumbent (Inherited provenance:
        // the occupancy is the secondary's report, reconciled later by the
        // incumbent's own terminal or the holder's next idle re-confirmation,
        // exactly like a failover-reconstructed slot). If the slot already
        // holds the incumbent's hash the model is already correct (no-op);
        // if it holds a DIFFERENT live hash, leave it (the hash-keyed
        // terminal path owns it). No incumbent ⇒ nothing to reconcile.
        if let Some(inc) = incumbent
            && let Some(idx) = self.worker_idx_for(&secondary_id, worker_id)
            && self.workers[idx].is_idle()
        {
            // Seed the incumbent into BOTH the slot and the ledger so the
            // slot-hash invariant holds and the incumbent's terminal settles
            // it by hash — mirroring the failover-resume occupancy crossing.
            if let Some(entry) = self.in_flight.get(&inc.hash) {
                // Already tracked in the ledger (the common case: this primary
                // dispatched the incumbent earlier and only the SLOT belief
                // drifted). Re-seat the slot from the ledger's task; the ledger
                // entry stays as-is.
                let task = entry.task.clone();
                let estimated = self.estimator.estimate(&task);
                // The slot is Idle (guarded just above), so the enforced
                // idle-guard (#517) takes.
                let _assigned =
                    self.workers[idx].assign(inc.hash.clone(), task, estimated, SlotProvenance::Inherited);
            } else {
                tracing::warn!(
                    secondary = %secondary_id,
                    worker_id,
                    incumbent_hash = %inc.hash,
                    "reported incumbent is not in the in-flight ledger; \
                     leaving the slot idle (the holder's own TaskRequest / \
                     terminal will reconcile) — cannot fabricate a ledger \
                     entry without the task body"
                );
            }
        }

        // The requeued task is a pool-entry edge: nudge the dispatch
        // recheck so it lands on a genuinely-idle worker. Decoupled emit,
        // never a direct dispatch call.
        self.cluster_state
            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
    }
}
