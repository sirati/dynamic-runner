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

/// Minimum spacing between the rate-limited pathological-bounce WARNs the
/// reconcile path drives through `illegal_assignment_warn`. The per-event
/// reconcile log is DEBUG (the bounce is expected, no-loss optimistic-dispatch
/// churn at scale — #531 RCA); this interval gates the ONE escalated WARN that
/// surfaces a SUSTAINED bounce rate (the pathological-loop signature). Matched
/// to the keepalive-egress throttle cadence — a minute is short enough to catch
/// a live loop, long enough to stay off the operator's normal logs.
pub(in crate::primary) const ILLEGAL_ASSIGNMENT_WARN_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// React to a secondary's illegal-assignment bounce: reconcile this
    /// primary's `(secondary, worker_id)` occupancy model + REQUEUE the
    /// bounced task. Never a failure (no terminal accounting).
    ///
    /// Two coupled steps, both idempotent:
    ///
    /// 1. RECOVER the assigned (bounced) task, kind-dispatched (the SAME
    ///    split `handle_task_failed` makes at its head):
    ///    * An affine IMPORT (`affine_id_for_hash` is `Some`) recovers like
    ///      `handle_affine_task_failed`'s backpressure arm — reset its
    ///      per-secondary cell `Queued → NotDone` and re-derive the blocked
    ///      dependents now (#665). The import is per-secondary and NOT
    ///      worker-assignable, so it must never take the work-pool requeue
    ///      (which can never re-surface it and would leave its cell `Queued`
    ///      — the `InFlightHere` strand); its slot is freed slot-direct via
    ///      `free_affine_slot_on_terminal`.
    ///    * Any other task: if it is still in the in-flight ledger this
    ///      primary committed it (optimistically, onto a slot the secondary
    ///      refused), so free that hold and return the binary to `Pending`
    ///      (`InFlight → Pending`, broadcast in lockstep — the SAME
    ///      `TaskRequeued` origination the dead-secondary / backpressure
    ///      paths emit). A hash already absent from the ledger (a raced
    ///      terminal / a prior requeue) is a safe no-op.
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

        // The per-event reconcile log is DEBUG: a bounce is an EXPECTED,
        // fully-reconciled, no-loss in-flight race (the primary's optimistic
        // per-(secondary, worker_id) dispatch raced the secondary's physical
        // respawn/requeue-rebind — #531 RCA: 414 events at 154-worker
        // saturation, all cleanly requeued). Keep the full structured fields
        // for forensics.
        tracing::debug!(
            secondary = %secondary_id,
            worker_id,
            assigned_hash = %assigned.hash,
            incumbent_hash = incumbent.as_ref().map(|i| i.hash.as_str()),
            "secondary bounced an ILLEGAL assignment: the assigned worker was \
             not idle. The per-(secondary, worker_id) occupancy model diverged \
             from physical reality; reconciling the slot to the incumbent and \
             requeuing the bounced task (NOT failing it)"
        );

        // Rate-limited escalation: a SUSTAINED bounce rate is the
        // pathological-loop signature (a genuine repeated same-(secondary,
        // worker) bounce — #518 H3.3 — or a future regression), distinct from
        // the expected steady-state churn the DEBUG line above absorbs. Emit at
        // most one WARN per interval, naming this bounce's identity as a sample
        // plus the count suppressed since the last emit, so the operator sees
        // the RATE without per-event spam. The reconcile path is the single
        // choke point every bounce passes through, so this global throttle
        // captures the fleet-wide rate.
        if let Some(suppressed) = self.illegal_assignment_warn.permit() {
            tracing::warn!(
                sample_secondary = %secondary_id,
                sample_worker_id = worker_id,
                sample_assigned_hash = %assigned.hash,
                suppressed_since_last_warn = suppressed,
                "illegal-assignment bounces are occurring (expected at scale, \
                 handled no-loss); reporting the RATE so a PATHOLOGICAL loop \
                 (a repeated same-(secondary,worker) bounce) is visible. A high \
                 suppressed count between these throttled WARNs indicates a \
                 sustained bounce loop, not steady-state optimistic-dispatch \
                 churn"
            );
        }

        // (1) RECOVER the bounced task — kind-dispatched on whether it is an
        // affine IMPORT (a `SecondaryAffine` def, identified by `affine_id_for_hash`
        // resolving its content hash to a bitvector cell) or any other task. The
        // SAME split `handle_task_failed` makes at its head (failed.rs: route a
        // backpressure-shaped affine bounce to `handle_affine_task_failed`): an
        // import is per-secondary, NOT worker-assignable, and tracked by the
        // per-secondary cell, so it must NEVER take the work-pool requeue (which
        // can never re-surface it AND leaves its cell `Queued` — the original
        // `InFlightHere` wedge); it recovers through the affine seams instead.
        if let Some(affine_id) = self.cluster_state.affine_id_for_hash(&assigned.hash) {
            // AFFINE-IMPORT BOUNCE (#665): an on-demand import committed against
            // this primary's model slot (`dispatch_affine_import_on_demand` →
            // `commit_assignment`) but the secondary's PHYSICAL pool had
            // shrunk/respawned (stale roster: out-of-range id / 0-worker pool /
            // mid-respawn), so it bounced `IllegallyAssignedToNonidleWorker`
            // instead of landing. This is the WIRE twin of a backpressure
            // `TaskFailed` import bounce — the import never ran here — so it
            // recovers byte-for-byte like `handle_affine_task_failed`'s
            // backpressure arm: reset the cell `Queued → NotDone` and re-derive
            // the blocked dependents NOW (otherwise the cell stays
            // `Queued`-with-no-holder forever and the dependent work sits
            // `InFlightHere` in `blocked_per_secondary` forever — the strand).
            //
            // The bounce carries NO capacity-exhaustion signal (it is a
            // roster/occupancy divergence, not "every worker busy"), so the
            // recheck is the PROMPT `TasksAdded` — the same shape the non-capacity
            // backpressure arm and the on-demand `Refused` recovery use, with no
            // backpressure-flag brake.
            //
            // `affine_unqueue_mutation` is `Some` here (gated on `affine_id_for_hash`
            // just above); the `if let` is defensive, mirroring failed.rs:572.
            if let Some(m) = self.affine_unqueue_mutation(&secondary_id, &assigned.hash) {
                self.apply_and_broadcast_cluster_mutations(vec![m]).await;
            }
            self.reenqueue_affine_unblocked_on_cell(
                &secondary_id,
                affine_id,
                WorkerMgmtSignal::TasksAdded,
            )
            .await;
            // Free the holding slot for THIS per-secondary run, addressed by the
            // terminal's OWN `(secondary, worker_id)` — slot-direct, NOT the shared
            // hash-keyed `free_slot_on_terminal`. The import runs the same hash on
            // multiple secondaries concurrently; the hash-keyed ledger holds only
            // one holder, so the shared path would free the WRONG secondary's slot
            // and orphan this worker's slot `Assigned` forever (the exact reason
            // failed.rs:705 uses `free_affine_slot_on_terminal`). No `pool.requeue`
            // and no `TaskRequeued`: the import is never a pool item — it re-derives
            // on-demand off the reset cell.
            self.free_affine_slot_on_terminal(&secondary_id, worker_id, &assigned.hash);
        } else if let Some(entry) =
            self.free_slot_on_terminal(&secondary_id, worker_id, &assigned.hash)
        {
            // NON-IMPORT BOUNCE. `free_slot_on_terminal` resolves the holder slot
            // from the LEDGER entry (by hash) — independent of the wire
            // `worker_id` — frees that slot, drops the ledger entry, and releases
            // the type slot, returning the binary. A hash not in the ledger yields
            // `None` (already requeued / terminal): safe no-op.
            //
            // Affine-aware (the SAME recovery seam the dead-secondary and
            // backpressure-failed requeue-of-recovered-work sites use): an
            // affine-dependent WORK task must clear the affine scheduler's
            // `placed_work` dedup on requeue, or it is hidden from the global
            // worker view AND blocked from re-placement — permanently
            // unassignable (and NOT recovered by the reconcile, since a bounced
            // work is not in `blocked_per_secondary`). `requeue_affine_aware`
            // clears the guard so the same-tick `TasksAdded` recheck re-derives
            // its per-secondary unit; a non-affine-dep task takes the unchanged
            // `pool.requeue` (byte-identical to before).
            self.requeue_affine_aware(entry.task);
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
                // CROSS-MEMBER GUARD (#531): the ledger is hash-keyed and
                // single-entry, so the incumbent hash may be attributed to a
                // DIFFERENT member than the one bouncing here — precisely the
                // #518 cross-member re-seat, which repoints the hash's ledger
                // entry onto the authoritative holder A while a duplicate copy
                // keeps running on this bouncing member B. Re-seating B's slot
                // Inherited-holding-A's-hash would corrupt A: B's next
                // TaskRequest trips `reconcile_inherited_slot`, which removes
                // the hash's ledger entry (A's) and requeues A's still-running
                // task. So re-seat ONLY when the ledger already attributes the
                // hash to THIS bouncing member; otherwise leave B's slot Idle
                // (it bounces the next assignment, handled no-loss by this same
                // path — the cheap, self-healing outcome — while A's ledger
                // entry and A's own terminal stay the single owner of the hash).
                if entry.secondary_id != secondary_id {
                    tracing::debug!(
                        secondary = %secondary_id,
                        worker_id,
                        incumbent_hash = %inc.hash,
                        ledger_holder = %entry.secondary_id,
                        "reported incumbent is authoritatively held by a \
                         DIFFERENT member in the ledger (a cross-member re-seat \
                         duplicate still running here); NOT re-seating this \
                         slot — the authoritative holder owns the hash and \
                         re-seating here would later corrupt its ledger entry"
                    );
                } else {
                    // Already tracked in the ledger AND attributed to this
                    // member (the common case: this primary dispatched the
                    // incumbent earlier and only the SLOT belief drifted).
                    // Re-seat the slot from the ledger's task; the ledger entry
                    // stays as-is.
                    let task = entry.task.clone();
                    let estimated = self.estimator.estimate(&task);
                    // The slot is Idle (guarded just above), so the enforced
                    // idle-guard (#517) takes.
                    let _assigned = self.workers[idx].assign(
                        inc.hash.clone(),
                        task,
                        estimated,
                        SlotProvenance::Inherited,
                    );
                }
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
