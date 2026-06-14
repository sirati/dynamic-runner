//! `PendingPool` bring-up reservation: the FORMATION-WINDOW overlay that
//! tags each queued task with the member it is RESERVED for, so a
//! member's dispatch view sees only its own share until every expected
//! member has either confirmed (and drained its share) or timed out.
//!
//! ## The single concern this owns
//!
//! WHICH task a member's worker may SEE during cold/staggered bring-up.
//! The #382 mesh-veto already withholds a `TaskAssignment` from an
//! UNCONFIRMED member; but with no per-member cap the first-confirmed
//! members' idle workers greedily drain the WHOLE global pool before the
//! late confirmers arrive (the #494 14/14/0×13 pack). The reservation
//! caps what each member can pull to a pre-computed share, so a member
//! only ever drains its own slice and the late confirmers' slices stay
//! held until they arrive.
//!
//! The reservation is POOL-SIDE and RECLAIMABLE: nothing leaves the
//! pool, no task is sent to anyone here. A reserved task sits in its
//! normal `(phase, type, affinity)` bucket — still counted for the phase
//! machine, still taken via `take_selected` — but is INVISIBLE to a
//! member that is not its reservation holder while the window is open.
//!
//! ## Identity key
//!
//! Reservations are keyed on the task's stable `(phase_id, task_id)`
//! identity — the SAME identity `partition_ingest` and the task-dep
//! tracker use. Every task carries a non-empty `task_id` (validated at
//! `extend`), and the identity survives a `take` / `requeue` / bucket
//! reshuffle, so a reservation cannot dangle onto a stale locator.
//!
//! ## Window lifecycle (driven by the coordinator)
//!
//! * `open_reservation(plan)` — the coordinator, at bring-up, partitions
//!   the initial pending pool across the connected fleet via its existing
//!   projected-load interleave (one task per idle worker; the surplus
//!   stays unreserved) and hands the resulting per-task → member
//!   assignment here. Opens the window.
//! * `redistribute_member(member, fallbacks)` — a member was declared
//!   DEAD (the heartbeat path's genuine member-removal — NOT the
//!   mesh-ready proceed-deadline, which leaves a slow-to-form member's
//!   share HELD for it to claim late; redistributing at the deadline
//!   would re-clump the share onto the already-confirmed members). Its
//!   still-pooled reserved tasks fold, round-robin, onto the supplied
//!   ordered fallback members (the coordinator supplies the load-ordered
//!   survivors — the interleave owner stays the coordinator).
//!   Cascade-safe: invoked once per dead member, survivors accumulate.
//! * The window CLOSES AUTOMATICALLY — there is no explicit close. It
//!   closes the moment the holder map empties, which happens exactly when
//!   formation is truly over: every reserved task has either DRAINED (its
//!   confirmed holder took it via `take_at → note_taken`) or had its DEAD
//!   holder REDISTRIBUTED (and the survivors then drained those too).
//!   Closing earlier (e.g. at the mesh-ready proceed-deadline) would free
//!   a slow-to-form member's still-HELD share onto the already-confirmed
//!   members — the very re-clump this fix exists to prevent — so the
//!   incremental drain/redistribute mechanisms are the SOLE close path.
//!   Once closed, `reservation_admits` is a no-op filter, so
//!   streamed/steady-state tasks dispatch through the normal interleave
//!   with no reservation in the way.
//!
//! ## The view seam
//!
//! `reservation_admits(member, task)` is the per-item predicate the
//! distributed coordinator layers onto its `dispatch_view_for_worker` (via
//! the existing `WorkerView::filter` combinator). A reserved task is
//! WITHHELD from `member` whenever the task is reserved to a DIFFERENT
//! holder while the window is open — the case the pack exploited. It is
//! admitted:
//!   * always when the window is closed (no overlay);
//!   * always for an UNRESERVED task — the capacity-bounded partition
//!     reserves at most one task per idle worker, so every task beyond
//!     total idle capacity is unreserved and free for the formed fleet
//!     (the steady-state path);
//!   * to its HOLDER, and ONLY its holder, while the window is open.
//!
//! There is NO freed-on-confirm widening (#507): a holder's share stays
//! bound to it until it DRAINS (`note_taken`) or its DEAD holder is
//! REDISTRIBUTED (`redistribute_member`). Widening a confirmed holder's
//! share to the whole fleet let a co-located, high-worker node steal a
//! just-confirmed member's share before its own workers pulled it. The
//! capacity bound makes holder-only safe: a holder is never reserved more
//! tasks than it has idle workers, so it always drains its own share and
//! nothing strands by NOT widening. The pool therefore needs no
//! mesh-confirmation fact at all — the admit decision is purely
//! holder-identity, owned wholly by the pool.
//!
//! The local single-node manager NEVER opens a reservation, so its
//! `view_for_worker` path is wholly unaffected — the overlay is inert
//! until a distributed coordinator opens it.

use std::collections::HashMap;

use dynrunner_core::{Identifier, PhaseId, TaskInfo};

use super::pool::PendingPool;

/// One queued task's bring-up identity for the reservation overlay: the
/// stable `(phase_id, task_id)` the pool already uses for dep tracking.
/// Public so a coordinator can build an `open_reservation` plan keyed on
/// the identity it derives per task.
pub type ReservationKey = (PhaseId, String);

/// The bring-up formation-window reservation overlay. Empty (`active ==
/// false`, no holders) outside the bring-up window — the inert default
/// every pool starts with and every steady-state pool returns to.
#[derive(Debug, Default)]
pub(super) struct TaskReservation {
    /// `(phase_id, task_id)` → the member id this task is reserved for
    /// while the window is open. A task absent from this map is
    /// UNRESERVED (free for any member — a streamed/late task).
    holder: HashMap<ReservationKey, String>,
    /// Is the formation window open? `false` makes the whole overlay a
    /// no-op (`reservation_admits` returns `true` for everything), even
    /// if a residual `holder` entry survived — closed-but-nonempty never
    /// happens in practice (close zeroes the map) but the flag is the
    /// authoritative "is the overlay live" gate.
    active: bool,
}

impl TaskReservation {
    /// True iff `member` may SEE `key` under the current overlay.
    ///
    /// The overlay's SOLE job is to protect a still-FORMING member's
    /// share from any OTHER member during the formation window. A reserved
    /// task admits:
    ///   * everyone, when the window is closed;
    ///   * everyone, when it carries no holder (an UNRESERVED task — the
    ///     capacity-bounded partition leaves every task beyond total idle
    ///     capacity unreserved, free for the formed fleet);
    ///   * its HOLDER, and ONLY its holder, while the window is open.
    ///
    /// It is WITHHELD from any member that is not its holder. There is NO
    /// freed-on-confirm widening (the #507 removal): a holder's share stays
    /// bound to it until the holder DRAINS it (`note_taken`) or its DEAD
    /// holder is REDISTRIBUTED (`redistribute_member`). Widening on confirm
    /// let a co-located, high-worker node steal a just-confirmed member's
    /// share before that member's own workers pulled it (the 14/2/0×N
    /// pack). The capacity bound makes holder-only safe: a holder is never
    /// reserved MORE tasks than it has idle workers, so it can always drain
    /// its whole share itself — there is no over-subscription overflow to
    /// strand by NOT widening. A genuinely-undrainable surplus (tasks
    /// beyond total idle capacity) is unreserved from the start, not held
    /// to anyone.
    fn admits(&self, member: &str, key: &ReservationKey) -> bool {
        if !self.active {
            return true;
        }
        match self.holder.get(key) {
            Some(h) => h == member,
            None => true,
        }
    }

    /// A reserved task just left the queue (its holder confirmed and a
    /// worker took it). Drop its holder entry; the window closes the
    /// moment the last reserved task drains, so the overlay is
    /// self-maintaining across the normal confirm→dispatch path with no
    /// explicit close call. Mirrors `DispatchBackoff::note_taken`'s slot
    /// in `take_at` — called via a disjoint `self.reservation` field
    /// borrow so the live bucket borrow there stays valid.
    pub(super) fn note_taken(&mut self, key: &ReservationKey) {
        if !self.active {
            return;
        }
        self.holder.remove(key);
        if self.holder.is_empty() {
            self.active = false;
        }
    }
}

impl<I: Identifier> PendingPool<I> {
    /// `(phase_id, task_id)` identity key for a task — the reservation
    /// overlay's stable handle. Mirrors the key `partition_ingest` builds.
    fn reservation_key(item: &TaskInfo<I>) -> ReservationKey {
        (item.phase_id.clone(), item.task_id.clone())
    }

    /// OPEN the bring-up reservation window with a pre-computed per-task
    /// → member partition. `plan` pairs each task identity the
    /// coordinator's interleave assigned with the member it reserved it
    /// for. Tasks omitted from `plan` stay UNRESERVED (free for anyone).
    ///
    /// Single concern: install the formation-window overlay. The PARTITION
    /// POLICY (which member each task goes to, via the projected-load
    /// interleave) is the coordinator's — this just stores the result. A
    /// re-open replaces the prior overlay wholesale.
    pub fn open_reservation(
        &mut self,
        plan: impl IntoIterator<Item = (ReservationKey, String)>,
    ) {
        self.reservation.holder = plan.into_iter().collect();
        self.reservation.active = !self.reservation.holder.is_empty();
    }

    /// Whether the bring-up reservation window is currently open. Once it
    /// closes (every reserved task drained or redistributed) the overlay
    /// is inert and the coordinator stops scoping views.
    pub fn reservation_active(&self) -> bool {
        self.reservation.active
    }

    /// May `member` SEE `item` under the current reservation overlay?
    /// The per-item predicate the distributed coordinator layers onto its
    /// dispatch view via `WorkerView::filter`. A reserved task is withheld
    /// from every member that is not its holder while the window is open
    /// (holder-only — no freed-on-confirm widening, #507). See
    /// `TaskReservation::admits` and the module docs for the full contract.
    pub fn reservation_admits(&self, member: &str, item: &TaskInfo<I>) -> bool {
        self.reservation
            .admits(member, &Self::reservation_key(item))
    }

    /// REDISTRIBUTE a timed-out member's reserved share. Every task still
    /// QUEUED in the pool (a `take` already removed the drained ones, so
    /// they never reach here) that is reserved for `member` folds,
    /// round-robin, onto `fallbacks` — the coordinator-supplied ordered
    /// list of not-yet-terminal members (load-ordered by the coordinator,
    /// the sole interleave owner). The timed-out member's holder entries
    /// are rewritten to the chosen fallback.
    ///
    /// Cascade-safe: called once per timed-out member. If a later member
    /// also times out, its (possibly already-redistributed-onto) share
    /// folds again onto the then-current survivors. When `fallbacks` is
    /// empty (the lone-survivor / everyone-else-gone edge) the share is
    /// UNRESERVED instead (admits everyone) rather than stranded — the
    /// pool must never hold a task reserved for a member that can no
    /// longer take it.
    ///
    /// Closes the window if nothing remains reserved afterwards.
    pub fn redistribute_member(&mut self, member: &str, fallbacks: &[String]) {
        if !self.reservation.active {
            return;
        }
        // Only QUEUED tasks can be redistributed: a drained task is gone
        // from every bucket, so its identity below resolves no live item
        // and a stale holder entry for it is harmless (it admits the
        // already-departed task to nobody, but nobody asks). Rebuild the
        // holder for the timed-out member's QUEUED identities only.
        let queued_keys: std::collections::HashSet<ReservationKey> =
            self.iter().map(Self::reservation_key).collect();

        // The timed-out member's still-queued reserved identities, in the
        // pool's deterministic queued-iteration order so the round-robin
        // fold is reproducible.
        let to_move: Vec<ReservationKey> = self
            .iter()
            .map(Self::reservation_key)
            .filter(|k| self.reservation.holder.get(k).map(String::as_str) == Some(member))
            .collect();

        for (i, key) in to_move.into_iter().enumerate() {
            match fallbacks.is_empty() {
                // No survivor to hold it: drop the reservation entirely so
                // it admits everyone (never strand on a gone member).
                true => {
                    self.reservation.holder.remove(&key);
                }
                false => {
                    let pick = fallbacks[i % fallbacks.len()].clone();
                    self.reservation.holder.insert(key, pick);
                }
            }
        }

        // Drop any holder entry for `member` whose task already drained
        // (not in `queued_keys`) so a confirmed-then-timed-out member
        // can't leave a dangling self-reservation.
        self.reservation
            .holder
            .retain(|k, h| h != member || queued_keys.contains(k));

        if self.reservation.holder.is_empty() {
            self.reservation.active = false;
        }
    }
}
