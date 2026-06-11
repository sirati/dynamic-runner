//! Re-armable primary-liveness deadline for the pre-`Operational` span.
//!
//! # Single concern
//!
//! ONE concern: the `unconfigured_deadline`'s PURPOSE is detecting a DEAD
//! (or unreachable) primary during setup — never punishing a SLOW fleet
//! assembly. This type makes that purpose structural: it is a shared
//! deadline cell that [`super::SecondaryCoordinator::wait_for_setup`]
//! RE-ARMS to `now + horizon` on every frame whose sender is the primary
//! (directed or broadcast — any frame from the primary proves it alive),
//! and that the orchestration boundary
//! (`run_until_setup_or_done_inner`) sleeps against. The deadline only
//! fires after a full `horizon` of PRIMARY silence, not after a full
//! horizon of incomplete setup.
//!
//! # Why (the asm-dataset LMU fleet death)
//!
//! Pre-fix the deadline was a FIXED `tokio::time::timeout(deadline,
//! setup)` armed at setup entry. The primary's quorum-proceed straggler
//! window (`connect_timeout`, 600s) and the secondaries' setup deadline
//! (`unconfigured_deadline`, 600s) shared the value, and the secondaries
//! armed EARLIER (they boot before the primary starts waiting) — so ANY
//! missing secondary made the primary wait out its full window while
//! every welcomed, announce-received, provably-primary-connected
//! secondary died at its own deadline first: 15/15 exited
//! "setup deadline elapsed despite peers reachable" at 11:15:50 and the
//! primary proceeded with quorum at 11:16:10 into a dead fleet. With the
//! re-arm, a live assembling primary (its setup-liveness digest beacon,
//! its `PeerJoined`/cold-seed broadcasts, its directed setup frames)
//! keeps the connected fleet alive indefinitely; the deadline fires only
//! on true primary silence — the thing it exists to detect.
//!
//! # Shape
//!
//! A cheap `Rc<Cell<…>>` handle (the secondary is `LocalSet`-bound,
//! single-threaded): the coordinator holds one clone as a field (the
//! re-arm writer inside `wait_for_setup`), the orchestration clones
//! another BEFORE constructing the setup future (so the `sleep_until`
//! reader never borrows `self`). The select arm rebuilds
//! `sleep_until(stored)` each iteration from the STORED instant — the
//! persistent-deadline law: sibling-arm activity can never reset it, and
//! an extension only ever moves the stored instant FORWARD, so the arm
//! wakes at the old instant, observes `!expired()`, and re-sleeps to the
//! new one.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use tokio::time::Instant;

/// Shared re-armable deadline: `arm()` / `extend()` set the stored
/// instant to `now + horizon`; `deadline()` / `expired()` read it.
/// Clones share the one cell. (`pub(crate)` only because the coordinator
/// field carrying it is `pub(super)` on the crate-public struct; nothing
/// outside `crate::secondary` constructs or reads one.)
#[derive(Clone, Debug)]
pub(crate) struct SetupDeadline {
    /// `None` until [`Self::arm`] (construction happens in
    /// `SecondaryCoordinator::new`, possibly outside a tokio runtime, so
    /// no `Instant::now()` is taken there). `Some(at)` once armed.
    at: Rc<Cell<Option<Instant>>>,
    horizon: Duration,
}

impl SetupDeadline {
    /// Build an un-armed deadline with the given primary-silence horizon
    /// (the `unconfigured_deadline` config value). Takes no `Instant` —
    /// safe to call outside a runtime.
    pub(in crate::secondary) fn new(horizon: Duration) -> Self {
        Self {
            at: Rc::new(Cell::new(None)),
            horizon,
        }
    }

    /// Arm (or re-arm) the deadline at `now + horizon`. Called once at
    /// the orchestration boundary when the setup span begins.
    pub(in crate::secondary) fn arm(&self) {
        self.at.set(Some(Instant::now() + self.horizon));
    }

    /// Push the deadline out to `now + horizon` — the primary has just
    /// proven itself alive (a frame from it was received). Identical to
    /// [`Self::arm`]; named separately so call sites read as what they
    /// are (liveness evidence, not span entry).
    pub(in crate::secondary) fn extend(&self) {
        self.arm();
    }

    /// The stored absolute deadline. Panics if never armed — the
    /// orchestration arms before any reader runs (encoding that ordering
    /// as an `expect` keeps a future mis-wiring loud instead of silently
    /// sleeping forever).
    pub(in crate::secondary) fn deadline(&self) -> Instant {
        self.at
            .get()
            .expect("SetupDeadline read before arm() — the orchestration arms it at setup entry")
    }

    /// Whether the stored deadline has truly elapsed. The select arm
    /// checks this after `sleep_until` fires: a wake at a deadline that
    /// was extended while sleeping is NOT an expiry — the arm re-sleeps
    /// to the new stored instant.
    pub(in crate::secondary) fn expired(&self) -> bool {
        Instant::now() >= self.deadline()
    }

    /// The instant the CURRENT wait window began — the last `arm()` /
    /// `extend()` (i.e. setup entry, or the most recent primary-liveness
    /// evidence). Derived from the stored deadline (`deadline − horizon`)
    /// so the one cell stays the single source of truth for "how long has
    /// this secondary been waiting for instructions": the escalating
    /// wait-mark narration ([`super::wait_marks`]) and the give-up policy
    /// (the deadline itself) share the same clock by construction. Same
    /// armed-before-read contract as [`Self::deadline`].
    pub(in crate::secondary) fn anchor(&self) -> Instant {
        self.deadline() - self.horizon
    }

    /// The configured primary-silence horizon (the `unconfigured_deadline`
    /// config value this cell was built with).
    pub(in crate::secondary) fn horizon(&self) -> Duration {
        self.horizon
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `extend()` moves the stored deadline forward and `expired()`
    /// tracks the STORED instant — the re-arm semantics the orchestration
    /// loop relies on (a wake at a superseded instant is not an expiry).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn extend_moves_the_stored_deadline_forward() {
        let d = SetupDeadline::new(Duration::from_secs(10));
        d.arm();
        let first = d.deadline();
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(!d.expired(), "6s into a 10s horizon is not expired");
        d.extend();
        assert!(
            d.deadline() > first,
            "extend() must move the stored instant forward"
        );
        // The OLD deadline elapses; the extended one has not.
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(
            !d.expired(),
            "12s after arm but only 6s after extend — the re-armed horizon governs"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(d.expired(), "11s of silence past the last extend expires");
    }

    /// Clones share the one cell: a writer clone's `extend()` is visible
    /// to a reader clone — the coordinator-field / orchestration-handle
    /// split the borrow checker forces.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn clones_share_the_deadline_cell() {
        let writer = SetupDeadline::new(Duration::from_secs(5));
        let reader = writer.clone();
        writer.arm();
        tokio::time::advance(Duration::from_secs(4)).await;
        writer.extend();
        tokio::time::advance(Duration::from_secs(4)).await;
        assert!(
            !reader.expired(),
            "the reader clone must observe the writer clone's extension"
        );
    }
}
