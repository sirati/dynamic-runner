//! Escalating wait-mark schedule for the secondary's instructions wait.
//!
//! # Single concern
//!
//! ONE concern: the OWNER-specified escalating narration schedule over the
//! secondary's "waiting for instructions from setup" span — a log mark
//! after 30s, 1m and 5m of waiting, with the 10-minute point being the
//! give-up abort itself (the [`super::setup_deadline::SetupDeadline`]
//! expiry — its default horizon, `unconfigured_deadline` = 600s, IS the
//! spec's 10m; no second deadline machinery here). This module owns only
//! WHEN a mark is due; [`super::SecondaryCoordinator::wait_for_setup`]
//! owns what the mark says.
//!
//! # The shared clock
//!
//! The marks measure the SAME quantity the deadline measures: time since
//! the wait window's anchor (setup entry, re-anchored by every frame whose
//! sender is the primary — see `note_setup_primary_liveness`). The anchor
//! is READ off the shared [`SetupDeadline`] cell rather than tracked
//! separately, so the narration can never drift from the abort policy: a
//! mark saying "waited 5m" and the deadline aborting at 10m are, by
//! construction, statements about one clock. When the anchor moves
//! (instructions/evidence arrived), the schedule resets — a FRESH silence
//! window narrates from 30s again.
//!
//! # Persistent-deadline law
//!
//! The next-mark instant is STORED state derived from the stored anchor —
//! the `select!` arm rebuilds `sleep_until(next_mark_at())` each iteration
//! from it, so sibling-arm activity (the ~20s anti-entropy digest tick,
//! the handshake-retry arm) can never reset the schedule (the
//! watchdog-needs-a-fires-under-load law; the #324 class shipped dead by
//! arming a per-iteration sleep). A wake at a superseded mark (the anchor
//! moved while sleeping) is observed by [`SetupWaitMarks::fire`] returning
//! `None` and the arm re-sleeps — the exact discipline `SetupDeadline`'s
//! own select arm uses.

use std::time::Duration;

use tokio::time::Instant;

use super::setup_deadline::SetupDeadline;

/// The escalating mark offsets (30s, 1m, 5m). The 10m point of the owner's
/// schedule is NOT listed: it is the deadline expiry itself (the abort is
/// the 10m log line) — one give-up policy, one knob.
pub(in crate::secondary) const WAIT_MARKS: [Duration; 3] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(300),
];

/// Escalating wait-mark schedule over the [`SetupDeadline`]'s anchor.
///
/// Construction order contract: built AFTER the deadline is armed (the
/// orchestration arms at setup entry, before `wait_for_setup` runs) — the
/// anchor read panics on an un-armed cell, same as `SetupDeadline::deadline`.
pub(in crate::secondary) struct SetupWaitMarks {
    /// The shared deadline cell (anchor source — never written here).
    deadline: SetupDeadline,
    /// Snapshot of the anchor the current schedule is measured from.
    anchor: Instant,
    /// Index of the next un-fired mark in [`WAIT_MARKS`].
    next: usize,
}

impl SetupWaitMarks {
    pub(in crate::secondary) fn new(deadline: SetupDeadline) -> Self {
        let anchor = deadline.anchor();
        Self {
            deadline,
            anchor,
            next: 0,
        }
    }

    /// Re-read the shared anchor; on movement (primary evidence arrived —
    /// the wait was answered) restart the schedule from the new anchor.
    /// Returns whether a reset happened.
    fn resync(&mut self) -> bool {
        let current = self.deadline.anchor();
        if current != self.anchor {
            self.anchor = current;
            self.next = 0;
            true
        } else {
            false
        }
    }

    /// The instant the select arm should sleep until: the next un-fired
    /// mark, or — once all marks have fired for this window — a park
    /// instant STRICTLY past the deadline's own expiry (the orchestration's
    /// deadline arm aborts first; parking exactly AT the expiry would make
    /// this arm ready-spin the inner loop and starve the orchestration
    /// select that owns the abort).
    pub(in crate::secondary) fn next_mark_at(&mut self) -> Instant {
        self.resync();
        match WAIT_MARKS.get(self.next) {
            Some(offset) => self.anchor + *offset,
            None => self.deadline.deadline() + self.deadline.horizon(),
        }
    }

    /// Called when the arm's sleep completes: `Some(waited)` iff a mark
    /// genuinely fired (advancing the schedule), `None` when the wake was
    /// superseded (the anchor moved while sleeping — schedule reset, the
    /// arm re-sleeps to the fresh window's first mark).
    pub(in crate::secondary) fn fire(&mut self) -> Option<Duration> {
        if self.resync() {
            return None;
        }
        let mark = *WAIT_MARKS.get(self.next)?;
        if Instant::now() >= self.anchor + mark {
            self.next += 1;
            Some(mark)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn armed(horizon: Duration) -> SetupDeadline {
        let d = SetupDeadline::new(horizon);
        d.arm();
        d
    }

    /// The owner schedule, exactly: marks fire at 30s, 1m and 5m of
    /// waiting — and at no instant in between (the escalating shape, not
    /// a periodic heartbeat).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn marks_fire_at_30s_1m_5m_and_not_between() {
        let mut marks = SetupWaitMarks::new(armed(Duration::from_secs(600)));

        // t=29s: before the first mark — nothing fires.
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(marks.fire(), None, "no mark before 30s");

        // t=30s: the 30s mark.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(marks.fire(), Some(Duration::from_secs(30)));
        assert_eq!(marks.fire(), None, "the 30s mark fires once");

        // t=59s: between 30s and 1m — nothing.
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(marks.fire(), None, "no mark between 30s and 1m");

        // t=60s: the 1m mark.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(marks.fire(), Some(Duration::from_secs(60)));

        // t=299s: between 1m and 5m — nothing.
        tokio::time::advance(Duration::from_secs(239)).await;
        assert_eq!(marks.fire(), None, "no mark between 1m and 5m");

        // t=300s: the 5m mark — the last one this module owns (10m is the
        // deadline abort).
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(marks.fire(), Some(Duration::from_secs(300)));
        assert_eq!(marks.fire(), None, "no marks past 5m — 10m is the abort");
    }

    /// Arrival of instructions mid-wait (the deadline re-arm at 90s)
    /// STOPS the original schedule — its 5m mark never fires — and a
    /// fresh silence window escalates from 30s again, anchored at the
    /// arrival.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn arrival_at_90s_stops_the_schedule_no_5m_mark() {
        let deadline = armed(Duration::from_secs(600));
        let mut marks = SetupWaitMarks::new(deadline.clone());

        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(marks.fire(), Some(Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(marks.fire(), Some(Duration::from_secs(60)));

        // t=90s: instructions/evidence arrive — the loop extends the
        // shared deadline cell (this is the ONLY coupling; the marks read
        // the same cell).
        tokio::time::advance(Duration::from_secs(30)).await;
        deadline.extend();

        // A wake at the superseded old-5m instant (t=300s) is NOT a mark.
        // Walk there via the schedule's own park/next instants: at t=120s
        // (30s into the NEW window) the fresh window's first mark fires —
        // proving the reset — and at the old anchor's t=300s nothing
        // fires for the 5m offset of the OLD window.
        tokio::time::advance(Duration::from_secs(30)).await; // t=120
        // First wake after the anchor moved: the resync observation (no
        // mark — the loop re-sleeps to the fresh window's first mark,
        // which is due NOW), then the mark itself fires.
        assert_eq!(
            marks.fire(),
            None,
            "the first wake after a re-anchor only resyncs (no mark)"
        );
        assert_eq!(
            marks.fire(),
            Some(Duration::from_secs(30)),
            "a fresh silence window re-escalates from 30s, anchored at arrival"
        );
        tokio::time::advance(Duration::from_secs(180)).await; // t=300 (old anchor + 5m)
        assert_eq!(
            marks.fire(),
            Some(Duration::from_secs(60)),
            "t=300s is 210s into the NEW window: its 1m mark is due (fired \
             late, in order) — the OLD window's 5m mark is gone"
        );
        // The new window's 5m mark is due at t=390s, not t=300s.
        assert_eq!(marks.fire(), None);
        tokio::time::advance(Duration::from_secs(90)).await; // t=390
        assert_eq!(marks.fire(), Some(Duration::from_secs(300)));
    }

    /// `next_mark_at` after the last mark parks STRICTLY past the
    /// deadline expiry, so the exhausted arm can never ready-spin while
    /// the orchestration's abort arm is due.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn exhausted_schedule_parks_past_the_abort() {
        let deadline = armed(Duration::from_secs(600));
        let mut marks = SetupWaitMarks::new(deadline.clone());
        for expect in WAIT_MARKS {
            tokio::time::advance(
                (deadline.anchor() + expect) - Instant::now(),
            )
            .await;
            assert_eq!(marks.fire(), Some(expect));
        }
        assert!(
            marks.next_mark_at() > deadline.deadline(),
            "the exhausted schedule must park past the abort instant"
        );
    }

    /// The select-arm wake discipline: a sleep armed at the old window's
    /// mark that wakes AFTER the anchor moved observes `None` (no mark),
    /// and the next sleep targets the fresh window.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn superseded_wake_is_not_a_mark() {
        let deadline = armed(Duration::from_secs(600));
        let mut marks = SetupWaitMarks::new(deadline.clone());
        let first = marks.next_mark_at();

        // Evidence at t=10s moves the anchor; the in-flight sleep still
        // wakes at the OLD t=30s instant.
        tokio::time::advance(Duration::from_secs(10)).await;
        deadline.extend();
        tokio::time::advance(Duration::from_secs(20)).await; // the old wake instant
        assert!(Instant::now() >= first);
        assert_eq!(marks.fire(), None, "a superseded wake fires no mark");
        assert_eq!(
            marks.next_mark_at(),
            deadline.anchor() + WAIT_MARKS[0],
            "the re-sleep targets the fresh window's first mark"
        );
    }
}
