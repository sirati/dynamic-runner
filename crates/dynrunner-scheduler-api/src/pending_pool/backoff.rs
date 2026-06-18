//! Per-task re-dispatch backoff.
//!
//! Single concern: rate-limit how soon a task that RE-ENTERED the
//! queue (`requeue` / `reinject`) becomes dispatch-eligible again.
//! The task-level twin of the worker pool's startup-crash respawn
//! backoff: without it, a task whose every dispatch bounces (a
//! backpressure requeue against a crash-looping worker slot) or whose
//! every attempt fails instantly cycles assign → bounce/fail →
//! requeue → re-assign at memory speed (asm-tokenizer
//! run_20260612_095601: one hash re-dispatched 24,323 times, 27k+
//! assignments inside single-second log windows).
//!
//! Mechanism: each re-entry bumps the task's streak; the FIRST
//! re-entry of a streak is free (one bounce is not a spin — the
//! immediate-redispatch semantics of dead-host requeues and drain-edge
//! retry passes are preserved), and from the second the task is
//! stamped with an `eligible_at` instant (`base * 2^(streak-2)`,
//! saturating at `cap`). The dispatch read paths
//! ([`super::PendingPool::view_for_worker`] /
//! [`super::PendingPool::pop_for_worker`]) skip items whose stamp has
//! not expired; everything else (drain accounting, `queued_count`,
//! reclaim via `take_first_match`, bulk `drain_queued`) sees the items
//! normally — a backed-off task still holds its phase open.
//!
//! The streak persists across attempts (cleared only on a TERMINAL
//! observation — success or permanent failure) so a task that keeps
//! bouncing keeps doubling, exactly like the worker respawn streak.
//! [`DispatchBackoff::next_expiry`] exposes the earliest wake an
//! event-driven manager loop should park on instead of polling.
//!
//! The wake is a LEVEL, not an edge. When a stamp first expires the
//! task is eligible again but may not get dispatched on the single
//! recheck the wake triggers (the only eligible worker was transport-
//! gate-skipped, no worker was idle at that instant, an affine-dep
//! gated it). Pre-#640 the secondary's periodic TaskRequest re-poll was
//! the level-triggered safety net; #640 gated that to failover-only and
//! there is no periodic dispatch-sweep arm on the primary, so a wake
//! that lazily-dropped the expired stamp and returned `None` would park
//! the op-loop's backoff arm on `pending()` FOREVER after one fire — a
//! genuine 25-min dispatch deadlock (asm-tokenizer: in_flight=0, zero
//! progress, task_backoff arm count static at 1). To restore the
//! level, an EXPIRED-BUT-UNTAKEN task keeps surfacing a BOUNDED re-poll
//! wake (`now + re_poll_interval`, never `now` raw — a bounded interval
//! cannot hot-spin while a task is legitimately undispatchable) until
//! it is actually [`note_taken`](DispatchBackoff::note_taken) (taken by
//! a worker) or re-stamped by a fresh requeue. This is a primary-side
//! backoff-arm fix; it does NOT restore the per-worker secondary
//! re-poll #640 removed.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::time::{Duration, Instant};

/// Re-dispatch delay at the SECOND consecutive re-entry (the first is
/// free — see [`DispatchBackoff::note_requeued`]). Matches the
/// distributed primary's per-secondary backpressure window.
pub const DISPATCH_BACKOFF_BASE: Duration = Duration::from_millis(500);

/// Saturation for the per-task re-dispatch backoff: a task that
/// bounces or fails forever is retried at most once a minute — the
/// same ceiling as the worker pool's startup-crash respawn backoff.
pub const DISPATCH_BACKOFF_CAP: Duration = Duration::from_secs(60);

/// Re-poll cadence for a task whose backoff has EXPIRED but which has
/// not yet been dispatched (the wake's single recheck missed: no idle
/// worker, the only candidate was transport-gate-skipped, an affine-dep
/// gated it). The level-triggered net that re-services the eligible-
/// but-undispatched task until it is actually taken. Bounded (not
/// `now` raw) so it cannot hot-spin while a task is legitimately
/// undispatchable, yet short enough that a missed dispatch is retried
/// within seconds rather than stranding for the whole backoff cap.
pub const DISPATCH_REPOLL_INTERVAL: Duration = Duration::from_secs(1);

/// Per-task re-dispatch backoff state. Owned by
/// [`super::PendingPool`]; see the module docs for the contract.
#[derive(Debug)]
pub(super) struct DispatchBackoff {
    /// Consecutive re-entries since the task's last terminal. Drives
    /// the exponential delay; cleared only by [`Self::clear`].
    streak: HashMap<String, u32>,
    /// `task_id → eligible_at` for tasks CURRENTLY queued under an
    /// unexpired stamp. Removed when the task is taken
    /// ([`Self::note_taken`]), when the stamp lazily expires inside
    /// [`Self::next_expiry`], or on terminal ([`Self::clear`]).
    until: HashMap<String, Instant>,
    /// Min-heap over `(eligible_at, task_id)` for [`Self::next_expiry`].
    /// Entries are lazily invalidated against `until` (a re-stamp or a
    /// take leaves a stale heap entry behind; the pop loop drops it).
    expiry: BinaryHeap<Reverse<(Instant, String)>>,
    /// Tasks whose stamp has EXPIRED but which have not yet been taken
    /// (the wake's recheck missed). [`Self::next_expiry`] keeps
    /// surfacing a bounded re-poll wake while this is non-empty so the
    /// op-loop backoff arm re-services them; entries are cleared by
    /// [`Self::note_taken`] (dispatched), [`Self::clear`] (terminal), or
    /// a fresh [`Self::note_requeued`] (re-stamped under a new window).
    pending_redispatch: HashSet<String>,
    base: Duration,
    cap: Duration,
    re_poll_interval: Duration,
}

impl Default for DispatchBackoff {
    fn default() -> Self {
        Self {
            streak: HashMap::new(),
            until: HashMap::new(),
            expiry: BinaryHeap::new(),
            pending_redispatch: HashSet::new(),
            base: DISPATCH_BACKOFF_BASE,
            cap: DISPATCH_BACKOFF_CAP,
            re_poll_interval: DISPATCH_REPOLL_INTERVAL,
        }
    }
}

impl DispatchBackoff {
    /// Override the exponential parameters (tests use millisecond
    /// scales; managers may tune per deployment).
    pub(super) fn set_params(&mut self, base: Duration, cap: Duration) {
        self.base = base;
        self.cap = cap;
    }

    /// Override the expired-but-undispatched re-poll cadence. Tests use
    /// a millisecond scale to keep the level-trigger assertion fast.
    pub(super) fn set_re_poll_interval(&mut self, interval: Duration) {
        self.re_poll_interval = interval;
    }

    /// The bounded level-trigger re-poll cadence (default
    /// [`DISPATCH_REPOLL_INTERVAL`], overridable via
    /// [`Self::set_re_poll_interval`]). Read by the phase-drain re-surface
    /// level-trigger so it reuses the same bounded interval the dispatch
    /// backoff arm uses (and honours a test's millisecond override).
    pub(super) fn re_poll_interval(&self) -> Duration {
        self.re_poll_interval
    }

    /// A task re-entered the queue after a bounced/failed attempt:
    /// bump its streak and stamp its next-eligible instant. Tasks
    /// without an identity (empty `task_id`) cannot be tracked and
    /// are never stamped. Returns the applied delay for the caller's
    /// log line (`None` when untracked).
    ///
    /// The FIRST re-entry of a streak is FREE (no stamp): one bounce
    /// or one failure is not a spin, and the immediate-redispatch
    /// semantics every requeue consumer historically relied on (dead-
    /// host work continuity, the drain-edge retry pass) stay intact.
    /// The brake engages from the SECOND consecutive re-entry — the
    /// evidence-of-repetition rule the worker pool's startup-crash
    /// backoff uses (`ever_ready` resets; repetition doubles).
    pub(super) fn note_requeued(&mut self, task_id: &str, now: Instant) -> Option<Duration> {
        if task_id.is_empty() {
            return None;
        }
        let streak = self.streak.entry(task_id.to_string()).or_insert(0);
        *streak = streak.saturating_add(1);
        if *streak == 1 {
            // A first-free bounce is immediately eligible but carries NO
            // future stamp, so it never lands in `expiry`/`until`. Without
            // registering it here the level-net's tail (`next_expiry`) sees
            // an empty heap + empty `pending_redispatch` and returns `None`,
            // parking the op-loop backoff arm forever — the task sits queued,
            // eligible, never re-pushed to an idle worker, holding its phase's
            // drain gate open. Record it as awaiting re-dispatch so the
            // bounded re-poll arm fires (cleared on dispatch via `note_taken`,
            // so no hot-spin).
            self.pending_redispatch.insert(task_id.to_string());
            return Some(Duration::ZERO);
        }
        // `base * 2^(streak-2)`, saturating at `cap`. The shift count
        // is clamped so a pathological streak cannot overflow the
        // multiplier before the `min` clamps the product.
        let exp = (*streak - 2).min(30);
        let delay = self.base.saturating_mul(1u32 << exp).min(self.cap);
        let eligible_at = now + delay;
        self.until.insert(task_id.to_string(), eligible_at);
        self.expiry.push(Reverse((eligible_at, task_id.to_string())));
        // A fresh stamp supersedes any expired-but-undispatched state:
        // the task is now parked under a new (future) window, not
        // awaiting a missed-dispatch re-poll.
        self.pending_redispatch.remove(task_id);
        Some(delay)
    }

    /// Is `task_id` dispatch-eligible at `now`? Untracked ids (never
    /// stamped, stamp consumed, or empty) are always eligible.
    pub(super) fn is_eligible(&self, task_id: &str, now: Instant) -> bool {
        match self.until.get(task_id) {
            Some(eligible_at) => now >= *eligible_at,
            None => true,
        }
    }

    /// The task left the queue (dispatched): drop its stamp and its
    /// expired-but-undispatched re-poll state, but KEEP the streak — a
    /// later re-entry must keep doubling. This is the signal that ENDS
    /// the level-triggered re-poll: a taken task no longer needs a wake.
    pub(super) fn note_taken(&mut self, task_id: &str) {
        self.until.remove(task_id);
        self.pending_redispatch.remove(task_id);
    }

    /// Terminal observation (success or permanent failure): forget
    /// the task entirely.
    pub(super) fn clear(&mut self, task_id: &str) {
        self.streak.remove(task_id);
        self.until.remove(task_id);
        self.pending_redispatch.remove(task_id);
    }

    /// The earliest wake the op-loop backoff arm should park on, or
    /// `None` when nothing needs re-servicing.
    ///
    /// LEVEL-triggered (see the module docs). Two wake kinds, in order:
    ///
    ///   1. A still-FUTURE stamp: the earliest `eligible_at` across the
    ///      queued backed-off tasks. Parking on the absolute instant
    ///      makes the task visible the moment its window expires.
    ///   2. A bounded RE-POLL: once a stamp expires the task is
    ///      eligible (`until` is dropped so `is_eligible` agrees) but
    ///      may not be dispatched on the single recheck the wake
    ///      triggers (no idle worker, transport-gate skip, affine-dep).
    ///      It is moved to `pending_redispatch` and, while any such
    ///      task remains untaken, this returns `now + re_poll_interval`
    ///      so the arm re-fires until the task is actually taken (or
    ///      re-stamped). The interval is bounded, so a legitimately
    ///      undispatchable task cannot hot-spin — it is re-checked at
    ///      most once per interval, not every poll instant.
    ///
    /// A future stamp (kind 1) always wins over a re-poll (kind 2):
    /// when a real window is still pending there is no point waking
    /// early on a sibling that is merely awaiting a worker.
    pub(super) fn next_expiry(&mut self, now: Instant) -> Option<Instant> {
        while let Some(Reverse((at, id))) = self.expiry.peek() {
            match self.until.get(id) {
                // Live stamp, still in the future: this is the wake.
                Some(current) if current == at && *at > now => return Some(*at),
                // Live stamp, expired: the task is eligible — drop the
                // stamp so `is_eligible` and this scan agree, but record
                // it as awaiting re-dispatch so the level persists until
                // the task is actually taken.
                Some(current) if current == at => {
                    let id = id.clone();
                    self.until.remove(&id);
                    self.expiry.pop();
                    self.pending_redispatch.insert(id);
                }
                // Stale heap entry (re-stamped elsewhere, taken, or
                // cleared): drop and keep scanning.
                _ => {
                    self.expiry.pop();
                }
            }
        }
        // No future stamp parked. If any expired task still awaits
        // dispatch, keep the level alive with a bounded re-poll wake.
        if self.pending_redispatch.is_empty() {
            None
        } else {
            Some(now + self.re_poll_interval)
        }
    }
}
