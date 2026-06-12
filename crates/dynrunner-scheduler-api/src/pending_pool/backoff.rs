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
//! [`DispatchBackoff::next_expiry`] exposes the earliest FUTURE stamp
//! so an event-driven manager loop can park a wake on it instead of
//! polling; expired stamps are lazily dropped so the wake can never
//! hot-fire on an already-eligible task.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

/// Re-dispatch delay at the SECOND consecutive re-entry (the first is
/// free — see [`DispatchBackoff::note_requeued`]). Matches the
/// distributed primary's per-secondary backpressure window.
pub const DISPATCH_BACKOFF_BASE: Duration = Duration::from_millis(500);

/// Saturation for the per-task re-dispatch backoff: a task that
/// bounces or fails forever is retried at most once a minute — the
/// same ceiling as the worker pool's startup-crash respawn backoff.
pub const DISPATCH_BACKOFF_CAP: Duration = Duration::from_secs(60);

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
    base: Duration,
    cap: Duration,
}

impl Default for DispatchBackoff {
    fn default() -> Self {
        Self {
            streak: HashMap::new(),
            until: HashMap::new(),
            expiry: BinaryHeap::new(),
            base: DISPATCH_BACKOFF_BASE,
            cap: DISPATCH_BACKOFF_CAP,
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

    /// The task left the queue (dispatched): drop its stamp but KEEP
    /// the streak — a later re-entry must keep doubling.
    pub(super) fn note_taken(&mut self, task_id: &str) {
        self.until.remove(task_id);
    }

    /// Terminal observation (success or permanent failure): forget
    /// the task entirely.
    pub(super) fn clear(&mut self, task_id: &str) {
        self.streak.remove(task_id);
        self.until.remove(task_id);
    }

    /// Earliest FUTURE eligible-at stamp across the queued
    /// backed-off tasks, or `None` when nothing is parked. Lazily
    /// drops heap entries that are stale (re-stamped / taken /
    /// cleared) and stamps that have EXPIRED (the task is eligible
    /// again — keeping the stamp would make an event-loop wake parked
    /// on this value hot-fire forever on a task no worker is free
    /// for).
    pub(super) fn next_expiry(&mut self, now: Instant) -> Option<Instant> {
        while let Some(Reverse((at, id))) = self.expiry.peek() {
            match self.until.get(id) {
                // Live stamp, still in the future: this is the wake.
                Some(current) if current == at && *at > now => return Some(*at),
                // Live stamp, expired: the task is eligible — drop the
                // stamp so `is_eligible` and this scan agree.
                Some(current) if current == at => {
                    let id = id.clone();
                    self.until.remove(&id);
                    self.expiry.pop();
                }
                // Stale heap entry (re-stamped elsewhere, taken, or
                // cleared): drop and keep scanning.
                _ => {
                    self.expiry.pop();
                }
            }
        }
        None
    }
}
