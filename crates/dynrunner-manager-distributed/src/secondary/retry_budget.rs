//! Retry budget for the secondary's primary-side retry passes.
//!
//! # Concern
//!
//! Decide whether the promoted-secondary's primary-side retry loop
//! should run another pass. The decision combines two independent
//! exhaustion axes:
//!
//!   1. **Attempt count** — a hard upper bound on how many times we
//!      re-inject failed tasks back into the pool. Comes from
//!      [`SecondaryConfig::retry_max_passes`] (and originally from
//!      `PyO3.distributed_config.retry_max_passes`).
//!   2. **Wallclock deadline** — when the surrounding SLURM allocation
//!      ends. Read once at construction from `$SLURM_JOB_END_TIME`. A
//!      retry pass started after `deadline - safety_margin` has no
//!      hope of completing before the container is reaped, so we
//!      decline the pass even if the attempt-count budget has slots
//!      left.
//!
//! # Module boundary
//!
//! Owns: the retry budget calculation. Period.
//!
//! Crosses: [`SecondaryCoordinator`] holds one instance and consults
//! it in three places (see `primary.rs::primary_drain_check_and_retry`
//! and `processing.rs`'s two drain-down exit conditions). Callers
//! never poke the internal counter / deadline — the only verbs are
//! [`RetryBudget::should_retry`] (read) and
//! [`RetryBudget::record_attempt`] (write).
//!
//! # Env-var format
//!
//! SLURM (≥17.x) exports `SLURM_JOB_END_TIME` as a Unix epoch
//! timestamp (seconds since 1970-01-01 UTC), emitted from
//! `src/common/env.c` via `setenvf(..., "%ld", end_time)` where
//! `end_time` is a `time_t`. We parse as `i64`; anything that fails
//! to parse falls back to attempt-count-only mode and is logged at
//! WARN. Absent env-var (e.g. local-mode runs, non-SLURM hosts) is
//! NOT a warning — that's the normal legacy case.
//!
//! # `Instant` vs `SystemTime`
//!
//! [`Instant`] is monotonic; [`SystemTime`] is wallclock. The SLURM
//! deadline is wallclock by nature, but every comparison in
//! [`RetryBudget::should_retry`] happens against the in-process
//! "now". We anchor once at construction: convert the absolute
//! wallclock deadline into a monotonic `Instant` by computing
//! `Instant::now() + (deadline_systime - SystemTime::now())`. Any
//! subsequent wall-clock skew (NTP step, suspend) drifts the
//! deadline by the same delta — acceptable given the 60s safety
//! margin already absorbs minute-scale slop.

use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Default safety margin: a retry pass started this close to the
/// SLURM job-end is skipped even if attempts remain. 60s is a
/// minimum floor — re-injection itself is cheap (push items back into
/// the pool, kick idle workers), but the workers then have to
/// actually re-execute the tasks, which takes longer. 60s prevents
/// the "started a retry that physically can't finish" pathology
/// without leaving so much headroom that legitimate retries get
/// skipped in tight allocations. Not currently configurable; defer
/// to a future change if operators need to override.
pub(crate) const DEFAULT_SAFETY_MARGIN: Duration = Duration::from_secs(60);

/// Single owner of "is the retry budget exhausted?"
///
/// Combines an attempt counter (hard cap) with an optional wallclock
/// deadline (SLURM allocation end). [`Self::should_retry`] returns
/// `true` only when BOTH axes still permit another pass.
#[derive(Debug)]
pub(crate) struct RetryBudget {
    /// Anchored monotonic deadline. `None` means "no wallclock
    /// deadline known" — typical for non-SLURM runs and the
    /// legacy code path. Set at construction only; never mutated.
    job_end: Option<Instant>,
    /// Cushion subtracted from `job_end` before the deadline-check
    /// fires. See [`DEFAULT_SAFETY_MARGIN`].
    safety_margin: Duration,
    /// Hard upper bound on attempts (the legacy
    /// `retry_max_passes`). When `attempts_used == max_passes`,
    /// [`Self::should_retry`] returns false regardless of wallclock.
    max_passes: u32,
    /// Monotonic count of completed retry passes. Bumped by
    /// [`Self::record_attempt`] from the re-injection site in
    /// `primary_drain_check_and_retry`.
    attempts_used: u32,
}

impl RetryBudget {
    /// Construct a budget with no wallclock deadline (attempt-count
    /// only). Used by tests and as a fallback ingredient by
    /// [`Self::from_env_and_legacy`]; not currently called from
    /// production code (the production constructor inlines the
    /// no-deadline shape inside its env-read), so flagged
    /// `allow(dead_code)` until a future caller exposes a
    /// no-env path explicitly.
    #[allow(dead_code)]
    pub(crate) fn new(max_passes: u32, safety_margin: Duration) -> Self {
        Self {
            job_end: None,
            safety_margin,
            max_passes,
            attempts_used: 0,
        }
    }

    /// Production constructor: reads `$SLURM_JOB_END_TIME` once and
    /// anchors the deadline. Absent env-var → legacy mode (no warn).
    /// Parse failure → legacy mode + WARN-level log (operator
    /// signal: SLURM gave us an unexpected format).
    ///
    /// Reads the env-var ONCE; the returned budget caches the
    /// anchored `Instant`. Callers must NOT call this on every
    /// retry-check — it's a startup-time call.
    pub(crate) fn from_env_and_legacy(
        max_passes: u32,
        safety_margin: Duration,
    ) -> Self {
        let job_end = read_slurm_job_end_time();
        Self {
            job_end,
            safety_margin,
            max_passes,
            attempts_used: 0,
        }
    }

    /// True iff both axes still permit another retry pass:
    ///   - `attempts_used < max_passes` (attempt-count axis), and
    ///   - either no deadline OR `now + safety_margin < deadline`.
    pub(crate) fn should_retry(&self) -> bool {
        if self.attempts_used >= self.max_passes {
            return false;
        }
        match self.job_end {
            None => true,
            Some(deadline) => {
                let cutoff = Instant::now() + self.safety_margin;
                cutoff < deadline
            }
        }
    }

    /// Bump the attempt counter. Once a load-bearing piece of the
    /// secondary's primary retry path; post-2026-05-17 the per-
    /// (phase, bucket) counter on
    /// `SecondaryCoordinator::primary_retry_passes_used` owns the
    /// attempt-count axis, and `should_retry()` is consulted only
    /// for its SLURM-wallclock half. Kept on the type so the dual-
    /// axis design stays intact for any future caller that does
    /// want to bump (and so the internal unit tests in this file
    /// retain the API they exercise).
    #[allow(dead_code)]
    pub(crate) fn record_attempt(&mut self) {
        self.attempts_used = self.attempts_used.saturating_add(1);
    }

    /// Test/inspection accessor for the consumed-attempts count.
    /// Production code never reads this — it consults
    /// [`Self::should_retry`] instead. Kept on the type alongside
    /// `record_attempt` for the same dual-axis preservation
    /// rationale; live callers exist in this file's unit tests
    /// and (potentially) future re-injection sites.
    #[allow(dead_code)]
    pub(crate) fn attempts_used(&self) -> u32 {
        self.attempts_used
    }
}

/// Read and anchor `$SLURM_JOB_END_TIME` if present and parseable.
///
/// Returns `None` (no warn) when the env-var is absent — that's the
/// normal legacy path. Returns `None` (WITH warn) when the env-var
/// is set but doesn't parse as `i64` Unix-epoch seconds — that's an
/// operator-visible anomaly.
fn read_slurm_job_end_time() -> Option<Instant> {
    let raw = env::var("SLURM_JOB_END_TIME").ok()?;
    match raw.trim().parse::<i64>() {
        Ok(epoch_secs) => anchor_epoch_seconds(epoch_secs),
        Err(err) => {
            tracing::warn!(
                slurm_job_end_time = %raw,
                error = %err,
                "SLURM_JOB_END_TIME present but not a Unix-epoch \
                 integer; falling back to attempt-count-only retry \
                 budget"
            );
            None
        }
    }
}

/// Anchor an absolute wallclock epoch into a monotonic [`Instant`]
/// using the in-process "now" as the conversion pivot. Returns
/// `None` if the epoch is in the past (wallclock-now already past
/// deadline) — no point keeping a deadline that's already fired,
/// `should_retry` would just keep returning false.
fn anchor_epoch_seconds(epoch_secs: i64) -> Option<Instant> {
    let now_sys = SystemTime::now();
    let now_inst = Instant::now();
    // Negative epoch is nonsense in this context (SLURM never emits
    // pre-1970 times); reject and fall back to legacy.
    let target_sys = if epoch_secs >= 0 {
        UNIX_EPOCH.checked_add(Duration::from_secs(epoch_secs as u64))?
    } else {
        tracing::warn!(
            epoch = epoch_secs,
            "SLURM_JOB_END_TIME is negative; falling back to \
             attempt-count-only retry budget"
        );
        return None;
    };
    match target_sys.duration_since(now_sys) {
        Ok(delta) => Some(now_inst + delta),
        Err(_) => {
            tracing::warn!(
                epoch = epoch_secs,
                "SLURM_JOB_END_TIME is already in the past at \
                 startup; treating retry budget as wallclock-\
                 exhausted (attempt-count gate still applies)"
            );
            // Return an Instant in the past so `should_retry`'s
            // deadline branch trips immediately. Subtracting
            // safety_margin from this in `should_retry` keeps it in
            // the past, so the cutoff < deadline check fails.
            Some(now_inst.checked_sub(Duration::from_secs(1)).unwrap_or(now_inst))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The env-var tests use `SLURM_JOB_END_TIME` directly. Cargo
    // runs tests in-process by default, so two tests that both
    // mutate this env-var would race. We gate env-mutating tests
    // behind a single mutex so each one sees a clean slate.
    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(val: Option<&str>, f: F) {
        // Rust 2024 marks env::{set_var,remove_var} as `unsafe`
        // because parallel-threaded mutation of the process env
        // table is a data race. The `ENV_LOCK` mutex below
        // serialises all callers within this test module so the
        // invariant the unsafe block requires (no other thread
        // touches the env table concurrently) holds. No other test
        // module in this crate touches `SLURM_JOB_END_TIME`.
        let _guard = ENV_LOCK.lock().unwrap();
        let prev = env::var("SLURM_JOB_END_TIME").ok();
        unsafe {
            match val {
                Some(v) => env::set_var("SLURM_JOB_END_TIME", v),
                None => env::remove_var("SLURM_JOB_END_TIME"),
            }
        }
        f();
        unsafe {
            match prev {
                Some(p) => env::set_var("SLURM_JOB_END_TIME", p),
                None => env::remove_var("SLURM_JOB_END_TIME"),
            }
        }
    }

    #[test]
    fn legacy_mode_when_no_env() {
        with_env(None, || {
            let mut b = RetryBudget::from_env_and_legacy(
                2,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(b.should_retry(), "pass 0 of 2 should retry");
            b.record_attempt();
            assert!(b.should_retry(), "pass 1 of 2 should retry");
            b.record_attempt();
            assert!(
                !b.should_retry(),
                "attempt-count exhausted (2 of 2) — no more retries"
            );
        });
    }

    #[test]
    fn deadline_in_future_allows_retry() {
        // Far-future deadline: 1 hour from now.
        let future_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 3600;
        with_env(Some(&future_epoch.to_string()), || {
            let b = RetryBudget::from_env_and_legacy(
                3,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(
                b.should_retry(),
                "future deadline with attempts remaining should permit"
            );
        });
    }

    #[test]
    fn deadline_inside_safety_margin_blocks_retry() {
        // Deadline 10s from now; default safety margin is 60s. So
        // deadline - safety_margin is 50s in the PAST, cutoff is
        // 60s in the future — cutoff > deadline → blocked.
        let near_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 10;
        with_env(Some(&near_epoch.to_string()), || {
            let b = RetryBudget::from_env_and_legacy(
                3,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(
                !b.should_retry(),
                "deadline inside safety margin should block retry \
                 even with attempts left"
            );
        });
    }

    #[test]
    fn deadline_in_past_blocks_retry() {
        // Deadline 1 hour ago.
        let past_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 3600;
        with_env(Some(&past_epoch.to_string()), || {
            let b = RetryBudget::from_env_and_legacy(
                3,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(
                !b.should_retry(),
                "past deadline should block retry"
            );
        });
    }

    #[test]
    fn unparseable_env_falls_back_to_legacy() {
        with_env(Some("not-a-number"), || {
            let mut b = RetryBudget::from_env_and_legacy(
                1,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(b.should_retry(), "legacy fallback, attempt 0 of 1");
            b.record_attempt();
            assert!(
                !b.should_retry(),
                "legacy fallback, attempt 1 of 1 exhausts"
            );
        });
    }

    #[test]
    fn rfc3339_string_falls_back_to_legacy() {
        // SLURM is documented to emit Unix epoch, but some older
        // versions / scontrol output uses RFC3339-ish dates. Verify
        // we cleanly fall back rather than misparsing.
        with_env(Some("2099-01-01T00:00:00"), || {
            let mut b = RetryBudget::from_env_and_legacy(
                1,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(b.should_retry(), "legacy fallback on RFC3339 input");
            b.record_attempt();
            assert!(!b.should_retry());
        });
    }

    #[test]
    fn zero_max_passes_never_retries() {
        with_env(None, || {
            let b = RetryBudget::from_env_and_legacy(
                0,
                DEFAULT_SAFETY_MARGIN,
            );
            assert!(
                !b.should_retry(),
                "max_passes=0 disables retry entirely"
            );
        });
    }

    #[test]
    fn record_attempt_saturates() {
        let mut b = RetryBudget::new(u32::MAX, DEFAULT_SAFETY_MARGIN);
        for _ in 0..3 {
            b.record_attempt();
        }
        assert_eq!(b.attempts_used(), 3);
    }

    #[test]
    fn new_constructor_sets_no_deadline() {
        // The plain `new` path is used by tests that don't want
        // env-coupling. Verify it produces a budget that behaves
        // identically to legacy mode.
        let mut b = RetryBudget::new(2, DEFAULT_SAFETY_MARGIN);
        assert!(b.should_retry());
        b.record_attempt();
        assert!(b.should_retry());
        b.record_attempt();
        assert!(!b.should_retry());
    }
}
