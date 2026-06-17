//! Per-secondary operational-loop health snapshot (#589).
//!
//! # Single concern
//!
//! Compute the loop-health fields the secondary stamps onto its 5-minute
//! `SecondaryResourceSampleRecord` broadcast — the iter-rate, the
//! WALL-CLOCK-TIME dominant arm (name + time-share + ms/s, including the
//! `idle` select!-wait pseudo-arm), and the oldest-unACKed age — from raw
//! sources that already exist on the secondary: the operational-loop's
//! `arm_stats` counters + per-arm/idle TIME accumulators and the
//! buffered-report-replay queue's oldest-unACKed age.
//!
//! Necessary-but-not-sufficient is the #586 lesson: host CPU%/mem/swap
//! (the #575 axis) was ~15% while the oploop spent 52-62% of its
//! iterations on `mem_check` — the operator's view said HEALTHY while the
//! coordinator was starved. Loop-health is the missing axis. Host health
//! ≠ loop health.
//!
//! # Boundary
//!
//! - This module OWNS: the prior-snapshot tracking (so the deltas are over
//!   the emit window, not lifetime totals), the dominant-arm
//!   max-by-TIME-delta selection (including the `idle` pseudo-arm), and the
//!   rate/share/ms-per-sec arithmetic.
//! - This module does NOT OWN: the arm-stats source (the oploop's own
//!   `arm_stats.record(arm_id)` + `begin_select` timing calls — read here
//!   via the existing `ArmStatsSnapshot` accessor, no new instrumentation),
//!   the
//!   report-replay queue (the caller passes in the already-computed
//!   `max_unacked_for_secs`), the emit cadence (the operational
//!   `select!`'s 5-minute interval wakes the caller), or the wire
//!   stamping (`member_gen` / `emitted_at_ms` are layered on by the
//!   resource buffer assembler, exactly as today).
//!
//! # Cold-start contract
//!
//! On the very first emit of a freshly-spawned loop there is no prior
//! snapshot — the tracker has nothing to delta against. The returned
//! [`LoopHealthFields`] is `Default` (all zeros / empty string), which
//! the wire layer's `skip_serializing_if` elides and the observer's
//! 25%-threshold-against-zero gate treats as "no signal yet". On the
//! next emit 5 minutes later the tracker has both endpoints and emits
//! real numbers.

use crate::oploop_instrumentation::{IDLE_ARM_NAME, OpLoopArmStats};
use dynrunner_protocol_primary_secondary::SecondaryResourceSampleRecord;
use std::time::Instant;

/// The loop-health values the secondary stamps onto its 5-minute
/// [`SecondaryResourceSampleRecord`] broadcast. Pure data — no
/// authority, no timer, no I/O — so the unit tests can drive the
/// dominant-arm + iter-rate math without a clock or a runtime.
///
/// `Default` is the wire-compat / cold-start sentinel: every field
/// zero / empty matches the `skip_serializing_if` predicates on
/// [`SecondaryResourceSampleRecord`], so a freshly-spawned loop's
/// first emit costs zero wire bytes for the loop-health axis.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoopHealthFields {
    /// Iteration rate of the operational `select!` loop over the emit
    /// window, in milli-iters-per-second (`12_500` = 12.5 iter/s).
    /// Sum of every arm's delta divided by elapsed seconds, ×1000 for
    /// sub-iter resolution against an integer wire. `0` is the cold-
    /// start sentinel.
    pub oploop_iters_per_sec_milli: u64,
    /// The name of the single hottest arm by WALL-CLOCK TIME delta over
    /// the window — a real `select!` arm OR the
    /// [`crate::oploop_instrumentation::IDLE_ARM_NAME`] pseudo-arm (the
    /// select!-wait). Empty when there is no prior snapshot OR the window's
    /// total time delta is zero (a silent loop). The observer's max-by-pct
    /// fleet aggregation passes over the empty case. TIME, not COUNT: a
    /// lightly-loaded loop reports `idle`; an overloaded loop reports its
    /// slowest arm.
    pub dominant_arm_name: String,
    /// The dominant arm's share of the window's total wall-clock time
    /// (`idle + Σ arm-time`), in milli-percent (`55_000` = 55.0%). `0`
    /// when [`Self::dominant_arm_name`] is empty.
    pub dominant_arm_pct_milli: u32,
    /// The dominant arm's body-time per wall-clock second over the window,
    /// in milliseconds-per-second (`425` = 425ms of each second spent in
    /// this arm). `0` when [`Self::dominant_arm_name`] is empty.
    pub dominant_arm_time_ms_per_sec: u64,
    /// The longest age of any retained confirmable report on the
    /// secondary's buffered-report-replay queue at emit time, in
    /// seconds. `0` when the queue is empty (no unacked) — the
    /// steady-state.
    pub max_unacked_for_secs: u32,
}

impl LoopHealthFields {
    /// Stamp these fields onto a [`SecondaryResourceSampleRecord`]
    /// being assembled by the resource buffer. Single seam so the
    /// buffer's `aggregate` knows ONLY which fields to forward —
    /// it never inspects the loop-health values, never branches on
    /// them, and never needs to grow when a new loop-health field
    /// lands here.
    pub fn stamp_onto(self, record: &mut SecondaryResourceSampleRecord) {
        record.oploop_iters_per_sec_milli = self.oploop_iters_per_sec_milli;
        record.dominant_arm_name = self.dominant_arm_name;
        record.dominant_arm_pct_milli = self.dominant_arm_pct_milli;
        record.dominant_arm_time_ms_per_sec = self.dominant_arm_time_ms_per_sec;
        record.max_unacked_for_secs = self.max_unacked_for_secs;
    }
}

/// One arm-stats endpoint the tracker holds between emits. Owns ONLY
/// what the next emit's delta needs: the iter counter (for the iter-rate
/// axis), the per-arm BODY-time nanos + the IDLE nanos (the time axis the
/// dominant selection deltas, named so a re-spawn that reordered the arm
/// set still produces correct deltas-by-name), and the captured instant.
#[derive(Debug, Clone)]
struct ArmEndpoint {
    iter: u64,
    /// Per-arm cumulative body-time nanos `(name, nanos)`, arm-id order.
    arm_nanos: Vec<(&'static str, u64)>,
    /// Cumulative idle (select!-wait) nanos.
    idle_nanos: u64,
    at: Instant,
}

/// Loop-local tracker the operational emit arm instantiates ONCE
/// alongside the resource buffer and carries across emits.
///
/// Holds the prior arm-stats endpoint (`None` until the first emit
/// runs and seeds it). The unacked-age input is NOT carried — it is
/// instantaneous at the emit instant and computed by the caller, so
/// the tracker stays single-concern (arm-stats delta arithmetic).
#[derive(Debug, Default)]
pub struct LoopHealthTracker {
    prior: Option<ArmEndpoint>,
}

impl LoopHealthTracker {
    /// Fresh tracker — the next `compute` call will seed the prior
    /// endpoint and return `Default` (cold-start contract).
    pub fn new() -> Self {
        Self::default()
    }

    /// Build the [`LoopHealthFields`] for the current emit by deltas
    /// against the prior endpoint.
    ///
    /// First call: there is no prior, so the tracker stores the
    /// current endpoint and returns `Default` (the wire-elision /
    /// no-signal sentinel). Second and subsequent calls: the prior
    /// is replaced with the current endpoint AFTER the deltas have
    /// been computed against it — so successive emits are
    /// back-to-back windows with no overlap and no gap.
    ///
    /// `max_unacked_for_secs` is a pass-through — the caller already
    /// computes the queue's oldest-unACKed age at emit time and the
    /// tracker just folds it into the returned record.
    pub fn compute(
        &mut self,
        arm_stats: &OpLoopArmStats,
        max_unacked_for_secs: u32,
        now: Instant,
    ) -> LoopHealthFields {
        let snap = arm_stats.snapshot();
        let current = ArmEndpoint {
            iter: snap.iter,
            arm_nanos: snap.arm_nanos.clone(),
            idle_nanos: snap.idle_nanos,
            at: now,
        };
        let Some(prior) = self.prior.take() else {
            // Cold start: seed the prior and emit the wire-elision
            // sentinel. `max_unacked_for_secs` is still reported (it is
            // not a delta — instantaneous at emit), so a freshly-
            // spawned loop with a stuck report from t0 reports the
            // queue age on its very first emit. The two loop-health
            // axes are intentionally independent.
            self.prior = Some(current);
            return LoopHealthFields {
                max_unacked_for_secs,
                ..Default::default()
            };
        };

        let elapsed = current.at.saturating_duration_since(prior.at);
        let elapsed_secs = elapsed.as_secs_f64();
        let delta_iter = current.iter.saturating_sub(prior.iter);

        // iters/sec ×1000 (milli-iters). Guard the elapsed≤0 corner —
        // a same-instant re-compute (test seam) reports zero rate
        // rather than dividing by zero. saturating_mul on the u128
        // numerator is overkill but keeps the integer pipe overflow-
        // safe at pathological iter counts.
        let oploop_iters_per_sec_milli = if elapsed_secs > 0.0 {
            ((delta_iter as f64 * 1000.0) / elapsed_secs)
                .round()
                .min(u64::MAX as f64)
                .max(0.0) as u64
        } else {
            0
        };

        // Dominant arm by WALL-CLOCK TIME, including the `idle`
        // (select!-wait) pseudo-arm. Delta the per-arm body-time BY NAME
        // (not by index) so a re-instantiated arm set whose order changed
        // across the window still deltas correctly; idle is a single
        // scalar delta. The `idle` pseudo-arm competes with the real arms
        // uniformly: light load ⇒ idle dominates (healthy); overload ⇒ a
        // slow body dominates over a small idle (the wedge signature).
        let prior_by_name: std::collections::HashMap<&'static str, u64> =
            prior.arm_nanos.iter().copied().collect();
        let idle_delta = current.idle_nanos.saturating_sub(prior.idle_nanos);
        // Combined (name, time-delta-nanos) over {idle} ∪ {real arms}.
        let candidates = std::iter::once((IDLE_ARM_NAME, idle_delta)).chain(
            current.arm_nanos.iter().map(|(name, cur)| {
                let prev = prior_by_name.get(name).copied().unwrap_or(0);
                (*name, cur.saturating_sub(prev))
            }),
        );
        let mut total_delta: u64 = 0;
        let mut max_delta: u64 = 0;
        let mut max_name: &str = "";
        for (name, d) in candidates {
            total_delta = total_delta.saturating_add(d);
            if d > max_delta {
                max_delta = d;
                max_name = name;
            }
        }
        // Share in milli-percent + body-time-per-second (ms/s). A zero
        // total time delta is a silent window (no time accounted —
        // pre-instrumentation arm-stats or a same-instant recompute):
        // empty name + zero pct + zero rate.
        let (dominant_arm_name, dominant_arm_pct_milli, dominant_arm_time_ms_per_sec) =
            if total_delta > 0 && !max_name.is_empty() {
                // (max_delta * 100_000) / total_delta — u128 keeps the
                // numerator overflow-safe at any nanos magnitude.
                let pct = ((max_delta as u128) * 100_000u128) / (total_delta as u128);
                // ms/s = (max_delta nanos / 1e6 ms) / elapsed_secs.
                let ms_per_sec = if elapsed_secs > 0.0 {
                    ((max_delta as f64 / 1_000_000.0) / elapsed_secs)
                        .round()
                        .min(u64::MAX as f64)
                        .max(0.0) as u64
                } else {
                    0
                };
                (max_name.to_string(), pct.min(100_000) as u32, ms_per_sec)
            } else {
                (String::new(), 0, 0)
            };

        // Advance the prior to the current endpoint AFTER the deltas
        // have been read — so the NEXT emit's window starts where this
        // one ended (no overlap, no gap).
        self.prior = Some(current);

        LoopHealthFields {
            oploop_iters_per_sec_milli,
            dominant_arm_name,
            dominant_arm_pct_milli,
            dominant_arm_time_ms_per_sec,
            max_unacked_for_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// First call seeds the prior and returns the cold-start sentinel
    /// (the wire-elision contract). The max-unacked-for-secs input is
    /// still passed through.
    #[test]
    fn first_call_returns_default_with_unacked_passthrough() {
        let arm_stats = OpLoopArmStats::new(&["a", "b"], 0);
        arm_stats.record(0);
        arm_stats.record(1);
        let mut tracker = LoopHealthTracker::new();
        let fields = tracker.compute(&arm_stats, 42, Instant::now());
        // Cold start: arm-stats delta values are zero (no prior), but
        // max_unacked_for_secs (instantaneous, not a delta) is reported.
        assert_eq!(fields.oploop_iters_per_sec_milli, 0);
        assert!(fields.dominant_arm_name.is_empty());
        assert_eq!(fields.dominant_arm_pct_milli, 0);
        assert_eq!(fields.max_unacked_for_secs, 42);
    }

    /// Second call computes iter-rate from the delta against the prior
    /// endpoint over the elapsed seconds.
    #[test]
    fn iters_per_sec_milli_from_delta_over_elapsed_secs() {
        let arm_stats = OpLoopArmStats::new(&["a"], 0);
        for _ in 0..10 {
            arm_stats.record(0);
        }
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        let _seed = tracker.compute(&arm_stats, 0, t0);
        // 90 more iters over 10s ⇒ 9 iter/s ⇒ 9_000 milli-iter/s.
        for _ in 0..90 {
            arm_stats.record(0);
        }
        let fields = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(10));
        assert_eq!(fields.oploop_iters_per_sec_milli, 9_000);
    }

    /// Dominant arm is selected by the largest WALL-CLOCK-TIME delta and
    /// the share is rendered in milli-percent + ms/s. Inject body-time
    /// directly via the low-level accumulator (deterministic, no sleep).
    #[test]
    fn dominant_arm_pct_milli_is_share_of_total_time() {
        let arm_stats = OpLoopArmStats::new(&["inbox", "mem_check", "other"], 0);
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        let _seed = tracker.compute(&arm_stats, 0, t0);
        // Window: inbox 10ms body, mem_check 80ms body, other 10ms body,
        // idle 0 ⇒ total 100ms; mem_check is 80% by TIME.
        arm_stats.record_arm_time(0, Duration::from_millis(10));
        arm_stats.record_arm_time(1, Duration::from_millis(80));
        arm_stats.record_arm_time(2, Duration::from_millis(10));
        let fields = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(60));
        assert_eq!(fields.dominant_arm_name, "mem_check");
        // 80ms/100ms = 80% = 80_000 milli-percent.
        assert_eq!(fields.dominant_arm_pct_milli, 80_000);
        // 80ms over 60s window ⇒ round(80/60) = 1 ms/s.
        assert_eq!(fields.dominant_arm_time_ms_per_sec, 1);
    }

    /// Light load: the loop spends almost all its time WAITING in the
    /// select! — the `idle` pseudo-arm dominates BY TIME even though a
    /// timer arm FIRED far more often. This is the count-vs-time
    /// distinction: a frequent-but-fast arm must NOT read as dominant.
    #[test]
    fn idle_dominates_under_light_load_despite_a_high_count_fast_arm() {
        let arm_stats = OpLoopArmStats::new(&["inbox", "mem_check"], 0);
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        let _seed = tracker.compute(&arm_stats, 0, t0);
        // mem_check FIRES 1000× but each body is trivially short (total
        // 50ms of body time); the loop was idle 950ms of the 1s window.
        for _ in 0..1000 {
            arm_stats.record(1);
        }
        arm_stats.record_arm_time(1, Duration::from_millis(50));
        arm_stats.record_idle(Duration::from_millis(950));
        let fields = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(1));
        // By COUNT mem_check would win (1000 fires); by TIME idle wins.
        assert_eq!(fields.dominant_arm_name, "idle");
        // 950ms / 1000ms = 95%.
        assert_eq!(fields.dominant_arm_pct_milli, 95_000);
        // idle 950ms over a 1s window ⇒ 950 ms/s.
        assert_eq!(fields.dominant_arm_time_ms_per_sec, 950);
    }

    /// Overload: a RARE arm (fires few times) whose body is SLOW
    /// dominates BY TIME over a frequent fast arm — the wedge signature
    /// that the count axis would have hidden.
    #[test]
    fn slow_low_count_arm_dominates_by_time_over_a_fast_high_count_arm() {
        let arm_stats = OpLoopArmStats::new(&["inbox", "mem_check"], 0);
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        let _seed = tracker.compute(&arm_stats, 0, t0);
        // inbox fires 2× but each body is a long 400ms blocking pass
        // (800ms total); mem_check fires 500× but only 100ms total; idle
        // 100ms. By COUNT mem_check wins; by TIME inbox wins (overload on
        // the data path's slow body).
        for _ in 0..2 {
            arm_stats.record(0);
        }
        for _ in 0..500 {
            arm_stats.record(1);
        }
        arm_stats.record_arm_time(0, Duration::from_millis(800));
        arm_stats.record_arm_time(1, Duration::from_millis(100));
        arm_stats.record_idle(Duration::from_millis(100));
        let fields = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(1));
        assert_eq!(fields.dominant_arm_name, "inbox");
        // 800ms / 1000ms = 80%.
        assert_eq!(fields.dominant_arm_pct_milli, 80_000);
        assert_eq!(fields.dominant_arm_time_ms_per_sec, 800);
    }

    /// Successive windows do not overlap: a SECOND post-seed emit reads
    /// deltas against the FIRST post-seed (the tracker advanced its
    /// prior).
    #[test]
    fn successive_emits_are_back_to_back_windows() {
        let arm_stats = OpLoopArmStats::new(&["a"], 0);
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        tracker.compute(&arm_stats, 0, t0); // seed
        for _ in 0..50 {
            arm_stats.record(0);
        }
        let f1 = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(10));
        // 50 / 10 = 5 iter/s = 5_000 milli.
        assert_eq!(f1.oploop_iters_per_sec_milli, 5_000);
        for _ in 0..200 {
            arm_stats.record(0);
        }
        let f2 = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(20));
        // Window 2 sees only the 200 new iters / 10s = 20 iter/s; NOT
        // the lifetime 250/20 = 12.5. Back-to-back windows are the
        // operator-meaningful cadence.
        assert_eq!(f2.oploop_iters_per_sec_milli, 20_000);
    }

    /// A silent loop (no time accounted between seed and emit — neither
    /// idle nor any arm body) reports an empty dominant-arm name and zero
    /// pct/rate — a wire-eliding result.
    #[test]
    fn silent_loop_reports_empty_dominant_and_zero_pct() {
        let arm_stats = OpLoopArmStats::new(&["a", "b"], 0);
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        tracker.compute(&arm_stats, 0, t0);
        let fields = tracker.compute(&arm_stats, 0, t0 + Duration::from_secs(10));
        assert!(fields.dominant_arm_name.is_empty());
        assert_eq!(fields.dominant_arm_pct_milli, 0);
        assert_eq!(fields.dominant_arm_time_ms_per_sec, 0);
        assert_eq!(fields.oploop_iters_per_sec_milli, 0);
    }

    /// Same-instant re-compute (test seam — elapsed ≤ 0) reports zero
    /// rate rather than dividing by zero.
    #[test]
    fn same_instant_re_compute_does_not_divide_by_zero() {
        let arm_stats = OpLoopArmStats::new(&["a"], 0);
        let mut tracker = LoopHealthTracker::new();
        let t0 = Instant::now();
        tracker.compute(&arm_stats, 0, t0);
        for _ in 0..5 {
            arm_stats.record(0);
        }
        let fields = tracker.compute(&arm_stats, 0, t0); // SAME instant
        assert_eq!(fields.oploop_iters_per_sec_milli, 0);
    }
}
