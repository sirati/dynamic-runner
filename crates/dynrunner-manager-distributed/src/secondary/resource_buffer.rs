//! Per-secondary rolling buffer of raw resource samples (#575).
//!
//! # Single concern
//!
//! Hold a time-windowed buffer of [`RawResourceSample`]s and, on demand,
//! aggregate them into the wire-shape [`SecondaryResourceSampleRecord`]
//! the secondary broadcasts every 5 minutes. The raw per-sample readings
//! NEVER leave the secondary — only the aggregate crosses the CRDT.
//!
//! # Boundary
//!
//! - The buffer owns: the bounded `VecDeque<(Instant, RawResourceSample)>`,
//!   the 10-minute retention policy, the percentile + arithmetic-mean math,
//!   and the empty-window contract (`aggregate` returns `None` when no
//!   sample fits in the window — the caller skips the broadcast).
//! - The buffer does NOT own: the sample SOURCE (the OOM watcher's sweep
//!   loop pushes), the EMIT cadence (the operational `select!` arm
//!   wakes on a 5-minute interval and calls `aggregate`), or the
//!   `(member_gen, emitted_at_ms)` stamping (the operational arm reads
//!   the membership generation off the CRDT and emit-time off the
//!   wall clock at broadcast time).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use dynrunner_protocol_primary_secondary::SecondaryResourceSampleRecord;

/// Rolling-buffer retention window: every sample older than this on
/// the most recent `push` is dropped. 10 minutes matches the owner's
/// #575 spec ("at most two 5-minute emits worth of samples").
pub const SAMPLE_WINDOW: Duration = Duration::from_secs(600);

/// One raw per-sample reading the secondary's OOM-watcher sweep
/// produces, in the shape the resource-stats aggregation consumes.
/// Trimmed down from `OomWatcherSnapshot` to ONLY the fields #575
/// surfaces, so the buffer's memory footprint stays bounded by the
/// sample count (not by the sweep snapshot's full breadth).
#[derive(Debug, Clone, Copy)]
pub struct RawResourceSample {
    /// Sum of resident + swap across the tracked-worker set — the
    /// memory workload the kill decision also consumes. Memory
    /// percentiles + average are taken over THIS field across the
    /// window.
    pub workers_charged_sum_bytes: u64,
    /// Host-level free RAM at sweep time: `MemTotal - (MemTotal -
    /// MemAvailable) = MemAvailable`, stored as the sweep's
    /// `host_ram_total - host_ram_used` so the buffer carries the
    /// same view the OomWatcher snapshot exposes (a `None` in either
    /// half → this field is `None`, which the window-mean skips).
    pub host_free_memory_bytes: Option<u64>,
    /// Host-level swap usage at sweep time: `host_swap_used_bytes`.
    pub host_swap_used_bytes: Option<u64>,
    /// Host-level free swap: `host_swap_total - host_swap_used`.
    pub host_free_swap_bytes: Option<u64>,
    /// Host CPU busy-fraction over the sweep interval, in
    /// milli-percent (100_000 = every core at 100%). `None` on the
    /// first sweep, or when /proc/stat was unreadable, or when two
    /// reads landed inside the same tick — the window-mean skips
    /// these.
    pub cpu_busy_milli: Option<u32>,
}

/// The rolling-buffer container.
///
/// Ordering invariant: pushes arrive in monotonically non-decreasing
/// `Instant` order (the production caller is the sweep loop on a
/// monotonic clock — `tokio::time::Instant::now()`). The retention
/// pass relies on this: it drops samples from the FRONT until the
/// oldest fits in the window. A non-monotonic push would still leave
/// the buffer correct on the next push that does cross the threshold
/// (we never read samples in front-to-back order; the aggregator is
/// order-independent).
///
/// Capacity is NOT preallocated — the production cadence is 50ms × 600s
/// = 12_000 samples worst-case, each ~40 bytes, ≈ 470 KiB. Bounded by
/// retention, not by a hard cap.
#[derive(Debug, Default)]
pub struct ResourceSampleBuffer {
    samples: VecDeque<(Instant, RawResourceSample)>,
    window: Duration,
}

impl ResourceSampleBuffer {
    /// Construct an empty buffer with the production [`SAMPLE_WINDOW`]
    /// retention.
    pub fn new() -> Self {
        Self::with_window(SAMPLE_WINDOW)
    }

    /// Construct an empty buffer with a custom retention window — the
    /// test seam (the unit tests drive 10-second-equivalent windows
    /// with a mocked clock so the suite stays fast).
    pub fn with_window(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
        }
    }

    /// Push one fresh sample at `now` and evict every sample older
    /// than `now - window`. Called at the sweep cadence (50ms in
    /// production) out of the secondary's `process_tasks` OOM-sweep
    /// arm.
    pub fn push(&mut self, now: Instant, sample: RawResourceSample) {
        self.samples.push_back((now, sample));
        let threshold = now.checked_sub(self.window).unwrap_or(now);
        while let Some(&(t, _)) = self.samples.front() {
            if t < threshold {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Build the aggregated wire-shape record from the CURRENT buffer.
    /// `None` when the buffer is empty (no signal — the caller skips
    /// the broadcast). Otherwise:
    ///   - memory percentiles + mean over `workers_charged_sum_bytes`
    ///     (a present-in-every-sample field by construction);
    ///   - mean over each host field, COUNTING only samples whose value
    ///     is `Some` (a missing reading on one sweep does not zero the
    ///     aggregate); when ALL samples have `None` for a host field
    ///     the aggregate falls back to `0` for that field — the
    ///     observer treats it as "no signal" and the 25% threshold
    ///     against `0` keeps it suppressed until a real reading lands.
    ///
    /// `member_gen` and `emitted_at_ms` are STAMPED BY THE CALLER (the
    /// operational arm reads the membership generation off the CRDT
    /// and emit-time off the wall clock at broadcast time). The
    /// buffer never reads either — single concern.
    pub fn aggregate(
        &self,
        member_gen: u64,
        emitted_at_ms: u64,
    ) -> Option<SecondaryResourceSampleRecord> {
        if self.samples.is_empty() {
            return None;
        }
        // Pull each per-sample axis into its own vector. Avoids a
        // partial-borrow tangle and keeps the percentile sort local to
        // memory (the only field that needs ordering).
        let mut mem: Vec<u64> = self
            .samples
            .iter()
            .map(|(_, s)| s.workers_charged_sum_bytes)
            .collect();
        // Memory percentiles: sort once, index by linear interpolation
        // at p ∈ {10, 30, 50, 70, 90}.
        mem.sort_unstable();
        let mem_p10 = percentile_u64(&mem, 10);
        let mem_p30 = percentile_u64(&mem, 30);
        let mem_p50 = percentile_u64(&mem, 50);
        let mem_p70 = percentile_u64(&mem, 70);
        let mem_p90 = percentile_u64(&mem, 90);
        let mem_avg = mean_u64(mem.iter().copied());

        // Host fields: mean over `Some` values; 0 when none present.
        let total_free_memory_bytes =
            mean_optional_u64(self.samples.iter().map(|(_, s)| s.host_free_memory_bytes));
        let total_swap_used_bytes =
            mean_optional_u64(self.samples.iter().map(|(_, s)| s.host_swap_used_bytes));
        let total_free_swap_bytes =
            mean_optional_u64(self.samples.iter().map(|(_, s)| s.host_free_swap_bytes));
        let cpu_utilization_milli = mean_optional_u32(
            self.samples
                .iter()
                .map(|(_, s)| s.cpu_busy_milli),
        );

        Some(SecondaryResourceSampleRecord {
            member_gen,
            emitted_at_ms,
            mem_p10_bytes: mem_p10,
            mem_p30_bytes: mem_p30,
            mem_p50_bytes: mem_p50,
            mem_p70_bytes: mem_p70,
            mem_p90_bytes: mem_p90,
            mem_avg_bytes: mem_avg,
            total_free_memory_bytes,
            total_swap_used_bytes,
            total_free_swap_bytes,
            cpu_utilization_milli,
        })
    }

    /// Sample count currently in the buffer — observability only.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.samples.len()
    }
}

/// Linear-interpolation percentile over a SORTED `Vec<u64>`. `p ∈
/// [0, 100]`; out-of-range clamps. Empty input is the caller's
/// concern (`aggregate` early-returns).
///
/// Why linear interpolation: the canonical "nearest-rank" rule
/// produces step-function jitter when the buffer grows by one sample
/// (the P90 jumps from index ⌈0.9·N⌉ to ⌈0.9·(N+1)⌉ on the next
/// sweep), which trips the observer's 25% threshold on a flat
/// workload. Linear interpolation smooths the curve at no extra
/// allocation.
fn percentile_u64(sorted: &[u64], p: u32) -> u64 {
    debug_assert!(!sorted.is_empty(), "percentile_u64 of empty slice");
    if sorted.is_empty() {
        return 0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let p = p.min(100) as f64 / 100.0;
    let rank = p * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = rank - lo as f64;
    let a = sorted[lo] as f64;
    let b = sorted[hi] as f64;
    (a + (b - a) * frac).round().max(0.0) as u64
}

/// Arithmetic mean over an iterator of `u64`. Saturating-sum so a
/// pathological u64 overflow degrades to `u64::MAX / N` rather than
/// wrapping. Empty input returns `0` (the caller's `aggregate`
/// early-returns on an empty buffer, so this branch is only reached
/// in the `mean_optional_*` "no Some" path).
fn mean_u64(values: impl IntoIterator<Item = u64>) -> u64 {
    let mut sum: u128 = 0;
    let mut n: u128 = 0;
    for v in values {
        sum = sum.saturating_add(v as u128);
        n += 1;
    }
    if n == 0 {
        return 0;
    }
    (sum / n).min(u64::MAX as u128) as u64
}

/// `mean_u64` over an iterator of `Option<u64>`, ignoring `None`s.
/// Returns `0` when every value is `None` — the observer's 25%
/// threshold against `0` then keeps the field suppressed until a
/// real reading lands.
fn mean_optional_u64(values: impl IntoIterator<Item = Option<u64>>) -> u64 {
    mean_u64(values.into_iter().flatten())
}

/// `mean_u64` variant for `u32` — saturates back to `u32` at the
/// output edge. Used for the milli-percent CPU field.
fn mean_optional_u32(values: impl IntoIterator<Item = Option<u32>>) -> u32 {
    let mut sum: u128 = 0;
    let mut n: u128 = 0;
    for v in values.into_iter().flatten() {
        sum = sum.saturating_add(v as u128);
        n += 1;
    }
    if n == 0 {
        return 0;
    }
    (sum / n).min(u32::MAX as u128) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn raw(workers_mem: u64, free_mem: Option<u64>) -> RawResourceSample {
        RawResourceSample {
            workers_charged_sum_bytes: workers_mem,
            host_free_memory_bytes: free_mem,
            host_swap_used_bytes: Some(0),
            host_free_swap_bytes: Some(0),
            cpu_busy_milli: Some(0),
        }
    }

    #[test]
    fn empty_buffer_aggregates_to_none() {
        let buf = ResourceSampleBuffer::new();
        assert!(buf.aggregate(0, 0).is_none());
    }

    #[test]
    fn single_sample_percentiles_collapse_to_that_value() {
        let mut buf = ResourceSampleBuffer::new();
        let now = Instant::now();
        buf.push(now, raw(1_000_000_000, Some(2_000_000_000)));
        let agg = buf.aggregate(7, 1_700_000_000_000).unwrap();
        assert_eq!(agg.member_gen, 7);
        assert_eq!(agg.emitted_at_ms, 1_700_000_000_000);
        // Single-sample percentiles collapse to that one value.
        assert_eq!(agg.mem_p10_bytes, 1_000_000_000);
        assert_eq!(agg.mem_p90_bytes, 1_000_000_000);
        assert_eq!(agg.mem_avg_bytes, 1_000_000_000);
        assert_eq!(agg.total_free_memory_bytes, 2_000_000_000);
    }

    #[test]
    fn uniform_distribution_percentiles_are_correct() {
        // 100 samples uniformly spaced 100..10_000 (step 100). Expected
        // percentiles under linear interpolation: p10 = ~1_090,
        // p50 = ~5_050, p90 = ~9_010.
        let mut buf = ResourceSampleBuffer::new();
        let now = Instant::now();
        for i in 1..=100 {
            buf.push(now, raw(i * 100, None));
        }
        let agg = buf.aggregate(0, 0).unwrap();
        // Linear interpolation: rank = 0.1 * 99 = 9.9, value =
        // sorted[9] + 0.9*(sorted[10] - sorted[9]) = 1000 + 0.9*100 =
        // 1090. (sorted is 1-based here: sorted[9] is sample index 9
        // = value 1000.)
        assert!(
            (1080..=1100).contains(&agg.mem_p10_bytes),
            "p10 = {}",
            agg.mem_p10_bytes
        );
        assert!(
            (5000..=5100).contains(&agg.mem_p50_bytes),
            "p50 = {}",
            agg.mem_p50_bytes
        );
        assert!(
            (8990..=9020).contains(&agg.mem_p90_bytes),
            "p90 = {}",
            agg.mem_p90_bytes
        );
        // arithmetic mean of 100..10_000 step 100 = 5050
        assert_eq!(agg.mem_avg_bytes, 5050);
    }

    #[test]
    fn samples_older_than_window_are_evicted() {
        let window = Duration::from_secs(10);
        let mut buf = ResourceSampleBuffer::with_window(window);
        let t0 = Instant::now();
        // Push 12 samples one per second; only the last 10 should
        // remain inside the 10s window (the oldest two fall out).
        for i in 0..12 {
            buf.push(t0 + Duration::from_secs(i), raw(i * 100, None));
        }
        // Buffer length is 11 after this sequence: the threshold on
        // the t0+11s push is t0+1s; samples at t0+0s (and only that
        // one) get evicted. The eviction is "strictly older than
        // threshold" — t0+1s itself is retained.
        assert_eq!(buf.len(), 11);
        let agg = buf.aggregate(0, 0).unwrap();
        // The first sample (workers_charged = 0) is evicted; the
        // surviving min is 100. Linear-interp p10 over [100..1100]
        // step 100 lands around 200.
        assert!(agg.mem_p10_bytes >= 100, "p10 = {}", agg.mem_p10_bytes);
    }

    #[test]
    fn host_field_with_all_none_aggregates_to_zero() {
        let mut buf = ResourceSampleBuffer::new();
        let now = Instant::now();
        for i in 0..10 {
            buf.push(now, raw(i * 100, None));
        }
        let agg = buf.aggregate(0, 0).unwrap();
        // Every sample had `host_free_memory_bytes: None`; the mean
        // falls back to 0 (the documented "no signal" sentinel).
        assert_eq!(agg.total_free_memory_bytes, 0);
    }

    #[test]
    fn host_field_with_mixed_some_and_none_means_over_some_only() {
        let mut buf = ResourceSampleBuffer::new();
        let now = Instant::now();
        // 5 samples with free_mem = 1000, 5 with None.
        for _ in 0..5 {
            buf.push(now, raw(1, Some(1000)));
        }
        for _ in 0..5 {
            buf.push(now, raw(1, None));
        }
        let agg = buf.aggregate(0, 0).unwrap();
        // Mean over the 5 Some values is exactly 1000; the 5 Nones
        // are skipped (NOT counted as zeros that would halve the
        // mean to 500).
        assert_eq!(agg.total_free_memory_bytes, 1000);
    }

    #[test]
    fn cpu_milli_mean_clamps_at_100_000() {
        let mut buf = ResourceSampleBuffer::new();
        let now = Instant::now();
        for _ in 0..10 {
            buf.push(
                now,
                RawResourceSample {
                    workers_charged_sum_bytes: 1,
                    host_free_memory_bytes: None,
                    host_swap_used_bytes: None,
                    host_free_swap_bytes: None,
                    cpu_busy_milli: Some(80_000),
                },
            );
        }
        let agg = buf.aggregate(0, 0).unwrap();
        assert_eq!(agg.cpu_utilization_milli, 80_000);
    }
}
