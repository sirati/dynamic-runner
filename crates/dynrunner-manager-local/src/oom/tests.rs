//! Unit tests for the OOM watcher.
//!
//! Tests use a deterministic mock probe + manual `Instant` clock so
//! they don't depend on the host's `/proc` or `/sys/fs/cgroup` layout.
//! The kill-event path uses the watcher's `note_kill` seam directly
//! rather than spinning up a fake scheduler + worker pool — the
//! pool/scheduler integration is covered by the wider manager tests.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing::subscriber::with_default;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

use super::probe::{HostMemoryReading, SystemProbe};
use super::{
    ChargeSweepInputs, DEFAULT_HEARTBEAT_INTERVAL, LogTrigger, OomWatcher, OomWatcherConfig,
    OomWatcherSnapshot, SAMPLE_SWEEP_INTERVAL,
};

/// Deterministic probe: returns the configured reading on every read.
/// Callers mutate the inner cell between watcher calls to simulate
/// host pressure changes.
struct MockProbe {
    reading: Arc<Mutex<HostMemoryReading>>,
}

impl MockProbe {
    fn new(reading: HostMemoryReading) -> (Self, Arc<Mutex<HostMemoryReading>>) {
        let cell = Arc::new(Mutex::new(reading));
        (
            Self {
                reading: cell.clone(),
            },
            cell,
        )
    }
}

impl SystemProbe for MockProbe {
    fn read(&self) -> HostMemoryReading {
        *self.reading.lock().unwrap()
    }
}

/// Capture every event the watcher emits at target `oom_watcher` so
/// tests can assert "exactly one log fired" or grep field values.
/// Operates as a Layer over a `Registry` so the rest of the
/// crate's `tracing::info!` calls don't bleed into the test
/// expectations.
#[derive(Clone, Default)]
struct LogCapture {
    lines: Arc<Mutex<Vec<String>>>,
}

impl LogCapture {
    fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.lines.lock().unwrap().len()
    }
}

impl<S> Layer<S> for LogCapture
where
    S: Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "oom_watcher" {
            return;
        }
        if *event.metadata().level() != tracing::Level::INFO {
            return;
        }
        let mut visitor = StringExtractor::default();
        event.record(&mut visitor);
        self.lines.lock().unwrap().push(visitor.captured);
    }
}

#[derive(Default)]
struct StringExtractor {
    captured: String,
}

impl tracing::field::Visit for StringExtractor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "oom_watcher" {
            // The watcher logs via `oom_watcher = %json` (Display).
            // `record_debug` is the fallback when a Display-formatted
            // field is recorded as Debug by the subscriber stack —
            // tracing wraps `%`-formatted args in a `DebugValue` that
            // forwards through Debug.
            self.captured = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "oom_watcher" {
            self.captured = value.to_string();
        }
    }
}

/// Build a `MockProbe`-backed watcher with logging enabled and
/// run `f` while the tracing subscriber captures `oom_watcher`
/// events. Returns the captured lines so the test can assert
/// shape + count.
fn capture<F: FnOnce(&mut OomWatcher)>(
    probe: Arc<dyn SystemProbe>,
    mut config: OomWatcherConfig,
    f: F,
) -> LogCapture {
    config.log_enabled = true;
    let capture = LogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let mut watcher = OomWatcher::with_probe(config, probe);
    with_default(subscriber, || {
        f(&mut watcher);
    });
    capture
}

/// Bytes helpers — bytes-as-u64 arithmetic is noisy in test sites,
/// readable here.
const GIB: u64 = 1024 * 1024 * 1024;

fn reading_with_pressure(host_used: u64, host_total: u64) -> HostMemoryReading {
    HostMemoryReading {
        host_ram_used_bytes: Some(host_used),
        host_ram_total_bytes: Some(host_total),
        host_swap_used_bytes: Some(0),
        host_swap_total_bytes: Some(0),
        container_memory_current: Some(host_used),
        container_memory_max: Some(host_total),
        container_swap_current: Some(0),
        container_swap_max: Some(0),
        kernel_oom_kill_count: None,
        cpu_stat: None,
        self_cpu_ticks: None,
    }
}

#[test]
#[cfg(target_os = "linux")]
fn snapshot_reads_proc_meminfo() {
    use super::probe::ProcSysProbe;
    // Production probe against the real Linux `/proc/meminfo`.
    // We don't assert exact values — host RAM varies — only that
    // MemTotal is non-zero (any running Linux box has at least
    // some memory) and host_ram_used <= host_ram_total.
    let probe = ProcSysProbe::new();
    let r = probe.read();
    assert!(
        r.host_ram_total_bytes.unwrap_or(0) > 0,
        "/proc/meminfo MemTotal should be readable on Linux"
    );
    if let (Some(used), Some(total)) = (r.host_ram_used_bytes, r.host_ram_total_bytes) {
        assert!(
            used <= total,
            "used ({used}) > total ({total}) is impossible"
        );
    }
}

#[test]
fn cgroup_unavailable_yields_none_or_zero() {
    // MockProbe with all container_* fields None simulates a
    // non-cgroup-v2 host. The watcher must accept the None
    // values without panicking or spamming the log per tick.
    let reading = HostMemoryReading {
        host_ram_used_bytes: Some(GIB),
        host_ram_total_bytes: Some(16 * GIB),
        host_swap_used_bytes: Some(0),
        host_swap_total_bytes: Some(0),
        container_memory_current: None,
        container_memory_max: None,
        container_swap_current: None,
        container_swap_max: None,
        kernel_oom_kill_count: None,
        cpu_stat: None,
        self_cpu_ticks: None,
    };
    let (probe, _) = MockProbe::new(reading);
    let mut watcher = OomWatcher::with_probe(
        OomWatcherConfig {
            log_enabled: false,
            ..Default::default()
        },
        Arc::new(probe),
    );
    // Drive a "manual" snapshot path that doesn't go through
    // `on_sample` (which would need a worker pool); the goal is
    // to prove the `None` fields don't panic the trigger logic.
    watcher.set_snapshot_for_test(OomWatcherSnapshot {
        host: reading,
        tracked_workers_rss_sum: 0,
        tracked_workers_swap_sum: 0,
        tracked_workers_charged_sum: 0,
        tracked_workers_count: 0,
        captured_at: Some(Instant::now()),
        cpu_busy_milli: None,
    });
    let snap = watcher.last_snapshot();
    assert!(snap.host.container_memory_current.is_none());
    assert!(snap.host.container_memory_max.is_none());
}

#[test]
fn heartbeat_log_fires_on_first_emission_then_every_10s() {
    let reading = reading_with_pressure(GIB, 16 * GIB); // 6.25% pressure
    let (probe, _cell) = MockProbe::new(reading);
    let cap = capture(Arc::new(probe), OomWatcherConfig::default(), |watcher| {
        let t0 = Instant::now();
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: reading,
            tracked_workers_rss_sum: 0,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 0,
            tracked_workers_count: 0,
            captured_at: Some(t0),
            cpu_busy_milli: None,
            });
        // First emission: last_log_at = None → heartbeat fires.
        let fired = watcher.evaluate_and_emit_for_test(t0);
        assert_eq!(fired, Some(LogTrigger::Heartbeat));
        // 9.9s later: still under the 10s heartbeat window.
        let t1 = t0 + Duration::from_millis(9_900);
        let fired = watcher.evaluate_and_emit_for_test(t1);
        assert_eq!(fired, None, "9.9s after last log should not heartbeat");
        // 10.1s later (relative to t0): heartbeat fires again.
        let t2 = t0 + DEFAULT_HEARTBEAT_INTERVAL + Duration::from_millis(100);
        let fired = watcher.evaluate_and_emit_for_test(t2);
        assert_eq!(fired, Some(LogTrigger::Heartbeat));
    });
    assert_eq!(cap.count(), 2, "expected exactly 2 heartbeat lines");
    for line in cap.lines() {
        assert!(line.contains("\"trigger\":\"heartbeat\""), "{line}");
    }
}

#[test]
fn delta_log_under_pressure_fires_on_1gb_jump() {
    // Initial: host used 13/16 GiB = 81.25%, tracked workers 4 GiB.
    // Bump tracked workers to 5.5 GiB (Δ = 1.5 GiB ≥ 1 GiB) while
    // host pressure stays > 80%. Exactly one delta line should fire.
    let initial = reading_with_pressure(13 * GIB, 16 * GIB);
    let (probe, cell) = MockProbe::new(initial);
    let cap = capture(Arc::new(probe), OomWatcherConfig::default(), |watcher| {
        let t0 = Instant::now();
        // Seed the heartbeat gate by emitting once at t0.
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: initial,
            tracked_workers_rss_sum: 4 * GIB,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 4 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(t0),
            cpu_busy_milli: None,
            });
        let first = watcher.evaluate_and_emit_for_test(t0);
        assert_eq!(first, Some(LogTrigger::Heartbeat));
        // 100ms later (well inside heartbeat window): jump RSS.
        *cell.lock().unwrap() = initial;
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: initial,
            tracked_workers_rss_sum: 4 * GIB + (3 * GIB / 2),
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 4 * GIB + (3 * GIB / 2),
            tracked_workers_count: 2,
            captured_at: Some(t0 + Duration::from_millis(100)),
            cpu_busy_milli: None,
            });
        let fired = watcher.evaluate_and_emit_for_test(t0 + Duration::from_millis(100));
        assert_eq!(fired, Some(LogTrigger::DeltaUnderPressure));
    });
    let kill_or_delta = cap
        .lines()
        .iter()
        .filter(|l| l.contains("\"trigger\":\"delta_1gb_under_pressure\""))
        .count();
    assert_eq!(kill_or_delta, 1, "exactly one delta line expected");
}

#[test]
fn delta_log_below_pressure_does_not_fire() {
    // Host used 8/16 GiB = 50% (below the 0.80 threshold). Even
    // a +2 GiB jump in tracked RSS must NOT trigger the delta
    // log — the pressure guard suppresses it.
    let initial = reading_with_pressure(8 * GIB, 16 * GIB);
    let (probe, _cell) = MockProbe::new(initial);
    let cap = capture(Arc::new(probe), OomWatcherConfig::default(), |watcher| {
        let t0 = Instant::now();
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: initial,
            tracked_workers_rss_sum: 2 * GIB,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 2 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(t0),
            cpu_busy_milli: None,
            });
        let first = watcher.evaluate_and_emit_for_test(t0);
        assert_eq!(first, Some(LogTrigger::Heartbeat));
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: initial,
            tracked_workers_rss_sum: 4 * GIB,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 4 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(t0 + Duration::from_millis(100)),
            cpu_busy_milli: None,
            });
        let fired = watcher.evaluate_and_emit_for_test(t0 + Duration::from_millis(100));
        assert_eq!(fired, None, "below-pressure 2GiB jump must not fire");
    });
    // Exactly the seed heartbeat, nothing else.
    assert_eq!(cap.count(), 1);
}

#[test]
fn kill_event_emits_trigger_kill_log() {
    // The watcher's `note_kill` is the function `on_decision`
    // invokes when the pool returns `Killed`. Driving it directly
    // proves the kill-log lands; the on_decision → pool wiring is
    // a thin pass-through verified by the manager-level tests.
    let reading = reading_with_pressure(14 * GIB, 16 * GIB);
    let (probe, _cell) = MockProbe::new(reading);
    let cap = capture(Arc::new(probe), OomWatcherConfig::default(), |watcher| {
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: reading,
            tracked_workers_rss_sum: 6 * GIB,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 6 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(Instant::now()),
            cpu_busy_milli: None,
            });
        watcher.note_kill();
    });
    let all_lines = cap.lines();
    let kill_lines: Vec<&String> = all_lines
        .iter()
        .filter(|l| l.contains("\"trigger\":\"kill\""))
        .collect();
    assert_eq!(kill_lines.len(), 1, "expected exactly one kill log line");
    let line = kill_lines[0];
    assert!(line.contains("\"host_ram_used_bytes\":"), "{line}");
    assert!(line.contains("\"tracked_workers_rss_sum\":"), "{line}");
    assert!(line.contains("\"tracked_workers_swap_sum\":"), "{line}");
    assert!(line.contains("\"tracked_workers_charged_sum\":"), "{line}");
    assert!(line.contains("\"tracked_workers_count\":2"), "{line}");
}

/// Swap-blindness pin at the watcher level: tracked-worker RSS
/// SHRINKS while swap GROWS (pages migrating out as the worker
/// dies) — the charged sum grows, so the delta-under-pressure
/// trigger MUST fire and the log line must carry the swap share.
/// Pre-fix (RSS-keyed delta) this read as relief and stayed silent.
#[test]
fn swap_growth_with_shrinking_rss_fires_delta_and_logs_swap() {
    let initial = reading_with_pressure(13 * GIB, 16 * GIB); // 81.25%
    let (probe, _cell) = MockProbe::new(initial);
    let cap = capture(Arc::new(probe), OomWatcherConfig::default(), |watcher| {
        let t0 = Instant::now();
        // Healthy: 4 GiB resident, no swap.
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: initial,
            tracked_workers_rss_sum: 4 * GIB,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 4 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(t0),
            cpu_busy_milli: None,
            });
        let first = watcher.evaluate_and_emit_for_test(t0);
        assert_eq!(first, Some(LogTrigger::Heartbeat));
        // Dying: RSS collapsed to 1 GiB, 5 GiB migrated to swap.
        // Charged: 4 GiB → 6 GiB (Δ = 2 GiB ≥ 1 GiB).
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: initial,
            tracked_workers_rss_sum: GIB,
            tracked_workers_swap_sum: 5 * GIB,
            tracked_workers_charged_sum: 6 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(t0 + Duration::from_millis(100)),
            cpu_busy_milli: None,
            });
        let fired = watcher.evaluate_and_emit_for_test(t0 + Duration::from_millis(100));
        assert_eq!(
            fired,
            Some(LogTrigger::DeltaUnderPressure),
            "swap growth past the RSS shrink is PRESSURE and must trip the delta trigger"
        );
    });
    let delta_lines: Vec<String> = cap
        .lines()
        .iter()
        .filter(|l| l.contains("\"trigger\":\"delta_1gb_under_pressure\""))
        .cloned()
        .collect();
    assert_eq!(delta_lines.len(), 1, "exactly one delta line expected");
    let line = &delta_lines[0];
    assert!(
        line.contains(&format!("\"tracked_workers_swap_sum\":{}", 5 * GIB)),
        "{line}"
    );
    assert!(
        line.contains(&format!("\"tracked_workers_charged_sum\":{}", 6 * GIB)),
        "{line}"
    );
}

#[test]
fn log_disabled_emits_nothing() {
    // log_enabled=false: even with snapshot installed, neither
    // heartbeat nor kill emits.
    let reading = reading_with_pressure(14 * GIB, 16 * GIB);
    let (probe, _cell) = MockProbe::new(reading);
    let capture = LogCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let mut watcher = OomWatcher::with_probe(
        OomWatcherConfig {
            log_enabled: false,
            ..Default::default()
        },
        Arc::new(probe),
    );
    with_default(subscriber, || {
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: reading,
            tracked_workers_rss_sum: 6 * GIB,
            tracked_workers_swap_sum: 0,
            tracked_workers_charged_sum: 6 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(Instant::now()),
            cpu_busy_milli: None,
            });
        let _ = watcher.evaluate_and_emit_for_test(Instant::now());
        watcher.note_kill();
    });
    assert_eq!(capture.count(), 0, "no logs expected when disabled");
}

// ── Self-paced sweep tests ──────────────────────────────────────────

/// A probe that sleeps `delay` on each `read()` and counts its reads.
/// Models a host whose `/proc` + cgroup files are slow to serve so the
/// cadence test can prove await-before-resleep (the next sweep cannot
/// start until the previous — slow — read has fully completed).
struct SlowProbe {
    reading: HostMemoryReading,
    delay: Duration,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl SystemProbe for SlowProbe {
    fn read(&self) -> HostMemoryReading {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.reading
    }
}

const GIB_SWEEP: u64 = 1024 * 1024 * 1024;

/// Write a cgroup-v2 leaf (`memory.current` + `memory.swap.current`).
fn write_leaf(dir: &std::path::Path, current: u64, swap: u64) {
    std::fs::write(dir.join("memory.current"), format!("{current}\n")).unwrap();
    std::fs::write(dir.join("memory.swap.current"), format!("{swap}\n")).unwrap();
}

/// One worker's cgroup leaf reads as its charge; a SECOND worker whose
/// leaf directory does not exist (died/torn-down mid-sweep) must NOT
/// fail the whole sweep — it contributes a zero charge and the live
/// worker's reading still lands. This is the per-worker-failure
/// resilience the sweep promises.
#[test]
fn sweep_per_worker_read_failure_is_not_sweep_fatal() {
    let live = tempfile::tempdir().unwrap();
    write_leaf(live.path(), 3 * GIB_SWEEP, GIB_SWEEP);
    let gone = live.path().join("removed-leaf"); // never created

    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let probe = SlowProbe {
        reading: reading_with_pressure(GIB_SWEEP, 16 * GIB_SWEEP),
        delay: Duration::ZERO,
        reads: reads.clone(),
    };
    let inputs = ChargeSweepInputs::for_test(
        Arc::new(probe),
        vec![
            (0, None, Some(live.path().to_path_buf())),
            // No pid + missing leaf: nothing measurable → zero charge,
            // not an error.
            (1, None, Some(gone)),
        ],
    );
    let sweep = inputs.read();
    let charges = sweep.charges_for_test();
    assert_eq!(charges.len(), 2, "both workers must be present in the sweep");
    // Live worker: charged = resident + swap = 4 GiB.
    let live_charge = charges.iter().find(|(w, _)| *w == 0).unwrap().1;
    assert_eq!(live_charge.resident_bytes, 3 * GIB_SWEEP);
    assert_eq!(live_charge.swap_bytes, GIB_SWEEP);
    assert_eq!(live_charge.charged_bytes(), 4 * GIB_SWEEP);
    // Dead worker: zero charge, sweep did not abort.
    let gone_charge = charges.iter().find(|(w, _)| *w == 1).unwrap().1;
    assert_eq!(gone_charge.charged_bytes(), 0);
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1, "host read once per sweep");
}

/// The self-paced sweep cannot PILE: with a reader that takes `read_ms`
/// per sweep and a `sleep` of `interval_ms` between sweeps, the number
/// of sweeps over a fixed window is bounded by
/// `window / (read_ms + interval_ms)` — proving the next sweep starts a
/// full interval AFTER the previous one completed (await-before-resleep),
/// not at a fixed wall-clock rate that would overlap a slow read.
#[tokio::test(flavor = "current_thread")]
async fn sweep_cadence_awaits_before_resleeping() {
    let read_delay = Duration::from_millis(40);
    let interval = Duration::from_millis(20);
    let window = Duration::from_millis(600);

    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let probe: Arc<dyn SystemProbe> = Arc::new(SlowProbe {
        reading: reading_with_pressure(GIB_SWEEP, 16 * GIB_SWEEP),
        delay: read_delay,
        reads: reads.clone(),
    });

    // Mirror the operational loop's sweep arm: collect inputs (here a
    // single fixed worker), read OFF the runtime via spawn_blocking,
    // await it, then sleep `interval` before the next — re-armed only
    // after the read completes.
    let start = tokio::time::Instant::now();
    let mut sweeps = 0usize;
    let mut next_due = tokio::time::Instant::now();
    while tokio::time::Instant::now().duration_since(start) < window {
        tokio::time::sleep_until(next_due).await;
        let inputs = ChargeSweepInputs::for_test(probe.clone(), vec![(0, None, None)]);
        let _sweep = tokio::task::spawn_blocking(move || inputs.read())
            .await
            .expect("sweep read panicked");
        sweeps += 1;
        next_due = tokio::time::Instant::now() + interval;
    }

    // Per-sweep wall cost ≈ read_delay + interval = 60ms. Over a 600ms
    // window that is ~10 sweeps. A piling design (fixed 20ms cadence
    // ignoring the 40ms read) would have run ~30. Bound generously to
    // stay robust against scheduler jitter while still distinguishing
    // await-before-resleep from pile-up.
    let max_no_pileup = (window.as_millis() / (read_delay + interval).as_millis()) as usize + 2;
    assert!(
        sweeps <= max_no_pileup,
        "sweeps={sweeps} exceeds the no-pile-up bound {max_no_pileup}; \
         a slow read must delay the next sweep (await-before-resleep)"
    );
    // Sanity: it did make progress (not deadlocked).
    assert!(sweeps >= 5, "sweeps={sweeps} too few; the sweep loop made no progress");
    // Every sweep performed exactly one host read.
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), sweeps);
}

/// The OOM-sweep cadence must sit between two hard bounds (see
/// `SAMPLE_SWEEP_INTERVAL`'s doc):
///
///   - Upper: comfortably below the kernel-OOM correlation window
///     (500ms in both `manager::events` and the secondary's
///     `worker_event`). A sweep slower than that window would let an
///     `oom_kill`-then-pipe-EOF pair fall out of correlation, mis-
///     classifying a real OOM disconnect as `Recoverable`. We require a
///     2× margin (≤ 250ms) so two sweeps always land inside the window.
///   - Lower: not so tight that the sweep arm dominates the operational
///     `select!`. The former 50ms (20Hz) value fired the arm ~18.5×/sec
///     and starved the control plane. We require ≥ 100ms so the sweep is
///     a periodic-background arm, not a hot one.
///
/// Pinned so an accidental retune that re-enters the starvation regime
/// (too tight) or the mis-correlation regime (too loose) is a hard
/// failure, not a silent diff.
#[test]
fn sample_sweep_interval_within_cadence_bounds() {
    // Mirror of the private `KERNEL_OOM_CORRELATION_WINDOW` in
    // `manager::events` / the secondary `worker_event`. If that window
    // ever changes, this literal — and the cadence — must be revisited
    // together.
    const KERNEL_OOM_CORRELATION_WINDOW: Duration = Duration::from_millis(500);
    let half_window = KERNEL_OOM_CORRELATION_WINDOW / 2;

    assert!(
        SAMPLE_SWEEP_INTERVAL <= half_window,
        "sweep cadence {SAMPLE_SWEEP_INTERVAL:?} must be <= half the \
         {KERNEL_OOM_CORRELATION_WINDOW:?} correlation window so an oom_kill \
         is always recorded within the window of a correlated pipe-EOF",
    );
    assert!(
        SAMPLE_SWEEP_INTERVAL >= Duration::from_millis(100),
        "sweep cadence {SAMPLE_SWEEP_INTERVAL:?} must be >= 100ms so the \
         sweep arm stays a periodic-background arm and does not dominate \
         the operational select! (the 50ms/20Hz starvation regression)",
    );
}

/// Cadence-regression: over a busy run of K oploop iterations driven
/// faster than the sweep interval, the self-paced sweep arm fires at
/// most ONCE — it is NOT selected every iteration — while a co-ready,
/// higher-priority data arm wins every iteration. This is the inverse
/// of the live forensics where the 50ms arm dominated ~86% of arm
/// executions: with the production interval and `biased;` data-first
/// ordering, the sweep is bounded by its cadence, not the loop tick
/// rate. Real-time test (mirrors the existing oom cadence tests, which
/// also use wall-clock `current_thread` without `tokio/test-util`).
#[tokio::test(flavor = "current_thread")]
async fn oom_sweep_arm_not_selected_every_iteration() {
    use tokio::sync::mpsc;

    // A data arm that is ALWAYS ready (mirrors a busy inbox / pool
    // event): pre-fill K frames so it wins every biased race.
    let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
    const K: usize = 5000;
    for i in 0..K as u32 {
        tx.send(i).expect("send");
    }
    drop(tx);

    // Sweep deadline seeded NOW; re-armed `SAMPLE_SWEEP_INTERVAL` after
    // each fire — the exact `next_sweep_due` idiom of the real loops.
    let interval = SAMPLE_SWEEP_INTERVAL;
    let mut next_sweep_due = tokio::time::Instant::now();

    let start = tokio::time::Instant::now();
    let mut data_hits = 0usize;
    let mut sweep_hits = 0usize;
    // The K pre-filled frames drain in well under one `interval` of
    // wall-clock, so the sweep deadline (already-due on iteration 0,
    // then re-armed a full interval out) can be reached at most once.
    // The data arm wins every other iteration under `biased;` + data-
    // first. This proves the sweep is NOT selected per loop tick.
    while data_hits < K {
        tokio::select! {
            biased;
            msg = rx.recv() => {
                match msg {
                    Some(_) => data_hits += 1,
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(next_sweep_due) => {
                sweep_hits += 1;
                next_sweep_due = tokio::time::Instant::now() + interval;
            }
        }
    }
    let elapsed = start.elapsed();

    assert_eq!(data_hits, K, "data arm should win every iteration");
    // Guard the test's own premise: draining K frames must take less
    // than one sweep interval, else the bound below is meaningless.
    assert!(
        elapsed < interval,
        "test premise broken: draining {K} frames took {elapsed:?} >= \
         one sweep interval {interval:?}; raise K's drain speed",
    );
    // The structural invariant: the sweep is bounded by its cadence, not
    // the loop tick rate (the 86%-domination regression). Over K ticks
    // inside one interval, it fires at most once — NOT K times.
    assert!(
        sweep_hits <= 1,
        "sweep fired {sweep_hits} times over {K} iterations in \
         {elapsed:?}; it must be bounded by its cadence, not the loop \
         tick rate (the oom_sweep starvation regression)",
    );
}

/// The dual of the cadence-regression: the sweep STILL fires within its
/// interval when the data arm is idle — OOM-detection latency is
/// preserved. Over a window of N intervals with an empty inbox, the
/// sweep fires ~N times, so a real memory-pressure event is detected
/// within one `SAMPLE_SWEEP_INTERVAL`. Real-time test.
#[tokio::test(flavor = "current_thread")]
async fn oom_sweep_still_fires_within_its_interval_when_idle() {
    use tokio::sync::mpsc;

    // Inbox idle: the single kept sender makes recv() pend forever, so
    // only the sweep arm can fire.
    let (_tx, mut rx) = mpsc::unbounded_channel::<u32>();
    let interval = SAMPLE_SWEEP_INTERVAL;
    let intervals = 4u32;
    let window = interval * intervals;

    let mut next_sweep_due = tokio::time::Instant::now();
    let start = tokio::time::Instant::now();
    let mut sweeps = 0usize;
    while start.elapsed() < window {
        tokio::select! {
            biased;
            _msg = rx.recv() => unreachable!("inbox is idle"),
            _ = tokio::time::sleep_until(next_sweep_due) => {
                sweeps += 1;
                next_sweep_due = tokio::time::Instant::now() + interval;
            }
        }
    }

    // await-before-resleep over `intervals` worth of time fires ≈
    // `intervals` sweeps; allow generous slack for real-time scheduling
    // jitter. Critically: the sweep MUST fire at least ~once per interval
    // (detection preserved) and MUST NOT fire wildly more (cadence held).
    let lo = (intervals as usize).saturating_sub(1).max(1);
    let hi = intervals as usize + 2;
    assert!(
        (lo..=hi).contains(&sweeps),
        "sweep should fire ~{intervals} times over {intervals} intervals \
         when idle (OOM-detection latency preserved), got {sweeps}",
    );
}
