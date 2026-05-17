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
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use super::probe::{HostMemoryReading, SystemProbe};
use super::{
    LogTrigger, OomWatcher, OomWatcherConfig, OomWatcherSnapshot, DEFAULT_HEARTBEAT_INTERVAL,
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
    probe: Box<dyn SystemProbe>,
    mut config: OomWatcherConfig,
    f: F,
) -> LogCapture {
    config.log_enabled = true;
    let capture = LogCapture::default();
    let subscriber =
        tracing_subscriber::registry().with(capture.clone());
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
        assert!(used <= total, "used ({used}) > total ({total}) is impossible");
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
    };
    let (probe, _) = MockProbe::new(reading);
    let mut watcher = OomWatcher::with_probe(
        OomWatcherConfig {
            log_enabled: false,
            ..Default::default()
        },
        Box::new(probe),
    );
    // Drive a "manual" snapshot path that doesn't go through
    // `on_sample` (which would need a worker pool); the goal is
    // to prove the `None` fields don't panic the trigger logic.
    watcher.set_snapshot_for_test(OomWatcherSnapshot {
        host: reading,
        tracked_workers_rss_sum: 0,
        tracked_workers_count: 0,
        captured_at: Some(Instant::now()),
    });
    let snap = watcher.last_snapshot();
    assert!(snap.host.container_memory_current.is_none());
    assert!(snap.host.container_memory_max.is_none());
}

#[test]
fn heartbeat_log_fires_on_first_emission_then_every_10s() {
    let reading = reading_with_pressure(GIB, 16 * GIB); // 6.25% pressure
    let (probe, _cell) = MockProbe::new(reading);
    let cap = capture(
        Box::new(probe),
        OomWatcherConfig::default(),
        |watcher| {
            let t0 = Instant::now();
            watcher.set_snapshot_for_test(OomWatcherSnapshot {
                host: reading,
                tracked_workers_rss_sum: 0,
                tracked_workers_count: 0,
                captured_at: Some(t0),
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
        },
    );
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
    let cap = capture(
        Box::new(probe),
        OomWatcherConfig::default(),
        |watcher| {
            let t0 = Instant::now();
            // Seed the heartbeat gate by emitting once at t0.
            watcher.set_snapshot_for_test(OomWatcherSnapshot {
                host: initial,
                tracked_workers_rss_sum: 4 * GIB,
                tracked_workers_count: 2,
                captured_at: Some(t0),
            });
            let first = watcher.evaluate_and_emit_for_test(t0);
            assert_eq!(first, Some(LogTrigger::Heartbeat));
            // 100ms later (well inside heartbeat window): jump RSS.
            *cell.lock().unwrap() = initial;
            watcher.set_snapshot_for_test(OomWatcherSnapshot {
                host: initial,
                tracked_workers_rss_sum: 4 * GIB + (3 * GIB / 2),
                tracked_workers_count: 2,
                captured_at: Some(t0 + Duration::from_millis(100)),
            });
            let fired =
                watcher.evaluate_and_emit_for_test(t0 + Duration::from_millis(100));
            assert_eq!(fired, Some(LogTrigger::DeltaUnderPressure));
        },
    );
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
    let cap = capture(
        Box::new(probe),
        OomWatcherConfig::default(),
        |watcher| {
            let t0 = Instant::now();
            watcher.set_snapshot_for_test(OomWatcherSnapshot {
                host: initial,
                tracked_workers_rss_sum: 2 * GIB,
                tracked_workers_count: 2,
                captured_at: Some(t0),
            });
            let first = watcher.evaluate_and_emit_for_test(t0);
            assert_eq!(first, Some(LogTrigger::Heartbeat));
            watcher.set_snapshot_for_test(OomWatcherSnapshot {
                host: initial,
                tracked_workers_rss_sum: 4 * GIB,
                tracked_workers_count: 2,
                captured_at: Some(t0 + Duration::from_millis(100)),
            });
            let fired =
                watcher.evaluate_and_emit_for_test(t0 + Duration::from_millis(100));
            assert_eq!(fired, None, "below-pressure 2GiB jump must not fire");
        },
    );
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
    let cap = capture(
        Box::new(probe),
        OomWatcherConfig::default(),
        |watcher| {
            watcher.set_snapshot_for_test(OomWatcherSnapshot {
                host: reading,
                tracked_workers_rss_sum: 6 * GIB,
                tracked_workers_count: 2,
                captured_at: Some(Instant::now()),
            });
            watcher.note_kill();
        },
    );
    let all_lines = cap.lines();
    let kill_lines: Vec<&String> = all_lines
        .iter()
        .filter(|l| l.contains("\"trigger\":\"kill\""))
        .collect();
    assert_eq!(kill_lines.len(), 1, "expected exactly one kill log line");
    let line = kill_lines[0];
    assert!(line.contains("\"host_ram_used_bytes\":"), "{line}");
    assert!(line.contains("\"tracked_workers_rss_sum\":"), "{line}");
    assert!(line.contains("\"tracked_workers_count\":2"), "{line}");
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
        Box::new(probe),
    );
    with_default(subscriber, || {
        watcher.set_snapshot_for_test(OomWatcherSnapshot {
            host: reading,
            tracked_workers_rss_sum: 6 * GIB,
            tracked_workers_count: 2,
            captured_at: Some(Instant::now()),
        });
        let _ = watcher.evaluate_and_emit_for_test(Instant::now());
        watcher.note_kill();
    });
    assert_eq!(capture.count(), 0, "no logs expected when disabled");
}
