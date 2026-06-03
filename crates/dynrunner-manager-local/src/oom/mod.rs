//! OOM-watcher subsystem.
//!
//! Single concern: own the per-host / per-cgroup / per-worker memory
//! sampling cadence + decision cadence + structured logging triggers for
//! both `LocalManager` and `SecondaryCoordinator`. Each caller drives
//! the watcher through the same API; nothing in this module branches
//! on caller mode.
//!
//! ## Boundary
//!
//! - The watcher owns: sample-interval ticker, decision-interval
//!   ticker, the latest [`OomWatcherSnapshot`], the log-emission
//!   triggers, and the call into `WorkerPool::check_resource_pressure`.
//! - The watcher does NOT own: the kill-outcome handler. Each caller
//!   (LocalManager monitor / Secondary resource) keeps its own
//!   outcome handler — the watcher merely forwards the
//!   `ResourcePressureResult` and records a "kill happened" event
//!   so the next log line carries the right `trigger`.
//!
//! ## Two ticks
//!
//! - **Sample tick** (default 50ms, 20Hz): read host RAM, cgroup
//!   memory.current, swap, tracked worker RSS sum into the snapshot.
//!   Optionally emit a structured log line if a trigger fires.
//! - **Decision tick** (caller's existing cadence, default 100ms):
//!   ask the scheduler whether a worker must be killed. Reads the
//!   most recent worker-RSS state via
//!   `WorkerPool::check_resource_pressure` (which internally calls
//!   `update_all_resource_usage`); the watcher's own snapshot is
//!   refreshed independently by the sample tick.
//!
//! Sample updates always run `pool.update_all_resource_usage()` so the
//! watcher's `tracked_workers_rss_sum` is fresh on every sample tick.

pub mod disconnect;
pub mod probe;

pub use disconnect::classify_disconnect;

use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, ResourceKind, ResourceMap};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::Scheduler;

use crate::pool::{ResourcePressureResult, WorkerPool};

use self::probe::{HostMemoryReading, ProcSysProbe, SystemProbe};

/// 1 GiB in bytes. Threshold for the delta-under-pressure log
/// trigger; locked with the consumer.
const DELTA_TRIGGER_BYTES: u64 = 1024 * 1024 * 1024;

/// Host-RAM utilisation above which the delta-trigger fires. Locked
/// with the consumer.
const DELTA_PRESSURE_RATIO: f64 = 0.80;

/// Default sample cadence (50ms = 20Hz). Independent of the
/// decision cadence so a slow decision tick still produces a dense
/// forensic record.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

/// Default heartbeat: a log line every 10s even when nothing
/// triggered. Bounds the silence window during a healthy run so
/// post-mortem grep can correlate by timestamp.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Trigger label that appears in the structured log line.
///
/// `Heartbeat` — the every-10s wakeup.
/// `DeltaUnderPressure` — a tracked field grew ≥ 1 GiB since the
/// last log AND host_ram_used / host_ram_total > 0.80.
/// `Kill` — a `WorkerPool::check_resource_pressure` decision tick
/// returned `Killed` this round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogTrigger {
    Heartbeat,
    DeltaUnderPressure,
    Kill,
}

impl LogTrigger {
    fn as_str(self) -> &'static str {
        match self {
            LogTrigger::Heartbeat => "heartbeat",
            LogTrigger::DeltaUnderPressure => "delta_1gb_under_pressure",
            LogTrigger::Kill => "kill",
        }
    }
}

/// In-memory snapshot of the last sample. Carries both the host /
/// cgroup reading and the worker-pool-derived fields.
///
/// All numeric fields are bytes (where applicable) or counts;
/// `Option<u64>` carries "unavailable on this host" without
/// conflating it with zero. The structured log line maps `None` to
/// JSON `null` so downstream parsers can grep numeric fields
/// uniformly.
#[derive(Debug, Clone, Copy, Default)]
pub struct OomWatcherSnapshot {
    pub host: HostMemoryReading,
    pub tracked_workers_rss_sum: u64,
    pub tracked_workers_count: u32,
    pub captured_at: Option<Instant>,
}

/// Constructor knobs for [`OomWatcher`]. Splits the cadence /
/// logging policy from the probe injection so unit tests can
/// substitute a deterministic probe + clock without touching the
/// per-caller wiring.
pub struct OomWatcherConfig {
    /// How often to read host/cgroup state and update per-worker RSS.
    pub sample_interval: Duration,
    /// How often to invoke the scheduler's pressure decision.
    pub decision_interval: Duration,
    /// Cadence of the unconditional heartbeat log line. Only used
    /// when `log_enabled = true`.
    pub heartbeat_interval: Duration,
    /// Master switch for the structured JSON log emission. When
    /// `false`, the watcher still samples and decides (so the
    /// per-worker RSS stays current and the pool kills as before),
    /// but no `info!(target: "oom_watcher", ...)` events fire.
    pub log_enabled: bool,
}

impl Default for OomWatcherConfig {
    fn default() -> Self {
        Self {
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            decision_interval: Duration::from_millis(100),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            log_enabled: false,
        }
    }
}

/// The watcher itself. Drives sample + decision cadences and emits
/// the structured log line when a trigger fires.
///
/// Caller pattern (both LocalManager and Secondary):
/// ```ignore
/// let mut watcher = OomWatcher::new(OomWatcherConfig {
///     sample_interval: cfg.sample_interval,
///     decision_interval: cfg.resource_check_interval,
///     heartbeat_interval: oom::DEFAULT_HEARTBEAT_INTERVAL,
///     log_enabled: cfg.log_oom_watcher,
/// });
/// let mut sample_int = watcher.sample_interval_ticker();
/// let mut decision_int = watcher.decision_interval_ticker();
/// loop {
///     tokio::select! {
///         _ = sample_int.tick() => { watcher.on_sample(&mut pool); }
///         _ = decision_int.tick() => {
///             match watcher.on_decision(&mut pool, &scheduler, &max, in_pressure) {
///                 ResourcePressureResult::Killed { .. } => { /* caller handles */ }
///                 ResourcePressureResult::NoAction => {}
///             }
///         }
///     }
/// }
/// ```
pub struct OomWatcher {
    config: OomWatcherConfig,
    probe: Box<dyn SystemProbe>,
    /// Set when `on_decision` returns `Killed` so the next log
    /// emission carries `trigger=kill` instead of heartbeat/delta.
    /// Drained by the next `emit_log_line` call.
    pending_kill_event: bool,
    /// Latest sample (or default at startup before the first sample
    /// tick fires).
    last_snapshot: OomWatcherSnapshot,
    /// Timestamp of the last emitted log line. `None` until the
    /// first emission. Used to gate the heartbeat trigger.
    last_log_at: Option<Instant>,
    /// Field values at the time of the last emission. Used to
    /// evaluate the ≥1 GiB delta trigger.
    last_log_values: TrackedDeltaFields,
    /// Previous sample's `kernel_oom_kill_count`. `None` before the
    /// first sample, or when the probe's workers `memory.events` is
    /// unavailable. Used to compute the per-sample delta — a positive
    /// delta means the kernel ran the OOM-killer on the workers
    /// subgroup in the window since the last sample.
    last_kernel_oom_count: Option<u64>,
    /// Wall-clock time of the most recent positive kernel-oom delta.
    /// Consumed by [`Self::kernel_oom_recent`] so the manager-side
    /// disconnect reclassifier can ask "did a kernel-OOM land within
    /// the last N milliseconds?". `None` until the first positive
    /// delta. Production sample cadence is 50ms; a 500ms window
    /// covers ~10 samples worth of race tolerance between
    /// `oom_kill` counter increment and pipe-EOF observation.
    recent_kernel_oom_at: Option<Instant>,
}

/// The three fields the delta trigger compares against their value
/// at the last log emission. Kept together so a single struct
/// snapshot moves with each log line.
#[derive(Debug, Clone, Copy, Default)]
struct TrackedDeltaFields {
    host_ram_used: u64,
    container_memory_current: u64,
    tracked_workers_rss_sum: u64,
}

impl OomWatcher {
    /// Construct with the production [`ProcSysProbe`].
    pub fn new(config: OomWatcherConfig) -> Self {
        Self::with_probe(config, Box::new(ProcSysProbe::new()))
    }

    /// Construct with the production [`ProcSysProbe`] AND a path to
    /// the workers cgroup `memory.events` file so kernel-OOM detection
    /// becomes active. Pass `None` (or use [`Self::new`]) when the
    /// nested workers cgroup was not materialised (flat-layout
    /// fallback, non-cgroup-v2 host) — the probe then leaves
    /// `kernel_oom_kill_count` as `None` and no upgrade ever fires.
    ///
    /// The path is typically `<workers-cgroup>/memory.events` derived
    /// from [`crate::cgroup::NestedCgroupHandle::workers_path`].
    pub fn new_with_workers_cgroup(
        config: OomWatcherConfig,
        workers_memory_events_path: Option<std::path::PathBuf>,
    ) -> Self {
        let probe = ProcSysProbe::new().with_workers_memory_events(workers_memory_events_path);
        Self::with_probe(config, Box::new(probe))
    }

    /// Construct with a caller-supplied probe. Used by unit tests to
    /// inject deterministic mock readings without touching `/proc`.
    pub fn with_probe(config: OomWatcherConfig, probe: Box<dyn SystemProbe>) -> Self {
        Self {
            config,
            probe,
            pending_kill_event: false,
            last_snapshot: OomWatcherSnapshot::default(),
            last_log_at: None,
            last_log_values: TrackedDeltaFields::default(),
            last_kernel_oom_count: None,
            recent_kernel_oom_at: None,
        }
    }

    /// True iff a positive `oom_kill` delta on the workers cgroup was
    /// observed within the last `window`. Consumed by the manager's
    /// disconnect reclassifier: when a worker's pipe-EOF lands in the
    /// same window as a kernel-OOM event, the disconnect upgrades
    /// from `Recoverable` to `ResourceExhausted(memory)` (the kernel
    /// beat the userland scheduler — see [`crate::oom::disconnect`]
    /// for the full classifier).
    pub fn kernel_oom_recent(&self, window: Duration) -> bool {
        self.kernel_oom_recent_at(Instant::now(), window)
    }

    /// Test seam for [`Self::kernel_oom_recent`] with an explicit
    /// `now`. Production callers use the wall-clock form.
    pub fn kernel_oom_recent_at(&self, now: Instant, window: Duration) -> bool {
        match self.recent_kernel_oom_at {
            Some(at) => now.duration_since(at) <= window,
            None => false,
        }
    }

    /// Build a tokio interval driving the sample tick. The caller
    /// installs it into its `select!` loop.
    pub fn sample_interval_ticker(&self) -> tokio::time::Interval {
        tokio::time::interval(self.config.sample_interval)
    }

    /// Build a tokio interval driving the decision tick.
    pub fn decision_interval_ticker(&self) -> tokio::time::Interval {
        tokio::time::interval(self.config.decision_interval)
    }

    /// Latest captured snapshot. Mostly for debugging / introspection
    /// — the watcher consumes its own state internally for the
    /// trigger logic.
    pub fn last_snapshot(&self) -> &OomWatcherSnapshot {
        &self.last_snapshot
    }

    /// Sample tick: refresh per-worker RSS via the pool, read host
    /// and cgroup state via the probe, evaluate log triggers, emit
    /// when any fires.
    ///
    /// Generic over the pool's transport/identifier so the same
    /// surface serves both manager modes.
    pub fn on_sample<M, I>(&mut self, pool: &mut WorkerPool<M, I>)
    where
        M: ManagerEndpoint + 'static,
        I: Identifier,
    {
        self.on_sample_at(pool, Instant::now())
    }

    /// Test seam: explicit `now` so unit tests can drive heartbeat
    /// cadence with a fake clock.
    pub fn on_sample_at<M, I>(&mut self, pool: &mut WorkerPool<M, I>, now: Instant)
    where
        M: ManagerEndpoint + 'static,
        I: Identifier,
    {
        pool.update_all_resource_usage();
        let host = self.probe.read();
        // Kernel-OOM detection: compute the per-sample delta of the
        // workers-cgroup `oom_kill` counter. A positive delta means
        // the kernel ran the OOM-killer on that subgroup since the
        // last sample. The window is wall-clock-based (see
        // `kernel_oom_recent`) so the manager's disconnect
        // reclassifier can correlate a worker pipe-EOF with the
        // kernel event regardless of how many samples landed
        // between them.
        if let Some(current) = host.kernel_oom_kill_count {
            if let Some(prev) = self.last_kernel_oom_count
                && current > prev
            {
                self.recent_kernel_oom_at = Some(now);
            }
            self.last_kernel_oom_count = Some(current);
        }
        let tracked_workers_rss_sum = sum_worker_rss(pool);
        let tracked_workers_count = pool.workers.len() as u32;
        self.last_snapshot = OomWatcherSnapshot {
            host,
            tracked_workers_rss_sum,
            tracked_workers_count,
            captured_at: Some(now),
        };

        if !self.config.log_enabled {
            return;
        }
        if let Some(trigger) = self.evaluate_trigger(now) {
            self.emit_log_line(trigger, now);
        }
    }

    /// Decision tick: run the scheduler's pressure check via the
    /// pool. Records "kill happened" so the next log emission
    /// carries `trigger=kill`, AND emits a kill log immediately
    /// (regardless of heartbeat / delta cadence) so the forensic
    /// record captures the kill in the same tick window.
    ///
    /// Returns the pool's verdict verbatim so each caller's outcome
    /// handler (LocalManager monitor / Secondary resource) can take
    /// the mode-specific follow-up action.
    pub fn on_decision<M, S, I>(
        &mut self,
        pool: &mut WorkerPool<M, I>,
        scheduler: &S,
        max_resources: &ResourceMap,
        in_pressure_phase: bool,
    ) -> ResourcePressureResult<I>
    where
        M: ManagerEndpoint + 'static,
        S: Scheduler<I>,
        I: Identifier,
    {
        let result = pool.check_resource_pressure(scheduler, max_resources, in_pressure_phase);
        if matches!(result, ResourcePressureResult::Killed { .. }) {
            // Refresh the snapshot's worker counts so the kill log
            // carries the post-kill state alongside the host
            // reading. The pool already updated per-worker RSS
            // inside `check_resource_pressure`; re-summing is cheap
            // and keeps the log line self-consistent.
            let host = self.probe.read();
            self.last_snapshot.host = host;
            self.last_snapshot.tracked_workers_rss_sum = sum_worker_rss(pool);
            self.last_snapshot.tracked_workers_count = pool.workers.len() as u32;
            self.last_snapshot.captured_at = Some(Instant::now());
            self.note_kill();
        }
        result
    }

    /// Record that a kill happened in this decision tick window and
    /// emit the `trigger=kill` structured log (when logging is
    /// enabled). Split out of [`Self::on_decision`] so unit tests
    /// can drive the kill-emission path without constructing a
    /// scheduler + worker pool fixture.
    ///
    /// Internal contract: the caller must have already refreshed
    /// `self.last_snapshot` so the emitted log carries
    /// representative numbers. `on_decision` does this; the test
    /// hook sets the snapshot via `with_snapshot_for_test` first.
    pub fn note_kill(&mut self) {
        self.pending_kill_event = true;
        if self.config.log_enabled {
            self.emit_log_line(LogTrigger::Kill, Instant::now());
        }
    }

    /// Test seam: install a snapshot before driving the trigger
    /// logic with a fake clock. `pub(crate)` so the in-crate
    /// tests reach it without exposing test-only API to outside
    /// consumers. Compiled only under `#[cfg(test)]` so the
    /// dead-code lint doesn't flag the seam in release builds.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn set_snapshot_for_test(&mut self, snapshot: OomWatcherSnapshot) {
        self.last_snapshot = snapshot;
    }

    /// Test seam: drive the sample tick's trigger evaluation +
    /// log emission against an already-installed snapshot,
    /// supplying the `now` instant explicitly. Skips the
    /// pool/probe re-read so a fake-clock test can chain calls
    /// without touching `/proc`.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn evaluate_and_emit_for_test(&mut self, now: Instant) -> Option<LogTrigger> {
        if !self.config.log_enabled {
            return None;
        }
        if let Some(trigger) = self.evaluate_trigger(now) {
            self.emit_log_line(trigger, now);
            Some(trigger)
        } else {
            None
        }
    }

    /// Internal: which trigger (if any) should fire on this sample.
    /// `Kill` is handled separately via `on_decision` so the kill
    /// log lands as soon as the kill happens, not on the next
    /// sample boundary.
    fn evaluate_trigger(&self, now: Instant) -> Option<LogTrigger> {
        // Heartbeat: first emission OR last emission older than the
        // heartbeat interval.
        let heartbeat_due = match self.last_log_at {
            None => true,
            Some(last) => now.duration_since(last) >= self.config.heartbeat_interval,
        };
        if heartbeat_due {
            return Some(LogTrigger::Heartbeat);
        }

        // Delta-under-pressure: any tracked field grew ≥ 1 GiB AND
        // host pressure > 0.80. Both conditions must hold. Compares
        // against the values captured at the last log emission so
        // a single 1-GiB jump produces exactly one log.
        let snap = &self.last_snapshot;
        let prev = &self.last_log_values;
        let cur_host_ram_used = snap.host.host_ram_used_bytes.unwrap_or(0);
        let cur_cgroup_cur = snap.host.container_memory_current.unwrap_or(0);
        let cur_rss_sum = snap.tracked_workers_rss_sum;

        let host_total = snap.host.host_ram_total_bytes.unwrap_or(0);
        let pressure_ratio = if host_total == 0 {
            0.0
        } else {
            cur_host_ram_used as f64 / host_total as f64
        };
        if pressure_ratio <= DELTA_PRESSURE_RATIO {
            return None;
        }

        let delta = |cur: u64, prev: u64| -> u64 { cur.saturating_sub(prev) };
        let any_grew_by_1gib = delta(cur_host_ram_used, prev.host_ram_used) >= DELTA_TRIGGER_BYTES
            || delta(cur_cgroup_cur, prev.container_memory_current) >= DELTA_TRIGGER_BYTES
            || delta(cur_rss_sum, prev.tracked_workers_rss_sum) >= DELTA_TRIGGER_BYTES;

        if any_grew_by_1gib {
            Some(LogTrigger::DeltaUnderPressure)
        } else {
            None
        }
    }

    /// Internal: emit one structured JSON log line via `tracing` at
    /// `target: "oom_watcher"`. Updates the gate state (last_log_at,
    /// last_log_values) so subsequent triggers compare against this
    /// emission's values.
    fn emit_log_line(&mut self, trigger: LogTrigger, now: Instant) {
        let snap = &self.last_snapshot;
        let ts = chrono_like_iso8601_now();
        let json = serde_json::json!({
            "ts": ts,
            "host_ram_used_bytes": snap.host.host_ram_used_bytes,
            "host_ram_total_bytes": snap.host.host_ram_total_bytes,
            "host_swap_used_bytes": snap.host.host_swap_used_bytes,
            "host_swap_total_bytes": snap.host.host_swap_total_bytes,
            "container_memory_current": snap.host.container_memory_current,
            "container_memory_max": snap.host.container_memory_max,
            "container_swap_current": snap.host.container_swap_current,
            "container_swap_max": snap.host.container_swap_max,
            "tracked_workers_rss_sum": snap.tracked_workers_rss_sum,
            "tracked_workers_count": snap.tracked_workers_count,
            "trigger": trigger.as_str(),
            "note": "tracked_workers_rss_sum measures direct workers only — daemon-delegated subprocesses NOT included",
        });
        tracing::info!(target: "oom_watcher", oom_watcher = %json);

        self.last_log_at = Some(now);
        self.last_log_values = TrackedDeltaFields {
            host_ram_used: snap.host.host_ram_used_bytes.unwrap_or(0),
            container_memory_current: snap.host.container_memory_current.unwrap_or(0),
            tracked_workers_rss_sum: snap.tracked_workers_rss_sum,
        };
        if matches!(trigger, LogTrigger::Kill) {
            self.pending_kill_event = false;
        }
    }
}

/// Sum the RSS bytes across all workers in the pool. Reads the
/// already-cached `actual_usage` (populated by
/// `update_all_resource_usage` / `check_resource_pressure`); no
/// /proc access here.
fn sum_worker_rss<M, I>(pool: &WorkerPool<M, I>) -> u64
where
    M: ManagerEndpoint + 'static,
    I: Identifier,
{
    let mem_kind = ResourceKind::memory();
    pool.workers
        .iter()
        .map(|w| w.actual_usage.get(&mem_kind))
        .sum()
}

/// Minimal RFC-3339-ish UTC timestamp builder.
///
/// We avoid pulling in `chrono` for one log line; the format is
/// the grep-stable shape downstream tooling already consumes
/// elsewhere in the framework (see `secondary::wire::timestamp_now`).
fn chrono_like_iso8601_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    // Plain seconds + millis epoch-derived UTC. ISO 8601-compatible
    // formatting requires day/month math; for forensic correlation
    // we just need monotonic-ish strings that sort. The format
    // matches the wider framework's existing timestamp shape so
    // operators don't have to learn a new one.
    format_unix_seconds_as_iso8601(secs, millis)
}

fn format_unix_seconds_as_iso8601(secs: u64, millis: u32) -> String {
    // Days-since-epoch algorithm based on the civil-from-days
    // formula (Howard Hinnant, public domain). Avoids the chrono
    // dep for the watcher's one timestamp call.
    let days = (secs / 86_400) as i64;
    let seconds_of_day = (secs % 86_400) as u32;
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, m, d, hour, minute, second, millis
    )
}

#[cfg(test)]
mod tests;
