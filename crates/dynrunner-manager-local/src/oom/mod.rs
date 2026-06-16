//! OOM-watcher subsystem.
//!
//! Single concern: own the per-host / per-cgroup / per-worker memory
//! sampling cadence + decision + structured logging triggers for
//! both `LocalManager` and `SecondaryCoordinator`. Each caller drives
//! the watcher through the same API; nothing in this module branches
//! on caller mode.
//!
//! ## Boundary
//!
//! - The watcher owns: the sweep cadence policy
//!   ([`OomWatcher::sweep_interval`]), the read-input collection
//!   ([`OomWatcher::collect_sweep_inputs`]) + the blocking read it
//!   produces ([`ChargeSweepInputs::read`]), the apply-back
//!   ([`OomWatcher::apply_sweep`]), the latest [`OomWatcherSnapshot`],
//!   the log-emission triggers, and the call into
//!   `WorkerPool::decide_resource_pressure`.
//! - The watcher does NOT own: the kill-outcome handler. Each caller
//!   (LocalManager monitor / Secondary resource) keeps its own
//!   outcome handler — the watcher merely forwards the
//!   `ResourcePressureResult` and records a "kill happened" event
//!   so the next log line carries the right `trigger`.
//!
//! ## One self-paced sweep
//!
//! The blocking cgroup / `/proc` reads do NOT run on the operational
//! `select!`. Each sweep:
//!   1. [`OomWatcher::collect_sweep_inputs`] snapshots the CURRENT
//!      worker set's (pid, cgroup-leaf) inputs off the pool — picking
//!      up respawns / type-shifts each sweep — into `'static` data.
//!   2. [`ChargeSweepInputs::read`] runs on the blocking pool
//!      (`spawn_blocking`): host RAM / cgroup reading once plus each
//!      worker's memory charge. A worker that died mid-sweep yields a
//!      zero charge, never a sweep-fatal error.
//!   3. [`OomWatcher::apply_sweep`] writes the charges back into each
//!      slot, refreshes the snapshot, runs kernel-OOM delta detection,
//!      and emits a structured log line when a trigger fires.
//!   4. The caller runs the pressure decision inline
//!      ([`OomWatcher::on_decision`]) on the just-applied charges, then
//!      `sleep`s [`SAMPLE_SWEEP_INTERVAL`] before the NEXT sweep
//!      (await-before-resleep: a slow sweep cannot pile).
//!
//! One operational-loop wakeup per sweep replaces the former
//! per-fire sample + decision timer arms.

pub mod disconnect;
pub mod probe;

pub use disconnect::{DisconnectFault, classify_disconnect, classify_disconnect_fault};

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dynrunner_core::{Identifier, ResourceKind, ResourceMap, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::Scheduler;

use crate::monitor::{MemoryCharge, measure_worker_charge};
use crate::pool::{ResourcePressureResult, WorkerPool};

use self::probe::{CpuStat, HostMemoryReading, ProcSysProbe, SystemProbe, cpu_busy_milli_excl_self};

/// Default cadence of the self-paced charge sweep: the sweep task
/// `sleep`s this long after EACH sweep completes (await-before-resleep
/// — a slow sweep cannot pile, the next starts this interval after the
/// previous returned). 50ms = the historical 20Hz sample cadence.
pub const SAMPLE_SWEEP_INTERVAL: Duration = DEFAULT_SAMPLE_INTERVAL;

/// The per-worker inputs the blocking charge read consumes: the slot's
/// id, its tracked pid (for the `/proc` fallback), and its cgroup-v2
/// leaf directory (cgroup-first read). Plain owned data so the whole
/// set can be moved into a `spawn_blocking` closure with NO pool borrow
/// held across the blocking call.
#[derive(Debug, Clone)]
struct WorkerReadInput {
    worker_id: WorkerId,
    pid: Option<u32>,
    cgroup_dir: Option<PathBuf>,
}

/// Everything one blocking sweep needs, captured as `'static + Send`
/// owned data: the (shared) probe and the current worker set's read
/// inputs. Built off the pool on the async side via
/// [`OomWatcher::collect_sweep_inputs`]; its [`Self::read`] runs on the
/// blocking pool and touches `/proc` + cgroup files only.
pub struct ChargeSweepInputs {
    probe: Arc<dyn SystemProbe>,
    workers: Vec<WorkerReadInput>,
}

impl ChargeSweepInputs {
    /// Number of workers this sweep will read charges for. Read off
    /// the inputs BEFORE moving them into the blocking pool — the
    /// caller uses it for per-sweep telemetry (#586).
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Perform every blocking file read for this sweep: the host /
    /// cgroup reading once, plus each worker's memory charge. Pure
    /// blocking IO — intended to run inside `spawn_blocking`. A
    /// per-worker read that finds no measurable charge yields a zero
    /// [`MemoryCharge`] (the historical "nothing measured" disposition
    /// of [`measure_worker_charge`]); a worker that died mid-sweep
    /// therefore contributes a zero charge rather than failing the
    /// whole sweep.
    pub fn read(self) -> ChargeSweep {
        let host = self.probe.read();
        let charges = self
            .workers
            .into_iter()
            .map(|w| {
                let charge = measure_worker_charge(w.pid, w.cgroup_dir.as_deref());
                (w.worker_id, charge)
            })
            .collect();
        ChargeSweep { host, charges }
    }

    /// Test seam: assemble a sweep's inputs directly from
    /// `(worker_id, pid, cgroup_dir)` triples + a probe, without a
    /// `WorkerPool` fixture. Lets the read-resilience / cadence tests
    /// drive [`Self::read`] against tempdir cgroup leaves and a mock /
    /// slow probe.
    #[cfg(test)]
    pub(crate) fn for_test(
        probe: Arc<dyn SystemProbe>,
        workers: Vec<(WorkerId, Option<u32>, Option<PathBuf>)>,
    ) -> Self {
        Self {
            probe,
            workers: workers
                .into_iter()
                .map(|(worker_id, pid, cgroup_dir)| WorkerReadInput {
                    worker_id,
                    pid,
                    cgroup_dir,
                })
                .collect(),
        }
    }
}

/// The plain-data result of one [`ChargeSweepInputs::read`]: the host
/// reading and per-worker `(WorkerId, MemoryCharge)` pairs. Carries no
/// borrow — it crosses back from the blocking pool to the async side,
/// where [`OomWatcher::apply_sweep`] writes it into the pool and the
/// watcher snapshot.
pub struct ChargeSweep {
    host: HostMemoryReading,
    charges: Vec<(WorkerId, MemoryCharge)>,
}

impl ChargeSweep {
    /// Test seam: the per-worker `(WorkerId, MemoryCharge)` pairs this
    /// sweep read, for asserting read resilience without a pool.
    #[cfg(test)]
    pub(crate) fn charges_for_test(&self) -> &[(WorkerId, MemoryCharge)] {
        &self.charges
    }
}

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
    /// Resident component of the tracked-worker sum (charged − swap).
    pub tracked_workers_rss_sum: u64,
    /// Swap component of the tracked-worker sum. Growth here is
    /// PRESSURE: pages migrating to swap shrink RSS while the worker
    /// is dying, so the swap share must be visible alongside it.
    pub tracked_workers_swap_sum: u64,
    /// Charged sum (resident + swap) — the same number the kill
    /// decision consumes per worker via `actual_usage`.
    pub tracked_workers_charged_sum: u64,
    pub tracked_workers_count: u32,
    pub captured_at: Option<Instant>,
    /// Host CPU busy fraction in milli-percent over the interval
    /// between THIS sweep and the prior one (100_000 = every core at
    /// 100% — the aggregate `/proc/stat` "cpu" line already sums
    /// across cores). `None` on the FIRST sweep (no prior to diff
    /// against), or when the probe returned no `cpu_stat` (parse
    /// failure / non-Linux), or when two reads landed inside the
    /// same tick. Consumed by the secondary's resource-stats rolling
    /// buffer (#575) and ignored by every other reader.
    pub cpu_busy_milli: Option<u32>,
}

/// Constructor knobs for [`OomWatcher`]. Splits the cadence /
/// logging policy from the probe injection so unit tests can
/// substitute a deterministic probe + clock without touching the
/// per-caller wiring.
pub struct OomWatcherConfig {
    /// The self-paced sweep cadence: the loop `sleep`s this long after
    /// EACH sweep completes before starting the next. Surfaced by
    /// [`OomWatcher::sweep_interval`].
    pub sample_interval: Duration,
    /// Cadence of the unconditional heartbeat log line. Only used
    /// when `log_enabled = true`.
    pub heartbeat_interval: Duration,
    /// Master switch for the structured JSON log emission. When
    /// `false`, the watcher still sweeps and decides (so the
    /// per-worker RSS stays current and the pool kills as before),
    /// but no `info!(target: "oom_watcher", ...)` events fire.
    pub log_enabled: bool,
}

impl Default for OomWatcherConfig {
    fn default() -> Self {
        Self {
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            log_enabled: false,
        }
    }
}

/// The watcher itself. Drives the self-paced sweep + the inline
/// pressure decision and emits the structured log line when a trigger
/// fires.
///
/// Caller pattern (both LocalManager and Secondary): one operational
/// `select!` arm parks on the sweep's stored deadline; on fire it
/// collects inputs, runs the blocking read off-runtime, applies, and
/// decides inline, then re-arms the deadline `sweep_interval` later.
/// ```ignore
/// let mut watcher = OomWatcher::new(OomWatcherConfig {
///     sample_interval: oom::SAMPLE_SWEEP_INTERVAL,
///     heartbeat_interval: oom::DEFAULT_HEARTBEAT_INTERVAL,
///     log_enabled: cfg.log_oom_watcher,
/// });
/// // ... when the sweep arm fires:
/// let inputs = watcher.collect_sweep_inputs(&pool);
/// let sweep = tokio::task::spawn_blocking(move || inputs.read()).await.unwrap();
/// watcher.apply_sweep(&mut pool, sweep);
/// match watcher.on_decision(&mut pool, &scheduler, &max, in_pressure) {
///     ResourcePressureResult::Killed { .. } => { /* caller handles */ }
///     ResourcePressureResult::NoAction => {}
/// }
/// ```
pub struct OomWatcher {
    config: OomWatcherConfig,
    probe: Arc<dyn SystemProbe>,
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
    /// Previous sweep's raw cumulative `/proc/stat` CPU readout. `None`
    /// before the first sweep, OR when the probe returned no
    /// `cpu_stat` (parse failure / non-Linux). The next sweep diffs
    /// against this to populate `OomWatcherSnapshot::cpu_busy_milli`;
    /// a sweep that itself returns no `cpu_stat` keeps the stored prev
    /// intact (so a transient parse hiccup doesn't reset the diff
    /// chain across subsequent good reads).
    last_cpu_stat: Option<CpuStat>,
    /// Previous sweep's cumulative `/proc/self/stat` CPU ticks (utime +
    /// stime). Diffed against the next sweep over the SAME interval as
    /// `last_cpu_stat` to subtract THIS process's own CPU from the host
    /// busy fraction (#575 "workload, not framework"). `None` before
    /// the first sweep or when `/proc/self/stat` was unreadable; a
    /// sweep that itself returns no self readout keeps the stored prev
    /// intact, mirroring `last_cpu_stat`. When either side is `None`
    /// the subtraction is skipped and the host fraction is reported
    /// as-is.
    last_self_cpu_ticks: Option<u64>,
}

/// The three fields the delta trigger compares against their value
/// at the last log emission. Kept together so a single struct
/// snapshot moves with each log line.
#[derive(Debug, Clone, Copy, Default)]
struct TrackedDeltaFields {
    host_ram_used: u64,
    container_memory_current: u64,
    /// Charged (resident + swap) tracked-worker sum: swap growth is
    /// pressure and must trip the delta trigger like RSS growth.
    tracked_workers_charged_sum: u64,
}

impl OomWatcher {
    /// Construct with the production [`ProcSysProbe`].
    pub fn new(config: OomWatcherConfig) -> Self {
        Self::with_probe(config, Arc::new(ProcSysProbe::new()))
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
        Self::with_probe(config, Arc::new(probe))
    }

    /// Construct with a caller-supplied probe. Used by unit tests to
    /// inject deterministic mock readings without touching `/proc`.
    pub fn with_probe(config: OomWatcherConfig, probe: Arc<dyn SystemProbe>) -> Self {
        Self {
            config,
            probe,
            pending_kill_event: false,
            last_snapshot: OomWatcherSnapshot::default(),
            last_log_at: None,
            last_log_values: TrackedDeltaFields::default(),
            last_kernel_oom_count: None,
            recent_kernel_oom_at: None,
            last_cpu_stat: None,
            last_self_cpu_ticks: None,
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

    /// The interval the self-paced sweep `sleep`s between completing
    /// one sweep and starting the next (await-before-resleep). Driven
    /// by the operator's [`OomWatcherConfig::sample_interval`] knob.
    pub fn sweep_interval(&self) -> Duration {
        self.config.sample_interval
    }

    /// Latest captured snapshot. Mostly for debugging / introspection
    /// — the watcher consumes its own state internally for the
    /// trigger logic.
    pub fn last_snapshot(&self) -> &OomWatcherSnapshot {
        &self.last_snapshot
    }

    /// Collect the per-worker read inputs for ONE sweep off the
    /// CURRENT pool, plus a cloned handle to the probe — the
    /// `'static + Send` data a [`spawn_blocking`](tokio::task::spawn_blocking)
    /// closure runs [`ChargeSweepInputs::read`] on. Reads the live
    /// worker set each call, so respawns / type-shifts between sweeps
    /// are picked up automatically and no pool borrow is held across
    /// the blocking read.
    ///
    /// Generic over the pool's transport/identifier so the same
    /// surface serves both manager modes.
    pub fn collect_sweep_inputs<M, I>(&self, pool: &WorkerPool<M, I>) -> ChargeSweepInputs
    where
        M: ManagerEndpoint + 'static,
        I: Identifier,
    {
        let workers = pool
            .workers
            .iter()
            .map(|w| WorkerReadInput {
                worker_id: w.worker_id,
                pid: w.pid,
                cgroup_dir: w.subcgroup_dir().map(Path::to_path_buf),
            })
            .collect();
        ChargeSweepInputs {
            probe: Arc::clone(&self.probe),
            workers,
        }
    }

    /// Apply a completed [`ChargeSweep`] (the blocking read's result)
    /// to the pool and the watcher: write each worker's charge into
    /// its `actual_usage` / `actual_swap_bytes`, refresh the snapshot,
    /// run kernel-OOM delta detection on the sweep's host reading, and
    /// evaluate the structured-log triggers. The cheap, loop-owned
    /// half of a sweep — runs on the async runtime with a `&mut pool`
    /// borrow, NOT on the blocking pool.
    pub fn apply_sweep<M, I>(&mut self, pool: &mut WorkerPool<M, I>, sweep: ChargeSweep)
    where
        M: ManagerEndpoint + 'static,
        I: Identifier,
    {
        self.apply_sweep_at(pool, sweep, Instant::now())
    }

    /// Test seam: [`Self::apply_sweep`] with an explicit `now` so unit
    /// tests can drive heartbeat / kernel-OOM cadence with a fake
    /// clock.
    pub fn apply_sweep_at<M, I>(
        &mut self,
        pool: &mut WorkerPool<M, I>,
        sweep: ChargeSweep,
        now: Instant,
    ) where
        M: ManagerEndpoint + 'static,
        I: Identifier,
    {
        // Write each measured charge back into its slot. A worker that
        // was respawned/removed between input collection and apply is
        // simply skipped (its id no longer indexes a slot); a slot the
        // sweep had no input for keeps its prior reading until the next
        // sweep picks it up.
        for (worker_id, charge) in sweep.charges {
            if let Some(worker) = pool.workers.get_mut(worker_id as usize) {
                worker.set_memory_charge(charge);
            }
        }

        let host = sweep.host;
        // Kernel-OOM detection: compute the per-sweep delta of the
        // workers-cgroup `oom_kill` counter. A positive delta means
        // the kernel ran the OOM-killer on that subgroup since the
        // last sweep. The window is wall-clock-based (see
        // `kernel_oom_recent`) so the manager's disconnect
        // reclassifier can correlate a worker pipe-EOF with the
        // kernel event regardless of how many sweeps landed
        // between them.
        if let Some(current) = host.kernel_oom_kill_count {
            if let Some(prev) = self.last_kernel_oom_count
                && current > prev
            {
                self.recent_kernel_oom_at = Some(now);
            }
            self.last_kernel_oom_count = Some(current);
        }
        let (charged_sum, swap_sum) = sum_worker_charge(pool);
        let tracked_workers_count = pool.workers.len() as u32;
        // Derive the host CPU busy fraction over the interval between
        // THIS sweep and the prior one (#575), with THIS process's own
        // CPU subtracted so the figure reflects the WORKLOAD rather
        // than framework overhead. The prior cumulative host reading
        // lives on `last_cpu_stat` and the prior own-process ticks on
        // `last_self_cpu_ticks`; both are diffed over the SAME interval
        // (so the own share is in the same unit as the host fraction
        // and is directly subtractable). We update each prev ONLY when
        // the probe surfaced a fresh reading, so a transient parse
        // hiccup does not reset the diff chain across subsequent good
        // reads. A missing self readout falls back to NOT subtracting
        // (host as-is) rather than erroring.
        let cpu_busy = match (self.last_cpu_stat, host.cpu_stat) {
            (Some(prev), Some(cur)) => cpu_busy_milli_excl_self(
                prev,
                cur,
                self.last_self_cpu_ticks,
                host.self_cpu_ticks,
            ),
            _ => None,
        };
        if let Some(cur) = host.cpu_stat {
            self.last_cpu_stat = Some(cur);
        }
        if let Some(cur_self) = host.self_cpu_ticks {
            self.last_self_cpu_ticks = Some(cur_self);
        }
        self.last_snapshot = OomWatcherSnapshot {
            host,
            tracked_workers_rss_sum: charged_sum.saturating_sub(swap_sum),
            tracked_workers_swap_sum: swap_sum,
            tracked_workers_charged_sum: charged_sum,
            tracked_workers_count,
            captured_at: Some(now),
            cpu_busy_milli: cpu_busy,
        };

        if !self.config.log_enabled {
            return;
        }
        if let Some(trigger) = self.evaluate_trigger(now) {
            self.emit_log_line(trigger, now);
        }
    }

    /// Run the scheduler's pressure decision via the pool on the
    /// charges the most recent [`Self::apply_sweep`] populated. Records
    /// "kill happened" so the next log emission carries `trigger=kill`,
    /// AND emits a kill log immediately (regardless of heartbeat /
    /// delta cadence) so the forensic record captures the kill in the
    /// same sweep window.
    ///
    /// Pure decision: no `/proc` / cgroup read happens here — the
    /// self-paced sweep is the single owner of the blocking charge
    /// read. Returns the pool's verdict verbatim so each caller's
    /// outcome handler (LocalManager monitor / Secondary resource) can
    /// take the mode-specific follow-up action.
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
        let result = pool.decide_resource_pressure(scheduler, max_resources, in_pressure_phase);
        if matches!(result, ResourcePressureResult::Killed { .. }) {
            // Refresh the snapshot's worker counts so the kill log
            // carries the post-kill tracked-worker sums. The host
            // reading stays the one this sweep already applied (the
            // decision runs inline right after `apply_sweep`, so it is
            // fresh) — no re-read on the async runtime. Re-summing the
            // pool charges is cheap and keeps the kill line
            // self-consistent with the post-kill worker set.
            let (charged_sum, swap_sum) = sum_worker_charge(pool);
            self.last_snapshot.tracked_workers_rss_sum = charged_sum.saturating_sub(swap_sum);
            self.last_snapshot.tracked_workers_swap_sum = swap_sum;
            self.last_snapshot.tracked_workers_charged_sum = charged_sum;
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
        let cur_charged_sum = snap.tracked_workers_charged_sum;

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
            || delta(cur_charged_sum, prev.tracked_workers_charged_sum) >= DELTA_TRIGGER_BYTES;

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
            "tracked_workers_swap_sum": snap.tracked_workers_swap_sum,
            "tracked_workers_charged_sum": snap.tracked_workers_charged_sum,
            "tracked_workers_count": snap.tracked_workers_count,
            "trigger": trigger.as_str(),
            "note": "tracked_workers_* sums measure the workers' own cgroup subtrees (or the worker processes themselves in the flat fallback) — daemon-delegated subprocesses NOT included; charged = rss + swap, the kill-decision input",
        });
        tracing::info!(target: "oom_watcher", oom_watcher = %json);

        self.last_log_at = Some(now);
        self.last_log_values = TrackedDeltaFields {
            host_ram_used: snap.host.host_ram_used_bytes.unwrap_or(0),
            container_memory_current: snap.host.container_memory_current.unwrap_or(0),
            tracked_workers_charged_sum: snap.tracked_workers_charged_sum,
        };
        if matches!(trigger, LogTrigger::Kill) {
            self.pending_kill_event = false;
        }
    }
}

/// Sum the `(charged, swap)` bytes across all workers in the pool.
/// Reads the already-cached `actual_usage` / `actual_swap_bytes`
/// (populated by `update_all_resource_usage` /
/// `check_resource_pressure`); no /proc or cgroup access here. The
/// memory kind in `actual_usage` carries the CHARGED bytes
/// (resident + swap — see [`crate::monitor::MemoryCharge`]); the
/// swap component rides separately on the handle.
fn sum_worker_charge<M, I>(pool: &WorkerPool<M, I>) -> (u64, u64)
where
    M: ManagerEndpoint + 'static,
    I: Identifier,
{
    let mem_kind = ResourceKind::memory();
    pool.workers
        .iter()
        .fold((0u64, 0u64), |(charged, swap), w| {
            (
                charged.saturating_add(w.actual_usage.get(&mem_kind)),
                swap.saturating_add(w.actual_swap_bytes),
            )
        })
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
