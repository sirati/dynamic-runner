//! The periodic reporter driver.
//!
//! # Single concern
//!
//! Own the two cadences (the 10-minute stats announcement and the
//! 1-minute idle-secondary check) and drive the pure sub-modules
//! (`stats` → `format` / `idle`) on each tick, emitting any wake-worthy
//! output to the importance channel. The driver holds the
//! last-announcement baseline and the idle gates; nothing about the
//! delta/inclusion rules or the idle decision leaks out of the pure
//! modules into here.
//!
//! # Inputs are injected (testability)
//!
//! The driver does not reach into a coordinator. It pulls each
//! [`StatsSnapshot`] from an injected [`CrdtSnapshotSource`] and reads
//! "now" from an injected [`Clock`]. Production wires a source that
//! projects the live CRDT and the real monotonic clock; tests wire a
//! scripted source + the paused `tokio::time` clock so both cadences
//! and the idle gate are deterministic with no wall-clock race.

use std::time::{Duration, Instant};

use dynrunner_core::IMPORTANT_TARGET;

use super::format::render_report;
use super::idle::IdleDetector;
use super::stats::StatsSnapshot;

/// The 10-minute periodic-stats cadence.
pub const STATS_INTERVAL: Duration = Duration::from_secs(600);
/// The idle-secondary poll cadence and the idle threshold are both one
/// minute: a secondary idle across a full poll interval (with ready
/// work) has been idle for ≥ the threshold.
pub const IDLE_INTERVAL: Duration = Duration::from_secs(60);
pub const IDLE_THRESHOLD: Duration = Duration::from_secs(60);

/// Source of CRDT-derived snapshots. The driver calls `snapshot()` once
/// per tick. Production projects the live replicated `ClusterState`;
/// tests return scripted snapshots.
///
/// This is the seam that decouples the reporter from how the live CRDT
/// is reached. A zero-authority observer's `ClusterState` is owned
/// `&mut` by its run loop for the loop's whole lifetime, so a
/// concurrently-running reporter needs a shared read handle to it; that
/// handle is the one piece the integration site supplies through this
/// trait.
pub trait CrdtSnapshotSource: Send {
    fn snapshot(&self) -> StatsSnapshot;
}

/// A [`CrdtSnapshotSource`] backed by a shared, swappable cell. The
/// reporter reads the most recently published snapshot; a producer
/// (the integration site, once it holds a live CRDT read handle)
/// publishes a fresh projection via [`SharedSnapshotSource::publish`].
///
/// This is the concrete seam the observer integration uses: the
/// reporter task owns a clone of the cell and the producer owns
/// another, so live CRDT projections flow in without the reporter ever
/// touching the coordinator. Until a producer publishes, `snapshot()`
/// returns the seeded value (a fresh observer's `default()` — every
/// metric zero, so the reporter correctly stays silent).
#[derive(Clone)]
pub struct SharedSnapshotSource {
    cell: std::sync::Arc<std::sync::Mutex<StatsSnapshot>>,
}

impl SharedSnapshotSource {
    pub fn new(initial: StatsSnapshot) -> Self {
        Self {
            cell: std::sync::Arc::new(std::sync::Mutex::new(initial)),
        }
    }

    /// Publish a fresh CRDT projection for the reporter to read on its
    /// next tick. Lock-poison-recovering (a panicked prior holder must
    /// not wedge the reporter).
    ///
    /// The production caller is the observer run loop's live-feed seam:
    /// it pushes a `StatsSnapshot::from_cluster_state(...)` projection
    /// right after the snapshot restore and on each
    /// `run_until_setup_or_done` return (see the live-feed doc in
    /// `observer_late_joiner/run.rs`).
    pub fn publish(&self, snapshot: StatsSnapshot) {
        let mut guard = self.cell.lock().unwrap_or_else(|p| p.into_inner());
        *guard = snapshot;
    }
}

impl CrdtSnapshotSource for SharedSnapshotSource {
    fn snapshot(&self) -> StatsSnapshot {
        self.cell.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

/// Monotonic clock seam. Production uses [`TokioClock`]; tests inject a
/// clock driven off the paused `tokio::time` virtual clock so the idle
/// threshold elapses deterministically.
pub trait Clock: Send {
    fn now(&self) -> Instant;
}

/// Production clock: `Instant::now()` (or, under a paused
/// `tokio::time`, `tokio::time::Instant::now().into_std()` — see
/// [`TokioClock`]).
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

/// Reporter state across ticks: the last ANNOUNCED snapshot (the delta
/// baseline, advanced only when a report actually emits) and the idle
/// detector. Construct once before driving the cadences.
pub struct Reporter {
    last_announced: StatsSnapshot,
    idle: IdleDetector,
}

impl Default for Reporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Reporter {
    pub fn new() -> Self {
        Self {
            last_announced: StatsSnapshot::default(),
            idle: IdleDetector::new(IDLE_THRESHOLD),
        }
    }

    /// Process one STATS tick: render against the last-announced
    /// baseline; on a non-empty report, emit it and advance the
    /// baseline. An all-omitted tick emits nothing and leaves the
    /// baseline untouched (so the next real change still diffs against
    /// the last ANNOUNCEMENT, not the last tick).
    pub fn on_stats_tick(&mut self, snapshot: &StatsSnapshot) {
        if let Some(report) = render_report(snapshot, &self.last_announced) {
            // The whole report is one importance-channel event so the
            // dual-sink routes it to stdio atomically under
            // `--important-stdio-only` (C1's filter keys on the target).
            tracing::info!(target: IMPORTANT_TARGET, "periodic cluster stats (10m):\n{report}");
            self.last_announced = snapshot.clone();
        }
    }

    /// Process one IDLE tick: fold the snapshot into the gates and emit
    /// one alert per newly-stalled secondary.
    pub fn on_idle_tick(&mut self, snapshot: &StatsSnapshot, now: Instant) {
        for secondary in self.idle.tick(snapshot, now) {
            tracing::info!(
                target: IMPORTANT_TARGET,
                secondary = %secondary,
                "secondary has been idle (0 in-flight tasks) for ≥1 minute while ready work is queued"
            );
        }
    }
}

/// Drive both cadences until `cancel` resolves. Pulls a fresh snapshot
/// from `source` on every tick. Cancel-safe: each arm awaits a tokio
/// interval tick or the cancel future, all cancel-safe; dropping the
/// driver abandons the in-flight tick cleanly.
///
/// Production spawns this concurrently with the observer run loop and
/// cancels it when the run loop returns. The 10-minute and 1-minute
/// intervals are separate `tokio::time::interval`s so a paused-clock
/// test advances each independently.
pub async fn run_reporter<S, C, F>(source: S, clock: C, cancel: F)
where
    S: CrdtSnapshotSource,
    C: Clock,
    F: std::future::Future<Output = ()>,
{
    let mut reporter = Reporter::new();
    let mut stats_interval = tokio::time::interval(STATS_INTERVAL);
    let mut idle_interval = tokio::time::interval(IDLE_INTERVAL);
    // Both intervals fire immediately on first poll by default; skip
    // that initial burst so the first stats report lands one full
    // period in (a fresh observer has nothing wake-worthy at t=0) and
    // the first idle check is one threshold in.
    stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    idle_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = stats_interval.tick().await; // consume the immediate tick
    let _ = idle_interval.tick().await; // consume the immediate tick

    tokio::pin!(cancel);
    loop {
        tokio::select! {
            _ = stats_interval.tick() => {
                let snapshot = source.snapshot();
                reporter.on_stats_tick(&snapshot);
            }
            _ = idle_interval.tick() => {
                let snapshot = source.snapshot();
                reporter.on_idle_tick(&snapshot, clock.now());
            }
            _ = &mut cancel => break,
        }
    }
}
