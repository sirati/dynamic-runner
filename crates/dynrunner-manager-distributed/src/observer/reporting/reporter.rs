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

use super::format::{ResourceBaseline, render_report, render_report_full};
use super::idle::IdleDetector;
use super::stats::StatsSnapshot;
use crate::observer::lost_visibility::{EndedOutage, WakeNoteSlot};

/// The 10-minute periodic-stats cadence.
pub const STATS_INTERVAL: Duration = Duration::from_secs(600);
/// The 1-hour safety-net cadence in units of [`STATS_INTERVAL`]: every
/// 6th 10-minute grid tick since the last emission bypasses the
/// skip-eligible predicate (so a routine-throughput-only run is still
/// reported at least once an hour, with the accumulated delta against
/// the LAST-PRINTED snapshot — the safety net's whole point).
pub const SAFETY_NET_TICKS: u8 = 6;
/// The idle-secondary poll cadence and the idle threshold are both one
/// minute: a secondary idle across a full poll interval (with ready
/// work) has been idle for ≥ the threshold.
pub const IDLE_INTERVAL: Duration = Duration::from_secs(60);
pub const IDLE_THRESHOLD: Duration = Duration::from_secs(60);
/// Minimum spacing between a LATE stats log (run immediately on
/// reconnection after a logged outage that swallowed ≥1 grid occurrence)
/// and the next ON-GRID occurrence: if the next scheduled occurrence
/// would land less than this after the late one, that single occurrence
/// is skipped (the one after it fires on the original grid — the grid
/// itself NEVER shifts). The owner's spec sets this to 5 minutes.
pub const LATE_STATS_MIN_SPACING: Duration = Duration::from_secs(300);

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
    /// The production caller is the `ObserverCoordinator` run loop's
    /// live-feed seam: it pushes a
    /// `StatsSnapshot::from_cluster_state(...)` projection on each loop
    /// iteration (see the publish site in `observer/coordinator.rs`).
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
/// baseline, advanced only when a report actually emits), the idle
/// detector, and the 1-hour safety-net counter. Construct once before
/// driving the cadences.
pub struct Reporter {
    last_announced: StatsSnapshot,
    /// Per-field LAST-PRINTED baseline for the #575 resource-stat
    /// averages (held alongside `last_announced` because the
    /// resource lines advance per-field on emission, not atomically
    /// with the whole snapshot — see `format::render_report`'s
    /// returned `next_resource_baseline`). A resource line that was
    /// OMITTED leaves its baseline slot untouched, so the next emit
    /// decides inclusion against the same prior value the operator
    /// last saw.
    last_printed_resource: ResourceBaseline,
    idle: IdleDetector,
    /// Grid ticks consumed since the last actual emission. Increments on
    /// every grid tick the reporter processes (whether it emits or
    /// skips); resets to 0 on any emission. When `>= SAFETY_NET_TICKS - 1`
    /// at the START of a tick, this tick is the 1-hour safety net and
    /// bypasses the skip-eligible predicate so the accumulated delta of
    /// any skipped throughput ticks is reported. The 1-hour grid is
    /// measured from the LAST-PRINTED announcement (invariant 3): the
    /// safety net fires "at least once per 6 ticks of silence", not on a
    /// wall clock — so it tracks exactly the operator-visible quiet
    /// window.
    ticks_since_print: u8,
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
            last_printed_resource: ResourceBaseline::default(),
            idle: IdleDetector::new(IDLE_THRESHOLD),
            ticks_since_print: 0,
        }
    }

    /// Process one STATS tick with the 10-min skip predicate + the
    /// 1-hour safety net (the production cadence arm). On a tick whose
    /// diff against the last announcement is a subset of the
    /// skip-eligible counter set AND the safety net is not due, this
    /// returns `false` WITHOUT emitting and WITHOUT advancing the
    /// last-announced baseline — so the next 10-min tick still diffs
    /// against the same baseline and the skipped delta accumulates
    /// (invariant 1: skipped ticks never advance last_printed;
    /// invariant 3: the eventual 1-hour print's delta is against the
    /// last actually-printed snapshot).
    ///
    /// When the safety net IS due (`ticks_since_print + 1 >=
    /// SAFETY_NET_TICKS`) the predicate is bypassed and this delegates
    /// to [`on_stats_tick`] regardless of which fields moved. A safety
    /// net tick with diff = ∅ still elides (owner-approved: the
    /// emission is silent when there is literally nothing wake-worthy
    /// to say; the operator can use SIGUSR1 for a heartbeat read).
    ///
    /// Returns whether a report was emitted — same contract as the
    /// unskipping `on_stats_tick`.
    pub fn on_stats_tick_skippable(&mut self, snapshot: &StatsSnapshot) -> bool {
        let safety_net_due = self.ticks_since_print + 1 >= SAFETY_NET_TICKS;
        if !safety_net_due && snapshot.diff_subset_of_skip_eligible(&self.last_announced) {
            // Routine throughput-only change AND the 1-hour boundary is
            // not yet due: elide. The counter advances because the grid
            // tick was still consumed; the next tick is one step closer
            // to the safety net.
            self.ticks_since_print = self.ticks_since_print.saturating_add(1);
            return false;
        }
        // Either the diff includes a non-throughput field, or the
        // safety-net boundary is due: run the normal delta-emit path.
        // `on_stats_tick` resets `ticks_since_print` on a genuine
        // emission; an all-omitted hour boundary advances the counter
        // like any other skip (so subsequent ticks keep treating the
        // run as "overdue for a print" until something emits).
        let emitted = self.on_stats_tick(snapshot);
        if !emitted {
            self.ticks_since_print = self.ticks_since_print.saturating_add(1);
        }
        emitted
    }

    /// Process one STATS tick: render against the last-announced
    /// baseline; on a non-empty report, emit it and advance the
    /// baseline. An all-omitted tick emits nothing and leaves the
    /// baseline untouched (so the next real change still diffs against
    /// the last ANNOUNCEMENT, not the last tick). Returns whether a
    /// report was emitted (the caller flushes the wake-note slot after a
    /// genuine emission — the emitted report is a wake-stream host).
    pub fn on_stats_tick(&mut self, snapshot: &StatsSnapshot) -> bool {
        let outcome = render_report(snapshot, &self.last_announced, &self.last_printed_resource);
        if let Some(report) = outcome.body {
            // The whole report is one importance-channel event so the
            // dual-sink routes it to stdio atomically under
            // `--important-stdio-only` (C1's filter keys on the target).
            tracing::info!(target: IMPORTANT_TARGET, "periodic cluster stats (10m):\n{report}");
            // Advance the operational baseline atomically + the
            // per-field resource baseline per-line (the renderer wrote
            // each included field into `next_resource_baseline`; an
            // omitted line preserves its slot).
            self.last_announced = snapshot.clone();
            self.last_printed_resource = outcome.next_resource_baseline;
            self.ticks_since_print = 0;
            true
        } else {
            false
        }
    }

    /// Process an operator-driven FORCE-PRINT (SIGUSR1 against the
    /// observer). Always emits the full snapshot (every field, including
    /// unchanged and zero values — invariant 4) on the importance
    /// channel, advances the last-announced baseline, and resets the
    /// 1-hour safety-net counter. After this the next 10-min tick diffs
    /// against the post-signal snapshot (invariant 5).
    ///
    /// Returns `true` unconditionally — the force-print is the
    /// operator's explicit "show me everything", so the caller flushes
    /// the wake-note slot exactly as it would for a periodic emission.
    pub fn on_force_print(&mut self, snapshot: &StatsSnapshot) -> bool {
        let report = render_report_full(snapshot);
        tracing::info!(
            target: IMPORTANT_TARGET,
            "cluster stats (force-print):\n{report}"
        );
        self.last_announced = snapshot.clone();
        // The force-print emits EVERY currently-`Some` resource line, so
        // advance the per-field baseline to the just-printed values (an
        // omitted "None" field stays None in the baseline — there is
        // nothing to compare against next time anyway). Mirrors the
        // operational baseline reset above (every field printed → every
        // field advanced).
        self.last_printed_resource = ResourceBaseline {
            mem_p10_bytes: snapshot.avg_mem_p10_bytes,
            mem_p30_bytes: snapshot.avg_mem_p30_bytes,
            mem_p50_bytes: snapshot.avg_mem_p50_bytes,
            mem_p70_bytes: snapshot.avg_mem_p70_bytes,
            mem_p90_bytes: snapshot.avg_mem_p90_bytes,
            mem_avg_bytes: snapshot.avg_mem_avg_bytes,
            total_free_memory_bytes: snapshot.avg_total_free_memory_bytes,
            total_swap_used_bytes: snapshot.avg_total_swap_used_bytes,
            total_free_swap_bytes: snapshot.avg_total_free_swap_bytes,
            cpu_utilization_milli: snapshot.avg_cpu_utilization_milli,
            // #589 loop-health baseline advance — same rule as the
            // #575 resource fields: every Some line printed by the
            // force-path advances its slot; the dominant-arm baseline
            // is the SHARE (the gate axis), not the name.
            oploop_iters_per_sec_milli: snapshot.avg_oploop_iters_per_sec_milli,
            dominant_arm_pct_milli: snapshot.dominant_arm.as_ref().map(|v| v.pct_milli),
            max_unacked_for_secs: snapshot.max_unacked_for_secs,
        };
        self.ticks_since_print = 0;
        true
    }

    /// Process one IDLE tick: fold the snapshot into the gates and emit
    /// one alert per newly-stalled secondary. Returns whether ≥1 alert
    /// was emitted (a wake-stream host for the note flush).
    pub fn on_idle_tick(&mut self, snapshot: &StatsSnapshot, now: Instant) -> bool {
        let mut emitted = false;
        for secondary in self.idle.tick(snapshot, now) {
            tracing::info!(
                target: IMPORTANT_TARGET,
                secondary = %secondary,
                "secondary has been idle (0 in-flight tasks) for ≥1 minute while ready work is queued"
            );
            emitted = true;
        }
        emitted
    }

    /// Flush a final stats line before observer exit; delegates to
    /// [`on_stats_tick`](Self::on_stats_tick) (a short run renders every
    /// nonzero metric; a steady run with no change stays silent).
    /// Returns whether a report was emitted, like the tick it delegates
    /// to.
    pub fn flush_final(&mut self, snapshot: &StatsSnapshot) -> bool {
        self.on_stats_tick(snapshot)
    }
}

/// Grid bookkeeping for the periodic stats log across a connection
/// outage. Owned HERE because the GRID is this module's concern: the
/// loss policy ([`crate::observer::lost_visibility`]) only says "a logged
/// outage just ended, it began at T"; whether a grid occurrence elapsed
/// inside the down window, whether a late run is due, and whether the
/// following on-grid occurrence must be skipped are decisions of the
/// grid's owner.
///
/// # The grid never shifts
///
/// The `tokio::time::interval` driving the stats cadence is NEVER
/// touched: a late run is an EXTRA `on_stats_tick` invocation, and a
/// skip is a consumed-but-not-run tick. Both leave the original schedule
/// intact (the occurrence after a skipped one fires on the original
/// grid).
///
/// # While the connection is down (current behaviour, preserved)
///
/// The periodic reporter has NO connectivity input: grid ticks keep
/// firing during an outage and apply the normal `>0`-and-changed delta
/// rule. With the CRDT mirror frozen (no inbound frames) those ticks are
/// typically silent, but a tick that still sees un-announced
/// pre-outage changes emits exactly as it always did. This struct adds
/// no down-gating — it only RECORDS each tick so the late-run decision
/// ("did an occurrence elapse while down?") can be answered at regain.
#[derive(Debug, Default)]
pub(crate) struct StatsGridGate {
    /// The instant of the most recent grid occurrence (skipped or run).
    last_grid_tick: Option<Instant>,
    /// `Some(t)` after a late stats log EMITTED at `t`: the IMMEDIATELY
    /// next grid occurrence is skipped iff it lands within
    /// [`LATE_STATS_MIN_SPACING`] of `t`. Cleared by that next occurrence
    /// either way — only ever one skip candidate per late run.
    late_emit: Option<Instant>,
}

impl StatsGridGate {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record a grid occurrence at `now` and decide whether to RUN it.
    /// Returns `false` exactly when this is the single occurrence
    /// following a late emit AND it lands less than
    /// [`LATE_STATS_MIN_SPACING`] after that late emit (the spec's
    /// skip-one exception); `true` otherwise.
    pub(crate) fn grid_tick(&mut self, now: Instant) -> bool {
        self.last_grid_tick = Some(now);
        match self.late_emit.take() {
            Some(late) => now.duration_since(late) >= LATE_STATS_MIN_SPACING,
            None => true,
        }
    }

    /// Whether a late stats run is due for an outage that began at
    /// `down_since`: true iff ≥1 grid occurrence elapsed while the
    /// connection was down (the most recent occurrence landed at or after
    /// the loss instant). `false` when no occurrence has ever fired.
    pub(crate) fn late_run_due(&self, down_since: Instant) -> bool {
        self.last_grid_tick.is_some_and(|t| t >= down_since)
    }

    /// Record that a late stats log actually EMITTED at `now`, arming the
    /// skip-one check for the next grid occurrence. A late run whose
    /// delta rendered nothing does NOT arm the skip (no spam to avoid —
    /// the next on-grid occurrence is then the first emission).
    pub(crate) fn record_late_emit(&mut self, now: Instant) {
        self.late_emit = Some(now);
    }
}

/// Drive both cadences until `cancel` resolves. Pulls a fresh snapshot
/// from `source` on every tick. Cancel-safe: each arm awaits a tokio
/// interval tick, an `UnboundedReceiver::recv` (cancel-safe per tokio's
/// docs — a sibling win cannot lose a queued signal), or the cancel
/// future; dropping the driver abandons the in-flight tick cleanly.
///
/// Production spawns this concurrently with the observer run loop and
/// cancels it when the run loop returns. The 10-minute and 1-minute
/// intervals are separate `tokio::time::interval`s so a paused-clock
/// test advances each independently.
///
/// # The wake-stream outage seam
///
/// `outage_rx` carries the loss policy's [`EndedOutage`] signal (a
/// LOGGED outage just regained visibility). If ≥1 grid occurrence
/// elapsed inside the down window ([`StatsGridGate::late_run_due`]), ONE
/// stats log runs immediately — naturally carrying the parked
/// reconnection note, since every emission here flushes the shared
/// [`WakeNoteSlot`] right after it emits. The grid never shifts; the
/// single next occurrence is skipped iff it would land within
/// [`LATE_STATS_MIN_SPACING`] of the late emission. Grid ticks while the
/// connection is down keep their pre-existing behaviour (they run the
/// normal delta rule — see [`StatsGridGate`]).
pub async fn run_reporter<S, C, F>(
    source: S,
    clock: C,
    outage_rx: tokio::sync::mpsc::UnboundedReceiver<EndedOutage>,
    force_print_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    note: WakeNoteSlot,
    cancel: F,
) where
    S: CrdtSnapshotSource,
    C: Clock,
    F: std::future::Future<Output = ()>,
{
    let mut reporter = Reporter::new();
    let mut grid_gate = StatsGridGate::new();
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

    // Once a sender side drops (the run loop is tearing down; cancel is
    // imminent) the arm parks instead of hot-looping on `None`. The
    // force-print receiver follows the same idiom as the outage
    // receiver — closed channel ⇒ parked arm.
    let mut outage_rx = Some(outage_rx);
    let mut force_print_rx = Some(force_print_rx);

    tokio::pin!(cancel);
    loop {
        tokio::select! {
            _ = stats_interval.tick() => {
                if grid_gate.grid_tick(clock.now()) {
                    // Routine periodic arm: the skippable path applies
                    // the 10-min skip-eligible predicate AND the 1-hour
                    // safety net (see `Reporter::on_stats_tick_skippable`).
                    let snapshot = source.snapshot();
                    if reporter.on_stats_tick_skippable(&snapshot) {
                        note.flush_after_host();
                    }
                }
            }
            _ = idle_interval.tick() => {
                let snapshot = source.snapshot();
                if reporter.on_idle_tick(&snapshot, clock.now()) {
                    note.flush_after_host();
                }
            }
            ended = recv_outage(&mut outage_rx) => {
                if grid_gate.late_run_due(ended.down_since) {
                    // Rule 3: ≥1 grid occurrence elapsed while down — run
                    // ONE stats log immediately. The late-emit follows
                    // the un-skippable delta rule: an outage just ended
                    // and the operator must see the state, even when
                    // the only changes are routine throughput (the
                    // skip-eligible predicate is for ROUTINE 10-min
                    // ticks, not for one-off recovery emissions).
                    let snapshot = source.snapshot();
                    if reporter.on_stats_tick(&snapshot) {
                        note.flush_after_host();
                        grid_gate.record_late_emit(clock.now());
                    }
                }
            }
            forced = recv_force_print(&mut force_print_rx) => {
                let () = forced;
                // SIGUSR1 force-print: the operator explicitly asked
                // for a current status, so render the FULL snapshot
                // (every field, including unchanged + zero — see
                // `render_report_full`), advance the last-announced
                // baseline, and reset the 1-hour safety counter
                // (handled inside `on_force_print`). The wake-note
                // rides this emission like any other periodic event.
                let snapshot = source.snapshot();
                if reporter.on_force_print(&snapshot) {
                    note.flush_after_host();
                }
            }
            _ = &mut cancel => {
                if reporter.flush_final(&source.snapshot()) {
                    note.flush_after_host();
                }
                break;
            }
        }
    }
}

/// Await the next [`EndedOutage`] from an optional receiver; a closed
/// channel parks the arm (take the receiver, pend forever) instead of
/// resolving `None` in a hot loop — mirroring the coordinator's
/// `recv_panik` idiom. Cancel-safe (`UnboundedReceiver::recv` is).
async fn recv_outage(
    rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<EndedOutage>>,
) -> EndedOutage {
    match rx {
        Some(r) => match r.recv().await {
            Some(ended) => ended,
            None => {
                rx.take();
                std::future::pending().await
            }
        },
        None => std::future::pending().await,
    }
}

/// Await the next SIGUSR1 force-print delivery from the optional
/// receiver; mirrors [`recv_outage`]'s closed-channel-parks idiom so the
/// select arm never hot-loops on `None`. Cancel-safe
/// (`UnboundedReceiver::recv` is).
async fn recv_force_print(
    rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<()>>,
) -> () {
    match rx {
        Some(r) => match r.recv().await {
            Some(()) => (),
            None => {
                rx.take();
                std::future::pending().await
            }
        },
        None => std::future::pending().await,
    }
}
