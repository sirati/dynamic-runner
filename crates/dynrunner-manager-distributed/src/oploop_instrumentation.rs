//! [`OpLoopArmStats`] — per-iteration `select!`-arm accounting for an
//! operational loop, for naming a future production wedge's hot arm.
//!
//! # Concern (and ONLY this concern)
//!
//! OBSERVE which `select!` arm of an operational loop won each iteration, how
//! much WALL-CLOCK TIME the loop spent in each arm's body vs WAITING in the
//! `select!` (the `idle` pseudo-arm), and how long the loop has gone without
//! the INBOUND (ingest) arm winning. Pure observation: the per-iteration path
//! is a handful of relaxed atomic stores plus one uncontended cursor lock, NO
//! allocation, NO behaviour change. The production ingest-wedge signature
//! ("relocated primary ingests exactly 4 of 16, never returns to its inbox;
//! ~97% CPU spin through the wedge") is, in arm terms, "some OTHER arm wins
//! every iteration while the inbox arm never wins again". This component
//! converts that into one log line — `arm_counts=[...]`, `arm_ms=[idle=..,
//! ...]`, `since_inbox=K`, `last_arm=X` — so the next occurrence names its own
//! arm, and the TIME axis distinguishes a healthy idle loop (idle dominates)
//! from an overloaded one (a slow arm body dominates).
//!
//! # Why a separate component
//!
//! The recording is shared between the loop (writer, on the watched runtime)
//! and the off-runtime [`crate::runtime_watchdog`] checker thread (reader, on
//! its own OS thread). Wrapping the counters + the per-arm/idle time
//! accumulators in an [`Arc`] of relaxed atomics lets BOTH halves touch them
//! without blocking each other: the loop never blocks on the watchdog, and the
//! watchdog reads a coherent-enough snapshot even while the runtime is
//! wedged/spinning (the watchdog's whole reason to exist). The only non-atomic
//! piece — the in-flight timing cursor that splits idle vs body — is touched
//! ONLY by the loop writer (in `begin_select`/`record`), never by the
//! watchdog reader, so the no-blocking-watchdog invariant is preserved. The
//! cadence policy for the loop's own periodic emit + starvation WARN lives
//! HERE (not in the loop body) so the loop stays a pure `record(arm)` caller
//! and the threshold/rate-limit knobs are unit-testable in isolation.
//!
//! # Boundary
//!
//! - Loop side: build one [`OpLoopArmStats`] per loop entry (naming its arms +
//!   which one is the inbound arm), call [`OpLoopArmStats::begin_select`] once
//!   per iteration immediately before the `tokio::select!`, and
//!   [`OpLoopArmStats::record`] once with the winning arm's id as the first
//!   body statement. The begin_select/record pair splits each iteration's
//!   wall-clock into idle (select!-wait) and the winning arm's body; `record`
//!   also drives the internal cadence (periodic INFO stats line +
//!   rate-limited starvation WARN). The loop never sees a threshold or a
//!   timing accumulator — those policies live HERE.
//! - Watchdog / stats side: [`OpLoopArmStats::snapshot`] renders an
//!   [`ArmStatsSnapshot`] whose [`std::fmt::Display`] is the one compact line.
//!   The watchdog holds the same [`Arc`] and dumps the snapshot when it fires.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The pseudo-arm name for time the `select!` spent WAITING for any arm to
/// become ready — the loop's IDLE time. Not a real `select!` arm; it is
/// accumulated separately (see [`OpLoopArmStats::record_idle`]) and folds
/// into the time-based dominant-arm competition alongside the real arms so
/// a lightly-loaded loop reads as `idle:NN%` (healthy) rather than naming
/// whichever timer happened to fire most. Lives HERE (not in the loop or
/// the loop-health tracker) so every consumer of an arm-time breakdown
/// sees the SAME idle label.
pub const IDLE_ARM_NAME: &str = "idle";

/// Iterations-since-inbox past which the loop is suspected starved of its
/// ingest arm. The production wedge ran at ~97% CPU spin, so a wedged loop
/// crosses this in well under a second — but we also gate the WARN on a wall-
/// clock minimum (see [`STARVATION_WARN_MIN_ELAPSED`]) so a merely busy loop
/// that legitimately races many timer/bus arms between two ingests does not
/// cry wolf.
const STARVATION_WARN_ITER_THRESHOLD: u64 = 10_000;

/// Wall-clock minimum the inbox arm must have been starved before the WARN
/// fires, regardless of iteration count. Matches the runtime-watchdog's 30 s
/// starvation threshold: a healthy loop ingests far more often than this; a
/// 30 s ingest gap with pending work is the production wedge.
const STARVATION_WARN_MIN_ELAPSED: Duration = Duration::from_secs(30);

/// Minimum spacing between successive starvation WARN lines. A sustained wedge
/// keeps crossing the threshold every iteration; without this the loop would
/// emit a WARN per iteration (millions/min under a spin). One per 30 s tracks
/// a persistent wedge without flooding the log.
const STARVATION_WARN_COOLDOWN: Duration = Duration::from_secs(30);

/// Cadence of the periodic INFO stats line (the "stats emission" twin). Long
/// enough to be near-free on the hot path (one wall-clock read + compare per
/// iteration, an actual emit only every interval), short enough that a
/// SHORT-LIVED run (a minutes-scale validation nano) emits at least one line
/// and a manual sample window sees a fresh arm breakdown — the original 600s
/// first-emit was invisible to a 4-minute run, which defeats the line's
/// diagnostic purpose. One compact line per 2 minutes per loop is noise-free
/// at any run length.
const STATS_LINE_INTERVAL: Duration = Duration::from_secs(120);

/// Wall-clock unix milliseconds, saturating to 0 before the epoch. The same
/// unit the [`crate::runtime_watchdog`] heartbeat uses, so the two components'
/// timestamps are directly comparable in a dump.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Shared per-iteration arm accounting for ONE operational loop. Cheap to
/// `record` into (relaxed atomics, no alloc); cheap to `snapshot` out of (a
/// short read, one small `Vec` alloc for the rendered line — off the hot
/// path). Held behind an [`Arc`] so the loop and the off-runtime watchdog
/// share one instance.
pub struct OpLoopArmStats {
    /// Stable per-arm names, index == arm id. `'static` so a snapshot can
    /// borrow them without cloning on the hot path.
    arm_names: &'static [&'static str],
    /// The arm id of the INBOUND (ingest) arm — the one whose starvation is
    /// the wedge signature.
    inbox_arm: usize,
    /// Monotonic count of loop iterations recorded.
    iter: AtomicU64,
    /// Per-arm win counts, index-aligned with `arm_names`.
    counts: Vec<AtomicU64>,
    /// Per-arm cumulative BODY time in nanoseconds, index-aligned with
    /// `arm_names`. The wall-clock the loop spent IN each arm's body
    /// (from the arm winning the `select!` until the next `begin_select`).
    /// Read lock-free by the watchdog/snapshot; written only by the loop
    /// writer via the timing cursor. The TIME axis the dominant-arm
    /// selection now uses (a frequent-but-fast arm no longer "dominates"
    /// a rare-but-slow one).
    arm_nanos: Vec<AtomicU64>,
    /// Cumulative IDLE time in nanoseconds — the wall-clock the `select!`
    /// spent WAITING for any arm to become ready (between one arm's body
    /// finishing, i.e. the next `begin_select`, and the next arm winning).
    /// Competes as the [`IDLE_ARM_NAME`] pseudo-arm in the time-based
    /// dominant selection: a lightly-loaded loop's idle dominates
    /// (healthy); an overloaded loop's slow body dominates over a small
    /// idle (the wedge signature). Over any window
    /// `idle + Σ arm_nanos ≈ wall-clock`.
    idle_nanos: AtomicU64,
    /// Loop-writer-only timing cursor: the in-flight instants needed to
    /// split idle vs body. Touched ONLY by the loop (in `begin_select` +
    /// `record`) — never by the off-runtime watchdog reader (which reads
    /// the atomic accumulators above), so it never blocks the loop and the
    /// no-lock watchdog-read invariant holds. A `Mutex` (not atomics)
    /// because an `Instant` is not atomic; uncontended in practice (one
    /// writer), so the lock is effectively free.
    timing: Mutex<TimingCursor>,
    /// The last arm id that won (as `u64` for a single atomic).
    last_arm: AtomicU64,
    /// Wall-clock millis the last arm won.
    last_arm_at_millis: AtomicU64,
    /// `iter` value the inbox arm last won at — `since_inbox = iter - this`.
    iter_at_last_inbox: AtomicU64,
    /// Wall-clock millis the inbox arm last won — drives the time-axis
    /// starvation gate independent of iteration count.
    inbox_at_millis: AtomicU64,
    /// Wall-clock millis the last starvation WARN fired (0 = never).
    last_warn_at_millis: AtomicU64,
    /// Wall-clock millis the last periodic stats line emitted (0 = never).
    last_stats_at_millis: AtomicU64,
}

/// The loop-writer-only in-flight timing state that splits each
/// iteration's wall-clock into IDLE (the `select!` wait) and the winning
/// arm's BODY. Two transition points drive it, in source order each
/// iteration: [`OpLoopArmStats::begin_select`] (just before the
/// `tokio::select!`) and [`OpLoopArmStats::record`] (the FIRST statement of
/// the winning arm's body). The window between `begin_select` and the next
/// `record` is IDLE; the window between a `record` and the next
/// `begin_select` is that arm's BODY (it also absorbs the cheap loop-top
/// bookkeeping of the next iteration — loop overhead, honestly NOT idle).
///
/// Pure cursor — holds no accumulation; on each transition it folds the
/// elapsed span into the [`OpLoopArmStats`] atomic accumulators and stores
/// the new mark. `None` marks are the loop-entry / first-transition state.
#[derive(Debug, Default)]
struct TimingCursor {
    /// Instant the most recent `begin_select` happened (the IDLE-window
    /// open). `None` before the first `begin_select`.
    select_enter: Option<Instant>,
    /// The arm currently executing its body and the instant it won the
    /// `select!` (the BODY-window open). `None` between a `begin_select`
    /// and the next arm winning (i.e. while idle / waiting).
    running: Option<(usize, Instant)>,
}

impl OpLoopArmStats {
    /// Build a fresh stats block for a loop with the given `arm_names`
    /// (index == arm id) and the index of the inbound arm. `inbox_arm` MUST be
    /// a valid index into `arm_names`.
    pub fn new(arm_names: &'static [&'static str], inbox_arm: usize) -> Arc<Self> {
        assert!(
            inbox_arm < arm_names.len(),
            "inbox_arm index out of range for arm_names"
        );
        let now = now_millis();
        Arc::new(Self {
            arm_names,
            inbox_arm,
            iter: AtomicU64::new(0),
            counts: arm_names.iter().map(|_| AtomicU64::new(0)).collect(),
            arm_nanos: arm_names.iter().map(|_| AtomicU64::new(0)).collect(),
            idle_nanos: AtomicU64::new(0),
            timing: Mutex::new(TimingCursor::default()),
            last_arm: AtomicU64::new(0),
            last_arm_at_millis: AtomicU64::new(now),
            iter_at_last_inbox: AtomicU64::new(0),
            // Seed the inbox clock to "now" so the first 30 s of a loop that
            // has not yet had a single ingest does not instantly read as a
            // 30 s starvation; the loop-entry sweep + first events settle in.
            inbox_at_millis: AtomicU64::new(now),
            last_warn_at_millis: AtomicU64::new(0),
            last_stats_at_millis: AtomicU64::new(now),
        })
    }

    /// Record that `arm` won this iteration, then drive the internal cadence
    /// (periodic INFO stats line + rate-limited starvation WARN). The hot-path
    /// cost is a handful of relaxed atomic stores plus one wall-clock read for
    /// the cadence compare; the actual log emits happen only at their
    /// intervals. `arm` out of range is recorded against `iter`/`last_arm`
    /// only (defensive — a caller bug, never the steady state).
    pub fn record(&self, arm: usize) {
        self.record_at(arm, Instant::now())
    }

    /// As [`Self::record`] but against a caller-supplied monotonic `at`
    /// instant — the timing seam the unit tests drive deterministically
    /// (inject instants, never sleep). The production [`Self::record`]
    /// passes `Instant::now()`. Closes the IDLE window opened by the prior
    /// [`Self::begin_select`] (`at - select_enter` ⇒ idle) and opens this
    /// arm's BODY window. The count/cadence accounting is unchanged.
    pub fn record_at(&self, arm: usize, at: Instant) {
        // Time-axis: close the idle window (begin_select → this win) and
        // open the body window. Loop-writer-only cursor; recovers a poison
        // so observation never widens a fault.
        {
            let mut cur = self.timing.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(enter) = cur.select_enter.take() {
                self.record_idle(at.saturating_duration_since(enter));
            }
            cur.running = Some((arm, at));
        }

        let now = now_millis();
        let iter = self.iter.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(cell) = self.counts.get(arm) {
            cell.fetch_add(1, Ordering::Relaxed);
        }
        self.last_arm.store(arm as u64, Ordering::Relaxed);
        self.last_arm_at_millis.store(now, Ordering::Relaxed);
        if arm == self.inbox_arm {
            self.iter_at_last_inbox.store(iter, Ordering::Relaxed);
            self.inbox_at_millis.store(now, Ordering::Relaxed);
        }
        self.maybe_emit(now);
    }

    /// Mark the loop about to AWAIT its `select!` — the loop's ONLY
    /// per-iteration timing call (the `record` per arm is the existing
    /// count call). Closes the BODY window of the arm that was running
    /// (`now - running_start` ⇒ that arm's body time, which also absorbs
    /// the cheap loop-top bookkeeping of this iteration — loop overhead,
    /// not idle) and opens the IDLE window. Call it immediately before
    /// `tokio::select! { ... }`. The production caller passes
    /// `Instant::now()`; tests inject an instant.
    pub fn begin_select(&self, now: Instant) {
        let mut cur = self.timing.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((arm, start)) = cur.running.take() {
            self.record_arm_time(arm, now.saturating_duration_since(start));
        }
        cur.select_enter = Some(now);
    }

    /// Accumulate `dur` against `arm`'s cumulative body time. The
    /// low-level time accumulator (the [`Self::begin_select`] /
    /// [`Self::record_at`] cursor folds the split spans through here);
    /// public so the share arithmetic is unit-testable by feeding
    /// durations directly without a cursor or a clock. `arm` out of range
    /// is a defensive no-op (a caller bug, never the steady state).
    pub fn record_arm_time(&self, arm: usize, dur: Duration) {
        if let Some(cell) = self.arm_nanos.get(arm) {
            cell.fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Accumulate `dur` against the IDLE pseudo-arm (the `select!` wait).
    /// Low-level twin of [`Self::record_arm_time`]; driven by the cursor
    /// in [`Self::record_at`] and exposed for direct unit testing.
    pub fn record_idle(&self, dur: Duration) {
        self.idle_nanos
            .fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Cadence policy, factored out of `record` so the loop body never sees a
    /// threshold. Decides — from cheap atomic reads ONLY, no allocation — when
    /// to emit the periodic stats line and the starvation WARN, and builds the
    /// renderable snapshot ONLY on those emit edges. The steady-state hot path
    /// is two `u64` compares plus the gate arithmetic; the one `Vec` alloc the
    /// snapshot costs happens at most once per `STATS_LINE_INTERVAL` /
    /// `STARVATION_WARN_COOLDOWN`.
    fn maybe_emit(&self, now: u64) {
        // Periodic stats line (the "stats emission" twin).
        let last_stats = self.last_stats_at_millis.load(Ordering::Relaxed);
        if now.saturating_sub(last_stats) >= STATS_LINE_INTERVAL.as_millis() as u64 {
            self.last_stats_at_millis.store(now, Ordering::Relaxed);
            self.emit_stats_line(now);
        }

        // Starvation gate — derived from two atomic reads, NO snapshot alloc.
        let since_inbox = self
            .iter
            .load(Ordering::Relaxed)
            .saturating_sub(self.iter_at_last_inbox.load(Ordering::Relaxed));
        let inbox_idle =
            Duration::from_millis(now.saturating_sub(self.inbox_at_millis.load(Ordering::Relaxed)));
        if starvation_warn_due(
            since_inbox,
            inbox_idle,
            self.last_warn_at_millis.load(Ordering::Relaxed),
            now,
        ) {
            self.last_warn_at_millis.store(now, Ordering::Relaxed);
            // Build the renderable snapshot only now, on the rate-limited WARN
            // edge.
            let snap = self.snapshot_at(now);
            tracing::warn!(
                oploop = %snap,
                since_inbox = snap.since_inbox,
                inbox_idle_secs = snap.inbox_idle.as_secs(),
                "oploop INBOUND arm starved — the ingest arm has not won within \
                 the iteration/time threshold; the loop is servicing other arms \
                 only (the ingest-wedge signature)"
            );
        }
    }

    /// Emit ONE periodic-shape stats line at `now` — the single source of truth
    /// for the `"oploop arm stats"` line that both the interval-gated
    /// [`Self::maybe_emit`] and the unconditional [`Self::emit_final`] produce.
    /// Builds the renderable snapshot, logs it, and returns it so a test can
    /// assert the exact rendered content without intercepting `tracing` (the
    /// tracing call stays a thin one-liner; the line shape lives in
    /// [`ArmStatsSnapshot`]'s [`std::fmt::Display`]). NOT a gate — the caller
    /// owns the decision to emit; this method just renders + logs.
    fn emit_stats_line(&self, now: u64) -> ArmStatsSnapshot {
        let snap = self.snapshot_at(now);
        tracing::info!(oploop = %snap, "oploop arm stats");
        snap
    }

    /// Emit ONE final stats line UNCONDITIONALLY, bypassing the
    /// [`STATS_LINE_INTERVAL`] gate that [`Self::maybe_emit`] applies. Called
    /// exactly once on the operational loop's termination so EVERY run — even a
    /// short burst that completes inside one interval and so never tripped the
    /// periodic emit — leaves at least one `"oploop arm stats"` line carrying
    /// the cumulative per-arm tallies (the line operators + the test-547 gate
    /// grep for). Same line shape and emit path as the periodic twin (shared
    /// [`Self::emit_stats_line`] body — no duplicated formatting). Returns the
    /// rendered snapshot for the same test-assertion reason as the helper.
    /// Observation-only: no scheduling/behaviour effect.
    pub fn emit_final(&self) -> ArmStatsSnapshot {
        self.emit_stats_line(now_millis())
    }

    /// Render a snapshot of the current accounting. One small `Vec` alloc for
    /// the per-arm counts — off the per-iteration hot path (only the watchdog
    /// dump path + the periodic emit call it). Uses a fresh wall-clock read.
    pub fn snapshot(&self) -> ArmStatsSnapshot {
        self.snapshot_at(now_millis())
    }

    /// As [`Self::snapshot`] but against a caller-supplied `now` (lets `record`
    /// reuse its single wall-clock read for the cadence compares).
    fn snapshot_at(&self, now: u64) -> ArmStatsSnapshot {
        let iter = self.iter.load(Ordering::Relaxed);
        let counts: Vec<(&'static str, u64)> = self
            .arm_names
            .iter()
            .zip(self.counts.iter())
            .map(|(name, c)| (*name, c.load(Ordering::Relaxed)))
            .collect();
        let arm_nanos: Vec<(&'static str, u64)> = self
            .arm_names
            .iter()
            .zip(self.arm_nanos.iter())
            .map(|(name, c)| (*name, c.load(Ordering::Relaxed)))
            .collect();
        let idle_nanos = self.idle_nanos.load(Ordering::Relaxed);
        let last_arm_idx = self.last_arm.load(Ordering::Relaxed) as usize;
        let last_arm = self.arm_names.get(last_arm_idx).copied().unwrap_or("?");
        let since_inbox = iter.saturating_sub(self.iter_at_last_inbox.load(Ordering::Relaxed));
        let inbox_idle = Duration::from_millis(
            now.saturating_sub(self.inbox_at_millis.load(Ordering::Relaxed)),
        );
        ArmStatsSnapshot {
            iter,
            counts,
            arm_nanos,
            idle_nanos,
            last_arm,
            last_arm_age: Duration::from_millis(
                now.saturating_sub(self.last_arm_at_millis.load(Ordering::Relaxed)),
            ),
            since_inbox,
            inbox_idle,
        }
    }
}

/// Pure starvation-WARN decision, separated from the atomics so the
/// threshold/cooldown policy is unit-testable without any clock or `Arc`.
///
/// Fires iff the inbox arm has been idle for BOTH at least
/// [`STARVATION_WARN_ITER_THRESHOLD`] iterations AND at least
/// [`STARVATION_WARN_MIN_ELAPSED`] wall-clock, and the last WARN (if any) is
/// older than [`STARVATION_WARN_COOLDOWN`]. Requiring both axes keeps a busy-
/// but-healthy loop (many timer/bus arms racing between two close ingests)
/// quiet while still catching a true spin (10k+ iters AND 30 s with no
/// ingest).
fn starvation_warn_due(
    since_inbox: u64,
    inbox_idle: Duration,
    last_warn_at_millis: u64,
    now: u64,
) -> bool {
    if since_inbox < STARVATION_WARN_ITER_THRESHOLD || inbox_idle < STARVATION_WARN_MIN_ELAPSED {
        return false;
    }
    last_warn_at_millis == 0
        || now.saturating_sub(last_warn_at_millis) >= STARVATION_WARN_COOLDOWN.as_millis() as u64
}

/// A shared registry of the operational loops currently running on this node,
/// keyed by role label. Mirrors the [`crate::liveness::BeaconTarget`]
/// shared-handle pattern: each loop PUBLISHES its stats on entry (under its
/// role label) and CLEARS them on exit; the off-runtime
/// [`crate::runtime_watchdog`] checker thread READS them when it fires.
///
/// # Why role-KEYED, not a single slot
///
/// On the CO-LOCATED topology a promoted primary and that node's own
/// worker-secondary run CONCURRENTLY on ONE runtime — so at the freeze BOTH a
/// primary `operational_loop` and a secondary `process_tasks` loop may be
/// live. A single last-writer-wins slot would lose one; the production wedge
/// is the PRIMARY ingest loop, but the secondary loop is co-resident and worth
/// dumping too. A small role→stats map dumps EVERY live loop's arms in one
/// pass. The label is a free-form `&'static str` (e.g. `"primary"`,
/// `"secondary"`), so the two co-located loops never collide.
///
/// Cheap to clone (one `Arc`); the map is at most a couple of entries.
#[derive(Clone, Default)]
pub struct OpLoopArmStatsCell {
    inner: Arc<std::sync::Mutex<RoleStatsRegistry>>,
}

/// The role→stats association the [`OpLoopArmStatsCell`] guards. A `Vec` (not a
/// `HashMap`) because it holds at most a couple of entries — the co-located
/// `"primary"` + `"secondary"` loops — so a linear scan is cheaper than
/// hashing and keeps a stable render order. Factored into a `type` so the
/// shared-handle field stays a simple `Arc<Mutex<_>>`.
type RoleStatsRegistry = Vec<(&'static str, Arc<OpLoopArmStats>)>;

impl OpLoopArmStatsCell {
    /// A fresh, empty cell. No loop has published yet → [`Self::snapshot_line`]
    /// returns `None`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish (or replace) the `role`-labelled loop's stats — called by that
    /// loop on entry. A poison from a panicked holder is recovered
    /// (`into_inner`) so observation never widens a fault. Replacing an
    /// existing same-label entry (a retry-pass re-entry) keeps the latest.
    pub fn publish(&self, role: &'static str, stats: Arc<OpLoopArmStats>) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(slot) = guard.iter_mut().find(|(r, _)| *r == role) {
            slot.1 = stats;
        } else {
            guard.push((role, stats));
        }
    }

    /// Drop the `role`-labelled entry (called by that loop on exit) so a stale
    /// snapshot from an already-exited loop is never read. Leaves any
    /// co-resident loop's entry intact.
    pub fn clear(&self, role: &'static str) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.retain(|(r, _)| *r != role);
    }

    /// Render every live loop's snapshot as one compact line
    /// (`primary: iter=.. ; secondary: iter=..`), or `None` when no loop is
    /// running. This is the [`crate::runtime_watchdog`] snapshot-provider body.
    pub fn snapshot_line(&self) -> Option<String> {
        let guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if guard.is_empty() {
            return None;
        }
        Some(
            guard
                .iter()
                .map(|(role, stats)| format!("{role}: {}", stats.snapshot()))
                .collect::<Vec<_>>()
                .join(" ; "),
        )
    }

    /// Publish `stats` under `role` and return an RAII guard that CLEARS the
    /// `role` entry when dropped. Lets an operational loop publish ONCE at
    /// entry and have the un-publish happen automatically on EVERY exit path
    /// (clean break, an early `return`, or an unwind) without scattering a
    /// `clear` call at each — the single-concern way to keep a role's entry
    /// exactly as long-lived as its loop. The guard holds a clone of the
    /// (cheap, `Arc`-backed) cell, so it is independent of the coordinator's
    /// borrow.
    pub fn publish_scoped(
        &self,
        role: &'static str,
        stats: Arc<OpLoopArmStats>,
    ) -> OpLoopArmStatsGuard {
        self.publish(role, stats);
        OpLoopArmStatsGuard {
            cell: self.clone(),
            role,
        }
    }
}

/// RAII un-publish guard from [`OpLoopArmStatsCell::publish_scoped`]. Dropping
/// it clears the guarded loop's entry from the cell, so the entry's lifetime
/// tracks the loop's stack frame exactly — across `break`, `return`, or
/// unwind — with no per-exit-site bookkeeping.
pub struct OpLoopArmStatsGuard {
    cell: OpLoopArmStatsCell,
    role: &'static str,
}

impl Drop for OpLoopArmStatsGuard {
    fn drop(&mut self) {
        self.cell.clear(self.role);
    }
}

/// A rendered snapshot of an [`OpLoopArmStats`]. [`std::fmt::Display`] is the
/// one compact diagnostic line.
#[derive(Debug, Clone)]
pub struct ArmStatsSnapshot {
    /// Total iterations recorded.
    pub iter: u64,
    /// Per-arm `(name, count)`, arm-id order.
    pub counts: Vec<(&'static str, u64)>,
    /// Per-arm cumulative BODY time in nanoseconds `(name, nanos)`, arm-id
    /// order (index-aligned with [`Self::counts`]). The TIME axis the
    /// dominant-arm selection uses.
    pub arm_nanos: Vec<(&'static str, u64)>,
    /// Cumulative IDLE time in nanoseconds — the `select!`-wait. The
    /// [`IDLE_ARM_NAME`] pseudo-arm's time; competes with [`Self::arm_nanos`]
    /// in the time-based dominant selection.
    pub idle_nanos: u64,
    /// The arm that won most recently.
    pub last_arm: &'static str,
    /// Wall-clock age of the last arm win.
    pub last_arm_age: Duration,
    /// Iterations since the inbound arm last won.
    pub since_inbox: u64,
    /// Wall-clock since the inbound arm last won.
    pub inbox_idle: Duration,
}

impl std::fmt::Display for ArmStatsSnapshot {
    /// `iter=N arm_counts=[A=.., B=..] arm_ms=[idle=.., A=.., B=..]`
    /// `since_inbox=K inbox_idle=Ts last_arm=X` — the single line wired into
    /// the watchdog dump + the periodic stats emission. Counts AND the
    /// per-arm body-time (with the `idle` select!-wait pseudo-arm first) are
    /// rendered in arm-id order (stable across emits) so successive lines
    /// diff cleanly. The `arm_ms` breakdown is the disambiguator for the
    /// `iter` rate: high `idle` ms = healthy light load; low `idle` + a fat
    /// arm ms = overload on that arm.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let counts = self
            .counts
            .iter()
            .map(|(name, n)| format!("{name}={n}"))
            .collect::<Vec<_>>()
            .join(", ");
        // `idle` first, then the real arms in arm-id order; render as
        // whole milliseconds (the nanos resolution is for the share math,
        // not the human line).
        let mut times = vec![format!("{IDLE_ARM_NAME}={}", self.idle_nanos / 1_000_000)];
        times.extend(
            self.arm_nanos
                .iter()
                .map(|(name, nanos)| format!("{name}={}", nanos / 1_000_000)),
        );
        write!(
            f,
            "iter={} arm_counts=[{}] arm_ms=[{}] since_inbox={} inbox_idle={}s last_arm={}",
            self.iter,
            counts,
            times.join(", "),
            self.since_inbox,
            self.inbox_idle.as_secs(),
            self.last_arm,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARMS: &[&str] = &["command", "inbox", "heartbeat"];
    const INBOX: usize = 1;

    #[test]
    fn record_tallies_per_arm_and_iter() {
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        stats.record(0);
        stats.record(0);
        stats.record(2);
        let s = stats.snapshot();
        assert_eq!(s.iter, 3);
        assert_eq!(s.counts[0], ("command", 2));
        assert_eq!(s.counts[1], ("inbox", 0));
        assert_eq!(s.counts[2], ("heartbeat", 1));
        assert_eq!(s.last_arm, "heartbeat");
    }

    #[test]
    fn begin_select_record_split_attributes_idle_and_body() {
        // Drive one full iteration deterministically (inject instants, no
        // sleep): the gap begin_select→record is IDLE; the gap
        // record→next begin_select is the arm's BODY.
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        let t0 = Instant::now();
        // Iteration 1: wait 100ms (idle), then arm 0's body runs 30ms.
        stats.begin_select(t0);
        stats.record_at(0, t0 + Duration::from_millis(100));
        // Iteration 2 begins: closing arm 0's body at +130ms ⇒ 30ms body.
        stats.begin_select(t0 + Duration::from_millis(130));
        // Wait 50ms (idle), then arm 2's body runs (closed by emit below).
        stats.record_at(2, t0 + Duration::from_millis(180));
        // Close arm 2's body at +200ms ⇒ 20ms body.
        stats.begin_select(t0 + Duration::from_millis(200));

        let s = stats.snapshot();
        // idle = 100 + 50 = 150ms.
        assert_eq!(s.idle_nanos, 150 * 1_000_000, "idle = sum of select! waits");
        // arm 0 body = 30ms; arm 2 body = 20ms; inbox untouched.
        assert_eq!(s.arm_nanos[0], ("command", 30 * 1_000_000));
        assert_eq!(s.arm_nanos[1], ("inbox", 0));
        assert_eq!(s.arm_nanos[2], ("heartbeat", 20 * 1_000_000));
        // Counts are preserved alongside the new time axis.
        assert_eq!(s.counts[0], ("command", 1));
        assert_eq!(s.counts[2], ("heartbeat", 1));
    }

    #[test]
    fn record_arm_time_and_record_idle_accumulate() {
        // The low-level accumulators add directly, no cursor needed.
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        stats.record_arm_time(0, Duration::from_millis(10));
        stats.record_arm_time(0, Duration::from_millis(5));
        stats.record_arm_time(2, Duration::from_millis(7));
        stats.record_idle(Duration::from_millis(40));
        stats.record_idle(Duration::from_millis(2));
        let s = stats.snapshot();
        assert_eq!(s.arm_nanos[0], ("command", 15 * 1_000_000));
        assert_eq!(s.arm_nanos[2], ("heartbeat", 7 * 1_000_000));
        assert_eq!(s.idle_nanos, 42 * 1_000_000);
    }

    #[test]
    fn out_of_range_arm_time_is_defensive_noop() {
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        stats.record_arm_time(99, Duration::from_millis(10));
        let s = stats.snapshot();
        assert_eq!(s.arm_nanos.iter().map(|(_, n)| n).sum::<u64>(), 0);
    }

    #[test]
    fn since_inbox_grows_until_inbox_wins() {
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        // Three non-inbox iterations: since_inbox == iter (never ingested).
        stats.record(0);
        stats.record(2);
        stats.record(0);
        assert_eq!(stats.snapshot().since_inbox, 3);
        // Inbox wins: resets to 0.
        stats.record(INBOX);
        assert_eq!(stats.snapshot().since_inbox, 0);
        // Then climbs again.
        stats.record(2);
        stats.record(2);
        assert_eq!(stats.snapshot().since_inbox, 2);
    }

    #[test]
    fn display_is_the_compact_line() {
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        stats.record(0);
        stats.record(2);
        let line = stats.snapshot().to_string();
        // Arm-id order, since_inbox = 2 (no ingest yet), last_arm = heartbeat.
        // The arm_ms breakdown (idle first, then arm-id order) sits between
        // the counts and since_inbox.
        assert!(
            line.starts_with("iter=2 arm_counts=[command=1, inbox=0, heartbeat=1] arm_ms=["),
            "unexpected line: {line}"
        );
        assert!(line.contains("since_inbox=2"), "unexpected line: {line}");
        assert!(
            line.contains("arm_ms=[idle="),
            "idle pseudo-arm renders first in arm_ms: {line}"
        );
        assert!(line.ends_with("last_arm=heartbeat"), "unexpected line: {line}");
    }

    #[test]
    fn out_of_range_arm_is_defensive_noop_on_counts() {
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        stats.record(99); // out of range
        let s = stats.snapshot();
        assert_eq!(s.iter, 1);
        // No count incremented; last_arm renders "?" for the bad index.
        assert_eq!(s.counts.iter().map(|(_, n)| n).sum::<u64>(), 0);
        assert_eq!(s.last_arm, "?");
    }

    #[test]
    fn cell_holds_co_located_loops_keyed_by_role() {
        // Co-located topology: a primary loop AND a secondary loop publish
        // concurrently. The role-keyed cell keeps BOTH; the snapshot line names
        // each. (Mirrors the production wedge: the primary ingest loop is the
        // suspect, but the co-resident secondary is worth dumping too.)
        let cell = OpLoopArmStatsCell::new();
        assert_eq!(cell.snapshot_line(), None, "empty cell renders nothing");

        let primary = OpLoopArmStats::new(ARMS, INBOX);
        primary.record(INBOX);
        let secondary = OpLoopArmStats::new(ARMS, INBOX);
        secondary.record(2);

        cell.publish("primary", primary);
        cell.publish("secondary", secondary);
        let line = cell.snapshot_line().expect("two loops published");
        assert!(line.contains("primary: iter=1"), "line: {line}");
        assert!(line.contains("secondary: iter=1"), "line: {line}");
        assert!(line.contains(" ; "), "both loops joined: {line}");

        // Clearing one role leaves the other intact (the co-located twin).
        cell.clear("secondary");
        let line = cell.snapshot_line().expect("primary still live");
        assert!(line.starts_with("primary: "), "line: {line}");
        assert!(!line.contains("secondary"), "secondary gone: {line}");

        cell.clear("primary");
        assert_eq!(cell.snapshot_line(), None, "all cleared");
    }

    #[test]
    fn publish_scoped_guard_clears_on_drop() {
        // The RAII guard from `publish_scoped` clears the role on drop —
        // covering a loop's break/return/unwind exits without per-exit
        // bookkeeping.
        let cell = OpLoopArmStatsCell::new();
        {
            let stats = OpLoopArmStats::new(ARMS, INBOX);
            stats.record(INBOX);
            let _guard = cell.publish_scoped("primary", stats);
            assert!(cell.snapshot_line().is_some(), "published while guard lives");
        }
        assert_eq!(
            cell.snapshot_line(),
            None,
            "guard drop must clear the role entry"
        );
    }

    #[test]
    fn publish_replaces_same_role_keeps_latest() {
        // A retry-pass re-entry republishes under the same role; the cell keeps
        // the latest (no duplicate "primary:" entries).
        let cell = OpLoopArmStatsCell::new();
        let first = OpLoopArmStats::new(ARMS, INBOX);
        first.record(INBOX);
        cell.publish("primary", first);
        let second = OpLoopArmStats::new(ARMS, INBOX);
        second.record(2);
        second.record(2);
        cell.publish("primary", second);
        let line = cell.snapshot_line().expect("one entry");
        assert_eq!(line.matches("primary:").count(), 1, "no dup role: {line}");
        assert!(line.contains("iter=2"), "kept the latest: {line}");
    }

    #[test]
    fn emit_final_renders_the_line_unconditionally_within_the_interval() {
        // A short run: a handful of arms recorded, then the loop exits well
        // within STATS_LINE_INTERVAL — so the periodic gate has NOT elapsed and
        // `maybe_emit` would NOT have emitted. `emit_final` must still produce
        // the cumulative arm-stats line so the run is not BLIND on exit.
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        stats.record(0);
        stats.record(0);
        stats.record(2);

        // Precondition: the interval gate has NOT elapsed since construction
        // (last_stats seeded to loop-entry "now"), so the periodic emit is dued
        // off — the very case that blinds short runs.
        let last_stats = stats.last_stats_at_millis.load(Ordering::Relaxed);
        let elapsed_since_stats = now_millis().saturating_sub(last_stats);
        assert!(
            elapsed_since_stats < STATS_LINE_INTERVAL.as_millis() as u64,
            "test premise: interval must NOT have elapsed ({elapsed_since_stats}ms)"
        );

        // emit_final fires regardless and carries the cumulative tallies, the
        // SAME line shape the periodic twin would have produced.
        let snap = stats.emit_final();
        let line = snap.to_string();
        assert_eq!(snap.iter, 3, "cumulative iter count on exit");
        assert!(
            line.starts_with("iter=3 arm_counts=[command=2, inbox=0, heartbeat=1]"),
            "emit_final line: {line}"
        );
        assert!(line.ends_with("last_arm=heartbeat"), "emit_final line: {line}");
    }

    #[test]
    fn starvation_warn_requires_both_iter_and_time() {
        // Below the iter threshold → never, regardless of time/cooldown.
        assert!(!starvation_warn_due(
            STARVATION_WARN_ITER_THRESHOLD - 1,
            STARVATION_WARN_MIN_ELAPSED + Duration::from_secs(100),
            0,
            1_000_000,
        ));
        // Above iter but below the time floor → never.
        assert!(!starvation_warn_due(
            STARVATION_WARN_ITER_THRESHOLD + 1,
            STARVATION_WARN_MIN_ELAPSED - Duration::from_millis(1),
            0,
            1_000_000,
        ));
        // Both axes crossed, no prior warn → fire.
        assert!(starvation_warn_due(
            STARVATION_WARN_ITER_THRESHOLD,
            STARVATION_WARN_MIN_ELAPSED,
            0,
            1_000_000,
        ));
    }

    #[test]
    fn snapshot_names_the_hot_arm_under_the_production_wedge_signature() {
        // Reproduce the production ingest-wedge signature in arm terms: the
        // inbox arm wins exactly 4 times (the "ingests exactly 4 of 16"), then
        // SOME other arm — here the heartbeat arm — wins forever and the inbox
        // arm never wins again (~97% spin, "never returns to its inbox"). This
        // is the oracle the integration harness relies on: on a real wedge the
        // published snapshot pins WHICH arm is hot and how starved the inbox is.
        let stats = OpLoopArmStats::new(ARMS, INBOX);
        for _ in 0..4 {
            stats.record(INBOX);
        }
        // The wedge: the heartbeat arm wins every subsequent iteration.
        for _ in 0..50_000 {
            stats.record(2); // "heartbeat"
        }
        let s = stats.snapshot();
        // The dominant arm is the hot-looping one — names the wedge.
        let (top_name, top_count) = s
            .counts
            .iter()
            .copied()
            .max_by_key(|(_, n)| *n)
            .expect("non-empty");
        assert_eq!(top_name, "heartbeat", "the hot arm must be the dominant count");
        assert_eq!(top_count, 50_000);
        // The inbox arm won exactly 4 times and never since — the signature.
        assert_eq!(s.counts[INBOX], ("inbox", 4));
        assert_eq!(s.since_inbox, 50_000, "since_inbox must measure the spin");
        assert_eq!(s.last_arm, "heartbeat");
        // And the rendered line carries all of it for the failure message.
        let line = s.to_string();
        assert!(line.contains("inbox=4"), "line: {line}");
        assert!(line.contains("heartbeat=50000"), "line: {line}");
        assert!(line.contains("since_inbox=50000"), "line: {line}");
        assert!(line.ends_with("last_arm=heartbeat"), "line: {line}");
    }

    #[test]
    fn starvation_warn_is_rate_limited() {
        let now = 1_000_000u64;
        // A warn fired just now (within cooldown) → suppress.
        assert!(!starvation_warn_due(
            STARVATION_WARN_ITER_THRESHOLD,
            STARVATION_WARN_MIN_ELAPSED,
            now - 1,
            now,
        ));
        // Cooldown elapsed → fire again.
        assert!(starvation_warn_due(
            STARVATION_WARN_ITER_THRESHOLD,
            STARVATION_WARN_MIN_ELAPSED,
            now - STARVATION_WARN_COOLDOWN.as_millis() as u64,
            now,
        ));
    }
}
