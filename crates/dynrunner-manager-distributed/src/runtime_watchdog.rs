//! [`RuntimeWatchdog`] — async-runtime starvation self-detector + frame-dump trigger.
//!
//! # Concern (and ONLY this concern)
//!
//! Detect that THIS node's single `current_thread` tokio runtime has stopped
//! making progress (wedged or busy-spinning) and, when it has, fire the
//! Python-side frame dump so the next occurrence NAMES the wedged loop.
//! Detection + dump-trigger only. NO failover coupling, NO process exit, NO
//! liveness/election knowledge — those are owned elsewhere (the failover half
//! is a separate, owner-adjudicated design).
//!
//! # Why a separate component (not folded into the liveness beacon)
//!
//! The [`crate::liveness::LivenessBeacon`] is a PURE UDP emitter whose
//! boundary doc states it "knows NOTHING about elections, the mesh, or
//! operational state." Bolting staleness-detection + a SIGUSR1 dump onto its
//! loop would conflate two concerns. This watchdog instead OWNS its own
//! independent OS thread — borrowing the beacon's *survival* property (a
//! mostly-sleeping thread firing a few syscalls is promptly scheduled by CFS
//! even on a fully-pegged core, so it runs even while the runtime it watches
//! is frozen) without sharing the beacon's code or concern.
//!
//! # Mechanism
//!
//! Two halves share one [`AtomicU64`] unix-millis heartbeat:
//!
//! - **Heartbeat (on the runtime under watch):** a `LocalSet` task that
//!   writes `now_millis()` into the shared cell every [`HEARTBEAT_INTERVAL`].
//!   If the runtime is wedged/spinning, this task cannot run, so the cell
//!   stops advancing.
//! - **Checker (off the runtime, dedicated OS thread):** wakes every
//!   [`CHECK_INTERVAL`], reads the cell, and applies the pure
//!   [`starvation_action`] decision. When the heartbeat is older than
//!   [`STARVATION_THRESHOLD`] it logs an unmistakable ERROR and raises
//!   `SIGUSR1` against this process (`libc::raise`) so the Python-side
//!   `faulthandler` (registered in the secondary bootstrap) dumps every
//!   thread's stack AUTOMATICALLY — no operator needed. The dump is
//!   rate-limited to once per [`DUMP_COOLDOWN`] so a sustained freeze emits
//!   one dump per minute, not a flood.
//!
//! # Boundary
//!
//! Input surface: nothing but a `spawn()` call at secondary boot. Output: a
//! log line + a self-directed signal. The caller drives the heartbeat by
//! `spawn_local`-ing the returned future onto its `LocalSet`; the checker
//! thread is owned by the returned handle and joined on drop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A provider the checker thread calls when it declares the runtime starved,
/// to render an extra diagnostic line ALONGSIDE the SIGUSR1 frame dump.
///
/// In the supported topology this returns the operational loop's live
/// arm-stats snapshot (see [`crate::oploop_instrumentation`]) so the same dump
/// that captures the wedged Python stack ALSO names which `select!` arm the
/// loop was hot-spinning on — the ingest-wedge signature. `None` means "no
/// snapshot available" (the loop is not running, or no provider was wired);
/// the checker then dumps frames only, unchanged. Boxed + `Send + Sync`
/// because the checker reads it from its own OS thread.
pub type WedgeSnapshotProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// Cadence at which the runtime-side heartbeat task refreshes the shared
/// timestamp. Short relative to [`STARVATION_THRESHOLD`] so a healthy runtime
/// keeps the cell well within the threshold under normal scheduling jitter.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Cadence at which the off-runtime checker thread re-evaluates staleness.
/// Matched to the heartbeat so a single skipped beat is not yet alarming but
/// a sustained freeze is caught within ~one interval of crossing the
/// threshold.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Heartbeat age past which the runtime is declared starved. Generous enough
/// to never trip on ordinary scheduling jitter / a co-resident CPU burst that
/// merely delays a few beats, but far below the multi-minute production
/// freeze (50+ min observed) it exists to capture.
const STARVATION_THRESHOLD: Duration = Duration::from_secs(30);

/// Minimum spacing between successive frame dumps. A sustained freeze keeps
/// crossing the threshold every check; without this the checker would raise
/// `SIGUSR1` every [`CHECK_INTERVAL`]. One dump per minute is enough to track
/// a persistent wedge without flooding the dump file.
const DUMP_COOLDOWN: Duration = Duration::from_secs(60);

/// Wall-clock unix milliseconds, saturating to 0 before the epoch. The shared
/// heartbeat cell is written and read in this unit so the two halves need no
/// shared monotonic clock (they run on the same host, so wall-clock deltas are
/// sound for the coarse 30 s threshold).
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// What the checker should do this tick, given the heartbeat age and how long
/// since the last dump. Pure so the threshold + cooldown policy is unit-tested
/// without any thread, signal, or clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchdogAction {
    /// Heartbeat is fresh (or freshly recovered): runtime is making progress.
    Healthy,
    /// Heartbeat is stale but a dump fired within the cooldown — stay quiet
    /// (still wedged, already reported recently).
    StarvedSuppressed,
    /// Heartbeat is stale and the cooldown has elapsed: log + dump now.
    StarvedDump,
}

/// Pure staleness decision.
///
/// `heartbeat_age` is `now − last_heartbeat`; `since_last_dump` is
/// `now − last_dump` (`None` when no dump has fired yet). Separated from the
/// thread loop so the threshold/cooldown policy is testable in isolation.
pub fn starvation_action(
    heartbeat_age: Duration,
    since_last_dump: Option<Duration>,
) -> WatchdogAction {
    if heartbeat_age < STARVATION_THRESHOLD {
        return WatchdogAction::Healthy;
    }
    match since_last_dump {
        Some(d) if d < DUMP_COOLDOWN => WatchdogAction::StarvedSuppressed,
        _ => WatchdogAction::StarvedDump,
    }
}

/// A running runtime-starvation watchdog. Owns the off-runtime checker
/// thread; dropping it stops and joins the thread. The heartbeat half lives
/// as a `LocalSet` task the caller spawned from [`RuntimeWatchdog::spawn`]'s
/// returned future — it ends naturally when the runtime (and its `LocalSet`)
/// is torn down.
pub struct RuntimeWatchdog {
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl RuntimeWatchdog {
    /// Stand up the watchdog.
    ///
    /// Returns the handle (owns the checker thread) and a heartbeat future
    /// the caller MUST `spawn_local` onto the runtime it wants watched — that
    /// future is the "I'm alive" pulse, so it deliberately runs ON the
    /// watched runtime (if that runtime wedges, the pulse stops and the
    /// off-thread checker fires).
    ///
    /// The dump is delivered by `libc::raise(SIGUSR1)`, picked up by the
    /// Python-side `faulthandler` registered at secondary bootstrap. If that
    /// handler is not installed the signal's default disposition terminates
    /// the process — acceptable, because by then the runtime is already wedged
    /// and the operator wanted it surfaced; in the supported topology the
    /// handler IS installed (`dynamic_runner._secondary_bootstrap`).
    ///
    /// `wedge_snapshot` is an optional [`WedgeSnapshotProvider`]: when the
    /// checker fires it calls this and, on `Some(line)`, logs the line at ERROR
    /// alongside the frame dump so the dump NAMES the wedged loop's hot
    /// `select!` arm. `None` (or a provider returning `None`) leaves the dump
    /// behaviour exactly as before.
    pub fn spawn(
        wedge_snapshot: Option<WedgeSnapshotProvider>,
    ) -> (Self, impl std::future::Future<Output = ()>) {
        let heartbeat = Arc::new(AtomicU64::new(now_millis()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let checker_heartbeat = Arc::clone(&heartbeat);
        let checker_shutdown = Arc::clone(&shutdown);
        let join = std::thread::Builder::new()
            .name("runtime-watchdog".to_string())
            .spawn(move || {
                run_checker(checker_heartbeat, checker_shutdown, wedge_snapshot);
            })
            .ok();

        let heartbeat_future = heartbeat_loop(heartbeat);
        (
            Self {
                shutdown,
                join,
            },
            heartbeat_future,
        )
    }

    /// Signal the checker thread to stop and join it. Idempotent; also
    /// invoked by `Drop`.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for RuntimeWatchdog {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The runtime-side heartbeat: refresh the shared cell every
/// [`HEARTBEAT_INTERVAL`]. Runs as a `LocalSet` task ON the watched runtime,
/// so it stops advancing the cell precisely when that runtime stops making
/// progress.
async fn heartbeat_loop(heartbeat: Arc<AtomicU64>) {
    let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        heartbeat.store(now_millis(), Ordering::Relaxed);
    }
}

/// The off-runtime checker body. Wakes every [`CHECK_INTERVAL`], reads the
/// heartbeat, and acts on [`starvation_action`]. Lives on its own OS thread so
/// it keeps running while the watched runtime is frozen.
fn run_checker(
    heartbeat: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    wedge_snapshot: Option<WedgeSnapshotProvider>,
) {
    let mut last_dump: Option<Instant> = None;
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(CHECK_INTERVAL);
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let now = now_millis();
        let last = heartbeat.load(Ordering::Relaxed);
        let heartbeat_age = Duration::from_millis(now.saturating_sub(last));
        let since_last_dump = last_dump.map(|t| t.elapsed());
        match starvation_action(heartbeat_age, since_last_dump) {
            WatchdogAction::Healthy | WatchdogAction::StarvedSuppressed => {}
            WatchdogAction::StarvedDump => {
                tracing::error!(
                    heartbeat_age_secs = heartbeat_age.as_secs(),
                    "async runtime starved for {}s — the main-thread executor is \
                     wedged/spinning; dumping Python frames (SIGUSR1 → faulthandler)",
                    heartbeat_age.as_secs(),
                );
                // Name the wedged loop's hot arm alongside the frame dump. The
                // arm-stats snapshot is read off the SAME relaxed atomics the
                // loop writes; reading it from this off-runtime thread is sound
                // even while the watched runtime is frozen (the atomics keep
                // their last-written values). `None` leaves the behaviour
                // unchanged. See `crate::oploop_instrumentation`.
                if let Some(line) = wedge_snapshot.as_ref().and_then(|f| f()) {
                    tracing::error!(
                        oploop = %line,
                        "wedged operational-loop arm snapshot (which arm the \
                         starved runtime was hot-looping on)"
                    );
                }
                // Self-directed SIGUSR1: the Python faulthandler registered at
                // secondary bootstrap dumps every thread's stack to its target
                // file. `raise` needs no ptrace and no extra privilege.
                // SAFETY: `libc::raise` is async-signal-safe and merely posts a
                // signal to the calling process; no invariants to uphold.
                unsafe {
                    libc::raise(libc::SIGUSR1);
                }
                last_dump = Some(Instant::now());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_heartbeat_is_healthy() {
        // Just under the threshold: never alarm.
        assert_eq!(
            starvation_action(STARVATION_THRESHOLD - Duration::from_millis(1), None),
            WatchdogAction::Healthy,
        );
        assert_eq!(
            starvation_action(Duration::ZERO, Some(Duration::ZERO)),
            WatchdogAction::Healthy,
        );
    }

    #[test]
    fn first_starvation_dumps() {
        // At/over the threshold with no prior dump → dump.
        assert_eq!(
            starvation_action(STARVATION_THRESHOLD, None),
            WatchdogAction::StarvedDump,
        );
        assert_eq!(
            starvation_action(STARVATION_THRESHOLD + Duration::from_secs(100), None),
            WatchdogAction::StarvedDump,
        );
    }

    #[test]
    fn sustained_starvation_is_rate_limited() {
        // Still starved, but a dump fired within the cooldown → suppress.
        assert_eq!(
            starvation_action(
                STARVATION_THRESHOLD + Duration::from_secs(5),
                Some(DUMP_COOLDOWN - Duration::from_millis(1)),
            ),
            WatchdogAction::StarvedSuppressed,
        );
    }

    #[test]
    fn cooldown_elapsed_dumps_again() {
        // Still starved and the cooldown has elapsed → dump again.
        assert_eq!(
            starvation_action(
                STARVATION_THRESHOLD + Duration::from_secs(70),
                Some(DUMP_COOLDOWN),
            ),
            WatchdogAction::StarvedDump,
        );
        assert_eq!(
            starvation_action(
                STARVATION_THRESHOLD + Duration::from_secs(70),
                Some(DUMP_COOLDOWN + Duration::from_secs(10)),
            ),
            WatchdogAction::StarvedDump,
        );
    }

    #[test]
    fn recovery_after_dump_is_healthy() {
        // Heartbeat resumed (age back under threshold) even though a dump
        // recently fired: healthy wins — the cooldown only gates dumps WHILE
        // starved.
        assert_eq!(
            starvation_action(Duration::from_secs(1), Some(Duration::from_secs(2))),
            WatchdogAction::Healthy,
        );
    }
}
