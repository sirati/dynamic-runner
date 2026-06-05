//! Emergency-shutdown trigger watcher.
//!
//! Single concern: observe any of N trigger sources for an
//! operator-initiated emergency stop and, on FIRST trigger, fire a
//! [`PanikSignal`] through a oneshot channel and exit. The coordinator
//! is the sole consumer; it owns the actual broadcast + worker-teardown
//! + exit response.
//!
//! Trigger sources currently implemented:
//!   - **filesystem**: poll a fixed set of paths at
//!     [`PanikWatcherConfig::poll_interval`] cadence; first matching
//!     `fs::metadata` succeeds → fire (carrying the matched path).
//!   - **SIGTERM**: opt-in via
//!     [`PanikWatcherConfig::listen_for_sigterm`]; installs a
//!     `tokio::signal::unix::signal(SignalKind::terminate())` stream;
//!     first SIGTERM → fire with sentinel path
//!     [`SIGTERM_SENTINEL_PATH`] so log readers can distinguish the
//!     source. Enabled on the secondary path so a SIGTERM from a host
//!     shutdown-manager (SLURM time-limit / scancel forwarding via
//!     `podman exec <c> kill -TERM <pid>`) triggers the same
//!     worker-teardown + exit(137) cascade as a file panik.
//!
//! Trigger sources race inside a single watcher task via
//! `tokio::select!` — first source wins, sender fires, task returns.
//! Adding a new source means adding a `select!` arm and an enabling
//! flag in the config; the coordinator-facing API (one oneshot
//! receiver carrying a `PanikSignal`) is unchanged.
//!
//! # Why a polling watcher rather than `inotify`?
//!
//! Operator-initiated emergency stop: latency is not the critical
//! axis. A 10-second poll cadence is what the user spec'd
//! (2026-05-17 design thread) and inotify would add a Linux-specific
//! dependency for marginal benefit. The poll cadence is configurable
//! per-deployment via [`PanikWatcherConfig::poll_interval`].
//!
//! # Why a oneshot signal rather than a watch channel?
//!
//! A panik signal fires once and the node leaves the mesh. Once any
//! path matches we signal once and exit; the coordinator announces its
//! own departure (self-authored `ClusterMutation::PeerRemoved
//! { SelfDeparture }`, observability only) and exits locally. A watch
//! channel would imply "re-signal on every poll while file exists",
//! which is redundant — the node has already departed.
//!
//! # Cancellation strategy
//!
//! `JoinHandle::abort()` on drop. The watcher task's body is
//! cancellation-safe at every yield point: `std::fs::metadata` is
//! synchronous and brief (sub-millisecond), `tokio::time::sleep`
//! abort-safe by Tokio's contract, and `Signal::recv` is
//! cancellation-safe per Tokio's docs. Dropping a [`PanikWatcher`]
//! aborts the task; the in-task `signal_tx: oneshot::Sender` is
//! dropped as the task's stack unwinds, and the receiver observes
//! `Err(RecvError)` on its next poll — matching the "watcher
//! gracefully shut down" semantics the coordinator expects.
//!
//! # Module ownership
//!
//! Single concern strictly: any trigger source → oneshot signal. The
//! coordinator (`PrimaryCoordinator` / `SecondaryCoordinator` /
//! observer-mode `SecondaryCoordinator`) owns:
//!   - selecting against the signal in its operational loop,
//!   - announcing its own departure (file source: self-authored
//!     `ClusterMutation::PeerRemoved { SelfDeparture }`),
//!   - killing workers + their child trees (process-group kill),
//!   - tearing down the operational loop,
//!   - returning a panik outcome to the PyO3 wrapper, which calls
//!     `exit(137)`.
//!
//! Adding a new trigger source is purely internal to this module —
//! the coordinator-facing API does not change. Tests can exercise
//! each trigger standalone (touch a temp file / raise a signal) and
//! assert signal arrives within bounded wait, without spinning up a
//! cluster.

use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Sentinel for "no SIGTERM sender PID captured yet" in
/// [`LAST_SIGTERM_SENDER_PID`]. `0` is a VALID `si_pid` (the kernel sets
/// it to 0 for kernel-originated signals such as an OOM-kill), so the
/// sentinel must be a value the kernel never reports as a sender PID.
/// `pid_t` is signed; a negative sentinel can never collide with a real
/// PID (which is always `>= 0`).
const NO_SENDER_PID: i32 = i32::MIN;

/// Process-static slot the [`SA_SIGINFO`] action stores the SIGTERM
/// sender's `si_pid` into. Read by [`wait_for_sigterm_if_enabled`] once
/// tokio's signal stream reports a SIGTERM (the registry runs both the
/// store and tokio's self-pipe write in one multiplexed handler
/// invocation, so the store is visible by the time the stream resolves).
///
/// A process-static is intrinsic to signal handling: the C handler
/// signature carries no user context, so the captured PID has nowhere to
/// live but a static. Scoped to this module; the only writer is the
/// action installed by [`install_sigterm_sender_capture`].
static LAST_SIGTERM_SENDER_PID: AtomicI32 = AtomicI32::new(NO_SENDER_PID);

/// Install (once per process) a chained `SA_SIGINFO` action that records
/// the SIGTERM sender's `si_pid` into [`LAST_SIGTERM_SENDER_PID`].
///
/// `signal_hook_registry::register_sigaction` registers under the SAME
/// multiplexed C handler tokio uses for its own SIGTERM stream, so this
/// action and tokio's self-pipe write both run on a single delivery —
/// the watcher keeps tokio's `select!`-friendly stream for the async
/// wakeup and reads the captured PID afterwards. The action body does
/// nothing but a relaxed atomic store (async-signal-safe).
///
/// Idempotent via a `Once`: re-running the watcher (e.g. the secondary
/// `SetupPending` caller-loop re-entry, or multiple in-process managers)
/// must not stack duplicate actions. A failed install is logged and
/// tolerated — the SIGTERM trigger still fires (tokio's stream is
/// independent); only the sender-PID enrichment is lost, leaving the log
/// to report `None`.
fn install_sigterm_sender_capture() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        // SAFETY: `register_sigaction` is unsafe because the action runs
        // in signal context — our action is async-signal-safe (a single
        // relaxed atomic store; no allocation, no lock, no shared state
        // beyond the atomic). It installs via `SA_SIGINFO` and chains the
        // previous handler (tokio's), so tokio's stream still wakes. The
        // closure literal is lexically inside this `unsafe` block, so the
        // `si_pid()` union-field read it makes is covered: `si_pid()` is
        // valid for SIGTERM (a kill-class signal carrying its source), and
        // the registry only dispatches this action for the signal it was
        // registered under (SIGTERM), so the `siginfo_t` union
        // discriminant matches.
        let result = unsafe {
            signal_hook_registry::register_sigaction(libc::SIGTERM, |info: &libc::siginfo_t| {
                LAST_SIGTERM_SENDER_PID.store(info.si_pid(), Ordering::Relaxed);
            })
        };
        if let Err(e) = result {
            tracing::error!(
                error = %e,
                "panik watcher: failed to install SIGTERM sender-capture action; \
                 sender PID will be unavailable in the panik log (trigger still active)"
            );
        }
    });
}

/// Read and clear the last captured SIGTERM sender PID. Returns
/// `Some(pid)` if the capture action recorded one (and resets the slot
/// so a subsequent SIGTERM in the same process starts fresh), or `None`
/// if no PID was captured (install failed, or the slot was never
/// written). A captured `0` means a kernel-originated SIGTERM (e.g. the
/// OOM-killer) — surfaced as `Some(0)`, distinct from `None`.
fn take_sigterm_sender_pid() -> Option<u32> {
    match LAST_SIGTERM_SENDER_PID.swap(NO_SENDER_PID, Ordering::Relaxed) {
        NO_SENDER_PID => None,
        pid => Some(pid as u32),
    }
}

/// Sentinel `matched_path` carried by a [`PanikSignal`] that was
/// triggered by SIGTERM rather than by a filesystem path. Documented
/// as a non-path string (angle-bracketed) so it cannot collide with
/// any real path operators pass via `--panik-file`. Downstream log
/// readers and the departure announcement's `SelfDeparture` reason use
/// this to attribute the shutdown source.
pub const SIGTERM_SENTINEL_PATH: &str = "<SIGTERM>";

/// Construct the sentinel as a `PathBuf` for downstream comparison
/// without exposing the literal at every consumer.
pub fn sigterm_sentinel_path() -> PathBuf {
    PathBuf::from(SIGTERM_SENTINEL_PATH)
}

/// `true` iff a [`PanikSignal`] was triggered by SIGTERM rather than
/// by a filesystem path. Used by log/audit consumers that want to
/// branch on source without re-checking the sentinel literal.
pub fn is_sigterm_signal(matched_path: &Path) -> bool {
    matched_path == Path::new(SIGTERM_SENTINEL_PATH)
}

/// Caller-supplied watcher configuration.
#[derive(Debug, Clone)]
pub struct PanikWatcherConfig {
    /// Filesystem paths to poll. Every path is checked on every tick;
    /// the FIRST matching one fires the signal. Empty vector disables
    /// the file-trigger source — the watcher still runs if any other
    /// source is configured (e.g. SIGTERM); if NO source is
    /// configured, [`spawn_panik_watcher`] returns a never-firing
    /// receiver without spawning a polling loop (the coordinator's
    /// `select!` arm just stays unhit forever).
    ///
    /// Per user spec (2026-05-17): the default is BOTH a per-host
    /// path (e.g. `/tmp/<consumer>.panik`) AND a shared-network path
    /// (e.g. `/app/log-network/<consumer>.panik`) — operators can
    /// trigger from either the local node or the cluster gateway.
    /// Resolving the consumer-specific filename is the consumer's
    /// concern, not the framework's; the framework just polls
    /// whatever paths it's handed.
    pub paths: Vec<PathBuf>,
    /// Poll cadence. User-spec'd default is 10s; configurable so
    /// tests can pin behaviour at sub-second timing.
    pub poll_interval: Duration,
    /// When `true`, also install a
    /// `tokio::signal::unix::signal(SignalKind::terminate())` stream
    /// alongside the file polling loop and fire the panik signal on
    /// first SIGTERM. The fired
    /// [`PanikSignal::matched_path`] is [`SIGTERM_SENTINEL_PATH`] so
    /// downstream logging records the source.
    ///
    /// Default `false` preserves existing behavior for any caller
    /// that hasn't been migrated. Enabled on the secondary path so a
    /// SIGTERM from the host shutdown-manager (e.g. SLURM time-limit
    /// or scancel forwarded as `podman exec <c> kill -TERM <pid>`)
    /// triggers the same worker-teardown + exit(137) cascade as a
    /// file panik. Primary/local/observer paths leave this `false`
    /// because their shutdown semantics are out of scope for the
    /// host-driven secondary cascade.
    pub listen_for_sigterm: bool,
}

impl Default for PanikWatcherConfig {
    fn default() -> Self {
        Self {
            paths: Vec::new(),
            poll_interval: Duration::from_secs(10),
            listen_for_sigterm: false,
        }
    }
}

/// Payload the watcher sends through the oneshot channel on first
/// detection.
#[derive(Debug, Clone)]
pub struct PanikSignal {
    /// The first path observed to exist (in input order). Carried in
    /// the departure announcement's `SelfDeparture` reason so the
    /// terminal log shows which sentinel triggered the shutdown.
    pub matched_path: PathBuf,
    /// PID of the process that sent the SIGTERM, when the trigger source
    /// was SIGTERM and the kernel reported a sender. `Some(0)` is a
    /// kernel-originated SIGTERM (e.g. the OOM-killer); `Some(pid > 0)`
    /// names the sending process (slurmstepd on a SLURM TIMEOUT/scancel,
    /// the wrapper/shutdown-manager, etc.). `None` for a filesystem-path
    /// trigger (no signal) or when the sender could not be captured.
    /// Load-bearing diagnostic: it reveals WHO killed the secondary.
    pub sender_pid: Option<u32>,
}

/// Handle returned by [`spawn_panik_watcher`]. Hold for the lifetime
/// of the operational loop; dropping it aborts the watcher task.
#[derive(Debug)]
pub struct PanikWatcher {
    /// Receiver end of the oneshot signal. Wrapped in `Option` so the
    /// coordinator can take ownership for its `select!` (see
    /// [`Self::take_signal_rx`]) while keeping the handle around to
    /// abort the task on shutdown. Direct read: `recv().await`
    /// returns `Ok(PanikSignal)` on first detection, or `Err(_)` if
    /// the watcher task drops its sender (abort or panic).
    signal_rx: Option<oneshot::Receiver<PanikSignal>>,
    /// JoinHandle for the watcher task. `Drop` calls `.abort()`.
    join: JoinHandle<()>,
}

impl PanikWatcher {
    /// Take ownership of the signal receiver. The coordinator calls
    /// this once at run start so it can move the receiver into its
    /// `select!` arm. After this call, the `PanikWatcher` retains
    /// only the JoinHandle (for abort-on-drop). A second call
    /// returns `None`.
    pub fn take_signal_rx(&mut self) -> Option<oneshot::Receiver<PanikSignal>> {
        self.signal_rx.take()
    }
}

impl Drop for PanikWatcher {
    fn drop(&mut self) {
        // Abort the watcher task on drop so a coordinator that exits
        // its operational loop without explicit cleanup still
        // terminates the watcher promptly. Aborting an idle task
        // (in `tokio::time::sleep`) is safe and immediate; aborting
        // mid-stat is also safe — `std::fs::metadata` is a synchronous
        // syscall that returns before the next yield, so cancellation
        // observes either "before stat" or "after stat" but never
        // mid-stat.
        self.join.abort();
    }
}

/// Future returning the path observed to exist by polling (with no
/// sender PID — a file trigger carries no signal), or `pending()`
/// forever when no paths are configured. Loops: stat every path, return
/// first match; sleep `poll_interval`; repeat. Cancellation-safe at
/// every yield (`tokio::time::sleep`). The `Option<u32>` second element
/// keeps the two `select!` arms type-uniform; it is always `None` here.
async fn wait_for_file_match(
    paths: Vec<PathBuf>,
    poll_interval: Duration,
) -> (PathBuf, Option<u32>) {
    if paths.is_empty() {
        // Structurally present `select!` arm that never fires when
        // file-trigger is unconfigured. Keeps the spawn body
        // source-agnostic.
        std::future::pending::<(PathBuf, Option<u32>)>().await
    } else {
        loop {
            for path in &paths {
                if std::fs::metadata(path).is_ok() {
                    return (path.clone(), None);
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Future returning ([`sigterm_sentinel_path`], `Some(sender_pid)`) on
/// first SIGTERM, or `pending()` forever when SIGTERM listening is
/// disabled or the `tokio::signal::unix::signal(SignalKind::terminate())`
/// install fails. Cancellation-safe (`Signal::recv` is documented as
/// such).
///
/// Sender capture: alongside tokio's stream we install (once per
/// process, via [`install_sigterm_sender_capture`]) a chained
/// `SA_SIGINFO` action that records the delivery's `si_pid`. The
/// `signal-hook-registry` master handler runs BOTH our action and
/// tokio's self-pipe write on the same delivery, so by the time the
/// stream resolves the captured PID is already stored — read it via
/// [`take_sigterm_sender_pid`]. `None` if capture was unavailable.
///
/// Install-failure handling: log at `tracing::error!` and degrade to
/// `pending()`. File polling stays functional; the operator can still
/// trigger via sentinel file. We avoid panicking inside the watcher
/// task because that would propagate as `JoinHandle` failure and drop
/// the sender, which the coordinator's `select!` would observe as a
/// silent `RecvError` — indistinguishable from clean abort. Logging +
/// pending is the explicit "degraded but still alive" shape.
async fn wait_for_sigterm_if_enabled(enabled: bool) -> (PathBuf, Option<u32>) {
    if !enabled {
        return std::future::pending::<(PathBuf, Option<u32>)>().await;
    }
    // Install the sender-capture action BEFORE the tokio stream so the
    // `SA_SIGINFO` slot is armed for the very first delivery. Idempotent
    // (Once-guarded); a failure here only loses the sender-PID
    // enrichment, not the trigger.
    install_sigterm_sender_capture();
    let mut stream = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                error = %e,
                "panik watcher: failed to install SIGTERM handler; \
                 SIGTERM trigger disabled (file trigger still active)"
            );
            return std::future::pending::<(PathBuf, Option<u32>)>().await;
        }
    };
    // `recv().await` returns `None` only when the stream is dropped,
    // which doesn't happen here because we own it on the stack.
    // First `Some(())` is the first SIGTERM delivered to this
    // process after the handler was installed. The capture action ran
    // in the same multiplexed handler invocation, so the sender PID is
    // already stored.
    let _ = stream.recv().await;
    (sigterm_sentinel_path(), take_sigterm_sender_pid())
}

/// Spawn the watcher task. Returns a [`PanikWatcher`] handle.
///
/// Behaviour:
/// - **All triggers unconfigured** (`cfg.paths.is_empty() &&
///   !cfg.listen_for_sigterm`): returns immediately with a
///   never-firing receiver and a no-op task. The coordinator's
///   `select!` arm is structurally present but never hits, which
///   matches the "panik-disabled" operational mode.
/// - **Otherwise**: spawns a single task that races configured
///   trigger sources via `tokio::select!`. First trigger wins, the
///   `PanikSignal` is fired with the appropriate `matched_path` (the
///   filesystem path for a file trigger, [`SIGTERM_SENTINEL_PATH`]
///   for SIGTERM), and the task returns. Adding a new trigger source
///   means adding a `select!` arm and an enabling flag in the
///   config; no caller changes.
pub fn spawn_panik_watcher(cfg: PanikWatcherConfig) -> PanikWatcher {
    let (signal_tx, signal_rx) = oneshot::channel();

    if cfg.paths.is_empty() && !cfg.listen_for_sigterm {
        // No trigger source configured; the caller's `select!` arm
        // will never hit. A no-op task is the cleanest shape — when
        // the caller calls `take_signal_rx` and awaits, the receiver
        // resolves to `Err(_)` immediately because the dropped
        // `signal_tx` here closes the channel. Matches the
        // "watcher unwired" semantic.
        drop(signal_tx);
        let join = tokio::spawn(async move {});
        return PanikWatcher {
            signal_rx: Some(signal_rx),
            join,
        };
    }

    let join = tokio::spawn(async move {
        // Race configured trigger sources. Both arms are cancellation
        // -safe at every yield (`sleep` per tokio contract,
        // `Signal::recv` per tokio docs), so the loser future is
        // dropped cleanly when the winner returns. Each arm returns
        // `PathBuf`; the task body is source-agnostic — adding a new
        // trigger means a new helper + a new `select!` arm.
        let (matched_path, sender_pid) = tokio::select! {
            p = wait_for_file_match(cfg.paths, cfg.poll_interval) => p,
            p = wait_for_sigterm_if_enabled(cfg.listen_for_sigterm) => p,
        };
        let _ = signal_tx.send(PanikSignal {
            matched_path,
            sender_pid,
        });
    });
    PanikWatcher {
        signal_rx: Some(signal_rx),
        join,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Empty paths config returns a never-firing receiver. Used by
    /// the coordinator when the operator hasn't passed any
    /// `--panik-file` flags.
    #[tokio::test]
    async fn empty_paths_yields_disabled_watcher() {
        let mut w = spawn_panik_watcher(PanikWatcherConfig::default());
        let signal_rx = w
            .take_signal_rx()
            .expect("first take_signal_rx must succeed");
        // The receiver MUST resolve quickly (no-op task drops the
        // sender on spawn). Bounded wait to surface the no-leak
        // contract.
        let result = tokio::time::timeout(Duration::from_millis(100), signal_rx).await;
        let result = result.expect("disabled watcher should resolve immediately");
        assert!(
            result.is_err(),
            "disabled watcher signals via sender-drop, not via successful Ok"
        );
    }

    /// Happy path: file appears, watcher detects within one poll
    /// interval, signal carries the matched path.
    #[tokio::test]
    async fn detects_file_creation_and_carries_path() {
        let tmp = TempDir::new().unwrap();
        let panik_path = tmp.path().join("panik");
        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            paths: vec![panik_path.clone()],
            poll_interval: Duration::from_millis(20),
            ..Default::default()
        });
        let signal_rx = w.take_signal_rx().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        std::fs::write(&panik_path, b"stop").unwrap();
        let signal = tokio::time::timeout(Duration::from_millis(200), signal_rx)
            .await
            .expect("watcher must fire within poll budget")
            .expect("sender must not drop before firing");
        assert_eq!(signal.matched_path, panik_path);
    }

    /// First match wins ordering: when multiple paths match
    /// simultaneously, the watcher reports the first one (input
    /// vector order). Operators rely on this to express priority
    /// (per-host sentinel before shared-network sentinel).
    #[tokio::test]
    async fn first_matching_path_wins() {
        let tmp = TempDir::new().unwrap();
        let path_a = tmp.path().join("panik-a");
        let path_b = tmp.path().join("panik-b");
        std::fs::write(&path_a, b"a").unwrap();
        std::fs::write(&path_b, b"b").unwrap();
        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            paths: vec![path_a.clone(), path_b.clone()],
            poll_interval: Duration::from_millis(20),
            ..Default::default()
        });
        let signal_rx = w.take_signal_rx().unwrap();
        let signal = tokio::time::timeout(Duration::from_millis(200), signal_rx)
            .await
            .expect("watcher must fire within poll budget")
            .expect("sender must not drop before firing");
        assert_eq!(signal.matched_path, path_a);
    }

    /// Drop-aborts-task: dropping the handle without firing leaves
    /// the receiver erroring (sender dropped via task abort).
    #[tokio::test]
    async fn drop_aborts_task_and_closes_signal() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("panik");
        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            paths: vec![path.clone()],
            poll_interval: Duration::from_secs(60),
            ..Default::default()
        });
        let signal_rx = w.take_signal_rx().unwrap();
        // Drop the handle BEFORE the watcher's first poll completes.
        // Sleep arm is 60s so the watcher is parked in the sleep
        // when we drop; abort unwinds the task, sender drops,
        // receiver errors.
        drop(w);
        let result = tokio::time::timeout(Duration::from_millis(200), signal_rx).await;
        let result = result.expect("aborted watcher must close channel within budget");
        assert!(
            result.is_err(),
            "aborted watcher must surface Err on the receiver"
        );
    }

    /// take_signal_rx is single-use. Subsequent calls return None.
    #[tokio::test]
    async fn take_signal_rx_is_single_use() {
        let mut w = spawn_panik_watcher(PanikWatcherConfig::default());
        assert!(w.take_signal_rx().is_some());
        assert!(w.take_signal_rx().is_none());
    }

    // ----- SIGTERM trigger tests -----
    //
    // SIGTERM is process-global. `cargo test` runs tests on multiple
    // threads in one process, and `tokio::signal::unix::signal(...)`
    // registers a process-global driver that fans the signal out to
    // every live `Signal` instance regardless of which runtime
    // created it. To avoid cross-test contamination (one test raising
    // SIGTERM hitting another test's watcher) we serialize all
    // SIGTERM tests behind a single `Mutex`.
    //
    // **Critical**: when the disabled-path watcher is configured
    // with `listen_for_sigterm: false`, our spawn_panik_watcher does
    // NOT install a signal handler for that task. BUT another test
    // in this binary (or a prior test that left a `Signal` alive)
    // may already have installed the process-wide disposition. Once
    // installed, the kernel default of "terminate process on
    // SIGTERM" is REPLACED. So even with our watcher disabled, a
    // SIGTERM raised in this test process will NOT kill the
    // process — it will go to whichever `Signal` instances are
    // registered. We rely on this property to keep tests safe.
    //
    // To be extra safe, every SIGTERM test installs a "guard
    // `Signal`" via `tokio::signal::unix::signal(SignalKind::terminate())`
    // before raising, which ensures the process-wide handler is
    // active (replacing the kernel's "terminate" default) even if
    // no test has previously installed one.
    use nix::sys::signal::{Signal, raise};
    use tokio::sync::Mutex;

    /// Serializes SIGTERM-raising tests so they don't contaminate
    /// each other's watchers via the process-global signal handler.
    /// Uses `tokio::sync::Mutex` because the guard is held across
    /// `.await` (the workspace `clippy::await_holding_lock` lint
    /// denies `std::sync::MutexGuard` in that position — for good
    /// reason: `std` guards can deadlock async runtimes when the
    /// future moves tasks).
    static SIGTERM_TEST_LOCK: Mutex<()> = Mutex::const_new(());

    /// Install a guard `Signal` to ensure the process-wide SIGTERM
    /// disposition is "deliver to tokio" rather than "terminate
    /// process". The handle is kept alive for the duration of the
    /// test. Returning the handle (rather than using `_ =`) prevents
    /// premature drop. Returns `None` if install fails (the test
    /// should then skip or assert separately).
    fn install_sigterm_guard() -> Option<tokio::signal::unix::Signal> {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok()
    }

    /// `listen_for_sigterm: true` with empty paths fires the panik
    /// signal on first SIGTERM with the sentinel path. This is the
    /// core SIGTERM contract the host-side shutdown-manager relies
    /// on: a `kill -TERM <inside-pid>` into the secondary container
    /// triggers the same cascade as a sentinel file appearing on
    /// disk.
    #[tokio::test]
    async fn sigterm_enabled_fires_panik_with_sentinel_path() {
        let _guard = SIGTERM_TEST_LOCK.lock().await;
        // Guard Signal: keep SIGTERM disposition pointed at tokio
        // even if no other test/watcher is installed in this
        // runtime. Held until the end of the test.
        let _sigterm_guard = install_sigterm_guard().expect("SIGTERM handler install failed");

        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            listen_for_sigterm: true,
            ..Default::default()
        });
        let signal_rx = w.take_signal_rx().unwrap();
        // Yield twice so the watcher task is polled and reaches
        // its `Signal::recv().await` inside the `select!`. A single
        // `yield_now` is insufficient on current-thread runtimes
        // because the spawned task may need more than one yield
        // point to reach the await; a small sleep is more robust
        // than counting yields.
        tokio::time::sleep(Duration::from_millis(50)).await;
        raise(Signal::SIGTERM).expect("raise SIGTERM");
        let signal = tokio::time::timeout(Duration::from_millis(500), signal_rx)
            .await
            .expect("watcher must fire within budget after SIGTERM")
            .expect("sender must not drop before firing");
        assert_eq!(signal.matched_path, sigterm_sentinel_path());
        assert!(is_sigterm_signal(&signal.matched_path));
        // The chained `SA_SIGINFO` action must have captured the sender
        // PID. `raise` delivers a self-directed SIGTERM, so the recorded
        // `si_pid` is this test process's own PID. The load-bearing
        // diagnostic: a captured sender, not `None`.
        let me = std::process::id();
        assert_eq!(
            signal.sender_pid,
            Some(me),
            "SIGTERM sender PID must be captured and equal this process's PID"
        );
    }

    /// SIGTERM-and-file both enabled: SIGTERM arrives first; signal
    /// carries the SIGTERM sentinel rather than any file path.
    /// Exercises the `select!` race — first source wins.
    #[tokio::test]
    async fn sigterm_and_file_both_enabled_first_wins_sigterm() {
        let _guard = SIGTERM_TEST_LOCK.lock().await;
        let _sigterm_guard = install_sigterm_guard().expect("SIGTERM handler install failed");

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("panik");
        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            paths: vec![path.clone()],
            // 5 seconds: long enough that the file-poll arm is parked
            // in `sleep` when we raise SIGTERM; the SIGTERM arm wins.
            poll_interval: Duration::from_secs(5),
            listen_for_sigterm: true,
        });
        let signal_rx = w.take_signal_rx().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Raise SIGTERM; do NOT touch the file.
        raise(Signal::SIGTERM).expect("raise SIGTERM");
        let signal = tokio::time::timeout(Duration::from_millis(500), signal_rx)
            .await
            .expect("watcher must fire within budget after SIGTERM")
            .expect("sender must not drop before firing");
        assert_eq!(signal.matched_path, sigterm_sentinel_path());
    }

    /// SIGTERM-and-file both enabled: file appears first; signal
    /// carries the file path rather than the SIGTERM sentinel.
    /// Verifies that enabling SIGTERM doesn't regress file-trigger
    /// behaviour.
    #[tokio::test]
    async fn sigterm_and_file_both_enabled_first_wins_file() {
        let _guard = SIGTERM_TEST_LOCK.lock().await;
        let _sigterm_guard = install_sigterm_guard().expect("SIGTERM handler install failed");

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("panik");
        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            paths: vec![path.clone()],
            poll_interval: Duration::from_millis(20),
            listen_for_sigterm: true,
        });
        let signal_rx = w.take_signal_rx().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Touch the file; do NOT raise SIGTERM.
        std::fs::write(&path, b"stop").unwrap();
        let signal = tokio::time::timeout(Duration::from_millis(500), signal_rx)
            .await
            .expect("watcher must fire within budget after file create")
            .expect("sender must not drop before firing");
        assert_eq!(signal.matched_path, path);
        assert!(!is_sigterm_signal(&signal.matched_path));
        // A file trigger carries no signal, hence no sender PID.
        assert_eq!(signal.sender_pid, None);
    }

    /// `listen_for_sigterm: false` does NOT install a tokio
    /// signal handler from the watcher's perspective. We cannot
    /// directly observe "no handler installed" from outside the
    /// module, but we can assert behavioural equivalence to the
    /// pre-SIGTERM watcher: a watcher with empty paths and
    /// `listen_for_sigterm: false` is the disabled-watcher path
    /// (sender drops, receiver errors). This proves the new field
    /// did not silently turn on SIGTERM listening when the caller
    /// didn't opt in.
    ///
    /// We do NOT raise SIGTERM in this test — if no other test had
    /// run first, the process-wide disposition would still be the
    /// kernel default (terminate process), and raising would kill
    /// the test runner. Instead we cover non-installation
    /// indirectly: the watcher's task body short-circuits on the
    /// "no trigger sources" path and drops the sender.
    #[tokio::test]
    async fn sigterm_disabled_yields_disabled_watcher_with_empty_paths() {
        let mut w = spawn_panik_watcher(PanikWatcherConfig {
            listen_for_sigterm: false,
            ..Default::default()
        });
        let signal_rx = w
            .take_signal_rx()
            .expect("first take_signal_rx must succeed");
        let result = tokio::time::timeout(Duration::from_millis(100), signal_rx).await;
        let result = result.expect("disabled watcher should resolve immediately");
        assert!(
            result.is_err(),
            "watcher with empty paths and listen_for_sigterm=false must \
             behave like a fully-disabled watcher (sender drops)"
        );
    }

    /// Sentinel path is the documented public constant. Locks the
    /// wire-format so downstream log parsers and `SelfDeparture`
    /// reason consumers can rely on the literal across revisions.
    #[test]
    fn sigterm_sentinel_path_is_stable_literal() {
        assert_eq!(SIGTERM_SENTINEL_PATH, "<SIGTERM>");
        assert_eq!(sigterm_sentinel_path(), PathBuf::from("<SIGTERM>"));
        assert!(is_sigterm_signal(&PathBuf::from("<SIGTERM>")));
        assert!(!is_sigterm_signal(&PathBuf::from("/tmp/panik")));
        // Not a typo target: any other angle-bracket string is NOT
        // the sentinel.
        assert!(!is_sigterm_signal(&PathBuf::from("<sigterm>")));
    }
}
