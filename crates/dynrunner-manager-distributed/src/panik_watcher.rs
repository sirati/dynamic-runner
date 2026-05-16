//! Filesystem-poll watcher for operator-initiated emergency shutdown.
//!
//! Single concern: poll a fixed set of paths at a configurable cadence
//! until ANY of them exists (`fs::metadata` succeeds), then signal the
//! coordinator via a oneshot channel and exit. The coordinator is the
//! sole consumer; it owns the actual broadcast + worker-teardown +
//! exit response.
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
//! The panik latch is sticky-monotonic-true. Once any path matches we
//! signal once and exit; the coordinator's apply rule
//! (`ClusterState::apply` on `ClusterMutation::PanikRequested`)
//! latches the cluster-wide flag so subsequent same-path matches on
//! other nodes converge silently. A watch channel would imply
//! "re-broadcast on every poll while file exists", which violates
//! the apply rule's sticky-first-wins idempotency contract.
//!
//! # Cancellation strategy
//!
//! `JoinHandle::abort()` on drop. The watcher task's body is
//! cancellation-safe at every yield point: `std::fs::metadata` is
//! synchronous and brief (sub-millisecond), `tokio::time::sleep`
//! abort-safe by Tokio's contract. Dropping a [`PanikWatcher`] aborts
//! the task; the in-task `signal_tx: oneshot::Sender` is dropped as
//! the task's stack unwinds, and the receiver observes
//! `Err(RecvError)` on its next poll — matching the "watcher
//! gracefully shut down" semantics the coordinator expects.
//!
//! # Module ownership
//!
//! Single concern strictly: filesystem stat → oneshot signal. The
//! coordinator (`PrimaryCoordinator` / `SecondaryCoordinator` /
//! observer-mode `SecondaryCoordinator`) owns:
//!   - selecting against the signal in its operational loop,
//!   - broadcasting `ClusterMutation::PanikRequested`,
//!   - killing workers + their child trees (process-group kill),
//!   - tearing down the operational loop,
//!   - returning a panik outcome to the PyO3 wrapper, which calls
//!     `exit(137)`.
//!
//! Keeping the boundary thin means tests can exercise the watcher
//! standalone (touch a temp file, assert signal arrives within 2
//! polls) without spinning up a cluster.

use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Caller-supplied watcher configuration.
#[derive(Debug, Clone)]
pub struct PanikWatcherConfig {
    /// Filesystem paths to poll. Every path is checked on every tick;
    /// the FIRST matching one fires the signal. Empty vector means
    /// "no watcher" — [`spawn_panik_watcher`] returns a never-firing
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
}

impl Default for PanikWatcherConfig {
    fn default() -> Self {
        Self {
            paths: Vec::new(),
            poll_interval: Duration::from_secs(10),
        }
    }
}

/// Payload the watcher sends through the oneshot channel on first
/// detection.
#[derive(Debug, Clone)]
pub struct PanikSignal {
    /// The first path observed to exist (in input order). Carried in
    /// the broadcast `ClusterMutation::PanikRequested.reason` so the
    /// terminal log shows which sentinel triggered the shutdown.
    pub matched_path: PathBuf,
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

/// Spawn the watcher task. Returns a [`PanikWatcher`] handle.
///
/// Behaviour:
/// - Empty `cfg.paths`: returns immediately with a never-firing
///   receiver and a no-op task. The coordinator's `select!` arm is
///   structurally present but never hits, which matches the
///   "panik-disabled" operational mode.
/// - Non-empty `cfg.paths`: spawns a background task that loops:
///   stat every path, fire signal + return on first match; sleep
///   `poll_interval`; repeat.
pub fn spawn_panik_watcher(cfg: PanikWatcherConfig) -> PanikWatcher {
    let (signal_tx, signal_rx) = oneshot::channel();

    if cfg.paths.is_empty() {
        // No watcher; the caller's `select!` arm will never hit. A
        // no-op task is the cleanest shape — when the caller calls
        // `take_signal_rx` and awaits, the receiver resolves to
        // `Err(_)` immediately because the dropped `signal_tx` here
        // closes the channel. Matches the "watcher unwired" semantic.
        drop(signal_tx);
        let join = tokio::spawn(async move {});
        return PanikWatcher {
            signal_rx: Some(signal_rx),
            join,
        };
    }

    let join = tokio::spawn(async move {
        // Take signal_tx by value once; the future code uses
        // `.is_some()` for the fired-already check (defensive — a
        // task that observes a stat-success twice in one iteration
        // due to multiple matching paths should still send only
        // once).
        let mut signal_tx = Some(signal_tx);
        loop {
            for path in &cfg.paths {
                if std::fs::metadata(path).is_ok()
                    && let Some(tx) = signal_tx.take()
                {
                    let _ = tx.send(PanikSignal {
                        matched_path: path.clone(),
                    });
                    return;
                }
            }
            // `tokio::time::sleep` is cancellation-safe by contract;
            // aborting the task during the sleep cleanly unwinds.
            tokio::time::sleep(cfg.poll_interval).await;
        }
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
        let signal_rx = w.take_signal_rx().expect("first take_signal_rx must succeed");
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
}
