use std::fmt;

use crate::cluster_state::OutcomeSummary;

/// Outcome of a `PrimaryCoordinator::run()` invocation.
///
/// Distinct from a free-form `String` so callers (especially the PyO3
/// `RustPrimaryCoordinator::run` and `RustDistributedManager::run`
/// wrappers) can match on the `ClusterCollapsed` variant when the
/// run finishes with tasks unaccounted for, surface the per-category
/// counters, and translate the structured error into a non-zero
/// process exit. CI / ops scripts that previously read exit code 0
/// despite hundreds of un-dispatched tasks (the `Completed: 10 /
/// Failed: 0 / Total: 484` failure mode reported by asm-tokenizer)
/// now see a typed failure they can trust.
///
/// `Other(String)` is the pass-through for every other failure mode
/// the inner pipeline raises today (transport handshake errors, pool
/// rejections, peer-mesh setup failures, etc.); a blanket
/// `From<String>` makes the existing `?`-on-`Result<(), String>`
/// helper sites compile unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    /// The run loop exited (transport collapse, inactivity timeout,
    /// etc.) with tasks left neither in the completed set nor in
    /// any failure bucket. `stranded = total - outcome.total_terminal()`.
    ///
    /// `outcome` carries the per-class breakdown so post-mortem
    /// tooling can render `succeeded` separately from the three
    /// `fail_*` buckets; pre-restructure shape `{ stranded,
    /// completed, failed }` collapsed all failure classes into a
    /// single `failed` field and hid OOM-vs-final-vs-retryable
    /// downstream of the error variant.
    ClusterCollapsed {
        stranded: usize,
        outcome: OutcomeSummary,
    },
    /// Operator-initiated emergency stop via the panik-watcher.
    /// The primary observed its panik file (any of the configured
    /// `--panik-file` paths), broadcast
    /// `ClusterMutation::PanikRequested` to every secondary on
    /// the peer mesh, and is returning so the PyO3 wrapper can
    /// call `std::process::exit(137)`. The SLURM wrapper sees
    /// exit 137 and reaps the podman container; secondaries on
    /// other nodes have either already observed their own panik
    /// file or learn about the cluster-wide stop through the
    /// broadcast and follow suit.
    ///
    /// `matched_path` is the first panik file that existed on
    /// this node (input-order priority — see
    /// `PanikWatcherConfig.paths` doc). `reason` is the shape
    /// `"panik file: <path>"` carried in the broadcast
    /// `ClusterMutation::PanikRequested.reason` so terminal logs
    /// across the cluster all surface the same sentinel.
    ///
    /// Why a separate variant rather than `Other(String)`: the
    /// PyO3 boundary needs to translate panik into
    /// `exit(137)` specifically (vs. `exit(1)` for other
    /// errors); a string-matched discriminator would be fragile.
    PanikShutdown {
        matched_path: std::path::PathBuf,
        reason: String,
    },
    /// Any other run-time failure — transport setup, pool
    /// construction, broadcast deliveries that exhausted retries, etc.
    Other(String),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClusterCollapsed { stranded, outcome } => write!(
                f,
                "{stranded} tasks left unassigned because cluster routing collapsed \
                 (succeeded={s} fail_retry={r} fail_oom={o} fail_final={fi} stranded={stranded})",
                s = outcome.succeeded,
                r = outcome.fail_retry,
                o = outcome.fail_oom,
                fi = outcome.fail_final,
            ),
            Self::PanikShutdown {
                matched_path,
                reason,
            } => write!(
                f,
                "primary panik shutdown: {reason} (matched_path={})",
                matched_path.display()
            ),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for RunError {}

impl From<String> for RunError {
    fn from(s: String) -> Self {
        Self::Other(s)
    }
}

impl From<&str> for RunError {
    fn from(s: &str) -> Self {
        Self::Other(s.to_owned())
    }
}
