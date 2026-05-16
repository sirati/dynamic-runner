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
