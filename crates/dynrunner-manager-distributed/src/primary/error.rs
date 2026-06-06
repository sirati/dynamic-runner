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
    /// `--panik-file` paths), announced its own departure via a
    /// self-authored `ClusterMutation::PeerRemoved { SelfDeparture }`
    /// (membership/observability only — peers LOG it and mark this
    /// node Dead, the run is NOT terminated on peers), and is
    /// returning so the PyO3 wrapper can call `std::process::exit(137)`.
    /// The SLURM wrapper sees exit 137 and reaps the podman container;
    /// peers continue / re-elect as appropriate.
    ///
    /// `matched_path` is the first panik file that existed on
    /// this node (input-order priority — see
    /// `PanikWatcherConfig.paths` doc). `reason` is the shape
    /// `"panik file: <path>"` carried in the `SelfDeparture` payload.
    ///
    /// Why a separate variant rather than `Other(String)`: the
    /// PyO3 boundary needs to translate panik into
    /// `exit(137)` specifically (vs. `exit(1)` for other
    /// errors); a string-matched discriminator would be fragile.
    PanikShutdown {
        matched_path: std::path::PathBuf,
        reason: String,
    },
    /// Demoted submitter in setup-promote mode (`required_setup_on_promote
    /// = true`) timed out waiting for the promoted secondary to broadcast
    /// its first `ClusterMutation::TaskAdded` / `TasksSpawned` /
    /// `RunComplete`. The operational loop's setup-pending arm
    /// (`config.setup_promote_deadline`) fired with `setup_pending`
    /// still latched true.
    ///
    /// Distinct from `Other(String)` so the PyO3 boundary can render a
    /// clear, structured failure (rather than the legacy log-and-swallow
    /// path that surfaces as a stranded-count discrepancy or a silent
    /// 4-hour hang). Distinct from `ClusterCollapsed` because no task
    /// was ever assigned — there is no per-category breakdown to render,
    /// only the elapsed wall-clock that pins the operator's diagnostic
    /// pointer at "the promoted secondary never started broadcasting".
    SetupDeadlineExpired {
        /// Wall-clock duration the demoted submitter spent in the
        /// setup-pending wait before the arm fired.
        elapsed: std::time::Duration,
    },
    /// A `(phase_id, task_id)` duplicate was detected in the INITIAL
    /// task batch — BEFORE any phase started (#3a). A collision in the
    /// initial batch is a producer-side bug that would silently mask
    /// one of the colliding tasks, so the run is aborted cluster-wide:
    /// the primary broadcasts `ClusterMutation::RunAborted { reason }`
    /// (every secondary/observer exits non-zero) and returns this so
    /// its own PyO3 boundary surfaces a non-zero exit.
    ///
    /// Distinct from `Other(String)` so the PyO3 boundary translates it
    /// to a structured `PyRuntimeError` (the `Other` path is
    /// log-and-swallowed → exit 0, which would hide the abort).
    /// Mirrors `SetupDeadlineExpired`'s role: a structured pre-dispatch
    /// terminal with no per-task breakdown to render — only the reason
    /// naming the colliding identities. (A duplicate detected AFTER a
    /// phase started — #3b — does NOT reach this variant: it
    /// invalidates the not-yet-terminal tasks run-wide and the run
    /// CONTINUES to its normal completion.)
    DuplicateTaskIdPrePhase {
        /// Human-readable reason naming the colliding `(phase, task_id)`
        /// identity/identities. Same string carried in the broadcast
        /// `RunAborted { reason }` so the primary-side exception and the
        /// secondary-side log agree.
        reason: String,
    },
    /// A run-loop POLICY ABORT — a deliberate, consumer-/policy-driven
    /// non-zero exit that is NOT a strand/collapse and NOT a pre-phase
    /// duplicate. The canonical case is the observer's invalid-task Policy-B
    /// fatal-exit (the windowed invalid-task monitor signalled), but this is a
    /// GENERAL home for the policy-abort class.
    ///
    /// Distinct from `Other(String)` so the PyO3 boundary RAISES on it (the
    /// run was deliberately aborted by a policy — it MUST surface non-zero,
    /// never the `Other` log-and-swallow). Distinct from `ClusterCollapsed`
    /// because nothing was stranded by a routing collapse: reporting a policy
    /// abort as "cluster collapsed (N stranded)" would mis-point the
    /// operator's diagnostic (goal #235 — surface the RIGHT exit reason).
    ///
    /// NOTE: `DuplicateTaskIdPrePhase` is a sibling policy-abort that predates
    /// this variant; a future audit may fold it into this class. Left as-is
    /// for now (its dedicated boundary mapping + message are load-bearing).
    FatalPolicyExit {
        /// Human-readable reason naming the policy that aborted the run
        /// (e.g. the invalid-task monitor's threshold breach).
        reason: String,
    },
    /// A runtime `spawn_tasks` batch (typically from `on_phase_end`)
    /// was REJECTED by the validator — every named task failed
    /// `UnknownDependency` / `DuplicateTaskHash` — so the framework
    /// silently dropped planned work. On the producer path a wholesale-
    /// rejected next-phase batch nets ZERO dispatch yet every seeded task
    /// already terminated, so `run_complete_check`'s counter exit trips
    /// and the run exits rc=0 with zero outputs (the asm-dataset-nix
    /// c39034f2 silent total=0). Surfacing this loudly is the safety net:
    /// a non-empty spawn plan that dispatches nothing must never present
    /// as a clean run.
    ///
    /// Distinct from `Other(String)` so the PyO3 boundary RAISES on it
    /// (the `Other` path is log-and-swallowed → exit 0, which is exactly
    /// the silent failure this variant exists to prevent). Distinct from
    /// `ClusterCollapsed` because nothing was stranded by a routing
    /// collapse — the work never entered the ledger to be stranded. The
    /// per-index `SpawnError` the caller already receives from
    /// `spawn_tasks` is UNCHANGED; this is the run-level backstop for the
    /// case where the consumer logs those per-task errors and proceeds.
    SpawnRejected {
        /// The `task_id`s the validator rejected (capped for the
        /// message; the count is the authoritative signal).
        rejected_task_ids: Vec<String>,
    },
    /// Any other run-time failure — transport setup, pool
    /// construction, broadcast deliveries that exhausted retries, etc.
    ///
    /// The ONLY swallow-eligible variant: a stay-local primary's unexpected
    /// generic `Other` is log-and-swallowed at the PyO3 boundary (exit 0,
    /// surfacing via the stranded-count accounting) — a pre-existing
    /// blast-radius-minimization behavior. EVERY known failure that must
    /// surface non-zero is a STRUCTURED variant above, so `Other` is reached
    /// only by a genuinely-unexpected generic failure.
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
            Self::SetupDeadlineExpired { elapsed } => write!(
                f,
                "setup-promote deadline expired after {:.1}s: the promoted \
                 secondary never broadcast TaskAdded / TasksSpawned / RunComplete \
                 — discovery may be hung on the consumer side, or the secondary's \
                 SLURM job died before its first broadcast. Tune \
                 `setup_promote_deadline` upward if the consumer's `discover_items` \
                 walk is legitimately long-running.",
                elapsed.as_secs_f64()
            ),
            Self::DuplicateTaskIdPrePhase { reason } => write!(
                f,
                "run aborted: duplicate task identity in the initial batch \
                 (before any phase started) — {reason}. A (phase_id, task_id) \
                 collision in the initial task set is a producer-side bug that \
                 would silently mask one of the colliding tasks; the run was \
                 torn down cluster-wide rather than proceeding on an ambiguous \
                 task set. Fix the producer so every (phase_id, task_id) is \
                 unique within the run."
            ),
            Self::FatalPolicyExit { reason } => write!(
                f,
                "run aborted by policy: {reason}. A run-loop policy (e.g. the \
                 observer's invalid-task monitor) signalled a deliberate non-zero \
                 exit — the run did not complete cleanly."
            ),
            Self::SpawnRejected { rejected_task_ids } => {
                // Cap the inline list so a large rejected batch doesn't
                // flood the message; the count is the load-bearing signal.
                const SHOWN: usize = 8;
                let n = rejected_task_ids.len();
                let shown: Vec<&str> = rejected_task_ids
                    .iter()
                    .take(SHOWN)
                    .map(String::as_str)
                    .collect();
                let suffix = if n > SHOWN {
                    format!(" (+{} more)", n - SHOWN)
                } else {
                    String::new()
                };
                write!(
                    f,
                    "runtime spawn_tasks rejected {n} task(s): [{}]{suffix}. A \
                     phase plan named these (phase_id, task_id) identities but the \
                     validator rejected every one (unknown dependency or duplicate \
                     hash), so the framework dispatched ZERO of them — the run would \
                     otherwise have exited rc=0 with that planned work silently \
                     dropped. Fix the producer so each spawned task's dependencies \
                     resolve to ledger entries, or so its (phase_id, task_id) is \
                     unique.",
                    shown.join(", ")
                )
            }
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
