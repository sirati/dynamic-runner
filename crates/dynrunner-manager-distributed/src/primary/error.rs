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
    /// A structured pre-dispatch terminal with no per-task breakdown to
    /// render — only the reason naming the colliding identities. (A
    /// duplicate detected AFTER a phase started — #3b — does NOT reach
    /// this variant: it
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
    /// The run's setup peer (a `SeedSource::ColdStart` / `RelocatedSeed`
    /// primary — mesh-always: the primary ALWAYS runs on a compute peer, never
    /// the setup peer) found NO eligible compute peer to relocate to at the
    /// bootstrap role branch — `select_relocation_target` returned `None`. A
    /// run with no promotable compute peer is an unsupported topology (e.g.
    /// every secondary joined `can_be_primary = false`, or only observers are
    /// present). UNIFORM across every backend — a LIVE error path on the
    /// in-process mpsc mesh AND the SLURM QUIC mesh alike (no longer
    /// "unreachable" for the in-process topology). Surfaced as a hard
    /// structured error rather than silently keeping the setup peer as the
    /// run's primary; the PyO3 boundary RAISES it (never the `Other` swallow).
    NoRelocationTarget,
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
                "run terminated non-zero (deliberate fatal-exit): {reason}. The \
                 canonical case is a run-loop policy (e.g. the observer's \
                 invalid-task monitor) signalling a deliberate non-zero exit, but \
                 the leading reason is authoritative for the actual cause — the run \
                 did not complete cleanly. NOTE: a host signal (SIGTERM/SLURM \
                 TIMEOUT/scancel/OOM-killer) is NOT this terminal; a host-signal \
                 teardown surfaces as a panik (exit 137) whose reason names the \
                 sender pid, never as a policy abort."
            ),
            Self::NoRelocationTarget => write!(
                f,
                "bootstrap could not relocate the primary role: no eligible \
                 compute peer to promote (no alive worker-secondary advertised \
                 `can_be_primary`). The submitter must NEVER stay the run's \
                 primary (mesh-always pillar 2), so this topology is \
                 unsupported — launch at least one compute peer that can host \
                 the primary role (a real peer-mesh secondary, not an observer)."
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
