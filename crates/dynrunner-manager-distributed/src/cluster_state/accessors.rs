//! Read-only accessors over the replicated cluster ledger.
//!
//! Single concern: every method here is `&self` and never mutates the
//! ledger; the apply rules and lifecycle / event hooks live in
//! sibling sub-modules. The accessors form the read API that tests,
//! metrics, and the off-`apply` reader paths (e.g. the observer
//! resource-holdings announcer's epoch mirror clone, the dispatcher
//! loops' iter_pending walk) consume.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dynrunner_core::{ErrorType, Identifier, PhaseId, TaskInfo, WorkerId};
use dynrunner_protocol_primary_secondary::RoleTable;

use super::{ClusterState, OutcomeSummary, StateCounts, TaskState};

impl<I: Identifier> ClusterState<I> {
    pub fn task_state(&self, hash: &str) -> Option<&TaskState<I>> {
        self.tasks.get(hash)
    }

    /// Iterator over `(&hash, &TaskState)` for every entry in the
    /// ledger. Used by post-promotion hydration that needs to make
    /// state-dependent decisions per task (Pending → into pool;
    /// terminal → contribute task_id to completed-deps seed;
    /// InFlight → skip).
    pub fn tasks_iter(&self) -> impl Iterator<Item = (&String, &TaskState<I>)> {
        self.tasks.iter()
    }

    pub fn iter_pending(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Pending { task } => Some((h, task)),
            _ => None,
        })
    }

    pub fn iter_in_flight(&self) -> impl Iterator<Item = (&String, &str, WorkerId)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::InFlight {
                secondary, worker, ..
            } => Some((h, secondary.as_str(), *worker)),
            _ => None,
        })
    }

    pub fn counts(&self) -> StateCounts {
        let mut c = StateCounts::default();
        for s in self.tasks.values() {
            match s {
                TaskState::Pending { .. } => c.pending += 1,
                TaskState::InFlight { .. } => c.in_flight += 1,
                TaskState::Completed { .. } => c.completed += 1,
                TaskState::Failed { .. } => c.failed += 1,
                TaskState::Unfulfillable { .. } => c.unfulfillable += 1,
                TaskState::Blocked { .. } => c.blocked += 1,
                TaskState::Cancelled { .. } => c.cancelled += 1,
            }
        }
        c
    }

    /// Per-ErrorType partition of terminal-state tasks, in the shape
    /// the operator-facing log lines consume (`succeeded` / `fail_retry`
    /// / `fail_oom` / `fail_final`). Iterates the CRDT-replicated
    /// `tasks` map once; `O(n)` over the ledger.
    ///
    /// Distinguished from [`Self::counts`] by the failure-class
    /// breakdown: `counts().failed` collapses every `Failed { kind, .. }`
    /// into a single number, whereas `outcome_counts()` partitions
    /// the same set by `kind` per the [`OutcomeSummary`] mapping rules.
    /// `counts()` stays the small-and-fast accessor for tests / state-
    /// machine assertions; `outcome_counts()` is the operator-readable
    /// shape.
    ///
    /// CRDT-authoritative: every replica observes the same partition
    /// after the same mutation set lands, so this is the correct read
    /// for any "what did the cluster as a whole achieve" log line —
    /// including the demoted primary's terminal log, which the
    /// per-node `completed_tasks`/`failed_tasks` HashSets historically
    /// undercounted whenever cross-secondary completions reached the
    /// CRDT but not the local mirror (the asm-tokenizer "0/0/0/0/0"
    /// post-Step-6 cosmetic).
    pub fn outcome_counts(&self) -> OutcomeSummary {
        let mut o = OutcomeSummary::default();
        for s in self.tasks.values() {
            match s {
                TaskState::Completed { .. } => o.succeeded += 1,
                TaskState::Failed { kind, .. } => match kind {
                    ErrorType::Recoverable => o.fail_retry += 1,
                    ErrorType::ResourceExhausted(k) if k.as_str() == "memory" => {
                        o.fail_oom += 1
                    }
                    // Defensive: the apply rule for `TaskFailed` routes
                    // `Unfulfillable` straight to `TaskState::Unfulfillable`
                    // (the discrete state below), so this arm is unreachable
                    // in practice. Kept for exhaustiveness — if a legacy
                    // wire path ever lands a `Failed { Unfulfillable, .. }`
                    // entry the count still partitions correctly.
                    ErrorType::ResourceExhausted(_)
                    | ErrorType::NonRecoverable
                    | ErrorType::Unfulfillable { .. } => o.fail_final += 1,
                },
                // Discrete `Unfulfillable` state: reinjectable resource-
                // availability failure. Tallied as `fail_final` for the
                // operator-readable buckets until the dedicated
                // reinject/blocked bucket lands; same mapping as the
                // legacy `Failed { Unfulfillable, .. }` arm above so the
                // total partition stays stable across the variant cutover.
                TaskState::Unfulfillable { .. } => o.fail_final += 1,
                // Operator-initiated panik cancellation. Partitioned
                // separately from `fail_final` so a terminal-report
                // log line distinguishes "operator stopped the run"
                // from "the worker actually hit a non-recoverable
                // failure". Mapping mirrors the rationale on
                // `TaskState::Cancelled`: cancellation is not a
                // failure class, it's an emergency-terminal class.
                TaskState::Cancelled { .. } => o.cancelled += 1,
                // Non-terminal: Pending, InFlight, and Blocked all
                // contribute to neither bucket. Blocked tasks are
                // cascade-paused dependents that will auto-resume to
                // Pending when their prereq completes; they're not a
                // terminal outcome and counting them as one would
                // double-tally on the eventual resumed run.
                TaskState::Pending { .. }
                | TaskState::InFlight { .. }
                | TaskState::Blocked { .. } => {}
            }
        }
        o
    }

    /// Iterator over `(task_hash, &TaskInfo)` for every entry in the
    /// ledger, regardless of state. Used by the post-promotion
    /// hydration path to seed `mark_tasks_completed` with the
    /// task_ids of terminal tasks (so surviving Pending tasks'
    /// `task_depends_on` references resolve).
    pub fn iter_all(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().map(|(h, s)| {
            let t = match s {
                TaskState::Pending { task }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::Blocked { task, .. }
                | TaskState::Cancelled { task, .. } => task,
            };
            (h, t)
        })
    }

    /// Iterator over `(task_hash, &TaskInfo)` for terminal entries
    /// (`Completed`, `Failed`, `Unfulfillable`). `Blocked` is non-
    /// terminal (auto-resumes to `Pending` when its prereq completes)
    /// and is excluded.
    pub fn iter_terminal(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Completed { task }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::Cancelled { task, .. } => Some((h, task)),
            _ => None,
        })
    }

    pub fn current_primary(&self) -> Option<&str> {
        self.current_primary.as_deref()
    }

    pub fn primary_epoch(&self) -> u64 {
        self.primary_epoch
    }

    /// Clone the shared [`Arc<AtomicU64>`] mirror of `primary_epoch`
    /// for an off-`apply` reader to install into a long-lived task.
    /// The mirror is updated synchronously by every `apply` /
    /// `restore` arm that bumps `primary_epoch`, **before** role-
    /// change hooks fire — see field doc on `primary_epoch_mirror`
    /// for the memory-ordering contract.
    ///
    /// The one production reader today is the observer's resource-
    /// holdings announcer (`crate::observer::announcer`), which reads
    /// the mirror at send time so a broadcast that retries past a
    /// further `PrimaryChanged` automatically picks up the newer
    /// epoch instead of carrying the stale trigger-time value.
    pub fn primary_epoch_mirror(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.primary_epoch_mirror)
    }

    pub fn phase_deps(&self) -> &HashMap<PhaseId, Vec<PhaseId>> {
        &self.phase_deps
    }

    /// Borrow the replicated per-peer holdings map. Each entry is
    /// the set of opaque resource strings the named peer most
    /// recently announced (via `ClusterMutation::PeerResourceHoldingsUpdated`).
    /// The framework does not interpret the strings; downstream
    /// consumers attach meaning.
    pub fn peer_holdings(&self) -> &HashMap<String, HashSet<String>> {
        &self.peer_holdings
    }

    /// Snapshot of the replicated [`RoleTable`]. Borrowed for the
    /// lifetime of `&self`; callers wanting an owned copy should
    /// `.clone()` the returned reference. The transport-side
    /// write-through cache (registered via the
    /// [`RoleChangeHookRegistrar`] impl) is the expected reader on
    /// the hot path — `role_table()` is for cluster-state inspect
    /// callers like tests / metrics.
    pub fn role_table(&self) -> &RoleTable {
        &self.role_table
    }

    /// Resolve a task_id to its wire-canonical hash via a linear scan
    /// over `self.tasks`. Returns `None` if no entry in the ledger
    /// carries that `task_id`.
    ///
    /// O(n) over the ledger; the CRDT does not maintain a task_id →
    /// hash reverse index (the live `PendingPool` does, but it's
    /// task_id-keyed by design and lives only on the primary;
    /// every replica must resolve locally to converge on dependency
    /// states). The scan keeps the dependency-tracking concern
    /// self-contained inside cluster_state.
    pub fn task_hash_for_task_id(&self, task_id: &str) -> Option<&str> {
        self.tasks.iter().find_map(|(h, s)| {
            let task = match s {
                TaskState::Pending { task }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::Blocked { task, .. }
                | TaskState::Cancelled { task, .. } => task,
            };
            (task.task_id.as_deref() == Some(task_id)).then_some(h.as_str())
        })
    }

    /// Whether the run has been declared finished by the primary.
    /// Sticky monotonic flag: once set, never clears for the lifetime
    /// of this state. Secondaries read this to break their main loop
    /// when the peer mesh is still up but the run is genuinely over.
    pub fn run_complete(&self) -> bool {
        self.run_complete
    }

    /// Whether a panik shutdown has been declared (any node observed
    /// the panik filesystem signal and the broadcast
    /// `ClusterMutation::PanikRequested` has applied locally). Sticky
    /// monotonic flag: once set, never clears for the lifetime of
    /// this state. Coordinators read this from the operational loop
    /// to skip dispatch and gate shutdown.
    pub fn panik_active(&self) -> bool {
        self.panik_active
    }

    /// First-applying panik reason. `Some(_)` iff `panik_active`.
    /// Carries the originator's caller-supplied justification (e.g.
    /// `"file: /tmp/asm-tokenizer.panik"`).
    pub fn panik_reason(&self) -> Option<&str> {
        self.panik_reason.as_deref()
    }

    /// Peer that originated the first-applying `PanikRequested`
    /// broadcast. `Some(_)` iff `panik_active`. Forensic-only — no
    /// apply rule consults this field.
    pub fn panik_source(&self) -> Option<&str> {
        self.panik_source.as_deref()
    }
}
