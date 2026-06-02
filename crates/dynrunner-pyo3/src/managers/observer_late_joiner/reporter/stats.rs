//! CRDT-derived statistics snapshot.
//!
//! # Single concern
//!
//! Read a `&ClusterState<I>` once and project it into the flat
//! [`StatsSnapshot`] the reporter formats + diffs. Everything here is a
//! pure function of the replicated ledger: there is no authority, no
//! pool, and no wall-clock — exactly the inputs a zero-authority
//! observer holds.
//!
//! # Why recompute here (not read pool accessors)
//!
//! An observer holds NO `PendingPool` — it carries only the replicated
//! `ClusterState`. So "waiting-on-deps" and "ready-in-queue", which on
//! the primary are pool-state reads, are RECOMPUTED here from the CRDT:
//! the `Pending` task set, each task's `task_depends_on`, and the
//! static phase-dependency graph (`phase_deps`). The recomputation
//! mirrors the pool's own resolution rule (a dep is satisfied once its
//! prereq is terminal; a phase is dispatchable once every phase it
//! depends on has fully terminated) so the two views converge.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{Identifier, PhaseId};
use dynrunner_manager_distributed::{ClusterState, TaskState};

/// One flat projection of the CRDT at an instant. Every field is a
/// non-negative count except `per_secondary_in_flight`, which the idle
/// detector consumes. The reporter diffs successive snapshots and
/// applies the >0-and-changed inclusion rule against this shape.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// `OutcomeSummary::succeeded` — unique-hash completion count.
    pub succeeded: usize,
    /// `OutcomeSummary::fail_retry` — last-observed `Recoverable`.
    pub fail_retry: usize,
    /// `OutcomeSummary::fail_oom` — last-observed memory `ResourceExhausted`.
    pub fail_oom: usize,
    /// `NonRecoverable` / non-memory `ResourceExhausted`, with BOTH the
    /// discrete `Unfulfillable` AND the discrete `InvalidTask` sets
    /// NETTED OUT — see the trap note at the assignment in
    /// `from_cluster_state`. Reported as a line disjoint from both
    /// `unfulfillable` and `invalid_task`.
    pub fail_final: usize,
    /// `StateCounts::unfulfillable` — the DISCRETE `TaskState::Unfulfillable`
    /// count. Read from `counts()`, NOT `outcome_counts()`: the latter
    /// folds Unfulfillable into `fail_final`, which would double-count
    /// it here. This is its OWN reported line.
    pub unfulfillable: usize,
    /// `StateCounts::invalid_task` — the DISCRETE `TaskState::InvalidTask`
    /// count (terminal, non-reinjectable structural failures: missing
    /// dep / duplicate id). Read from `counts()`, NOT `outcome_counts()`,
    /// for exactly the same reason as `unfulfillable`: `outcome_counts()`
    /// folds `InvalidTask` into `fail_final` (see the accessor's
    /// `InvalidTask => fail_final += 1` arm), so reading it from the
    /// outcome bucket and then reporting a discrete line would
    /// double-count. This is its OWN reported line, disjoint from
    /// `fail_final`.
    pub invalid_task: usize,
    /// Tasks in `TaskState::InFlight` — currently executing somewhere.
    pub in_flight: usize,
    /// `Pending` tasks with at least one UNsatisfied `task_depends_on`
    /// prereq (the prereq is not yet terminal). Recomputed from the
    /// CRDT; the pool's `blocked` map analog.
    pub waiting_on_deps: usize,
    /// `TaskState::Blocked` count — cascade-paused dependents of an
    /// `Unfulfillable` prereq. A SEPARATE category from `waiting_on_deps`
    /// per owner-decision C-2.
    pub blocked: usize,
    /// `Pending` tasks whose deps are all satisfied AND whose phase is
    /// dispatchable (every dep-phase fully terminated). The CRDT analog
    /// of the pool's queued-in-active-phase count.
    pub ready_in_queue: usize,
    /// secondary-id → number of `TaskState::InFlight` tasks it is
    /// running. Only secondaries with ≥1 in-flight task appear; the
    /// idle detector folds this against its accumulated known-secondary
    /// set. Excludes the count's contribution to occupancy denominators
    /// (those are the deferred Part-D addon).
    pub per_secondary_in_flight: HashMap<String, usize>,
}

impl StatsSnapshot {
    /// Project a `&ClusterState` into a flat snapshot. `O(n)` over the
    /// ledger (a handful of single passes; not hot-path code — runs on
    /// a 10-minute cadence).
    ///
    /// # Production caller is the live-feed seam
    ///
    /// This is the projection the observer run loop publishes into the
    /// reporter's `SharedSnapshotSource` whenever it applies a mesh
    /// broadcast. Wiring that push requires a `&self` CRDT-read
    /// accessor on `SecondaryCoordinator` (the run loop owns the CRDT
    /// `&mut` for its whole lifetime), which is outside this subtask's
    /// file scope — see the seam doc in `observer_late_joiner/run.rs`.
    /// Until that producer lands, the only caller is the reporter's
    /// own test suite; `allow(dead_code)` documents that the function
    /// is deliberately production-reachable-but-not-yet-wired rather
    /// than accidentally orphaned.
    #[allow(dead_code)]
    pub fn from_cluster_state<I: Identifier>(state: &ClusterState<I>) -> Self {
        let outcome = state.outcome_counts();
        let counts = state.counts();

        // task_id → terminal? Built once so the per-task dep walk is
        // O(deps) rather than O(ledger) per dep. Terminal == the pool's
        // resolution rule: Completed | Failed | Unfulfillable. Blocked,
        // Pending and InFlight are NOT terminal (a dep on any of them is
        // unsatisfied).
        let mut terminal_task_ids: HashSet<&str> = HashSet::new();
        for (_, st) in state.tasks_iter() {
            if is_terminal(st) {
                let id = task_id_of(st);
                if !id.is_empty() {
                    terminal_task_ids.insert(id);
                }
            }
        }

        // Phase → has any non-terminal task? Drives the
        // dispatchable-phase derivation below. A phase with no entries
        // contributes `false` (treated as fully terminated / vacuously
        // satisfiable) so a dependent of an empty upstream phase is not
        // wedged forever.
        let mut phase_has_live: HashMap<&PhaseId, bool> = HashMap::new();
        for (_, st) in state.tasks_iter() {
            let task = task_of(st);
            let entry = phase_has_live.entry(&task.phase_id).or_insert(false);
            if !is_terminal(st) {
                *entry = true;
            }
        }

        let phase_deps = state.phase_deps();

        let mut waiting_on_deps = 0usize;
        let mut ready_in_queue = 0usize;
        let mut per_secondary_in_flight: HashMap<String, usize> = HashMap::new();

        for (_, st) in state.tasks_iter() {
            match st {
                TaskState::InFlight { secondary, .. } => {
                    *per_secondary_in_flight.entry(secondary.clone()).or_insert(0) += 1;
                }
                TaskState::Pending { task } => {
                    let deps_satisfied = task
                        .task_depends_on
                        .iter()
                        .all(|d| terminal_task_ids.contains(d.task_id.as_str()));
                    if !deps_satisfied {
                        waiting_on_deps += 1;
                    } else if phase_dispatchable(&task.phase_id, phase_deps, &phase_has_live) {
                        ready_in_queue += 1;
                    }
                    // else: deps satisfied but the phase is still gated
                    // on an upstream phase — not "ready", not
                    // "waiting-on-deps"; intentionally surfaced by
                    // neither category (mirrors the pool, where such an
                    // item sits in a Blocked-phase bucket).
                }
                _ => {}
            }
        }

        StatsSnapshot {
            succeeded: outcome.succeeded,
            fail_retry: outcome.fail_retry,
            fail_oom: outcome.fail_oom,
            // `outcome_counts().fail_final` FOLDS BOTH the discrete
            // `TaskState::Unfulfillable` AND the discrete
            // `TaskState::InvalidTask` sets into itself (see the
            // accessor: `Unfulfillable => fail_final += 1` and
            // `InvalidTask => fail_final += 1`). Since `unfulfillable`
            // and `invalid_task` are each reported as their OWN line,
            // subtract BOTH here so the three lines are disjoint and a
            // single failed task is not double-counted across two
            // metrics. This is the complement of the documented
            // `counts()`-vs-`outcome_counts()` trap: read
            // `counts().{unfulfillable,invalid_task}` for the discrete
            // lines AND net them back out of `fail_final`.
            fail_final: outcome
                .fail_final
                .saturating_sub(counts.unfulfillable)
                .saturating_sub(counts.invalid_task),
            unfulfillable: counts.unfulfillable,
            invalid_task: counts.invalid_task,
            in_flight: counts.in_flight,
            waiting_on_deps,
            blocked: counts.blocked,
            ready_in_queue,
            per_secondary_in_flight,
        }
    }
}

// These helpers are reached only through `from_cluster_state`, so they
// share its "production-reachable-but-not-yet-wired" status (the live
// CRDT-projection feed is the integration seam — see the function's
// doc). Test builds DO call `from_cluster_state` and thus exercise
// every helper; the `allow` only silences the plain-`build` lib pass.

/// True iff `state` is a terminal state for dependency-resolution
/// purposes (the pool resolves a dep once its prereq is `Completed` OR
/// permanently failed; in the CRDT the permanent-failure set is
/// `Failed` ∪ `Unfulfillable` ∪ `InvalidTask`). `Blocked` is
/// cascade-paused, not terminal.
#[allow(dead_code)]
fn is_terminal<I>(state: &TaskState<I>) -> bool {
    matches!(
        state,
        TaskState::Completed { .. }
            | TaskState::Failed { .. }
            | TaskState::Unfulfillable { .. }
            | TaskState::InvalidTask { .. }
    )
}

#[allow(dead_code)]
fn task_id_of<I>(state: &TaskState<I>) -> &str {
    task_of(state).task_id.as_str()
}

#[allow(dead_code)]
fn task_of<I>(state: &TaskState<I>) -> &dynrunner_core::TaskInfo<I> {
    match state {
        TaskState::Pending { task }
        | TaskState::InFlight { task, .. }
        | TaskState::Completed { task }
        | TaskState::Failed { task, .. }
        | TaskState::Unfulfillable { task, .. }
        | TaskState::Blocked { task, .. }
        | TaskState::InvalidTask { task, .. } => task,
    }
}

/// A phase is dispatchable iff every phase it depends on (transitively)
/// has no live (non-terminal) task. Mirrors the pool's activation
/// cascade: a phase activates once its dependency phases are Done, and
/// a phase reaches Done once its tasks have all terminated.
///
/// `phase_has_live` is consulted as the per-phase "any live task"
/// predicate; a phase absent from the map (no entries) is vacuously
/// satisfied (`false`). The walk is depth-bounded by the dep graph,
/// which `PendingPool::new` already cycle-rejects, so this terminates.
#[allow(dead_code)]
fn phase_dispatchable(
    phase: &PhaseId,
    phase_deps: &HashMap<PhaseId, Vec<PhaseId>>,
    phase_has_live: &HashMap<&PhaseId, bool>,
) -> bool {
    let mut stack: Vec<&PhaseId> = phase_deps.get(phase).into_iter().flatten().collect();
    let mut seen: HashSet<&PhaseId> = HashSet::new();
    while let Some(dep) = stack.pop() {
        if !seen.insert(dep) {
            continue;
        }
        if phase_has_live.get(dep).copied().unwrap_or(false) {
            return false;
        }
        if let Some(parents) = phase_deps.get(dep) {
            stack.extend(parents.iter());
        }
    }
    true
}
