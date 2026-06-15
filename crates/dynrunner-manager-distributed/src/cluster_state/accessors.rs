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

use dynrunner_core::{
    ErrorType, Identifier, PhaseId, TaskInfo, TaskOutputs, TerminalOutcomeCounts, WorkerId,
};
use dynrunner_protocol_primary_secondary::{
    DiscoveryDebt, RoleTable, SecondaryCapacityRecord, SecondaryResourceSampleRecord,
};

use super::settled::{SettledClass, SettledEntry};
use super::{
    ClusterState, OutcomeSummary, PeerReadmission, PhaseRollup, PhaseTaskPartition, SetupProgress,
    StateCounts, TaskState,
};

/// One LOGICAL ledger entry, wherever its body lives: a fat in-memory
/// `TaskState` or a settled (spilled) entry's slim index view. The read
/// shape for callers that ask presence / terminality / identity
/// questions about an arbitrary hash — exactly the questions a settled
/// entry can answer without touching the disk.
#[derive(Debug)]
pub(crate) enum TaskView<'a, I> {
    Live(&'a TaskState<I>),
    Settled(&'a SettledEntry),
}

impl<I> TaskView<'_, I> {
    /// Terminal for dependency-resolution / phase-completion purposes —
    /// a settled entry is terminal by construction (only join
    /// fixed-point terminals settle).
    pub(crate) fn is_terminal(&self) -> bool {
        match self {
            TaskView::Live(state) => state.is_terminal(),
            TaskView::Settled(_) => true,
        }
    }

    pub(crate) fn task_id(&self) -> &str {
        match self {
            TaskView::Live(state) => &state.task().task_id,
            TaskView::Settled(entry) => &entry.task_id,
        }
    }

    pub(crate) fn phase_id(&self) -> &PhaseId {
        match self {
            TaskView::Live(state) => &state.task().phase_id,
            TaskView::Settled(entry) => &entry.phase_id,
        }
    }

    /// The retry-attempt generation (F2) — the broadcast choke point's
    /// attempt stamp reads this so a copy-current mutation for a
    /// settled hash stamps the true generation, not a cold 0.
    pub(crate) fn attempt(&self) -> u32 {
        match self {
            TaskView::Live(state) => state.attempt(),
            TaskView::Settled(entry) => entry.attempt(),
        }
    }

    /// Whether this entry is a RESOLVED SecondaryAffine import gate
    /// (`AffineReady`) — the #497 P5 per-secondary-import gate detector,
    /// answered identically whether the gate is still FAT or has SPILLED.
    ///
    /// `AffineReady` is the join fixed-point a SecondaryAffine gate reaches
    /// and is therefore SETTLE-ELIGIBLE: once it spills, its fat
    /// `TaskState::AffineReady` is evicted and only the slim
    /// `SettledClass::AffineReady` index entry remains. Both arms answer the
    /// SAME question without a disk read, so a build's gate detection
    /// survives the spill (the live-`task_state`-only check went blind).
    ///
    /// The `Live` arm still confirms `kind.is_secondary_affine()`; the
    /// `Settled` arm needs no kind check because `SettledClass::AffineReady`
    /// is produced ONLY from `TaskState::AffineReady`, which a SecondaryAffine
    /// gate is the only task that ever reaches — the class IS the
    /// "AffineReady SecondaryAffine gate" fact.
    pub(crate) fn is_affine_ready_gate(&self) -> bool {
        match self {
            TaskView::Live(TaskState::AffineReady { task, .. }) => {
                task.kind.is_secondary_affine()
            }
            TaskView::Live(_) => false,
            TaskView::Settled(entry) => matches!(entry.class, SettledClass::AffineReady),
        }
    }
}

/// A SecondaryAffine gate resolved over the FULL LOGICAL ledger in ONE
/// fat∪settled read — the SINGLE source both the gate DETECTION
/// ([`ClusterState::is_affine_ready_gate`]) and the import-DRIVE body
/// ([`ClusterState::affine_gate_task`]) now derive from (#516). Carries the
/// OWNED gate [`TaskInfo`] (`task`) AND the gate-recognition fact
/// (`is_ready_gate`).
///
/// Why a struct, not two reads: detection and the body resolve used to be
/// SEPARATE ledger reads that merely had to AGREE on the same fat∪settled
/// universe — the #515 RCA was exactly a DRIFT between them (fe840943 made
/// detection fat∪settled but left the body fat-only, so a spilled gate was
/// detected-but-could-not-resolve → phantom "absent gate"). #509 re-aligned
/// them, but two reads can drift again. Folding both answers into ONE read
/// makes the invariant "detected ⟹ body-resolvable" TAUTOLOGICAL: there is one
/// lookup, one hit, both fields off the same entry.
#[derive(Debug)]
pub(crate) struct ResolvedAffineGate<I> {
    /// The gate's body — the OWNED [`TaskInfo`] the import drives, cloned so
    /// the `cluster_state` borrow ends at the call site (the body crosses into
    /// the off-loop import task).
    pub(crate) task: TaskInfo<I>,
    /// Whether the resolved entry IS a `AffineReady` SecondaryAffine gate (the
    /// `unmet_local_affine_dep` recognition predicate). The body resolves for
    /// ANY state/class (an import re-routed by the #509 sync race re-runs once
    /// its `TaskAdded` lands the gate `Pending`); this flag isolates the
    /// ready-gate fact detection keys on.
    pub(crate) is_ready_gate: bool,
}

impl<I: Identifier> ClusterState<I> {
    /// Borrow the IN-MEMORY (fat) state for `hash`. A SETTLED entry —
    /// fat body spilled to disk — returns `None` here: callers asking
    /// presence / terminality / identity questions must use
    /// [`Self::task_view`] / [`Self::contains_task`]; this accessor is
    /// for the LIVE-state-specific reads (reinject's Unfulfillable
    /// gate, the retry bucket's Failed gate, post-spawn classification
    /// — states that are never settled).
    pub fn task_state(&self, hash: &str) -> Option<&TaskState<I>> {
        self.tasks.get(hash)
    }

    /// Whether `hash` is a LOGICAL ledger entry — fat or settled.
    pub fn contains_task(&self, hash: &str) -> bool {
        self.tasks.contains_key(hash) || self.settled_contains(hash)
    }

    /// The logical-entry view for `hash`: the fat state, or the settled
    /// slim view, or `None` for a hash the ledger does not know.
    pub(crate) fn task_view(&self, hash: &str) -> Option<TaskView<'_, I>> {
        if let Some(state) = self.tasks.get(hash) {
            return Some(TaskView::Live(state));
        }
        self.settled_entry(hash).map(TaskView::Settled)
    }

    /// Resolve `hash`'s SecondaryAffine gate over the FULL LOGICAL ledger
    /// in ONE fat∪settled read — the unified resolver (#516) both the gate
    /// DETECTION and the import-DRIVE body route through, so they can NEVER
    /// again key DIFFERENT ledger universes.
    ///
    /// Reads fat (`tasks.get`) FIRST, else the settled index (`settled.get`,
    /// then the per-key pread of the spilled fat body — the SAME pread the
    /// snapshot-stream responder uses). `None` ONLY when the hash is in
    /// NEITHER half — the gate's `TaskAdded` has genuinely not synced to this
    /// node yet (the #509 sync race), which the drive classifies as a
    /// transient (re-routable) condition, never a permanent loss. A gate that
    /// resolved-then-SPILLED (its `AffineReady` join fixed-point is
    /// settle-eligible) is NOT absent — its body is read back from disk and
    /// `is_ready_gate` stays `true` via the slim `SettledClass::AffineReady`
    /// index entry.
    ///
    /// `is_ready_gate` answers the SAME gate-recognition question as the
    /// fat/settled arms of [`TaskView::is_affine_ready_gate`]: a fat entry
    /// must be `AffineReady` with a SecondaryAffine kind; a settled entry's
    /// `SettledClass::AffineReady` IS the "AffineReady SecondaryAffine gate"
    /// fact (the class is produced ONLY from `TaskState::AffineReady`, which
    /// only a SecondaryAffine gate ever reaches).
    pub(crate) fn resolve_affine_ready_gate(&self, hash: &str) -> Option<ResolvedAffineGate<I>> {
        // ONE fat∪settled lookup. The gate-recognition fact is the single
        // [`TaskView::is_affine_ready_gate`] predicate (its sole owner); the
        // body comes off the SAME resolved entry — fat in-place, or the
        // spilled fat body read back from disk. Detection and body can no
        // longer key different ledger universes: there is one `task_view`.
        let view = self.task_view(hash)?;
        let is_ready_gate = view.is_affine_ready_gate();
        let task = match view {
            // Fat: the body is the live state's TaskInfo, already in memory.
            TaskView::Live(state) => state.task().clone(),
            // Settled: read the fat body back from the spill file (the same
            // per-key pread the snapshot-stream responder uses). `None` here
            // would mean a settled index entry whose record is unreadable —
            // the SAME absent outcome `affine_gate_task` returned before, so
            // the resolver returns `None` and the drive re-routes (#509).
            TaskView::Settled(_) => self.settled_record(hash)?.0.task().clone(),
        };
        Some(ResolvedAffineGate {
            task,
            is_ready_gate,
        })
    }

    /// Whether `hash` is a RESOLVED SecondaryAffine import gate
    /// (`AffineReady`) — the #497 P5 spill-safe gate detector the
    /// secondary's `unmet_local_affine_dep` keys on. A thin projection of
    /// the unified [`Self::resolve_affine_ready_gate`] (#516): the gate is
    /// detected IFF the hash resolves over the fat∪settled ledger AND that
    /// SAME resolved entry IS a ready gate. A hash the ledger does not know
    /// is `false`.
    pub(crate) fn is_affine_ready_gate(&self, hash: &str) -> bool {
        self.resolve_affine_ready_gate(hash)
            .is_some_and(|gate| gate.is_ready_gate)
    }

    /// The OWNED [`TaskInfo`] of a SecondaryAffine gate — a thin projection
    /// of the unified [`Self::resolve_affine_ready_gate`] (#516): the gate
    /// body the import drives, resolved over the SAME fat∪settled read
    /// detection uses, so a gate that resolved-then-SPILLED still resolves
    /// (the fat-only read went blind pre-#509) and a detected gate is ALWAYS
    /// body-resolvable (the drift class #515 flagged is structurally gone).
    /// `None` ONLY for the #509 sync race (hash in neither half), which the
    /// drive classifies as transient (re-routable), never a permanent loss.
    ///
    /// The body resolves for ANY resolved state/class (NOT gated on
    /// `is_ready_gate`): a #509-rerouted import re-runs once its `TaskAdded`
    /// lands the gate `Pending`, before it reaches `AffineReady`.
    pub(crate) fn affine_gate_task(&self, hash: &str) -> Option<TaskInfo<I>> {
        self.resolve_affine_ready_gate(hash).map(|gate| gate.task)
    }

    /// Iterator over `(&hash, &TaskState)` for every FAT (in-memory)
    /// entry. A SETTLED entry's fat body lives in the spill file and is
    /// NOT yielded — callers that need the full logical ledger pair
    /// this with [`Self::settled_entries`] (the hydrate and observer
    /// stats paths do). Used by post-promotion hydration that needs to
    /// make state-dependent decisions per task (Pending → into pool;
    /// terminal → contribute task_id to completed-deps seed;
    /// InFlight → skip).
    pub fn tasks_iter(&self) -> impl Iterator<Item = (&String, &TaskState<I>)> {
        self.tasks.iter()
    }

    /// Read-only handle on the `blocked_by` reverse-index (#547) for the
    /// invariant test in `tests/blocked_by_index.rs` — comparing the
    /// incrementally-maintained index against a fresh ledger scan. NOT a
    /// production accessor: `resume_blocked_on` reads `self.blocked_by`
    /// directly (its sole production consumer), and exposing it would tempt
    /// callers to keep external references across `set_task_state` writes
    /// (which mutate the index) — a soundness footgun for a node-local
    /// derivation.
    #[cfg(test)]
    pub(crate) fn blocked_by_for_test(
        &self,
    ) -> &std::collections::HashMap<String, std::collections::HashSet<String>> {
        &self.blocked_by
    }

    /// Test-only seam: route a `Blocked → Blocked-different-on` rewrite
    /// through the universal `set_task_state` write path. Exercises the
    /// `blocked_by` reverse-index re-bucketing branch that no production
    /// public mutation triggers today (the closest equivalent is the
    /// snapshot-restore convergence path through `merge_task_state`), so
    /// `tests/blocked_by_index.rs` can assert the invariant without
    /// re-implementing the memo-maintaining write site.
    #[cfg(test)]
    pub(crate) fn rewrite_blocked_for_test(
        &mut self,
        hash: &str,
        new_on: String,
        task: TaskInfo<I>,
        attempt: u32,
    ) {
        self.set_task_state(
            hash,
            TaskState::Blocked {
                task,
                on: new_on,
                attempt,
            },
            None,
        );
    }

    pub fn iter_pending(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Pending { task, .. } => Some((h, task)),
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
                TaskState::InvalidTask { .. } => c.invalid_task += 1,
                TaskState::SkippedAlreadyDone { .. } => c.skipped_already_done += 1,
                TaskState::SetupCompleted { .. } => c.setup_succeeded += 1,
                TaskState::AffineReady { .. } => c.affine_ready += 1,
                TaskState::QueuedAfterLocalDependency { .. } => {
                    c.queued_after_local_dependency += 1
                }
            }
        }
        // Settled (spilled) entries are LOGICAL ledger entries — fold
        // their slim classes into the same buckets the fat states feed.
        // (`QueuedAfterLocalDependency` is non-terminal and never settles,
        // so it has no `SettledClass` arm.)
        for (_, entry) in self.settled_entries() {
            match entry.class {
                SettledClass::Completed => c.completed += 1,
                SettledClass::FailedFinal(_) => c.failed += 1,
                SettledClass::InvalidTask => c.invalid_task += 1,
                SettledClass::SkippedAlreadyDone => c.skipped_already_done += 1,
                SettledClass::SetupCompleted => c.setup_succeeded += 1,
                SettledClass::AffineReady => c.affine_ready += 1,
            }
        }
        c
    }

    /// Whether the run has NOT YET DISPATCHED ANY task — no entry sits in
    /// a POST-DISPATCH state. The four post-dispatch states are the only
    /// ones a worker outcome (or an active assignment) can produce:
    /// `InFlight` (assigned to a worker), `Completed`/`Failed` (a worker
    /// reported a terminal), and `Unfulfillable` (a worker reported a
    /// missing cluster resource). Every other state is reachable WITHOUT
    /// dispatch — `Pending`/`Blocked`/`QueuedAfterLocalDependency` are
    /// undispatched-pending (a `Blocked` dependent is stamped at SEED by
    /// `apply_tasks_spawned`'s dep-classify when its prereq is still
    /// pending, never having run), and `SkippedAlreadyDone`/`InvalidTask`/
    /// `SetupCompleted`/`AffineReady` are spawn-time / in-process terminals
    /// that never reach a worker.
    ///
    /// Single concern: classify whether THIS inherited ledger reflects a
    /// run that has begun executing. Derived purely from the CRDT
    /// (`counts()` — the one state-classification owner), so it is the
    /// mesh-always "primary = pure function of CRDT" fact a freshly-built
    /// primary reads to tell a BOOTSTRAP-relocation cold target (the setup
    /// peer relocated BEFORE `perform_initial_assignment`, so nothing is
    /// dispatched) from a FAILOVER survivor-inherit (the prior operational
    /// primary MUST have dispatched the initial batch to be operational, so
    /// ≥1 post-dispatch entry exists). A failover therefore can NEVER read
    /// as unstarted — the property the bring-up reservation gate relies on
    /// to preserve the failover exclusion.
    pub fn run_is_unstarted(&self) -> bool {
        let c = self.counts();
        c.in_flight + c.completed + c.failed + c.unfulfillable == 0
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
                TaskState::Failed { kind, .. } => fold_failed_kind(kind, &mut o),
                // Discrete `Unfulfillable` state: reinjectable resource-
                // availability failure. Tallied as `fail_final` for the
                // operator-readable buckets until the dedicated
                // reinject/blocked bucket lands; same mapping as the
                // legacy `Failed { Unfulfillable, .. }` arm above so the
                // total partition stays stable across the variant cutover.
                TaskState::Unfulfillable { .. } => o.fail_final += 1,
                // Discrete `InvalidTask` state: terminal, non-
                // reinjectable structural failure. Tallied as
                // `fail_final` (sibling to `Unfulfillable`) until the
                // dedicated invalid_task stat line lands in Part C; the
                // mapping keeps the operator-readable partition stable.
                TaskState::InvalidTask { .. } => o.fail_final += 1,
                // Discovery-time skip: a SUCCESS-LIKE terminal kept in its
                // OWN accounting bucket (`skipped`), NOT folded into
                // `succeeded` (the run-complete summary / narrator success
                // count must report only work this run performed) and NOT
                // any failure bucket. It IS a terminal, fully-accounted
                // outcome, so `total_terminal()` counts it — otherwise the
                // finalize accounting (`stranded = total - total_terminal()`)
                // would mis-classify every skip as STRANDED and false-abort
                // a clean skip-bearing run as `ClusterCollapsed`.
                TaskState::SkippedAlreadyDone { .. } => o.skipped += 1,
                // Succeeded setup-kind task: a SUCCESS-LIKE terminal in its
                // OWN bucket (`setup_succeeded`), NEVER `succeeded` (the
                // run-complete success count reports only worker WORK). Like
                // `skipped`, it IS a terminal outcome so `total_terminal()`
                // counts it.
                TaskState::SetupCompleted { .. } => o.setup_succeeded += 1,
                // SecondaryAffine gate (READY-not-EXECUTED): an INERT
                // terminal in its OWN bucket (`affine_ready`), NEVER
                // `succeeded`/`setup_succeeded`/any failure class (the
                // primary never executed it). Like `skipped`/`setup_succeeded`
                // it IS a terminal outcome so `total_terminal()` counts it
                // (no STRANDED false-abort at finalize).
                TaskState::AffineReady { .. } => o.affine_ready += 1,
                // Non-terminal: Pending, InFlight, QueuedAfterLocalDependency,
                // and Blocked all contribute to neither bucket.
                // `QueuedAfterLocalDependency` is a live work task awaiting
                // its secondary's local import — counting it would double-
                // tally on the eventual run. Blocked tasks are cascade-paused
                // dependents that will auto-resume to Pending when their
                // prereq completes; they're not a terminal outcome and
                // counting them as one would double-tally on the eventual
                // resumed run.
                TaskState::Pending { .. }
                | TaskState::InFlight { .. }
                | TaskState::QueuedAfterLocalDependency { .. }
                | TaskState::Blocked { .. } => {}
            }
        }
        // Settled (spilled) entries: the same per-class mapping, off the
        // slim index. `FailedFinal` routes through the ONE shared
        // `fold_failed_kind` so the kind partition cannot drift from the
        // fat arm's. (`QueuedAfterLocalDependency` is non-terminal and never
        // settles, so it has no `SettledClass` arm.)
        for (_, entry) in self.settled_entries() {
            match &entry.class {
                SettledClass::Completed => o.succeeded += 1,
                SettledClass::FailedFinal(kind) => fold_failed_kind(kind, &mut o),
                SettledClass::InvalidTask => o.fail_final += 1,
                SettledClass::SkippedAlreadyDone => o.skipped += 1,
                SettledClass::SetupCompleted => o.setup_succeeded += 1,
                SettledClass::AffineReady => o.affine_ready += 1,
            }
        }
        o
    }

    /// Iterator over `(task_hash, &TaskInfo)` for every FAT (in-memory)
    /// entry, regardless of state. SETTLED entries carry no in-memory
    /// `TaskInfo` and are not yielded — the remaining production reader
    /// (the preferred-secondaries validator) is live-entry-shaped by
    /// design (a terminal task never re-dispatches, so its preference
    /// list is operationally dead); identity-shaped lookups go through
    /// [`Self::task_deps_for_identity`] / [`Self::task_hash_for_dep`],
    /// which DO consult the settled index.
    pub fn iter_all(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().map(|(h, s)| {
            let t = match s {
                TaskState::Pending { task, .. }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task, .. }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::InvalidTask { task, .. }
                | TaskState::SkippedAlreadyDone { task, .. }
                | TaskState::SetupCompleted { task, .. }
                | TaskState::AffineReady { task, .. }
                | TaskState::QueuedAfterLocalDependency { task, .. }
                | TaskState::Blocked { task, .. } => task,
            };
            (h, t)
        })
    }

    /// Iterator over `(task_hash, &TaskInfo)` for FAT (in-memory)
    /// terminal entries (`Completed`, `Failed`, `Unfulfillable`,
    /// `InvalidTask`, `SkippedAlreadyDone`, `SetupCompleted`,
    /// `AffineReady`). `Blocked` and `QueuedAfterLocalDependency` are
    /// non-terminal and are excluded. A `SkippedAlreadyDone` IS surfaced —
    /// its dependents
    /// resolve their `task_depends_on` reference against it exactly as
    /// against a `Completed` prereq. SETTLED terminals are not yielded
    /// (no in-memory `TaskInfo`); pair with [`Self::settled_entries`]
    /// for the full terminal set.
    pub fn iter_terminal(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Completed { task, .. }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::InvalidTask { task, .. }
            | TaskState::SkippedAlreadyDone { task, .. }
            | TaskState::SetupCompleted { task, .. }
            // `AffineReady` IS terminal for dependency-resolution: a build
            // gated on the gate resolves its dep against it exactly as
            // against a `Completed`/`SetupCompleted` prereq.
            | TaskState::AffineReady { task, .. } => Some((h, task)),
            _ => None,
        })
    }

    pub fn current_primary(&self) -> Option<&str> {
        self.current_primary.as_deref()
    }

    /// Whether the named peer-id is currently a live member of the
    /// cluster — i.e. its `peer_state` entry exists and is `Alive`. A
    /// never-seen id (no `PeerJoined` applied) and a `Dead` id (a
    /// `PeerRemoved`/sticky-removal id) both read `false`. This is the
    /// read-side of the `peer_state` membership ledger the `PeerJoined`/
    /// `PeerRemoved` apply rules maintain; the liveness bit itself stays
    /// module-private (callers get a `bool`, never the `PeerState` enum).
    pub fn is_peer_alive(&self, peer_id: &str) -> bool {
        self.peer_state
            .get(peer_id)
            .is_some_and(|entry| entry.state == super::types::PeerState::Alive)
    }

    /// The replicated-membership view of `peer_id`, projected for
    /// diagnostics consumers (the egress no-route message split): a live
    /// member, an authoritatively-removed one (`PeerRemoved` ledger), or
    /// an id that never joined. Distinct from the transport
    /// `MembershipView` — a peer can be a live replicated member while
    /// this node has no transport wire to it, and an honest "no route"
    /// line must say which of those two states it is in.
    pub fn peer_membership(&self, peer_id: &str) -> super::types::PeerMembership {
        match self.peer_state.get(peer_id).map(|e| &e.state) {
            Some(super::types::PeerState::Alive) => super::types::PeerMembership::AliveMember,
            Some(super::types::PeerState::Dead) => super::types::PeerMembership::RemovedMember,
            None => super::types::PeerMembership::NeverJoined,
        }
    }

    /// The membership-incarnation generation recorded for `peer_id` —
    /// `0` for a never-seen id (the cold generation). The originators of
    /// `PeerJoined`/`PeerRemoved` stamp this onto the mutation (the
    /// generation is read-derived, never version-stamped): a removal
    /// kills the CURRENT incarnation; a re-emit re-joins it.
    pub fn peer_member_gen(&self, peer_id: &str) -> u64 {
        self.peer_state.get(peer_id).map_or(0, |e| e.member_gen)
    }

    /// The re-admission ticket for a REMOVED peer (`peer_state` `Dead`),
    /// or `None` for a live / never-seen id: the generation a
    /// re-admitting `PeerJoined` must carry (`dead generation + 1`) plus
    /// the advertisement preserved on the capability tombstone, so the
    /// primary's frame-ingest re-admission seam restores the EXACT
    /// capability the member departed with (the removed node never
    /// re-advertises — it does not know it was removed).
    pub fn removed_peer_readmission(&self, peer_id: &str) -> Option<PeerReadmission> {
        let entry = self.peer_state.get(peer_id)?;
        if entry.state != super::types::PeerState::Dead {
            return None;
        }
        let (is_observer, can_be_primary) = match self.capabilities.get(peer_id) {
            Some(super::types::CapabilityEntry::Departed {
                is_observer,
                can_be_primary,
                ..
            })
            | Some(super::types::CapabilityEntry::Advertised {
                is_observer,
                can_be_primary,
                ..
            }) => (*is_observer, *can_be_primary),
            None => (false, false),
        };
        Some(PeerReadmission {
            member_gen: entry.member_gen + 1,
            is_observer,
            can_be_primary,
        })
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

    /// True iff `phase` was declared `may_be_empty` by the consumer
    /// (`PhaseSpec.may_be_empty`, replicated via
    /// `ClusterMutation::PhaseMayBeEmptySet`). Read by the empty-drain
    /// proceed-or-fail policy to let an intentional pure-sequencing gate
    /// drain through with zero dispatched items instead of failing loud.
    pub fn phase_may_be_empty(&self, phase: &PhaseId) -> bool {
        self.phase_may_be_empty.contains(phase)
    }

    /// True iff `phase` was declared `barrier=False` by the consumer
    /// (`PhaseSpec.barrier=False`, replicated via
    /// `ClusterMutation::PhaseNoBarrierSet`). Read by:
    ///
    ///   * the runtime-spawn barrier-violation interlock in
    ///     `apply_spawn_tasks` (primary + promoted-secondary): a target
    ///     phase accepts a runtime spawn iff it has already started OR
    ///     this returns `true` (the pipelined-edge opt-in);
    ///   * the pool's initial-state assignment via
    ///     [`super::super::pending_pool::PendingPool::set_no_barrier_phases`]
    ///     so a no-barrier phase starts `Active` rather than `Blocked`.
    ///
    /// Empty on the common strict-barrier run (every phase barrier=True);
    /// non-empty iff the consumer opted at least one phase in.
    pub fn phase_no_barrier(&self, phase: &PhaseId) -> bool {
        self.phase_no_barrier.contains(phase)
    }

    /// The full set of phases the consumer declared `barrier=False`.
    /// Used by callers that need the SET (pool initialisation passes the
    /// whole set in one go through `set_no_barrier_phases`); per-phase
    /// queries should use [`Self::phase_no_barrier`] instead.
    pub fn phase_no_barrier_set(&self) -> &std::collections::HashSet<PhaseId> {
        &self.phase_no_barrier
    }

    /// The replicated respawn-policy CAPS (`ClusterMutation::
    /// RespawnPolicySet`, set once per run by the submitter's seed when
    /// `--respawn-policy` is enabled). `None` = the run launched with
    /// the policy disabled. Read by the promoted primary's hydrate to
    /// re-arm the respawn DECISION pipeline after failover/relocation —
    /// the sibling [`Self::respawn_events`] ledger carries the budget's
    /// SPEND; this carries its CAPS.
    pub fn respawn_policy(&self) -> Option<super::types::ReplicatedRespawnPolicy> {
        self.respawn_policy
    }

    /// True iff `phase`'s `on_phase_end` edge COMPLETED on some
    /// authoritative primary (hook fired + hook-queued commands drained +
    /// `mark_phase_done` issued), replicated via
    /// `ClusterMutation::PhaseEnded` (#343). Read by the hydration-time
    /// no-redo decision: a terminal-only phase is seeded straight to
    /// `Done` — suppressing an `on_phase_end` re-fire (#326) — ONLY when
    /// this fact is present; without it the phase flows through the live
    /// cascade and fires its FIRST `on_phase_end` (the freshly-discovered
    /// all-`SkippedAlreadyDone` phase).
    pub fn phase_ended(&self, phase: &PhaseId) -> bool {
        self.phases_ended.contains(phase)
    }

    /// Phase-boundary policy: `phase`'s formal boundary (its start edge AND
    /// its complete edge) is OPEN iff every direct phase-dep of `phase` has
    /// formally completed (its [`Self::phase_ended`] is `true`). The single
    /// predicate the narrator's start/complete gates and the coordinator's
    /// `phase_can_proceed` + `fire_initial_phase_starts` all consult, so the
    /// invariant
    ///
    ///   * I1. phase P cannot FORMALLY START before every phase-dep of P has
    ///     formally completed (regardless of barrier);
    ///   * I2. phase P cannot FORMALLY COMPLETE before every phase-dep of P
    ///     has formally completed (regardless of barrier),
    ///
    /// is enforced once, at one boundary, against the same replicated
    /// `PhaseEnded` fact. Strict — `barrier=False` is the I3 dispatch
    /// authorization (a task of P may execute before P's predecessor's
    /// `PhaseEnded` fires; the runtime-spawn barrier interlock in
    /// `apply_spawn_tasks` and the pool's `set_no_barrier_phases` own that
    /// path), not a relaxation of the formal boundary; this predicate
    /// therefore reads no barrier set.
    ///
    /// Vacuously `true` for a phase whose deps slot is missing from
    /// `phase_deps` (an undeclared / no-deps phase has no upstream to wait
    /// on — the strict initial-active root). Reads only the per-phase dep
    /// slot and the `phases_ended` set; no live-bit consultation,
    /// no transitive walk — the strict "all direct deps' `PhaseEnded`
    /// fired" semantics ride the replicated fact directly. Transitivity
    /// is implicit: a dep's own `PhaseEnded` could only have fired
    /// against ITS own boundary, so the predicate is closed under the
    /// dep chain by induction.
    pub fn phase_boundary_open(&self, phase: &PhaseId) -> bool {
        let Some(deps) = self.phase_deps.get(phase) else {
            return true;
        };
        deps.iter().all(|dep| self.phases_ended.contains(dep))
    }

    /// Per-phase derived view recomputed from the CRDT: for every phase
    /// that owns at least one task, the [`PhaseRollup`] of `has_any`,
    /// `has_live`, and `dispatchable`.
    ///
    /// # Single source of the phase state machine
    ///
    /// This is the no-duplication seam for "is this phase started /
    /// done / dispatchable". An observer holds NO `PendingPool` — it
    /// carries only the replicated `ClusterState` — so the pool's
    /// pool-state reads are RECOMPUTED here from the ledger: the
    /// per-task terminal set, the per-phase live bit, and the static
    /// `phase_deps` graph. The recomputation mirrors the pool's own
    /// resolution rule (a dep is satisfied once its prereq is terminal;
    /// a phase is dispatchable once every phase it depends on has fully
    /// terminated) so the pool view and the CRDT view converge.
    ///
    /// Both the operator run-narrator (`crate::run_narrator`, this
    /// crate) and the pyo3 stats snapshot
    /// (`StatsSnapshot::from_cluster_state`, the leaf `dynrunner-pyo3`
    /// crate) read this rather than each re-deriving the terminal-set +
    /// dispatchability walk.
    ///
    /// `O(n)` over the ledger (a single pass to build the live/any maps)
    /// plus a depth-bounded dep walk per phase; not hot-path code (the
    /// callers run on completion / on a multi-minute cadence). The dep
    /// walk is bounded by the dep graph, which `PendingPool::new` already
    /// cycle-rejects, so it terminates.
    pub fn phase_rollups(&self) -> HashMap<&PhaseId, PhaseRollup> {
        // Phase → (has any task, has any live task). Built in one pass
        // over the ledger. A phase absent from the map owns no tasks
        // (vacuously not-live / not-present).
        let mut base: HashMap<&PhaseId, (bool, bool)> = HashMap::new();
        for st in self.tasks.values() {
            let task = match st {
                TaskState::Pending { task, .. }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task, .. }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::InvalidTask { task, .. }
                | TaskState::SkippedAlreadyDone { task, .. }
                | TaskState::SetupCompleted { task, .. }
                | TaskState::AffineReady { task, .. }
                | TaskState::QueuedAfterLocalDependency { task, .. }
                | TaskState::Blocked { task, .. } => task,
            };
            let entry = base.entry(&task.phase_id).or_insert((false, false));
            entry.0 = true;
            if !st.is_terminal() {
                entry.1 = true;
            }
        }
        // Settled (spilled) entries: terminal by construction — they set
        // `has_any` for their phase and are never live.
        for (_, settled) in self.settled_entries() {
            base.entry(&settled.phase_id).or_insert((false, false)).0 = true;
        }

        // `phase_dispatchable` consults the per-phase live bit; project
        // it once for the dep walk. A phase absent here is vacuously
        // satisfied (`false`).
        let phase_has_live: HashMap<&PhaseId, bool> =
            base.iter().map(|(p, (_, live))| (*p, *live)).collect();

        base.iter()
            .map(|(&phase, &(has_any, has_live))| {
                (
                    phase,
                    PhaseRollup {
                        has_any,
                        has_live,
                        dispatchable: phase_dispatchable(phase, &self.phase_deps, &phase_has_live),
                    },
                )
            })
            .collect()
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

    /// Whether the named peer-id carries the explicit
    /// `RoleTable.can_be_primary` capability — the single authoritative
    /// "may this peer ever host the primary role" property. Set by the
    /// peer at join (`PeerJoined { can_be_primary = true }`) and
    /// updatable at runtime by a client (`SetCanBePrimary`); NOT deduced
    /// from membership / liveness / observer status. Read-side of the
    /// capability set the apply rules maintain; callers get a `bool`,
    /// the set itself stays reachable via `role_table()`.
    pub fn can_be_primary(&self, peer_id: &str) -> bool {
        self.role_table.can_be_primary.contains(peer_id)
    }

    /// The peer ids the `capabilities` 2P-set holds as `Departed`
    /// tombstones — the AUTHORITATIVE departure view (a genuine
    /// `PeerRemoved` wrote each one). The post-mesh roster re-emit
    /// (`rebroadcast_full_roster`) iterates these to re-emit a
    /// `PeerRemoved` per departed id so a reconnecting node's LIVENESS
    /// view catches up (B5/C6 — the 2P-set view, NOT `self.secondaries`,
    /// which has already dropped them). Capability convergence itself no
    /// longer hinges on this re-emit: it rides the snapshot-healable
    /// 2P-set + the digest's `capabilities_hash`.
    pub fn departed_capability_ids(&self) -> impl Iterator<Item = &str> {
        self.capabilities.iter().filter_map(|(id, entry)| {
            matches!(entry, super::types::CapabilityEntry::Departed { .. }).then_some(id.as_str())
        })
    }

    /// Resolve a dependency's full `(phase_id, task_id)` identity to its
    /// wire-canonical hash via a linear scan over `self.tasks`. Returns
    /// `None` if no entry in the ledger carries that exact identity.
    ///
    /// The match is on BOTH phase and task_id: the same `task_id` in
    /// two different phases is a distinct task with a distinct hash, so
    /// a dep resolves only against the ledger entry whose phase AND
    /// task_id agree with the dep.
    ///
    /// O(n) over the ledger; the CRDT does not maintain a reverse index
    /// (the live `PendingPool` does, but it lives only on the primary;
    /// every replica must resolve locally to converge on dependency
    /// states). The scan keeps the dependency-tracking concern
    /// self-contained inside cluster_state.
    pub fn task_hash_for_dep(&self, phase_id: &PhaseId, task_id: &str) -> Option<&str> {
        self.tasks
            .iter()
            .find_map(|(h, s)| {
                let task = match s {
                    TaskState::Pending { task, .. }
                    | TaskState::InFlight { task, .. }
                    | TaskState::Completed { task, .. }
                    | TaskState::Failed { task, .. }
                    | TaskState::Unfulfillable { task, .. }
                    | TaskState::InvalidTask { task, .. }
                    | TaskState::SkippedAlreadyDone { task, .. }
                    | TaskState::SetupCompleted { task, .. }
                    | TaskState::AffineReady { task, .. }
                    | TaskState::QueuedAfterLocalDependency { task, .. }
                    | TaskState::Blocked { task, .. } => task,
                };
                (task.task_id == task_id && &task.phase_id == phase_id).then_some(h.as_str())
            })
            // Settled (spilled) entries resolve deps too — a completed
            // prereq is the COMMON dep target and is exactly what spills.
            .or_else(|| {
                self.settled_entries().find_map(|(h, entry)| {
                    (entry.task_id == task_id && &entry.phase_id == phase_id)
                        .then_some(h.as_str())
                })
            })
    }

    /// The `task_depends_on` edges of the task with the given full
    /// `(phase_id, task_id)` identity — fat or settled. The dispatch-time
    /// `inherit_outputs` ancestry walk reads a COMPLETED (and typically
    /// SETTLED) predecessor's dep edges here; the slim index retains them
    /// for exactly this reader. Owned clone (the walk recurses while the
    /// caller may hold no borrow).
    pub(crate) fn task_deps_for_identity(
        &self,
        phase_id: &PhaseId,
        task_id: &str,
    ) -> Option<Vec<dynrunner_core::TaskDep>> {
        self.tasks
            .values()
            .find_map(|s| {
                let task = s.task();
                (task.task_id == task_id && &task.phase_id == phase_id)
                    .then(|| task.task_depends_on.clone())
            })
            .or_else(|| {
                self.settled_entries().find_map(|(_, entry)| {
                    (entry.task_id == task_id && &entry.phase_id == phase_id)
                        .then(|| entry.task_depends_on.clone())
                })
            })
    }

    /// Borrow a completed dependency's cached [`TaskOutputs`] by its
    /// full `(phase_id, task_id)` identity. Returns `None` if no task
    /// with that identity has reached `Completed` with a non-empty
    /// `result_data` payload yet (the task is still in-flight, never
    /// published outputs, the dep names an identity the ledger does not
    /// know, or the payload failed to decode and dependents that need a
    /// non-empty view should treat the empty `TaskOutputs` insert as
    /// their answer — see the cache-populate helper).
    ///
    /// Resolution is phase-aware: the dep's `(phase_id, task_id)` is
    /// resolved to the unique ledger hash (so the same `task_id` in two
    /// phases reads two distinct output entries), then the hash-keyed
    /// cache is read. The dispatch-time predecessor-outputs assembler
    /// reads this accessor to attach each dependent's predecessor
    /// outputs to its `TaskAssignment`. The borrow is invalidated by
    /// the next `&mut self` apply call; callers that need ownership
    /// across an apply boundary must `.clone()` the returned reference.
    pub fn outputs_for(&self, phase_id: &PhaseId, task_id: &str) -> Option<&TaskOutputs> {
        let hash = self.task_hash_for_dep(phase_id, task_id)?;
        self.task_outputs.get(hash)
    }

    /// Gather every recorded [`TaskOutputs`] for the tasks of `phase_id`,
    /// keyed by `task_id`. The phase-lifecycle `on_phase_end` hook reads
    /// this to hand a consumer's callback the just-completed phase's
    /// PUBLISHED outputs (`publish_string` / `publish(.., key=..)`) WITHOUT
    /// a filesystem path — the bytes already rode the wire on each task's
    /// `DonePayload` → `result_data` and landed in this `task_outputs`
    /// cache when the `TaskCompleted` applied, so by the time the cascade
    /// fires `on_phase_end(phase_id, ..)` (AFTER that apply) the phase's
    /// outputs are present here.
    ///
    /// Only tasks that actually recorded outputs appear (a task that
    /// published nothing contributes no entry, mirroring `outputs_for`'s
    /// `None`). Resolution walks the ledger once, matching each entry's
    /// `phase_id`, then reads the hash-keyed `task_outputs` cache — the
    /// same phase-aware identity rule `outputs_for` uses, lifted to the
    /// whole phase. Returns owned clones so the caller (the
    /// `&mut self`-holding coordinator firing `on_phase_end`) holds no
    /// borrow across the callback.
    pub fn phase_task_outputs(
        &self,
        phase_id: &PhaseId,
    ) -> std::collections::BTreeMap<String, TaskOutputs> {
        self.tasks
            .iter()
            .filter_map(|(hash, state)| {
                let task = match state {
                    TaskState::Pending { task, .. }
                    | TaskState::InFlight { task, .. }
                    | TaskState::Completed { task, .. }
                    | TaskState::Failed { task, .. }
                    | TaskState::Unfulfillable { task, .. }
                    | TaskState::InvalidTask { task, .. }
                    | TaskState::SkippedAlreadyDone { task, .. }
                    | TaskState::SetupCompleted { task, .. }
                    | TaskState::AffineReady { task, .. }
                    | TaskState::QueuedAfterLocalDependency { task, .. }
                    | TaskState::Blocked { task, .. } => task,
                };
                if &task.phase_id != phase_id {
                    return None;
                }
                let outputs = self.task_outputs.get(hash)?;
                Some((task.task_id.clone(), outputs.clone()))
            })
            // Settled (spilled) entries of the phase: their identity comes
            // off the slim index; the output VALUES never left the
            // `task_outputs` map (it is the hot output index — not
            // evicted), so the same hash-keyed read serves them.
            .chain(self.settled_entries().filter_map(|(hash, entry)| {
                if &entry.phase_id != phase_id {
                    return None;
                }
                let outputs = self.task_outputs.get(hash)?;
                Some((entry.task_id.clone(), outputs.clone()))
            }))
            .collect()
    }

    /// Per-phase [`PhaseTaskPartition`] over the replicated ledger: one
    /// ledger pass matching `task.phase_id == phase`, every entry placed in
    /// exactly one of the four buckets (`to_run` = `Pending` / `InFlight` /
    /// `Blocked`, `done` = `Completed`, `failed` = `Failed` /
    /// `Unfulfillable` / `InvalidTask`, `skipped` = `SkippedAlreadyDone` —
    /// see the struct doc for the bucket rationale).
    ///
    /// The SINGLE owner of the "what is each of this phase's tasks,
    /// operationally" projection: the operator run-narrator's per-phase
    /// "<N> to run, <M> skipped (already done)" line, its running
    /// "overall" total (which sums this across started phases — terminal
    /// tasks read `done`/`failed` there, never inflating "to run"), and
    /// any structural reader all read it here, so none re-walks the ledger
    /// with a private partition rule. Ledger-derived, so it is
    /// failover-consistent (every replica converges to the same answer
    /// after the same mutation set lands).
    pub fn phase_task_partition(&self, phase: &PhaseId) -> PhaseTaskPartition {
        let mut p = PhaseTaskPartition::default();
        for state in self.tasks.values() {
            if &state.task().phase_id != phase {
                continue;
            }
            match state {
                TaskState::Pending { .. }
                | TaskState::InFlight { .. }
                // `QueuedAfterLocalDependency` is live remaining work — a
                // task committed to a secondary but not yet running (awaiting
                // its local import) — so it is `to_run`, exactly like
                // `InFlight`/`Pending`/`Blocked`.
                | TaskState::QueuedAfterLocalDependency { .. }
                | TaskState::Blocked { .. } => p.to_run += 1,
                // `SetupCompleted` is success-like work this run performed
                // in-process — folded into `done` for the per-phase
                // progress partition (the OUTCOME-level `setup_succeeded`
                // bucket keeps it out of the global success count; this
                // phase-progress view is a distinct concern). `AffineReady`
                // (a resolved SecondaryAffine gate) folds into `done` for the
                // SAME reason: the gate resolved so the phase advances, while
                // the OUTCOME-level `affine_ready` bucket keeps it out of the
                // global success count.
                TaskState::Completed { .. }
                | TaskState::SetupCompleted { .. }
                | TaskState::AffineReady { .. } => p.done += 1,
                TaskState::Failed { .. }
                | TaskState::Unfulfillable { .. }
                | TaskState::InvalidTask { .. } => p.failed += 1,
                TaskState::SkippedAlreadyDone { .. } => p.skipped += 1,
            }
        }
        // Settled (spilled) entries of this phase: same buckets, off the
        // slim class (`to_run` is impossible — only terminals settle).
        for (_, entry) in self.settled_entries() {
            if &entry.phase_id != phase {
                continue;
            }
            match entry.class {
                SettledClass::Completed
                | SettledClass::SetupCompleted
                | SettledClass::AffineReady => p.done += 1,
                SettledClass::FailedFinal(_) | SettledClass::InvalidTask => p.failed += 1,
                SettledClass::SkippedAlreadyDone => p.skipped += 1,
            }
        }
        p
    }

    /// Setup-task lifecycle progress over the replicated ledger (#508) —
    /// the value shape of [`SetupProgress`]: `(complete, total)` over every
    /// SETUP-kind task (`TaskKind::is_setup`) the run planned.
    ///
    /// The SINGLE owner of the "how far has the setup phase got" projection
    /// the operator run-narrator's setup milestones read. Derived from the
    /// SAME `tasks` ledger the primary's `setup_dispatch` already drives the
    /// setup lifecycle through (`Pending → InFlight → SetupCompleted`, or a
    /// terminal failure via `apply_fail_permanent`) — never a separate
    /// replicated tally, so it stays failover-consistent (every replica
    /// converges to the same answer after the same mutation set lands).
    ///
    /// `total` counts setup-kind entries regardless of state; `complete`
    /// counts the terminal ones — a setup SUCCESS (`SetupCompleted`) or a
    /// setup terminal FAILURE (`Failed` / `Unfulfillable` / `InvalidTask`),
    /// i.e. [`TaskState::is_terminal`] true. "Complete" is "no longer
    /// pending" (the operator's setup-progress concern); the success/failure
    /// split is the run summary's, not this view's.
    ///
    /// Fat (in-memory) entries are classified by `task.kind.is_setup()`. A
    /// settled (spilled) entry carries no `kind`; only the
    /// `SettledClass::SetupCompleted` class is setup-distinguishable, so a
    /// settled setup SUCCESS is counted (`complete` and `total`) while a
    /// settled setup FAILURE is indistinguishable from a worker failure and
    /// is not — acceptable because setup tasks are few and execute at run
    /// start (long before the terminal-ledger spill), so they live in the
    /// fat map for the whole window the narrator emits over.
    pub fn setup_progress(&self) -> SetupProgress {
        let mut s = SetupProgress::default();
        for state in self.tasks.values() {
            if !state.task().kind.is_setup() {
                continue;
            }
            s.total += 1;
            if state.is_terminal() {
                s.complete += 1;
            }
        }
        // Settled setup successes: the only setup-distinguishable settled
        // class. A settled entry is terminal by construction, so it counts
        // toward both `complete` and `total`.
        for (_, entry) in self.settled_entries() {
            if matches!(entry.class, SettledClass::SetupCompleted) {
                s.total += 1;
                s.complete += 1;
            }
        }
        s
    }

    /// Whether the run has been declared finished by the primary.
    /// Sticky monotonic flag: once set, never clears for the lifetime
    /// of this state. Secondaries read this to break their main loop
    /// when the peer mesh is still up but the run is genuinely over.
    pub fn run_complete(&self) -> bool {
        self.run_complete
    }

    /// The abort reason if the run has been declared ABORTED by the
    /// primary (`ClusterMutation::RunAborted`), else `None`. The
    /// failure twin of [`Self::run_complete`]: sticky monotonic — once
    /// `Some`, never clears. Secondaries check this BEFORE the
    /// `run_complete` break in `process_tasks` and exit non-zero
    /// (`RunOutcome::Terminal` projecting to `SecondaryTerminal::Aborted`);
    /// the `mesh_watchdog` disarms on it too.
    pub fn run_aborted(&self) -> Option<&str> {
        self.run_aborted.as_deref()
    }

    /// The terminal verdict's FINALIZED per-class outcome counts, carried
    /// atomically on the `RunComplete`/`RunAborted` mutation and latched
    /// set-once. `Some` exactly when a terminal verdict has landed — so a
    /// node that observes the verdict (the primary that authored it, OR an
    /// observer/secondary that applied it) ALSO has the authoritative counts
    /// in hand, with no separate per-task convergence to wait on. The
    /// run-narrator's terminal summary reads THIS (the single source of
    /// truth the primary decided the verdict from) instead of re-folding its
    /// own — possibly unconverged — ledger via `outcome_counts()`. `None`
    /// until the first terminal verdict lands.
    pub fn terminal_outcome(&self) -> Option<TerminalOutcomeCounts> {
        self.terminal_outcome
    }

    /// Whether a GRACEFUL abort has been requested
    /// (`ClusterMutation::GracefulAbortRequested`) — the replicated
    /// dispatch-freeze latch. Sticky monotonic, like
    /// [`Self::run_complete`]: once set, never clears. Consumed by the
    /// primary's dispatch-view gate (no new work leaves the ready pool),
    /// the respawn-admission gate, and the drain/relocate/terminal
    /// decisions; by each secondary's drain-exit decision; and by the
    /// observer's verdict derivation (`run_complete ∧ graceful_abort` =
    /// the graceful-abort verdict).
    pub fn graceful_abort_requested(&self) -> bool {
        self.graceful_abort_requested
    }

    /// Whether THIS exact secondary incarnation has been marked for
    /// graceful wind-down (`ClusterMutation::WindDownRequested` with a
    /// matching `(secondary_id, member_gen)`) — the per-peer,
    /// incarnation-scoped sibling of [`Self::graceful_abort_requested`].
    /// The directed secondary consults this with its OWN id and its OWN
    /// live `member_gen`; a directive minted for a prior incarnation
    /// (lower generation) never matches, so a re-seated id is not wound
    /// down by a stale directive. Grow-only + monotone, like the global
    /// graceful-abort latch: once the pair is recorded, it stays. Consumed
    /// by the directed secondary's graceful-drain exit gate (it departs at
    /// its next quiescence; see `process_tasks`).
    pub fn wind_down_requested(&self, secondary_id: &str, member_gen: u64) -> bool {
        self.wind_down_requested
            .contains(&(secondary_id.to_string(), member_gen))
    }

    /// Iterator over every `(secondary_id, member_gen)` pair the grow-only
    /// [`ClusterMutation::WindDownRequested`] set contains — the narrator's
    /// read surface for the per-incarnation wind-down directives. Sibling of
    /// the point-query [`Self::wind_down_requested`]: that one answers
    /// "AM I targeted at THIS generation?" for the directed secondary; this
    /// one yields every recorded pair so the [`crate::run_narrator`]'s
    /// edge-set can emit ONE WARN line per `(id, gen)` the moment it lands
    /// in the converged mirror. Borrow-only — the set is grow-only and
    /// per-pair narration is once-only via the narrator's
    /// `wind_down_announced` edge-set, so an owned-clone copy would be pure
    /// waste at the observer cadence.
    pub fn wind_down_requested_pairs(&self) -> impl Iterator<Item = (&str, u64)> {
        self.wind_down_requested
            .iter()
            .map(|(id, member_gen)| (id.as_str(), *member_gen))
    }

    /// Count of `InFlight` ledger entries currently assigned to
    /// `secondary` — the CRDT-derived "active workers" occupancy of one
    /// secondary. Pure projection of the replicated `tasks` ledger, so
    /// every replica (live primary, promoted primary, observer) reads
    /// the same answer. Consumed by the graceful-abort relocation
    /// policy (`RelocationPolicy::MostActiveWorkers`) and its drain
    /// decisions.
    pub fn inflight_count_for_secondary(&self, secondary: &str) -> usize {
        self.tasks
            .values()
            .filter(|state| {
                matches!(state, TaskState::InFlight { secondary: s, .. } if s == secondary)
            })
            .count()
    }

    /// The replicated per-run discovery-debt latch (V6). `Undeclared` (the
    /// default/bottom) means no discovery is owed — the run was cold-seeded
    /// (mode-1 / legacy) or never declared debt; `Owed` means a relocated
    /// compute-peer primary still owes a `discover_items` seed; `Settled`
    /// (the lattice TOP) means its discovery has completed (the tasks are now
    /// in the CRDT). Sticky-monotone THREE-state lattice: ratchets only UP
    /// (`Undeclared → Owed → Settled`), never back (join = `max` over
    /// `Undeclared ⊑ Owed ⊑ Settled`). The discover-on-promotion driver
    /// (Phase 5b) gates on `== Owed`; both `Undeclared` and `Settled` are
    /// `!= Owed`, so a cold/legacy run and a post-discovery run alike skip it.
    pub fn discovery_debt(&self) -> DiscoveryDebt {
        self.discovery_debt
    }

    /// Borrow a secondary's static capacity record (worker-slot count +
    /// advertised resources), or `None` if no `SecondaryCapacity`
    /// mutation for that id has been applied yet. Set once per
    /// secondary by the `SecondaryCapacity` apply rule.
    pub fn secondary_capacity(&self, secondary: &str) -> Option<&SecondaryCapacityRecord> {
        self.secondary_capacities.get(secondary)
    }

    /// The set of secondary ids the cluster has a replicated capacity
    /// record for — the known-secondary roster derived purely from the
    /// CRDT. A freshly-promoted primary and observers read this to
    /// reconstruct the worker roster on failover (the roster was
    /// historically 100% primary-local and lost on promotion).
    pub fn known_secondaries(&self) -> impl Iterator<Item = &str> {
        self.secondary_capacities.keys().map(String::as_str)
    }

    /// The subset of [`Self::known_secondaries`] whose membership is NOT
    /// authoritatively removed — the roster a primary-local rebuild
    /// (worker slots, secondary connections) may derive state from.
    ///
    /// Capacity records are set-once and never deleted: a removed peer's
    /// `secondary_capacities` entry outlives its membership (preserved so
    /// a re-admission restores the EXACT capacity the member departed
    /// with — the removed node never re-advertises). The membership
    /// ledger (`peer_state` `Dead`, written in lockstep with the
    /// `CapabilityEntry::Departed` tombstone by `apply_peer_removed`) is
    /// therefore the filter: a `RemovedMember` id is excluded; a
    /// re-admission (a generation-advancing `PeerJoined`) flips the same
    /// entry back to `Alive` and the id re-enters this view with its
    /// preserved capacity. A `NeverJoined` capacity-bearer is INCLUDED —
    /// membership may lag capacity at this replica (out-of-order apply),
    /// and only an authoritative removal may suppress a rebuild.
    pub fn live_known_secondaries(&self) -> impl Iterator<Item = &str> {
        self.known_secondaries()
            .filter(|id| self.peer_membership(id) != super::types::PeerMembership::RemovedMember)
    }

    /// The replicated-membership roster of peers that POSITIVELY run a
    /// live worker-secondary: a peer counts IFF it (a) advertised
    /// worker-secondary capacity (`secondary_capacities` carries a record
    /// with `worker_count > 0` — the positive "has a secondary" signal,
    /// originated by the primary alongside `PeerJoined` on welcome) AND
    /// (b) is currently a live member (`is_peer_alive`). BOTH predicates
    /// are positive — "has a secondary" and "is live" — never a negation
    /// of the primary or observer role.
    ///
    /// Positive-filter rationale: roles are an independent subset of
    /// {primary, secondary, observer} per host, so "is an alive secondary"
    /// MUST be answered by the secondary capability itself, not by
    /// `!primary && !observer`. A peer that advertises BOTH a primary and a
    /// worker-secondary under one peer-id counts (it advertised workers); an
    /// observer is excluded by lacking worker capacity (`worker_count == 0`
    /// is structural for observers), NOT by a `!is_observer` test; a
    /// primary-only host is excluded by having no worker capacity.
    ///
    /// Membership is the faithful liveness signal in the SETUP /
    /// pre-operational window, where no `peer_keepalives` map exists yet:
    /// the set grows as each peer's `PeerJoined` + `SecondaryCapacity`
    /// land (applied even pre-`Operational` via the setup recv loop's
    /// `ClusterMutation` arm). The OPERATIONAL signal is the coordinator's
    /// keepalive map; `alive_secondary_ids` selects whichever signal
    /// exists in the current regime.
    pub fn alive_secondary_members(&self) -> impl Iterator<Item = &str> {
        self.secondary_capacities
            .iter()
            .filter(|(_, record)| record.worker_count > 0)
            .map(|(id, _)| id.as_str())
            .filter(move |id| self.is_peer_alive(id))
    }

    /// The latest aggregated resource-sample record (#575) for each
    /// LIVE compute secondary — pairs the [`Self::alive_secondary_members`]
    /// roster with whatever
    /// [`crate::cluster_state::state::ClusterState::latest_resource_samples`]
    /// the LWW apply rule recorded for that id.
    ///
    /// Excludes secondaries that have not yet emitted a 5-minute aggregate
    /// (the `latest_resource_samples` lookup misses them); the observer's
    /// projection treats absent secondaries as "no signal yet" and folds
    /// only the present ones into its averages. Equally excludes any
    /// secondary whose membership is dead (the alive-secondary-members
    /// filter is what gates "compute member, currently up"), so a stale
    /// LWW record left by a removed incarnation never reaches the
    /// observer projection.
    ///
    /// Consumed ONLY by the observer's important-update reporter; the
    /// primary's scheduling/budget surface never reads it (resource
    /// stats are observability-only per #575).
    pub fn live_compute_resource_samples(
        &self,
    ) -> impl Iterator<Item = (&str, &SecondaryResourceSampleRecord)> {
        self.alive_secondary_members()
            .filter_map(move |id| self.latest_resource_samples.get(id).map(|r| (id, r)))
    }

    /// Count of [`Self::alive_secondary_members`] — the fleet-liveness
    /// quantity the primary's operational loop arms fleet-dead on. It
    /// answers "is there ANY alive worker-secondary this primary can
    /// dispatch to": a remote member receives dispatch over the wire, the
    /// primary's own co-located member (the same-peer worker-secondary of
    /// a promoted/compute-peer primary) receives it through the in-process
    /// loopback — both are dispatch capacity, so NEITHER is excluded.
    ///
    /// This deliberately carries NO `id != current_primary` cut. The cut
    /// once excluded the recognized primary's own same-peer secondary so a
    /// primary partitioned from every remote would strand-and-exit rather
    /// than run on its own host — a split-brain worry that is OWNED
    /// ELSEWHERE: a superseding `PrimaryChanged` fires the demote hook the
    /// moment it reaches this node, and a replicated `RunAborted` verdict
    /// stands the loop down at its top. What the cut actually did in
    /// production (run_20260612_035452) was read a lone-survivor fleet —
    /// whose ONLY live member was the acting primary's own host, the
    /// owner-supported self-quorum path — as permanently zero and abort a
    /// healthy run at the fleet-dead timeout while the co-located workers
    /// were mid-task. Death of the co-located member is still detected by
    /// the same machinery as everyone else's: the keepalive sweep's hard
    /// backstop is deliberately unfiltered, its removal flips the
    /// membership ledger to `Dead`, and this count then honestly reads
    /// zero.
    pub fn alive_worker_secondary_count(&self) -> usize {
        self.alive_secondary_members().count()
    }

    /// Total advertised worker-slot count across every secondary with a
    /// replicated capacity record. CRDT-derived occupancy DENOMINATOR
    /// for the worker-roster stats and the failover roster
    /// reconstruction — sum of every secondary's `worker_count`.
    pub fn total_worker_count(&self) -> u64 {
        self.secondary_capacities
            .values()
            .map(|c| u64::from(c.worker_count))
            .sum()
    }
}

/// The ONE `Failed { kind }` → outcome-bucket partition, shared by the
/// fat-state arm and the settled-index arm of [`ClusterState::
/// outcome_counts`] so the two cannot drift:
///   - `Recoverable`                 → `fail_retry`
///   - `ResourceExhausted("memory")` → `fail_oom`
///   - everything else (incl. the defensively-unreachable
///     `Unfulfillable`/`InvalidTask` kinds a legacy wire path could
///     land inside a `Failed`) → `fail_final`.
fn fold_failed_kind(kind: &ErrorType, o: &mut OutcomeSummary) {
    match kind {
        ErrorType::Recoverable => o.fail_retry += 1,
        ErrorType::ResourceExhausted(k) if k.as_str() == "memory" => o.fail_oom += 1,
        ErrorType::ResourceExhausted(_)
        | ErrorType::NonRecoverable
        | ErrorType::Unfulfillable { .. }
        | ErrorType::InvalidTask { .. } => o.fail_final += 1,
    }
}

/// A phase is dispatchable iff every phase it depends on (transitively)
/// has no live (non-terminal) task. Mirrors the pool's activation
/// cascade: a phase activates once its dependency phases are Done, and a
/// phase reaches Done once its tasks have all terminated.
///
/// `phase_has_live` is consulted as the per-phase "any live task"
/// predicate; a phase absent from the map (no entries) is vacuously
/// satisfied (`false`). The walk is depth-bounded by the dep graph,
/// which `PendingPool::new` already cycle-rejects, so it terminates.
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
