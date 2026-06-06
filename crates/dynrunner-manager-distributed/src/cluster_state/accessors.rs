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

use dynrunner_core::{ErrorType, Identifier, PhaseId, TaskInfo, TaskOutputs, WorkerId};
use dynrunner_protocol_primary_secondary::{RoleTable, SecondaryCapacityRecord};

use super::{ClusterState, OutcomeSummary, PhaseRollup, StateCounts, TaskState};

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
                    ErrorType::ResourceExhausted(k) if k.as_str() == "memory" => o.fail_oom += 1,
                    // Defensive: the apply rule for `TaskFailed` routes
                    // `Unfulfillable` straight to `TaskState::Unfulfillable`
                    // (the discrete state below), so this arm is unreachable
                    // in practice. Kept for exhaustiveness — if a legacy
                    // wire path ever lands a `Failed { Unfulfillable, .. }`
                    // entry the count still partitions correctly.
                    ErrorType::ResourceExhausted(_)
                    | ErrorType::NonRecoverable
                    | ErrorType::Unfulfillable { .. }
                    | ErrorType::InvalidTask { .. } => o.fail_final += 1,
                },
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
                TaskState::Pending { task, .. }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::InvalidTask { task, .. }
                | TaskState::Blocked { task, .. } => task,
            };
            (h, t)
        })
    }

    /// Iterator over `(task_hash, &TaskInfo)` for terminal entries
    /// (`Completed`, `Failed`, `Unfulfillable`, `InvalidTask`).
    /// `Blocked` is non-terminal (auto-resumes to `Pending` when its
    /// prereq completes) and is excluded.
    pub fn iter_terminal(&self) -> impl Iterator<Item = (&String, &TaskInfo<I>)> {
        self.tasks.iter().filter_map(|(h, s)| match s {
            TaskState::Completed { task }
            | TaskState::Failed { task, .. }
            | TaskState::Unfulfillable { task, .. }
            | TaskState::InvalidTask { task, .. } => Some((h, task)),
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
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::InvalidTask { task, .. }
                | TaskState::Blocked { task, .. } => task,
            };
            let entry = base.entry(&task.phase_id).or_insert((false, false));
            entry.0 = true;
            if !st.is_terminal() {
                entry.1 = true;
            }
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
            matches!(entry, super::types::CapabilityEntry::Departed).then_some(id.as_str())
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
        self.tasks.iter().find_map(|(h, s)| {
            let task = match s {
                TaskState::Pending { task, .. }
                | TaskState::InFlight { task, .. }
                | TaskState::Completed { task }
                | TaskState::Failed { task, .. }
                | TaskState::Unfulfillable { task, .. }
                | TaskState::InvalidTask { task, .. }
                | TaskState::Blocked { task, .. } => task,
            };
            (task.task_id == task_id && &task.phase_id == phase_id).then_some(h.as_str())
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

    /// Count of [`Self::alive_secondary_members`] that are NOT the
    /// recognized primary ([`Self::current_primary`]). The fleet-liveness
    /// quantity the primary's operational loop arms fleet-dead on: it
    /// answers "are there any alive worker-secondaries OTHER than the
    /// host I currently recognize as primary".
    ///
    /// The `id != current_primary` cut is the single thing that
    /// distinguishes this from the raw `alive_secondary_members` count,
    /// and it is exactly what excludes the recognized primary's OWN
    /// same-peer secondary capability. When the peer that is recognized as
    /// primary also advertises a worker-secondary under that same peer-id,
    /// its id IS `current_primary` AND appears in `alive_secondary_members`.
    /// The filter excludes that single same-peer entry by IDENTITY — never
    /// by a magic string and never by counting its own workers — so the
    /// count drops to zero exactly when every OTHER worker-secondary is
    /// gone, even though the recognized primary's own secondary is still
    /// alive. That is the fleet-dead arming condition (run cannot make
    /// progress) without the split-brain hazard of keeping a superseded
    /// primary alive on the strength of its own same-peer secondary.
    ///
    /// When the recognized primary advertises no worker-secondary under its
    /// peer-id, it is absent from `alive_secondary_members` and the filter
    /// is a no-op — the count is simply "all alive worker-secondaries". A
    /// `None` `current_primary` (pre-`PrimaryChanged`) makes the
    /// `Some(id) != None` filter universally true, so it is likewise a
    /// no-op.
    pub fn alive_remote_secondary_count(&self) -> usize {
        let current_primary = self.current_primary();
        self.alive_secondary_members()
            .filter(|id| Some(*id) != current_primary)
            .count()
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
