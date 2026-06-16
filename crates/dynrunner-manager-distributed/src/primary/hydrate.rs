//! Authoritative-primary pool rehydration from the replicated
//! `cluster_state` ledger.
//!
//! Single concern: turn the in-memory CRDT into the primary-local
//! derived caches a freshly-composed authoritative `PrimaryCoordinator`
//! needs to resume operational dispatch seeded from the cluster view
//! instead of empty state. `hydrate_from_cluster_state` rebuilds the
//! `PendingPool` (plus matching entries in the unified hash-keyed
//! `in_flight` ledger and the `completed_tasks` set), then
//! `reconstruct_workers_from_cluster_state` rebuilds the remote-worker
//! roster (`self.workers`) from the replicated per-secondary capacity ×
//! `TaskState::InFlight` occupancy. All of these are pure derived caches
//! of the replicated ledger.
//!
//! Faithful port of the now-removed secondary-side
//! `populate_primary_from_cluster_state` (lived in the deleted
//! `secondary/primary/` authority mirror); this is its single surviving
//! home. Unlike that port, it does NOT silently drain empty phases: the
//! primary's COORDINATOR owns the narrated lifecycle cascade
//! (`fire_initial_phase_starts` + `drain_empty_active_phases` +
//! `process_phase_lifecycle`), so empty-phase draining is the caller's
//! concern (see the note at the pool build below). One deviation: the
//! `PrimaryCoordinator` owns no local worker pool (workers are remote
//! `RemoteWorkerState` entries; there is no `active_tasks` set), so
//! the source's "Pending-in-cluster-state but locally-active" arm has
//! no analog here. A `Pending` / `Blocked` entry always becomes a
//! pool item; the loopback secondary's in-flight work is owned through
//! the `InFlight` arm as remote-in-flight, never double-counted as
//! local-active.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::{ErrorType, Identifier, PhaseId, TaskInfo};
use dynrunner_scheduler_api::{PendingPool, PendingPoolError, ResourceEstimator, Scheduler};

use crate::cluster_state::TaskState;
use crate::primary::PrimaryCoordinator;
use crate::state::{SecondaryConnection, SecondaryConnectionState};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Build a fresh `PendingPool` for the authoritative primary view
    /// from the replicated `cluster_state` ledger.
    ///
    /// One concern: turn the in-memory CRDT ledger into a fresh
    /// `PendingPool` for post-composition dispatch. The lattice
    /// (Pending / InFlight / Completed / Failed / Unfulfillable /
    /// Blocked) is iterated once; only `Pending` / `Blocked`
    /// entries enter the pool, terminal entries contribute their
    /// `task_id` to the PER-CLASS dep seed (completed-satisfying,
    /// retry-pending, or dormant — see the seed-vec docs in the body),
    /// and `InFlight` entries are
    /// recorded in the unified `in_flight` ledger with no holding slot
    /// (the originating dispatcher owns the work; this coordinator
    /// picks up completion only via the broadcast path).
    ///
    /// The pool is rebuilt on every call: the cluster ledger is the
    /// authoritative source, and a partial patch would risk
    /// double-counting in-flight items this coordinator can't observe
    /// from outside.
    ///
    /// Why we seed terminal task_ids: variants in the `Pending` set may
    /// declare `task_depends_on` against a toolchain task_id whose task
    /// is no longer pending (already terminal). Without seeding the new
    /// pool with those task_ids, `extend()`'s validation rejects every
    /// variant whose toolchain finished pre-composition as
    /// `UnknownTaskDep`. WHICH seed a terminal feeds is per-class —
    /// only a `Completed`/`SkippedAlreadyDone` prereq SATISFIES its
    /// dependents' deps (`mark_tasks_completed`); a `Failed`/
    /// `InvalidTask` prereq seeds the retry-pending marker
    /// (`mark_tasks_failed_pending_retry`) and an `Unfulfillable` one
    /// the dormant set (`mark_tasks_dormant`), both of which hold
    /// dependents BLOCKED — see the seed-vec docs in the body.
    /// Exercised directly by the hydrate tests and by the production
    /// snapshot-seeded construction caller (`seed_from_promotion_snapshot`).
    ///
    /// # Composition validity is the caller's run-policy
    ///
    /// The pool build is a graph VALIDATION (`PendingPool::new` checks the
    /// phase graph; `extend` checks per-task `task_id` uniqueness +
    /// dependency existence + cycle-freedom). A validation failure means the
    /// REPLICATED ledger this pool is derived from describes an impossible
    /// task graph — a duplicate `(phase_id, task_id)` identity (the
    /// asm-dataset LMU run_~1429 production fatal: a discovered batch carried
    /// a dup task_id, which `compute_task_hash` cannot collapse because it
    /// folds `(phase_id, path, identifier)` not `task_id`, so both land as
    /// distinct CRDT entries and only `extend`'s task_id check surfaces it), a
    /// missing dep, or a cycle. This is NOT a derived-cache concern: hydrate
    /// SURFACES it as `Err` and the run-phase caller (`discover_on_promotion`
    /// / `run_pipeline`) owns the run-fatal policy (latch + broadcast the
    /// `RunAborted` verdict, return a typed `RunError`) — the SAME terminal-
    /// verdict path `fire_pending_run_abort` (#3a) and `invalidate_all_pending`
    /// (#3b) use. Pre-fix this site logged an ERROR and silently left
    /// `pending = None` ("primary will start with no pending tasks"): the run
    /// then sat with an empty pool, never aborted, and the fleet died one-by-
    /// one on their unconfigured deadlines. On `Err` the derived caches built
    /// so far (`completed_tasks`, `all_binaries`, etc.) are LEFT as-is and
    /// `pending` is set `None`; the caller aborts the run, so no consumer ever
    /// reads them.
    pub(crate) fn hydrate_from_cluster_state(&mut self) -> Result<(), PendingPoolError> {
        let mut completed_task_ids: HashSet<String> = HashSet::new();
        let mut primary_completed: HashSet<String> = HashSet::new();
        let mut items: Vec<TaskInfo<I>> = Vec::new();
        let mut in_flight_pairs: Vec<(String, PhaseId)> = Vec::new();
        let mut in_flight_seed: Vec<(String, PhaseId, String, u32, TaskInfo<I>)> = Vec::new();
        // The run-start task universe — the OOM/retry candidate source
        // (`retry_bucket` filters `all_binaries` × `failed_tasks`). EVERY
        // ledger entry carries its `TaskInfo` regardless of state, so the
        // universe is the `task()` of every entry: a pure derived cache of
        // the CRDT, rebuilt on hydrate exactly like the pool.
        let mut all_binaries: Vec<TaskInfo<I>> = Vec::new();
        // The hash-keyed `ErrorType` ledger the retry bucket cross-references
        // (`all_binaries` × `failed_tasks`). Rebuilt from every `Failed`
        // CRDT entry so the retry subsystem is NOT inert on a promoted
        // primary — without this, inherited `Failed` tasks with remaining
        // retry budget are never reinjected (the candidate filter finds an
        // empty `failed_tasks`). The CRDT `Failed { kind }` carries the SAME
        // `ErrorType` the live path inserted (`task::failed.rs` /
        // `command_channel::handler.rs` store `error_type` into both
        // `failed_tasks` and the `TaskFailed { kind }` mutation), so this is
        // a faithful projection. `Unfulfillable` / `InvalidTask` never reach
        // this map: the `TaskFailed` apply arm demultiplexes those
        // `ErrorType`s into their DISCRETE `TaskState` variants (not
        // `Failed`), so a `Failed` state's `kind` is only ever
        // `Recoverable` / `NonRecoverable` / `ResourceExhausted` — exactly
        // the live-path `failed_tasks` contents. `NonRecoverable` matches no
        // `BucketKind`, so it stays inert (never re-retried), mirroring the
        // live path. The replicated `retry_passes_used` budget gates
        // re-retry, so seeding every recoverable `Failed` is safe.
        let mut failed_tasks: HashMap<String, ErrorType> = HashMap::new();
        // Pool-side terminal-FAILURE dep seeds — the per-class dependency
        // semantics a FAILED terminal carries (mirroring the
        // `apply_tasks_spawned` classification a freshly-spawned dependent
        // gets, so a pre-existing dependent converges to the same state):
        //   * `soft_failed_seed` — every `Failed { .. }` root (any kind)
        //     plus every `InvalidTask` root: the root's task_id becomes
        //     KNOWN to `extend` (dependents land `blocked`, never
        //     `UnknownTaskDep`, never DISPATCHABLE) and the phase's
        //     drain-edge machinery owns the outcome exactly as on the live
        //     path — the retry buckets revive a retryable root with budget
        //     (`reinject` clears the marker, dependents stay blocked) or
        //     decline (`finalize_soft_failures` cascade-fails the
        //     dependents with the canonical broadcast `upstream-failed`
        //     terminal). A root the original primary already finalized
        //     replays to the same decision: the replicated
        //     `retry_passes_used` budget is inherited, so the bucket
        //     declines again and the finalize cascade re-derives the doom
        //     (accounted; its already-terminal dependents are not in the
        //     pool, so nothing double-fires). `InvalidTask` rides the same
        //     seed because a structurally-dead root dooms its dependents
        //     like a NonRecoverable one (the spawn-side cascade-fail
        //     classification); it never enters the coordinator
        //     `failed_tasks` kind ledger, so no bucket ever revives it.
        //   * `dormant_seed` — every `Unfulfillable` root: KNOWN to
        //     `extend` (dependents land `blocked`) with NO doom marker —
        //     the operator-reinjectable dormancy contract
        //     (`note_item_failed`'s Unfulfillable arm): dependents are
        //     LIVE blocked work holding the run open until the reinject
        //     command / fulfillability matcher revives the root.
        let mut soft_failed_seed: Vec<(String, PhaseId)> = Vec::new();
        let mut dormant_seed: Vec<String> = Vec::new();
        // V3: the set of phases that have fired `on_phase_start`, derived
        // from the CRDT on BOTH the cold and promote paths (replacing the
        // old split: `originate_cold_seed`'s `.clear()` + the promote-only
        // `seed_from_promotion_snapshot` `has_any` projection). A phase is
        // "started" iff it holds ≥1 task that has PROGRESSED past
        // `Pending`/`Blocked` — i.e. ≥1 `InFlight` or DISPATCH-OBSERVED
        // terminal (`Completed`/`Failed`/`Unfulfillable`/`InvalidTask`)
        // entry. `SkippedAlreadyDone` deliberately does NOT count (#343):
        // a skip is a SPAWN-TIME terminal that proves seeding, not
        // activation — see its match arm below. This is
        // `has_any && (in_flight || dispatch-observed-terminal-present)`:
        //   * COLD: a freshly-seeded CRDT is all-`Pending` ⇒ the set is EMPTY
        //     (the equivalent of the old `.clear()`), so the subsequent
        //     `fire_initial_phase_starts` legitimately fires each phase's
        //     first `on_phase_start`.
        //   * RESUME (promotion): a phase that started pre-failover dispatched
        //     work, so it holds an `InFlight`/terminal entry ⇒ it is seeded,
        //     and `fire_initial_phase_starts` does NOT re-fire it.
        //   * BLOCKED-ONLY phase: a phase whose every task is `Blocked`
        //     (waiting on an unfinished prereq) was never `Active` ⇒ never
        //     fired `on_phase_start` ⇒ correctly NOT seeded (the old
        //     promote-side `has_any` over-suppressed it; this is the V3
        //     correction). When its prereq completes it activates and fires
        //     its legitimate first `on_phase_start`.
        let mut started_phases: HashSet<PhaseId> = HashSet::new();
        // Phase-completion derivation (failover-promotion resume). A phase
        // was already COMPLETED-AND-ENDED on the run's original primary iff it
        // holds ≥1 terminal task AND zero live (`Pending`/`Blocked`/`InFlight`)
        // tasks — exactly the `(queued=0, in_flight=0, blocked=0)` condition
        // `maybe_transition_drain` recognises as `Drained`, observed here at
        // hydration time. The original primary already fired its `on_phase_end`
        // (and any consumer hook that spawned its children — those children are
        // present in this same inherited CRDT). Seeding such phases straight to
        // `PhaseState::Done` AFTER `extend` (below) keeps them out of the
        // post-hydrate lifecycle cascade's `poll_drain_transitions`, so
        // `on_phase_end` does NOT re-fire and the consumer hook does NOT
        // re-spawn its (identical-identity) children. Derived purely from task
        // states (no explicit phase-done CRDT marker needed — the lattice
        // already distinguishes terminal from live, which is exactly the
        // `(0,0,0)` signal). On a COLD seed every task is `Pending`, so
        // `phases_with_terminal` is empty and the seed set is empty: the
        // cold-path `fire_initial_phase_starts` + empty-phase cascade run
        // unchanged. A phase with a MIX of terminal + live tasks is NOT seeded
        // (it has live work → its live tasks dispatch and its real
        // `on_phase_end` fires once that work drains, exactly as the original
        // primary would have).
        let mut phases_with_terminal: HashSet<PhaseId> = HashSet::new();
        let mut phases_with_live_work: HashSet<PhaseId> = HashSet::new();

        for (hash, state) in self.cluster_state.tasks_iter() {
            // L5: resolve dep refs via the store (two shared borrows of
            // `self.cluster_state` — the iter + the resolve — is fine).
            all_binaries.push(self.cluster_state.task_to_info(state));
            let def = state.def();
            match state {
                // A FAILED terminal must NOT satisfy dependents' deps: it
                // never produced its outputs. Its task_id seeds the pool's
                // retry-pending (`soft_failed`) marker — see the
                // `soft_failed_seed` doc above for the full per-class
                // routing — so dependents hydrate BLOCKED (never
                // dispatchable) and the drain-edge buckets/finalize own
                // the revive-or-cascade decision exactly as the live
                // primary would have. `Failed` additionally re-seeds the
                // hash-keyed `failed_tasks` ledger from the carried `kind`,
                // so the retry bucket has a candidate source post-promotion.
                // The CRDT entry itself stays `Failed`; the bucket's reset
                // (`TaskRetried`, budget-gated) is what later moves it to
                // `Pending` — hydrate only rebuilds the projection.
                //
                // It does NOT seed `primary_completed` (the hash-keyed
                // `completed_tasks` set): on the live path that set and
                // `failed_tasks` are STRICTLY DISJOINT — a terminal hash
                // sits in exactly one of {completed, failed} (see
                // `task::complete.rs`). The run-complete counter sums both
                // (`completed_tasks.len() + failed_tasks.len()`), so seeding
                // an inherited `Failed` into both would count it TWICE and
                // trip a premature false-complete. The late-TaskFailed
                // dedup in `task::failed.rs` ORs
                // `failed_tasks.contains_key`, so it still dedupes without
                // the `completed_tasks` membership.
                TaskState::Failed { kind, .. } => {
                    started_phases.insert(def.phase_id.clone());
                    phases_with_terminal.insert(def.phase_id.clone());
                    soft_failed_seed.push((def.task_id.clone(), def.phase_id.clone()));
                    failed_tasks.insert(hash.clone(), kind.clone());
                }
                // The ONLY dispatch-observed terminal that satisfies
                // dependents' deps: a completed prereq produced its
                // outputs, so its task_id enters the dep-resolution
                // completed seed and dependents pre-resolve in `extend()`.
                TaskState::Completed { .. } => {
                    started_phases.insert(def.phase_id.clone());
                    phases_with_terminal.insert(def.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(def.task_id.clone());
                }
                // Operator-reinjectable dormancy: the entry stays
                // `Unfulfillable` in the CRDT (revivable via the command
                // channel / fulfillability matcher) and its task_id seeds
                // the pool's dormant-known set, so dependents hydrate
                // BLOCKED — live work that holds the run open — instead of
                // dispatching against outputs that were never produced.
                // The hash still enters `primary_completed` purely for the
                // run-completion counter (the historical accounting slot
                // for the non-`Failed`-state terminals; the entry is in
                // neither bucket-kind ledger, so no retry path touches it).
                TaskState::Unfulfillable { .. } => {
                    started_phases.insert(def.phase_id.clone());
                    phases_with_terminal.insert(def.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    dormant_seed.push(def.task_id.clone());
                }
                // Structurally-dead root (non-reinjectable): dependents can
                // NEVER run, the same doom a `Failed { NonRecoverable }`
                // root carries (the `apply_tasks_spawned` cascade-fail
                // classification). Seeded soft-failed so the drain edge
                // finalizes it — no bucket ever revives it (its hash is
                // not in the kind ledger) — and the finalize cascade dooms
                // the dependents with the accounted, broadcast
                // `upstream-failed` terminal. Hash into `primary_completed`
                // for the run-completion counter, as before.
                TaskState::InvalidTask { .. } => {
                    started_phases.insert(def.phase_id.clone());
                    phases_with_terminal.insert(def.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    soft_failed_seed.push((def.task_id.clone(), def.phase_id.clone()));
                }
                // `SkippedAlreadyDone` is a terminal that was NEVER
                // dispatched, so it is seeded like the other terminal-ish
                // states (its task_id resolves its dependents'
                // `task_depends_on` in `extend()`, the phase carries a
                // terminal, and the hash enters `completed_tasks`) but is
                // CRITICALLY NOT pushed into `items`: re-dispatching an
                // already-done item on failover would re-run work whose
                // outputs already exist (the NO-REDO failover-routing
                // decision). The CRDT entry itself stays
                // `SkippedAlreadyDone`.
                //
                // It does NOT mark the phase STARTED (#343): a skip is a
                // SPAWN-TIME terminal — it proves the phase was seeded, not
                // that it ever activated and fired `on_phase_start`. A
                // freshly-discovered all-skipped phase has fired NOTHING
                // anywhere, and must flow through the live cascade
                // (`on_phase_start` → drain → `on_phase_end`) like any
                // other never-started phase. An INHERITED all-skipped phase
                // whose edges DID fire on the original primary carries the
                // replicated `PhaseEnded` fact and is seeded straight to
                // `Done` below — a `Done` phase is never `Active`, so
                // `fire_initial_phase_starts` (which iterates active phases
                // only) never consults the started set for it and neither
                // hook re-fires. A mixed phase (skips + dispatched work) is
                // marked started by its non-skip entries, exactly as the
                // original primary observed it.
                TaskState::SkippedAlreadyDone { .. } => {
                    phases_with_terminal.insert(def.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(def.task_id.clone());
                }
                // A succeeded setup task: a success-like terminal that
                // satisfies dependents' deps (its task_id enters the
                // dep-resolution seed, so build tasks gated on it pre-resolve
                // in `extend()`) and is NOT re-dispatched on failover (never
                // pushed into `items` — re-running it would re-execute the
                // setup work whose effect is already done). Unlike a
                // spawn-time skip it WAS executed in-process, so it marks the
                // phase STARTED (it is genuine evidence the phase activated),
                // matching the `Completed` arm. The CRDT entry stays
                // `SetupCompleted`. It enters `primary_completed` for the
                // run-completion counter slot, like the other non-`Failed`
                // terminals.
                TaskState::SetupCompleted { .. } => {
                    started_phases.insert(def.phase_id.clone());
                    phases_with_terminal.insert(def.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(def.task_id.clone());
                }
                // Cascade-paused dependent. Re-seed as Pending into the
                // new primary's pool: the prereq's TaskCompleted apply
                // arm has already (or will shortly) auto-resume the
                // CRDT entry to Pending across every replica, and the
                // pool needs the binary present to dispatch on the
                // next tick. If the prereq is still Unfulfillable when
                // this coordinator composes, the pool's dep-validation
                // will surface the unresolved dep as a normal blocked
                // state — same dormancy, owned by the pool's existing
                // dep machine rather than a parallel "Blocked" set.
                TaskState::Blocked { .. } => {
                    phases_with_live_work.insert(def.phase_id.clone());
                    // L5: resolve dep refs via the store.
                    items.push(self.cluster_state.task_to_info(state));
                }
                // Unlike the secondary's hydration, the
                // `PrimaryCoordinator` owns no local `active_tasks`
                // set — its workers are remote `RemoteWorkerState`
                // entries and any work it itself dispatched is tracked
                // as `InFlight` in cluster_state. A `Pending` entry is
                // therefore always genuinely pending: into the pool.
                TaskState::Pending { .. } => {
                    phases_with_live_work.insert(def.phase_id.clone());
                    // L5: resolve dep refs via the store.
                    items.push(self.cluster_state.task_to_info(state));
                }
                TaskState::InFlight {
                    secondary, worker, ..
                } => {
                    // The originating dispatcher dispatched the work; this
                    // coordinator inherits it on promotion and will observe
                    // completion via the broadcast path (peer's TaskComplete
                    // on success / TaskFailed on terminal failure). To make
                    // that observation affect the pool + roster correctly we
                    // need three things:
                    //   1. Seed the task_id into `in_flight_tasks` so
                    //      `extend()`'s dep validation accepts Pending
                    //      variants whose `task_depends_on` references
                    //      an in-flight task. Without this every such
                    //      dependent fails `UnknownTaskDep` and the new
                    //      primary degrades to "no pending tasks".
                    //   2. Bump `in_flight_per_phase` for the in-flight
                    //      task's phase so phase-lifecycle drains
                    //      correctly when completion arrives — the
                    //      counter must drop from N+1 to N, not from
                    //      0 to 0.
                    //   3. Insert into the unified `in_flight` ledger keyed
                    //      by file_hash with `local_worker_id = Some(worker)`
                    //      (the SAME secondary-local id `commit_assignment`
                    //      writes on the live path, replicated into
                    //      `TaskState::InFlight { worker }` by D2). The
                    //      matching `RemoteWorkerState` slot is reconstructed
                    //      `Assigned` by `reconstruct_workers_from_cluster_state`
                    //      below, so when the broadcast TaskComplete lands in
                    //      `handle_task_complete`, `free_slot_on_terminal`
                    //      resolves the stable `(secondary, worker)` holder to
                    //      that slot, frees it, yields the (phase_id,
                    //      secondary, task), and forwards to
                    //      `note_item_completed`.
                    // (1) and (2) are owned by the pool via
                    // `mark_tasks_in_flight` below; (3) is the ledger
                    // seed performed after `extend` succeeds.
                    started_phases.insert(def.phase_id.clone());
                    phases_with_live_work.insert(def.phase_id.clone());
                    in_flight_pairs.push((def.task_id.clone(), def.phase_id.clone()));
                    in_flight_seed.push((
                        hash.clone(),
                        def.phase_id.clone(),
                        secondary.clone(),
                        *worker,
                        // L5: resolve dep refs via the store.
                        self.cluster_state.task_to_info(state),
                    ));
                }
            }
        }

        // SETTLED (spilled) entries: the same per-class seeds as the fat
        // terminal arms above, served off the slim index — the promoted
        // node's local file+index IS its settled base, consumed here
        // WITHOUT replaying fat bodies through memory. Per class,
        // faithfully mirroring the fat arms:
        //   * Completed → started + terminal + completed mirror + dep seed;
        //   * FailedFinal → started + terminal + soft-failed seed + the
        //     hash-keyed kind ledger (NOT the completed mirror — the two
        //     sets are strictly disjoint, see the Failed arm above);
        //   * InvalidTask → started + terminal + completed mirror +
        //     soft-failed seed;
        //   * SkippedAlreadyDone → terminal + completed mirror + dep seed,
        //     and NOT started (#343 — a skip proves seeding, not
        //     activation).
        // `all_binaries` (the retry/OOM candidate universe) deliberately
        // gains NO settled entries: the retry buckets only target
        // Recoverable/OOM kinds, which never settle, so no settled
        // TaskInfo can ever be a candidate.
        for (hash, entry) in self.cluster_state.settled_entries() {
            use crate::cluster_state::SettledClass;
            match &entry.class {
                SettledClass::Completed => {
                    started_phases.insert(entry.phase_id.clone());
                    phases_with_terminal.insert(entry.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(entry.task_id.clone());
                }
                SettledClass::FailedFinal(kind) => {
                    started_phases.insert(entry.phase_id.clone());
                    phases_with_terminal.insert(entry.phase_id.clone());
                    soft_failed_seed.push((entry.task_id.clone(), entry.phase_id.clone()));
                    failed_tasks.insert(hash.clone(), kind.clone());
                }
                SettledClass::InvalidTask => {
                    started_phases.insert(entry.phase_id.clone());
                    phases_with_terminal.insert(entry.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    soft_failed_seed.push((entry.task_id.clone(), entry.phase_id.clone()));
                }
                SettledClass::SkippedAlreadyDone => {
                    phases_with_terminal.insert(entry.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(entry.task_id.clone());
                }
                // SetupCompleted → started + terminal + completed mirror +
                // dep seed, mirroring the fat `SetupCompleted` arm above (a
                // succeeded setup task was executed in-process, so it marks
                // the phase started, satisfies dependents, and is never
                // re-dispatched).
                SettledClass::SetupCompleted => {
                    started_phases.insert(entry.phase_id.clone());
                    phases_with_terminal.insert(entry.phase_id.clone());
                    primary_completed.insert(hash.clone());
                    completed_task_ids.insert(entry.task_id.clone());
                }
            }
        }

        self.completed_tasks = primary_completed;
        // V3: seed `phase_started_emitted` from the CRDT-derived started set
        // (a phase with ≥1 progressed task). On the cold path this is empty
        // (all-`Pending`), so `fire_initial_phase_starts` fires every phase's
        // first `on_phase_start`; on the promote path the inherited started
        // phases are seeded so they do NOT re-fire. The SOLE seeder of this
        // set at construction time (the live `fire_initial_phase_starts`
        // `insert` guard is the only OTHER writer, and it runs after hydrate).
        self.phase_started_emitted = started_phases;
        // Rebuild the OOM/retry candidate universe from the CRDT so a
        // promoted primary's retry bucket has a candidate source (it was
        // empty on the seeded path before this). Pure derived cache.
        self.all_binaries = all_binaries;
        // Rebuild the hash-keyed failure ledger the retry bucket
        // cross-references against `all_binaries`. Without this a promoted
        // primary's retry subsystem is inert (empty candidate set) even
        // though `all_binaries` and the budget are present. Pure derived
        // cache of the CRDT `Failed` entries.
        self.failed_tasks = failed_tasks;
        // `single_worker_mode` is an ephemeral within-bucket dispatch-shape
        // flag, NOT a failover decision input: a freshly-promoted primary
        // starts unmasked and the next OOM-bucket entry re-arms it. Reset
        // to the cold value so a stale `true` never carries across hydrate.
        self.single_worker_mode = false;
        items.sort_by_key(|i| std::cmp::Reverse(i.size));

        let phase_deps = self.cluster_state.phase_deps().clone();

        // Phase set = union of (declared phases via deps map),
        // (phases observed in the items), and (phases of in-flight
        // tasks). The third source matters when a phase has had every
        // item dispatched pre-composition: the items list is empty for
        // that phase, but `mark_tasks_in_flight` will bump its
        // counter and the phase must exist in `phase_state` for
        // drain transitions to fire.
        let mut phase_ids: HashSet<PhaseId> = items.iter().map(|i| i.phase_id.clone()).collect();
        for (_, phase_id) in &in_flight_pairs {
            phase_ids.insert(phase_id.clone());
        }
        for (_, phase_id, _, _, _) in &in_flight_seed {
            phase_ids.insert(phase_id.clone());
        }
        for (child, parents) in &phase_deps {
            phase_ids.insert(child.clone());
            for p in parents {
                phase_ids.insert(p.clone());
            }
        }

        let pool = match PendingPool::new(phase_ids, phase_deps) {
            Ok(mut p) => {
                // Apply the consumer's `PhaseSpec.barrier=False` opt-in
                // BEFORE any task seeding so the no-barrier phases start
                // `Active` rather than `Blocked`. The set comes from the
                // replicated `cluster_state.phase_no_barrier` (originated
                // by the seed batch's `PhaseNoBarrierSet` — paired with
                // `PhaseDepsSet`), so a hydrated/promoted primary uses
                // the SAME initial state the live primary used. Empty
                // (no-op) on the common strict-barrier run.
                p.set_no_barrier_phases(
                    self.cluster_state.phase_no_barrier_set().iter().cloned(),
                );
                p.mark_tasks_completed(completed_task_ids);
                // Per-class terminal-FAILURE seeds (see the seed-vec docs
                // at the top): Failed/InvalidTask roots enter the
                // retry-pending marker, Unfulfillable roots the
                // dormant-known set. Both make the root's task_id KNOWN to
                // `extend` below — dependents land BLOCKED — without
                // satisfying the dep (the `mark_tasks_completed` contract,
                // which is reserved for terminals that produced outputs).
                p.mark_tasks_failed_pending_retry(soft_failed_seed);
                p.mark_tasks_dormant(dormant_seed);
                p.mark_tasks_in_flight(in_flight_pairs);
                if let Err(e) = p.extend(items) {
                    tracing::error!(
                        error = %e,
                        "post-composition: invalid task graph in cluster_state \
                         (duplicate task_id / unknown dep / cycle); surfacing as \
                         a run-fatal — the caller aborts the run via the terminal \
                         verdict path"
                    );
                    self.pending = None;
                    return Err(e);
                }
                // Seed already-completed-AND-ENDED phases straight to `Done`
                // (failover-promotion resume). The decision is keyed on the
                // REPLICATED `PhaseEnded` fact (#343), not inferred from the
                // task lattice alone: a phase is seeded `Done` iff it holds
                // ≥1 terminal task, NO live (`Pending`/`Blocked`/`InFlight`)
                // work, AND the CRDT says its `on_phase_end` edge already
                // COMPLETED on some primary (the cascade originates the fact
                // at the same edge it calls `mark_phase_done`, so any
                // children the hook spawned are present in this same CRDT).
                // Without the seed the freshly built pool would start such a
                // phase `Active`/`Blocked` and the post-hydrate cascade
                // would re-`(0,0,0)`-drain it → re-fire `on_phase_end` → the
                // consumer hook re-spawns its identical-identity children →
                // run-wide invalidation (#326). Marking it `Done` removes it
                // from `poll_drain_transitions` entirely.
                //
                // The fact gate is what distinguishes that INHERITED shape
                // from a freshly-discovered all-`SkippedAlreadyDone` phase
                // (#343): the skip seed makes a phase terminal-only the
                // moment it lands, BEFORE its hook ever ran ANYWHERE — the
                // task lattice alone cannot tell the two apart. No fact ⇒
                // the phase flows through the live cascade, fires its FIRST
                // `on_phase_end`, and the consumer's lazy-spawn injection
                // lands. (The residual die-between-hook-and-fact-broadcast
                // window re-fires the hook on the next primary; the
                // deterministic re-spawn is absorbed by the idempotent
                // failover-replay dedup in `apply_spawn_tasks` — fail-safe,
                // unlike suppressing a never-fired hook, which loses the
                // injection unrecoverably.)
                //
                // Runs AFTER `extend` (a failure leaves `pending = None` and
                // any seeded phase state would be stranded) and the
                // activation convergence inside `seed_completed_phases`
                // flips a live dependent phase `Blocked → Active` if all its
                // deps are now `Done`. EMPTY on the cold path (no
                // `PhaseEnded` fact exists before the first end edge), so
                // the cold-start `fire_initial_phase_starts` + empty-phase
                // cascade are untouched.
                let completed_phases: Vec<PhaseId> = phases_with_terminal
                    .iter()
                    .filter(|ph| !phases_with_live_work.contains(*ph))
                    .filter(|ph| self.cluster_state.phase_ended(ph))
                    .cloned()
                    .collect();
                if !completed_phases.is_empty() {
                    tracing::info!(
                        completed_phases = completed_phases.len(),
                        "seeding already-completed phases as Done on resume \
                         (failover-promotion); their on_phase_end will NOT re-fire"
                    );
                    p.seed_completed_phases(completed_phases);
                }
                // NB: empty-phase draining is NOT done here. The primary's
                // COORDINATOR owns the narrated lifecycle cascade
                // (`fire_initial_phase_starts` + `drain_empty_active_phases` +
                // `process_phase_lifecycle`), which drains trivially-empty
                // initial phases WHILE firing their `on_phase_start` /
                // `on_phase_end(.., 0, 0)` callbacks. A silent
                // `cascade_drain_done` here (the secondary-hydration port,
                // where no such callbacks exist) would mark an empty initial
                // phase `Done` BEFORE the coordinator could narrate it,
                // suppressing the cold-path empty-phase callbacks. Both the
                // run-entry path (`run_pipeline`, pre-loop cascade) and the
                // mid-run rehydrate (`task::mutation`'s discovery-rebuild,
                // which cascades explicitly) own the drain at their level.
                p
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "post-composition: invalid phase graph in cluster_state \
                     (cycle / unknown phase dep); surfacing as a run-fatal — the \
                     caller aborts the run via the terminal verdict path"
                );
                self.pending = None;
                return Err(e);
            }
        };

        // Reconstruct the remote-worker roster from the two replicated
        // sources — D1's per-secondary capacity (`worker_count` +
        // resources across `known_secondaries()`) crossed with the live
        // `TaskState::InFlight { secondary, worker }` occupancy — so a
        // promoted primary holds the FULL roster (idle + occupied slots)
        // and `alive_worker_count() > 0`. Without this the roster is
        // empty on promotion and the first `TaskRequest` resolves no slot.
        // Run before the ledger seed so each inherited `in_flight` entry's
        // stable `(secondary, worker)` holder key resolves onto a slot
        // this pass has already moved `Idle -> Assigned`.
        self.reconstruct_workers_from_cluster_state();

        // Reconstruct the secondary roster (`self.secondaries` +
        // `self.secondary_keepalives`) from the same CRDT source. The
        // on-demand promotion path bypasses connect.rs / peer_setup.rs (the
        // only writers of `self.secondaries`), so without this a promoted
        // primary's roster is empty: `record_keepalive` no-ops, and
        // `collect_heartbeat_report` can mark NO secondary dead — a
        // secondary dying AFTER promotion strands its inherited in-flight
        // tasks forever. (The keepalive EMITTER is roster-independent —
        // `broadcast_primary_keepalive` fans to the mesh members — so only
        // the death-clock bookkeeping depends on this rebuild.) Same
        // "derived cache of the CRDT" treatment `self.workers` gets above.
        self.reconstruct_secondaries_from_cluster_state();

        // Seed the unified `in_flight` ledger only after `extend`
        // succeeded — a failure on the items batch leaves
        // `pending = None` and any ledger entry we'd populated would be
        // stranded. Each inherited task is seeded with `local_worker_id =
        // Some(worker)` — the same secondary-local id `commit_assignment`
        // records on the live path, replicated by D2 into
        // `TaskState::InFlight { worker }` — so when its broadcast
        // TaskComplete / TaskFailed lands, `free_slot_on_terminal`
        // resolves the stable `(secondary, worker)` holder onto the
        // reconstructed `Assigned` slot, frees it, and runs the correct
        // phase's `note_item_*`. This folds in the deleted
        // `pre_owned_in_flight` ledger — there is now ONE ledger,
        // populated identically at dispatch and hydration.
        for (hash, phase_id, secondary, worker, binary) in in_flight_seed {
            self.seed_inflight(hash, phase_id, secondary, worker, std::sync::Arc::new(binary));
        }

        // Single source of truth for the run-completion accounting:
        // the cluster ledger's task count (`tasks.len()`), identical
        // to the reactive `handle_cluster_mutation` `TaskAdded` refresh.
        self.total_tasks = self.cluster_state.task_count();

        let pending_count = pool.len();
        let in_flight_count = self.in_flight.len();
        self.pending = Some(pool);

        tracing::info!(
            pending = pending_count,
            in_flight = in_flight_count,
            succeeded = self.completed_tasks.len(),
            total = self.total_tasks,
            "hydrated primary task list from cluster_state"
        );
        Ok(())
    }

    /// Reconstruct the remote-worker roster (`self.workers`) from the two
    /// replicated CRDT sources, so a freshly-promoted primary holds the
    /// FULL roster (idle + occupied slots) and can dispatch.
    ///
    /// One concern: cross D1's per-secondary capacity (the roster —
    /// `secondary_capacity(id).worker_count` + advertised resources
    /// across `known_secondaries()`) with D2's live `TaskState::InFlight
    /// { secondary, worker }` occupancy, mirroring how
    /// `hydrate_from_cluster_state` rebuilds the pool from one replicated
    /// source. Today `self.workers` is built ONLY at initial assignment
    /// from `self.secondaries`; `hydrate` / `activate_local_primary`
    /// never rebuilt it, so a promoted primary started
    /// `alive_worker_count() == 0` and a `TaskRequest` resolved no slot.
    /// This makes `self.workers` a pure DERIVED CACHE of the replicated
    /// state on the failover path too.
    ///
    /// The roster build faithfully mirrors `perform_initial_assignment`'s
    /// loop (the live primary's roster shape): round-robin across
    /// NAME-SORTED secondaries, one global `worker_id` monotonic counter,
    /// `resource_budgets = initial_budget(round, &max_res)` with `round`
    /// the secondary-local worker index and `max_res` the memory amount
    /// extracted from the advertised resources. Producing the identical
    /// shape is load-bearing: `worker_idx_for` / `local_worker_id_in_secondary`
    /// resolve a stable `(secondary, local_id)` against the contiguous
    /// per-secondary ordering, and `view_for_worker(global_wid, ..)`
    /// consumes the global id — so a reconstructed roster must match what
    /// a live primary would have built for the occupancy crossing and
    /// subsequent dispatch to be correct.
    ///
    /// Occupancy crossing: after the all-idle roster is built, every
    /// `TaskState::InFlight { secondary, worker, task }` moves its
    /// matching slot `Idle -> Assigned`, keyed by the CRDT hash
    /// (`compute_task_hash`-equivalent ledger key) so a later inbound
    /// terminal frees it through `free_slot_on_terminal`'s stable-id
    /// resolution. An InFlight entry whose `(secondary, worker)` resolves
    /// no slot (capacity record missing, or worker id past the advertised
    /// count) is skipped with a warning — the entry still lives in the
    /// inherited `in_flight` ledger (seeded by hydrate), so its terminal
    /// is attributed BY HASH through the ledger's defensive no-slot arm.
    ///
    /// The roster is rebuilt wholesale on every call: the replicated
    /// capacity ledger is the authoritative source and a partial patch
    /// would risk stale slots this coordinator can't observe from
    /// outside (same rationale as the pool rebuild).
    pub(crate) fn reconstruct_workers_from_cluster_state(&mut self) {
        // Roster source: the replicated per-secondary capacity records
        // (D1) of the NOT-authoritatively-removed members
        // (`live_known_secondaries` — capacity records are set-once and
        // outlive a `PeerRemoved`, so an unfiltered rebuild would
        // resurrect a removed peer's slots; a re-admission flips the
        // membership back to Alive and re-includes the preserved record),
        // name-sorted for the same deterministic ordering
        // `perform_initial_assignment` uses (it sorts `self.secondaries`'
        // keys). Pull the (id, worker_count, max_res) snapshot up front so
        // the build loop holds no overlapping borrow on `self`. `max_res`
        // mirrors initial assignment: the memory `ResourceAmount` from the
        // advertised set, as a single-entry `ResourceMap`.
        let mem_kind = dynrunner_core::ResourceKind::memory();
        let mut secondary_ids: Vec<String> = self
            .cluster_state
            .live_known_secondaries()
            .map(String::from)
            .collect();
        secondary_ids.sort();
        let roster: Vec<(String, u32, dynrunner_core::ResourceMap)> = secondary_ids
            .into_iter()
            .filter_map(|id| {
                self.cluster_state.secondary_capacity(&id).map(|cap| {
                    let ram_bytes = cap
                        .resources
                        .iter()
                        .find(|r| r.kind == mem_kind)
                        .map(|r| r.amount)
                        .unwrap_or(0);
                    (
                        id,
                        cap.worker_count,
                        dynrunner_core::ResourceMap::from([(mem_kind.clone(), ram_bytes)]),
                    )
                })
            })
            .collect();

        // Build the all-idle roster in ROUND-ROBIN order across
        // secondaries with one monotonic global `worker_id`, faithfully
        // mirroring `perform_initial_assignment` so the resulting Vec
        // ordering / global ids / per-worker budgets match what the live
        // primary built. (`local_worker_id_in_secondary` /
        // `worker_idx_for` only need the per-secondary 0-based order,
        // which round-robin preserves; the global id and budgets matter
        // for the dispatch view.)
        let max_workers_per_secondary = roster.iter().map(|(_, n, _)| *n).max().unwrap_or(0);
        let mut workers: Vec<crate::primary::RemoteWorkerState<I>> = Vec::new();
        let mut global_worker_id: u32 = 0;
        for round in 0..max_workers_per_secondary {
            for (id, worker_count, max_res) in &roster {
                if round < *worker_count {
                    let budget = self.scheduler.initial_budget(round, max_res);
                    workers.push(crate::primary::RemoteWorkerState {
                        worker_id: global_worker_id,
                        secondary_id: id.clone(),
                        resource_budgets: budget,
                        state: crate::primary::SlotState::Idle,
                    });
                    global_worker_id += 1;
                }
            }
        }
        self.workers = workers;

        // Occupancy crossing: move each replicated `TaskState::InFlight`'s
        // slot `Idle -> Assigned`, keyed by the CRDT hash so the inherited
        // ledger entry's stable `(secondary, worker)` holder resolves it
        // on terminal. Collected first to release the `tasks_iter` borrow
        // before the `&mut self` slot writes.
        let occupancy: Vec<(String, String, u32, TaskInfo<I>)> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(hash, state)| match state {
                TaskState::InFlight {
                    secondary, worker, ..
                } => Some((
                    hash.clone(),
                    secondary.clone(),
                    *worker,
                    // L5: resolve dep refs via the store.
                    self.cluster_state.task_to_info(state),
                )),
                _ => None,
            })
            .collect();
        for (hash, secondary, worker, task) in occupancy {
            match self.worker_idx_for(&secondary, worker) {
                Some(idx) => {
                    let estimated = self.estimator.estimate(&task);
                    // `Inherited` provenance: this occupancy is a STALE
                    // GUESS reconstructed from the replicated `InFlight`
                    // ledger, NOT a live observation. A survivor worker
                    // whose pre-kill task completed during the primary-
                    // less window is reconstructed `Assigned` here even
                    // though it is idle; marking the slot `Inherited` lets
                    // the worker's own post-`PrimaryChanged` `TaskRequest`
                    // reconcile it (free + requeue) rather than wedging on
                    // a phantom-busy slot. A slot whose worker is genuinely
                    // still running resolves normally when the broadcast
                    // `TaskComplete` lands. The slot was freshly rebuilt
                    // Idle just above, so the enforced idle-guard (#517)
                    // takes; it refuses only the pathological case of two
                    // InFlight ledger entries naming the SAME (secondary,
                    // worker), which the guard's WARN surfaces (the ledger
                    // still tracks the second by hash, as the `None` arm).
                    let _assigned = self.workers[idx].assign(
                        hash,
                        std::sync::Arc::new(task),
                        estimated,
                        crate::primary::SlotProvenance::Inherited,
                    );
                }
                None => {
                    tracing::warn!(
                        secondary = %secondary,
                        worker,
                        task_hash = %hash,
                        "inherited InFlight task resolves no reconstructed worker \
                         slot (capacity record missing or worker id out of range); \
                         leaving the slot count unchanged — the ledger entry still \
                         tracks it by hash"
                    );
                }
            }
        }

        tracing::info!(
            workers = self.workers.len(),
            secondaries = roster.len(),
            "reconstructed remote-worker roster from replicated capacity \
             and in-flight occupancy"
        );
    }

    /// Reconstruct the secondary roster (`self.secondaries` +
    /// `self.secondary_keepalives`) from the replicated per-secondary
    /// capacity ledger, so the heartbeat monitor + keepalive emitter
    /// operate on the CRDT-derived roster on the failover path too.
    ///
    /// One concern: turn `cluster_state.known_secondaries()` (D1's
    /// replicated capacity records) into the minimal per-secondary
    /// connection + keepalive state the three heartbeat methods read.
    /// Sibling of [`Self::reconstruct_workers_from_cluster_state`]: both
    /// derive a primary-local cache from the same CRDT roster source, each
    /// owning its own cache (workers vs. secondary connections), so neither
    /// reaches into the other's. Today `self.secondaries` is written ONLY
    /// by `connect.rs` / `peer_setup.rs` (the bootstrap handshake); the
    /// on-demand promotion path bypasses both, so before this a promoted
    /// primary's `self.secondaries` was empty and
    /// `broadcast_primary_keepalive` / `record_keepalive` /
    /// `collect_heartbeat_report` all degraded. This makes the roster a
    /// pure DERIVED CACHE of the replicated state on failover.
    ///
    /// The promoted primary reaches every secondary over the UNIFIED mesh
    /// transport via the egress edge (`Destination::All` /
    /// `Destination::Secondary(id)`), NOT the per-`SecondaryConnection` `QuicConnection`
    /// handle — that handle is the bootstrap dialer's artifact and cannot
    /// (and need not) be reconstructed here. The three heartbeat methods
    /// read only `is Operational` + the metadata fields (`num_workers`,
    /// `resources`, `is_observer`), never `transport`, so a metadata-only
    /// `Operational` seed with `transport = None` satisfies all of them.
    /// `is_observer` is read from the replicated `RoleTable.observers`
    /// projection so the seed matches the bootstrap welcome's flag.
    ///
    /// `secondary_keepalives` is seeded `Instant::now()` per known
    /// secondary — the same treatment `seed_keepalive` gives a bootstrap
    /// secondary at welcome — so the death deadline counts from promotion,
    /// not from `Instant`'s epoch (which would declare every inherited
    /// secondary instantly dead on the first heartbeat tick).
    ///
    /// Rebuilt wholesale on every call (like the worker roster): the
    /// replicated capacity ledger is the authoritative source.
    pub(crate) fn reconstruct_secondaries_from_cluster_state(&mut self) {
        let observers = self.cluster_state.role_table().observers.clone();
        // Same membership filter as the worker-roster sibling: a removed
        // peer's preserved capacity record must not re-enter the
        // connection/keepalive roster (it would re-arm a death clock for
        // a tombstoned member); a re-admitted peer re-enters naturally.
        let roster: Vec<(String, u32, Vec<dynrunner_core::ResourceAmount>, bool)> = self
            .cluster_state
            .live_known_secondaries()
            .map(String::from)
            .filter_map(|id| {
                let can_be_primary = self.cluster_state.can_be_primary(&id);
                self.cluster_state
                    .secondary_capacity(&id)
                    .map(|cap| (id, cap.worker_count, cap.resources.clone(), can_be_primary))
            })
            .collect();

        // The next minted id must exceed every id already HANDED OUT across
        // the failover so a respawn after promotion never collides with one
        // the pre-failover primary already minted (it had advanced the
        // counter past `config.num_secondaries`). Two replicated sources are
        // authoritative for "ids already handed out":
        //   * `known_secondaries()` — the capacity roster (a secondary that
        //     has broadcast its `SecondaryCapacity`); and
        //   * `respawn_events()` keys — the F7 respawn ledger, whose key is
        //     the minted `new_id`. A respawn the pre-failover primary minted
        //     + ledgered but whose secondary has NOT yet broadcast its
        //     `SecondaryCapacity` is INVISIBLE to `known_secondaries()`; the
        //     ledger is the only record of that already-handed-out id, so it
        //     must fold into the max or the promoted primary re-mints it and
        //     collides. Both encode ids as `secondary-{n}`, parsed by the
        //     shared `parse_secondary_index`.
        // `next_secondary_id` is a pure derived cache of the CRDT's id space;
        // the `.max(self.next_secondary_id)` floor preserves the
        // `config.num_secondaries` bootstrap reservation when both sources
        // are smaller than that floor. Deliberately the UNFILTERED
        // `known_secondaries()` (NOT `live_known_secondaries`): a REMOVED
        // peer's id was still handed out, and its index must stay burned
        // so a later mint never re-issues a dead member's identity.
        let max_known = self
            .cluster_state
            .known_secondaries()
            .filter_map(super::secondary_id::parse_secondary_index)
            .chain(
                self.cluster_state
                    .respawn_events()
                    .keys()
                    .filter_map(|id| super::secondary_id::parse_secondary_index(id)),
            )
            .max();
        if let Some(m) = max_known {
            self.next_secondary_id = self.next_secondary_id.max(m + 1);
        }

        self.secondaries.clear();
        self.secondary_keepalives.clear();
        let now = Instant::now();
        for (id, worker_count, resources, can_be_primary) in roster {
            let is_observer = observers.contains(&id);
            // Metadata-only Operational seed: walk the typestate to
            // Operational (the only state the heartbeat deadline applies
            // to) carrying the advertised capacity + observer flag +
            // primary-capability (read from the replicated `RoleTable`,
            // the authoritative source after hydration), with no
            // `QuicConnection` (reached via the unified mesh instead).
            let conn = SecondaryConnection::new(id.clone())
                .receive_welcome(
                    worker_count,
                    resources,
                    String::new(),
                    0,
                    None,
                    is_observer,
                    can_be_primary,
                )
                // Metadata-only seed: no liveness port either — each peer's
                // beacon address is re-learned from PeerInfo, and a
                // secondary's beacon re-points to this promoted primary off
                // the `PrimaryChanged` apply (its liveness addr is already in
                // the secondary's `peer_liveness_addrs` from the prior
                // PeerInfo). So a hydrated roster needs none.
                .receive_cert_exchange(String::new(), None, None, 0, None)
                .begin_peer_discovery()
                .peers_ready()
                .assignments_sent();
            self.secondaries
                .insert(id.clone(), SecondaryConnectionState::Operational(conn));
            self.secondary_keepalives.insert(id, now);
        }

        tracing::info!(
            secondaries = self.secondaries.len(),
            "reconstructed secondary roster (connection + keepalive state) \
             from replicated capacity ledger"
        );
    }
}
