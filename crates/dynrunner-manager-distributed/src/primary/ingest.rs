//! Initial-batch ingest classification + the run-wide invalidation op.
//!
//! Single concern: turn the `PendingPool::partition_ingest` DATA into
//! the manager-side POLICY for the `invalid_task` feature —
//!   * **#2 dependency-existence** — tasks whose `task_depends_on` names
//!     a literally-absent `(phase_id, task_id)` become terminal
//!     `InvalidTask` (the cluster keeps running);
//!   * **#3a pre-phase duplicate** — a `(phase_id, task_id)` collision in
//!     the INITIAL batch aborts the whole run (`RunAborted` + a
//!     structured `RunError`);
//!   * **#3b post-phase duplicate** — handled by `invalidate_all_pending`
//!     (invoked from the runtime `SpawnTasks` path), which latches the
//!     `RunAborted` verdict FIRST and then fails every not-yet-terminal
//!     task run-wide (a terminal run abort, like #3a — only the
//!     detection edge differs).
//!
//! The pool returns DATA; this module (the manager) decides what
//! `ClusterMutation`s / `RunError`s that data becomes and broadcasts
//! them through the canonical `apply_and_broadcast_cluster_mutations`
//! pipeline. The 3a/3b discriminator is `phase_started_emitted.is_empty()`:
//! the initial-batch ingest runs BEFORE `fire_initial_phase_starts`, so
//! it is unconditionally 3a; the runtime `SpawnTasks` path runs after a
//! phase has started, so it is 3b.

use std::collections::{HashMap, HashSet};

use dynrunner_core::{BoundedString, ErrorType, Identifier, PhaseId, TaskInfo};
use dynrunner_protocol_primary_secondary::{ClusterMutation, Destination, DistributedMessage};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::cluster_state::{TaskState, apply_locally_for_broadcast};
use crate::primary::wire::{compute_task_hash, timestamp_now};
use crate::primary::{PrimaryCoordinator, RunError};

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Build the `ClusterMutation::PhaseMayBeEmptySet` carrying the
    /// consumer's registered `may_be_empty` opt-out set, for emission
    /// paired with `PhaseDepsSet` from every seed originator. Single place
    /// the wire shape (sorted `Vec` for deterministic frames) is built, so
    /// the cold-seed and relocated-seed originators don't each re-spell it.
    /// An empty set yields an empty-`Vec` mutation that the apply arm treats
    /// as a NoOp — harmless on the common no-opt-out run.
    pub(crate) fn phase_may_be_empty_mutation(&self) -> ClusterMutation<I> {
        let mut phases: Vec<PhaseId> = self.phase_may_be_empty_decl.iter().cloned().collect();
        phases.sort();
        ClusterMutation::PhaseMayBeEmptySet { phases }
    }

    /// Build the `ClusterMutation::RespawnPolicySet` carrying this
    /// coordinator's enabled respawn caps, for emission paired with
    /// `PhaseDepsSet` from every seed originator (same run-constant
    /// lifecycle as [`Self::phase_may_be_empty_mutation`]). `None` when
    /// `--respawn-policy` is disabled (`enable_respawn` never ran) — a
    /// disabled policy replicates nothing and every replica's `None`
    /// stays "respawn off". Replicating the caps is what lets a
    /// relocated/promoted primary re-arm the respawn DECISION at hydrate
    /// (the spend ledger was already replicated; the caps were the
    /// missing half — see `respawn::remote`).
    pub(crate) fn respawn_policy_mutation(&self) -> Option<ClusterMutation<I>> {
        self.respawn_budget
            .as_ref()
            .map(|b| ClusterMutation::RespawnPolicySet {
                max_per_secondary: b.max_per_secondary,
                max_total: b.max_total,
                cooldown_ms: b.cooldown.as_millis() as u64,
            })
    }

    /// Build the `TaskSkippedAlreadyDone` transitions for the marked
    /// subset of a discovered batch — the ONE place both seed seams
    /// (`originate_cold_seed` cold-start + `discover_on_promotion`
    /// relocate/pre-staged) partition the already-done items off the
    /// freshly-seeded `Pending` set.
    ///
    /// Each entry marked `true` gets a `TaskSkippedAlreadyDone { hash }`
    /// keyed on its content hash; the prior `TaskAdded` in the same batch
    /// has already seeded that hash as `Pending`, and this transition
    /// materialises it terminal `SkippedAlreadyDone`. The apply rule
    /// gates on `Pending` (a skip is the WEAKEST terminal), so emitting it
    /// for a hash that some earlier transition already moved off `Pending`
    /// (e.g. a missing-dep `InvalidTask`) is a harmless NoOp — no
    /// special-casing here.
    ///
    /// Unmarked entries contribute nothing (they stay `Pending`).
    pub(crate) fn skip_transitions(
        &self,
        marked: &[(TaskInfo<I>, bool)],
    ) -> Vec<ClusterMutation<I>> {
        marked
            .iter()
            .filter(|(_, skipped)| *skipped)
            .map(|(task, _)| ClusterMutation::TaskSkippedAlreadyDone {
                hash: compute_task_hash(task),
            })
            .collect()
    }

    /// Map the framework-staging `pre_succeeded` setup-task hashes (#489 P3)
    /// to `Pending → SetupCompleted` transitions — the setup-task parallel of
    /// [`Self::skip_transitions`]. Each hash was seeded `Pending` by the
    /// `TaskAdded` fan-out (the setup task rides the batch alongside its
    /// dependent work task); this materialises it PRE-SUCCEEDED in the SAME
    /// local-apply pass, so hydrate routes its `task_id` into
    /// `completed_task_ids` and the dependent work task's `TaskDep`
    /// pre-resolves to `Pending` (never `Blocked`) — the #488-free path. The
    /// `SetupCompleted` apply rule gates on `Pending` | `InFlight`, so a hash
    /// already moved off `Pending` is a harmless NoOp. Shared by both
    /// originators (`originate_cold_seed` / `discover_on_promotion`) so the
    /// pre-succeeded seeding has one owner.
    ///
    /// Empty when the flag is off (the augmentation produced no setup tasks).
    pub(crate) fn setup_completed_transitions(
        &self,
        pre_succeeded: &[String],
    ) -> Vec<ClusterMutation<I>> {
        pre_succeeded
            .iter()
            .map(|hash| ClusterMutation::SetupCompleted { hash: hash.clone() })
            .collect()
    }

    /// Cold-start CRDT origination: turn the bootstrap task batch into the
    /// freshly-seeded replicated ledger `hydrate_from_cluster_state` then
    /// builds the pool from.
    ///
    /// THE cold-start half of the unified `run_pipeline` init: it sets
    /// `self.phase_deps`, classifies the batch, seeds EVERY surviving binary
    /// into the LOCAL `cluster_state` (as `Pending` via `TaskAdded` +
    /// `PhaseDepsSet`, then transitioning the #2 missing-dep set to
    /// `InvalidTask` and the discovery-marked already-done subset to
    /// `SkippedAlreadyDone`), and stages the version-stamped frames for the
    /// post-connection fleet broadcast. It does NOT build the pool or set
    /// `total_tasks` — those are outputs of the subsequent hydrate (the SOLE
    /// pool builder after F1). The promotion path skips this entirely (its
    /// CRDT was restored by `seed_from_promotion_snapshot`).
    ///
    /// `marked_batch` pairs every discovered task with its discovery-time
    /// `skipped_already_done` bit. The bit rides alongside the task (not on
    /// `TaskInfo<I>`): the pool partition / validation consumes the bare
    /// tasks, and the marked subset is materialised terminal via
    /// `skip_transitions` (shared with `discover_on_promotion`). An
    /// all-already-done cold corpus needs NO explicit `RunComplete`: every
    /// skip is seeded into hydrate's `completed_tasks` projection, so the
    /// operational loop's `completed + failed >= total_tasks` counter exit
    /// trips exactly as a fully-completed run. The `discover_on_promotion`
    /// seam finalizes through this SAME counter machinery — its own trailing
    /// `hydrate_from_cluster_state` runs AFTER its skip batch lands, so the
    /// identical projection accounts for its skips and it likewise originates
    /// no explicit run-terminal.
    ///
    /// Routing of `PendingPool::partition_ingest`'s three partitions:
    ///   * **duplicates** (#3a) → record the abort directive in
    ///     `self.pending_run_abort` and return WITHOUT seeding (the run is
    ///     doomed). `run_pipeline` fires the `RunAborted` broadcast at its
    ///     post-connection gate so it reaches the connected fleet. This
    ///     short-circuit runs BEFORE any CRDT origination — a doomed run
    ///     seeds nothing (C-3 constraint 3).
    ///   * **invalid_deps** (#2 missing-dep) → seeded into the CRDT as
    ///     `Pending` then immediately transitioned to `InvalidTask` locally
    ///     (the broadcast of that transition rides the staged frames), so
    ///     hydrate routes them to the dep-resolution seed (terminal) — NOT
    ///     the pool — exactly as the pre-F1 `mark_tasks_failed` pool pre-seed
    ///     did. Their `task_id` resolves any valid dependent's `task_depends_on`.
    ///   * **valid** → validated against a TRANSIENT pool (a cycle among
    ///     valid tasks is still surfaced as a hard `RunError::Other`, the
    ///     `extend` contract), then seeded into the CRDT. The transient pool
    ///     is discarded; hydrate rebuilds the authoritative one.
    ///
    /// `self.all_binaries` is set to `valid ∪ invalid_deps` (NOT the
    /// duplicates — the abort path seeds nothing). The non-abort path seeds
    /// EVERY entry of `all_binaries` as `TaskAdded` before hydrate, so
    /// hydrate's `cluster_state.task_count() == all_binaries.len()` (C-3
    /// constraint 4).
    pub(crate) fn originate_cold_seed(
        &mut self,
        marked_batch: Vec<(TaskInfo<I>, bool)>,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    ) -> Result<(), RunError> {
        // Framework flagged staging (#489 P3): when `--stage-via-setup-tasks`
        // is on, augment the batch with a per-file PRE-SUCCEEDED setup task
        // per file-backed work task + the work task's `TaskDep` on it. A no-op
        // (identity transform) when the flag is off — the default cold seed is
        // byte-for-byte unchanged. The `pre_succeeded` hashes are transitioned
        // `Pending → SetupCompleted` below alongside the skip transitions.
        let crate::primary::StagingAugmentation {
            batch: marked_batch,
            pre_succeeded: staging_pre_succeeded,
        } = crate::primary::augment_batch_for_staging(
            marked_batch,
            self.config.staging_strategy,
        );
        // NB: `phase_started_emitted` is NOT cleared here (V3). Seeding it is
        // now `hydrate_from_cluster_state`'s sole concern, derived from the
        // CRDT (`has_any && has-a-non-pending/blocked-task` per phase) on BOTH
        // the cold and promote paths: a freshly-seeded cold CRDT is all
        // `Pending`, so the derived set is empty (the equivalent of the old
        // `.clear()`); a promotion's inherited ledger has progressed tasks, so
        // its started phases are seeded. Removing the non-idempotent `.clear()`
        // here makes `originate_cold_seed` re-runnable without wiping a
        // legitimately-populated set (V1: idempotency-on-resume from the apply
        // layer's TaskAdded/PhaseDepsSet NoOps PLUS no local non-idempotent
        // side-effect). hydrate runs AFTER this in `run_pipeline`, so the
        // derived seed reflects the just-applied cold ledger.

        // The discovery `skipped_already_done` markers ride alongside the
        // tasks. The pool partition / validation below works on the bare
        // scheduling units, so project them out here; `marked_batch` is
        // retained intact and consulted once at the end by
        // `skip_transitions` to materialise the already-done subset
        // terminal (keyed by content hash, so the projected-out order
        // does not matter).
        let mut batch: Vec<TaskInfo<I>> =
            marked_batch.iter().map(|(task, _)| task.clone()).collect();

        // Sort by size descending for better packing — same intent as
        // pre-Phase-4b. The pool preserves insertion order within a bucket,
        // so we pre-sort here and seed once.
        batch.sort_by_key(|b| std::cmp::Reverse(b.size));

        // Phase set: union of (1) every phase referenced by an item, (2)
        // every phase mentioned as a key or parent in the deps map. The
        // transient pool's constructor validates that every dep references a
        // known phase.
        let mut phase_set: HashSet<PhaseId> = batch.iter().map(|b| b.phase_id.clone()).collect();
        for (k, v) in &phase_deps {
            phase_set.insert(k.clone());
            for p in v {
                phase_set.insert(p.clone());
            }
        }
        // Capture the canonical phase-deps graph for the run. The seed's
        // `PhaseDepsSet` mutation below replicates it so every secondary's
        // `cluster_state.phase_deps` mirrors the same map — the
        // post-promotion hydration consults it to rebuild a `PendingPool`
        // with the same dependency machine.
        self.phase_deps = phase_deps.clone();

        // TRANSIENT validation pool: `partition_ingest` is a pure `&self`
        // read (its known-set construction is documented read-only) and
        // `extend` is the atomic graph validator — run both against a
        // throwaway pool so the dependency-existence partition + the
        // cycle-rejection contract are preserved, then discard it. hydrate
        // rebuilds the authoritative pool from the seeded CRDT (F1-α: no
        // second CRDT→pool loop, no mutation of any persistent pool state).
        let mut validation_pool = PendingPool::new(phase_set.clone(), phase_deps)
            .map_err(|e| format!("PendingPool: {e:?}"))?;
        let partition = validation_pool.partition_ingest(batch);

        // #3a: a duplicate in the INITIAL batch aborts the whole run.
        // Discriminator is structural: this runs before
        // `fire_initial_phase_starts`, so `phase_started_emitted` is empty —
        // unconditionally pre-phase. Record the directive and return WITHOUT
        // seeding; `run_pipeline` fires the abort at the post-connection gate
        // so the `RunAborted` broadcast actually reaches the fleet.
        if !partition.duplicates.is_empty() {
            // The #3a/#3b discriminator is STRUCTURAL — this path runs before
            // `fire_initial_phase_starts`, so it is unconditionally pre-phase
            // (3a). It does NOT read `phase_started_emitted` (whose seeding
            // moved to hydrate, V3): the code-path ordering is the
            // discriminator, not the set's emptiness.
            let reasons: Vec<String> = partition
                .duplicates
                .iter()
                .map(|(_, reason)| reason.clone())
                .collect();
            let reason = format!(
                "{} duplicate task identity/identities in the initial batch: {}",
                partition.duplicates.len(),
                reasons.join("; ")
            );
            tracing::error!(reason = %reason, "initial-batch duplicate detected; aborting run");
            self.pending_run_abort = Some(reason);
            // Do not seed a doomed run.
            self.all_binaries = Vec::new();
            return Ok(());
        }

        // #2: validate the survivors against the transient pool with the
        // missing-dep ids pre-marked failed (so a valid dependent neither
        // fails `extend` nor strands), surfacing a CYCLE among valid tasks as
        // the same hard error the pre-F1 path did.
        let invalid_ids: Vec<String> = partition
            .invalid_deps
            .iter()
            .map(|(task, _)| task.task_id.clone())
            .collect();
        validation_pool.mark_tasks_failed(invalid_ids);
        validation_pool
            .extend(partition.valid.clone())
            .map_err(|e| {
                RunError::Other(format!("PendingPool::extend rejected task graph: {e}"))
            })?;
        // The validation pool has served its purpose; hydrate is the
        // authoritative builder.
        drop(validation_pool);

        // `all_binaries` keeps BOTH the valid survivors and the invalid-dep
        // tasks: the latter are seeded into the CRDT (so the `TaskFailed
        // { InvalidTask }` transition has a target) and counted in
        // `total_tasks` (hydrate derives it from `task_count()`, so the
        // operational loop's exit denominator accounts for them — they
        // terminate as InvalidTask, not as stranded).
        let mut all: Vec<TaskInfo<I>> =
            Vec::with_capacity(partition.valid.len() + partition.invalid_deps.len());
        all.extend(partition.valid.iter().cloned());
        let invalid_deps = partition.invalid_deps;
        for (task, _reason) in &invalid_deps {
            all.push(task.clone());
        }
        self.all_binaries = all;

        // Seed the LOCAL ledger: `PhaseDepsSet` + one `TaskAdded` per binary
        // (every entry of `all_binaries`, including the invalid-dep set —
        // C-3 constraint 4). The version-stamped applied frames are staged
        // for the post-connection broadcast (a pre-connection broadcast is
        // dropped, so the local-apply and the broadcast are split across the
        // connect boundary — C-3 constraints 1+2).
        let mut seed: Vec<ClusterMutation<I>> = Vec::with_capacity(self.all_binaries.len() + 2);
        seed.push(ClusterMutation::PhaseDepsSet {
            deps: self.phase_deps.clone(),
        });
        // Pair the static phase graph with the consumer's `may_be_empty`
        // opt-out set (the empty-drain proceed-or-fail discriminator), so a
        // promoted primary inherits the same opt-out. NoOp on apply when the
        // set is empty (the common no-opt-out run).
        seed.push(self.phase_may_be_empty_mutation());
        // Pair the respawn-policy CAPS with the phase graph (same
        // run-constant lifecycle) so a promoted primary inherits the
        // respawn decision's admission gate. Absent when the policy is
        // disabled — replicas keep `None` ("respawn off").
        seed.extend(self.respawn_policy_mutation());
        seed.extend(
            self.all_binaries
                .iter()
                .map(|b| ClusterMutation::TaskAdded {
                    hash: compute_task_hash(b),
                    task: b.clone(),
                }),
        );
        // Discovery already-done partition: materialise the marked subset
        // `Pending → SkippedAlreadyDone` in the SAME local-apply pass, so
        // hydrate sees them terminal (out of pool, NEVER dispatched) — the
        // already-done items are real tasks of their phase with a special
        // terminal state. The transition keys on the content hash that the
        // `TaskAdded` fan-out above already seeded as `Pending`; a marked
        // entry that is ALSO a missing-dep gets its `InvalidTask`
        // transition below and the skip NoOps (the apply rule's
        // weakest-terminal lockout). One shared helper with
        // `discover_on_promotion` — no duplicated partition logic.
        seed.extend(self.skip_transitions(&marked_batch));
        // Framework flagged staging (#489 P3): transition each pre-staged
        // file's setup task `Pending → SetupCompleted` in the SAME local-apply
        // pass (parallel to the skip transitions above), so hydrate routes its
        // `task_id` into `completed_task_ids` and its dependent work task's
        // `TaskDep` pre-resolves to `Pending`. Empty when the flag is off.
        seed.extend(self.setup_completed_transitions(&staging_pre_succeeded));
        // #2: transition each missing-dep task `Pending → InvalidTask` in the
        // SAME local-apply pass, so hydrate sees them terminal (dep-seed, out
        // of pool) — the faithful equivalent of the pre-F1 pool pre-fail. The
        // `Pending → InvalidTask` apply rule fans a `TaskCompletedEvent`
        // (carrying `invalid_task:<reason>`) to the task-completed dispatcher
        // (spawned at `run_pipeline` entry, before this), which is the
        // framework's emission for the observer's invalid_task monitor.
        for (task, reason) in invalid_deps {
            tracing::warn!(
                task_id = %task.task_id,
                phase = %task.phase_id,
                reason = %reason,
                "task has a missing dependency; marking invalid_task"
            );
            seed.push(ClusterMutation::TaskFailed {
                hash: compute_task_hash(&task),
                kind: ErrorType::InvalidTask {
                    reason: BoundedString::from(reason),
                },
                error: "missing dependency".to_string(),
                // Stamped at the origination choke point
                // (`apply_locally_for_broadcast`): `version` minted,
                // `attempt` read from the task's current generation (C-1).
                version: Default::default(),
                attempt: Default::default(),
            });
        }

        // Apply locally (stamps versions, filters NoOps) and STAGE the
        // applied frames for the post-connection broadcast. The resumed /
        // re-inject surfaces are empty for a fresh seed (no `Blocked`
        // dependents exist before the first dispatch), so we discard them.
        let batch = apply_locally_for_broadcast(&mut self.cluster_state, seed);
        self.pending_cold_seed_broadcast = batch.applied;
        Ok(())
    }

    /// Mode-2 relocated-seed origination: seed ONLY the phase graph + the
    /// discovery-debt marker, NO tasks. The CRDT-pure replacement for the
    /// deleted setup-defer handshake — the empty ledger + the `Owed` marker
    /// IS the "awaiting seed" state a relocated compute-peer primary (or an
    /// in-process `--source-already-staged` local primary) inherits and
    /// resolves via [`Self::discover_on_promotion`].
    ///
    /// A SEPARATE originator (NOT a flag on [`Self::originate_cold_seed`],
    /// whose body is dense with #2/#3a ingest classification that is
    /// meaningless for an empty relocated seed — folding a `declare_debt`
    /// flag in would add a mode-`if` to a single-concern function):
    ///   1. `PhaseDepsSet { deps: phase_deps }` so every replica — including
    ///      the compute-peer primary that will run discovery — has the phase
    ///      graph before hydrate (the consumer declares the phase graph
    ///      independent of discovery, which only resolves the per-task list).
    ///   2. `DiscoveryDebtDeclared` — ratchets `discovery_debt` `Undeclared →
    ///      Owed`, the signal `discover_on_promotion` gates on.
    ///
    /// Seeds NO `TaskAdded` (there are no tasks yet) and sets
    /// `self.all_binaries = Vec::new()`. The staged version-stamped frames
    /// ship post-connect via the existing [`Self::broadcast_cold_seed`]
    /// drain (it ships `pending_cold_seed_broadcast` verbatim; the relocated
    /// seed reuses that staging field). The local apply lands the marker on
    /// THIS node so the seed is coherent with its own ledger even before the
    /// broadcast reaches the fleet.
    // The PRODUCTION caller is the `run_pipeline` `SeedSource::RelocatedSeed`
    // match arm (`primary/coordinator.rs`), reached when the pyo3 layer
    // constructs that variant from the pre-staged signal — the mode-2 SLURM
    // submitter (`managers/primary/run.rs`) and the in-process
    // `--source-already-staged` local primary (`managers/distributed/run.rs`).
    pub(crate) fn originate_relocated_seed(
        &mut self,
        phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    ) {
        // Capture the canonical phase-deps graph for the run (mirrors
        // `originate_cold_seed`): the `PhaseDepsSet` mutation replicates it so
        // every replica's `cluster_state.phase_deps` mirrors the same map,
        // and the post-discovery hydrate consults it to rebuild the pool.
        self.phase_deps = phase_deps.clone();
        // No tasks discovered yet — the relocated/local primary runs
        // discovery itself post-connect.
        self.all_binaries = Vec::new();

        let mut seed: Vec<ClusterMutation<I>> = vec![
            ClusterMutation::PhaseDepsSet { deps: phase_deps },
            // Pair the `may_be_empty` opt-out with the phase graph (same
            // static-graph lifecycle) so the relocated/promoted primary's
            // empty-drain policy inherits it. NoOp on apply when empty.
            self.phase_may_be_empty_mutation(),
            ClusterMutation::DiscoveryDebtDeclared,
        ];
        // Pair the respawn-policy CAPS with the phase graph (same
        // run-constant lifecycle) so the relocated/promoted primary
        // re-arms the respawn decision at hydrate. Absent when disabled.
        seed.extend(self.respawn_policy_mutation());
        // Apply locally (stamps versions, filters NoOps) and STAGE the
        // applied frames for the post-connection broadcast — the same split
        // across the connect boundary `originate_cold_seed` uses (a
        // pre-connection broadcast is dropped). `broadcast_cold_seed` ships
        // `pending_cold_seed_broadcast` verbatim.
        let batch = apply_locally_for_broadcast(&mut self.cluster_state, seed);
        self.pending_cold_seed_broadcast = batch.applied;
    }

    /// Broadcast the staged cold-start seed frames to the connected fleet.
    ///
    /// Called from `run_pipeline` AFTER `wait_for_connections` (a
    /// pre-connection broadcast is dropped) so every secondary mirrors the
    /// seed. The frames were already applied locally + version-stamped by
    /// `originate_cold_seed`; this ships them verbatim — no re-apply, no
    /// version drift. Drains `pending_cold_seed_broadcast`, so the promotion
    /// path (which never originated a cold seed) ships nothing — a natural
    /// no-op, the `SeedSource` arm being the sole discriminator. Also runs
    /// the `preferred_secondaries` validation now that both the seeded task
    /// set and the connected secondary roster are settled.
    pub(crate) async fn broadcast_cold_seed(&mut self) {
        let frames = std::mem::take(&mut self.pending_cold_seed_broadcast);
        let task_count = self.all_binaries.len();
        if !frames.is_empty() {
            let msg = DistributedMessage::ClusterMutation {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                mutations: frames,
            };
            if let Err(error) = self.send_to(Destination::All, msg).await {
                tracing::warn!(
                    error = %error,
                    "cold-seed ClusterMutation broadcast delivery failed"
                );
            }
        }
        // Validate `preferred_secondaries` lists against the known secondary
        // set NOW that both inputs are settled: the seed is applied (so every
        // task's `preferred_secondaries` is in `all_binaries`) and
        // `wait_for_connections` has populated `self.secondaries`. The
        // validator emits one structured warn per unknown id; a later
        // `PeerLifecycleEvent::Added` re-validation in `handle_cluster_mutation`
        // can silence it.
        let known: std::collections::HashSet<&str> =
            self.secondaries.keys().map(|s| s.as_str()).collect();
        self.preferred_secondaries_validator
            .validate(self.all_binaries.iter(), &known);
        tracing::info!(tasks = task_count, "seeded cluster ledger");
    }

    /// Broadcast the pending #3a abort, if one was recorded at ingest.
    ///
    /// Called from `run_pipeline` AFTER `wait_for_connections` so the
    /// `RunAborted` broadcast reaches the connected secondaries (at
    /// ingest time none were connected yet). Returns `Err(RunError::
    /// DuplicateTaskIdPrePhase)` so the primary's own PyO3 boundary
    /// surfaces a non-zero exit; `Ok(())` when no abort is pending (the
    /// clean path). Single read of `pending_run_abort`.
    pub(crate) async fn fire_pending_run_abort(&mut self) -> Result<(), RunError> {
        let Some(reason) = self.pending_run_abort.take() else {
            return Ok(());
        };
        // The single terminal-verdict mechanism (#313) — same broadcast/
        // apply/settle path as `RunComplete`, so the abort inherits the
        // identical delivery semantics: the CRDT `run_aborted` flag lands
        // on every connected secondary and its `process_tasks` loop
        // returns `RunOutcome::Terminal` (projecting to
        // `SecondaryTerminal::Aborted`).
        self.broadcast_terminal_verdict(ClusterMutation::RunAborted {
            reason: reason.clone(),
        })
        .await;
        Err(RunError::DuplicateTaskIdPrePhase { reason })
    }

    /// Broadcast the pending consumer `on_run_start`-raise abort, if one
    /// was recorded on the promoted-primary path.
    ///
    /// The pyo3 promotion recipe fires `on_run_start` synchronously BEFORE
    /// `run_consuming`; on a raise it records the reason via
    /// `PrimaryCoordinator::record_pre_run_hook_abort`. Called from
    /// `run_pipeline` AFTER `wait_for_connections` (the same post-connection
    /// abort gate as `fire_pending_run_abort`), so the `RunAborted` broadcast
    /// reaches the connected secondaries — they exit their setup-wait on the
    /// replicated verdict instead of their deadline, and the observer receives
    /// it. Returns `Err(RunError::FatalPolicyExit)` so the primary's own PyO3
    /// boundary surfaces a non-zero exit (the SAME deliberate consumer-policy
    /// abort an `on_phase_end` raise surfaces); `Ok(())` when no abort is
    /// pending (the clean path AND every cold-start / non-raising promotion,
    /// where the directive is `None`). Detected BEFORE the operational loop, so
    /// — like `fire_pending_run_abort` — the abort is the broadcast + the typed
    /// return, NOT an `emit_run_fail_signal` (no loop is running to drain the
    /// worker-management bus). Single read of `pre_run_hook_abort`.
    pub(crate) async fn fire_pre_run_hook_abort(&mut self) -> Result<(), RunError> {
        let Some(reason) = self.pre_run_hook_abort.take() else {
            return Ok(());
        };
        tracing::error!(
            reason = %reason,
            "consumer on_run_start raised on the promoted primary; aborting run"
        );
        self.broadcast_terminal_verdict(ClusterMutation::RunAborted {
            reason: reason.clone(),
        })
        .await;
        Err(RunError::FatalPolicyExit { reason })
    }

    /// Abort the run on an INVALID COMPOSED GRAPH surfaced by the
    /// authoritative pool builder (`hydrate_from_cluster_state` returned
    /// `Err`) during bring-up.
    ///
    /// THE single policy home for the post-composition fatal (the
    /// asm-dataset LMU run_~1429 bring-up defect): a promoted primary's
    /// mode-2 discovery — or any pre-loop hydrate — found the replicated
    /// ledger describes an impossible task graph (a duplicate
    /// `(phase_id, task_id)` identity, a missing dep, or a cycle), which the
    /// hash-keyed CRDT dedup cannot collapse (`compute_task_hash` folds
    /// `(phase_id, path, identifier)`, not `task_id`) and only `extend`'s
    /// task_id/dep/cycle validation surfaces. Pre-fix `hydrate` logged ERROR
    /// and silently left `pending = None`; the run then never aborted — it
    /// sat with an empty pool while every secondary died one-by-one on its
    /// unconfigured deadline (the fleet never heard a verdict).
    ///
    /// Routed through the SAME terminal-verdict mechanism the #3a path
    /// (`fire_pending_run_abort`) uses — NOT a new special case: latch +
    /// broadcast the replicated `RunAborted { reason }` (so every secondary's
    /// setup-wait run-terminal gate at `setup.rs`'s loop head exits on the
    /// verdict instead of its deadline, and the observer receives it), then
    /// return the typed [`RunError::InvalidComposedGraph`] so this primary's
    /// own PyO3 boundary surfaces a non-zero exit. Detected BEFORE the
    /// operational loop, so — like #3a — the abort is the broadcast + the
    /// typed return, NOT an `emit_run_fail_signal` (no loop is running to
    /// drain the worker-management bus). A re-elected successor that hits the
    /// SAME duplicate re-aborts the run identically (its own hydrate-Err →
    /// this path), and the FIRST verdict latch wins (sticky first-writer).
    pub(crate) async fn abort_run_on_invalid_composition(
        &mut self,
        e: dynrunner_scheduler_api::PendingPoolError,
    ) -> RunError {
        let reason = format!("invalid composed task graph in cluster_state: {e}");
        tracing::error!(reason = %reason, "aborting run on invalid composed task graph");
        self.broadcast_terminal_verdict(ClusterMutation::RunAborted {
            reason: reason.clone(),
        })
        .await;
        RunError::InvalidComposedGraph { reason }
    }

    /// Fail every not-yet-terminal task across the WHOLE run as
    /// `InvalidTask` — the #3b op (a duplicate detected AFTER a phase
    /// started). The duplicate made the run's task set ambiguous, so
    /// this IS a terminal run abort: the verdict is latched + broadcast
    /// FIRST (`ClusterMutation::RunAborted` carrying the
    /// duplicate-identity reason), the dispatch freeze is latched
    /// synchronously, and only THEN is the ledger wiped.
    ///
    /// # Ordering is load-bearing (asm-dataset run_20260611_112116)
    ///
    /// Pre-fix this wiped the ledger WITHOUT authoring a verdict ("the
    /// cluster continues"): the phase machine then re-derived "phase
    /// ended" from the wiped ledger, the consumer's `on_phase_end` hook
    /// (correctly, from its view) raised against the zeroed handoff
    /// state, and THAT raise became the `RunAborted` reason the fleet
    /// read — a false narrative ("spawned=0") burying the true
    /// invalid-task verdict. Latching the verdict before the wipe makes
    /// the first writer the honest one (the latch is sticky
    /// first-writer-wins, see the `RunAborted` apply arm) and the
    /// run-terminal gate in `process_phase_lifecycle` keeps every phase
    /// hook from running against invalidated state.
    ///
    /// The primary's own structured exit rides the decoupled
    /// worker-management bus (`PolicyFatalExit` — a deliberate policy
    /// abort, surfacing `RunError::FatalPolicyExit` so the PyO3
    /// boundary RAISES): the emit synchronously freezes dispatch
    /// (step-0 seam), and the operational loop's stand-down /
    /// worker-mgmt exits drive the clean shutdown. The finalize tail's
    /// later broadcast of its own abort render NoOps locally and is
    /// filtered from the wire (`apply_locally_for_broadcast`), so the
    /// reason never churns.
    ///
    /// Scans `cluster_state.tasks_iter()` and emits a `TaskFailed
    /// { kind: InvalidTask }` for every entry in a non-terminal state
    /// (`Pending` / `InFlight` / `Blocked`). Already-terminal entries
    /// (`Completed` / `Failed` / `InvalidTask`) and the settled
    /// `Unfulfillable` failure-class entries are skipped — the
    /// `TaskFailed` apply rule's terminal lockout would NoOp them
    /// anyway, so the skip keeps the broadcast minimal and the reasons
    /// accurate. Each emitted entry's `Pending|InFlight|Blocked →
    /// InvalidTask` transition fans a `TaskCompletedEvent` to the
    /// dispatcher exactly like the #2 path.
    ///
    /// Routed through the canonical broadcast/apply pipeline.
    pub(crate) async fn invalidate_all_pending(&mut self, reason: String) {
        // 1. Latch + broadcast the terminal verdict FIRST — before any
        //    ledger wipe — so every replica (and this node's own phase
        //    machine, via the run-terminal cascade gate) reads the TRUE
        //    duplicate-identity reason. Single origination mechanism
        //    (#313): same apply/broadcast/settle path as every other
        //    terminal verdict.
        self.broadcast_terminal_verdict(ClusterMutation::RunAborted {
            reason: reason.clone(),
        })
        .await;
        // 2. Synchronously freeze dispatch + drive the primary's own
        //    structured exit through the decoupled worker-management bus
        //    (the same chokepoint the on_phase_end-raise path uses). The
        //    freeze is effective the moment this returns; the loop's
        //    drain classifies the signal into `RunError::FatalPolicyExit`.
        self.emit_run_fail_signal(crate::worker_signal::WorkerMgmtSignal::PolicyFatalExit {
            reason: reason.clone(),
        });
        // 3. Only now wipe the ledger.
        // Collect targets first (immutable borrow), then build the
        // mutation batch — `apply_and_broadcast` takes `&mut self`.
        let targets: Vec<(String, TaskInfo<I>)> = self
            .cluster_state
            .tasks_iter()
            .filter_map(|(hash, state)| match state {
                TaskState::Pending { task, .. }
                | TaskState::InFlight { task, .. }
                | TaskState::Blocked { task, .. } => Some((hash.clone(), task.clone())),
                TaskState::Completed { .. }
                | TaskState::Failed { .. }
                | TaskState::Unfulfillable { .. }
                | TaskState::InvalidTask { .. }
                // Already terminal: a #3b run-wide invalidation must not
                // overwrite a skip or a succeeded setup task (the apply
                // rule's weakest-terminal lockout would NoOp it anyway;
                // skipping it here keeps the broadcast minimal).
                | TaskState::SkippedAlreadyDone { .. }
                | TaskState::SetupCompleted { .. } => None,
            })
            .collect();
        if targets.is_empty() {
            return;
        }
        tracing::error!(
            count = targets.len(),
            reason = %reason,
            "duplicate task identity after a phase started; run aborted \
             (verdict latched first) and all not-yet-terminal tasks \
             invalidated run-wide"
        );
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(targets.len());
        for (hash, _task) in targets {
            mutations.push(ClusterMutation::TaskFailed {
                hash,
                kind: ErrorType::InvalidTask {
                    reason: BoundedString::from(reason.clone()),
                },
                error: "run-wide invalidation (duplicate task identity)".to_string(),
                // Stamped at the origination choke point
                // (apply_locally_for_broadcast): `version` minted,
                // `attempt` read from the task's current generation (C-1).
                version: Default::default(),
                attempt: Default::default(),
            });
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;
    }
}
