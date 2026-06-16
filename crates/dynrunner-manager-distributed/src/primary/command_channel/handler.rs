//! Per-variant command handlers. The `handle_primary_command` entry
//! is the single match called from the operational loop's `select!`;
//! each arm forwards to an `apply_*` method on `PrimaryCoordinator`
//! defined below so the mutation's state-machine semantics stay
//! alongside the rest of the coordinator's state.

use dynrunner_core::{ErrorType, Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::ClusterMutation;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::cluster_state::TaskState;
use crate::primary::PrimaryCoordinator;
use crate::worker_signal::WorkerMgmtSignal;

use super::types::{PrimaryCommand, SpawnError, validate_spawn_tasks};

/// One in-flight chunked `SpawnTasks` apply (#547) — the per-batch state the
/// coordinator's `spawn_continuation_queue` holds between
/// `PumpSpawnContinuation` ticks.
///
/// `remaining` is the un-applied tail of the original input (drained from the
/// front each chunk). `next_base_index` is the absolute index of
/// `remaining[0]` in the original input, used to translate the per-chunk
/// validator errors into absolute-input-index `SpawnError` entries the
/// caller's reply oneshot carries (so the chunked path is byte-identical to
/// the pre-#547 single-shot path from the caller's standpoint).
/// `accumulated_errors` is the running batch result; `reply` is the
/// original caller's oneshot the final chunk fires.
pub(crate) struct SpawnContinuation<I: Identifier> {
    pub(crate) remaining: Vec<TaskInfo<I>>,
    pub(crate) next_base_index: usize,
    pub(crate) accumulated_errors: Vec<(usize, SpawnError)>,
    pub(crate) reply: oneshot::Sender<Result<Vec<(usize, SpawnError)>, String>>,
}

/// Dispatch one received command to its handler. Single line at the
/// `select!` call site keeps the operational-loop's match arm
/// transport-shape-pure.
///
/// `command_rx` threads the operational-loop's command-channel receiver
/// into the `FailPermanent` cascade so a callback-issued `spawn_tasks`
/// fired by an `on_phase_end` running inside `apply_fail_permanent`'s
/// recursive `note_item_failed` step applies inline. The same receiver
/// is also threaded into the in-cascade drain step that called us in
/// the first place (when the dispatcher was invoked from inside
/// `process_phase_lifecycle`), so the drain remains a single source
/// of truth across nested cascade levels.
pub async fn handle_primary_command<S, E, I>(
    coordinator: &mut PrimaryCoordinator<S, E, I>,
    command: PrimaryCommand<I>,
    command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
) where
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    match command {
        PrimaryCommand::FailPermanent {
            hash,
            error,
            reason,
            reply,
        } => {
            let result = coordinator
                .apply_fail_permanent(hash, error, reason, command_rx)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::ReinjectTask { hash, reply } => {
            let result = coordinator.apply_reinject_task(hash).await;
            let _ = reply.send(result);
        }
        PrimaryCommand::UpdatePreferredSecondaries {
            hash,
            secondaries,
            reply,
        } => {
            let result = coordinator
                .apply_update_preferred_secondaries(hash, secondaries)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::SetCanBePrimary {
            peer_id,
            can_be_primary,
            reply,
        } => {
            let result = coordinator
                .apply_set_can_be_primary(peer_id, can_be_primary)
                .await;
            let _ = reply.send(result);
        }
        PrimaryCommand::SpawnTasks { tasks, reply } => {
            // Chunked-continuation orchestrator (#547). The dispatch arm
            // RETURNS as soon as one chunk completes; subsequent chunks
            // ride later select! iterations driven by
            // `PumpSpawnContinuation` so sibling arms (heartbeat,
            // inbox, worker-mgmt, matcher) re-fire between chunks.
            coordinator.apply_spawn_tasks(tasks, reply).await;
        }
        PrimaryCommand::PumpSpawnContinuation => {
            // One-chunk drain of the FRONT continuation in
            // `spawn_continuation_queue`. The handler re-kicks itself
            // (sends another `PumpSpawnContinuation` onto `command_tx`)
            // whenever the queue still has work after the chunk
            // completes — this re-entry is what releases the COMMAND
            // arm's body so select!'s sibling arms can re-fire.
            coordinator.pump_spawn_continuation().await;
        }
    }
}

impl<S, E, I> PrimaryCoordinator<S, E, I>
where
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Resolve a task hash through the CRDT ledger and return
    /// `(phase_id, task_id)` for the pool's bookkeeping. The CRDT is
    /// the single authoritative source for the post-failure metadata
    /// the pool needs; the local `pending_pool` doesn't itself index
    /// by hash.
    pub(super) fn task_meta_for_hash(
        &self,
        hash: &str,
    ) -> Option<(dynrunner_core::PhaseId, String)> {
        // `task_view` (not `task_state`): a SETTLED hash's identity is
        // served off the slim index — exactly the post-terminal metadata
        // this lookup exists for.
        let view = self.cluster_state.task_view(hash)?;
        Some((view.phase_id().clone(), view.task_id().to_string()))
    }

    /// Handler for `PrimaryCommand::FailPermanent`. Wraps the existing
    /// `pending_pool::on_item_failed_permanent` primitive so the
    /// cascade-to-dependents semantics that primitive owns also apply
    /// to externally-requested failures, then broadcasts the
    /// `TaskFailed` mutation so every node mirrors the terminal state.
    ///
    /// Cascade routing splits on `error`:
    /// * `ErrorType::Unfulfillable { .. }` — dependents are broadcast
    ///   as `ClusterMutation::TaskBlocked { hash, on: <root> }`, so
    ///   the CRDT mirrors land in `TaskState::Blocked { on, task }`
    ///   on every replica. The matching `TaskCompleted` apply arm
    ///   auto-resumes them to `Pending` when the prereq later
    ///   completes via the reinject + re-run path. Dependents are
    ///   NOT recorded in the local per-pass `failed_tasks` ledger —
    ///   they're cascade-paused, not failed.
    /// * Any other `ErrorType` — dependents are recorded in the local
    ///   `failed_tasks` ledger with the same error (the legacy shape
    ///   a worker-driven cascade-fail produces).
    ///
    /// Pool-side auto-resume of cascade-paused dependents is wired
    /// through `apply_and_broadcast_cluster_mutations`: when the
    /// prereq's `TaskCompleted` later flows through the apply path,
    /// `cluster_state::apply_locally_for_broadcast` surfaces every
    /// just-resumed `TaskInfo<I>` and the caller re-injects each
    /// into the live `PendingPool` so the next dispatch tick picks
    /// them up. The CRDT and pool stay coherent without a per-task
    /// re-cascade walk here.
    // `pub(crate)` (not `pub(super)`): the permanent-failure cascade is the
    // shared non-recoverable terminal path. Besides the command channel, the
    // setup-task terminal sink (`primary::setup_dispatch::settle_setup_terminal`)
    // routes a FAILED setup task through it so a setup failure cascades to
    // dependents identically to a non-recoverable worker terminal.
    pub(crate) async fn apply_fail_permanent(
        &mut self,
        hash: String,
        error: ErrorType,
        reason: String,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), String> {
        let Some((phase_id, task_id)) = self.task_meta_for_hash(&hash) else {
            return Err(format!("fail_permanent: unknown task hash {hash}"));
        };
        // Record the failure in the local per-pass ledger so the
        // operational loop's accounting + the per-phase counters match
        // the wire-side state. Mirrors `handle_task_failed`'s
        // `failed_tasks.insert(...)` step (the same in-memory side-
        // effect a worker-originated failure would have).
        self.failed_tasks.insert(hash.clone(), error.clone());

        // Cascade-to-dependents via the pool primitive. The returned
        // list is the dependents that the pool just gave up on; how
        // the caller observes them depends on the error class
        // (cascade-pause for Unfulfillable, cascade-fail otherwise).
        // `task_id` is non-optional per the framework's boundary
        // contract.
        let cascaded_blocks: Vec<(String, String)> = {
            let cascaded = self
                .pool_mut()
                .on_item_failed_permanent(&phase_id, task_id.as_str());
            let is_unfulfillable = matches!(error, ErrorType::Unfulfillable { .. });
            let mut blocks = Vec::new();
            for cascaded_binary in &cascaded {
                let cascaded_hash = crate::primary::wire::compute_task_hash(cascaded_binary);
                if is_unfulfillable {
                    blocks.push((cascaded_hash, hash.clone()));
                } else {
                    self.failed_tasks.insert(cascaded_hash, error.clone());
                }
            }
            blocks
        };

        // Broadcast the terminal state for the originating task plus
        // any cascade-paused dependents (Unfulfillable case only).
        // The CRDT-applied broadcast is the single source of truth
        // for every observer; ordering the originating TaskFailed
        // first means receivers see the prereq's Unfulfillable state
        // before the dependents' Blocked state — the cascade root is
        // visible whenever a dependent's `on` field is consulted.
        //
        // Applied BEFORE the `note_item_failed` lifecycle cascade below —
        // the uniform apply-then-cascade order the worker-terminal paths
        // (`handle_task_failed` / `handle_task_complete`) already use, and
        // load-bearing since #358: the apply's `merge_task_state` join owns
        // the per-phase Failed EVENT tally bump, so a phase that drains
        // inside the cascade must fire `on_phase_end` with a tally that
        // already includes THIS failure.
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(1 + cascaded_blocks.len());
        mutations.push(ClusterMutation::TaskFailed {
            hash,
            kind: error,
            error: reason,
            // Both stamped at the origination choke point
            // (apply_locally_for_broadcast): `version` minted, `attempt`
            // read from the task's current generation (C-1).
            version: Default::default(),
            attempt: Default::default(),
        });
        for (dep_hash, on_hash) in cascaded_blocks {
            mutations.push(ClusterMutation::TaskBlocked {
                hash: dep_hash,
                on: on_hash,
            });
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;

        // Phase + lifecycle bookkeeping. Must run AFTER the pool
        // mutation so `process_phase_lifecycle` observes the post-
        // cascade pool state. `kind = None`: the pool ALREADY observed
        // this failure's identity + permanence through the
        // `on_item_failed_permanent` call above, so the routing must take
        // the legacy in-flight-only decrement — a retry-pending marker
        // here would be redundant with the final `failed_tasks` entry.
        self.note_item_failed(&phase_id, Some(task_id.as_str()), None, command_rx)
            .await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::ReinjectTask`. Accepts only entries
    /// whose CRDT state is the discrete `TaskState::Unfulfillable { .. }`
    /// — the operator-resolvable-failure class. Decrements the per-task
    /// budget; on exhaustion the local state stays `Unfulfillable` and
    /// the caller receives `Err`.
    pub(super) async fn apply_reinject_task(&mut self, hash: String) -> Result<(), String> {
        // Inspect CRDT state first — the local pool isn't indexed by
        // hash, and the discrete-variant gate has to read the
        // authoritative ledger.
        let binary = match self.cluster_state.task_state(&hash) {
            // L5: resolve dep refs via the store.
            Some(state @ TaskState::Unfulfillable { .. }) => self.cluster_state.task_to_info(state),
            Some(_) => {
                return Err(format!(
                    "reinject_task: hash {hash} not in Unfulfillable state"
                ));
            }
            // A SETTLED hash is a known terminal (never Unfulfillable —
            // Unfulfillable entries stay fat by the settle predicate), so
            // the honest refusal names the state mismatch, not an unknown
            // hash.
            None if self.cluster_state.contains_task(&hash) => {
                return Err(format!(
                    "reinject_task: hash {hash} not in Unfulfillable state"
                ));
            }
            None => {
                return Err(format!("reinject_task: unknown task hash {hash}"));
            }
        };

        // Budget check (P3). `None` == unbounded (the bypass branch): no
        // used counter is originated, since there is no cap to enforce.
        // `Some(cap)`: `remaining = cap − used` is derived LOCALLY from the
        // replicated grow-only-MAX `unfulfillable_reinject_used` field; a
        // `remaining == 0` refuses. On a successful reinject below the used
        // count is max-bumped via the originator so it survives failover (a
        // promoted primary inherits `used` and does NOT re-grant the
        // budget). The replicated counter counts UP (was a node-local
        // DECREMENTING `remaining`); the local derivation is the read-side.
        let max = self.config.unfulfillable_reinject_max_per_task;
        let used = self.cluster_state.unfulfillable_reinject_used_for(&hash);
        if let Some(cap) = max {
            let remaining = cap.saturating_sub(used);
            if remaining == 0 {
                tracing::warn!(
                    task_hash = %hash,
                    cap,
                    event = "unfulfillable_reinject_budget_exhausted",
                    "reinject budget exhausted for task; staying Failed"
                );
                return Err(format!(
                    "reinject_task: budget exhausted for hash {hash} \
                     (cap={cap})"
                ));
            }
        }

        // Local pool reinject: same primitive the retry-pass code path
        // uses. Re-injecting flips Drained/Done phase state back to
        // Active for this binary's phase, putting the item back into
        // the bucket head so the next dispatch tick picks it up.
        self.failed_tasks.remove(&hash);
        self.pool_mut().reinject(std::sync::Arc::new(binary));

        // Originate the bumped used count (P3) ONLY when a cap is set —
        // an unbounded `None` cap has no budget to enforce, so skip the
        // origination entirely (no counter to grow). Grow-only MAX, so a
        // promoted primary inherits `used + 1` and the budget survives
        // failover.
        if max.is_some() {
            self.cluster_state
                .record_unfulfillable_reinject_used(hash.clone(), used + 1);
        }

        // Broadcast so every node's CRDT mirror moves the entry off
        // `Failed` synchronously.
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskReinjected {
            hash,
            // Stamped at the origination choke point (apply_locally_for_broadcast).
            version: Default::default(),
        }])
        .await;
        // The reinjected binary is a pool-entry edge — EMIT a
        // `TasksAdded` so the worker-management recheck picks it up. The
        // matcher auto-fires this command, and a free worker that
        // already got "no work" before the reinject won't re-poll on its
        // own; the decoupled recheck closes that gap. Decoupled emit,
        // never a direct dispatch call (the dispatch-decoupling law).
        self.cluster_state
            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
        Ok(())
    }

    /// Handler for `PrimaryCommand::UpdatePreferredSecondaries`.
    /// Broadcasts the per-task preferred-secondaries update so every
    /// node's CRDT mirror sees the new preference list AND mirrors
    /// the new list onto the live primary's `PendingPool` entry so
    /// the next scheduler tick reads the updated preference. The
    /// pool stores `TaskInfo<I>` clones (taken at injection time);
    /// without this mirror the CRDT write would only become visible
    /// to the scheduler on a snapshot-restore cycle — every
    /// dispatch between the two would see the stale preference list.
    ///
    /// The pool match is keyed on the wire-canonical task hash via
    /// the generic `pool::update_first_match_in_place` primitive,
    /// so the pool itself stays oblivious to hashing.
    pub(super) async fn apply_update_preferred_secondaries(
        &mut self,
        hash: String,
        secondaries: Vec<String>,
    ) -> Result<(), String> {
        // Presence over the LOGICAL ledger (`contains_task`): a SETTLED
        // hash is a known task — the pool match below finds nothing (a
        // terminal never re-dispatches) and the broadcast NoOps, but the
        // command must not mis-report a real task as unknown.
        if !self.cluster_state.contains_task(&hash) {
            return Err(format!(
                "update_preferred_secondaries: unknown task hash {hash}"
            ));
        }
        // Mirror onto the live pool's TaskInfo clone. Done BEFORE the
        // broadcast so a hypothetical synchronous reader of the pool
        // (post-apply, pre-broadcast) sees the new preferences and
        // the CRDT-side mirror simultaneously. The hash-keyed
        // predicate closes over `compute_task_hash`; the pool API
        // takes any predicate so it doesn't have to learn about
        // wire-canonical hashing.
        let target_hash = hash.clone();
        let new_preferences = dynrunner_core::SoftPreferredSecondaries::new(secondaries.clone());
        let matched = self.pool_mut().update_first_match_in_place(
            |t| crate::primary::wire::compute_task_hash(t) == target_hash,
            |t| t.preferred_secondaries = new_preferences.clone(),
        );
        if !matched {
            // The pool may legitimately not hold the binary (in-flight
            // / completed / not yet seeded), and that's fine — only
            // queued/blocked items need the live mirror. CRDT side
            // still broadcasts so every replica's `TaskInfo` clone
            // converges on the new preference list.
            tracing::debug!(
                task_hash = %hash,
                "update_preferred_secondaries: hash not present in pool; \
                 CRDT mirror only"
            );
        }
        self.apply_and_broadcast_cluster_mutations(vec![
            ClusterMutation::TaskPreferredSecondariesUpdated {
                hash,
                secondaries,
                // Stamped at the origination choke point (apply_locally_for_broadcast).
                version: Default::default(),
            },
        ])
        .await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::SetCanBePrimary`. Broadcasts a
    /// `ClusterMutation::SetCanBePrimary` so every node's
    /// `RoleTable.can_be_primary` set converges on the new capability.
    /// Pure cluster-state mutation — no pool side effect (capability is
    /// a coordinator-edge fact, not a per-task one). Always `Ok`: a
    /// client may permit or forbid any peer id at any time, including
    /// one not yet joined (the apply rule is idempotent and does not
    /// gate on membership).
    pub(super) async fn apply_set_can_be_primary(
        &mut self,
        peer_id: String,
        can_be_primary: bool,
    ) -> Result<(), String> {
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::SetCanBePrimary {
            peer_id,
            can_be_primary,
            // Stamped at the origination choke point.
            cap_version: Default::default(),
        }])
        .await;
        Ok(())
    }

    /// Handler for `PrimaryCommand::SpawnTasks`. Pre-validates every
    /// input task (duplicate-hash + unknown-dependency check) against
    /// the current cluster ledger, builds a single
    /// `ClusterMutation::TasksSpawned` carrying the valid subset, and
    /// applies+broadcasts it. Returns per-index errors for the
    /// rejected entries; the rest of the batch proceeds regardless.
    ///
    /// Post-apply, every freshly-Pending task is re-injected into the
    /// live primary's `PendingPool` so the next dispatch tick picks
    /// it up. Tasks that landed in `Blocked` are not pool-resident
    /// (they wait for the auto-resume mechanism in
    /// `resume_blocked_on` to fire on a later `TaskCompleted`). Tasks
    /// that landed in `Failed { NonRecoverable, .. }` (cascade-fail
    /// against an upstream `Failed { NonRecoverable, .. }` dep) are
    /// recorded in the per-pass `failed_tasks` ledger so the
    /// operational loop's accounting matches the wire-side state —
    /// same shape `apply_fail_permanent` produces for worker-
    /// originated permanent failures.
    ///
    /// `files=` caveat (#336 P2 / #493 option-A): runtime-spawned
    /// `TaskInfo`s must NOT carry `required_files` (the `files=` shape).
    /// The runtime-spawn path here does NOT call
    /// `augment_batch_for_staging` — only the initial cold seed
    /// (`originate_cold_seed`) and the relocated discovery
    /// (`discover_on_promotion`) do. A spawn-time task that lists a file
    /// would dispatch WITHOUT a derived upload setup task or the
    /// matching dep, racing against an absent file. Pre-flight (declare
    /// `files=` on the submitter's `discover_items` batch) or
    /// pre-upload from the spawner before the `spawn_tasks` call.
    /// Adding augmentation here is its own seam decision (the runtime
    /// spawn lacks the cold-seed's identifier-cloning anchor and the
    /// dedup is per-batch rather than ledger-global), kept deliberately
    /// out of scope for #493.
    /// Chunked-continuation orchestrator for `PrimaryCommand::SpawnTasks`
    /// (#547). One large `tasks` Vec splits into chunks of
    /// [`Self::APPLY_SPAWN_CHUNK_SIZE`] each; chunks ride MULTIPLE select!
    /// iterations driven by `PumpSpawnContinuation` so sibling arms
    /// (`heartbeat_tick`, `inbox.recv()`, `worker_mgmt_rx`, matcher) re-fire
    /// between chunks — the pre-fix 150 s wedge starved them all for the
    /// whole batch duration. A small batch (size `<=`
    /// `APPLY_SPAWN_CHUNK_SIZE` AND the queue is currently empty) takes the
    /// single-shot fast path, byte-identical to the pre-#547 behaviour.
    ///
    /// Pre-check the WITHIN-BATCH `(phase_id, task_id)` duplicate class once
    /// against the full input — its semantics demand the WHOLE batch be
    /// rejected if any pair is found (#3b: the run is run-aborted), so
    /// detecting it per-chunk would require either re-finding cross-chunk
    /// pairs after the fact or aborting mid-stream (both ugly). The owner-
    /// approved shape is one upfront `(phase_id, task_id)` HashSet build
    /// here; chunks then never encounter the class. The
    /// `DuplicateTaskHash` (already-in-ledger, idempotent re-spawn) class
    /// stays per-chunk — the validator drops those entries and surfaces
    /// per-index errors, and the prior ledger entry is the authoritative
    /// copy (the failover-replay shape the pre-#547 path handles).
    pub(super) async fn apply_spawn_tasks(
        &mut self,
        tasks: Vec<TaskInfo<I>>,
        reply: oneshot::Sender<Result<Vec<(usize, SpawnError)>, String>>,
    ) {
        // Empty input ⇒ no work, no continuation. Reply Ok(empty errors).
        if tasks.is_empty() {
            let _ = reply.send(Ok(Vec::new()));
            return;
        }

        // Pre-check WITHIN-BATCH `(phase_id, task_id)` duplicates ONCE on
        // the full input. The hashset build is O(N), cheap relative to the
        // per-task BLAKE3 + dep-probe + insert work the chunked apply will
        // do. Reaching the chunked apply with a within-batch dupe is a
        // contract violation by this pre-check, so the per-chunk validator
        // does not need to invoke `invalidate_all_pending` mid-stream.
        let in_batch_dupes = detect_in_batch_dupes::<I>(&tasks);
        if !in_batch_dupes.is_empty() {
            // Build the per-index error result + run-wide invalidate.
            // `apply_spawn_tasks` IS the runtime-spawn path by construction
            // (the initial batch goes through `ingest_initial_batch`), so
            // reaching this handler is unconditionally 3b.
            let reason = format!(
                "{} duplicate task identity/identities within a single runtime \
                 spawn batch: {}",
                in_batch_dupes.len(),
                in_batch_dupes
                    .iter()
                    .map(|(_, h)| format!("duplicate task hash {h}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            self.invalidate_all_pending(reason).await;
            let errors: Vec<(usize, SpawnError)> = in_batch_dupes
                .into_iter()
                .map(|(idx, hash)| (idx, SpawnError::DuplicateInBatch(hash)))
                .collect();
            let _ = reply.send(Ok(errors));
            return;
        }

        // ALWAYS route through the continuation queue, even for a single-
        // chunk input (#582). The pre-#582 single-shot fast path
        // (`tasks.len() <= APPLY_SPAWN_CHUNK_SIZE` AND queue empty) drained
        // the entire batch inside the COMMAND arm body with NO yield back to
        // `select!` — so under a sustained submission cadence (consumer
        // run_20260615_192743: ~74 tasks/batch × 10 batches/sec) the COMMAND
        // arm was perpetually ready, and #586's `biased;` then guaranteed
        // ARM_HEARTBEAT (id=4, below the data arms in source-order priority)
        // could lose every tiebreak indefinitely → keepalive deafness, 75s
        // collective-silence episode, 11 false departures. Routing through
        // the queue makes every batch take ≥1 select! re-entry between the
        // apply and the caller's reply, so sibling arms (heartbeat, inbox,
        // worker_mgmt, matcher) re-fire between successive spawn batches no
        // matter how fast the submitter streams. The extra select! iteration
        // per batch is the single tokio task slice that keeps the data path
        // cooperative; throughput cost is negligible at consumer rates and
        // the chunked path's apply semantics are byte-identical to the
        // pre-fix fast path for a same-sized single-chunk input.
        //
        // BIASED-PRIORITY CONTRACT (#586 + #582 amendment): `biased;` in the
        // operational-loop select! resolves RACE-WINNER ORDERING for ties —
        // when multiple arms become ready in the same poll, the
        // higher-priority arm wins. It does NOT bound how long a winning
        // arm's body runs; a body that does not yield monopolises the loop
        // until it returns. Every higher-priority data-arm body
        // (ARM_COMMAND, ARM_INBOX) must therefore yield to siblings between
        // work units — either via the continuation-queue pattern below or
        // via `yield_now().await` — so the heartbeat-deadline fairness gate
        // (`fire_heartbeat_if_overdue` at the top of each loop iteration)
        // is the last line of defence, not the only one.
        // Chunked path. Install the continuation at the BACK of the queue —
        // FIFO across concurrent external batches (the COMMAND arm yields
        // between chunks, so a second SpawnTasks can land mid-drain; we
        // must finish the first batch's chunks before starting the second).
        let was_empty = self.spawn_continuation_queue.is_empty();
        self.spawn_continuation_queue.push_back(SpawnContinuation {
            remaining: tasks,
            next_base_index: 0,
            accumulated_errors: Vec::new(),
            reply,
        });
        // F5 ATOMIC-BATCH SEAM (mutation_capture is armed): the surrounding
        // `drain_callback_queued_commands_capturing` walk has armed the
        // capture sink, so EVERY local apply this batch produces is diverted
        // into a buffer rather than broadcast, and the whole buffer flushes
        // as ONE wire frame after the drain returns (the F5 atomicity
        // contract: effect + terminal land together or not at all). Inside
        // that window there is NOTHING for sibling select! arms to do — the
        // wire egress is held back by the capture, so the #582
        // yield-between-chunks contract does not apply. Drain the spawn-
        // continuation queue SYNCHRONOUSLY here so the captured batch
        // includes every applied mutation; otherwise the capture window
        // would close before the kick-scheduled `PumpSpawnContinuation`
        // could run, and the spawn's mutations would land in a SEPARATE
        // later frame — breaking the atomic effect+terminal invariant
        // (`handler_effect_and_handled_terminal_ride_one_frame`).
        //
        // Outside the capture window this branch is skipped: the normal
        // path enqueues + kicks `PumpSpawnContinuation` onto `command_tx`,
        // and the operational-loop's ARM_COMMAND arm services one chunk
        // per select! iteration — the #582 fairness contract.
        if self.mutation_capture.is_some() {
            while !self.spawn_continuation_queue.is_empty() {
                self.pump_spawn_continuation().await;
            }
            return;
        }
        // If the queue was empty, the drain loop is dormant — kick it. If
        // it wasn't empty, an earlier kick is already in flight (the
        // current pump-handler will re-kick when it advances), so we do
        // NOT enqueue a duplicate (which would double-drain a single chunk
        // and is harmless but wastes one extra select! iteration).
        if was_empty {
            self.kick_spawn_continuation();
        }
    }

    /// Drain one chunk from the FRONT continuation in
    /// `spawn_continuation_queue`. Idempotent against an empty queue (a
    /// stray `PumpSpawnContinuation` arrival — e.g. a kick that races a
    /// run-reset clear — is benign).
    pub(super) async fn pump_spawn_continuation(&mut self) {
        // Peek+take rather than `front_mut` to avoid borrowing the queue
        // across the long-running `apply_spawn_tasks_chunk` await — the
        // chunk apply mutates `self.cluster_state` etc., so a `&mut self`
        // overlap with the queue borrow would be a compile error.
        let Some(mut cont) = self.spawn_continuation_queue.pop_front() else {
            // No work: a stray kick. Either the queue was drained between
            // the sender and this handler, or a run-reset emptied it.
            return;
        };

        // Drain ONE chunk. `min(CHUNK, remaining)` keeps the final tail
        // chunk's size honest.
        let chunk_size = std::cmp::min(Self::APPLY_SPAWN_CHUNK_SIZE, cont.remaining.len());
        // Split off the front `chunk_size` entries. `drain(..n).collect()`
        // is the canonical pop-front-N for `Vec`.
        let chunk: Vec<TaskInfo<I>> = cont.remaining.drain(..chunk_size).collect();
        let base_index = cont.next_base_index;
        cont.next_base_index += chunk_size;
        let chunk_errors = self.apply_spawn_tasks_chunk(chunk, base_index).await;
        cont.accumulated_errors.extend(chunk_errors);

        if cont.remaining.is_empty() {
            // Final chunk: fire the original reply with the accumulated
            // errors. A dropped receiver is non-fatal (the contract is
            // shared with every other reply-bearing command).
            let _ = cont.reply.send(Ok(cont.accumulated_errors));
            // Did NOT push back. Kick the next continuation (if any).
            if !self.spawn_continuation_queue.is_empty() {
                self.kick_spawn_continuation();
            }
            return;
        }

        // More chunks to do: push back at the FRONT (preserving FIFO with
        // any siblings that joined behind us), kick another tick.
        self.spawn_continuation_queue.push_front(cont);
        self.kick_spawn_continuation();
    }

    /// Send one `PumpSpawnContinuation` onto `command_tx` so the operational
    /// `select!` re-enters the COMMAND arm with a continuation tick the next
    /// iteration. `try_send` (not `send().await`) because we are inside the
    /// COMMAND arm already and must NOT block on backpressure on the very
    /// channel we're draining — a `Full` here means the command channel is
    /// saturated, which is itself a degenerate state (the bounded channel
    /// is sized for normal command-rate, not 256-K-task storms; a full
    /// channel means many continuations queued). In that pathological case
    /// we fall back to `tokio::spawn_local`ing the send so it parks until
    /// the channel drains rather than dropping the continuation.
    fn kick_spawn_continuation(&self) {
        let tx = self.command_tx.clone();
        if let Err(err) = tx.try_send(PrimaryCommand::PumpSpawnContinuation) {
            match err {
                tokio_mpsc::error::TrySendError::Full(cmd) => {
                    // Park the send on the LocalSet — preserves the kick
                    // exactly, avoids dropping the continuation.
                    tokio::task::spawn_local(async move {
                        // A closed channel here is the same teardown signal
                        // every other command sender sees; swallow it
                        // (the coordinator is winding down — no continuation
                        // can fire anyway).
                        let _ = tx.send(cmd).await;
                    });
                }
                tokio_mpsc::error::TrySendError::Closed(_) => {
                    // Coordinator is winding down — drop the kick (the
                    // queue's continuations have already-dropped reply
                    // oneshots and never fire from a closed-channel state).
                }
            }
        }
    }

    /// Apply one chunk of a (possibly chunked) `SpawnTasks` input. Returns
    /// per-index `SpawnError` entries with their indices TRANSLATED into the
    /// caller's ABSOLUTE input index (`chunk[i]` becomes index
    /// `base_index + i`) so the caller's reply oneshot is consistent across
    /// chunked and non-chunked apply paths.
    ///
    /// The orchestrator pre-checks within-batch hash uniqueness on the
    /// FULL input before chunking, so this helper never sees
    /// `SpawnError::DuplicateInBatch` from its own input — any in-batch
    /// dupe would have aborted the run BEFORE reaching here. The
    /// `DuplicateTaskHash` (already-in-ledger) class IS valid per-chunk
    /// (a chunked input can re-introduce an identity prior chunks already
    /// added) — the validator's `task_present` closure consults
    /// `cluster_state.contains_task`, which sees the prior chunks'
    /// applied entries, exactly as a single-shot run-all-N would.
    async fn apply_spawn_tasks_chunk(
        &mut self,
        tasks: Vec<TaskInfo<I>>,
        base_index: usize,
    ) -> Vec<(usize, SpawnError)> {
        // Snapshot the per-index `task_id`s BEFORE the validator moves
        // `tasks`, so the run-level loud-fail backstop below can name the
        // rejected identities. The validator's `errors` carry the index
        // into THIS vec, so the index resolves a `task_id` here.
        let task_ids_by_index: Vec<String> = tasks.iter().map(|t| t.task_id.clone()).collect();
        // Shared validator: pure read against `cluster_state` —
        // mirrored on the promoted-secondary path
        // (`SecondaryCoordinator::apply_spawn_tasks`) AND on the local
        // manager's command-channel handler so the duplicate-hash +
        // unknown-dep rules can't drift between the three apply sites.
        // The closure-based signature lets the shared helper live in
        // `dynrunner-core` (no `ClusterState` dependency); each backend
        // supplies two closures that probe its own ledger shape.
        // Snapshot the pool's phase-state map BEFORE building the
        // validator closures — the validator's third closure
        // (`phase_accepts_runtime_spawn`) needs a `&pool` view that
        // doesn't keep the live `&mut self` borrow alive through the
        // validator call, since the `tasks.iter()` snapshot above and
        // the cluster_state probes inside the closures conflict with
        // an overlapping `self.pool()` borrow. The composed gate is:
        //   accept ⇔ phase is no-barrier (`PhaseSpec.barrier=False`)
        //            OR every dep of phase has finished its work
        //              (state ∈ {`Drained`, `Done`}).
        // The `Drained|Done` half accepts the LEGITIMATE on_phase_end
        // -> spawn_tasks idiom (the producer / lazy-spawn pattern):
        // the upstream fires `on_phase_end` AT the drain edge, BEFORE
        // `mark_phase_done`, so its state is `Drained` (not yet
        // `Done`) and the spawn into the downstream-but-still-
        // `Blocked` phase races the about-to-fire activation. A
        // simpler `phase_state(Y) != Blocked` test would
        // reject these legitimate spawns; walking Y's deps and
        // accepting when every upstream has reached its drain edge
        // captures the right semantic — the upstream's work is
        // genuinely done, only the lifecycle-cascade hasn't run yet.
        // The interlock still rejects the genuine BARRIER violation
        // (any dep still `Active`/`Draining`/`Blocked` — actual live
        // work upstream), which barrier=False is the consumer's
        // opt-in to authorize.
        // The two halves are joined here in one place — single source
        // of truth for the runtime-spawn gate, mirrored by the
        // promoted-secondary's `apply_spawn_tasks` site below.
        let phase_states: std::collections::HashMap<_, _> = self
            .pool()
            .phase_state_iter()
            .map(|(p, s)| (p.clone(), s))
            .collect();
        let phase_deps_for_check: std::collections::HashMap<_, _> =
            self.cluster_state.phase_deps().clone();
        let phase_no_barrier: std::collections::HashSet<dynrunner_core::PhaseId> =
            self.cluster_state.phase_no_barrier_set().clone();
        let (valid_tasks, errors) = validate_spawn_tasks(
            // Duplicate detection over the LOGICAL ledger: a SETTLED hash
            // is a present task — re-spawning it must be rejected exactly
            // like a fat duplicate.
            |hash| self.cluster_state.contains_task(hash),
            // Phase-aware dep resolution: a dep names a full
            // `(phase_id, task_id)`. Reuse the SAME `task_hash_for_dep`
            // lookup `apply_tasks_spawned` uses, so the pre-validator and
            // the apply rule agree — a dep that resolves to no ledger
            // entry for its NAMED phase is `UnknownDependency` here
            // rather than silently landing Pending never-runnable.
            |phase_id, task_id| {
                self.cluster_state
                    .task_hash_for_dep(phase_id, task_id)
                    .is_some()
            },
            // Runtime-spawn barrier interlock: the target phase must
            // either be in the declared no-barrier set (the consumer's
            // explicit `PhaseSpec.barrier=False` opt-in) OR have every
            // upstream dep at-or-past its drain edge
            // (`phase_state ∈ {Drained, Done}` — the upstream's work
            // is genuinely done, only the lifecycle-cascade may not
            // have flipped this phase from `Blocked` to `Active` yet).
            // The latter accepts the LEGITIMATE on_phase_end -> spawn
            // idiom; the rejection is the genuine BARRIER violation
            // (an upstream still in `Active`/`Draining`/`Blocked`,
            // i.e. real live work in flight). An unknown phase id
            // (defensive — the validator's unknown-dep / unknown-phase
            // machinery owns that class separately) treats the gate
            // as open here.
            |phase_id: &dynrunner_core::PhaseId| {
                use dynrunner_scheduler_api::pending_pool::PhaseState;
                if phase_no_barrier.contains(phase_id) {
                    return true;
                }
                // Walk this phase's deps; accept iff every dep has
                // reached drain (state ∈ {Drained, Done}). An undeclared
                // phase (no entry in `phase_deps_for_check` and no
                // entry in `phase_states`) treats the gate as open —
                // the validator's other classes own that diagnosis.
                let deps = match phase_deps_for_check.get(phase_id) {
                    Some(d) => d,
                    None => return true,
                };
                deps.iter().all(|dep| {
                    phase_states
                        .get(dep)
                        .map(|s| matches!(s, PhaseState::Drained | PhaseState::Done))
                        .unwrap_or(true)
                })
            },
            tasks,
        );

        // #3b WITHIN-BATCH dupe class is detected ONCE by the orchestrator
        // before chunking via `detect_in_batch_dupes`, so the per-chunk
        // validator never returns `SpawnError::DuplicateInBatch` here. The
        // assertion documents the invariant; in release builds it is a
        // silent NoOp.
        debug_assert!(
            !errors
                .iter()
                .any(|(_, e)| matches!(e, SpawnError::DuplicateInBatch(_))),
            "apply_spawn_tasks_chunk must never see a DuplicateInBatch — \
             apply_spawn_tasks pre-checks the full input before chunking"
        );

        // Translate per-chunk error indices INTO ABSOLUTE input-index
        // entries (chunk[i] is the caller's input[base_index + i]) so the
        // reply oneshot is consistent across the single-shot and chunked
        // paths. A non-chunked call passes `base_index = 0`, byte-identical
        // to the pre-#547 result.
        let errors: Vec<(usize, SpawnError)> = errors
            .into_iter()
            .map(|(idx, err)| (idx + base_index, err))
            .collect();

        if valid_tasks.is_empty() {
            // No mutation to broadcast; the per-index errors are the
            // entire result. Skip the apply+broadcast pass so we
            // don't emit an empty-batch wire event.
            //
            // Loud-fail backstop: a NON-EMPTY spawn batch whose every
            // task the validator REJECTED nets the phase ZERO dispatch.
            // Without recording it, `total_tasks` is never refreshed,
            // `run_complete_check`'s counter exit trips against the pre-
            // spawn total, and the run exits rc=0 with that planned work
            // silently dropped (the producer-path silent total=0). Record
            // the rejected identities so `run()`'s final accounting
            // surfaces a loud `RunError::SpawnRejected` instead of a clean
            // exit. Scoped to the all-rejected case (`valid_tasks.is_empty()`
            // AND `!errors.is_empty()`): an empty INPUT batch is a benign
            // no-op (nothing to dispatch, nothing rejected), and a PARTIAL
            // rejection still dispatches its survivors below — the per-index
            // `errors` already inform the caller of the dropped ones. The
            // per-index reply the caller receives is UNCHANGED.
            //
            // EXCLUDE `DuplicateTaskHash` (already-in-ledger) rejections: an
            // idempotent re-spawn (the FAILOVER-replay case — a promoted
            // primary's `on_phase_end` hook re-firing) drops NO work. The
            // prior ledger entries ARE the authoritative copies; they
            // dispatch + complete on their own and are counted in
            // `total_tasks`. Recording them here would surface a spurious
            // `RunError::SpawnRejected` for work that is not lost — the same
            // LMU-gating class as the run-wide-invalidation path above, just
            // one branch deeper (an all-duplicate replay batch nets
            // `valid_tasks.is_empty()`). A genuinely-lost rejection
            // (`UnknownDependency`) IS still recorded.
            let lost: Vec<&(usize, SpawnError)> = errors
                .iter()
                .filter(|(_, e)| !matches!(e, SpawnError::DuplicateTaskHash(_)))
                .collect();
            if !lost.is_empty() {
                // `errors` carries ABSOLUTE input indices (translated by
                // `+ base_index` above); `task_ids_by_index` is CHUNK-LOCAL
                // (built before the validator moved `tasks` — i.e. the
                // chunk's `Vec`, not the caller's original input). Subtract
                // `base_index` to land back in the chunk's local index
                // space the per-chunk `task_ids_by_index` is keyed by.
                self.spawn_rejected_task_ids.extend(
                    lost.iter()
                        .map(|(idx, _)| task_ids_by_index[*idx - base_index].clone()),
                );
            }
            return errors;
        }

        // Compute hashes of the valid subset so we can post-apply
        // inspect each entry's CRDT state to decide pool-side
        // bookkeeping. The hash function is deterministic; the
        // apply rule recomputes the same value internally, so the
        // hashes here line up with cluster_state's HashMap keys.
        let valid_hashes: Vec<String> = valid_tasks
            .iter()
            .map(crate::primary::wire::compute_task_hash)
            .collect();

        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TasksSpawned {
            tasks: valid_tasks,
        }])
        .await;

        // Symmetric with the receive-side mirror in
        // `handle_cluster_mutation` (primary/task/mutation.rs): every
        // path that grows the CRDT ledger via TasksSpawned must
        // refresh the operational-loop's exit-counter denominator from
        // the post-apply CRDT view. The CRDT is authoritative;
        // `total_tasks` is a derived view that mirrors
        // `cluster_state.task_count()`. Without this refresh the
        // live-primary's exit check (`completed + failed >=
        // total_tasks`) trips against the pre-spawn total the moment
        // every pre-spawn task terminates — the asm-tokenizer phase-3
        // memmap race where `on_phase_end("unify_vocab")` issues
        // `spawn_tasks(memmap_items)`, the CRDT grows, the pool
        // accepts the reinject below, but the loop exits before the
        // post-spawn task dispatches because `total_tasks` still
        // reads its run-start value.
        //
        // Idempotent against the no-spawn-grew case: the early-return
        // on `valid_tasks.is_empty()` above means we only reach here
        // when the apply actually grew the ledger; even if it didn't,
        // re-reading `task_count()` is a same-value write.
        self.total_tasks = self.cluster_state.task_count();

        // Pool-side bookkeeping for the live primary. Read every
        // valid entry's post-apply state and route by classification:
        //   * Pending → reinject into the pool so the next dispatch
        //     tick picks it up. `reinject` is the right primitive
        //     here (vs `extend`): the pool's dep-tracking is the
        //     CRDT's concern post-Phase-B, the pool just dispatches
        //     what's in it.
        //   * Blocked → CRDT auto-resume on a later `TaskCompleted`
        //     fires `resume_blocked_on`; the existing
        //     `apply_and_broadcast_cluster_mutations` plumb
        //     re-injects via `resumed_for_dispatch`. No pool action
        //     here.
        //   * Failed → record in the in-pass `failed_tasks` ledger so
        //     accounting matches the wire-side state. Same shape
        //     `apply_fail_permanent` produces for the legacy
        //     cascade-fail path.
        let mut pool_grew = false;
        for hash in valid_hashes {
            match self.cluster_state.task_state(&hash) {
                Some(state @ TaskState::Pending { .. }) => {
                    // L5: resolve dep refs via the store.
                    let task = self.cluster_state.task_to_info(state);
                    self.pool_mut().reinject(std::sync::Arc::new(task));
                    pool_grew = true;
                }
                Some(TaskState::Failed { kind, .. }) => {
                    self.failed_tasks.insert(hash, kind.clone());
                }
                _ => {
                    // Blocked / other states: no pool-side action.
                }
            }
        }

        // If any spawned task entered the pool as Pending, that's a
        // pool-entry edge — EMIT a `TasksAdded` so the worker-management
        // recheck dispatches it. A callback that issues `spawn_tasks`
        // (e.g. an `on_phase_end` spawning the next phase's items) needs
        // free workers nudged: they already got "no work" and won't
        // re-poll. Decoupled emit, never a direct dispatch call (the
        // dispatch-decoupling law). Blocked-only spawns make no demand
        // until a prereq completes (which itself emits `TasksAdded`).
        if pool_grew {
            self.cluster_state
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
        }

        errors
    }
}

/// Detect within-batch `(phase_id, task_id)` duplicates ONCE on the full
/// `apply_spawn_tasks` input — the pre-chunk gate that lets the per-chunk
/// validator never see `SpawnError::DuplicateInBatch`. The classification
/// rules (the "first occurrence is the winner, second is the loser") match
/// `validate_spawn_tasks`'s within-batch arm exactly so chunked vs
/// non-chunked dispatch produces byte-identical per-index errors. Returns
/// the per-loser entries with their absolute input indices + the duplicated
/// hash, for the caller to thread into both `invalidate_all_pending`'s
/// reason string and the per-index reply.
fn detect_in_batch_dupes<I: Identifier>(tasks: &[TaskInfo<I>]) -> Vec<(usize, String)> {
    let mut seen: std::collections::HashMap<(dynrunner_core::PhaseId, String), String> =
        std::collections::HashMap::with_capacity(tasks.len());
    let mut out: Vec<(usize, String)> = Vec::new();
    for (idx, task) in tasks.iter().enumerate() {
        let identity = (task.phase_id.clone(), task.task_id.clone());
        // The hash identifies the TASK (full content hash); the
        // within-batch dupe is a same-`(phase_id, task_id)`-pair, second
        // arrival. The FIRST insertion records the hash for the loser
        // entry; the second `entry` lookup returns Occupied and the loser
        // is recorded against its (chunk-local) absolute input index.
        let hash = crate::primary::wire::compute_task_hash(task);
        match seen.entry(identity) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(hash);
            }
            std::collections::hash_map::Entry::Occupied(o) => {
                out.push((idx, o.get().clone()));
            }
        }
    }
    out
}
