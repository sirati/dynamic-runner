//! The `ClusterState<I>` struct, its trait impls, and minimal
//! constructors.
//!
//! Single concern: storage shape + identity. Field semantics
//! (clone-skip, snapshot-skip, dispatcher-channel forward contracts)
//! are documented inline on each field; the behavior that reads or
//! mutates the fields lives in sibling sub-modules (`accessors`,
//! `apply`, `apply_peer`, `apply_tasks`, `events`, `snapshot`,
//! `broadcast`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dynrunner_core::{Identifier, PhaseId, TaskOutputs, TerminalOutcomeCounts};
use dynrunner_protocol_primary_secondary::{
    DiscoveryDebt, RoleTable, SecondaryCapacityRecord, SecondaryResourceSampleRecord,
};

use crate::fulfillability_matcher::MatcherTriggerEvent;
use crate::peer_lifecycle::PeerLifecycleEvent;
use crate::task_completed::TaskCompletedEvent;
use crate::custom_message_outcome::CustomMessageOutcomeEvent;
use crate::task_state_change::TaskStateChangeEvent;
use crate::worker_signal::WorkerMgmtSignal;

use crate::primary::retry_bucket::BucketKind;

use super::types::{CapabilityEntry, PeerEntry, PhaseTally, RoleChangeHook, TaskState};

/// The replicated cluster-state CRDT.
pub struct ClusterState<I> {
    pub(super) tasks: HashMap<String, TaskState<I>>,
    pub(super) current_primary: Option<String>,
    pub(super) primary_epoch: u64,
    /// Lock-free mirror of `primary_epoch` exposed to off-`apply`
    /// readers (e.g. the observer's resource-holdings announcer task
    /// — see [`crate::observer::announcer::run_observer_announcer`]).
    /// Written synchronously by the `apply` path (and `restore`)
    /// **before** `fire_role_change_hooks` runs, so any hook
    /// observer that reads the mirror in response to a role-change
    /// notification sees the post-mutation value.
    ///
    /// Cloned (cheap — `Arc::clone`) on `Clone` rather than reset:
    /// unlike `role_change_hooks` / `peer_state`, the mirror has no
    /// runtime-handle semantics (it's an atomic counter, not a
    /// channel sender), and snapshot-restore paths overwrite the
    /// value to match the restored `primary_epoch` anyway, so
    /// preserving the Arc is consistent with the field's read-only
    /// downstream consumer contract.
    pub(super) primary_epoch_mirror: Arc<std::sync::atomic::AtomicU64>,
    /// Per-run static phase dependency graph. Set once at run start
    /// via `ClusterMutation::PhaseDepsSet` (originated by the primary,
    /// applied on every node) and never overwritten — the deps are
    /// derived from the consumer's `TaskDefinition` declaration and
    /// don't change for the duration of a run.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// Per-run static set of phases the consumer declared `may_be_empty`
    /// (`PhaseSpec.may_be_empty`). Set once at run start via
    /// `ClusterMutation::PhaseMayBeEmptySet`, paired with `phase_deps`
    /// (same static-graph lifecycle, originated by the primary, applied on
    /// every node). Read by the empty-drain proceed-or-fail policy
    /// (`PrimaryCoordinator::phase_can_proceed`): a non-leaf phase that
    /// drained with zero dispatched items proceeds (instead of failing
    /// loud) iff it is in this set — the explicit opt-out for an
    /// intentional pure-sequencing gate. Empty on the common no-opt-out
    /// run.
    pub(super) phase_may_be_empty: std::collections::HashSet<PhaseId>,
    /// Per-run static set of phases the consumer declared
    /// `PhaseSpec.barrier=False` — the explicit pipelined-edge opt-in.
    /// Set once at run start via `ClusterMutation::PhaseNoBarrierSet`,
    /// paired with `phase_deps` (same static-graph lifecycle, originated
    /// by the primary, applied on every node). Read by the runtime-spawn
    /// barrier-violation interlock in `apply_spawn_tasks` (primary +
    /// promoted-secondary): a target phase accepts runtime spawn iff it
    /// has already started OR it is in this set. Mirrored on the pool's
    /// initial-state assignment (`set_no_barrier_phases`) so a no-barrier
    /// phase starts `Active` rather than `Blocked`. Empty on the common
    /// strict-barrier run.
    pub(super) phase_no_barrier: std::collections::HashSet<PhaseId>,
    /// Per-run static respawn-policy CAPS. Set once at run start via
    /// `ClusterMutation::RespawnPolicySet` (originated by the submitter
    /// primary in the same seed batch as `PhaseDepsSet`, same
    /// run-constant lifecycle) IFF `--respawn-policy` was enabled; `None`
    /// for the run's lifetime otherwise. Read by the promoted primary's
    /// hydrate to re-arm the respawn DECISION pipeline after failover /
    /// relocation (the sibling `respawn_events` ledger replicates the
    /// budget's SPEND; this replicates its CAPS). First-write-wins on
    /// apply and restore — mirrors `phase_may_be_empty`.
    pub(super) respawn_policy: Option<super::types::ReplicatedRespawnPolicy>,
    /// Set by `ClusterMutation::RunComplete`. Sticky monotonic flag —
    /// once true, the run is over and every node should drain and
    /// exit. Read by the secondary's operational loop to break out
    /// even when peers haven't disconnected.
    pub(super) run_complete: bool,
    /// Set by `ClusterMutation::RunAborted { reason }`. The failure
    /// twin of `run_complete`: sticky monotonic — once `Some`, the run
    /// has been aborted cluster-wide and every node should exit
    /// non-zero. `None` until the first abort lands. The secondary's
    /// `process_tasks` loop checks this BEFORE the `run_complete` break
    /// and returns `RunOutcome::Terminal` (projecting to
    /// `SecondaryTerminal::Aborted`); the `mesh_watchdog` disarms
    /// on it too (failover has nothing left to guard once the run is
    /// aborting). Carries the abort reason for the PyO3-boundary log.
    pub(super) run_aborted: Option<String>,
    /// The primary's FINALIZED per-class outcome partition, carried ON the
    /// terminal verdict (`RunComplete` / `RunAborted`) and latched here
    /// set-once together with the latch above — the verdict's COUNT payload.
    /// Sticky monotonic exactly like `run_aborted`: the FIRST verdict's
    /// counts win (`Option`, never overwritten once `Some`), so the latch and
    /// the counts converge to every replica ATOMICALLY (one mutation). The
    /// narrator — on the primary AND on a zero-authority observer — reads
    /// THESE counts for its terminal summary instead of re-folding its own
    /// (possibly unconverged) ledger mirror: observing the verdict means its
    /// counts are in hand. `None` until the first terminal verdict lands.
    pub(super) terminal_outcome: Option<TerminalOutcomeCounts>,
    /// Set by `ClusterMutation::GracefulAbortRequested`. The dispatch
    /// FREEZE latch — sticky monotonic, like `run_complete`: once true,
    /// no new work may leave the ready pool toward a worker anywhere in
    /// the cluster; in-flight tasks run to completion, each secondary
    /// tears down as its own work drains, and the run terminates with
    /// the graceful-abort verdict (`run_complete ∧ graceful_abort` —
    /// the verdict is the COMPOSITION of the two sticky facts).
    /// Originated only by the authoritative primary on an observer's
    /// `GracefulAbortRequest`; replicated (live broadcast + snapshot +
    /// AE digest) so a failover-promoted primary INHERITS the freeze
    /// and also refuses to schedule (the no-redo law). Consumed by
    /// PRIMARY decisions (the dispatch-view gate, the respawn-admission
    /// gate, the drain/relocate/terminal decisions) — the observer only
    /// derives its reported verdict from the same fact.
    pub(super) graceful_abort_requested: bool,
    /// Set by `ClusterMutation::WindDownRequested`. The PER-PEER,
    /// incarnation-scoped drain directive — the granular sibling of the
    /// fleet-wide `graceful_abort_requested` bit. Each `(secondary_id,
    /// member_gen)` pair names exactly one seated secondary incarnation
    /// the primary marked to drain its current work and gracefully depart
    /// at its next quiescence (the #467 re-admit-while-replacement-seated
    /// case: the redundant replacement winds down so its SLURM job is
    /// released, while the re-admitted original and the rest of the run
    /// continue). Grow-only set (join = union), like the respawn-event
    /// ledger; the carried `member_gen` is the arbiter so a stale
    /// directive can never re-target a higher incarnation of the same id.
    /// Replicated (live broadcast + snapshot + AE digest) so it survives a
    /// primary failover and reaches the directed secondary on any mesh
    /// hop. Consumed by a SECONDARY decision (its own graceful-drain exit
    /// gate in `process_tasks`) — the matching `(id, gen)` is the
    /// directed node's cue to self-depart once `active_tasks` is empty.
    pub(super) wind_down_requested: HashSet<(String, u64)>,
    /// Replicated "this run still owes discovery" fact (V6). Declared by
    /// the mode-2 seed BEFORE relocation; settled exactly once by the
    /// compute-peer primary's discover-on-promotion driver after it has
    /// originated its discovery seed (TaskAdded batch, or the empty-corpus
    /// RunComplete). Sticky-monotone THREE-state lattice (join = `max` over
    /// `Undeclared ⊑ Owed ⊑ Settled`): a replica only ever moves UP, never
    /// back. A run that never declares debt is `Undeclared` from t0 (the
    /// cold mode-1 path and every legacy run), which is `!= Owed`, so it is
    /// unaffected.
    pub(super) discovery_debt: DiscoveryDebt,
    /// Replicated role bookkeeping. Updated in lockstep with
    /// `current_primary` on every `PrimaryChanged` apply so the
    /// transport-layer cache (registered via `role_change_hooks`)
    /// always observes a coherent snapshot.
    pub(super) role_table: RoleTable,
    /// Hooks fired AFTER a `RoleTable` mutation. The cluster_state
    /// owns the hooks; transports register their write-through
    /// cache here at construction time. Stored as `Vec` for future-
    /// proofing — a single registrant covers today's `PeerTransport`
    /// cache use case.
    ///
    /// Skipped from `Clone` (and reset on snapshot/restore paths): a
    /// cloned `ClusterState` is conceptually a separate replica and
    /// has no transport attached, so carrying the source replica's
    /// hooks would fire a remote transport's cache from a state it
    /// does not own. Tests that need hooks on a cloned state must
    /// re-register on the clone.
    pub(super) role_change_hooks: Vec<RoleChangeHook>,
    /// Per-id LIVENESS ledger (only) maintained by the `PeerJoined` and
    /// `PeerRemoved` apply rules. Holds the alive/dead bit; the role
    /// capabilities (`is_observer` / `can_be_primary`) live in the
    /// separate replicated `capabilities` 2P-set (C6 — one source of
    /// truth, no CRD-4). The `RoleTable.observers` / `RoleTable.can_be_primary`
    /// sets are READ-TIME projections of `capabilities × this map's Alive
    /// bit` (`reproject_roles`); this map is the authoritative "have we
    /// ever seen this id, and is it currently alive or dead-forever"
    /// liveness answer, composed with — never merged into — the capability
    /// set at projection time.
    ///
    /// Skipped from `Clone` (a cloned replica is a fresh node-local view
    /// paired with the node-local `lifecycle_tx` dispatcher channel, and
    /// has no reason to inherit the source's runtime peer view).
    ///
    /// Snapshot/restore: the ALIVE subset DOES cross the wire — `snapshot`
    /// projects the `Alive` ids out of this map into
    /// `ClusterStateSnapshot::alive_members` (only the `HashSet<String>`,
    /// not the module-private `PeerEntry`), and `restore` reconstructs a
    /// fresh `Alive` `PeerEntry` for each incoming id into a VACANT slot
    /// (Dead-wins / sticky: a local `Dead` is never resurrected; absence
    /// is not read as Dead — honest-liveness). Dead ids are NOT
    /// snapshotted. The steady-state writers remain the live
    /// `PeerJoined`/`PeerRemoved` broadcasts. Liveness is INTENTIONALLY
    /// excluded from the digest (each node owns its own view); only
    /// capability converges via anti-entropy.
    pub(super) peer_state: HashMap<String, PeerEntry>,
    /// Replicated role-capability 2P-set (C6) — the SINGLE source of
    /// truth for `is_observer` / `can_be_primary`, decoupled from
    /// liveness. Keyed by peer id; each entry is `Advertised { .. } |
    /// Departed` (a tombstone written on a genuine `PeerRemoved`). Merged
    /// monotonically via `merge_capability` (apply's peer arms +
    /// restore's per-id loop) and folded into the digest
    /// (`capabilities_hash`) — the 2P-set IS snapshot-healable, so a
    /// flagged divergence is one a pull's `restore` actually heals
    /// (detect-WITH-heal). The `RoleTable.observers` /
    /// `RoleTable.can_be_primary` sets are read-time projections of this
    /// map AND the local `peer_state` Alive bit (`reproject_roles`); this
    /// map alone is replicated, the projections are node-local derived.
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, `task_outputs`). Round-trips through
    /// `snapshot`/`restore`.
    pub(super) capabilities: HashMap<String, CapabilityEntry>,
    /// Sender end of the peer-lifecycle dispatcher mpsc. Installed
    /// via [`Self::install_lifecycle_sender`] when the coordinator
    /// wires its dispatcher task; `None` while no coordinator has
    /// attached (tests that exercise the apply path in isolation
    /// observe the same `None` state and the emit becomes a silent
    /// drop). The receiver end is owned by
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`].
    ///
    /// Skipped from `Clone`, snapshot, and restore — same rationale
    /// as `role_change_hooks` and `peer_state`: a cloned replica is
    /// a fresh node-local view and inheriting the source's sender
    /// would route this replica's events into the source's
    /// dispatcher, violating the CCD-9 "apply path never crosses
    /// node boundaries" invariant.
    pub(super) lifecycle_tx: Option<tokio::sync::mpsc::UnboundedSender<PeerLifecycleEvent>>,
    /// Sender end of the fulfillability-matcher trigger mpsc. Installed
    /// via [`Self::install_matcher_trigger_sender`] when the
    /// coordinator wires its matcher pipeline; `None` while no
    /// coordinator has attached. Receiver is consumed by
    /// [`crate::fulfillability_matcher::drain_matcher_batch`] from
    /// inside the operational `select!` loop. Skipped from Clone /
    /// snapshot / restore for the same reason as `lifecycle_tx`.
    pub(super) matcher_trigger_tx: Option<tokio::sync::mpsc::UnboundedSender<MatcherTriggerEvent>>,
    /// Sender end of the worker-management signal bus mpsc. Installed
    /// via [`Self::install_worker_mgmt_sender`] when worker management
    /// wires its operational loop; `None` while nothing has attached.
    /// Receiver is consumed by
    /// [`crate::worker_signal::recv_worker_signal_batch`] from inside
    /// worker management's operational `select!` loop. Skipped from
    /// Clone / snapshot / restore for the same reason as `lifecycle_tx`.
    pub(super) worker_mgmt_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkerMgmtSignal>>,
    /// Sender end of the task-completion dispatcher mpsc. Installed
    /// via [`Self::install_task_completed_sender`] when the
    /// coordinator wires its dispatcher task; `None` while no
    /// coordinator has attached (the apply path in isolation observes
    /// the same `None` state and the emit becomes a silent drop).
    /// Receiver is owned by
    /// [`crate::task_completed::run_task_completed_dispatcher`].
    ///
    /// Skipped from `Clone`, snapshot, and restore — same rationale as
    /// `lifecycle_tx` / `matcher_trigger_tx`: a cloned replica is a
    /// fresh node-local view and inheriting the source's sender would
    /// route this replica's events into the source's dispatcher,
    /// violating the CCD-9 "apply path never crosses node boundaries"
    /// invariant.
    pub(super) task_completed_tx: Option<tokio::sync::mpsc::UnboundedSender<TaskCompletedEvent>>,
    /// Sender for the #520 per-transition narration channel. Installed
    /// via [`Self::install_task_state_change_sender`] when the OBSERVER
    /// wires its narrator; `None` everywhere else (primary / secondary
    /// never narrate, so they never install it — the emit is a silent drop
    /// for them, like every other apply-path channel with no receiver).
    /// Carries EVERY winning task transition (assign / complete / fail /
    /// non-terminal) with the holder, built at the merge join so the
    /// observer narrates it path-independently. Skipped from `Clone`,
    /// snapshot, and restore — same CCD-9 rationale as `task_completed_tx`.
    pub(super) task_state_change_tx:
        Option<tokio::sync::mpsc::UnboundedSender<TaskStateChangeEvent>>,
    /// Sender for the #570 F5 custom-message outcome narration channel.
    /// Installed via [`Self::install_custom_message_outcome_sender`]
    /// when the OBSERVER wires its narrator; `None` everywhere else
    /// (primary / secondary never narrate, so they never install it —
    /// the emit is a silent drop for them, like every other apply-path
    /// channel with no receiver). Carries the per-mutation outcome
    /// (`Handled` | `Failed { reason }`) captured at the apply site
    /// BEFORE the per-origin watermark compactor erases the
    /// Handled/Failed label, so the observer narrates the truth even
    /// though the post-compaction state cannot tell the two terminals
    /// apart (the #568 / #570 boundary). Skipped from `Clone`,
    /// snapshot, and restore — same CCD-9 rationale as
    /// `task_completed_tx`.
    pub(super) custom_message_outcome_tx:
        Option<tokio::sync::mpsc::UnboundedSender<CustomMessageOutcomeEvent>>,
    /// Per-peer set of opaque resource strings each peer announces
    /// it currently holds locally. Maintained by the
    /// `PeerResourceHoldingsUpdated` apply rule and round-tripped via
    /// `ClusterStateSnapshot::peer_holdings` so a late-joiner sees
    /// current holdings before the next per-peer announce arrives.
    /// Opaque to the CRDT: the framework does not interpret the
    /// strings; the fulfillability-matcher hook attaches meaning.
    pub(super) peer_holdings: HashMap<String, HashSet<String>>,
    /// Replicated keyed-output cache. One entry per task that has
    /// reached `Completed` and committed a non-empty `TaskOutputs`
    /// via its `TaskCompleted` mutation's `result_data` payload.
    ///
    /// Keyed by the wire-canonical CONTENT HASH (the same key as the
    /// `tasks` ledger), NOT `task_id`. The content hash folds in
    /// `phase_id`, so the same `task_id` in two different phases keys to
    /// two distinct cache entries (no cross-phase output collision). The
    /// dispatch-time predecessor assembler resolves a dep's full
    /// `(phase_id, task_id)` identity to its hash, then reads this cache
    /// by that hash (`co_present_outputs_for` / `record_task_outputs_value`
    /// both key by hash).
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, and `phase_deps` semantics). Included in
    /// `snapshot` / `restore` so a late-joiner sees every committed
    /// output set before the next live `TaskCompleted` broadcast
    /// reaches it. Populated by the `TaskCompleted` apply arm (see
    /// the `record_task_outputs` helper in `apply_tasks.rs`).
    ///
    /// A hash with no `tasks` ledger entry (a late-arriving mutation for
    /// a task this replica never saw) is skipped — there is no anchor to
    /// key the cache against. Malformed `result_data` (failed JSON decode)
    /// logs a `tracing::warn!` and stores an empty `TaskOutputs` so
    /// dependents that hard-require a key see a controlled-empty view
    /// rather than racing the cache between "populated" and "absent".
    pub(super) task_outputs: HashMap<String, TaskOutputs>,
    /// Per-secondary static capacity (worker-slot count + advertised
    /// resource amounts). Set once per secondary by the
    /// `SecondaryCapacity` apply rule (originated by the primary at the
    /// `SecondaryWelcome` accept in `primary/connect.rs`) and never
    /// overwritten — capacity is static for a secondary's lifetime in
    /// the run.
    ///
    /// Replicated CRDT data — clone preserves it (matches `tasks`,
    /// `peer_holdings`, and `task_outputs` semantics). Included in
    /// `snapshot` / `restore` so a freshly-promoted primary and late-
    /// joining observers hold the FULL per-secondary roster the moment
    /// they restore a snapshot, before any live `SecondaryCapacity`
    /// broadcast reaches them. This is the failover-correctness fix for
    /// the worker roster being 100% primary-local: a promoted primary
    /// reconstructs `alive_worker_count()` / `self.workers` from this
    /// replicated source rather than starting empty.
    pub(super) secondary_capacities: HashMap<String, SecondaryCapacityRecord>,
    /// Latest aggregated resource-sample broadcast from each compute
    /// secondary (#575) — keyed by `secondary` id, valued by the LWW
    /// [`SecondaryResourceSampleRecord`] the secondary's
    /// `SecondaryResourceSample` mutation supplied.
    ///
    /// LWW on the per-record stamp `(member_gen, emitted_at_ms)`: a
    /// strictly-greater stamp wins (the apply rule overwrites in place),
    /// equal or older is a NoOp. Member-gen takes precedence so a
    /// respawned member's first aggregate dominates whatever stale
    /// record the dead incarnation left; emit-time breaks ties within
    /// one membership.
    ///
    /// Replicated CRDT data — clone preserves it; included in
    /// snapshot/restore so a freshly-promoted primary and late-joining
    /// observers hold the latest resource picture per secondary. Anti-
    /// entropy-reconciled (see `digest.rs`).
    ///
    /// CONSUMED BY: the observer's important-update reporter — the
    /// projection averages each field across `alive_secondary_members()`
    /// and applies the per-field 25% inclusion threshold (#575). The
    /// primary NEVER reads it for a scheduling decision; resource stats
    /// are observability-only.
    pub(super) latest_resource_samples: HashMap<String, SecondaryResourceSampleRecord>,
    /// Node-local per-task monotone "next seq" counter — the originator's
    /// half of the `TaskVersion` stamp. The originating primary (or a
    /// promoted secondary holding a `ClusterState`) bumps this at the
    /// version-stamp choke point (`broadcast::stamp_versions`) to mint a
    /// strictly-increasing `(primary_epoch, seq)` per hash.
    ///
    /// NOT replicated: it is the local originator's counter, not part of
    /// the converged ledger — skipped from `Clone`, snapshot, restore, and
    /// the digest (classified node-local in the exhaustive-destructure
    /// guards, like the dispatcher senders). A replica that never
    /// originates a mutation for a hash never reads it; a freshly-promoted
    /// primary cold-starts the counter, and `next_task_version` mints
    /// against the (already-advanced) `primary_epoch`, so a post-promotion
    /// stamp still strictly exceeds every pre-promotion version.
    pub(super) task_seq: HashMap<String, u32>,
    /// Replicated per-phase EVENT tallies (F4) — grow-only MAX of a
    /// monotone event count, keyed by `(PhaseId, PhaseTally)`. Replaces the
    /// two node-local `phase_completed` / `phase_failed` maps the
    /// coordinator held. EVENT-shaped: a fail → reinject → succeed task
    /// increments BOTH `Failed` and `Completed` (each terminal observation
    /// is one event), so this is NOT a projection of the single terminal
    /// `TaskState`. The live primary bumps it on every `note_item_*`; the
    /// snapshot / AE path replicates it, so a promoted primary reports the
    /// SAME event-shaped `on_phase_end(p, completed, failed, ..)` numbers —
    /// the consumer-hook contract. (The proceed-or-fail decision itself is
    /// ledger-derived via `phase_rollups`, not tally-derived; these tallies
    /// feed ONLY the `on_phase_end` hook now.)
    ///
    /// Merge: grow-only MAX (see `grow_max.rs`); converges under per-key
    /// `max`; never LWW, never decrement. Replicated via snapshot + AE
    /// digest, NOT a new `ClusterMutation` variant.
    pub(super) phase_event_tallies: HashMap<(PhaseId, PhaseTally), u32>,
    /// Replicated per-(phase, bucket) retry-pass USED counter (P3) —
    /// grow-only MAX of a monotone used count, keyed by
    /// `(PhaseId, BucketKind)`. Replaces the node-local
    /// `PrimaryCoordinator::retry_passes_used`. It already counts UP (the
    /// retry-bucket core returns the new used count and the async caller
    /// bumps it here), so MAX-merge is exactly right. The budget check
    /// (`used >= max_passes`) reads this; a promoted primary inherits the
    /// used-count via max-merge from the restored snapshot so the budget is
    /// NOT re-granted on failover.
    ///
    /// Merge: grow-only MAX (see `grow_max.rs`). Replicated via snapshot +
    /// AE digest.
    pub(super) retry_passes_used: HashMap<(PhaseId, BucketKind), u32>,
    /// Replicated per-hash unfulfillable-reinject USED counter (P3) —
    /// grow-only MAX of a monotone used count, keyed by task hash. Replaces
    /// the node-local DECREMENTING `unfulfillable_reinject_remaining`. The
    /// reinject handler derives `remaining = cap − used_for(hash)` LOCALLY,
    /// refuses when `remaining == 0`, and bumps the used count on a
    /// successful reinject; when the cap is `None` (unbounded) no used
    /// counter is originated (there is no cap to enforce). A promoted
    /// primary inherits the used-count so the reinject budget is NOT
    /// re-granted on failover.
    ///
    /// Merge: grow-only MAX (see `grow_max.rs`). Replicated via snapshot +
    /// AE digest.
    pub(super) unfulfillable_reinject_used: HashMap<String, u32>,
    /// Replicated respawn ledger (F7) — grow-only SET keyed by `new_id`
    /// (the minted replacement secondary id, globally unique per accepted
    /// event), value `RespawnEventRecord { original_id, cause, at }`.
    /// Replaces the node-local `VecDeque<RespawnEvent>` ring the
    /// coordinator held. The respawn admission budget
    /// (`max_per_secondary` / `max_total` / `cooldown`) is a failover
    /// decision input, so the ledger `should_respawn` consults must be
    /// replicated: a promoted primary inherits the full ledger via
    /// union-merge on restore, so a just-respawned family is NOT re-granted
    /// a fresh per-secondary budget and the cooldown timer does NOT restart.
    ///
    /// UNcapped (no ring eviction): the total budget bounds growth — once
    /// `max_total` events land every further request is rejected, so the
    /// set never exceeds `max_total + in-flight` entries.
    ///
    /// Merge: grow-only SET union-by-key (see `grow_max.rs`); converges
    /// under union (a key's value is written exactly once, so shared keys
    /// never diverge); never removes a key, never mutates a value.
    /// Replicated via snapshot + AE digest, NOT a new `ClusterMutation`
    /// variant.
    pub(super) respawn_events: HashMap<String, super::types::RespawnEventRecord>,
    /// Replicated "phase ended" facts (#343) — grow-only SET of the phases
    /// whose `on_phase_end` edge COMPLETED on the authoritative primary
    /// (hook fired + hook-queued commands drained + `mark_phase_done`
    /// issued). Maintained by the `ClusterMutation::PhaseEnded` apply rule
    /// (set-insert; join = OR/union). Read by the promoted-primary no-redo
    /// decision (`hydrate_from_cluster_state`'s `seed_completed_phases`
    /// filter): a terminal-only phase is seeded straight to `Done` —
    /// suppressing a re-fire (#326) — ONLY when this set says the hook
    /// already fired; otherwise the phase flows through the live cascade
    /// and fires its FIRST `on_phase_end` (the freshly-discovered
    /// all-`SkippedAlreadyDone` phase, whose terminal-only shape exists
    /// from the moment it is seeded).
    ///
    /// Merge: grow-only SET union (never removes a phase). Replicated via
    /// the live `PhaseEnded` broadcast + snapshot + AE digest.
    pub(super) phases_ended: HashSet<PhaseId>,
    /// Replicated custom-message inbox (F5) — the IMPORTANT
    /// secondary→primary consumer messages, keyed by the per-origin
    /// `(origin, seq)` idempotency pair, each `Unhandled { topic, data }`
    /// (awaiting a `custom_message_handler` invocation on the primary)
    /// or a terminal tombstone (`Handled` / `Failed`, payload dropped).
    /// Maintained by the `CustomMessagePosted` / `CustomMessageHandled`
    /// / `CustomMessageFailed` apply rules (see `apply_custom.rs` —
    /// vacant-insert / sticky-latch lattice, Handled-wins join).
    /// Consumed by a PRIMARY decision: the handler-dispatch decision
    /// ("which messages do I still owe a handler invocation?") on both
    /// the live and the promoted primary (the promotion-replay
    /// failover-safety the feature exists for; `Failed` entries are
    /// never replayed). Replicated via the live mutation broadcasts +
    /// snapshot + AE digest. NOT grow-only: the per-origin watermark
    /// compaction physically prunes terminal tombstones (see
    /// `custom_terminal_watermarks`).
    pub(super) custom_messages: HashMap<(String, u64), super::types::CustomMsgState>,
    /// Per-origin contiguous-prefix TERMINAL watermark (F5 compaction):
    /// `origin → w` asserts every seq in `1..=w` for that origin is
    /// terminal (`Handled` or `Failed` — the label is erased) AND
    /// physically pruned from `custom_messages`. A grow-max register
    /// per origin (the house `grow_max` shape): the apply-side
    /// compaction advances it over contiguous terminal tombstones;
    /// `Posted`/`Handled`/`Failed` re-applications at `seq <= w` are
    /// NoOps by watermark check; the restore merge takes the per-origin
    /// MAX and prunes the newly-subsumed local entries. Replicated via
    /// snapshot + AE digest (no mutation of its own — it is derived
    /// from the terminal stream).
    pub(super) custom_terminal_watermarks: HashMap<String, u64>,
    /// Node-local per-peer throttle for the "PeerJoined for dead id at a
    /// non-advancing generation ignored" WARN (#416). A removed-but-alive
    /// peer redials forever; until its transport leg re-admits, EVERY
    /// authenticated frame re-applies the same non-advancing `PeerJoined`
    /// and re-trips that WARN — 45+ min untrottled in
    /// run_20260611_123632. The WARN must stay (it names a real
    /// re-admission stall) but be quiet: one per peer per
    /// [`DEAD_REJOIN_WARN_INTERVAL`], the suppressed count carried on the
    /// next emit. Keyed by peer id so distinct peers don't share a window;
    /// the `WarnThrottle` interval covers the within-episode re-emit spam.
    ///
    /// NOT replicated — a pure node-local diagnostic gate (each replica
    /// throttles its own log stream): skipped from `Clone`, snapshot,
    /// restore, and the digest (classified node-local in the
    /// exhaustive-destructure guards, like `task_seq`). A cloned /
    /// restoring replica cold-starts the
    /// throttle (its first dead-rejoin observation emits immediately).
    pub(super) dead_rejoin_warn: HashMap<String, crate::warn_throttle::WarnThrottle>,
    /// Node-local memo of the last [`digest`] over the CURRENT generation
    /// of the replicated ledger. The digest is a pure O(ledger) XOR-fold
    /// over 66k+ tasks (+ outputs, capabilities, grow-max maps); the
    /// anti-entropy receive cadence re-derived it from scratch on EVERY
    /// inbound `StateDigest` frame (two handlers on a co-located node),
    /// starving the
    /// `current_thread` runtime. [`digest`] populates this memo through
    /// `&self` (interior mutability — a `Cell`, so the read signature is
    /// unchanged and no caller learns of the memo); the mutation seams
    /// ([`invalidate_digest_cache`]) clear it. A clean read returns the
    /// memo; a cleared read recomputes once and re-populates.
    ///
    /// INVARIANT (a stale memo silently breaks anti-entropy convergence,
    /// so this is load-bearing): every `&mut self` API entry that can
    /// change a digest-folded field clears this memo. Those entries are
    /// EXACTLY [`apply_with_resumed_blocked`], [`restore_collecting_resumed`],
    /// and the four grow-max originators ([`record_phase_event_tally`],
    /// [`record_retry_pass_used`], [`record_unfulfillable_reinject_used`],
    /// [`record_respawn_event`]). Every other folded-field mutator
    /// (`apply_custom_*`, `record_task_outputs_value`, `merge_task_state`,
    /// `apply_peer_*`, …) is an internal `pub(super)` helper reachable ONLY
    /// through one of those entries, so it is transitively covered — the
    /// memo is cleared once at the entry, never double-cleared per helper.
    ///
    /// A `Cell` (not `RefCell`): [`StateDigest`] is `Copy`, so get/set are
    /// branch-free and panic-free, and `ClusterState` is single-threaded-
    /// owned (every coordinator that holds it runs on a `current_thread` /
    /// `LocalSet` runtime — see the `Rc<Cell<…>>` idiom in
    /// `secondary/setup_deadline.rs`), so no lock is needed. NOT replicated,
    /// NOT folded, NOT cloned, NOT snapshotted — a pure derivation of the
    /// replicated fields, classified node-local in every exhaustive-
    /// destructure guard.
    pub(super) digest_cache: std::cell::Cell<
        Option<dynrunner_protocol_primary_secondary::StateDigest>,
    >,
    /// Node-local counter of how many times [`digest`] ran the full fold
    /// (a memo MISS). Bumped through `&self` interior mutability inside the
    /// recompute branch; read by the digest-memo tests to pin "a cache hit
    /// does NOT recompute". Carries no convergence signal — classified
    /// node-local in every destructure guard.
    pub(super) digest_fold_count: std::cell::Cell<u64>,
    /// Node-local incremental memo of the per-bucket range fold (#492 P2),
    /// the O(1)-read twin of the O(ledger) [`Self::tasks_range_digest`]
    /// one-pass fold. Maintained INCREMENTALLY (not invalidate-and-recompute
    /// like `digest_cache`): every task-state mutation XORs the old per-entry
    /// term out and the new term in, co-located at the SAME sites that change
    /// a task's stored state, so a probe read is a cheap clone instead of the
    /// O(66k) fold that wedged the single-threaded oploop (#504).
    ///
    /// Tracks the LOGICAL ledger (fat `tasks` ∪ spilled `settled`), the same
    /// universe `tasks_range_digest` folds; a spill / unsettle MOVES an entry
    /// between the two halves without changing its term or bucket, so it is
    /// memo-neutral (exactly as it is `tasks_hash`-neutral). The invariant
    /// `XOR(memo-folds) == tasks_hash` (and `sum(memo-counts) ==
    /// tasks_count`) is pinned by `range_digest_memo_matches_fresh_fold` and
    /// asserted across every apply/settle/hydrate/promotion path.
    ///
    /// Classification: a pure DERIVATION of the replicated `tasks` + `settled`
    /// (like `digest_cache`/`settled`'s accumulator) — NOT replicated, NOT
    /// folded into the digest, NOT cloned (a cloned replica rebuilds it from
    /// its restored ledger, see `clone`), NOT snapshotted. Bound to a
    /// `_`-name in every exhaustive-destructure guard.
    pub(super) range_fold_memo: super::range_fold_memo::RangeFoldMemo,
    /// Node-local incremental reverse-index of `Blocked` dependents (#547).
    ///
    /// For every `(prereq_hash, dependent_hash)` such that the LIVE in-memory
    /// `tasks` entry under `dependent_hash` is `TaskState::Blocked { on:
    /// prereq_hash, .. }`, this map records `prereq_hash -> { …dependent_hash
    /// … }`. Maintained INCREMENTALLY by the SINGLE
    /// [`Self::set_task_state`] write seam (see `range_digest.rs`): when a
    /// state CHANGES, the OLD slot's `Blocked.on` mapping is dropped and the
    /// NEW slot's `Blocked.on` mapping (if Blocked) is inserted, so the index
    /// stays equal to a fresh scan over `self.tasks` by construction.
    ///
    /// Single concern: O(|dependents|) dependent-lookup for
    /// [`Self::resume_blocked_on`], replacing the prior O(|tasks|) full-fat-
    /// HashMap scan. The amplifier in #547's Mechanism 2 (recursive
    /// `AffineReady` apply on a large spawn batch fires N_gates ×
    /// `resume_blocked_on` synchronously inside `apply_locally_for_broadcast`)
    /// hit O(N_gates × |tasks|) ≈ 46 M HashMap iterations per spawn batch on
    /// the asm-dataset reproducer; this index makes each `resume_blocked_on`
    /// cost proportional to the actual dependents to wake, not the ledger
    /// size.
    ///
    /// Why the SETTLED half is irrelevant: `settle_eligible` rejects
    /// `Blocked` (the only settle-eligible states are terminals), so a
    /// `Blocked` entry never spills and a settled entry never re-enters
    /// `tasks` as `Blocked` (rehydrate inserts the terminal state it was
    /// settled at, and any subsequent merge that lands `Blocked` routes
    /// through `set_task_state`). The index therefore only needs to track
    /// the fat in-memory half.
    ///
    /// Classification: a pure DERIVATION of `self.tasks` (like
    /// `range_fold_memo` / `digest_cache`) — NOT replicated, NOT folded into
    /// the digest, NOT crossed over the wire. Clone copies it verbatim
    /// (the source's `tasks` is byte-identical to the clone's, so the
    /// derivation transfers), snapshot/restore re-build it through the
    /// same per-entry `set_task_state` seam every merge routes through.
    /// Bound to a `_`-name in every exhaustive-destructure guard.
    pub(super) blocked_by: HashMap<String, HashSet<String>>,
    /// Node-local incremental tally of the per-outcome terminal partition,
    /// the O(1)-read twin of the O(ledger) [`Self::outcome_counts`] double-
    /// walk (#…). Maintained INCREMENTALLY by the SINGLE
    /// [`Self::set_task_state`] write seam (see `outcome_tally.rs` +
    /// `range_digest.rs`): on a transition old→new it DECREMENTS the old
    /// state's outcome bucket (if terminal) and INCREMENTS the new state's
    /// (if terminal), via the SOLE `outcome_tally::outcome_bucket_of`
    /// classification, so the maintained tally and the `#[cfg(test)]`
    /// `outcome_counts_by_scan` full-walk oracle cannot drift.
    ///
    /// Tracks the LOGICAL terminal ledger (fat `tasks` ∪ spilled `settled`):
    /// a terminal is counted ONCE, at the `set_task_state` that made it
    /// terminal. A spill / unsettle MOVES the fat body between the two halves
    /// WITHOUT routing through `set_task_state` (it touches `self.tasks`
    /// directly), so the already-counted terminal STAYS counted while settled
    /// (its outcome class is unchanged) — tally-NEUTRAL by construction,
    /// exactly as a spill is `tasks_hash`-neutral and `range_fold_memo` /
    /// `blocked_by` are spill-neutral.
    ///
    /// Classification: a pure DERIVATION of `self.tasks` ∪ `self.settled`
    /// (like `range_fold_memo` / `blocked_by` / `digest_cache`) — NOT
    /// replicated, NOT folded into the digest, NOT crossed over the wire.
    /// Clone copies it verbatim (the source's logical ledger is the clone's,
    /// so the source's maintained tally is exactly correct for the clone);
    /// snapshot/restore re-build it through the same per-entry
    /// `set_task_state` seam every merge routes through. Bound to a `_`-name
    /// in every exhaustive-destructure guard.
    pub(super) outcome_tally: super::outcome_tally::OutcomeTally,
    /// Settled-entry disk spill: the node-local STORAGE BACKEND for the
    /// join-fixed-point slice of `tasks` (see `cluster_state::settled`).
    /// A settled entry's fat body lives in the append-only spill file;
    /// this store holds the slim index, the shared read fds, and the
    /// settled half of the digest's tasks fold. Empty (and inert) until
    /// the spill driver attaches a writer segment.
    ///
    /// Classification: the index/accumulator are pure DERIVATIONS of
    /// replicated entries (like `digest_cache`), the fds node-local
    /// runtime handles. Clone carries it READ-ONLY (`clone_read_only` —
    /// a cloned replica keeps serving settled reads through the shared
    /// `Arc<File>`, never writes the source's file); snapshot/stream
    /// serve settled entries from the FILE (`settled_record`), restore
    /// converges through the `merge_task_state` settled consult; the
    /// digest folds the accumulator (`tasks_hash_acc`).
    pub(super) settled: super::settled::SettledStore,
    /// Always-on node-local OUTPUT store: the durable disk home a
    /// completed task's `TaskOutputs` payload lands in AT COMPLETION, so
    /// the payload is never even transiently kept in the resident
    /// `task_outputs` map (the owner's zero-residence requirement). Opened
    /// at construction by the spill driver (decoupled from the lazy settle
    /// sweep); the `TaskCompleted` apply writes through-then-drops on a
    /// reader node and fold-only-drops on a non-reader (secondary).
    ///
    /// Classification: the index/accumulator are pure DERIVATIONS of the
    /// replicated `TaskCompleted` stream (like `settled`), the fds
    /// node-local runtime handles. Clone carries it READ-ONLY (matches
    /// `settled`); the digest folds the accumulator (`outputs_hash_acc`);
    /// reads route through `outputs_for_hash`. Node-local in every
    /// exhaustive-destructure guard.
    pub(super) output_store: super::output_store::OutputStore,
    /// Frozen task-definition registry: the content-addressed,
    /// REPLICATED store of the IMMUTABLE core of every task's
    /// `TaskInfo` (`super::task_def_store::TaskDefStore`). A def is
    /// deduplicated by the same content hash the `tasks` ledger keys on,
    /// so the registry converges by construction (equal content ⇒ equal
    /// hash ⇒ same id on every node).
    ///
    /// Classification: REPLICATED state like `tasks` — Clone carries it
    /// FULLY (a content-addressed registry is the same on every node, and
    /// the `Arc` clones are cheap). NOT folded into the anti-entropy
    /// digest: a def's content is already implied by the `tasks` fold
    /// through the content-based join key, so folding the index would
    /// double-count and diverge (see `digest.rs`). Empty until an
    /// originator interns its first def; rebuilt on restore from the
    /// self-describing inline def each `TaskState` carries by value
    /// (`register_restored_def` per task — there is no separate def-transfer
    /// head/stream field; the def rides its state).
    pub(super) definitions: super::task_def_store::TaskDefStore<I>,
    /// The AF-id affine state sub-store (BOXED — one heap allocation, one
    /// pointer inline): the REPLICATED per-secondary bitvector — per
    /// `(secondary_id, affine_id)` a 2-bit completion cell
    /// (`NotDone/Queued/Failed/Done`) modelling a `SecondaryAffine` def's
    /// per-secondary state (the layer the affine SCHEDULER reads) — PLUS the
    /// node-local per-cell LWW generation stamp counter. See
    /// `cluster_state::affine_state`.
    ///
    /// BOXED because `ClusterState` is held by value across `.await` in the
    /// operational futures, so any inline growth costs stack on the (large,
    /// near-2MB) current-thread-runtime futures — the box keeps the AF-id
    /// concern's footprint to one pointer.
    ///
    /// Classification: the box carries BOTH a replicated half and a node-local
    /// half, so the destructure guards classify it as MIXED (treated replicated
    /// for Clone/snapshot/digest of its bitvector; the gen counter is reset on
    /// Clone, not snapshotted, not digest-folded — handled inside
    /// `AffineState`'s own `Clone` and the seam methods). Carried in Clone /
    /// snapshot (bitvector) / digest (bitvector) / restore (bitvector merge).
    pub(super) affine: Box<super::affine_state::AffineState>,
    /// Slurm-authoritative life-state snapshot consulted by the apply-path
    /// sticky-removal reversibility tiebreak (#546): an apply of
    /// `PeerJoined` for a peer this node already marked `Dead` at a
    /// non-advancing membership generation reverses the dead-mark when
    /// slurm itself reports the job still `Alive` — the local
    /// declaration was a false-positive from local deafness, not a real
    /// death. Without positive authoritative evidence the original
    /// sticky-removal stands.
    ///
    /// `None` until the deployment layer wires
    /// [`Self::set_authority_snapshot`] (the construction default — every
    /// test fixture and non-SLURM run reads `None`, so the tiebreak is
    /// a no-op and the original behavior is preserved). NOT replicated /
    /// cloned / snapshotted / digest-folded: a pure node-local runtime
    /// handle the apply path consults synchronously, classified
    /// node-local in every exhaustive-destructure guard.
    pub(super) authority_snapshot:
        Option<Arc<dyn crate::authority_snapshot::SlurmAuthoritativeSnapshot>>,
    /// Scoped marker: `true` only while a snapshot RESTORE / catch-up merge
    /// is in progress. Set by the RAII guard
    /// ([`super::snapshot::CatchUpRestoreGuard`]) at the head of the SOLE
    /// restore chokepoint ([`Self::restore_collecting_resumed`]) and cleared
    /// on the guard's drop, so it captures the WHOLE restore scope —
    /// including the `merge_task_state` cascade-fail recursion (which runs
    /// inside that scope and is equally catch-up). Read at the single
    /// narration-emit chokepoint ([`Self::emit_task_state_change_event`]) to
    /// stamp [`crate::task_state_change::NarrationSource`]: a transition the
    /// restore writes is `CatchUp`, a transition a live broadcast apply
    /// writes (outside any restore scope) is `LiveBroadcast`. This is the
    /// operator-facing discriminator the CRDT-path-INDEPENDENT merge seam
    /// otherwise erases — the seam stays path-independent, the marker only
    /// records which path the scope took.
    ///
    /// A `Cell<bool>` (not a bool / signature thread): the emit chokepoint
    /// reads through `&self`, and `ClusterState` is single-threaded-owned
    /// (every coordinator runs it on a `current_thread` / `LocalSet`
    /// runtime — the observer's narration drain is single-threaded), so the
    /// Cell needs no lock and the read/write are branch-free and panic-free
    /// (same idiom as `digest_cache`). Default `false` — a non-restoring
    /// node (and every test fixture) reads `LiveBroadcast`, the prior
    /// behaviour. NOT replicated / cloned / snapshotted / digest-folded: a
    /// pure node-local scope marker, classified node-local in every
    /// exhaustive-destructure guard (like `digest_cache`).
    pub(super) in_catch_up_restore: std::cell::Cell<bool>,
}

impl<I> Clone for ClusterState<I>
where
    I: Clone,
{
    fn clone(&self) -> Self {
        // Exhaustive destructure (NO `..` rest pattern) — the structural
        // completeness guard, mirroring `snapshot()`/`digest()`. A new
        // replicated field omitted here would be SILENTLY DROPPED on every
        // clone with NO compile error (the historic hazard the hand-rolled
        // builder hid); the destructure makes the omission a compile error.
        let ClusterState {
            tasks,
            current_primary,
            primary_epoch,
            primary_epoch_mirror,
            phase_deps,
            run_complete,
            run_aborted,
            terminal_outcome,
            graceful_abort_requested,
            wind_down_requested,
            discovery_debt,
            role_table,
            // Deliberately not cloned — see field doc.
            role_change_hooks: _role_change_hooks,
            // Deliberately not cloned — see field doc.
            peer_state: _peer_state,
            capabilities,
            // Deliberately not cloned — see field doc.
            lifecycle_tx: _lifecycle_tx,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            matcher_trigger_tx: _matcher_trigger_tx,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            worker_mgmt_tx: _worker_mgmt_tx,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            task_completed_tx: _task_completed_tx,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            task_state_change_tx: _task_state_change_tx,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            custom_message_outcome_tx: _custom_message_outcome_tx,
            peer_holdings,
            task_outputs,
            secondary_capacities,
            latest_resource_samples,
            // Node-local originator counter — reset on clone (a cloned
            // replica originates nothing inherited from the source).
            task_seq: _task_seq,
            phase_event_tallies,
            retry_passes_used,
            unfulfillable_reinject_used,
            respawn_events,
            respawn_policy,
            phase_may_be_empty,
            phase_no_barrier,
            phases_ended,
            custom_messages,
            custom_terminal_watermarks,
            // Node-local log-throttle — not cloned (a cloned replica
            // throttles its own log stream from a cold start).
            dead_rejoin_warn: _dead_rejoin_warn,
            // Node-local digest memo + fold counter — not cloned (a cloned
            // replica recomputes on its first digest call, from a cold
            // counter). Bound for the exhaustive-destructure guard.
            digest_cache: _digest_cache,
            digest_fold_count: _digest_fold_count,
            // Node-local range-fold memo — a pure derivation of the (cloned)
            // logical ledger, so it is copied through verbatim below: the
            // clone's `tasks` + `settled` are byte-identical to the source's,
            // so the source's maintained fold is exactly correct for the
            // clone (and a direct copy is cheaper than re-folding the ledger).
            range_fold_memo,
            // Node-local Blocked reverse-index (#547) — a pure derivation of
            // the cloned `tasks` map, so it is copied through verbatim below
            // for the same reason as `range_fold_memo`: a direct map clone is
            // cheaper than re-walking `tasks` to rebuild the index.
            blocked_by,
            // Node-local outcome tally — a pure derivation of the cloned
            // logical ledger, so it is copied through verbatim below for the
            // same reason as `range_fold_memo` / `blocked_by`: the clone's
            // `tasks` ∪ `settled` is the source's, so the source's maintained
            // partition is exactly correct for the clone.
            outcome_tally,
            // Settled store: carried READ-ONLY (index + shared read fds;
            // the writer affiliation is dropped — one-writer rule).
            settled,
            // Always-on output store: carried READ-ONLY (index + shared
            // read fd + accumulator; the append affiliation is dropped —
            // one-writer rule, same as `settled`).
            output_store,
            // Frozen task-def registry — carried FULLY (REPLICATED state
            // like `tasks`; the content-addressed registry is the same on
            // every node and the `Arc` clones are cheap).
            definitions,
            // AF-id affine state — carried via `AffineState`'s own Clone (the
            // bitvector is cloned, the node-local gen counter reset).
            affine,
            // Node-local runtime handle (slurm-authoritative life-state
            // snapshot for #546) — NOT cloned. A cloned replica is bound
            // to the same snapshot later via `set_authority_snapshot` if
            // its deployment wires one; otherwise the tiebreak stays
            // a no-op.
            authority_snapshot: _authority_snapshot,
            // Node-local scoped restore marker — NOT cloned (a cloned
            // replica is a fresh node-local view; it cold-starts the
            // marker `false`, exactly like `digest_cache`).
            in_catch_up_restore: _in_catch_up_restore,
        } = self;
        Self {
            tasks: tasks.clone(),
            current_primary: current_primary.clone(),
            primary_epoch: *primary_epoch,
            // Arc-clone is the right semantics here — see field doc.
            primary_epoch_mirror: Arc::clone(primary_epoch_mirror),
            phase_deps: phase_deps.clone(),
            // Replicated static phase-graph metadata — clone preserves it
            // (same lifecycle as `phase_deps`).
            phase_may_be_empty: phase_may_be_empty.clone(),
            // Replicated static phase-graph metadata — clone preserves it
            // (same lifecycle as `phase_deps` / `phase_may_be_empty`).
            phase_no_barrier: phase_no_barrier.clone(),
            run_complete: *run_complete,
            run_aborted: run_aborted.clone(),
            // Replicated set-once verdict-count payload — clone preserves it
            // (the sticky twin of `run_aborted`).
            terminal_outcome: *terminal_outcome,
            // Replicated sticky latch — clone preserves it (like `run_complete`).
            graceful_abort_requested: *graceful_abort_requested,
            // Replicated grow-only SET — clone preserves it (like `respawn_events`).
            wind_down_requested: wind_down_requested.clone(),
            // Replicated CRDT data — clone preserves it (like `run_complete`).
            discovery_debt: *discovery_debt,
            role_table: role_table.clone(),
            // Deliberately not cloned — see field doc.
            role_change_hooks: Vec::new(),
            // Deliberately not cloned — see field doc.
            peer_state: HashMap::new(),
            // Replicated CRDT data — clone preserves it.
            capabilities: capabilities.clone(),
            // Deliberately not cloned — see field doc.
            lifecycle_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            matcher_trigger_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            worker_mgmt_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            task_completed_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            task_state_change_tx: None,
            // Deliberately not cloned — same rationale as `lifecycle_tx`.
            custom_message_outcome_tx: None,
            // Replicated CRDT data — clone preserves it.
            peer_holdings: peer_holdings.clone(),
            // Replicated CRDT data — clone preserves it.
            task_outputs: task_outputs.clone(),
            // Replicated CRDT data — clone preserves it.
            secondary_capacities: secondary_capacities.clone(),
            // Replicated CRDT data (#575) — clone preserves it.
            latest_resource_samples: latest_resource_samples.clone(),
            // Node-local originator counter — reset on clone (a cloned
            // replica originates nothing inherited from the source).
            task_seq: HashMap::new(),
            // Replicated grow-only-MAX maps — clone preserves them.
            phase_event_tallies: phase_event_tallies.clone(),
            retry_passes_used: retry_passes_used.clone(),
            unfulfillable_reinject_used: unfulfillable_reinject_used.clone(),
            // Replicated grow-only SET (F7) — clone preserves it.
            respawn_events: respawn_events.clone(),
            // Replicated run-constant respawn caps — clone preserves it
            // (same lifecycle as `phase_may_be_empty`).
            respawn_policy: *respawn_policy,
            // Replicated grow-only SET (#343) — clone preserves it.
            phases_ended: phases_ended.clone(),
            // Replicated custom-message inbox + watermarks (F5) — clone
            // preserves them (replicated CRDT data, like `tasks`).
            custom_messages: custom_messages.clone(),
            custom_terminal_watermarks: custom_terminal_watermarks.clone(),
            // Node-local log-throttle — cold-start on the clone.
            dead_rejoin_warn: HashMap::new(),
            // Node-local digest memo — cold (the clone recomputes its
            // digest on first use, so it never inherits the source's memo).
            digest_cache: std::cell::Cell::new(None),
            digest_fold_count: std::cell::Cell::new(0),
            // Node-local range-fold memo — copied verbatim: the clone's
            // logical ledger is byte-identical to the source's, so the
            // source's maintained fold already satisfies the invariant for
            // the clone (no re-fold needed).
            range_fold_memo: range_fold_memo.clone(),
            // Node-local Blocked reverse-index — copied verbatim (same
            // rationale as `range_fold_memo`: the source's `tasks` is the
            // clone's, so the source's maintained index already satisfies
            // the invariant for the clone).
            blocked_by: blocked_by.clone(),
            // Node-local outcome tally — copied verbatim (same rationale as
            // `range_fold_memo` / `blocked_by`: the source's logical ledger
            // is the clone's, so the source's maintained partition already
            // satisfies the invariant for the clone).
            outcome_tally: outcome_tally.clone(),
            // Settled store: read-only carry — the cloned replica keeps
            // serving settled reads through the shared `Arc<File>`
            // segments but never writes the source's file.
            settled: settled.clone_read_only(),
            // Output store: read-only carry (its `Clone` drops the append
            // half and marks the clone degraded — one-writer rule), so the
            // cloned replica keeps serving output reads through the shared
            // read fd but never appends to the source's file.
            output_store: output_store.clone(),
            // Frozen task-def registry — REPLICATED, full clone (like
            // `tasks`; `Arc` clones are cheap).
            definitions: definitions.clone(),
            // AF-id affine state — `Box<AffineState>`; the inner `Clone`
            // carries the replicated bitvector and resets the node-local gen.
            affine: affine.clone(),
            // Node-local runtime handle — see field doc.
            authority_snapshot: None,
            // Node-local scoped restore marker — fresh `false` on the clone
            // (a cloned replica runs no restore until it calls one itself).
            in_catch_up_restore: std::cell::Cell::new(false),
        }
    }
}

impl<I> std::fmt::Debug for ClusterState<I>
where
    I: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Exhaustive destructure (NO `..` rest pattern) — the structural
        // completeness guard, mirroring `snapshot()`/`digest()`/`clone()`:
        // a new field omitted here is a compile error, so the Debug never
        // silently drops a field.
        let ClusterState {
            tasks,
            current_primary,
            primary_epoch,
            primary_epoch_mirror: _primary_epoch_mirror,
            phase_deps,
            phase_may_be_empty,
            phase_no_barrier,
            run_complete,
            run_aborted,
            terminal_outcome,
            graceful_abort_requested,
            wind_down_requested,
            discovery_debt,
            role_table,
            role_change_hooks,
            peer_state,
            capabilities,
            lifecycle_tx,
            matcher_trigger_tx,
            worker_mgmt_tx,
            task_completed_tx,
            task_state_change_tx,
            custom_message_outcome_tx,
            peer_holdings,
            task_outputs,
            secondary_capacities,
            latest_resource_samples,
            task_seq,
            phase_event_tallies,
            retry_passes_used,
            unfulfillable_reinject_used,
            respawn_events,
            respawn_policy,
            phases_ended,
            custom_messages,
            custom_terminal_watermarks,
            dead_rejoin_warn,
            digest_cache,
            digest_fold_count,
            range_fold_memo: _range_fold_memo,
            blocked_by,
            outcome_tally: _outcome_tally,
            settled,
            output_store,
            definitions,
            affine,
            authority_snapshot,
            in_catch_up_restore,
        } = self;
        f.debug_struct("ClusterState")
            .field("tasks", tasks)
            .field("current_primary", current_primary)
            .field("primary_epoch", primary_epoch)
            .field("phase_deps", phase_deps)
            .field("phase_may_be_empty", phase_may_be_empty)
            .field("phase_no_barrier", phase_no_barrier)
            .field("run_complete", run_complete)
            .field("run_aborted", run_aborted)
            .field("terminal_outcome", terminal_outcome)
            .field("graceful_abort_requested", graceful_abort_requested)
            .field("wind_down_requested", wind_down_requested)
            .field("discovery_debt", discovery_debt)
            .field("role_table", role_table)
            .field("role_change_hooks", &role_change_hooks.len())
            .field("peer_state", peer_state)
            .field("capabilities", capabilities)
            .field("lifecycle_tx", &lifecycle_tx.is_some())
            .field("matcher_trigger_tx", &matcher_trigger_tx.is_some())
            .field("worker_mgmt_tx", &worker_mgmt_tx.is_some())
            .field("task_completed_tx", &task_completed_tx.is_some())
            .field("task_state_change_tx", &task_state_change_tx.is_some())
            .field(
                "custom_message_outcome_tx",
                &custom_message_outcome_tx.is_some(),
            )
            .field("peer_holdings", peer_holdings)
            .field("task_outputs", &task_outputs.len())
            .field("secondary_capacities", secondary_capacities)
            .field("latest_resource_samples", &latest_resource_samples.len())
            .field("task_seq", &task_seq.len())
            .field("phase_event_tallies", &phase_event_tallies.len())
            .field("retry_passes_used", &retry_passes_used.len())
            .field(
                "unfulfillable_reinject_used",
                &unfulfillable_reinject_used.len(),
            )
            .field("respawn_events", &respawn_events.len())
            .field("respawn_policy", respawn_policy)
            .field("phases_ended", phases_ended)
            .field("custom_messages", &custom_messages.len())
            .field("custom_terminal_watermarks", &custom_terminal_watermarks.len())
            .field("dead_rejoin_warn", &dead_rejoin_warn.len())
            .field("digest_cache", &digest_cache.get().is_some())
            .field("digest_fold_count", &digest_fold_count.get())
            .field("blocked_by", &blocked_by.len())
            .field("settled", settled)
            .field("output_store", output_store)
            .field("definitions", definitions)
            .field("affine", affine)
            .field("authority_snapshot", &authority_snapshot.is_some())
            .field("in_catch_up_restore", &in_catch_up_restore.get())
            .finish()
    }
}

impl<I> Default for ClusterState<I> {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            current_primary: None,
            primary_epoch: 0,
            primary_epoch_mirror: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            phase_deps: HashMap::new(),
            phase_may_be_empty: std::collections::HashSet::new(),
            phase_no_barrier: std::collections::HashSet::new(),
            run_complete: false,
            run_aborted: None,
            terminal_outcome: None,
            graceful_abort_requested: false,
            wind_down_requested: HashSet::new(),
            discovery_debt: DiscoveryDebt::default(),
            role_table: RoleTable::default(),
            role_change_hooks: Vec::new(),
            peer_state: HashMap::new(),
            capabilities: HashMap::new(),
            lifecycle_tx: None,
            matcher_trigger_tx: None,
            worker_mgmt_tx: None,
            task_completed_tx: None,
            task_state_change_tx: None,
            custom_message_outcome_tx: None,
            peer_holdings: HashMap::new(),
            task_outputs: HashMap::new(),
            secondary_capacities: HashMap::new(),
            latest_resource_samples: HashMap::new(),
            task_seq: HashMap::new(),
            phase_event_tallies: HashMap::new(),
            retry_passes_used: HashMap::new(),
            unfulfillable_reinject_used: HashMap::new(),
            respawn_events: HashMap::new(),
            respawn_policy: None,
            phases_ended: HashSet::new(),
            custom_messages: HashMap::new(),
            custom_terminal_watermarks: HashMap::new(),
            dead_rejoin_warn: HashMap::new(),
            digest_cache: std::cell::Cell::new(None),
            digest_fold_count: std::cell::Cell::new(0),
            range_fold_memo: super::range_fold_memo::RangeFoldMemo::default(),
            blocked_by: HashMap::new(),
            outcome_tally: super::outcome_tally::OutcomeTally::default(),
            settled: super::settled::SettledStore::default(),
            output_store: super::output_store::OutputStore::default(),
            definitions: super::task_def_store::TaskDefStore::default(),
            affine: Box::new(super::affine_state::AffineState::default()),
            authority_snapshot: None,
            // Node-local scoped restore marker — `false` until a restore
            // scope arms it via the RAII guard.
            in_catch_up_restore: std::cell::Cell::new(false),
        }
    }
}

impl<I: Identifier> ClusterState<I> {
    /// Install the slurm-authoritative life-state snapshot the apply-path
    /// sticky-removal reversibility tiebreak (#546) consults. Called by
    /// [`crate::primary::PrimaryCoordinator::set_authority_snapshot`] so
    /// the apply path and the operational-loop consumers see the SAME
    /// snapshot.
    pub fn set_authority_snapshot(
        &mut self,
        snapshot: Arc<dyn crate::authority_snapshot::SlurmAuthoritativeSnapshot>,
    ) {
        self.authority_snapshot = Some(snapshot);
    }
}

impl<I: Identifier> ClusterState<I> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Total logical ledger entries: the fat in-memory map PLUS the
    /// settled (spilled) index — a settled entry is still a ledger
    /// entry, its fat body just lives on disk.
    pub fn task_count(&self) -> usize {
        self.tasks.len() + self.settled.len()
    }

    /// Count of FAT (in-memory, unsettled) entries — the observability
    /// complement of [`Self::task_count`] for the spill stats line.
    pub fn tasks_in_memory(&self) -> usize {
        self.tasks.len()
    }

    /// Split the fat (in-memory, un-spilled) task map by settle-eligibility —
    /// the honest decomposition of [`Self::tasks_in_memory`] for the spill
    /// stats line. `.0` = settle-ELIGIBLE entries still in memory (terminal
    /// join fixed-points awaiting/lagging spill — a spill-degrade signal when
    /// it grows or persists); `.1` = NOT-settle-eligible entries (Pending/
    /// InFlight/Blocked/Queued/Unfulfillable — live work in flight, a liveness
    /// backlog, normally the bulk). Their sum is [`Self::tasks_in_memory`].
    pub fn fat_task_breakdown(&self) -> (usize, usize) {
        let mut eligible = 0usize;
        let mut not_eligible = 0usize;
        // `self.tasks` is `HashMap<String, TaskState<I>>`; its values ARE the
        // `&TaskState<I>` that `settle_eligible` classifies (no record wrapper).
        for state in self.tasks.values() {
            if crate::cluster_state::settled::settle_eligible(state) {
                eligible += 1;
            } else {
                not_eligible += 1;
            }
        }
        (eligible, not_eligible)
    }

    /// Clear the node-local [`digest`](Self::digest) memo. Called at every
    /// `&mut self` API entry that can change a digest-folded field (see the
    /// `digest_cache` field doc for the exhaustive seam inventory and the
    /// "a stale memo silently breaks anti-entropy" invariant). Cheap +
    /// idempotent: a no-op-mutation (`ApplyOutcome::NoOp`) caller still
    /// clears, costing at most one extra fold on the next read — the safe
    /// trade against ever serving a stale digest (mutations are rare versus
    /// the per-inbound-frame digest reads the memo exists to spare).
    pub(super) fn invalidate_digest_cache(&mut self) {
        self.digest_cache.set(None);
    }
}
