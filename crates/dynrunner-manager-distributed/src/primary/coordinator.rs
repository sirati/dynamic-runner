use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;
use tracing::Instrument;

use dynrunner_core::{ErrorType, Identifier, PhaseId, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DiscoveryDebt};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler, WorkerBudgetInfo};
use tokio::sync::mpsc as tokio_mpsc;

use super::assignment::InitialAssignmentOutcome;
use super::command_channel::{COMMAND_CHANNEL_CAPACITY, PrimaryCommand};
use super::config::{OnCustomMessage, OnPhaseEnd, OnPhaseStart, PhaseHookRaiseLatch, PrimaryConfig};
use super::error::RunError;
use super::preferred_secondaries;
use super::respawn::{
    RespawnBudget, RespawnOutcome, SecondarySpawner, respawn_dispatcher_listener,
};

use crate::cluster_state::{ClusterState, OutcomeSummary};
use crate::state::SecondaryConnectionState;
use crate::worker_signal::WorkerMgmtSignal;

/// This module's tracing target (the default `module_path!` — the run-loop
/// exit line + the rest of the coordinator's events use it), named as a const
/// so the exit-log shape test (`tests::bringup_composition_fatal`) captures
/// exactly this module's emissions. Mirrors `secondary::setup::LOG_TARGET`.
#[cfg(test)]
pub(crate) const LOG_TARGET: &str = module_path!();

/// The single-task lifecycle typestate of a remote worker slot.
///
/// Replaces the removed `(current_task: Option<TaskInfo>, is_idle:
/// bool)` two-source-of-truth pair. The held task — its identity hash
/// included — lives INSIDE the `Assigned` variant, so a slot can never
/// be simultaneously "idle but holding a task" or "busy but holding
/// nothing": the divergence class is gone by construction.
///
/// Assignment is reachable ONLY from `Idle` (every assign site goes
/// through [`RemoteWorkerState::assign`], which `debug_assert`s the
/// pre-state and overwrites unconditionally only after the caller has
/// established idleness). A slot returns to `Idle` ONLY through a
/// terminal outcome keyed by the held `task_hash`
/// ([`PrimaryCoordinator::free_slot_on_terminal`]) — never on a bare
/// `TaskRequest`. This makes reassignment-before-terminal
/// architecturally impossible: the `task_hash` is the slot's held-task
/// IDENTITY (the ledger key), not a reorder-detector.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum SlotState<I: Identifier> {
    /// No task held; the only state from which assignment is legal.
    Idle,
    /// Holds exactly one task. `task_hash` is the canonical
    /// `compute_task_hash(&task)` identity recorded at dispatch and
    /// matched against an inbound terminal's `task_hash` before the
    /// slot frees.
    Assigned {
        task_hash: String,
        task: TaskInfo<I>,
        estimated: ResourceMap,
        /// How this slot came to hold the task — the discriminator the
        /// promoted-primary occupancy reconciliation keys on. See
        /// [`SlotProvenance`].
        provenance: SlotProvenance,
    },
}

/// How an `Assigned` slot's occupancy was established — the failover-
/// resume reconciliation discriminator.
///
/// On a routine live dispatch this primary itself sent the
/// `TaskAssignment`, so the occupancy is KNOWN-LIVE: the worker is
/// genuinely running the task and a stray `TaskRequest` for that slot
/// must NOT free it (the R1 invariant — a delayed/duplicate request is a
/// no-op on a busy slot).
///
/// On a PROMOTION the new primary reconstructs the slot from the
/// replicated `TaskState::InFlight` occupancy
/// ([`PrimaryCoordinator::reconstruct_workers_from_cluster_state`]). That
/// occupancy is a STALE GUESS, not a live observation: a survivor worker
/// may have FINISHED its pre-kill task during the primary-less election
/// window, so its completion landed on no primary and was LOST — the CRDT
/// still says `InFlight` but the worker is idle. Such a slot is
/// `Inherited`: its live occupancy is UNCONFIRMED. The worker's own
/// post-`PrimaryChanged` `TaskRequest` (driven by the secondary's
/// `repoll_idle_workers`, gated on the worker being idle) is the
/// ground-truth re-confirmation that it is idle, so a request landing on
/// an `Inherited` slot RECONCILES it: free the slot + return the task to
/// `Pending` for re-dispatch (re-run is idempotent/acceptable — the
/// alternative is a permanent deadlock). A worker that was genuinely
/// still running its inherited task never requests for that slot (its
/// secondary's pool reports it busy); when it finishes, the broadcast
/// `TaskComplete` frees the slot through the normal terminal path. Either
/// way the `Inherited` state is transient and resolved by the worker's
/// own next signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotProvenance {
    /// This primary committed the assignment via `commit_assignment`
    /// (it sent the `TaskAssignment`) — occupancy is known-live.
    Dispatched,
    /// Reconstructed from replicated `TaskState::InFlight` at hydration
    /// (a promoted primary) — occupancy is an unconfirmed CRDT guess
    /// awaiting the worker's live re-confirmation.
    Inherited,
}

/// Outcome of [`PrimaryCoordinator::reconcile_inherited_slot`] — the
/// failover-resume occupancy reconciliation's three-way verdict.
///
/// The terminal-veto arm exists because the lost-completion heuristic and
/// the late delivery of the lost terminal RACE (the run_20260610_221140
/// requeue-vs-complete incident): a terminal that already landed in the
/// replicated ledger must veto the requeue — re-queueing completed work
/// re-executes it.
pub(super) enum InheritedSlotReconcile<I> {
    /// Genuine lost-completion reconciliation: the slot was freed, the
    /// task returned to `Pending` in the pool, and this `TaskRequeued`
    /// must be broadcast in lockstep. (Boxed: `ClusterMutation` is a
    /// large enum and this variant rides a hot-path return value.)
    Requeued(Box<ClusterMutation<I>>),
    /// The replicated ledger ALREADY records a terminal for the held
    /// hash — the requeue is VETOED. Nothing was touched; the caller
    /// settles the slot/ledger/pool residue through the single
    /// CRDT-terminal settle path
    /// ([`PrimaryCoordinator::settle_local_state_on_crdt_terminal`]).
    VetoedByTerminal { task_hash: String },
    /// Not an inherited-occupancy slot (live-dispatched or idle) — left
    /// untouched.
    NotInherited,
}

impl<I: Identifier> SlotState<I> {
    fn is_idle(&self) -> bool {
        matches!(self, SlotState::Idle)
    }

    /// The held task, if any. `None` for an `Idle` slot.
    fn task(&self) -> Option<&TaskInfo<I>> {
        match self {
            SlotState::Idle => None,
            SlotState::Assigned { task, .. } => Some(task),
        }
    }

    /// The provenance of an `Assigned` slot; `None` for an `Idle` slot.
    /// The discriminator the failover-resume occupancy reconciliation
    /// reads to tell a live-dispatched slot from an unconfirmed
    /// inherited one.
    fn provenance(&self) -> Option<SlotProvenance> {
        match self {
            SlotState::Idle => None,
            SlotState::Assigned { provenance, .. } => Some(*provenance),
        }
    }

    /// The estimated resource footprint of the held task; empty when
    /// `Idle`. Feeds the scheduler's `estimated_usage` budget view.
    fn estimated(&self) -> ResourceMap {
        match self {
            SlotState::Idle => ResourceMap::new(),
            SlotState::Assigned { estimated, .. } => estimated.clone(),
        }
    }
}

/// Virtual worker tracked by the authoritative primary for each remote worker.
///
/// R1 replaces the removed `(current_task, is_idle)` pair with a single
/// [`SlotState<I>`] typestate field: assignment reachable ONLY from
/// `Idle`, the held task (and its hash) carried inside the `Assigned`
/// variant. The pair is removed here so the slot-keyed attribution it
/// enabled cannot survive the rebuild.
#[derive(Debug, Clone)]
pub(crate) struct RemoteWorkerState<I: Identifier> {
    pub(super) worker_id: u32,
    pub(super) secondary_id: String,
    pub(super) resource_budgets: ResourceMap,
    /// The slot's single-task lifecycle state. Sole source of truth
    /// for "is this worker idle?" and "what does it hold?".
    pub(super) state: SlotState<I>,
}

impl<I: Identifier> RemoteWorkerState<I> {
    /// True iff no task is held — the only state from which assignment
    /// is legal.
    pub(super) fn is_idle(&self) -> bool {
        self.state.is_idle()
    }

    /// The held task, if any.
    pub(super) fn held_task(&self) -> Option<&TaskInfo<I>> {
        self.state.task()
    }

    /// Move the slot `Idle -> Assigned` with explicit `provenance`. The
    /// slot MUST be `Idle`; the `debug_assert` makes a reassign-before-
    /// terminal bug a test-time panic, while production faithfully
    /// overwrites (the caller has already gated on idleness through the
    /// dispatch view / scheduler decision). Mirrors
    /// `WorkerHandle::assign_task`'s `take_idle().ok_or(...)` contract on
    /// the worker-process side.
    ///
    /// `provenance` is [`SlotProvenance::Dispatched`] at every live
    /// dispatch site (this primary sent the `TaskAssignment`) and
    /// [`SlotProvenance::Inherited`] only at the failover-resume occupancy
    /// crossing (reconstructed from replicated `InFlight`).
    pub(super) fn assign(
        &mut self,
        task_hash: String,
        task: TaskInfo<I>,
        estimated: ResourceMap,
        provenance: SlotProvenance,
    ) {
        debug_assert!(
            self.state.is_idle(),
            "slot assigned while not Idle (reassignment-before-terminal)"
        );
        self.state = SlotState::Assigned {
            task_hash,
            task,
            estimated,
            provenance,
        };
    }

    /// True iff the slot holds an `Inherited` (unconfirmed-occupancy)
    /// assignment reconstructed from replicated `InFlight` at promotion —
    /// the slot the failover-resume reconciliation may free on a survivor
    /// worker's live idle re-confirmation. `false` for `Idle` and for a
    /// live `Dispatched` slot (whose occupancy is known and must never be
    /// freed by a bare `TaskRequest`).
    pub(super) fn is_inherited(&self) -> bool {
        self.state.provenance() == Some(SlotProvenance::Inherited)
    }

    /// Force the slot back to `Idle`, returning the previously-held
    /// task (if any). Used by the dead-secondary requeue path (the worker
    /// is being dropped), the dispatch-send rollback, and the failover-
    /// resume occupancy reconciliation
    /// ([`PrimaryCoordinator::reconcile_inherited_slot`], which frees an
    /// unconfirmed inherited slot on the worker's live idle
    /// re-confirmation); the routine terminal path goes through
    /// [`PrimaryCoordinator::free_slot_on_terminal`] which gates on the
    /// hash.
    pub(super) fn vacate(&mut self) -> Option<TaskInfo<I>> {
        match std::mem::replace(&mut self.state, SlotState::Idle) {
            SlotState::Idle => None,
            SlotState::Assigned { task, .. } => Some(task),
        }
    }

    pub(super) fn budget_info(&self) -> WorkerBudgetInfo<I> {
        WorkerBudgetInfo {
            worker_id: self.worker_id,
            reserved_budgets: self.resource_budgets.clone(),
            actual_usage: ResourceMap::new(),
            is_idle: self.state.is_idle(),
            is_opportunistic: false,
            has_initial_assignment: !self.state.is_idle(),
            current_task: self.state.task().cloned(),
            estimated_usage: self.state.estimated(),
        }
    }
}

/// One entry in the primary's single hash-keyed in-flight ledger.
///
/// Records every task the authoritative primary believes is currently
/// executing somewhere in the cluster — whether this coordinator
/// dispatched it (a local `RemoteWorkerState` slot holds it) or
/// inherited it from the replicated `cluster_state` at hydration. In
/// BOTH cases the entry carries `local_worker_id = Some(..)`: the live
/// path records the slot's secondary-local id at `commit_assignment`,
/// and the failover-resume path now reconstructs the holding slot from
/// the replicated capacity × `TaskState::InFlight { worker }` occupancy
/// (`reconstruct_workers_from_cluster_state`) and seeds the same id.
/// Folds in and replaces the deleted `pre_owned_in_flight` two-tier
/// fallback: there is now ONE ledger, consulted BY HASH on every
/// terminal, so attribution is unambiguous regardless of dispatch
/// origin.
///
/// The holding slot is keyed by STABLE identity `(secondary_id,
/// local_worker_id)`, never by a positional `Vec` index. A positional
/// index desyncs the instant `self.workers.retain(..)` compacts the Vec
/// on a sibling secondary's death, shifting every survivor after the
/// removed group; the stable id survives compaction because a worker's
/// `local_worker_id` (its position WITHIN its own secondary's
/// contiguous group) is unaffected by removing a DIFFERENT secondary's
/// group. `free_slot_on_terminal` re-resolves the id to a live index
/// via [`PrimaryCoordinator::worker_idx_for`] on every terminal.
#[derive(Debug, Clone)]
pub(crate) struct InFlightEntry<I: Identifier> {
    /// Phase whose in-flight counter this entry holds open; the
    /// terminal cascade decrements it via `note_item_*`.
    pub(super) phase: PhaseId,
    /// Secondary the task was dispatched to (or inherited as targeting).
    /// Half of the stable `(secondary_id, local_worker_id)` holder key.
    pub(super) secondary_id: String,
    /// Secondary-local worker id of the holding slot (the wire
    /// `worker_id`, stable under Vec compaction). The other half of the
    /// stable holder key; resolved to a live `self.workers` index through
    /// [`PrimaryCoordinator::worker_idx_for`]. Always `Some(..)` on every
    /// origination path today (live dispatch via `commit_assignment`,
    /// failover resume via `seed_inflight`); the `Option` and the
    /// matching `free_slot_on_terminal` `None` arm survive as a defensive
    /// safe-no-op guard for a slot that no longer exists.
    pub(super) local_worker_id: Option<u32>,
    /// The full task — its `task_id` resolves dep edges, its `type_id`
    /// releases the per-type concurrency slot.
    pub(super) task: TaskInfo<I>,
}

/// The outcome surfaced by [`PrimaryCoordinator::run_consuming`].
///
/// Two regimes:
///
/// - [`PrimaryRunOutcome::Local`]: the submitter stayed local and ran the
///   operational loop to completion in-place. After the owned-`self`
///   `run_consuming` returns, the submitter binding is gone — so the
///   post-run counts the PyO3 boundary used to read off
///   `primary.completed_count()` travel back through this value, read off
///   this coordinator's own `cluster_state`. The `result` carries the
///   structured exit contract: `Ok(())` ⇒ exit 0, an `Err` ⇒ a non-zero
///   exit at the PyO3 boundary.
/// - [`PrimaryRunOutcome::Relocated`]: the submitter relinquished the
///   primary role at bootstrap and `run_consuming` destructured `self`
///   into an [`crate::observer::ObserverHandoff`] (a submitter can never
///   run the observer on its OWN primary — claudemd-HIGH-1). The handoff
///   is the ONLY field: the [`crate::process::Node`], NOT this coordinator
///   and NOT the PyO3 boundary, builds + runs the standalone observer from
///   it and re-sources the final counts from the observer's converged
///   ledger after `observer.run()`. This keeps the primary from owning the
///   observer's lifecycle.
pub enum PrimaryRunOutcome<I: Identifier> {
    /// The local primary ran the operational loop to completion in-place.
    /// `result` follows the in-place pipeline outcome; the counts come from
    /// the primary's own replicated ledger. The PyO3 boundary applies its
    /// legacy per-variant exit mapping (`Other`/`Ok` ⇒ swallow/exit 0).
    Local {
        result: Result<(), RunError>,
        completed: usize,
        failed: usize,
        stranded: usize,
    },
    /// The submitter relocated: it relinquished the primary role and handed
    /// off everything a freshly-spun observer needs to resume seamlessly.
    /// The `Node` builds the observer via
    /// [`crate::observer::ObserverCoordinator::from_handoff`], runs it to
    /// its terminal, and produces the final accounting — none of which is
    /// this coordinator's (now-consumed) concern.
    Relocated {
        /// Boxed: `ObserverHandoff` is a large payload (the full observer
        /// resume state); boxing keeps `PrimaryRunOutcome` small so the
        /// common `Local` variant does not carry the relocate payload's size
        /// (clippy `large_enum_variant`). The relocate path is rare, so the
        /// one allocation is negligible.
        handoff: Box<crate::observer::ObserverHandoff<I>>,
    },
}

// Manual `Debug` (not derived): the `Relocated` payload, `ObserverHandoff`,
// carries non-`Debug` live resources (the `MeshClient`/`RoleInbox` mesh ends
// + the dispatcher `JoinHandle`s), so it cannot be `Debug`. The `Local`
// variant keeps its full structural debug (a test formats it, `{outcome:?}`);
// `Relocated` prints an opaque marker — there is nothing safely printable on
// a live-handle handoff, and no caller debug-formats a relocated outcome.
impl<I: Identifier> std::fmt::Debug for PrimaryRunOutcome<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimaryRunOutcome::Local {
                result,
                completed,
                failed,
                stranded,
            } => f
                .debug_struct("PrimaryRunOutcome::Local")
                .field("result", result)
                .field("completed", completed)
                .field("failed", failed)
                .field("stranded", stranded)
                .finish(),
            PrimaryRunOutcome::Relocated { .. } => f
                .debug_struct("PrimaryRunOutcome::Relocated")
                .finish_non_exhaustive(),
        }
    }
}

/// Run-time discriminator for the bootstrap tail, derived STRUCTURALLY from
/// the [`crate::process::SeedSource`] this coordinator's `run_pipeline`
/// receives — never a stored construction-time policy, never a live-roster
/// read.
///
/// One concern: "am I a SETUP PEER (I seed the run, then hand the primary
/// role to a compute peer) or am I the PROMOTED DESTINATION (I AM the compute
/// peer that won the role; I run the operational loop in place)?". That fact
/// is EXACTLY the `SeedSource` discriminant the coordinator already receives,
/// so it carries no new state:
/// - [`crate::process::SeedSource::ColdStart`] /
///   [`crate::process::SeedSource::RelocatedSeed`] ⇒ [`Self::SetupPeer`]: this
///   node bootstrapped the run (originated the corpus or the discovery
///   marker) and MUST relocate the primary onto a compute peer (mesh-always
///   pillar 2 — uniform across the in-process mpsc mesh AND the SLURM QUIC
///   mesh; the transport behind `Mesh` is the only difference).
/// - [`crate::process::SeedSource::PromotionSnapshot`] ⇒
///   [`Self::PromotedDestination`]: this node IS the compute peer the role was
///   handed to (it inherited the converged snapshot). It runs the operational
///   loop in place and never relocates again.
///
/// Same single-discriminator discipline `SeedSource` already enforces for
/// CRDT origination, reused for the bootstrap tail — there is no second
/// source of truth and no local-vs-distributed branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootstrapRole {
    /// The run's setup peer: relocate the primary role to a compute peer at
    /// the bootstrap tail, then park (the demote hook fired by the relocate's
    /// local apply carries this coordinator out as a standalone observer).
    SetupPeer,
    /// The compute-peer destination the role was relocated/promoted onto:
    /// activate THIS node as the local primary and run the operational loop
    /// in place.
    PromotedDestination,
}

impl BootstrapRole {
    /// Derive the bootstrap role structurally from the seed the pipeline
    /// received. A `Copy` discriminant captured BEFORE the `match seed` arm
    /// consumes the payload, so the relocate-vs-operational decision keys on
    /// the typed `SeedSource` and nothing else.
    fn from_seed<I: Identifier>(seed: &crate::process::SeedSource<I>) -> Self {
        match seed {
            crate::process::SeedSource::ColdStart { .. }
            | crate::process::SeedSource::RelocatedSeed { .. } => BootstrapRole::SetupPeer,
            crate::process::SeedSource::PromotionSnapshot => BootstrapRole::PromotedDestination,
        }
    }
}

/// The primary coordinator: orchestrates work across secondaries.
///
/// Holds a [`crate::process::MeshClient`] (egress) + a
/// [`crate::process::RoleInbox`] (ingress) — its entire view of the mesh —
/// never a transport. Every primary send goes through the [`Self::send_to`]
/// egress edge, which resolves a typed `Destination` to a concrete peer-id
/// with the primary's own `node_id` as the bootstrap fallback (H1), STAMPS
/// the resolved role-bearing `Destination` on the frame, and hands it to
/// `self.client.send(..)` — a single queued send the mesh-pump (owned by
/// the [`crate::process::Node`]) drains and resolves loopback-vs-remote
/// against the live slot set. The coordinator never decides loopback
/// itself: a self-addressed `Destination` simply loopbacks at the mesh;
/// `Destination::Secondary(id)` / `Destination::All` route or fan.
/// `self.inbox.recv()` is the unified inbound — the pump has already
/// demuxed each frame to this role's slot. This mirrors the secondary /
/// observer side's collapse onto the same `MeshClient` + `RoleInbox` pair.
pub struct PrimaryCoordinator<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> {
    pub(super) config: PrimaryConfig,
    /// Egress capability: the locality-oblivious mesh send handle. Every
    /// primary send routes through [`Self::send_to`] → `self.client.send`,
    /// queued onto the mesh-pump (M4). The pump resolves loopback-vs-remote
    /// against the live slot set; a `Destination` resolving to this host's
    /// own role loopbacks in-process, all others go over the wire.
    pub(super) client: crate::process::MeshClient<I>,
    /// Ingress stream: frames the mesh-pump demuxed to this primary's slot
    /// (loopback siblings + remote peers). Drained by `self.inbox.recv()`
    /// in the operational loop's inbound arm; `None` is teardown.
    pub(super) inbox: crate::process::RoleInbox<I>,
    pub(super) scheduler: S,
    pub(super) estimator: E,

    // Secondary state
    pub(super) secondaries: HashMap<String, SecondaryConnectionState>,

    // Worker tracking (virtual workers across all secondaries)
    pub(super) workers: Vec<RemoteWorkerState<I>>,

    // Task state
    pub(super) total_tasks: usize,
    /// Number of tasks left unaccounted for at the end of the most
    /// recent `run()` call: `total - completed - failed`. Populated
    /// inside `run()` after the operational loop and the retry passes
    /// have both drained, so it reflects the final accounting that
    /// `RunError::ClusterCollapsed` carries on the wire. Zero on a
    /// clean run; `>0` on the cluster-collapse path the tokenizer hit
    /// on 2026-05-10. Reset to 0 at the start of every `run()`.
    pub(super) stranded_count: usize,
    /// Sticky per-run latch: a pre-loop [`Self::send_to`] observed the local
    /// egress receiver (the mesh-pump) dropped — i.e. THIS node is winding
    /// down and the mesh is gone. The egress-side TWIN of the operational
    /// loop's `inbox.recv() -> None` collapse criterion, set in exactly ONE
    /// place (the `client.send` `Err` arm of `send_to`) so no individual send
    /// site is special-cased. `run_pipeline`'s `PromotedDestination` arm reads
    /// it at its pre-loop gates and short-circuits into the SOLE
    /// strand-classification site (`finalize_terminal_accounting`) — the
    /// uniform pre-loop analogue of the operational loop's `break`-then-
    /// finalize — instead of letting a `send_to` `?`-escape as a raw
    /// `RunError::Other`. Set ONLY on the local-pump-gone arm; a
    /// `resolve_destination` miss (a routing-state error, NOT a collapse)
    /// never sets it. Reset to `false` at the start of every run.
    pub(super) mesh_pump_gone: bool,
    /// THE run-started discriminator for incremental setup delivery: set
    /// once THIS coordinator's run-start batch (`perform_initial_assignment`
    /// plus `send_transfer_complete`) has fired, latched in `run_pipeline`'s
    /// `PromotedDestination` arm — the only site that sequences both halves.
    /// Read by `serve_setup_on_cert_exchange` (`peer_setup.rs`) to choose the
    /// per-member serve variant: while `false` (bring-up) a cert-exchanged
    /// member is served its peer roster ONLY (the run-start halves stay on
    /// the post-connect-wait batch, so the quorum-proceed policy still
    /// governs when the run starts); once `true`, a member completing its
    /// welcome/cert-exchange has MISSED the batch (the fan-out walked the
    /// roster known at run start), so the serve sends it the FULL setup trio
    /// — without this a mid-run joiner (a respawned replacement) parks in
    /// `wait_for_setup` forever, never emits `MeshReady`, and is never
    /// assignable (the run_20260612_045106 secondary-4 zombie).
    ///
    /// Deliberately NODE-LOCAL and per-coordinator-incarnation (NOT a CRDT /
    /// inherited fact): a promoted primary inherits a mid-run ledger but must
    /// still classify members welcoming during ITS connect wait as bring-up —
    /// its own imminent batch serves them their (possibly work-carrying)
    /// run-start halves, and an early incremental trio would flip them
    /// operational so the batch's real `InitialAssignment` lands in their
    /// operational loop's drop arm. Reset to `false` at the start of every
    /// run.
    pub(super) run_start_batch_fired: bool,
    /// Per-task identities a runtime `spawn_tasks` batch could not apply
    /// because the validator rejected them (`UnknownDependency` —
    /// an `on_phase_end`-spawned task naming a `(phase_id, task_id)`
    /// prereq absent from the ledger; or `DuplicateTaskHash`). Each
    /// rejection is planned work the framework SILENTLY dropped — on the
    /// producer path a wholesale-rejected build batch nets the next phase
    /// ZERO dispatch yet every seeded task already terminated, so
    /// `run_complete_check`'s counter exit trips and the run exits rc=0
    /// with zero outputs (the asm-dataset-nix c39034f2 silent total=0).
    ///
    /// Accumulated by `apply_spawn_tasks` (the sole writer); read once by
    /// `run()`'s final accounting, which surfaces a loud
    /// `RunError::SpawnRejected` instead of the silent clean exit. Reset
    /// to empty at the start of every `run()`. The per-index `SpawnError`
    /// reply the caller already receives is UNCHANGED — this is the
    /// run-level loud-fail backstop for the case where the consumer logs
    /// those per-task errors and proceeds, masking a zero-dispatch phase.
    pub(super) spawn_rejected_task_ids: Vec<String>,
    pub(super) all_binaries: Vec<TaskInfo<I>>,
    /// Phase-aware pending pool. Lazily initialised at `run()` start so
    /// the constructor doesn't need the phase set / dependency graph;
    /// `pool_mut()` / `pool()` accessors expose it after that. `None`
    /// before `run()` is called.
    pub(super) pending: Option<PendingPool<I>>,
    /// Canonical phase dependency graph for the run, captured at
    /// `run()` start. Broadcast as `ClusterMutation::PhaseDepsSet`
    /// from `seed_cluster_state` so every node's `cluster_state.phase_deps`
    /// mirrors the same map; the post-promotion hydration on a
    /// secondary then reads it from there to rebuild its `PendingPool`
    /// with the same phase-state machine the primary used. Empty
    /// between runs.
    pub(super) phase_deps: HashMap<PhaseId, Vec<PhaseId>>,
    /// The set of phases the consumer declared `may_be_empty`
    /// (`PhaseSpec.may_be_empty`), registered before `run()` via
    /// [`Self::register_phase_may_be_empty`]. The seed originators
    /// (`originate_cold_seed` / `originate_relocated_seed`) emit it as
    /// `ClusterMutation::PhaseMayBeEmptySet` alongside `PhaseDepsSet`, so
    /// every node — including a promoted primary — sees the same empty-drain
    /// opt-out. Empty between runs and on the common no-opt-out run.
    pub(super) phase_may_be_empty_decl: std::collections::HashSet<PhaseId>,
    pub(super) completed_tasks: HashSet<String>,
    /// THE single hash-keyed in-flight ledger. Records every task the
    /// primary believes is executing in the cluster, keyed by its
    /// canonical `compute_task_hash`. Populated identically at dispatch
    /// (locally-assigned via `commit_assignment`) AND at hydration
    /// (inherited from `cluster_state` via `seed_inflight`) — both carry
    /// `local_worker_id = Some(..)` against a holding `RemoteWorkerState`
    /// slot (live-built at dispatch; failover-rebuilt from the replicated
    /// capacity × InFlight occupancy). Drained BY HASH on every terminal
    /// outcome through [`Self::free_slot_on_terminal`]. Folds in and
    /// replaces the deleted `pre_owned_in_flight` two-tier fallback —
    /// there is one ledger, so a completion is attributed unambiguously
    /// to the held task regardless of whether the dispatch was local or
    /// inherited.
    pub(super) in_flight: HashMap<String, InFlightEntry<I>>,
    /// Failed-task ledger keyed by task hash. The value carries the
    /// most-recent ErrorType so the dispatcher can report per-class
    /// failure counts (Recoverable → fail_retry, ResourceExhausted
    /// (memory) → fail_oom, NonRecoverable / non-memory exhaustion →
    /// fail_final) without re-scanning the task pool.
    ///
    /// A retry-success removes the entry; a retry-fail overwrites
    /// the ErrorType with the new failure's classification (the same
    /// retry can shift from Recoverable to ResourceExhausted etc.).
    /// At end-of-run, the entries that remain are the permanent
    /// failures; their ErrorType classification is the operator's
    /// post-mortem signal.
    pub(super) failed_tasks: HashMap<String, ErrorType>,
    // Per-phase completed/failed EVENT tallies are the replicated
    // grow-only-MAX `ClusterState::phase_event_tallies` (F4), bumped by the
    // `merge_task_state` join on every winning `TaskCompleted`/`TaskFailed`
    // apply (#358) — so EVERY mirror's tally advances with the
    // per-completion broadcast in real time and a promoted primary reports
    // the SAME event-shaped `on_phase_end` numbers (no anti-entropy lag).
    // The per-(phase, bucket) retry-pass counter is the replicated
    // `ClusterState::retry_passes_used` (P3) so the retry budget survives
    // failover. Neither is node-local on the coordinator any more.
    /// Currently in-flight count per `TypeId`, against
    /// `config.max_concurrent_per_type`. Incremented on dispatch
    /// (in both `assign_initial` and `assign_normal` paths),
    /// decremented on TaskComplete / TaskFailed. Capacity check
    /// is "current count + 1 <= cap" (next dispatch must fit
    /// after taking this slot).
    pub(super) in_flight_per_type: HashMap<dynrunner_core::TypeId, u32>,
    /// Lifecycle hooks. `None` outside the run window or when the
    /// caller didn't supply a hook.
    pub(super) on_phase_start: Option<OnPhaseStart>,
    pub(super) on_phase_end: Option<OnPhaseEnd>,
    /// Side-channel by which the [`on_phase_end`](Self::on_phase_end)
    /// closure records that the consumer's hook RAISED. The cascade
    /// reads (and clears) it immediately after firing the hook; on a
    /// recorded raise it emits
    /// [`WorkerMgmtSignal::PolicyFatalExit`] onto the worker-management
    /// bus so the run surfaces `RunError::FatalPolicyExit`. Defaults to
    /// a detached (never-read-by-anyone-else) latch shared with NO
    /// closure — the cascade's `take()` is then always `None`, a no-op —
    /// until a caller wires the SAME latch into both the closure (via the
    /// pyo3 `make_on_phase_end_with_raise_latch`) and this field (via
    /// [`Self::set_phase_hook_raise_latch`]). Callers that build their
    /// closure against a detached latch (the local manager, the
    /// secondary, tests) leave this default in place and keep the legacy
    /// warn-and-continue.
    pub(super) phase_hook_raise_latch: PhaseHookRaiseLatch,
    /// Consumer custom-message hook (F5). `None` when the consumer's
    /// `TaskDefinition` exposes no `custom_message_handler` attribute —
    /// the dispatch decision then consumes important messages unhandled
    /// (WARN + `Handled`, so the replicated inbox never grows
    /// unboundedly on a hook-less consumer). Installed pre-run via
    /// [`Self::set_custom_message_handler`].
    pub(super) on_custom_message: Option<OnCustomMessage>,
    /// Mutation-capture sink (F5 atomicity): `Some` only inside the
    /// atomic effect+terminal window opened by
    /// [`Self::begin_mutation_capture`] — while armed, every
    /// `apply_and_broadcast_cluster_mutations` call applies locally as
    /// usual but appends its NoOp-filtered batch here instead of
    /// broadcasting, and [`Self::take_mutation_capture`] hands the
    /// accumulated batch to the one-frame flush
    /// ([`Self::broadcast_applied_mutations`]). `None` on every other
    /// path — the steady-state broadcast behaviour is unchanged.
    pub(super) mutation_capture: Option<Vec<ClusterMutation<I>>>,
    /// Node-local custom-message backlog monitor (F5 keep-up WARN):
    /// first-observed instants per `Unhandled` inbox key + the
    /// rate-limit state behind the "handler is not keeping up"
    /// diagnostic. Observed on the heartbeat tick
    /// ([`Self::observe_custom_backlog`]). NOT replicated by design —
    /// posted-at wall-clock age is a node-local observation; a promoted
    /// primary restarts the ages from first sight, which only DELAYS a
    /// WARN, never fabricates one.
    pub(super) custom_backlog_monitor: crate::primary::custom_message::CustomBacklogMonitor,
    /// Terminal-ordering gate parking lot (`primary/terminal_gate.rs`):
    /// wire task terminals whose `msgs_posted_through` stamp is not yet
    /// covered by their origin's custom-inbox terminal watermark, FIFO
    /// in arrival order (per-origin stamps are monotonic, so the FIFO
    /// scan releases same-origin terminals in arrival order by
    /// construction). Drained by [`Self::release_gated_terminals`] on
    /// the custom-message dispatch cadence. Node-local by design: a
    /// parked terminal is acked-but-unprocessed, so a primary death
    /// while parked degrades to the standard lost-terminal
    /// reconciliation (requeue / re-dispatch) — the same window an
    /// ack-then-die-before-apply always had, here bounded by the
    /// at-least-once delivery of the awaited messages.
    pub(super) gated_terminals:
        std::collections::VecDeque<dynrunner_protocol_primary_secondary::DistributedMessage<I>>,
    /// SYNCHRONOUS run-fail dispatch freeze (`lifecycle/worker_mgmt.rs`
    /// owns the latch writes): set the instant a `RunShouldFail` /
    /// `PolicyFatalExit` is EMITTED onto the worker-management bus —
    /// before the bus is ever drained — and read by the dispatch-view
    /// pipeline's step-0 seam alongside the graceful-abort latch. The
    /// bus consumption (`worker_mgmt_fail_outcome` + the loop break)
    /// stays asynchronous per the decoupling law; this latch only
    /// guarantees no assignment escapes the emit→break window (the
    /// run_20260611_005220 post-raise 6-task leak). Node-local and
    /// never cleared: a run-fail emit is terminal for this primary.
    pub(super) run_fail_dispatch_freeze: bool,
    /// The consumer's discovery policy for a relocated (mode-2) primary or
    /// an in-process `--source-already-staged` local primary, plus the
    /// phase graph it seeds alongside the discovered tasks. `None` on every
    /// cold mode-1 / legacy primary — the [`Self::discover_on_promotion`]
    /// driver is then inert because the CRDT's `discovery_debt()` is
    /// `Undeclared` anyway. Registered BEFORE `run` via
    /// [`Self::register_setup_discovery`]; taken on the single discovery
    /// fire (the `Option::take` IS the fire-once latch, alongside the
    /// `discovery_debt() == Owed` gate — the V6 CRDT-intrinsic latch).
    pub(super) setup_discovery: Option<crate::discovery::SetupDiscovery<I>>,
    /// Phases that have already had `on_phase_start` fired. The pool's
    /// state machine doesn't track "did we observe this transition" —
    /// that's the manager's bookkeeping, kept here so the pool stays
    /// purely about queue + dependency state.
    pub(super) phase_started_emitted: HashSet<PhaseId>,

    // Per-secondary last-keepalive tracking for failover detection (F1).
    pub(super) secondary_keepalives: HashMap<String, Instant>,

    /// The shared own-tick-health authority (`crate::own_tick_health`): the
    /// SAME primitive the secondary's silence judgments consume. The
    /// heartbeat sweep feeds each tick's instant to it; a lagged tick means
    /// THIS node's runtime was frozen/starved for the interim, so every
    /// silence age it would measure is inflated by its own stall —
    /// declaring removals off that sweep would author deaths of live peers.
    /// `observe_tick` returning `true` defers the WHOLE sweep to the NEXT
    /// (on-cadence) tick, by which time the ingest/processing clocks have
    /// refreshed. Deferral is BOUNDED (the chronic escalation, armed at
    /// construction with the hard silence window): once one starved streak
    /// has spanned a full death verdict, sweeps resume and judge on the
    /// primitive's starvation-honest judged clock (via
    /// `silence_judged_marks` below) instead of wall-clock ages.
    pub(super) own_tick_health: crate::own_tick_health::OwnTickHealth,

    /// Per-secondary judged-silence marks (owned by the liveness module,
    /// `primary::heartbeat`): each member's last evidence-of-life instant
    /// paired with the judged-clock reading when a sweep first observed
    /// that evidence. Maintained on EVERY sweep so the chronic-starvation
    /// escalation has per-member history the moment it engages; read by
    /// the escalated sweep (and the dispatch-altitude silent-set read) as
    /// `judged_now - judged_at_evidence`, which never exceeds the wall
    /// silence. Entry removed on requeue and welcome (incarnation
    /// boundaries), self-corrected on evidence advance.
    pub(super) silence_judged_marks:
        HashMap<String, super::heartbeat::SilenceJudgedMark>,

    /// Decider-health gate on the staleness INPUTS (the companion of
    /// the tick-lag guard above, for the OTHER starvation axis): tracks
    /// arrival-vs-drained pending persistence over the transport's
    /// ingest-edge clocks and defers every staleness-based dead-peer
    /// declaration while the mesh pump is provably not moving inbound
    /// frames. Owned by the liveness module (`primary::heartbeat`);
    /// fed once per heartbeat tick, consulted by the dispatch-altitude
    /// silent-set read between ticks.
    pub(super) ingest_gate: super::heartbeat::IngestEdgeGate,

    /// Self-suspect gate on the staleness PATTERN (the third
    /// decider-health guard, covering the WIRE axis the other two
    /// cannot observe): when EVERY remote judged member is silent
    /// simultaneously, the sweep suspects this node's own
    /// ingest/egress before declaring N independent deaths, and
    /// defers — bounded by the hard silence window. Owned by the
    /// liveness module (`primary::heartbeat`); fed by each sweep's
    /// classification, consulted by the dispatch-altitude silent-set
    /// read between ticks (the same contract as `ingest_gate`).
    pub(super) collective_silence_gate: super::heartbeat::CollectiveSilenceGate,

    /// Per-secondary count of staged silence WARN stages already logged
    /// for the secondary's CURRENT silence streak. Owned by the liveness
    /// module (`primary::heartbeat`); the heartbeat tick reads it to fire
    /// each WARN stage at most once, and clears the entry on keepalive
    /// recovery, welcome, and requeue so a fresh streak re-warns from the
    /// first stage. Absent entry == zero stages warned. Private to the
    /// liveness concern: dispatch never reads it (the silent-id set is the
    /// only liveness fact dispatch consumes, via the two boundary methods).
    pub(super) silence_warn_stage: HashMap<String, usize>,

    /// Throttle for the primary's OWN egress-keepalive delivery failures
    /// (`broadcast_primary_keepalive`). A primary whose keepalive sends
    /// all fail is MUTE — invisible to itself and silently feeding every
    /// peer's primary-silence suspicion — so the failure path is loud at
    /// WARN, but throttled (a member mid-disconnect produces one failure
    /// per tick on an already-handled transition, so an un-throttled WARN
    /// would spam). The keepalive owns its own narration; the per-send
    /// debug lines stay for fine-grained tracing.
    pub(super) keepalive_egress_warn: crate::warn_throttle::WarnThrottle,

    /// The set of members whose operational MAIN LOOP has provably run
    /// this incarnation: a mesh `Keepalive` with the SECONDARY emitter
    /// role was received from them (the secondary's keepalive arm spins
    /// up only post-`wait_for_setup`). Owned by the liveness module
    /// (`primary::heartbeat`). Read by `collect_heartbeat_report` to
    /// BOUND the Operational-state setup exemption: a pre-Operational
    /// member is exempt from the silence schedule only while its
    /// keepalive emitter has not started — once proven, it is judged
    /// like any Operational member, so a member whose connection
    /// typestate wedged pre-Operational (while its node kept working)
    /// cannot die silence-invisible. Entry removed on welcome (a fresh
    /// incarnation re-earns its exemption) and on requeue (the member
    /// is gone).
    pub(super) keepalive_proven: HashSet<String>,

    /// Per-secondary backoff timestamps. When a secondary returns
    /// "No idle worker available" Recoverable (its dispatch.rs
    /// `is_idle_state()` check found every worker non-idle —
    /// either real saturation or a stuck-pool state-sync issue),
    /// primary records the secondary as backpressured with an
    /// expiry timestamp; until expiry, `dispatch_to_idle_workers`
    /// and `handle_task_request` skip workers belonging to that
    /// secondary so the kickstart amplifier doesn't spin tasks
    /// against an unresponsive node. Cleared on the next
    /// successful TaskComplete from that secondary (proves it's
    /// healthy).
    pub(super) backpressured_secondaries: HashMap<String, Instant>,

    /// First moment (operational-loop iteration) where
    /// `cluster_state.alive_worker_secondary_count()` read zero while
    /// the pool still has pending work. Cleared whenever an alive
    /// worker-secondary is present again (re-handshake / partial fleet
    /// survival). After `config.fleet_dead_timeout` of continuous
    /// emptiness, the operational loop exits cleanly with the queued
    /// tasks left stranded. See `fleet_dead_timeout` docs for the
    /// rationale.
    pub(super) fleet_dead_since: Option<Instant>,

    /// Set of secondary ids that have reported `MeshReady`. The
    /// primary's `wait_for_mesh_ready` step blocks on this set
    /// growing to the connected-secondaries set before it issues its
    /// `PrimaryChanged` announcement — without that wait, the
    /// newly-named primary becomes authoritative against a still-
    /// forming peer mesh and every pre-mesh-formation message goes
    /// nowhere. Recorded by `handle_mesh_ready`; consumed by
    /// `wait_for_mesh_ready` AND the dispatch-readiness predicate
    /// [`Self::member_mesh_confirmed`].
    pub(super) mesh_ready_secondaries: HashSet<String>,

    /// Members whose mesh-gate veto has ALREADY been WARN-named this
    /// unconfirmed spell. The gate's veto
    /// (`should_skip_worker_for_dispatch` withholding an unconfirmed
    /// member) is consulted per-worker on every dispatch recheck, and
    /// during bring-up EVERY member passes through a brief unconfirmed
    /// window — an unthrottled WARN would name healthy members dozens of
    /// times before their `MeshReady` lands. One WARN per member per
    /// spell carries the full diagnostic (the #360 evidence was read off
    /// this exact line); repeats are DEBUG. Cleared by
    /// `handle_mesh_ready` when the member confirms, so a member that
    /// later regresses to unconfirmed (a promoted primary's empty
    /// confirmation set) is named again.
    pub(super) mesh_gate_veto_warned: HashSet<String>,

    // primary promotion
    pub(super) primary_id: Option<String>,

    // Stage-file notifications queued before `run()` (or during init,
    // before secondary connections are up). Flushed once the welcome
    // + peer-connect handshake completes — at that point `send_to`
    // can route to a known secondary. Each entry is
    // `(secondary_id, file_hash, content_hash, src_path, dest_path)`
    // where `file_hash` is the task identifier (cache lookup key)
    // and `content_hash` is the SHA256 of the file contents (used
    // by the secondary's staging integrity check).
    pub(super) pending_stage_files: Vec<(String, String, String, String, String)>,

    /// Replicated cluster ledger. The primary originates `TaskAdded`,
    /// `TaskCompleted`, `TaskFailed`, and (post-Phase-L) `TaskAssigned`
    /// mutations; each one is applied locally and broadcast so every
    /// secondary's mirror converges to the same view. CRDT semantics
    /// (idempotent under repetition + reorder within the per-task
    /// happens-before constraint) live in `cluster_state.rs`.
    pub(super) cluster_state: ClusterState<I>,

    /// Outbound snapshot-stream driver: serves `RequestSnapshotStream`
    /// pulls (late joiners, behind peers) one bounded package per
    /// operational-loop wakeup — see `crate::snapshot_stream`. The
    /// loop's wake arm drains it; the request handler feeds it.
    pub(super) snapshot_streams: crate::snapshot_stream::SnapshotStreamResponder,
    /// Settled-CRDT spill driver: sweeps join-fixed-point ledger
    /// entries to the node-local spill file on a cadence (one
    /// `spawn_blocking` write in flight, durable-then-evict) — see
    /// `crate::settled_spill`. The operational loop owns its one arm.
    pub(super) settled_spill: crate::settled_spill::SettledSpillDriver,
    /// Inbound snapshot-stream progress (per responder): lets this
    /// node's own anti-entropy pulls RESUME an interrupted stream
    /// (same stream id + cursor) instead of re-pulling from scratch.
    pub(super) inbound_snapshots: crate::snapshot_stream::InboundSnapshotStreams,
    /// Disciplined anti-entropy PULL driver (the #491 storm-killer): the
    /// single-flight probe→select→pull FSM. The digest-receive path feeds
    /// it `note_behind` instead of the eager per-digest immediate pull; the
    /// operational loop's pull arm drives its timers + translates its
    /// directives into `send_to`. Almost always Idle on the authoritative
    /// primary (it is rarely behind a follower), but load-bearing for a
    /// freshly-promoted primary still warming its mirror. See
    /// `crate::pull_coordinator`.
    pub(super) pull_coordinator: crate::pull_coordinator::PullCoordinator,

    /// Cross-thread / cross-runtime ingress for the
    /// `PrimaryHandle` PyO3 surface. Each handler sits alongside
    /// the coordinator's per-mutation semantics; the receiver
    /// is read inside the operational loop's `select!` and the
    /// sender is cloned out via `command_sender()` before `run()`
    /// starts.
    ///
    /// Held as `Option` so the operational loop can take the
    /// receiver out for the duration of the select-driven phase
    /// (Rust's borrow checker won't let us hold a `&mut Receiver`
    /// inside the same `&mut self` that the per-arm handlers need)
    /// and put it back when the loop exits. Outside the loop, the
    /// option is `Some` so cloned senders keep working between runs.
    pub(super) command_rx: Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,

    /// Sender side of the command channel, cloned to consumers via
    /// `command_sender()`. Stored on `Self` so the lifetime is tied
    /// to the coordinator — when the coordinator is dropped, all
    /// cloned senders return `SendError` on subsequent `send()`
    /// calls and the PyO3 side surfaces that as a Python exception.
    pub(super) command_tx: tokio_mpsc::Sender<PrimaryCommand<I>>,

    // The per-task unfulfillable-reinject counter is now the replicated
    // grow-only-MAX `ClusterState::unfulfillable_reinject_used` (P3) — a
    // per-hash USED count, NOT the old decrementing node-local `remaining`.
    // The handler derives `remaining = cap − used` locally so the reinject
    // budget survives failover.
    /// Peer-lifecycle dispatcher channel receiver, paired with the
    /// `lifecycle_tx` installed on `cluster_state` at construction.
    /// Taken out at `run()` start and handed to
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`] inside
    /// the operational LocalSet so the dispatcher's lifetime tracks
    /// the operational loop's tokio runtime.
    pub(super) lifecycle_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>>,
    /// Consumers of peer-lifecycle events. Appended to via
    /// [`Self::register_lifecycle_listener`] before `run()` enters;
    /// `std::mem::take` moves the whole vector into the spawned
    /// dispatcher at `run()` start, after which the field is empty
    /// and any post-run `register_lifecycle_listener` calls are
    /// silently appending to a dead-letter list (no dispatcher will
    /// see them). The single-shot lifecycle is consistent with the
    /// rest of the coordinator's `run()`-once contract.
    pub(super) peer_lifecycle_listeners: Vec<Box<dyn crate::peer_lifecycle::LifecycleListener>>,

    /// Handle to the peer-lifecycle dispatcher task spawned at
    /// `run()` start. `Some` between the dispatcher's spawn and its
    /// abort+await at run exit; `None` outside an active run (the
    /// `cleanup_lifecycle_dispatcher` helper takes it and joins).
    ///
    /// Owning the handle is the load-bearing piece that distinguishes
    /// "dispatcher exits on its own" (the `cluster_state` drop path,
    /// which only happens when the whole coordinator drops) from
    /// "dispatcher exits when `run()` returns" (this field's
    /// abort+await). The dispatcher's input channel sender lives on
    /// `cluster_state`, so a `run()` returning Err while the
    /// coordinator object stays alive (the PyO3 wrapper keeps the
    /// handle, the SLURM pipeline may inspect it) would leave the
    /// dispatcher blocked on `rx.recv().await` forever — never seeing
    /// a closed-channel `None`. The abort fires its
    /// `JoinHandle::abort()`; the await catches the dispatcher's exit
    /// (or the `JoinError::Cancelled` outcome) so cleanup is
    /// synchronous with `run()` returning.
    pub(super) lifecycle_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Task-completion dispatcher channel receiver, paired with the
    /// `task_completed_tx` installed on `cluster_state` at construction.
    /// Taken out at `run()` start and handed to
    /// [`crate::task_completed::run_task_completed_dispatcher`] inside
    /// the operational LocalSet so the dispatcher's lifetime tracks
    /// the operational loop's tokio runtime. Mirrors `lifecycle_rx`
    /// exactly; the two dispatchers are independent modules with
    /// independent channels and independent listener vectors.
    pub(super) task_completed_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::task_completed::TaskCompletedEvent>>,

    /// Consumers of task-completion events. Appended to via
    /// [`Self::register_task_completed_listener`] before `run()`
    /// enters; `std::mem::take` moves the whole vector into the
    /// spawned dispatcher at `run()` start, after which the field is
    /// empty and any post-run `register_task_completed_listener` calls
    /// are silently appending to a dead-letter list. Mirrors
    /// `peer_lifecycle_listeners`.
    pub(super) task_completed_listeners: Vec<Box<dyn crate::task_completed::TaskCompletedListener>>,

    /// Handle to the task-completion dispatcher task spawned at
    /// `run()` start. Same shape + cleanup discipline as
    /// `lifecycle_dispatcher_handle`; the
    /// `cleanup_task_completed_dispatcher` helper takes it and joins
    /// on every exit path of `run()`.
    pub(super) task_completed_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Matcher-trigger receiver, paired with the
    /// `matcher_trigger_tx` installed on `cluster_state` at
    /// construction. Taken out at `run()` start so the operational
    /// `select!` arm can `drain_matcher_batch` against it. `None`
    /// once the loop has taken ownership; subsequent runs against the
    /// same coordinator are not supported (single-shot lifecycle).
    pub(super) matcher_trigger_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::fulfillability_matcher::MatcherTriggerEvent>,
    >,

    /// Optional consumer-supplied fulfillability matcher. `None`
    /// (the default) disables the matcher pipeline entirely — the
    /// `select!` arm collapses to `pending::<Never>` shape and never
    /// fires. `Some(m)` installs the matcher; the operational loop
    /// calls `m.should_reinject(...)` once per `Unfulfillable` task
    /// per batch of holdings-update events.
    ///
    /// Registered via [`Self::set_fulfillability_matcher`] BEFORE
    /// `run()` enters (same pre-run-only contract as
    /// `register_lifecycle_listener`; the field is `mem::take`-d into
    /// the operational loop at run start so post-run registration is
    /// silently dropped).
    pub(super) fulfillability_matcher:
        Option<Box<dyn crate::fulfillability_matcher::FulfillabilityMatcher<I>>>,

    /// Monotonic identity allocator for newly spawned secondaries.
    /// Initialised to `config.num_secondaries` so the IDs the
    /// preparation phase already minted (`secondary-0..secondary-N-1`)
    /// are reserved; the first respawn returns `secondary-N`. Mutated
    /// exclusively from the operational loop via
    /// [`Self::mint_secondary_id`].
    pub(super) next_secondary_id: u32,

    /// Optional opaque handle to the deployment-mode job manager
    /// (today: `Arc<Mutex<SlurmJobManager<…>>>` parked here by the
    /// SLURM PyO3 pipeline). Stored as `Arc<dyn Any + Send + Sync>`
    /// so `manager-distributed` stays decoupled from `dynrunner-slurm`;
    /// the respawn caller downcasts at the call site. Setter is
    /// callable after preparation but before `run()` enters.
    pub(super) slurm_job_manager: Option<Arc<dyn Any + Send + Sync>>,

    /// In-flight respawn tasks. The operational `select!` loop drains
    /// finished tasks here to apply each [`respawn::RespawnOutcome`].
    /// Not cloned, snapshotted, or restored — fresh coordinators
    /// start with an empty `JoinSet`.
    pub(super) respawn_tasks: JoinSet<RespawnOutcome>,

    /// Per-provider respawn implementation, supplied by the
    /// deployment layer (multi-process / SLURM). `None` disables the
    /// respawn pipeline at construction; the operational `select!`
    /// arm short-circuits (no dispatcher listener registered, no
    /// `respawn_lifecycle_rx` to poll). The trait object is `Send +
    /// Sync` so the operational arm can clone the `Arc` across
    /// `spawn_local` boundaries.
    pub(super) respawn_spawner: Option<Arc<dyn SecondarySpawner>>,

    /// Active respawn budget. `None` mirrors `respawn_spawner = None`
    /// — the policy is disabled at construction and the operational
    /// arm never consults it.
    pub(super) respawn_budget: Option<RespawnBudget>,

    /// Correlation table for the REMOTE respawn backend (the
    /// promoted/relocated primary's `RemoteSecondarySpawner`, whose
    /// physical provider lives in the submitter/observer process). The
    /// stub registers a waiter per request; the inbox arm's
    /// `RespawnSpawnResult` / `RespawnRevokeResult` handler completes
    /// it through this handle. `None` on the local-provider topology
    /// (the submitter primary) and when the policy is disabled. Set by
    /// [`Self::enable_respawn_remote`] only.
    pub(super) remote_respawn_pending: Option<super::respawn::RemoteRespawnPending>,

    /// Transport-recovery port handed to the observer at relocation
    /// (BUG-B reconnect). The submitter primary never uses it itself —
    /// it carries it ONLY so that when this primary relocates onto a
    /// compute peer and steps down into a standalone observer, the
    /// observer can rebuild its dropped `-R` reverse tunnels (the
    /// submitter's transport has no dial path / no QUIC reconnect ticker).
    /// `None` on backends whose transport heals its own links (e.g.
    /// `--multi-computer local` mpsc mesh). Wired from the deployment
    /// layer via [`Self::set_tunnel_reconnector`], symmetric with
    /// `respawn_spawner`. See [`crate::observer::reconnect`].
    pub(super) tunnel_reconnector: crate::observer::ReconnectorHandle,

    /// The job-ledger consult port for the observer's cluster-empty
    /// terminal verdict. The submitter primary never uses it itself — it
    /// carries it ONLY so that when this primary relocates onto a compute
    /// peer and steps down into a standalone observer, the observer can
    /// consult squeue for the run's job ids and render a terminal verdict
    /// when the whole cluster has left the queue. `None` on backends with
    /// no job ledger (e.g. `--multi-computer local`). Wired from the
    /// deployment layer via [`Self::set_job_ledger_probe`], symmetric with
    /// `tunnel_reconnector`. See [`crate::observer::job_ledger`].
    pub(super) job_ledger_probe: crate::observer::JobLedgerProbeHandle,

    /// The upload-action port for setup-task UPLOADS (#336 P1). Consulted
    /// by the in-process setup executor when a setup task whose affinity is
    /// THIS primary carries an [`dynrunner_core::UploadFileRef`], AND
    /// carried onto the observer tail at relocation (the submitter→observer
    /// is the framework auto-staging upload affinity — it physically holds
    /// the source files). `None` on a coordinator with no uploader wired
    /// (no upload setup task it hosts); an upload-ref task assigned to a
    /// primary with `None` here fails as a wiring error. Wired from the
    /// deployment layer via [`Self::set_upload_action`], symmetric with
    /// `tunnel_reconnector`. See [`crate::upload_action`].
    pub(super) upload_action: crate::upload_action::UploadActionHandle,

    /// Sender side of the dispatcher → operational-loop respawn
    /// lifecycle channel (carries the full
    /// [`crate::peer_lifecycle::PeerLifecycleEvent`] stream:
    /// `Removed` drives spawn requests, `Added` drives the
    /// pending-replacement reconciliation). Cloned into the
    /// registered listener at `run()` start so synchronous
    /// `on_event` calls have a place to enqueue. Held as `Option`
    /// so the channel is only constructed when the respawn policy
    /// is enabled (avoids an idle channel sitting on every
    /// coordinator).
    ///
    /// Unbounded shape so the synchronous lifecycle-dispatcher
    /// `on_event` arm never blocks and never drops: mass-death-grace
    /// finalize bursts that previously blew past a bounded cap now
    /// enqueue every death; the total-budget cap on
    /// `RespawnBudget::max_total` is what bounds the memory cost in
    /// practice (the operational loop reject-accepts beyond it).
    pub(super) respawn_lifecycle_tx:
        Option<tokio::sync::mpsc::UnboundedSender<crate::peer_lifecycle::PeerLifecycleEvent>>,

    /// Receiver side of the dispatcher → operational-loop respawn
    /// lifecycle channel. Taken out for the duration of the
    /// operational loop, the same shape as `command_rx` /
    /// `matcher_trigger_rx`. `None` outside an active loop (or
    /// when the respawn policy is disabled).
    pub(super) respawn_lifecycle_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>>,

    /// Pending replacements awaiting their join: minted replacement id
    /// → the removed member it replaces. Node-local operational state
    /// of the respawn pipeline (NOT replicated — the submitting node
    /// is the only one able to revoke what it submitted; a failed-over
    /// primary's replacements are reclaimed by the old node's
    /// run-teardown sweep, the same story as its `job_ids`). Inserted
    /// on every ACCEPTED dispatch; an entry leaves when its
    /// replacement joins the membership (the legitimate-occupant case
    /// — no revocation) or when its original is re-admitted first (the
    /// squatter case — `SecondarySpawner::revoke` is issued).
    /// Size is bounded by `RespawnBudget::max_total`. Wraps the map with
    /// the derived "awaiting a join" gate the respawn listener consults to
    /// drop `Added` events while no replacement is pending (so the respawn
    /// arm parks instead of busy-waking on membership joins).
    pub(super) pending_replacements: super::respawn::PendingReplacements,

    /// Receiver side of the liveness-beacon listener → operational-loop
    /// channel. The [`crate::liveness::LivenessListener`] (bound on this
    /// node's runtime by the run boundary) forwards each decoded beacon's
    /// node-id here; the operational loop drains it and calls
    /// `record_keepalive` — the UNION half of the death-clock (a secondary
    /// is reaped iff its beacon AND its mesh frames are both absent for the
    /// threshold). `None` when no listener was wired (channel-only
    /// fixtures), in which case the loop arm parks on `pending()`.
    pub(super) liveness_ping_rx: Option<tokio::sync::mpsc::UnboundedReceiver<String>>,

    /// The runtime→beacon-thread bridge for the PRIMARY→secondaries liveness
    /// direction: the SET of this primary's live secondaries' liveness
    /// `SocketAddr`s. The coordinator PUBLISHES the set into it whenever the
    /// secondary roster changes (`publish_beacon_targets`); the dedicated
    /// [`crate::liveness::LivenessBeacon`] thread READS it each tick and
    /// sends to every address. This is the half a CPU-starved primary needs:
    /// its OUTBOUND mesh keepalive freezes with its build-pegged runtime, but
    /// this off-runtime beacon keeps asserting the primary's liveness so its
    /// secondaries' failover-detector does not false-elect a successor.
    /// Default (empty) until the run boundary installs the listener-derived
    /// addrs and spawns the beacon; empty → the beacon no-ops.
    pub(super) beacon_target: crate::liveness::BeaconTarget,

    /// The node-scoped peer→liveness-address book (a clone of the one the
    /// co-located `SecondaryCoordinator` populated from `PeerInfo`). The
    /// promoted primary reads it to resolve each live secondary's raw beacon
    /// `SocketAddr` when (re)building its `beacon_target` set — it observes
    /// no `PeerInfo` of its own, so this shared cell is its only source of
    /// its secondaries' beacon addresses. Default (empty) for fixtures /
    /// the never-promoted bootstrap primary, where the primary's beacon
    /// no-ops (the mesh-frame keepalive still reaches secondaries).
    pub(super) peer_liveness_addrs: crate::liveness::PeerLivenessAddrs,

    /// The PRIMARY's own dedicated-thread liveness beacon handle, spawned by
    /// the run boundary on the node's runtime (a `std::thread` + `UdpSocket`,
    /// off the build-starvable tokio runtime). Held for the primary's
    /// lifetime so its `Drop` joins the thread at teardown. `None` when no
    /// beacon was spawned (bind failure, or a channel-only fixture) — the
    /// secondaries then fall back to the mesh-frame liveness legs alone.
    pub(super) primary_beacon: Option<crate::liveness::LivenessBeacon>,

    /// Construction-time primary endpoint and pubkey snapshot used
    /// to build [`SecondarySpawnSpec`]. The per-provider spawner
    /// adapters cache their own copies (see
    /// `PyMultiProcessSpawner` constructor) and ignore the spec's
    /// equivalent fields; carrying them on the spec keeps the trait
    /// contract honest for future providers that don't have
    /// adapter-side cache.
    pub(super) respawn_primary_endpoint: String,
    pub(super) respawn_primary_pubkey_pem: String,

    /// Dedup state for "task names a preferred secondary id we have
    /// never heard of" warnings. The validator does not own the
    /// known-secondaries set nor the task list; the call sites in
    /// `lifecycle.rs::seed_cluster_state` (initial validation) and
    /// `task.rs::handle_cluster_mutation` (post-PeerJoined revalidation)
    /// supply both per invocation. Single concern lives in
    /// [`preferred_secondaries::PreferredSecondariesValidator`].
    pub(super) preferred_secondaries_validator:
        preferred_secondaries::PreferredSecondariesValidator,

    /// Panik-watcher signal receiver. Installed via
    /// [`Self::register_panik_signal_rx`] before `run()`; `None`
    /// when the operator did not pass any `--panik-file` paths. The
    /// operational `select!` arm in
    /// `lifecycle/operational_loop.rs` reads this slot, parks on
    /// `pending().await` when None, and on `Ok(signal)` announces a
    /// self-authored `ClusterMutation::PeerRemoved { SelfDeparture }`
    /// (membership/observability only — peers LOG it, the run is not
    /// terminated on peers) then returns `RunError::PanikShutdown` for
    /// the PyO3 wrapper to translate into `std::process::exit(137)`.
    ///
    /// Unlike the secondary, the primary owns no local worker pool
    /// (workers run on secondaries, accessed remotely via the
    /// `RemoteWorkerState` ledger), so the primary's panik-react
    /// path has no `kill_all_workers_with_grace` step — just the
    /// broadcast + exit. Worker teardown is each secondary's
    /// concern; the broadcast is what tells every other node to
    /// run its own teardown.
    pub(super) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,

    /// The operator's SIGUSR2 graceful-abort trigger. Injected via
    /// [`Self::register_graceful_abort_trigger`] before `run()` by the PyO3
    /// entry path, which arms it at process entry (BEFORE the bootstrap mesh
    /// bind, so a SIGUSR2 during the primary's bootstrap window is latched
    /// rather than killing the process via the kernel default). The
    /// operational `select!` arm consumes it: a PRIMARY receiving SIGUSR2 IS
    /// the abort authority, so the arm short-circuits straight into
    /// [`Self::initiate_graceful_abort`] (the SAME latch the wire
    /// `GracefulAbortRequest` handler drives) — no mesh delivery needed.
    /// `None` when un-injected (tests / embeddings): the arm parks on
    /// `pending().await` and the primary NEVER self-arms a second
    /// `user_defined2` stream (the single-owner rule). NOT taken out into the
    /// loop's locals — the loop reads it through a disjoint-field borrow so it
    /// survives a relocation that cancels the loop, and
    /// [`Self::into_observer_handoff`] carries it onto the relocated observer
    /// (a delivery latched here surfaces on the observer's first poll).
    pub(super) graceful_abort_trigger: Option<crate::GracefulAbortTrigger>,

    /// Runtime loss-of-primacy signal (BUG-6). The owning
    /// [`crate::process::Node`] installs the receive end; a self→other
    /// `RoleTable.primary` flip (a `PrimaryChanged` / merge / restore
    /// naming another peer, fired through the `register_role_change_hook`
    /// fabric on apply AND restore) sends `()` here. [`Self::run_consuming`]
    /// races this against the pipeline future: a signal makes the submitter
    /// primary destructure itself into an [`crate::observer::ObserverHandoff`]
    /// and return [`PrimaryRunOutcome::Relocated`] so the `Node` builds the
    /// standalone observer. This coordinator NEVER produces the signal and
    /// never decides the disposition — both are the `Node`'s concern; this
    /// is purely the receive end. `None` once taken at run start, or when no
    /// demote source is wired (the `select!` arm parks forever then).
    pub(super) demote_rx: Option<tokio_mpsc::UnboundedReceiver<()>>,

    /// Set by the panik arm in the operational `select!` loop when
    /// the watcher signal fires. Carries the (matched_path, reason)
    /// pair the panik handler produced.
    ///
    /// One-concern wiring identical to the secondary's `fatal_exit`
    /// pattern: the arm only WRITES this; the outer `run_pipeline`
    /// only READS. Avoids changing the inner loop's `Result<(),
    /// String>` signature into `Result<(), RunError>` (which would
    /// ripple through every `?` site, every `From<String>`
    /// conversion, and several helper methods). The outer wrapper
    /// observes a Some here after the operational loop returns Ok
    /// and translates it into `Err(RunError::PanikShutdown { … })`
    /// so the PyO3 boundary can match the structured variant and
    /// call `exit(137)`.
    pub(super) panik_outcome: Option<(std::path::PathBuf, String)>,

    /// Worker-management signal receiver, paired with the
    /// `worker_mgmt_tx` installed on `cluster_state` at construction.
    /// Taken out at the operational loop's start so its `select!` arm
    /// can `recv_worker_signal_batch` against it; put back at loop
    /// exit so retry-pass re-entries keep draining the same channel.
    /// Same `take()`/restore lifecycle as `matcher_trigger_rx`. `None`
    /// once a previous loop entry already consumed it AND the local was
    /// dropped (closed-channel gate) — single-shot per channel.
    pub(super) worker_mgmt_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::worker_signal::WorkerMgmtSignal>>,

    /// Set by the operational `select!` loop's worker-management arm
    /// when it drains a [`WorkerMgmtSignal::RunShouldFail`] or
    /// [`WorkerMgmtSignal::PolicyFatalExit`]. Carries the TYPED outcome
    /// the run should surface (`RunError::Other` for the generic
    /// run-should-fail wedge; `RunError::FatalPolicyExit` for a
    /// consumer-policy fatal abort such as an `on_phase_end` raise) so
    /// the outer `run_pipeline` returns it verbatim — the
    /// signal-to-`RunError` classification lives ONCE in the drain arm,
    /// not split between the emit side and the pipeline tail. Same
    /// write-only/read-only discipline as `panik_outcome`: the arm
    /// WRITES, the outer wrapper READS — keeping the inner loop's
    /// `Result<(), String>` signature untouched. The worker arm OWNS the
    /// clean-shutdown drive; the
    /// phase layer that emitted the signal never breaks the loop
    /// directly (decoupling law).
    pub(super) worker_mgmt_fail_outcome: Option<RunError>,

    /// Set at cold-start seed (`originate_cold_seed`) when the
    /// dependency-existence partition found a `(phase_id, task_id)`
    /// DUPLICATE before any phase started (#3a). Carries the abort
    /// reason. The bootstrap proceeds far enough to connect secondaries
    /// (so the `RunAborted` broadcast reaches them), then `run_pipeline`
    /// reads this directly after `wait_for_connections`, broadcasts
    /// `ClusterMutation::RunAborted { reason }`, and returns
    /// `RunError::DuplicateTaskIdPrePhase` — a hard cluster shutdown.
    /// `None` on a clean seed. Write-only at seed, read-once at the
    /// abort gate (same discipline as `panik_outcome`).
    pub(super) pending_run_abort: Option<String>,

    /// Set when the consumer's `on_run_start` lifecycle hook RAISED on
    /// the promoted-primary path (the pyo3 promotion recipe fires the
    /// hook synchronously BEFORE `run_consuming`, then records the raise
    /// here via [`Self::record_pre_run_hook_abort`]). Carries the raise
    /// reason. The bootstrap proceeds far enough to connect secondaries
    /// (so the `RunAborted` broadcast reaches them), then `run_pipeline`
    /// reads this at the post-connection abort gate
    /// ([`crate::primary::ingest`]'s `fire_pre_run_hook_abort`),
    /// broadcasts `ClusterMutation::RunAborted { reason }`, and returns
    /// `RunError::FatalPolicyExit` — a deliberate consumer-policy abort,
    /// the SAME terminal an `on_phase_end` raise surfaces (the cold-start
    /// path already `?`-propagates an `on_run_start` raise out before
    /// `run()`; this is the promoted-path twin). `None` when the hook
    /// did not raise (or was never fired). Write-only at the pre-run hook
    /// fire, read-once at the abort gate.
    pub(super) pre_run_hook_abort: Option<String>,

    /// OOM-bucket dispatch-shape gate. `true` only while a per-phase
    /// OOM retry bucket is actively reinjecting and draining; `false`
    /// otherwise. The retry-bucket primitive
    /// ([`crate::primary::retry_bucket`]) is the sole writer:
    /// it flips this `true` on `BucketKind::Oom` entry, and back to
    /// `false` on every `Ok(false)` return of the OOM bucket (no
    /// candidates left OR budget exhausted).
    ///
    /// Read by dispatch-shape sites (`dispatch_to_idle_workers`,
    /// `handle_task_request`)
    /// through the accessor [`Self::single_worker_mode`] / the
    /// composed predicate [`Self::should_skip_worker_for_dispatch`].
    /// Call sites never branch on this directly — the masking + the
    /// strict-preferred-secondaries filter live behind a single
    /// dispatch-shape pipeline so the rest of the coordinator stays
    /// agnostic to OOM-bucket semantics.
    ///
    /// User spec (2026-05-17): during the OOM bucket the retries
    /// should run with 1 worker per secondary and tasks ordered by
    /// node memory DESC so memory-pressed work gets a fresh shot
    /// against maximum RAM headroom. Concurrent normal-pass workers
    /// share the masking for the duration of the OOM bucket; this
    /// is documented as acceptable throughput tax (concurrent normal
    /// dispatch tends to share secondaries with the OOM-bucket
    /// retries anyway).
    pub(super) single_worker_mode: bool,

    /// Per-task reconciliation-probe deadlines (#308) — the persistent
    /// timing state behind the "do you still hold task X?" backstop.
    /// Owns every deadline / outstanding-response window / poll cadence
    /// on its own clock (stored `Instant`s) so they elapse on wall time
    /// regardless of `select!` activity; the operational loop polls it
    /// once per iteration ([`Self::reconciliation_probe_tick`]) and the
    /// inbound `TaskHoldResponse` arm feeds it answers
    /// ([`Self::handle_task_hold_response`]). Constructed from
    /// `config.task_reconciliation_timeout` + the keepalive-derived
    /// response window. See [`crate::primary::reconciliation_probe`].
    pub(super) recon_prober: super::reconciliation_probe::ReconciliationProber,

    /// The consumer's run configuration — the byte-identical token
    /// sequence the framework forwards onto a joining / respawned /
    /// promoted node's command line. A NODE-LOCAL launch constant
    /// seeded from `config.forwarded_argv` at construction (mirroring
    /// `next_secondary_id`); NOT replicated lattice data, so it never
    /// touches `cluster_state`. The `RequestRunConfig` responder
    /// (`task::mutation::handle_request_run_config`) reads it READ-ONLY
    /// and unicasts it back to a requesting peer. Empty for a run with
    /// no forwarded args.
    pub(super) forwarded_argv: Vec<String>,

    /// Cold-start seed frames staged for the post-connection fleet
    /// broadcast. `originate_cold_seed` (run BEFORE `wait_for_connections`,
    /// so hydrate can build the pool from the local CRDT in time for
    /// `fire_initial_phase_starts`) applies the seed
    /// (`PhaseDepsSet`/`TaskAdded` + the `#2` invalid-dep `TaskFailed`
    /// transitions) to the LOCAL `cluster_state` and parks the
    /// version-stamped `applied` frames here. `broadcast_cold_seed` (run
    /// AFTER `wait_for_connections`, so the broadcast reaches the connected
    /// fleet) drains and ships them — a pre-connection broadcast is dropped,
    /// so the local-apply and the broadcast are deliberately split across the
    /// connect boundary. Empty on the `PromotionSnapshot` path (the inherited
    /// CRDT replicates via the digest/restore anti-entropy path), making the
    /// post-connection broadcast a natural no-op there — the `SeedSource` arm
    /// is the discriminator, never a runtime `if seeded`.
    pub(super) pending_cold_seed_broadcast: Vec<ClusterMutation<I>>,

    /// Per-iteration `select!`-arm accounting for the operational loop,
    /// published by [`Self::operational_loop`] at loop entry so the
    /// off-runtime [`crate::runtime_watchdog`] checker thread can dump
    /// WHICH arm a wedged loop is hot-spinning on (the ingest-wedge
    /// signature: the inbound arm never wins again). `None` until the
    /// operational loop is entered; observation-only — never read by any
    /// control-flow decision. See [`crate::oploop_instrumentation`].
    pub(super) op_loop_arm_stats:
        Option<std::sync::Arc<crate::oploop_instrumentation::OpLoopArmStats>>,

    /// Optional shared bridge to the off-runtime [`crate::runtime_watchdog`]:
    /// when set (via [`Self::set_op_loop_arm_stats_cell`] at node bootstrap),
    /// [`Self::operational_loop`] publishes its live arm stats into this cell
    /// on entry and clears them on exit, so the single long-lived watchdog can
    /// dump WHICHEVER loop (secondary or this promoted primary) is running at
    /// the freeze. `None` in unit/integration fixtures that read
    /// `op_loop_arm_stats` directly instead. See
    /// [`crate::oploop_instrumentation::OpLoopArmStatsCell`].
    pub(super) op_loop_arm_stats_cell:
        Option<crate::oploop_instrumentation::OpLoopArmStatsCell>,
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Max commands `drain_callback_queued_commands` processes between
    /// cooperative `yield_now().await`s. Bounds how long the single-thread
    /// executor can be monopolised by a self-requeueing command drain before
    /// sibling LocalSet tasks (the inbound recv, mesh pump, QUIC driver) get
    /// to run. Large enough that the common case (a handful of callback-queued
    /// commands) never yields; small enough that a pathological requeue loop
    /// can't wedge the runtime.
    const DRAIN_YIELD_BUDGET: u32 = 1024;

    pub fn new(
        config: PrimaryConfig,
        client: crate::process::MeshClient<I>,
        inbox: crate::process::RoleInbox<I>,
        demote_rx: tokio_mpsc::UnboundedReceiver<()>,
        scheduler: S,
        estimator: E,
    ) -> Self {
        let (command_tx, command_rx) = tokio_mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        // Peer-lifecycle dispatcher channel: built at construction so
        // the apply path on `cluster_state` has a sender to enqueue
        // through from the very first `PeerJoined`/`PeerRemoved`
        // mutation. The receiver waits on `self` until `run()`
        // spawns the dispatcher; events emitted in the interim
        // queue on the unbounded channel and drain on the first
        // dispatcher poll.
        let (lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::unbounded_channel();
        // Matcher-trigger dispatcher channel. Built at construction
        // for the same reason as `lifecycle_tx`: the apply path on
        // `cluster_state` needs a sender ready from the very first
        // `PeerResourceHoldingsUpdated` apply (E1). The receiver
        // waits on `self` until `run()` enters the operational
        // `select!` and drains it via
        // `crate::fulfillability_matcher::drain_matcher_batch`.
        let (matcher_trigger_tx, matcher_trigger_rx) = tokio::sync::mpsc::unbounded_channel();
        // Worker-management signal bus. Built at construction for the
        // same reason as `matcher_trigger_tx`: the phase/task layer's
        // emit calls (`fire_initial_phase_starts` →
        // `PhaseStartedNeedsWorkers`; the per-phase proceed-or-fail
        // decision → `RunShouldFail`; every pool-entry / worker-free
        // edge → `TasksAdded`) need a sender ready from the very first
        // mutation. The receiver waits on `self` until the operational
        // loop takes it and drains coalesced batches via
        // `crate::worker_signal::recv_worker_signal_batch`. No longer
        // test-only: this is the PRODUCTION sender wire-up.
        let (worker_mgmt_tx, worker_mgmt_rx) = tokio::sync::mpsc::unbounded_channel();
        // Task-completion dispatcher channel. Same construction-time
        // motivation as `lifecycle_tx`: the apply path on
        // `cluster_state` needs a sender ready from the very first
        // `TaskCompleted`/`TaskFailed` apply. The receiver waits on
        // `self` until `run()` spawns the dispatcher; events emitted
        // in the interim queue on the unbounded channel and drain on
        // the first dispatcher poll.
        let (task_completed_tx, task_completed_rx) = tokio::sync::mpsc::unbounded_channel();
        // Seed the monotonic id allocator past the IDs the prep phase
        // already minted (`secondary-0..secondary-{num_secondaries - 1}`)
        // so the first respawn lands on `secondary-{num_secondaries}`.
        let next_secondary_id = config.num_secondaries;
        // Snapshot the node-local run-config off the config before it
        // moves into `this.config`, mirroring `next_secondary_id`. The
        // responder reads this verbatim; it never re-reads `config`.
        let forwarded_argv = config.forwarded_argv.clone();
        // Reconciliation prober (#308), built off `config` before it
        // moves into `this.config` (mirroring the snapshots above). The
        // response window — how long an emitted probe waits before the
        // prober gives up and re-arms — is the cluster's established
        // "should have heard back by now" quantum
        // (`keepalive_interval × keepalive_miss_threshold`, ≈15s at the
        // defaults); the silent-holder case past it belongs to the
        // keepalive machinery, not the probe.
        let recon_prober = super::reconciliation_probe::ReconciliationProber::new(
            config.task_reconciliation_timeout,
            config
                .keepalive_interval
                .saturating_mul(config.keepalive_miss_threshold),
        );
        // Own-tick-health authority, built off the keepalive cadence before
        // `config` moves into `this.config` (mirroring the snapshots above).
        // The primary gates its WHOLE sweep on the DEFER verdict, so it
        // opts into the chronic escalation: once a starved streak has
        // spanned the hard silence window (deferral has then outlived a
        // full death verdict), sweeps resume on the starvation-honest
        // judged clock instead of deferring forever — the
        // run_20260611_200548 fix (dead members were never removed, so
        // the respawn pipeline never fired).
        let own_tick_health = crate::own_tick_health::OwnTickHealth::new_with_chronic_escalation(
            config.keepalive_interval,
            config
                .keepalive_interval
                .saturating_mul(config.silence_hard_multiple),
        );
        let snapshot_streams = crate::snapshot_stream::SnapshotStreamResponder::new(&config.node_id);
        let inbound_snapshots = crate::snapshot_stream::InboundSnapshotStreams::new(&config.node_id);
        let pull_coordinator = crate::pull_coordinator::PullCoordinator::new(&config.node_id);
        // Settled-CRDT spill: attach this coordinator's spill segment to
        // the state it owns (degrades to disabled — fat-but-correct — on
        // any setup failure; see `settled_spill`).
        let mut cluster_state = ClusterState::new();
        let settled_spill =
            crate::settled_spill::SettledSpillDriver::start("primary", &mut cluster_state);
        let mut this = Self {
            config,
            client,
            inbox,
            scheduler,
            estimator,
            secondaries: HashMap::new(),
            workers: Vec::new(),
            total_tasks: 0,
            stranded_count: 0,
            mesh_pump_gone: false,
            run_start_batch_fired: false,
            spawn_rejected_task_ids: Vec::new(),
            all_binaries: Vec::new(),
            pending: None,
            phase_deps: HashMap::new(),
            phase_may_be_empty_decl: std::collections::HashSet::new(),
            completed_tasks: HashSet::new(),
            in_flight: HashMap::new(),
            failed_tasks: HashMap::new(),
            in_flight_per_type: HashMap::new(),
            on_phase_start: None,
            on_phase_end: None,
            // Detached by default: no closure shares this end, so the
            // cascade's `take()` is always `None` until a caller wires a
            // real latch via `set_phase_hook_raise_latch`.
            phase_hook_raise_latch: PhaseHookRaiseLatch::detached(),
            on_custom_message: None,
            mutation_capture: None,
            custom_backlog_monitor: Default::default(),
            gated_terminals: std::collections::VecDeque::new(),
            run_fail_dispatch_freeze: false,
            setup_discovery: None,
            phase_started_emitted: HashSet::new(),
            secondary_keepalives: HashMap::new(),
            own_tick_health,
            silence_judged_marks: HashMap::new(),
            keepalive_proven: HashSet::new(),
            ingest_gate: super::heartbeat::IngestEdgeGate::new(),
            collective_silence_gate: super::heartbeat::CollectiveSilenceGate::new(),
            silence_warn_stage: HashMap::new(),
            keepalive_egress_warn: crate::warn_throttle::WarnThrottle::new(
                super::heartbeat::KEEPALIVE_EGRESS_WARN_INTERVAL,
            ),
            backpressured_secondaries: HashMap::new(),
            fleet_dead_since: None,
            mesh_ready_secondaries: HashSet::new(),
            mesh_gate_veto_warned: HashSet::new(),
            primary_id: None,
            pending_stage_files: Vec::new(),
            cluster_state,
            snapshot_streams,
            settled_spill,
            inbound_snapshots,
            pull_coordinator,
            command_rx: Some(command_rx),
            command_tx,
            lifecycle_rx: Some(lifecycle_rx),
            peer_lifecycle_listeners: Vec::new(),
            lifecycle_dispatcher_handle: None,
            task_completed_rx: Some(task_completed_rx),
            task_completed_listeners: Vec::new(),
            task_completed_dispatcher_handle: None,
            matcher_trigger_rx: Some(matcher_trigger_rx),
            fulfillability_matcher: None,
            next_secondary_id,
            slurm_job_manager: None,
            respawn_tasks: JoinSet::new(),
            respawn_spawner: None,
            respawn_budget: None,
            remote_respawn_pending: None,
            tunnel_reconnector: None,
            job_ledger_probe: None,
            upload_action: None,
            respawn_lifecycle_tx: None,
            respawn_lifecycle_rx: None,
            pending_replacements: super::respawn::PendingReplacements::default(),
            liveness_ping_rx: None,
            beacon_target: crate::liveness::BeaconTarget::new(),
            peer_liveness_addrs: crate::liveness::PeerLivenessAddrs::new(),
            primary_beacon: None,
            respawn_primary_endpoint: String::new(),
            respawn_primary_pubkey_pem: String::new(),
            preferred_secondaries_validator:
                preferred_secondaries::PreferredSecondariesValidator::new(),
            panik_signal_rx: None,
            graceful_abort_trigger: None,
            demote_rx: Some(demote_rx),
            panik_outcome: None,
            worker_mgmt_rx: Some(worker_mgmt_rx),
            worker_mgmt_fail_outcome: None,
            pending_run_abort: None,
            pre_run_hook_abort: None,
            single_worker_mode: false,
            recon_prober,
            forwarded_argv,
            pending_cold_seed_broadcast: Vec::new(),
            op_loop_arm_stats: None,
            op_loop_arm_stats_cell: None,
        };
        // Install the peer-lifecycle sender on `cluster_state` so the
        // `PeerJoined` / `PeerRemoved` apply rules' emit calls route
        // through the dispatcher channel from this point onward.
        // Done before any other registration so a mutation that
        // somehow lands during construction still has a sender to
        // enqueue against (defensive: today no mutation is applied
        // pre-`run()`, but the contract should not depend on that).
        this.cluster_state.install_lifecycle_sender(lifecycle_tx);
        // Same shape as the lifecycle sender install: the apply path
        // on `cluster_state` now has a sender to enqueue trigger
        // events through; the operational `select!` will own the
        // receiver from `run()` onward.
        this.cluster_state
            .install_matcher_trigger_sender(matcher_trigger_tx);
        // Same shape as the matcher-trigger sender install: the
        // phase/task apply + emit path on `cluster_state` now has a
        // worker-management bus sender to enqueue signals through; the
        // operational `select!` loop owns the receiver from this point
        // onward and reacts off the emit path.
        this.cluster_state
            .install_worker_mgmt_sender(worker_mgmt_tx);
        // Same shape: install the task-completion sender so the
        // `TaskCompleted` / `TaskFailed` apply rules' emit calls route
        // through the dispatcher channel from this point onward.
        this.cluster_state
            .install_task_completed_sender(task_completed_tx);
        // Recognition→routing publish (mirrors `SecondaryCoordinator::new`):
        // the role-change hook publishes `role_table.primary` into the
        // mesh's `RoleHolderView` for the INGRESS relay. EGRESS resolution
        // is unchanged and stays at this edge (`Self::send_to` reads
        // `cluster_state.current_primary()` with the primary's own
        // `node_id` as the bootstrap fallback); the transport stays
        // `PeerId`-only and never mirrors the role table.
        crate::process::attach_primary_recognition(
            &mut this.cluster_state,
            this.client.role_holder_view(),
        );
        // Subscribe the primary-side "important" (LLM-wake-worthy)
        // emission for `PrimaryChanged` to the same role-change hook
        // fabric. Self-contained observability concern: it reads only
        // the post-mutation `RoleTable` the hook is handed and emits at
        // `target: dynrunner_important`, so the CRDT apply path stays
        // free of any logging coupling. A promoted secondary runs its
        // own same-peer primary coordinator, so the hook rides promotion.
        super::important_events::register_primary_changed_important_hook(&mut this.cluster_state);
        this
    }

    /// THE egress edge: resolve a typed
    /// [`dynrunner_protocol_primary_secondary::Destination`], STAMP the
    /// resolved role-bearing `Destination` on the frame, and hand it to
    /// `self.client.send(..)` — a single queued send the mesh-pump drains
    /// and resolves loopback-vs-remote against the live slot set. This edge
    /// never touches a transport and never decides loopback itself.
    ///
    /// The `resolve_destination` HEAD stays here because its bootstrap
    /// fallback is primary-specific (H1): this primary's own
    /// `config.node_id` (the submitter primary IS the bootstrap primary),
    /// so `Destination::Primary` is always resolvable. The HEAD's role is
    /// the resolvability check — a `None` is the honest "no route to
    /// primary" surfaced as `Err`, matching the prior cold-cache hard
    /// error. The resolved [`dynrunner_protocol_primary_secondary::SendTarget`]
    /// itself is NOT routed here: the role-bearing `Destination` is what the
    /// mesh demuxes against. `Secondary(id)` / `Observer(id)` already carry
    /// their host (the mesh resolves loopback-vs-remote off that id);
    /// `All` fans (origin-excluded at the mesh); `Primary` on the primary
    /// resolves to a loopback — it IS the primary — and the mesh delivers
    /// the stamped `Destination::Primary` to the local primary slot.
    pub(super) async fn send_to(
        &mut self,
        dst: dynrunner_protocol_primary_secondary::Destination,
        msg: dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    ) -> Result<(), String> {
        use dynrunner_protocol_primary_secondary::resolve_destination;
        // Role invariant — the primary NEVER addresses `Destination::Primary`:
        // this coordinator IS the primary (the operational loop's authority
        // invariant), and the mesh's `Primary` dispatch arm is LOOPBACK-ONLY,
        // so a primary-addressed egress frame can only land back in this
        // coordinator's OWN inbox. A handler that re-emits per receipt then
        // self-sustains a memory-speed inbox cycle whose egress pressure
        // starves the pump's wire ingress — the run_20260610_121427 ingest
        // wedge (the deleted `handle_task_request` self-relay). No primary
        // concern needs a self-send; reject loudly so a future caller cannot
        // reintroduce the cycle.
        if matches!(dst, dynrunner_protocol_primary_secondary::Destination::Primary) {
            tracing::error!(
                msg_kind = ?msg.msg_type(),
                "primary egress addressed Destination::Primary — a self-send \
                 loopback (the inbox-cycle hazard); rejecting the frame"
            );
            return Err(
                "primary egress must not address Destination::Primary (self-send loopback)"
                    .to_string(),
            );
        }
        // Resolvability check (H1 bootstrap fallback = this primary's own
        // node_id). The concrete `SendTarget` is discarded — the mesh
        // resolves loopback-vs-remote off the stamped role-bearing
        // `Destination` against its live slots; we only need to know the
        // destination HAS a route before queuing.
        resolve_destination(
            dst.clone(),
            self.cluster_state.current_primary(),
            Some(&self.config.node_id),
            &self.config.node_id,
        )
        .ok_or_else(|| {
            "Destination unresolvable: no current primary in the role table".to_string()
        })?;
        // Stamp the role-bearing target so the receiving pump demuxes it
        // (C3), then queue. The mesh decides loopback-vs-remote; nothing is
        // silently dropped here.
        //
        // A `client.send` `Err` means the egress queue's receiver — the local
        // mesh-pump — has been dropped: THIS node is winding down and the mesh
        // is gone. That is the egress-side TWIN of the operational loop's
        // `inbox.recv() -> None` collapse criterion (the SAME mesh-pump,
        // observed from the send side). Latch it here — the SOLE detection
        // point — so the pre-loop chain in `run_pipeline` can short-circuit
        // into the strand-classification finalize tail uniformly, regardless
        // of which pre-loop send hit the dead pump. The `resolve_destination`
        // miss above is a routing-state error, NOT a local-pump-gone collapse,
        // so it deliberately does NOT set the latch. The `Err` is still
        // returned unchanged for callers that consult it directly (e.g.
        // `perform_initial_assignment`'s typed-outcome short-circuit) or
        // log-and-swallow it.
        let result = self.client.send(dst.clone(), msg.with_target(dst));
        if result.is_err() {
            self.mesh_pump_gone = true;
        }
        result
    }

    /// Directed fan of one primary-originated frame to every
    /// OBSERVER-role member of the replicated roster
    /// (`RoleTable.observers` — alive-filtered by `reproject_roles`,
    /// covering BOTH observer kinds: the relocated submitter-observer
    /// and a late-joined observer, each recorded via `PeerJoined {
    /// is_observer: true }`), excluding this host.
    ///
    /// WHY a directed fan exists at all: `Destination::All` resolves to
    /// the transport's broadcast — a fire-once fan over the DIRECT
    /// connection table, with no relay (the same delivery gap
    /// `send_transfer_complete` documents for the setup trio). An
    /// observer reachable only through a forwarder (a late-joined
    /// observer behind a gateway leg, or any observer whose direct leg
    /// to this host died) silently misses EVERY broadcast-class frame
    /// while its data plane stays healthy over anti-entropy with its own
    /// direct peers — the production face: an observer ingesting live
    /// CRDT mutations while declaring the named primary silent for 600s.
    /// The PRIMARY-class frames the observer's liveness judgment keys on
    /// — the keepalive ([`Self::broadcast_primary_keepalive`]) and the
    /// `PrimaryChanged` re-point — must therefore ALSO ride the directed
    /// `Destination::Observer(id)` edge, which the transport router
    /// relays toward a not-directly-connected target.
    ///
    /// A directly-connected observer receives a duplicate; both frame
    /// classes are idempotent at the receiver (a liveness-clock refresh
    /// / an epoch-LWW apply) and the observer count is tiny, so the
    /// duplication is deliberate and cheap.
    ///
    /// RETURNS the observer ids whose directed send FAILED, leaving the
    /// log policy to the caller: the keepalive fan folds these into its
    /// throttled egress-health WARN (a mute primary must be loud about
    /// its OWN silence), while the `PrimaryChanged` re-point logs them at
    /// debug (a member mid-disconnect produces one per tick on an
    /// already-handled transition — the same reason the per-send line
    /// below them stays debug). The per-send debug line is retained for
    /// fine-grained tracing regardless of caller.
    pub(super) async fn send_to_each_observer(
        &mut self,
        msg: dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    ) -> Vec<String> {
        // Name-sorted owned snapshot: deterministic fan order, and the
        // `&self.cluster_state` borrow drops before the `&mut self`
        // sends (the `send_transfer_complete` collect idiom).
        let mut observers: Vec<String> = self
            .cluster_state
            .role_table()
            .observers
            .iter()
            .filter(|id| *id != &self.config.node_id)
            .cloned()
            .collect();
        observers.sort();
        let mut failed = Vec::new();
        for id in observers {
            if let Err(error) = self
                .send_to(
                    dynrunner_protocol_primary_secondary::Destination::Observer(
                        dynrunner_protocol_primary_secondary::PeerId::from(id.as_str()),
                    ),
                    msg.clone(),
                )
                .await
            {
                tracing::debug!(
                    observer = %id,
                    error = %error,
                    "directed observer fan delivery failed"
                );
                failed.push(id);
            }
        }
        failed
    }

    /// Register a [`crate::peer_lifecycle::LifecycleListener`] to be
    /// invoked off the apply path for every `PeerJoined`/`PeerRemoved`
    /// state transition. Must be called BEFORE `run()` enters; calls
    /// after `run()` has consumed the listener vector are dropped
    /// silently (the field is `mem::take`-d into the dispatcher at
    /// run start, and the dispatcher is the only reader).
    ///
    /// Single concern: own the registration surface; the dispatcher
    /// task in `crate::peer_lifecycle::dispatcher` owns the
    /// invocation semantics.
    pub fn register_lifecycle_listener(
        &mut self,
        listener: Box<dyn crate::peer_lifecycle::LifecycleListener>,
    ) {
        self.peer_lifecycle_listeners.push(listener);
    }

    /// Register the panik-watcher signal receiver. Must be called
    /// BEFORE `run()` enters; calls afterwards have no effect on
    /// the active loop (the field is `Option::take`-n into the
    /// operational loop's local state on first entry).
    ///
    /// Mirrors `SecondaryCoordinator::register_panik_signal_rx`.
    /// The PyO3 wrapper owns spawning
    /// [`crate::panik_watcher::spawn_panik_watcher`] and threading
    /// its `take_signal_rx()` here.
    pub fn register_panik_signal_rx(
        &mut self,
        rx: tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>,
    ) {
        self.panik_signal_rx = Some(rx);
    }

    /// Inject the pre-armed operator SIGUSR2 graceful-abort trigger — the
    /// primary sibling of `ObserverCoordinator::set_graceful_abort_trigger`.
    /// The PyO3 entry path arms the ONE process trigger at entry (BEFORE the
    /// bootstrap mesh bind, so a signal received during the primary's
    /// bootstrap window is latched rather than killing the process) and hands
    /// it here. The operational loop's graceful-abort arm consumes it; on a
    /// relocation the trigger rides [`Self::into_observer_handoff`] onto the
    /// standalone observer (a latched pre-relocation delivery surfaces on the
    /// observer's first poll). MUST be called before `run()` enters — an
    /// un-injected primary parks the arm and never self-arms a second stream
    /// (the single-owner rule).
    pub fn register_graceful_abort_trigger(&mut self, trigger: crate::GracefulAbortTrigger) {
        self.graceful_abort_trigger = Some(trigger);
    }

    /// Register the BUG-6 demote signal on this primary's own
    /// `cluster_state` (the [`crate::process::Node`]'s teardown lever).
    ///
    /// One concern: SIGNAL the owning `Node` whenever the replicated
    /// `RoleTable.primary` flips OFF this primary's own id — i.e. another
    /// peer became primary. The flip is observed through the
    /// `register_role_change_hook` fabric, which fires on EVERY genuine
    /// `RoleTable` mutation: a directly-applied `PrimaryChanged`
    /// (`apply.rs`), a peer-merge (`apply_peer.rs`), AND a snapshot
    /// restore/heal (`snapshot.rs`). Keying on ANY self→other flip — not
    /// only the directly-applied event — is the partition-heal-safe BUG-6
    /// contract: a heal that adopts a different primary via merge/restore is
    /// NOT a `PrimaryChanged` apply, yet it must still demote the stale
    /// primary, or the one-primary invariant breaks into a durable
    /// two-primary.
    ///
    /// The hook ONLY signals (it fires synchronously inside the apply path —
    /// CCD-9: never cross a node boundary, never block); the [`crate::process::Node`]'s
    /// loop drains `demote_tx`'s receiver (the `demote_rx` this coordinator
    /// holds) and surrenders the primary by value into an
    /// [`crate::observer::ObserverHandoff`] (`run_consuming` →
    /// `PrimaryRunOutcome::Relocated`). Best-effort: a dropped receiver means
    /// the node is already winding down. Must be called pre-`run`, alongside
    /// the other registration setters; the `Node` registers it the moment it
    /// builds the primary (bootstrap submitter OR promotion).
    pub fn register_demote_on_displaced(
        &mut self,
        demote_tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) {
        use dynrunner_protocol_primary_secondary::RoleChangeHookRegistrar;
        let own_id = self.config.node_id.clone();
        self.cluster_state.register_role_change_hook(Box::new(
            move |table: &dynrunner_protocol_primary_secondary::address::RoleTable| {
                // A self→other flip is: the table names SOME primary, and it
                // is not us. The table naming nobody (cleared) is not a
                // displacement (no successor to hand off to); the table
                // naming US is the no-op self-confirm. Either way: only fire
                // when another peer holds the role.
                let displaced = match table.primary.as_deref() {
                    Some(p) => p != own_id,
                    None => false,
                };
                if displaced {
                    // Best-effort: a dropped receiver = node winding down.
                    let _ = demote_tx.send(());
                }
            },
        ));
    }

    /// Seed this freshly-constructed (promotion-built) primary from the
    /// promoting host's converged `cluster_state` snapshot, then rebuild the
    /// authoritative derived caches.
    ///
    /// One concern: turn a promotion snapshot into a ready-to-`run`
    /// authoritative view. The [`crate::process::Node`] calls this BEFORE
    /// `run` on the promotion path (the secondary signalled; the `Node`
    /// builds the primary and seeds it here). The snapshot is restored into
    /// this primary's own ledger (an idempotent lattice merge), then
    /// [`Self::hydrate_from_cluster_state`] +
    /// [`Self::reconstruct_workers_from_cluster_state`] +
    /// [`Self::reconstruct_secondaries_from_cluster_state`] rebuild the
    /// pool / worker roster / secondary table as pure derived caches of the
    /// replicated ledger. This is a NORMAL pre-`run` construction input — NOT
    /// a `run_activated` resume (which is gone): the seeded primary then
    /// enters the ordinary `run` path and originates `PrimaryChanged` itself.
    /// Install the promoting host's settled-CRDT base as THIS primary's
    /// settled base — the slim index + the shared read fds onto the
    /// promoting host's (still-mapped) spill file. The promoted primary's
    /// local file+index IS its settled base: the join-fixed-point ledger
    /// slice is inherited WITHOUT replaying any fat body through memory
    /// (the no-redo failover decision). Called by the
    /// [`crate::process::PromotedPrimaryBuilder`] BEFORE
    /// [`Self::seed_from_promotion_snapshot`] — the fat snapshot restore
    /// then merges the disjoint fat half over this settled half. Legal
    /// only on a freshly-built (empty) state; debug-asserts that.
    pub fn adopt_settled_base(&mut self, base: crate::cluster_state::SettledStore) {
        self.cluster_state.install_settled_base(base);
    }

    pub fn seed_from_promotion_snapshot(
        &mut self,
        snapshot: crate::cluster_state::ClusterStateSnapshot<I>,
    ) {
        self.cluster_state.restore(snapshot);
        // Constructor-time seed: a composition failure here cannot broadcast a
        // verdict (the run loop + its dispatchers have not started yet — this
        // runs in the `Node`'s builder BEFORE `run`). Leave it surfaced by the
        // subsequent `run_pipeline` re-hydrate, which ALWAYS re-runs the SOLE
        // pool builder and routes an `Err` through the terminal-verdict path
        // (`abort_run_on_invalid_composition`). So here we only log + leave
        // `pending = None` (set by hydrate on `Err`); the rosters below are
        // still rebuilt from the inherited capacity ledger (independent of the
        // pool), and the run aborts cleanly at the run-phase hydrate.
        if let Err(e) = self.hydrate_from_cluster_state() {
            tracing::error!(
                error = %e,
                "promotion-snapshot seed found an invalid composed task graph; \
                 leaving the pool empty — the run-phase hydrate re-surfaces it \
                 and aborts the run via the terminal verdict path"
            );
        }
        self.reconstruct_workers_from_cluster_state();
        self.reconstruct_secondaries_from_cluster_state();
        // Re-arm the respawn DECISION from the replicated policy caps
        // (`ClusterMutation::RespawnPolicySet`, originated by the
        // submitter's seed): the spend ledger (`respawn_events`) already
        // rode the snapshot, and with the caps restored here the budget
        // gate is complete — the inert-after-relocation hole closes at
        // the same hydrate that reconstructs every other replicated
        // fact. Execution is delegated over the mesh to the
        // provider-host process (`enable_respawn_remote`); a `None`
        // policy (run launched with `--respawn-policy=disabled`)
        // re-arms nothing.
        if let Some(policy) = self.cluster_state.respawn_policy() {
            self.enable_respawn_remote(policy.into());
        }
        // `phase_started_emitted` is seeded by `hydrate_from_cluster_state`
        // (V3) — derived from the CRDT's per-phase progressed-task set on
        // BOTH the cold and promote paths, so it is no longer a promote-ONLY
        // projection here. The inherited ledger's started phases (those with
        // ≥1 InFlight/terminal task) are seeded so a promoted primary does
        // NOT re-fire `on_phase_start`; a blocked-only inherited phase is
        // correctly NOT seeded (the V3 correction over the old `has_any`).
    }

    /// Tear down the peer-lifecycle dispatcher task spawned at
    /// `run()` start. No-op when the dispatcher was never spawned
    /// (e.g. the early-return path before the spawn site, or a
    /// coordinator whose `run()` was never called).
    ///
    /// # Why explicit rather than Drop
    ///
    /// A `Drop` guard cannot abort + await — it has no async
    /// context, and the host tokio runtime may already be torn down
    /// by the time the coordinator is dropped (the PyO3 LocalSet is
    /// scoped to the `py.detach` block). Calling `abort()` from
    /// `Drop` without an awaiting reaper risks a runtime-gone panic
    /// in the dispatcher's last-poll cleanup. Explicit
    /// invocation from the `run()` outer wrapper keeps the abort and
    /// the join inside the live LocalSet.
    pub(super) async fn cleanup_lifecycle_dispatcher(&mut self) {
        if let Some(handle) = self.lifecycle_dispatcher_handle.take() {
            handle.abort();
            // Ignore the `JoinError` — abort-cancelled is the
            // expected shape (`JoinError::is_cancelled() == true`).
            // The body of the task itself never returns a fallible
            // value (it's `Future<Output = ()>`), so the only thing
            // an Ok branch could carry is the unit value — nothing
            // to consume.
            let _ = handle.await;
        }
    }

    /// Register a [`crate::task_completed::TaskCompletedListener`] to
    /// be invoked off the apply path for every `TaskCompleted` /
    /// `TaskFailed` (state-changing) apply rule. Must be called BEFORE
    /// `run()` enters; calls after `run()` has consumed the listener
    /// vector are dropped silently (the field is `mem::take`-d into
    /// the dispatcher at run start, and the dispatcher is the only
    /// reader). Mirrors [`Self::register_lifecycle_listener`].
    pub fn register_task_completed_listener(
        &mut self,
        listener: Box<dyn crate::task_completed::TaskCompletedListener>,
    ) {
        self.task_completed_listeners.push(listener);
    }

    /// Tear down the task-completion dispatcher task spawned at
    /// `run()` start. Mirrors [`Self::cleanup_lifecycle_dispatcher`]
    /// exactly — the same abort+await dance with the same Drop-vs-
    /// LocalSet rationale.
    pub(super) async fn cleanup_task_completed_dispatcher(&mut self) {
        if let Some(handle) = self.task_completed_dispatcher_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Install the consumer-supplied fulfillability matcher. Must be
    /// called BEFORE `run()` enters; the operational loop reads the
    /// field directly from `self` and a post-run setter call has no
    /// effect (the loop has already captured the trait object).
    ///
    /// At most one matcher per coordinator — re-installation replaces
    /// the prior matcher silently. Consumer policy lives entirely
    /// behind the trait method; the coordinator's only job is "fire
    /// `ReinjectTask` when `should_reinject` returns true".
    pub fn set_fulfillability_matcher(
        &mut self,
        matcher: Box<dyn crate::fulfillability_matcher::FulfillabilityMatcher<I>>,
    ) {
        self.fulfillability_matcher = Some(matcher);
    }

    /// Hand out a never-before-used secondary id and advance the
    /// monotonic counter. The first call on a freshly-constructed
    /// coordinator returns `secondary-{num_secondaries}` (the prep
    /// phase owns `secondary-0..secondary-{num_secondaries - 1}`).
    ///
    /// Single concern: identity allocation. The caller is responsible
    /// for invoking this from the operational loop (the single
    /// `&mut self` writer); minting from a spawned task would race
    /// against the loop's own borrow and is rejected by the borrow
    /// checker anyway — the doc-line is a reminder for future maintainers
    /// who might be tempted to clone the coordinator into a task.
    pub fn mint_secondary_id(&mut self) -> String {
        let n = self.next_secondary_id;
        self.next_secondary_id += 1;
        super::secondary_id::format_secondary_id(n)
    }

    /// Park the deployment-mode job manager on the coordinator so the
    /// respawn path can submit a fresh secondary job from inside the
    /// operational loop. Must be called AFTER the preparation phase
    /// returns (so the job manager is live) and BEFORE `run()` enters
    /// (so the operational loop sees `Some(_)` from the first iteration).
    ///
    /// Stored type-erased through `Arc<dyn Any + Send + Sync>` to keep
    /// `manager-distributed` decoupled from any specific batch-system
    /// crate. The respawn caller downcasts via
    /// [`Self::slurm_job_manager`] back to the concrete handle it parked.
    pub fn set_slurm_job_manager(&mut self, jm: Arc<dyn Any + Send + Sync>) {
        self.slurm_job_manager = Some(jm);
    }

    /// Install the transport-recovery port (BUG-B reconnect) the submitter
    /// primary hands to its observer tail at relocation. The submitter
    /// never reconnects itself — it stays the primary until it relocates —
    /// so this is pure forward-wiring: when the primary relocates onto a
    /// compute peer and `into_observer_handoff` runs, the observer inherits
    /// this handle and uses it to rebuild dropped `-R` reverse tunnels.
    /// Must be set BEFORE `run()` enters (pre-run wiring contract, same as
    /// [`Self::set_slurm_job_manager`] / [`Self::enable_respawn`]). Absence
    /// leaves the observer with no reconnector (the transport-self-heals
    /// path). See [`crate::observer::reconnect`].
    pub fn set_tunnel_reconnector(&mut self, reconnector: Arc<dyn crate::observer::TunnelReconnector>) {
        self.tunnel_reconnector = Some(reconnector);
    }

    /// Park the job-ledger consult port the primary hands to its observer
    /// tail at relocation (the cluster-empty-verdict sibling of
    /// [`Self::set_tunnel_reconnector`]). The submitter never consults it
    /// itself — pure forward-wiring: when the primary relocates onto a
    /// compute peer and `into_observer_handoff` runs, the observer inherits
    /// this handle and consults squeue for the run's job ids on a long
    /// lost-visibility episode. Must be set BEFORE `run()` enters (same
    /// pre-run wiring contract as `set_tunnel_reconnector`). Absence leaves
    /// the observer with no ledger (the never-terminal report-and-retry
    /// path). See [`crate::observer::job_ledger`].
    pub fn set_job_ledger_probe(&mut self, probe: Arc<dyn crate::observer::JobLedgerProbe>) {
        self.job_ledger_probe = Some(probe);
    }

    /// Park the upload-action port (#336 P1) the in-process setup executor
    /// uses to perform a setup task's file upload, and which the primary
    /// hands to its observer tail at relocation (the submitter→observer is
    /// the framework auto-staging upload affinity). Must be set BEFORE
    /// `run()` enters (same pre-run wiring contract as
    /// [`Self::set_tunnel_reconnector`]). Absence leaves the executor with
    /// no uploader — a setup task carrying an upload-file ref then fails as
    /// a wiring error (a no-ref task is unaffected: it no-op-succeeds). See
    /// [`crate::upload_action`].
    pub fn set_upload_action(&mut self, action: Arc<dyn crate::upload_action::UploadAction>) {
        self.upload_action = Some(action);
    }

    /// Read the parked deployment-mode job manager. Returns `None`
    /// outside the SLURM-pipeline path (in-process / local-channel
    /// pipelines never call [`Self::set_slurm_job_manager`]); the
    /// respawn caller downcasts the inner `Arc<dyn Any + Send + Sync>`
    /// back to its concrete type at the call site.
    pub fn slurm_job_manager(&self) -> Option<&Arc<dyn Any + Send + Sync>> {
        self.slurm_job_manager.as_ref()
    }

    /// Enable the secondary respawn pipeline. `spawner` is the
    /// per-provider [`SecondarySpawner`] (multi-process or SLURM);
    /// `budget` is the per-coordinator caps; `primary_endpoint` and
    /// `primary_pubkey_pem` populate the [`SecondarySpawnSpec`]
    /// fields handed to the spawner per respawn (today's adapters
    /// cache their own copies and ignore the spec values; the
    /// snapshot is held for forward-compat).
    ///
    /// Single concern: install the policy + provider on the
    /// coordinator. Must be called BEFORE `run()` enters (same
    /// pre-run contract as the other registration setters); the
    /// operational loop captures the wiring at run start and never
    /// looks for it elsewhere.
    ///
    /// Absence of this setter leaves the respawn pipeline disabled:
    /// no peer-lifecycle listener is registered and the operational
    /// `select!` arm is structurally unreachable. This matches the
    /// CCD-5 contract — no hot-site `if policy_enabled` checks.
    pub fn enable_respawn(
        &mut self,
        spawner: Arc<dyn SecondarySpawner>,
        budget: RespawnBudget,
        primary_endpoint: String,
        primary_pubkey_pem: String,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.respawn_spawner = Some(spawner);
        self.respawn_budget = Some(budget);
        self.respawn_lifecycle_tx = Some(tx.clone());
        self.respawn_lifecycle_rx = Some(rx);
        self.respawn_primary_endpoint = primary_endpoint;
        self.respawn_primary_pubkey_pem = primary_pubkey_pem;
        // Register the dispatcher listener up-front; the
        // peer-lifecycle dispatcher consumes the listener vector at
        // `run()` start, so the registration MUST land before the
        // run is entered. Same contract as
        // `register_lifecycle_listener` (which this call delegates
        // to under the hood). The listener carries the
        // pending-replacement "awaiting a join" gate so it drops `Added`
        // events while no replacement is pending (the membership-join
        // busy-arm fix).
        let awaiting_join = self.pending_replacements.awaiting_join_gate();
        self.register_lifecycle_listener(respawn_dispatcher_listener(tx, awaiting_join));
    }

    /// Enable the respawn pipeline with the REMOTE execution backend —
    /// the promoted/relocated-primary topology, where the respawn
    /// DECISION runs here but the physical provider lives in the
    /// submitter/observer process (mesh node-id
    /// [`dynrunner_core::SETUP_NODE_ID`], host-id-stable across that
    /// process's primary→observer demotion). Builds a
    /// [`super::respawn::RemoteSecondarySpawner`] over this
    /// coordinator's own mesh egress and installs it through the SAME
    /// [`Self::enable_respawn`] wiring the local providers use — one
    /// decision path, two execution backends behind one trait.
    ///
    /// Invoked by [`Self::seed_from_promotion_snapshot`] when the
    /// restored ledger carries a replicated respawn policy; same
    /// pre-`run` contract as `enable_respawn`. A no-op when a spawner
    /// is already installed (the submitter's local provider wins — the
    /// pre-relocation window must keep calling the provider directly).
    pub fn enable_respawn_remote(&mut self, budget: RespawnBudget) {
        if self.respawn_spawner.is_some() {
            return;
        }
        let pending = super::respawn::RemoteRespawnPending::default();
        let spawner = super::respawn::RemoteSecondarySpawner::new(
            self.client.clone(),
            dynrunner_protocol_primary_secondary::PeerId::from(dynrunner_core::SETUP_NODE_ID),
            self.config.node_id.clone(),
            pending.clone(),
        );
        self.remote_respawn_pending = Some(pending);
        // The spec's endpoint/pubkey snapshot is unused by the remote
        // providers (the SLURM wrapper generator captures its own
        // deployment context; a respawned secondary fetches its run
        // config — argv AND trust anchor — over the peer mesh at cold
        // start), so the promoted primary passes empty strings rather
        // than inventing values it does not hold.
        self.enable_respawn(Arc::new(spawner), budget, String::new(), String::new());
    }

    /// Install the liveness-beacon ping receiver (from a
    /// [`crate::liveness::LivenessListener`] the run boundary bound on
    /// this node's runtime). Must be called BEFORE `run()` enters — the
    /// operational loop takes it out at run start, mirroring
    /// `respawn_lifecycle_rx`. Each forwarded node-id refreshes that
    /// secondary's death-clock as the UNION half (beacon OR mesh frame).
    pub fn set_liveness_ping_rx(&mut self, rx: tokio::sync::mpsc::UnboundedReceiver<String>) {
        self.liveness_ping_rx = Some(rx);
    }

    /// A clone of the PRIMARY→secondaries beacon-target cell. The run
    /// boundary hands this to [`crate::liveness::LivenessBeacon::spawn`] so
    /// the dedicated beacon thread reads the secondary-address SET the
    /// coordinator publishes into it (on each roster change). Mirrors the
    /// secondary's `beacon_target()` accessor.
    pub fn beacon_target(&self) -> crate::liveness::BeaconTarget {
        self.beacon_target.clone()
    }

    /// Install the node-scoped peer→liveness-address book (a clone of the
    /// one the co-located `SecondaryCoordinator` populated from `PeerInfo`).
    /// Called by the run boundary BEFORE `run()`. The promoted primary reads
    /// it to resolve its secondaries' raw beacon addresses when building its
    /// `beacon_target` set — it observes no `PeerInfo` of its own.
    pub fn set_peer_liveness_addrs(&mut self, book: crate::liveness::PeerLivenessAddrs) {
        self.peer_liveness_addrs = book;
    }

    /// Install the PRIMARY's own dedicated-thread liveness beacon handle.
    /// Called by the run boundary AFTER it spawns the beacon (with this
    /// coordinator's [`Self::beacon_target`]), BEFORE `run()`. Held for the
    /// primary's lifetime so its `Drop` joins the beacon thread at teardown.
    /// The promoted primary publishes its initial recipient set from the
    /// hydrated roster at operational entry; subsequent roster changes
    /// republish it.
    pub fn set_primary_beacon(&mut self, beacon: crate::liveness::LivenessBeacon) {
        self.primary_beacon = Some(beacon);
    }

    /// Clone of the cross-thread `PrimaryCommand` sender. Callers
    /// (PyO3 `PrimaryHandle`, future Rust-side control planes)
    /// clone this BEFORE invoking `run()` so they have an ingress
    /// for "from outside the operational loop, please apply this
    /// mutation". The sender itself is `Clone` and `Send` so the
    /// returned handle is freely passable across threads / async
    /// runtimes.
    pub fn command_sender(&self) -> tokio_mpsc::Sender<PrimaryCommand<I>> {
        self.command_tx.clone()
    }

    /// Swap the internal command-channel pair for an externally-
    /// supplied one. The PyO3 layer uses this so the
    /// `PrimaryHandle` it exposes to Python at `__init__` time is
    /// the same channel the (later-constructed) `PrimaryCoordinator`
    /// reads from — without this, the channel created in `new()`
    /// can't be reached from Python before `run()` starts because
    /// the coordinator itself is built inside the detached tokio
    /// runtime.
    ///
    /// Must be called BEFORE `run()` enters the operational loop;
    /// calling it after the loop has taken the receiver out (via
    /// `command_rx.take()`) replaces the stored-back receiver but
    /// the loop has already moved on to the local copy. The PyO3
    /// surface enforces this with the
    /// `set_unfulfillable_reinject_max_per_task` setter's
    /// "before run() only" contract; the channel-swap is on the
    /// same contract.
    pub fn replace_command_channel(
        &mut self,
        tx: tokio_mpsc::Sender<PrimaryCommand<I>>,
        rx: tokio_mpsc::Receiver<PrimaryCommand<I>>,
    ) {
        self.command_tx = tx;
        self.command_rx = Some(rx);
    }

    /// Pre-register the per-phase lifecycle callbacks on a primary that
    /// is constructed outside the `run()` argument path.
    ///
    /// The bootstrap `run()` path takes these as arguments. A primary
    /// built ahead of `run()` (PHASE-C-SEAM[C5]: the `Process`
    /// construction site) sets them on the coordinator before spawning it
    /// instead. The operational loop and finalize tail read
    /// `self.on_phase_*` directly, so both paths fire the same
    /// `on_phase_start` / `on_phase_end` callbacks.
    pub fn register_phase_lifecycle_callbacks(
        &mut self,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) {
        self.on_phase_start = Some(on_phase_start);
        self.on_phase_end = Some(on_phase_end);
    }

    /// Wire the shared [`PhaseHookRaiseLatch`] the [`on_phase_end`] hook
    /// closure records raises into, so the cascade can read it and
    /// surface a consumer-hook raise as a fatal run failure.
    ///
    /// Pre-run setter (same contract as the other `register_*` / `set_*`
    /// installers): the caller (pyo3) creates ONE latch, builds the
    /// `on_phase_end` closure against a clone of it, and installs the
    /// other clone here BEFORE `run`. Only the real-primary paths wire a
    /// latch; callers that leave the default detached latch in place keep
    /// the legacy warn-and-continue (the closure records into a latch
    /// nobody reads).
    ///
    /// [`on_phase_end`]: Self::on_phase_end
    pub fn set_phase_hook_raise_latch(&mut self, latch: PhaseHookRaiseLatch) {
        self.phase_hook_raise_latch = latch;
    }

    /// Record that the consumer's `on_run_start` lifecycle hook RAISED on
    /// the promoted-primary path, so the run surfaces a fatal exit
    /// instead of absorbing the failure into a false-green completion.
    ///
    /// Pre-run setter (same contract as the other `set_*` installers):
    /// the pyo3 promotion recipe fires `on_run_start` synchronously
    /// BEFORE `run_consuming` and, on a raise, calls this with the raise
    /// reason. `run_pipeline` reads it once at the post-connection abort
    /// gate (`fire_pre_run_hook_abort`), broadcasts the replicated
    /// `RunAborted` verdict to the connected fleet, and returns
    /// `RunError::FatalPolicyExit` — the SAME deliberate consumer-policy
    /// abort an `on_phase_end` raise surfaces, and the promoted-path twin
    /// of the cold-start path's `?`-propagation of an `on_run_start`
    /// raise out before `run()`. First-write-wins (idempotent): a second
    /// record in the same run does not overwrite the originating cause.
    pub fn record_pre_run_hook_abort(&mut self, reason: String) {
        if self.pre_run_hook_abort.is_none() {
            self.pre_run_hook_abort = Some(reason);
        }
    }

    /// The pending consumer pre-run hook abort reason, if one was recorded
    /// (the directive the abort gate `fire_pre_run_hook_abort` consumes).
    /// Lets the caller that installed the directive confirm it landed
    /// before handing the coordinator off to `run_consuming` — the pyo3
    /// promotion recipe reads it back to prove a raising `on_run_start`
    /// reached the coordinator rather than being absorbed.
    pub fn pending_pre_run_hook_abort(&self) -> Option<&str> {
        self.pre_run_hook_abort.as_deref()
    }

    /// Install the consumer custom-message hook (F5) BEFORE `run` (the
    /// same pre-run-setter contract as the other `register_*` / `set_*`
    /// installers). The pyo3 layer builds the closure off the duck-typed
    /// `TaskDefinition.custom_message_handler` attribute — and only
    /// installs one when the attribute exists, so a `None` field is the
    /// "consumer has no handler" signal the dispatch decision consumes.
    pub fn set_custom_message_handler(&mut self, handler: OnCustomMessage) {
        self.on_custom_message = Some(handler);
    }

    /// Register the consumer's discovery policy + phase graph on a
    /// relocated (mode-2) / `--source-already-staged` local primary BEFORE
    /// `run`. Mirrors [`Self::register_phase_lifecycle_callbacks`]: the pyo3
    /// recipe builds the policy closure (the same `discover_items` excursion
    /// the secondary used) and hands it here; [`Self::discover_on_promotion`]
    /// takes it on the single discovery fire.
    ///
    /// Inert unless the CRDT declares discovery `Owed` (the mode-2 seed
    /// originates `DiscoveryDebtDeclared` before relocate; an in-process
    /// pre-staged local primary seeds it on the local CRDT). A primary that
    /// never owes discovery (cold mode-1 / legacy) ignores a registered
    /// policy entirely.
    pub fn register_setup_discovery(&mut self, discovery: crate::discovery::SetupDiscovery<I>) {
        self.setup_discovery = Some(discovery);
    }

    /// Wire the shared arm-stats bridge to the off-runtime
    /// [`crate::runtime_watchdog`] (called at node bootstrap, before `run`).
    /// The operational loop then publishes its live arm stats into the cell on
    /// entry and clears them on exit, so a freeze dump names THIS loop's hot
    /// arm. Observation-only; no behaviour change. See
    /// [`crate::oploop_instrumentation::OpLoopArmStatsCell`].
    pub fn set_op_loop_arm_stats_cell(
        &mut self,
        cell: crate::oploop_instrumentation::OpLoopArmStatsCell,
    ) {
        self.op_loop_arm_stats_cell = Some(cell);
    }

    /// Register the set of phases the consumer declared `may_be_empty`
    /// (`PhaseSpec.may_be_empty`), before `run()`. The seed originators emit
    /// it as `ClusterMutation::PhaseMayBeEmptySet` (paired with
    /// `PhaseDepsSet`) so the empty-drain proceed-or-fail policy sees the
    /// opt-out on every node, including a promoted primary. Same
    /// before-`run()` registration contract as
    /// [`Self::register_setup_discovery`]; empty (no call) on the common
    /// no-opt-out run.
    pub fn register_phase_may_be_empty(
        &mut self,
        phases: impl IntoIterator<Item = PhaseId>,
    ) {
        self.phase_may_be_empty_decl = phases.into_iter().collect();
    }

    /// Set the per-task budget cap for
    /// `PrimaryCommand::ReinjectTask` after construction. The CLI and
    /// PyO3 surfaces wire this through to the underlying
    /// `PrimaryConfig` field so the live coordinator and the
    /// CLI-supplied `--unfulfillable-reinject-max-per-task=N` flag
    /// stay in lockstep. Idempotent if the existing value matches;
    /// callable any time before `run()` enters the operational
    /// loop (the loop's `select!` reads the field directly).
    pub fn set_unfulfillable_reinject_max_per_task(&mut self, max: Option<u32>) {
        self.config.unfulfillable_reinject_max_per_task = max;
    }

    /// True iff `secondary_id` is currently in backpressure backoff
    /// (recently returned "No idle worker available" and the backoff
    /// hasn't expired). Used by both the kickstart and the
    /// TaskRequest path to skip dispatch onto unresponsive
    /// secondaries.
    pub(super) fn is_backpressured(&self, secondary_id: &str) -> bool {
        self.backpressured_secondaries
            .get(secondary_id)
            .is_some_and(|t| Instant::now() < *t)
    }

    /// True while the per-phase OOM retry bucket is actively
    /// reinjecting + draining. Sole writer: `try_run_phase_retry_bucket`
    /// (set true on `BucketKind::Oom` entry, reset on its `Ok(false)`
    /// returns). Read by the dispatch-shape pipeline. See the field
    /// doc on `single_worker_mode` for the user-spec rationale.
    pub(super) fn single_worker_mode(&self) -> bool {
        self.single_worker_mode
    }

    /// Secondary-local worker id (0-based) for the worker at index
    /// `worker_idx` in `self.workers`. Workers are stored grouped by
    /// secondary in `self.workers` (initial-assignment populated
    /// order); the local id is "position among same-secondary
    /// predecessors in the Vec".
    ///
    /// Single concern: index translation. Used by the dispatch-shape
    /// pipeline so OOM-bucket single-worker masking can read the
    /// secondary-local id without each call site re-doing the linear
    /// scan. The two existing call sites — `dispatch_to_idle_workers`
    /// and `handle_task_request` — already computed the same value
    /// inline; centralising keeps the masking site and the wire-
    /// emitted `local_worker_id` in lockstep.
    pub(super) fn local_worker_id_in_secondary(&self, worker_idx: usize) -> u32 {
        let sec_id = self.workers[worker_idx].secondary_id.as_str();
        self.workers[..worker_idx + 1]
            .iter()
            .filter(|w| w.secondary_id == sec_id)
            .count() as u32
            - 1
    }

    /// Inverse of [`Self::local_worker_id_in_secondary`]: resolve the
    /// stable `(secondary_id, local_worker_id)` identity to the worker's
    /// CURRENT Vec index, or `None` if no such slot exists (the secondary
    /// died, or the local id is out of range). This is THE single
    /// identity-to-position resolution path: every consumer that holds a
    /// stable `(secondary_id, local_worker_id)` and needs to touch the
    /// live `self.workers[..]` entry routes through here rather than
    /// re-deriving the per-secondary running count inline.
    ///
    /// Single concern: identity translation. Critically, the result is
    /// recomputed against the LIVE Vec on every call, so a positional
    /// index can never be cached past a `self.workers.retain(..)` death
    /// compaction — the desync that a stored `Vec` index suffered.
    pub(super) fn worker_idx_for(&self, secondary_id: &str, local_worker_id: u32) -> Option<usize> {
        let mut local_idx: u32 = 0;
        for (idx, w) in self.workers.iter().enumerate() {
            if w.secondary_id == secondary_id {
                if local_idx == local_worker_id {
                    return Some(idx);
                }
                local_idx += 1;
            }
        }
        None
    }

    /// True iff the worker at `worker_idx` must be skipped this
    /// dispatch tick. Composes the reasons-to-skip the dispatch
    /// pipeline knows about today:
    ///
    ///   * The worker's secondary is a half-joined member: it never
    ///     confirmed its peer-mesh leg
    ///     ([`Self::member_mesh_confirmed`]). PROACTIVE dispatch only — a
    ///     member whose mesh leg is unformed silently swallows its
    ///     terminals on the half-formed egress leg, so PUSHING work to
    ///     it strands the task and wedges the phase barrier (the
    ///     run_20260610_105906 strand; the first-dispatch variant is the
    ///     run_20260610_144905 #360 strand).
    ///   * The worker's secondary is in backpressure backoff
    ///     ([`Self::is_backpressured`]).
    ///   * OOM-bucket single-worker mode is active and this is not
    ///     worker 0 of its secondary ([`Self::single_worker_mode`]).
    ///
    /// Single concern: the dispatch-pipeline's "skip this worker"
    /// decision. Adding another reason-to-skip lands here, not as a
    /// parallel `if` at every call site. The call sites
    /// (`dispatch_to_idle_workers` + `handle_task_request`) stay
    /// agnostic to either policy.
    ///
    /// `bypass_backpressure` lifts ONLY the per-secondary backoff
    /// reason — never the OOM single-worker mask (that one is
    /// correctness for memory-pressed retries, not a transient
    /// rate-limit). A recheck driven by a genuine
    /// [`crate::worker_signal::WorkerMgmtSignal::TasksAdded`] passes
    /// `true`: circumstances changed (new work entered the pool, or a
    /// worker freed elsewhere), so a freed slot on a recently-
    /// backpressured secondary is a legitimate dispatch target again.
    /// The per-`TaskRequest` path and the periodic/non-signal kickstart
    /// pass `false` so a secondary that just said "no idle worker"
    /// isn't immediately re-hammered by its own request retry.
    ///
    /// `request_driven` lifts the mesh-confirmation gate: a
    /// `TaskRequest` that ARRIVED is itself direct proof the member's
    /// uplink to the primary delivers (the half-joined strand member
    /// could NOT send requests — they swallowed on the same wedged leg —
    /// so it never reaches this path). Honouring a request that
    /// demonstrably reached us can never strand on an unreachable
    /// member, so the reactive caller (`handle_task_request`) passes
    /// `true`. The PROACTIVE caller (`dispatch_to_idle_workers`, which
    /// PUSHES work to an idle worker that did NOT ask) passes `false`:
    /// that is the bypass the production strand rode, and the gate
    /// withholds work from a half-joined member there.
    pub(super) fn should_skip_worker_for_dispatch(
        &mut self,
        worker_idx: usize,
        bypass_backpressure: bool,
        request_driven: bool,
    ) -> bool {
        let sec_id = self.workers[worker_idx].secondary_id.as_str();
        // Mesh-confirmation gate — PROACTIVE push only (a request that
        // arrived is its own proof of a working leg, so `request_driven`
        // lifts it). An unconfirmed member is unassignable to a proactive
        // push until a `MeshReady` lands (`member_mesh_confirmed` flips
        // true) and recovers it — including its FIRST dispatch (#360: a
        // never-dispatched member's half-formed leg swallows terminals
        // exactly like a dispatched one's). The veto is named per the
        // silent-branch rule, ONE WARN per member per unconfirmed spell
        // (every member passes through this window during bring-up and
        // the predicate runs per-worker per-recheck — unthrottled it
        // floods); repeats are DEBUG. `handle_mesh_ready` re-arms the
        // WARN when the member confirms.
        if !request_driven && !self.member_mesh_confirmed(sec_id) {
            let sec_id = sec_id.to_string();
            let worker_id = self.workers[worker_idx].worker_id;
            if self.mesh_gate_veto_warned.insert(sec_id.clone()) {
                tracing::warn!(
                    secondary = %sec_id,
                    worker_id,
                    "member remains unassignable until its mesh leg confirms \
                     (no MeshReady received); skipping proactive dispatch to \
                     avoid stranding the task on its half-formed egress leg \
                     (further vetoes for this member logged at DEBUG until it \
                     confirms)"
                );
            } else {
                tracing::debug!(
                    secondary = %sec_id,
                    worker_id,
                    "member still unconfirmed (no MeshReady received); \
                     skipping proactive dispatch"
                );
            }
            return true;
        }
        if !bypass_backpressure && self.is_backpressured(sec_id) {
            return true;
        }
        if self.single_worker_mode && self.local_worker_id_in_secondary(worker_idx) != 0 {
            return true;
        }
        false
    }

    /// Secondary ids ordered by total advertised memory descending.
    /// Ties broken stably by id (lexicographic) so the OOM-bucket
    /// per-task `preferred_secondaries` assignment is reproducible
    /// across re-entries. Secondaries with no `memory` resource
    /// advertised sort last (treated as zero).
    ///
    /// Single concern: snapshot the cluster's per-node memory
    /// ranking at OOM-bucket entry. Re-sorting per iteration is
    /// explicitly NOT done — a secondary that dies mid-bucket will
    /// naturally fail dispatch, and the next bucket entry re-samples.
    /// Returns owned `String`s so callers can carry the snapshot
    /// across `&mut self` reinject/dispatch calls without lifetime
    /// surgery.
    pub(super) fn secondaries_sorted_by_memory_desc(&self) -> Vec<String> {
        let mem_kind = dynrunner_core::ResourceKind::memory();
        let mut entries: Vec<(String, u64)> = self
            .secondaries
            .iter()
            .map(|(id, state)| {
                let mem = state
                    .resources()
                    .iter()
                    .find(|r| r.kind == mem_kind)
                    .map(|r| r.amount)
                    .unwrap_or(0);
                (id.clone(), mem)
            })
            .collect();
        // Sort by (memory DESC, id ASC). Stable on id so a fleet with
        // multiple equal-memory nodes assigns retries deterministically
        // across re-runs.
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.into_iter().map(|(id, _)| id).collect()
    }

    /// Borrow the pending pool. Panics if called before `run()` has
    /// initialised it — every internal call site is inside the run
    /// pipeline so this is a contract violation, not a runtime path.
    pub(super) fn pool(&self) -> &PendingPool<I> {
        self.pending
            .as_ref()
            .expect("PendingPool initialised at run() start")
    }

    /// Mutably borrow the pending pool.
    pub(super) fn pool_mut(&mut self) -> &mut PendingPool<I> {
        self.pending
            .as_mut()
            .expect("PendingPool initialised at run() start")
    }

    /// The pool's earliest queued-task re-dispatch backoff expiry (see
    /// [`PendingPool::next_dispatch_backoff_expiry`]), or `None` when
    /// nothing is parked OR the pool is not yet initialised (the
    /// operational loop's backoff wake arm then parks forever, same as
    /// every other disabled-arm shape).
    pub(super) fn next_task_dispatch_backoff_expiry(&mut self) -> Option<std::time::Instant> {
        self.pending
            .as_mut()
            .and_then(|p| p.next_dispatch_backoff_expiry())
    }

    /// Build the dispatch-shape worker view for the worker at
    /// `worker_idx`. The pipeline is:
    ///
    ///   0. The GRACEFUL-ABORT scheduling gate — under the replicated
    ///      `graceful_abort_requested` freeze the view is EMPTIED, so no
    ///      scheduler ever sees a candidate and no work leaves the ready
    ///      pool toward any worker.
    ///   1. `pool.view_for_worker(global_wid, Some(&soft_pred))` —
    ///      priority-ordered eligible items with soft
    ///      `preferred_secondaries` tie-break.
    ///   2. Strict `preferred_secondaries` filter — ACTIVE iff
    ///      `single_worker_mode()` is true. Drops items whose
    ///      non-empty `preferred_secondaries` list omits this
    ///      worker's secondary.
    ///   3. `cap_filter_view` — drops items over a per-type cap.
    ///
    /// Single concern: the dispatch-pipeline's view-construction
    /// shape. Outside the OOM bucket step (2) is a no-op; inside
    /// it is load-bearing. ALL THREE dispatch sites
    /// (`dispatch_to_idle_workers`, `handle_task_request`, and
    /// `perform_initial_assignment`) call this once and consume the
    /// returned view directly — which is what makes step 0 THE single
    /// seam where new work stops under a graceful abort: every path
    /// from the ready pool to a worker constructs its view here, so the
    /// freeze needs no per-call-site checks. In-flight bookkeeping
    /// (completions, retries entering the pool, phase cascades) is
    /// untouched — only DISPATCH is frozen.
    pub(super) fn dispatch_view_for_worker(
        &self,
        worker_idx: usize,
    ) -> dynrunner_scheduler_api::WorkerView<'_, I> {
        let global_wid = self.workers[worker_idx].worker_id;
        let secondary_id = self.workers[worker_idx].secondary_id.as_str();
        let soft_predicate =
            preferred_secondaries::apply_preferred_secondaries_predicate::<I>(secondary_id);
        let pool = self.pool();
        let view = pool.view_for_worker(global_wid, Some(&soft_predicate));
        // Bring-up reservation scope (#494): while the formation window is
        // open, a member's view is capped so it can never drain a STILL-
        // FORMING (unconfirmed) member's reserved share — the late
        // confirmers' slices the 14/14/0 pack ate. A task reserved to a
        // confirmed holder, or to THIS member, or unreserved, is still
        // admitted (a confirmed holder's overflow is free for the formed
        // fleet, including a mid-run joiner). The pool owns the holder
        // map; the per-holder mesh-confirmation fact is THIS coordinator's
        // (`member_mesh_confirmed`), supplied at the seam. Inert (admits
        // everything) once the window closes OR on the local single-node
        // manager (which never opens one).
        let holder_confirmed = |holder: &str| self.member_mesh_confirmed(holder);
        let view =
            view.filter(|item| pool.reservation_admits(secondary_id, item, &holder_confirmed));
        // Step 0 — the dispatch freezes. Emptying the TYPED view
        // (rather than branching at the call sites) keeps every consumer
        // shape-oblivious: the scheduler sees zero candidates and the
        // dispatch loops fall through their existing `is_empty()` skips.
        // Two latches share the seam:
        //   * the replicated graceful-abort freeze — a promoted primary
        //     inherits it via its snapshot, so it survives failover with
        //     no extra plumbing (no-redo law);
        //   * the node-local run-fail freeze (`run_fail_dispatch_freeze`,
        //     latched SYNCHRONOUSLY by the `emit_run_fail_signal`
        //     chokepoint) — this primary is about to break out of its
        //     loop with a failure verdict, so no assignment may escape
        //     the emit→break window. Node-local is sufficient: the
        //     verdict path broadcasts `RunAborted`, which stands every
        //     peer down.
        if self.cluster_state.graceful_abort_requested() || self.run_fail_dispatch_freeze {
            return view.filter(|_| false);
        }
        let view = self.apply_strict_preferred_secondaries(view, secondary_id);
        self.cap_filter_view(view)
    }

    /// Apply the strict-preferred-secondaries filter to `view` iff
    /// the coordinator is in OOM-bucket single-worker mode; otherwise
    /// return `view` unchanged.
    ///
    /// Kept as a tiny standalone helper so the active-vs-inactive
    /// gating lives in exactly one place and the dispatch-pipeline
    /// helper above reads as a flat sequence of steps.
    fn apply_strict_preferred_secondaries<'p>(
        &self,
        view: dynrunner_scheduler_api::WorkerView<'p, I>,
        secondary_id: &str,
    ) -> dynrunner_scheduler_api::WorkerView<'p, I> {
        if !self.single_worker_mode() {
            return view;
        }
        view.filter(preferred_secondaries::filter_strict_preferred_secondaries::<I>(secondary_id))
    }

    /// Drop the worker view down to the per-type-cap-eligible items.
    /// `None` for an axis means unconstrained; `Some(N)` means at
    /// most `N` items of that type can be in-flight across all
    /// workers. Items whose type's capacity is already reached are
    /// removed from the view so the scheduler never sees them.
    /// Commit a freshly-scheduled LOCAL assignment as one atomic
    /// in-flight-bookkeeping event: reserve the per-type concurrency
    /// slot, move the holding slot `Idle -> Assigned{task_hash}`, AND
    /// record the task in the hash-keyed `in_flight` ledger. Every
    /// local dispatch site (`handle_task_request`,
    /// `dispatch_to_idle_workers`, `perform_initial_assignment`) routes
    /// through here so the three pieces of "this task is now in flight"
    /// state can never diverge — they are written together or not at
    /// all. The `take_selected` that removes the binary from the pool
    /// precedes this call (it yields the owned `task`).
    ///
    /// Reachable only from an `Idle` slot (enforced by
    /// `RemoteWorkerState::assign`'s `debug_assert`); the caller has
    /// already established idleness via the dispatch view / scheduler
    /// decision.
    pub(super) fn commit_assignment(
        &mut self,
        worker_idx: usize,
        task: TaskInfo<I>,
        task_hash: String,
        estimated: ResourceMap,
    ) {
        let secondary_id = self.workers[worker_idx].secondary_id.clone();
        // Record the STABLE secondary-local id (retain-immune), NOT the
        // positional Vec index — the terminal path re-resolves it via
        // `worker_idx_for` against the live Vec.
        let local_worker_id = self.local_worker_id_in_secondary(worker_idx);
        let phase = task.phase_id.clone();
        self.reserve_type_slot(&task.type_id);
        // Live dispatch: THIS primary just sent the `TaskAssignment`, so
        // the occupancy is known-live — `Dispatched` provenance. The
        // failover-resume reconciliation never touches such a slot.
        self.workers[worker_idx].assign(
            task_hash.clone(),
            task.clone(),
            estimated,
            crate::primary::SlotProvenance::Dispatched,
        );
        self.in_flight.insert(
            task_hash,
            InFlightEntry {
                phase,
                secondary_id,
                local_worker_id: Some(local_worker_id),
                task,
            },
        );
    }

    /// Undo a `commit_assignment` whose `TaskAssignment` send failed:
    /// the task was never delivered, so no terminal will ever arrive
    /// for it. Symmetric inverse of `commit_assignment` — release the
    /// type slot, vacate the slot to `Idle`, drop the ledger entry —
    /// then the caller requeues the binary into the pool. Leaving any
    /// of the three would strand the slot, the ledger, or the type
    /// budget (the asm-tokenizer "33 in_flight / active=0" jam class).
    pub(super) fn rollback_assignment(
        &mut self,
        worker_idx: usize,
        task_hash: &str,
        type_id: &dynrunner_core::TypeId,
    ) {
        self.release_type_slot(type_id);
        self.workers[worker_idx].vacate();
        self.in_flight.remove(task_hash);
    }

    /// Seed an inherited in-flight task into the SAME hash-keyed ledger
    /// at hydration. `local_worker_id` is the secondary-local worker id
    /// the replicated `TaskState::InFlight { worker }` recorded at the
    /// originating dispatch — the SAME id `commit_assignment` writes on
    /// the live path (`local_worker_id_in_secondary`), so the inherited
    /// entry's stable `(secondary_id, local_worker_id)` holder key
    /// resolves through [`Self::worker_idx_for`] onto the matching
    /// reconstructed `RemoteWorkerState` slot. The slot itself is moved
    /// `Idle -> Assigned` by `reconstruct_workers_from_cluster_state`
    /// (the roster × occupancy crossing); this records the ledger half so
    /// the broadcast `TaskComplete` / `TaskFailed` finds the entry BY
    /// HASH and `free_slot_on_terminal` frees the held slot. Folds in the
    /// deleted `pre_owned_in_flight` concept: the terminal cascade reads
    /// this entry exactly like a locally-dispatched one.
    pub(super) fn seed_inflight(
        &mut self,
        task_hash: String,
        phase: PhaseId,
        secondary_id: String,
        local_worker_id: u32,
        task: TaskInfo<I>,
    ) {
        // Mirror `commit_assignment`'s per-type slot reservation so the
        // inherited InFlight task's eventual terminal release
        // (`free_slot_on_terminal`'s `saturating_sub`) is symmetric. Without
        // this the cap counter underflows-to-clamped-0 and the type cap
        // desyncs (over-dispatch past `max_concurrent_per_type`) after
        // promotion. Read the type id before the move; `reserve_type_slot`
        // no-ops for uncapped types.
        let type_id = task.type_id.clone();
        self.in_flight.insert(
            task_hash,
            InFlightEntry {
                phase,
                secondary_id,
                local_worker_id: Some(local_worker_id),
                task,
            },
        );
        self.reserve_type_slot(&type_id);
    }

    /// THE single terminal-free helper. Given an inbound terminal's
    /// `(secondary_id, worker_id, task_hash)`, free the holding slot
    /// back to `Idle`, release the per-type concurrency slot, AND
    /// remove the `in_flight` ledger entry — the symmetric inverse of
    /// `commit_assignment`. Returns the freed `InFlightEntry` (phase /
    /// secondary / task) so the caller runs ONLY the per-phase cascade
    /// (`note_item_*`); the type-slot release is owned here so the
    /// caller never has to know whether a slot was reserved.
    ///
    /// Returns `None` (a safe no-op) when:
    ///   - the addressed slot holds a DIFFERENT hash (the worker was
    ///     already reassigned to a later task `Y`; this terminal is a
    ///     stale/reordered completion for the prior task `X`), or
    ///   - the hash is absent from the ledger entirely (already
    ///     terminal / never tracked).
    ///
    /// Every live ledger entry now carries `local_worker_id = Some(..)`
    /// against a holding slot — locally-dispatched (`commit_assignment`)
    /// and inherited-on-failover (`seed_inflight`, whose slot is
    /// reconstructed by `reconstruct_workers_from_cluster_state`) alike.
    /// The `local_worker_id = None` arm therefore survives only as a
    /// defensive safe-no-op: an entry with no resolvable holder is
    /// removed from the ledger and returned without a slot vacate or a
    /// type-slot release — the deleted `pre_owned_in_flight` branch's
    /// "no local type-slot was ever taken" contract, now expressed
    /// through the one ledger.
    ///
    /// Because the slot's `task_hash` IS the held-task identity (the
    /// ledger key), a reassigned slot can NEVER be freed by a prior
    /// task's terminal: reassignment-before-terminal is unreachable.
    ///
    /// The holding slot is found by re-resolving the ledger entry's
    /// STABLE `(secondary_id, local_worker_id)` to a live Vec index via
    /// [`Self::worker_idx_for`] — recomputed on every call, so a
    /// sibling secondary's death (`self.workers.retain(..)` compacts the
    /// Vec) can never leave this pointing at the wrong worker or out of
    /// bounds. The inbound wire `worker_id` is retained for diagnostics;
    /// the ledger entry is the authoritative holder record.
    pub(super) fn free_slot_on_terminal(
        &mut self,
        secondary_id: &str,
        worker_id: u32,
        task_hash: &str,
    ) -> Option<InFlightEntry<I>> {
        // The ledger is the single source of truth. If the hash isn't
        // tracked, the task is not (or no longer) in flight — nothing
        // to free.
        let holder = match self.in_flight.get(task_hash) {
            Some(e) => e.local_worker_id.map(|lw| (e.secondary_id.clone(), lw)),
            None => {
                tracing::trace!(
                    secondary = %secondary_id,
                    worker_id,
                    task_hash = %task_hash,
                    "terminal for non-tracked hash; ignoring"
                );
                return None;
            }
        };

        match holder {
            // Locally-dispatched entry: a slot holds it. Resolve the
            // STABLE holder identity to a live index, then verify the
            // addressed slot still holds THIS hash before freeing — a
            // slot that has moved on to a later task must not be
            // vacated by a stale terminal.
            Some((holder_secondary, holder_local_id)) => {
                let idx = match self.worker_idx_for(&holder_secondary, holder_local_id) {
                    Some(idx) => idx,
                    // The holding worker is gone (its secondary died and
                    // the slot was dropped by the requeue path) yet a
                    // ledger entry survived. This is not the routine
                    // recovery path (which removes the entry), so treat
                    // it as a stale terminal for a slot that no longer
                    // exists: leave the ledger untouched and no-op.
                    None => {
                        tracing::trace!(
                            secondary = %secondary_id,
                            worker_id,
                            task_hash = %task_hash,
                            "terminal for hash whose holding slot no longer exists; ignoring"
                        );
                        return None;
                    }
                };
                let held_matches = matches!(
                    &self.workers[idx].state,
                    SlotState::Assigned { task_hash: h, .. } if h == task_hash
                );
                if !held_matches {
                    tracing::trace!(
                        secondary = %secondary_id,
                        worker_id,
                        task_hash = %task_hash,
                        "terminal for non-held hash; ignoring"
                    );
                    return None;
                }
                self.workers[idx].state = SlotState::Idle;
                let entry = self.in_flight.remove(task_hash)?;
                self.release_type_slot(&entry.task.type_id);
                Some(entry)
            }
            // Inherited (pre-owned) entry: no local slot, no reserved
            // type slot. Remove the ledger entry and return it — the
            // cascade still decrements the correct phase's counter.
            None => self.in_flight.remove(task_hash),
        }
    }

    /// Recover every in-flight task targeting `secondary_id` when that
    /// secondary dies: requeue each task into the pool (which
    /// decrements its phase's in-flight counter) and drop the ledger
    /// entry. Covers BOTH locally-dispatched (a slot held it — the
    /// slot is dropped separately by the caller) and inherited
    /// (pre-owned, no slot) tasks through the ONE ledger, mirroring the
    /// reference `check_peer_timeouts` recovery.
    ///
    /// Returns one `ClusterMutation::TaskRequeued { hash }` per requeued
    /// task so the async caller broadcasts the `InFlight → Pending`
    /// transition through `apply_and_broadcast_cluster_mutations` in
    /// lockstep with the local pool requeue. This method owns the
    /// in-flight-ledger + pool-side recovery (a sync, data-only concern);
    /// the CRDT replication is owned by the broadcast helper, so the
    /// mutation set is RETURNED rather than emitted here (mirroring the
    /// pool-returns-data / manager-broadcasts split). Without the
    /// returned mutation the local requeue would leave a stale CRDT
    /// `InFlight` that strands the task on failover (`hydrate` routes
    /// `InFlight` to the ledger, not the pool).
    ///
    /// Requeue is NOT a terminal outcome — the task re-enters
    /// `Pending` — so it never touches `completed_tasks`/`failed_tasks`.
    /// The per-type slot IS released (a requeued task no longer occupies
    /// concurrency budget) and `pool.requeue` decrements the phase
    /// in-flight counter, keeping the ledger, the type budget, and the
    /// pool counters consistent.
    pub(super) fn recover_inflight_for_dead_secondary(
        &mut self,
        secondary_id: &str,
    ) -> Vec<ClusterMutation<I>> {
        let hashes: Vec<String> = self
            .in_flight
            .iter()
            .filter(|(_, e)| e.secondary_id == secondary_id)
            .map(|(h, _)| h.clone())
            .collect();
        let mut requeue_mutations = Vec::with_capacity(hashes.len());
        for hash in hashes {
            if let Some(entry) = self.in_flight.remove(&hash) {
                self.release_type_slot(&entry.task.type_id);
                if entry.task.kind.is_reassignable() {
                    // WORK task: recover it to `Pending` for another worker
                    // (`InFlight → Pending`, the dead-secondary requeue).
                    self.pool_mut().requeue(entry.task);
                    requeue_mutations.push(ClusterMutation::TaskRequeued {
                        hash,
                        // Stamped at the origination choke point (apply_locally_for_broadcast).
                        version: Default::default(),
                    });
                } else {
                    // NON-reassignable (a setup task): its executor is its
                    // source-owning member, so the holder's death is
                    // UNRECOVERABLE — drive it to a terminal `Failed`
                    // (NonRecoverable) instead of a requeue. Its dependents
                    // then follow the existing failed-dependency cascade
                    // (loud, no silent loss), exactly as for any other
                    // non-recoverable terminal. The task is NOT returned to
                    // the pool (no `requeue`), so it can never be re-dispatched.
                    requeue_mutations.push(ClusterMutation::TaskFailed {
                        hash,
                        kind: dynrunner_core::ErrorType::NonRecoverable,
                        error: "setup-task executor (its source-owning member) died; \
                                setup tasks are non-reassignable — terminal unrecoverable"
                            .to_string(),
                        // Stamped at the origination choke point (apply_locally_for_broadcast).
                        version: Default::default(),
                        attempt: Default::default(),
                    });
                }
            }
        }
        requeue_mutations
    }

    /// Reconcile ONE survivor worker's stale `Inherited` slot when its
    /// own live `TaskRequest` re-confirms it is idle: free the slot back
    /// to `Idle`, drop the inherited `in_flight` ledger entry, release the
    /// per-type concurrency slot, and requeue the task into the pool.
    /// Returns the `TaskRequeued` mutation (`InFlight → Pending`) for the
    /// caller to broadcast in lockstep with the local pool requeue (a
    /// stale CRDT `InFlight` would otherwise re-strand the task on a
    /// subsequent failover), or `None` when the addressed slot is not an
    /// inherited-occupancy slot (so the caller leaves it untouched).
    ///
    /// Single concern: the per-worker failover-resume occupancy
    /// reconciliation. The sibling of
    /// [`Self::recover_inflight_for_dead_secondary`] — same ledger +
    /// type-slot + pool-requeue + `TaskRequeued`-origination shape — but
    /// scoped to a SINGLE live worker whose hydrated occupancy proved
    /// stale, and it returns the slot to `Idle` (the worker is alive and
    /// about to take work) rather than dropping it (the dead-secondary
    /// path drops the whole worker). The hash is read from the slot's
    /// `Assigned` state, so the ledger entry and the slot can never
    /// disagree about WHICH task is being reconciled.
    ///
    /// Gated on [`RemoteWorkerState::is_inherited`]: a live `Dispatched`
    /// slot (this primary sent the assignment) is NEVER freed by a bare
    /// `TaskRequest` — the R1 invariant stays intact for the steady-state
    /// / relocated / rc-G2 paths, where a request for a busy slot is a
    /// delayed/duplicate no-op. Only an unconfirmed inherited slot, whose
    /// worker is now positively reporting idle, reconciles.
    ///
    /// TERMINAL VETO (the run_20260610_221140 requeue-vs-complete race):
    /// the requeue consults the replicated terminal ledger ATOMICALLY with
    /// its own decision (both run on the coordinator's single-writer
    /// loop). A hash the CRDT already records terminal — the lost
    /// completion delivered through a received `TaskCompleted` mutation or
    /// a snapshot restore, neither of which frees the inherited slot — is
    /// NEVER requeued: re-queueing completed work re-executes it. The
    /// veto arm returns the hash untouched so the caller settles the
    /// residue through the ONE CRDT-terminal settle path.
    pub(super) fn reconcile_inherited_slot(&mut self, worker_idx: usize) -> InheritedSlotReconcile<I> {
        if !self.workers[worker_idx].is_inherited() {
            return InheritedSlotReconcile::NotInherited;
        }
        // The held hash from the `Assigned` state — the ledger key the
        // inherited entry was seeded under.
        let task_hash = match &self.workers[worker_idx].state {
            SlotState::Assigned { task_hash, .. } => task_hash.clone(),
            SlotState::Idle => return InheritedSlotReconcile::NotInherited,
        };
        // Terminal veto: the requeue heuristic yields to a terminal that
        // has already landed in the replicated ledger. `task_view` (not
        // `task_state`) — a SETTLED hash IS a recorded terminal, and
        // missing it here would re-queue completed work.
        if self
            .cluster_state
            .task_view(&task_hash)
            .is_some_and(|v| v.is_terminal())
        {
            tracing::info!(
                secondary = %self.workers[worker_idx].secondary_id,
                worker_id = self.local_worker_id_in_secondary(worker_idx),
                task_hash = %task_hash,
                "inherited-occupancy requeue VETOED: the replicated ledger \
                 already records a terminal for the held hash (the lost \
                 completion was delivered out-of-band); settling the slot \
                 instead of re-queueing completed work"
            );
            return InheritedSlotReconcile::VetoedByTerminal { task_hash };
        }
        let Some(task) = self.workers[worker_idx].vacate() else {
            return InheritedSlotReconcile::NotInherited;
        };
        // Drop the inherited ledger entry + release the type slot + pool
        // requeue, mirroring `recover_inflight_for_dead_secondary`'s
        // symmetric inverse of a dispatch so the ledger, the type budget,
        // and the phase in-flight counter stay consistent.
        self.in_flight.remove(&task_hash);
        self.release_type_slot(&task.type_id);
        tracing::info!(
            secondary = %self.workers[worker_idx].secondary_id,
            // The SECONDARY-LOCAL worker id — the same namespace every
            // wire-side log ("task assigned" / "task complete") uses, so
            // an operator can correlate this slot with that worker's own
            // terminals. (The incident's `worker=2` vs `worker_id=0`
            // confusion was this log printing the GLOBAL roster id.)
            worker_id = self.local_worker_id_in_secondary(worker_idx),
            task_hash = %task_hash,
            "reconciled stale inherited worker occupancy on live idle \
             re-confirmation: freeing slot and requeueing task (completion \
             was lost during the primary-less failover window)"
        );
        self.pool_mut().requeue(task);
        InheritedSlotReconcile::Requeued(Box::new(ClusterMutation::TaskRequeued {
            hash: task_hash,
            // Stamped at the origination choke point (apply_locally_for_broadcast).
            version: Default::default(),
        }))
    }

    /// Hash-only adapter over [`Self::free_slot_on_terminal`] for a
    /// terminal learned WITHOUT a wire frame (a received `TaskCompleted` /
    /// `TaskFailed` ClusterMutation, or the reconcile veto's settle): the
    /// diagnostics identity is read from the ledger entry's own holder
    /// record rather than a frame's `(secondary_id, worker_id)` fields.
    pub(super) fn free_slot_on_terminal_by_hash(
        &mut self,
        task_hash: &str,
    ) -> Option<InFlightEntry<I>> {
        let (holder_secondary, holder_worker) = match self.in_flight.get(task_hash) {
            Some(e) => (e.secondary_id.clone(), e.local_worker_id.unwrap_or(0)),
            None => return None,
        };
        self.free_slot_on_terminal(&holder_secondary, holder_worker, task_hash)
    }

    /// Reclaim a hash from the QUEUED pool when its terminal lands — the
    /// requeue-then-terminal leg of the run_20260610_221140 race. A
    /// failover-recovery requeue (the inherited-slot reconciliation, or
    /// the dead-secondary recovery for a not-actually-dead holder)
    /// legitimately returned the task to `Pending` BEFORE the lost
    /// terminal's late delivery; the terminal proves the work is done, so
    /// the queued copy must leave the pool or it is re-dispatched and
    /// re-executed (production: re-assigned 25 s later, second terminal at
    /// delivery_seq 2847).
    ///
    /// Composition of the pool's documented primitives: `take_first_match`
    /// removes the queued copy WITHOUT touching counters, and
    /// `mark_in_flight` re-registers the (already-performed) execution so
    /// the caller's `note_item_*` cascade — in-flight decrement, per-task
    /// completion walk, drain transition, phase lifecycle — accounts it
    /// exactly like a normally-held terminal (the requeue had decremented
    /// the in-flight counter; the +1/−1 pair keeps it balanced).
    pub(super) fn reclaim_requeued_on_terminal(&mut self, task_hash: &str) -> Option<TaskInfo<I>> {
        // No pool, no queued copy to reclaim (a terminal landing before
        // `run()` built the pool — same gate `handle_cluster_mutation`'s
        // pool-coherence block uses).
        self.pending.as_ref()?;
        let task = self
            .pool_mut()
            .take_first_match(|t| super::wire::compute_task_hash(t) == task_hash)?;
        self.pool_mut().mark_in_flight(&task.phase_id);
        tracing::info!(
            task_id = %task.task_id,
            phase = %task.phase_id,
            task_hash = %task_hash,
            "terminal arrived for a task sitting QUEUED in the pool (a \
             failover-recovery requeue raced the lost terminal's late \
             delivery): reclaiming it from the queue so the completed work \
             is never re-executed"
        );
        Some(task)
    }

    /// Starvation oracle for the lazy on-demand dead-secondary requeue.
    /// True IFF the ONLY outstanding work is in-flight on silent
    /// secondaries — i.e. an idle worker has nothing it could dispatch,
    /// and the only reason the run isn't done is that silent holders are
    /// sitting on inherited/dispatched in-flight tasks.
    ///
    /// Composed of single-concern reads — no liveness/dispatch policy
    /// `if`-hacks:
    ///   1. `∃ silent secondary` — the liveness module's silent-id set
    ///      ([`Self::silent_secondary_ids`]); empty ⇒ false.
    ///   2. `no queued dispatchable work for active phases` —
    ///      [`PendingPool::has_queued_dispatchable`]. (`is_empty()`/`len()`
    ///      are NOT usable: they fold in-flight + blocked, so they would be
    ///      false/`>0` precisely when silent in-flight work exists.)
    ///   3. `blocked == 0` — [`PendingPool::blocked_len`]. A blocked item
    ///      will become dispatchable once its prereq resolves, so evicting
    ///      a holder now would be premature.
    ///   4. `in_flight non-empty` — there is something to recover (and the
    ///      guard against evicting a healthy secondary near run completion,
    ///      paired with blocked==0).
    ///   5. `every in_flight entry's secondary is silent` — so a non-silent
    ///      secondary making progress is never evicted; if any in-flight
    ///      task is held by a live secondary, the run is still advancing.
    ///
    /// The boundary the dispatch consumer sees is this predicate plus
    /// [`Self::declare_silent_secondaries_dead`]; it never learns how
    /// "silent" or "dispatchable" are computed.
    pub(super) fn only_silent_held_work_remains(&self) -> bool {
        let silent = self.silent_secondary_ids();
        if silent.is_empty() {
            return false;
        }
        if self.pool().has_queued_dispatchable() {
            return false;
        }
        if self.pool().blocked_len() != 0 {
            return false;
        }
        if self.in_flight.is_empty() {
            return false;
        }
        self.in_flight
            .values()
            .all(|e| silent.contains(&e.secondary_id))
    }

    /// The silent secondaries currently holding the only remaining work,
    /// packaged as [`DeadSecondary`] declarations for
    /// [`Self::declare_silent_secondaries_dead`]. Pairs with
    /// [`Self::only_silent_held_work_remains`]: the oracle gates whether to
    /// declare; this enumerates WHOM, reusing the liveness silent-id set
    /// and the recorded keepalive timestamps.
    pub(super) fn silent_held_dead_declarations(&self) -> Vec<super::heartbeat::DeadSecondary> {
        let now = Instant::now();
        self.silent_secondary_ids()
            .into_iter()
            .map(|id| {
                let last_keepalive = self.secondary_keepalives.get(&id).copied().unwrap_or(now);
                super::heartbeat::DeadSecondary {
                    secondary_id: id,
                    last_keepalive,
                }
            })
            .collect()
    }

    pub(super) fn cap_filter_view<'p>(
        &self,
        view: dynrunner_scheduler_api::WorkerView<'p, I>,
    ) -> dynrunner_scheduler_api::WorkerView<'p, I> {
        if self.config.max_concurrent_per_type.is_empty() {
            return view;
        }
        let caps = &self.config.max_concurrent_per_type;
        let in_flight = &self.in_flight_per_type;
        view.filter(|item| match caps.get(&item.type_id) {
            None => true,
            Some(cap) => in_flight.get(&item.type_id).copied().unwrap_or(0) < *cap,
        })
    }

    /// Account for a freshly-dispatched item against its type's
    /// concurrency budget. Paired with `release_type_slot` on
    /// TaskComplete / TaskFailed.
    pub(super) fn reserve_type_slot(&mut self, type_id: &dynrunner_core::TypeId) {
        if !self.config.max_concurrent_per_type.contains_key(type_id) {
            return;
        }
        *self.in_flight_per_type.entry(type_id.clone()).or_insert(0) += 1;
    }

    pub(super) fn release_type_slot(&mut self, type_id: &dynrunner_core::TypeId) {
        if !self.config.max_concurrent_per_type.contains_key(type_id) {
            return;
        }
        if let Some(count) = self.in_flight_per_type.get_mut(type_id) {
            *count = count.saturating_sub(1);
        }
    }

    /// Queue a `StageFile` notification to be sent to `secondary_id`
    /// once the secondary handshake completes. Must be called BEFORE
    /// `run()` (or from outside the run-loop) — once flushed,
    /// subsequent calls happen inline via `notify_stage_file`.
    pub fn queue_stage_file(
        &mut self,
        secondary_id: String,
        file_hash: String,
        content_hash: String,
        src_path: String,
        dest_path: String,
    ) {
        self.pending_stage_files
            .push((secondary_id, file_hash, content_hash, src_path, dest_path));
    }

    /// Number of tasks the cluster recorded as successfully completed.
    ///
    /// Reads through `cluster_state.outcome_counts().succeeded` so the
    /// count is the CRDT-authoritative tally rather than the per-node
    /// `completed_tasks` HashSet — analogous to [`Self::outcome_summary`]
    /// which routes through the same CRDT reader. The `completed_tasks`
    /// HashSet stays authoritative for per-task identity decisions
    /// (dedup on a re-applied `TaskComplete`, the operational-loop exit
    /// gate, the kickstart-suppression check); cross-class *counts* live
    /// one layer up, on the replicated ledger every replica converges
    /// to. This is the same CRDT read every node (authority, peer, or
    /// observer) uses for cross-class count reporting — the per-node
    /// counter mirror that used to diverge from it is gone.
    pub fn completed_count(&self) -> usize {
        self.cluster_state.outcome_counts().succeeded
    }

    /// Number of tasks the cluster recorded as terminally failed
    /// (any failure class — `Recoverable` whose retry budget is
    /// exhausted, `ResourceExhausted`, `NonRecoverable`, or
    /// `Unfulfillable`).
    ///
    /// Same CRDT-routing rationale as [`Self::completed_count`]:
    /// reads through `cluster_state.outcome_counts()` for the
    /// CRDT-authoritative tally rather than the per-node
    /// `failed_tasks` HashSet. Sums the three failure buckets
    /// (`fail_retry + fail_oom + fail_final`) to preserve the
    /// pre-migration semantics of "any task currently in a terminal
    /// failure state".
    pub fn failed_count(&self) -> usize {
        let o = self.cluster_state.outcome_counts();
        o.fail_retry + o.fail_oom + o.fail_final
    }

    /// Per-class outcome breakdown for the coordinator-facing log
    /// lines (`succeeded=… fail_retry=… fail_oom=… fail_final=…`).
    ///
    /// Reads through `cluster_state.outcome_counts()` so the count is
    /// the CRDT-authoritative tally rather than the per-node
    /// `completed_tasks`/`failed_tasks` HashSets. The HashSets stay
    /// authoritative for per-task identity decisions (dedup on a
    /// re-applied TaskComplete, the operational-loop exit gate, the
    /// kickstart-suppression check); cross-class *counts* live one
    /// layer up, on the replicated ledger every replica converges
    /// to.
    ///
    /// O(n) over the cluster_state task ledger — same bound as the
    /// older `failed_tasks`-only walk; the additional Completed/
    /// Pending/InFlight inspections are constant-time per entry.
    pub fn outcome_summary(&self) -> OutcomeSummary {
        self.cluster_state.outcome_counts()
    }

    /// Tasks the run loop never accounted for (neither completed nor
    /// failed). Populated by `run()` after the loop exits — common
    /// causes are transport collapse before every task dispatched,
    /// secondaries dying mid-run, or any exit path that left items
    /// queued / in-flight without a recorded outcome.
    ///
    /// Reset to 0 at the start of every `run()`. Zero on a clean run
    /// (the loop exits via the `completed + failed >= total` arm).
    /// `>0` is the structured-error case that surfaces as
    /// `RunError::ClusterCollapsed` on the wire — read either via the
    /// matched error variant or via this getter post-call.
    pub fn stranded_count(&self) -> usize {
        self.stranded_count
    }

    pub fn secondary_count(&self) -> usize {
        self.secondaries.len()
    }

    /// Test-only inspector for the primary's replicated cluster
    /// ledger. Returns the per-state counts so tests can assert
    /// convergence with secondaries' mirrors.
    #[cfg(test)]
    pub fn cluster_state_counts_for_test(&self) -> crate::cluster_state::StateCounts {
        self.cluster_state.counts()
    }

    /// Test-only inspector for the total retry passes consumed across
    /// all `(phase, bucket)` keys. The authoritative retry cascade
    /// lives here on the primary (the secondary is a pure reporter), so
    /// retry tests assert on this counter rather than a secondary-side
    /// mirror (which no longer exists).
    #[cfg(test)]
    pub fn retry_passes_used_for_test(&self) -> u32 {
        self.cluster_state.retry_passes_used_total()
    }

    /// Test-only read of the replicated per-phase Completed EVENT tally
    /// (F4) — the count `on_phase_end` would report for `phase`. Reads the
    /// CRDT field that replaced the old node-local `phase_completed` map.
    #[cfg(test)]
    pub fn phase_completed_for_test(&self, phase: &PhaseId) -> u32 {
        self.cluster_state
            .phase_event_tally_for(&(phase.clone(), crate::cluster_state::PhaseTally::Completed))
    }

    /// Test-only read of the replicated per-phase Failed EVENT tally (F4).
    /// Sibling to [`Self::phase_completed_for_test`].
    #[cfg(test)]
    pub fn phase_failed_for_test(&self, phase: &PhaseId) -> u32 {
        self.cluster_state
            .phase_event_tally_for(&(phase.clone(), crate::cluster_state::PhaseTally::Failed))
    }

    /// Test-only borrow of the primary's replicated cluster ledger.
    /// Lets tests read failure reasons (`TaskState::Failed.last_error`)
    /// to pin specific regression-mode error strings without parsing
    /// log output.
    #[cfg(test)]
    pub fn cluster_state_for_test(&self) -> &crate::cluster_state::ClusterState<I> {
        &self.cluster_state
    }

    /// Test-only mutable borrow of the replicated cluster ledger, used
    /// by the hydration tests to seed task states (`TaskAdded` →
    /// Pending, `TaskAssigned` → InFlight, `TaskCompleted` → terminal)
    /// directly via `ClusterState::apply` before
    /// `hydrate_from_cluster_state` runs — without going through the
    /// broadcast path (which needs an initialised pool for the
    /// auto-resume re-inject step).
    #[cfg(test)]
    pub fn cluster_state_mut_for_test(&mut self) -> &mut crate::cluster_state::ClusterState<I> {
        &mut self.cluster_state
    }

    /// Test-only count of workers the primary currently tracks as
    /// mid-dispatch (slot `Assigned`). Used by the composition hazard
    /// tests to assert a hydrated remote-in-flight task is NOT also
    /// counted as a local-active worker (the double-count hazard).
    #[cfg(test)]
    pub fn active_workers_for_test(&self) -> usize {
        self.workers.iter().filter(|w| !w.is_idle()).count()
    }

    /// Test-only count of ALIVE worker slots (idle + busy) — the same
    /// value the phase-floor liveness check's `alive_worker_count()`
    /// reads (`self.workers.len()`). Used by the roster-reconstruction
    /// tests to assert a promoted primary holds the full roster and is
    /// dispatch-capable (`> 0`) where it previously started empty.
    #[cfg(test)]
    pub fn alive_worker_count_for_test(&self) -> usize {
        self.workers.len()
    }

    /// Test-only inspector for the per-secondary staged-WARN counter
    /// (number of WARN stages already logged this silence streak). `None`
    /// when no stage has fired (or the streak was reset). Used by the
    /// fire-once / reset-on-recovery policy test.
    #[cfg(test)]
    pub fn silence_warn_stage_for_test(&self, secondary_id: &str) -> Option<usize> {
        self.silence_warn_stage.get(secondary_id).copied()
    }

    /// Test-only length of the hash-keyed in-flight ledger. Replaces
    /// the removed `pre_owned_in_flight_len_for_test`: the ledger now
    /// unifies locally-dispatched and inherited (pre-owned) in-flight
    /// tasks, so hydration tests assert against this single count.
    #[cfg(test)]
    pub fn in_flight_len_for_test(&self) -> usize {
        self.in_flight.len()
    }

    /// Test-only inspector for the per-type concurrency counter
    /// (`in_flight_per_type[type_id]`, 0 when never reserved). Lets the
    /// failover tests assert that an inherited InFlight task's type slot
    /// was reserved on hydrate so its terminal release stays symmetric.
    #[cfg(test)]
    pub fn in_flight_per_type_for_test(&self, type_id: &dynrunner_core::TypeId) -> u32 {
        self.in_flight_per_type.get(type_id).copied().unwrap_or(0)
    }

    /// Test-only inspector: does the `(secondary_id, worker_id)` slot
    /// currently hold a task whose hash equals `task_hash`? Lets the
    /// reorder/reassignment tests assert the slot's held-task identity
    /// directly without reaching into `SlotState` internals.
    #[cfg(test)]
    pub fn slot_holds_hash_for_test(
        &self,
        secondary_id: &str,
        worker_id: u32,
        task_hash: &str,
    ) -> bool {
        self.worker_idx_for(secondary_id, worker_id)
            .map(|idx| {
                matches!(
                    &self.workers[idx].state,
                    SlotState::Assigned { task_hash: h, .. } if h == task_hash
                )
            })
            .unwrap_or(false)
    }

    /// Test-only inspector: is the `(secondary_id, worker_id)` slot
    /// idle? Mirrors `slot_holds_hash_for_test` for the negative
    /// assertion (a stale terminal must NOT free a reassigned slot).
    #[cfg(test)]
    pub fn slot_is_idle_for_test(&self, secondary_id: &str, worker_id: u32) -> bool {
        self.worker_idx_for(secondary_id, worker_id)
            .map(|idx| self.workers[idx].is_idle())
            .unwrap_or(false)
    }

    /// Test-only inspector: is the `(secondary_id, worker_id)` slot an
    /// `Inherited`-provenance assignment (reconstructed from replicated
    /// `InFlight` at promotion, occupancy unconfirmed)? Lets the
    /// failover-resume reconciliation tests assert that hydrate marks a
    /// reconstructed slot inherited (vs a live `Dispatched` slot).
    #[cfg(test)]
    pub fn slot_is_inherited_for_test(&self, secondary_id: &str, worker_id: u32) -> bool {
        self.worker_idx_for(secondary_id, worker_id)
            .map(|idx| self.workers[idx].is_inherited())
            .unwrap_or(false)
    }

    /// Test-only seam: register one idle remote worker owned by
    /// `secondary_id`. The composition flow's worker registration runs
    /// through the welcome / initial-assignment handshake the composed
    /// primary deliberately skips (it picks up a cluster that already
    /// handshaked pre-promotion); the dispatch hazard test seeds a
    /// worker directly so it can drive `dispatch_to_idle_workers` and
    /// assert the resulting `TaskAssignment` routes over the loopback.
    ///
    /// A worker registered here models a FULLY-OPERATIONAL member, so it
    /// is also marked mesh-confirmed (the `MeshReady` the welcome handshake
    /// would have delivered) — otherwise the dispatch-readiness gate in
    /// `should_skip_worker_for_dispatch` would correctly withhold work from
    /// it. A test that wants to exercise the half-joined (unconfirmed)
    /// member uses [`Self::mark_member_mesh_unconfirmed_for_test`] to undo
    /// this.
    #[cfg(test)]
    pub fn register_idle_worker_for_test(
        &mut self,
        secondary_id: String,
        worker_id: u32,
        resource_budgets: ResourceMap,
    ) {
        self.mesh_ready_secondaries.insert(secondary_id.clone());
        self.workers.push(RemoteWorkerState {
            worker_id,
            secondary_id,
            resource_budgets,
            state: SlotState::Idle,
        });
    }

    /// Test-only seam: declare `secondary_id` DEAD through the genuine
    /// member-removal primitive (`requeue_dead_secondary`) — the path the
    /// heartbeat monitor drives on keepalive-miss. Recovers its in-flight
    /// tasks, retains its workers out, redistributes any held bring-up
    /// reservation share, and originates `PeerRemoved`. Lets the
    /// reservation tests exercise the redistribute trigger without a
    /// wall-clock keepalive timeout.
    #[cfg(test)]
    pub async fn requeue_dead_secondary_for_test(
        &mut self,
        secondary_id: &str,
    ) -> Result<(), String> {
        let dead = crate::primary::heartbeat::DeadSecondary {
            secondary_id: secondary_id.to_string(),
            last_keepalive: std::time::Instant::now(),
        };
        self.requeue_dead_secondary(
            dead,
            dynrunner_protocol_primary_secondary::RemovalCause::KeepaliveMiss,
        )
        .await
    }

    /// Test-only seam: model a HALF-JOINED member by dropping `secondary_id`
    /// from the mesh-confirmation set, so `should_skip_worker_for_dispatch`
    /// withholds work from its workers (the unformed-mesh-leg dispatch gate).
    /// Pairs with [`Self::register_idle_worker_for_test`] (which marks every
    /// registered member confirmed) to drive the strand-prevention test.
    #[cfg(test)]
    pub fn mark_member_mesh_unconfirmed_for_test(&mut self, secondary_id: &str) {
        self.mesh_ready_secondaries.remove(secondary_id);
    }

    /// Test-only seam: deliver a member's `MeshReady` confirmation (the
    /// late-join recovery edge) so a previously-unconfirmed member becomes
    /// assignable. Mirrors what `handle_mesh_ready` does on the wire.
    #[cfg(test)]
    pub fn confirm_member_mesh_for_test(&mut self, secondary_id: &str) {
        self.mesh_ready_secondaries.insert(secondary_id.to_string());
    }

    /// Test-only inspector for whether the peer-lifecycle dispatcher
    /// handle is still held by the coordinator. After a clean `run()`
    /// exit (Ok OR Err), [`Self::cleanup_lifecycle_dispatcher`] must
    /// have taken + aborted + joined the handle, leaving this `false`.
    /// Used by `lifecycle_dispatcher_joinhandle_aborted_on_run_exit`
    /// to pin the cleanup contract.
    #[cfg(test)]
    pub fn lifecycle_dispatcher_handle_present_for_test(&self) -> bool {
        self.lifecycle_dispatcher_handle.is_some()
    }

    /// Test-only: register a worker and immediately stage one
    /// in-flight task on it, routed through the real
    /// `commit_assignment` lifecycle so the slot, the `in_flight`
    /// ledger, and the per-type slot are all seeded consistently
    /// (replaces the manual `RemoteWorkerState { current_task: Some(..)
    /// }` construction the removed two-field model allowed). Returns the
    /// computed task hash so the caller can drive a matching terminal.
    #[cfg(test)]
    pub fn stage_in_flight_for_test(
        &mut self,
        secondary_id: String,
        worker_id: u32,
        task: TaskInfo<I>,
    ) -> String {
        self.workers.push(RemoteWorkerState {
            worker_id,
            secondary_id,
            resource_budgets: ResourceMap::new(),
            state: SlotState::Idle,
        });
        let idx = self.workers.len() - 1;
        let task_hash = crate::primary::wire::compute_task_hash(&task);
        self.commit_assignment(idx, task, task_hash.clone(), ResourceMap::new());
        task_hash
    }

    /// Run the full coordination pipeline.
    ///
    /// `phase_deps` declares the per-phase `depends_on` graph. Items in
    /// `binaries` whose `phase_id` doesn't appear in `phase_deps` are
    /// treated as a single zero-deps phase (the framework still
    /// registers it). `on_phase_start` / `on_phase_end` fire as the
    /// pool's state machine transitions phases through
    /// `Blocked → Active → Drained → Done` — Phase 5B wires these
    /// closures to the Python `TaskDefinition` lifecycle hooks.
    ///
    /// # Cleanup discipline
    ///
    /// Thin wrapper around [`Self::run_pipeline`] whose secondary
    /// concern is to drive `run()`'s cleanup contract — every exit
    /// path (happy-path `Ok`, structured `RunError`, `?`-propagated
    /// error) flows through `cleanup_lifecycle_dispatcher` before
    /// returning, so the peer-lifecycle dispatcher task spawned in
    /// `run_pipeline` is always aborted and joined before this method
    /// returns. Without the wrapper, an error-return from inside the
    /// pipeline would leave the dispatcher blocked on its input
    /// channel forever (the channel's sender lives on `cluster_state`,
    /// which the coordinator still owns post-`run`).
    pub async fn run(
        &mut self,
        seed: crate::process::SeedSource<I>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<(), RunError> {
        // Role-tag the whole run future so every event this task emits is
        // attributed to the primary role and routed to the per-role full
        // log. See `dynrunner_core::role_span`. `.instrument` (not a held
        // guard) so the span is correctly entered/exited across `.await`,
        // and it wraps the cleanup tail too so those events are tagged.
        let span = tracing::info_span!(
            dynrunner_core::PRIMARY_ROLE_SPAN,
            kind = "primary",
            node = %self.config.node_id
        );
        async {
            let result = self.run_pipeline(seed, on_phase_start, on_phase_end).await;
            // Cleanup BOTH dispatchers on every exit (Ok or Err). The two
            // dispatchers own independent channels + listener vectors; both
            // run from spawn-at-`run()`-start to abort-at-`run()`-exit and
            // both must be joined before `run()` returns so the PyO3 wrapper
            // / SLURM pipeline don't leak them.
            self.cleanup_run_dispatchers().await;
            result
        }
        .instrument(span)
        .await
    }

    /// Owned-`self` run entry for the PyO3 boundary.
    ///
    /// Mirrors [`Self::run`] but OWNS `self`, returning a
    /// [`PrimaryRunOutcome`].
    ///
    /// Two regimes, selected by which future resolves first:
    ///
    /// - The pipeline runs to completion → [`PrimaryRunOutcome::Local`]: the
    ///   post-run accounting (`completed`/`failed`/`stranded`) sourced from
    ///   this coordinator's own `cluster_state`, plus the structured
    ///   exit-contract `result` (the `RunError`-or-`Ok` shape the PyO3
    ///   boundary maps to exit codes). The dispatchers tear down in-place.
    /// - The runtime loss-of-primacy signal (`demote_rx`, BUG-6) fires first
    ///   → [`PrimaryRunOutcome::Relocated`]: the pipeline future is dropped,
    ///   `self` destructures into an [`crate::observer::ObserverHandoff`]
    ///   (carrying the live dispatcher handles across — they are NOT torn
    ///   down here; the observer's single-teardown owns them), and the
    ///   handoff rides out to the [`crate::process::Node`], which builds and
    ///   runs the standalone observer. The submitter never runs the observer
    ///   on its own primary (claudemd-HIGH-1); this coordinator only carries
    ///   the handoff out.
    ///
    /// `run_consuming` owns `self`, so it is the ONLY entry that can perform
    /// the by-value handoff — the `&mut self` [`Self::run`] cannot surrender
    /// `self` and so ignores the demote signal entirely.
    pub async fn run_consuming(
        mut self,
        seed: crate::process::SeedSource<I>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<PrimaryRunOutcome<I>, RunError> {
        let span = tracing::info_span!(
            dynrunner_core::PRIMARY_ROLE_SPAN,
            kind = "primary",
            node = %self.config.node_id
        );
        async move {
            // Take the demote receiver out so the `select!` arm can borrow
            // it without aliasing the `&mut self` the pipeline future holds.
            // The pipeline never touches `demote_rx`, so leaving the field
            // `None` for the duration is safe.
            let mut demote_rx = self.demote_rx.take();
            // Which arm of the race won. Local-only enum so the borrow of
            // `self` (via the pipeline future) is fully released before the
            // by-value relocate handoff.
            enum Pipeline {
                Completed(Result<(), RunError>),
                Demoted,
            }
            // Race the pipeline against the runtime loss-of-primacy signal.
            // The losing future is dropped at the end of the `select!`, so
            // after it returns we can move `self` by value (the pipeline
            // future no longer borrows it) on the relocate branch.
            let demoted = {
                let pipeline = self.run_pipeline(seed, on_phase_start, on_phase_end);
                tokio::pin!(pipeline);
                tokio::select! {
                    result = &mut pipeline => {
                        // Pipeline finished first: stay local.
                        Pipeline::Completed(result)
                    }
                    _ = async {
                        match demote_rx.as_mut() {
                            Some(rx) => {
                                // `None` (every sender dropped) means no
                                // demote will ever arrive — park forever so
                                // this arm never resolves on a closed channel.
                                if rx.recv().await.is_none() {
                                    std::future::pending::<()>().await;
                                }
                            }
                            // No demote source wired: park forever.
                            None => std::future::pending::<()>().await,
                        }
                    } => Pipeline::Demoted,
                }
            };
            match demoted {
                Pipeline::Completed(result) => {
                    // Stay-local: tear the dispatchers down in-place (same
                    // contract as `run`), read the counts off this
                    // coordinator's own replicated ledger, carry the pipeline
                    // outcome through `result`, then drop `self`.
                    self.cleanup_run_dispatchers().await;
                    Ok(PrimaryRunOutcome::Local {
                        result,
                        completed: self.completed_count(),
                        failed: self.failed_count(),
                        stranded: self.stranded_count(),
                    })
                }
                Pipeline::Demoted => {
                    // Relocated: destructure `self` into the handoff. The two
                    // dispatcher handles ride across into the observer's
                    // single-teardown, so we deliberately do NOT call
                    // `cleanup_run_dispatchers` here. The `Node` builds the
                    // observer from the handoff and re-sources the final
                    // counts from the observer's converged ledger.
                    Ok(PrimaryRunOutcome::Relocated {
                        handoff: Box::new(self.into_observer_handoff()),
                    })
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Tear down both run-spawned dispatchers (peer-lifecycle +
    /// task-completion) on exit. The single in-place cleanup choke point
    /// both `run` and `run_consuming` share — keeps the abort+join
    /// discipline in one home.
    async fn cleanup_run_dispatchers(&mut self) {
        self.cleanup_lifecycle_dispatcher().await;
        self.cleanup_task_completed_dispatcher().await;
    }

    /// Consume this primary BY VALUE into the standalone observer's
    /// hand-off payload.
    ///
    /// A FULL DESTRUCTURE drops the scheduler `S`, the estimator `E`, the
    /// worker pool, the secondary table, and every other primary-only
    /// concern structurally — the `PrimaryCoordinator<Tr, S, E, I>` becomes
    /// an [`ObserverHandoff<Tr, I>`], a strictly smaller, zero-authority
    /// payload. No `mem::take` / `Option::take` is used on any field of
    /// `self`; the move is compiler-checked by the destructure pattern.
    ///
    /// Carries across exactly what the observer needs to resume without
    /// dropping a live event: the mesh `client` (egress) + `inbox`
    /// (ingress), the replicated `cluster_state` (with its already-installed
    /// `task_completed_tx`), the node id, the deadlines as an
    /// [`ObserverConfig`], the `started_phases` narration seed (the
    /// `phase_started_emitted` set), the panik signal receiver, and the two
    /// dispatcher join handles. The `holdings` are empty: the submitter primary
    /// advertised no resource-holdings (workers run on secondaries). The
    /// `client` + `inbox` move across unchanged — the observer keeps
    /// addressing the mesh through the SAME role slot (the `Process` retags
    /// the slot primary→observer in place, so the channel is stable and no
    /// frame is lost across the swap).
    // PHASE-C-SEAM[C4]: the submitter→observer swap is owned by the
    // `Node`. `Node` ALONE calls `into_observer_handoff` →
    // `ObserverCoordinator::from_handoff` + `observer.run()`. The relocate
    // fork's `run_consuming` arm carries this handoff out as
    // `PrimaryRunOutcome::Relocated { handoff }`.
    fn into_observer_handoff(self) -> crate::observer::ObserverHandoff<I> {
        // Full destructure: every field NOT named below is dropped by the
        // `..` — scheduler/estimator/pool/secondaries/etc. — so the larger
        // `PrimaryCoordinator` is structurally consumed.
        let Self {
            config,
            client,
            inbox,
            cluster_state,
            phase_started_emitted,
            lifecycle_dispatcher_handle,
            task_completed_dispatcher_handle,
            panik_signal_rx,
            graceful_abort_trigger,
            tunnel_reconnector,
            job_ledger_probe,
            upload_action,
            respawn_spawner,
            ..
        } = self;

        crate::observer::ObserverHandoff {
            client,
            inbox,
            cluster_state,
            node_id: config.node_id.clone(),
            deadlines: crate::observer::ObserverConfig {
                node_id: config.node_id,
                fleet_dead_timeout: config.fleet_dead_timeout,
                peer_timeout: config.peer_timeout,
                // The submitter's panik watcher already ran; its signal
                // receiver rides across directly (below), so the observer
                // does NOT re-spawn a watcher from these — they are inert
                // on the handoff path.
                panik_watcher_paths: Vec::new(),
                panik_watcher_poll_interval: Duration::from_secs(60),
                fleet_death_presumption:
                    crate::observer::ObserverConfig::DEFAULT_FLEET_DEATH_PRESUMPTION,
            },
            started_phases: phase_started_emitted,
            panik_signal_rx,
            // The operator's SIGUSR2 trigger rides across the relocation: the
            // SAME armed stream the primary held now drives the observer's
            // graceful-abort arm, so a SIGUSR2 latched during the primary
            // tenure (or arriving on the relocated observer) surfaces on the
            // observer's first poll rather than hitting the kernel default.
            // `None` when un-injected (the primary never self-armed one).
            graceful_abort_trigger,
            // The dispatchers are spawned unconditionally at `run_pipeline`
            // entry (`spawn_run_dispatchers`), so both handles are present
            // at the relocate point. Carried so the observer's
            // single-teardown ABORTS them cleanly (the inherited
            // task-completed dispatcher is superseded by the observer's own
            // fresh channel in `from_handoff`; see its reconciliation note).
            task_completed_dispatcher_handle: task_completed_dispatcher_handle
                .expect("task-completed dispatcher spawned at run_pipeline entry"),
            lifecycle_dispatcher_handle: lifecycle_dispatcher_handle
                .expect("peer-lifecycle dispatcher spawned at run_pipeline entry"),
            holdings: HashSet::new(),
            // Hand the observer the transport-recovery port so it can
            // rebuild its dropped `-R` reverse tunnels (BUG-B reconnect).
            // `None` on backends that self-heal — the observer then has
            // nothing to drive.
            reconnector: tunnel_reconnector,
            // Hand the observer the upload-action port: the submitter→observer
            // is the framework auto-staging upload affinity, so the observer
            // it steps down into executes upload setup tasks in-process (#336
            // P1). `None` on backends with no uploader wired.
            upload_action,
            // The respawn PROVIDER belongs to this PROCESS, not the
            // primary role: the relocated submitter keeps it across its
            // demotion and serves remote respawn-execution requests from
            // whichever primary holds the decision (see
            // `primary::respawn::remote`). `None` when the run launched
            // with the policy disabled.
            respawn_provider: respawn_spawner,
            // The job-ledger consult port: the relocated submitter keeps
            // the SAME `SlurmJobManager` it submitted the cohort from, so
            // the observer it steps down into can consult squeue for the
            // run's job ids and render the cluster-empty terminal verdict.
            // `None` on backends with no job ledger.
            job_ledger: job_ledger_probe,
        }
    }

    /// THE single primary run-loop chokepoint: run the pipeline and emit a
    /// GUARANTEED terminal log line on EVERY exit path (success, error,
    /// relocation-to-parked).
    ///
    /// Both `run` (the `&mut self` test entry) and `run_consuming` (the
    /// production by-value entry) drive the pipeline through this one wrapper,
    /// so a single emit here covers every way the primary's run loop returns.
    /// Pre-fix there was NO such line: the asm-dataset LMU run_~1429 primary
    /// logged its last line at the post-composition ERROR and then nothing —
    /// no exit, no verdict, no proof it ever returned, so an operator could
    /// not tell a wedged primary from a cleanly-exited one. The emit reads the
    /// `Result` the pipeline returned: `Ok(())` → INFO "primary exiting"
    /// (clean), `Err(e)` → ERROR "primary exiting" with the `RunError` reason.
    /// (A relocation never returns from `run_pipeline` — the demote arm in
    /// `run_consuming` cancels the pipeline future, so the "Relocated" outcome
    /// is narrated by the Node's role-swap path, not here. This chokepoint is
    /// specifically "the primary's RUN LOOP returned".)
    async fn run_pipeline(
        &mut self,
        seed: crate::process::SeedSource<I>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<(), RunError> {
        let result = self
            .run_pipeline_inner(seed, on_phase_start, on_phase_end)
            .await;
        match &result {
            Ok(()) => tracing::info!(
                node = %self.config.node_id,
                "primary exiting: run loop returned cleanly (Ok)"
            ),
            Err(e) => tracing::error!(
                node = %self.config.node_id,
                reason = %e,
                "primary exiting: run loop returned an error"
            ),
        }
        result
    }

    /// Original `run()` body, factored out so the public `run` wrapper
    /// can drive cleanup-on-exit regardless of how this function
    /// returns. See [`Self::run`] for the rationale. Wrapped by
    /// [`Self::run_pipeline`], the single exit-log chokepoint.
    ///
    /// The body runs the whole pipeline through `&mut self`: it bootstraps
    /// the mesh, activates this node as the local primary, and runs the
    /// operational loop to completion in-place.
    async fn run_pipeline_inner(
        &mut self,
        seed: crate::process::SeedSource<I>,
        on_phase_start: OnPhaseStart,
        on_phase_end: OnPhaseEnd,
    ) -> Result<(), RunError> {
        // Reset the stranded counter so a previous run's residue
        // can't leak into this one. Populated below after both loops
        // drain; the structured-error path consults it.
        self.stranded_count = 0;
        // Same per-run reset for the pre-loop mesh-pump-gone latch: only set
        // by `send_to` when the local egress receiver has dropped during a
        // pre-loop send; a coordinator re-used across runs must not inherit a
        // previous run's collapse signal.
        self.mesh_pump_gone = false;
        // Same per-run reset for the run-started discriminator: latched by
        // the `PromotedDestination` arm once THIS run's run-start batch has
        // fired; a coordinator re-used across runs must not serve a new
        // run's bring-up members the previous run's mid-run trio.
        self.run_start_batch_fired = false;
        // Same per-run reset for the spawn-rejection ledger: only written
        // by `apply_spawn_tasks` when the validator rejects a runtime
        // `spawn_tasks` task; a coordinator re-used across runs must not
        // inherit a stale rejection.
        self.spawn_rejected_task_ids.clear();
        // Same per-run reset for the worker-management run-should-fail
        // outcome: only written when the worker-management arm drains a
        // `RunShouldFail`; a coordinator re-used across runs must not
        // inherit a stale outcome.
        self.worker_mgmt_fail_outcome = None;
        // Per-run reset for the #3a abort directive: written by
        // `originate_cold_seed` below and read once at the abort gate; a
        // coordinator re-used across runs must not inherit a previous run's
        // seed residue.
        self.pending_run_abort = None;

        self.on_phase_start = Some(on_phase_start);
        self.on_phase_end = Some(on_phase_end);
        // `phase_started_emitted` is NOT cleared here. Its reset is the
        // `ColdStart` seed's concern (`originate_cold_seed` clears it), so
        // on the `PromotionSnapshot` path the projection
        // `seed_from_promotion_snapshot` seeded from `phase_rollups().has_any`
        // SURVIVES — without this a promoted primary would re-fire
        // `on_phase_start` + re-emit the "starting job phase" line for every
        // already-started phase. The `SeedSource` arm (via `originate_cold_seed`)
        // is the sole discriminator, not a runtime `if seeded` fork.
        //
        // Same shape for the per-phase EVENT tallies (F4) and the
        // per-(phase, bucket) retry-pass counter (P3): NOT cleared here
        // because they are the replicated grow-only-MAX `ClusterState`
        // fields. A `ColdStart` CRDT is empty (the accessor returns 0), a
        // `PromotionSnapshot` CRDT carries the inherited counts via
        // max-merge — and a stale clear could resurrect a re-granted budget
        // on failover, which the grow-only-MAX merge is specifically
        // designed to prevent. The `SeedSource` selects the CRDT
        // origination, not a counter reset.

        // Spawn the peer-lifecycle + task-completion dispatchers BEFORE
        // any wire mutation can land. See `spawn_run_dispatchers`.
        self.spawn_run_dispatchers();

        // Unified run-init: land the seed into the CRDT, then ALWAYS hydrate.
        // The `SeedSource` arm selects ONLY who originates the CRDT first —
        // NOT the pool/cache derivation, which is `hydrate`'s job on both
        // paths. Cold-start = "hydrate from a freshly-seeded CRDT";
        // promotion = "hydrate from the inherited CRDT".
        //
        // C-3 ordering: `originate_cold_seed` applies the seed to the LOCAL
        // ledger ONLY (the fleet broadcast is staged and shipped post-connect
        // by `broadcast_cold_seed`, so it reaches the connected secondaries —
        // a pre-connection broadcast is dropped). Running it here, before
        // `hydrate`, means the pool is available for `fire_initial_phase_starts`
        // / `perform_initial_assignment` below; the #3a abort short-circuit
        // inside `originate_cold_seed` seeds nothing on a doomed run.

        // Capture the bootstrap role STRUCTURALLY from the seed BEFORE the
        // `match` below consumes the payload. This `Copy` discriminant is the
        // SOLE relocate-vs-operational decision (mesh-always: a setup peer
        // ALWAYS relocates the primary onto a compute peer; the promoted
        // destination runs in place). It keys on the typed `SeedSource` and
        // NOTHING else — no stored policy, no local-vs-distributed branch.
        let bootstrap_role = BootstrapRole::from_seed(&seed);
        match seed {
            crate::process::SeedSource::ColdStart {
                binaries,
                phase_deps,
            } => self.originate_cold_seed(binaries, phase_deps)?,
            // Mode-2 relocate / pre-staged: originate ONLY the phase graph +
            // the `DiscoveryDebtDeclared` marker (`discovery_debt = Owed`), NO
            // tasks. `discover_on_promotion` (fired post-connect below) reads
            // the `Owed` marker, runs the registered discovery policy, and
            // seeds the discovered tasks itself — so the seed step here only
            // stages the phase graph + the marker, not the corpus.
            crate::process::SeedSource::RelocatedSeed { phase_deps } => {
                self.originate_relocated_seed(phase_deps)
            }
            // The CRDT was already restored by `seed_from_promotion_snapshot`;
            // hydrate (re-)derives the pool + caches from the inherited ledger.
            crate::process::SeedSource::PromotionSnapshot => {}
        }
        // The SOLE pool builder (eliminates the pre-F1 `PendingPool::new` +
        // `ingest_initial_batch` duplication against `hydrate`'s own build):
        // it derives the pool, `total_tasks`, the unified `in_flight` ledger,
        // and the worker / secondary rosters from whatever the CRDT now holds.
        //
        // A composition failure (the seeded ledger describes an impossible
        // task graph — a duplicate `(phase_id, task_id)` identity, a missing
        // dep, or a cycle) is a run-fatal during bring-up: route it through
        // the SAME terminal-verdict path the #3a/#3b duplicate aborts use
        // (latch + broadcast `RunAborted`, surface the typed `RunError`) so
        // the fleet exits on the verdict instead of stranding on an empty
        // pool. The mode-2 `RelocatedSeed` ledger is still empty here (its
        // corpus seeds in `discover_on_promotion`, which guards its own
        // hydrate the same way), so this gate fires only for a cold-seed /
        // promotion-snapshot ledger that already carries the dup.
        if let Err(e) = self.hydrate_from_cluster_state() {
            return Err(self.abort_run_on_invalid_composition(e).await);
        }

        // Point-in-time "primary starting" observation. Read inline — NOT
        // captured into a local that flows downstream: on the mode-2
        // `RelocatedSeed`/`Owed` path the ledger is still empty here (its
        // corpus is seeded later by `discover_on_promotion`), so this logs the
        // pre-discovery count for THIS line only. The finalize entries read
        // their own LIVE `self.total_tasks`, after discovery's re-hydrate, so
        // no stale snapshot can ever reach the stranded denominator.
        tracing::info!(
            total = self.total_tasks,
            num_secondaries = self.config.num_secondaries,
            "primary starting"
        );

        // Take/put-back the command-channel receiver for the whole
        // pre-operational-loop chain: `wait_for_connections` below and the
        // initial-phase-start + empty-phase cascade (relocated to AFTER
        // connect — see the block past `wait_for_connections`) both need
        // `&mut Receiver` while also holding `&mut self`, which would alias
        // if we passed `&mut self.command_rx` directly. Mirrors the
        // discipline `operational_loop` uses (see
        // `lifecycle/operational_loop.rs:51`); the window between take and
        // put-back is benign here because we're still in `run` before the
        // operational loop has started — no concurrent sender access path
        // exists. Put-back happens after `wait_for_mesh_ready` so the
        // loop's own `self.command_rx.take()` re-acquires the same receiver.
        let mut command_rx = self.command_rx.take();

        // Phase 1+2: Wait for all secondaries to send welcome + cert exchange.
        // UNCONDITIONAL (both roles): a setup peer needs the fleet registered
        // so `alive_secondary_members()` is non-empty for relocation-target
        // selection; the promoted destination needs it for its assignment.
        self.wait_for_connections(&mut command_rx).await?;

        // #3a abort gate (UNCONDITIONAL, must precede the role branch).
        // `originate_cold_seed` recorded a pending abort iff the INITIAL batch
        // had a `(phase_id, task_id)` duplicate (pre-phase). Fire it HERE —
        // the first point the secondaries are connected — so the `RunAborted`
        // broadcast reaches them (at seed time none were connected). Returns
        // `Err(RunError::DuplicateTaskIdPrePhase)` on the abort path (the
        // primary's PyO3 boundary surfaces a non-zero exit); a no-op on the
        // clean path AND on a `PromotionSnapshot` (which never originated a
        // cold seed, so `pending_run_abort` is `None`). Run BEFORE the role
        // branch so a doomed cold-seed run aborts WITHOUT relocating: the
        // setup peer holds the abort directive node-locally (it is not CRDT
        // state the relocate target inherits), so a relocate-before-abort
        // would silently strand the doomed run. Hard cluster shutdown — short-
        // circuits before mesh formation, relocation, seeding, or assignment.
        self.fire_pending_run_abort().await?;

        // Consumer `on_run_start`-raise abort gate (UNCONDITIONAL, must precede
        // the role branch — same placement rationale as `fire_pending_run_abort`
        // above). The pyo3 promotion recipe fired `on_run_start` BEFORE
        // `run_consuming` and, on a raise, recorded the reason
        // (`record_pre_run_hook_abort`); fire it HERE — the first point the
        // secondaries are connected — so the `RunAborted` broadcast reaches them.
        // Returns `Err(RunError::FatalPolicyExit)` on the abort path (the
        // primary's PyO3 boundary surfaces a non-zero exit); a no-op on the clean
        // path AND on every cold-start / non-raising promotion (the directive is
        // `None`). The promoted-path twin of the cold-start path's `?`-propagation
        // of an `on_run_start` raise out before `run()`.
        self.fire_pre_run_hook_abort().await?;

        // ── Peer-mesh formation (UNCONDITIONAL — both roles need a routable
        // mesh). `send_peer_lists` fans out `PeerInfo`; the Node mesh-pump
        // dials the peer-secondary links off it (see `send_peer_lists` /
        // `wait_for_setup`'s PeerInfo arm) — a TRANSPORT fact, independent of
        // any secondary's role/operational state. The setup peer's
        // `relocate_primary_to` broadcasts `PrimaryChanged { Transferred }`
        // over the links `wait_for_connections` already established to the
        // connected fleet; the promoted destination uses the same routable
        // mesh for its operational broadcasts. The promoted destination
        // additionally awaits `MeshReady` (peer mesh settled) on its own arm
        // AFTER it has sent the initial assignment — the setup peer does NOT
        // (gating its relocate on a secondary-operational signal it never
        // triggers is a circular deadlock; see the role branch below). ──────

        // Phase 3: Send peer lists.
        self.send_peer_lists().await?;

        // Phase 4: Wait for peer connections (skip for single secondary).
        self.wait_for_peer_connections().await?;

        // The primary is a first-class mesh member: register its own
        // host-id in every replica's `peer_state` / `RoleTable` / relay
        // membership via a self-authored `PeerJoined`, the same CRDT path
        // the secondary accept site uses for each secondary. Originated
        // here — after the fleet is connected (so the broadcast reaches
        // every secondary) and BEFORE the role branch below (so membership
        // is recorded uniformly for both roles). Membership only: this does
        // NOT announce `PrimaryChanged` and does NOT add the primary to the
        // `PeerInfo` dial-list.
        self.originate_primary_membership().await;

        // Post-mesh roster re-broadcast: each secondary's
        // `SecondaryCapacity` + `PeerJoined` was originated pre-mesh at
        // `handle_welcome` (before the later-welcoming secondaries' peer
        // links existed) and never re-emitted, so a secondary that
        // welcomed early holds an incomplete capacity roster. Re-emit the
        // FULL roster the primary holds NOW that the mesh has converged
        // (post `wait_for_peer_connections`), so every secondary's mirror
        // matches the primary's complete view before a failover could
        // promote one of them onto an incomplete worker roster. This is a
        // pure re-emission (the records already exist in the primary's own
        // mirror), so it ships straight over the mesh; the receiver-side
        // set-once idempotency absorbs the records a secondary already
        // holds. Both roles pass here.
        self.rebroadcast_full_roster().await;

        // Mesh-readiness is NOT gated here (transport ⊥ role/operational).
        // `MeshReady` is the secondary's report that its peer-mesh settled,
        // and a secondary only emits it from its OPERATIONAL loop — which it
        // reaches only after `wait_for_setup` consumes an `InitialAssignment`.
        // Under mesh-always the SETUP PEER never sends one (it relocates the
        // role away in the branch below); the InitialAssignment is the
        // operational primary's (the relocate TARGET's) concern. So gating the
        // setup peer's relocate on `MeshReady` is a circular deadlock —
        // wait_for_mesh_ready ⇒ secondary-operational ⇒ InitialAssignment ⇒
        // operational-primary ⇒ relocation ⇒ blocked behind the wait. The
        // event the gate actually protects (the role announce landing on a
        // settled peer mesh) is observed where it is satisfiable: the
        // `BootstrapRole::PromotedDestination` arm runs `wait_for_mesh_ready`
        // AFTER it has sent `InitialAssignment` + `TransferComplete`, by which
        // point the secondaries it just drove operational can emit `MeshReady`
        // over the REAL mesh. The peer-mesh links themselves form at the Node
        // mesh-pump (dialed off `PeerInfo`, see `send_peer_lists` above) — a
        // transport fact independent of any secondary's operational state, so
        // the setup peer's `PrimaryChanged { Transferred }` already routes over
        // the live links to the connected fleet `wait_for_connections`
        // established.

        // Phase 4.5 (UNCONDITIONAL): ship the cold-start seed to the fleet.
        // Drains the staged `PhaseDepsSet` + `TaskAdded` fan-out (+ the #2
        // invalid-dep `TaskFailed` transitions) that `originate_cold_seed`
        // applied LOCALLY and parked for this post-connection broadcast (a
        // pre-connection broadcast is dropped). Run BEFORE the role branch —
        // not just before `perform_initial_assignment` — because on a
        // `ColdStart` the SETUP PEER is the ONLY corpus holder until this
        // broadcast lands: the relocate target captures its promotion snapshot
        // when `PrimaryChanged { Transferred }` names it, and that snapshot
        // must already carry the seeded `TaskAdded`s for the target's run to
        // have any work. So the corpus fan-out happens here, then the setup
        // peer relocates. On a `RelocatedSeed` it ships only the phase graph +
        // the `Owed` marker (the target's `discover_on_promotion` produces the
        // corpus). On a `PromotionSnapshot` the staged set is empty (the
        // inherited CRDT replicates via anti-entropy), so this is a no-op.
        self.broadcast_cold_seed().await;

        // ── Bootstrap role branch (mesh-always, SeedSource-keyed) ──────────
        match bootstrap_role {
            // The run's setup peer (ColdStart / RelocatedSeed): hand the
            // primary role to a compute peer and park. It does the MINIMUM —
            // connect + form mesh + ship the corpus (above) + relocate — and
            // does NOT discover / fire phase-starts / stage / assign /
            // transfer: those are the OPERATIONAL primary's concern, and the
            // relocate TARGET re-runs this SAME `run_pipeline` (as a
            // `PromotionSnapshot` ⇒ `PromotedDestination`), doing all of them
            // itself over its inherited roster.
            BootstrapRole::SetupPeer => {
                // An empty candidate set is an unsupported/degenerate topology
                // — the setup peer must NEVER stay the run's primary (mesh-
                // always), so surface a hard structured error rather than
                // silently staying local. NOW A LIVE error path on EVERY
                // backend (in-process mpsc AND SLURM QUIC) — no longer
                // "unreachable" for any topology.
                let Some(chosen) =
                    self.select_relocation_target(super::lifecycle::RelocationPolicy::LowestId)
                else {
                    // #313 — terminal RUN VERDICT before exiting. The fleet
                    // `wait_for_connections` registered is CONNECTED but NO
                    // peer advertised `can_be_primary`, so an election can
                    // NEVER produce a primary — failover cannot salvage this
                    // run. Without the verdict the connected non-promotable
                    // secondaries / observers idle into their own timeouts
                    // holding SLURM slots. Same reason string as the local
                    // error so both sides of the wire agree.
                    self.broadcast_terminal_verdict(ClusterMutation::RunAborted {
                        reason: RunError::NoRelocationTarget.to_string(),
                    })
                    .await;
                    return Err(RunError::NoRelocationTarget);
                };
                self.relocate_primary_to(chosen).await;
                // Park: the demote hook fired by the relocate's local apply
                // (the role table now names target ≠ self) makes the demote
                // arm the only way out of the pipeline future.
                // `run_consuming`'s demote arm wins the `select!`,
                // destructures `self` into an observer handoff, and returns
                // `PrimaryRunOutcome::Relocated`. Returning `Ok(())` here would
                // let the `select!` see the pipeline as COMPLETED (not
                // demoted), wrongly keeping the setup `Local`; parking is what
                // guarantees the demote arm wins.
                std::future::pending::<()>().await;
                unreachable!("relocate bootstrap future is cancelled by the demote arm");
            }
            // The compute-peer destination (PromotionSnapshot): run the FULL
            // operational pre-loop chain in place, then the operational tail.
            BootstrapRole::PromotedDestination => {
                // Mode-2 discover-on-promotion (V6). Runs the consumer's
                // discovery policy IFF the CRDT declares discovery `Owed` (a
                // relocated compute-peer primary, or an in-process
                // `--source-already-staged` local primary, that inherited the
                // empty-ledger + `Owed` marker via its snapshot), originates
                // `PhaseDepsSet + TaskAdded* + DiscoverySettled` (NO
                // run-terminal — an all-skipped / empty corpus finalizes
                // through the counter machinery once its trailing re-hydrate
                // projects the skips into `completed_tasks`, exactly as mode-1),
                // and re-hydrates the pool. INERT on a failover/promotion of an
                // already-seeded run (reads `Settled`, so the gate short-
                // circuits). ORDERING is load-bearing: BEFORE
                // `fire_initial_phase_starts` + the empty-phase cascade so, by
                // the time the cascade evaluates its `discovery_debt() != Owed`
                // gate, the driver has already flipped `Owed → Settled` and
                // hydrated the discovered tasks. The setup peer never reaches
                // this function (it relocated above), so a setup peer that owed
                // debt without a discovery policy hands the `Owed` marker on
                // untouched — the "Owed but no policy = hard error" branch is
                // reachable ONLY by a `PromotionSnapshot` primary (which MUST
                // carry the policy). See `mode2-discovery-design.md` Part 3.
                self.discover_on_promotion().await?;

                // Fire on_phase_start for every phase the pool initialised as
                // Active (zero-deps phases), THEN cascade trivially-empty
                // phases. Subsequent activations triggered by `mark_phase_done`
                // are observed via `process_phase_lifecycle`.
                //
                // The fire-before-cascade COUPLING is kept intact (the
                // cascade's `on_phase_end(.., 0, 0)` for an empty initial phase
                // must come AFTER that phase's `on_phase_start`, so
                // `fire_initial_phase_starts` stays immediately before the
                // cascade). The 3a/3b duplicate discriminator is STRUCTURAL
                // (the code path — `originate_cold_seed` vs `apply_spawn_tasks`
                // — not a runtime read of `phase_started_emitted`);
                // `phase_started_emitted` is seeded by hydrate (V3).
                self.fire_initial_phase_starts();

                // Trivially-empty Active phases (no items at all) need to drain
                // and cascade Done before initial assignment, otherwise their
                // `Blocked` dependents — which may hold all the run's actual
                // work — never become visible to `view_for_worker`. Triggers
                // `on_phase_end(.., 0, 0)` for each empty phase via the
                // lifecycle cascade. Runs BEFORE the seed/assignment step
                // below, preserving the load-bearing "cascade before initial
                // assignment" ordering.
                //
                // Required at this pre-loop site (not optional): a consumer
                // `on_phase_end` callback fired by the initial-empty-phase
                // cascade can itself queue `spawn_tasks(next_phase_items)`, and
                // the cascade's next `drain_empty_active_phases` poll would
                // otherwise false-fire `on_phase_end(.., 0, 0)` on the
                // successor phase exactly the way the in-loop bug class did.
                // The `command_rx` taken above is handed into
                // `process_phase_lifecycle` so the cascade's per-iteration
                // drain step picks up callback-queued `SpawnTasks` /
                // `FailPermanent` / `ReinjectTask` /
                // `UpdatePreferredSecondaries` commands inline.
                //
                // Required because `operational_loop`'s entry-time exit check
                // (`completed + failed >= total_tasks && active_workers == 0`)
                // trips IMMEDIATELY on entry if every pre-loop-dispatched task
                // happens to finish (and have its on_phase_end fire) during a
                // pre-loop wait — without inline drain, the SpawnTasks command
                // sits on the channel until the entry-time check that exits the
                // loop without ever polling it. Asm-tokenizer's lazy-spawn
                // consumer pattern (`FullPipelineTask.on_phase_end →
                // primary_handle.spawn_tasks`) is the live consumer.
                //
                // Gated on `discovery_debt() != Owed` (V6): while discovery is
                // owed the ledger is unseeded and every declared phase is a
                // transiently-empty `Active` — draining it now would mark every
                // phase `Drained` and fire spurious `on_phase_end(.., 0, 0)`.
                // `discover_on_promotion` (run above) flips `Owed → Settled`
                // after seeding, so by the time the cascade evaluates this gate
                // the debt is cleared and the discovered work is hydrated. On
                // every already-seeded / failover path the marker is
                // `Undeclared`/`Settled`, so the gate is open. Both halves (the
                // drain + the cascade) stay paired under ONE gate.
                if self.cluster_state.discovery_debt() != DiscoveryDebt::Owed {
                    self.pool_mut().drain_empty_active_phases();
                    self.process_phase_lifecycle(&mut command_rx).await;
                }

                // PROMOTION REPLAY (F5): dispatch every `Unhandled`
                // custom-message inbox entry the inherited CRDT carries
                // to the local handler — a primary that died between an
                // important landing and its handler invocation leaves
                // the entry `Unhandled` in every replica, and THIS host
                // (any peer can be primary) holds the consumer's
                // TaskDefinition to consume it. Runs AFTER the
                // hydrate + initial cascade above (the pool is live, so
                // a handler's spawn_tasks lands dispatchable work) and
                // BEFORE `perform_initial_assignment` (the replayed
                // spawns join the initial assignment). A no-op on a
                // cold start (empty inbox) and on a hook-less consumer
                // (consume-unhandled WARN path).
                self.dispatch_unhandled_custom_messages(&mut command_rx)
                    .await;

                // Phase 2.5: Auto-stage. Run the staging walk on behalf of
                // callers that didn't pre-queue via `queue_stage_file` /
                // `queue_initial_staging_from_binaries`. Gate semantics live on
                // `staging::maybe_auto_stage_initial`: "we have a root to walk,
                // items are file-backed, we're not in pre-staged mode, and no
                // caller pre-populated the queue" — any one false skips
                // silently. Runs AFTER the fleet is registered (the staging
                // fan-out is per-secondary) and BEFORE
                // `perform_initial_assignment`, which drains
                // `pending_stage_files` into each recipient's
                // `InitialAssignment.staged_files`. This is the operational
                // primary's concern (the relocate target re-runs it itself);
                // the setup peer never reaches here.
                self.maybe_auto_stage_initial()?;

                // V2: rebuild the remote-worker roster from the replicated
                // per-secondary capacity NOW — `wait_for_connections` has
                // originated every connected secondary's `SecondaryCapacity`
                // (and `rebroadcast_full_roster` above re-emitted the full
                // set), so `known_secondaries()` is populated.
                // `reconstruct_workers_from_cluster_state` is the SOLE roster
                // builder. It MUST run BEFORE `perform_initial_assignment`
                // commits any slot: the wholesale replace re-derives occupancy
                // only from CRDT `InFlight`, so a re-invoke AFTER assignment
                // began committing would zero committed-but-not-yet-originated
                // slots — FORBIDDEN.
                self.reconstruct_workers_from_cluster_state();

                // Phase 4.9: OPEN the bring-up reservation (#494). Partition
                // the initial pending pool across the FULL EXPECTED member set
                // via the projected-load interleave so each member gets a
                // reserved share. MUST run AFTER the roster is reconstructed
                // (it reads `self.workers`) and BEFORE
                // `perform_initial_assignment` — both the initial batch and the
                // operational idle-worker recheck construct their views through
                // `dispatch_view_for_worker`, which scopes to the member's
                // reserved share, so a first-confirmed member drains only its
                // slice instead of the whole pool while the fleet forms (the
                // 14/14/0 pack). The veto in `should_skip_worker_for_dispatch`
                // still withholds any send to an unconfirmed member — the
                // reservation only caps what a CONFIRMED member may pull.
                self.seed_bringup_reservation();

                // Phase 5: the initial per-secondary assignment — a pure
                // scheduler over the reconstructed `self.workers`, staged-files
                // inline. The cold-seed broadcast already ran UNCONDITIONALLY
                // above (it had to precede a possible relocate); the assignment
                // is the operational primary's concern and assigns over the
                // inherited roster.
                //
                // A `ClusterCollapsed` outcome means a secondary died during
                // the initial assignment (the mesh-pump went away mid-send —
                // the egress-side twin of the operational loop's
                // `recv() -> None` collapse). The operational loop tolerates a
                // mid-loop collapse by breaking and letting the finalize tail
                // classify the strand; an assignment-time collapse must reach
                // that SAME classification rather than `?`-escaping as a raw
                // `RunError::Other` (which is the gap the uniform-relocate
                // reorder exposed). So skip the rest of the pre-loop chain
                // (transfer-complete / op-loop) — every send would just hit the
                // same dead mesh — and route straight into the SOLE
                // strand-classification site, where the full (un-dispatched)
                // pool surfaces as stranded and the honest `RunAborted`
                // terminal is broadcast. Put `command_rx` back first for
                // symmetry with the take at the top of the pre-loop chain.
                if let InitialAssignmentOutcome::ClusterCollapsed =
                    self.perform_initial_assignment().await?
                {
                    return self.bail_to_finalize(command_rx).await;
                }

                // Phase 6: Send transfer complete.
                self.send_transfer_complete().await?;

                // RUN-START LATCH: both halves of the run-start batch have
                // now fired over the roster known at this instant. From here
                // on, a member completing its welcome/cert-exchange has
                // MISSED the batch, so the incremental per-member serve
                // (`serve_setup_on_cert_exchange`) must hand it the FULL
                // setup trio itself — see `run_start_batch_fired`'s field
                // doc. Latched HERE (the one site that sequences both
                // halves) and nowhere else. No frame dispatch happens
                // between `perform_initial_assignment` above and this line
                // (the pre-loop chain never recvs the inbox), so no
                // cert-exchange can race the latch.
                self.run_start_batch_fired = true;

                // Pre-loop collapse gate (transfer-complete window): a
                // secondary dying AFTER its initial assignment send succeeded
                // but before/at `send_transfer_complete` makes that fan-out's
                // per-secondary `send_to` hit the now-gone local mesh-pump —
                // latched on `mesh_pump_gone`. (`send_transfer_complete` now
                // fans the gate-release per CRDT-known secondary over the
                // directed router path, but its `Err` arm is still uniformly the
                // mesh-pump-gone collapse — the per-peer no-route outcome is
                // resolved asynchronously inside the pump and never sets the
                // latch.) Route it through the SAME finalize tail as the
                // assignment-time collapse (rather than the warn-swallow letting
                // the doomed run proceed into the operational loop over a dead
                // mesh): the assigned-but-unconfirmed pool surfaces as stranded
                // with the honest `RunAborted` terminal. Mirror of the
                // operational loop's break-then-finalize, observed from the
                // send side.
                if self.mesh_pump_gone {
                    return self.bail_to_finalize(command_rx).await;
                }

                // Phase 6.5: wait for the peer mesh to settle before this
                // operational primary asserts authority and starts driving
                // dispatch over it. The `PrimaryChanged` self-announce
                // (`activate_local_primary`) and every subsequent operational
                // broadcast route over the QUIC peer mesh; the pre-fix gap fired
                // the announce ~750µs after cert-exchange, against a
                // still-forming mesh (per-peer dial budget: 10s QUIC + 10s WSS),
                // so every pre-mesh-formation peer-broadcast routed into the void
                // for the duration. Holding until every secondary signals
                // `MeshReady` (mesh formed, watchdog elapsed, or single-secondary
                // instant) is the event-driven "wait until the mesh is real",
                // bounded by `config.mesh_ready_timeout` (warning + proceed on
                // straggler, never deadlock).
                //
                // Non-circular HERE (unlike the old unconditional pre-branch
                // placement): this runs AFTER `perform_initial_assignment` +
                // `send_transfer_complete` above, so the secondaries this primary
                // just drove operational can reach their `process_tasks` loop and
                // emit `MeshReady`. The gate belongs on THIS
                // `BootstrapRole::PromotedDestination` arm only — an operational
                // primary that has already sent the assignment is the sole role
                // for which `MeshReady` is satisfiable. The setup peer (which
                // relocated the role away without ever sending an assignment)
                // must NOT gate on it.
                self.wait_for_mesh_ready(&mut command_rx).await?;

                // Put the command-channel receiver back on `self` so
                // `operational_loop`'s own `self.command_rx.take()` picks it up
                // again. Symmetric with the take at the top of the pre-loop
                // chain.
                self.command_rx = command_rx;

                // Initial-setup-done important event — the honest once-per-run
                // "all initial setup complete, entering steady-state"
                // milestone: the fleet is connected, staged, peer-linked, the
                // primary's own membership is recorded, the ledger is seeded +
                // tasks assigned, transfer-complete is sent, and the peer mesh
                // has settled. A single emit at this point fires EXACTLY ONCE
                // per run on the operational primary.
                tracing::info!(
                    target: super::important_events::IMPORTANT_TARGET,
                    "initial setup done",
                );

                // Bootstrap tail: activate THIS node as the local primary and
                // run the operational loop to completion in place. The
                // `wait_for_mesh_ready` above held until the peer mesh settled,
                // so the self-announce warms each replica's role cache to a
                // real connection.
                self.bootstrap_tail_dispatch().await
            }
        }
    }

    /// The operational bootstrap tail: activate THIS node as the local primary
    /// and run the operational loop to completion in place.
    ///
    /// One concern: the OPERATIONAL primary's bootstrap tail. Reached ONLY on
    /// the `BootstrapRole::PromotedDestination` arm of `run_pipeline` (a
    /// `SeedSource::PromotionSnapshot` — the relocated target / failover-
    /// promoted primary). The setup-peer RELOCATE is NOT here: it fired inline
    /// in `run_pipeline`'s `BootstrapRole::SetupPeer` arm (before
    /// `discover_on_promotion` / assignment), so this tail is uniform across
    /// every operational primary — there is no relocate-vs-stay fork and no
    /// local-vs-distributed branch.
    ///
    /// Activates the local primary (originates its self-announce as a SINGLE
    /// epoch transition — re-asserting at the inherited epoch when the promoted
    /// snapshot already names this host, see `originate_primary_changed` —
    /// warms the role cache, emits a keepalive), then runs the shared
    /// operational-loop-and-finalize tail to completion in place.
    pub(crate) async fn bootstrap_tail_dispatch(&mut self) -> Result<(), RunError> {
        self.activate_local_primary().await?;
        self.run_operational_and_finalize().await
    }

    /// Shared operational-loop-and-finalize tail. The single mechanism
    /// the bootstrap pipeline (`run_pipeline`, pool built from `binaries`)
    /// converges on once its pool is seeded. Runs the main
    /// operational loop,
    /// the structured-abort checks (panik / worker-mgmt-fail /
    /// setup-deadline), the retry passes, final accounting, and the
    /// terminal RUN VERDICT broadcast + settle window (`RunComplete` /
    /// `RunAborted` — see `broadcast_terminal_verdict` for the #313
    /// verdict-vs-failover exit-path classification).
    ///
    /// The task count is read LIVE from `self.total_tasks` (refreshed by
    /// `hydrate_from_cluster_state` on every seed, including discovery's
    /// re-hydrate) at each use — never a caller-captured snapshot that could
    /// predate `discover_on_promotion`.
    async fn run_operational_and_finalize(&mut self) -> Result<(), RunError> {
        // Operational loop (main pass).
        self.operational_loop().await?;

        // Panik check: if the operational loop's panik arm fired,
        // the cluster has already been instructed (via the broadcast)
        // to shut down. Surface as `RunError::PanikShutdown` and
        // skip every remaining phase (retry passes, drain, accounting,
        // RunComplete settle window). The PyO3 wrapper translates
        // PanikShutdown into `std::process::exit(137)` so the SLURM
        // wrapper reaps the container.
        if let Some((matched_path, reason)) = self.panik_outcome.take() {
            tracing::error!(
                matched_path = %matched_path.display(),
                reason = %reason,
                "primary run aborted by panik signal; surfacing PanikShutdown"
            );
            return Err(RunError::PanikShutdown {
                matched_path,
                reason,
            });
        }

        // Worker-management run-should-fail check: if the operational
        // loop's worker-management arm recorded a break outcome (a
        // `RunShouldFail` signal — emitted by the phase layer's
        // proceed-or-fail decision OR the phase-floor liveness check —
        // OR a `PolicyFatalExit` from a consumer `on_phase_end` raise),
        // surface the TYPED outcome the arm classified and skip the
        // retry-pass / drain / accounting tail. The arm already mapped
        // the signal to the right `RunError` variant (generic wedge →
        // `Other`, consumer-policy abort → `FatalPolicyExit`), so this
        // is a pure pass-through. The worker arm OWNS the clean-shutdown
        // drive; the phase/task layer that emitted the signal never
        // broke the loop directly (decoupling law). Same write-by-arm /
        // read-by-pipeline discipline as `panik_outcome`.
        if let Some(outcome) = self.worker_mgmt_fail_outcome.take() {
            tracing::error!(
                error = %outcome,
                "primary run aborted by worker-management run-should-fail signal"
            );
            // #313 — the terminal RUN VERDICT. This latch is a DELIBERATE
            // fail-loud terminal (a `RunShouldFail` / `PolicyFatalExit`
            // policy decision over replicated facts — a promoted successor
            // would inherit the same facts and re-decide it), so broadcast
            // the failure twin of `RunComplete` before exiting. Without it
            // the primary just vanished: the secondaries idled into their
            // own timeouts and the observer reported nothing (the "fatal
            // error lost" class). Best-effort by design — see
            // `broadcast_terminal_verdict` for the full exit-path
            // classification (panik and generic `Other` bootstrap/transport
            // failures deliberately do NOT broadcast: those are failover's
            // jurisdiction).
            self.broadcast_terminal_verdict(ClusterMutation::RunAborted {
                reason: outcome.to_string(),
            })
            .await;
            return Err(outcome);
        }

        // Phase 10: Retry pass(es). Each Recoverable / NonRecoverable
        // failure in the main pass terminated its dispatch slot and
        // landed the task hash in `failed_tasks`. Re-inject those
        // tasks and run the operational loop again so they get one
        // more chance — bounded by `config.retry_max_passes` (default
        // 1). Tasks that fail again stay permanently in
        // `failed_tasks`. Without this loop a Recoverable failure
        // either retries forever (the legacy busy-loop bug) or never
        // retries at all; the pass-based shape gives task-level
        // retry that matches the local manager's behaviour.
        self.run_retry_passes().await?;

        // Same panik check, post-retry-passes. If panik fired during
        // a retry pass's operational-loop re-entry, `panik_outcome`
        // is Some and `run_retry_passes` bailed at the top of its
        // next iteration. Pick it up here.
        if let Some((matched_path, reason)) = self.panik_outcome.take() {
            tracing::error!(
                matched_path = %matched_path.display(),
                reason = %reason,
                "primary run aborted by panik signal during retry passes; \
                 surfacing PanikShutdown"
            );
            return Err(RunError::PanikShutdown {
                matched_path,
                reason,
            });
        }

        // Drain in-flight completions, run the final per-task accounting,
        // broadcast the terminal mutation, and — on a routing collapse —
        // return the structured `RunError::ClusterCollapsed`. This is the
        // SOLE strand-classification site: the assignment-time collapse path
        // (`run_pipeline`'s `PromotedDestination` arm) routes through the
        // SAME helper, so a secondary dying during the initial assignment is
        // classified identically to one dying mid-operational-loop. On a
        // clean run (`stranded == 0`) the helper returns `Ok(())` and we fall
        // through to the spawn-rejection backstop + clean-finish tail below.
        self.finalize_terminal_accounting().await?;

        // The wholesale spawn-rejection backstop lives INSIDE
        // `finalize_terminal_accounting` (it is final ledger state, and the
        // terminal-verdict broadcast must match the local return — #313):
        // a non-empty `spawn_rejected_task_ids` made the call above
        // broadcast `RunAborted` and return `Err(RunError::SpawnRejected)`,
        // which the `?` propagated. Reaching here means a genuinely clean
        // finish.

        let outcome = self.outcome_summary();
        tracing::info!(
            succeeded = outcome.succeeded,
            fail_retry = outcome.fail_retry,
            fail_oom = outcome.fail_oom,
            fail_final = outcome.fail_final,
            total = self.total_tasks,
            "primary finished"
        );

        Ok(())
    }

    /// Pre-loop collapse bail-out: put the borrowed command-channel receiver
    /// back on `self` (symmetric with the take at the top of the pre-loop
    /// chain) and route into the SOLE strand-classification site.
    ///
    /// One concern: the shared tail every pre-loop cluster-collapse gate in
    /// `run_pipeline`'s `PromotedDestination` arm converges on, so the
    /// `command_rx` put-back + `finalize_terminal_accounting` call is written
    /// ONCE rather than duplicated per gate. The gates differ only in WHICH
    /// collapse signal they observe (the per-send `mesh_pump_gone` latch);
    /// the bail-out itself is uniform.
    async fn bail_to_finalize(
        &mut self,
        command_rx: Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) -> Result<(), RunError> {
        self.command_rx = command_rx;
        self.finalize_terminal_accounting().await
    }

    /// The SOLE strand-classification + terminal-broadcast site.
    ///
    /// One concern: turn the run's final per-task ledger state into the
    /// terminal outcome — drain any in-flight completions, compute the
    /// stranded count, broadcast the honest terminal mutation
    /// (`RunAborted` on a routing collapse, `RunComplete` on a clean run),
    /// settle, and return `Err(RunError::ClusterCollapsed { .. })` iff any
    /// task was stranded (else `Ok(())`, so the caller continues to its
    /// clean-finish tail).
    ///
    /// Reached from BOTH terminal paths so the classification is identical
    /// on each:
    /// - `run_operational_and_finalize` — the operational loop exited
    ///   (clean completion, fleet-dead, or a mid-loop transport collapse the
    ///   loop tolerates by breaking); and
    /// - `run_pipeline`'s `PromotedDestination` arm — ANY pre-loop send hit a
    ///   cluster-collapse send failure (the local mesh-pump is gone, the
    ///   egress-side twin of the operational loop's `recv() -> None` collapse
    ///   criterion), so the loop is never entered and the un-dispatched pool
    ///   is stranded. The pre-loop gate is driven uniformly by the
    ///   `mesh_pump_gone` latch (set in `send_to`), so a collapse during the
    ///   peer-list / cold-seed / roster-rebroadcast / initial-assignment /
    ///   transfer-complete chain is classified identically.
    ///
    /// The run's task count is read LIVE from `self.total_tasks` — the
    /// single-source-of-truth that `hydrate_from_cluster_state` refreshes on
    /// every (re)seed, including `discover_on_promotion`'s mode-2 re-hydrate.
    /// It is NOT a caller-captured snapshot: a value snapshotted in
    /// `run_pipeline` before `discover_on_promotion` ran would be the stale
    /// pre-discovery `0` on the `Owed`/`RelocatedSeed` path and falsely
    /// classify a post-discovery collapse as a clean run.
    async fn finalize_terminal_accounting(&mut self) -> Result<(), RunError> {
        // Live denominator (see doc comment): the count as of NOW, after any
        // discovery-driven re-hydrate, never a pre-discovery snapshot.
        let total = self.total_tasks;
        // Drain any TaskComplete / TaskFailed messages that crossed the
        // wire while the operational loop was winding down but hadn't
        // been pulled by `transport.recv` yet. Without this, the
        // accounting below sees pre-drain counts and classifies
        // successful completions as `stranded`, false-positiving clean
        // runs into `RunError::ClusterCollapsed`. Bounded by 500ms so
        // the cost on a fully-quiesced happy-path exit is one
        // 50ms quiet-window probe; the longer ceiling covers
        // heavily-pipelined teardowns where a burst of TaskCompletes
        // is still in flight as the loop exits. On a collapsed mesh the
        // inbound is already closed, so the drain returns immediately.
        self.drain_pending_messages(Duration::from_millis(500))
            .await?;

        // ── Run-authority verdict gates (zombie split-brain,
        // run_20260610_221140) ──
        //
        // Placed AFTER the drain so the freshest replicated facts — a
        // RunAborted mutation or a PrimaryChanged announcement still
        // sitting in the inbox at loop exit — are ingested before any
        // verdict is authored. Both gates fire BEFORE the broadcast
        // ladder below: a node that fails either one authors NO verdict
        // and never reaches the caller's "primary finished" log.
        //
        // Gate 1 — verdict adoption: the replicated `run_aborted` latch
        // is sticky and, at this point, FOREIGN by construction (every
        // own-abort path — the #3a pre-phase duplicate, the worker-mgmt
        // fail latch, NoRelocationTarget — returns its structured error
        // without reaching this finalize; this function's own abort
        // ladder runs below the gates and returns immediately after
        // broadcasting). Adopt it: the run is over cluster-wide with the
        // authoring primary's reason; a second verdict — least of all a
        // contradictory `RunComplete` — must not be authored.
        if let Some(reason) = self.cluster_state.run_aborted().map(str::to_owned) {
            tracing::error!(
                reason = %reason,
                "standing down on the cluster's replicated RunAborted \
                 verdict: adopting it as this node's terminal (no verdict \
                 of our own is authored)"
            );
            return Err(RunError::AbortedByClusterVerdict { reason });
        }
        // Gate 2 — primary recognition: the terminal verdict is gated on
        // holding the CURRENT epoch. A primary the replicated register no
        // longer names (a higher-epoch holder exists) lost authority; its
        // local totals are not authoritative and it must not conclude the
        // run (production: the deposed epoch-2 primary's rc=0 "primary
        // finished succeeded=165 fail_final=108" against the cluster's
        // abort verdict). The mid-run stand-down is the BUG-6 demote
        // signal's jurisdiction (`run_consuming` drops the pipeline);
        // this gate is the exit-edge backstop for a primary that learned
        // of its deposition only at (or after) run end — e.g. through the
        // finalize drain just above. `current_primary() == None` (nobody
        // ever named) passes: a bootstrap-degenerate lone primary is not
        // deposed.
        if let Some(current) = self.cluster_state.current_primary().map(str::to_owned)
            && current != self.config.node_id
        {
            let epoch = self.cluster_state.primary_epoch();
            tracing::error!(
                current_primary = %current,
                epoch,
                "deposed: the replicated register names another primary — \
                 authoring NO terminal verdict and exiting without a clean \
                 finish (this node's totals are not authoritative)"
            );
            return Err(RunError::Deposed {
                current_primary: current,
                epoch,
            });
        }

        // Final accounting: any task in `total_tasks` that is neither
        // in `completed_tasks` nor in `failed_tasks` is *stranded* —
        // the run loop exited (transport closed, all secondaries dead,
        // inactivity timeout, etc.) before the per-task outcome could
        // be recorded. Surfacing this category as a distinct counter
        // (rather than silently letting it vanish into "total -
        // completed - failed = unaccounted") is the load-bearing
        // observability fix: pre-fix, asm-tokenizer's primary returned
        // exit 0 with `Completed: 10 / Failed: 0 / Total: 484` and CI
        // / ops scripts checking exit code saw green when 474 tasks
        // had never even been dispatched. Post-fix, the same scenario
        // produces a structured `RunError::ClusterCollapsed` with the
        // per-category counts, the diagnostic log line below, and a
        // non-zero exit at the PyO3 boundary.
        let outcome = self.outcome_summary();
        self.stranded_count = total.saturating_sub(outcome.total_terminal());
        let stranded = self.stranded_count;

        // Graceful-abort verdict — checked BEFORE the strand
        // classification. Under the replicated `graceful_abort_requested`
        // latch the non-terminal residue is the DELIBERATELY-frozen ready
        // pool, not a routing-collapse strand, so classifying it as
        // `ClusterCollapsed` (and broadcasting `RunAborted`) would
        // mis-diagnose an operator-requested wind-down as a fault. The
        // honest terminal is `RunComplete` WITH the latch set: every
        // remaining secondary exits through its normal run-complete drain
        // break, and every node (observer included) derives the composed
        // graceful-abort verdict `run_complete ∧ graceful_abort` from the
        // two sticky facts. The local return is the structured
        // `RunError::GracefulAbort` so this primary's own boundary reports
        // the SAME verdict the observer narrates (distinct from success
        // AND from a hard abort), whether or not any task was left
        // unscheduled.
        if self.cluster_state.graceful_abort_requested() {
            self.broadcast_terminal_verdict(ClusterMutation::RunComplete)
                .await;
            tracing::warn!(
                target: super::important_events::IMPORTANT_TARGET,
                succeeded = outcome.succeeded,
                fail_retry = outcome.fail_retry,
                fail_oom = outcome.fail_oom,
                fail_final = outcome.fail_final,
                skipped = outcome.skipped,
                unscheduled = stranded,
                total,
                "run gracefully aborted — fleet drained; {stranded} task(s) \
                 deliberately left unscheduled"
            );
            return Err(RunError::GracefulAbort {
                unscheduled: stranded,
                outcome,
            });
        }

        // Terminal broadcast so non-promoted secondaries / the connected
        // observer on the peer mesh know the run is over and can exit.
        // Without this, after a post-promotion handoff scenario, the local
        // primary disconnects but peers can't tell whether the run
        // finished or the primary just crashed — they sit in failover
        // detection holding SLURM job slots indefinitely. Idempotent on
        // re-application; failures here are non-fatal (this is a cleanup
        // signal).
        //
        // The variant is gated on the terminal fact: a routing collapse
        // (`stranded > 0`) broadcasts `RunAborted { reason }`, a wholesale
        // runtime spawn rejection broadcasts `RunAborted` (#313 — pre-fix
        // it broadcast a FALSE `RunComplete`, so the fleet/observer
        // narrated a clean success while the primary exited non-zero), and
        // a clean run broadcasts `RunComplete`. All settle identically —
        // `broadcast_terminal_verdict` is the single mechanism (see its
        // doc for the full verdict-vs-failover exit-path classification) —
        // and each is terminal for a straggler secondary (`RunAborted` →
        // `SecondaryTerminal::Aborted`, `RunComplete` → done), so either
        // way every peer still on the mesh releases its SLURM slot. The
        // honest variant is what makes the failure reach the observer's
        // important channel: the narrator projects `RunAborted` to "run
        // aborted — …" on `IMPORTANT_TARGET` (a `RunComplete` would have
        // narrated a false success). The local `run()` return is unchanged
        // — only the peer-facing broadcast becomes honest.
        //
        // Priority: the strand is the stronger terminal (a collapse may
        // ALSO leave spawn-rejection residue; the collapse names the root
        // cause). `reason` reuses the corresponding `RunError` Display
        // render (the per-class breakdown / the rejected-id list), so the
        // primary-side exception and the peer-side report agree; `outcome`
        // is `Copy`, so the later `ClusterCollapsed` return is unaffected.
        let terminal_mutation = if stranded > 0 {
            ClusterMutation::RunAborted {
                reason: RunError::ClusterCollapsed { stranded, outcome }.to_string(),
            }
        } else if !self.spawn_rejected_task_ids.is_empty() {
            ClusterMutation::RunAborted {
                reason: RunError::SpawnRejected {
                    rejected_task_ids: self.spawn_rejected_task_ids.clone(),
                }
                .to_string(),
            }
        } else {
            ClusterMutation::RunComplete
        };
        self.broadcast_terminal_verdict(terminal_mutation).await;

        if stranded > 0 {
            tracing::error!(
                succeeded = outcome.succeeded,
                fail_retry = outcome.fail_retry,
                fail_oom = outcome.fail_oom,
                fail_final = outcome.fail_final,
                skipped = outcome.skipped,
                stranded,
                total,
                "{stranded} tasks left unassigned because cluster routing collapsed \
                 (succeeded={s} fail_retry={r} fail_oom={o} fail_final={fi} \
                 skipped={sk} stranded={stranded})",
                s = outcome.succeeded,
                r = outcome.fail_retry,
                o = outcome.fail_oom,
                fi = outcome.fail_final,
                sk = outcome.skipped,
            );
            return Err(RunError::ClusterCollapsed { stranded, outcome });
        }

        // Loud-fail backstop for the silent zero-dispatch path. A runtime
        // `spawn_tasks` batch (typically `on_phase_end` spawning the next
        // phase) whose EVERY task the validator rejected nets that phase
        // ZERO dispatch — `apply_spawn_tasks` never refreshed `total_tasks`,
        // so `run_complete_check`'s counter exit tripped against the
        // pre-spawn total and the run reached this tail with that planned
        // work silently dropped (the asm-dataset-nix c39034f2 producer-path
        // silent total=0). Surfacing it as a structured
        // `RunError::SpawnRejected` makes the submitter's PyO3 boundary
        // RAISE instead of returning rc=0 — a non-empty spawn plan that
        // dispatched nothing must never present as a clean run. Sequenced
        // AFTER the strand check (the stronger terminal); the matching
        // `RunAborted` verdict was chosen by the SAME ladder above (#313),
        // so the wire fact and the local return cannot diverge. The
        // per-index `SpawnError` the consumer received from `spawn_tasks`
        // is unchanged; this is the run-level net the consumer's per-task
        // WARN-and-continue otherwise slips through.
        if !self.spawn_rejected_task_ids.is_empty() {
            let rejected_task_ids = std::mem::take(&mut self.spawn_rejected_task_ids);
            tracing::error!(
                rejected = rejected_task_ids.len(),
                "runtime spawn_tasks rejected every task in a batch — the \
                 phase dispatched ZERO tasks and the run would otherwise have \
                 exited rc=0 with that planned work silently dropped"
            );
            return Err(RunError::SpawnRejected { rejected_task_ids });
        }

        Ok(())
    }

    /// Spawn the peer-lifecycle + task-completion dispatcher tasks.
    ///
    /// The (sender, receiver) pairs were built in `new()` and the
    /// senders already installed on `cluster_state`; here we hand each
    /// receiver and its registered listeners to a `spawn_local`'d
    /// dispatcher task. The returned `JoinHandle`s are stored on `self`
    /// so the `run` outer wrapper can abort + join them
    /// on every exit path (a leaked dispatcher would otherwise block
    /// forever on its input channel, whose sender lives on
    /// `cluster_state` which the coordinator still owns post-run).
    ///
    /// Single-shot by contract: the `take()`s leave `None` behind, so a
    /// re-entrant caller silently skips. Called by the bootstrap
    /// (`run_pipeline`) path BEFORE any wire mutation can land.
    fn spawn_run_dispatchers(&mut self) {
        // Propagate the primary role span (current at this call, which runs
        // inside the instrumented `run` future) into the
        // spawned dispatcher tasks so the events THEY emit are attributed to
        // the primary role too. `spawn_local` otherwise detaches the span
        // context. See `dynrunner_core::role_span`.
        if let Some(rx) = self.lifecycle_rx.take() {
            let listeners = std::mem::take(&mut self.peer_lifecycle_listeners);
            let handle = tokio::task::spawn_local(
                crate::peer_lifecycle::run_peer_lifecycle_dispatcher(rx, listeners)
                    .instrument(tracing::Span::current()),
            );
            self.lifecycle_dispatcher_handle = Some(handle);
        }
        if let Some(rx) = self.task_completed_rx.take() {
            let listeners = std::mem::take(&mut self.task_completed_listeners);
            let handle = tokio::task::spawn_local(
                crate::task_completed::run_task_completed_dispatcher(rx, listeners)
                    .instrument(tracing::Span::current()),
            );
            self.task_completed_dispatcher_handle = Some(handle);
        }
    }

    /// Fire `on_phase_start` for every phase the pool currently
    /// reports as `Active` that we haven't notified yet. Idempotent:
    /// re-running visits only newly-active phases. Called once at
    /// run start (for zero-deps phases) and again from
    /// `process_phase_lifecycle` after `mark_phase_done` cascades.
    pub(super) fn fire_initial_phase_starts(&mut self) {
        let active: Vec<PhaseId> = self.pool().active_phases();
        for p in active {
            if self.phase_started_emitted.insert(p.clone()) {
                // Starting-job-phase / phase-transition (phase start)
                // important event. This `insert` guard is the single
                // once-per-phase edge for both the initial-active phases
                // and the runtime activations cascaded by
                // `mark_phase_done`, so it is the canonical phase-start
                // occurrence point. Emitted at the importance target;
                // task spawning the consumer drives off `on_phase_start`
                // below rides the same transition.
                tracing::info!(
                    target: super::important_events::IMPORTANT_TARGET,
                    phase = %p,
                    "starting job phase",
                );
                // Tell worker management a phase started and how many
                // workers it minimally needs to make progress. This is a
                // pure EMIT onto the decoupled worker-management bus — the
                // phase layer states demand and knows nothing of how (or
                // whether) worker management scales up; the consuming arm
                // counts alive workers and drives respawn / RunShouldFail.
                // An empty phase (one that will cascade-drain with no
                // items) makes no worker demand, so we skip the emit.
                let min = self.phase_min_workers(&p);
                if min > 0 {
                    self.cluster_state.emit_worker_mgmt(
                        WorkerMgmtSignal::PhaseStartedNeedsWorkers {
                            phase: p.clone(),
                            min,
                        },
                    );
                }
                if let Some(cb) = self.on_phase_start.as_mut() {
                    cb(&p);
                }
            }
        }
    }

    /// Minimum worker count a phase needs to make progress: `1` if the
    /// phase has any pending or in-flight work, else `0`. A pure query on
    /// the pool — the floor is "at least one worker to dispatch the
    /// phase's work"; additional workers are throughput, not correctness,
    /// and that scale-up policy is worker management's concern. Used by
    /// [`Self::fire_initial_phase_starts`] to populate
    /// [`WorkerMgmtSignal::PhaseStartedNeedsWorkers`].
    fn phase_min_workers(&self, phase: &PhaseId) -> usize {
        // Consult the optional pool directly: before the pool is built
        // (pre-run) no phase owns work, so the floor is 0. Once built, a
        // phase needs a worker iff it has pending or in-flight items.
        let Some(pool) = self.pending.as_ref() else {
            return 0;
        };
        let pending = pool.iter().any(|t| &t.phase_id == phase);
        let in_flight = pool.in_flight(phase) > 0;
        usize::from(pending || in_flight)
    }

    /// Per-phase proceed-or-fail policy, evaluated once a phase has
    /// drained AND its retry buckets are exhausted, immediately before
    /// `mark_phase_done`. A pure, synchronous predicate DERIVED FROM THE
    /// REPLICATED LEDGER (`phase_rollups`) — no I/O, no mutation, no
    /// worker-management call (the caller routes a FAIL through the
    /// decoupled signal bus).
    ///
    /// Default policy:
    /// - PROCEED when the phase owns tasks and every one of them is terminal
    ///   (`has_any && !has_live`). This subsumes the former completed /
    ///   failed / skipped-as-existing accounting because skipped items are
    ///   now REAL terminal tasks (`TaskState::SkippedAlreadyDone`):
    ///     * a phase with ≥1 Completed terminal produced output its
    ///       dependents consume;
    ///     * a phase whose items reached a terminal FAILED outcome advances
    ///       per the canonical retry-bucket-exhaustion contract (the retry
    ///       buckets have already run; surviving failures are PERMANENT and
    ///       recorded, surfaced in the outcome summary, NOT aborted — see
    ///       [`crate::primary::retry_bucket`]);
    ///     * an ALL-SKIPPED phase (the `--skip-existing` "nothing left to do"
    ///       case) is STRUCTURALLY indistinguishable here from any other
    ///       all-terminal phase — its items ARE in the ledger (their outputs
    ///       already exist on the shared fs), so it proceeds without a
    ///       special skip-count branch.
    /// - PROCEED when the consumer DECLARED the phase `may_be_empty`
    ///   (`PhaseSpec.may_be_empty`, replicated via
    ///   `ClusterMutation::PhaseMayBeEmptySet`) — the explicit opt-out for
    ///   an intentional pure-sequencing gate / terminal-empty phase that
    ///   legitimately has no work of its own. "Fail loud BY DEFAULT" means
    ///   an explicit opt-out exists; this is it.
    ///
    /// The FAIL branch (`RunShouldFail`) is reserved for the genuine
    /// wedges, both reached only via the `_` fallback (no terminal-drained
    /// tasks for this phase):
    /// - a phase that reached the drain still owning LIVE residual work
    ///   (`phase_min_workers > 0`) — advancing would strand its dependents
    ///   on never-resolved inputs; and
    /// - (F-honesty) an activated phase that drained GENUINELY EMPTY — zero
    ///   ledger tasks of its own, not declared `may_be_empty`, AND that
    ///   leaves the pool with NO outstanding real work (`is_empty()`). This
    ///   is the silent-partial-success the consumers hit when
    ///   `on_phase_end`-driven lazy injection (or discovery) was suppressed:
    ///   the phase's planned work was never injected, so with nothing else
    ///   outstanding the run would complete clean rc=0 having produced
    ///   nothing. The topology (leaf vs non-leaf) is NOT the discriminator —
    ///   the suppressed phase is a LEAF in one asm-dataset chain and NON-LEAF
    ///   in the asm-tokenizer chain (`tokenize→unify_vocab→memmap`); both are
    ///   the same bug. The discriminators are the explicit `may_be_empty`
    ///   declaration AND the outstanding-work probe: an empty drain that
    ///   leaves real work in the pool (queued, in-flight, OR blocked — the
    ///   dependents this phase's `Done` is about to UNBLOCK) stranded NOTHING
    ///   and PROCEEDS; only an empty drain that empties the run is the wedge.
    ///
    /// The phase-layer veto here is the structural backstop; the live
    /// no-progress decision (a phase that started, needs workers, and has
    /// none) is the worker arm's, reached via `PhaseStartedNeedsWorkers`.
    ///
    /// `phase` is the only input: the ledger rollup, the residual-work probe
    /// (`phase_min_workers`), and the `may_be_empty` opt-out lookup are all
    /// keyed on it.
    pub(super) fn phase_can_proceed(&self, phase: &PhaseId) -> bool {
        // The decision is derived from the replicated ledger, not from an
        // event tally. Skipped-as-existing items are now REAL terminal tasks
        // (`TaskState::SkippedAlreadyDone`), so any phase that had ANY
        // discovered item has a `PhaseRollup` entry — and a phase that
        // drained with tasks (every task reached a terminal state) reads
        // `has_any && !has_live`. That single condition subsumes the former
        // completed/failed/skipped accounting:
        //   * a phase with ≥1 Completed/Failed terminal → has_any, !has_live;
        //   * an all-skipped phase (the `--skip-existing` "nothing left to
        //     do" case) → has_any, !has_live (a skip IS terminal) — STRUCTURAL
        //     proceed, no special skip-count branch;
        //   * a genuinely-empty phase (zero discovered items) → no rollup
        //     entry (or has_any == false) → the `_` fallback below.
        let rollups = self.cluster_state.phase_rollups();
        match rollups.get(phase) {
            // The phase owns tasks and every one of them is terminal — it
            // resolved its work, advance (the canonical retry-bucket-
            // exhaustion contract: surviving failures are permanent-and-
            // recorded and surfaced in the outcome summary, not aborted).
            Some(r) if r.has_any && !r.has_live => true,
            // No terminal-drained tasks for this phase. Either it owns LIVE
            // work (a residual not-yet-terminal task) or it owns no tasks at
            // all. Three discriminators, in the same priority order the
            // pre-ledger policy used:
            _ => {
                // The consumer explicitly declared this phase MAY drain empty
                // (a pure sequencing gate / terminal-empty phase) — the "fail
                // loud by default" opt-out. Advance.
                if self.cluster_state.phase_may_be_empty(phase) {
                    return true;
                }
                // Residual unresolved work for THIS phase is a wedge
                // (advancing strands its dependents on never-resolved
                // inputs) — veto. `phase_min_workers` reads the pool's
                // pending/in-flight floor for this phase.
                if self.phase_min_workers(phase) > 0 {
                    return false;
                }
                // The phase drained genuinely empty and is NOT declared
                // `may_be_empty`. The F-honesty wedge is a SILENT PARTIAL
                // SUCCESS — the run would complete clean rc=0 having produced
                // nothing because a phase that should have injected work
                // didn't. But "empty" alone does not prove that wedge: the
                // COMMON multi-phase shape is an empty EARLY phase that owns
                // no work of its own while its dependents own the real work,
                // BLOCKED only on this phase reaching `Done` — exactly what
                // `mark_phase_done` delivers. There the empty drain stranded
                // NOTHING. The discriminator is whether the run still owns ANY
                // outstanding real work (`!pool().is_empty()` — queued +
                // in-flight + blocked, all phases; the #312 cross-phase
                // discriminator). Outstanding work remains ⇒ PROCEED; pool
                // genuinely empty ⇒ the run would finish having done nothing
                // ⇒ veto, fail loud.
                !self.pool().is_empty()
            }
        }
    }

    /// Cooperatively drain every command an `on_phase_end` callback queued
    /// via the in-runtime `PrimaryHandle` path, dispatching each through the
    /// `handle_primary_command` chokepoint in FIFO order.
    ///
    /// # Why cooperative
    ///
    /// This whole node runs on ONE `current_thread` tokio runtime + LocalSet
    /// (the relocated primary's `py.detach` block in
    /// `dynrunner-pyo3/.../secondary/run.rs`). A synchronous `try_recv`-drain
    /// that never reaches a coop yield point can monopolise the single
    /// executor thread — starving the operational loop's `inbox.recv()`, the
    /// mesh pump, and the QUIC driver. The hazard is concrete: a queued
    /// command (e.g. a callback's `SpawnTasks`) can, via the recursive phase
    /// cascade (`apply_fail_permanent` → `note_item_failed` →
    /// `process_phase_lifecycle`), re-queue ANOTHER command onto
    /// `command_rx`, so this loop can run unboundedly without the inner
    /// `handle_primary_command().await` ever yielding to siblings.
    ///
    /// We bound the work between yields to [`Self::DRAIN_YIELD_BUDGET`] and
    /// `yield_now().await` once the budget is spent. Semantics are preserved:
    /// the drain STILL drains fully (it only stops on an empty channel) and
    /// processing ORDER is FIFO (one `try_recv` per iteration); the yield
    /// merely lets sibling LocalSet tasks make progress between batches.
    ///
    /// `Box::pin` breaks the async-recursion cycle (this is reachable from
    /// `process_phase_lifecycle`, which this can re-enter via the cascade);
    /// without it the compiler can't size the future.
    pub(super) async fn drain_callback_queued_commands(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let mut processed_since_yield: u32 = 0;
        loop {
            let cmd = match command_rx.as_mut() {
                Some(rx) => rx.try_recv().ok(),
                None => None,
            };
            let Some(cmd) = cmd else { break };
            Box::pin(crate::primary::command_channel::handle_primary_command(
                self, cmd, command_rx,
            ))
            .await;
            processed_since_yield += 1;
            if processed_since_yield >= Self::DRAIN_YIELD_BUDGET {
                tokio::task::yield_now().await;
                processed_since_yield = 0;
            }
        }
    }

    /// CAPTURING drain variant (F5 atomicity): drain + dispatch exactly
    /// like [`Self::drain_callback_queued_commands`] — same chokepoint,
    /// same FIFO, same yield budget, ZERO duplicated handler logic —
    /// but with the coordinator's mutation-capture sink armed, then
    /// apply `terminal` under the SAME capture before disarming.
    ///
    /// Every cluster mutation the drained commands originate
    /// (transitively — including any cascade-fired `PhaseEnded` etc.)
    /// is applied LOCALLY exactly as on the plain drain, but diverted
    /// off the wire into the returned batch; `terminal` (the message's
    /// `CustomMessageHandled` fact) is then routed through the same
    /// `apply_and_broadcast_cluster_mutations` chokepoint, so it lands
    /// LAST in the batch by construction — the effect mutations always
    /// precede the terminal fact in the one flushed frame
    /// (hook-mutations-before-the-fact, the `PhaseEnded` ordering
    /// rule). The caller flushes the returned batch as ONE wire frame
    /// via `broadcast_applied_mutations` — every replica applies the
    /// handler's effect and the terminal together or not at all.
    ///
    /// Non-mutation side effects of the drained commands (pool
    /// re-injection, the buffered `TasksAdded` worker-mgmt emits) still
    /// happen during the drain, but their CONSUMERS (the operational
    /// loop's worker-mgmt arm) only run after control returns to the
    /// `select!` loop — i.e. after the caller's flush — so they fire
    /// AFTER the batch lands, never before.
    pub(super) async fn drain_callback_queued_commands_capturing(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
        terminal: ClusterMutation<I>,
    ) -> Vec<ClusterMutation<I>> {
        self.begin_mutation_capture();
        self.drain_callback_queued_commands(command_rx).await;
        self.apply_and_broadcast_cluster_mutations(vec![terminal])
            .await;
        self.take_mutation_capture()
    }

    /// DISCARD drain variant (F5 all-or-nothing): drop every queued
    /// command UNEXECUTED, rejecting each through
    /// [`PrimaryCommand::reject`] so a caller blocked on its reply
    /// learns the discard. Used when a `custom_message_handler` RAISED:
    /// the handler's queued commands are its partial effect, and a
    /// raising handler's effect must never land — not in the local
    /// pool, not in the local CRDT, not on the wire (discarding
    /// EXECUTED effects would be unsound: local applies leak through
    /// snapshot anti-entropy). Returns the discard count for the
    /// caller's structured ERROR.
    ///
    /// Scope note: the channel cannot distinguish provenance, so a
    /// command a concurrent OS thread races onto it DURING the
    /// (synchronous, GIL-holding) handler invocation would be discarded
    /// with the handler's own — the dispatch decision pre-drains
    /// bystanders before invoking precisely to keep that window at the
    /// theoretical minimum.
    pub(super) fn discard_callback_queued_commands(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
        reason: &str,
    ) -> usize {
        let mut discarded = 0;
        while let Some(cmd) = command_rx.as_mut().and_then(|rx| rx.try_recv().ok()) {
            cmd.reject(reason);
            discarded += 1;
        }
        discarded
    }

    /// The phase-lifecycle cascade's run-terminal gate: `true` iff the
    /// replicated `RunAborted` verdict is latched, in which case the
    /// cascade must stop — no `on_phase_end` fire, no `PhaseEnded`
    /// origination, no `mark_phase_done`, no dependent `on_phase_start`
    /// — because every one of those would derive "phase progress" from
    /// post-abort (possibly invalidated/wiped) state. One predicate so
    /// the three checkpoints inside [`Self::process_phase_lifecycle`]
    /// (loop top, per drained phase, post-command-drain) cannot drift.
    ///
    /// Deliberately scoped to the ABORT latch: `run_complete` ends the
    /// run through the normal completion exits, and the graceful-abort
    /// drain RELIES on the cascade running to its end.
    fn run_terminal_cascade_gate(&self) -> bool {
        match self.cluster_state.run_aborted() {
            Some(reason) => {
                tracing::debug!(
                    reason = %reason,
                    "run-terminal verdict latched; suppressing the \
                     phase-lifecycle cascade (no phase hook may run \
                     against post-abort state)"
                );
                true
            }
            None => false,
        }
    }

    /// Drive `Drained` phases through `on_phase_end` → `mark_phase_done`
    /// → newly-Active phases through `on_phase_start`. Called from
    /// the same code paths that update `completed_tasks` / `failed_tasks`
    /// (i.e. after `pool.on_item_finished` runs). The cascade keeps
    /// running until no phase is in `Drained` — phases with empty
    /// dependency chains can transition through several states in
    /// one tick.
    ///
    /// `command_rx` carries the operational-loop's command-channel
    /// receiver (the `take`n local; see `operational_loop.rs:51`). After
    /// each cascade iteration's `on_phase_end` fires, we drain any
    /// commands the user callback queued via the in-runtime
    /// `PrimaryHandle` path (e.g. `spawn_tasks(next_phase_items)`) and
    /// dispatch each through the existing `handle_primary_command`
    /// chokepoint BEFORE the next `drain_empty_active_phases` poll. The
    /// drain is the load-bearing step: without it the cascade's next
    /// poll observes the not-yet-applied spawn as an empty successor
    /// phase and false-fires `on_phase_end(.., 0, 0)` for it,
    /// dropping every callback-injected task.
    ///
    /// The pre-loop waits `wait_for_connections` and `wait_for_mesh_ready`
    /// pass the LIVE `command_rx` (the `take`n receiver, `Some`): the
    /// PyPrimaryHandle IS reachable before operational-loop entry (it
    /// shares the pre-`run` `command_sender()` clone), so an
    /// `on_phase_end` fired by a TaskComplete arriving during either wait
    /// can queue `SpawnTasks` and have it drain inline via the same
    /// `dispatch_message` → cascade path. The post-loop drain
    /// (`drain_pending_messages`) passes `&mut None` — by then the
    /// operational loop has already exited and won't re-enter, so there is
    /// no in-runtime callback path left to drain.
    pub(super) async fn process_phase_lifecycle(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // Discovery-owed defence-in-depth (V6): while the CRDT declares
        // discovery `Owed` the ledger is unseeded and every declared phase is
        // a transiently-empty `Active` — firing `on_phase_end(.., 0, 0)` now
        // would surface a spurious "empty drain" for every phase before
        // `discover_on_promotion` has had a chance to populate them (a
        // consumer callback walking just-discovered outputs would OSError on
        // missing paths). The gate clears the moment the driver originates
        // `DiscoverySettled`; subsequent cascade calls resume normal
        // operation. Idempotent on every cold mode-1 / legacy / already-seeded
        // path: the marker is `Undeclared`/`Settled` (`!= Owed`), so the gate
        // is always satisfied there. The run-init pre-loop cascade is already
        // gated identically at its call site; this is the per-call backstop
        // for the inbound-driven (`dispatch_message`) cascade entries.
        if self.cluster_state.discovery_debt() == DiscoveryDebt::Owed {
            return;
        }
        loop {
            // Run-terminal gate: once the replicated `RunAborted` verdict is
            // latched (e.g. the #3b run-wide invalidation latches it BEFORE
            // wiping the ledger), the run is over and the phase machine must
            // not re-derive "phase ended" from post-abort state — firing
            // `on_phase_end` against an invalidated/wiped ledger runs the
            // consumer hook on facts that no longer describe the run (the
            // production "spawned=0" raise that overwrote the true abort
            // reason, asm-dataset run_20260611_112116). Checked per cascade
            // round AND per drained phase below, because the latch can flip
            // MID-cascade (a hook-queued spawn tripping the #3b invalidation
            // inside the command drain).
            if self.run_terminal_cascade_gate() {
                return;
            }
            let drained = self.pool_mut().poll_drain_transitions();
            if drained.is_empty() {
                break;
            }
            for p in &drained {
                // Per-phase re-check of the run-terminal gate (see the
                // loop-top doc): an earlier phase's hook in THIS drained
                // batch may have latched the verdict via its queued
                // commands.
                if self.run_terminal_cascade_gate() {
                    return;
                }
                // Per-phase retry-bucket cascade — runs BEFORE
                // `on_phase_end` so phase B (which depends on A)
                // doesn't activate until phase A's retry buckets
                // are exhausted. See `crate::primary::retry_bucket`
                // for the partition and counter semantics.
                //
                // Recoverable bucket first: a Recoverable failure
                // that succeeds on retry leaves no entry in
                // `failed_tasks`, so the subsequent OOM-bucket
                // probe finds nothing and falls through cleanly.
                // OOM bucket second: the dispatch modifiers (when
                // wired) constrain memory-heavy work to a single
                // worker per secondary in memory-DESC order, so
                // running it AFTER the Recoverable bucket has
                // settled keeps the constraint scoped to actually-
                // over-budget tasks.
                //
                // On `Ok(true)`: the bucket reinjected at least one
                // task; the phase has flipped Drained → Active and
                // `drained_pending` no longer contains it. Skip
                // `on_phase_end` and `mark_phase_done` for this
                // phase; the next drain edge will revisit it.
                if self
                    .try_run_phase_retry_bucket(
                        p,
                        crate::primary::retry_bucket::BucketKind::Recoverable,
                        command_rx,
                    )
                    .await
                    .unwrap_or(false)
                {
                    continue;
                }
                if self
                    .try_run_phase_retry_bucket(
                        p,
                        crate::primary::retry_bucket::BucketKind::Oom,
                        command_rx,
                    )
                    .await
                    .unwrap_or(false)
                {
                    continue;
                }
                // Both buckets DECLINED: this phase's soft (retry-pending)
                // failures are permanent now — finalize them and
                // cascade-fail their blocked dependents (the dependents can
                // never run; leaving them blocked wedged the run forever —
                // see `finalize_phase_soft_failures`). Runs BEFORE the
                // tally read below so `on_phase_end` reports a tally that
                // already includes the cascaded terminals (the broadcast's
                // local apply bumps the per-phase Failed EVENT tally).
                self.finalize_phase_soft_failures(p).await;
                // Read the replicated EVENT tallies (F4): identical numbers
                // to the old node-local maps on the live path, CORRECT on
                // the promoted path (the events were replicated). EVENT-
                // shaped — a fail → reinject → succeed task contributed to
                // BOTH — so `on_phase_end` reports the same event numbers a
                // promoted primary would, not a terminal projection.
                let completed = self.cluster_state.phase_event_tally_for(&(
                    p.clone(),
                    crate::cluster_state::PhaseTally::Completed,
                ));
                let failed = self
                    .cluster_state
                    .phase_event_tally_for(&(p.clone(), crate::cluster_state::PhaseTally::Failed));
                // Gather the just-completed phase's PUBLISHED task outputs
                // BEFORE taking the `&mut self.on_phase_end` borrow (the
                // gather is an immutable `&self.cluster_state` read). The
                // cascade fires this hook AFTER the phase's `TaskCompleted`
                // applies, so every produced output is already in the
                // `task_outputs` cache here — the callback reads a finished
                // task's output WITHOUT a filesystem path (the keyed-outputs
                // primitive, lifted from per-dependent `predecessor_outputs`
                // to the whole-phase `on_phase_end` surface). Owned clones,
                // so no borrow outlives the call.
                let phase_outputs = self.cluster_state.phase_task_outputs(p);
                if let Some(cb) = self.on_phase_end.as_mut() {
                    cb(p, completed, failed, &phase_outputs);
                }
                // Honest on_phase_end: if the consumer's hook RAISED, the
                // closure recorded the reason into the shared raise-latch
                // (it could not break the cascade itself — its `()` return
                // is unchanged). The cascade reads-and-clears the latch
                // here and EMITS `PolicyFatalExit` onto the decoupled
                // worker-management bus — the SAME emit shape the
                // proceed-or-fail decision below uses for `RunShouldFail`.
                // A consumer-hook raise is a deliberate policy abort, so it
                // surfaces the structured `RunError::FatalPolicyExit` (the
                // PyO3 boundary RAISES it) rather than the warn-and-continue
                // false-green this latch replaces. The phase layer NEVER
                // drives shutdown directly (decoupling law): it only emits;
                // the worker-management arm owns the clean-shutdown drive
                // and records the typed break outcome the pipeline tail
                // surfaces.
                if let Some(reason) = self.phase_hook_raise_latch.take() {
                    // Routed through the run-fail emit chokepoint
                    // (`emit_run_fail_signal`), which SYNCHRONOUSLY
                    // latches the dispatch-view step-0 freeze before
                    // the bus emit — the cascade below still marks the
                    // phase done and fires dependent starts, but no
                    // dispatch path can assign their work in the
                    // emit→break window (the post-raise assignment
                    // leak).
                    self.emit_run_fail_signal(WorkerMgmtSignal::PolicyFatalExit {
                        reason: format!("on_phase_end hook for phase {p} raised: {reason}"),
                    });
                }
                // Apply any commands the on_phase_end callback queued
                // via the in-runtime PrimaryHandle path. Without this,
                // a queued SpawnTasks would sit on the channel until
                // the next operational-loop select! tick — but the
                // cascade's next drain_empty_active_phases poll runs
                // BEFORE that tick and would see the not-yet-applied
                // next phase as empty, false-firing on_phase_end(.., 0,
                // 0) and dropping every callback-injected task. Drain-
                // dispatch is the same handler the operational loop's
                // command arm uses, so the per-command CRDT broadcast
                // + pool reinjection semantics are identical to a
                // channel-delivered command (no parallel apply path,
                // no shape divergence).
                // Drain one command at a time so each `try_recv` borrow
                // releases before the dispatch re-borrows `command_rx`
                // (the recursive cascade fired by e.g.
                // `apply_fail_permanent` needs `&mut command_rx` to
                // drain its OWN post-callback queue). Using
                // `.ok()` collapses the recv result into an
                // `Option<Cmd>` so the match-borrow on `command_rx`
                // doesn't escape the let-binding.
                //
                // `Box::pin` breaks the async-recursion cycle
                // (process_phase_lifecycle → handle_primary_command →
                // apply_fail_permanent → note_item_failed →
                // process_phase_lifecycle); without it the compiler
                // can't size the future. Pinned at THIS site (rather
                // than e.g. on `apply_fail_permanent`) because the
                // cascade re-entry only happens via this dispatch
                // call — so the box allocation is gated on a
                // callback actually queueing a command.
                self.drain_callback_queued_commands(command_rx).await;
                // Run-terminal re-check AFTER the drain: the hook's own
                // queued commands can trip the #3b invalidation (a spawn
                // batch carrying a duplicate identity), which latches the
                // verdict mid-cascade. The end edge deliberately does NOT
                // complete here (no `PhaseEnded`, no `mark_phase_done`,
                // no dependent starts) — the run is aborting and the
                // frozen dispatch view already guarantees nothing
                // downstream can be assigned.
                if self.run_terminal_cascade_gate() {
                    return;
                }
                // Per-phase proceed-or-fail decision, evaluated AFTER the
                // retry-bucket cascade has exhausted every reinjection
                // path (both buckets above returned `false`) and BEFORE
                // the phase is marked done. On PROCEED the phase advances
                // exactly as before (mark_phase_done flips dependents
                // Blocked → Active) — this is the path taken by every
                // phase that produced a completion OR whose failures are
                // permanent-and-recorded (the canonical retry-bucket-
                // exhaustion contract: advance, surface fail_* in the
                // outcome summary). On FAIL — the genuine wedge where a
                // phase reached this drain with NO terminal accounting yet
                // still owns residual work — we EMIT RunShouldFail onto
                // the decoupled worker-management bus (which owns the
                // clean-shutdown drive) and leave the phase un-done. The
                // emit is a pure signal; the phase layer never drives
                // shutdown directly (decoupling law). See
                // `phase_can_proceed` for the exact policy.
                if self.phase_can_proceed(p) {
                    // Originate the replicated "phase ended" fact (#343) at
                    // the SAME decision point as `mark_phase_done` — the
                    // fact is that call's replicated counterpart ("the end
                    // edge COMPLETED: hook fired, hook-queued commands
                    // drained, phase advancing"). A promoted primary's
                    // hydrate consumes it to seed this phase straight to
                    // `Done` WITHOUT re-firing `on_phase_end` (#326), while
                    // its absence makes a never-ended terminal-only phase
                    // (the fresh all-skipped shape) flow through the live
                    // cascade and fire for the first time. Originated AFTER
                    // `drain_callback_queued_commands` (above) so the
                    // hook's injection mutations precede the fact on the
                    // wire: a death in between re-fires the hook on the
                    // next primary and the deterministic re-spawn is
                    // absorbed by the idempotent failover-replay dedup —
                    // the fail-SAFE side. NOT originated on the raise /
                    // fail-loud branches: an end edge that did not complete
                    // must REPLAY on the next primary, not be suppressed.
                    self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::PhaseEnded {
                        phase: p.clone(),
                    }])
                    .await;
                    self.pool_mut().mark_phase_done(p);
                } else {
                    // Same run-fail chokepoint as the raise branch
                    // above: the emit synchronously freezes dispatch.
                    self.emit_run_fail_signal(WorkerMgmtSignal::RunShouldFail {
                        reason: format!(
                            "phase {p} reached drain with no terminal \
                             outcome ({completed} completed, {failed} \
                             failed) — either it still owns unresolved \
                             residual work, or it is a non-leaf phase that \
                             was never injected / discovered (advancing \
                             would strand its dependents on inputs that \
                             were never produced)"
                        ),
                    });
                }
            }
            // mark_phase_done may have flipped Blocked → Active for
            // dependents; emit on_phase_start for them.
            self.fire_initial_phase_starts();
            // Newly-Active dependents may themselves be empty (a phase
            // chain like 0→1→2→3 with all items in phase 3 cascades
            // through this branch on every iteration). Re-drain so the
            // next poll_drain_transitions catches them and the loop
            // continues; without this the cascade stops one phase
            // short and items in the final phase never dispatch.
            self.pool_mut().drain_empty_active_phases();
        }
    }

    /// Per-completion bookkeeping shared between `handle_task_complete`
    /// and the failover path: increments per-phase counters and runs
    /// the lifecycle cascade. Decoupled so the call sites stay focused
    /// on their wire-message logic.
    ///
    /// `task_id` carries the per-task identifier so the pool can resolve
    /// `task_depends_on` edges. Pass `Some(id)` for successful
    /// completions; transient failures should call `note_item_failed`
    /// instead (which suppresses the dep-resolution side-effect).
    ///
    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the cascade so callback-issued in-runtime
    /// `PrimaryHandle` commands apply inline (see
    /// `process_phase_lifecycle` doc).
    pub(super) async fn note_item_completed(
        &mut self,
        phase_id: &PhaseId,
        task_id: Option<&str>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // The per-phase Completed EVENT tally (F4) is NOT bumped here: the
        // single bump owner is the `merge_task_state` join (#358), which the
        // caller's `ClusterMutation::TaskCompleted` apply already ran BEFORE
        // this bookkeeping fires — so the cascade below reads a tally that
        // includes the triggering task, and every MIRROR bumped identically
        // on the same broadcast. A second bump here would double-count
        // against the originator's own apply-locally pass.
        self.pool_mut().on_item_finished(phase_id, task_id);
        self.process_phase_lifecycle(command_rx).await;
    }

    /// Per-failure bookkeeping. Same shape as `note_item_completed`.
    ///
    /// `task_id` + `kind` decide how the POOL observes the failure:
    ///
    /// * `Some(id)` with a non-`Unfulfillable` `kind` — the
    ///   retry-pending failure marker
    ///   (`pool.on_item_failed_pending_retry`): the in-flight count
    ///   drops AND the id is soft-marked so blocked dependents doomed by
    ///   it stop holding the phase's drain edge hostage. Dependents are
    ///   NOT cascaded yet — the drain edge's retry buckets may reinject
    ///   the task (revival clears the marker); only when they decline
    ///   does `finalize_phase_soft_failures` make the failure permanent
    ///   and cascade. Without the marker, a terminally-failed prereq's
    ///   blocked dependents kept the phase `Draining` forever, the drain
    ///   edge never came, and the run hung after the failure report (the
    ///   distributed-local-subprocess e2e hang, 2026-06-10).
    /// * `Some(id)` with `kind = Unfulfillable` — dependents stay
    ///   BLOCKED (no marker): unfulfillable is the operator-resolvable
    ///   class, revived through the reinject command / fulfillability
    ///   matcher rather than the phase retry buckets, and its dependents
    ///   dormancy is the deliberate contract (`apply_fail_permanent`'s
    ///   cascade-pause split).
    /// * `None` task_id or `None` kind — the legacy in-flight-only
    ///   decrement (`on_item_finished(phase, None)`), for callers whose
    ///   failure identity/permanence the pool already observed (e.g.
    ///   `apply_fail_permanent`, whose `on_item_failed_permanent` call
    ///   precedes this bookkeeping).
    pub(super) async fn note_item_failed(
        &mut self,
        phase_id: &PhaseId,
        task_id: Option<&str>,
        kind: Option<&dynrunner_core::ErrorType>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        // Same as `note_item_completed`: the per-phase Failed EVENT tally
        // (F4) bump is owned by the `merge_task_state` join (#358) — the
        // caller's `ClusterMutation::TaskFailed` apply bumped it before this
        // bookkeeping runs, identically on every mirror.
        match (task_id, kind) {
            (Some(id), Some(k))
                if !matches!(k, dynrunner_core::ErrorType::Unfulfillable { .. }) =>
            {
                self.pool_mut().on_item_failed_pending_retry(phase_id, id);
            }
            _ => {
                self.pool_mut().on_item_finished(phase_id, None);
            }
        }
        self.process_phase_lifecycle(command_rx).await;
    }
}
