//! Wire-format mutations for the replicated cluster ledger.
//!
//! See `dynrunner_manager_distributed::cluster_state` for the in-memory
//! state machine that consumes these mutations.

use std::collections::HashMap;

use dynrunner_core::{ErrorType, PhaseId, ResourceAmount, TaskInfo, TaskVersion, WorkerId};
use serde::{Deserialize, Serialize};

use crate::removal_cause::RemovalCause;

/// The static, per-secondary capacity a secondary advertises once at
/// connect time: how many worker slots it can run concurrently and the
/// opaque resource amounts it brought to the cluster.
///
/// This is the value half of the replicated capacity map (see
/// `dynrunner_manager_distributed::cluster_state`) and the payload the
/// [`ClusterMutation::SecondaryCapacity`] variant carries. It is static
/// for a secondary's lifetime in the run — the framework records it once
/// and never overwrites it (set-once apply semantics), so a freshly-
/// promoted primary and late-joining observers reconstruct the full
/// roster from the replicated map alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryCapacityRecord {
    /// Concurrent worker slots the secondary can run.
    pub worker_count: u32,
    /// Opaque resource amounts advertised at connect. The framework
    /// does not interpret these; downstream scheduler / matcher policy
    /// attaches meaning (same opacity contract as `peer_holdings`).
    pub resources: Vec<ResourceAmount>,
}

/// Replicated per-RUN "discovery owed" fact (V6) — a THREE-state
/// sticky-monotone scalar lattice. The value half of the discovery-debt
/// CRDT field: set by [`ClusterMutation::DiscoveryDebtDeclared`] /
/// [`ClusterMutation::DiscoverySettled`], summarised verbatim into
/// [`crate::StateDigest`], and carried through the cluster-state snapshot.
///
/// The join is `max` over the total order `Undeclared ⊑ Owed ⊑ Settled`;
/// once a replica reaches a higher state it never reverts. Three states
/// (not two) because the cold/never-declared BOTTOM and the post-settle TOP
/// must be DISTINCT — collapsing them makes the apply rule self-contradict
/// (a cold replica's first `Declared` must set `Owed`, while a stale
/// `Declared` redelivered after `Settled` must be a NoOp; with one shared
/// value the apply rule cannot tell them apart). A distinct `Undeclared`
/// bottom resolves it: `Declared` is Applied ONLY from `Undeclared`.
///
/// Consumer contract: every reader (run_complete_check, the cascade gate,
/// the discover-on-promotion driver) checks `== Owed`. `Undeclared` and
/// `Settled` are both `!= Owed`, so a run that never declares debt (cold
/// mode-1 / legacy) is `Undeclared` from t0 and unaffected.
///
/// Lives in the protocol crate (not `cluster_state`) because it crosses the
/// wire inside [`crate::StateDigest`] (the anti-entropy digest carries the
/// full lattice height so the detector can compare it in both directions —
/// a bool would conflate `Undeclared` and `Settled` and silently break
/// convergence). Sibling to [`SecondaryCapacityRecord`].
///
/// The variants are declared in lattice order so the DERIVED `Ord` IS the
/// lattice order (`Undeclared < Owed < Settled`): the snapshot/restore join
/// is `max`, and the AE detector is "behind iff the peer is STRICTLY
/// higher" (`self < other`). Do NOT reorder the variants — the derived
/// `Ord` is load-bearing.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Serialize, Deserialize, Hash,
)]
pub enum DiscoveryDebt {
    /// Lattice BOTTOM (the default): no discovery has been declared. The
    /// cold-seeded (mode-1 / legacy) shape and the wire-compat decode of a
    /// pre-field frame. `!= Owed`, so every completion check proceeds.
    #[default]
    Undeclared,
    /// Discovery is owed: the run was relocated to a compute peer that must
    /// run `discover_items` and seed the ledger before any completion
    /// check. The lattice MIDDLE. The discover-on-promotion driver (Phase
    /// 5b) gates on this state.
    Owed,
    /// Lattice TOP: discovery has completed (or the empty-corpus terminal
    /// was originated). Sticky — once `Settled` a replica never reverts,
    /// and an `Owed`/`Undeclared` observation never overwrites it.
    Settled,
}

/// Why a `PrimaryChanged` was originated. Advisory routing metadata
/// only — the CRDT apply rule and snapshot merge are `reason`-BLIND
/// ("highest epoch wins, one primary" never reads it). It distinguishes
/// a node naming ITSELF primary (an election win / self-announce) from
/// the submitter handing authority to a DIFFERENT chosen peer (a
/// bootstrap transfer), so a receiver can route a transfer through its
/// setup FSM rather than the failover-self path.
///
/// `#[serde(default)]` on the carrying field defaults a wire frame with
/// no reason to [`Self::Election`]; this project does coordinated
/// restarts, so a frame from a peer running an older crate (which omits
/// the field) is safely read as the self-announce shape that was the
/// only shape before this field existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PrimaryChangeReason {
    /// A node named ITSELF primary (`new == originator`): an election
    /// win (`fire_local_promotion`) or the bootstrap/failover self-
    /// announce (`originate_primary_changed`). The default.
    #[default]
    Election,
    /// The submitter named a DIFFERENT chosen peer (`new != originator`):
    /// a bootstrap transfer of full primary authority to a compute peer.
    Transferred,
}

/// One CRDT mutation. Idempotent under repetition; safe under reorder
/// within the per-task happens-before constraint that the dispatcher
/// emits `TaskAdded` before any subsequent mutation for the same hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "I: Serialize", deserialize = "I: for<'a> Deserialize<'a>",))]
pub enum ClusterMutation<I> {
    TaskAdded {
        hash: String,
        task: TaskInfo<I>,
    },
    TaskAssigned {
        hash: String,
        secondary: String,
        worker: WorkerId,
        /// Primary-stamped assignment-lifecycle version (D-V). Stamped at
        /// the origination choke point; the receiver writes it onto the
        /// resulting `InFlight` state so a stale (pre-reset) assignment
        /// loses to a higher-version requeue/reinject reset. Defaults to
        /// the `(0, 0)` strict minimum for a legacy sender.
        #[serde(default)]
        version: TaskVersion,
        /// Primary-stamped retry-attempt generation (F2). Stamped at the
        /// SAME origination choke point that stamps `version`, reading the
        /// task's CURRENT attempt from the ledger; the receiver writes it
        /// onto the resulting `InFlight` so a worker outcome for the
        /// retried generation out-ranks the `TaskRetried` reset, and a
        /// stale assignment for a lower attempt LOSES. `#[serde(default)]`
        /// decodes a legacy sender's frame to attempt-0 (the cold
        /// generation).
        #[serde(default)]
        attempt: u32,
    },
    TaskCompleted {
        hash: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result_data: Option<Vec<u8>>,
        /// Primary-stamped retry-attempt generation (F2). Stamped at the
        /// origination choke point (reads the task's current attempt); the
        /// receiver writes it onto the resulting `Completed` so a late
        /// stale outcome for a lower attempt cannot resurrect a higher-
        /// generation reset. `#[serde(default)]` decodes a legacy sender's
        /// frame to attempt-0.
        #[serde(default)]
        attempt: u32,
    },
    TaskFailed {
        hash: String,
        kind: ErrorType,
        error: String,
        /// Primary-stamped terminal-payload version (D-V). Stamped at the
        /// origination choke point; lets two divergent failure records
        /// converge on the higher version (and the per-task content hash
        /// settles an equal-version divergence). Defaults to `(0, 0)`.
        #[serde(default)]
        version: TaskVersion,
        /// Primary-stamped retry-attempt generation (F2). Stamped at the
        /// origination choke point (reads the task's current attempt); the
        /// receiver writes it onto the resulting `Failed` so the retry
        /// originator reads the current generation to mint the next reset.
        /// `#[serde(default)]` decodes a legacy sender's frame to
        /// attempt-0.
        #[serde(default)]
        attempt: u32,
    },
    PrimaryChanged {
        new: String,
        epoch: u64,
        /// Why the primary changed (advisory routing metadata; the
        /// epoch-LWW apply rule and snapshot merge ignore it). See
        /// [`PrimaryChangeReason`]. `#[serde(default)]` makes a frame
        /// from a peer that predates this field read as
        /// [`PrimaryChangeReason::Election`] — the only shape that
        /// existed before, wire-safe under coordinated restart.
        #[serde(default)]
        reason: PrimaryChangeReason,
    },
    /// Per-run static phase dependency graph. Emitted once by the
    /// primary at run start (alongside the bulk `TaskAdded` batch);
    /// receivers store it on their `ClusterState` so the post-promotion
    /// hydration path has the same dependency machine the live primary
    /// used. Re-application is a no-op when the local map is already
    /// non-empty (the graph is static for the run's lifetime).
    PhaseDepsSet {
        deps: HashMap<PhaseId, Vec<PhaseId>>,
    },
    /// Per-run static set of phases the consumer declared MAY drain with
    /// zero dispatched items (`PhaseSpec.may_be_empty`) — a pure
    /// sequencing gate that legitimately has no work of its own. Emitted
    /// once by the primary at run start, paired with [`Self::PhaseDepsSet`]
    /// (same static-graph lifecycle), and stored on every receiver's
    /// `ClusterState` so the post-promotion empty-drain proceed-or-fail
    /// policy sees the same opt-out the live primary used.
    ///
    /// A SEPARATE mutation rather than a field on `PhaseDepsSet`: the
    /// opt-out is its own single concern (the honest empty-drain policy),
    /// it is empty on the overwhelmingly-common no-opt-out run, and a new
    /// variant is wire-safe without touching the many `PhaseDepsSet`
    /// originators — a peer that predates this variant simply never emits
    /// it and treats its absence as "no phase opted out". `Vec<PhaseId>`
    /// wire shape (collected into a `HashSet` on apply) for the same
    /// deterministic-ordering reason the other set-shaped wire fields use
    /// `Vec`. Re-application is a no-op once the local set is seeded
    /// (static for the run), mirroring `PhaseDepsSet`.
    PhaseMayBeEmptySet {
        phases: Vec<PhaseId>,
    },
    /// "The run is done — every secondary should drain and exit."
    ///
    /// Emitted exactly once by the primary just before it returns
    /// from `run()`, after `run_retry_passes` settles. Without this
    /// signal, non-promoted secondaries (which were waiting for a
    /// `PrimaryChanged` or driving their workers via the promoted
    /// peer) have no termination cue when the local primary
    /// disconnects: they enter failover detection, can't tell the
    /// run is genuinely over vs. just a primary crash, and stay
    /// alive holding SLURM job slots indefinitely.
    ///
    /// Receivers set a local `run_complete` flag; the operational
    /// loop's exit condition broadens to `run_complete && pool
    /// drained` so the post-promotion residual peers all exit
    /// shortly after the primary returns.
    RunComplete,
    /// "The run was ABORTED — every secondary and observer should exit
    /// non-zero." The failure twin of [`Self::RunComplete`].
    ///
    /// Emitted exactly once by the primary when an unrecoverable
    /// cluster-wide fault is detected BEFORE any phase has started —
    /// today the only originator is the pre-phase duplicate-task-id
    /// case (#3a): a `(phase_id, task_id)` collision in the INITIAL
    /// batch is a producer-side bug that would silently mask one of the
    /// colliding tasks, so the whole run is torn down rather than
    /// proceeding on an ambiguous task set. (A duplicate detected AFTER
    /// a phase started — #3b — does NOT abort: it invalidates the
    /// not-yet-terminal tasks run-wide and the cluster CONTINUES.)
    ///
    /// Receivers set a sticky `run_aborted: Option<String>` ledger
    /// field (mirroring `run_complete`). The secondary's
    /// `process_tasks` loop checks `run_aborted()` BEFORE the
    /// `run_complete()` break and returns `RunOutcome::Terminal`
    /// (projecting to `SecondaryTerminal::Aborted`), which
    /// the secondary / observer PyO3 wrappers translate to
    /// `std::process::exit(1)`. The primary itself surfaces a structured
    /// `RunError` at its own PyO3 boundary. Broadcast over the SAME
    /// `apply_and_broadcast_cluster_mutations` path as `RunComplete`, so
    /// it inherits the identical delivery / settle semantics.
    RunAborted {
        reason: String,
    },
    /// "This run still OWES discovery." Sets the replicated
    /// `discovery_debt` lattice to `Owed`.
    ///
    /// Originated by the mode-2 (relocated) submitter in the SAME
    /// origination pass as the phase graph, BEFORE relocating: the
    /// submitter has no local corpus to discover (the files are already on
    /// the cluster), so instead of seeding `TaskAdded`s it declares the
    /// debt + the `PhaseDepsSet`, and the empty CRDT + this marker IS the
    /// "awaiting seed" state the compute-peer primary later settles.
    ///
    /// Payload-free latch-SET, NOT a `Set(bool)` toggle — the monotonicity
    /// lives in the apply rule, not the wire payload: a `Declared` that
    /// arrives AFTER [`Self::DiscoverySettled`] is a NoOp (`Settled` is the
    /// sticky lattice TOP), so reordered delivery can never un-settle a run.
    /// This is the same payload-free-latch reason [`Self::RunComplete`] is
    /// not a `SetRunComplete(bool)`.
    DiscoveryDebtDeclared,
    /// "Discovery is now DONE." Ratchets the replicated `discovery_debt`
    /// lattice to `Settled` (the TOP). The failover-safe twin of
    /// [`Self::DiscoveryDebtDeclared`].
    ///
    /// Originated by the compute-peer primary's discover-on-promotion
    /// driver as the FINAL mutation of the same batch that carries the
    /// discovered `PhaseDepsSet` + `TaskAdded` set (or, on the empty-corpus
    /// path, alongside the `RunComplete`), so "the tasks are in the CRDT"
    /// and "the debt is cleared" land atomically on the wire — no window
    /// where a peer sees `Settled` without the tasks.
    ///
    /// Sticky-monotone apply: ratchets `Owed → Settled` and is `Applied`
    /// iff it changed the local value, else a NoOp; once `Settled` it never
    /// reverts (mirrors [`Self::RunComplete`]'s `false → true` ratchet,
    /// with `Settled` as the latched TOP).
    DiscoverySettled,
    /// External-control reinjection: the primary's
    /// `PrimaryHandle::reinject_task` accepts a hash whose ledger
    /// state is the discrete `TaskState::Unfulfillable { .. }` variant
    /// (the operator-resolvable-failure class — a required cluster
    /// resource that wasn't held by any peer at dispatch time) and
    /// transitions the task back to `Pending` so the next dispatch
    /// tick re-attempts it. Broadcast so every node's CRDT mirror
    /// moves the entry off `Unfulfillable` synchronously with the
    /// originator; the live primary's pool then picks the hash up via
    /// the standard reinject path.
    ///
    /// Re-application is a no-op when the local state isn't
    /// `Unfulfillable`. Carries no reason payload: the entry's
    /// previous `reason` belongs to the pre-reinject Unfulfillable
    /// state and is reset on transition to Pending.
    TaskReinjected {
        hash: String,
        /// Primary-stamped reset version (D-V / C3). A reinject is an
        /// authoritative rank-DROP (`Unfulfillable → Pending`); the
        /// stamped version is written onto the resulting `Pending` so it
        /// strictly supersedes the pre-reset state and a late stale
        /// assignment cannot resurrect. Defaults to `(0, 0)`.
        #[serde(default)]
        version: TaskVersion,
    },
    /// Dead-secondary recovery requeue: the secondary that held
    /// `hash` in `TaskState::InFlight { secondary, .. }` died, so the
    /// authoritative primary takes the task back for re-dispatch and
    /// transitions the CRDT entry `InFlight → Pending`.
    ///
    /// Originated by the primary's `recover_inflight_for_dead_secondary`
    /// (one per requeued in-flight task) and broadcast through the
    /// canonical `apply_and_broadcast_cluster_mutations` path, so every
    /// replica's CRDT mirror moves the entry off `InFlight` in lockstep
    /// with the primary's local pool requeue. Without it the local pool
    /// requeue would have no CRDT counterpart: a stale `InFlight` would
    /// survive in the ledger, and on failover `hydrate_from_cluster_state`
    /// (which routes `InFlight` to the in-flight ledger, NOT the pool)
    /// would neither re-dispatch the task nor keep it dispatchable — a
    /// lost task.
    ///
    /// Distinct from [`Self::TaskReinjected`] (`Unfulfillable → Pending`,
    /// external-control resolution of a missing-resource failure): this
    /// is internal failover recovery transitioning OUT of `InFlight`, a
    /// different source state and a different concern.
    ///
    /// Re-application is a NoOp when the local state isn't `InFlight`:
    /// a terminal that arrived first wins (a `TaskCompleted` /
    /// `TaskFailed` that raced the death observation must not be
    /// resurrected to `Pending`), and an already-`Pending` entry is
    /// idempotent under at-least-once delivery. Carries no payload
    /// beyond `hash`: the `TaskInfo` preserved on the `InFlight` entry
    /// is moved into the new `Pending` state verbatim so the requeued
    /// task re-dispatches the same binary.
    TaskRequeued {
        hash: String,
        /// Primary-stamped reset version (D-V / C3). A requeue is an
        /// authoritative rank-DROP (`InFlight → Pending`); the stamped
        /// version is written onto the resulting `Pending` so it strictly
        /// supersedes the pre-reset `InFlight` and a redelivered stale
        /// `TaskAssigned` cannot resurrect the dead-secondary assignment.
        /// Defaults to `(0, 0)`.
        #[serde(default)]
        version: TaskVersion,
    },
    /// Per-phase retry reinjection: the primary's retry-bucket decided a
    /// `TaskState::Failed { attempt: n }` task gets one more attempt, so it
    /// transitions the ledger `Failed → Pending { attempt: n+1 }`.
    ///
    /// Distinct from [`Self::TaskRequeued`] (`InFlight → Pending`, dead-
    /// secondary recovery) and [`Self::TaskReinjected`] (`Unfulfillable →
    /// Pending`, external-control resolution): this is the per-phase retry
    /// budget reinjecting a genuinely-FAILED task, a different source state
    /// and a different concern. It is the ONLY band-crossing reset that
    /// BUMPS the per-task `attempt` (F2): the reset crosses the
    /// terminal→non-terminal band, where `version` is the wrong arbiter
    /// (band dominates version in the join), so the higher `attempt` — the
    /// TOP of the join key — is what makes the reset survive anti-entropy.
    /// Without the bump, a peer holding the stale `Failed { attempt: n }`
    /// would revert the reset on every `restore`/anti-entropy heal (band
    /// dominates), orphaning the in-flight retry = lost work.
    ///
    /// `attempt` is computed by the ORIGINATOR (it reads the current
    /// `Failed { attempt: n }` via the `Failed`-only F2-β gate and emits
    /// `attempt: n+1`); the apply rule trusts it. `version` is stamped at
    /// the origination choke point like the other reset variants.
    ///
    /// Apply gates `Failed`-only (mirrors `TaskReinjected`'s
    /// `Unfulfillable`-only gate): a reset cannot resurrect a `Completed` /
    /// `InvalidTask` / `Unfulfillable` / `InFlight` / `Pending` /
    /// `Blocked` task. Re-application is a NoOp (the source is no longer
    /// `Failed`). The preserved `TaskInfo` is moved into the new `Pending`
    /// so the retry re-dispatches the same binary.
    TaskRetried {
        hash: String,
        /// Originator-computed retry-attempt generation (F2): `n+1` where
        /// `n` is the current `Failed { attempt: n }`'s generation. The TOP
        /// of the per-task join key, so the reset dominates the prior
        /// `Failed` across every merge path. NOT version-stamped (it is the
        /// originator's read-derived generation, not a minted monotone-per-
        /// hash counter). `#[serde(default)]` decodes a legacy sender's
        /// frame to attempt-0.
        #[serde(default)]
        attempt: u32,
        /// Primary-stamped reset version (D-V / C3). A retry reset is an
        /// authoritative rank-DROP (`Failed → Pending`); the stamped
        /// version is written onto the resulting `Pending` so it strictly
        /// supersedes within the new attempt generation too. Defaults to
        /// `(0, 0)`.
        #[serde(default)]
        version: TaskVersion,
    },
    /// A cascade-paused dependent: `hash`'s prerequisite (identified
    /// by `on`, the prereq's task hash) just transitioned to
    /// `TaskState::Unfulfillable` and the dependent cannot make
    /// progress until the prereq is reinjected and completes.
    ///
    /// Originated by the primary's `apply_fail_permanent` when the
    /// failing task carries `ErrorType::Unfulfillable`: every
    /// transitive dependent surfaced by the pool's cascade is
    /// broadcast under this variant so every replica's CRDT converges
    /// to `TaskState::Blocked { on, task }` for it. The matching
    /// `TaskCompleted` apply arm auto-resumes any
    /// `Blocked { on: <completed hash>, .. }` entry back to `Pending`,
    /// event-driven across every replica.
    ///
    /// Distinct from `TaskFailed { kind: Unfulfillable, .. }` (which
    /// targets the originating task whose resource is missing): a
    /// Blocked dependent is dormant, not failed, and its
    /// `TaskInfo` is preserved verbatim so the eventual resume to
    /// `Pending` re-dispatches the same binary.
    TaskBlocked {
        hash: String,
        on: String,
    },
    /// Discovery-time skip: the originator determined the item's outputs
    /// already exist on the shared filesystem, so the ledger entry is
    /// materialized DIRECTLY terminal (`TaskState::SkippedAlreadyDone`) and
    /// never dispatched. The item IS a real task in the phase (so the phase
    /// is no longer "a phase without tasks that should error") — it simply
    /// reached a spawn-time terminal that never transitions, reinjects, or
    /// re-fails.
    ///
    /// Carries only `hash`: the `TaskInfo` lives on the ledger entry the
    /// prior `TaskAdded` (same batch) seeded as `Pending`, exactly like the
    /// `TaskRequeued` / `TaskReinjected` resets carry only the hash and reuse
    /// the preserved `TaskInfo`. The apply rule transitions `Pending →
    /// SkippedAlreadyDone`; any other state is a NoOp (a skip is the WEAKEST
    /// terminal — an in-flight assignment or a real terminal locks it out).
    ///
    /// A SEPARATE mutation (not a flag on `TaskAdded`, not a field on a
    /// terminal): `TaskAdded` is the universal vacant-insert-as-`Pending`, so
    /// a `skipped: bool` would force a mode-`if` into that one arm and make
    /// every caller reason about a bit it never sets. Wire-safe under rolling
    /// upgrade exactly like `PhaseMayBeEmptySet` / `DiscoveryDebtDeclared` —
    /// the originator is always the newest primary; a consumer that never
    /// marks a skip never originates it, so non-adopters see zero new wire.
    TaskSkippedAlreadyDone {
        hash: String,
    },
    /// External-control update of the per-task preferred-secondaries
    /// list. The future dispatch policy consults this field when
    /// picking a worker; this mutation lets external control planes
    /// (PyO3 `PrimaryHandle::update_preferred_secondaries`, future
    /// scheduler advisories) update it mid-run.
    ///
    /// NOTE: the per-task `preferred_secondaries` storage on
    /// `TaskInfo` and the dispatch-side consumer of this mutation
    /// land with the preferred-secondaries field. This variant exists
    /// today so the command-channel ingress is wireable end-to-end;
    /// the apply side is a typed NoOp until the field lands.
    TaskPreferredSecondariesUpdated {
        hash: String,
        secondaries: Vec<String>,
        /// Primary-stamped preferred-metadata version (D-V / R4). Stamped
        /// at the origination choke point and written onto the task's
        /// `TaskInfo.preferred_version`; two concurrent preferred updates
        /// converge on the higher version regardless of the task's state.
        /// Defaults to `(0, 0)`.
        #[serde(default)]
        version: TaskVersion,
    },
    /// A peer has joined the cluster. The apply rule maintains the
    /// replicated `peer_state` LIVENESS map on `ClusterState` and merges
    /// the join's `(is_observer, can_be_primary)` advertisement into the
    /// replicated `capabilities` 2P-set (C6 — the SINGLE source of truth
    /// for role capabilities, decoupled from liveness).
    ///
    /// Receiver semantics (see `ClusterState::apply`):
    ///
    /// - If the peer is currently `Dead` in `peer_state` the
    ///   broadcast is a NoOp; ids never resurrect, fresh ids must be
    ///   minted for respawn.
    /// - Otherwise the entry is marked `Alive` (insert-or-update,
    ///   preserving any existing pubkey/endpoint metadata) and a
    ///   `PeerLifecycleEvent::Added` is enqueued on the dispatcher
    ///   channel.
    /// - The `(is_observer, can_be_primary, cap_version)` advertisement
    ///   is merged into the `capabilities` 2P-set (`is_observer` ratchets
    ///   up; `can_be_primary` follows the higher `cap_version`). The
    ///   `RoleTable.observers` / `RoleTable.can_be_primary` sets are then
    ///   re-projected from `capability × local-alive` and role-change
    ///   hooks fire when a role-bearing mutation applied.
    ///
    /// This variant is the authoritative source of "this peer is alive"
    /// in the replicated ledger and one of the writers of the capability
    /// 2P-set (the other is `SetCanBePrimary`).
    ///
    /// `can_be_primary` is the SEPARATE, EXPLICIT per-peer capability the
    /// joining peer advertises — the twin of `is_observer`. It is NOT
    /// deduced from membership/liveness/observer status; a runtime
    /// [`Self::SetCanBePrimary`] can flip it at any time after join. The
    /// `RoleTable.can_be_primary` projection ANDs in the LOCAL alive bit
    /// at read time, so a pre-armed capability for a not-yet-alive peer is
    /// held in the 2P-set and projects in once the peer is Alive.
    /// `#[serde(default)]` (defaulting `false`) keeps wire compat with a
    /// peer that predates the field — a missing field decodes as "not
    /// primary-capable", the conservative default.
    PeerJoined {
        peer_id: String,
        is_observer: bool,
        #[serde(default)]
        can_be_primary: bool,
        /// Primary-stamped capability version (C6 / D-V). Stamped at the
        /// origination choke point and merged into the receiver's
        /// `capabilities` 2P-set; the higher `cap_version` arbitrates a
        /// `can_be_primary` flip-back so a missed `SetCanBePrimary(false)`
        /// heals. `is_observer` is a pure OR ratchet and ignores it.
        /// `#[serde(default)]` decodes a pre-field sender's frame to the
        /// `(0, 0)` strict minimum (it loses to any stamped version, so a
        /// legacy re-emit never regresses a converged capability).
        #[serde(default)]
        cap_version: TaskVersion,
    },
    /// Runtime update of a peer's primary-capability — the dedicated
    /// mutation that lets a CLIENT permit/forbid a specific peer from ever
    /// hosting the primary at any point in the run, independent of the
    /// join-time `PeerJoined { can_be_primary }` advertisement.
    ///
    /// Originated by the primary's command channel
    /// (`PrimaryCommand::SetCanBePrimary`, exposed through the framework
    /// client API) and broadcast over the canonical
    /// `apply_and_broadcast_cluster_mutations` path so every replica's
    /// `capabilities` 2P-set converges. The apply rule merges an
    /// `Advertised { can_be_primary, cap_version }` into the 2P-set (the
    /// higher `cap_version` wins, so a newer `false` beats an older
    /// `true`); the `RoleTable.can_be_primary` projection is then rebuilt
    /// from `capability × local-alive`. Idempotent: re-applying a value
    /// that does not change the merged entry is a NoOp.
    SetCanBePrimary {
        peer_id: String,
        can_be_primary: bool,
        /// Primary-stamped capability version (C6 / D-V). Stamped at the
        /// origination choke point and merged into the receiver's
        /// `capabilities` 2P-set: the higher `cap_version` wins, so a
        /// newer `false` beats an older `true` (and a re-converging node
        /// adopts the latest value, not a stale one). `#[serde(default)]`
        /// decodes a pre-field sender's frame to the `(0, 0)` strict
        /// minimum.
        #[serde(default)]
        cap_version: TaskVersion,
    },
    /// A peer has been removed from the cluster (authoritative
    /// observation by the primary; `cause` carries the reason — see
    /// [`RemovalCause`]).
    ///
    /// Sticky-per-id semantics: once a peer's `peer_state` entry is
    /// `Dead`, every subsequent mutation for the same id is a NoOp
    /// (re-`PeerRemoved` and any later `PeerJoined`). Respawning a
    /// secondary requires a fresh id; this prevents a late-arriving
    /// stale `PeerJoined` from undoing an authoritative removal.
    ///
    /// When the removed peer was an observer the entry is dropped
    /// from `RoleTable.observers` and role-change hooks fire. The
    /// apply emits a `PeerLifecycleEvent::Removed` on the dispatcher
    /// channel for downstream consumers (scheduler / telemetry).
    PeerRemoved {
        id: String,
        cause: RemovalCause,
    },
    /// A peer announces the current set of opaque resource strings it
    /// holds locally. The framework does NOT interpret the strings —
    /// downstream consumers (e.g. the asm-dataset-nix scheduler treats
    /// them as nix outpaths) attach meaning. The CRDT layer's only
    /// concern is replicating the per-peer announcement so every node
    /// converges to the same `peer_id → holdings` map.
    ///
    /// Wire shape uses `Vec<String>` rather than `HashSet<String>` to
    /// keep deterministic serde ordering and codec simplicity on the
    /// wire; the apply rule collects into a `HashSet<String>` for
    /// storage so duplicate strings inside a single announce collapse
    /// and equality checks are set-based.
    ///
    /// `epoch` carries the primary epoch under which the announcing
    /// peer believed the cluster was operating. The apply rule
    /// no-ops any announce whose `epoch < self.primary_epoch` — a
    /// stale announce from a pre-failover view of the cluster must
    /// not overwrite holdings observed under the current primary.
    /// `epoch == self.primary_epoch` and `epoch > self.primary_epoch`
    /// (a peer that already learned of a newer primary before its
    /// announce reaches us) both apply — the announce is about
    /// per-peer holdings, not about primary identity, and "newer
    /// announce wins" is the same supersede-old-pending shape the
    /// other CRDT entries use.
    ///
    /// Re-application against an unchanged set (same `peer_id`, same
    /// `holdings` as already stored) is a NoOp under the standard
    /// idempotency contract.
    PeerResourceHoldingsUpdated {
        peer_id: String,
        holdings: Vec<String>,
        epoch: u64,
    },
    /// A secondary's static, advertised capacity — the worker-slot
    /// count and resource amounts it brought to the cluster.
    ///
    /// Originated by the primary at the same point it originates
    /// `PeerJoined` (the `SecondaryWelcome` accept in `primary/connect.rs`),
    /// carrying the `worker_count` + `resources` the welcome announced.
    /// Replicated into the snapshotted `secondary_capacities` map on
    /// `ClusterState` so a freshly-promoted primary AND late-joining
    /// observers hold the full per-secondary roster the moment they
    /// restore a snapshot — without it a promoted primary starts with
    /// `alive_worker_count() == 0` and cannot dispatch (the roster was
    /// 100% primary-local and `PeerJoined` dropped the `worker_count`).
    ///
    /// Set-once apply semantics (see `ClusterState::apply`): the first
    /// apply for a given `secondary` records the record; every
    /// subsequent apply for the same id is a NoOp. Capacity is static
    /// for the secondary's lifetime in the run, so re-application
    /// (snapshot replay, redundant peer-forwarding, the idempotent
    /// PeerJoined re-emit from `send_peer_lists`) never clobbers the
    /// first-recorded value.
    SecondaryCapacity {
        secondary: String,
        worker_count: u32,
        resources: Vec<ResourceAmount>,
    },
    /// Runtime task injection: introduce a batch of brand-new
    /// `TaskInfo<I>` entries into the replicated ledger so the live
    /// primary can dispatch them and every replica's CRDT mirror
    /// converges to the new task set.
    ///
    /// Single mutation per batch (plural `tasks`): a 100-task Phase-1
    /// graph computed at runtime is ONE wire-broadcast event, not
    /// 100. The receiver iterates the inner `Vec` and applies each
    /// task against the local ledger independently — duplicates (by
    /// content hash) are silently NoOp'd, surviving entries land in
    /// the appropriate initial state based on their `task_depends_on`
    /// resolution against the existing ledger:
    ///
    ///   * No deps OR all deps `Completed` → `Pending { task }`.
    ///   * Any dep `Unfulfillable` → `Blocked { task, on: dep_hash }`.
    ///   * Any dep `Failed { NonRecoverable, .. }` → cascade-fail as
    ///     `Failed { kind: NonRecoverable, task, last_error:
    ///     "upstream-failed", version: default }`.
    ///   * Else (any dep in `Pending` / `InFlight` / `Blocked`) →
    ///     `Blocked { task, on: first-unresolved-dep-hash }`.
    ///
    /// `task_depends_on` references are by task_id (matching the
    /// existing pool-side semantics); the apply rule resolves each
    /// id to a hash via a linear scan over `self.tasks` and uses
    /// the resolved hash for `Blocked.on`. Pre-apply validation in
    /// the originator's command handler (`apply_spawn_tasks`) rejects
    /// per-task entries whose `task_depends_on` references an id not
    /// known to the ledger (those failures surface as per-index
    /// `SpawnError::UnknownDependency` on the command's reply
    /// oneshot, not as wire-side state); the apply rule therefore
    /// trusts that every dep id it encounters resolves to a present
    /// hash.
    ///
    /// Auto-resume on a later `TaskCompleted` works for free:
    /// `cluster_state::resume_blocked_on` walks every
    /// `Blocked { on, .. }` entry and resumes when the prereq's
    /// hash matches — newly-injected Blocked entries participate in
    /// the same auto-resume mechanism as cascade-paused dependents
    /// from `apply_fail_permanent`.
    TasksSpawned {
        tasks: Vec<TaskInfo<I>>,
    },
}
