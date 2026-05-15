//! Wire-format mutations for the replicated cluster ledger.
//!
//! See `dynrunner_manager_distributed::cluster_state` for the in-memory
//! state machine that consumes these mutations.

use std::collections::HashMap;

use dynrunner_core::{ErrorType, PhaseId, WorkerId, TaskInfo};
use serde::{Deserialize, Serialize};

use crate::removal_cause::RemovalCause;

/// One CRDT mutation. Idempotent under repetition; safe under reorder
/// within the per-task happens-before constraint that the dispatcher
/// emits `TaskAdded` before any subsequent mutation for the same hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "I: Serialize",
    deserialize = "I: for<'a> Deserialize<'a>",
))]
pub enum ClusterMutation<I> {
    TaskAdded { hash: String, task: TaskInfo<I> },
    TaskAssigned {
        hash: String,
        secondary: String,
        worker: WorkerId,
    },
    TaskCompleted { hash: String },
    TaskFailed {
        hash: String,
        kind: ErrorType,
        error: String,
    },
    PrimaryChanged { new: String, epoch: u64 },
    /// Per-run static phase dependency graph. Emitted once by the
    /// primary at run start (alongside the bulk `TaskAdded` batch);
    /// receivers store it on their `ClusterState` so the post-promotion
    /// hydration path has the same dependency machine the live primary
    /// used. Re-application is a no-op when the local map is already
    /// non-empty (the graph is static for the run's lifetime).
    PhaseDepsSet {
        deps: HashMap<PhaseId, Vec<PhaseId>>,
    },
    /// "The run is done — every secondary should drain and exit."
    ///
    /// Emitted exactly once by the primary just before it returns
    /// from `run()`, after `run_retry_passes` settles. Without this
    /// signal, non-promoted secondaries (which were waiting for
    /// PromotePrimary or driving their workers via the promoted
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
    TaskReinjected { hash: String },
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
    },
    /// A peer has joined the cluster. The apply rule maintains the
    /// replicated `peer_state` map on `ClusterState` and the legacy
    /// `RoleTable.observers` projection that election filtering reads.
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
    /// - When `is_observer = true` and the id was not already in
    ///   `RoleTable.observers`, the set is widened and role-change
    ///   hooks fire. `is_observer = false` is a no-op against the
    ///   observer set — only the matching `PeerRemoved` removes peers
    ///   from it.
    ///
    /// This variant is the single-writer cutover for
    /// `RoleTable.observers` and the authoritative source of "this
    /// peer is alive" in the replicated ledger.
    PeerJoined {
        peer_id: String,
        is_observer: bool,
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
}
