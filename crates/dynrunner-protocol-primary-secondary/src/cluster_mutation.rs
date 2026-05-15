//! Wire-format mutations for the replicated cluster ledger.
//!
//! See `dynrunner_manager_distributed::cluster_state` for the in-memory
//! state machine that consumes these mutations.

use std::collections::HashMap;

use dynrunner_core::{ErrorType, PhaseId, WorkerId, TaskInfo};
use serde::{Deserialize, Serialize};

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
    /// A peer has joined the cluster. The minimal apply rule for
    /// this variant updates *only* the replicated observer set:
    /// when `is_observer = true`, `peer_id` is inserted into
    /// `RoleTable.observers` (idempotent — set semantics);
    /// `is_observer = false` is a no-op at this stage of the
    /// unification refactor.
    ///
    /// TODO(Batch D): widen this variant's apply semantics to
    /// cover the full peer-lifecycle apply rule (member roster,
    /// liveness window, etc.). The narrow observer-only apply
    /// here is the single-writer cutover for `RoleTable.observers`
    /// — replacing the legacy `ClusterState::set_observers` write
    /// path so observer membership flows through one CRDT path
    /// only. Removal of an observer (or any peer) waits on the
    /// matching `PeerRemoved` variant that Batch D will introduce.
    PeerJoined {
        peer_id: String,
        is_observer: bool,
    },
}
