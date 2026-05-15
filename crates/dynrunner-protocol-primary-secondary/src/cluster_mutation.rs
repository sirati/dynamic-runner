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
    /// External-control reinjection: the primary's
    /// `PrimaryHandle::reinject_task` accepts a hash whose ledger
    /// state is `Failed { NonRecoverable, ... }` (the
    /// "operator-resolvable failure" class, e.g. unfulfillable
    /// resource request) and transitions the task back to `Pending`
    /// so the next dispatch tick re-attempts it. Broadcast so every
    /// node's CRDT mirror moves the entry off `Failed` synchronously
    /// with the originator; the live primary's pool then picks the
    /// hash up via the standard reinject path.
    ///
    /// Re-application is a no-op when the local state isn't
    /// `Failed`. Carries no error/attempts payload: the entry's
    /// previous `last_error`/`attempts` belong to the pre-reinject
    /// failure attempt and are reset.
    TaskReinjected { hash: String },
    /// External-control update of the per-task preferred-secondaries
    /// list. The Phase-4 dispatch policy consults this field when
    /// picking a worker; this mutation lets external control planes
    /// (PyO3 `PrimaryHandle::update_preferred_secondaries`, future
    /// scheduler advisories) update it mid-run.
    ///
    /// NOTE: the per-task `preferred_secondaries` storage on
    /// `TaskInfo` and the dispatch-side consumer of this mutation
    /// land with the Phase-4 preferred-secondaries field. This
    /// variant exists today so the command-channel ingress is
    /// wireable end-to-end; the apply side is a typed NoOp until
    /// Phase-4 lands.
    // TODO(phase-4): consume on the apply side once
    // `TaskInfo.preferred_secondaries` exists.
    TaskPreferredSecondariesUpdated {
        hash: String,
        secondaries: Vec<String>,
    },
}
