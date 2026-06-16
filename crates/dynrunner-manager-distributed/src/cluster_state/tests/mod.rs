//! Tests for the `cluster_state` CRDT.
//!
//! Single concern: pin the per-mutation apply semantics, the snapshot
//! /restore lattice merge, the peer-lifecycle role-table projection,
//! the dispatcher-channel emit boundaries, and the per-peer resource-
//! holdings round-trip.

use super::*;
use dynrunner_core::{
    ErrorType, PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskInfo, TaskVersion, TypeId,
};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, RemovalCause, RoleChangeHookRegistrar, RoleTable,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

mod affine;
mod apply_basics;
mod blocked_by_index;
mod cascade_and_reinject;
mod convergence;
mod custom_message_outcome;
mod custom_messages;
mod digest;
mod discovery_debt;
mod dispatchers;
mod graceful_abort;
mod grow_max;
mod non_task_convergence;
mod outcome_tally;
mod output_store;
mod panik;
mod peer_lifecycle;
mod peer_resources;
mod phase_boundary_open;
mod phase_ended;
mod range_digest;
mod respawn_ledger;
mod role_table;
mod run_aborted;
mod secondary_capacity;
mod setup_kind;
mod settled;
mod snapshot;
mod stream;
mod task_outputs;
mod task_state_change;

/// Test-only Blocked-seed helper that routes through the canonical
/// `set_task_state` write seam — keeps the `blocked_by` reverse-index (#547)
/// in sync, exactly like the production apply arms do. Pre-fix tests that
/// inserted `TaskState::Blocked` directly via `s.tasks.insert` (a path that
/// also bypasses the range-fold memo and the #520 narration emit) silently
/// broke the cascade tests once the index became load-bearing for
/// `resume_blocked_on`.
pub(super) fn seed_blocked(
    state: &mut ClusterState<RunnerIdentifier>,
    hash: &str,
    task: TaskInfo<RunnerIdentifier>,
    on: String,
    attempt: u32,
) {
    state.rewrite_blocked_for_test(hash, on, task, attempt);
}

pub(super) fn mk_task(name: &str) -> TaskInfo<RunnerIdentifier> {
    TaskInfo {
        path: PathBuf::from(format!("/tasks/{name}")),
        size: 0,
        identifier: RunnerIdentifier::from(name),
        phase_id: PhaseId::from("p0"),
        type_id: TypeId::from("t0"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: Vec::new(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        required_files: None,
        resolved_path: None,
    }
}
