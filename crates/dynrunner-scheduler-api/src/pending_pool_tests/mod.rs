//! Unit tests for `PendingPool`. Registered from
//! `pending_pool/mod.rs` via `#[path]` and split across submodules
//! by tested concern. Each submodule pulls the shared fixture
//! helpers (`t`, `phase`, `pool_with`) and the parent re-exports of
//! `PendingPool` / `PendingPoolError` / `PhaseState` from this file.

use std::collections::HashMap;

pub(super) use super::{PendingPool, PendingPoolError, PhaseState};
pub(super) use dynrunner_core::{AffinityId, PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};

mod bucket_dispatch;
mod phase_graph;
mod phase_lifecycle;
mod take_first_match;
mod task_deps;
mod worker_view;

/// Test fixture: build a `TaskInfo<()>` with the provided phase / type / affinity.
/// An empty affinity string is mapped to `None` so the bucket falls into the
/// free-pool sentinel inside the pool.
pub(super) fn t(phase: &str, ty: &str, affinity: &str, size: u64) -> TaskInfo<()> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{phase}_{ty}_{affinity}_{size}")),
        size,
        identifier: (),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from(ty),
        affinity_id: if affinity.is_empty() {
            None
        } else {
            Some(AffinityId::from(affinity))
        },
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

pub(super) fn phase(s: &str) -> PhaseId {
    PhaseId::from(s)
}

pub(super) fn pool_with(phases: &[&str], deps: &[(&str, &[&str])]) -> PendingPool<()> {
    let phases: Vec<PhaseId> = phases.iter().map(|p| phase(p)).collect();
    let mut deps_map: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    for (child, parents) in deps {
        deps_map.insert(
            phase(child),
            parents.iter().map(|p| phase(p)).collect(),
        );
    }
    PendingPool::new(phases, deps_map).expect("valid graph")
}
