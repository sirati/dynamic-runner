//! Unit tests for `PendingPool`. Registered from
//! `pending_pool/mod.rs` via `#[path]` and split across submodules
//! by tested concern. Each submodule pulls the shared fixture
//! helpers (`t`, `phase`, `pool_with`) and the parent re-exports of
//! `PendingPool` / `PendingPoolError` / `PhaseState` from this file.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) use super::{PendingPool, PendingPoolError, PhaseState};
pub(super) use dynrunner_core::{
    AffinityId, PhaseId, SoftPreferredSecondaries, TaskInfo, TaskKind, TypeId,
};

mod bucket_dispatch;
mod dispatch_backoff;
mod partition;
mod phase_graph;
mod phase_lifecycle;
mod query_predicates;
mod reservation;
mod setup_kind;
mod take_first_match;
mod task_deps;
mod worker_view;

/// Monotonic per-fixture counter so each `t(...)`-built task gets a
/// unique synthetic task_id. The framework's contract is that every
/// task_id is unique within a run; the fixture honours that even
/// where the test body doesn't reference the id, so dedup-validation
/// never trips on an unrelated test fixture.
static T_FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Test fixture: build a `TaskInfo<()>` with the provided phase / type / affinity.
/// An empty affinity string is mapped to `None` so the bucket falls into the
/// free-pool sentinel inside the pool. Synthesises a unique
/// per-call `task_id` to satisfy the boundary contract.
pub(super) fn t(phase: &str, ty: &str, affinity: &str, size: u64) -> TaskInfo<()> {
    let n = T_FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
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
        task_id: format!("fixture-{n}"),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        resolved_path: None,
    }
}

/// Test fixture twin of [`t`] that builds a `TaskKind::Setup` task —
/// the same shape, but the first-class kind flips so the scheduling
/// seam (`view_for_worker` / `pop_for_worker`) must treat it as
/// non-worker-assignable.
pub(super) fn setup_t(phase: &str, ty: &str, affinity: &str, size: u64) -> TaskInfo<()> {
    let mut task = t(phase, ty, affinity, size);
    task.kind = TaskKind::Setup;
    task
}

pub(super) fn phase(s: &str) -> PhaseId {
    PhaseId::from(s)
}

pub(super) fn pool_with(phases: &[&str], deps: &[(&str, &[&str])]) -> PendingPool<()> {
    let phases: Vec<PhaseId> = phases.iter().map(|p| phase(p)).collect();
    let mut deps_map: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
    for (child, parents) in deps {
        deps_map.insert(phase(child), parents.iter().map(|p| phase(p)).collect());
    }
    PendingPool::new(phases, deps_map).expect("valid graph")
}
