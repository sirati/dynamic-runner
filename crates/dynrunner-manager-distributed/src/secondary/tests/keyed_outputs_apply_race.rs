//! Regression: the promoted-secondary's TaskCompleted handling must
//! synchronise `self.cluster_state.task_outputs` BEFORE any
//! subsequent dispatch reads it via `gather_predecessor_outputs`.
//!
//! Bug shape (asm-dataset-nix, single-host --jobs 2):
//!   1. Producer task `matrix_eval` completes on the promoted
//!      secondary's own worker.
//!   2. `worker_event::TaskCompleted` calls `note_primary_item_completed`,
//!      which releases the `dep_graph` dependent in `primary_pending`
//!      via `pool.on_item_finished`.
//!   3. The same await frame issues a self-`request_task_for_worker`
//!      → `handle_primary_task_request`; this gathers
//!      `predecessor_outputs` against `self.cluster_state` (see
//!      `secondary/primary/task_request.rs`).
//!   4. The canonical `ClusterMutation::TaskCompleted` originator on
//!      this path is the demoted-local primary's
//!      `handle_task_complete` (in `primary/task/complete.rs`), reached
//!      via the `send_to_current_primary` loopback. That apply +
//!      broadcast runs in another await frame after the mpsc dequeue —
//!      strictly later than step 3.
//!   5. dep_graph dispatches with `predecessor_outputs: {}` and the
//!      worker fails with "available keys: []".
//!
//! Pre-fix, step 4 was the only writer of `cluster_state.task_outputs`
//! for own-worker completions on the promoted-secondary path. The fix
//! introduces `apply_task_completed_locally_if_primary` as a
//! synchronous local-only apply, invoked at the head of the
//! TaskCompleted arm so step 3 reads a populated cache.
//!
//! Test shape: drive the helper directly on a `SecondaryCoordinator`
//! whose `cluster_state` already has the producing task registered
//! via `TaskAdded`, and assert `cluster_state.outputs_for(task_id)`
//! returns the wire-decoded outputs. Mirror the not-primary branch to
//! pin the `is_primary` gate (off-primary nodes do NOT locally apply;
//! the receive-side broadcast apply is canonical on that path).

#![cfg(test)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use dynrunner_core::{
    PhaseId, ResultValue, SoftPreferredSecondaries, TaskInfo, TaskOutputs, TypeId,
};
use dynrunner_protocol_primary_secondary::ClusterMutation;

use super::super::test_helpers::{election_config, make_secondary, TestId};

/// Build a `TaskInfo` whose `task_id` is `Some(name)` — the
/// `cluster_state` `task_outputs` cache keys on `task_id`, so the
/// producing task must carry one for the cache to populate.
fn mk_task(name: &str) -> TaskInfo<TestId> {
    TaskInfo {
        path: PathBuf::from(format!("/tmp/{name}")),
        size: 100,
        identifier: TestId(name.into()),
        phase_id: PhaseId::from("p"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: name.into(),
        task_depends_on: vec![],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    }
}

/// Build the Python encoder's wire bytes for a `result_data` payload
/// carrying `outputs` only — byte-identical to
/// `_encode_done_payload`'s output for `WorkerOutput()` (default zero
/// counters) + a non-empty `_outputs_accumulator`. Mirrors the helper
/// in `cluster_state/tests/task_outputs.rs`.
fn encode_wire(outputs: &TaskOutputs) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "outputs": outputs,
    }))
    .expect("encode wire")
}

fn outputs_with(key: &str, value: &str) -> TaskOutputs {
    let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
    m.insert(key.to_string(), ResultValue::Inline(value.to_string()));
    TaskOutputs(m)
}

/// (1) When `is_primary == true`, the helper synchronously applies
/// `ClusterMutation::TaskCompleted` so `cluster_state.outputs_for`
/// returns the just-completed task's outputs in the SAME await frame.
/// This is the invariant `secondary/primary/task_request.rs`'s
/// `gather_predecessor_outputs` depends on for keyed-outputs
/// dispatch.
#[tokio::test(flavor = "current_thread")]
async fn promoted_secondary_locally_applies_task_completed() {
    let mut sec = make_secondary(election_config("sec-0"));
    sec.is_primary = true;
    // Seed the producing task in the CRDT so `record_task_outputs`'s
    // hash → task_id resolution succeeds. Without `TaskAdded` the
    // helper would NoOp on the hash lookup in apply.rs and the cache
    // would stay empty regardless of the fix.
    sec.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: "h-producer".into(),
        task: mk_task("producer"),
    });
    let outputs = outputs_with("matrix_size", "42");
    let bytes = encode_wire(&outputs);

    assert!(
        sec.cluster_state.outputs_for("producer").is_none(),
        "pre-condition: task_outputs is empty before the local apply"
    );

    sec.apply_task_completed_locally_if_primary(
        "h-producer".into(),
        Some(bytes),
    );

    assert_eq!(
        sec.cluster_state.outputs_for("producer"),
        Some(&outputs),
        "post-condition: task_outputs is populated synchronously \
         (this is what gather_predecessor_outputs reads in the same \
         await frame on the same-host promoted-primary path)"
    );
}

/// (2) Off-primary nodes (live-primary case, non-promoted secondaries)
/// must NOT locally apply the mutation on the worker-event path: their
/// completion is forwarded to the live primary, which is the canonical
/// originator and broadcasts back. A local apply here would duplicate
/// the receive-side apply that the inbound broadcast triggers — gated
/// out by `is_primary`.
#[tokio::test(flavor = "current_thread")]
async fn non_promoted_secondary_does_not_locally_apply() {
    let mut sec = make_secondary(election_config("sec-0"));
    // `is_primary == false` by default; explicit for the test's pin.
    sec.is_primary = false;
    sec.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: "h-producer".into(),
        task: mk_task("producer"),
    });
    let outputs = outputs_with("matrix_size", "42");
    let bytes = encode_wire(&outputs);

    sec.apply_task_completed_locally_if_primary(
        "h-producer".into(),
        Some(bytes),
    );

    assert!(
        sec.cluster_state.outputs_for("producer").is_none(),
        "off-primary nodes leave the local apply to the receive-side \
         broadcast; the helper's `is_primary` gate enforces this so \
         the broadcast topology stays single-originator"
    );
}

/// (3) Pre-fix call-site sequence: replicate the bug by running
/// `gather_predecessor_outputs` BEFORE the local apply. Asserts the
/// dependent's predecessor-output for the just-completed prerequisite
/// is empty — this is exactly the symptom the consumer saw
/// ("available keys: []") and serves as a sanity check that the fix
/// flips it.
#[tokio::test(flavor = "current_thread")]
async fn pre_apply_dispatch_observes_empty_outputs() {
    use crate::primary::task::predecessor_outputs::gather_predecessor_outputs;
    use dynrunner_core::TaskDep;

    let mut sec = make_secondary(election_config("sec-0"));
    sec.is_primary = true;
    sec.cluster_state.apply(ClusterMutation::TaskAdded {
        hash: "h-producer".into(),
        task: mk_task("producer"),
    });
    // Dependent task that references "producer" — gather will look up
    // `outputs_for("producer")` and expect Some(...) once the apply has
    // run.
    let dependent = TaskInfo {
        path: PathBuf::from("/tmp/dependent"),
        size: 100,
        identifier: TestId("dependent".into()),
        phase_id: PhaseId::from("p"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: "dependent".into(),
        task_depends_on: vec![TaskDep {
            task_id: "producer".into(),
            inherit_outputs: false,
        }],
        preferred_secondaries: SoftPreferredSecondaries::default(),
        resolved_path: None,
    };

    // Pre-apply: gather sees an empty `task_outputs` entry — the
    // direct-dep contract is "present key with empty TaskOutputs"
    // (a placeholder so dependents can rely on key presence). This
    // is the symptom shape the consumer saw before the fix.
    let pre = gather_predecessor_outputs(&sec.cluster_state, &dependent);
    let pre_producer = pre
        .get("producer")
        .expect("direct-dep key present even pre-apply");
    assert!(
        pre_producer.0.is_empty(),
        "pre-apply: producer outputs are empty (bug symptom: \
         'available keys: []' downstream)"
    );

    // Apply: synchronously populates `task_outputs`.
    let outputs = outputs_with("matrix_size", "42");
    let bytes = encode_wire(&outputs);
    sec.apply_task_completed_locally_if_primary(
        "h-producer".into(),
        Some(bytes),
    );

    // Post-apply: gather returns the real outputs in the same frame.
    let post = gather_predecessor_outputs(&sec.cluster_state, &dependent);
    assert_eq!(
        post.get("producer"),
        Some(&outputs),
        "post-apply: producer outputs flow through to the dependent's \
         predecessor map — the same-frame dispatch read that the bug \
         broke is now correct"
    );
}
