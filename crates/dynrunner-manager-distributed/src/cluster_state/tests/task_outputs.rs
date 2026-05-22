//! Tests for the replicated keyed-output cache.
//!
//! Pins the `task_outputs` apply-time populate via the `TaskCompleted`
//! mutation's `result_data` payload, the `outputs_for(task_id)`
//! reader, the malformed-payload warn-and-store-empty path, and the
//! snapshot/restore round-trip.

use super::*;

use dynrunner_core::{ResultValue, TaskOutputs};
use std::collections::BTreeMap;

fn outputs_with(key: &str, value: &str) -> TaskOutputs {
    let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
    m.insert(key.to_string(), ResultValue::Inline(value.to_string()));
    TaskOutputs(m)
}

#[test]
fn outputs_for_unknown_task_id_is_none() {
    // No TaskCompleted applied yet — the cache is empty and the
    // reader returns None for any lookup. Pins the absent-key shape.
    let s = ClusterState::<RunnerIdentifier>::new();
    assert!(s.outputs_for("anything").is_none());
}

#[test]
fn task_completed_populates_task_outputs_cache() {
    // Happy path: a `TaskCompleted` carrying a JSON-encoded
    // `TaskOutputs` payload inserts an entry under the completing
    // task's `task_id`. `mk_task("a")` constructs a task with
    // `task_id = Some("a")`.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let outputs = outputs_with("nonce", "xyz");
    let bytes = serde_json::to_vec(&outputs).expect("serialise outputs");
    let outcome = s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: Some(bytes),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert_eq!(s.outputs_for("a"), Some(&outputs));
}

#[test]
fn task_completed_with_no_result_data_does_not_populate() {
    // `result_data == None` is the "worker did not publish outputs"
    // signal. The cache must remain empty for that task_id; dependents
    // see the absent-key shape and inherit zero predecessor outputs.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: None,
    });
    assert!(s.outputs_for("a").is_none());
}

#[test]
fn task_completed_malformed_result_data_stores_empty_outputs() {
    // Garbage bytes that don't deserialise as `TaskOutputs` trigger
    // the warn-and-store-empty path: the cache gains an entry under
    // the completing task's `task_id` carrying an empty `TaskOutputs`.
    // Storing the empty entry (vs skipping) prevents dependents that
    // hard-require a key from racing the cache between "populated"
    // and "absent".
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let garbage: Vec<u8> = b"not-json-at-all".to_vec();
    let outcome = s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: Some(garbage),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert_eq!(s.outputs_for("a"), Some(&TaskOutputs::default()));
}

#[test]
fn anonymous_task_outputs_are_silently_dropped() {
    // A task with `task_id = None` cannot be referenced by dependents
    // (deps key by `task_id`). The populate path silently skips the
    // insert — no key under which to cache. Pins the anonymous-task
    // behaviour against any future regression that accidentally keys
    // by hash for anonymous tasks.
    let mut anon = mk_task("ignored");
    anon.task_id = None;
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: anon,
    });
    let outputs = outputs_with("k", "v");
    let bytes = serde_json::to_vec(&outputs).expect("serialise");
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: Some(bytes),
    });
    // The cache is empty — the insert was skipped because the task
    // has no `task_id`. Reading any key returns None.
    assert!(s.outputs_for("ignored").is_none());
    assert!(s.outputs_for("").is_none());
}

#[test]
fn task_outputs_round_trip_via_snapshot() {
    // Snapshot/restore must carry the keyed-output cache so a
    // late-joiner can resolve a dependent's predecessor outputs
    // immediately, without waiting for the prereq's `TaskCompleted`
    // to retransmit.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let outputs = outputs_with("nonce", "xyz");
    let bytes = serde_json::to_vec(&outputs).expect("serialise");
    s.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: Some(bytes),
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    assert_eq!(joiner.outputs_for("a"), Some(&outputs));
}

#[test]
fn restore_first_write_wins_on_task_outputs_collision() {
    // If the local replica already has an entry for a given task_id
    // (live `TaskCompleted` reached it before the snapshot did), the
    // snapshot's entry for the same task_id is dropped — first-write-
    // wins. Each `TaskCompleted` for a given hash records exactly
    // one entry, so the snapshot's and the local's values would in
    // practice agree; the test pins the merge rule against any
    // future drift.
    let local_outputs = outputs_with("k", "local-value");
    let snap_outputs = outputs_with("k", "snapshot-value");

    let mut local = ClusterState::<RunnerIdentifier>::new();
    local.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    local.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: Some(serde_json::to_vec(&local_outputs).unwrap()),
    });

    let mut source = ClusterState::<RunnerIdentifier>::new();
    source.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    source.apply(ClusterMutation::TaskCompleted {
        hash: "h".into(),
        result_data: Some(serde_json::to_vec(&snap_outputs).unwrap()),
    });

    local.restore(source.snapshot());
    // Local's entry survives; snapshot's same-key entry is ignored.
    assert_eq!(local.outputs_for("a"), Some(&local_outputs));
}
