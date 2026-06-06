//! Tests for the replicated keyed-output cache.
//!
//! Pins the `task_outputs` apply-time populate via the `TaskCompleted`
//! mutation's `result_data` payload, the
//! `outputs_for(phase_id, task_id)` reader (which resolves the dep's
//! full identity to its hash, then reads the hash-keyed cache), the
//! malformed-payload warn-and-store-empty path, and the
//! snapshot/restore round-trip.
//!
//! Wire-shape contract: `result_data` is the Python worker's
//! [`DonePayload`] wrapper — a JSON object containing optional
//! `warnings`/`filtered` counters and an optional `outputs` map of
//! the producing task's keyed outputs. The decoder extracts only the
//! `outputs` field; counters are dropped silently. Tests in this
//! module construct payloads via [`encode_wire`] so the bytes are
//! byte-identical to what `python/dynamic_runner/worker/runtime.py`
//! `_encode_done_payload` produces — preventing a regression where
//! tests use a different (broken) shape than the encoder and mask
//! the very bug they should pin.

use super::*;

use dynrunner_core::{ResultValue, TaskOutputs};
use std::collections::BTreeMap;

fn outputs_with(key: &str, value: &str) -> TaskOutputs {
    let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
    m.insert(key.to_string(), ResultValue::Inline(value.to_string()));
    TaskOutputs(m)
}

/// Build the Python encoder's wire bytes for a `result_data` payload
/// carrying `outputs` only (the common case for a task that publishes
/// keyed outputs without using the `WorkerOutput` counters). The
/// shape is byte-identical to `_encode_done_payload`'s output for
/// `WorkerOutput()` (default zero counters) + a non-empty
/// `_outputs_accumulator`: `{"outputs": {key → {"kind": ..., "value": ...}}}`.
fn encode_wire(outputs: &TaskOutputs) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "outputs": outputs,
    }))
    .expect("encode wire")
}

#[test]
fn outputs_for_unknown_task_id_is_none() {
    // No TaskCompleted applied yet — the cache is empty and the
    // reader returns None for any lookup. Pins the absent-key shape.
    let s = ClusterState::<RunnerIdentifier>::new();
    assert!(
        s.outputs_for(&dynrunner_core::PhaseId::from("p0"), "anything")
            .is_none()
    );
}

#[test]
fn task_completed_populates_task_outputs_cache() {
    // Happy path: a `TaskCompleted` carrying a JSON-encoded
    // `TaskOutputs` payload inserts an entry under the completing
    // task's `task_id`. `mk_task("a")` constructs a task with
    // `task_id = "a"`.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let outputs = outputs_with("nonce", "xyz");
    let bytes = encode_wire(&outputs);
    let outcome = s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(bytes),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert_eq!(
        s.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a"),
        Some(&outputs)
    );
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
        attempt: 0,
        hash: "h".into(),
        result_data: None,
    });
    assert!(
        s.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a")
            .is_none()
    );
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
        attempt: 0,
        hash: "h".into(),
        result_data: Some(garbage),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);
    assert_eq!(
        s.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a"),
        Some(&TaskOutputs::default())
    );
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
    let bytes = encode_wire(&outputs);
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(bytes),
    });

    let snap = s.snapshot();
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.restore(snap);
    assert_eq!(
        joiner.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a"),
        Some(&outputs)
    );
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
        attempt: 0,
        hash: "h".into(),
        result_data: Some(encode_wire(&local_outputs)),
    });

    let mut source = ClusterState::<RunnerIdentifier>::new();
    source.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    source.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(encode_wire(&snap_outputs)),
    });

    local.restore(source.snapshot());
    // Local's entry survives; snapshot's same-key entry is ignored.
    assert_eq!(
        local.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a"),
        Some(&local_outputs)
    );
}

#[test]
fn python_encode_full_wrapper_decodes_outputs() {
    // Pins the bug-vector that motivated the wrapper-decode fix:
    // the Python worker's `_encode_done_payload` emits a JSON
    // object with `warnings` + `filtered` + `outputs` top-level
    // keys (counters present, keyed outputs present). The decoder
    // MUST extract `outputs` and drop the counters silently — the
    // pre-fix decoder tried to deserialise the whole body as
    // `TaskOutputs` and failed with "missing field `kind`",
    // landing in the warn-and-store-empty path.
    //
    // This test constructs the wire bytes byte-identical to the
    // Python encoder (see `python/dynamic_runner/worker/runtime.py
    // ::_encode_done_payload`) and asserts the cache populates with
    // the inner outputs map. If the decoder ever regresses to
    // unwrapping the wrong layer, this test fails.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let wire = serde_json::to_vec(&serde_json::json!({
        "warnings": 2,
        "filtered": 1,
        "outputs": {
            "nonce": {"kind": "inline", "value": "xyz"},
            "artifact": {"kind": "file", "value": "/app/out/foo.tar"},
        }
    }))
    .expect("encode python wire shape");
    let outcome = s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(wire),
    });
    assert_eq!(outcome, ApplyOutcome::Applied);

    let cached = s
        .outputs_for(&dynrunner_core::PhaseId::from("p0"), "a")
        .expect("cache populated");
    assert_eq!(
        cached.0.get("nonce"),
        Some(&ResultValue::Inline("xyz".to_string()))
    );
    assert_eq!(
        cached.0.get("artifact"),
        Some(&ResultValue::File("/app/out/foo.tar".to_string()))
    );
    assert_eq!(cached.0.len(), 2);
}

#[test]
fn python_encode_outputs_only_decodes_outputs() {
    // Encoder shape when the task uses `publish_string` / `publish`
    // but `WorkerOutput()` is default-constructed (zero counters):
    // both `warnings` and `filtered` are omitted, only `outputs`
    // rides the wire. Decoder must still extract the map.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let wire = serde_json::to_vec(&serde_json::json!({
        "outputs": {
            "k": {"kind": "inline", "value": "v"},
        }
    }))
    .expect("encode python wire shape");
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(wire),
    });
    let cached = s
        .outputs_for(&dynrunner_core::PhaseId::from("p0"), "a")
        .expect("cache populated");
    assert_eq!(
        cached.0.get("k"),
        Some(&ResultValue::Inline("v".to_string()))
    );
}

/// A task placed in `phase` carrying `task_id = name`. Used by the
/// `phase_task_outputs` gather test so two phases hold distinct tasks.
fn mk_task_in_phase(name: &str, phase: &str) -> TaskInfo<RunnerIdentifier> {
    TaskInfo {
        phase_id: dynrunner_core::PhaseId::from(phase),
        ..mk_task(name)
    }
}

#[test]
fn phase_task_outputs_gathers_only_the_named_phase() {
    // The `on_phase_end` primitive: when a phase drains, the hook is
    // handed `{task_id: TaskOutputs}` for THAT phase's published tasks,
    // read off the same `task_outputs` cache `outputs_for` uses — no
    // filesystem path. Pin: two phases each publish outputs; gathering
    // one phase returns ONLY its tasks' outputs (keyed by task_id), and
    // a task that published nothing contributes no entry.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    // Phase "build" — two tasks; one publishes, one does not.
    s.apply(ClusterMutation::TaskAdded {
        hash: "h_common".into(),
        task: mk_task_in_phase("common_dep", "build"),
    });
    s.apply(ClusterMutation::TaskAdded {
        hash: "h_variant".into(),
        task: mk_task_in_phase("variant", "build"),
    });
    // Phase "dependency_graph" — one task that publishes the pickle-ish
    // payload the consumer reads from on_phase_end.
    s.apply(ClusterMutation::TaskAdded {
        hash: "h_dep".into(),
        task: mk_task_in_phase("dependency_graph", "dependency_graph"),
    });

    let common_outputs = outputs_with("artifact_drv", "/nix/store/abc");
    let dep_outputs = outputs_with("dependency_graph_pkl", "BASE64PICKLE");
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h_common".into(),
        result_data: Some(encode_wire(&common_outputs)),
    });
    // `variant` completes with NO published outputs.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h_variant".into(),
        result_data: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h_dep".into(),
        result_data: Some(encode_wire(&dep_outputs)),
    });

    // Gathering the "dependency_graph" phase returns ONLY its task's
    // outputs, keyed by task_id — the exact handle on_phase_end gets.
    let dep_phase = s.phase_task_outputs(&dynrunner_core::PhaseId::from("dependency_graph"));
    assert_eq!(dep_phase.len(), 1, "only the dep_graph task: {dep_phase:?}");
    assert_eq!(dep_phase.get("dependency_graph"), Some(&dep_outputs));

    // Gathering "build" returns ONLY the task that published; the
    // output-less `variant` contributes no entry (mirrors `outputs_for`'s
    // None for a task that published nothing).
    let build_phase = s.phase_task_outputs(&dynrunner_core::PhaseId::from("build"));
    assert_eq!(
        build_phase.len(),
        1,
        "only common_dep published: {build_phase:?}"
    );
    assert_eq!(build_phase.get("common_dep"), Some(&common_outputs));
    assert!(!build_phase.contains_key("variant"));

    // A phase with no tasks at all gathers to an empty map.
    assert!(
        s.phase_task_outputs(&dynrunner_core::PhaseId::from("nonexistent"))
            .is_empty()
    );
}

#[test]
fn python_encode_counters_only_populates_empty_cache() {
    // Encoder shape when the task returns a `WorkerOutput` with
    // nonzero counters but never calls `publish_string` /
    // `publish(key=...)`: `outputs` is omitted, `warnings` and/or
    // `filtered` are present. Decoder's `#[serde(default)]` on the
    // `outputs` field yields `TaskOutputs::default()`; the cache
    // gains an empty entry under the completing task's `task_id`
    // (matches the snapshot-restore-after-empty-completion
    // semantics — present-key-with-empty-value is a load-bearing
    // signal vs. absent-key).
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.apply(ClusterMutation::TaskAdded {
        hash: "h".into(),
        task: mk_task("a"),
    });
    let wire = serde_json::to_vec(&serde_json::json!({
        "warnings": 7,
    }))
    .expect("encode python wire shape");
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(wire),
    });
    let cached = s
        .outputs_for(&dynrunner_core::PhaseId::from("p0"), "a")
        .expect("cache populated");
    assert!(cached.0.is_empty());
}
