//! Tests for the always-on node-local OUTPUT store (zero-residence).
//!
//! Pins the owner's hard requirement: a completed task's output payload is
//! NEVER kept in the resident `task_outputs` map once a disk home is
//! attached — it is write-through-then-dropped to the always-on output
//! store at the `TaskCompleted` apply, read back storage-agnostically
//! through `outputs_for_hash`, and folded into the digest IDENTICALLY on a
//! reader (disk-backed) and a non-reader (stores nothing) so the two
//! converge the same outputs digest.

use super::*;

use dynrunner_core::{ResultValue, TaskOutputs};
use std::collections::BTreeMap;

fn outputs_with(key: &str, value: &str) -> TaskOutputs {
    let mut m: BTreeMap<String, ResultValue> = BTreeMap::new();
    m.insert(key.to_string(), ResultValue::Inline(value.to_string()));
    TaskOutputs(m)
}

fn encode_wire(outputs: &TaskOutputs) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "outputs": outputs })).expect("encode wire")
}

/// Apply a `TaskAdded` + `TaskCompleted` carrying `outputs` for `(hash,
/// task_id)`.
fn complete_with(
    s: &mut ClusterState<RunnerIdentifier>,
    hash: &str,
    task_id: &str,
    outputs: &TaskOutputs,
) {
    s.apply(ClusterMutation::TaskAdded {
        hash: hash.into(),
        task: mk_task(task_id),
        def_id: None,
    });
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: hash.into(),
        result_data: Some(encode_wire(outputs)),
    });
}

#[test]
fn reader_with_disk_home_keeps_zero_resident_and_serves_off_disk() {
    // A READER node with an attached disk home: after N TaskCompleted
    // with inline outputs, the resident output map is EMPTY (zero
    // residence) AND every output is still readable via the accessor
    // (served off the always-on disk store).
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.attach_output_segment_for_test(&dir.path().join("outputs.cbor"), true);

    let mut expected = Vec::new();
    for i in 0..16 {
        let outputs = outputs_with("nonce", &format!("v{i}"));
        let hash = format!("h{i}");
        let task_id = format!("t{i}");
        complete_with(&mut s, &hash, &task_id, &outputs);
        expected.push((task_id, outputs));
    }

    // ZERO RESIDENCE: nothing accumulated in the resident map.
    assert_eq!(
        s.task_outputs_resident_len(),
        0,
        "reader with disk home must keep ZERO resident output payloads"
    );
    // Every output still readable through the storage-agnostic accessor.
    for (task_id, outputs) in &expected {
        assert_eq!(
            s.outputs_for(&dynrunner_core::PhaseId::from("p0"), task_id),
            Some(outputs.clone()),
            "output for {task_id} must be readable off the disk store"
        );
    }
    // The disk store actually holds the records.
    assert_eq!(s.output_store().index_len(), 16);
    assert!(s.output_store().committed_bytes() > 0);
}

#[test]
fn secondary_stores_nothing_yet_converges_same_outputs_digest() {
    // A PLAIN SECONDARY (non-reader: stores nothing, no disk home needed
    // for fold) and a PRIMARY (disk-backed reader) apply the SAME
    // TaskCompleted stream and must compute the SAME outputs digest — the
    // digest is over the LOGICAL outputs (folded at apply on both),
    // independent of whether the payload is persisted.

    // Primary: disk-backed reader (zero resident, persisted to disk).
    let pdir = tempfile::tempdir().expect("tempdir");
    let mut primary = ClusterState::<RunnerIdentifier>::new();
    primary.attach_output_segment_for_test(&pdir.path().join("outputs.cbor"), true);

    // Secondary: non-reader, stores NOTHING (no disk write, no resident).
    // A non-reader needs a segment only to declare retains_payload=false;
    // attach one so the path is exercised, then assert nothing is stored.
    let sdir = tempfile::tempdir().expect("tempdir");
    let mut secondary = ClusterState::<RunnerIdentifier>::new();
    secondary.attach_output_segment_for_test(&sdir.path().join("outputs.cbor"), false);

    for i in 0..8 {
        let outputs = outputs_with("k", &format!("v{i}"));
        let hash = format!("h{i}");
        let task_id = format!("t{i}");
        complete_with(&mut primary, &hash, &task_id, &outputs);
        complete_with(&mut secondary, &hash, &task_id, &outputs);
    }

    // Secondary stored NOTHING: no resident payloads, no disk records.
    assert_eq!(
        secondary.task_outputs_resident_len(),
        0,
        "secondary must hold zero resident output payloads"
    );
    assert_eq!(
        secondary.output_store().index_len(),
        0,
        "secondary (non-reader) must persist nothing to disk"
    );
    // Yet the outputs digest CONVERGES with the disk-backed primary.
    assert_eq!(
        secondary.digest().task_outputs_hash,
        primary.digest().task_outputs_hash,
        "secondary and primary must converge the SAME outputs digest"
    );
    assert_eq!(
        secondary.digest().task_outputs_count,
        primary.digest().task_outputs_count,
        "secondary and primary must agree on the outputs count"
    );
}

#[test]
fn no_disk_home_falls_back_to_resident_and_reads() {
    // A bare ClusterState with NO disk home attached (the existing unit-
    // fixture shape, and the degraded-writer fallback): a reader retains
    // the payload RESIDENT so reads still work — correctness is universal,
    // zero-residence is the optimization that kicks in once a home exists.
    let mut s = ClusterState::<RunnerIdentifier>::new();
    let outputs = outputs_with("nonce", "xyz");
    complete_with(&mut s, "h", "a", &outputs);

    assert_eq!(
        s.task_outputs_resident_len(),
        1,
        "no disk home → resident fallback retains the payload"
    );
    assert_eq!(
        s.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a"),
        Some(outputs)
    );
}

#[test]
fn no_disk_home_digest_matches_disk_backed() {
    // The resident-fallback node and a disk-backed reader applying the
    // same stream must ALSO converge the outputs digest (the fallback
    // counts the term in the resident fold; the store counts it in the
    // accumulator — XOR associativity makes both equal the logical fold).
    let mut fallback = ClusterState::<RunnerIdentifier>::new();

    let dir = tempfile::tempdir().expect("tempdir");
    let mut disk = ClusterState::<RunnerIdentifier>::new();
    disk.attach_output_segment_for_test(&dir.path().join("outputs.cbor"), true);

    for i in 0..5 {
        let outputs = outputs_with("k", &format!("v{i}"));
        let hash = format!("h{i}");
        let task_id = format!("t{i}");
        complete_with(&mut fallback, &hash, &task_id, &outputs);
        complete_with(&mut disk, &hash, &task_id, &outputs);
    }

    assert_eq!(
        fallback.digest().task_outputs_hash,
        disk.digest().task_outputs_hash
    );
    assert_eq!(
        fallback.digest().task_outputs_count,
        disk.digest().task_outputs_count
    );
}

#[test]
fn snapshot_serves_outputs_off_disk_and_round_trips() {
    // A disk-backed reader's snapshot must carry the zero-resident
    // outputs (gathered storage-agnostically off the disk store), so a
    // joiner restores them.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut source = ClusterState::<RunnerIdentifier>::new();
    source.attach_output_segment_for_test(&dir.path().join("outputs.cbor"), true);
    let outputs = outputs_with("nonce", "xyz");
    complete_with(&mut source, "h", "a", &outputs);
    assert_eq!(source.task_outputs_resident_len(), 0);

    // The snapshot carries the output even though it was never resident.
    let snap = source.snapshot();
    assert_eq!(snap.task_outputs.get("h"), Some(&outputs));

    // A disk-backed joiner restores and serves it (zero resident again).
    let jdir = tempfile::tempdir().expect("tempdir");
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.attach_output_segment_for_test(&jdir.path().join("outputs.cbor"), true);
    joiner.restore(snap);
    assert_eq!(
        joiner.outputs_for(&dynrunner_core::PhaseId::from("p0"), "a"),
        Some(outputs)
    );
    assert_eq!(
        joiner.task_outputs_resident_len(),
        0,
        "joiner write-through-drops the restored output to its own disk store"
    );
}

#[test]
fn idempotent_recompletion_does_not_double_fold() {
    // A duplicate TaskCompleted (an at-least-once redelivery) must NOT
    // double-fold the outputs digest term — first-fold-wins. The duplicate
    // NoOps in merge_task_state before reaching the output store anyway,
    // but the store's own first-fold guard is the belt-and-suspenders.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.attach_output_segment_for_test(&dir.path().join("outputs.cbor"), true);
    let outputs = outputs_with("k", "v");
    complete_with(&mut s, "h", "a", &outputs);
    let d1 = s.digest();

    // Redeliver the same TaskCompleted.
    s.apply(ClusterMutation::TaskCompleted {
        attempt: 0,
        hash: "h".into(),
        result_data: Some(encode_wire(&outputs)),
    });
    let d2 = s.digest();
    assert_eq!(d1.task_outputs_hash, d2.task_outputs_hash);
    assert_eq!(d1.task_outputs_count, d2.task_outputs_count);
    assert_eq!(s.output_store().index_len(), 1);
}
