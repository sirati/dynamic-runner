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
    s.attach_output_segment_for_test(&dir.path().join("outputs.cbor"));

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
fn every_role_persists_zero_resident_and_converges_same_outputs_digest() {
    // EVERY role persists to its own disk home (no non-reader mode): two
    // disk-backed nodes — a primary and a plain secondary — apply the SAME
    // TaskCompleted stream and must each keep ZERO resident payloads, hold
    // every record on disk, and converge the SAME outputs digest (the
    // digest is over the LOGICAL outputs, folded at apply on both).
    let pdir = tempfile::tempdir().expect("tempdir");
    let mut primary = ClusterState::<RunnerIdentifier>::new();
    primary.attach_output_segment_for_test(&pdir.path().join("outputs.cbor"));

    let sdir = tempfile::tempdir().expect("tempdir");
    let mut secondary = ClusterState::<RunnerIdentifier>::new();
    secondary.attach_output_segment_for_test(&sdir.path().join("outputs.cbor"));

    for i in 0..8 {
        let outputs = outputs_with("k", &format!("v{i}"));
        let hash = format!("h{i}");
        let task_id = format!("t{i}");
        complete_with(&mut primary, &hash, &task_id, &outputs);
        complete_with(&mut secondary, &hash, &task_id, &outputs);
    }

    // Both roles: zero residence, every record on disk.
    assert_eq!(
        secondary.task_outputs_resident_len(),
        0,
        "a disk-backed secondary must hold zero resident output payloads"
    );
    assert_eq!(
        secondary.output_store().index_len(),
        8,
        "every role persists every output to disk"
    );
    assert_eq!(primary.output_store().index_len(), 8);
    // The outputs digest CONVERGES across the two nodes.
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
    disk.attach_output_segment_for_test(&dir.path().join("outputs.cbor"));

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
    source.attach_output_segment_for_test(&dir.path().join("outputs.cbor"));
    let outputs = outputs_with("nonce", "xyz");
    complete_with(&mut source, "h", "a", &outputs);
    assert_eq!(source.task_outputs_resident_len(), 0);

    // The snapshot carries the output even though it was never resident.
    let snap = source.snapshot();
    assert_eq!(snap.task_outputs.get("h"), Some(&outputs));

    // A disk-backed joiner restores and serves it (zero resident again).
    let jdir = tempfile::tempdir().expect("tempdir");
    let mut joiner = ClusterState::<RunnerIdentifier>::new();
    joiner.attach_output_segment_for_test(&jdir.path().join("outputs.cbor"));
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
    s.attach_output_segment_for_test(&dir.path().join("outputs.cbor"));
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

/// The output-store target the decode-skip log-hygiene test captures.
const OUTPUT_TARGET: &str = "dynrunner_manager_distributed::cluster_state::output_store";

/// Frame one body the way the output-store writer does: a `u32`-LE length
/// prefix in front of the CBOR-or-junk body.
fn frame_record(body: &[u8]) -> Vec<u8> {
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(body);
    out
}

/// A DECODABLE output record's framed bytes (a real `OutputRecord` CBOR
/// behind the length prefix). Produced by driving the PRODUCTION
/// write-through path: complete one task on a disk-backed state, then lift
/// the single framed record the store wrote. Exercises the real on-disk
/// record shape (not a fabricated stub).
fn decodable_framed(hash: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("one.cbor");
    let mut s = ClusterState::<RunnerIdentifier>::new();
    s.attach_output_segment_for_test(&path);
    let outputs = outputs_with("k", &format!("v-{hash}"));
    complete_with(&mut s, hash, hash, &outputs);
    assert_eq!(s.output_store().index_len(), 1, "exactly one record written");
    std::fs::read(&path).expect("read back the single framed record")
}

/// An UNDECODABLE ("old/foreign on-disk format") record's framed bytes:
/// the framing (length prefix) is INTACT — so the read locates + advances
/// past it exactly — but the body is junk no `OutputRecord` decode accepts.
fn undecodable_framed() -> Vec<u8> {
    // 0xFF bytes are not a valid CBOR document head for our record map.
    frame_record(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
}

/// THE log-hygiene invariant (mirrors `settled.rs`'s
/// `undecodable_records_are_skipped_and_warn_is_throttled`): an output
/// file with a MIX of decodable + undecodable(old-format) records → the
/// decodable ones load, the undecodable ones are SKIPPED (not aborting the
/// read, the index untouched), and EXACTLY ONE rolled-up WARN naming the
/// skipped count is emitted per throttle interval — never one ERROR per
/// record, and no re-scan.
///
/// `start_paused` drives the `WarnThrottle` interval deterministically.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn undecodable_records_are_skipped_and_warn_is_throttled() {
    use std::io::Write as _;

    let log = crate::test_capture::TargetCapture::for_target(OUTPUT_TARGET);
    let _guard = {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_default(tracing_subscriber::Registry::default().with(log.clone()))
    };

    // Lay down a mixed file: valid, junk, valid, junk, junk — tracking each
    // record's (offset, len) so the reader is driven by coordinates exactly
    // as the production index entries are.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("mixed.cbor");
    let records: Vec<(bool, Vec<u8>)> = vec![
        (true, decodable_framed("good-a")),
        (false, undecodable_framed()),
        (true, decodable_framed("good-b")),
        (false, undecodable_framed()),
        (false, undecodable_framed()),
    ];
    let mut coords: Vec<(bool, u64, u32)> = Vec::new();
    let mut offset = 0u64;
    {
        let mut f = std::fs::File::create(&path).expect("create mixed output file");
        for (decodable, framed) in &records {
            f.write_all(framed).expect("write framed record");
            coords.push((*decodable, offset, framed.len() as u32));
            offset += framed.len() as u64;
        }
        f.flush().expect("flush");
    }
    let committed = offset;

    let mut s = ClusterState::<RunnerIdentifier>::new();
    let read_fd = std::sync::Arc::new(std::fs::File::open(&path).expect("read fd"));
    s.attach_output_read_segment_for_test(read_fd, committed);

    // Drive a read for every record. The decodable ones decode; the
    // undecodable ones return None (skipped) — the read locates each by its
    // own (offset, len), so a junk record never derails its neighbours.
    let mut decoded = 0usize;
    let mut skipped = 0usize;
    for (decodable, off, len) in &coords {
        let got = s.output_store().read_at_for_test(*off, *len);
        if *decodable {
            assert!(got.is_some(), "a decodable record must load");
            decoded += 1;
        } else {
            assert!(got.is_none(), "an undecodable record must be skipped (None)");
            skipped += 1;
        }
    }
    assert_eq!(decoded, 2, "both decodable records loaded");
    assert_eq!(skipped, 3, "all three old-format records were skipped");

    // Exactly ONE rolled-up WARN this interval — NOT one per skipped record.
    let warns: Vec<_> = log
        .events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .collect();
    assert_eq!(
        warns.len(),
        1,
        "three skips in one window emit exactly one rolled-up WARN, not per-record"
    );
    // The first skip emits immediately (0 prior suppressed); the other two
    // are suppressed + counted, surfacing on the NEXT permitted emit.
    assert_eq!(
        warns[0]
            .event
            .fields
            .get("also_skipped_since_last")
            .map(String::as_str),
        Some("0"),
        "the first skip emits immediately carrying a zero suppressed count"
    );
    // No ERROR-level line for a mere old-format body — the loud ERROR is
    // reserved for STRUCTURAL faults (read-past-committed / IO error), which
    // this coherent file never triggers.
    assert!(
        log.events().iter().all(|e| e.level != tracing::Level::ERROR),
        "an old-format record must NOT emit a per-record ERROR"
    );

    // Past the interval, the next skip re-emits, NAMING the two suppressed
    // in between — the throttle rolls up rather than dropping the count.
    tokio::time::advance(std::time::Duration::from_secs(61)).await;
    let (_decodable, off, len) = coords[1];
    assert!(
        s.output_store().read_at_for_test(off, len).is_none(),
        "the old-format record is still skipped on a later read"
    );
    let warns: Vec<_> = log
        .events()
        .into_iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .collect();
    assert_eq!(warns.len(), 2, "past the interval the throttle re-emits");
    assert_eq!(
        warns[1]
            .event
            .fields
            .get("also_skipped_since_last")
            .map(String::as_str),
        Some("2"),
        "the second WARN names the two skips suppressed inside the interval"
    );
}
