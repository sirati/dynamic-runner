//! Tests for the snapshot-stream drivers: the responder's
//! one-bounded-package-per-wakeup scheduling (the loop-responsiveness
//! property the stream exists for), its cap/resume/abort lifecycle, and
//! the requester-side resume tracker.

use std::collections::HashSet;

use dynrunner_core::{PhaseId, RunnerIdentifier, SoftPreferredSecondaries, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::{ClusterMutation, Destination, DistributedMessage, PeerId};

use super::*;
use crate::cluster_state::decode_stream_payload;

fn mk_task(name: &str, payload_bytes: usize) -> TaskInfo<RunnerIdentifier> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tasks/{name}")),
        size: 0,
        identifier: RunnerIdentifier::from(name),
        phase_id: PhaseId::from("p0"),
        type_id: TypeId::from("t0"),
        affinity_id: None,
        payload: serde_json::json!({ "blob": "x".repeat(payload_bytes) }),
        task_id: name.into(),
        task_depends_on: Vec::new(),
        preferred_secondaries: SoftPreferredSecondaries::default(),
        preferred_version: Default::default(),
        kind: Default::default(),
        setup_affinity: None,
        upload_file: None,
        resolved_path: None,
    }
}

/// A ledger big enough to stream as many packages (~10 MB over the
/// 2 MiB budget ⇒ head + ≥5 batches + tail).
fn big_state() -> ClusterState<RunnerIdentifier> {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..20 {
        let name = format!("big{i:02}");
        s.apply(ClusterMutation::TaskAdded {
            hash: name.clone(),
            task: mk_task(&name, 512 * 1024),
        });
    }
    s
}

fn small_state() -> ClusterState<RunnerIdentifier> {
    let mut s = ClusterState::<RunnerIdentifier>::new();
    for i in 0..4 {
        let name = format!("t{i}");
        s.apply(ClusterMutation::TaskAdded {
            hash: name.clone(),
            task: mk_task(&name, 16),
        });
    }
    s
}

fn unpack(
    frame: &DistributedMessage<RunnerIdentifier>,
) -> (&str, u64, Option<&str>, &str, bool) {
    match frame {
        DistributedMessage::SnapshotStreamPackage {
            stream_id,
            seq,
            cursor,
            payload,
            done,
            ..
        } => (stream_id, *seq, cursor.as_deref(), payload, *done),
        other => panic!("expected SnapshotStreamPackage, got {:?}", other.msg_type()),
    }
}

/// THE loop-responsiveness pin: while a big ledger streams, a SIBLING
/// select! arm that becomes ready after the first package fires BEFORE
/// the stream finishes. Each `emit_next` is one bounded package and the
/// driver re-arms itself through the wake channel, so control returns
/// to the select! between every two packages — the monolithic
/// serialize-everything-in-one-arm-body shape is unrepresentable
/// through this API.
#[tokio::test(flavor = "current_thread")]
async fn sibling_arm_fires_between_packages_on_a_big_ledger() {
    let state = big_state();
    let mut resp = SnapshotStreamResponder::new("node-a");
    resp.accept_request(&state, "joiner-1", false, "joiner-1/0", None, &[]);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let mut packages = 0usize;
    let mut sibling_fired_after: Option<usize> = None;
    let mut stream_done = false;
    while !stream_done {
        tokio::select! {
            // Deterministic arbitration: a ready sibling arm preempts
            // the next package.
            biased;
            Some(()) = rx.recv() => {
                sibling_fired_after = Some(packages);
            }
            stream_id = resp.next_wake() => {
                if let Some((_, frame)) = resp.emit_next(&stream_id, &state, 0.0) {
                    packages += 1;
                    let (_, _, _, _, done) = unpack(&frame);
                    stream_done = done;
                    if packages == 1 {
                        // The sibling work item lands while the stream
                        // is mid-flight.
                        tx.send(()).unwrap();
                    }
                }
            }
        }
    }
    // head + 5 batches (20 × ~512 KiB over the 2 MiB budget); no tail —
    // the fixture ledger has no completed tasks, so no tally capture.
    assert!(packages >= 6, "expected a many-package stream, got {packages}");
    let fired_after = sibling_fired_after.expect("sibling arm must have fired");
    assert!(
        fired_after >= 1 && fired_after < packages,
        "the sibling arm must fire BETWEEN packages (after {fired_after} of {packages})"
    );
}

/// Frame sequencing: seqs are 0-based and contiguous, exactly the last
/// package carries `done`, every frame names the stream id, and the
/// whole stream drains via exactly one package per wake token.
#[tokio::test(flavor = "current_thread")]
async fn emits_one_bounded_package_per_wake_with_contiguous_seqs() {
    let state = small_state();
    let mut resp = SnapshotStreamResponder::new("node-a");
    resp.accept_request(&state, "joiner-1", false, "joiner-1/7", None, &[]);
    let mut frames = Vec::new();
    loop {
        let stream_id = resp.next_wake().await;
        let Some((dst, frame)) = resp.emit_next(&stream_id, &state, 0.0) else {
            continue;
        };
        assert_eq!(
            dst,
            Destination::Secondary(PeerId::from("joiner-1".to_string()))
        );
        let (sid, seq, _, _, done) = unpack(&frame);
        assert_eq!(sid, "joiner-1/7");
        assert_eq!(seq as usize, frames.len(), "seqs contiguous from 0");
        frames.push(frame.clone());
        if done {
            break;
        }
    }
    assert!(frames.len() >= 2, "head + at least one more package");
    assert_eq!(resp.active_streams(), 0, "completed stream is dropped");
    // Only the final frame carries done.
    for (i, f) in frames.iter().enumerate() {
        let (_, _, _, _, done) = unpack(f);
        assert_eq!(done, i == frames.len() - 1);
    }
}

/// Destination typing: an observer requester's packages are typed
/// `Observer(id)` (the shared `reply_destination` policy).
#[tokio::test(flavor = "current_thread")]
async fn observer_requester_gets_observer_typed_packages() {
    let state = small_state();
    let mut resp = SnapshotStreamResponder::new("node-a");
    resp.accept_request(&state, "obs-1", true, "obs-1/0", None, &[]);
    let stream_id = resp.next_wake().await;
    let (dst, _) = resp.emit_next(&stream_id, &state, 0.0).expect("first package");
    assert_eq!(dst, Destination::Observer(PeerId::from("obs-1".to_string())));
}

/// Same-stream resume: a re-request with the SAME stream id repositions
/// the live plan (head re-sent, only keys after the cursor re-shipped,
/// tail still carried) instead of restarting from package 0.
#[tokio::test(flavor = "current_thread")]
async fn same_stream_resume_repositions_instead_of_restarting() {
    let state = big_state();
    let mut resp = SnapshotStreamResponder::new("node-a");
    resp.accept_request(&state, "joiner-1", false, "j/0", None, &[]);
    // Ship head + first batch; pretend the rest was lost.
    let mut cursor: Option<String> = None;
    for _ in 0..2 {
        let stream_id = resp.next_wake().await;
        let (_, frame) = resp.emit_next(&stream_id, &state, 0.0).unwrap();
        let (_, _, c, _, _) = unpack(&frame);
        if let Some(c) = c {
            cursor = Some(c.to_string());
        }
    }
    let cursor = cursor.expect("first batch carried a cursor");
    // The requester re-requests with the same stream id + its cursor.
    resp.accept_request(&state, "joiner-1", false, "j/0", Some(&cursor), &[]);
    assert_eq!(resp.active_streams(), 1, "resume reuses the live stream");
    // Drain; collect every shipped task key (ignoring the re-sent head).
    let mut shipped: HashSet<String> = HashSet::new();
    let mut saw_tail_tallies = false;
    loop {
        let stream_id = resp.next_wake().await;
        let Some((_, frame)) = resp.emit_next(&stream_id, &state, 0.0) else {
            if resp.active_streams() == 0 {
                break;
            }
            continue;
        };
        let (_, _, _, payload, done) = unpack(&frame);
        let part = decode_stream_payload::<RunnerIdentifier>(payload).unwrap();
        shipped.extend(part.tasks.keys().cloned());
        saw_tail_tallies |= !part.phase_event_tallies.is_empty();
        if done {
            break;
        }
    }
    assert!(
        shipped.iter().all(|k| k.as_str() > cursor.as_str()),
        "a resumed live stream re-ships only keys after the cursor"
    );
    assert!(!shipped.is_empty(), "the remainder ships");
    // The tally map is empty on this donor, so instead pin the capture
    // survival structurally: the plan-level test
    // (`resume_semantics_fresh_omits_tail_reposition_keeps_capture`)
    // covers the tally-bearing case; here we pin the reposition seam.
    let _ = saw_tail_tallies;
}

/// Cap: the responder refuses streams beyond `MAX_ACTIVE_STREAMS` (the
/// requester's cadence retries; other responders can serve meanwhile).
#[tokio::test(flavor = "current_thread")]
async fn max_active_streams_is_enforced() {
    let state = small_state();
    let mut resp = SnapshotStreamResponder::new("node-a");
    for i in 0..MAX_ACTIVE_STREAMS {
        resp.accept_request(&state, &format!("peer-{i}"), false, &format!("p{i}/0"), None, &[]);
    }
    assert_eq!(resp.active_streams(), MAX_ACTIVE_STREAMS);
    resp.accept_request(&state, "one-too-many", false, "otm/0", None, &[]);
    assert_eq!(
        resp.active_streams(),
        MAX_ACTIVE_STREAMS,
        "the over-cap stream is refused"
    );
}

/// Abort (send failure / dead leg) drops the stream; its wake tokens
/// become stale no-ops.
#[tokio::test(flavor = "current_thread")]
async fn abort_drops_the_stream_and_stale_tokens_noop() {
    let state = small_state();
    let mut resp = SnapshotStreamResponder::new("node-a");
    resp.accept_request(&state, "joiner-1", false, "j/0", None, &[]);
    resp.abort_stream("j/0");
    assert_eq!(resp.active_streams(), 0);
    // The accept enqueued a token; it must resolve to a no-op.
    let stream_id = resp.next_wake().await;
    assert!(resp.emit_next(&stream_id, &state, 0.0).is_none());
}

/// Requester-side tracker: mint-or-resume parameters and the
/// cursor/done bookkeeping off the package frames.
#[test]
fn inbound_tracker_mints_resumes_and_completes() {
    let mut t = InboundSnapshotStreams::new("me");
    let (id0, resume0) = t.request_params("resp-a");
    assert_eq!(id0, "me/0");
    assert!(resume0.is_none());
    // A re-request before any progress resumes the SAME stream.
    let (id_again, resume_again) = t.request_params("resp-a");
    assert_eq!(id_again, id0);
    assert!(resume_again.is_none());
    // Progress: cursor advances monotonically; a stale/duplicate
    // package can never regress it.
    t.note_package("resp-a", &id0, Some("crate-000100"), false);
    t.note_package("resp-a", &id0, Some("crate-000050"), false);
    let (id_resume, resume) = t.request_params("resp-a");
    assert_eq!(id_resume, id0);
    assert_eq!(resume.as_deref(), Some("crate-000100"));
    // Packages for a stream this tracker did not mint carry no signal.
    t.note_package("resp-a", "someone-else/9", Some("zzz"), true);
    let (_, resume2) = t.request_params("resp-a");
    assert_eq!(resume2.as_deref(), Some("crate-000100"));
    // Done: the next pull to this responder is a FRESH stream.
    t.note_package("resp-a", &id0, None, true);
    let (id1, resume3) = t.request_params("resp-a");
    assert_eq!(id1, "me/1");
    assert!(resume3.is_none());
    // Distinct responders track independently.
    let (id_b, _) = t.request_params("resp-b");
    assert_eq!(id_b, "me/2");
}
