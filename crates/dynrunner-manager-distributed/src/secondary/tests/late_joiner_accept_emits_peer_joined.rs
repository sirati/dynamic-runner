#![cfg(test)]

use super::super::test_helpers::{
    FakeWorkerFactory, RecordingPeer, SecondaryHarness, TestId, election_config,
    make_secondary_recording,
};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use tokio::sync::mpsc as tokio_mpsc;

/// Build a secondary over a `RecordingPeer` mesh stub so the test can
/// inspect what got broadcast over the peer mesh on the late-joiner accept
/// path. The coordinator's `MeshClient` QUEUES sends, so a test calls
/// `sec.drain_egress().await` after the `dispatch_message` and before
/// reading `peer_log`.
#[allow(clippy::type_complexity)]
fn make_secondary_with_recording_peer(
    secondary_id: &str,
) -> (
    SecondaryHarness<RecordingPeer<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    // `sec_to_pri_rx` is returned by this helper's signature; its sender
    // was the (now removed) channel uplink. These tests drive
    // `dispatch_message` directly and assert on the `RecordingPeer` mesh
    // log (the `PeerJoined` broadcast / snapshot-reply land there), not on
    // `sec_to_pri_rx`, so dropping the uplink does not change what they
    // exercise.
    let (_sec_to_pri_tx, sec_to_pri_rx) =
        tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let (sec, peer_log) = make_secondary_recording(election_config(secondary_id), 1);
    (sec, sec_to_pri_rx, peer_log)
}

/// Find the first `ClusterMutation::PeerJoined` mutation in the
/// recorded peer-bus traffic. The `RecordingPeer` collates both
/// `broadcast` and `send_to_peer` envelopes into one log; the
/// snapshot reply uses `send_to_peer` and the originator's
/// `apply_and_broadcast_mutations` uses `broadcast` — both land in
/// the same vector, so the filter walks any-arm-any-frame.
fn find_peer_joined(
    log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) -> Option<(String, bool, bool)> {
    log.borrow().iter().find_map(|msg| match msg {
        DistributedMessage::ClusterMutation { mutations, .. } => {
            mutations.iter().find_map(|m| match m {
                ClusterMutation::PeerJoined {
                    peer_id,
                    is_observer,
                    can_be_primary,
                    ..
                } => Some((peer_id.clone(), *is_observer, *can_be_primary)),
                _ => None,
            })
        }
        _ => None,
    })
}

/// An OBSERVER late-joiner (`is_observer: true` on the join request)
/// must be projected into `role_table.observers` and the broadcast
/// `PeerJoined` must carry `is_observer = true`.
#[tokio::test(flavor = "current_thread")]
async fn observer_late_joiner_accept_emits_peer_joined_observer_true() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _pri_rx, peer_log) = make_secondary_with_recording_peer("responder");
            sec.enter_operational_for_test();

            assert!(
                sec.cluster_state.role_table().observers.is_empty(),
                "observer set must be empty pre-accept; got {:?}",
                sec.cluster_state.role_table().observers
            );

            // The joiner DECLARES its role on the request frame.
            let req = DistributedMessage::RequestSnapshotStream {
                target: None,
                sender_id: "late-observer-1".into(),
                timestamp: 0.0,
                stream_id: "late-observer-1/0".into(),
                resume_after: None,
                is_observer: true,
                // An observer is never primary-capable.
                can_be_primary: false,
            };
            sec.dispatch_message(req, &mut FakeWorkerFactory)
                .await
                .expect("RequestSnapshotStream handler succeeds");
            // Flush the queued PeerJoined broadcast / snapshot reply onto the
            // RecordingPeer log (MeshClient::send is queued).
            sec.drain_egress().await;

            // Originator-side apply landed locally: the observer joiner
            // shows up in the observer projection.
            assert!(
                sec.cluster_state
                    .role_table()
                    .observers
                    .contains("late-observer-1"),
                "observer late-joiner must be projected into \
                 role_table.observers; observers={:?}",
                sec.cluster_state.role_table().observers
            );

            // The mutation broadcast over the peer mesh carries the
            // joiner's TRUE role.
            let observed = find_peer_joined(&peer_log).expect(
                "RequestSnapshotStream accept must originate one \
                 PeerJoined mutation on the peer bus",
            );
            assert_eq!(
                observed,
                ("late-observer-1".to_string(), true, false),
                "observer late-joiner PeerJoined must carry is_observer=true + \
                 can_be_primary=false; got {:?}",
                observed
            );
        })
        .await;
}

/// A WORKER late-joiner (`is_observer: false`) must NOT be projected
/// into `role_table.observers` and the broadcast `PeerJoined` must
/// carry `is_observer = false`. This is the regression guard against
/// the old hardcoded `is_observer: true` that mis-ratcheted a
/// re-bootstrapping worker up to observer.
#[tokio::test(flavor = "current_thread")]
async fn worker_late_joiner_accept_emits_peer_joined_observer_false() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, _pri_rx, peer_log) = make_secondary_with_recording_peer("responder");
            sec.enter_operational_for_test();

            let req = DistributedMessage::RequestSnapshotStream {
                target: None,
                sender_id: "late-worker-1".into(),
                timestamp: 0.0,
                stream_id: "late-worker-1/0".into(),
                resume_after: None,
                is_observer: false,
                // A re-bootstrapping compute worker declares it can host the
                // primary on demand; the relay must carry that truth.
                can_be_primary: true,
            };
            sec.dispatch_message(req, &mut FakeWorkerFactory)
                .await
                .expect("RequestSnapshotStream handler succeeds");
            // Flush the queued PeerJoined broadcast / snapshot reply onto the
            // RecordingPeer log (MeshClient::send is queued).
            sec.drain_egress().await;

            // A worker joiner must NOT enter the observer projection.
            assert!(
                !sec.cluster_state
                    .role_table()
                    .observers
                    .contains("late-worker-1"),
                "worker late-joiner must NOT be mis-projected as observer; \
                 observers={:?}",
                sec.cluster_state.role_table().observers
            );

            // The broadcast PeerJoined carries is_observer=false.
            let observed = find_peer_joined(&peer_log).expect(
                "RequestSnapshotStream accept must originate one \
                 PeerJoined mutation on the peer bus",
            );
            assert_eq!(
                observed,
                ("late-worker-1".to_string(), false, true),
                "worker late-joiner PeerJoined must carry is_observer=false + \
                 can_be_primary=true; got {:?}",
                observed
            );
        })
        .await;
}
