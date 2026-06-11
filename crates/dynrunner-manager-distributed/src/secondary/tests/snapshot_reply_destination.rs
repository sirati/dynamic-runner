#![cfg(test)]

//! Snapshot-RPC reply addressing on the SECONDARY responder
//! (`dispatch_message`'s `RequestClusterSnapshot` arm): the
//! `ClusterSnapshot` reply must be TYPED off the requester's
//! self-declared role (`is_observer` on the request frame) —
//! `Destination::Observer(id)` for an observer requester,
//! `Destination::Secondary(id)` for a worker.
//!
//! The PeerId in the Destination selects the HOST at egress; the role
//! variant is the receiver-side ingress demux selector. Pre-fix the
//! responder hardcoded `Secondary(requester)`, so an observer's
//! anti-entropy pull was answered under the wrong role: the frame
//! reached the right host but missed the slot demux and fell to the
//! fan-to-live-slots WARN (the production "directed frame names a role
//! with no live local slot … kind=ClusterSnapshot
//! target=Secondary(setup)" on the relocated submitter).

use super::super::test_helpers::{
    FakeWorkerFactory, RecordingPeer, SecondaryHarness, TestId, election_config,
    make_secondary_recording,
};
use dynrunner_protocol_primary_secondary::{Destination, DistributedMessage, PeerId};

/// Build a secondary over a `RecordingPeer` so the test can inspect the
/// frames it emits (the snapshot reply lands in the log with its stamped
/// routing `target`). The coordinator's `MeshClient` QUEUES sends, so a
/// test calls [`SecondaryHarness::drain_egress`] before reading the log.
#[allow(clippy::type_complexity)]
fn make_recording_secondary(
    secondary_id: &str,
) -> (
    SecondaryHarness<RecordingPeer<TestId>>,
    std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
) {
    make_secondary_recording(election_config(secondary_id), 1)
}

/// Drive one `RequestClusterSnapshot` (from `requester`, declaring
/// `is_observer`) through the secondary responder and return the stamped
/// routing `target` of the `ClusterSnapshot` reply it emitted.
async fn reply_target_for(requester: &str, is_observer: bool) -> Destination {
    let (mut sec, peer_log) = make_recording_secondary("responder");
    sec.enter_operational_for_test();

    let req = DistributedMessage::RequestClusterSnapshot {
        target: None,
        sender_id: requester.into(),
        timestamp: 0.0,
        is_observer,
        can_be_primary: !is_observer,
    };
    sec.dispatch_message(req, &mut FakeWorkerFactory)
        .await
        .expect("RequestClusterSnapshot handler succeeds");
    // Flush the queued reply onto the RecordingPeer log
    // (MeshClient::send is queued).
    sec.drain_egress().await;

    peer_log
        .borrow()
        .iter()
        .find_map(|m| match m {
            DistributedMessage::ClusterSnapshot { .. } => Some(
                m.target()
                    .expect("the egress edge stamps the routing target")
                    .clone(),
            ),
            _ => None,
        })
        .expect("the responder must emit a ClusterSnapshot reply")
}

/// An OBSERVER requester (`is_observer: true` on the request frame) must
/// be answered under `Destination::Observer(requester)` — the requester's
/// self-declared role is the reply's ingress demux selector.
///
/// REVERT-CHECK: pre-fix the responder hardcoded
/// `Destination::Secondary(requester)` and the reply missed the
/// observer's slot demux (the fan-fallback WARN on the relocated
/// submitter).
#[tokio::test(flavor = "current_thread")]
async fn observer_requester_gets_observer_typed_reply() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // "peer-0" is in the RecordingPeer's seeded membership, so the
            // reply passes the egress `has_route` gate.
            let dst = reply_target_for("peer-0", true).await;
            assert_eq!(
                dst,
                Destination::Observer(PeerId::from("peer-0")),
                "an observer requester's snapshot reply must be Observer-typed"
            );
        })
        .await;
}

/// A WORKER requester (`is_observer: false`) keeps the Secondary-typed
/// reply — the non-observer half of the same policy.
#[tokio::test(flavor = "current_thread")]
async fn worker_requester_gets_secondary_typed_reply() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let dst = reply_target_for("peer-0", false).await;
            assert_eq!(
                dst,
                Destination::Secondary(PeerId::from("peer-0")),
                "a worker requester's snapshot reply must be Secondary-typed"
            );
        })
        .await;
}
