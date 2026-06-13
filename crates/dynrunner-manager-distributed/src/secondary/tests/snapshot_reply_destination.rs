#![cfg(test)]

//! Snapshot-RPC reply addressing on the SECONDARY responder
//! (`dispatch_message`'s `RequestSnapshotStream` arm): every
//! `SnapshotStreamPackage` answer must be TYPED off the requester's
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
//! with no live local slot … kind=SnapshotStreamPackage
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

/// Drive one `RequestSnapshotStream` (from `requester`, declaring
/// `is_observer`) through the secondary responder and return the stamped
/// routing `target` of the first `SnapshotStreamPackage` it emitted.
async fn reply_target_for(requester: &str, is_observer: bool) -> Destination {
    let (mut sec, peer_log) = make_recording_secondary("responder");
    sec.enter_operational_for_test();

    let req = DistributedMessage::RequestSnapshotStream {
        target: None,
        sender_id: requester.into(),
        timestamp: 0.0,
        stream_id: format!("{requester}/0"),
        resume_after: None,
        task_ranges: Vec::new(),
        is_observer,
        can_be_primary: !is_observer,
    };
    sec.dispatch_message(req, &mut FakeWorkerFactory)
        .await
        .expect("RequestSnapshotStream handler succeeds");
    // Produce the registered stream's packages (the process loop's
    // stream arm in production), then flush the queued frames onto the
    // RecordingPeer log (MeshClient::send is queued).
    sec.drive_snapshot_streams_for_test().await;
    sec.drain_egress().await;

    peer_log
        .borrow()
        .iter()
        .find_map(|m| match m {
            DistributedMessage::SnapshotStreamPackage { .. } => Some(
                m.target()
                    .expect("the egress edge stamps the routing target")
                    .clone(),
            ),
            _ => None,
        })
        .expect("the responder must emit a SnapshotStreamPackage answer")
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

/// The secondary responder answers a snapshot pull REGARDLESS of its
/// routing stamp. Three production stamp shapes:
///
///   - `Some(Destination::Secondary(<responder>))` — the egress-stamped
///     anti-entropy pull (`reconcile_against_peer` types every pull
///     `Secondary(sender)` and the puller's egress stamps it);
///   - `Some(Destination::Primary)` — a primary-addressed pull the
///     mesh-ingress fan-fallback hands to the secondary slot when the
///     named role has no live local slot (stale sender-side role
///     knowledge);
///   - `Some(Destination::Secondary(<THE REQUESTER'S OWN ID>))` — the
///     observed anonymous-joiner-era frame shape (production:
///     `kind=RequestSnapshotStream target=Secondary(PeerId(
///     "observer-29b1a066-5f52"))`, the joiner's own id): an AE pull
///     ADDRESSED TO a peer that hosts no secondary slot is fanned by
///     the receiver's role-miss fallback, and any process it relays
///     through sees the requester's own id in the stamp.
///
/// The stamp is the WIRE ENVELOPE's routing header, not request
/// semantics — the responder serves its replica for every shape, and it
/// NEVER re-emits (boomerangs) the request itself: a coordinator slot
/// is a routing TERMINUS, so the only outbound this frame may produce
/// is the `ClusterSnapshot` answer (+ the `PeerJoined` origination).
/// Pins the masking-refutation half of the starved-primary RCA: the
/// SECONDARY dispatch arm was never the stamped-drop site (that was the
/// primary's `target: None` pattern).
#[tokio::test(flavor = "current_thread")]
async fn target_stamped_request_is_answered_not_dropped() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for stamp in [
                Destination::Secondary(PeerId::from("responder")),
                Destination::Primary,
                // The production replay: the stamp names the REQUESTER's
                // own id (the requester here is "peer-0").
                Destination::Secondary(PeerId::from("peer-0")),
            ] {
                let (mut sec, peer_log) = make_recording_secondary("responder");
                sec.enter_operational_for_test();

                let req = DistributedMessage::RequestSnapshotStream {
                    target: None,
                    sender_id: "peer-0".into(),
                    timestamp: 0.0,
                    stream_id: "peer-0/0".into(),
                    resume_after: None,
                    task_ranges: Vec::new(),
                    is_observer: false,
                    can_be_primary: true,
                }
                .with_target(stamp.clone());
                sec.dispatch_message(req, &mut FakeWorkerFactory)
                    .await
                    .expect("stamped RequestSnapshotStream handler succeeds");
                sec.drive_snapshot_streams_for_test().await;
                sec.drain_egress().await;

                let answered = peer_log
                    .borrow()
                    .iter()
                    .any(|m| matches!(m, DistributedMessage::SnapshotStreamPackage { .. }));
                assert!(
                    answered,
                    "the secondary must answer a snapshot pull stamped {stamp:?}; \
                     a silent stamp-filtered drop starves the puller"
                );
                // No boomerang: the responder must never re-emit the
                // REQUEST itself toward the stamp's named peer (the
                // joiner watching its own request return at its mesh
                // ingress is always a bug).
                let boomeranged = peer_log
                    .borrow()
                    .iter()
                    .any(|m| matches!(m, DistributedMessage::RequestSnapshotStream { .. }));
                assert!(
                    !boomeranged,
                    "the responder must never forward/re-emit the request (stamp {stamp:?})"
                );
            }
        })
        .await;
}

/// A ROSTERLESS late-joiner (in no replicated roster — it is bootstrapping
/// precisely to learn the roster) with a live direct transport leg is
/// answered: the reply's no-route gate reads the TRANSPORT membership
/// (`has_route` over the published view), never the CRDT roster, so the
/// joiner's accept-side leg is a sufficient return wire. Replays the
/// production joiner shape: requests arrive un-stamped (`target: None` —
/// the raw `join_running_cluster` send bypasses any coordinator egress)
/// from an id the responder has never seen in any `PeerJoined`.
#[tokio::test(flavor = "current_thread")]
async fn rosterless_joiner_with_direct_leg_is_answered() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut sec, peer_log, membership) = super::super::test_helpers::
                make_secondary_recording_with_membership(election_config("responder"), 1);
            sec.enter_operational_for_test();

            // The joiner's direct leg registers in the TRANSPORT membership
            // (the accept-side connection) — but no CRDT roster entry
            // exists for it anywhere.
            membership
                .borrow_mut()
                .push(PeerId::from("late-joiner-7"));
            sec.publish_membership();

            let req = DistributedMessage::RequestSnapshotStream {
                target: None,
                sender_id: "late-joiner-7".into(),
                timestamp: 0.0,
                stream_id: "late-joiner-7/0".into(),
                resume_after: None,
                task_ranges: Vec::new(),
                is_observer: true,
                can_be_primary: false,
            };
            sec.dispatch_message(req, &mut FakeWorkerFactory)
                .await
                .expect("rosterless joiner request handler succeeds");
            sec.drive_snapshot_streams_for_test().await;
            sec.drain_egress().await;

            let reply_target = peer_log.borrow().iter().find_map(|m| match m {
                DistributedMessage::SnapshotStreamPackage { .. } => {
                    Some(m.target().cloned())
                }
                _ => None,
            });
            assert_eq!(
                reply_target,
                Some(Some(Destination::Observer(PeerId::from("late-joiner-7")))),
                "the reply must reach the rosterless joiner over its direct \
                 leg, typed off its self-declared role"
            );
        })
        .await;
}
