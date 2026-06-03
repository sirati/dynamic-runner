#![cfg(test)]

use super::super::test_helpers::{election_config, make_secondary};
use crate::observer::announcer::{AnnouncerSender, PeerResourceHoldingsUpdatedPayload};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};

/// Pins the production wiring contract: a `SecondaryCoordinator`
/// returns both an `AnnouncerHandle` AND a production
/// `PeerMeshAnnouncerSender` from `attach_observer_announcer`,
/// and the announcer's `send_holdings` ends up posting a
/// `DistributedMessage::ClusterMutation { mutations: vec![
/// PeerResourceHoldingsUpdated { … } ] }` onto the coordinator-
/// side outbox (which the operational `select!` will drain onto
/// `peer_transport.send(Role::Primary, …)`).
///
/// Without this integration, a refactor that breaks the bundle
/// shape (e.g. drops the outbox allocation or returns the wrong
/// sender variant) would compile but silently leave the
/// announcer talking to a dead channel — observable only as a
/// missing wire frame on the production peer mesh.
#[tokio::test(flavor = "current_thread")]
async fn observer_run_attaches_production_announcer_sender() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut sec = make_secondary(election_config("observer-int"));

            let holdings: std::collections::HashSet<String> = [
                "/nix/store/alpha".to_string(),
                "/nix/store/beta".to_string(),
            ]
            .into_iter()
            .collect();
            let (handle, mut sender) = sec.attach_observer_announcer(holdings);

            // The bundle exposes the four `run_observer_announcer`
            // inputs verbatim.
            assert_eq!(handle.peer_id, "observer-int");
            assert_eq!(
                handle.holdings,
                [
                    "/nix/store/alpha".to_string(),
                    "/nix/store/beta".to_string()
                ]
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
            );

            // The coordinator now owns the matching outbox receiver
            // (the operational `select!` would take it on entry).
            // Without this, the production drain arm would never
            // see the announcer's posts.
            assert!(
                sec.announcer_outbox_rx.is_some(),
                "attach_observer_announcer must install the outbox \
                     receiver on the coordinator so process_tasks' drain \
                     arm can dequeue announce posts"
            );
            assert!(
                sec.announcer_outbox_tx.is_some(),
                "the coordinator-side outbox sender clone must be \
                     installed so the channel stays alive across announcer-\
                     task shutdown"
            );

            // Take the outbox receiver out so this test can act as
            // the drain side; the production select! does the same
            // shape.
            let mut outbox_rx = sec.announcer_outbox_rx.take().expect("just asserted Some");

            // Drive one `send_holdings` and assert the wire shape
            // arrives on the outbox. Concurrent join because
            // `send_holdings` awaits the reply oneshot.
            let body = PeerResourceHoldingsUpdatedPayload {
                peer_id: "observer-int".into(),
                holdings: vec!["/nix/store/alpha".into(), "/nix/store/beta".into()],
                epoch: 11,
            };
            let send_fut = sender.send_holdings(&body);
            let drain_fut = async {
                let item = outbox_rx.recv().await.expect("outbox carries one item");
                item.reply.send(Ok(())).expect("send-side awaiting");
                item.msg
            };
            let (send_result, captured_msg) = tokio::join!(send_fut, drain_fut);
            send_result.expect("send_holdings resolves Ok when drain replies Ok");

            match captured_msg {
                DistributedMessage::ClusterMutation {
                    sender_id,
                    mutations,
                    ..
                } => {
                    assert_eq!(
                        sender_id, "observer-int",
                        "sender_id must equal the observer's secondary_id"
                    );
                    assert_eq!(mutations.len(), 1, "one mutation per announce");
                    match &mutations[0] {
                        ClusterMutation::PeerResourceHoldingsUpdated {
                            peer_id,
                            holdings,
                            epoch,
                        } => {
                            assert_eq!(peer_id, "observer-int");
                            assert_eq!(
                                holdings,
                                &vec![
                                    "/nix/store/alpha".to_string(),
                                    "/nix/store/beta".to_string()
                                ]
                            );
                            assert_eq!(*epoch, 11);
                        }
                        other => panic!("expected PeerResourceHoldingsUpdated; got {other:?}"),
                    }
                }
                other => panic!("expected DistributedMessage::ClusterMutation; got {other:?}"),
            }
        })
        .await;
}
