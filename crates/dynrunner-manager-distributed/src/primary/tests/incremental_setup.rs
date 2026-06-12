//! Incremental setup delivery — the all-or-nothing bring-up hostage
//! replay.
//!
//! Production shape (total-loss RCA): a fleet where some members are
//! slow/missing kept every ALREADY-WELCOMED member waiting for the
//! whole setup trio until the connect wait resolved (full fleet or the
//! quorum-proceed timeout). The welcomed members sat parked in
//! `AwaitingPrimary` — workers unspawned, peer mesh unformed, bootstrap
//! wires idle — for up to the whole straggler window.
//!
//! Pinned here:
//! 1. A member is served its peer list the moment its cert-exchange
//!    lands (`serve_setup_on_cert_exchange`), not when the fleet
//!    completes — and each later arrival RE-broadcasts the grown
//!    roster, so earlier members converge onto the newcomers (late
//!    peers are never permanently unknown to early peers).
//! 2. The run-start halves of the secondary's setup gate
//!    (`InitialAssignment` / `TransferComplete`) do NOT flow during the
//!    connect wait — the quorum-proceed policy still governs when the
//!    run starts.
//! 3. The per-member typestate walk: a served member advances
//!    `CertExchanging → PeerDiscovery` at its own serve, and the batch
//!    phases (`send_peer_lists` / `wait_for_peer_connections`) loop
//!    over the same ONE walk without regressing it.
//!
//! REVERT-CHECK: gate the roster broadcast back behind the connect-wait
//! resolution (drop the `serve_setup_on_cert_exchange` call from
//! `handle_cert_exchange`) and the mid-wait assertions below go RED —
//! the welcomed members receive nothing until the third arrives.

use super::*;

use dynrunner_protocol_primary_secondary::{Destination, MessageType};

fn welcome_frame(id: &str) -> DistributedMessage<TestId> {
    DistributedMessage::SecondaryWelcome {
        target: Some(Destination::Primary),
        sender_id: id.into(),
        timestamp: 0.0,
        secondary_id: id.into(),
        resources: vec![dynrunner_core::ResourceAmount {
            kind: dynrunner_core::ResourceKind::memory(),
            amount: 1024 * 1024 * 1024,
        }],
        worker_count: 1,
        hostname: "test-host".into(),
        is_observer: false,
        can_be_primary: true,
    }
}

fn cert_frame(id: &str, port: u16) -> DistributedMessage<TestId> {
    DistributedMessage::CertExchange {
        target: Some(Destination::Primary),
        sender_id: id.into(),
        timestamp: 0.0,
        secondary_id: id.into(),
        public_cert_pem: format!("CERT-{id}"),
        ipv4_address: Some("10.0.0.1".into()),
        ipv6_address: None,
        quic_port: port,
        liveness_port: None,
    }
}

/// Drain everything currently queued on a secondary's inbound channel.
fn drain(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<DistributedMessage<TestId>> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        out.push(msg);
    }
    out
}

/// The id-sets of every `PeerInfo` roster in `frames`, in arrival order.
fn rosters(frames: &[DistributedMessage<TestId>]) -> Vec<Vec<String>> {
    frames
        .iter()
        .filter_map(|m| {
            if let DistributedMessage::PeerInfo { peers, .. } = m {
                let mut ids: Vec<String> =
                    peers.iter().map(|p| p.secondary_id.clone()).collect();
                ids.sort();
                Some(ids)
            } else {
                None
            }
        })
        .collect()
}

/// True iff any frame is one of the run-start halves of the setup gate.
fn contains_run_start_frame(frames: &[DistributedMessage<TestId>]) -> bool {
    frames.iter().any(|m| {
        matches!(
            m.msg_type(),
            MessageType::InitialAssignment | MessageType::TransferComplete
        )
    })
}

/// Let the inbound frames travel wire → pump → inbox → connect-wait
/// handlers → queued egress → pump → outboxes. The connect wait is a
/// sibling future of the caller (joined on the same `LocalSet`), so
/// yielding is what hands it the frames.
async fn settle() {
    settle_pump().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    settle_pump().await;
}

/// Two of three members welcome; they must be served peer lists
/// INCREMENTALLY (each arrival re-broadcasting the grown roster), with
/// NO run-start frames flowing mid-wait; the third's arrival
/// re-broadcasts the roster so all three converge, and the wait then
/// resolves full-fleet.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn welcomed_members_are_served_incrementally_and_converge_on_late_arrival() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(3);
            // Keep id → (inbound rx, outbound tx) handles per fake member.
            let (id2, mut rx2, tx2) = ends.remove(2);
            let (id1, mut rx1, tx1) = ends.remove(1);
            let (id0, mut rx0, tx0) = ends.remove(0);

            let config = PrimaryConfig {
                num_secondaries: 3,
                connect_timeout: Duration::from_secs(600),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut no_commands = None;
            let wait = primary.wait_for_connections(&mut no_commands);

            let driver = async {
                // ── First member arrives ──
                tx0.send(welcome_frame(&id0)).unwrap();
                tx0.send(cert_frame(&id0, 5000)).unwrap();
                settle().await;
                let got0 = drain(&mut rx0);
                let r0 = rosters(&got0);
                assert!(
                    r0.iter().any(|r| r.contains(&id0)),
                    "the first welcomed member must be served its peer list \
                     IMMEDIATELY on its cert-exchange edge — not held hostage \
                     until the whole fleet welcomes (frames seen: {:?})",
                    got0.iter().map(|m| m.msg_type()).collect::<Vec<_>>()
                );
                assert!(
                    got0.iter()
                        .any(|m| matches!(m.msg_type(), MessageType::RunConfig)),
                    "the run-config push must land at the member's welcome"
                );

                // ── Second member arrives: BOTH must now hold the grown
                //    roster (the newcomer its first list, the early member
                //    the re-broadcast). ──
                tx1.send(welcome_frame(&id1)).unwrap();
                tx1.send(cert_frame(&id1, 5001)).unwrap();
                settle().await;
                let got0b = drain(&mut rx0);
                let got1 = drain(&mut rx1);
                let both: Vec<String> = {
                    let mut v = vec![id0.clone(), id1.clone()];
                    v.sort();
                    v
                };
                assert!(
                    rosters(&got0b).contains(&both),
                    "the EARLIER member must receive the re-broadcast roster \
                     carrying the newcomer (mesh convergence), got {:?}",
                    rosters(&got0b)
                );
                assert!(
                    rosters(&got1).contains(&both),
                    "the newcomer's first peer list must carry the fleet so \
                     far, got {:?}",
                    rosters(&got1)
                );

                // ── Run-start governance: nothing of the run-start half
                //    may flow while the connect wait is unresolved. ──
                for (id, frames) in [(&id0, &got0), (&id0, &got0b), (&id1, &got1)] {
                    assert!(
                        !contains_run_start_frame(frames),
                        "no InitialAssignment/TransferComplete may reach {id} \
                         before the connect wait resolves (the quorum-proceed \
                         policy governs the run start)"
                    );
                }

                // ── Third member arrives → full fleet, wait resolves. ──
                tx2.send(welcome_frame(&id2)).unwrap();
                tx2.send(cert_frame(&id2, 5002)).unwrap();
            };

            let (res, ()) = tokio::join!(wait, driver);
            res.expect("full-fleet connect must resolve Ok");

            // The third's arrival re-broadcast the roster: ALL THREE hold
            // a converged 3-member roster.
            settle().await;
            let all: Vec<String> = {
                let mut v = vec![id0.clone(), id1.clone(), id2.clone()];
                v.sort();
                v
            };
            for (id, rx) in [(&id0, &mut rx0), (&id1, &mut rx1), (&id2, &mut rx2)] {
                let frames = drain(rx);
                assert!(
                    rosters(&frames).contains(&all),
                    "{id} must converge onto the full 3-member roster after \
                     the third member's arrival re-broadcast, got {:?}",
                    rosters(&frames)
                );
            }
        })
        .await;
}

/// The third member NEVER arrives (the production straggler shape): the
/// two welcomed members are served mid-wait anyway, and the wait
/// resolves at quorum without ever having gated their setup payloads on
/// the missing member.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn straggler_window_does_not_hold_welcomed_members_hostage() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(3);
            let (_id2, _rx2, _tx2) = ends.remove(2);
            let (id1, mut rx1, tx1) = ends.remove(1);
            let (id0, mut rx0, tx0) = ends.remove(0);

            let config = PrimaryConfig {
                num_secondaries: 3,
                // Short straggler window so the quorum-proceed arm fires
                // promptly under the paused clock.
                connect_timeout: Duration::from_secs(30),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let mut no_commands = None;
            let wait = primary.wait_for_connections(&mut no_commands);

            let mut served_mid_wait = false;
            let driver = async {
                tx0.send(welcome_frame(&id0)).unwrap();
                tx0.send(cert_frame(&id0, 5000)).unwrap();
                tx1.send(welcome_frame(&id1)).unwrap();
                tx1.send(cert_frame(&id1, 5001)).unwrap();
                settle().await;
                // Mid-wait (t ≈ 0s of a 30s window): both welcomed members
                // already hold a roster naming each other.
                let both: Vec<String> = {
                    let mut v = vec![id0.clone(), id1.clone()];
                    v.sort();
                    v
                };
                let got0 = drain(&mut rx0);
                let got1 = drain(&mut rx1);
                served_mid_wait = rosters(&got0).contains(&both)
                    && rosters(&got1).contains(&both);
                assert!(
                    !contains_run_start_frame(&got0) && !contains_run_start_frame(&got1),
                    "run-start frames must not flow inside the straggler window"
                );
            };

            let (res, ()) = tokio::join!(wait, driver);
            res.expect("a 2-of-3 connect must quorum-proceed Ok");
            assert!(
                served_mid_wait,
                "welcomed members must be served their peer lists INSIDE the \
                 straggler window — a missing third member must not gate them"
            );
        })
        .await;
}

/// The per-member typestate walk: a served member advances
/// `CertExchanging → PeerDiscovery` at its own cert-exchange edge; the
/// batch `send_peer_lists` does not regress it; and
/// `wait_for_peer_connections` walks it on to `InitialAssigning`.
#[tokio::test(flavor = "current_thread")]
async fn serve_walks_member_typestate_individually() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let config = PrimaryConfig {
                num_secondaries: 1,
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Direct-handler drive: the handlers match `target: None`
            // (the pump's local delivery clears the routing header), so
            // mirror that here.
            let mut welcome = welcome_frame("sec-0");
            welcome.clear_target();
            primary.handle_welcome(welcome).await;
            assert!(matches!(
                primary.secondaries.get("sec-0"),
                Some(crate::state::SecondaryConnectionState::Handshaking(_))
            ));

            let mut cert = cert_frame("sec-0", 5000);
            cert.clear_target();
            primary.handle_cert_exchange(cert).await;
            assert!(
                matches!(
                    primary.secondaries.get("sec-0"),
                    Some(crate::state::SecondaryConnectionState::PeerDiscovery(_))
                ),
                "the incremental serve must walk the member \
                 CertExchanging → PeerDiscovery at its own cert edge"
            );

            // The batch phase re-runs the same ONE walk — a no-op for an
            // already-served member, never a regression.
            primary
                .send_peer_lists()
                .await
                .expect("send_peer_lists must succeed");
            assert!(matches!(
                primary.secondaries.get("sec-0"),
                Some(crate::state::SecondaryConnectionState::PeerDiscovery(_))
            ));

            primary
                .wait_for_peer_connections()
                .await
                .expect("wait_for_peer_connections must succeed");
            assert!(matches!(
                primary.secondaries.get("sec-0"),
                Some(crate::state::SecondaryConnectionState::InitialAssigning(_))
            ));
        })
        .await;
}
