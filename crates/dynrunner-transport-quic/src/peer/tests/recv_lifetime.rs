//! `recv_peer_tick_survives_outer_drop` — the reconnect-tick arm
//! must continue to fire after the outer recv future is dropped.
//! Pre-fix `tick_rx.take()` returned `None` from the inner select
//! and the tracker stayed empty.

use std::time::Duration;

use super::super::PeerNetwork;
use super::TestId;
use dynrunner_protocol_primary_secondary::{PeerConnectionInfo, PeerTransport};

/// Regression: the reconnect-tick channel must survive an outer
/// caller dropping the `recv_peer` future mid-poll. Pre-fix,
/// `recv_peer` opened with `let mut tick_rx =
/// self.reconnect_tick_rx.take();` and only restored
/// `self.reconnect_tick_rx` inside each arm body. If the outer
/// caller's `tokio::select!` dropped the `recv_peer` future while
/// the inner select was pending, the stack-local `tick_rx` (still
/// holding the receiver) dropped together with the outer future and
/// `self.reconnect_tick_rx` stayed `None` forever — silently
/// disabling the periodic reconnect tick for the lifetime of the
/// coordinator.
///
/// This test pins the contract directly:
/// 1. Pre-arm a fake peer in `peer_dial_info` (NOT in `connections`)
///    so a fired tick has an observable side effect — the
///    reconnect tracker registers the peer's "disconnect" and
///    `tracked_count` goes from 0 → 1.
/// 2. Race a `recv_peer` against a short timeout, letting the
///    timeout win. `recv_peer` is polled (entering the inner
///    `select!`) and then dropped. Pre-fix this destroys the tick
///    receiver; post-fix the receiver stays in the field.
/// 3. Inject a synthetic tick through the test-only sender clone.
/// 4. Race a second `recv_peer` against a slightly longer timeout.
///    The buffered tick must be consumed: the tick arm fires,
///    `process_reconnect_tick` runs, and the tracker registers the
///    fake peer. Pre-fix the tick arm would be `pending().await`
///    (because `tick_rx.take()` returned `None`) and the tracker
///    would stay empty.
#[tokio::test(flavor = "current_thread")]
async fn recv_peer_tick_survives_outer_drop() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut peer: PeerNetwork<TestId> = PeerNetwork::start("peer-a").await.unwrap();

            // Pre-arm a fake peer entry so `process_reconnect_tick`
            // has work to do. The fake peer id sorts higher than
            // ours so the lower-id-dials rule lets `spawn_redial`
            // reach `spawn_dial_task`; the dial itself fails
            // silently because no server is bound — irrelevant to
            // the side effect we assert on (the tracker increment).
            peer.peer_dial_info.insert(
                "peer-z".into(),
                PeerConnectionInfo {
                    secondary_id: "peer-z".into(),
                    cert: peer.cert_pem().to_string(),
                    ipv4: Some("127.0.0.1".into()),
                    ipv6: None,
                    port: 1,
                    is_observer: false,
                    liveness_port: None,
                },
            );
            assert_eq!(peer.reconnect_tracker.tracked_count(), 0);

            // Step 1: race recv_peer against a short timeout so
            // recv_peer is polled (entering the inner select with
            // all three arms Pending) and then dropped. The
            // timeout is much shorter than the natural 5s tick
            // cadence so no real tick can sneak in and complete
            // the recv_peer before the drop.
            tokio::select! {
                _ = peer.recv_peer() => {
                    panic!("recv_peer should not resolve in this race");
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }

            // Pre-fix invariant check: tracker still empty.
            // (Both pre- and post-fix this should hold — no tick
            // fired yet.)
            assert_eq!(
                peer.reconnect_tracker.tracked_count(),
                0,
                "no tick should have fired during the dropped recv_peer"
            );

            // Step 2: inject a synthetic tick via the test-only
            // sender. Two failure modes the contract guards
            // against, both rolled into this assertion:
            //   - Channel closed: pre-fix the original receiver
            //     was moved into a stack-local inside recv_peer
            //     and dropped along with the dropped future, so
            //     the underlying mpsc channel has no receiver and
            //     `send` returns `Err`.
            //   - Channel alive but receiver detached from
            //     `self.reconnect_tick_rx`: a hypothetical fix
            //     that kept the channel alive but stashed the
            //     receiver somewhere `recv_peer` no longer polls
            //     would let the send succeed but produce the
            //     same silent-disable. Step 3 below catches that
            //     variant.
            peer.reconnect_tick_tx_for_test.send(()).expect(
                "tick channel must survive recv_peer drop; \
                     pre-fix this fails because the receiver was \
                     moved into the dropped future",
            );

            // Step 3: race a second recv_peer against a longer
            // timeout. Post-fix: the tick arm picks up the
            // buffered tick, runs `process_reconnect_tick`,
            // tracker registers "peer-z" disconnect (count → 1),
            // recv_peer loops back to await again, and the
            // timeout eventually wins. Pre-fix: the tick arm is
            // wired to a `None`-taken receiver via
            // `pending::<Option<()>>().await`, the buffered tick
            // is never observed, and the tracker stays empty.
            tokio::select! {
                _ = peer.recv_peer() => {
                    panic!("recv_peer should not resolve in this race");
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }

            assert_eq!(
                peer.reconnect_tracker.tracked_count(),
                1,
                "buffered tick must be observed after outer recv_peer drop; \
                 pre-fix this is 0 because the tick receiver was destroyed",
            );
        })
        .await;
}
