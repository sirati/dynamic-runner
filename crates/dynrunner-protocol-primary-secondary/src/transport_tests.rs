//! In-process harness tests for [`PeerTransport::fetch_run_config`].
//!
//! The protocol crate carries the `fetch_run_config` DEFAULT trait impl
//! but no production [`PeerTransport`] backend (the real ones live in the
//! transport-quic / transport-channel crates). So the harness here is a
//! hand-rolled in-process mock transport — a pair of unbounded channels
//! (fetcher→peer, peer→fetcher) plus a fixed connected-id set — that
//! implements just enough of [`PeerTransport`] to drive the default
//! method. The mock records EVERY frame the fetcher sends so the
//! "unwelcomed" invariant (no `SecondaryWelcome` / `CertExchange` /
//! `ClusterMutation` / capacity frame) is asserted on the real send
//! stream, not inferred.
//!
//! This mirrors the snapshot-bootstrap harness in
//! `dynrunner-transport-channel/tests/snapshot_bootstrap.rs` (which
//! pins `join_running_cluster` against the channel transport) one layer
//! down: a self-contained mock so the test needs no transport-backend
//! dev-dep.

use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::address::PeerId;
use crate::messages::MessageType;
use crate::transport::{FetchRunConfigError, PeerTransport};
use crate::{DistributedMessage, PeerConnectionInfo};

/// Concrete `I` for the harness — the blanket `Identifier` impl
/// (`dynrunner-core`) covers `String`, so the mesh frames are concrete.
type TestId = String;

/// In-process mock peer transport: one outbound channel to the
/// (hand-driven) responder and one inbound channel back. Records the
/// `MessageType` of every frame the unit-under-test sends.
struct MockTransport {
    /// Fetcher→peer: every `send_to_peer` pushes here so the test's
    /// responder loop can observe the request.
    outbound: UnboundedSender<DistributedMessage<TestId>>,
    /// Peer→fetcher: `recv_peer` awaits this; the responder feeds the
    /// `RunConfig` reply (or anything else, for the drop-and-keep arm).
    inbound: UnboundedReceiver<DistributedMessage<TestId>>,
    /// The ids `connected_ids` reports — the cold-start path's folded
    /// bootstrap primary modelled as a single connected member.
    connected: Vec<PeerId>,
    /// Every frame type the unit-under-test handed to `send_to_peer`, in
    /// order. The "unwelcomed" assertion reads this.
    sent_types: Vec<MessageType>,
}

impl PeerTransport<TestId> for MockTransport {
    async fn broadcast(&mut self, _msg: DistributedMessage<TestId>) -> Result<(), String> {
        // fetch_run_config never broadcasts; if it did, recording the
        // type here would surface it. Unreachable in these tests.
        unreachable!("fetch_run_config must not broadcast");
    }

    async fn send_to_peer(
        &mut self,
        _peer_id: &str,
        msg: DistributedMessage<TestId>,
    ) -> Result<(), String> {
        self.sent_types.push(msg.msg_type());
        self.outbound
            .send(msg)
            .map_err(|_| "mock outbound closed".to_string())
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        self.inbound.recv().await
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<TestId>> {
        self.inbound.try_recv().ok()
    }

    fn peer_count(&self) -> usize {
        self.connected.len()
    }

    fn has_peer(&self, id: &PeerId) -> bool {
        self.connected.iter().any(|p| p == id)
    }

    fn connected_ids(&self) -> Vec<PeerId> {
        self.connected.clone()
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // fetch_run_config never dials (the caller's pre-folded mesh is
        // the seam — see method-doc). A call here would be a regression.
        unreachable!("fetch_run_config must not call connect_to_peers");
    }
}

/// Build a mock whose mesh has one connected peer (the folded bootstrap
/// primary). Returns the transport plus the two channel ends the test's
/// responder side drives: the fetcher's outbound receiver (to observe the
/// request) and the inbound sender (to feed the reply).
fn mock_with_primary(
    primary_id: &str,
) -> (
    MockTransport,
    UnboundedReceiver<DistributedMessage<TestId>>,
    UnboundedSender<DistributedMessage<TestId>>,
) {
    let (out_tx, out_rx) = unbounded_channel();
    let (in_tx, in_rx) = unbounded_channel();
    let transport = MockTransport {
        outbound: out_tx,
        inbound: in_rx,
        connected: vec![PeerId::from(primary_id)],
        sent_types: Vec::new(),
    };
    (transport, out_rx, in_tx)
}

/// Happy path: the responder answers `RequestRunConfig` with a
/// `RunConfig` carrying a known argv; the fetcher returns it
/// token-for-token, and the only frame it sent is the request (no
/// welcome / cert / mutation).
#[tokio::test]
async fn fetch_run_config_returns_seeded_argv_token_for_token() {
    let (mut transport, mut out_rx, in_tx) = mock_with_primary("primary");

    let seeded: Vec<String> = vec![
        "--epochs".into(),
        "3".into(),
        "--lr".into(),
        "0.001".into(),
        "--flag-with-spaces".into(),
        "a b c".into(),
    ];
    let seeded_for_responder = seeded.clone();

    // Responder: wait for the request on the fetcher's outbound channel,
    // assert it is a RequestRunConfig whose return address is the
    // fetcher's id, then feed back a RunConfig with the seeded argv.
    let responder = tokio::spawn(async move {
        let req = out_rx.recv().await.expect("fetcher sent a request");
        match req {
            DistributedMessage::RequestRunConfig { sender_id, .. } => {
                assert_eq!(
                    sender_id, "secondary-7",
                    "request carries the fetcher's real id as the unicast return address"
                );
            }
            other => panic!("expected RequestRunConfig, got {:?}", other.msg_type()),
        }
        in_tx
            .send(DistributedMessage::RunConfig {
                target: None,
                sender_id: "primary".into(),
                timestamp: 1.0,
                forwarded_argv: seeded_for_responder,
            })
            .expect("reply delivered");
    });

    let got = transport
        .fetch_run_config("secondary-7", Duration::from_secs(2))
        .await
        .expect("fetch_run_config succeeds");

    responder.await.expect("responder task joined");

    // Token-for-token equality (a default-empty argv would NOT pass — the
    // seeded vec is non-empty and order-sensitive).
    assert_eq!(got, seeded);

    // Unwelcomed (audit D2): the ONLY frame the fetcher sent is the
    // request. No SecondaryWelcome / CertExchange / ClusterMutation /
    // capacity frame ever crossed the wire.
    assert_eq!(
        transport.sent_types,
        vec![MessageType::RequestRunConfig],
        "fetcher must send exactly one RequestRunConfig and nothing else"
    );
    for forbidden in [
        MessageType::SecondaryWelcome,
        MessageType::CertExchange,
        MessageType::ClusterMutation,
        MessageType::PeerInfo,
        MessageType::Keepalive,
    ] {
        assert!(
            !transport.sent_types.contains(&forbidden),
            "fetcher must NOT send {forbidden:?} (read-only peer gossip, not a join)"
        );
    }
}

/// A non-`RunConfig` frame arriving in the window is dropped and the
/// fetcher keeps waiting; the subsequent `RunConfig` is the one returned.
/// Proves the fetch ignores cross-talk (live broadcasts) without aborting.
#[tokio::test]
async fn fetch_run_config_drops_crosstalk_then_returns_run_config() {
    let (mut transport, mut out_rx, in_tx) = mock_with_primary("primary");

    let responder = tokio::spawn(async move {
        let _req = out_rx.recv().await.expect("fetcher sent a request");
        // Cross-talk first: a Keepalive the fetch must drop.
        in_tx
            .send(DistributedMessage::Keepalive {
                target: None,
                sender_id: "primary".into(),
                timestamp: 1.0,
                secondary_id: "primary".into(),
                active_workers: 0,
                emitter_role: crate::messages::KeepaliveRole::Primary,
            })
            .expect("crosstalk delivered");
        // Then the real reply.
        in_tx
            .send(DistributedMessage::RunConfig {
                target: None,
                sender_id: "primary".into(),
                timestamp: 2.0,
                forwarded_argv: vec!["--only".into(), "this".into()],
            })
            .expect("reply delivered");
    });

    let got = transport
        .fetch_run_config("secondary-7", Duration::from_secs(2))
        .await
        .expect("fetch_run_config succeeds past the crosstalk");
    responder.await.expect("responder task joined");

    assert_eq!(got, vec!["--only".to_string(), "this".to_string()]);
}

/// Budget exhaustion: the responder never replies, so the fetch returns
/// a clean `Timeout` (the shim exits non-zero) — it does not hang or
/// panic. A short budget keeps the test fast; the request still went out.
#[tokio::test]
async fn fetch_run_config_budget_exhaustion_is_clean_timeout() {
    // Keep the inbound sender alive (held in `_in_tx`) but never send a
    // reply: the fetch must time out on the budget, not on an inbound
    // close (the close arm also maps to Timeout, but we want to pin the
    // pure-budget path).
    let (mut transport, mut out_rx, _in_tx) = mock_with_primary("primary");

    // Drain the request so the outbound channel doesn't fill (and to
    // prove the request DID go out before the timeout).
    let drained = tokio::spawn(async move { out_rx.recv().await });

    let err = transport
        .fetch_run_config("secondary-7", Duration::from_millis(150))
        .await
        .expect_err("fetch_run_config must time out with no reply");
    assert!(
        matches!(err, FetchRunConfigError::Timeout),
        "expected Timeout, got {err:?}"
    );

    let req = drained.await.expect("drain task joined");
    assert!(
        matches!(req, Some(DistributedMessage::RequestRunConfig { .. })),
        "the request went out before the budget expired"
    );
}

/// No connected peer: the caller's dial leg folded nobody in, so there is
/// no one to ask. The fetch surfaces `NoReachablePeer` immediately rather
/// than burning the budget on an empty `recv`.
#[tokio::test]
async fn fetch_run_config_no_peer_errors_fast() {
    let (out_tx, _out_rx) = unbounded_channel();
    let (_in_tx, in_rx) = unbounded_channel();
    let mut transport = MockTransport {
        outbound: out_tx,
        inbound: in_rx,
        connected: Vec::new(),
        sent_types: Vec::new(),
    };

    let err = transport
        .fetch_run_config("secondary-7", Duration::from_secs(5))
        .await
        .expect_err("no peer to ask");
    assert!(
        matches!(err, FetchRunConfigError::NoReachablePeer),
        "expected NoReachablePeer, got {err:?}"
    );
    assert!(
        transport.sent_types.is_empty(),
        "no request can be sent when there is no connected peer"
    );
}
