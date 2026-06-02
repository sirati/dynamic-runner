//! Unit tests for [`EitherPrimaryTransport`] — the secondary→primary
//! uplink sum-type. Pins three contracts:
//!
//!   1. `Network` variant delegates `send` / `flush` / `recv` to the
//!      inner network transport;
//!   2. `set_loopback` switches the variant in place (one-way), and the
//!      `Loopback` variant then delegates to the held
//!      `ChannelPrimaryTransportEnd`;
//!   3. after the switch, sends/recvs route over the loopback channel —
//!      i.e. a promoted node's uplink reaches its co-located primary.

use dynrunner_core::{MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::DistributedMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::either_primary::EitherPrimaryTransport;
use crate::secondary_transport::ChannelPrimaryTransportEnd;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

fn keepalive(sender: &str) -> DistributedMessage<TestId> {
    DistributedMessage::Keepalive {
        sender_id: sender.into(),
        timestamp: 1.0,
        secondary_id: sender.into(),
        active_workers: 0,
    }
}

/// `DistributedMessage` derives no `PartialEq` (it's an internal wire
/// enum), so identity in these tests is asserted on the `sender_id` of a
/// `Keepalive` — the only message shape the tests construct. Panics on
/// any other variant so a mis-routed message can't masquerade as a
/// match.
fn keepalive_sender(msg: &DistributedMessage<TestId>) -> &str {
    match msg {
        DistributedMessage::Keepalive { sender_id, .. } => sender_id,
        other => panic!("expected Keepalive, got {:?}", other.msg_type()),
    }
}

/// Stub network transport that records every `send`, every `flush`, and
/// serves `recv` from a script. Lets the tests prove the `Network` arm
/// delegates each method to the inner transport unchanged.
struct StubNetwork {
    sent: Vec<DistributedMessage<TestId>>,
    flushed: usize,
    inbound: mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    flush_result: Result<(), String>,
}

impl MessageSender<DistributedMessage<TestId>> for StubNetwork {
    async fn send(&mut self, msg: DistributedMessage<TestId>) -> Result<(), String> {
        self.sent.push(msg);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), String> {
        self.flushed += 1;
        self.flush_result.clone()
    }
}

impl MessageReceiver<DistributedMessage<TestId>> for StubNetwork {
    async fn recv(&mut self) -> Option<DistributedMessage<TestId>> {
        self.inbound.recv().await
    }
}

/// `Network` variant forwards `send`, `flush`, and `recv` to the inner
/// network transport verbatim.
#[tokio::test]
async fn network_variant_delegates_send_flush_recv() {
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let stub = StubNetwork {
        sent: Vec::new(),
        flushed: 0,
        inbound: inbound_rx,
        // Non-default error so we can assert the result threads through
        // rather than the trait's no-op default masking it.
        flush_result: Err("writer-task-exited".into()),
    };
    let mut uplink: EitherPrimaryTransport<StubNetwork, TestId> =
        EitherPrimaryTransport::Network(stub);

    assert!(!uplink.is_loopback());

    // send delegates
    uplink.send(keepalive("sec")).await.unwrap();
    // flush delegates — and the inner Err propagates (proves no masking)
    assert_eq!(uplink.flush().await, Err("writer-task-exited".into()));

    // recv delegates — feed the inner channel and observe it surface
    inbound_tx.send(keepalive("primary")).unwrap();
    let got = uplink.recv().await.expect("recv yields the inbound msg");
    assert_eq!(keepalive_sender(&got), "primary");

    // recv returns None once the inner channel closes
    drop(inbound_tx);
    assert!(uplink.recv().await.is_none());

    // Pull the stub back out to assert the recorded side-effects.
    let stub = match uplink {
        EitherPrimaryTransport::Network(s) => s,
        EitherPrimaryTransport::Loopback(_) => panic!("variant must still be Network"),
    };
    assert_eq!(stub.sent.len(), 1);
    assert_eq!(keepalive_sender(&stub.sent[0]), "sec");
    assert_eq!(stub.flushed, 1);
}

/// `set_loopback` switches the variant in place; afterwards the uplink
/// delegates to the loopback channel end, and a send reaches whatever
/// reads the pri-side of the loopback (the co-located composed primary),
/// while a recv pulls from the channel the primary writes to.
#[tokio::test]
async fn set_loopback_switches_and_delegates_over_channel() {
    let (_inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let stub = StubNetwork {
        sent: Vec::new(),
        flushed: 0,
        inbound: inbound_rx,
        flush_result: Ok(()),
    };
    let mut uplink: EitherPrimaryTransport<StubNetwork, TestId> =
        EitherPrimaryTransport::Network(stub);

    // Build the loopback pair the way the promotion driver will:
    //   sec→pri : the secondary's `send` reaches the composed primary;
    //   pri→sec : the composed primary's writes reach the secondary's recv.
    let (sec_to_pri_tx, mut sec_to_pri_rx) = mpsc::unbounded_channel();
    let (pri_to_sec_tx, pri_to_sec_rx) = mpsc::unbounded_channel();
    let loopback = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };

    uplink.set_loopback(loopback);
    assert!(uplink.is_loopback());

    // send now goes over the loopback to the (composed-primary-side) rx
    uplink.send(keepalive("sec")).await.unwrap();
    let routed = sec_to_pri_rx
        .recv()
        .await
        .expect("send routes over the sec→pri loopback to the co-located primary");
    assert_eq!(keepalive_sender(&routed), "sec");

    // recv now pulls from the pri→sec loopback the composed primary writes
    pri_to_sec_tx.send(keepalive("primary")).unwrap();
    let got = uplink.recv().await.expect("recv pulls from pri→sec loopback");
    assert_eq!(keepalive_sender(&got), "primary");

    // flush on the channel end is the trait no-op default → Ok
    assert_eq!(uplink.flush().await, Ok(()));

    // The pre-promotion network transport was dropped on the switch — no
    // stray send to it could leak to the former primary. (Asserted
    // structurally: the variant is Loopback, so the StubNetwork is gone.)
    assert!(matches!(uplink, EitherPrimaryTransport::Loopback(_)));
}

/// The switch is idempotent under `is_loopback` gating: a second
/// `set_loopback` simply replaces the channel; the variant stays
/// Loopback. (The driver gates on `is_loopback` to avoid a redundant
/// swap, but the primitive itself tolerates a re-set.)
#[tokio::test]
async fn set_loopback_is_one_way_replace() {
    let (_pri_to_sec_tx, pri_to_sec_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
    let (sec_to_pri_tx, _sec_to_pri_rx) = mpsc::unbounded_channel();
    let first = ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx,
        rx: pri_to_sec_rx,
    };
    let mut uplink: EitherPrimaryTransport<StubNetwork, TestId> =
        EitherPrimaryTransport::Loopback(first);
    assert!(uplink.is_loopback());

    let (sec_to_pri_tx2, mut sec_to_pri_rx2) = mpsc::unbounded_channel();
    let (_pri_to_sec_tx2, pri_to_sec_rx2) = mpsc::unbounded_channel();
    uplink.set_loopback(ChannelPrimaryTransportEnd {
        tx: sec_to_pri_tx2,
        rx: pri_to_sec_rx2,
    });
    assert!(uplink.is_loopback());

    // sends now route over the SECOND loopback
    uplink.send(keepalive("sec")).await.unwrap();
    let routed = sec_to_pri_rx2.recv().await.expect("routes over the second loopback");
    assert_eq!(keepalive_sender(&routed), "sec");
}
