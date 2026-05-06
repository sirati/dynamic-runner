use std::collections::HashMap;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerTransport, SecondaryTransport,
};
use dynrunner_protocol_manager_worker::{Command, Response};
use tokio::sync::mpsc;

// ── Manager ↔ Runner channel transport ──

/// Manager-side endpoint backed by tokio mpsc channels.
pub struct ChannelManagerEnd {
    cmd_tx: mpsc::UnboundedSender<Command>,
    resp_rx: mpsc::UnboundedReceiver<Response>,
}

/// Runner-side endpoint backed by tokio mpsc channels.
pub struct ChannelRunnerEnd {
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    resp_tx: mpsc::UnboundedSender<Response>,
}

/// Create a pair of channel endpoints for in-process testing.
pub fn channel_pair() -> (ChannelManagerEnd, ChannelRunnerEnd) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    (
        ChannelManagerEnd { cmd_tx, resp_rx },
        ChannelRunnerEnd { cmd_rx, resp_tx },
    )
}

impl MessageSender<Command> for ChannelManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        self.cmd_tx.send(msg).map_err(|e| e.to_string())
    }
}

impl MessageReceiver<Response> for ChannelManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        self.resp_rx.recv().await
    }

}

impl MessageReceiver<Command> for ChannelRunnerEnd {
    async fn recv(&mut self) -> Option<Command> {
        self.cmd_rx.recv().await
    }
}

impl MessageSender<Response> for ChannelRunnerEnd {
    async fn send(&mut self, msg: Response) -> Result<(), String> {
        self.resp_tx.send(msg).map_err(|e| e.to_string())
    }
}

// ── Primary ↔ Secondary channel transport ──

/// Channel-based transport for the primary side of distributed coordination.
///
/// Holds per-secondary outgoing senders and a single incoming receiver
/// that aggregates messages from all secondaries.
pub struct ChannelSecondaryTransportEnd<I: Identifier> {
    pub outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    pub incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelSecondaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.incoming_rx.recv().await
    }
}

impl<I: Identifier> SecondaryTransport<I> for ChannelSecondaryTransportEnd<I> {
    async fn send_to(&mut self, secondary_id: &str, msg: DistributedMessage<I>) -> Result<(), String> {
        if let Some(tx) = self.outgoing.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), Vec<(String, String)>> {
        let mut errors = Vec::new();
        for (secondary_id, tx) in &self.outgoing {
            if let Err(e) = tx.send(msg.clone()) {
                errors.push((secondary_id.clone(), e.to_string()));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Channel-based transport for the secondary side of distributed coordination.
///
/// Sends to the primary and receives from it via unbounded mpsc channels.
pub struct ChannelPrimaryTransportEnd<I: Identifier> {
    pub tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    pub rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.tx.send(msg).map_err(|e| e.to_string())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.rx.recv().await
    }
}

// ── Peer-to-peer channel transport (for multi-secondary in-process tests) ──

/// Channel-based PeerTransport. Each instance owns one inbox (mpsc receiver)
/// and a dictionary of outboxes (mpsc senders), one per remote peer.
/// `broadcast` clones the message and fans it out to every outbox; the
/// inbox receives whatever other peers sent to *this* secondary.
pub struct ChannelPeerTransport<I: Identifier> {
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
}

impl<I: Identifier + Clone> PeerTransport<I> for ChannelPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        for tx in self.outgoing.values() {
            // Closed senders are tolerated — the peer simply went away.
            let _ = tx.send(msg.clone());
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        if let Some(tx) = self.outgoing.get(peer_id) {
            tx.send(msg).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.incoming_rx.recv().await
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.incoming_rx.try_recv().ok()
    }

    fn peer_count(&self) -> usize {
        self.outgoing.len()
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op: peers are pre-wired via `peer_mesh`.
    }
}

/// Wire up an all-to-all peer mesh for the given ids and return one
/// `ChannelPeerTransport` per id, in input order. Each transport's
/// `outgoing` map contains every other peer's inbox sender.
pub fn peer_mesh<I: Identifier>(peer_ids: &[String]) -> Vec<ChannelPeerTransport<I>> {
    // Allocate inbox + outbox-sender for each peer.
    let mut inboxes: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>> = HashMap::new();
    let mut receivers: HashMap<String, mpsc::UnboundedReceiver<DistributedMessage<I>>> =
        HashMap::new();
    for id in peer_ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inboxes.insert(id.clone(), tx);
        receivers.insert(id.clone(), rx);
    }

    let mut transports = Vec::with_capacity(peer_ids.len());
    for id in peer_ids {
        let incoming_rx = receivers
            .remove(id)
            .expect("inbox was inserted above for every id");
        let mut outgoing = HashMap::new();
        for other in peer_ids {
            if other == id {
                continue;
            }
            outgoing.insert(other.clone(), inboxes[other].clone());
        }
        transports.push(ChannelPeerTransport {
            incoming_rx,
            outgoing,
        });
    }
    transports
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        manager
            .send(Command::ProcessTask { relative_path: "test/bin".into(), payload: None })
            .await
            .unwrap();

        let cmd = runner.recv().await.unwrap();
        match cmd {
            Command::ProcessTask { relative_path, .. } => {
                assert_eq!(relative_path, "test/bin");
            }
            _ => panic!("expected ProcessTask"),
        }
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        runner
            .send(Response::Done {
                result_data: Some(b"2:5".to_vec()),
            })
            .await
            .unwrap();

        let resp = manager.recv().await.unwrap();
        match resp {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"2:5");
            }
            _ => panic!("expected Done"),
        }
    }

    #[tokio::test]
    async fn stop_command() {
        let (mut manager, mut runner) = channel_pair();

        manager.send(Command::Stop).await.unwrap();

        let cmd = runner.recv().await.unwrap();
        assert!(matches!(cmd, Command::Stop));
    }

    #[tokio::test]
    async fn multiple_responses() {
        let (mut manager, mut runner) = channel_pair();

        runner.send(Response::Ready).await.unwrap();
        runner
            .send(Response::PhaseUpdate {
                phase_name: "ANGR_1".into(),
            })
            .await
            .unwrap();
        runner.send(Response::Keepalive).await.unwrap();

        let r1 = manager.recv().await.unwrap();
        assert!(matches!(r1, Response::Ready));
        let r2 = manager.recv().await.unwrap();
        assert!(matches!(r2, Response::PhaseUpdate { .. }));
        let r3 = manager.recv().await.unwrap();
        assert!(matches!(r3, Response::Keepalive));
    }

    #[tokio::test]
    async fn runner_disconnect_returns_none() {
        let (manager, mut runner) = channel_pair();

        // Drop the manager end
        drop(manager);

        // Runner should get a send error
        let result = runner.send(Response::Ready).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn manager_disconnect_returns_none() {
        let (mut manager, runner) = channel_pair();

        // Drop the runner end
        drop(runner);

        // Manager recv should return None (disconnected)
        let resp = manager.recv().await;
        assert!(resp.is_none());
    }

    /// `peer_mesh` wires N transports with all-to-all senders. A broadcast
    /// from one peer should reach every other peer's inbox; nothing should
    /// loop back to the sender.
    #[tokio::test]
    async fn peer_mesh_broadcasts_to_all_others() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        struct TestId(String);

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<TestId>(&ids);

        assert_eq!(transports.len(), 3);
        assert_eq!(transports[0].peer_count(), 2);
        assert_eq!(transports[1].peer_count(), 2);
        assert_eq!(transports[2].peer_count(), 2);

        let msg = DistributedMessage::Keepalive {
            sender_id: "a".into(),
            timestamp: 1.0,
            secondary_id: "a".into(),
            active_workers: 0,
        };
        transports[0].broadcast(msg).await.unwrap();

        // a does not receive its own broadcast
        assert!(transports[0].try_recv_peer().is_none());
        // b and c do
        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_some());
    }

    /// `send_to_peer` reaches exactly one inbox.
    #[tokio::test]
    async fn peer_mesh_send_to_specific_peer() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        struct TestId(String);

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<TestId>(&ids);

        let msg = DistributedMessage::Keepalive {
            sender_id: "a".into(),
            timestamp: 1.0,
            secondary_id: "a".into(),
            active_workers: 0,
        };
        transports[0].send_to_peer("b", msg).await.unwrap();

        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_none());
        assert!(transports[0].try_recv_peer().is_none());
    }
}
