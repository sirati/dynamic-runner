use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    Clocks, DistributedMessage, InboundOutcome, PeerConnectionInfo, PeerTransport, Router,
    SecondaryTransport, SendOutcome,
};
use dynrunner_protocol_manager_worker::{Command, Response};
use tokio::sync::mpsc;

/// Unix-epoch wall-clock seconds for the wire-side `Clocks::wire`
/// envelope timestamp. Local-clock TTL/cooldown decisions inside Router
/// use the monotonic `Instant::now()` carried alongside it.
fn timestamp_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Snapshot the `Clocks` pair Router consumes — kept centralised so
/// every entry point stays in lockstep with the QUIC transport's
/// equivalent helper (cf. `transport-quic/src/peer/transport_impl.rs`).
fn now_clocks() -> Clocks {
    Clocks {
        now: Instant::now(),
        wire: timestamp_secs(),
    }
}

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

/// Channel-based [`PeerTransport`]. Each instance owns one inbox
/// (mpsc receiver), a dictionary of outboxes (mpsc senders, one per
/// directly-reachable peer), and a [`Router`] that owns ALL routing
/// decisions — direct vs relay, blacklist, redial-cooldown gate.
/// Adjacency is set up by [`peer_mesh`] (all-to-all) or
/// [`peer_mesh_with_adjacency`] (caller-supplied undirected links);
/// tests simulate partitions by mutating `outgoing` through
/// [`ChannelPeerTransport::disconnect_from`] /
/// [`ChannelPeerTransport::connect_to`].
///
/// `last_outcome` exposes the most recent `Router::send_to_peer`
/// outcome for test assertions (the channel transport doesn't dial,
/// so the redial signal is observable here rather than producing
/// background work). It is `pub` deliberately — tests assert against
/// it directly instead of bypassing the [`PeerTransport`] trait, per
/// the "abstractions the test path circumvents are wrong" rule.
pub struct ChannelPeerTransport<I: Identifier> {
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    /// Peer-mesh routing dispatcher. Owns ALL routing state (in-flight
    /// relays, blacklist, per-peer route observation, monotonic
    /// relay-id counter). The transport itself never inspects routing
    /// state.
    pub router: Router<I>,
    /// Most recent `Router::send_to_peer` outcome — exposed for test
    /// assertions per `M5` of the relay-routing plan. Not part of the
    /// stable public API; production callers should ignore it.
    pub last_outcome: Option<SendOutcome>,
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
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        let outcome = self
            .router
            .send_to_peer(peer_id, msg, &mut self.outgoing, clocks)
            .map_err(|e| e.to_string())?;
        self.last_outcome = Some(outcome.clone());
        // Channel transport does not dial — partition heal in tests is
        // manual via `connect_to`. The redial signal carried inside
        // `SendOutcome::Relayed` is observable through `last_outcome`
        // for assertions; nothing else acts on it here.
        match outcome {
            SendOutcome::NoRoute => Err(format!(
                "no route to peer '{peer_id}': direct unreachable and no forwarder available"
            )),
            SendOutcome::Direct | SendOutcome::Relayed { .. } => Ok(()),
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        let mut clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            let msg = self.incoming_rx.recv().await?;
            clocks = now_clocks();
            self.router.prune(clocks.now);
            match self
                .router
                .process_inbound(msg, &mut self.outgoing, clocks)
            {
                InboundOutcome::Deliver { msg, .. } => return Some(msg),
                InboundOutcome::Handled { .. } => continue,
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            let msg = self.incoming_rx.try_recv().ok()?;
            match self.router.process_inbound_sync(msg, clocks) {
                InboundOutcome::Deliver { msg, .. } => return Some(msg),
                InboundOutcome::Handled { .. } => continue,
            }
        }
    }

    fn peer_count(&self) -> usize {
        self.outgoing.len()
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op: peers are pre-wired via `peer_mesh` /
        // `peer_mesh_with_adjacency`. Test drivers simulate partition
        // heal via `connect_to` directly.
    }
}

impl<I: Identifier> ChannelPeerTransport<I> {
    /// Remove a peer's outbox so a subsequent send finds no direct
    /// channel — the Router will then route via relay (or no-route)
    /// just as if the underlying network link had broken. Used by
    /// partition tests; idempotent on already-disconnected peers.
    pub fn disconnect_from(&mut self, peer_id: &str) {
        self.outgoing.remove(peer_id);
    }

    /// Re-add a peer's outbox so a subsequent send can again take the
    /// direct path — used by partition-heal tests. Overwrites any
    /// existing entry.
    pub fn connect_to(
        &mut self,
        peer_id: String,
        sender: mpsc::UnboundedSender<DistributedMessage<I>>,
    ) {
        self.outgoing.insert(peer_id, sender);
    }
}

/// Build the full set of unordered pairs `(a, b)` where `a < b` in
/// input order, given a peer-id list. The shared subroutine behind
/// the all-to-all [`peer_mesh`] adjacency.
fn all_undirected_pairs(ids: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(ids.len() * ids.len().saturating_sub(1) / 2);
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            out.push((a.clone(), b.clone()));
        }
    }
    out
}

/// Wire up an all-to-all peer mesh for the given ids and return one
/// [`ChannelPeerTransport`] per id, in input order. Each transport's
/// outbox map contains every other peer's inbox sender.
///
/// Delegates to [`peer_mesh_with_adjacency`] with the full set of
/// unordered pairs so a single adjacency wiring path serves both
/// fully-connected and partial-mesh test fixtures.
pub fn peer_mesh<I: Identifier>(peer_ids: &[String]) -> Vec<ChannelPeerTransport<I>> {
    let links = all_undirected_pairs(peer_ids);
    peer_mesh_with_adjacency(peer_ids, &links)
}

/// Wire up a partial peer mesh defined by an explicit adjacency list
/// and return one [`ChannelPeerTransport`] per id, in `peer_ids`
/// input order. Each `(a, b)` link is **undirected**: a's outbox gains
/// b's sender AND b's outbox gains a's sender.
///
/// # Panics
///
/// - Panics if a link references an id not present in `peer_ids` —
///   silently dropping it would half-wire a partition the test
///   thought was symmetric.
/// - Panics if the same unordered pair appears more than once
///   (including the directed-style `(a, b) + (b, a)` form). A
///   misconfigured fixture that accidentally lists a link twice would
///   half-wire a partition test exactly the way a real bug would, so
///   we refuse to construct the mesh rather than mask the typo.
pub fn peer_mesh_with_adjacency<I: Identifier>(
    peer_ids: &[String],
    links: &[(String, String)],
) -> Vec<ChannelPeerTransport<I>> {
    // Allocate inbox + outbox-sender for each peer.
    let mut inboxes: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>> = HashMap::new();
    let mut receivers: HashMap<String, mpsc::UnboundedReceiver<DistributedMessage<I>>> =
        HashMap::new();
    for id in peer_ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inboxes.insert(id.clone(), tx);
        receivers.insert(id.clone(), rx);
    }

    // Build the per-peer outgoing tables from the undirected adjacency
    // list. We key by the canonical (lo, hi) ordering so a duplicate
    // — whether listed twice in the same direction or once each
    // direction — surfaces as a panic rather than silent re-insert.
    let mut outgoing: HashMap<String, HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>> =
        peer_ids
            .iter()
            .map(|id| (id.clone(), HashMap::new()))
            .collect();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for (a, b) in links {
        assert!(a != b, "peer_mesh_with_adjacency: self-link '{a}' is not allowed");
        assert!(
            inboxes.contains_key(a),
            "peer_mesh_with_adjacency: link references unknown peer '{a}'"
        );
        assert!(
            inboxes.contains_key(b),
            "peer_mesh_with_adjacency: link references unknown peer '{b}'"
        );
        let canonical = if a <= b {
            (a.clone(), b.clone())
        } else {
            (b.clone(), a.clone())
        };
        assert!(
            seen.insert(canonical.clone()),
            "peer_mesh_with_adjacency: duplicate link {canonical:?} (adjacency is undirected; \
             list each unordered pair at most once)"
        );
        outgoing
            .get_mut(a)
            .expect("peer table allocated above")
            .insert(b.clone(), inboxes[b].clone());
        outgoing
            .get_mut(b)
            .expect("peer table allocated above")
            .insert(a.clone(), inboxes[a].clone());
    }

    let mut transports = Vec::with_capacity(peer_ids.len());
    for id in peer_ids {
        let incoming_rx = receivers
            .remove(id)
            .expect("inbox was inserted above for every id");
        let outgoing_for_peer = outgoing
            .remove(id)
            .expect("outgoing table allocated for every id");
        transports.push(ChannelPeerTransport {
            incoming_rx,
            outgoing: outgoing_for_peer,
            router: Router::new(id.clone()),
            last_outcome: None,
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
