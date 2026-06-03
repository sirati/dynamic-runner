//! Channel-based [`PeerTransport`] implementation.
//!
//! Owns a per-peer inbox + outbox table and delegates all routing
//! decisions to [`Router`]. Tests simulate partitions by mutating
//! `outgoing` through `disconnect_from` / `connect_to`.
//!
//! Role-blind by construction (transport ⊥ roles): it addresses peers
//! only by [`PeerId`] and never resolves or mentions primary/secondary.
//! Role-resolution lives at the coordinator egress edge; this transport
//! just delivers to the id it is handed. The bootstrap primary is folded
//! in as an ordinary directed-routable member via
//! [`ChannelPeerTransport::register_primary_link`] — the channel analog
//! of the QUIC `PeerNetwork::register_primary_link` — so
//! `send_to_peer(primary)` / `has_peer(primary)` resolve over it while it
//! is excluded from the `broadcast` fan-out and the `peer_count`
//! mesh-health cardinality, exactly as the QUIC transport does.

use std::collections::HashMap;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, InboundOutcome, PeerConnectionInfo, PeerId, PeerTransport, Router,
    SendOutcome,
};
use tokio::sync::mpsc;

use crate::now_clocks;

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
    /// Local peer-id. The Router holds this id for relay-path
    /// bookkeeping; duplicating it at the transport level is cheap
    /// (`String`, populated once at mesh construction) and lets the
    /// transport answer its own identity without reaching into the
    /// router.
    pub(crate) local_id: String,
    pub(crate) incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    pub(crate) outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    /// Peer-mesh routing dispatcher. Owns ALL routing state (in-flight
    /// relays, blacklist, per-peer route observation, monotonic
    /// relay-id counter). The transport itself never inspects routing
    /// state.
    pub router: Router<I>,
    /// Most recent `Router::send_to_peer` outcome — exposed for test
    /// assertions per `M5` of the relay-routing plan. Not part of the
    /// stable public API; production callers should ignore it.
    pub last_outcome: Option<SendOutcome>,
    /// Id of the bootstrap-primary's DIRECTED mesh link, if registered
    /// via [`Self::register_primary_link`]. The secondary registers its
    /// channel link to the in-process primary here so
    /// `send_to_peer(primary)` / `has_peer(primary)` resolve over the
    /// folded wire — the primary becomes a routable mesh member from the
    /// secondary's side.
    ///
    /// It is a DIRECTED-only member, NOT a full broadcast/health peer: it
    /// is excluded from the [`PeerTransport::broadcast`] fan-out and from
    /// the [`PeerTransport::peer_count`] mesh-health cardinality, so this
    /// registration does not change broadcast topology or mesh-watchdog
    /// behaviour. Symmetric with `PeerNetwork::primary_link_id` on the
    /// QUIC side.
    pub(crate) primary_link_id: Option<String>,
}

impl<I: Identifier + Clone> PeerTransport<I> for ChannelPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        for (peer_id, tx) in &self.outgoing {
            // The bootstrap-primary directed link is routable
            // (`send_to_peer` / `has_peer`) but NOT a broadcast member:
            // the secondary's mesh broadcast reaches peers only, exactly
            // as before the primary was folded in. Folding the primary
            // into the (deliver-once) broadcast fan-out is a later leaf.
            if self.primary_link_id.as_deref() == Some(peer_id.as_str()) {
                continue;
            }
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
            match self.router.process_inbound(msg, &mut self.outgoing, clocks) {
                // `msg` is `Box<DistributedMessage<I>>`; unbox to yield
                // the routed frame to the application layer.
                InboundOutcome::Deliver { msg, .. } => return Some(*msg),
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
                InboundOutcome::Deliver { msg, .. } => return Some(*msg),
                InboundOutcome::Handled { .. } => continue,
            }
        }
    }

    fn peer_count(&self) -> usize {
        // Mesh-health cardinality: the count of real peer secondaries.
        // The bootstrap-primary directed link (if registered) is routable
        // but is NOT a mesh peer for health purposes — it must not inflate
        // the count the mesh-watchdog / MeshReady report read, or a fleet
        // reaching only the primary would falsely look "mesh-formed".
        // Excluded symmetrically with `broadcast`.
        let direct = self.outgoing.len();
        let primary_link = usize::from(
            self.primary_link_id
                .as_deref()
                .is_some_and(|id| self.outgoing.contains_key(id)),
        );
        direct - primary_link
    }

    fn has_peer(&self, id: &PeerId) -> bool {
        // Real per-id membership: a peer is a member iff it has a direct
        // outbox in `outgoing`. The bootstrap-primary directed link IS
        // such an entry, so `has_peer(primary)` is `true` once registered
        // — it is excluded only from the broadcast fan-out and the
        // mesh-health `peer_count`, not from per-id reachability.
        // Partition tests that `disconnect_from` / `connect_to` a peer
        // flip this predicate.
        self.outgoing.contains_key(id.as_str())
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op: peers are pre-wired via `peer_mesh` /
        // `peer_mesh_with_adjacency`. Test drivers simulate partition
        // heal via `connect_to` directly.
    }

    // `register_with_cluster_state` / `peer_for_role` are intentionally
    // NOT overridden: the transport is role-blind (transport ⊥ roles),
    // so it carries no role cache and resolves no role. The trait's no-op
    // / `None` defaults are exactly correct here — role-resolution lives
    // at the coordinator egress edge. [de-role-trait #136]

    fn local_id(&self) -> &str {
        &self.local_id
    }
}

impl<I: Identifier> ChannelPeerTransport<I> {
    /// Build a transport from a raw inbox receiver + per-peer outbox
    /// table, rather than the all-to-all [`crate::peer_mesh`] wiring.
    ///
    /// This is the channel-transport analogue of the public
    /// [`crate::ChannelSecondaryTransportEnd`] `{ outgoing, incoming_rx }`
    /// shape: a fixture that already owns hand-rolled per-peer channels
    /// (e.g. a secondary whose inbound `incoming_rx` is fed by the
    /// in-process primary and whose `outgoing[primary]` reaches it via
    /// [`Self::register_primary_link`]) wraps them in a real
    /// [`PeerTransport`] without standing up a full mesh of
    /// `ChannelPeerTransport`s. The `Router` is constructed the same way
    /// [`crate::peer_mesh_with_adjacency`] does, so relay behaves
    /// identically — the only difference is who supplied the channels.
    pub fn from_raw_channels(
        local_id: String,
        outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
        incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ) -> Self {
        Self {
            router: Router::new(local_id.clone()),
            local_id,
            incoming_rx,
            outgoing,
            last_outcome: None,
            primary_link_id: None,
        }
    }

    /// Fold a channel link to the bootstrap primary into THIS transport
    /// as a DIRECTED-routable member keyed by `primary_id`. The channel
    /// analog of `PeerNetwork::register_primary_link`: after this call
    /// the link is just another mesh connection — "the tunnel is just a
    /// way of joining the mesh."
    ///
    /// - **Outbound:** `to_primary` is inserted into [`Self::outgoing`],
    ///   so [`PeerTransport::send_to_peer`]`(primary_id, ..)` reaches the
    ///   primary and [`PeerTransport::has_peer`]`(primary_id)` is true.
    /// - **Inbound** is the transport's single [`Self::incoming_rx`] (the
    ///   primary's frames are fed into it by the constructing fixture /
    ///   run-mode wiring), so primary frames surface through
    ///   [`PeerTransport::recv_peer`] like any other peer's.
    ///
    /// The primary is a DIRECTED-only member: it is recorded in
    /// [`Self::primary_link_id`] and excluded from
    /// [`PeerTransport::broadcast`] and the [`PeerTransport::peer_count`]
    /// mesh-health cardinality, preserving the bootstrap behaviour where
    /// the secondary's mesh broadcast and mesh-health count never include
    /// the primary. Symmetric with the QUIC side.
    pub fn register_primary_link(
        &mut self,
        primary_id: String,
        to_primary: mpsc::UnboundedSender<DistributedMessage<I>>,
    ) {
        tracing::debug!(primary = %primary_id, "folded primary channel link into the mesh");
        self.outgoing.insert(primary_id.clone(), to_primary);
        self.primary_link_id = Some(primary_id);
    }

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
