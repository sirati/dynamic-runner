//! [`ColocatedPrimaryTransport`] — the sole `Tr: PeerTransport` a
//! co-located parked `PrimaryCoordinator` uses to reach the cluster's
//! secondaries once this node is promoted.
//!
//! # Concern
//!
//! A SLURM secondary node that composes a parked primary runs both
//! coordinators on one `LocalSet`. Post-collapse the `PrimaryCoordinator`
//! addresses secondaries through ONE `Tr: PeerTransport` —
//! `send(Address::…)` / `send_to_peer(id, msg)` / `broadcast(msg)` /
//! `recv_peer()`. This type is that `Tr`, real-by-construction (every
//! send routes to a live loopback or the live mesh — no no-op path),
//! hybrid by the SAME key the inbound demux uses (own-secondary vs
//! remote). It also keeps its `SecondaryTransport` impl so the
//! method mapping the `PeerTransport` impl delegates to stays in one
//! place:
//!
//!   * `recv()` — drains the ROLE-AWARE inbound tap
//!     (`UnifiedSecondaryTransport::attach_colocated_primary_tap`, in
//!     the `dynrunner-transport-tunnel` crate). While this node holds
//!     `Role::Primary`, every remote secondary's primary-facing frame
//!     (TaskRequest / TaskComplete / TaskFailed / MeshReady /
//!     SecondaryWelcome / CertExchange / SecondaryFatalError) is
//!     diverted here by the secondary's transport.
//!   * `send_to(own_secondary_id, msg)` — LOOPBACK: injects into the
//!     co-located secondary's own inbound queue (via the secondary
//!     transport's `UnifiedSecondaryTransport::loopback_injector`), so
//!     an assignment for this node's own workers reaches its
//!     `recv_peer` exactly as a wire frame would.
//!   * `send_to(remote_secondary_id, msg)` — MESH: queues on the
//!     [`MeshSendHandle`], which the owning `PeerNetwork`'s `recv_peer`
//!     drains + dispatches relay-aware.
//!   * `broadcast(msg)` — loopback (own secondary) + mesh broadcast (all
//!     remote secondaries). The primary's CRDT propagation
//!     (`ClusterMutation`, `RunComplete`, keepalives) flows through here.
//!
//! This is the faithful generalization of the in-process composition's
//! `ChannelSecondaryTransportEnd` (per-secondary writer fan-out + single
//! inbound recv): the loopback injector is the own-secondary writer, the
//! mesh handle is the remote-secondary writers, and the tap is the
//! aggregated inbound.
//!
//! # Single-threaded by construction
//!
//! One `LocalSet`; all channels are `tokio::sync::mpsc`. Every send is
//! synchronous (`UnboundedSender::send` / the proxy-queue handle), so no
//! borrow is held across an await.

use dynrunner_core::{Identifier, MessageReceiver};
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerTransport, SecondaryTransport,
};
use tokio::sync::mpsc;

use crate::MeshSendHandle;

/// `T: SecondaryTransport` for a parked co-located primary. See module
/// docs for the routing model.
pub struct ColocatedPrimaryTransport<I: Identifier> {
    /// This node's own secondary id. `send_to`/`broadcast` route a
    /// frame targeting this id to the loopback (the co-located
    /// secondary), everything else to the mesh.
    own_secondary_id: String,
    /// Loopback injector into the co-located secondary's inbound queue.
    loopback_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    /// Cloneable mesh-send capability (relay-aware; drained by the
    /// owning `PeerNetwork::recv_peer`).
    mesh: MeshSendHandle<I>,
    /// Role-aware inbound tap from the secondary's unified transport.
    /// Yields the primary-facing frames while this node holds
    /// `Role::Primary`.
    inbound_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> ColocatedPrimaryTransport<I> {
    /// Compose the parked primary's transport from the three handles the
    /// secondary's unified transport + mesh expose:
    ///   * `own_secondary_id` — this node's secondary id (the loopback
    ///     key).
    ///   * `loopback_tx` — `UnifiedSecondaryTransport::loopback_injector()`.
    ///   * `mesh` — `EitherPeerTransport::mesh_send_handle()` (the mesh's
    ///     cloneable send capability).
    ///   * `inbound_rx` — `UnifiedSecondaryTransport::attach_colocated_primary_tap()`.
    pub fn new(
        own_secondary_id: String,
        loopback_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
        mesh: MeshSendHandle<I>,
        inbound_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ) -> Self {
        Self {
            own_secondary_id,
            loopback_tx,
            mesh,
            inbound_rx,
        }
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ColocatedPrimaryTransport<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        // Drain the role-aware tap. `None` only when the secondary's
        // unified transport has been torn down (its loopback/tap sender
        // dropped) — the primary's operational loop treats a closed
        // transport as end-of-inbound exactly as it does for the
        // channel/network transports.
        self.inbound_rx.recv().await
    }
}

impl<I: Identifier> SecondaryTransport<I> for ColocatedPrimaryTransport<I> {
    async fn send_to(
        &mut self,
        secondary_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        if secondary_id == self.own_secondary_id {
            // Loopback to the co-located secondary's inbound queue.
            self.loopback_tx
                .send(msg)
                .map_err(|_| "co-located secondary inbound loopback closed".to_string())
        } else {
            // Remote secondary over the shared mesh (relay-aware).
            self.mesh.send_to_peer(secondary_id, msg)
        }
    }

    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), Vec<(String, String)>> {
        // Loopback to the co-located secondary AND fan out over the
        // mesh to every remote secondary. The co-located secondary is
        // NOT a mesh peer of itself, so the loopback leg is required for
        // it to observe the primary's broadcasts (CRDT mutations,
        // RunComplete, keepalive). Per-leg failures are collected into
        // the `(id, err)` shape the trait's `broadcast` contract uses.
        let mut errors = Vec::new();
        if self.loopback_tx.send(msg.clone()).is_err() {
            errors.push((
                self.own_secondary_id.clone(),
                "co-located secondary inbound loopback closed".to_string(),
            ));
        }
        if let Err(e) = self.mesh.broadcast(msg) {
            errors.push(("<mesh>".to_string(), e));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl<I: Identifier> PeerTransport<I> for ColocatedPrimaryTransport<I> {
    /// Faithful map of the `SecondaryTransport` send path: own-secondary
    /// → loopback, remote → mesh. `send_to` already encodes that hybrid
    /// routing; delegate so there is one routing decision, not two
    /// copies. The parked primary addresses peers purely by id
    /// (`Address::Peer`) and broadcasts (`Address::Broadcast`); the
    /// trait's default `send` dispatch covers both over these.
    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        SecondaryTransport::send_to(self, peer_id, msg).await
    }

    /// Loopback (own secondary) + mesh fan-out (every remote
    /// secondary). Delegates to the `SecondaryTransport::broadcast`
    /// routing and collapses its per-leg `Vec<(id, err)>` into the
    /// single `String` the `PeerTransport` broadcast contract uses —
    /// the parked primary's keepalive / CRDT fan-out reads a single
    /// error, and the per-secondary signal is the heartbeat monitor,
    /// not this return value (same collapse the submitter primary's
    /// keepalive emitter already relies on).
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        SecondaryTransport::broadcast(self, msg)
            .await
            .map_err(|failures| {
                failures
                    .into_iter()
                    .map(|(id, err)| format!("{id}: {err}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            })
    }

    /// Drain the role-aware inbound tap — the same stream
    /// `MessageReceiver::recv` exposes. While this node holds
    /// `Role::Primary`, the secondary's unified transport diverts every
    /// remote secondary's primary-facing frame here; `None` only when
    /// that transport has been torn down (its tap sender dropped),
    /// which the operational loop treats as end-of-inbound exactly as
    /// for the network/channel transports.
    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.inbound_rx.recv().await
    }

    /// Non-blocking peek of the inbound tap. No role-layer interception
    /// is needed: the secondary's unified transport already unwrapped /
    /// relayed any `RoleAddressed` / `RoleMisaddressHint` envelope
    /// before diverting a primary-facing frame here, so what arrives on
    /// this tap is always an application frame.
    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.inbound_rx.try_recv().ok()
    }

    /// Mesh-health cardinality is owned by the secondary's unified
    /// transport (which owns the real `PeerNetwork`); this co-located
    /// view sends THROUGH the shared mesh-send handle but holds no peer
    /// table of its own. Report 0 — the parked primary reads peer
    /// health off `self.secondaries` / `cluster_state`, never off this
    /// transport's `peer_count` (the failover/watchdog readers live on
    /// the SECONDARY's transport).
    fn peer_count(&self) -> usize {
        0
    }

    /// No-op: the mesh this transport sends through is dialed and owned
    /// by the co-located secondary's `UnifiedSecondaryTransport`; the
    /// parked primary never originates peer dials.
    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {}

    /// The parked primary's own id (its `PrimaryConfig::node_id` ==
    /// this node's `own_secondary_id`). Stamped onto any `RoleAddressed`
    /// envelope the default `send` would construct; in practice the
    /// authority addresses by `Address::Peer` / `Address::Broadcast`
    /// (never `Address::Role`), so this is belt-and-suspenders for the
    /// trait contract.
    fn local_id(&self) -> &str {
        &self.own_secondary_id
    }
}
