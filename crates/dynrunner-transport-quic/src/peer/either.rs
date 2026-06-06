//! Sum-type wrapper that lets call sites pick `PeerNetwork` or
//! `NoPeerTransport` at runtime without splitting the
//! `SecondaryCoordinator` generic parameter into two compile-time
//! variants.
//!
//! The `PeerTransport` trait uses `impl Future` return types
//! (RPIT-in-trait), which makes it not object-safe — `Box<dyn
//! PeerTransport<I>>` won't compile. An enum is the obvious workaround:
//! we keep the static dispatch and just match-and-delegate on every
//! method.
//!
//! Why this exists: clusters that firewall inter-compute-node
//! networking (LMU SLURM, etc.) have peer dials that always fail. The
//! caller sets `disable_peer_overlay = true` in `DistributedConfig`,
//! the secondary picks the `Disabled` variant, and the doomed
//! 10s-per-peer dial cascade goes away — no peer mesh, no per-peer
//! socket, no QUIC accept loops.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, PeerConnectionInfo, PeerId, PeerTransport,
};
use tokio::sync::mpsc;

use super::{MeshSendHandle, NoPeerTransport, PeerNetwork};

/// The bootstrap-primary link a firewalled (no-peer-mesh) secondary
/// folds in via [`EitherPeerTransport::register_primary_link`].
///
/// A firewalled / single-secondary fleet runs no peer mesh — peer dials
/// always time out, so the overlay is disabled — but the secondary
/// still dials the primary at startup over the bootstrap WSS/QUIC wire.
/// With no mesh `connections` table to fold that wire into (the `Real`
/// arm's mechanism), this is the faithful no-mesh analog: it owns the
/// dialed [`crate::NetworkClient`] as the SOLE reachable member, keyed
/// by the primary's id, with no inter-secondary dialing.
///
/// Both wire directions are folded onto the one routable member, exactly
/// as [`PeerNetwork::register_primary_link`] does, minus the mesh:
/// - **Outbound:** [`crate::NetworkClient::mesh_writer`] mints a fan-in
///   send handle into the wire; [`PeerTransport::send_to_peer`] to
///   `primary_id` writes over it, any other id is a no-route `Err`.
/// - **Inbound:** a forwarder task drains the client's `recv()` into a
///   single fan-in channel that [`PeerTransport::recv_peer`] /
///   [`PeerTransport::try_recv_peer`] read, so the primary's frames
///   surface like any other inbound.
///
/// The primary is a routable member (mirrors the `Real` arm): it is
/// reachable via `send_to_peer` / `has_peer`, counted by `peer_count`
/// (`1`, role-blind — the transport counts every member), AND it receives
/// `broadcast` (`Destination::All`) like any member — it is the sole one,
/// so an `All` fan-out lands on it over the folded wire. The role-aware
/// "how many alive secondaries" policy is the coordinator edge's
/// `alive_secondary_count()` over global state, not the transport's.
///
/// `pub` only so it can be the payload of the `pub`
/// [`EitherPeerTransport::DisabledWithPrimary`] variant (the same
/// reason `PeerNetwork` / `NoPeerTransport` are `pub`); its fields are
/// private, so it stays opaque — callers only ever touch it through the
/// `PeerTransport` trait methods.
pub struct FirewalledPrimaryLink<I: Identifier> {
    /// The primary's peer-id; the sole routable member's key.
    primary_id: String,
    /// Outbound fan-in handle into the dialed bootstrap wire.
    outbound: mpsc::UnboundedSender<DistributedMessage<I>>,
    /// Inbound fan-in fed by the wire-drain forwarder task.
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> FirewalledPrimaryLink<I> {
    /// Fold the dialed primary bootstrap `client` into a no-mesh link:
    /// take its outbound fan-in writer and spawn a forwarder that drains
    /// its inbound into the link's single fan-in channel.
    fn fold(primary_id: String, client: crate::NetworkClient<I>) -> Self {
        use dynrunner_core::MessageReceiver;

        tracing::info!(
            primary = %primary_id,
            "folded primary bootstrap wire into the no-mesh (firewalled) transport"
        );
        let outbound = client.mesh_writer();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        tokio::task::spawn_local(async move {
            let mut client = client;
            while let Some(msg) = client.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
            tracing::debug!("firewalled primary bootstrap-wire inbound forwarder done");
        });
        Self {
            primary_id,
            outbound,
            incoming_rx,
        }
    }

    /// Stage a link directly from pre-made channel ends, no real wire —
    /// the test analog of [`Self::fold`] (mirrors how `primary_link.rs`
    /// stages the `Real` arm's link straight into the `connections`
    /// table). The directed-routing logic — `send_to_peer`/`has_peer`
    /// resolve the primary, `broadcast` (`All`) reaches it as a member —
    /// is independent of the wire; the real-wire fan-in is covered by the
    /// `network/tests.rs` mesh-writer fan-in test.
    #[cfg(test)]
    fn staged(
        primary_id: String,
        outbound: mpsc::UnboundedSender<DistributedMessage<I>>,
        incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ) -> Self {
        Self {
            primary_id,
            outbound,
            incoming_rx,
        }
    }
}

impl<I: Identifier> EitherPeerTransport<I> {
    /// Build a `DisabledWithPrimary` arm from pre-made channel ends, for
    /// wire-free tests of the no-mesh primary-routing path. See
    /// [`FirewalledPrimaryLink::staged`].
    #[cfg(test)]
    pub(super) fn disabled_with_staged_primary(
        primary_id: String,
        outbound: mpsc::UnboundedSender<DistributedMessage<I>>,
        incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ) -> Self {
        Self::DisabledWithPrimary(Box::new(FirewalledPrimaryLink::staged(
            primary_id,
            outbound,
            incoming_rx,
        )))
    }
}

/// Runtime-selected peer transport.
///
/// `Real` carries a fully-functional `PeerNetwork`. `Disabled` carries
/// a `NoPeerTransport` (broadcasts succeed silently, `recv_peer`
/// blocks forever, `peer_count == 0`). Once a firewalled secondary
/// folds its bootstrap primary wire in via
/// [`Self::register_primary_link`], the `Disabled` arm transitions to
/// `DisabledWithPrimary`, where the primary is the sole reachable member
/// over that wire (still no peer mesh). Picked once at secondary
/// startup; the only mid-run transition is the one-shot
/// `Disabled` → `DisabledWithPrimary` fold.
pub enum EitherPeerTransport<I: Identifier> {
    // `PeerNetwork<I>` is ~500 bytes; boxing keeps the enum
    // size close to `Disabled`'s zero so the runtime-select doesn't
    // pessimise the disabled arm (clippy::large_enum_variant).
    Real(Box<PeerNetwork<I>>),
    Disabled(NoPeerTransport),
    // No-peer-mesh, but the bootstrap primary wire has been folded in
    // as the sole routable member. Boxed for the same
    // large_enum_variant reason as `Real` (the link owns an mpsc
    // receiver + sender).
    DisabledWithPrimary(Box<FirewalledPrimaryLink<I>>),
}

impl<I: Identifier> EitherPeerTransport<I> {
    /// Mint a cloneable [`MeshSendHandle`] for an on-demand co-located
    /// primary's send-proxy, when a real mesh exists.
    ///
    /// `Some` for `Real` (a live `PeerNetwork` whose `recv_peer` drains
    /// the proxy); `None` for `Disabled` — a firewalled / single-
    /// secondary deployment has no mesh and therefore no remote
    /// secondaries for a co-located primary to reach. The composition
    /// only wires a co-located primary when this returns `Some`.
    pub fn mesh_send_handle(&self) -> Option<MeshSendHandle<I>> {
        match self {
            Self::Real(p) => Some(p.mesh_send_handle()),
            // No mesh → no remote secondaries for a co-located primary to
            // reach, whether or not the bootstrap primary wire is folded.
            Self::Disabled(_) | Self::DisabledWithPrimary(_) => None,
        }
    }

    /// Fold the secondary's dialed primary bootstrap wire in as a
    /// directed-routable member keyed by `primary_id`, so
    /// `send_to_peer(primary)` / `has_peer(primary)` resolve over the
    /// existing wire and the primary's inbound frames surface via
    /// `recv_peer` — both directions, on whichever arm is active:
    ///
    /// - **`Real`:** forwards to [`PeerNetwork::register_primary_link`],
    ///   folding the wire into the live mesh's `connections` table.
    /// - **`Disabled`:** a firewalled inter-compute fabric / single-
    ///   secondary fleet has no mesh `connections` table. The faithful
    ///   no-mesh analog folds the wire into a [`FirewalledPrimaryLink`]
    ///   that owns it as the SOLE reachable member (no inter-secondary
    ///   dialing) and transitions the arm to `DisabledWithPrimary`. The
    ///   primary then routes directly over the bootstrap wire — the only
    ///   communication path a firewalled fleet has.
    ///
    /// The `client` is consumed in both arms (the active arm owns the
    /// wire; there is no separate `uplink` leg).
    ///
    /// `DisabledWithPrimary` re-registration would double-fold the wire;
    /// it is unreachable in practice (the composition calls this exactly
    /// once, right after the single bootstrap dial) and treated as a
    /// programmer error.
    pub fn register_primary_link(&mut self, primary_id: String, client: crate::NetworkClient<I>) {
        match self {
            Self::Real(p) => p.register_primary_link(primary_id, client),
            Self::Disabled(_) => {
                *self = Self::DisabledWithPrimary(Box::new(FirewalledPrimaryLink::fold(
                    primary_id, client,
                )));
            }
            Self::DisabledWithPrimary(_) => unreachable!(
                "register_primary_link called twice on a no-mesh transport: the bootstrap \
                 primary wire is folded exactly once, right after the single startup dial"
            ),
        }
    }
}

impl<I: Identifier> PeerTransport<I> for EitherPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match self {
            Self::Real(p) => p.broadcast(msg).await,
            Self::Disabled(p) => PeerTransport::<I>::broadcast(p, msg).await,
            // Deliver to the sole reachable member (mirrors the `Real`
            // arm, which fans `broadcast` to EVERY connection — the
            // folded primary included; it excludes nobody). A firewalled
            // fleet's only mesh member is the folded primary, so
            // `Destination::All` must reach it over `link.outbound` —
            // otherwise every `All` send (keepalive and beyond) would be
            // silently dropped, starving the primary of a degraded
            // secondary's keepalives. Role-blind: this delivers to a
            // member, not "the primary role".
            Self::DisabledWithPrimary(link) => link
                .outbound
                .send(msg)
                .map_err(|_| "primary bootstrap wire closed".to_string()),
        }
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        match self {
            Self::Real(p) => p.send_to_peer(peer_id, msg).await,
            Self::Disabled(p) => PeerTransport::<I>::send_to_peer(p, peer_id, msg).await,
            Self::DisabledWithPrimary(link) => {
                if peer_id == link.primary_id {
                    link.outbound
                        .send(msg)
                        .map_err(|_| "primary bootstrap wire closed".to_string())
                } else {
                    Err(format!(
                        "no route to peer '{peer_id}': no peer mesh (firewalled); only the \
                         bootstrap primary '{}' is reachable",
                        link.primary_id
                    ))
                }
            }
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        match self {
            Self::Real(p) => p.recv_peer().await,
            Self::Disabled(p) => PeerTransport::<I>::recv_peer(p).await,
            Self::DisabledWithPrimary(link) => link.incoming_rx.recv().await,
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        match self {
            Self::Real(p) => p.try_recv_peer(),
            Self::Disabled(p) => PeerTransport::<I>::try_recv_peer(p),
            Self::DisabledWithPrimary(link) => link.incoming_rx.try_recv().ok(),
        }
    }

    fn peer_count(&self) -> usize {
        match self {
            // `&**p` derefs Box<PeerNetwork<I>> back to &PeerNetwork<I>
            // so the qualified trait-call resolves the same way as
            // before boxing.
            Self::Real(p) => PeerTransport::<I>::peer_count(&**p),
            Self::Disabled(p) => PeerTransport::<I>::peer_count(p),
            // Pure membership cardinality, role-blind (transport ⊥
            // roles): the folded bootstrap-primary link is a member, so
            // it is counted — exactly as the `Real` arm counts the
            // primary folded into its `connections` table. The role-aware
            // "how many alive secondaries" question is the coordinator
            // edge's `alive_secondary_count()` over global state; the
            // transport never does role arithmetic on its `peer_count`.
            Self::DisabledWithPrimary(_) => 1,
        }
    }

    fn has_peer(&self, id: &PeerId) -> bool {
        // Delegate to the active arm: the real mesh answers from its
        // QUIC connection table; the disabled arm is always `false`; the
        // no-mesh-with-primary arm has the bootstrap primary as its sole
        // reachable member.
        match self {
            Self::Real(p) => PeerTransport::<I>::has_peer(&**p, id),
            Self::Disabled(p) => PeerTransport::<I>::has_peer(p, id),
            Self::DisabledWithPrimary(link) => id.as_str() == link.primary_id,
        }
    }

    fn connected_ids(&self) -> Vec<PeerId> {
        // Mirror the per-arm membership answers of `peer_count`/`has_peer`:
        // the real mesh enumerates its connection table; the disabled arm
        // is empty; the no-mesh-with-primary arm has the bootstrap primary
        // as its sole member. Role-blind throughout.
        match self {
            Self::Real(p) => PeerTransport::<I>::connected_ids(&**p),
            Self::Disabled(p) => PeerTransport::<I>::connected_ids(p),
            Self::DisabledWithPrimary(link) => vec![PeerId::from(link.primary_id.as_str())],
        }
    }

    async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        match self {
            Self::Real(p) => {
                <PeerNetwork<I> as PeerTransport<I>>::connect_to_peers(&mut **p, peers).await
            }
            Self::Disabled(p) => PeerTransport::<I>::connect_to_peers(p, peers).await,
            // No inter-secondary dialing on a firewalled fleet — the only
            // wire is the bootstrap primary link, already folded.
            Self::DisabledWithPrimary(_) => {}
        }
    }
}
