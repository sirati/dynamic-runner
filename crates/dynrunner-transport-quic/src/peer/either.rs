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
    DistributedMessage, PeerConnectionInfo, PeerTransport, Role, RoleChangeHookRegistrar,
};

use super::{MeshSendHandle, NoPeerTransport, PeerNetwork};

/// Runtime-selected peer transport.
///
/// `Real` carries a fully-functional `PeerNetwork`. `Disabled` carries
/// a `NoPeerTransport` (broadcasts succeed silently, `recv_peer`
/// blocks forever, `peer_count == 0`). Picked once at secondary
/// startup; never switches mid-run.
pub enum EitherPeerTransport<I: Identifier> {
    // `PeerNetwork<I>` is ~500 bytes; boxing keeps the enum
    // size close to `Disabled`'s zero so the runtime-select doesn't
    // pessimise the disabled arm (clippy::large_enum_variant).
    Real(Box<PeerNetwork<I>>),
    Disabled(NoPeerTransport),
}

impl<I: Identifier> EitherPeerTransport<I> {
    /// Mint a cloneable [`MeshSendHandle`] for a co-located parked
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
            Self::Disabled(_) => None,
        }
    }
}

impl<I: Identifier> PeerTransport<I> for EitherPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match self {
            Self::Real(p) => p.broadcast(msg).await,
            Self::Disabled(p) => PeerTransport::<I>::broadcast(p, msg).await,
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
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        match self {
            Self::Real(p) => p.recv_peer().await,
            Self::Disabled(p) => PeerTransport::<I>::recv_peer(p).await,
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        match self {
            Self::Real(p) => p.try_recv_peer(),
            Self::Disabled(p) => PeerTransport::<I>::try_recv_peer(p),
        }
    }

    fn peer_count(&self) -> usize {
        match self {
            // `&**p` derefs Box<PeerNetwork<I>> back to &PeerNetwork<I>
            // so the qualified trait-call resolves the same way as
            // before boxing.
            Self::Real(p) => PeerTransport::<I>::peer_count(&**p),
            Self::Disabled(p) => PeerTransport::<I>::peer_count(p),
        }
    }

    async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        match self {
            Self::Real(p) => {
                <PeerNetwork<I> as PeerTransport<I>>::connect_to_peers(&mut **p, peers).await
            }
            Self::Disabled(p) => PeerTransport::<I>::connect_to_peers(p, peers).await,
        }
    }

    fn register_with_cluster_state(&self, registrar: &mut dyn RoleChangeHookRegistrar) {
        // Forward to the inner arm. `NoPeerTransport` keeps the
        // trait default (no-op): a disabled peer overlay has no
        // cache to populate, and `peer_for_role` will keep
        // returning `None` on that side — the safe answer for
        // single-secondary deployments.
        match self {
            Self::Real(p) => PeerTransport::<I>::register_with_cluster_state(&**p, registrar),
            Self::Disabled(p) => PeerTransport::<I>::register_with_cluster_state(p, registrar),
        }
    }

    fn peer_for_role(&self, role: &Role) -> Option<String> {
        match self {
            Self::Real(p) => PeerTransport::<I>::peer_for_role(&**p, role),
            Self::Disabled(p) => PeerTransport::<I>::peer_for_role(p, role),
        }
    }

    fn local_id(&self) -> &str {
        // Forward to the inner arm. `NoPeerTransport` keeps the
        // trait default of `""` — a disabled peer overlay has no
        // role envelopes to construct and no misaddress hints can
        // reach it, so the empty default is safe here.
        match self {
            Self::Real(p) => PeerTransport::<I>::local_id(&**p),
            Self::Disabled(p) => PeerTransport::<I>::local_id(p),
        }
    }
}
