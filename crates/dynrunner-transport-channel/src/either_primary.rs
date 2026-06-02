//! Sum-type wrapper for the secondary's uplink to its primary, letting
//! the link switch at runtime between a remote-network transport and an
//! in-process mpsc loopback without splitting the `SecondaryCoordinator`
//! generic parameter into two compile-time variants.
//!
//! Mirrors `dynrunner-transport-quic`'s `EitherPeerTransport`: the
//! `MessageSender`/`MessageReceiver` traits use `impl Future` return
//! types (RPIT-in-trait), which makes them not object-safe, so a
//! `Box<dyn ..>` won't compile. An enum keeps the static dispatch and
//! just match-and-delegates on every method.
//!
//! # Why this exists
//!
//! A secondary's uplink to the primary is fixed at construction as a
//! single concrete transport (a `NetworkClient` over QUIC/WSS in
//! production, a `ChannelPrimaryTransportEnd` in the in-process
//! manager). When a node is promoted to primary it stands up a
//! co-located `PrimaryCoordinator` and its own still-running
//! `SecondaryCoordinator` must reach that new local primary over an
//! in-process mpsc loopback — exactly like any remote secondary, only
//! the transport impl differs. The uplink's true semantic is therefore
//! "remote-network OR local-loopback", and which one is live can change
//! once, at promotion. This enum models that semantic so the swap is a
//! variant change on a single owned field rather than an impossible
//! reassignment of a monomorphised generic to a different concrete
//! type.
//!
//! The switch is a one-way transition (Network → Loopback) performed by
//! the secondary's loop-driver at the promotion site; nothing switches
//! back (a promoted node stays primary for the rest of the run).

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::DistributedMessage;

use crate::secondary_transport::ChannelPrimaryTransportEnd;

/// Runtime-selected secondary→primary uplink.
///
/// `Network` carries the construction-time remote transport (generic
/// `N`; `NetworkClient` in production, a channel end in the in-process
/// manager). `Loopback` carries the in-process mpsc loopback to a
/// co-located, freshly-composed `PrimaryCoordinator` — installed at the
/// promotion site when this node becomes primary.
///
/// Picked as `Network` at secondary startup; transitions to `Loopback`
/// exactly once, at promotion. The `set_loopback` consumer makes the
/// transition explicit and one-directional.
pub enum EitherPrimaryTransport<N, I: Identifier> {
    Network(N),
    Loopback(ChannelPrimaryTransportEnd<I>),
}

impl<N, I: Identifier> EitherPrimaryTransport<N, I> {
    /// Switch the uplink to the in-process loopback handed to a
    /// co-located composed primary. One-way: a promoted node never
    /// reverts to its remote uplink. Replaces the variant in place so
    /// the secondary's owned field stays the same value; the old
    /// `Network` transport is dropped (its connection to the former
    /// primary is no longer the secondary's link — the co-located
    /// composed primary owns the peer mesh now).
    pub fn set_loopback(&mut self, loopback: ChannelPrimaryTransportEnd<I>) {
        *self = Self::Loopback(loopback);
    }

    /// True iff the uplink is the local-loopback variant (i.e. this node
    /// has been promoted and composed its co-located primary). Exposed
    /// so the loop-driver can make the swap idempotent without inspecting
    /// the inner transports.
    pub fn is_loopback(&self) -> bool {
        matches!(self, Self::Loopback(_))
    }
}

impl<N, I> MessageSender<DistributedMessage<I>> for EitherPrimaryTransport<N, I>
where
    N: MessageSender<DistributedMessage<I>>,
    I: Identifier,
{
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        match self {
            Self::Network(n) => n.send(msg).await,
            Self::Loopback(c) => c.send(msg).await,
        }
    }

    async fn flush(&mut self) -> Result<(), String> {
        match self {
            Self::Network(n) => n.flush().await,
            Self::Loopback(c) => c.flush().await,
        }
    }
}

impl<N, I> MessageReceiver<DistributedMessage<I>> for EitherPrimaryTransport<N, I>
where
    N: MessageReceiver<DistributedMessage<I>>,
    I: Identifier,
{
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        match self {
            Self::Network(n) => n.recv().await,
            Self::Loopback(c) => c.recv().await,
        }
    }
}
