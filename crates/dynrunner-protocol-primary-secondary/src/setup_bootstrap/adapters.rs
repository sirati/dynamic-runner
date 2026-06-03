//! Adapter structs that wrap the existing
//! `MessageSender + MessageReceiver` / [`PeerTransport`] surface and
//! narrow it to [`SetupBootstrapMessage`] for the setup-phase window.
//!
//! - [`SecondarySetupBootstrap`] — secondary side, borrows the same
//!   underlying primary-bound transport the operational path uses.
//! - [`PrimaryPeerSetupBootstrap`] — primary side, borrows the unified
//!   [`PeerTransport`] mesh fan-out the operational path uses.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};

use crate::setup_bootstrap::message::SetupBootstrapMessage;
use crate::setup_bootstrap::trait_defs::{SetupBootstrap, SetupBootstrapBroadcast};
use crate::{DistributedMessage, PeerTransport};

/// Secondary-side adapter: wraps a `&mut T` (any
/// `MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>`)
/// and narrows its message type to [`SetupBootstrapMessage`].
///
/// Construction is cheap — just a mutable borrow — so the call site
/// builds an adapter for the duration of a single `send` / `recv` and
/// drops it. The underlying transport stays available for operational
/// messaging through its original sender/receiver shape (the legacy
/// `PrimaryTransport` marker trait retired in Step 11; the underlying
/// `MessageSender + MessageReceiver` carries every former `PrimaryTransport`
/// impl unchanged via the same blanket the marker used).
///
/// # Why a borrow, not an owned value?
///
/// The secondary coordinator owns `primary_transport: PT` as a field;
/// every setup call site mutates it briefly. Passing `&mut PT` keeps
/// the lifetime story trivial and lets the operational code (which
/// also wants `&mut self.primary_transport` for non-setup recv) coexist
/// with no extra plumbing. The adapter holds the borrow only for the
/// duration of the `send` / `recv` await — never across phases — so
/// nothing else competes.
pub struct SecondarySetupBootstrap<'a, T> {
    transport: &'a mut T,
}

impl<'a, T> SecondarySetupBootstrap<'a, T> {
    /// Build the adapter for the duration of one setup-phase
    /// send/recv. The caller keeps owning the underlying transport.
    pub fn new(transport: &'a mut T) -> Self {
        Self { transport }
    }
}

impl<T, I> SetupBootstrap<I> for SecondarySetupBootstrap<'_, T>
where
    I: Identifier,
    T: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
{
    async fn send(&mut self, msg: SetupBootstrapMessage) -> Result<(), String> {
        // Step 1: lossless conversion to the wire-shape.
        let wire: DistributedMessage<I> = msg.into();
        // Step 2: forward through the underlying transport. The wire
        // bytes are identical to the pre-Step-10 path — only the
        // call-site type has narrowed.
        <T as MessageSender<DistributedMessage<I>>>::send(self.transport, wire).await
    }

    async fn recv(&mut self) -> Option<SetupBootstrapMessage> {
        loop {
            let msg = <T as MessageReceiver<DistributedMessage<I>>>::recv(self.transport).await?;
            match SetupBootstrapMessage::try_from(msg) {
                Ok(setup) => return Some(setup),
                Err(other) => {
                    // Non-setup frame during the setup window. The
                    // operational dispatcher would normally handle
                    // this, but during setup-phase wait loops the
                    // caller has narrowed its scope to setup frames
                    // only. Skip and log — see module docs for the
                    // rationale.
                    tracing::warn!(
                        kind = ?other.msg_type(),
                        "SetupBootstrap.recv dropped non-setup frame during setup window"
                    );
                }
            }
        }
    }
}

/// Primary-side adapter over the UNIFIED `Tr: PeerTransport<I>`.
/// Narrows the transport's mesh broadcast/recv to
/// [`SetupBootstrapMessage`] for the setup-phase window.
///
/// Same constructional / lifetime trade-off as the other two adapters:
/// build briefly, drop after the setup send/recv, let the operational
/// path keep using the underlying transport. The fan-out is
/// `Address::Broadcast(Scope::AllSecondaries)` (every peer of the
/// primary is a secondary, so this reaches the full fleet), and the
/// peer transport already collapses per-secondary delivery failures
/// into one `String` at the trait boundary (the per-secondary signal is
/// the heartbeat monitor, not the setup broadcast result).
pub struct PrimaryPeerSetupBootstrap<'a, Tr> {
    transport: &'a mut Tr,
}

impl<'a, Tr> PrimaryPeerSetupBootstrap<'a, Tr> {
    pub fn new(transport: &'a mut Tr) -> Self {
        Self { transport }
    }
}

impl<Tr, I> SetupBootstrapBroadcast<I> for PrimaryPeerSetupBootstrap<'_, Tr>
where
    I: Identifier,
    Tr: PeerTransport<I>,
{
    async fn broadcast(&mut self, msg: SetupBootstrapMessage) -> Result<(), String> {
        let wire: DistributedMessage<I> = msg.into();
        // `Scope::AllSecondaries` is the right scope for the primary's
        // PeerInfo fan-out: every shared-outgoing entry is a secondary
        // (the primary is not its own peer), and F2 needs exactly the
        // all-secondaries set. The peer transport's `send` default-impl
        // routes this to `broadcast`, whose `Result<(), String>` IS the
        // narrow trait shape — no partial-failure list to fold.
        self.transport
            .send(
                crate::Address::Broadcast(crate::Scope::AllSecondaries),
                wire,
            )
            .await
    }

    async fn recv(&mut self) -> Option<SetupBootstrapMessage> {
        loop {
            let msg = self.transport.recv_peer().await?;
            match SetupBootstrapMessage::try_from(msg) {
                Ok(setup) => return Some(setup),
                Err(other) => {
                    tracing::warn!(
                        kind = ?other.msg_type(),
                        "SetupBootstrapBroadcast.recv dropped non-setup frame during setup window"
                    );
                }
            }
        }
    }
}
