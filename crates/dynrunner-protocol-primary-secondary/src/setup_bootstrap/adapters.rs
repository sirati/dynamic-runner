//! Adapter structs that wrap the existing
//! `MessageSender + MessageReceiver` / [`SecondaryTransport`] surface
//! and narrow it to [`SetupBootstrapMessage`] for the setup-phase
//! window.
//!
//! - [`SecondarySetupBootstrap`] — secondary side, borrows the same
//!   underlying primary-bound transport the operational path uses.
//! - [`PrimarySetupBootstrap`] — primary side, borrows the
//!   `SecondaryTransport` fan-out the operational path uses.
//!
//! The `format_partial_failures` helper at the bottom renders the
//! structured per-secondary failure list (which the underlying
//! broadcast surfaces) into a single `String` summary for the trait
//! shape.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};

use crate::{DistributedMessage, PeerTransport, SecondaryTransport};
use crate::setup_bootstrap::message::SetupBootstrapMessage;
use crate::setup_bootstrap::trait_defs::{SetupBootstrap, SetupBootstrapBroadcast};

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

/// Primary-side adapter: wraps a `&mut T: SecondaryTransport<I>` and
/// narrows its broadcast/recv to [`SetupBootstrapMessage`].
///
/// The same constructional / lifetime trade-off as
/// [`SecondarySetupBootstrap`] applies — build briefly, drop after the
/// setup send/recv, let the operational path keep using the underlying
/// transport.
pub struct PrimarySetupBootstrap<'a, T> {
    transport: &'a mut T,
}

impl<'a, T> PrimarySetupBootstrap<'a, T> {
    pub fn new(transport: &'a mut T) -> Self {
        Self { transport }
    }
}

impl<T, I> SetupBootstrapBroadcast<I> for PrimarySetupBootstrap<'_, T>
where
    I: Identifier,
    T: SecondaryTransport<I>,
{
    async fn broadcast(&mut self, msg: SetupBootstrapMessage) -> Result<(), String> {
        let wire: DistributedMessage<I> = msg.into();
        // The underlying `SecondaryTransport::broadcast` preserves the
        // structured per-secondary failure list; we walk it here to
        // emit the same per-secondary warn breadcrumbs the
        // pre-Step-10 `send_peer_lists` emitted (preserving the
        // structured key-value log shape that log aggregators
        // consume) before folding the list into the single-String
        // summary the trait shape exposes. This keeps the operator-
        // visible log line shape identical across the refactor while
        // still exposing the count/summary upstream.
        match self.transport.broadcast(wire).await {
            Ok(()) => Ok(()),
            Err(failures) => {
                for (secondary_id, error) in &failures {
                    tracing::warn!(
                        secondary = %secondary_id,
                        error = %error,
                        "setup bootstrap broadcast: per-secondary delivery failed"
                    );
                }
                Err(format_partial_failures(&failures))
            }
        }
    }

    async fn recv(&mut self) -> Option<SetupBootstrapMessage> {
        loop {
            let msg = self.transport.recv().await?;
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

/// Primary-side adapter over the UNIFIED `Tr: PeerTransport<I>` —
/// the post-collapse sibling of [`PrimarySetupBootstrap`]. Narrows the
/// transport's mesh broadcast/recv to [`SetupBootstrapMessage`] for the
/// setup-phase window.
///
/// Same constructional / lifetime trade-off as the other two adapters:
/// build briefly, drop after the setup send/recv, let the operational
/// path keep using the underlying transport. The fan-out is
/// `Address::Broadcast(Scope::AllSecondaries)` (every peer of the
/// primary is a secondary, so this reaches the full fleet), and the
/// peer transport already collapses per-secondary delivery failures
/// into one `String` — so no `format_partial_failures` walk is needed
/// here (the per-secondary signal is the heartbeat monitor, not the
/// setup broadcast result).
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

/// Render a per-secondary partial-failure list as a compact summary
/// string. The structured form
/// (`Vec<(secondary_id, error_message)>`) lives on
/// [`SecondaryTransport::broadcast`]'s return; the narrow
/// [`SetupBootstrapBroadcast::broadcast`] surface collapses it to a
/// String at the trait boundary. Callers that need per-peer diagnostics
/// should use the underlying [`SecondaryTransport`] directly — those
/// callers (heartbeat, keepalive, …) are explicitly NOT setup-phase
/// and have no business going through this adapter.
fn format_partial_failures(failures: &[(String, String)]) -> String {
    let count = failures.len();
    let summary = failures
        .iter()
        .map(|(id, err)| format!("{id}={err}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("setup bootstrap broadcast: {count} secondaries failed: {summary}")
}
