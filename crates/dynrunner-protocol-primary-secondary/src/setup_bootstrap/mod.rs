//! Setup-phase bootstrap channel.
//!
//! # What this module gives the rest of the workspace
//!
//! Step 10 of the transport-unification refactor (per
//! `rosy-weaving-cascade.md`, Decision D). The legacy
//! [`SecondaryTransport`] trait — together with the
//! `MessageSender + MessageReceiver` shape that today's secondary holds
//! for its submitter-bound channel (formerly a marker-trait
//! `PrimaryTransport`, retired in Step 11) — served two distinct
//! purposes:
//!   1. **Bootstrap channel** for the setup-phase frames
//!      ([`DistributedMessage::SecondaryWelcome`],
//!      [`DistributedMessage::CertExchange`],
//!      [`DistributedMessage::PeerInfo`]) — these flow before the peer
//!      mesh exists, because the cert exchange is what *establishes*
//!      it.
//!   2. **Runtime communication channel** for everything else
//!      (TaskRequest, ClusterMutation, Keepalive, …). The unification
//!      refactor has been steadily migrating this leg to
//!      [`PeerTransport`] since Step 5.
//!
//! This file isolates concern (1) into its own dedicated transport
//! surface. The narrow [`SetupBootstrapMessage`] enum carries exactly
//! the three setup-phase variants; the [`SetupBootstrap`] /
//! [`SetupBootstrapBroadcast`] traits expose `send` / `broadcast` /
//! `recv` over that narrow type. **Anyone reaching for the trait for
//! runtime messaging is structurally blocked** — there is no
//! `SetupBootstrapMessage::TaskRequest`, no
//! `SetupBootstrapMessage::ClusterMutation`. Adding a fourth variant
//! is the design smell that says "use [`PeerTransport`] instead".
//!
//! # Wire compatibility
//!
//! `SetupBootstrapMessage` is **not** a separate serde shape. Sending a
//! [`SetupBootstrapMessage::SecondaryWelcome`] travels over the wire as
//! a [`DistributedMessage::SecondaryWelcome`] — the field layout is
//! identical and the adapter performs an infallible
//! [`From`] conversion before handing the frame to the underlying
//! transport. Receivers see the same byte sequence today's primary /
//! secondary already emits; the on-the-wire format is unchanged. This
//! is the load-bearing invariant Step 10 must preserve so the
//! setup-promote discriminator tests in
//! `crates/dynrunner-manager-distributed/src/{primary,secondary}/tests.rs`
//! keep passing unmodified.
//!
//! # Implementation pattern
//!
//! Step 10 does not rewrite the underlying connection. The same
//! per-secondary writer / inbound channel today's
//! [`SecondaryTransport`] (primary side) / `MessageSender +
//! MessageReceiver` (secondary side) already owns gets a
//! **narrower-typed view** via [`SecondarySetupBootstrap`] /
//! [`PrimarySetupBootstrap`]. The adapter wraps a `&mut T` of the
//! existing transport, converts between [`SetupBootstrapMessage`] and
//! [`DistributedMessage`] at the API boundary, and forwards. This
//! mirrors the [`TunneledPeerTransport`] pattern from Step 5b (same
//! wire, narrower API).
//!
//! [`PeerTransport`]: crate::PeerTransport
//! [`SecondaryTransport`]: crate::SecondaryTransport
//! [`TunneledPeerTransport`]: ../../../dynrunner-transport-tunnel/index.html
//! [`DistributedMessage`]: crate::DistributedMessage


pub mod adapters;
pub mod message;
pub mod trait_defs;

#[cfg(test)]
mod tests;

pub use adapters::{
    PrimaryPeerSetupBootstrap, PrimarySetupBootstrap, SecondarySetupBootstrap,
};
pub use message::SetupBootstrapMessage;
pub use trait_defs::{SetupBootstrap, SetupBootstrapBroadcast};
