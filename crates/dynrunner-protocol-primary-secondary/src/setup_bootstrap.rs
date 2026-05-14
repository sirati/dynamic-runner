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

use dynrunner_core::{Identifier, MessageReceiver, MessageSender, ResourceAmount};

use crate::{DistributedMessage, PeerConnectionInfo, SecondaryTransport};

/// The three setup-phase wire frames the bootstrap channel handles.
///
/// Each variant carries **exactly** the same fields as the corresponding
/// [`DistributedMessage`] variant — see [`From`] /
/// [`TryFrom`] for the lossless conversions. The duplicated shape is
/// intentional: the narrow enum is the API gate that prevents callers
/// from sending runtime traffic through the bootstrap path. The wire
/// representation is identical, so the legacy transport on the other
/// side decodes the byte stream into a [`DistributedMessage`] just like
/// before — nothing changes on the wire.
///
/// # Why not generic over `I`?
///
/// Unlike [`DistributedMessage<I>`] which threads `I` through task-
/// related variants ([`DistributedMessage::TaskRequest`],
/// [`DistributedMessage::InitialAssignment`], …), the three setup-phase
/// frames carry **no** identifier-typed payload — they negotiate
/// connection-level metadata (id, cert, addresses, observer flag, peer
/// list). The conversion impls below parametrize `I` at the impl level
/// so the wire-shape on the other side stays `DistributedMessage<I>`,
/// but the bootstrap enum itself doesn't need the parameter. Adding
/// `PhantomData<I>` would compile but adds noise without buying any
/// type-safety the conversion impls don't already provide.
///
/// # Why not `Box<DistributedMessage>` with a runtime tag?
///
/// A runtime-tagged subset would compile, but it would also let a
/// caller smuggle a `DistributedMessage::TaskRequest` into the field
/// and rely on the conversion failing at the boundary. That's a
/// runtime check, not a structural guarantee. The whole point of the
/// Step 10 refactor is the **type-level** prohibition: the compiler
/// rejects `SetupBootstrapMessage::TaskRequest` at the call site.
#[derive(Debug, Clone)]
pub enum SetupBootstrapMessage {
    /// Secondary → primary: "I am `secondary_id`, here are my
    /// resources / worker count / observer flag." First frame the
    /// secondary sends after the underlying transport accepts the
    /// connection. Mirrors [`DistributedMessage::SecondaryWelcome`].
    SecondaryWelcome {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        resources: Vec<ResourceAmount>,
        worker_count: u32,
        hostname: String,
        is_observer: bool,
    },
    /// Secondary → primary: "Here is my peer-mesh public cert + the
    /// addresses I'm reachable on." Sent immediately after
    /// [`Self::SecondaryWelcome`]. Mirrors
    /// [`DistributedMessage::CertExchange`].
    CertExchange {
        sender_id: String,
        timestamp: f64,
        secondary_id: String,
        public_cert_pem: String,
        ipv4_address: Option<String>,
        ipv6_address: Option<String>,
        quic_port: u16,
    },
    /// Primary → all secondaries (broadcast): "Here is the full peer
    /// list — every secondary's id + cert + addresses + observer
    /// flag." Receiving secondaries dial each entry to form the peer
    /// mesh. Mirrors [`DistributedMessage::PeerInfo`].
    PeerInfo {
        sender_id: String,
        timestamp: f64,
        peers: Vec<PeerConnectionInfo>,
    },
    // NOTE: Do NOT add a fourth variant. Runtime messaging belongs on
    // `PeerTransport` (Address::Peer / Address::Role / Address::Broadcast).
    // The very narrowness of this enum is the architectural guarantee.
}

/// Conversion to the existing wire-shape. Total — every
/// [`SetupBootstrapMessage`] variant maps to exactly one
/// [`DistributedMessage`] variant with the same fields. The `I`
/// parameter on [`DistributedMessage`] is constrained at the impl
/// level so wire-shape compatibility is preserved end-to-end without
/// `SetupBootstrapMessage` itself needing the parameter.
impl<I> From<SetupBootstrapMessage> for DistributedMessage<I> {
    fn from(msg: SetupBootstrapMessage) -> Self {
        match msg {
            SetupBootstrapMessage::SecondaryWelcome {
                sender_id,
                timestamp,
                secondary_id,
                resources,
                worker_count,
                hostname,
                is_observer,
            } => DistributedMessage::SecondaryWelcome {
                sender_id,
                timestamp,
                secondary_id,
                resources,
                worker_count,
                hostname,
                is_observer,
            },
            SetupBootstrapMessage::CertExchange {
                sender_id,
                timestamp,
                secondary_id,
                public_cert_pem,
                ipv4_address,
                ipv6_address,
                quic_port,
            } => DistributedMessage::CertExchange {
                sender_id,
                timestamp,
                secondary_id,
                public_cert_pem,
                ipv4_address,
                ipv6_address,
                quic_port,
            },
            SetupBootstrapMessage::PeerInfo {
                sender_id,
                timestamp,
                peers,
            } => DistributedMessage::PeerInfo {
                sender_id,
                timestamp,
                peers,
            },
        }
    }
}

/// Reverse conversion: partial — only the three setup variants
/// succeed. Any other [`DistributedMessage`] variant is returned via
/// `Err` so the caller can route it to the operational channel rather
/// than misinterpreting it as a setup frame.
///
/// This is what makes [`SetupBootstrap::recv`] / [`SetupBootstrapBroadcast::recv`]
/// safe in the face of an underlying wire that may carry interleaved
/// non-setup frames: the adapter filters; non-matching frames are
/// surfaced as `Err(msg)` and the adapter logs + drops them at the
/// recv boundary. Operational frames during the setup window are
/// extremely rare (the setup phase completes in milliseconds), but the
/// type system shouldn't ask the caller to trust that.
impl<I> TryFrom<DistributedMessage<I>> for SetupBootstrapMessage {
    type Error = DistributedMessage<I>;

    fn try_from(msg: DistributedMessage<I>) -> Result<Self, Self::Error> {
        match msg {
            DistributedMessage::SecondaryWelcome {
                sender_id,
                timestamp,
                secondary_id,
                resources,
                worker_count,
                hostname,
                is_observer,
            } => Ok(SetupBootstrapMessage::SecondaryWelcome {
                sender_id,
                timestamp,
                secondary_id,
                resources,
                worker_count,
                hostname,
                is_observer,
            }),
            DistributedMessage::CertExchange {
                sender_id,
                timestamp,
                secondary_id,
                public_cert_pem,
                ipv4_address,
                ipv6_address,
                quic_port,
            } => Ok(SetupBootstrapMessage::CertExchange {
                sender_id,
                timestamp,
                secondary_id,
                public_cert_pem,
                ipv4_address,
                ipv6_address,
                quic_port,
            }),
            DistributedMessage::PeerInfo {
                sender_id,
                timestamp,
                peers,
            } => Ok(SetupBootstrapMessage::PeerInfo {
                sender_id,
                timestamp,
                peers,
            }),
            other => Err(other),
        }
    }
}

/// Secondary-side bootstrap channel: a 1:1 wire to the primary.
///
/// The secondary's setup sequence is:
///   1. `send(SecondaryWelcome{...})`
///   2. `send(CertExchange{...})`
///   3. `recv()` until [`SetupBootstrapMessage::PeerInfo`] arrives
///
/// After step 3 the bootstrap retires and operational messaging takes
/// over on [`PeerTransport`]. The trait method count stays at two; if
/// a future change wants to thread additional setup-phase frames
/// through here, the right move is to add a [`SetupBootstrapMessage`]
/// variant — not a new trait method — so the narrow-type guarantee
/// stays load-bearing.
///
/// [`PeerTransport`]: crate::PeerTransport
pub trait SetupBootstrap<I: Identifier> {
    /// Send one setup-phase frame to the primary.
    fn send(
        &mut self,
        msg: SetupBootstrapMessage,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next setup-phase frame from the primary, or `None`
    /// if the underlying wire closed.
    ///
    /// Non-setup frames that arrive during the setup window (extremely
    /// rare — the setup phase completes in milliseconds) are logged at
    /// `warn` and skipped; the next call returns the next setup-eligible
    /// frame. The caller does not see operational traffic through this
    /// surface — that's the structural guarantee Step 10 enforces.
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Option<SetupBootstrapMessage>>;
}

/// Primary-side bootstrap channel: a 1:N fan-out to every connected
/// secondary plus a recv multiplexed across them.
///
/// The primary's setup sequence is:
///   - `recv()` until every secondary has emitted `SecondaryWelcome` +
///     `CertExchange` (orchestrated by
///     `dynrunner_manager_distributed::primary::connect::wait_for_connections`)
///   - `broadcast(PeerInfo{...})` once enough secondaries have completed
///     cert exchange (orchestrated by
///     `dynrunner_manager_distributed::primary::peer_setup::send_peer_lists`)
///
/// Asymmetric to [`SetupBootstrap`] because the primary doesn't
/// per-secondary unicast at setup — `PeerInfo` is a fan-out, and
/// `SecondaryWelcome` / `CertExchange` arrive from N secondaries onto
/// the same recv loop.
pub trait SetupBootstrapBroadcast<I: Identifier> {
    /// Fan-out a setup-phase frame to every connected secondary.
    /// Partial-failure summary is preserved by the underlying
    /// [`SecondaryTransport::broadcast`]; the adapter folds it into a
    /// `String` for the trait shape.
    fn broadcast(
        &mut self,
        msg: SetupBootstrapMessage,
    ) -> impl std::future::Future<Output = Result<(), String>>;

    /// Receive the next setup-phase frame from any connected secondary,
    /// or `None` if the underlying wire closed. Non-setup frames are
    /// logged at `warn` and skipped (same semantics as
    /// [`SetupBootstrap::recv`]).
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Option<SetupBootstrapMessage>>;
}

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

#[cfg(test)]
mod tests {
    use super::*;

    // Sanity that the bidirectional conversion is lossless and that
    // any non-setup variant fails the TryFrom — this is the
    // structural invariant the whole module rests on.

    #[test]
    fn welcome_roundtrip() {
        let original: SetupBootstrapMessage = SetupBootstrapMessage::SecondaryWelcome {
            sender_id: "s0".into(),
            timestamp: 1.0,
            secondary_id: "s0".into(),
            resources: Vec::new(),
            worker_count: 2,
            hostname: "host".into(),
            is_observer: false,
        };
        let wire: DistributedMessage<()> = original.clone().into();
        let back: SetupBootstrapMessage = wire.try_into().expect("setup variant");
        match (original, back) {
            (
                SetupBootstrapMessage::SecondaryWelcome {
                    secondary_id: a,
                    worker_count: aw,
                    is_observer: ai,
                    ..
                },
                SetupBootstrapMessage::SecondaryWelcome {
                    secondary_id: b,
                    worker_count: bw,
                    is_observer: bi,
                    ..
                },
            ) => {
                assert_eq!(a, b);
                assert_eq!(aw, bw);
                assert_eq!(ai, bi);
            }
            _ => panic!("variant changed across roundtrip"),
        }
    }

    #[test]
    fn cert_exchange_roundtrip() {
        let original: SetupBootstrapMessage = SetupBootstrapMessage::CertExchange {
            sender_id: "s0".into(),
            timestamp: 2.0,
            secondary_id: "s0".into(),
            public_cert_pem: "PEM".into(),
            ipv4_address: Some("10.0.0.1".into()),
            ipv6_address: None,
            quic_port: 4242,
        };
        let wire: DistributedMessage<()> = original.into();
        match wire {
            DistributedMessage::CertExchange {
                public_cert_pem,
                ipv4_address,
                ipv6_address,
                quic_port,
                ..
            } => {
                assert_eq!(public_cert_pem, "PEM");
                assert_eq!(ipv4_address.as_deref(), Some("10.0.0.1"));
                assert!(ipv6_address.is_none());
                assert_eq!(quic_port, 4242);
            }
            _ => panic!("converted to wrong wire variant"),
        }
    }

    #[test]
    fn peer_info_roundtrip() {
        let original: SetupBootstrapMessage = SetupBootstrapMessage::PeerInfo {
            sender_id: "primary".into(),
            timestamp: 3.0,
            peers: vec![PeerConnectionInfo {
                secondary_id: "s0".into(),
                cert: "PEM".into(),
                ipv4: None,
                ipv6: None,
                port: 0,
                is_observer: false,
            }],
        };
        let wire: DistributedMessage<()> = original.into();
        let back: SetupBootstrapMessage = wire.try_into().expect("setup variant");
        match back {
            SetupBootstrapMessage::PeerInfo { peers, .. } => {
                assert_eq!(peers.len(), 1);
                assert_eq!(peers[0].secondary_id, "s0");
            }
            _ => panic!("variant changed across roundtrip"),
        }
    }

    #[test]
    fn non_setup_variant_rejected() {
        // Any non-setup variant must Err out at the TryFrom boundary.
        // Pick `Keepalive` — the canonical runtime frame and the one
        // most likely to race a setup-phase recv in practice.
        let runtime: DistributedMessage<()> = DistributedMessage::Keepalive {
            sender_id: "s0".into(),
            timestamp: 4.0,
            secondary_id: "s0".into(),
            active_workers: 0,
        };
        let result: Result<SetupBootstrapMessage, DistributedMessage<()>> = runtime.try_into();
        assert!(
            result.is_err(),
            "Keepalive must not convert to SetupBootstrapMessage"
        );
    }
}
