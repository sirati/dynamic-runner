//! `SetupBootstrapMessage` — the narrow enum carrying exactly the
//! three setup-phase wire frames (`SecondaryWelcome`,
//! `CertExchange`, `PeerInfo`) plus its lossless conversions
//! to/from [`DistributedMessage<I>`].

use dynrunner_core::ResourceAmount;

use crate::{DistributedMessage, PeerConnectionInfo};

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
        /// Primary-capability marker — twin of `is_observer`. See
        /// [`DistributedMessage::SecondaryWelcome::can_be_primary`].
        can_be_primary: bool,
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
    // `PeerTransport` (`send_to_peer` / `broadcast`, addressed by the
    // typed `Destination` at the coordinator edge). The very narrowness
    // of this enum is the architectural guarantee.
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
                can_be_primary,
            } => DistributedMessage::SecondaryWelcome {
                sender_id,
                timestamp,
                secondary_id,
                resources,
                worker_count,
                hostname,
                is_observer,
                can_be_primary,
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
                can_be_primary,
            } => Ok(SetupBootstrapMessage::SecondaryWelcome {
                sender_id,
                timestamp,
                secondary_id,
                resources,
                worker_count,
                hostname,
                is_observer,
                can_be_primary,
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
