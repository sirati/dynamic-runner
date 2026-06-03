//! Inherent accessor methods on `DistributedMessage<I>`
//! (`sender_id`, `timestamp`, `msg_type`). Extracted from
//! `distributed.rs` so the enum-shape file stays focused on the wire
//! variant declarations.

use crate::messages::distributed::DistributedMessage;
use crate::messages::message_type::MessageType;

impl<I> DistributedMessage<I> {
    pub fn sender_id(&self) -> &str {
        match self {
            Self::SecondaryWelcome { sender_id, .. }
            | Self::Entropy { sender_id, .. }
            | Self::CertExchange { sender_id, .. }
            | Self::PeerInfo { sender_id, .. }
            | Self::InitialAssignment { sender_id, .. }
            | Self::TaskRequest { sender_id, .. }
            | Self::TaskAssignment { sender_id, .. }
            | Self::TransferComplete { sender_id, .. }
            | Self::StageFile { sender_id, .. }
            | Self::RequestClusterSnapshot { sender_id, .. }
            | Self::ClusterSnapshot { sender_id, .. }
            | Self::MeshReady { sender_id, .. }
            | Self::TaskComplete { sender_id, .. }
            | Self::TaskFailed { sender_id, .. }
            | Self::Keepalive { sender_id, .. }
            | Self::TimeoutDetected { sender_id, .. }
            | Self::TimeoutQuery { sender_id, .. }
            | Self::TimeoutResponse { sender_id, .. }
            | Self::PromotionVote { sender_id, .. }
            | Self::PromotionConfirm { sender_id, .. }
            | Self::SecondaryFatalError { sender_id, .. }
            | Self::ClusterMutation { sender_id, .. }
            | Self::Relay { sender_id, .. }
            | Self::RelayBackoff { sender_id, .. } => sender_id,
        }
    }

    pub fn timestamp(&self) -> f64 {
        match self {
            Self::SecondaryWelcome { timestamp, .. }
            | Self::Entropy { timestamp, .. }
            | Self::CertExchange { timestamp, .. }
            | Self::PeerInfo { timestamp, .. }
            | Self::InitialAssignment { timestamp, .. }
            | Self::TaskRequest { timestamp, .. }
            | Self::TaskAssignment { timestamp, .. }
            | Self::TransferComplete { timestamp, .. }
            | Self::StageFile { timestamp, .. }
            | Self::RequestClusterSnapshot { timestamp, .. }
            | Self::ClusterSnapshot { timestamp, .. }
            | Self::MeshReady { timestamp, .. }
            | Self::TaskComplete { timestamp, .. }
            | Self::TaskFailed { timestamp, .. }
            | Self::Keepalive { timestamp, .. }
            | Self::TimeoutDetected { timestamp, .. }
            | Self::TimeoutQuery { timestamp, .. }
            | Self::TimeoutResponse { timestamp, .. }
            | Self::PromotionVote { timestamp, .. }
            | Self::PromotionConfirm { timestamp, .. }
            | Self::SecondaryFatalError { timestamp, .. }
            | Self::ClusterMutation { timestamp, .. }
            | Self::Relay { timestamp, .. }
            | Self::RelayBackoff { timestamp, .. } => *timestamp,
        }
    }

    pub fn msg_type(&self) -> MessageType {
        match self {
            Self::SecondaryWelcome { .. } => MessageType::SecondaryWelcome,
            Self::Entropy { .. } => MessageType::Entropy,
            Self::CertExchange { .. } => MessageType::CertExchange,
            Self::PeerInfo { .. } => MessageType::PeerInfo,
            Self::InitialAssignment { .. } => MessageType::InitialAssignment,
            Self::TaskRequest { .. } => MessageType::TaskRequest,
            Self::TaskAssignment { .. } => MessageType::TaskAssignment,
            Self::TransferComplete { .. } => MessageType::TransferComplete,
            Self::StageFile { .. } => MessageType::StageFile,
            Self::RequestClusterSnapshot { .. } => MessageType::RequestClusterSnapshot,
            Self::ClusterSnapshot { .. } => MessageType::ClusterSnapshot,
            Self::MeshReady { .. } => MessageType::MeshReady,
            Self::TaskComplete { .. } => MessageType::TaskComplete,
            Self::TaskFailed { .. } => MessageType::TaskFailed,
            Self::Keepalive { .. } => MessageType::Keepalive,
            Self::TimeoutDetected { .. } => MessageType::TimeoutDetected,
            Self::TimeoutQuery { .. } => MessageType::TimeoutQuery,
            Self::TimeoutResponse { .. } => MessageType::TimeoutResponse,
            Self::PromotionVote { .. } => MessageType::PromotionVote,
            Self::PromotionConfirm { .. } => MessageType::PromotionConfirm,
            Self::SecondaryFatalError { .. } => MessageType::SecondaryFatalError,
            Self::ClusterMutation { .. } => MessageType::ClusterMutation,
            Self::Relay { .. } => MessageType::RelayMessage,
            Self::RelayBackoff { .. } => MessageType::RelayBackoff,
        }
    }

    /// Whether this frame is consumed by the AUTHORITATIVE (primary)
    /// role's dispatch rather than the follower-secondary's.
    ///
    /// Single source of truth for the role-aware inbound demux a
    /// co-located primary+secondary node performs: when this node holds
    /// `Role::Primary`, a frame for which this returns `true` (a remote
    /// secondary's worker-lifecycle / setup report — the frames the
    /// `PrimaryCoordinator`'s `dispatch_message` owns) routes to the
    /// co-located PRIMARY; everything else (peer keepalive, CRDT mirror,
    /// election, role/relay envelopes) routes to the SECONDARY. As a
    /// follower this classifier is not consulted — every frame goes to
    /// the secondary.
    ///
    /// The set mirrors the `PrimaryCoordinator`'s `dispatch_message`
    /// match arms exactly:
    ///   - `SecondaryWelcome` / `CertExchange` — connection setup the
    ///     authority owns.
    ///   - `TaskRequest` / `TaskComplete` / `TaskFailed` — the
    ///     worker-lifecycle reports the authority attributes + accounts.
    ///   - `MeshReady` — the per-secondary mesh-settled signal the
    ///     authority gates promotion on.
    ///   - `SecondaryFatalError` — the authority's fleet-death handler.
    ///
    /// `ClusterMutation` and `Keepalive` are DELIBERATELY excluded —
    /// they are consumed by BOTH roles (the authority applies/tracks,
    /// the follower mirrors), so they always route to the secondary
    /// (which mirrors the CRDT for every node) and the co-located
    /// primary observes the same mutations through its own mesh-member
    /// broadcast receipt. `RequestClusterSnapshot` is the
    /// secondary-side snapshot responder's concern (it serves the full
    /// replicated CRDT it mirrors), so it also stays with the secondary.
    pub fn is_primary_facing(&self) -> bool {
        matches!(
            self,
            Self::SecondaryWelcome { .. }
                | Self::CertExchange { .. }
                | Self::TaskRequest { .. }
                | Self::TaskComplete { .. }
                | Self::TaskFailed { .. }
                | Self::MeshReady { .. }
                | Self::SecondaryFatalError { .. }
        )
    }
}
