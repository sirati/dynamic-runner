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
            | Self::PromotePrimary { sender_id, .. }
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
            | Self::RelayBackoff { sender_id, .. }
            | Self::RoleAddressed { sender_id, .. }
            | Self::RoleMisaddressHint { sender_id, .. } => sender_id,
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
            | Self::PromotePrimary { timestamp, .. }
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
            | Self::RelayBackoff { timestamp, .. }
            | Self::RoleAddressed { timestamp, .. }
            | Self::RoleMisaddressHint { timestamp, .. } => *timestamp,
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
            Self::PromotePrimary { .. } => MessageType::PromotePrimary,
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
            Self::RoleAddressed { .. } => MessageType::RoleAddressed,
            Self::RoleMisaddressHint { .. } => MessageType::RoleMisaddressHint,
        }
    }
}
