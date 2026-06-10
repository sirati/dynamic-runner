//! Inherent accessor methods on `DistributedMessage<I>`
//! (`sender_id`, `timestamp`, `msg_type`, and the Phase-C routing
//! `target`). Extracted from `distributed.rs` so the enum-shape file
//! stays focused on the wire variant declarations.

use crate::address::Destination;
use crate::messages::distributed::DistributedMessage;
use crate::messages::message_type::MessageType;

impl<I> DistributedMessage<I> {
    /// The Phase-C mesh routing target stamped on this frame, if any.
    ///
    /// `None` on a freshly-constructed frame; the egress edge stamps
    /// `Some(resolved)` via [`DistributedMessage::with_target`] /
    /// [`DistributedMessage::set_target`] once the coordinators are
    /// rewired. The receiving mesh-pump reads this to demux the frame to
    /// the right local role-slot (a pure role→slot table) WITHOUT a
    /// content classifier.
    pub fn target(&self) -> Option<&Destination> {
        match self {
            Self::SecondaryWelcome { target, .. }
            | Self::Entropy { target, .. }
            | Self::CertExchange { target, .. }
            | Self::PeerInfo { target, .. }
            | Self::InitialAssignment { target, .. }
            | Self::TaskRequest { target, .. }
            | Self::TaskAssignment { target, .. }
            | Self::TransferComplete { target, .. }
            | Self::StageFile { target, .. }
            | Self::RequestClusterSnapshot { target, .. }
            | Self::ClusterSnapshot { target, .. }
            | Self::RequestRunConfig { target, .. }
            | Self::RunConfig { target, .. }
            | Self::StateDigest { target, .. }
            | Self::MeshReady { target, .. }
            | Self::TaskComplete { target, .. }
            | Self::TaskFailed { target, .. }
            | Self::Keepalive { target, .. }
            | Self::TimeoutDetected { target, .. }
            | Self::TimeoutQuery { target, .. }
            | Self::TimeoutResponse { target, .. }
            | Self::PromotionVote { target, .. }
            | Self::PromotionConfirm { target, .. }
            | Self::SecondaryFatalError { target, .. }
            | Self::ClusterMutation { target, .. }
            | Self::Relay { target, .. }
            | Self::RelayBackoff { target, .. } => target.as_ref(),
        }
    }

    /// Stamp the resolved routing `target` on this frame IN PLACE.
    ///
    /// Called by the coordinator egress edge after resolving a
    /// [`Destination`] to its concrete host (the egress maps the
    /// role-erased `SendTarget` back to a role-bearing `Destination` and
    /// stamps it here). The mesh-pump reads it at ingress.
    pub fn set_target(&mut self, dst: Destination) {
        let slot = match self {
            Self::SecondaryWelcome { target, .. }
            | Self::Entropy { target, .. }
            | Self::CertExchange { target, .. }
            | Self::PeerInfo { target, .. }
            | Self::InitialAssignment { target, .. }
            | Self::TaskRequest { target, .. }
            | Self::TaskAssignment { target, .. }
            | Self::TransferComplete { target, .. }
            | Self::StageFile { target, .. }
            | Self::RequestClusterSnapshot { target, .. }
            | Self::ClusterSnapshot { target, .. }
            | Self::RequestRunConfig { target, .. }
            | Self::RunConfig { target, .. }
            | Self::StateDigest { target, .. }
            | Self::MeshReady { target, .. }
            | Self::TaskComplete { target, .. }
            | Self::TaskFailed { target, .. }
            | Self::Keepalive { target, .. }
            | Self::TimeoutDetected { target, .. }
            | Self::TimeoutQuery { target, .. }
            | Self::TimeoutResponse { target, .. }
            | Self::PromotionVote { target, .. }
            | Self::PromotionConfirm { target, .. }
            | Self::SecondaryFatalError { target, .. }
            | Self::ClusterMutation { target, .. }
            | Self::Relay { target, .. }
            | Self::RelayBackoff { target, .. } => target,
        };
        *slot = Some(dst);
    }

    /// Builder form of [`DistributedMessage::set_target`]: consume the
    /// frame, stamp `dst`, and return it. The egress edge uses whichever
    /// form fits its call shape; both stamp the same field.
    pub fn with_target(mut self, dst: Destination) -> Self {
        self.set_target(dst);
        self
    }

    /// Strip the routing `target` back to `None` IN PLACE.
    ///
    /// The `target` is the WIRE ENVELOPE's routing header: the egress stamps
    /// the resolved [`Destination`] so the receiving mesh-pump can demux the
    /// frame to the right local role-slot WITHOUT a content classifier. Once
    /// the pump has done that demux, the header has served its purpose — the
    /// APPLICATION frame the role's handler then sees is target-agnostic
    /// (every handler pattern-matches `target: None`, never a routed value).
    /// So the mesh-pump clears the header at the local-delivery boundary,
    /// keeping the routing concern entirely inside the mesh layer and the
    /// handlers oblivious to it. Idempotent on an already-`None` frame.
    pub fn clear_target(&mut self) {
        let slot = match self {
            Self::SecondaryWelcome { target, .. }
            | Self::Entropy { target, .. }
            | Self::CertExchange { target, .. }
            | Self::PeerInfo { target, .. }
            | Self::InitialAssignment { target, .. }
            | Self::TaskRequest { target, .. }
            | Self::TaskAssignment { target, .. }
            | Self::TransferComplete { target, .. }
            | Self::StageFile { target, .. }
            | Self::RequestClusterSnapshot { target, .. }
            | Self::ClusterSnapshot { target, .. }
            | Self::RequestRunConfig { target, .. }
            | Self::RunConfig { target, .. }
            | Self::StateDigest { target, .. }
            | Self::MeshReady { target, .. }
            | Self::TaskComplete { target, .. }
            | Self::TaskFailed { target, .. }
            | Self::Keepalive { target, .. }
            | Self::TimeoutDetected { target, .. }
            | Self::TimeoutQuery { target, .. }
            | Self::TimeoutResponse { target, .. }
            | Self::PromotionVote { target, .. }
            | Self::PromotionConfirm { target, .. }
            | Self::SecondaryFatalError { target, .. }
            | Self::ClusterMutation { target, .. }
            | Self::Relay { target, .. }
            | Self::RelayBackoff { target, .. } => target,
        };
        *slot = None;
    }

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
            | Self::RequestRunConfig { sender_id, .. }
            | Self::RunConfig { sender_id, .. }
            | Self::StateDigest { sender_id, .. }
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
            | Self::RequestRunConfig { timestamp, .. }
            | Self::RunConfig { timestamp, .. }
            | Self::StateDigest { timestamp, .. }
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

    /// Whether this frame carries a per-task TERMINAL report
    /// ([`DistributedMessage::TaskComplete`] /
    /// [`DistributedMessage::TaskFailed`]).
    ///
    /// "Terminal-bearing" is the classifier the secondary's reporting
    /// concern uses to decide whether a primary-bound send is REPLAYABLE
    /// on a no-route absorb: a `TaskComplete` / `TaskFailed` resolves a
    /// task's in-flight entry at the authority, so losing it strands the
    /// task forever (phantom-busy). It is the SINGLE source of that
    /// classification, owned by the enum so every site that gates by
    /// "does this report resolve a task?" reads one predicate. The
    /// backpressure-shaped `TaskFailed` (the deferred-lost reinject) IS
    /// terminal-bearing here — it too resolves an in-flight slot at the
    /// authority (a requeue), so it must replay across a no-route.
    ///
    /// Everything else through the primary-bound send chokepoint
    /// (`TaskRequest` capacity hints, `Keepalive`, `MeshReady`) is
    /// legitimately DROPPABLE — a missed one is re-emitted on the next
    /// tick — so it is NOT terminal-bearing.
    pub fn is_terminal_bearing(&self) -> bool {
        matches!(self, Self::TaskComplete { .. } | Self::TaskFailed { .. })
    }

    /// The per-task hash this frame resolves, for the
    /// [`DistributedMessage::TaskComplete`] /
    /// [`DistributedMessage::TaskFailed`] terminal variants; `None` for
    /// every other variant.
    ///
    /// Pairs with [`Self::is_terminal_bearing`]: the reporting concern
    /// reads it to LOG which task a retained / re-delivered terminal
    /// carries (the strand-diagnostic the no-route absorb was previously
    /// silent about).
    pub fn task_hash(&self) -> Option<&str> {
        match self {
            Self::TaskComplete { task_hash, .. } | Self::TaskFailed { task_hash, .. } => {
                Some(task_hash)
            }
            _ => None,
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
            Self::RequestRunConfig { .. } => MessageType::RequestRunConfig,
            Self::RunConfig { .. } => MessageType::RunConfig,
            Self::StateDigest { .. } => MessageType::StateDigest,
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
}
