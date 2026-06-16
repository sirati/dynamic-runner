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
            | Self::RequestSnapshotStream { target, .. }
            | Self::SnapshotStreamPackage { target, .. }
            | Self::RequestRunConfig { target, .. }
            | Self::RunConfig { target, .. }
            | Self::StateDigest { target, .. }
            | Self::PullProbe { target, .. }
            | Self::PullProbeReply { target, .. }
            | Self::PullFail { target, .. }
            | Self::MeshReady { target, .. }
            | Self::GracefulAbortRequest { target, .. }
            | Self::TaskComplete { target, .. }
            | Self::TaskFailed { target, .. }
            | Self::TerminalAck { target, .. }
            | Self::CustomMessage { target, .. }
            | Self::TaskHoldQuery { target, .. }
            | Self::TaskHoldResponse { target, .. }
            | Self::RespawnSpawnRequest { target, .. }
            | Self::RespawnSpawnResult { target, .. }
            | Self::RespawnRevokeRequest { target, .. }
            | Self::RespawnRevokeResult { target, .. }
            | Self::Keepalive { target, .. }
            | Self::TimeoutDetected { target, .. }
            | Self::TimeoutQuery { target, .. }
            | Self::TimeoutResponse { target, .. }
            | Self::PromotionVote { target, .. }
            | Self::PromotionConfirm { target, .. }
            | Self::SecondaryFatalError { target, .. }
            | Self::ClusterMutation { target, .. }
            | Self::Relay { target, .. }
            | Self::RelayBackoff { target, .. }
            | Self::RedialRequest { target, .. }
            | Self::FrameChunk { target, .. }
            | Self::SetupAssignment { target, .. }
            | Self::SetupTerminal { target, .. }
            | Self::IllegallyAssignedToNonidleWorker { target, .. }
            | Self::RequestInFlightRoster { target, .. }
            | Self::InFlightRoster { target, .. }
            | Self::WithdrawTask { target, .. }
            | Self::SuspectPeers { target, .. }
            | Self::ResolvedPeer { target, .. }
            | Self::RestartRequest { target, .. }
            | Self::RestartConfirm { target, .. }
            | Self::PeerProbe { target, .. }
            | Self::PeerProbeAck { target, .. } => target.as_ref(),
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
            | Self::RequestSnapshotStream { target, .. }
            | Self::SnapshotStreamPackage { target, .. }
            | Self::RequestRunConfig { target, .. }
            | Self::RunConfig { target, .. }
            | Self::StateDigest { target, .. }
            | Self::PullProbe { target, .. }
            | Self::PullProbeReply { target, .. }
            | Self::PullFail { target, .. }
            | Self::MeshReady { target, .. }
            | Self::GracefulAbortRequest { target, .. }
            | Self::TaskComplete { target, .. }
            | Self::TaskFailed { target, .. }
            | Self::TerminalAck { target, .. }
            | Self::CustomMessage { target, .. }
            | Self::TaskHoldQuery { target, .. }
            | Self::TaskHoldResponse { target, .. }
            | Self::RespawnSpawnRequest { target, .. }
            | Self::RespawnSpawnResult { target, .. }
            | Self::RespawnRevokeRequest { target, .. }
            | Self::RespawnRevokeResult { target, .. }
            | Self::Keepalive { target, .. }
            | Self::TimeoutDetected { target, .. }
            | Self::TimeoutQuery { target, .. }
            | Self::TimeoutResponse { target, .. }
            | Self::PromotionVote { target, .. }
            | Self::PromotionConfirm { target, .. }
            | Self::SecondaryFatalError { target, .. }
            | Self::ClusterMutation { target, .. }
            | Self::Relay { target, .. }
            | Self::RelayBackoff { target, .. }
            | Self::RedialRequest { target, .. }
            | Self::FrameChunk { target, .. }
            | Self::SetupAssignment { target, .. }
            | Self::SetupTerminal { target, .. }
            | Self::IllegallyAssignedToNonidleWorker { target, .. }
            | Self::RequestInFlightRoster { target, .. }
            | Self::InFlightRoster { target, .. }
            | Self::WithdrawTask { target, .. }
            | Self::SuspectPeers { target, .. }
            | Self::ResolvedPeer { target, .. }
            | Self::RestartRequest { target, .. }
            | Self::RestartConfirm { target, .. }
            | Self::PeerProbe { target, .. }
            | Self::PeerProbeAck { target, .. } => target,
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
            | Self::RequestSnapshotStream { target, .. }
            | Self::SnapshotStreamPackage { target, .. }
            | Self::RequestRunConfig { target, .. }
            | Self::RunConfig { target, .. }
            | Self::StateDigest { target, .. }
            | Self::PullProbe { target, .. }
            | Self::PullProbeReply { target, .. }
            | Self::PullFail { target, .. }
            | Self::MeshReady { target, .. }
            | Self::GracefulAbortRequest { target, .. }
            | Self::TaskComplete { target, .. }
            | Self::TaskFailed { target, .. }
            | Self::TerminalAck { target, .. }
            | Self::CustomMessage { target, .. }
            | Self::TaskHoldQuery { target, .. }
            | Self::TaskHoldResponse { target, .. }
            | Self::RespawnSpawnRequest { target, .. }
            | Self::RespawnSpawnResult { target, .. }
            | Self::RespawnRevokeRequest { target, .. }
            | Self::RespawnRevokeResult { target, .. }
            | Self::Keepalive { target, .. }
            | Self::TimeoutDetected { target, .. }
            | Self::TimeoutQuery { target, .. }
            | Self::TimeoutResponse { target, .. }
            | Self::PromotionVote { target, .. }
            | Self::PromotionConfirm { target, .. }
            | Self::SecondaryFatalError { target, .. }
            | Self::ClusterMutation { target, .. }
            | Self::Relay { target, .. }
            | Self::RelayBackoff { target, .. }
            | Self::RedialRequest { target, .. }
            | Self::FrameChunk { target, .. }
            | Self::SetupAssignment { target, .. }
            | Self::SetupTerminal { target, .. }
            | Self::IllegallyAssignedToNonidleWorker { target, .. }
            | Self::RequestInFlightRoster { target, .. }
            | Self::InFlightRoster { target, .. }
            | Self::WithdrawTask { target, .. }
            | Self::SuspectPeers { target, .. }
            | Self::ResolvedPeer { target, .. }
            | Self::RestartRequest { target, .. }
            | Self::RestartConfirm { target, .. }
            | Self::PeerProbe { target, .. }
            | Self::PeerProbeAck { target, .. } => target,
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
            | Self::RequestSnapshotStream { sender_id, .. }
            | Self::SnapshotStreamPackage { sender_id, .. }
            | Self::RequestRunConfig { sender_id, .. }
            | Self::RunConfig { sender_id, .. }
            | Self::StateDigest { sender_id, .. }
            | Self::PullProbe { sender_id, .. }
            | Self::PullProbeReply { sender_id, .. }
            | Self::PullFail { sender_id, .. }
            | Self::MeshReady { sender_id, .. }
            | Self::GracefulAbortRequest { sender_id, .. }
            | Self::TaskComplete { sender_id, .. }
            | Self::TaskFailed { sender_id, .. }
            | Self::TerminalAck { sender_id, .. }
            | Self::CustomMessage { sender_id, .. }
            | Self::TaskHoldQuery { sender_id, .. }
            | Self::TaskHoldResponse { sender_id, .. }
            | Self::RespawnSpawnRequest { sender_id, .. }
            | Self::RespawnSpawnResult { sender_id, .. }
            | Self::RespawnRevokeRequest { sender_id, .. }
            | Self::RespawnRevokeResult { sender_id, .. }
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
            | Self::RedialRequest { sender_id, .. }
            | Self::FrameChunk { sender_id, .. }
            | Self::SetupAssignment { sender_id, .. }
            | Self::SetupTerminal { sender_id, .. }
            | Self::IllegallyAssignedToNonidleWorker { sender_id, .. }
            | Self::RequestInFlightRoster { sender_id, .. }
            | Self::InFlightRoster { sender_id, .. }
            | Self::WithdrawTask { sender_id, .. }
            | Self::SuspectPeers { sender_id, .. }
            | Self::ResolvedPeer { sender_id, .. }
            | Self::RestartRequest { sender_id, .. }
            | Self::RestartConfirm { sender_id, .. }
            | Self::PeerProbe { sender_id, .. }
            | Self::PeerProbeAck { sender_id, .. } => sender_id,
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
            | Self::RequestSnapshotStream { timestamp, .. }
            | Self::SnapshotStreamPackage { timestamp, .. }
            | Self::RequestRunConfig { timestamp, .. }
            | Self::RunConfig { timestamp, .. }
            | Self::StateDigest { timestamp, .. }
            | Self::PullProbe { timestamp, .. }
            | Self::PullProbeReply { timestamp, .. }
            | Self::PullFail { timestamp, .. }
            | Self::MeshReady { timestamp, .. }
            | Self::GracefulAbortRequest { timestamp, .. }
            | Self::TaskComplete { timestamp, .. }
            | Self::TaskFailed { timestamp, .. }
            | Self::TerminalAck { timestamp, .. }
            | Self::CustomMessage { timestamp, .. }
            | Self::TaskHoldQuery { timestamp, .. }
            | Self::TaskHoldResponse { timestamp, .. }
            | Self::RespawnSpawnRequest { timestamp, .. }
            | Self::RespawnSpawnResult { timestamp, .. }
            | Self::RespawnRevokeRequest { timestamp, .. }
            | Self::RespawnRevokeResult { timestamp, .. }
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
            | Self::RedialRequest { timestamp, .. }
            | Self::FrameChunk { timestamp, .. }
            | Self::SetupAssignment { timestamp, .. }
            | Self::SetupTerminal { timestamp, .. }
            | Self::IllegallyAssignedToNonidleWorker { timestamp, .. }
            | Self::RequestInFlightRoster { timestamp, .. }
            | Self::InFlightRoster { timestamp, .. }
            | Self::WithdrawTask { timestamp, .. }
            | Self::SuspectPeers { timestamp, .. }
            | Self::ResolvedPeer { timestamp, .. }
            | Self::RestartRequest { timestamp, .. }
            | Self::RestartConfirm { timestamp, .. }
            | Self::PeerProbe { timestamp, .. }
            | Self::PeerProbeAck { timestamp, .. } => *timestamp,
        }
    }

    /// Whether this primary-bound frame must be RETAINED by the
    /// reporting secondary until the primary's app-level
    /// [`DistributedMessage::TerminalAck`] confirms its landing (#352):
    /// the per-task TERMINAL reports
    /// ([`DistributedMessage::TaskComplete`] /
    /// [`DistributedMessage::TaskFailed`]) and an IMPORTANT
    /// [`DistributedMessage::CustomMessage`] (F5).
    ///
    /// This is the classifier the secondary's reporting concern uses to
    /// decide whether a primary-bound send is REPLAYABLE on a no-route
    /// absorb / unacked timeout: a `TaskComplete` / `TaskFailed`
    /// resolves a task's in-flight entry at the authority, so losing it
    /// strands the task forever (phantom-busy); an important custom
    /// message is the consumer's must-not-lose payload (the streamed-
    /// spawn batch), so it shares the same retention/ack contract. It
    /// is the SINGLE source of that classification, owned by the enum
    /// so every site that gates by "must this report provably reach the
    /// authority?" reads one predicate. The backpressure-shaped
    /// `TaskFailed` (the deferred-lost reinject) IS confirmable here —
    /// it too resolves an in-flight slot at the authority (a requeue),
    /// so it must replay across a no-route.
    ///
    /// Everything else through the primary-bound send chokepoint
    /// (`TaskRequest` capacity hints, `Keepalive`, `MeshReady`, a
    /// DROPPABLE `CustomMessage { important: false }`) is legitimately
    /// droppable — a missed periodic frame is re-emitted on the next
    /// tick; a droppable custom is at-most-once by contract — so it
    /// does NOT require the ack. Nor does
    /// [`DistributedMessage::TerminalAck`]: it CONFIRMS a landing, it
    /// does not carry one (and it never flows through the primary-bound
    /// chokepoint anyway — it is primary→secondary).
    pub fn requires_delivery_ack(&self) -> bool {
        matches!(
            self,
            Self::TaskComplete { .. }
                | Self::TaskFailed { .. }
                | Self::CustomMessage {
                    important: true,
                    ..
                }
        )
    }

    /// Sub-classifier of [`Self::requires_delivery_ack`]: is this frame an
    /// IMPORTANT [`DistributedMessage::CustomMessage`] (the F5 class)?
    ///
    /// The retention concern (`secondary/resource.rs`) splits its drop
    /// trigger by this predicate: a TERMINAL drops on
    /// [`DistributedMessage::TerminalAck`] (the only durability proof a
    /// terminal has — it has no replicated CRDT analogue), while an
    /// IMPORTANT custom drops on observing its own `CustomMessagePosted`
    /// arrive in the local CRDT mirror via a `ClusterMutation` broadcast
    /// (the durability proof that the primary not only applied but also
    /// FANNED OUT the entry — a primary that dies between local apply and
    /// the mesh-pump's wire fan-out under post-#539 would have already
    /// sent the `TerminalAck` from its dispatch tail, dropping retention
    /// with the entry stranded on its dead local CRDT only; the CRDT-
    /// observation drop trigger forecloses that window because the
    /// originator never sees its own broadcast come back unless the
    /// fan-out actually happened).
    ///
    /// Owned by the enum for the same reason `requires_delivery_ack` is:
    /// the secondary's retention chokepoint reads one predicate to choose
    /// the retention reason at first send, and the receiver-side CRDT-
    /// apply hook reads the same shape of fact (a `ClusterMutation`
    /// carrying `CustomMessagePosted` for `self.id`) without needing to
    /// know the frame's history.
    pub fn is_important_custom_message(&self) -> bool {
        matches!(
            self,
            Self::CustomMessage {
                important: true,
                ..
            }
        )
    }

    /// The per-task hash this frame resolves, for the
    /// [`DistributedMessage::TaskComplete`] /
    /// [`DistributedMessage::TaskFailed`] terminal variants; `None` for
    /// every other variant (a `CustomMessage` carries no task — its
    /// retention forensics log the `(origin, msg_seq)` key instead).
    ///
    /// Pairs with [`Self::requires_delivery_ack`]: the reporting concern
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

    /// The app-level delivery-confirmation sequence id (#352) stamped on
    /// a confirmable report (a terminal, or an important custom
    /// message), if any. `None` for every non-confirmable variant AND
    /// for a confirmable frame that was never routed through the
    /// stamping chokepoint (a pre-field wire sender, or a frame the
    /// secondary has constructed but not yet sent).
    ///
    /// Pairs with [`Self::requires_delivery_ack`]: the secondary's
    /// reporting concern stamps it once per report
    /// ([`Self::set_delivery_seq`]) and matches inbound
    /// [`DistributedMessage::TerminalAck`]s against it; the primary's
    /// ingest reads it to echo the ack.
    pub fn delivery_seq(&self) -> Option<u64> {
        match self {
            Self::TaskComplete { delivery_seq, .. }
            | Self::TaskFailed { delivery_seq, .. }
            | Self::CustomMessage { delivery_seq, .. } => *delivery_seq,
            _ => None,
        }
    }

    /// The per-origin custom-message sequence id that, paired with the
    /// originator's `origin_secondary_id`, forms the message's
    /// cluster-wide IDEMPOTENCY KEY `(origin, msg_seq)` — the SAME stamp
    /// the primary IDs an important custom by (carried on the
    /// `CustomMessagePosted` CRDT mutation). `None` for every
    /// non-`CustomMessage` variant.
    ///
    /// Distinct from [`Self::delivery_seq`]: `msg_seq` identifies the
    /// MESSAGE across the cluster (bumps for customs only), whereas
    /// `delivery_seq` is the originator-local retention/ack counter that
    /// bumps for ALL confirmable reports (terminals + important customs)
    /// and therefore DESYNCS from `msg_seq` whenever terminals interleave.
    /// CRDT-convergence retention drops MUST key on `msg_seq`.
    pub fn msg_seq(&self) -> Option<u64> {
        match self {
            Self::CustomMessage { msg_seq, .. } => Some(*msg_seq),
            _ => None,
        }
    }

    /// Stamp the app-level delivery-confirmation `seq` (#352) on a
    /// confirmable frame IN PLACE. A no-op on every other variant — the
    /// stamping chokepoint gates on [`Self::requires_delivery_ack`]
    /// first, so a non-confirmable frame (including a droppable
    /// `CustomMessage`) never reaches this.
    pub fn set_delivery_seq(&mut self, seq: u64) {
        if let Self::TaskComplete { delivery_seq, .. }
        | Self::TaskFailed { delivery_seq, .. }
        | Self::CustomMessage { delivery_seq, .. } = self
        {
            *delivery_seq = Some(seq);
        }
    }

    /// The per-origin CAUSAL custom-message watermark stamped on a task
    /// terminal (the message-vs-phase-end ordering gate), if any. `None`
    /// for every non-terminal variant AND for a terminal from a
    /// pre-field sender / one not yet routed through the stamping
    /// chokepoint — the gate is open in both cases (no causal claim).
    ///
    /// Pairs with [`Self::set_msgs_posted_through`]: the secondary's
    /// `send_to_primary` chokepoint stamps it once per report (sticky
    /// across replays); the primary's terminal-gate ingest reads it to
    /// decide deferral against the origin's replicated custom-inbox
    /// terminal watermark.
    pub fn msgs_posted_through(&self) -> Option<u64> {
        match self {
            Self::TaskComplete {
                msgs_posted_through,
                ..
            }
            | Self::TaskFailed {
                msgs_posted_through,
                ..
            } => *msgs_posted_through,
            _ => None,
        }
    }

    /// Stamp the causal custom-message watermark on a task terminal IN
    /// PLACE. A no-op on every other variant — the stamping chokepoint
    /// gates on [`Self::task_hash`] (terminal-bearing) first, so a
    /// non-terminal frame (including a custom message itself) never
    /// reaches this.
    pub fn set_msgs_posted_through(&mut self, watermark: u64) {
        if let Self::TaskComplete {
            msgs_posted_through,
            ..
        }
        | Self::TaskFailed {
            msgs_posted_through,
            ..
        } = self
        {
            *msgs_posted_through = Some(watermark);
        }
    }

    /// The ORIGINATING reporter of a confirmable frame (the terminal
    /// variants' `secondary_id`; a custom message's
    /// `origin_secondary_id`); `None` for every other variant.
    ///
    /// This — NOT the wire `sender_id` — is where a
    /// [`DistributedMessage::TerminalAck`] must be addressed: the
    /// retention buffer awaiting the ack lives on the originator, and a
    /// landing that travelled a relay / peer-forwarded path carries a
    /// forwarder's `sender_id` while the originator still waits.
    pub fn delivery_reporter(&self) -> Option<&str> {
        match self {
            Self::TaskComplete { secondary_id, .. } | Self::TaskFailed { secondary_id, .. } => {
                Some(secondary_id)
            }
            Self::CustomMessage {
                origin_secondary_id,
                ..
            } => Some(origin_secondary_id),
            _ => None,
        }
    }

    /// Whether the framing layer may transparently CHUNK this frame
    /// when its serialized size exceeds the wire limit (the
    /// `FrameChunk` transfer — see that variant's doc).
    ///
    /// A closed FRAMEWORK-FRAME allowlist, deliberately NOT "everything"
    /// (#364/#366: the wire cap on consumer payloads —
    /// `TaskComplete.result_data`, `CustomMessage.data` — is a
    /// contract, not a transport shortcoming; chunking them would
    /// silently relax it). `SnapshotStreamPackage` is the only member:
    /// packages are bounded by construction (~2 MiB target), but the
    /// bound is a soft target — a single oversized task entry rides a
    /// package alone — so eligibility keeps the wire-cap safety net on
    /// the framework-internal snapshot path without relaxing any
    /// consumer-payload contract. (Its monolithic predecessor,
    /// `ClusterSnapshot`, grew with the whole ledger and was the frame
    /// this mechanism was built for.)
    /// A `Relay` envelope is eligible iff its INNER frame is, so an
    /// indirect (forwarded) snapshot transfer chunks hop-by-hop exactly
    /// like a direct one.
    ///
    /// The classifier is owned by the enum so the sender's split gate
    /// and the receiver's post-reassembly re-check read the SAME
    /// predicate (the receiver re-checks so a non-conformant sender
    /// cannot smuggle a consumer payload past the cap through chunks).
    pub fn chunk_eligible(&self) -> bool {
        match self {
            Self::SnapshotStreamPackage { .. } => true,
            Self::Relay { inner, .. } => inner.chunk_eligible(),
            _ => false,
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
            Self::RequestSnapshotStream { .. } => MessageType::RequestSnapshotStream,
            Self::SnapshotStreamPackage { .. } => MessageType::SnapshotStreamPackage,
            Self::RequestRunConfig { .. } => MessageType::RequestRunConfig,
            Self::RunConfig { .. } => MessageType::RunConfig,
            Self::StateDigest { .. } => MessageType::StateDigest,
            Self::PullProbe { .. } => MessageType::PullProbe,
            Self::PullProbeReply { .. } => MessageType::PullProbeReply,
            Self::PullFail { .. } => MessageType::PullFail,
            Self::MeshReady { .. } => MessageType::MeshReady,
            Self::GracefulAbortRequest { .. } => MessageType::GracefulAbortRequest,
            Self::TaskComplete { .. } => MessageType::TaskComplete,
            Self::TaskFailed { .. } => MessageType::TaskFailed,
            Self::TerminalAck { .. } => MessageType::TerminalAck,
            Self::CustomMessage { .. } => MessageType::CustomMessage,
            Self::TaskHoldQuery { .. } => MessageType::TaskHoldQuery,
            Self::TaskHoldResponse { .. } => MessageType::TaskHoldResponse,
            Self::RespawnSpawnRequest { .. } => MessageType::RespawnSpawnRequest,
            Self::RespawnSpawnResult { .. } => MessageType::RespawnSpawnResult,
            Self::RespawnRevokeRequest { .. } => MessageType::RespawnRevokeRequest,
            Self::RespawnRevokeResult { .. } => MessageType::RespawnRevokeResult,
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
            Self::RedialRequest { .. } => MessageType::RedialRequest,
            Self::FrameChunk { .. } => MessageType::FrameChunk,
            Self::SetupAssignment { .. } => MessageType::SetupAssignment,
            Self::SetupTerminal { .. } => MessageType::SetupTerminal,
            Self::IllegallyAssignedToNonidleWorker { .. } => {
                MessageType::IllegallyAssignedToNonidleWorker
            }
            Self::RequestInFlightRoster { .. } => MessageType::RequestInFlightRoster,
            Self::InFlightRoster { .. } => MessageType::InFlightRoster,
            Self::WithdrawTask { .. } => MessageType::WithdrawTask,
            Self::SuspectPeers { .. } => MessageType::SuspectPeers,
            Self::ResolvedPeer { .. } => MessageType::ResolvedPeer,
            Self::RestartRequest { .. } => MessageType::RestartRequest,
            Self::RestartConfirm { .. } => MessageType::RestartConfirm,
            Self::PeerProbe { .. } => MessageType::PeerProbe,
            Self::PeerProbeAck { .. } => MessageType::PeerProbeAck,
        }
    }
}
