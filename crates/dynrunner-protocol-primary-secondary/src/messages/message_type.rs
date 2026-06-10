//! `MessageType` enum — the wire-side discriminator for
//! [`DistributedMessage<I>`]. One-to-one with each variant of the
//! generic message enum.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    // Primary <-> Secondary
    SecondaryWelcome,
    Entropy,
    CertExchange,
    PeerInfo,
    InitialAssignment,
    TaskRequest,
    TaskAssignment,
    TransferComplete,
    StageFile,
    /// Late-joiner / reconnecting node asks any connected peer for a
    /// full snapshot of the current `ClusterState`. Replaces the
    /// pre-Phase-B `FullTaskList` broadcast — under continuously-
    /// replicated state, the only legitimate "ship the full ledger"
    /// path is on demand from a node that's missing it.
    RequestClusterSnapshot,
    /// Response to `RequestClusterSnapshot`: a full `ClusterStateSnapshot`
    /// the receiver merges into its local mirror via `ClusterState::restore`.
    ClusterSnapshot,
    /// Joining / reconnecting / respawned secondary asks any connected
    /// peer for the cluster-wide run configuration (the consumer's
    /// `forwarded_argv`). Replicated, so any peer can answer; carries no
    /// payload beyond routing.
    RequestRunConfig,
    /// Response to `RequestRunConfig`: the cluster-wide `forwarded_argv`
    /// the requester splices onto its argv to reconstruct the run-config.
    RunConfig,
    /// Periodic anti-entropy fingerprint (`StateDigest`). Every role
    /// broadcasts it on the convergence cadence; a receiver behind the
    /// sender pulls a snapshot via `RequestClusterSnapshot`. Detector
    /// only — carries no task payloads and triggers no merge by itself.
    StateDigest,
    MeshReady,
    /// Observer -> Primary: "gracefully abort the run" — the ONE
    /// management command a zero-authority observer may send. The
    /// primary originates `ClusterMutation::GracefulAbortRequested`
    /// (the replicated sticky dispatch-freeze latch) on receipt;
    /// idempotent under re-sends.
    GracefulAbortRequest,
    // Secondary <-> Secondary (peer-to-peer)
    TaskComplete,
    TaskFailed,
    /// Primary -> reporting secondary: app-level delivery confirmation
    /// for one terminal-bearing report landing (#352). Carries the
    /// confirmed report's `delivery_seq` verbatim; the receiving
    /// secondary drops the matching retention-buffer entry. Delivery
    /// bookkeeping only — never a liveness signal.
    TerminalAck,
    /// Secondary -> Primary consumer-defined message (F5): an opaque
    /// `(topic, data)` payload keyed by the per-origin
    /// `(origin_secondary_id, msg_seq)` idempotency pair. Droppable
    /// (`important = false`, fire-and-forget) or important
    /// (`important = true`, #352-retained + acked via `TerminalAck` +
    /// CRDT-resident until the primary's handler consumes it).
    CustomMessage,
    /// Primary -> holder secondary: per-task reconciliation probe
    /// (#308) — "do you still hold task X?". Emitted once a task has
    /// been in flight past the reconciliation deadline with no
    /// terminal. Accounting reconciliation only, never liveness.
    TaskHoldQuery,
    /// Holder secondary -> primary: the probe's answer. `held = true`
    /// re-arms the primary's per-task deadline; `held = false` is the
    /// holder's positive denial, on which the primary fails + requeues
    /// the task via the backpressure-shaped path.
    TaskHoldResponse,
    Keepalive,
    TimeoutDetected,
    TimeoutQuery,
    TimeoutResponse,
    PromotionVote,
    PromotionConfirm,
    /// Secondary signalling an unrecoverable local fault (e.g. peer
    /// mesh fully failed to form). Sent once, immediately before the
    /// secondary process exits non-zero. Primary treats the sender as
    /// dead and runs the standard requeue path.
    SecondaryFatalError,
    /// Replicated cluster-state mutation. Carries one or more
    /// `ClusterMutation`s (TaskAdded / TaskAssigned / TaskCompleted /
    /// TaskFailed / PrimaryChanged) for receivers to apply against
    /// their local `ClusterState`. See
    /// `dynrunner_manager_distributed::cluster_state` for the CRDT
    /// semantics; this variant is purely the wire envelope.
    ClusterMutation,
    /// Wire-only envelope: peer-to-peer relay when the direct A↔B
    /// link is unreachable but A↔C↔B is. The application layer never
    /// observes this variant; `PeerTransport::recv_peer` unwraps it
    /// or forwards it transparently.
    RelayMessage,
    /// Wire-only signal from a forwarder back to its predecessor:
    /// "I couldn't forward your relay; mark me tried and pick
    /// another." Identified by `(original_sender, relay_id)`.
    RelayBackoff,
}
