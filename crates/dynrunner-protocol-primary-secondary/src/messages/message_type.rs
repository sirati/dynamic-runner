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
    /// Late-joiner / reconnecting / behind node asks one peer to STREAM
    /// it the replicated `ClusterState` as bounded packages. Replaces
    /// the pre-Phase-B `FullTaskList` broadcast and the monolithic
    /// snapshot RPC — under continuously-replicated state, the only
    /// legitimate "ship the full ledger" path is on demand from a node
    /// that's missing it, and the ledger ships incrementally (the CRDT
    /// join needs no consistent cut).
    RequestSnapshotStream,
    /// One package of the stream answering `RequestSnapshotStream`: a
    /// partial `ClusterStateSnapshot` the receiver merges into its local
    /// mirror via `ClusterState::restore`.
    SnapshotStreamPackage,
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
    /// sender pulls a snapshot via `RequestSnapshotStream`. Detector
    /// only — carries no task payloads and triggers no merge by itself.
    StateDigest,
    /// Pull-model probe (the single-flight anti-entropy convergence
    /// replacement): a behind node broadcasts it to DIRECT neighbours only
    /// (never relayed), carrying its own `StateDigest`; each neighbour
    /// answers with a `PullProbeReply` reporting its inbox depth + whether
    /// it is ahead. Collapses the eager per-digest immediate-pull fan-out
    /// into one probe→pull cycle per cooldown.
    PullProbe,
    /// Reply to a `PullProbe`: the responder's current inbox depth + the
    /// `ahead` bit (does it hold ledger data the requester lacks). The
    /// requester picks the smallest-inbox ahead responder as its pull
    /// target. Sent directly back to the requester; never relayed.
    PullProbeReply,
    /// The chosen pull target could not serve a `RequestSnapshotStream`
    /// because its direct link to the requester dropped. Rides the
    /// relay-toward-the-role-holder path so it reaches the requester
    /// INDIRECTLY; the requester then falls to the next candidate.
    PullFail,
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
    /// Primary -> provider-host observer: "submit a replacement
    /// secondary" — the remote-execution leg of the respawn pipeline.
    /// The decision (budget/ledger) stays on the primary; the physical
    /// provider lives in the submitter/observer process. Keyed by the
    /// primary-minted `new_secondary_id` (correlation + idempotency).
    RespawnSpawnRequest,
    /// Provider-host observer -> primary: the spawn outcome, correlated
    /// by `new_secondary_id`. Re-sent from the outcome cache on a
    /// duplicate request (lost-result replay).
    RespawnSpawnResult,
    /// Primary -> provider-host observer: revoke a still-pending
    /// replacement (its original re-admitted). Idempotent at the
    /// provider per the `SecondarySpawner::revoke` contract.
    RespawnRevokeRequest,
    /// Provider-host observer -> primary: the revoke outcome,
    /// correlated by `new_secondary_id`.
    RespawnRevokeResult,
    Keepalive,
    TimeoutDetected,
    TimeoutQuery,
    TimeoutResponse,
    PromotionVote,
    PromotionConfirm,
    /// Primary -> all secondaries (#556 mesh-consensus respawn): the
    /// round-1 opening frame, broadcasting the suspect list with the
    /// round's `consensus_id` so the cluster can resolve false-positives
    /// before any restart vote.
    SuspectPeers,
    /// Secondary -> primary (#556): per-suspect false-positive
    /// contradiction echoed back to the primary's `SuspectPeers` round —
    /// the witnessing secondary heard from a suspected peer recently and
    /// drops it from the round's tally.
    ResolvedPeer,
    /// Primary -> all secondaries (#556): the round-2 commit frame.
    /// After narrowing the suspect list, the primary broadcasts the
    /// remaining candidates for restart confirmation; only on quorum
    /// `RestartConfirm` does the actual respawn / scancel proceed.
    RestartRequest,
    /// Secondary -> primary (#556): the commit-frame ack with the
    /// secondary's final per-candidate vote (`still_suspicious`) AND any
    /// last-second retractions (`resolved_since`).
    RestartConfirm,
    /// Secondary -> suspected secondary (#556, point-to-point active
    /// probe): elicits a fresh keepalive from a peer the prober has
    /// flagged silent. Reusable via Router/Relay.
    PeerProbe,
    /// Suspected secondary -> prober (#556, point-to-point ack): the
    /// reachable peer's reply to a `PeerProbe`. The prober credits it as
    /// positive liveness evidence and forwards a `ResolvedPeer` to the
    /// primary on the current `consensus_id`.
    PeerProbeAck,
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
    /// Wire-only signal from the non-dial-owning side of a
    /// member↔member leg to the leg's dial owner: "my end of our wire
    /// is dead — prune your entry for me and re-dial." Consumed by the
    /// transport's recv path; the application layer never observes it.
    RedialRequest,
    /// Wire-only chunk of one oversized chunk-eligible framework frame
    /// (the `ClusterSnapshot` wire-cap fix). Split and reassembled
    /// entirely inside the framing layer's framed-IO pumps; the
    /// application layer (and even the Router) never observes it.
    FrameChunk,
    /// Primary -> setup-task affinity member: "run this `TaskKind::Setup`
    /// task in-process." Routed to the member's setup executor, not its
    /// worker pool.
    SetupAssignment,
    /// Setup-task affinity member -> primary: the terminal of an in-process
    /// setup-task execution (success → `SetupCompleted`, failure →
    /// `TaskFailed { NonRecoverable }`).
    SetupTerminal,
    /// Secondary -> primary: a work task is now queued behind that
    /// secondary's local SecondaryAffine import (#497). The primary
    /// originates `ClusterMutation::QueuedAfterLocalDependencySet`.
    TaskQueuedAfterLocalDependency,
    /// Secondary -> primary: that secondary's local SecondaryAffine import
    /// for a queued work task is done — release it (#497). The primary
    /// originates the EXISTING `ClusterMutation::TaskAssigned`.
    LocalDependencyReleased,
    /// Secondary -> primary: the primary assigned a task to a NON-idle
    /// worker slot (#517). The secondary honors the assigned `worker_id`
    /// (never re-picks) and bounces this typed report — NOT a `TaskFailed`,
    /// so it is never accounted as a failure; the primary reconciles its
    /// diverged `(secondary, worker_id)` occupancy and requeues the task.
    IllegallyAssignedToNonidleWorker,
    /// Primary -> a just-re-admitted member: report your ACTUAL in-flight
    /// work (#518). A falsely-removed-but-alive member kept running its
    /// tasks while the primary requeued them onto OTHER members; on
    /// re-admission the member is the source of truth for what its workers
    /// run, so the primary pulls that roster to reconcile + dedup the
    /// cross-member duplicates. Carries no payload beyond the routing/common
    /// fields (the addressee is the re-admitted member).
    RequestInFlightRoster,
    /// Re-admitted member -> primary: the answer to `RequestInFlightRoster`
    /// (#518) — the hashes its workers are ACTUALLY running right now, read
    /// off the member's own `active_tasks` bookkeeping (the source of
    /// truth). The primary reconciles each reported hash: a hash it had
    /// requeued onto a DIFFERENT member is authoritatively the reporter's,
    /// so the duplicate copy is withdrawn (`WithdrawTask`).
    InFlightRoster,
    /// Primary -> the member running a DUPLICATE copy (#518): withdraw the
    /// named task from the named worker. The authoritative holder is the
    /// re-admitted original; this member's copy is the requeued duplicate
    /// and stands down. NOT a `TaskFailed` (no terminal accounting, no
    /// retry-budget burn) — the requeue-inverse, like the #517 bounce. The
    /// member drops a not-yet-started copy; a copy already executing is left
    /// to the primary's terminal-dedup (no mid-run abort exists).
    WithdrawTask,
}
