//! Task-completion event type.
//!
//! Why this exists: the cluster-state CRDT's `TaskCompleted` and
//! `TaskFailed` apply rules are the authoritative emitter for "this
//! task reached a terminal state". Downstream consumers — phase
//! orchestrators, control-plane policy that injects follow-up work,
//! Python task-protocol hooks — need to react to those transitions
//! without polling the CRDT.
//!
//! The single concern of this module is the *shape* of the event the
//! apply path enqueues onto the dispatcher mpsc; no emission logic,
//! no consumer logic, no CRDT wiring lives here.
//!
//! Symmetric to [`crate::peer_lifecycle::event::PeerLifecycleEvent`]:
//! both surface terminal CRDT transitions; they differ only in the
//! mutation family that triggers them.

/// Terminal task transition surfaced on the dispatcher mpsc when a
/// `ClusterMutation::TaskCompleted` or `ClusterMutation::TaskFailed`
/// apply moves a task into a terminal state.
///
/// Field semantics:
/// - `task_id`: the consumer-supplied identifier from `TaskInfo.task_id`.
///   Always populated (non-empty) — every task carries a required id
///   per the framework's boundary contract. Surfaced rather than the
///   hash because every consumer documented so far keys their
///   bookkeeping by task_id.
/// - `task_hash`: the wire-canonical content hash (matches the
///   `hash` field on the originating mutation). Stable across replicas
///   so consumers that DO want the CRDT-internal key can still get it.
/// - `success`: `true` iff the apply rule transitioned the task to
///   `TaskState::Completed`. `false` for every other terminal:
///   `TaskFailed { kind: ErrorType::Unfulfillable, .. }` lands in
///   `TaskState::Unfulfillable`; every other `ErrorType` lands in
///   `TaskState::Failed`. Both fire with `success = false`.
/// - `error_kind`: `None` on success; on failure the wire-stable
///   `ErrorType::wire_value()` tag (`"oom"`, `"non_recoverable"`,
///   `"recoverable"`, `"unfulfillable:<reason>"`, etc.). The tag is
///   strictly informational at this layer — the apply rule's typed
///   `ErrorType` is the authoritative source — but exposing the
///   wire-stable string keeps consumers stable across future variant
///   additions (a new `ErrorType` variant gets a new `wire_value()`
///   prefix; consumers that match on the string surface get the new
///   tag automatically without a re-build). This is the carried error
///   *type* identity.
/// - `last_error`: `None` on success; on failure the operator-facing
///   error *message* (the `last_error` body the apply rule stored on
///   the ledger entry). Carried ALONGSIDE `error_kind` because a
///   failure with more than one possible cause is only fully
///   identified by type AND message: a downstream aggregator dedups
///   distinct failures on this message string (two `non_recoverable`
///   failures with different messages are distinct events), so the
///   event must carry the message — not force every consumer to
///   re-read `cluster_state.task_state(hash).last_error` out of band.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskCompletedEvent {
    pub task_id: String,
    pub task_hash: String,
    pub success: bool,
    pub error_kind: Option<String>,
    pub last_error: Option<String>,
}
