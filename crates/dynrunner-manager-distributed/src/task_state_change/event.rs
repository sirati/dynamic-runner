//! Task state-change event type (the #520 observer-narration primitive).
//!
//! Why this exists: the observer narrates EVERY CRDT task transition it
//! mirrors to the operator's `--important-stdio` stream — assignment,
//! completion, terminal/recoverable/oom failure, and every other
//! non-terminal transition. The authoritative emitter for "this task
//! changed state" is the CRDT's per-task merge join
//! ([`crate::cluster_state::ClusterState::merge_task_state`]) — the ONE
//! place a transition lands on ANY node (originator apply-locally, mirror
//! apply of a broadcast, AND snapshot restore all route through it). So
//! the event is BUILT there, exactly once per winning transition, and
//! fires byte-identically regardless of which ingestion path produced it.
//!
//! The single concern of this module is the *shape* of the event the
//! merge join enqueues onto the dispatcher mpsc; no emission logic, no
//! consumer logic, no CRDT wiring lives here.
//!
//! # Sibling to, not a reuse of, [`crate::task_completed::TaskCompletedEvent`]
//!
//! `TaskCompletedEvent` is a DIFFERENT concern: it fires only on the
//! SUCCESS / FAILURE terminals (silent on skip / setup / affine /
//! non-terminal) for downstream *Policy bucketing* (the invalid-task
//! fatal monitor, the error aggregator), and carries no holder. This
//! event fires on EVERY winning transition (including assignment and
//! every non-terminal state) and carries the holder
//! `{secondary}-{worker}` the operator narrative needs — a strictly
//! observational layer the observer alone consumes. The two ride the
//! SAME merge seam but answer different questions, so they are distinct
//! event types on distinct channels (no double-emit: the observer's
//! narrator consumes ONLY this one).
//!
//! # NO observer-only CRDT
//!
//! Every field derives from the CRDT the primary already maintains: the
//! state classification from the post-merge `TaskState` discriminant, the
//! holder from the post-merge `InFlight`/`QueuedAfterLocalDependency`
//! state (assignment) or the PRE-merge holder captured at the join (a
//! terminal supersedes the `InFlight` that held it, so the prior holder is
//! the operator's "completed/failed ON which worker" answer), and the fail
//! reason/`last_error` from the failure record. There is no replicated
//! tally added for narration.

use dynrunner_core::WorkerId;

/// The CRDT transaction coordinates of one winning task transition — the
/// id the operator correlates an observer narration line to the
/// originating CRDT change.
///
/// These are exactly the fields the per-task monotone join
/// (`crate::cluster_state::merge::task_join_key`) arbitrates on: the
/// primary-stamped [`dynrunner_core::TaskVersion`] `(primary_epoch, seq)`
/// — the C3/D-V authoritative per-transition stamp — paired with the F2
/// retry `attempt` generation. There is NO invented id: this is the
/// CRDT's own transaction arbiter surfaced verbatim.
///
/// The version-LESS terminals (`Completed`, `Blocked`,
/// `SkippedAlreadyDone`, `SetupCompleted`) carry no per-transition
/// `TaskVersion` stamp — the terminal RANK settles them, not a version —
/// so for those `epoch`/`seq` are the [`dynrunner_core::TaskVersion`]
/// default `(0, 0)` and the `attempt` is the meaningful coordinate. This
/// is honest by construction: the rendered `crdt_txn=e0.v0.a{attempt}`
/// tells the operator the transition was terminal-rank-settled at
/// generation `attempt`, not version-arbitrated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskTxnId {
    /// The `TaskVersion.primary_epoch` of the winning state (the cluster
    /// epoch that stamped it). `0` for a version-less terminal.
    pub primary_epoch: u64,
    /// The `TaskVersion.seq` of the winning state (the per-task monotone
    /// counter within the epoch). `0` for a version-less terminal.
    pub seq: u32,
    /// The F2 retry-attempt generation, present on EVERY state.
    pub attempt: u32,
}

impl std::fmt::Display for TaskTxnId {
    /// `e{primary_epoch}.v{seq}.a{attempt}` — the compact CRDT-transaction
    /// rendering the narration line carries (`crdt_txn=e0.v0.a0`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}.v{}.a{}", self.primary_epoch, self.seq, self.attempt)
    }
}

/// The classification of one winning task transition, mapped to the
/// operator-narration level + wording the observer emits. Derived purely
/// from the POST-merge [`crate::cluster_state::TaskState`] discriminant
/// (and, for the fail classes, the carried `ErrorType` folded the SAME way
/// [`crate::cluster_state::ClusterState::outcome_counts`] folds it — so the
/// ERROR-vs-WARN level is the CRDT's own authoritative bucketing, never a
/// re-derivation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStateChange {
    /// `InFlight` — the task was assigned to a worker. Narrated INFO. The
    /// holder rides the event's `holder` field.
    Assigned,
    /// `Completed` — worker work succeeded. Narrated INFO. The holder is
    /// the PRIOR `InFlight` entry the completion superseded.
    Completed,
    /// A TERMINAL failure (`ErrorType` ∈ {NonRecoverable, non-memory
    /// ResourceExhausted, Unfulfillable, InvalidTask} — exactly the
    /// `fail_final` fold). Narrated ERROR with the full `last_error`.
    TerminalFailure { reason: String, last_error: String },
    /// A RECOVERABLE failure (`ErrorType::Recoverable` — the `fail_retry`
    /// fold). Narrated WARN.
    RecoverableFailure { reason: String },
    /// An OOM failure (`ErrorType::ResourceExhausted("memory")` — the
    /// `fail_oom` fold). Narrated WARN.
    OomFailure { reason: String },
    /// Any other winning transition — every non-terminal state
    /// (`Pending` incl. a retry/cascade resume, `Blocked`,
    /// `QueuedAfterLocalDependency`) and the non-fail terminals the
    /// completion channel stays silent on (`SkippedAlreadyDone`,
    /// `SetupCompleted`, `AffineReady`). Narrated INFO "changed state to
    /// {state}", where `state` is the human tag below.
    Other { state: &'static str },
}

/// Which CRDT ingestion path produced one winning task transition —
/// the discriminator the observer's narrator routes on (#636-followup).
///
/// The merge seam is DELIBERATELY path-INDEPENDENT for the CRDT (a
/// transition that lands via snapshot restore is byte-identical to one
/// that lands via a live broadcast — see
/// [`crate::cluster_state::ClusterState::merge_task_state`]); that
/// unification is correct for convergence but WRONG for the operator,
/// who would otherwise see a relocated/late-join observer's whole
/// bootstrap InFlight partition narrated task-by-task as if it were live
/// changes (the ~10-min spew at 40k scale). This tag — and ONLY this tag
/// — re-introduces the operator-facing distinction the CRDT erased: the
/// seam stays CRDT-path-independent, it just stamps which path it took.
///
/// Stamped at the single emit chokepoint
/// ([`crate::cluster_state::ClusterState::emit_task_state_change_event`])
/// from a scoped restore marker on `ClusterState`, NOT threaded through
/// the write-path signatures — so a `set_task_state` / `merge_task_state`
/// / `rewrite_task_state` caller never learns of narration source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NarrationSource {
    /// The transition landed via a LIVE broadcast apply (the steady-state
    /// path): the originator's local apply or a mirror apply of a peer's
    /// broadcast. Narrated INDIVIDUALLY, one operator line per transition.
    LiveBroadcast,
    /// The transition landed via a snapshot RESTORE / catch-up merge (a
    /// bootstrap full-ledger pull, a recovery re-stream, a failover
    /// re-bootstrap) — a re-narration of converged STATE, not a live
    /// change. Folded into a per-restore-batch SUMMARY line, exactly as
    /// the pre-loop bootstrap is folded by `narrate_baseline`.
    CatchUp,
}

/// One winning task transition surfaced on the state-change dispatcher
/// mpsc when [`crate::cluster_state::ClusterState::merge_task_state`]
/// accepts an `incoming` state. Fires at most once per winning join key
/// per node (an idempotent redelivery / re-restore NoOps the join, so it
/// never double-narrates).
///
/// Field semantics:
/// - `task_id`: the consumer-supplied identifier from `TaskInfo.task_id`
///   (the operator-facing id, same surface
///   [`crate::task_completed::TaskCompletedEvent`] exposes).
/// - `change`: the classified transition + level mapping (see
///   [`TaskStateChange`]).
/// - `holder`: `Some((secondary, worker))` for an assignment (the new
///   `InFlight`/`QueuedAfterLocalDependency` holder) or a completion /
///   failure (the PRIOR holder captured at the merge). `None` for a
///   transition that has no holder on either side (e.g. a spawn-time
///   `Pending`, a `Blocked` cascade-pause, a `SkippedAlreadyDone` skip).
/// - `from`: the human state tag of the PRE-write state (the slot's
///   prior occupant), captured at the `set_task_state` apply seam BEFORE
///   the move-in overwrites it. `None` for a logical CREATE (the slot was
///   vacant — a spawn-time first write), where there is no prior state to
///   name. The narrator renders the transition as "from {from} to {new}".
/// - `txn`: the CRDT transaction coordinates of the WINNING (post-write)
///   state — the id the operator correlates the line to the originating
///   CRDT change. See [`TaskTxnId`].
/// - `source`: which CRDT ingestion path produced the transition (see
///   [`NarrationSource`]). AUTHORITATIVELY stamped by
///   [`crate::cluster_state::ClusterState::emit_task_state_change_event`]
///   from the scoped restore marker — the build site at
///   `set_task_state` sets the [`NarrationSource::LiveBroadcast`]
///   default and the emit chokepoint overwrites it when the restore
///   marker is set, so the write path carries no narration-source
///   concern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskStateChangeEvent {
    pub task_id: String,
    pub change: TaskStateChange,
    pub holder: Option<(String, WorkerId)>,
    pub from: Option<&'static str>,
    pub txn: TaskTxnId,
    pub source: NarrationSource,
}
