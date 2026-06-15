//! Custom-message outcome event type (the #570 observer-narration
//! primitive — the event-driven follow-up #568 deferred).
//!
//! Why this exists: the F5 custom-message inbox's per-origin
//! contiguous-prefix terminal watermark
//! ([`crate::cluster_state::ClusterState::compact_custom_watermark`])
//! ERASES the `Handled`/`Failed` label at the SAME apply that lands the
//! terminal — both terminals are payload-dropping tombstones and the
//! compactor advances over BOTH (`apply_custom.rs`:[110-114, 146-149,
//! 161-180]). After compaction the CRDT state has no way to tell the two
//! terminals apart, so the #520 state-derived [`crate::run_narrator`]
//! cannot honestly emit a "handled" line distinct from a "FAILED" one
//! (the boundary-pinning
//! `custom_message_terminals_are_silent_in_state_narrator` test in #568
//! documents that silence). The fix is event-driven: the apply rule
//! BUILDS this event from the PER-MUTATION variant (which still carries
//! the label, and — on `Failed` — the raise reason verbatim) and emits
//! it BEFORE the compactor erases the label, so the observer narrates
//! the spec-fixed INFO/ERROR line with the actual outcome.
//!
//! The single concern of this module is the *shape* of the event the
//! apply rule enqueues onto the dispatcher mpsc; no emission logic, no
//! consumer logic, no CRDT wiring lives here.
//!
//! # Sibling to, not a reuse of, [`crate::task_state_change`]
//!
//! [`crate::task_state_change::TaskStateChangeEvent`] is a DIFFERENT
//! concern: it fires on every winning TASK transition the
//! `merge_task_state` join admits (`Pending`/`InFlight`/`Completed`/`Failed`/…)
//! and carries the holder. This event fires on the F5 custom-message
//! TERMINAL apply (`CustomMessageHandled` / `CustomMessageFailed` — the
//! two arms the watermark compactor erases) and carries the originating
//! secondary id + sequence + the (Handled | Failed{reason}) outcome.
//! The two ride DIFFERENT apply seams (per-task merge vs. F5 apply rule)
//! and answer DIFFERENT questions, so they are distinct event types on
//! distinct channels (no double-emit: the observer's narrator consumes
//! ONLY this one).
//!
//! # NO new CRDT fields
//!
//! The `reason` carried inside [`CustomMessageOutcome::Failed`] rides
//! the WIRE mutation
//! [`dynrunner_protocol_primary_secondary::ClusterMutation::CustomMessageFailed`]
//! (event-only / narration-only — `#[serde(default,
//! skip_serializing_if = "String::is_empty")]`), NEVER the CRDT state
//! map. The apply rule consumes it for the emit and immediately
//! discards it; the resulting `CustomMsgState::Failed` tombstone is
//! label-less today, exactly as before, and the per-origin watermark
//! compactor sweeps it the same way. There is no replicated tally /
//! payload / map added for narration — the lattice is unchanged.

/// The terminal outcome of one F5 custom-message apply — the variant
/// label the per-origin watermark erases at compaction-time, captured
/// HERE before the erase so the observer narrates the truth.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CustomMessageOutcome {
    /// The primary's consumer handler RETURNED cleanly for `(origin,
    /// seq)`. Narrated INFO at the observer.
    Handled,
    /// The primary's consumer handler RAISED for `(origin, seq)`.
    /// Carries the raise reason VERBATIM (the same string the
    /// originating primary already structured-logs at the originator's
    /// `tracing::error!` in `primary/custom_message.rs`, plumbed through
    /// the wire mutation's `reason` field so every replica's apply
    /// emit — observer's included — narrates the same reason).
    /// Narrated ERROR at the observer.
    Failed { reason: String },
}

/// One winning F5 custom-message TERMINAL apply surfaced on the #570
/// outcome-narration mpsc when
/// [`crate::cluster_state::ClusterState::apply_custom_message_handled`]
/// or
/// [`crate::cluster_state::ClusterState::apply_custom_message_failed`]
/// returns
/// [`crate::cluster_state::ApplyOutcome::Applied`]. Fires at most once
/// per winning `(origin, seq)` terminal per node: the apply rule's
/// sticky-latch guards (`Unhandled → Handled`/`Failed`; watermark
/// covers, or already-terminal NoOps) mean a redelivered terminal
/// NoOps and never double-emits.
///
/// Field semantics:
/// - `origin`: the originating secondary id (the F5 key's first
///   component).
/// - `seq`: the originating secondary's per-origin monotone sequence
///   (the F5 key's second component).
/// - `outcome`: the per-mutation outcome label, captured BEFORE the
///   per-origin watermark compactor erases it. See
///   [`CustomMessageOutcome`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustomMessageOutcomeEvent {
    pub origin: String,
    pub seq: u64,
    pub outcome: CustomMessageOutcome,
}
