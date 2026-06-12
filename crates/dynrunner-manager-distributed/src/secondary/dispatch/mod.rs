//! Inbound primary→secondary message dispatch and its supporting helpers.
//!
//! # Sub-module layout
//!
//! - [`router`] — `dispatch_message`, the wide-match router that
//!   handles every `DistributedMessage` variant arriving over the
//!   primary transport. Length exception (~580 lines): the body is
//!   one cohesive concern (route-then-handle); per-arm extraction
//!   would require threading every destructured wire field through a
//!   method signature for no behavioural gain.
//! - [`helpers`] — `apply_cluster_mutations`, `stage_and_register`,
//!   `report_unresolvable_task`. Used by both the router and by
//!   `wait_for_setup` / `handle_initial_assignment` so each rule has
//!   exactly one writer.

mod helpers;
mod router;

/// Wire `error_message` marker for the duplicate-assignment reply: the
/// receiving secondary is ALREADY EXECUTING the assigned `file_hash`
/// (it sits in the live own-worker bookkeeping —
/// `SecondaryLifecycle::holds_task`, the same truth source the
/// reconciliation-probe responder answers from). The frame is a
/// COHERENCE report, not a terminal and not backpressure: the task
/// never left the holder, so the primary must KEEP it in flight on
/// this holder and settle through the eventual real terminal.
///
/// Why this exists (the post-failover assign loop): a promoted primary
/// whose replicated ledger lost the `InFlight` fact (the originating
/// primary died between the `TaskAssignment` send and its
/// `TaskAssigned` broadcast landing anywhere; or a false-dead requeue
/// raced a live holder) re-dispatches a hash the holder is still
/// running. Pre-fix the router answered with the GENERIC
/// "No idle worker available" backpressure bounce, which the primary
/// classifies as requeue — assign → bounce → requeue → re-assign,
/// indefinitely (paced by the pool's re-dispatch backoff but never
/// converging); with an idle worker present the fallback instead
/// DOUBLE-RAN the hash on the same node and clobbered its
/// `active_tasks` entry. This marker carries the "already running"
/// truth so the primary's classifier
/// (`primary::task::failed::is_backpressure_shaped`'s sibling arm)
/// can converge to the InFlight-on-holder state instead of looping.
///
/// Sibling of the other emitter-owned wire markers
/// (`secondary::resource::NO_FAULT_PREEMPT_WIRE_MESSAGE`,
/// `primary::reconciliation_probe::RECONCILIATION_LOST_WIRE_MESSAGE`):
/// the emitting module owns the constant; both sides reference it so
/// the contract can never drift.
pub(crate) const TASK_ALREADY_HELD_WIRE_MESSAGE: &str =
    "task already held by this secondary; assignment is a duplicate";
