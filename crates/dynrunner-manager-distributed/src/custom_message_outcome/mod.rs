//! Custom-message outcome event module (the #570 observer-narration
//! primitive — the event-driven follow-up #568 deferred).
//!
//! Single concern: the *shape* of the per-terminal-apply event the F5
//! custom-message apply rule
//! ([`crate::cluster_state::ClusterState::apply_custom_message_handled`]
//! /
//! [`crate::cluster_state::ClusterState::apply_custom_message_failed`])
//! enqueues onto the outcome-narration channel BEFORE the per-origin
//! watermark compactor
//! ([`crate::cluster_state::ClusterState::compact_custom_watermark`])
//! erases the Handled/Failed label. Like
//! [`crate::task_state_change`] — which has exactly ONE consumer (the
//! observer's operator narrator) and a single emit + install pair on
//! `ClusterState` — this module owns only the event value type; the
//! emit side lives on `ClusterState`
//! (`emit_custom_message_outcome_event` /
//! `install_custom_message_outcome_sender`, the same install/emit pair
//! every other apply-path channel uses) and the consume side lives in
//! the observer.
//!
//! The CCD-9 invariant holds at the boundary exactly as for
//! [`crate::task_state_change`]: the apply path NEVER invokes the
//! consumer directly — it only `tx.send()`s onto the channel;
//! consumption happens strictly off-apply on the observer's loop.

pub mod event;

pub use event::{CustomMessageOutcome, CustomMessageOutcomeEvent};
