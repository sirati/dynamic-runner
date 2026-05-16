//! Observer-side resource-holdings announcer.
//!
//! # Concern
//!
//! Single concern: react to `AnnounceTrigger`s on a channel by sending
//! exactly one `PeerResourceHoldingsUpdated` broadcast carrying the
//! observer's static `holdings` and the cluster's **current**
//! `primary_epoch`. The task body owns the retry-with-backoff loop
//! that wraps every individual delivery attempt, so the registering
//! site (a synchronous `RoleChangeHook` closure) never blocks.
//!
//! # Sub-module layout
//!
//! - [`types`] — the three boundary-crossing types
//!   (`AnnounceTrigger`, `PeerResourceHoldingsUpdatedPayload`, and
//!   the `AnnouncerSender` trait).
//! - [`task`] — `run_observer_announcer`, the spawned-task body
//!   that drains triggers and drives the retry-with-backoff loop.
//! - [`sender`] — `PeerMeshAnnouncerSender`, the production
//!   `AnnouncerSender` impl that bridges to the coordinator's
//!   `peer_transport` through an outbox channel.
//! - [`tests`] — task-body + production-sender wire-shape tests.
//!
//! See each submodule's header for the full per-file design notes
//! (epoch-supersede semantics, retry-loop rationale, why an outbox
//! rather than `Arc<Mutex<P>>`, etc.).

pub mod sender;
pub mod task;
pub mod types;

#[cfg(test)]
mod tests;

pub use sender::{AnnouncerOutboxItem, PeerMeshAnnouncerSender};
pub use task::run_observer_announcer;
pub use types::{AnnounceTrigger, AnnouncerSender, PeerResourceHoldingsUpdatedPayload};
