//! Primary coordinator module.
//!
//! The orchestration entrypoint is split across several sibling modules:
//!
//! - [`coordinator`] — the `PrimaryCoordinator<T, P, S, E, I>` struct
//!   plus the `new()` constructor, the `run()` cleanup wrapper, the
//!   `run_pipeline()` body, and the small `note_item_*`/
//!   `process_phase_lifecycle` helpers shared by the wire handlers.
//!   Held as a single file because the inherent impl is one cohesive
//!   concern: drive the operational loop while owning every piece of
//!   coordinator state. Each set of wire-handlers (lifecycle.rs,
//!   task.rs, heartbeat.rs, …) lives in its own sibling module
//!   already.
//! - [`config`] — `PrimaryConfig` + `Default` + `wire_local_path`,
//!   plus the `OnPhaseStart` / `OnPhaseEnd` lifecycle-callback type
//!   aliases.
//! - [`error`] — the structured `RunError` enum and `From<String>` /
//!   `From<&str>` blanket impls.
//!
//! The sibling concerns each own their wire arms:
//! [`assignment`], [`command_channel`], [`connect`],
//! [`fulfillability_matcher`], [`heartbeat`], [`lifecycle`],
//! [`peer_setup`], [`preferred_secondaries`], [`respawn`], [`staging`],
//! [`task`], [`wire`].

mod assignment;
mod command_channel;
mod connect;
mod coordinator;
mod config;
mod error;
mod fulfillability_matcher;
mod heartbeat;
mod hydrate;
mod lifecycle;
mod peer_setup;
pub mod preferred_secondaries;
pub mod respawn;
pub(crate) mod retry_bucket;
pub mod staging;
pub(crate) mod task;
pub mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

pub use command_channel::{
    validate_spawn_tasks, PrimaryCommand, SpawnError, COMMAND_CHANNEL_CAPACITY,
};
pub use config::{OnPhaseEnd, OnPhaseStart, PrimaryConfig};
pub use coordinator::PrimaryCoordinator;
pub use error::RunError;

// Submodule-visible coordinator-state types. `pub(crate)` so test-only
// modules under `crate::primary::heartbeat::tests` (etc.) can construct
// these directly without going through the wire-message path; the
// `pub(super) struct` declaration on the coordinator side keeps the
// fields scoped to siblings within `primary/`.
pub(crate) use coordinator::{PendingMassDeath, RemoteWorkerState};
