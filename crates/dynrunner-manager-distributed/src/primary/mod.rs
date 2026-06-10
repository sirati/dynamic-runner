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
mod config;
mod connect;
mod coordinator;
mod custom_message;
mod discovery;
mod error;
mod fulfillability_matcher;
mod heartbeat;
mod hydrate;
mod important_events;
mod ingest;
mod lifecycle;
mod peer_setup;
pub mod preferred_secondaries;
pub mod respawn;
pub(crate) mod retry_bucket;
mod secondary_id;
pub mod staging;
pub(crate) mod task;
pub mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

pub use command_channel::{
    COMMAND_CHANNEL_CAPACITY, PrimaryCommand, SpawnError, validate_spawn_tasks,
};
pub use config::{OnCustomMessage, OnPhaseEnd, OnPhaseStart, PhaseHookRaiseLatch, PrimaryConfig};
pub use coordinator::{PrimaryCoordinator, PrimaryRunOutcome};
pub use error::RunError;

// Submodule-visible coordinator-state types. `pub(crate)` so test-only
// modules under `crate::primary::heartbeat::tests` (etc.) can construct
// these directly without going through the wire-message path; the
// `pub(super) struct` declaration on the coordinator side keeps the
// fields scoped to siblings within `primary/`.
pub(crate) use coordinator::{RemoteWorkerState, SlotProvenance, SlotState};

/// Settle window the authority sleeps after broadcasting a run-terminal
/// CRDT mutation (`RunComplete` / `RunAborted`) before the dispatcher
/// tears down its transport. Without it, a fast dispatcher exit races
/// the broadcast and some peers miss the signal — leftover SLURM jobs in
/// CG state for wrappers whose secondaries never saw it. 500ms is far
/// more than the QUIC delivery latency of an in-process / podman-bridge
/// mesh; the cost on a happy-path exit is negligible. Shared by the
/// `RunComplete` path (`coordinator`) and the `RunAborted` path
/// (`ingest`) so the two stay in lockstep.
pub(crate) const PRIMARY_BROADCAST_SETTLE: std::time::Duration =
    std::time::Duration::from_millis(500);
