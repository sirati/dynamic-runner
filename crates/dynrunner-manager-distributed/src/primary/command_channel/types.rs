//! In-crate re-export hub for the command-channel wire types.
//!
//! Single concern: keep the historical `crate::primary::command_channel::
//! types::{PrimaryCommand, SpawnError, validate_spawn_tasks,
//! COMMAND_CHANNEL_CAPACITY}` import path stable for distributed-crate
//! callers while the authoritative definitions live in `dynrunner-core`
//! (so both managers share one wire shape without a manager → manager
//! dependency edge).
//!
//! Module boundary:
//!   * Owns: nothing. Pure re-export.
//!   * Does NOT own: any per-task validation rule, enum variant shape,
//!     or channel capacity. All four live in
//!     `dynrunner_core::spawn_tasks_validator`.

pub use dynrunner_core::{
    validate_spawn_tasks, PrimaryCommand, SpawnError, COMMAND_CHANNEL_CAPACITY,
};
