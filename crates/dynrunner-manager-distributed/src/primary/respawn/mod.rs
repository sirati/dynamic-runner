//! Scaffolding for secondary respawn.
//!
//! Single concern: own the types and trait that describe how a
//! replacement secondary is requested, spawned, budgeted, and reported
//! back to the operational loop. Per-provider implementations
//! (multi-process, SLURM) live in sibling files and depend only on
//! this module's API surface; the operational loop owns the
//! `JoinSet<RespawnOutcome>` field declared on `PrimaryCoordinator`
//! and drains it in its `select!`. No call site outside this module
//! needs to know the internals of any specific spawner.
//!
//! The crossing-the-boundary surface is the [`SecondarySpawner`]
//! trait plus the value types [`SecondarySpawnSpec`], [`SpawnError`],
//! [`RespawnBudget`], [`RespawnOutcome`], and [`RespawnEvent`].
//! Everything else (per-task tracking, primary-internal helpers,
//! CLI flag plumbing) lands in sibling subtasks.

mod budget;
mod handler;
mod listener;
mod types;

pub use listener::respawn_dispatcher_listener;
pub use types::{
    RESPAWN_EVENTS_CAP, RespawnBudget, RespawnDecision, RespawnEvent, RespawnOutcome,
    RespawnRequest, SecondarySpawnSpec, SecondarySpawner, SpawnError,
};

#[cfg(test)]
mod tests;
