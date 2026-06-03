//! Observer-side periodic stats reporter (CRDT-derived).
//!
//! # Concern
//!
//! Per owner-decision C-4, the 10-minute cluster stats are EMITTED BY
//! THE OBSERVER from the replicated CRDT: the observer holds the full
//! cluster state and carries zero authority, making it the natural
//! "wake-an-LLM" reporter. This module owns that reporting concern
//! end-to-end and emits ONLY to the importance channel
//! (`tracing` target `dynrunner_important`), which C1's dual-sink
//! routes to stdio under `--important-stdio-only`.
//!
//! # Module boundary
//!
//! The reporter is a SELF-CONTAINED concern. Its delta-snapshot
//! baseline and per-secondary idle gate live HERE — never as new fields
//! on a coordinator or `ClusterState` struct. The only thing crossing
//! IN is a read of the CRDT, taken through the [`CrdtSnapshotSource`]
//! seam (so the live-CRDT access mechanism stays the integration
//! site's concern and the reporter logic is testable in isolation).
//!
//! Sub-modules, each a single concern:
//!   * [`stats`]  — project `&ClusterState` → [`StatsSnapshot`] (pure).
//!   * [`format`] — delta + the `>0`-and-changed inclusion rule (pure).
//!   * [`idle`]   — the idle-secondary gate state machine (pure).
//!   * [`run`]    — the two-cadence driver + importance-channel emit.
//!
//! # Occupancy stats (Part-C addon, now implemented)
//!
//! The two occupancy ratios `{secondaries with ≥1 task}/{total}` and
//! `{workers with tasks}/{total}` are CRDT-derived in [`stats`]: the
//! NUMERATORS from the live `TaskState::InFlight` entries
//! (`per_secondary_in_flight.len()` for busy secondaries; a distinct
//! `(secondary, worker)` set for busy workers) and the DENOMINATORS
//! from Part D's replicated capacity accessors
//! (`ClusterState::known_secondaries().count()` and
//! `total_worker_count()`). They render in [`format`] as a
//! `MetricShape::Ratio` `{busy}/{total}`, included only when the
//! numerator is `> 0` and either component changed since the last
//! announcement.

pub mod format;
pub mod idle;
pub mod run;
pub mod stats;

#[cfg(test)]
mod tests;

// The integration site (`observer_late_joiner/run.rs`) wires the
// reporter with these. The `Clock` / `CrdtSnapshotSource` traits stay
// `pub` in `run` (the seam contracts; the test suite + a future live
// producer name them) but are not re-exported here until an external
// caller needs them by short name.
pub use run::{SharedSnapshotSource, TokioClock, run_reporter};
pub use stats::StatsSnapshot;
