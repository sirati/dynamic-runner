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
//! # Deferred extension points (left clean, NOT implemented)
//!
//! * **`invalid_task` stat line.** When Part B's `TaskState::InvalidTask`
//!   lands and `StateCounts` grows an `invalid_task` count, add the
//!   field to [`StatsSnapshot`] (sourced from `counts().invalid_task`,
//!   the same trap-avoidance as `unfulfillable`) and add one
//!   `MetricLine` to `format::render_report`. No other module changes.
//! * **Occupancy stats** `{secondaries with ≥1 task}/{total}` and
//!   `{workers with tasks}/{total}`. The NUMERATORS are already
//!   derivable here (`per_secondary_in_flight` gives distinct busy
//!   secondaries; a `(secondary, worker)` set gives distinct busy
//!   workers). The DENOMINATORS need Part D's replicated capacity
//!   record. When it lands, source the totals through a new
//!   `CrdtSnapshotSource`-provided field and add two `MetricLine`s.

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
pub use run::{run_reporter, SharedSnapshotSource, TokioClock};
pub use stats::StatsSnapshot;
