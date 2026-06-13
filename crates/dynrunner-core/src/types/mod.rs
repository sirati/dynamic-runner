//! Core type definitions used throughout the runtime.
//!
//! Split into sub-modules by concern:
//! - [`identifiers`]: opaque `Arc<str>` newtypes for resource/phase/type/affinity ids
//!   plus the [`Identifier`] trait alias and [`RunnerIdentifier`] alias.
//! - [`resource`]: [`ResourceAmount`], [`ResourceMap`], [`SoftPreferredSecondaries`].
//! - [`task`]: [`TaskInfo`] (the scheduling unit), the [`TaskInput`] alias,
//!   and the [`TaskDep`] dep-graph edge primitive.
//! - [`outputs`]: [`TaskOutputs`], [`ResultValue`], and the soft-cap helper.
//! - [`done_payload`]: [`DonePayload`] — the Python worker's wire
//!   wrapper around `TaskOutputs` plus the `warnings`/`filtered`
//!   counters; consumed by both manager crates' decoder paths.
//! - [`error`]: [`ErrorType`], [`TaskResult`], [`FailedTask`].
//! - [`version`]: [`TaskVersion`] — the primary-stamped monotone per-task
//!   metadata version that makes the non-lattice CRDT fields convergent.
//!
//! Tests live in `types_tests.rs` (sibling of this `types/` directory) and
//! exercise the public API of all sub-modules through the re-exports below.

pub mod done_payload;
pub mod error;
pub mod identifiers;
pub mod outputs;
pub mod resource;
pub mod task;
pub mod version;

pub use done_payload::DonePayload;
pub use error::{ErrorType, FailedTask, TaskResult};
pub use identifiers::{AffinityId, Identifier, PhaseId, ResourceKind, RunnerIdentifier, TypeId};
pub use outputs::{INLINE_VALUE_HARD_CAP_BYTES, ResultValue, TaskOutputs, check_soft_caps};
pub use resource::{ResourceAmount, ResourceMap, SoftPreferredSecondaries};
pub use task::{TaskDep, TaskInfo, TaskInput, TaskKind};
pub use version::TaskVersion;

pub type WorkerId = u32;

// Re-export items used by `types_tests.rs` (which still does `use super::*;`)
// so the test surface stays identical to the pre-split monolithic types.rs.
#[cfg(test)]
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};
#[cfg(test)]
#[allow(unused_imports)]
use std::path::PathBuf;

#[cfg(test)]
#[path = "../types_tests.rs"]
mod tests;
