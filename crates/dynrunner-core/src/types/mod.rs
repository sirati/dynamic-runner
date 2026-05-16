//! Core type definitions used throughout the runtime.
//!
//! Split into sub-modules by concern:
//! - [`identifiers`]: opaque `Arc<str>` newtypes for resource/phase/type/affinity ids
//!   plus the [`Identifier`] trait alias and [`RunnerIdentifier`] alias.
//! - [`resource`]: [`ResourceAmount`], [`ResourceMap`], [`SoftPreferredSecondaries`].
//! - [`task`]: [`TaskInfo`] (the scheduling unit) and the [`TaskInput`] alias.
//! - [`error`]: [`ErrorType`], [`TaskResult`], [`FailedTask`].
//!
//! Tests live in `types_tests.rs` (sibling of this `types/` directory) and
//! exercise the public API of all sub-modules through the re-exports below.

pub mod error;
pub mod identifiers;
pub mod resource;
pub mod task;

pub use error::{ErrorType, FailedTask, TaskResult};
pub use identifiers::{AffinityId, Identifier, PhaseId, ResourceKind, RunnerIdentifier, TypeId};
pub use resource::{ResourceAmount, ResourceMap, SoftPreferredSecondaries};
pub use task::{TaskInfo, TaskInput};

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
