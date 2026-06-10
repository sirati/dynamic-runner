//! Per-worker manager-side bookkeeping.
//!
//! Sub-modules:
//! - [`exit_status`]: `WorkerExitStatus` + the non-blocking
//!   `try_reap_subprocess` waitpid wrapper.
//! - [`event`]: the `WorkerEvent` enum the manager dispatches on.
//! - [`handle`]: `WorkerHandle` — the bulk of the per-worker state
//!   machine. Kept as one file at ~400 lines because the impl block
//!   IS the single concern (assignment, poll loop, reclaim, stop,
//!   bookkeeping) and chopping it across files would scatter one
//!   struct's API.
//! - [`exit_status_tests`]: cfg-test only.

pub mod event;
pub mod exit_status;
pub mod handle;

#[cfg(test)]
mod custom_message_tests;
#[cfg(test)]
mod exit_status_tests;

pub use event::WorkerEvent;
pub use exit_status::WorkerExitStatus;
pub use handle::WorkerHandle;
