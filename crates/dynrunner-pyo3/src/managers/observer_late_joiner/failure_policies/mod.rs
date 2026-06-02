//! Observer-side terminal-failure policies.
//!
//! # Concern
//!
//! The observer is the cluster's zero-authority LLM-wake reporter, and
//! per owner-decisions B and C-5 it is also the node that reacts to
//! terminal failures. Both reactions are thin POLICIES over the ONE
//! shared windowed-failure-collector primitive
//! ([`dynrunner_manager_distributed::task_completed::collector`]): the
//! collector owns the window/dedup mechanics, these policies own only
//! WHICH failures count, HOW LONG the window is, and WHAT happens when it
//! elapses.
//!
//!   * [`invalid_task`] — **Policy B**: on the FIRST `invalid_task`-kind
//!     terminal failure, arm a 1-minute window, then signal the
//!     observer's fatal-exit (one-shot). Only the OBSERVER exits on
//!     invalid_task; the cluster keeps running (owner decision).
//!   * [`aggregation`] — **Policy C**: on the first failure within a
//!     rolling 10-minute window, collect `min(1min, remainder)`, then
//!     emit every distinct error in detail on the importance channel
//!     (`xN other tasks` for repeats), once per distinct message per
//!     rolling window. Never exits; resets each rolling window.
//!
//! # Module boundary
//!
//! Each policy is a `CollectorPolicy` impl + (B only) its fatal-exit
//! signal handle. They share NO state with each other and hold NO
//! coordinator reference — the collector's listener/driver split keeps
//! all the threading concerns in the primitive. The integration site
//! (`observer_late_joiner/run.rs`) builds each policy's collector,
//! registers the listener via `register_task_completed_listener`, and
//! spawns the driver; Policy B's fatal-exit receiver is consumed by the
//! operational loop exactly like the panik signal.

pub mod aggregation;
pub mod invalid_task;

#[cfg(test)]
mod tests;

pub use aggregation::ErrorAggregationPolicy;
pub use invalid_task::InvalidTaskMonitorPolicy;
