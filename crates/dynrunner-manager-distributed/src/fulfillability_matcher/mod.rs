//! Fulfillability-matcher dispatch module.
//!
//! Three concerns, one per submodule:
//!
//! - [`event`] defines the [`MatcherTriggerEvent`] value type that flows
//!   from the apply path (the future
//!   `ClusterMutation::PeerResourceHoldingsUpdated` apply rule, E1) to
//!   the matcher pipeline. One event per applied holdings update; the
//!   pipeline collapses bursts into a single matcher pass per
//!   `Unfulfillable` task.
//! - [`matcher`] defines the [`FulfillabilityMatcher`] trait every
//!   consumer (PyO3 bridge adapter, future Rust-side matcher) implements.
//!   Single method: `should_reinject(task, reason, holdings) -> bool`.
//! - [`pipeline`] is the batched drain helper the primary's operational
//!   `select!` awaits. It accumulates `MatcherTriggerEvent`s with a
//!   50ms idle window and yields one collapsed `MatcherBatch` so the
//!   coordinator walks `Unfulfillable` tasks exactly once per burst.
//!
//! The module boundary mirrors `peer_lifecycle`: the apply path NEVER
//! invokes a matcher directly — it only `tx.send()`s an event onto the
//! dispatcher channel. Matcher execution runs strictly off-apply (the
//! operational loop's `select!` arm), so a slow / panicking /
//! Python-GIL-blocked matcher cannot stall `ClusterState::apply`.

pub mod event;
pub mod matcher;
pub mod pipeline;

pub use event::MatcherTriggerEvent;
pub use matcher::FulfillabilityMatcher;
pub use pipeline::{MATCHER_BATCH_IDLE_WINDOW, MatcherBatch, drain_matcher_batch};
