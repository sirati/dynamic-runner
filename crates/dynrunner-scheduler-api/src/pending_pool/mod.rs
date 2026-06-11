//! `PendingPool<I>`: the scheduling-side data structure that owns the
//! queued and in-flight `TaskInfo<I>` items, grouped by
//! `(PhaseId, TypeId, AffinityId)`, plus a per-phase state machine
//! that gates dispatch on `depends_on` barriers.
//!
//! ## Concerns owned by this module
//! * Bucketing items by `(phase, type, affinity_or_sentinel)`.
//! * Tracking which workers are soft-pinned to which bucket.
//! * Tracking in-flight counts per phase.
//! * Validating the phase dependency graph at construction time
//!   (no cycles, no unknown deps).
//! * Driving the phase state machine
//!   `Blocked → Active → Draining → Drained → Done`.
//!
//! ## Concerns NOT owned by this module
//! * Worker selection / scheduler decisions (the `Scheduler` trait).
//! * Resource estimation.
//! * `on_phase_end` callbacks — managers fire those after polling
//!   drain transitions.
//! * Sorting items by size — callers extend the pool in the order
//!   they want items dispatched (the pool preserves insertion order
//!   within a bucket).
//!
//! ## Submodule layout
//! The single logical type [`PendingPool`] is implemented across
//! several sibling files, each owning one operational concern. All of
//! them add `impl<I: Identifier> PendingPool<I> { … }` blocks to the
//! same type defined in [`pool`]; submodule visibility (`pub(super)`)
//! is used so internal fields and helpers remain encapsulated within
//! `pending_pool/` without leaking outside the crate.
//!
//! * [`types`] — value types ([`PhaseState`], [`Bucket`],
//!   [`BucketKey`], [`PreferencePredicate`], [`PendingPoolError`])
//!   and the affinity sentinel helpers.
//! * [`view`] — [`WorkerView`] value type and its caller-side
//!   combinators (`filter`, `sort_by_key`).
//! * [`pool`] — the [`PendingPool`] struct definition, constructor
//!   `new`, and cluster-state pre-seeding (`mark_tasks_completed`,
//!   `mark_tasks_failed`, `mark_tasks_failed_pending_retry`,
//!   `mark_tasks_dormant`, `mark_tasks_in_flight`).
//! * [`extend`] — `extend` and its private helpers (`commit_item`,
//!   `collect_known_task_ids`).
//! * [`dispatch`] — `pop_for_worker`, `view_for_worker`,
//!   `take_from_view` and the shared `take_at` / `choose_bucket_for`
//!   internals.
//! * [`lifecycle`] — `on_item_finished`, `on_item_failed_permanent`,
//!   `mark_in_flight`, `requeue`, `reinject`, `drain_queued`,
//!   `release_worker`, `poll_drain_transitions`, `mark_phase_done`,
//!   `drain_empty_active_phases`, plus the private state-machine
//!   helpers `maybe_transition_drain` / `queued_count`.
//! * [`queries`] — read-only accessors (`len`, `is_empty`, `iter`,
//!   `is_run_complete`, `active_phases`, `phase_state`, `in_flight`)
//!   plus the queued-side primitives `retain`,
//!   `update_first_match_in_place`, `take_first_match`.

mod dispatch;
mod extend;
mod lifecycle;
mod partition;
mod pool;
mod queries;
mod types;
mod view;

pub use partition::IngestPartition;
pub use pool::PendingPool;
pub use types::{BucketKey, PendingPoolError, PhaseState, PreferencePredicate};
pub use view::WorkerView;

#[cfg(test)]
#[path = "../pending_pool_tests/mod.rs"]
mod tests;
