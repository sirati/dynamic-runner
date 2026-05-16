//! Operational-loop concerns of the `SecondaryCoordinator`.
//!
//! # Sub-module layout
//!
//! - [`process_tasks`] — the tick-driven `select!` event loop body.
//!   Length exception: ~720 lines of `select!` arms; per-arm
//!   extraction would force every arm to plumb the loop-state
//!   (timers, interval handles, OOM watcher) back through method
//!   parameters for no behavioural gain.
//! - [`keepalive`] — periodic `Keepalive` emission +
//!   `check_primary_link_threshold` (tick-driven failover-window
//!   re-check).
//! - [`worker_event`] — `handle_worker_event`, the worker-event
//!   handler that bridges local worker outcomes to cluster-mutation
//!   broadcasts.
//! - [`shutdown`] — `stop_all_workers`, the bulk-stop hook called at
//!   run finalisation.

mod keepalive;
mod process_tasks;
mod shutdown;
mod worker_event;
