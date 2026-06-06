//! Cross-thread / cross-async-runtime ingress for "from outside the
//! operational loop, please apply this mutation to the running primary".
//!
//! Single concern: a typed, reply-bearing command channel whose receiver
//! is read inside the primary's operational-loop `select!` and whose
//! sender is cloned out to consumers (PyO3 `PrimaryHandle`, future
//! Rust-side control-plane callers). Each command carries a
//! `oneshot::Sender<Result<...>>` so the caller can block / await the
//! handler's outcome and surface success / failure synchronously.
//!
//! Module boundary:
//!   * [`types`] owns the `PrimaryCommand<I>` enum, the per-task
//!     `SpawnError` enum, and `COMMAND_CHANNEL_CAPACITY`. Pure data —
//!     no methods on `PrimaryCoordinator`.
//!   * [`handler`] owns the dispatch entry `handle_primary_command`
//!     and the `apply_*` methods on `PrimaryCoordinator`. Each arm
//!     forwards to one method so the mutation's state-machine
//!     semantics stay together with the rest of the coordinator's
//!     state.
//!   * The operational loop's `select!` arm (`lifecycle.rs`) calls
//!     `handle_primary_command(self, cmd).await` — single line, no
//!     per-variant logic at the call site.
//!
//! What callers see (Python and Rust):
//!   * `mpsc::Sender<PrimaryCommand<I>>` — clone, build a command +
//!     `oneshot::channel()`, `send().await`, then `await` the reply.
//!   * `oneshot::Sender::send`-side error paths on the handler side
//!     are non-fatal: a dropped `reply` receiver just means the caller
//!     stopped caring (e.g. timed out, panicked). No coordinator state
//!     change rolls back on `reply.send(...)` failing.
//!
//! Capacity: the inbound channel is bounded (`COMMAND_CHANNEL_CAPACITY`)
//! so a runaway caller can't OOM the primary. Backpressure surfaces to
//! the sender side as a slow `send().await`; the handler-side reply
//! oneshot is the per-command flow-control signal.
//!
//! # Wire / CRDT effects
//!
//! Each handler routes through the same
//! `apply_and_broadcast_cluster_mutations` primitive the rest of the
//! coordinator uses, so the live primary's local apply and the cluster-
//! wide CRDT broadcast happen together. Variants:
//!   * `FailPermanent` — drives `pending_pool::on_item_failed_permanent`
//!     (with the cascade-to-dependents semantics that primitive owns)
//!     and broadcasts `TaskFailed { kind: NonRecoverable, .. }`.
//!   * `ReinjectTask` — accepts only entries whose CRDT state is the
//!     discrete `TaskState::Unfulfillable { .. }` variant (the
//!     operator-resolvable-failure class); flips the local pool's
//!     Unfulfillable → re-injected, broadcasts `TaskReinjected{hash}`,
//!     and decrements the per-task budget
//!     `unfulfillable_reinject_remaining[hash]` (initialised from
//!     `PrimaryConfig::unfulfillable_reinject_max_per_task`; `None`
//!     means unbounded). Budget exhaustion is a structured-log event,
//!     never a panic.
//!   * `UpdatePreferredSecondaries` — broadcasts
//!     `TaskPreferredSecondariesUpdated{hash, secondaries}` so every
//!     node's mirror sees the same update. Local `TaskInfo`-side
//!     storage of the field lands in Phase 4; the command-variant +
//!     reply path are in place today so the PyO3 surface can ship.

mod handler;
mod types;

#[cfg(test)]
mod tests;

pub use handler::handle_primary_command;
pub use types::{COMMAND_CHANNEL_CAPACITY, PrimaryCommand, SpawnError, validate_spawn_tasks};
