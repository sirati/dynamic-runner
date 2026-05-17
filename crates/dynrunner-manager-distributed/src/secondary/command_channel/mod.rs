//! Cross-thread / cross-async-runtime ingress for "from outside the
//! operational loop, please apply this mutation to the running
//! secondary". Symmetric mirror of `primary::command_channel`.
//!
//! Single concern: a typed, reply-bearing command channel whose
//! receiver is read inside the secondary's `process_tasks` `select!`
//! and whose sender is cloned out to consumers (PyO3 `PrimaryHandle`
//! minted from a `PySecondaryCoordinator`, future Rust-side
//! control-plane callers). Each command carries a
//! `oneshot::Sender<Result<...>>` so the caller can block / await the
//! handler's outcome and surface success / failure synchronously.
//!
//! Module boundary:
//!   * The `PrimaryCommand<I>` enum itself is **shared with the primary**
//!     — both coordinators dispatch the same wire-typed commands. The
//!     enum lives in `primary::command_channel::types` and is publicly
//!     re-exported from `crate::primary`. Re-defining it on the
//!     secondary side would force every PyO3 handle to know which
//!     coordinator it talks to; keeping one shared enum lets a
//!     `PyPrimaryHandle` minted from either coordinator round-trip the
//!     same commands.
//!   * [`handler`] owns the dispatch entry `handle_secondary_command`
//!     and the `apply_*` methods on `SecondaryCoordinator`. Each arm
//!     forwards to one method so the mutation's state-machine
//!     semantics stay co-located with the rest of the coordinator's
//!     state. The `apply_spawn_tasks` half already lives in
//!     `secondary/primary/spawn_tasks.rs` (sharing the
//!     `validate_spawn_tasks` rules with the primary); the other three
//!     `apply_*` methods are siblings in this module's `handler.rs`.
//!   * The `process_tasks` `select!` arm calls
//!     `handle_secondary_command(self, cmd).await` — single line, no
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
//! Capacity: the inbound channel reuses the primary's
//! `COMMAND_CHANNEL_CAPACITY` (256) so a noisy caller can't OOM the
//! secondary while still giving multi-command batches some slack
//! before backpressure kicks in. Backpressure surfaces to the sender
//! side as a slow `send().await`; the handler-side reply oneshot is
//! the per-command flow-control signal.
//!
//! # Wire / CRDT effects
//!
//! Each handler routes through `apply_and_broadcast_mutations` (the
//! promoted-secondary's analog of the primary's
//! `apply_and_broadcast_cluster_mutations`). Variants:
//!   * `FailPermanent` — drives `primary_pending::on_item_failed_permanent`
//!     and broadcasts `TaskFailed { kind, error }` plus cascade-paused
//!     dependents (`TaskBlocked` on `Unfulfillable` cascades).
//!     Mirrors `PrimaryCoordinator::apply_fail_permanent` 1:1.
//!   * `ReinjectTask` — accepts only entries whose CRDT state is the
//!     discrete `TaskState::Unfulfillable { .. }` and there's at least
//!     one reinjection ticket left in
//!     `unfulfillable_reinject_remaining[hash]` (initialised from
//!     `SecondaryConfig::unfulfillable_reinject_max_per_task`; `None`
//!     means unbounded). On accept, transition Unfulfillable→Pending
//!     and broadcast `ClusterMutation::TaskReinjected{hash}`. Budget
//!     exhaustion is a structured-log event, never a panic.
//!   * `UpdatePreferredSecondaries` — broadcasts
//!     `TaskPreferredSecondariesUpdated{hash, secondaries}` AND mirrors
//!     the new preference list onto the live `primary_pending` entry
//!     via the shared `update_first_match_in_place` pool primitive.
//!   * `SpawnTasks` — delegates to the existing
//!     `SecondaryCoordinator::apply_spawn_tasks` (in
//!     `secondary/primary/spawn_tasks.rs`). Shared validator with the
//!     primary (`validate_spawn_tasks`), same per-task error shape,
//!     same single-broadcast guarantee.
//!
//! # Acting-as-primary precondition
//!
//! Every handler in this module assumes the secondary is currently
//! acting as primary (`is_primary == true` and `primary_pending` is
//! populated by `populate_primary_from_cluster_state`). Pre-promotion
//! the `select!` arm still drains the channel — the wire boundary
//! cannot tell whether the recipient has been promoted yet — and each
//! per-arm `apply_*` method documents its own behaviour when the
//! preconditions fail (typically returning an `Err` through the reply
//! oneshot so the caller surfaces a typed Python exception). The
//! production caller (`PySecondaryCoordinator`'s `PrimaryHandle`) is
//! held by the Python `on_run_start` callback for the secondary's
//! own `TaskDefinition` instance; it only calls `spawn_tasks` from
//! `on_phase_end`, which fires exclusively post-promotion.

mod handler;

pub(in crate::secondary) use handler::handle_secondary_command;
