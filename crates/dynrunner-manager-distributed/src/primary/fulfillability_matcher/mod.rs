//! Operational-loop integration of the fulfillability matcher.
//!
//! Single concern: given one batched [`MatcherBatch`] of holdings
//! updates, walk every `TaskState::Unfulfillable` entry in the CRDT,
//! ask the consumer-installed
//! [`crate::fulfillability_matcher::FulfillabilityMatcher`] predicate
//! whether to reinject, and on `true` enqueue a
//! `PrimaryCommand::ReinjectTask{hash}` onto the coordinator's own
//! command channel. Auto-fired reinjects share the per-task
//! `unfulfillable_reinject_remaining` budget with consumer-explicit
//! `PrimaryHandle::reinject_task` calls; the existing
//! `apply_reinject_task` handler is the single chokepoint for the
//! budget check.
//!
//! Module boundary:
//!   * The trait + the batched-drain pipeline live in
//!     `crate::fulfillability_matcher`. This file is the operational-
//!     loop SIDE of the boundary: it owns `&mut self` access to
//!     `cluster_state` (read) and `command_tx` (write).
//!   * The `select!` arm in `lifecycle.rs` is one line:
//!     `Some(batch) = drain_matcher_batch(rx, idle) => self
//!     .invoke_fulfillability_matcher_batch(batch).await`.
//!
//! Error / exception isolation:
//!   * Each `matcher.should_reinject(...)` call is wrapped in
//!     `std::panic::catch_unwind(AssertUnwindSafe(...))` so a Rust
//!     panic on one task is logged at `warn`, treated as `false`,
//!     and the remaining tasks in the batch still get checked.
//!     Mirrors the peer-lifecycle dispatcher's listener-call
//!     isolation. The PyO3 bridge ALSO swallows `PyErr` paths to
//!     `tracing::warn` before they cross the trait boundary, so
//!     Python exceptions land at `false` without ever reaching the
//!     `catch_unwind` — the catch is the defence against
//!     `pyo3::panic::PanicException` and Rust-side matcher bugs.
//!   * The auto-fired `ReinjectTask` is sent through the coordinator's
//!     OWN command channel — same path as a PyO3 / external caller.
//!     The send is non-blocking (`try_send` would also work; we use
//!     `send().await` because the operational loop already awaits
//!     other arms and the channel capacity is sized for control-plane
//!     bursts). Failure to enqueue (channel full / closed) is logged
//!     at `warn` and the task stays Unfulfillable — the next batch
//!     re-invokes the matcher.

use std::panic::AssertUnwindSafe;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::TaskState;
use crate::fulfillability_matcher::MatcherBatch;
use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;

#[cfg(test)]
mod tests;

impl<Tr, S, E, I> PrimaryCoordinator<Tr, S, E, I>
where
    Tr: PeerTransport<I>,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Process one collapsed batch of holdings-update events.
    ///
    /// Walks `cluster_state.tasks` once, filters to
    /// `TaskState::Unfulfillable { task, reason }`, and for each such
    /// entry calls `matcher.should_reinject(task, reason, holdings)`.
    /// `true` fires `PrimaryCommand::ReinjectTask { hash }` onto the
    /// coordinator's own command channel — the same path a PyO3 /
    /// external caller would use, so the budget check
    /// (`unfulfillable_reinject_remaining`) is enforced once at
    /// `apply_reinject_task` regardless of fire origin.
    ///
    /// Idempotency: re-firing `ReinjectTask` for a hash whose state
    /// has since transitioned off `Unfulfillable` is a NoOp at
    /// `apply_reinject_task` (returns `Err` — silently ignored on the
    /// reply oneshot we drop here because the matcher is fire-and-
    /// forget; the caller has no acknowledgement to wait on). Safe
    /// under bursts that interleave a manual reinject and a
    /// matcher-true on the same hash.
    pub(super) async fn invoke_fulfillability_matcher_batch(
        &mut self,
        batch: MatcherBatch,
    ) {
        // No matcher installed → nothing to do. The select! arm
        // should not be enabled in this case, but the guard is
        // defensive: `set_fulfillability_matcher` is a setter, not a
        // build-time invariant, and a future caller might drop the
        // matcher mid-run.
        let Some(matcher) = self.fulfillability_matcher.as_ref() else {
            return;
        };

        // Collect the (hash, task, reason) tuples that the matcher
        // should be asked about. Done as a Vec materialised before
        // any `command_tx.send` because we need to release the
        // `&self.cluster_state` borrow before firing the auto-
        // reinjects (which conceptually touch state).
        //
        // Clone the hash (string) so the post-walk auto-fire loop
        // owns it; the task + reason are passed to the matcher
        // borrowed and dropped at the end of each iteration.
        let unfulfillable: Vec<String> = {
            let mut accepts: Vec<String> = Vec::new();
            for (hash, state) in self.cluster_state.tasks_iter() {
                let TaskState::Unfulfillable { task, reason } = state else {
                    continue;
                };
                // Per-task panic isolation: a Rust matcher that
                // panics on one task must NOT take down the loop;
                // other Unfulfillable tasks in the same batch still
                // get checked. PyO3 bridges already return `false`
                // on PyErr (the bridge swallows the exception at
                // the FFI boundary); this guard is the Rust-trait-
                // side defence for non-Python matchers and for the
                // rare PyO3 `pyo3::panic::PanicException` that the
                // bridge converts to a Rust panic. Same pattern as
                // the peer-lifecycle dispatcher's listener-call
                // isolation.
                let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    matcher.should_reinject(hash, task, reason, &batch.holdings)
                }));
                match outcome {
                    Ok(true) => accepts.push(hash.clone()),
                    Ok(false) => {}
                    Err(panic) => {
                        let msg = if let Some(s) = panic.downcast_ref::<&'static str>() {
                            (*s).to_string()
                        } else if let Some(s) = panic.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "<non-string panic payload>".to_string()
                        };
                        tracing::warn!(
                            target: "dynrunner_fulfillability_matcher",
                            task_hash = %hash,
                            panic_message = %msg,
                            "fulfillability matcher panicked on task; \
                             treating as false and continuing with the batch",
                        );
                    }
                }
            }
            accepts
        };

        // Auto-fire ReinjectTask for every accept. Same command-
        // channel path consumer-explicit reinjects take; the budget
        // check (and the Unfulfillable-only state gate) live at
        // `apply_reinject_task`. We drop the reply oneshot because
        // the matcher is fire-and-forget — the next holdings update
        // re-invokes the matcher if the reinject didn't take.
        for hash in unfulfillable {
            let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
            let command = PrimaryCommand::ReinjectTask {
                hash: hash.clone(),
                reply: reply_tx,
            };
            if let Err(err) = self.command_tx.send(command).await {
                tracing::warn!(
                    target: "dynrunner_fulfillability_matcher",
                    task_hash = %hash,
                    error = %err,
                    "auto-fire of ReinjectTask failed; command channel \
                     closed or full — task stays Unfulfillable",
                );
            }
        }
    }
}

