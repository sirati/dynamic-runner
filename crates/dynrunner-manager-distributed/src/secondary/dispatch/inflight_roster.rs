//! The #518 worker-source-of-truth seam on the secondary side.
//!
//! Single concern: a member is the source of truth for what ITS workers
//! run. This module owns the two frames that let the primary reconcile a
//! cross-member duplicate after a FALSE death declaration:
//!
//!   - [`SecondaryCoordinator::report_inflight_roster`] answers the
//!     primary's `RequestInFlightRoster` — sent the moment the primary
//!     RE-ADMITS this falsely-removed-but-alive member — with the tasks
//!     this node's workers are ACTUALLY running, read off the live
//!     own-worker `active_tasks` bookkeeping (the same truth source the
//!     #308 hold-probe and the #517 incumbent report read).
//!
//!   - [`SecondaryCoordinator::withdraw_task`] honors the primary's
//!     `WithdrawTask` — the directive to stand down a DUPLICATE copy the
//!     primary requeued onto this member while the original holder was
//!     falsely dead. It drops a copy that has NOT yet started running (a
//!     `pending_first_bind` deferral); a copy already executing on a
//!     worker is LEFT IN PLACE — there is no mid-run worker abort, and
//!     clearing its `active_tasks` entry would orphan the worker's
//!     eventual terminal. The primary's hash-keyed terminal-dedup absorbs
//!     that residual terminal, so a copy that already started wastes
//!     compute but never corrupts accounting.

use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, InFlightRosterEntry};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use super::super::wire::timestamp_now;
use dynrunner_core::Identifier;

impl<M, S, E, I> SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Answer the primary's `RequestInFlightRoster` (#518): report the
    /// tasks this node's workers are ACTUALLY running, so the primary can
    /// recognise this member as the authoritative holder and withdraw any
    /// requeued duplicate it dispatched onto another member.
    ///
    /// The roster is read off the live own-worker `active_tasks` map
    /// (`hash -> worker_id`), each entry's structured identity resolved
    /// from the holding worker's `current_binary` — the same truth source
    /// the #517 incumbent report and the #308 hold-probe consume. A worker
    /// whose `current_binary` is absent (a transition window) is skipped:
    /// the roster carries only entries the primary can re-seat by hash +
    /// identity. The reply stamps this node's CURRENT membership generation
    /// (`peer_member_gen`) so the primary can reject a roster that crossed a
    /// re-removal in flight.
    pub(in crate::secondary) async fn report_inflight_roster(&mut self) {
        // Snapshot (hash, worker_id) from the live own-worker bookkeeping
        // BEFORE the pool borrow below (the map and the pool are disjoint
        // fields, but taking the pairs by value first keeps the borrows
        // non-overlapping). `active_tasks_mut` is the single accessor that
        // reaches the map from whichever state carries it.
        let pairs: Vec<(String, u32)> = self
            .active_tasks_mut()
            .iter()
            .map(|(hash, &wid)| (hash.clone(), wid))
            .collect();

        let mut entries: Vec<InFlightRosterEntry<I>> = Vec::with_capacity(pairs.len());
        for (hash, worker_id) in pairs {
            // Resolve the running task's structured identity from the
            // holding worker's `current_binary` — the same read
            // `select_honored_target_or_bounce` (#517) uses for the
            // incumbent. A worker without a `current_binary` (a transition
            // window) yields no identity, so the entry is dropped (the
            // primary needs the identity for the ledger re-seat).
            let task_id = self
                .pool_ref()
                .and_then(|p| p.workers.get(worker_id as usize))
                .and_then(|w| w.current_binary.as_ref())
                .map(|t| t.identifier.clone());
            if let Some(task_id) = task_id {
                entries.push(InFlightRosterEntry {
                    hash,
                    worker_id,
                    task_id,
                });
            }
        }

        let member_gen = self
            .cluster_state
            .peer_member_gen(&self.config.secondary_id);
        let reported = entries.len();
        let msg = DistributedMessage::InFlightRoster {
            target: None,
            sender_id: self.config.secondary_id.clone(),
            timestamp: timestamp_now(),
            secondary_id: self.config.secondary_id.clone(),
            member_gen,
            entries,
        };
        // Report to the primary role only — the authority owns the
        // reconciliation; the same contract as the #517 bounce.
        if let Err(e) = self.send_to_primary(msg).await {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                error = %e,
                "in-flight roster reply to the primary failed; the primary's \
                 re-admission dedup falls back to terminal-dedup for any \
                 cross-member duplicate"
            );
            return;
        }
        tracing::info!(
            secondary = %self.config.secondary_id,
            member_gen,
            reported,
            "answered the primary's in-flight roster request (re-admission \
             source-of-truth report)"
        );
    }

    /// Honor the primary's `WithdrawTask` (#518): stand down a DUPLICATE
    /// copy the primary requeued onto this member while the original holder
    /// was falsely declared dead.
    ///
    /// WITHDRAWABLE: a copy still parked in `pending_first_bind` (a
    /// respawn-HOLD deferral) has NOT been dispatched to a worker — dropping
    /// it cleanly releases the duplicate with no orphaned terminal.
    ///
    /// NOT WITHDRAWABLE (left in place): a copy already in `active_tasks` is
    /// RUNNING on a worker process. There is no mid-run worker abort
    /// (`abort_poll_task` cannot retract a message the worker already
    /// received), and clearing the `active_tasks` entry would orphan the
    /// worker's eventual `Disconnected`/terminal event. So the running copy
    /// is left to complete; the primary's hash-keyed terminal-dedup
    /// (`completed_tasks` / `failed_tasks`) absorbs its terminal. The
    /// withdraw therefore eliminates the double-RUN whenever the copy has
    /// not yet started, and degrades to "wasted compute, correct
    /// accounting" only for the already-started window.
    pub(in crate::secondary) fn withdraw_task(&mut self, msg: &DistributedMessage<I>) {
        let DistributedMessage::WithdrawTask {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } = msg
        else {
            return;
        };
        // Only act if addressed to us (the wire is directed but the
        // role-demux delivers the frame to this node's secondary slot;
        // confirm the named member is this node).
        if secondary_id != &self.config.secondary_id {
            return;
        }

        // A not-yet-started deferral is the cleanly-withdrawable case: drop
        // it and release its parked worker. `pending_first_bind` keys on
        // the worker id; match its `file_hash` to the withdrawn hash so a
        // stale withdraw for a different deferral is a safe no-op.
        let pending_matches = self
            .op_ref()
            .and_then(|op| op.pending_first_bind.get(worker_id))
            .is_some_and(|p| &p.file_hash == task_hash);
        if pending_matches {
            self.op_mut().pending_first_bind.remove(worker_id);
            tracing::warn!(
                secondary = %self.config.secondary_id,
                worker_id,
                task_hash = %task_hash,
                "withdrew a not-yet-started DUPLICATE copy (deferred first-bind) \
                 per the primary's #518 reconciliation; the authoritative \
                 original keeps running it"
            );
            return;
        }

        // Already running (or unknown): cannot abort mid-run. Leave it; the
        // primary's terminal-dedup absorbs the eventual terminal.
        if self.lifecycle.holding_worker(task_hash).is_some() {
            tracing::warn!(
                secondary = %self.config.secondary_id,
                worker_id,
                task_hash = %task_hash,
                "WithdrawTask for a DUPLICATE copy that is ALREADY RUNNING on a \
                 worker; cannot abort mid-run — leaving it to complete (the \
                 primary's hash-keyed terminal-dedup absorbs its terminal). \
                 Double-execution wasted compute on this copy but accounting \
                 stays correct"
            );
        } else {
            tracing::debug!(
                secondary = %self.config.secondary_id,
                worker_id,
                task_hash = %task_hash,
                "WithdrawTask for a hash this node no longer holds (it already \
                 terminated or was never dispatched here); no-op"
            );
        }
    }
}
