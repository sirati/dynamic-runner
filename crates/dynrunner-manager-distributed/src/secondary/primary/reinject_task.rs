//! Promoted-secondary side of `PrimaryCommand::ReinjectTask`.
//!
//! Single concern: mirror `PrimaryCoordinator::apply_reinject_task` so
//! external callers can transition a `TaskState::Unfulfillable` entry
//! back to `Pending` on the promoted-secondary path. The per-task
//! reinject budget lives on the coordinator
//! (`unfulfillable_reinject_remaining`) and is seeded lazily from
//! [`SecondaryConfig::unfulfillable_reinject_max_per_task`].
//!
//! Wire / CRDT effects: same shape the primary produces. Acceptance
//! emits `ClusterMutation::TaskReinjected{hash}`; rejection
//! (wrong-state, unknown hash, exhausted budget) returns `Err` via the
//! reply oneshot and the local state stays put.
//!
//! Budget independence: the primary's and the secondary's
//! `unfulfillable_reinject_remaining` maps are independent. On
//! promotion the freshly-promoted secondary starts with an empty map
//! (`HashMap::new()` from `SecondaryCoordinator::new`); the demoted
//! primary's counter does not transfer. The configured cap is
//! identical (operator passes the same kwarg through both PyO3
//! wrappers), so post-promotion the cap is honoured against a fresh
//! counter — i.e. the budget resets at promotion, deliberately. The
//! authoritative-pool transfer at promotion is an in-process event
//! (no wire round-trip), so the reset is observable only to
//! externally-controlled re-injection — internal retry passes use the
//! separate `primary_retry_budget`.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;
use crate::cluster_state::TaskState;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Handler for `PrimaryCommand::ReinjectTask` on the promoted-
    /// secondary path. Accepts only entries whose CRDT state is the
    /// discrete `TaskState::Unfulfillable { .. }` — the operator-
    /// resolvable-failure class. Decrements the per-task budget; on
    /// exhaustion the local state stays `Unfulfillable` and the
    /// caller receives `Err`.
    pub(in crate::secondary) async fn apply_reinject_task(
        &mut self,
        hash: String,
    ) -> Result<(), String> {
        // Inspect CRDT state first — the local pool isn't indexed by
        // hash, and the discrete-variant gate has to read the
        // authoritative ledger.
        let binary = match self.cluster_state.task_state(&hash) {
            Some(TaskState::Unfulfillable { task, .. }) => task.clone(),
            Some(_) => {
                return Err(format!(
                    "reinject_task: hash {hash} not in Unfulfillable state"
                ));
            }
            None => {
                return Err(format!(
                    "reinject_task: unknown task hash {hash}"
                ));
            }
        };

        // Budget check. None == unbounded (the bypass branch);
        // `Some(0)` means "exhausted, refuse"; `Some(n>0)` decrements
        // and proceeds. The map is initialised lazily — first reinject
        // for a hash seeds the counter from the configured cap.
        let max = self.config.unfulfillable_reinject_max_per_task;
        if let Some(cap) = max {
            let remaining = self
                .unfulfillable_reinject_remaining
                .entry(hash.clone())
                .or_insert(cap);
            if *remaining == 0 {
                tracing::warn!(
                    task_hash = %hash,
                    cap,
                    event = "unfulfillable_reinject_budget_exhausted",
                    "reinject budget exhausted for task; staying Failed"
                );
                return Err(format!(
                    "reinject_task: budget exhausted for hash {hash} \
                     (cap={cap})"
                ));
            }
            *remaining -= 1;
        }

        // Local pool reinject: same primitive the retry-pass code path
        // uses. Re-injecting flips Drained/Done phase state back to
        // Active for this binary's phase, putting the item back into
        // the bucket head so the next dispatch tick picks it up.
        // `primary_pending` may be `None` pre-promotion — the
        // originator's broadcast still goes out so the CRDT mirror
        // moves off Unfulfillable; the local pool re-injection is a
        // silent skip in that branch (the post-promotion hydration
        // step `populate_primary_from_cluster_state` will pick up the
        // freshly-Pending entry from the CRDT when it runs).
        self.primary_failed.remove(&hash);
        if let Some(pool) = self.primary_pending.as_mut() {
            pool.reinject(binary);
        }

        // Broadcast so every node's CRDT mirror moves the entry off
        // `Failed` synchronously.
        self.apply_and_broadcast_mutations(vec![
            ClusterMutation::TaskReinjected { hash },
        ])
        .await
    }
}
