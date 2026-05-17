//! Promoted-secondary side of
//! `PrimaryCommand::UpdatePreferredSecondaries`.
//!
//! Single concern: mirror
//! `PrimaryCoordinator::apply_update_preferred_secondaries` so external
//! callers can replace the per-task preferred-secondaries list on the
//! promoted-secondary path. Two side effects: (1) broadcast the CRDT
//! mutation so every node's mirror converges; (2) mirror the new
//! preference onto the live `primary_pending` entry via
//! `update_first_match_in_place` so the next scheduler tick sees the
//! updated preference without waiting for a snapshot-restore cycle.
//!
//! The pool match uses the same wire-canonical hash predicate the
//! primary's path uses, so a future change to the hashing recipe
//! propagates to both sites through the shared
//! `primary::wire::compute_task_hash`.

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::super::SecondaryCoordinator;

impl<PT, P, M, S, E, I> SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Handler for `PrimaryCommand::UpdatePreferredSecondaries` on the
    /// promoted-secondary path. Broadcasts the per-task preferred-
    /// secondaries update so every node's CRDT mirror sees the new
    /// preference list AND mirrors the new list onto the live
    /// `primary_pending` entry so the next scheduler tick reads the
    /// updated preference. The pool stores `TaskInfo<I>` clones
    /// (taken at injection time); without this mirror the CRDT write
    /// would only become visible to the scheduler on a
    /// snapshot-restore cycle — every dispatch between the two would
    /// see the stale preference list.
    pub(in crate::secondary) async fn apply_update_preferred_secondaries(
        &mut self,
        hash: String,
        secondaries: Vec<String>,
    ) -> Result<(), String> {
        if self.cluster_state.task_state(&hash).is_none() {
            return Err(format!(
                "update_preferred_secondaries: unknown task hash {hash}"
            ));
        }
        // Mirror onto the live pool's TaskInfo clone. Done BEFORE the
        // broadcast so a hypothetical synchronous reader of the pool
        // (post-apply, pre-broadcast) sees the new preferences and
        // the CRDT-side mirror simultaneously. The hash-keyed
        // predicate closes over `compute_task_hash`; the pool API
        // takes any predicate so it doesn't have to learn about
        // wire-canonical hashing.
        let target_hash = hash.clone();
        let new_preferences = dynrunner_core::SoftPreferredSecondaries::new(
            secondaries.clone(),
        );
        let matched = if let Some(pool) = self.primary_pending.as_mut() {
            pool.update_first_match_in_place(
                |t| crate::primary::wire::compute_task_hash(t) == target_hash,
                |t| t.preferred_secondaries = new_preferences.clone(),
            )
        } else {
            // Pre-promotion: `primary_pending` is `None`. The CRDT
            // broadcast below still propagates the update; the local
            // pool will be hydrated from `cluster_state` at promotion
            // time and pick up the new preference list then.
            false
        };
        if !matched {
            // The pool may legitimately not hold the binary (in-flight
            // / completed / not yet seeded, or pre-promotion), and
            // that's fine — only queued/blocked items need the live
            // mirror. CRDT side still broadcasts so every replica's
            // `TaskInfo` clone converges on the new preference list.
            tracing::debug!(
                task_hash = %hash,
                "update_preferred_secondaries: hash not present in pool; \
                 CRDT mirror only"
            );
        }
        self.apply_and_broadcast_mutations(vec![
            ClusterMutation::TaskPreferredSecondariesUpdated {
                hash,
                secondaries,
            },
        ])
        .await
    }
}
