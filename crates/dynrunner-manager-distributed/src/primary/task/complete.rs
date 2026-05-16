
use dynrunner_core::{Identifier, ResourceMap};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};

use crate::primary::PrimaryCoordinator;


impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    pub(crate) async fn handle_task_complete(&mut self, msg: DistributedMessage<I>) {
        if let DistributedMessage::TaskComplete {
            secondary_id,
            worker_id,
            task_hash,
            ..
        } = &msg
        {
            // Dedup gate (#50 peer-forwarding redundancy):
            // peers forward observed-via-peer-mesh TaskCompletes
            // to the primary so wire-loss on the originator's
            // direct send is covered by N-1 redundant paths.
            // First receipt does all side-effects (counter
            // updates, type-slot release, phase cascade, kickstart
            // dispatch, secondary forward); subsequent receipts
            // are no-ops. Without this gate, type_slot would
            // double-decrement, `note_item_completed` would
            // double-fire on the phase machine, and the log line
            // would emit N times per task.
            //
            // `completed_tasks` is the dedup key: it's only
            // populated below (and from `mirror_mutation_to_accounting`
            // on cluster_state broadcasts the primary RECEIVES,
            // which the live primary doesn't normally — see
            // `peer.rs` comment chain). Idempotent via HashSet.
            if self.completed_tasks.contains(task_hash) {
                return;
            }
            let secondary_id = secondary_id.clone();
            let worker_id = *worker_id;
            // A TaskComplete supersedes a prior TaskFailed for the
            // same hash. Without this, a forwarded retry-success
            // from the primary would leave the local primary's
            // `failed_tasks` populated with a hash that was actually
            // retried OK, inflating `failed_count()` reports.
            // Pre-demotion, `run_retry_passes` cleared the entire
            // `failed_tasks` set at the start of a retry pass and
            // added back only the tasks that failed AGAIN; once the
            // local primary stops running retry passes, the per-hash
            // remove here keeps the same invariant: a hash sits in
            // exactly one of {completed, failed}.
            self.failed_tasks.remove(task_hash);
            self.completed_tasks.insert(task_hash.clone());
            // Replicated-ledger update: every node mirrors the
            // post-completion state by applying this mutation. CRDT
            // semantics make duplicate applies (e.g. on a re-broadcast
            // for a task already terminal locally) a NoOp.
            self.apply_and_broadcast_cluster_mutations(vec![
                ClusterMutation::TaskCompleted {
                    hash: task_hash.clone(),
                },
            ])
            .await;

            // A successful TaskComplete from this secondary proves
            // it's healthy — clear any backpressure backoff. The
            // backoff window is short (500ms by default) so this
            // matters mostly for high-throughput runs where one
            // completion lands while a previous backpressure-window
            // is still active.
            self.backpressured_secondaries.remove(&secondary_id);

            // Mark the specific worker idle using secondary_id + local worker_id.
            // Capture the phase + type of the just-finished item so we
            // can fold it into per-phase counters, release the
            // per-type concurrency slot, and run the phase lifecycle
            // cascade.
            let mut completed_meta: Option<(
                dynrunner_core::PhaseId,
                dynrunner_core::TypeId,
                Option<String>,
            )> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        if let Some(task) = w.current_task.take() {
                            completed_meta = Some((
                                task.phase_id.clone(),
                                task.type_id.clone(),
                                task.task_id.clone(),
                            ));
                        }
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            // Operator-facing INFO: enough to grep "did task X
            // complete and how are aggregate counts moving?". The
            // per-task identity fields (task_id / phase / task_type)
            // are diagnostic noise on the routine path — they go to
            // the DEBUG sibling below so debugging keeps the info
            // but the operator log stays terse.
            let outcome = self.outcome_summary();
            tracing::info!(
                secondary = %secondary_id,
                worker_id,
                task_hash = %task_hash,
                succeeded = outcome.succeeded,
                fail_retry = outcome.fail_retry,
                fail_oom = outcome.fail_oom,
                fail_final = outcome.fail_final,
                "task complete"
            );
            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                task_id = ?completed_meta.as_ref().and_then(|(_, _, t)| t.as_deref()),
                phase = ?completed_meta.as_ref().map(|(p, _, _)| p.to_string()),
                task_type = ?completed_meta.as_ref().map(|(_, t, _)| t.to_string()),
                task_hash = %task_hash,
                "task complete: identity"
            );

            if let Some((phase, type_id, task_id)) = completed_meta {
                self.release_type_slot(&type_id);
                self.note_item_completed(&phase, task_id.as_deref());
            }

            // Kickstart dispatch to every idle worker. After
            // `note_item_completed` runs the phase-lifecycle cascade,
            // a previously-Blocked phase may have just transitioned
            // to Active. Workers that have been idle since startup
            // (because their initial TaskRequest got "no work" when
            // the new phase wasn't yet active) won't re-poll on their
            // own — they sent their last TaskRequest already, got
            // nothing, and are waiting for an unsolicited
            // TaskAssignment. Without this kickstart, a 2-phase task
            // graph where phase-N has 1 item and phase-(N+1) has the
            // rest would stall after the phase-N item finishes —
            // the originating secondary's worker DOES re-request via
            // its `request_task_for_worker(0)` in
            // `processing.rs:193`, but every OTHER secondary's
            // workers don't. Same kickstart pattern as
            // `run_retry_passes` uses after re-injection.
            //
            // Idempotent: if no phase advanced (the common case for
            // mid-phase completions where the phase still has queued
            // work), `dispatch_to_idle_workers` finds the soft-pin
            // soft-pin order returns the originating worker first and
            // the kickstart no-ops by definition. If multiple phases
            // cascaded done in one tick (chain of empty phases →
            // first populated phase), every newly-active phase's
            // items are seen.
            self.dispatch_to_idle_workers().await.ok();

            // Belt-and-suspenders: forward to every other secondary
            // so each one's `completed_tasks` cache stays current.
            // The originating secondary already broadcasts
            // peer-to-peer (processing.rs), but that's best-effort;
            // a primary-side forward closes the gap if a peer
            // broadcast was lost. Without this, on local-death-then-
            // failover, a secondary missing the peer broadcast
            // would re-dispatch the already-completed task. (Same
            // failover-survivability invariant the FullTaskList
            // broadcast used to enforce in 04d9012, now achieved
            // continuously via per-completion ClusterMutation
            // broadcasts.)
            self.forward_completion_to_secondaries(&msg, &secondary_id)
                .await;
        }
    }

    /// Send `msg` to every connected secondary except the one that
    /// originated it. Per-secondary failures are logged and continue
    /// — a missed completion forward just risks a re-dispatch on
    /// failover, not a run-wide failure.
    pub(super) async fn forward_completion_to_secondaries(
        &mut self,
        msg: &DistributedMessage<I>,
        origin_secondary_id: &str,
    ) {
        let recipients: Vec<String> = self
            .secondaries
            .keys()
            .filter(|id| id.as_str() != origin_secondary_id)
            .cloned()
            .collect();
        for secondary_id in &recipients {
            if let Err(e) = self
                .transport
                .send_to(secondary_id, msg.clone())
                .await
            {
                tracing::debug!(
                    secondary = %secondary_id,
                    error = %e,
                    "failed to forward task completion; that secondary may re-dispatch on failover"
                );
            }
        }
    }

}
