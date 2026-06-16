use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, PeerId,
};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::worker_signal::WorkerMgmtSignal;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// How often the per-completion aggregate INFO line ("task complete" +
    /// the moving succeeded/fail_retry/fail_oom/fail_final counts) is
    /// emitted: once every `COMPLETION_LOG_INTERVAL` completions, rather than
    /// on every one. At 46k scale the unthrottled emit (each computing
    /// `outcome_summary()`) was a dominant driver of the multi-GB TRACE
    /// firehose that wedged the runtime. Sampling keeps the moving aggregate
    /// operator-greppable at O(completions / interval) emission while the
    /// per-task identity DEBUG sibling stays per-completion (it is cheap and
    /// the e2e ordering checks key on it). The first completion of every
    /// `interval` window emits, so the aggregate appears promptly at run
    /// start and tracks throughout — never silently absent on a short run.
    const COMPLETION_LOG_INTERVAL: u64 = 64;

    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the cascade so a callback-issued `spawn_tasks`
    /// applies inline before the next `drain_empty_active_phases`
    /// poll. Pre-loop / post-loop callers pass `&mut None`.
    pub(crate) async fn handle_task_complete(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        if let DistributedMessage::TaskComplete {
            target: None,
            secondary_id,
            worker_id,
            task_hash,
            result_data,
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
            // Pre-start fence A side-map drop (#530a): a completion is a
            // terminal — the fence is over for this hash. Symmetric with
            // the in-flight ledger drop in `free_slot_on_terminal` below.
            self.drop_supplanted_holder(task_hash);
            // Replicated-ledger update: every node mirrors the
            // post-completion state by applying this mutation. CRDT
            // semantics make duplicate applies (e.g. on a re-broadcast
            // for a task already terminal locally) a NoOp.
            self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskCompleted {
                hash: task_hash.clone(),
                result_data: result_data.clone(),
                // Stamped at the origination choke point: read from the
                // task's current generation (C-1) so the completion
                // preserves the attempt it completed under.
                attempt: Default::default(),
            }])
            .await;

            // A successful TaskComplete from this secondary proves
            // it's healthy — clear any backpressure backoff. The
            // backoff window is short (500ms by default) so this
            // matters mostly for high-throughput runs where one
            // completion lands while a previous backpressure-window
            // is still active.
            self.backpressured_secondaries.remove(&secondary_id);

            // Resolve the just-finished item BY HASH and free its
            // holding slot through the single terminal-free helper:
            // `free_slot_on_terminal` frees the slot back to `Idle` AND
            // removes the `in_flight` ledger entry ONLY if the addressed
            // slot's held hash equals this `task_hash` (the hash IS the
            // slot's held-task identity — not a reorder-detector). A
            // reordered TaskRequest-then-Complete cannot misfire: the
            // request never freed the slot, so the slot still holds
            // this hash. A Complete for a slot already reassigned to a
            // later task returns `None` (its ledger entry is gone /
            // belongs to a different slot) and is a safe no-op. The
            // entry also unifies the formerly-separate pre-owned
            // (hydrated) in-flight tasks: a completion with no local
            // holding slot is attributed by the ledger entry just the
            // same.
            //
            // `completed_meta` (phase / type / task_id) comes from the
            // freed ledger entry's `task`, not a worker scan. The
            // type-slot release lives inside `free_slot_on_terminal`
            // (paired with the reserve in `commit_assignment`), so the
            // cascade below only runs the per-phase counter; `type_id`
            // is carried purely for the diagnostic DEBUG line.
            //
            // Requeue-raced fallback (run_20260610_221140): a hash absent
            // from the in-flight ledger may instead sit QUEUED in the pool
            // — an earlier failover-recovery requeue (inherited-slot
            // reconciliation / dead-secondary recovery) returned it to
            // `Pending` before this lost terminal's late delivery. Reclaim
            // the queued copy so the completed work is never re-dispatched;
            // the reclaim's `mark_in_flight` compensation lets the SAME
            // `note_item_completed` cascade below account it.
            let completed_meta: Option<(dynrunner_core::PhaseId, dynrunner_core::TypeId, String)> =
                self.free_slot_on_terminal(&secondary_id, worker_id, task_hash)
                    .map(|entry| {
                        (
                            entry.phase,
                            entry.task.type_id.clone(),
                            entry.task.task_id.clone(),
                        )
                    })
                    .or_else(|| {
                        self.reclaim_requeued_on_terminal(task_hash)
                            .map(|t| (t.phase_id.clone(), t.type_id.clone(), t.task_id.clone()))
                    });

            // Operator-facing INFO: enough to grep "did task X
            // complete and how are aggregate counts moving?". The
            // per-task identity fields (task_id / phase / task_type)
            // are diagnostic noise on the routine path — they go to
            // the DEBUG sibling below so debugging keeps the info
            // but the operator log stays terse.
            //
            // THROTTLED (#scaling): emit the moving aggregate once every
            // `COMPLETION_LOG_INTERVAL` completions instead of on every one.
            // At 46k scale the per-completion emit — each computing
            // `outcome_summary()` — drove the multi-GB TRACE firehose that
            // wedged the runtime on the NFS log write. The aggregate is the
            // SAME moving counts on whichever completion samples them, so a
            // sampled line is operator-sufficient; the per-task identity
            // DEBUG sibling stays per-completion below. The counter is
            // post-incremented so the FIRST completion (counter 0) emits,
            // surfacing the aggregate promptly. The line stays on the
            // NORMAL tracing target/level it used today (so the
            // `--important-stdio-only` gate still excludes it — the
            // important-stdio e2e NEGATIVE assertion is preserved) and
            // `outcome_summary()` is computed ONLY when the line is emitted.
            let emit_aggregate = self
                .completion_log_counter
                .is_multiple_of(Self::COMPLETION_LOG_INTERVAL);
            self.completion_log_counter = self.completion_log_counter.wrapping_add(1);
            if emit_aggregate {
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
            }
            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                task_id = ?completed_meta.as_ref().map(|(_, _, t)| t.as_str()),
                phase = ?completed_meta.as_ref().map(|(p, _, _)| p.to_string()),
                task_type = ?completed_meta.as_ref().map(|(_, t, _)| t.to_string()),
                task_hash = %task_hash,
                "task complete: identity"
            );

            if let Some((phase, _type_id, task_id)) = completed_meta {
                self.note_item_completed(&phase, Some(task_id.as_str()), command_rx)
                    .await;
            }

            // A task completed: its worker freed AND a previously-
            // Blocked phase may have just transitioned to Active in the
            // `note_item_completed` cascade. Either way, work may now be
            // dispatchable to a worker that won't re-poll on its own
            // (it sent its last TaskRequest already, got "no work", and
            // is waiting for an unsolicited TaskAssignment). EMIT a
            // `TasksAdded` onto the decoupled worker-management bus
            // rather than calling dispatch directly: phase/task
            // management states "work may be available" and knows
            // nothing of dispatch (the dispatch-decoupling law). The
            // operational loop's worker-management arm coalesces the
            // signal into one batched recheck over every free worker.
            // Without this emit, a 2-phase graph where phase-N has 1
            // item and phase-(N+1) has the rest would stall after the
            // phase-N item finishes (the originating secondary re-
            // requests via `request_task_for_worker(0)`, but every
            // OTHER secondary's workers don't). The negative control
            // test pins this emit as load-bearing.
            self.cluster_state
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);

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
                .send_to(
                    Destination::Secondary(PeerId::from(secondary_id.clone())),
                    msg.clone(),
                )
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
