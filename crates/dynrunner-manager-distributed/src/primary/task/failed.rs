use std::time::Instant;

use dynrunner_core::{Identifier, ResourceMap, TaskInfo};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, DistributedMessage, PeerTransport,
    SecondaryTransport,
};
use dynrunner_scheduler_api::{
    ResourceEstimator, Scheduler,
};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::command_channel::PrimaryCommand;
use crate::primary::PrimaryCoordinator;


impl<T: SecondaryTransport<I>, P: PeerTransport<I>, S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<T, P, S, E, I> {

    /// `command_rx` threads the operational-loop's command-channel
    /// receiver into the cascade so a callback-issued `spawn_tasks`
    /// applies inline before the next `drain_empty_active_phases`
    /// poll. Pre-loop / post-loop callers pass `&mut None`.
    pub(crate) async fn handle_task_failed(
        &mut self,
        msg: DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        if let DistributedMessage::TaskFailed {
            secondary_id,
            worker_id,
            task_hash,
            error_type,
            error_message,
            ..
        } = &msg
        {
            // Dedup gate (#50 peer-forwarding redundancy):
            // Same shape as `handle_task_complete`'s dedup —
            // peers forward observed peer-TaskFailed events to
            // the primary; the first receipt does all the
            // bookkeeping, subsequent ones are no-ops. Without
            // this, `note_item_failed` would double-fire (phase
            // counter goes wrong) and `type_slot` would double-
            // decrement.
            //
            // `failed_tasks` is the dedup key for non-superseded
            // failures. The `completed_tasks` check covers the
            // separate case "TaskFailed arrives late after the
            // task already TaskComplete'd" (the existing
            // line-538 invariant: never regress
            // completed → failed). Backpressure-shaped
            // TaskFailed bypasses both checks since it's a re-
            // queue signal, not a terminal state — handled
            // below at the `is_backpressure` arm where the
            // re-queue is idempotent (the binary either gets
            // requeued or has already been requeued by an
            // earlier landing).
            let dedup_is_backpressure = error_message == "No idle worker available"
                || error_message == "worker pipe broken; respawning"
                || error_message
                    == crate::secondary::resource::NO_FAULT_PREEMPT_WIRE_MESSAGE;
            if !dedup_is_backpressure
                && (self.completed_tasks.contains(task_hash)
                    || self.failed_tasks.contains_key(task_hash))
            {
                return;
            }
            let secondary_id = secondary_id.clone();
            let worker_id = *worker_id;
            let task_hash = task_hash.clone();
            let error_type = error_type.clone();
            let error_message = error_message.clone();
            // Find the specific worker and recover the binary if it's a
            // recoverable error so it can be re-assigned to another worker.
            let mut recovered_binary: Option<TaskInfo<I>> = None;
            let mut local_idx: u32 = 0;
            for w in &mut self.workers {
                if w.secondary_id == secondary_id {
                    if local_idx == worker_id {
                        recovered_binary = w.current_task.take();
                        w.estimated_resources = ResourceMap::new();
                        w.is_idle = true;
                        break;
                    }
                    local_idx += 1;
                }
            }

            // Backpressure detection: secondary's dispatch.rs sends
            // this exact error_message when its `is_idle_state()`
            // check finds every worker non-idle for an inbound
            // TaskAssignment. That's NOT a task failure — the worker
            // never ran the binary; the secondary just couldn't place
            // it. Treat it as a transient backoff signal:
            //   1. Re-queue the binary into the pool (don't lose it).
            //   2. Skip the failed_tasks insert — preserves the
            //      retry budget for actual failures.
            //   3. Mark the secondary as backpressured for a short
            //      window — the kickstart and TaskRequest paths
            //      both consult `is_backpressured()` and skip
            //      dispatch to that secondary's workers until the
            //      window expires or a successful TaskComplete from
            //      that secondary clears the flag.
            //   4. Skip the kickstart at the bottom of this function
            //      — the kickstart's whole point is "another worker
            //      may now be idle"; on backpressure, the failing
            //      secondary's workers are precisely what we DON'T
            //      want to re-target.
            // Without this: a single broken secondary (every
            // assignment to it bouncing as backpressure) consumes
            // the entire retry budget while the kickstart amplifier
            // keeps cycling work back to it. Tokenizer hit this on
            // a 1791-task cohort: 3128 errors all on one secondary,
            // 1511 permanent failures.
            // Two backpressure shapes — both mean "task DIDN'T
            // actually run; requeue at the pool, do not consume
            // retry budget":
            //
            //   1. "No idle worker available" — peer's worker
            //      pool was full at dispatch time.
            //   2. "worker pipe broken; respawning" — peer's
            //      target worker subprocess died between tasks;
            //      pipe-write failed; the peer is respawning.
            //      The not-yet-attempted task comes back with
            //      this marker so the primary requeues into the
            //      pool and re-dispatches once a peer signals
            //      capacity. Without this case Bug C produced
            //      silent task loss on every Broken-pipe assign
            //      attempt at a peer secondary (#46 secondary-
            //      side fix needed this primary-side companion
            //      to actually requeue rather than mark-as-failed).
            // The third marker (`NO_FAULT_PREEMPT_WIRE_MESSAGE`) signals
            // a no-fault scheduler-driven preempt — the secondary's
            // worker was killed by `ResourceStealingScheduler` because
            // the task was opportunistic or the worker was still under
            // its reserved budget. The displaced task is innocent;
            // routing it through the backpressure path (re-queue, no
            // retry-budget consumption) preserves the same contract as
            // the other two markers.
            let is_backpressure = error_message == "No idle worker available"
                || error_message == "worker pipe broken; respawning"
                || error_message
                    == crate::secondary::resource::NO_FAULT_PREEMPT_WIRE_MESSAGE;
            if is_backpressure {
                let backoff_ms = 500;
                self.backpressured_secondaries.insert(
                    secondary_id.clone(),
                    Instant::now() + std::time::Duration::from_millis(backoff_ms),
                );
                tracing::debug!(
                    secondary = %secondary_id,
                    worker_id,
                    backoff_ms,
                    "secondary returned backpressure; re-queuing task and applying backoff"
                );
                if let Some(binary) = recovered_binary {
                    self.release_type_slot(&binary.type_id);
                    self.pool_mut().requeue(binary);
                }
                return;
            }

            // Failure budget: one per task per pass. Recoverable
            // and NonRecoverable both terminate the dispatch slot
            // and add to `failed_tasks`. The `run()` pipeline calls
            // `retry_failed_tasks_pass` after the main operational
            // loop drains, which re-injects everything in
            // `failed_tasks` (clearing the set) and runs the loop
            // again. Up to `config.retry_max_passes` retry passes
            // (default 1) before failures are permanent.
            //
            // Critically NOT counted as a failure: secondary
            // disconnect → `requeue_dead_secondary` puts the
            // in-flight task back into the pool via
            // `pool.requeue` (NOT through this function). The task
            // never reached `failed_tasks`, so its retry budget
            // stays untouched.
            self.failed_tasks
                .insert(task_hash.clone(), error_type.clone());
            // Replicated-ledger update: every node mirrors the
            // post-failure state. The wire `error_type` is now the
            // typed `ErrorType` enum (Phase D), so the CRDT mutation
            // takes it verbatim — no string-token mapping.
            self.apply_and_broadcast_cluster_mutations(vec![
                ClusterMutation::TaskFailed {
                    hash: task_hash.clone(),
                    kind: error_type.clone(),
                    error: error_message.clone(),
                },
            ])
            .await;

            // Operator-facing WARN: per-class for retry/policy
            // decisions (error_type discriminates retry/oom/final);
            // the error message itself is essential for debugging
            // and stays on WARN. Per-task identity (task_id / phase
            // / task_type) is diagnostic noise on this path — it
            // moves to the DEBUG sibling below.
            tracing::warn!(
                secondary = %secondary_id,
                worker_id,
                task_hash = %task_hash,
                error_type = ?error_type,
                error = %error_message,
                "task failed"
            );
            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                task_id = ?recovered_binary.as_ref().map(|b| b.task_id.as_str()),
                phase = ?recovered_binary.as_ref().map(|b| b.phase_id.to_string()),
                task_type = ?recovered_binary.as_ref().map(|b| b.type_id.to_string()),
                task_hash = %task_hash,
                "task failed: identity"
            );

            if let Some(binary) = recovered_binary {
                self.release_type_slot(&binary.type_id);
                self.note_item_failed(&binary.phase_id, Some(binary.task_id.as_str()), command_rx).await;
            } else if let Some((phase, _secondary, binary)) =
                self.pre_owned_in_flight.remove(&task_hash)
            {
                // Pre-owned in-flight task (hydrated from cluster_state)
                // failing on the originating node: no local worker held
                // it, so no local type-slot was taken — hence no
                // `release_type_slot`. We still decrement the correct
                // phase's in-flight counter via `note_item_failed` so
                // the phase machine drains from N+1 to N. Symmetric with
                // the pre-owned fallback in `handle_task_complete`.
                self.note_item_failed(&phase, Some(binary.task_id.as_str()), command_rx).await;
            }

            // Same kickstart rationale as `handle_task_complete`:
            // `note_item_failed` may have just cascaded a phase
            // through Drained → Done and activated a dependent
            // phase; idle workers across other secondaries won't
            // re-poll on their own. Idempotent.
            self.dispatch_to_idle_workers().await.ok();

            // Forward task-terminal outcomes to peer secondaries so
            // their `failed_tasks` / `completed_tasks` caches stay
            // current — required for promoted-primary handoff
            // not to re-dispatch a task we just gave up on. Both
            // Recoverable and NonRecoverable are terminal in the
            // pass-based retry model: the retry pass re-injects into
            // the pool by re-running the operational loop, which is
            // the new "second chance"; an immediate requeue would
            // recreate the busy-loop bug.
            self.forward_completion_to_secondaries(&msg, &secondary_id)
                .await;
        }
    }

}
