use std::time::Instant;

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::worker_signal::WorkerMgmtSignal;

/// THE single classifier for a BACKPRESSURE-shaped `TaskFailed`: a
/// requeue signal, not a terminal failure — the task never ran to
/// completion anywhere, so it re-enters the pool and consumes no retry
/// budget. One predicate (previously the same marker list was inlined
/// twice in `handle_task_failed`) so the dedup gate and the requeue arm
/// can never drift, and a new marker is added in exactly one place.
///
/// The recognised markers:
///   * `"No idle worker available"` — the peer's worker pool was full
///     at dispatch time.
///   * `"worker pipe broken; respawning"` — the peer's target worker
///     subprocess died between tasks; the pipe-write never landed.
///   * [`crate::secondary::resource::NO_FAULT_PREEMPT_WIRE_MESSAGE`] —
///     a no-fault scheduler-driven preempt displaced the task.
///   * [`crate::primary::reconciliation_probe::RECONCILIATION_LOST_WIRE_MESSAGE`]
///     — the reconciliation probe's holder positively denied holding
///     the task (#308): its terminal will never come, but it did not
///     FAIL anywhere, so it requeues without burning retry budget.
///   * [`crate::secondary::TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE`] —
///     the pre-start fence B (#530b) refused a dispatch naming a stale
///     incarnation of the addressee secondary. The lease was wholly
///     invalid; the task never ran, so it requeues for re-dispatch
///     under the live incarnation without burning retry budget.
///
/// Deliberately NOT in this set:
/// [`crate::secondary::TASK_ALREADY_HELD_WIRE_MESSAGE`] — the
/// duplicate-assignment coherence report ("I am ALREADY RUNNING that
/// hash"). It is the requeue's exact OPPOSITE: the task must STAY in
/// flight on the holder (a requeue re-arms the post-failover assign
/// loop this marker exists to break), so `handle_task_failed`
/// recognises it FIRST and routes to
/// [`PrimaryCoordinator::note_task_already_held`] before any
/// slot/ledger mutation.
fn is_backpressure_shaped(error_message: &str) -> bool {
    error_message == "No idle worker available"
        || error_message == "worker pipe broken; respawning"
        || error_message == crate::secondary::resource::NO_FAULT_PREEMPT_WIRE_MESSAGE
        || error_message == crate::primary::reconciliation_probe::RECONCILIATION_LOST_WIRE_MESSAGE
        || error_message == crate::secondary::TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
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
            target: None,
            secondary_id,
            worker_id,
            task_hash,
            error_type,
            error_message,
            ..
        } = &msg
        {
            // Duplicate-assignment coherence report — recognised BEFORE
            // the dedup gate and BEFORE `free_slot_on_terminal`: the
            // holder is ALREADY RUNNING the hash, so neither the slot nor
            // the ledger nor the pool may be touched (see
            // `note_task_already_held` for the two-case contract). Falling
            // through would account a still-running task as a terminal
            // failure.
            if error_message == crate::secondary::TASK_ALREADY_HELD_WIRE_MESSAGE {
                self.note_task_already_held(secondary_id, *worker_id, task_hash)
                    .await;
                return;
            }
            // Pre-start fence A reply (#530a): the addressee REFUSED to start
            // a duplicate copy because the original (supplanted) holder is
            // alive again at gen >= supplanted gen. The supplanted holder
            // identity lives in this primary's `supplanted_holders` side-map
            // (the dispatch's fence stamp came from there); route the
            // reconcile through the existing already-held primitive, naming
            // the SUPPLANTED HOLDER as the authoritative one. The
            // already-held CASE-1b path withdraws the duplicate from the
            // ledger's current member (the rejecting addressee) and re-seats
            // it onto the supplanted holder. A side-map miss (a primary that
            // never minted the hint — e.g. a reply that crossed a failover)
            // is a defensive no-op: there is nothing on this primary to
            // reconcile against, and the eventual real terminal from the
            // genuinely-live holder still settles via the existing #518
            // post-start dedup machinery.
            if error_message == crate::secondary::TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE {
                if let Some((supplanted_id, _gen)) =
                    self.supplanted_holders.get(task_hash).cloned()
                {
                    tracing::warn!(
                        rejecting_member = %secondary_id,
                        supplanted_holder = %supplanted_id,
                        task_hash = %task_hash,
                        "fence A reply (#530a): the addressee refused to start a \
                         duplicate copy because the supplanted holder is alive \
                         again; reconciling the ledger onto the supplanted holder \
                         and withdrawing the duplicate from the addressee"
                    );
                    // worker_id 0: the supplanted holder's actual worker is
                    // unknown here; it is diagnostic-only in the re-seat path
                    // and the eventual terminal from the live holder resolves
                    // the holder by ledger entry, not the re-seat worker id.
                    self.note_task_already_held(&supplanted_id, 0, task_hash)
                        .await;
                } else {
                    tracing::debug!(
                        rejecting_member = %secondary_id,
                        task_hash = %task_hash,
                        "fence A reply (#530a) for a hash with no side-map \
                         entry on this primary (a failover lost the hint, or \
                         a spurious reply); no reconcile — best-effort #518 \
                         post-start dedup remains"
                    );
                }
                return;
            }
            // AFFINE failure (Model B): an affine task is the per-secondary
            // IMPORT primitive — its terminal is per-secondary (the bitvector
            // cell, Q1 non-sticky), phase-NEUTRAL, and consumes NO global retry
            // budget (affine is not phase-completion work; the dependent
            // re-routes to another secondary via re-placement, not a global
            // cascade). Handled OUTSIDE the global `failed_tasks` dedup for the
            // same per-secondary-rerun reason `handle_affine_task_complete` is.
            //
            // The gate fires for EVERY affine hash regardless of backpressure
            // shape — symmetric with the `handle_task_complete` affine gate
            // (which is also unconditional on the affine-id) — because the
            // affine subsystem is the SOLE owner of the per-secondary bitvector
            // cell, the one piece of state a bounce must mutate. The
            // backpressure-vs-genuine-terminal split is made INSIDE
            // `handle_affine_task_failed`: a backpressure bounce of an import
            // resets its cell `Queued → NotDone` (it never ran here, so the
            // dependent must re-derive the import via `StrandedHere`, not see a
            // `Failed`/`InFlightHere` wedge); a genuine terminal flips the cell
            // `Failed`. Routing a backpressure-shaped affine bounce through the
            // generic work-pool requeue arm below would `pool.requeue` a
            // `SecondaryAffine` task — which `is_worker_assignable() == false`
            // can never re-surface — AND leave its cell `Queued`, the original
            // `InFlightHere` wedge.
            if self.cluster_state.affine_id_for_hash(task_hash).is_some() {
                // `Box::pin` the affine sub-handler so its future does not
                // inline into `handle_task_failed`'s state machine (the
                // relocation-future stack-depth sensitivity; see the twin in
                // `complete.rs`).
                Box::pin(self.handle_affine_task_failed(&msg, command_rx)).await;
                return;
            }
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
            let is_backpressure = is_backpressure_shaped(error_message);
            if !is_backpressure
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
            // Resolve the held binary BY HASH and free its holding slot
            // through the single terminal-free helper.
            // `free_slot_on_terminal` frees the slot back to `Idle`,
            // releases the per-type concurrency slot, and removes the
            // `in_flight` ledger entry ONLY if the addressed slot's held
            // hash equals this `task_hash`; it returns the freed entry
            // (or the inherited pre-owned entry) so both the
            // backpressure-requeue and terminal-failure arms below can
            // recover the binary. A stale TaskFailed for a slot already
            // reassigned to a later task returns `None` — a safe no-op.
            // Both terminal-shaped paths in this function
            // (backpressure-requeue, terminal TaskFailed) share this
            // one resolution.
            let recovered_binary: Option<std::sync::Arc<TaskInfo<I>>> = self
                .free_slot_on_terminal(&secondary_id, worker_id, &task_hash)
                .map(|entry| entry.task);

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
            // The recognised marker set — every shape meaning "the task
            // DIDN'T actually run to a terminal anywhere; requeue at
            // the pool, do not consume retry budget" — is the single
            // [`is_backpressure_shaped`] predicate above (computed once
            // before the dedup gate; this arm and the gate share it so
            // they can never drift). See the predicate's doc for the
            // per-marker rationale (#46 Bug C, no-fault preempt, the
            // #308 reconciliation-probe loss).
            if is_backpressure {
                let backoff_ms = 500;
                self.backpressured_secondaries.insert(
                    secondary_id.clone(),
                    Instant::now() + std::time::Duration::from_millis(backoff_ms),
                );
                // Terminal veto (the requeue-BEFORE-terminal race twin of
                // `reconcile_inherited_slot`'s `VetoedByTerminal`): a
                // backpressure bounce is a re-queue heuristic ("the task
                // never ran here, return it to Pending"), but if the
                // replicated ledger ALREADY records a terminal for this
                // hash — the genuine completion was delivered out-of-band
                // while this stale bounce was in flight — re-queueing it
                // would re-dispatch completed work. Settle the residue
                // through the ONE canonical CRDT-terminal path instead,
                // never re-queue. `task_view` (not `task_state`) so a
                // SETTLED/spilled terminal still vetoes. This is the
                // symmetric pair to the terminal-handler settle: the
                // handler covers terminal-AFTER-requeue, this covers
                // terminal-BEFORE-(this)-requeue.
                if self
                    .cluster_state
                    .task_view(&task_hash)
                    .is_some_and(|v| v.is_terminal())
                {
                    tracing::info!(
                        secondary = %secondary_id,
                        worker_id,
                        task_hash = %task_hash,
                        "backpressure re-queue VETOED: the replicated ledger \
                         already records a terminal for this hash; settling \
                         the residue instead of re-queueing completed work"
                    );
                    self.settle_local_state_on_crdt_terminal(&task_hash, command_rx)
                        .await;
                    return;
                }
                tracing::debug!(
                    secondary = %secondary_id,
                    worker_id,
                    backoff_ms,
                    "secondary returned backpressure; re-queuing task and applying backoff"
                );
                // The slot + ledger + type-slot were already freed by
                // `free_slot_on_terminal` above; backpressure means the
                // task never ran, so requeue it into the pool (which
                // decrements the phase's in-flight counter and re-adds
                // the binary at the front of its bucket). No
                // `release_type_slot` here — the helper already did it.
                if let Some(binary) = recovered_binary {
                    // Affine-aware requeue (the SINGLE recovery seam): a plain
                    // `pool.requeue` of an affine-DEPENDENT work task would
                    // strand it — hidden from the global view by
                    // `has_affine_dep`, absent from every affine queue, and
                    // blocked from re-placement by the scheduler's `placed_work`
                    // dedup. `requeue_affine_aware` clears that guard (the #646
                    // twin for affine-dep work) so the same-tick `TasksAdded`
                    // recheck re-derives + re-queues its per-secondary unit; a
                    // non-affine-dep task takes the unchanged `pool.requeue`.
                    self.requeue_affine_aware(binary);
                    // Originate the replicated `InFlight → Pending`
                    // transition in lockstep with the local pool requeue —
                    // the SAME `TaskRequeued` origination every other
                    // requeue path emits (`recover_inflight_for_dead_
                    // secondary`, `reconcile_inherited_slot`). Without it
                    // the bounced task sat queued locally while every
                    // replica still recorded it `InFlight` on the bounced
                    // worker: fail-safe (a failover's hydrate routes the
                    // stale `InFlight` to the in-flight ledger and only the
                    // reconciliation probe's denial recovers it) but
                    // incoherent. Gated on the recovery actually having
                    // requeued (`recovered_binary` was `Some`): a duplicate
                    // bounce resolves no slot and must not re-originate
                    // against what may already be a NEWER assignment. The
                    // apply rule is `InFlight`-only, so a raced terminal
                    // that landed first wins (NoOp).
                    self.apply_and_broadcast_cluster_mutations(vec![
                        ClusterMutation::TaskRequeued {
                            hash: task_hash.clone(),
                            // Stamped at the origination choke point
                            // (apply_locally_for_broadcast).
                            version: Default::default(),
                        },
                    ])
                    .await;
                    // The requeued binary is a pool-entry edge. The recheck
                    // signal it emits depends on WHY the task bounced (#652):
                    //
                    //   * GENUINE CAPACITY exhaustion ("No idle worker available"
                    //     — the secondary's workers are busy with OTHER work): the
                    //     secondary is at CAPACITY, so the recheck must NOT
                    //     re-target it (re-dispatching to its just-freed model-slot
                    //     would bounce again → recheck → the 24k-redispatch
                    //     hot-loop, the general-dispatch sibling of the affine spin
                    //     and the brake the deleted DispatchBackoff used to
                    //     provide). Emit `TasksReadyBackpressureAware` → a
                    //     bypass=FALSE recheck: the per-secondary gate skips ONLY
                    //     the bounced secondary (other idle secondaries still take
                    //     the task immediately); the bounced secondary resumes on
                    //     its next REAL capacity event (a genuine terminal or a
                    //     capacity-growth, both of which clear its flag) — the
                    //     500ms timer is a pure bounded fallback.
                    //   * EVERY OTHER bounce shape (worker pipe broken/respawning,
                    //     no-fault preempt, reconciliation-loss, stale addressee
                    //     gen): the secondary is NOT capacity-exhausted — its
                    //     worker DIED / was preempted / the lease was stale — so it
                    //     must recover PROMPTLY on the same secondary once its
                    //     worker is back (it is the only place a respawned worker's
                    //     local state lives). Emit a genuine `TasksAdded` →
                    //     bypass=TRUE recheck (the unchanged pre-#652 behaviour),
                    //     so the respawn/preempt recovery is not held behind the
                    //     500ms window. The capacity hot-loop never arises for these
                    //     shapes — they do not repeat per-tick like an at-capacity
                    //     secondary does.
                    //
                    // Decoupled emit, never a direct dispatch call.
                    let signal = if error_message == "No idle worker available" {
                        WorkerMgmtSignal::TasksReadyBackpressureAware
                    } else {
                        WorkerMgmtSignal::TasksAdded
                    };
                    self.cluster_state.emit_worker_mgmt(signal);
                }
                return;
            }

            // Requeue-raced fallback (run_20260610_221140, the failure
            // twin of `handle_task_complete`'s): a hash absent from the
            // in-flight ledger may instead sit QUEUED in the pool — an
            // earlier failover-recovery requeue returned it to `Pending`
            // before this lost terminal's late delivery. Reclaim the
            // queued copy (with the `mark_in_flight` compensation) so the
            // terminal-failure cascade below accounts it and the failed
            // attempt is never silently re-dispatched alongside its retry
            // bookkeeping. AFTER the backpressure arm: a backpressure
            // bounce means the task never ran — a queued copy there is
            // exactly where it belongs.
            let recovered_binary =
                recovered_binary.or_else(|| self.reclaim_requeued_on_terminal(&task_hash));

            // Failure budget: one per task per pass. Recoverable
            // and NonRecoverable both terminate the dispatch slot
            // and add to `failed_tasks`. Retry is the per-phase
            // drain-edge bucket machinery (`try_run_phase_retry_bucket`
            // inside `process_phase_lifecycle`): the Recoverable bucket
            // re-injects matching `failed_tasks` entries for up to
            // `config.retry_max_passes` passes (default 1); once the
            // buckets decline, `finalize_phase_soft_failures` makes the
            // surviving failures permanent (cascading their dependents).
            //
            // Critically NOT counted as a failure: secondary
            // disconnect → `requeue_dead_secondary` puts the
            // in-flight task back into the pool via
            // `pool.requeue` (NOT through this function). The task
            // never reached `failed_tasks`, so its retry budget
            // stays untouched.
            self.failed_tasks
                .insert(task_hash.clone(), error_type.clone());
            // Pre-start fence A side-map drop (#530a): a terminal failure
            // ends the fence for this hash. Symmetric with the in-flight
            // ledger drop in `free_slot_on_terminal` above. NOT done on the
            // backpressure-requeue arm above (the hash re-enters the pool,
            // so the next dispatch must still be fenced) and NOT done on
            // the already-held arm (the task stays in flight on the holder).
            self.drop_supplanted_holder(&task_hash);
            // Genuine terminal (#497): a real `TaskFailed` settles the hash,
            // so clear its per-task reconciliation-loss requeue counter. NOT
            // done on the backpressure-requeue arm above (the reconciliation-
            // LOST requeue is exactly what the counter exists to count) —
            // only this terminal arm clears it. A cap-driven NonRecoverable
            // terminal also lands here, harmlessly clearing its own count.
            self.recon_prober.clear_requeues(&task_hash);
            // Replicated-ledger update: every node mirrors the
            // post-failure state. The wire `error_type` is now the
            // typed `ErrorType` enum (Phase D), so the CRDT mutation
            // takes it verbatim — no string-token mapping.
            self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskFailed {
                hash: task_hash.clone(),
                kind: error_type.clone(),
                error: error_message.clone(),
                // Both stamped at the origination choke point
                // (apply_locally_for_broadcast): `version` minted,
                // `attempt` read from the task's current generation (C-1).
                version: Default::default(),
                attempt: Default::default(),
            }])
            .await;

            // AF-sched terminal→bitvector seam (design point 7, Q1 non-sticky):
            // an affine build that failed on this secondary marks its cell
            // Failed (10). `None` for an ordinary work task. Failed is NOT
            // sticky under the AF-id LWW lattice — a later re-route/retry's
            // Queued/Done overrides it — so a dependent re-routes to another
            // secondary via the ordinary cascade/re-placement, never a new
            // affine-specific wedge.
            if let Some(m) = self.affine_terminal_mutation(&secondary_id, &task_hash, false) {
                self.apply_and_broadcast_cluster_mutations(vec![m]).await;
            }

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

            // Terminal failure: the slot + ledger + type-slot were
            // already freed by `free_slot_on_terminal` above. Run only
            // the per-phase cascade so the phase machine drains the
            // in-flight counter (N+1 → N) and any dependent phase that
            // just became unblocked cascades. Covers both
            // locally-dispatched and inherited (pre-owned) entries
            // uniformly — the latter simply had no slot/type-slot to
            // free, matching the deleted pre-owned fallback's contract.
            // The error kind rides along so the pool records the
            // retry-pending failure marker (blocked dependents doomed by
            // this task stop holding the drain edge hostage; the edge's
            // buckets then decide reinject-or-finalize) — see
            // `note_item_failed` for the routing rules.
            if let Some(binary) = recovered_binary {
                self.note_item_failed(
                    &binary.phase_id,
                    Some(binary.task_id.as_str()),
                    Some(&error_type),
                    command_rx,
                )
                .await;
            }

            // Same rationale as `handle_task_complete`: this terminal
            // freed a worker AND `note_item_failed` may have cascaded a
            // phase Drained → Done and activated a dependent phase; free
            // workers across other secondaries won't re-poll on their
            // own. EMIT a `TasksAdded` onto the decoupled bus rather
            // than calling dispatch directly (the dispatch-decoupling
            // law); the worker-management arm coalesces it into one
            // batched recheck.
            self.cluster_state
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);

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

    /// Handle a `TaskFailed` for an AFFINE task (Model B) — a per-secondary
    /// IMPORT terminal. SEPARATE from the work-task path: the affine task runs
    /// per-secondary, so its terminal is per-secondary (the bitvector cell) and
    /// phase-NEUTRAL — it consumes no global retry budget and runs no
    /// `note_item_failed` cascade (the dependent work task is not blocked on a
    /// global affine terminal; it re-routes via the placement trigger, whose
    /// rank reads the bitvector). Worker bookkeeping
    /// (`free_affine_slot_on_terminal`) applies to every per-secondary run.
    ///
    /// The single split this function owns: a BACKPRESSURE-shaped bounce is NOT
    /// a terminal — the import never ran on this secondary (a worker-pipe
    /// respawn, a full pool, a no-fault preempt …), so it RESETS the cell
    /// `Queued → NotDone` (the same `SecondaryCellUnqueued` mutation `steal_for`
    /// emits when a donor relinquishes a queued claim). The dependent work unit
    /// sitting `InFlightHere` at the front of this secondary's affine queue then
    /// reads `NotDone` on its next pop → `StrandedHere` →
    /// `dispatch_affine_import_on_demand` re-runs the import on a Ready worker.
    /// A genuine terminal failure instead flips the cell `Failed` (Q1
    /// non-sticky) and fast-fails any now-`Unsatisfiable` dependents. The
    /// backpressure case must NEVER `pool.requeue` the import (the generic
    /// work-pool arm in `handle_task_failed`): a `SecondaryAffine` task is not
    /// `is_worker_assignable`, so the work pool can never re-surface it, and the
    /// cell would stay `Queued` — the original permanent `InFlightHere` wedge.
    async fn handle_affine_task_failed(
        &mut self,
        msg: &DistributedMessage<I>,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let DistributedMessage::TaskFailed {
            secondary_id,
            worker_id,
            task_hash,
            error_message,
            ..
        } = msg
        else {
            return;
        };
        let secondary_id = secondary_id.clone();
        let worker_id = *worker_id;
        let task_hash = task_hash.clone();
        let is_backpressure = is_backpressure_shaped(error_message);

        if is_backpressure {
            // BACKPRESSURE BOUNCE: the import never ran here. Reset the cell
            // `Queued → NotDone` so the dependent re-derives the import via the
            // `StrandedHere` gate, rather than waiting forever on a `Queued`
            // (`InFlightHere`) cell whose terminal already left as this bounce.
            // `affine_unqueue_mutation` is `None` only for a hash with no
            // affine-id — but the caller already gated on `affine_id_for_hash`,
            // so it is `Some` here; the `if let` is defensive. NO `Failed`
            // flip, NO `fast_fail` (the import is recoverable), and NO
            // `drop_supplanted_holder` (symmetric with the work-pool
            // backpressure arm: the hash re-enters, so the next on-demand
            // dispatch must still be fenced).
            if let Some(m) = self.affine_unqueue_mutation(&secondary_id, &task_hash) {
                self.apply_and_broadcast_cluster_mutations(vec![m]).await;
            }
            // PER-SECONDARY UNBLOCK on bounce (#652 concern B): a work blocked
            // on this import here is waiting on a cell that just went `Queued →
            // NotDone` (the import never ran). Re-enqueue it so it re-pops
            // `StrandedHere` and re-derives the import on-demand — otherwise it
            // would wait forever on a cell no terminal will ever flip `Done`
            // (the bounce already consumed the only in-flight run). Reuses the
            // cell-finished re-enqueue seam (it re-appends + re-gates uniformly).
            if let Some(affine_id) = self.cluster_state.affine_id_for_hash(&task_hash) {
                self.reenqueue_affine_unblocked_on_cell(&secondary_id, affine_id)
                    .await;
            }
            tracing::debug!(
                secondary = %secondary_id,
                worker_id,
                task_hash = %task_hash,
                error = %error_message,
                "affine import BOUNCED (backpressure) on secondary; cell reset \
                 Queued → NotDone — dependent re-derives the import on-demand"
            );
        } else {
            // GENUINE TERMINAL: mark this secondary's cell Failed (per-secondary,
            // non-sticky — a later re-route/retry's Queued/Done overrides it).
            if let Some(m) = self.affine_terminal_mutation(&secondary_id, &task_hash, false) {
                self.apply_and_broadcast_cluster_mutations(vec![m]).await;
            }
            // EVENT-DRIVEN BATCH fast-fail: if this failure made the affine gate
            // all-eligible-`Failed` (the import can no longer run on any roster
            // secondary), terminal-fail EVERY now-`Unsatisfiable` dependent in ONE
            // batched sweep — instead of draining one per dispatch tick (the ~0.2
            // fails/sec starvation across thousands of dependents). The affine
            // subsystem owns the gate→dependents enumeration; this is the same
            // roster-aware `Unsatisfiable` predicate the per-dispatch gate uses,
            // applied eagerly. Runs AFTER the cell flip so the satisfiability read
            // sees this `Failed`. Threaded `command_rx`: the burst is failed DIRECTLY
            // via the batched `apply_fail_permanent_batch` (ONE broadcast, ONE
            // lifecycle pass) — never enqueued onto the bounded command channel,
            // whose overflow would drop dependents at scale.
            self.fast_fail_affine_dependents_if_unsatisfiable(&task_hash, command_rx)
                .await;
            // PER-SECONDARY BLOCKED RE-ROUTE on genuine fail (#652 concern B's
            // import-FAIL edge): a work BLOCKED on this import HERE must not wait
            // for the 5-min reconcile — its cell is now `Failed`, so drain it
            // from the per-secondary blocked map and re-decide RIGHT NOW: re-route
            // to a still-eligible secondary (clear its placement guard so the
            // next placement pass re-derives it), or terminalize it if the import
            // failed on every eligible secondary (the shared #648/#650 batch).
            // The fast-fail above already terminalized the bucket entries of the
            // globally-doomed dependents; this additionally clears their stale
            // blocked-map overlay and re-routes the still-satisfiable ones.
            if let Some(affine_id) = self.cluster_state.affine_id_for_hash(&task_hash) {
                self.reroute_affine_blocked_on(&secondary_id, Some(affine_id), command_rx)
                    .await;
            }
            self.drop_supplanted_holder(&task_hash);
            // POOL TERMINAL-FAILURE MIRROR (the failed twin of the complete
            // path's `note_affine_terminal`). The per-secondary cell flip above
            // is NOT a global terminal — the import may still run on another
            // secondary. But once it is `Failed` on EVERY eligible roster
            // secondary it can no longer run anywhere: its GLOBAL terminal is a
            // permanent failure. Record THAT in the pool's `failed_tasks` so the
            // import's own phase's affine guard (`phase_has_live_affine_prereq`,
            // which reads `!failed_tasks.contains`) clears and the phase can
            // drain past it — without this a genuinely-failed affine holds Gate B
            // forever. Phase-neutral + no dependent cascade (the fast-fail above
            // already terminal-failed the now-`Unsatisfiable` dependents); this
            // re-trigger is solely for the import's OWN phase. The lifecycle pass
            // observes the freshly-drained phase (the affine terminal path holds
            // no `command_rx`, so `&mut None` — the same shape the non-callback
            // cascade entries use).
            if let Some(affine_id) = self.cluster_state.affine_id_for_hash(&task_hash)
                && self.affine_import_globally_failed(affine_id)
                && let Some((phase, task_id)) = self
                    .cluster_state
                    .task_state(&task_hash)
                    .map(|s| (s.def().phase_id.clone(), s.def().task_id.clone()))
            {
                self.pool_mut().note_affine_failed(&phase, &task_id);
                self.process_phase_lifecycle(&mut None).await;
            }
            tracing::warn!(
                secondary = %secondary_id,
                worker_id,
                task_hash = %task_hash,
                "affine import FAILED on secondary (bitvector Failed, non-sticky); \
                 phase-neutral — dependents re-route via re-placement"
            );
        }

        // WORKER SLOT (both branches): free the holding slot for THIS
        // per-secondary run, addressed by the terminal's OWN (secondary, worker)
        // — slot-direct, NOT the shared hash-keyed `free_slot_on_terminal`. The
        // import runs the same hash on multiple secondaries concurrently; the
        // hash-keyed ledger holds only one holder, so the shared path would free
        // the wrong secondary's slot and orphan this worker's slot Assigned
        // forever. No phase cascade.
        self.free_affine_slot_on_terminal(&secondary_id, worker_id, &task_hash);

        // The freed worker is re-evaluated by the dispatch recheck; on a
        // backpressure reset the dependent re-pops `StrandedHere` and re-runs
        // the import, on a genuine terminal the placement trigger re-routes the
        // dependent away from this secondary (its cell is now Failed). Decoupled
        // emit.
        self.cluster_state
            .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
        self.forward_completion_to_secondaries(msg, &secondary_id)
            .await;
    }

    /// Drain-edge failure-permanence promotion for one drained phase:
    /// the phase's retry buckets have DECLINED (no candidates, or budget
    /// exhausted), so its soft (retry-pending) failures are permanent
    /// NOW and every transitive dependent blocked on them is permanently
    /// unfulfillable. Invoked by `process_phase_lifecycle` between the
    /// bucket pass and the `on_phase_end` tally read, so the cascaded
    /// terminals are already in the replicated per-phase tallies when
    /// the hook fires.
    ///
    /// Per cascaded dependent: the hash-keyed `failed_tasks` mirror gains
    /// the entry (the run-completion counter `completed + failed >=
    /// total` is what un-wedges the operational loop) and a
    /// `ClusterMutation::TaskFailed` with the canonical `upstream-failed`
    /// shape (kind `NonRecoverable`, the same classification the
    /// CRDT-side spawn cascade and `apply_fail_permanent` produce for a
    /// dependent of a non-recoverable terminal) is broadcast so every
    /// replica mirrors the terminal. Already-terminal hashes (a raced
    /// wire terminal) are skipped — the wire handler's dedup owns those.
    ///
    /// The aggregate emit names the unfulfillable-dependent count at the
    /// importance target: a dependency graph giving up is an operator-
    /// visible run event, not a silent bookkeeping detail.
    pub(crate) async fn finalize_phase_soft_failures(&mut self, phase: &dynrunner_core::PhaseId) {
        let finalized = self.pool_mut().finalize_soft_failures(phase);
        if finalized.is_empty() {
            return;
        }
        // Each promoted root's transitive dependents become permanent
        // upstream-failures — recorded + broadcast through the shared
        // cascade-terminal helper so the drain-edge path and the
        // immediate-permanence path (`note_item_failed`) build the SAME
        // cascade shape. Aggregated across roots into ONE broadcast.
        let mut pairs: Vec<(String, Vec<std::sync::Arc<dynrunner_core::TaskInfo<I>>>)> = finalized;
        let mut all_mutations: Vec<ClusterMutation<I>> = Vec::new();
        let mut total = 0usize;
        for (root_id, cascaded) in pairs.drain(..) {
            let (muts, n) = self.build_cascade_terminal_mutations(&root_id, cascaded);
            all_mutations.extend(muts);
            total += n;
        }
        if total == 0 {
            return;
        }
        // Operator-facing run event (the LLM-wake class): N dependents
        // just became permanently unfulfillable. Emitted ONCE per drain
        // edge with the aggregate count; the per-task WARNs in the helper
        // carry the identities.
        tracing::warn!(
            target: crate::primary::important_events::IMPORTANT_TARGET,
            phase = %phase,
            unfulfillable_dependents = total,
            "dependency graph can no longer complete here: cascading \
             permanent failure to dependents of terminally-failed tasks"
        );
        self.apply_and_broadcast_cluster_mutations(all_mutations).await;
    }

    /// Record + broadcast the permanent `upstream-failed` terminal of every
    /// transitive dependent in `cascaded` (the dependents the pool's
    /// permanent-failure cascade returned for the failed root `root_id`).
    ///
    /// The SINGLE owner of the cascade-fail-to-dependents shape, shared by
    /// the drain-edge path ([`Self::finalize_phase_soft_failures`]) and the
    /// immediate-permanence path (`note_item_failed`'s `Permanent` route).
    /// Each dependent gains the hash-keyed `failed_tasks` ledger entry (the
    /// run-completion counter reads it) and a broadcast
    /// `ClusterMutation::TaskFailed` with the canonical `NonRecoverable` /
    /// `upstream-failed` shape (the replicated per-phase rollup
    /// `phase_can_proceed` reads it). Already-terminal hashes (a raced wire
    /// terminal, or a re-delivery) are skipped — idempotent.
    pub(crate) async fn cascade_fail_dependents_terminal(
        &mut self,
        root_id: &str,
        cascaded: Vec<std::sync::Arc<dynrunner_core::TaskInfo<I>>>,
    ) {
        let (mutations, n) = self.build_cascade_terminal_mutations(root_id, cascaded);
        if n == 0 {
            return;
        }
        self.apply_and_broadcast_cluster_mutations(mutations).await;
    }

    /// Build (but do not broadcast) the per-dependent `TaskFailed`
    /// mutations for a permanent-failure cascade, recording each dependent
    /// in the local `failed_tasks` ledger as a side effect. Returns
    /// `(mutations, count)` so a caller can aggregate several roots into
    /// one broadcast. The in-memory ledger record is applied immediately
    /// (it must reflect the cascade even for a caller that batches the
    /// broadcast); a dependent already terminal in the local caches is
    /// skipped (idempotent).
    fn build_cascade_terminal_mutations(
        &mut self,
        root_id: &str,
        cascaded: Vec<std::sync::Arc<dynrunner_core::TaskInfo<I>>>,
    ) -> (Vec<ClusterMutation<I>>, usize) {
        let mut mutations: Vec<ClusterMutation<I>> = Vec::with_capacity(cascaded.len());
        for dep in cascaded {
            let dep_hash = crate::primary::wire::compute_task_hash(&dep);
            if self.completed_tasks.contains(&dep_hash)
                || self.failed_tasks.contains_key(&dep_hash)
            {
                continue;
            }
            self.failed_tasks
                .insert(dep_hash.clone(), dynrunner_core::ErrorType::NonRecoverable);
            // Pre-start fence A side-map drop (#530a): the cascade is a
            // terminal — no further dispatch will fence on this hash.
            self.drop_supplanted_holder(&dep_hash);
            tracing::warn!(
                task_id = %dep.task_id,
                phase = %dep.phase_id,
                failed_dependency = %root_id,
                "task can never run: its dependency terminally failed \
                 with no retry path; cascading permanent failure"
            );
            mutations.push(ClusterMutation::TaskFailed {
                hash: dep_hash,
                kind: dynrunner_core::ErrorType::NonRecoverable,
                error: format!(
                    "upstream-failed: dependency '{root_id}' terminally \
                     failed with no retry path"
                ),
                // Both stamped at the origination choke point
                // (apply_locally_for_broadcast): `version` minted, `attempt`
                // read from the task's current generation.
                version: Default::default(),
                attempt: Default::default(),
            });
        }
        let n = mutations.len();
        (mutations, n)
    }
}
