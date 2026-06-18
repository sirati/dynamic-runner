//! Best-effort dispatch escalation for estimate-stalled phases (#499).
//!
//! Single concern: when the normal dispatch recheck has run and the run
//! is STALLED purely on the resource ESTIMATE — the pool still holds
//! dispatch-eligible queued work, at least one idle worker is assignable,
//! and NO queued task's estimate fits ANY worker's per-worker reserved
//! budget — attempt dispatch ANYWAY against the largest single
//! secondary's full advertised capacity, exactly as the local manager's
//! "unassigned phase" boosts worker 0 to the cluster pool
//! (`dynrunner-manager-local`'s `run_unassigned_phase` /
//! `boost_worker0_budget_to_max`). The estimate may be pessimistic; the
//! task may actually run. Tasks whose estimate exceeds even the largest
//! secondary's full capacity are GENUINELY unfittable and are failed
//! INDIVIDUALLY as `ResourceExhausted` (matching local), never silently
//! dropped and never taking the whole pool down.
//!
//! WHY this exists: the per-worker reserved budget is `max / num_workers`
//! (the parallel-scheduling fraction). A task whose estimate exceeds that
//! fraction but fits a whole node `NoFit`s every worker, is never
//! dispatched, never completes, never fails — so the operational loop's
//! `completed + failed >= total` counter exit can never trip. The run
//! hangs until `fleet_dead_timeout` strands the WHOLE pool ->
//! `ClusterCollapsed`. This module converts that whole-pool strand into
//! best-effort dispatch + actionable per-task failure. It is the
//! distributed analog of the local manager's `66ffe185` fix (a 1386-task
//! run silently dropped 1291 tasks whose estimate exceeded the per-worker
//! share).
//!
//! WHY best-effort is SAFE here (the guards bound the downside):
//!   - A boosted task that actually fits a node RUNS (the common
//!     pessimistic-estimate case).
//!   - A boosted task that genuinely OOMs is killed by the OOM manager
//!     (`Scheduler::check_resource_pressure` + `cgroup_safety_margin`),
//!     requeued with backoff, and bounded by `retry_max_passes` /
//!     `fail_final` accounting (#465). No infinite loop, no silent loss.
//!   - Dispatch targets a WORKER on a secondary, never the co-located
//!     coordinator, so there is no new coordinator-OOM risk.
//!
//! Module boundary: this is a pure CONSUMER of existing primitives. It
//! reads the queued pool (`pool().iter()`), the estimator
//! (`estimator.estimate`), and each node's replicated advertised capacity
//! (`secondary_advertised_resources`, off the CRDT capacity ledger). It
//! re-runs the existing recheck (`dispatch_to_idle_workers`) once under a
//! temporarily-boosted budget, and fails the unfittable through the shared
//! terminal cascade (`apply_fail_permanent`, the SAME path the command
//! channel and setup sink use). The worker-management arm calls one method
//! (`escalate_estimate_stalled_dispatch`) AFTER the normal passes; no other
//! code learns the escalation concern.

use dynrunner_core::{ErrorType, Identifier, ResourceKind, ResourceMap};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::mpsc as tokio_mpsc;

use crate::primary::PrimaryCoordinator;
use crate::primary::command_channel::PrimaryCommand;
use crate::primary::wire::compute_task_hash;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Best-effort rescue for an estimate-stalled run. Called from the
    /// worker-management arm AFTER the normal dispatch recheck (and the
    /// setup / affine passes) so it only fires on work nothing else could
    /// place. Two effects, both gated on the estimate-stall precondition:
    ///
    ///   1. Tasks that fit the largest secondary's full capacity but no
    ///      idle worker's per-worker budget: re-attempt dispatch with ONE
    ///      idle worker on that secondary boosted to the node's full
    ///      capacity (best-effort — the estimate is treated as advisory, as
    ///      it is in the local manager's unassigned phase).
    ///   2. Tasks that exceed even the largest secondary's full capacity:
    ///      fail INDIVIDUALLY as `ResourceExhausted(memory)` with an
    ///      actionable message (estimate vs. node cap), so the run
    ///      terminates cleanly with that task accounted under `fail_*`
    ///      rather than the whole pool stranding on the counter exit.
    ///
    /// `command_rx` is threaded through to `apply_fail_permanent`'s
    /// cascade (its `note_item_failed` may run consumer `on_phase_end`
    /// callbacks that queue `spawn_tasks`), matching every other
    /// terminal-cascade caller's chokepoint discipline.
    ///
    /// No-op (fast return) whenever the run is NOT estimate-stalled:
    /// pool empty, no assignable idle worker, or some queued task already
    /// fits a per-worker budget (the normal recheck owns that — escalation
    /// never competes with a dispatch that can proceed normally).
    pub(crate) async fn escalate_estimate_stalled_dispatch(
        &mut self,
        command_rx: &mut Option<tokio_mpsc::Receiver<PrimaryCommand<I>>>,
    ) {
        let mem_kind = ResourceKind::memory();

        // The largest single secondary's full advertised capacity is the
        // distributed best-effort cap: a task runs on ONE worker on ONE
        // secondary, so the node-wide ceiling — not a summed cluster pool —
        // is the honest "could this task fit anywhere if it owned a node
        // alone" bound. Mirrors the local manager's `max_resources()`
        // (the single host's full RAM) adapted to a multi-node fleet.
        //
        // Ranked off the replicated capacity ledger (`secondary_advertised_resources`
        // reads the CRDT — the SAME source the roster is built from), over
        // the LIVE known secondaries only, so a `PeerRemoved` node is never
        // chosen as the boost target.
        let Some(largest_secondary) = self.largest_live_secondary_by_memory(&mem_kind) else {
            // No live secondary with a capacity record — nothing to escalate
            // against. The fleet-liveness / fleet-dead paths own the
            // no-capacity case.
            return;
        };
        let node_cap = self.secondary_advertised_resources(&largest_secondary);
        let node_cap_mem = node_cap.get(&mem_kind);

        // Precondition gate: only fire on a genuine ESTIMATE stall — the
        // pool has dispatch-eligible queued work, there is an assignable
        // idle worker, and NO queued eligible task fits the budget of any
        // IDLE ASSIGNABLE worker (so the normal recheck just ran and placed
        // nothing for a budget reason, not a transient gate). This keeps
        // escalation a pure estimate concern: a backpressure /
        // mesh-unconfirmed / backoff stall is NOT escalated (those resolve
        // on their own and the normal recheck will place the work).
        if !self.is_estimate_stalled() {
            return;
        }

        // Partition the eligible queued tasks by the node-cap bound:
        //   * fits the node cap  -> best-effort dispatch candidate
        //   * exceeds even node  -> genuinely unfittable, terminal-fail
        // Snapshot (hash, estimate) up front so the pool / worker borrows
        // below don't overlap the estimator read.
        let mut unfittable: Vec<(String, u64)> = Vec::new();
        let mut have_fittable_boost_candidate = false;
        {
            // `pool` and `estimator` are disjoint shared borrows of `self`,
            // bound once so the loop body never re-borrows `self`. The owned
            // `(hash, est)` results outlive this scope; the borrows do not.
            let pool = self.pool();
            let estimator = &self.estimator;
            for task in pool.iter() {
                if !pool.dispatch_eligible(task) {
                    continue;
                }
                let est_mem = estimator.estimate(task).get(&mem_kind);
                if est_mem > node_cap_mem {
                    unfittable.push((compute_task_hash(task), est_mem));
                } else {
                    have_fittable_boost_candidate = true;
                }
            }
        }

        // Fail the genuinely-unfittable tasks individually, FIRST — this is
        // the terminal that lets the counter exit eventually trip. Each
        // routes through the shared permanent-failure cascade
        // (`apply_fail_permanent`): records the `ResourceExhausted`
        // terminal in `failed_tasks`, broadcasts `TaskFailed`, and cascades
        // to dependents exactly as a worker-originated non-recoverable
        // failure would. Matches the local manager's per-task
        // `ResourceExhausted` with an estimate-vs-cap message.
        for (hash, est_mem) in unfittable {
            let reason = format!(
                "estimator value {} MiB exceeds the largest secondary's full \
                 capacity {} MiB ({}); task cannot run on this cluster \
                 (likely estimator overshoot or cluster sized too small)",
                est_mem / (1024 * 1024),
                node_cap_mem / (1024 * 1024),
                largest_secondary,
            );
            tracing::error!(
                task_hash = %hash,
                estimated_mib = est_mem / (1024 * 1024),
                node_cap_mib = node_cap_mem / (1024 * 1024),
                secondary = %largest_secondary,
                "unfittable task: estimate exceeds the largest node's full capacity; \
                 failing individually as ResourceExhausted (best-effort #499)"
            );
            // The cascade can re-enter command processing; ignore a
            // per-task error so one bad hash never aborts the sweep.
            self.apply_fail_permanent(
                hash,
                ErrorType::ResourceExhausted(mem_kind.clone()),
                reason,
                command_rx,
            )
            .await
            .ok();
        }

        // Best-effort dispatch for the fits-a-node-but-not-a-worker class:
        // temporarily boost the largest secondary's worker 0 to the node's
        // full capacity and run ONE normal recheck. The boost is scoped to
        // this synchronous pass and restored after — the distributed analog
        // of local's "boost worker 0, run the phase, workers stop" (here
        // the restore replaces "workers stop"). Other workers keep their
        // per-worker budget, so a normal-pass dispatch is never displaced;
        // only the otherwise-stuck oversized task gets a shot.
        if have_fittable_boost_candidate {
            self.dispatch_with_boosted_node_budget(&largest_secondary, node_cap)
                .await;
        }
    }

    /// True iff the run is stalled PURELY on the resource estimate: the
    /// pool has at least one dispatch-eligible queued task, at least one
    /// idle worker is assignable (mesh-confirmed, not backpressured, not
    /// masked by a run-fail freeze), and NO dispatch-eligible queued task
    /// fits the budget of any IDLE ASSIGNABLE worker (a busy worker's
    /// larger budget is irrelevant — it cannot take a task this tick).
    ///
    /// The idle-assignable condition establishes "an assignable worker is
    /// sitting idle while work waits"; the no-fit condition establishes the
    /// cause is the estimate gate specifically (not a transient
    /// backpressure / mesh / backoff gate, which the normal recheck
    /// resolves on its own).
    ///
    /// `&mut self` because the dispatch-gate consult
    /// ([`Self::is_dispatch_gated_for_estimate_check`]) latches a one-shot
    /// mesh-gate WARN; the read is otherwise side-effect-free.
    fn is_estimate_stalled(&mut self) -> bool {
        let mem_kind = ResourceKind::memory();

        // The largest per-worker budget among the IDLE, ASSIGNABLE workers
        // — the most a task could be dispatched to RIGHT NOW by the normal
        // recheck. A BUSY worker's (possibly larger) budget does NOT count:
        // it cannot take a queued task this tick, so its budget is
        // irrelevant to "is the run stuck". An idle slot the dispatch-shape
        // gate would withhold (unconfirmed mesh leg / backpressure / OOM
        // single-worker mask / run-fail freeze) also does NOT count — its
        // idleness is a transient gate, not an estimate stall. An explicit
        // loop (not an iterator chain) because the gate consult borrows
        // `&mut self`.
        let mut max_idle_assignable_budget: Option<u64> = None;
        for idx in 0..self.workers.len() {
            if self.workers[idx].is_idle() && !self.is_dispatch_gated_for_estimate_check(idx) {
                let budget = self.workers[idx].reserved_budget_mem();
                max_idle_assignable_budget =
                    Some(max_idle_assignable_budget.map_or(budget, |m| m.max(budget)));
            }
        }
        // No idle assignable worker: not an estimate stall (the work is
        // waiting on capacity to free, not on the estimate gate).
        let Some(max_worker_budget) = max_idle_assignable_budget else {
            return false;
        };

        // Snapshot the dispatch-eligible queued tasks' memory estimates
        // into an owned Vec FIRST, releasing the pool/estimator borrows
        // before the comparison. `pool` and `estimator` are DISJOINT shared
        // borrows of `self`, bound once so the closures don't re-borrow
        // `self` per item.
        let pool = self.pool();
        let estimator = &self.estimator;
        let eligible_estimates: Vec<u64> = pool
            .iter()
            .filter(|t| pool.dispatch_eligible(t))
            .map(|t| estimator.estimate(t).get(&mem_kind))
            .collect();

        if eligible_estimates.is_empty() {
            return false;
        }
        // Estimate-stalled iff NO eligible task fits the biggest IDLE
        // ASSIGNABLE worker's budget. If any does, the normal recheck owns
        // it — escalation must not pre-empt a dispatch that can proceed
        // normally.
        eligible_estimates
            .iter()
            .all(|&est| est > max_worker_budget)
    }

    /// The live known secondary with the largest advertised memory
    /// capacity (the CRDT capacity ledger, ties broken by id ASC for
    /// determinism), or `None` when no live secondary carries a capacity
    /// record. The boost-target node for the best-effort escalation. Reads
    /// the SAME source the roster is built from
    /// (`reconstruct_workers_from_cluster_state`), so the chosen node's cap
    /// agrees with its workers' per-worker budgets.
    fn largest_live_secondary_by_memory(&self, mem_kind: &ResourceKind) -> Option<String> {
        self.cluster_state
            .live_known_secondaries()
            .map(String::from)
            .map(|id| {
                let mem = self.secondary_advertised_resources(&id).get(mem_kind);
                (id, mem)
            })
            .filter(|(_, mem)| *mem > 0)
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
            .map(|(id, _)| id)
    }

    /// The dispatch-shape gate consulted by [`Self::is_estimate_stalled`]
    /// to decide whether an idle worker's idleness is a transient gate
    /// (withhold) versus a genuine estimate stall. Re-uses the live
    /// dispatch predicate so the two never diverge: an idle worker that
    /// `dispatch_to_idle_workers` would skip for a non-estimate reason is
    /// withheld here too.
    ///
    /// Uses the PROACTIVE shape (`request_driven = false`,
    /// `bypass_backpressure = false`) to match the worker-management
    /// recheck that runs just before this. `&mut self` because the
    /// underlying predicate latches a one-shot mesh-gate WARN; the call is
    /// inside the same `&mut self` reaction, so this is borrow-clean.
    fn is_dispatch_gated_for_estimate_check(&mut self, worker_idx: usize) -> bool {
        self.should_skip_worker_for_dispatch(worker_idx, false)
    }

    /// Run ONE dispatch recheck with EXACTLY ONE idle worker on
    /// `secondary` temporarily boosted to `node_cap`, then restore its
    /// per-worker budget. The distributed analog of the local manager's
    /// `boost_worker0_budget_to_max`: it lets the scheduler's
    /// `assign_normal` accept a task whose estimate exceeds the per-worker
    /// fraction but fits the node.
    ///
    /// WHY exactly one worker: local activates ONLY worker 0 during its
    /// unassigned phase so the boosted node-wide budget can never be
    /// double-committed (N workers each believing they own the full node
    /// would concurrently over-commit it -> kernel OOM). The distributed
    /// analog boosts ONE idle worker and leaves the rest at their normal
    /// (smaller) per-worker budget. Safe because the escalation only fires
    /// under [`Self::is_estimate_stalled`] — NO queued task fits any normal
    /// per-worker budget — so the non-boosted idle workers `NoFit`
    /// everything this pass anyway; only the boosted worker can take the
    /// oversized task. No worker masking needed.
    ///
    /// Picks the LARGEST-capacity idle, assignable worker on `secondary`
    /// (worker 0 has the full node budget already, so it is preferred when
    /// idle; when it is busy, the next-largest idle worker is boosted —
    /// the stall case the normal recheck cannot resolve).
    ///
    /// Scoped to this synchronous pass — the budget is restored
    /// unconditionally after the recheck so no later parallel-scheduling
    /// read observes the boosted value (local relies on "workers stop
    /// after"; the event-driven primary restores instead).
    async fn dispatch_with_boosted_node_budget(&mut self, secondary: &str, node_cap: ResourceMap) {
        // The largest-capacity idle, assignable worker on the secondary.
        // `should_skip_worker_for_dispatch` excludes a worker the recheck
        // would withhold anyway (FSM-suspect / backpressure / OOM mask /
        // run-fail freeze), so the boost lands on a slot the recheck will
        // actually consider.
        let mut best: Option<(usize, u64)> = None;
        for idx in 0..self.workers.len() {
            if self.workers[idx].secondary_id != secondary || !self.workers[idx].is_idle() {
                continue;
            }
            if self.should_skip_worker_for_dispatch(idx, false) {
                continue;
            }
            let budget = self.workers[idx].reserved_budget_mem();
            if best.is_none_or(|(_, b)| budget > b) {
                best = Some((idx, budget));
            }
        }
        let Some((worker_idx, _)) = best else {
            // No idle assignable worker on the largest secondary right now
            // (e.g. all busy / unconfirmed). Nothing to boost; the next
            // event re-evaluates against fresh state.
            return;
        };

        // Save → boost → recheck → restore. The boost is on the SAME field
        // the scheduler gate reads (`resource_budgets` -> `budget_info()`'s
        // `reserved_budgets`). Restored in all paths (the recheck only
        // returns transient send errors, swallowed inside it).
        let saved = self.workers[worker_idx].resource_budgets.clone();
        self.workers[worker_idx].resource_budgets = node_cap;

        // `bypass_backpressure = true`: this is a deliberate rescue of
        // otherwise-stuck work, the same disposition a genuine `TasksAdded`
        // recheck carries. Send failures are logged + rolled back inside.
        self.dispatch_to_idle_workers(true).await.ok();

        self.workers[worker_idx].resource_budgets = saved;
    }
}
