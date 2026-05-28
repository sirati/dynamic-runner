//! Local-backend command-channel dispatcher.
//!
//! Single concern: handle one `PrimaryCommand<I>` against a
//! `LocalManager` whose state is the local-only superset of the
//! distributed primary's (pool + side queues + per-task budget map +
//! `task_by_hash` mirror). Mirrors
//! `dynrunner_manager_distributed::primary::command_channel::handler::
//! handle_primary_command` 1:1 in shape — same enum, same per-variant
//! reply oneshot contract, same `validate_spawn_tasks` validator. The
//! only divergence is the per-variant body, which routes through the
//! local pool / side queues instead of a CRDT broadcast.
//!
//! Module boundary:
//!   * Owns: the local-backend body of each `PrimaryCommand` variant.
//!   * Does NOT own: the channel itself (the manager's
//!     `command_tx` / `command_rx` carry the wire), the per-task
//!     content hash (delegated to [`dynrunner_core::compute_task_hash`]),
//!     the validator rules (delegated to
//!     [`dynrunner_core::validate_spawn_tasks`]), or the
//!     `PyPrimaryHandle` pyclass (lives in `dynrunner-pyo3`).
//!
//! Per-variant local semantics:
//!
//! | Variant | Local-mode behavior |
//! |---|---|
//! | `SpawnTasks` | `validate_spawn_tasks` against `task_by_hash` + every known `task_id`; `pool.extend(valid)`; `stats.total += valid.len()`; mirror into `task_by_hash`; reply with per-task errors. |
//! | `FailPermanent` | Lookup hash → `(phase_id, task_id, task)` via `task_by_hash`; push to `failed_tasks` with the given `ErrorType`; cascade via `pool.on_item_failed_permanent` and push each cascaded dependent to `failed_tasks` with the same error; reply Ok. |
//! | `ReinjectTask` | Per-task budget check (lazily seeded from `config.unfulfillable_reinject_max_per_task`); on accept, remove from the matching side queue (`failed_tasks` / `resource_pressure_tasks` / `unassigned_tasks`) and `pool.reinject`; reply Ok / Err on budget exhaustion or hash not in any side queue. |
//! | `UpdatePreferredSecondaries` | `pool.update_first_match_in_place` to mirror onto the matching pool entry's `TaskInfo.preferred_secondaries`; `tracing::debug!` and Ok on no-match (the task may be in-flight or terminal — the preference is still meaningful for any future re-injection, so we don't fail). |

use dynrunner_core::{
    compute_task_hash, validate_spawn_tasks, FailedTask, Identifier, PrimaryCommand,
    SoftPreferredSecondaries,
};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::LocalManager;

/// Dispatch one `PrimaryCommand` against `mgr`'s local state. Single
/// match called from (a) the worker-loop's `select!` arm and (b) the
/// outer-loop's tail drain in `process_binaries`. Each arm computes a
/// reply, then `let _ = reply.send(...)` — a dropped reply receiver
/// (caller timed out / panicked) is non-fatal, mirroring the
/// distributed handler's contract.
pub(crate) async fn handle_local_command<M, S, E, I>(
    mgr: &mut LocalManager<M, S, E, I>,
    cmd: PrimaryCommand<I>,
) where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    match cmd {
        PrimaryCommand::SpawnTasks { tasks, reply } => {
            // Closure-based validator from `dynrunner-core`: same
            // rules the distributed primary and promoted-secondary
            // apply paths use. `task_by_hash` is our duplicate-hash
            // probe; the `task_id` known-set is also derived from
            // `task_by_hash` (the mirror covers every task the
            // manager has ever known about in this run).
            let (valid, errors) = validate_spawn_tasks(
                |hash| mgr.task_by_hash.contains_key(hash),
                |task_id| {
                    mgr.task_by_hash
                        .values()
                        .any(|t| t.task_id.as_str() == task_id)
                },
                tasks,
            );
            // Mirror BEFORE `extend`: matches the initial-batch
            // mirror in `process_binaries` so the invariant "every
            // task the pool knows about is mirrored" holds even if
            // `extend` rejects the batch (rejected tasks stay
            // mirrored for diagnostic resolvability).
            for t in &valid {
                mgr.task_by_hash.insert(compute_task_hash(t), t.clone());
            }
            if !valid.is_empty() {
                let n = valid.len() as u32;
                if let Err(e) = mgr.pool_mut().extend(valid) {
                    let _ = reply.send(Err(format!("spawn_tasks: pool extend rejected: {e}")));
                    return;
                }
                mgr.stats.total += n;
            }
            let _ = reply.send(Ok(errors));
        }
        PrimaryCommand::FailPermanent {
            hash,
            error,
            reason,
            reply,
        } => {
            let task = match mgr.task_by_hash.get(&hash) {
                Some(t) => t.clone(),
                None => {
                    let _ = reply.send(Err(format!(
                        "fail_permanent: unknown task hash {hash}"
                    )));
                    return;
                }
            };
            // Push the originating failure to the per-pass ledger.
            // The reason rides in the error_message so an operator
            // inspecting `manager.failed_tasks()` after the run can
            // see why FailPermanent fired (vs. a worker-driven
            // failure whose message comes from the worker).
            mgr.failed_tasks.push(FailedTask {
                binary: task.clone(),
                error_type: error.clone(),
                error_message: reason.clone(),
                retry_count: 0,
            });
            // Cascade-fail dependents via the pool primitive. Local
            // mode has no CRDT auto-resume mechanism so we surface
            // every dependent as failed with the same error — same
            // shape a worker-driven cascade would produce. Mirrors
            // the distributed primary's
            // `pending_pool::on_item_failed_permanent` cascade
            // semantics; the only difference is the local backend
            // pushes to `failed_tasks` instead of building
            // `TaskBlocked` mutations.
            let cascaded = mgr
                .pool_mut()
                .on_item_failed_permanent(&task.phase_id, task.task_id.as_str());
            for cascaded_task in cascaded {
                mgr.failed_tasks.push(FailedTask {
                    binary: cascaded_task,
                    error_type: error.clone(),
                    error_message: format!(
                        "cascade-fail from {hash} (root: {reason})"
                    ),
                    retry_count: 0,
                });
            }
            let _ = reply.send(Ok(()));
        }
        PrimaryCommand::ReinjectTask { hash, reply } => {
            // Budget check first — exhaustion must not pop a task
            // out of a side queue. Lazy-seed the per-hash counter
            // from `config.unfulfillable_reinject_max_per_task` on
            // first call; `None` (no cap) skips the entire check.
            if let Some(cap) = mgr.config.unfulfillable_reinject_max_per_task {
                let remaining = mgr
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
                    let _ = reply.send(Err(format!(
                        "reinject_task: budget exhausted for hash {hash} (cap={cap})"
                    )));
                    return;
                }
                *remaining -= 1;
            }
            // Find + remove from the matching side queue.
            // Search order: failed_tasks, resource_pressure_tasks,
            // unassigned_tasks. Same shape the retry / pressure
            // phase code path uses to reinject. A task that's
            // currently in the pool (queued / in-flight) is not in
            // any side queue and we refuse — local-mode reinject
            // is only meaningful for already-failed-and-side-
            // queued tasks.
            let found = if let Some(pos) = mgr
                .failed_tasks
                .iter()
                .position(|f| compute_task_hash(&f.binary) == hash)
            {
                Some(mgr.failed_tasks.remove(pos).binary)
            } else if let Some(pos) = mgr
                .resource_pressure_tasks
                .iter()
                .position(|f| compute_task_hash(&f.binary) == hash)
            {
                Some(mgr.resource_pressure_tasks.remove(pos).binary)
            } else if let Some(pos) = mgr
                .unassigned_tasks
                .iter()
                .position(|t| compute_task_hash(t) == hash)
            {
                Some(mgr.unassigned_tasks.remove(pos))
            } else {
                None
            };
            match found {
                Some(task) => {
                    mgr.pool_mut().reinject(task);
                    let _ = reply.send(Ok(()));
                }
                None => {
                    let _ = reply.send(Err(format!(
                        "reinject_task: hash {hash} not present in any side queue"
                    )));
                }
            }
        }
        PrimaryCommand::UpdatePreferredSecondaries {
            hash,
            secondaries,
            reply,
        } => {
            let target_hash = hash.clone();
            let new_pref = SoftPreferredSecondaries::new(secondaries);
            // Mirror onto the live pool entry if the task is still
            // queued / blocked. Local mode has no peer concept, so
            // the field itself has no scheduling effect today; the
            // mirror is correct for forward-compat (a manager
            // promoted to host secondaries via future-cluster
            // semantics would consult the field) and harmless
            // otherwise. `update_first_match_in_place` returns
            // `false` when the task is in-flight / terminal — we
            // log debug and still reply Ok because the operator's
            // intent (record this preference for the named task)
            // has no error path in local mode.
            let matched = mgr.pool_mut().update_first_match_in_place(
                |t| compute_task_hash(t) == target_hash,
                |t| t.preferred_secondaries = new_pref.clone(),
            );
            if !matched {
                tracing::debug!(
                    task_hash = %hash,
                    "update_preferred_secondaries: hash not in pool buckets; \
                     local mode has no peer concept so this is a no-op (task may \
                     be in-flight or terminal)"
                );
            }
            let _ = reply.send(Ok(()));
        }
    }
}
