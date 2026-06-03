//! Thin inherent-method shim wiring the manager event-loop call-sites
//! to [`crate::memprofile::MemProfileSampler`].
//!
//! Single concern: hand off task-assigned / task-completed / worker-
//! disconnected lifecycle pings from the manager loop to the sampler
//! WITHOUT every call-site repeating `if let Some(s) = &self.sampler`
//! and re-fetching the worker's `subcgroup_dir`. The sampler itself
//! knows nothing about manager state; the manager knows nothing about
//! the sampler's internals — the contract crossing this boundary is
//! `(task_id, worker_id, subcgroup_dir, started_at)` on assign,
//! `task_id` on complete, and `worker_id` on disconnect.
//!
//! Hook sites:
//!   * `manager/phases.rs::run_initial_assignments` — initial dispatch
//!     at the start of `process_binaries`.
//!   * `manager/worker_loop.rs::try_assign_normal` — every subsequent
//!     in-loop assignment.
//!   * `manager/events.rs::handle_event` — TaskCompleted and
//!     Disconnected arms.
//!
//! All four sites call into the helpers below; when
//! `self.sampler.is_none()` (operator did not opt into profiling) every
//! helper is a no-op so the hot path stays branchless aside from the
//! `Option::as_ref` test.

use dynrunner_core::{Identifier, TaskInfo, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::LocalManager;

impl<M, S, E, I> LocalManager<M, S, E, I>
where
    M: ManagerEndpoint + 'static,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    /// Notify the sampler that `binary` has just been successfully
    /// assigned to `worker_id`. No-op when the sampler is disabled.
    ///
    /// Reads the worker's cgroup-v2 leaf path via the existing
    /// [`crate::worker::WorkerHandle::subcgroup_dir`] accessor. When
    /// the pool was initialised without nested cgroups (graceful
    /// fallback when the host doesn't support delegated cgroup-v2 —
    /// also the current default for `LocalManager` mode, see the
    /// scope note on [`super::LocalManagerConfig::output_dir`]) the
    /// accessor returns `None` and this method short-circuits without
    /// firing a sampler command. The sampler's `on_task_assigned`
    /// requires the cgroup leaf path; we don't fabricate one.
    pub(super) fn notify_sampler_assigned(&self, worker_id: WorkerId, binary: &TaskInfo<I>) {
        let Some(sampler) = self.sampler.as_ref() else {
            return;
        };
        let Some(subcgroup_dir) = self
            .pool
            .workers
            .get(worker_id as usize)
            .and_then(|w| w.subcgroup_dir())
        else {
            return;
        };
        sampler.on_task_assigned(
            binary.task_id.clone(),
            worker_id,
            subcgroup_dir.to_path_buf(),
            std::time::Instant::now(),
        );
    }

    /// Notify the sampler that the task identified by `task_id` has
    /// completed. No-op when the sampler is disabled. `task_id` is
    /// required (every task carries one per the framework's
    /// boundary contract).
    pub(super) fn notify_sampler_completed(&self, task_id: String) {
        let Some(sampler) = self.sampler.as_ref() else {
            return;
        };
        sampler.on_task_completed(task_id);
    }

    /// Notify the sampler that `worker_id`'s transport has
    /// disconnected. The sampler walks its active map and flushes
    /// every profile attached to that worker — used when the worker
    /// pipe-EOFs (subprocess crash, kernel-OOM kill, transport
    /// hiccup) and no matching `TaskCompleted` will arrive.
    pub(super) fn notify_sampler_disconnected(&self, worker_id: WorkerId) {
        let Some(sampler) = self.sampler.as_ref() else {
            return;
        };
        sampler.on_worker_disconnected(worker_id);
    }
}
