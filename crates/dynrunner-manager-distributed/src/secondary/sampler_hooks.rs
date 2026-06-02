//! Thin inherent-method shim wiring the secondary's event-loop
//! call-sites to [`dynrunner_manager_local::memprofile::MemProfileSampler`].
//!
//! Single concern: hand off task-assigned / task-completed / worker-
//! disconnected lifecycle pings from the secondary's processing loop
//! to the sampler WITHOUT every call-site repeating
//! `if let Some(s) = &self.sampler` and re-fetching the worker's
//! `subcgroup_dir`. The sampler itself knows nothing about coordinator
//! state; the coordinator knows nothing about the sampler's internals
//! — the contract crossing this boundary is
//! `(task_id, worker_id, subcgroup_dir, started_at)` on assign,
//! `task_id` on complete, and `worker_id` on disconnect.
//!
//! Hook sites:
//!   * `secondary/setup.rs::handle_initial_assignment` — initial
//!     dispatch the primary handed us at startup.
//!   * `secondary/dispatch/router.rs` (`TaskAssignment` arm) — every
//!     in-loop assignment (a `TaskAssignment` addressed to this node's
//!     own worker arrives here whether it came over the wire from the
//!     authority or via the co-located primary's loopback — the unified
//!     transport made the origin opaque, subsuming the old
//!     self-assign-vs-wire split).
//!   * `secondary/processing/worker_event.rs` (`WorkerEvent::Ready`
//!     arm) — post-respawn pending-first-bind dispatch.
//!   * `secondary/processing/worker_event.rs` (`TaskCompleted` and
//!     `Disconnected` arms).
//!
//! All call sites fire through the helpers below; when
//! `self.sampler.is_none()` (operator did not opt into profiling) every
//! helper is a no-op so the hot path stays branchless aside from the
//! `Option::as_ref` test. Mirrors the same-named module on the
//! [`dynrunner_manager_local::manager::LocalManager`] path; the two
//! coordinators share the sampler's public API and differ only in
//! which `Self` exposes the hook surface.

use dynrunner_core::{Identifier, TaskInfo, WorkerId};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::SecondaryCoordinator;

impl<Tr, M, S, E, I> SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint + 'static,
    S: Scheduler<I> + Clone,
    E: ResourceEstimator<I> + Clone,
    I: Identifier,
{
    /// Notify the sampler that `binary` has just been successfully
    /// assigned to `worker_id`. No-op when the sampler is disabled.
    ///
    /// Reads the worker's cgroup-v2 leaf path via the existing
    /// [`dynrunner_manager_local::worker::WorkerHandle::subcgroup_dir`]
    /// accessor. When the pool was initialised without nested cgroups
    /// (graceful fallback when the host doesn't support delegated
    /// cgroup-v2) the accessor returns `None` and this method
    /// short-circuits without firing a sampler command. The sampler's
    /// `on_task_assigned` requires the cgroup leaf path; we don't
    /// fabricate one.
    pub(super) fn notify_sampler_assigned(
        &self,
        worker_id: WorkerId,
        binary: &TaskInfo<I>,
    ) {
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
    ///
    /// MUST be called BEFORE any path that drops the worker's
    /// [`dynrunner_manager_local::cgroup::SubcgroupHandle`] (worker
    /// restart, pool teardown). The Drop impl best-effort-rmdirs the
    /// leaf cgroup; a sampler tick that races past the rmdir reads
    /// `memory.current` against a now-empty directory and the last
    /// frame silently drops.
    pub(super) fn notify_sampler_disconnected(&self, worker_id: WorkerId) {
        let Some(sampler) = self.sampler.as_ref() else {
            return;
        };
        sampler.on_worker_disconnected(worker_id);
    }

    /// Drain and join the sampler. Idempotent: returns immediately
    /// when profiling is disabled or already torn down.
    ///
    /// MUST be called BEFORE any path that tears down the worker
    /// pool (`stop_all_workers`, `kill_all_workers_with_grace`) so
    /// the last tick's `memory.current` reads still see the
    /// per-worker cgroup leaves the pool's teardown is about to
    /// Drop-rmdir. After this returns the sampler is fully gone
    /// (background task joined, every writer's last frame finalised).
    pub(in crate::secondary) async fn shutdown_sampler_if_present(&mut self) {
        if let Some(sampler) = self.sampler.take() {
            sampler.shutdown().await;
        }
    }
}
