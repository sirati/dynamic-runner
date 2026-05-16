//! `Drop` impl for `MultiProcessSpawnerInner` — drains the cleanup
//! registry and reaps every respawn-produced `Child` with the
//! SIGTERM → grace → SIGKILL ladder.

use super::MultiProcessSpawnerInner;

impl Drop for MultiProcessSpawnerInner {
    /// Drain the cleanup registry and reap every respawn-produced
    /// `Child` with the SIGTERM → grace → SIGKILL ladder. The kill
    /// primitive lives in `subprocess_factory` so containerised
    /// (podman) workers — which trap SIGTERM to clean up their
    /// conmon-supervised container — share the same teardown shape
    /// as the initial-spawn path and the SLURM provider's cleanup.
    /// Re-implementing it here would be the duplicated-logic
    /// antipattern.
    fn drop(&mut self) {
        // Take ownership of the Vec under the lock — even a poisoned
        // lock yields the guarded Vec via `into_inner`, because
        // teardown must run regardless of upstream panic state.
        let mut children = match self.tracked_children.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        if children.is_empty() {
            return;
        }
        tracing::debug!(
            count = children.len(),
            "draining respawned secondary children on spawner drop",
        );
        crate::subprocess_factory::terminate_children(&mut children);
    }
}
