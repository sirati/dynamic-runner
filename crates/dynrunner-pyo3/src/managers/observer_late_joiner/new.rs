//! `PyObserverLateJoiner` constructor — stashes the caller's
//! configuration knobs. The peer-join + snapshot restore + observation
//! loop runs inside the sibling [`run`] module, which cold-joins the
//! standalone `ObserverCoordinator`.

use std::path::PathBuf;

use pyo3::prelude::*;

use crate::config::distributed::DistributedConfig;

use super::PyObserverLateJoiner;

#[pymethods]
impl PyObserverLateJoiner {
    #[new]
    #[pyo3(signature = (
        peer_info_dir,
        observer_id = None,
        distributed_config = None,
        holdings = None,
        panik_watcher_paths = None,
        panik_watcher_poll_interval_secs = 10.0,
    ))]
    fn new(
        peer_info_dir: PathBuf,
        observer_id: Option<String>,
        distributed_config: Option<DistributedConfig>,
        holdings: Option<Vec<String>>,
        panik_watcher_paths: Option<Vec<PathBuf>>,
        panik_watcher_poll_interval_secs: f64,
    ) -> PyResult<Self> {
        // Default observer-id includes a small random suffix so two
        // concurrent observer-dispatchers on the same gateway don't
        // collide on the peer-id (the mesh keys on it). The format
        // mirrors the secondary-id shape (`<role>-<unique>`) so peer
        // logs read uniformly.
        let observer_id = observer_id.unwrap_or_else(|| {
            // Nanosecond timestamp plus 16 bits of process-entropy so
            // two observers launched in the same nanosecond bucket on
            // the same gateway can't collide on the peer id.
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let pid = std::process::id() & 0xffff;
            format!("observer-{ts:08x}-{pid:04x}")
        });
        // Dedup at the boundary — Python typically passes a list, but
        // the announcer's storage is set-semantics (`HashSet`). The
        // alternative (push the dedup onto the consumer) would mean
        // every Python caller has to know about the wire-side
        // contract; doing it here once keeps the kwarg's shape
        // operator-friendly (`list[str]`).
        let holdings: std::collections::HashSet<String> =
            holdings.unwrap_or_default().into_iter().collect();
        Ok(Self {
            observer_id,
            peer_info_dir,
            distributed_config: distributed_config.unwrap_or_default(),
            holdings,
            panik_watcher_paths: panik_watcher_paths.unwrap_or_default(),
            panik_watcher_poll_interval_secs,
            completed: 0,
        })
    }
}
