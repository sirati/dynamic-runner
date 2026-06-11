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
        gateway_url = None,
        ssh_identity_file = None,
        ssh_config_file = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        peer_info_dir: PathBuf,
        observer_id: Option<String>,
        distributed_config: Option<DistributedConfig>,
        holdings: Option<Vec<String>>,
        panik_watcher_paths: Option<Vec<PathBuf>>,
        panik_watcher_poll_interval_secs: f64,
        gateway_url: Option<String>,
        ssh_identity_file: Option<String>,
        ssh_config_file: Option<String>,
    ) -> PyResult<Self> {
        // Resolve the gateway mode UP FRONT so a malformed URL fails at
        // construction (operator-visible, before any runtime spins up).
        // `--gateway local` means "the dir and the cluster are reachable
        // from this host" — identical to passing no gateway at all, so
        // both collapse to `None` and the local path stays byte-identical.
        let gateway = match gateway_url.as_deref() {
            None | Some("local") => None,
            Some(url) => match dynrunner_gateway::parse_gateway_url(url).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "observer late-joiner: invalid --gateway value: {e}"
                ))
            })? {
                dynrunner_gateway::GatewayConfig::Local => None,
                dynrunner_gateway::GatewayConfig::Ssh(mut cfg) => {
                    // The auth-file knobs are gateway-config concerns,
                    // folded in post-parse exactly as the SLURM
                    // pipeline does on its Python gateway config.
                    cfg.identity_file = ssh_identity_file;
                    cfg.config_file = ssh_config_file;
                    Some(cfg)
                }
            },
        };
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
            gateway,
            distributed_config: distributed_config.unwrap_or_default(),
            holdings,
            panik_watcher_paths: panik_watcher_paths.unwrap_or_default(),
            panik_watcher_poll_interval_secs,
            completed: 0,
        })
    }
}
