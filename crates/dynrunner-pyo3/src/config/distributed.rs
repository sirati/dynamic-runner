use pyo3::prelude::*;

/// Tuning knobs for primary tuning + distributed loops.
///
/// All durations are seconds (f64 for sub-second precision). Defaults match
/// the migration plan §18: 5s keepalive interval, 3 missed keepalives before
/// declaring a peer dead, 600s connect timeout, 300s peer timeout, 1s
/// retry delay between secondary→primary connect attempts.
///
/// `keepalive_miss_threshold` is read by the failover voting code (Phase 2);
/// configurable now so callers don't have to revisit when failover lands.
///
/// `retry_max_passes` governs both the live primary's `run_retry_passes`
/// and (post-demotion) the SLURM-promoted secondary's
/// `slurm_primary_drain_check_and_retry`. The live primary owns retry
/// while it's authoritative; once it sends `PromotePrimary` and demotes,
/// the SLURM-primary takes over retry for tasks IT dispatched. Same knob
/// drives both sides so the cluster-level retry budget stays consistent
/// across the handover.
#[pyclass(name = "DistributedConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedConfig {
    connect_timeout_secs: f64,
    connect_retry_delay_secs: f64,
    peer_timeout_secs: f64,
    keepalive_interval_secs: f64,
    keepalive_miss_threshold: u32,
    retry_max_passes: u32,
    /// When true, the secondary skips starting a `PeerNetwork` and
    /// uses `NoPeerTransport` instead. Intended for clusters that
    /// firewall inter-compute-node networking (LMU SLURM and similar)
    /// where every peer dial would time out anyway. Note: this
    /// disables the failover/promote-slurm-primary path — with no
    /// peer mesh, primary loss = job loss.
    disable_peer_overlay: bool,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 600.0,
            connect_retry_delay_secs: 1.0,
            peer_timeout_secs: 300.0,
            keepalive_interval_secs: 5.0,
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            disable_peer_overlay: false,
        }
    }
}

#[pymethods]
impl DistributedConfig {
    #[new]
    #[pyo3(signature = (
        connect_timeout_secs = None,
        connect_retry_delay_secs = None,
        peer_timeout_secs = None,
        keepalive_interval_secs = None,
        keepalive_miss_threshold = None,
        retry_max_passes = None,
        disable_peer_overlay = None,
    ))]
    fn new(
        connect_timeout_secs: Option<f64>,
        connect_retry_delay_secs: Option<f64>,
        peer_timeout_secs: Option<f64>,
        keepalive_interval_secs: Option<f64>,
        keepalive_miss_threshold: Option<u32>,
        retry_max_passes: Option<u32>,
        disable_peer_overlay: Option<bool>,
    ) -> Self {
        let d = DistributedConfig::default();
        Self {
            connect_timeout_secs: connect_timeout_secs.unwrap_or(d.connect_timeout_secs),
            connect_retry_delay_secs: connect_retry_delay_secs
                .unwrap_or(d.connect_retry_delay_secs),
            peer_timeout_secs: peer_timeout_secs.unwrap_or(d.peer_timeout_secs),
            keepalive_interval_secs: keepalive_interval_secs.unwrap_or(d.keepalive_interval_secs),
            keepalive_miss_threshold: keepalive_miss_threshold.unwrap_or(d.keepalive_miss_threshold),
            retry_max_passes: retry_max_passes.unwrap_or(d.retry_max_passes),
            disable_peer_overlay: disable_peer_overlay.unwrap_or(d.disable_peer_overlay),
        }
    }
}

impl DistributedConfig {
    pub(crate) fn connect_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.connect_timeout_secs)
    }
    pub(crate) fn connect_retry_delay(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.connect_retry_delay_secs)
    }
    pub(crate) fn peer_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.peer_timeout_secs)
    }
    pub(crate) fn keepalive_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.keepalive_interval_secs)
    }
    pub(crate) fn keepalive_miss_threshold(&self) -> u32 {
        self.keepalive_miss_threshold
    }
    pub(crate) fn retry_max_passes(&self) -> u32 {
        self.retry_max_passes
    }
    pub(crate) fn disable_peer_overlay(&self) -> bool {
        self.disable_peer_overlay
    }
}

