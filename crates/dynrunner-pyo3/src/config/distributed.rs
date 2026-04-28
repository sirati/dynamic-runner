use pyo3::prelude::*;

/// Tuning knobs for the distributed primary/secondary loops.
///
/// All durations are seconds (f64 for sub-second precision). Defaults match
/// the migration plan §18: 5s keepalive interval, 3 missed keepalives before
/// declaring a peer dead, 600s connect timeout, 300s peer timeout, 1s
/// retry delay between secondary→primary connect attempts.
///
/// `keepalive_miss_threshold` is read by the failover voting code (Phase 2);
/// configurable now so callers don't have to revisit when failover lands.
#[pyclass(name = "DistributedConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedConfig {
    connect_timeout_secs: f64,
    connect_retry_delay_secs: f64,
    peer_timeout_secs: f64,
    keepalive_interval_secs: f64,
    keepalive_miss_threshold: u32,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 600.0,
            connect_retry_delay_secs: 1.0,
            peer_timeout_secs: 300.0,
            keepalive_interval_secs: 5.0,
            keepalive_miss_threshold: 3,
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
    ))]
    fn new(
        connect_timeout_secs: Option<f64>,
        connect_retry_delay_secs: Option<f64>,
        peer_timeout_secs: Option<f64>,
        keepalive_interval_secs: Option<f64>,
        keepalive_miss_threshold: Option<u32>,
    ) -> Self {
        let d = DistributedConfig::default();
        Self {
            connect_timeout_secs: connect_timeout_secs.unwrap_or(d.connect_timeout_secs),
            connect_retry_delay_secs: connect_retry_delay_secs
                .unwrap_or(d.connect_retry_delay_secs),
            peer_timeout_secs: peer_timeout_secs.unwrap_or(d.peer_timeout_secs),
            keepalive_interval_secs: keepalive_interval_secs.unwrap_or(d.keepalive_interval_secs),
            keepalive_miss_threshold: keepalive_miss_threshold.unwrap_or(d.keepalive_miss_threshold),
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
}

