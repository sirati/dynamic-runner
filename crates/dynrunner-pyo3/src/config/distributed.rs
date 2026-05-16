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
/// and (post-demotion) the promoted secondary's
/// `primary_drain_check_and_retry`. The live primary owns retry
/// while it's authoritative; once it sends `PromotePrimary` and demotes,
/// the primary takes over retry for tasks IT dispatched. Same knob
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
    /// Mass-death detection grace window in seconds. When ALL
    /// currently-connected secondaries appear in the dead list at
    /// the same heartbeat tick (correlated cause — gateway-side SSH
    /// tunnel collapse or similar single-point-of-failure), the
    /// primary defers requeue for this duration before declaring
    /// actual death. Set to 0 to disable. Defaults to 60s — covers
    /// the typical SSH ControlMaster reconnect window plus slack.
    /// See `PrimaryConfig.mass_death_grace` (Rust) for the full
    /// rationale.
    mass_death_grace_secs: f64,
    /// Minimum number of simultaneous deaths required to trigger
    /// mass-death detection. Default 2 — keeps singleton runs from
    /// biasing toward correlated inference.
    mass_death_min_count: u32,
    /// When true, the secondary skips starting a `PeerNetwork` and
    /// uses `NoPeerTransport` instead. Intended for clusters that
    /// firewall inter-compute-node networking (LMU SLURM and similar)
    /// where every peer dial would time out anyway. Note: this
    /// disables the failover/promote-primary path — with no
    /// peer mesh, primary loss = job loss.
    disable_peer_overlay: bool,
    /// R1 primary-link failover threshold: number of recv-None probes
    /// after which the secondary arms failover. Defaults to 5
    /// (matches `dynrunner_manager_distributed::secondary::primary_link::DEFAULT_FAILURE_THRESHOLD`).
    /// Bound below 3 risks self-promoting on a single dropped TCP
    /// packet retransmit — strongly discouraged.
    primary_link_failure_threshold: u32,
    /// R1 primary-link failover window in seconds. Wall-clock time
    /// after the first observed recv-None probe within which the
    /// failure-count threshold must breach to avoid time-based
    /// arming. Defaults to 30s (matches `DEFAULT_FAILURE_WINDOW`).
    /// Used to bound failover latency on slow-keepalive
    /// configurations where 5 probes would exceed the SLURM time
    /// budget.
    primary_link_failure_window_secs: f64,
    /// Maximum wall-clock the secondary will spend in setup phases
    /// (welcome + cert exchange + wait_for_setup) before concluding
    /// the cluster is dead and exiting cold. Defaults to 60s.
    /// Bounds the asm-dataset-nix T7 late-arrival scenario: when a
    /// SLURM-dispatched secondary boots AFTER the run has logically
    /// completed and the primary is gone, the transport's internal
    /// connection retries hang for ~6min before container teardown
    /// reaps the secondary; this deadline reaps it in 60s instead.
    setup_deadline_secs: f64,
    /// Per-secondary OOM resource-check decision cadence in seconds.
    /// Mirrors `LocalManagerConfig.resource_check_interval_secs`.
    /// Default 0.1 (100ms). Pre-extraction this was a hardcoded
    /// 100ms literal in `secondary/processing.rs`; surfacing it
    /// via the config makes it tunable from the operator side and
    /// keeps the local-vs-secondary surfaces symmetric.
    resource_check_interval_secs: f64,
    /// Master switch for the structured OOM-watcher JSON log on
    /// secondaries (and the primary's in-process secondary, when
    /// it has workers). When `true`, the watcher emits heartbeat +
    /// delta + kill log lines at `target = "oom_watcher"`. Operators
    /// flip this via `--log-oom-watcher`. Default `false`.
    log_oom_watcher: bool,
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
            mass_death_grace_secs: 60.0,
            mass_death_min_count: 2,
            disable_peer_overlay: false,
            primary_link_failure_threshold: 5,
            primary_link_failure_window_secs: 30.0,
            setup_deadline_secs: 60.0,
            resource_check_interval_secs: 0.1,
            log_oom_watcher: false,
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
        mass_death_grace_secs = None,
        mass_death_min_count = None,
        disable_peer_overlay = None,
        primary_link_failure_threshold = None,
        primary_link_failure_window_secs = None,
        setup_deadline_secs = None,
        resource_check_interval_secs = None,
        log_oom_watcher = None,
    ))]
    fn new(
        connect_timeout_secs: Option<f64>,
        connect_retry_delay_secs: Option<f64>,
        peer_timeout_secs: Option<f64>,
        keepalive_interval_secs: Option<f64>,
        keepalive_miss_threshold: Option<u32>,
        retry_max_passes: Option<u32>,
        mass_death_grace_secs: Option<f64>,
        mass_death_min_count: Option<u32>,
        disable_peer_overlay: Option<bool>,
        primary_link_failure_threshold: Option<u32>,
        primary_link_failure_window_secs: Option<f64>,
        setup_deadline_secs: Option<f64>,
        resource_check_interval_secs: Option<f64>,
        log_oom_watcher: Option<bool>,
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
            mass_death_grace_secs: mass_death_grace_secs.unwrap_or(d.mass_death_grace_secs),
            mass_death_min_count: mass_death_min_count.unwrap_or(d.mass_death_min_count),
            disable_peer_overlay: disable_peer_overlay.unwrap_or(d.disable_peer_overlay),
            primary_link_failure_threshold: primary_link_failure_threshold
                .unwrap_or(d.primary_link_failure_threshold),
            primary_link_failure_window_secs: primary_link_failure_window_secs
                .unwrap_or(d.primary_link_failure_window_secs),
            setup_deadline_secs: setup_deadline_secs.unwrap_or(d.setup_deadline_secs),
            resource_check_interval_secs: resource_check_interval_secs
                .unwrap_or(d.resource_check_interval_secs),
            log_oom_watcher: log_oom_watcher.unwrap_or(d.log_oom_watcher),
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
    pub(crate) fn mass_death_grace(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.mass_death_grace_secs)
    }
    pub(crate) fn mass_death_min_count(&self) -> u32 {
        self.mass_death_min_count
    }
    pub(crate) fn disable_peer_overlay(&self) -> bool {
        self.disable_peer_overlay
    }
    pub(crate) fn primary_link_failure_threshold(&self) -> u32 {
        self.primary_link_failure_threshold
    }
    pub(crate) fn primary_link_failure_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.primary_link_failure_window_secs)
    }
    pub(crate) fn setup_deadline(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.setup_deadline_secs)
    }
    pub(crate) fn resource_check_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.resource_check_interval_secs)
    }
    pub(crate) fn log_oom_watcher(&self) -> bool {
        self.log_oom_watcher
    }
}

