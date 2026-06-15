use pyo3::prelude::*;

/// Tuning knobs for primary tuning + distributed loops.
///
/// All durations are seconds (f64 for sub-second precision). Defaults match
/// the migration plan §18: 5s keepalive interval, 3 missed keepalives before
/// declaring a peer dead, 300s peer timeout, 1s retry delay between
/// secondary→primary connect attempts. The connect timeout defaults to 600s
/// for the secondary's bootstrap dial; the PRIMARY's quorum-proceed window
/// DERIVES from the secondaries' `unconfigured_deadline_secs` when the knob
/// is unset (see `connect_timeout_secs`).
///
/// `keepalive_miss_threshold` is read by the failover voting code (Phase 2);
/// configurable now so callers don't have to revisit when failover lands.
///
/// `retry_max_passes` governs both the live primary's `run_retry_passes`
/// and (post-demotion) the promoted secondary's
/// `primary_drain_check_and_retry`. The live primary owns retry
/// while it's authoritative; once it relinquishes the role via
/// `PrimaryChanged` and demotes, the promoted primary takes over retry
/// for tasks IT dispatched. Same knob
/// drives both sides so the cluster-level retry budget stays consistent
/// across the handover.
#[pyclass(name = "DistributedConfig", get_all, set_all, from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedConfig {
    /// `None` = the operator did not set it. The distinction is
    /// load-bearing for the PRIMARY's quorum-proceed window: unset
    /// derives the deadline-fraction default
    /// (`dynrunner_manager_distributed::derive_connect_timeout` —
    /// 80% of `unconfigured_deadline_secs`, the cap itself; per-node
    /// container bring-up dominates the welcome-wait and does not
    /// scale with fleet size), while an explicit value is honored
    /// (still capped, with a WARN).
    /// The SECONDARY's bootstrap-dial budget reads the concrete
    /// [`Self::connect_timeout`] accessor (explicit value or the 600s
    /// default) — a transport patience knob that deliberately does NOT
    /// auto-scale.
    connect_timeout_secs: Option<f64>,
    connect_retry_delay_secs: f64,
    peer_timeout_secs: f64,
    keepalive_interval_secs: f64,
    keepalive_miss_threshold: u32,
    retry_max_passes: u32,
    /// Per-phase OOM-retry pass budget. Independent of
    /// `retry_max_passes`; defaults to the same value so existing
    /// configs keep the legacy "one retry across all classes" budget.
    /// Set to 0 to disable OOM retries entirely (a phase whose only
    /// failures are `ResourceExhausted(memory)` advances on the first
    /// drain edge after the failures land). See
    /// `PrimaryConfig.oom_retry_max_passes` (Rust) for the per-bucket
    /// scope and the LMU-regression rationale.
    oom_retry_max_passes: u32,
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
    /// Maximum wall-clock a secondary will spend NOT-YET-CONFIGURED —
    /// in the pre-`Operational` lifecycle states (`AwaitingPrimary` +
    /// `Configuring`), before the primary has announced itself and
    /// driven this secondary to `Operational`. Defaults to 600s
    /// (10 minutes). This governs how long a typed secondary waits for
    /// the primary's first announcement while it forms the peer mesh
    /// but cannot yet spawn workers, accept tasks, run an election, or
    /// send a keepalive. Set this large when the authority's
    /// `discover_items` walk is genuinely slow.
    unconfigured_deadline_secs: f64,
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
    /// Per-secondary stuck-worker reporting cadence in seconds.
    /// Mirrors `LocalManagerConfig.phase_status_log_intervals_secs`.
    /// After one of this secondary's OWN workers sits in the same
    /// phase past any of these durations, the secondary emits a
    /// status WARN (current phase + elapsed) — the OBSERVABILITY twin
    /// of the LocalManager reporter. Default `vec![60.0]`. LOGGING
    /// ONLY: no kill/timeout path is wired off these intervals.
    phase_status_log_intervals_secs: Vec<f64>,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: None,
            connect_retry_delay_secs: 1.0,
            peer_timeout_secs: 300.0,
            keepalive_interval_secs: 5.0,
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            oom_retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window_secs: 30.0,
            unconfigured_deadline_secs: 600.0,
            resource_check_interval_secs: 0.1,
            log_oom_watcher: false,
            phase_status_log_intervals_secs: vec![60.0],
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
        oom_retry_max_passes = None,
        primary_link_failure_threshold = None,
        primary_link_failure_window_secs = None,
        unconfigured_deadline_secs = None,
        resource_check_interval_secs = None,
        log_oom_watcher = None,
        phase_status_log_intervals_secs = None,
    ))]
    // PyO3 kwargs surface — collapsing to a builder is a separate
    // API refactor.
    #[allow(clippy::too_many_arguments)]
    fn new(
        connect_timeout_secs: Option<f64>,
        connect_retry_delay_secs: Option<f64>,
        peer_timeout_secs: Option<f64>,
        keepalive_interval_secs: Option<f64>,
        keepalive_miss_threshold: Option<u32>,
        retry_max_passes: Option<u32>,
        oom_retry_max_passes: Option<u32>,
        primary_link_failure_threshold: Option<u32>,
        primary_link_failure_window_secs: Option<f64>,
        unconfigured_deadline_secs: Option<f64>,
        resource_check_interval_secs: Option<f64>,
        log_oom_watcher: Option<bool>,
        phase_status_log_intervals_secs: Option<Vec<f64>>,
    ) -> Self {
        let d = DistributedConfig::default();
        // Default `oom_retry_max_passes` mirrors the effective
        // `retry_max_passes` (post-default-fallback) so an operator
        // who bumps `retry_max_passes=3` gets the same OOM budget
        // implicitly. Explicit `oom_retry_max_passes=N` overrides.
        let effective_retry_max_passes = retry_max_passes.unwrap_or(d.retry_max_passes);
        Self {
            // `None` is preserved (NOT defaulted away): "unset" is the
            // signal the primary-side derivation keys the scale-aware
            // quorum-proceed window on.
            connect_timeout_secs: connect_timeout_secs.or(d.connect_timeout_secs),
            connect_retry_delay_secs: connect_retry_delay_secs
                .unwrap_or(d.connect_retry_delay_secs),
            peer_timeout_secs: peer_timeout_secs.unwrap_or(d.peer_timeout_secs),
            keepalive_interval_secs: keepalive_interval_secs.unwrap_or(d.keepalive_interval_secs),
            keepalive_miss_threshold: keepalive_miss_threshold
                .unwrap_or(d.keepalive_miss_threshold),
            retry_max_passes: effective_retry_max_passes,
            oom_retry_max_passes: oom_retry_max_passes.unwrap_or(effective_retry_max_passes),
            primary_link_failure_threshold: primary_link_failure_threshold
                .unwrap_or(d.primary_link_failure_threshold),
            primary_link_failure_window_secs: primary_link_failure_window_secs
                .unwrap_or(d.primary_link_failure_window_secs),
            unconfigured_deadline_secs: unconfigured_deadline_secs
                .unwrap_or(d.unconfigured_deadline_secs),
            resource_check_interval_secs: resource_check_interval_secs
                .unwrap_or(d.resource_check_interval_secs),
            log_oom_watcher: log_oom_watcher.unwrap_or(d.log_oom_watcher),
            phase_status_log_intervals_secs: phase_status_log_intervals_secs
                .unwrap_or(d.phase_status_log_intervals_secs),
        }
    }
}

impl DistributedConfig {
    /// The operator's explicit connect-timeout, `None` when unset — the
    /// input to the primary-side quorum-proceed-window derivation.
    pub(crate) fn connect_timeout_override(&self) -> Option<std::time::Duration> {
        self.connect_timeout_secs
            .map(std::time::Duration::from_secs_f64)
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
    pub(crate) fn oom_retry_max_passes(&self) -> u32 {
        self.oom_retry_max_passes
    }
    pub(crate) fn primary_link_failure_threshold(&self) -> u32 {
        self.primary_link_failure_threshold
    }
    pub(crate) fn primary_link_failure_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.primary_link_failure_window_secs)
    }
    pub(crate) fn unconfigured_deadline(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.unconfigured_deadline_secs)
    }
    pub(crate) fn resource_check_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.resource_check_interval_secs)
    }
    pub(crate) fn log_oom_watcher(&self) -> bool {
        self.log_oom_watcher
    }
    /// The configured stuck-worker intervals as `Duration`s, in
    /// declaration order — the value a `SecondaryConfig` reads to drive
    /// the shared per-worker phase-progress reporter.
    pub(crate) fn phase_status_log_intervals(&self) -> Vec<std::time::Duration> {
        self.phase_status_log_intervals_secs
            .iter()
            .map(|s| std::time::Duration::from_secs_f64(*s))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the kwarg default: omitting `unconfigured_deadline_secs`
    /// yields the 10-minute pre-config deadline at the `Duration`
    /// accessor a `SecondaryConfig` reads.
    #[test]
    fn unconfigured_deadline_defaults_to_600s() {
        assert_eq!(
            DistributedConfig::default().unconfigured_deadline(),
            std::time::Duration::from_secs(600)
        );
        // And via the kwarg-merge constructor with everything omitted.
        let cfg = DistributedConfig::new(
            None, None, None, None, None, None, None, None, None,
            /* unconfigured_deadline_secs */ None, None, None, None,
        );
        assert_eq!(
            cfg.unconfigured_deadline(),
            std::time::Duration::from_secs(600)
        );
    }

    /// Pins a NON-default value through the merge layer: a passed
    /// `unconfigured_deadline_secs` propagates to the `Duration`
    /// accessor instead of being silently dropped (which the default
    /// would mask). This is the load-bearing end-of-plumb check at the
    /// pyo3 kwarg boundary.
    #[test]
    fn unconfigured_deadline_kwarg_propagates() {
        let cfg = DistributedConfig::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            /* unconfigured_deadline_secs */ Some(123.0),
            None,
            None,
            None,
        );
        assert_eq!(
            cfg.unconfigured_deadline(),
            std::time::Duration::from_secs(123)
        );
    }
}
