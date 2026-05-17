//! Public configuration + certificate types and the run-outcome enum
//! for the `SecondaryCoordinator`. Single concern: define the
//! boundary-crossing data shapes that callers and the PyO3 wrapper
//! see. The coordinator's own state machine lives in `coordinator.rs`
//! and `mod.rs`.

use std::path::PathBuf;
use std::time::Duration;

/// Outcome reported by `SecondaryCoordinator::run_until_setup_or_done`.
///
/// The PyO3 wrapper drives the secondary in a loop and inspects this
/// value to decide whether to run Python-side setup discovery before
/// re-entering, or to break out and shut down. The Rust-only callers
/// (tests, the existing `run` entry point) only ever observe `Done` —
/// `SetupPending` requires a `required_setup: true` wire promotion,
/// which never happens in those contexts.
///
/// - `SetupPending`: the secondary was promoted with `required_setup =
///   true` and the process-tasks loop yielded so the caller can run
///   Python's `task.discover_items` against the locally-mounted staged
///   source and feed the result back via `ingest_setup_discovery`. The
///   worker pool is left running; re-entering `run_until_setup_or_done`
///   resumes the loop.
/// - `Done`: the loop reached one of its normal terminations
///   (RunComplete observed, drain-down after primary disconnect, or
///   single-secondary clean exit). The worker pool has been stopped
///   and the secondary is finished.
/// - `PanikShutdown`: the operator-initiated panik watcher observed
///   its sentinel file. The coordinator broadcast
///   `ClusterMutation::PanikRequested`, took down every worker AND
///   its child tree with `pool.kill_all_workers_with_grace`, and is
///   returning so the PyO3 wrapper can call `std::process::exit(137)`
///   for the SLURM wrapper to reap. `matched_path` is the first
///   panik file that existed (used by the PyO3 wrapper for the
///   shutdown-cause log); `reason` is the human-readable shape
///   `"panik file: <path>"` carried in the broadcast
///   `ClusterMutation::PanikRequested.reason` field.
///
/// Note: `RunOutcome` is no longer `Copy`/`Eq` — the `PanikShutdown`
/// variant carries a `PathBuf` + `String` payload. Existing call
/// sites that pattern-match on the variant continue to compile;
/// no production site compared `RunOutcome` values for equality.
#[derive(Debug, Clone)]
pub enum RunOutcome {
    SetupPending,
    Done,
    PanikShutdown {
        matched_path: std::path::PathBuf,
        reason: String,
    },
}

/// Configuration for the secondary coordinator.
pub struct SecondaryConfig {
    pub secondary_id: String,
    pub num_workers: u32,
    pub max_resources: dynrunner_core::ResourceMap,
    pub hostname: String,
    pub keepalive_interval: Duration,
    /// Directory containing ZIP files (for SLURM mode). `None` for local/channel mode.
    pub src_network: Option<PathBuf>,
    /// Temporary directory for extracted binaries. Defaults to a temp dir if `None`.
    pub src_tmp: Option<PathBuf>,
    /// Peer timeout threshold (default: 120s). A peer is considered dead if no
    /// keepalive is received within this duration.
    pub peer_timeout: Duration,
    /// Number of missed keepalives from the primary before the secondary
    /// suspects primary death and starts the failover election (default 3,
    /// matching the primary's `keepalive_miss_threshold`).
    pub keepalive_miss_threshold: u32,
    /// Maximum number of retry passes the primary runs after the
    /// main pass drains. Mirrors `PrimaryConfig::retry_max_passes` —
    /// pre-demotion the local primary owned this; post-demotion the
    /// promoted secondary owns retry for tasks IT dispatched, so the
    /// same knob has to live on this side too. Default 1 (so total
    /// attempts per task = main pass + 1 retry pass = 2). 0 disables
    /// retry entirely on the primary side.
    ///
    /// Only consulted when this secondary is acting as primary
    /// (`is_primary == true`). On non-promoted secondaries the
    /// field is inert — the live primary's `retry_max_passes` is what
    /// drives retry while the live primary is still authoritative.
    pub retry_max_passes: u32,

    /// Number of consecutive primary-link recv-None probes after
    /// which the secondary arms failover (i.e. sets
    /// `primary_disconnected = true` and lets the election state
    /// machine take over). Default is `super::primary_link::DEFAULT_FAILURE_THRESHOLD`
    /// (5). Lower values arm faster — but bounding below 3 risks
    /// promoting on a single dropped TCP packet retransmit, which is
    /// wrong (per the architectural invariant: a transient packet
    /// drop is not a leadership event).
    pub primary_link_failure_threshold: u32,

    /// Wall-clock window after the first observed primary-link recv
    /// failure within which the threshold-attempts counter must
    /// breach to avoid time-based arming. Default is
    /// `super::primary_link::DEFAULT_FAILURE_WINDOW` (30s). Used to bound
    /// failover latency on slow-keepalive configurations where 5
    /// probes would exceed the SLURM time budget.
    pub primary_link_failure_window: Duration,

    /// Observer mode: this secondary participates in cluster updates
    /// (ClusterMutation broadcasts, PeerInfo, Keepalive, peer-routed
    /// task-state messages) but cannot become primary and has no
    /// workers. Use case: the dispatcher in SLURM mode hosts an
    /// in-process observer so it stays connected to the cluster as
    /// a non-candidate secondary even after a primary handoff/death
    /// — the surviving SLURM secondaries elect among themselves and
    /// the dispatcher's observer just receives the broadcasts.
    ///
    /// When `is_observer = true`:
    ///   - `num_workers` should be 0 (no work to take on); the
    ///     framework does not validate this, but processing-loop
    ///     paths that iterate workers behave correctly with an
    ///     empty pool.
    ///   - The election state machine refuses to enter `Candidate`
    ///     state — the observer never self-promotes even when it
    ///     would otherwise be the lowest-id alive peer. See
    ///     `election.rs::run_election_tick`'s `we_lead` branch.
    ///   - A `PromotePrimary` naming this secondary is rejected
    ///     with a loud error (defensive: should not happen if peers
    ///     honour the same flag, but protects against a misconfigured
    ///     peer or a wire-level forgery).
    ///
    /// Default `false` (regular secondary). The peer-mesh-side
    /// fortification (peers filtering observers from `lowest_alive`
    /// candidate selection) requires extending `PeerConnectionInfo`
    /// with this flag; tracked as a follow-up to this commit.
    pub is_observer: bool,

    /// How often the OOM/resource-pressure check fires inside the
    /// secondary's processing loop. Mirrors
    /// `LocalManagerConfig::resource_check_interval`. Default: 100ms.
    ///
    /// Pre-extraction this was a hardcoded `Duration::from_millis(100)`
    /// literal in `processing.rs` — the config-driven plumbing makes
    /// secondary and LocalManager symmetric so operators can tune the
    /// decision cadence via the same knob in both modes.
    pub resource_check_interval: Duration,

    /// Master switch for the structured OOM-watcher JSON log
    /// (`target = "oom_watcher"`). When `true`, the per-secondary
    /// watcher emits heartbeat + delta + kill log lines. When `false`
    /// (default), the watcher still samples and drives the scheduler
    /// decision but emits no log events. Mirrors
    /// `LocalManagerConfig::log_oom_watcher`; surfaced to operators
    /// via the `--log-oom-watcher` CLI flag and propagated to
    /// secondaries through the SLURM wrapper's `forwarded_argv`.
    pub log_oom_watcher: bool,

    /// Maximum wall-clock the secondary will spend in setup phases
    /// (send_welcome + send_cert_exchange + wait_for_setup) before
    /// concluding the cluster is dead and exiting cold. Default 60s.
    ///
    /// Concern: a late-arriving secondary scheduled AFTER the run
    /// has logically completed (primary already exited) cannot reach
    /// the now-dead primary URL. Without this deadline the transport
    /// layer's internal connection retries hold the boot path
    /// indefinitely (asm-dataset-nix T7 attempt 2 observed
    /// ~345 retries × 1s = ~6min before SLURM container teardown
    /// reaped the secondary). 60s gives a slow primary handshake
    /// enough headroom on healthy clusters; well under SLURM's
    /// per-job minimum so a dead-cluster boot reaps fast.
    ///
    /// `R1` (mid-run primary disconnect detection) deliberately
    /// lives in the processing loop, not the setup loop — the
    /// setup-phase `wait_for_setup` is documented as cancellation-
    /// unsafe under tokio::select! racing of `recv()` (see
    /// `setup.rs:79-96`), so we apply the deadline at the
    /// orchestration boundary instead of nested inside the recv
    /// loop. On timeout the recv future is cancelled at the outer
    /// boundary, no subsequent iteration touches the (possibly
    /// partial) transport state, so the cancellation hazard the
    /// setup-loop comment warns about does not arise.
    pub setup_deadline: Duration,

    /// Minimum wall-clock time the promoted-primary natural-quiesce
    /// `RunComplete`-broadcast branch waits after a `is_primary: false →
    /// true` transition before considering itself eligible to fire.
    ///
    /// The alive-demoted natural-quiesce branch (in `process_tasks`)
    /// declares the cluster done based on a CRDT-derived predicate
    /// (`task_count() > 0 && pending == 0 && in_flight == 0`), which
    /// is **incomplete-mirror-prone** in the immediate post-promotion
    /// window: a freshly promoted secondary may hold only the
    /// fraction of `TaskAdded` broadcasts the demoted primary had
    /// already flushed. Firing on the partial view (e.g. "5 of 10
    /// tasks added + all 5 already terminal") strands the in-flight
    /// remainder once the loopback `RunComplete` reaches the demoted
    /// primary and tears down its operational loop
    /// (asm-dataset-nix T11: 5/10 phase_build tasks unassigned).
    ///
    /// Default 2 s. Sized to bracket worst-case loopback latency
    /// (single-digit ms typical, hundreds of ms under network stress)
    /// with three orders of magnitude headroom, while still finite
    /// enough that an actually-quiesced cluster fires inside any
    /// reasonable SLURM time budget. Operators can shorten this for
    /// fast-feedback test environments or lengthen it for tunnelled
    /// production clusters with higher latency.
    ///
    /// This is a documented bandage: the structurally clean fix is
    /// a wire signal from the demoted primary saying "I'm done
    /// publishing", which the protocol does not have today.
    pub promoted_primary_quiesce_grace: Duration,

    /// Per-task cap for externally-controlled `ReinjectTask`
    /// re-injections (i.e. the `PrimaryHandle::reinject_task` Python
    /// surface, dispatched through the secondary-side command
    /// channel). Mirrors `PrimaryConfig::unfulfillable_reinject_max_per_task`
    /// — same semantics, same operator knob — only this copy is
    /// consulted when this node is acting as the promoted primary
    /// (when external reinject commands arrive on the secondary's
    /// command channel rather than the live primary's).
    ///
    /// `None` (default) means unbounded: the operator can re-inject
    /// the same Unfulfillable hash indefinitely. `Some(N)` caps the
    /// per-task budget; once a task hash has been re-injected `N`
    /// times via this surface, subsequent calls fail with the
    /// `unfulfillable_reinject_budget_exhausted` structured-log
    /// event and the entry stays in `TaskState::Unfulfillable`.
    ///
    /// Independent of the primary's counter — see
    /// `secondary/primary/reinject_task.rs` for the budget-reset-at-
    /// promotion semantics.
    pub unfulfillable_reinject_max_per_task: Option<u32>,

    /// Bytes to reserve for the secondary process itself when nesting
    /// the workers subgroup under cgroup-v2.
    ///
    /// `None` (default) means "skip nesting": the workers run in the
    /// same flat cgroup as the secondary, which is the pre-fix
    /// behaviour. A kernel cgroup-OOM at the parent cap then reaps
    /// the secondary alongside its workers — fine for development
    /// but bad in production because the framework loses the
    /// chance to observe the kill, requeue the displaced task, and
    /// report cleanly.
    ///
    /// `Some(0)` creates the nested workers subgroup but does NOT
    /// tighten its `memory.max` — useful for measuring the
    /// kernel-OOM-isolation benefit without giving up any of the
    /// container's RAM. `Some(n)` reserves `n` bytes for the
    /// secondary's own state and sets `workers/memory.max =
    /// parent.memory.max - n`. The framework's standard default
    /// (surfaced via the `--mem-manager-reserved` CLI flag) is
    /// `500 MiB`, sized for the estimator scratch + the per-
    /// secondary HashMaps + a comfortable margin.
    ///
    /// The actual cgroup write happens in
    /// [`crate::pool::WorkerPool::initialize`] via the
    /// [`crate::cgroup`] module; this field is the wire-shape
    /// the secondary-side config carries.
    pub mem_manager_reserved_bytes: Option<u64>,
}

impl Default for SecondaryConfig {
    fn default() -> Self {
        Self {
            secondary_id: String::new(),
            num_workers: 1,
            max_resources: dynrunner_core::ResourceMap::from([(dynrunner_core::ResourceKind::memory(), 1024 * 1024 * 1024)]),
            hostname: String::new(),
            keepalive_interval: Duration::from_secs(1),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            primary_link_failure_threshold: super::primary_link::DEFAULT_FAILURE_THRESHOLD,
            primary_link_failure_window: super::primary_link::DEFAULT_FAILURE_WINDOW,
            is_observer: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            setup_deadline: Duration::from_secs(60),
            promoted_primary_quiesce_grace: Duration::from_secs(2),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
        }
    }
}
/// Certificate info for peer connections, set before `run()`.
pub struct PeerCertInfo {
    pub public_cert_pem: String,
    pub ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
    pub quic_port: u16,
}
