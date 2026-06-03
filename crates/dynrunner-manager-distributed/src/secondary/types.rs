//! Public configuration + certificate types and the run-outcome enum
//! for the `SecondaryCoordinator`. Single concern: define the
//! boundary-crossing data shapes that callers and the PyO3 wrapper
//! see. The coordinator's own state machine lives in `coordinator.rs`
//! and `mod.rs`.

use std::path::PathBuf;
use std::time::Duration;

/// Per-run control signal reported by
/// `SecondaryCoordinator::run_until_setup_or_done`.
///
/// This is the orthogonal "what should the caller do next" axis — it is
/// NOT the per-secondary terminal (which terminal it reached lives on the
/// [`crate::secondary::SecondaryLifecycle`] and is read back via
/// [`SecondaryTerminal`]). The PyO3 wrapper drives the secondary in a loop
/// and inspects this value to decide whether to run Python-side setup
/// discovery before re-entering, or to break out, read the lifecycle
/// terminal, and shut down.
///
/// - `SetupPending`: the secondary observed pre-staged mode with an
///   empty replicated ledger — the authority deferred task discovery to
///   the corpus-mounting secondaries (it sent an empty `InitialAssignment
///   { pre_staged_mode: true }` rather than seeding the ledger). The
///   process-tasks loop yielded (via `SecondaryCoordinator::
///   setup_discovery_pending`) so the caller can run Python's
///   `task.discover_items` against the locally-mounted staged source and
///   feed the result back via `ingest_setup_discovery` — which broadcasts
///   `PhaseDepsSet + TaskAdded` onto the mesh for the co-located
///   authoritative primary to pick up. The worker pool is left running;
///   re-entering `run_until_setup_or_done` resumes the loop, and the
///   fire-once latch (set by `ingest_setup_discovery`) prevents a
///   re-yield.
/// - `Terminal`: the loop reached one of its terminal exits. The
///   coordinator has driven the matching `SecondaryLifecycle` terminal
///   transition; the caller reads [`SecondaryCoordinator::terminal`] to
///   learn which one (`Done` / `Aborted` / `Panik`) and act on it. (A
///   `fatal_exit` is the one terminal NOT reported here — it propagates as
///   an `Err` from the run loop while recording `Failed` on the lifecycle.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    SetupPending,
    Terminal,
}

/// The per-secondary terminal outcome, projected from the module-private
/// [`crate::secondary::SecondaryLifecycle`] terminal to the public boundary
/// the PyO3 wrapper reads.
///
/// One value of this type describes how *this secondary* ended. The
/// lifecycle terminal is the single source of truth; this is its public
/// projection (see `SecondaryLifecycle::terminal`). The PyO3 boundary maps:
///
/// - `Done`: normal completion (RunComplete observed / clean drain-down) —
///   `exit(0)` / `Ok`.
/// - `Aborted`: the replicated ledger recorded `ClusterMutation::RunAborted`
///   (the failure twin of RunComplete; the cluster-wide non-zero cue for a
///   pre-phase duplicate-task-id #3a). `process_tasks` checks
///   `cluster_state.run_aborted()` BEFORE the `run_complete()` break.
///   `reason` is carried for the boundary log; the PyO3 wrapper calls
///   `std::process::exit(1)`.
/// - `Panik`: the panik watcher observed its sentinel file (or SIGTERM).
///   The coordinator announced its own departure (file source: a
///   self-authored `ClusterMutation::PeerRemoved { SelfDeparture }` —
///   observability only, peers are NOT terminated), took down every worker
///   AND its child tree with `pool.kill_all_workers_with_grace`, and the
///   PyO3 wrapper calls `std::process::exit(137)` for the SLURM wrapper to
///   reap. `matched_path` is the first panik file that existed (PyO3
///   shutdown-cause log); `reason` is the `"panik file: <path>"` shape
///   carried in the `SelfDeparture` payload.
/// - `Failed`: an unrecoverable local fault was latched (`fatal_exit`). The
///   run loop returns `Err(reason)`; the lifecycle records this terminal
///   with the same `reason`. (The run loop's `Err` is what drives the
///   boundary here, not a `RunOutcome::Terminal`.)
#[derive(Debug, Clone)]
pub enum SecondaryTerminal {
    Done,
    Aborted {
        reason: String,
    },
    Panik {
        matched_path: std::path::PathBuf,
        reason: String,
    },
    Failed {
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
    /// Maximum number of retry passes the AUTHORITY runs after the main
    /// pass drains. Mirrors `PrimaryConfig::retry_max_passes`. Default 1
    /// (total attempts per task = main pass + 1 retry pass = 2); 0
    /// disables retry.
    ///
    /// INERT on the `SecondaryCoordinator`: the secondary holds no
    /// dispatch authority and runs no retry machine. The retry concern is
    /// owned entirely by the co-located `PrimaryCoordinator` via its OWN
    /// `PrimaryConfig::retry_max_passes`. This field rides on
    /// `SecondaryConfig` only so the PyO3 wrapper can carry the operator
    /// knob through a single config struct; the unified composition
    /// threads the value into the co-located primary's config.
    pub retry_max_passes: u32,

    /// Number of retry passes for the per-phase OOM-retry bucket the
    /// AUTHORITY runs. Mirrors `PrimaryConfig::oom_retry_max_passes`.
    /// Default 1; `oom_retry_max_passes = 0` disables the OOM bucket so
    /// `ResourceExhausted(memory)` failures stay terminal.
    ///
    /// INERT on the `SecondaryCoordinator`, same disposition as
    /// `retry_max_passes`: the OOM-retry partition is the co-located
    /// `PrimaryCoordinator`'s concern, driven by its own
    /// `PrimaryConfig::oom_retry_max_passes`.
    pub oom_retry_max_passes: u32,

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

    /// Maximum wall-clock a secondary will spend NOT-YET-CONFIGURED —
    /// in the pre-`Operational` lifecycle states (`AwaitingPrimary` +
    /// `Configuring`), i.e. before the primary has announced itself and
    /// driven this secondary to `Operational`. Default 600s (10 min).
    ///
    /// Concern: a not-yet-configured secondary forms the peer mesh as far
    /// as it can but cannot spawn workers, accept a `TaskAssignment`, run
    /// an election, or send a keepalive (those capabilities are gated on
    /// `Operational`). The long pre-config deadline bounds how long the
    /// secondary waits for the primary to announce before concluding the
    /// cluster never came up; the short election deadline
    /// (`keepalive_interval × keepalive_miss_threshold`) is a property of
    /// `Operational` and physically cannot fire pre-`Operational`. This
    /// is generous (10 min) because a genuinely slow `discover_items` walk
    /// on the authority can legitimately delay the first announcement.
    ///
    /// This field is additive plumbing for the typed secondary lifecycle;
    /// it carries the configured default down to the state machine, which
    /// reads it once it owns the pre-config deadline.
    pub unconfigured_deadline: Duration,

    /// Legacy post-promotion quiesce grace (default 2 s).
    ///
    /// INERT on the `SecondaryCoordinator` post-unification: the
    /// alive-demoted natural-quiesce `RunComplete`-broadcast branch this
    /// gated lived on the secondary's deleted authority mirror. In the
    /// unified model a promoted node runs its co-located
    /// `PrimaryCoordinator`, which owns run-completion (`run_complete_check`
    /// reads the authoritative pool + CRDT directly), and the demoted
    /// node becomes a pure observer that exits solely on
    /// `cluster_state.run_complete()`. No secondary-side code reads this
    /// field; it is retained on `SecondaryConfig` for wire/config-shape
    /// stability with the PyO3 surface and may be removed once that
    /// surface is reshaped.
    pub promoted_primary_quiesce_grace: Duration,

    /// Per-task cap for externally-controlled `ReinjectTask`
    /// re-injections (the `PrimaryHandle::reinject_task` Python surface).
    /// Mirrors `PrimaryConfig::unfulfillable_reinject_max_per_task` —
    /// same semantics, same operator knob.
    ///
    /// INERT on the `SecondaryCoordinator`: the secondary drains no
    /// command channel and applies no reinject (those are authority
    /// mutations). The cap is enforced by the co-located
    /// `PrimaryCoordinator` via its own
    /// `PrimaryConfig::unfulfillable_reinject_max_per_task`; this field
    /// rides on `SecondaryConfig` only so the PyO3 wrapper carries the
    /// knob through a single config struct.
    ///
    /// `None` (default) means unbounded: the operator can re-inject
    /// the same Unfulfillable hash indefinitely. `Some(N)` caps the
    /// per-task budget; once a task hash has been re-injected `N`
    /// times via this surface, subsequent calls fail with the
    /// `unfulfillable_reinject_budget_exhausted` structured-log
    /// event and the entry stays in `TaskState::Unfulfillable`. The
    /// budget is owned and enforced by the authority (the co-located
    /// `PrimaryCoordinator` once this node is promoted), per its own
    /// `PrimaryConfig` copy of the same knob.
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

    /// Run-level output directory for memprofile artifacts.
    ///
    /// Resolved at the PyO3 boundary
    /// (`PySecondaryCoordinator::run` and `PyDistributedManager::run`)
    /// from the operator's `--memprofile` flag plus the secondary's
    /// operator-supplied `output_dir` (preferred) or the
    /// [`dynrunner_manager_local::memprofile::config::SLURM_SECONDARY_OUTPUT_DIR`]
    /// constant (legacy backstop). `Some(path)` means "operator
    /// opted in AND at least one anchor is available". `None`
    /// (default) disables profiling entirely.
    ///
    /// `Some(path)` drives two coupled effects through
    /// `SecondaryCoordinator::run_until_setup_or_done`:
    ///   * `initialize_workers` flips the `mem_manager_reserved_bytes`
    ///     argument to `Some(0)` if the operator did not already
    ///     supply one, so per-worker cgroup-v2 leaves materialise
    ///     under `<workers>/worker-<id>/` even when no
    ///     enforcement reservation was configured;
    ///   * a [`dynrunner_manager_local::memprofile::MemProfileSampler`]
    ///     is spawned post-`initialize_workers` and its hooks fire
    ///     from every assign / complete / disconnect site on the
    ///     secondary (initial-assignment, peer-routed dispatch,
    ///     primary self-assign, post-Ready pending-first-bind,
    ///     `WorkerEvent::TaskCompleted`, `WorkerEvent::Disconnected`).
    ///     The sampler is drained before every worker-pool teardown
    ///     path (`stop_all_workers`, `kill_all_workers_with_grace`).
    ///
    /// `None` leaves the workers cgroup behaviour untouched and
    /// every hook short-circuits as a no-op.
    pub output_dir: Option<PathBuf>,

    /// Path the per-task `WorkerEvent::TaskCompleted` handler
    /// appends a CSV row to for every task completion. Mirrors
    /// `LocalManagerConfig::memuse_log_path`; resolved at the
    /// PyO3 boundary via
    /// [`dynrunner_manager_local::memuse::derive_memuse_log_path`]
    /// from the operator's run-level output dir (default:
    /// `{output_dir}/memuse.log`). `None` keeps the secondary
    /// silent — preserves the test-fixture flexibility every
    /// other dispatch path has.
    pub memuse_log_path: Option<PathBuf>,
}

impl Default for SecondaryConfig {
    fn default() -> Self {
        Self {
            secondary_id: String::new(),
            num_workers: 1,
            max_resources: dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024,
            )]),
            hostname: String::new(),
            keepalive_interval: Duration::from_secs(1),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            // Mirrors `retry_max_passes` so OOM tasks keep their
            // own retry budget by default; flip to 0 for a
            // fail-fast OOM response without disabling Recoverable
            // retries.
            oom_retry_max_passes: 1,
            primary_link_failure_threshold: super::primary_link::DEFAULT_FAILURE_THRESHOLD,
            primary_link_failure_window: super::primary_link::DEFAULT_FAILURE_WINDOW,
            is_observer: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            unconfigured_deadline: Duration::from_secs(600),
            promoted_primary_quiesce_grace: Duration::from_secs(2),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the additive default: a `SecondaryConfig` built without
    /// touching `unconfigured_deadline` carries the 10-minute pre-config
    /// deadline. Guards against the default silently changing or the
    /// field being dropped from the `Default` impl.
    #[test]
    fn unconfigured_deadline_defaults_to_600s() {
        assert_eq!(
            SecondaryConfig::default().unconfigured_deadline,
            Duration::from_secs(600)
        );
    }

    /// Pins a NON-default value end of the plumb: a `Duration` set on the
    /// struct is read back unchanged. A default-only test would pass even
    /// if a caller layer dropped the value (the default would mask it), so
    /// this exercises that the field actually carries a distinct value.
    #[test]
    fn unconfigured_deadline_carries_a_passed_value() {
        let cfg = SecondaryConfig {
            unconfigured_deadline: Duration::from_secs(42),
            ..SecondaryConfig::default()
        };
        assert_eq!(cfg.unconfigured_deadline, Duration::from_secs(42));
    }
}
