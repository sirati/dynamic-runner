use std::collections::HashMap;
use std::time::Duration;

use std::collections::BTreeMap;

use dynrunner_core::{
    Identifier, PhaseId, SETUP_NODE_ID, TaskInfo, TaskOutputs, resolve_against_root,
};

/// Per-phase lifecycle hook invoked by the coordinator when a phase
/// flips Blocked → Active. The pyo3 layer (Phase 5B) wires this to the
/// Python `TaskDefinition.on_phase_start` so user code can spin up
/// per-phase resources (e.g. dedicated worker pools, dataset shards)
/// before items dispatch.
pub type OnPhaseStart = Box<dyn FnMut(&PhaseId) + Send>;

/// Per-phase lifecycle hook invoked when a phase reaches Drained
/// (`queued == 0` and `in_flight == 0`). Receives the phase id, the
/// counts of completed and failed items in that phase, AND the phase's
/// PUBLISHED task outputs keyed by `task_id` (`{ task_id: TaskOutputs }`
/// — each entry the producer's `publish_string` / `publish(.., key=..)`
/// accumulator, already converged into the primary's `task_outputs`
/// cache by the time the cascade fires this hook). The pyo3 layer wires
/// this to `TaskDefinition.on_phase_end` so user code can finalise
/// per-phase aggregates — and read a just-completed task's published
/// output WITHOUT a filesystem path — before the next phase activates.
///
/// The outputs map is owned (clones off `cluster_state.task_outputs`) so
/// the callback holds no borrow against the `&mut self` coordinator that
/// fires it. It is empty for a phase whose tasks published nothing.
pub type OnPhaseEnd = Box<dyn FnMut(&PhaseId, u32, u32, &BTreeMap<String, TaskOutputs>) + Send>;

/// Configuration for the primary coordinator.
pub struct PrimaryConfig {
    pub node_id: String,
    pub num_secondaries: u32,
    pub connect_timeout: Duration,
    pub peer_timeout: Duration,
    /// Cadence at which the operational loop checks for missed keepalives
    /// from secondaries. A secondary is declared dead after
    /// `keepalive_miss_threshold * keepalive_interval` of silence.
    pub keepalive_interval: Duration,
    /// Number of missed keepalives that constitute a death (default 3).
    pub keepalive_miss_threshold: u32,
    /// Staged silence-escalation thresholds for the honest dead-secondary
    /// declaration policy, expressed as multiples of `keepalive_interval`
    /// so the single cadence knob stays the only timing authority (and a
    /// test can drive sub-second stages by shrinking the interval).
    ///
    /// Each entry is a WARN stage: the first time a secondary's continuous
    /// silence crosses `multiple × keepalive_interval`, the heartbeat tick
    /// logs once at that stage and never re-warns for it. The entries are
    /// strictly ascending and all strictly below
    /// [`Self::silence_hard_multiple`]. WARN stages are LOG-ONLY — they do
    /// not declare a secondary dead.
    ///
    /// Default `[4, 12, 18]` ≈ `20s / 1m / 1m30` at the 5s default
    /// interval.
    pub silence_warn_multiples: Vec<u32>,
    /// The HARD declaration backstop, as a multiple of `keepalive_interval`
    /// and the last entry of the staged silence schedule. Once a
    /// secondary's continuous silence crosses
    /// `silence_hard_multiple × keepalive_interval`, the heartbeat tick
    /// declares it dead and requeues its in-flight tasks REGARDLESS of
    /// dispatch state. The backstop is REQUIRED: a purely starvation-driven
    /// declaration would never empty `secondaries`, so the fleet-dead arm
    /// would never arm and a fully-silent fleet would hang forever.
    ///
    /// Default `24` ≈ `2m` at the 5s default interval — the same order of
    /// magnitude as the secondary-side `primary_silence_backstop` (the
    /// symmetric primary-death detection on the secondary side).
    pub silence_hard_multiple: u32,
    /// Pre-staged source mode (`--source-already-staged`): when set,
    /// the data is bind-mounted into each secondary container at
    /// `src_network` from this gateway-side host path. No
    /// primary-driven staging or hash verification is needed. The
    /// secondary resolves files directly via `src_network/<rel>`
    /// where `<rel>` is what the primary computes by stripping this
    /// prefix from `TaskInfo.path` before sending the wire's
    /// `local_path` (see `wire_local_path`). `None` outside
    /// pre-staged mode.
    pub source_pre_staged_root: Option<std::path::PathBuf>,
    /// Whether the dispatched task items are backed by real files
    /// on the secondary's filesystem (the historical contract).
    /// When `false`, the framework passes `local_path` through to
    /// the worker as an opaque identifier — no `stat()`, no content
    /// hashing, no extraction-cache resolution. Workers that read
    /// their payload via JSON/stdin/comm-fd (not by opening a file
    /// at `TaskInfo.path`) flip this to `false` via
    /// `TaskDefinition.uses_file_based_items=False` so the framework
    /// doesn't perform load-bearing IO on a path the worker never
    /// touches.
    ///
    /// `true` outside the opt-out (default).
    pub uses_file_based_items: bool,

    /// Setup-promote intent for THIS coordinator's bootstrap. When
    /// `true`, the submitter has deferred task discovery / upload /
    /// ledger seeding to the chosen compute peer (the
    /// `--source-already-staged` path: files live on the cluster, not
    /// on the submitter). The discovery-yield is carried on the wire by
    /// `InitialAssignment.pre_staged_mode` (the chosen peer's
    /// `setup_discovery_pending` discriminator), NOT by the
    /// primary-changed announcement. On THIS primary the flag drives
    /// `setup_pending()` (suppresses the run-complete exits while
    /// discovery is pending) and `emit_setup_defer_handshake` (sends
    /// the degenerate empty `InitialAssignment { pre_staged_mode: true }`
    /// instead of seeding the ledger). Failover election deliberately
    /// ignores this field: at election time the local ledger is already
    /// non-empty, so re-running discovery would double-seed.
    pub required_setup_on_promote: bool,

    /// Per-type global concurrency caps. When a `TypeId` is present
    /// with capacity `N`, the scheduler refuses to dispatch more than
    /// `N` items of that type concurrently across all workers.
    /// Absent type → unconstrained (the historical behaviour). Set
    /// from `TaskTypeSpec.max_concurrent` per type.
    ///
    /// Use case: cap compile-heavy phases (e.g. `cores/4`) while
    /// letting cheap IO-bound phases run at the full `--jobs` width
    /// without rewriting the estimator API.
    pub max_concurrent_per_type: HashMap<dynrunner_core::TypeId, u32>,

    /// Number of retry passes to run after the main operational loop
    /// drains. Default `1` (one retry pass; matches the local
    /// manager's `retry_max_attempts` semantics).
    ///
    /// Each pass re-injects the tasks that failed in the previous
    /// pass and runs the operational loop again. A task that fails
    /// in a pass and fails again in the next stays in `failed_tasks`
    /// permanently. Set to `0` to disable retries (every Recoverable
    /// failure is terminal — useful for fail-fast CI).
    ///
    /// Why a pass-based retry instead of per-task counter: a worker
    /// that mis-classifies a permanent error as Recoverable (EROFS,
    /// missing config, etc.) would otherwise retry the same task
    /// hundreds of times per second until the SLURM time budget
    /// expires. The pass-based model bounds the cost to one extra
    /// dispatch per failed task. Secondary-died-then-requeue
    /// (handled in `requeue_dead_secondary`) does NOT count as a
    /// failure — those tasks were never actually failed, just lost
    /// their worker.
    ///
    /// Scope: per-phase, per-bucket. Each phase's Recoverable bucket
    /// has its own pass counter; OOM bucket has a SEPARATE counter
    /// (`oom_retry_max_passes`) for the same phase.
    pub retry_max_passes: u32,

    /// Number of retry passes for the per-phase OOM-retry bucket.
    /// Default mirrors `retry_max_passes` (=1) so existing configs
    /// keep the legacy "one retry across all classes" budget; setting
    /// the two independently lets a workload that wants fail-fast
    /// memory-pressure response (`oom_retry_max_passes = 0`) keep
    /// transient-error retries (`retry_max_passes >= 1`), or vice
    /// versa.
    ///
    /// Each phase has its own counter under this cap. The bucket runs
    /// AT the phase-drain edge, BEFORE `on_phase_end` fires. When the
    /// counter reaches the cap, the surviving OOM-failed tasks become
    /// terminal for that phase: `on_phase_end` fires with the per-class
    /// counts unchanged, `mark_phase_done` advances dependents.
    /// `oom_retry_max_passes = 0` disables the OOM bucket entirely
    /// (failures still land in `failed_tasks` with the
    /// `ResourceExhausted(memory)` classification — the run's outcome
    /// summary still reports them — but no second-chance dispatch is
    /// attempted before the phase advances).
    pub oom_retry_max_passes: u32,

    /// Grace period after the count of alive REMOTE worker-secondaries
    /// (`ClusterState::alive_remote_secondary_count` — alive
    /// worker-secondaries other than the recognized primary) reaches zero
    /// with a non-empty pool, before the operational loop gives up and
    /// exits cleanly with the still-pending tasks left stranded. Default
    /// `30s`.
    ///
    /// Without this timer the framework idles forever when
    /// `alive_remote_secondary_count == 0 && pool not empty` — the
    /// existing exit conditions (counter-based + pool-drained)
    /// never trip because no events arrive (no remote secondary left
    /// to send TaskComplete/TaskRequest). Operator pain: have to
    /// `kill` the primary process by hand. Surfaced in tokenizer's
    /// cohort-3 runs where SSH-tunnel blips killed all 5
    /// secondaries simultaneously and the run sat idle for
    /// minutes before the operator noticed.
    ///
    /// Counting REMOTE secondaries (excluding the recognized primary by
    /// identity) is what arms this correctly on a host that runs a primary
    /// alongside its own secondary under one peer-id: the primary's own
    /// secondary never keeps the timer disarmed, so such a primary cut off
    /// from every remote secondary still arms and strands rather than
    /// hanging on its own loopback secondary.
    ///
    /// Set to `Duration::ZERO` for fail-fast (exit at the moment
    /// the fleet first goes empty). Set to a long value if a
    /// re-sbatch path is wired into `spawn_secondary` (none today)
    /// and you want time for replacement secondaries to come up.
    pub fleet_dead_timeout: Duration,

    /// Maximum time to wait for every connected secondary to send
    /// `MeshReady` before issuing the `PrimaryChanged` announcement.
    /// Secondaries emit `MeshReady` once their peer-mesh has settled
    /// (mesh formed, watchdog elapsed, or no peers were expected for
    /// single-secondary runs). Without the wait, the primary
    /// previously announced `PrimaryChanged` ~750µs after every
    /// secondary completed cert-exchange — that left the
    /// newly-named primary "authoritative" against an empty peer
    /// mesh for the full per-peer dial budget (10s QUIC + 10s
    /// WSS), with every pre-mesh-formation message routed into
    /// the void. Default `60s` — comfortably larger than the
    /// secondary-side 30s peer-mesh watchdog plus a slack for
    /// scheduling jitter. Stragglers past this deadline log a
    /// warning and the run proceeds anyway (so a bug in one
    /// secondary's mesh signalling can't deadlock the entire
    /// dispatch).
    pub mesh_ready_timeout: Duration,

    /// Local source-tree root the primary uses to read file
    /// contents for the initial staging walk (content-hash + per-
    /// secondary StageFile fan-out). Threaded by every pyo3-side
    /// caller that has it (SLURM pipeline, in-process distributed
    /// manager, network primary with local secondaries) so a
    /// single field tells the manager whether it can read source
    /// files from the primary's filesystem. `None` for callers
    /// that don't (pre-staged-source mode bind-mounts the source
    /// into each secondary; `uses_file_based_items=false` makes
    /// `local_path` opaque; tests with absolute on-disk paths and
    /// fake workers that never open them).
    pub source_dir: Option<std::path::PathBuf>,

    /// Per-task budget cap for `PrimaryCommand::ReinjectTask` (the
    /// `PrimaryHandle::reinject_task` PyO3 entry point). `None`
    /// (the default) means unbounded — a control plane that
    /// keeps observing operator-resolvable failures can re-inject
    /// the same hash as often as it wants. `Some(N)` allows at
    /// most N successful reinjects per task; the (N+1)-th call
    /// returns `Err` to the caller and emits the structured-log
    /// event `unfulfillable_reinject_budget_exhausted` so an
    /// observability pipeline can alert.
    ///
    /// The counter is per-task (keyed by hash), not per-run-pass.
    /// It is NOT consumed by the retry-pass infrastructure
    /// (`retry_max_passes`) — those two retry channels are
    /// independent: `retry_max_passes` is the framework's auto-
    /// retry budget for Recoverable failures; this field is the
    /// external-control-plane budget for the
    /// operator-resolvable-failure (`Unfulfillable`) class.
    pub unfulfillable_reinject_max_per_task: Option<u32>,

    /// Maximum wall-clock a demoted submitter in setup-promote mode
    /// (`required_setup_on_promote = true`) will sit in the operational
    /// loop waiting for the promoted secondary's first
    /// `ClusterMutation::TaskAdded` / `TasksSpawned` / `RunComplete`
    /// broadcast. The latch consulted by this timer is `setup_pending`,
    /// initialised from `required_setup_on_promote` and cleared by the
    /// first of those three mutations arriving via the mirror path
    /// (see `setup_pending` doc on `PrimaryCoordinator`).
    ///
    /// Rationale: in setup-promote mode the demoted submitter has no
    /// load-bearing exit path while `setup_pending = true` — the
    /// counter-based and pool-drain exits are gated off behind the
    /// latch, `cluster_state.run_complete()` requires the promoted
    /// secondary to broadcast first, the fleet-dead arm
    /// (`alive_remote_secondary_count() == 0 && !pool().is_empty()`) is
    /// held off by its empty-pool guard — the demoted submitter's pool
    /// is unseeded while `setup_pending`, so the arm cannot fire even
    /// though the remote-secondary count itself can reach zero (it
    /// excludes the recognized promoted primary),
    /// and the `both transports closed` fallback only fires once every
    /// QUIC writer has finished its tear-down (which can take hours
    /// after a SLURM hard-kill). If the promoted secondary's discovery
    /// hangs, or its SLURM job dies before broadcasting any progress,
    /// the demoted submitter has nothing to break the wait. This
    /// deadline is the explicit, observable backstop.
    ///
    /// On expiry the operational loop exits and the outer
    /// `run_pipeline` surfaces `RunError::SetupDeadlineExpired`. The
    /// deadline is auto-cancelled (the arm parks on `pending().await`)
    /// the moment `setup_pending` clears, so a long-but-eventually-
    /// successful discovery does not false-fire.
    ///
    /// Default `600s` (10 minutes) — comfortably larger than typical
    /// `discover_items` walks (file-tree scans, hash computations) on
    /// production source trees, and well under the SLURM 60-min job
    /// time-limit so the operator gets a clear failure before the
    /// container is reaped. Set to a long value (e.g. `3600s`) if the
    /// consumer's discovery is genuinely long-running and the operator
    /// has scheduled a correspondingly large SLURM time-limit.
    ///
    /// Non-setup-promote runs (`required_setup_on_promote = false`)
    /// start with `setup_pending = false`, so the arm parks
    /// immediately and never fires regardless of this value — the
    /// field is harmless for the legacy bootstrap path.
    pub setup_promote_deadline: Duration,

    /// The consumer's run configuration — the byte-identical token
    /// sequence the framework forwards onto a joining / respawned /
    /// promoted node's own command line so it reconstructs the exact
    /// run-config the submitter launched with. This is a NODE-LOCAL
    /// launch constant (every node sources its own copy: the submitter
    /// primary from a config kwarg, a secondary from its cold-start
    /// fetch, a promoted node from the same fetched copy threaded into
    /// its `PrimaryConfig`), NOT replicated lattice data — so it lives
    /// on the config, never in `ClusterState`.
    ///
    /// Read-only on the coordinator: the `RequestRunConfig` responder
    /// answers a requesting peer with this verbatim. Default empty (a
    /// run with no forwarded args).
    pub forwarded_argv: Vec<String>,
}

impl Default for PrimaryConfig {
    fn default() -> Self {
        Self {
            node_id: SETUP_NODE_ID.into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(600),
            peer_timeout: Duration::from_secs(300),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            // ≈20s / 1m / 1m30 WARN stages, ≈2m HARD backstop at the 5s
            // default interval. The hard backstop mirrors the secondary-
            // side `primary_silence_backstop` order of magnitude.
            silence_warn_multiples: vec![4, 12, 18],
            silence_hard_multiple: 24,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            required_setup_on_promote: false,
            max_concurrent_per_type: HashMap::new(),
            retry_max_passes: 1,
            // Mirrors `retry_max_passes` so OOM tasks keep their
            // historical "one retry then permanent" budget unless the
            // operator opts out (`--oom-retry-max-passes 0`).
            oom_retry_max_passes: 1,
            fleet_dead_timeout: Duration::from_secs(30),
            mesh_ready_timeout: Duration::from_secs(60),
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: Duration::from_secs(600),
            forwarded_argv: Vec::new(),
        }
    }
}

impl PrimaryConfig {
    /// Compute the wire-side `local_path` for a TaskInfo. In normal
    /// mode it's `binary.path` verbatim; in pre-staged mode it's
    /// the path's tail relative to `source_pre_staged_root`, so the
    /// secondary's `src_network.join(<wire>)` resolves to the
    /// in-container bind-mount. The three legitimate `binary.path`
    /// shapes (see [`resolve_against_root`]) collapse to the right
    /// wire form here; out-of-tree paths fall through with a warn —
    /// the secondary's `resolve_pre_staged` will then fail
    /// NonRecoverable, surfacing the misconfiguration instead of
    /// silently routing the wrong file.
    pub fn wire_local_path<I: Identifier>(&self, binary: &TaskInfo<I>) -> String {
        let Some(root) = self.source_pre_staged_root.as_deref() else {
            return binary.path.to_string_lossy().into_owned();
        };
        let resolved = resolve_against_root(&binary.path, root);
        match resolved.relative {
            Some(rel) => rel.to_string_lossy().into_owned(),
            None => {
                tracing::warn!(
                    path = %binary.path.display(),
                    resolved = %resolved.absolute.display(),
                    root = %root.display(),
                    "wire_local_path: TaskInfo path doesn't sit under \
                     source_pre_staged_root; passing through unchanged \
                     — secondary will fail NonRecoverable"
                );
                binary.path.to_string_lossy().into_owned()
            }
        }
    }
}
