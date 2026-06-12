use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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

/// Consumer custom-message hook (F5), invoked by the primary's
/// handler-dispatch decision once per delivered message:
/// `(origin_secondary_id, topic, data, important)`. The pyo3 layer wires
/// this to the duck-typed `TaskDefinition.custom_message_handler`
/// attribute (passing the live `PrimaryHandle` it captured, so the
/// handler IS the streamed-spawn site).
///
/// The `Result` return is the dispatch decision's input: `Ok(())` =
/// the consumer consumed the message (an IMPORTANT message is then
/// latched `Handled` in the replicated inbox, atomically with the
/// handler's effect mutations); `Err(reason)` = the consumer hook
/// RAISED — a USER ERROR: an important message transitions terminally
/// to `Failed` (never retried, the handler's partial effect discarded
/// unexecuted — see `primary/custom_message.rs`); a droppable one is
/// lost (at-most-once by contract).
pub type OnCustomMessage = Box<dyn FnMut(&str, &str, &[u8], bool) -> Result<(), String> + Send>;

/// A shared side-channel by which an [`OnPhaseEnd`] closure records that
/// the consumer's phase-end hook RAISED, without changing the closure's
/// `()` return.
///
/// Single concern: carry "the on_phase_end hook raised, with this
/// reason" from the closure (which runs at the phase-lifecycle call site
/// and is the only code that sees the raise) back to the coordinator
/// that fired it. The coordinator reads (and clears) the latch
/// immediately after invoking the closure; on a recorded raise it emits
/// [`crate::worker_signal::WorkerMgmtSignal::PolicyFatalExit`] onto the
/// decoupled worker-management bus — so the run surfaces a non-zero
/// [`crate::primary::RunError::FatalPolicyExit`] instead of the old
/// warn-and-continue false-green. The phase layer NEVER drives shutdown
/// directly (the dispatch-decoupling law): it only records, then emits a
/// signal the worker-management arm owns.
///
/// Why a shared latch rather than a callback-signature change: the
/// `OnPhaseEnd` type is consumed by the local manager, the secondary,
/// every test, and the promotion path; widening its return type ripples
/// across all of them. A captured latch keeps the callback shape
/// untouched — the consumer-hook closure (built in pyo3) captures one
/// clone and `record`s a raise; the coordinator holds the other clone
/// and `take`s it. Callers that do not wire a real latch (the local
/// manager, the secondary, tests) build their closure against a
/// [`PhaseHookRaiseLatch::detached`] one nobody reads — the closure's
/// raise log is unchanged and nothing surfaces, preserving the legacy
/// warn-and-continue exactly.
///
/// First-raise-wins: once a reason is recorded, a later raise in the
/// same run does not overwrite it (the originating cause is the one the
/// operator wants surfaced).
#[derive(Clone)]
pub struct PhaseHookRaiseLatch {
    inner: Arc<Mutex<Option<String>>>,
}

impl PhaseHookRaiseLatch {
    /// A fresh, empty latch. Clone it to share one end with the closure
    /// and the other with the coordinator that reads it.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// A latch with no reader. Constructed identically to [`Self::new`];
    /// the name documents the caller's intent — a closure built against
    /// a detached latch records into a latch the coordinator never reads
    /// (the local-manager / secondary / test paths), so a raise stays
    /// warn-and-continue.
    pub fn detached() -> Self {
        Self::new()
    }

    /// Record that the phase-end hook raised, with a human-readable
    /// reason. First-raise-wins: a no-op if a reason is already set.
    /// Called by the closure (off the GIL-reacquire path) the moment it
    /// observes the consumer hook raise.
    pub fn record(&self, reason: String) {
        let mut slot = self.inner.lock().expect("phase-hook raise latch poisoned");
        if slot.is_none() {
            *slot = Some(reason);
        }
    }

    /// Read and clear the recorded raise reason, if any. Called by the
    /// coordinator immediately after firing the hook; `Some(reason)`
    /// means the consumer hook raised and the run must fail.
    pub fn take(&self) -> Option<String> {
        self.inner
            .lock()
            .expect("phase-hook raise latch poisoned")
            .take()
    }
}

impl Default for PhaseHookRaiseLatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Default for [`PrimaryConfig::task_reconciliation_timeout`].
///
/// Deliberately GENEROUS (10 minutes): the probe is a backstop against
/// lost terminals, not a progress watchdog — the cost of a long timeout
/// is only how late a genuinely-lost task is recovered, while a short
/// one merely produces more (harmless, but noisy) probe round-trips for
/// long-running tasks. No existing knob is a coherent source: the
/// keepalive family measures CONNECTION silence (a holder running a
/// 20-minute nix build keepalives the whole time), and
/// `connect_timeout`/`peer_timeout` are setup/link budgets — none of
/// them measures "how long may one task legitimately stay quiet", so
/// the probe gets its own knob. Its own constant (rather than a bare
/// literal in `Default`) so the pyo3 config sites, which construct
/// `PrimaryConfig` exhaustively, name the same default.
pub const DEFAULT_TASK_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(600);

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

    /// Grace period after the count of alive worker-secondary members
    /// (`ClusterState::alive_worker_secondary_count` — every alive
    /// worker-secondary, the recognized primary's own co-located member
    /// included) reaches zero with a non-empty pool, before the
    /// operational loop gives up and exits cleanly with the still-pending
    /// tasks left stranded. Default `30s`.
    ///
    /// Without this timer the framework idles forever when
    /// `alive_worker_secondary_count == 0 && pool not empty` — the
    /// existing exit conditions (counter-based + pool-drained)
    /// never trip because no events arrive (no secondary left
    /// to send TaskComplete/TaskRequest). Operator pain: have to
    /// `kill` the primary process by hand. Surfaced in tokenizer's
    /// cohort-3 runs where SSH-tunnel blips killed all 5
    /// secondaries simultaneously and the run sat idle for
    /// minutes before the operator noticed.
    ///
    /// The primary's own co-located worker-secondary counts: in-process
    /// dispatch is dispatch, so a primary whose host carries the last
    /// live workers (the lone-survivor self-quorum path) keeps working
    /// instead of falsely stranding its pool. A genuinely-dead co-located
    /// secondary is removed by the keepalive sweep's unfiltered hard
    /// backstop like any other member, after which the timer arms
    /// honestly.
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

    /// Where to persist the roster's peer connection credentials (the
    /// per-peer pinned QUIC cert PEMs + dial info) on THIS node's
    /// LOCAL filesystem — see [`crate::peer_credentials`]. The primary
    /// stores the roster there at every `PeerInfo` fan-out
    /// (`send_peer_lists`), so a late-joiner observer spawned on the
    /// same host can pick the cert pins up and dial the mesh over
    /// QUIC with valid certs instead of degrading to WSS.
    ///
    /// `None` (the default) persists nothing — the right value for
    /// every primary that is NOT the local setup/submitter (a promoted
    /// compute-peer primary has no local late-joiner spawn dir to
    /// serve). The SLURM pipeline sets it to a file inside the run's
    /// local cert dir (`/tmp/db-runner-cert-<run_id>/`), explicitly
    /// NOT the shared cluster-visible `connection_info/` dir.
    pub peer_credentials_path: Option<std::path::PathBuf>,

    /// Per-task reconciliation-probe deadline (#308): how long a task
    /// may be in flight with NO terminal before the primary asks its
    /// holder secondary "do you still hold task X?"
    /// (`TaskHoldQuery`/`TaskHoldResponse`). A `held` answer re-arms
    /// the deadline — a task may legitimately run for many multiples
    /// of this value (long nix builds) and is simply re-confirmed once
    /// per window, so the timeout bounds RECOVERY LATENCY for a lost
    /// task, never task runtime. A `not held` answer fails + requeues
    /// the task through the backpressure-shaped path. No response
    /// inside the bounded window re-arms with NO action (the silent
    /// holder is the keepalive machinery's concern). Default
    /// [`DEFAULT_TASK_RECONCILIATION_TIMEOUT`] (600s) — see that
    /// constant for why this is its own knob rather than derived from
    /// the keepalive/connect families.
    pub task_reconciliation_timeout: Duration,
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
            forwarded_argv: Vec::new(),
            peer_credentials_path: None,
            task_reconciliation_timeout: DEFAULT_TASK_RECONCILIATION_TIMEOUT,
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

/// The quorum-proceed window must end STRICTLY BEFORE the secondaries'
/// setup deadline can fire — the fraction of `unconfigured_deadline` it
/// is capped at. The 20% margin absorbs the arming skew: secondaries arm
/// their deadline at boot, the primary arms its window only after the
/// submitter's sbatch + tunnel bring-up (≈20s in the asm-dataset LMU
/// trace), so equal values GUARANTEE the fleet dies first whenever any
/// secondary is missing.
pub const QUORUM_WINDOW_DEADLINE_FRACTION: f64 = 0.8;

/// Derive the primary's quorum-proceed (straggler) window — the
/// [`PrimaryConfig::connect_timeout`] value `wait_for_connections` waits
/// before proceeding with a partial fleet.
///
/// # The knob map this function exists to keep honest
///
/// THREE knobs historically shared the one 600s value, and the
/// asm-dataset LMU fleet death was the result:
///
/// * `DistributedConfig.connect_timeout_secs` (default 600) fed BOTH the
///   secondary's bootstrap DIAL budget (`dial_until_deadline` — a
///   transport patience knob, untouched here) AND the primary's
///   straggler window (this derivation's subject).
/// * `DistributedConfig.unconfigured_deadline_secs` (default 600) is the
///   secondaries' setup deadline — armed EARLIER than the primary's
///   window, so any missing secondary made the welcomed fleet expire
///   strictly before quorum-proceed.
/// * The old scale-aware `setup_deadline` (`max(60, n*15)`, ba889cd) was
///   removed with its knob (7d9129c7) — LMU_OPERATIONS still documents
///   it, but nothing computed it any more and the flat 600 won.
///
/// # Derivation
///
/// * `explicit = None` (operator left the knob at default): the full
///   [`QUORUM_WINDOW_DEADLINE_FRACTION`] ×
///   `secondary_unconfigured_deadline` — the default IS the cap (480s at
///   the 600s default deadline). The window is dominated by PER-NODE
///   container/image bring-up time, which does NOT scale with fleet
///   size — the old `max(60, n*15)` model derived exactly 60s for the
///   n=4 test-env fleet whose sequential ~50-90s-per-worker image loads
///   made welcoming within 60s physically impossible
///   (run_20260611_131736: 0/4 welcomed, fleet dead at +60s). One knob
///   (`unconfigured_deadline`) drives both sides coherently; an
///   operator who needs longer bring-up raises that one knob.
/// * `explicit = Some(v)`: the operator's value, capped at the same
///   fraction (WARN when the cap engages): a straggler window that
///   outlives the fleet's setup deadline can only ever proceed into
///   dead secondaries, which is strictly worse than proceeding earlier
///   with the same quorum. The cap is the structural belt to the
///   setup-liveness suspenders (the assembly beacon + the secondaries'
///   re-armable deadline keep a REACHABLE fleet alive regardless; the
///   cap bounds the blast radius for a fleet the beacon cannot reach).
pub fn derive_connect_timeout(
    explicit: Option<Duration>,
    secondary_unconfigured_deadline: Duration,
) -> Duration {
    let cap = secondary_unconfigured_deadline.mul_f64(QUORUM_WINDOW_DEADLINE_FRACTION);
    match explicit {
        Some(v) if v > cap => {
            tracing::warn!(
                requested_secs = v.as_secs_f64(),
                capped_secs = cap.as_secs_f64(),
                unconfigured_deadline_secs = secondary_unconfigured_deadline.as_secs_f64(),
                "explicit connect_timeout exceeds the fleet's setup deadline \
                 margin; capping the quorum-proceed window — a straggler wait \
                 that outlives the secondaries' unconfigured_deadline proceeds \
                 into a dead fleet (raise --unconfigured-deadline-secs to \
                 raise both)"
            );
            cap
        }
        Some(v) => v,
        None => cap,
    }
}

#[cfg(test)]
mod connect_timeout_tests {
    use super::*;

    /// Replay of run_20260611_131736 (asm-tokenizer test-env, n=4,
    /// --jobs 4): the UNSET default is the full 80%-of-deadline window
    /// (480s at the 600s default), NOT a fleet-size-scaled value.
    /// Per-node container/image bring-up (~50-90s per worker, sequential
    /// loads) dominates the welcome-wait and does NOT scale with n, so
    /// the old `max(60, n*15)` derived exactly 60s at n=4 and killed the
    /// run before any secondary could physically welcome.
    #[test]
    fn unset_default_is_deadline_fraction_not_fleet_scaled() {
        let d = Duration::from_secs(600);
        assert_eq!(
            derive_connect_timeout(None, d),
            Duration::from_secs(480),
            "unset derives 80% of the secondaries' unconfigured_deadline \
             (the production n=4 fleet died at the old n-scaled 60s)"
        );
        // And the default tracks the one knob: a raised deadline raises
        // the bring-up window with it.
        assert_eq!(
            derive_connect_timeout(None, Duration::from_secs(1200)),
            Duration::from_secs(960)
        );
    }

    /// The structural inversion guard: the derived straggler window is
    /// STRICTLY shorter than the secondaries' setup deadline, for both
    /// the unset-default and the explicit-operator shapes — equal values
    /// (the production 600/600) are exactly the fleet-death geometry.
    #[test]
    fn straggler_window_strictly_below_secondary_deadline() {
        let deadline = Duration::from_secs(600);
        // The production shape: explicit 600 vs deadline 600 → capped.
        let derived = derive_connect_timeout(Some(Duration::from_secs(600)), deadline);
        assert!(
            derived < deadline,
            "an explicit window equal to the setup deadline must be capped \
             below it (got {derived:?})"
        );
        assert_eq!(derived, Duration::from_secs(480), "80% of 600s");
        // An explicit value ABOVE the deadline (900 @ 600) is capped the
        // same way (with the WARN at the derivation site).
        let derived = derive_connect_timeout(Some(Duration::from_secs(900)), deadline);
        assert!(derived < deadline);
        assert_eq!(derived, Duration::from_secs(480));
        // The unset default is the cap itself — still strictly below.
        let derived = derive_connect_timeout(None, deadline);
        assert!(derived < deadline);
        assert_eq!(derived, Duration::from_secs(480));
    }

    /// An explicit operator value BELOW the cap is sovereign — the
    /// derivation never lengthens or shortens it (the 0.1s
    /// `test_lifecycle_hooks` fast-fail shape keeps working, and an
    /// operator's deliberate 120s stays 120s).
    #[test]
    fn explicit_value_below_cap_is_untouched() {
        assert_eq!(
            derive_connect_timeout(Some(Duration::from_millis(100)), Duration::from_secs(600)),
            Duration::from_millis(100)
        );
        assert_eq!(
            derive_connect_timeout(Some(Duration::from_secs(120)), Duration::from_secs(600)),
            Duration::from_secs(120)
        );
    }
}
