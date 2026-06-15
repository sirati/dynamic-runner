//! The standalone observer coordinator.
//!
//! # Single concern
//!
//! Own the lifecycle of a ZERO-AUTHORITY observer node: hold the
//! replicated [`ClusterState`] mirror, apply (never originate) every
//! mutation that flows through the mesh, narrate the run for the operator,
//! and exit ONLY on a run terminal it OBSERVED (the primary's RunComplete /
//! RunAborted verdict, or its own local panik). A loss of the observer's
//! OWN transport visibility is NEVER a terminal — it is reported + retried
//! (BUG-B; see [`crate::observer::lost_visibility`]). The observer is its
//! OWN role/component: it has no scheduler, no worker pool, no dispatch
//! authority, and originates no `PrimaryChanged`. It is NOT a
//! [`crate::SecondaryCoordinator`] in a mode — there is no `is_observer`
//! flag anywhere on this type.
//!
//! # Module boundary
//!
//! Callers (the PyO3 observer dispatcher, the relocation wave that builds
//! an [`ObserverHandoff`]) construct via [`ObserverCoordinator::new`]
//! (cold-join) or [`ObserverCoordinator::from_handoff`] (relocation) and
//! drive the single [`ObserverCoordinator::run`] loop. Everything else —
//! the visibility-recheck clock, the lost-visibility reporter, the
//! apply-only mirror, the send wrapper, the teardown discipline — is
//! private. The observer COMPOSES the
//! role-agnostic primitives (`anti_entropy`, `run_narrator`,
//! `panik_watcher`, `observer::{announcer, reporting, failure_response,
//! lifecycle}`, `task_completed`) rather than re-implementing any of them.
//!
//! # Apply-only mirror
//!
//! Every inbound `ClusterMutation` batch is applied through the PURE
//! [`ClusterState::apply`] per mutation — NEVER the primary/secondary
//! `handle_cluster_mutation` / `apply_cluster_mutations` paths (which do
//! authority-bearing work). The observer never re-broadcasts a mutation it
//! applied; the only frames it ever sends are its OWN: the bootstrap
//! snapshot request, the anti-entropy digest, the resource-holdings
//! announce, and (on panik) a self-departure `PeerRemoved`.
//!
//! # Top-of-loop ordering invariant (load-bearing)
//!
//! Each iteration, BEFORE awaiting events:
//!   1. narrate (emit any pending phase / summary line),
//!   2. `run_aborted()` ⇒ exit 1 (checked FIRST) — the PRIMARY's verdict,
//!   3. `run_complete()` ⇒ exit 0 — the PRIMARY's verdict,
//!   4. otherwise compute the observer's own VISIBILITY (any peer
//!      reachable + the named primary not silent) and feed the
//!      [`LostVisibilityReporter`] — which REPORTS lost / retrying (full
//!      log immediately; the operator wake stream only after the 5-minute
//!      threshold, with the reconnection note riding the next wake log —
//!      see the module's wake-stream policy) and NEVER exits.
//!
//! The observer terminates ONLY on an OBSERVED run-terminal: `run_complete`
//! (Done), `run_aborted` (Aborted — the primary's broadcast verdict), or
//! its OWN local panik (137). It carries ZERO authority over the run, so
//! its loss of transport visibility — zero peers, a silent named primary,
//! a dropped `-R` reverse tunnel — is NEVER a run verdict: it is reported
//! as "connection lost, retrying" AND it actively rebuilds its path. The
//! relocated submitter→observer's [`crate::observer::reconnect`] port
//! rebuilds the dropped `-R` tunnel (the compute peers dial the submitter
//! over those tunnels; that transport has no dial path / no QUIC reconnect
//! ticker of its own, so the observer must DRIVE the rebuild). The observer
//! keeps observing until the primary's terminal converges into the CRDT
//! (or the run truly ends, observed). See
//! [`crate::observer::lost_visibility`] + [`crate::observer::reconnect`].

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dynrunner_core::{BoundedString, IMPORTANT_TARGET, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, KeepaliveRole, RemovalCause, SendTarget,
    StateDigest, resolve_destination, routing_target_for,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;

use crate::anti_entropy;
use crate::cluster_state::ClusterState;
use crate::observer::announcer::{AnnouncerOutboxItem, PeerMeshAnnouncerSender};
use crate::observer::cluster_gone::{ClusterGoneDetector, ClusterGoneVerdict};
use crate::observer::failure_response::{ErrorAggregationPolicy, InvalidTaskMonitorPolicy};
use crate::observer::fleet_death::{FleetDeathDetector, FleetDeathVerdict};
use crate::observer::job_ledger::{ClusterTerminalOutcome, JobLedgerProbeHandle};
use crate::graceful_abort_trigger::GracefulAbortTrigger;
use crate::observer::lifecycle::{AnnouncerHandle, attach_observer_announcer};
use crate::observer::lost_visibility::{
    EndedOutage, LostVisibilityReporter, MeshLiveness, RetryDirective, Visibility, WakeNoteSlot,
};
use crate::observer::reconnect::ReconnectorHandle;
use crate::observer::reporting::{SharedSnapshotSource, StatsSnapshot, TokioClock, run_reporter};
use crate::observer::run_observer_announcer;
use crate::panik_watcher::{self, PanikSignal, PanikWatcherConfig};
use crate::primary::RunError;
use crate::process::{MeshClient, RoleInbox};
use crate::observer::task_narrator::ObserverTaskNarrator;
use crate::run_narrator::RunNarrator;
use crate::task_completed::{
    TaskCompletedEvent, TaskCompletedListener, run_collector, run_task_completed_dispatcher,
    windowed_failure_collector,
};
use crate::task_state_change::TaskStateChangeEvent;
use crate::warn_throttle::WarnThrottle;

/// Minimum spacing between two anti-entropy fault WARNs (empty mesh
/// registry / failed send). The cadence detects the same outage every
/// ~20s tick; this gate keeps the fault LOUD (never silent — the
/// run_20260610 wedge) without one WARN per tick for its duration. The
/// suppressed-occurrence count rides each emitted WARN.
const AE_FAULT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Configuration for a standalone observer. Carries only the values the
/// observer's own concerns read: the node identity, the lost-visibility
/// report thresholds, and the panik trigger inputs. It carries NO scheduler
/// / worker / dispatch fields — an observer has none of those concerns.
#[derive(Debug, Clone)]
pub struct ObserverConfig {
    /// This observer's own peer-id — the bootstrap-RPC return address and
    /// the `local_id` the send edge resolves loopback against (the
    /// observer never loops back, but the resolver still needs it).
    pub node_id: String,
    /// Visibility re-check cadence. The interval at which the run loop
    /// re-evaluates its visibility (and the [`LostVisibilityReporter`]
    /// emits its recurrence report) even with zero inbound traffic. This is
    /// NO LONGER a death timer (BUG-B): an observer that sees no peer
    /// reports lost-connection and keeps observing, it never strands the
    /// run on this window.
    pub fleet_dead_timeout: Duration,
    /// Named-primary silence threshold. The window a NAMED primary may be
    /// silent (no `Primary` keepalive, no `PrimaryChanged` re-point reaching
    /// THIS observer) before the observer REPORTS lost-visibility to the
    /// primary. NOT a strand: the observer keeps observing and retrying;
    /// the run verdict is the primary's, never the observer's view (BUG-B).
    pub peer_timeout: Duration,
    /// Panik trigger paths (sentinel files). Empty disables the file
    /// trigger.
    pub panik_watcher_paths: Vec<std::path::PathBuf>,
    /// Panik poll cadence.
    pub panik_watcher_poll_interval: Duration,
    /// LAST-RESORT fleet-death presumption threshold (see
    /// [`crate::observer::fleet_death`]): how long NOTHING may be
    /// received from ANY member — with the transport showing zero live
    /// legs and ≥3 driven reconnect recovery cycles failed — before the
    /// observer stops spinning on its stale CRDT snapshot, reports
    /// fleet-unreachable-presumed-dead loudly, and exits non-zero.
    /// Deliberately LONG (default
    /// [`Self::DEFAULT_FLEET_DEATH_PRESUMPTION`], 20 minutes — far past
    /// the `peer_timeout`, the keepalive family, and the 5-minute
    /// wake-loss threshold) so the never-fatal report-and-retry
    /// machinery (BUG-B / rc-B) stays the primary behaviour; this is the
    /// bounded terminal behind it, never a replacement for it.
    pub fleet_death_presumption: Duration,
}

impl ObserverConfig {
    /// Default for [`Self::fleet_death_presumption`]: 20 minutes — 4× the
    /// default 300s `peer_timeout` and 4× the 5-minute wake-loss
    /// threshold, i.e. "far past every timeout in the system".
    pub const DEFAULT_FLEET_DEATH_PRESUMPTION: Duration = Duration::from_secs(1200);
}

/// Everything a relocation hands to a freshly-spun observer so it
/// continues seamlessly. The relocation wave (a LATER wave) constructs
/// this; this struct + [`ObserverCoordinator::from_handoff`] only DEFINE
/// the shape. The observer's single-teardown OWNS the two inherited
/// dispatcher handles plus any task it spawns.
pub struct ObserverHandoff<I>
where
    I: Identifier,
{
    /// The mesh send capability the primary held, moved across (egress).
    /// The SAME client + inbox the primary used, so the slot/channel is
    /// stable across the primary→observer retag (H5) — no delivery gap.
    pub client: MeshClient<I>,
    /// The mesh inbound stream the primary held, moved across (ingress).
    pub inbox: RoleInbox<I>,
    /// The replicated ledger, carrying its already-installed
    /// `task_completed_tx` sender (the inherited dispatcher reads its
    /// receiver). Moved across so no live mutation is lost.
    pub cluster_state: ClusterState<I>,
    /// This observer's peer-id.
    pub node_id: String,
    /// The lost-visibility report thresholds + panik inputs.
    pub deadlines: ObserverConfig,
    /// The phases the pre-relocation emitter already announced as started
    /// — the `RunNarrator::with_started_phases` seed so the observer does
    /// not re-announce them but still narrates post-relocation starts.
    pub started_phases: HashSet<dynrunner_core::PhaseId>,
    /// The panik watcher's signal receiver, already taken from the
    /// inherited watcher. `None` when the relocated node ran no watcher.
    pub panik_signal_rx: Option<oneshot::Receiver<PanikSignal>>,
    /// The inherited task-completion dispatcher task handle — torn down by
    /// the observer's single-teardown.
    pub task_completed_dispatcher_handle: JoinHandle<()>,
    /// The inherited peer-lifecycle dispatcher task handle — torn down by
    /// the observer's single-teardown.
    pub lifecycle_dispatcher_handle: JoinHandle<()>,
    /// The observer's static resource-holdings set (default empty).
    pub holdings: HashSet<String>,
    /// The transport-recovery port (BUG-B reconnect). `Some` on the SLURM
    /// reverse-tunnel path (the submitter's `-R` tunnels must be rebuilt
    /// out-of-band when they drop); `None` when the transport heals itself
    /// (e.g. a `--multi-computer local` mpsc mesh, or a non-tunnel backend).
    /// Carried on the handoff so the relocation wave wires whatever the
    /// provider supplied — the observer never names ssh. See
    /// [`crate::observer::reconnect`].
    pub reconnector: ReconnectorHandle,
    /// The upload-action port for setup-task UPLOADS (#336 P1). On the
    /// framework auto-staging path the SUBMITTER is the upload affinity, and
    /// after a bootstrap relocation the submitter runs as THIS standalone
    /// observer (its primary role moved to a compute peer) — so the observer
    /// is exactly where an upload setup task executes. Consulted by the
    /// observer's in-process setup executor when an assigned setup task
    /// carries an [`dynrunner_core::UploadFileRef`]. Carried on the handoff
    /// (the relocating primary hands its `upload_action` over) so the
    /// uploader survives relocation; `None` on a cold-join observer that
    /// never submitted (it holds no source files). A no-ref setup task
    /// no-op-succeeds regardless. See [`crate::upload_action`].
    pub upload_action: crate::upload_action::UploadActionHandle,
    /// The operator's SIGUSR2 graceful-abort trigger the relocating primary
    /// held (armed once at process entry). Carried so the SAME stream drives
    /// the standalone observer's graceful-abort arm — a SIGUSR2 latched
    /// during the primary tenure surfaces on the observer's first poll, and
    /// the relocated observer keeps responding to operator SIGUSR2 instead of
    /// dying on the kernel default. `None` when the relocating primary was
    /// never injected one (`from_handoff` then leaves the observer to its own
    /// cold-join arm). See [`crate::graceful_abort_trigger`].
    pub graceful_abort_trigger: Option<crate::GracefulAbortTrigger>,
    /// The respawn EXECUTION provider (SLURM job-manager bridge /
    /// multi-process child registry) this PROCESS hosts. The provider
    /// belongs to the process, not the role: the relocated submitter
    /// keeps it across its primary→observer demotion and serves the
    /// promoted primary's `RespawnSpawnRequest` / `RespawnRevokeRequest`
    /// frames from it (the primary keeps the DECISION; see
    /// [`crate::primary::respawn::remote`]). `None` when the run
    /// launched with `--respawn-policy=disabled`. Cold-join observers
    /// (late-joiner consoles) host no provider.
    pub respawn_provider: Option<Arc<dyn crate::primary::respawn::SecondarySpawner>>,
    /// The job-ledger consult port (cluster-empty terminal verdict).
    /// `Some` on the relocated submitter→observer path, which physically
    /// hosts the `SlurmJobManager` it submitted the cohort from — so it
    /// can consult squeue for the run's job ids and render a terminal
    /// verdict when the whole cluster has left the queue. `None` for a
    /// cold-join observer (it submitted no jobs and cannot teardown a
    /// cluster it did not launch — current behaviour preserved). See
    /// [`crate::observer::job_ledger`].
    pub job_ledger: JobLedgerProbeHandle,
}

/// Terminal of one observer run. Drives the PyO3 boundary's exit-code
/// mapping (`Done`→0, `Aborted`→1, `Panik`→137) per the spec §8; the
/// strand backstops return `Err` instead, which the boundary raises as a
/// non-zero exit.
#[derive(Debug)]
pub enum ObserverTerminal {
    /// `run_complete()` true (and not aborted) — clean exit 0.
    Done,
    /// `run_aborted()` Some — exit 1 (non-zero).
    Aborted { reason: String },
    /// `run_complete() ∧ graceful_abort_requested()` — the operator's
    /// graceful abort ran its drain protocol to the end. The composed
    /// verdict (two sticky CRDT facts; there is no third terminal
    /// mutation), DISTINCT from `Done` (work was deliberately left
    /// unscheduled) and from `Aborted` (nothing failed — the wind-down was
    /// requested and clean). Exits clean; the narrator's terminal summary
    /// carries the counts.
    GracefulAbort,
    /// Panik signal fired — exit 137.
    Panik { matched_path: std::path::PathBuf },
}

/// The standalone, zero-authority observer.
///
/// The observer reaches the mesh through a [`MeshClient`] (egress) and a
/// [`RoleInbox`] (ingress) — it never names a transport (an observer never
/// dialled a bootstrap primary, so there is no uplink leg and no
/// `bootstrap_primary_id`). The observer holds the replicated
/// [`ClusterState`] DIRECTLY (it carries no pool / scheduler to wrap it
/// in).
pub struct ObserverCoordinator<I>
where
    I: Identifier,
{
    /// The mesh send capability (egress). Locality-oblivious + QUEUED
    /// (drained by the mesh-pump); the observer hands it a role-bearing
    /// [`Destination`] and the frame and never sees the transport.
    client: MeshClient<I>,
    /// The mesh inbound stream (ingress). The mesh-pump demuxes frames
    /// addressed to the observer slot into this inbox; the run loop drains
    /// it.
    inbox: RoleInbox<I>,
    cluster_state: ClusterState<I>,
    config: ObserverConfig,
    /// Seed for the `RunNarrator` — phases already announced upstream.
    started_phases: HashSet<dynrunner_core::PhaseId>,
    /// Inherited dispatcher handles. `None` on the cold-join path (`run`
    /// spawns the observer's own task-completed dispatcher); `Some` on the
    /// relocation path (carried across the handoff). Both — the inherited
    /// AND the one `run` spawns — are torn down once on every exit path.
    inherited_task_completed_dispatcher: Option<JoinHandle<()>>,
    lifecycle_dispatcher_handle: Option<JoinHandle<()>>,
    /// The panik watcher's signal receiver, consumed by the run loop's
    /// panik arm (BUG-4: the arm CONSUMES it — never a registered-but-
    /// never-consumed rx).
    panik_signal_rx: Option<oneshot::Receiver<PanikSignal>>,
    /// The resource-holdings announcer's wiring, attached at construction
    /// (BEFORE the cold-join factory's snapshot restore — see
    /// [`build_cold_join_observer`]) so the restore's role-change hook fires
    /// the initial `AnnounceTrigger` into the registered channel rather than
    /// dropping it. [`Self::run`] takes it to spawn the announcer task.
    announcer_handle: Option<AnnouncerHandle>,
    /// Receiver end of the task-completion dispatcher channel. Captured at
    /// construction (the sender is installed into `cluster_state` at the
    /// SAME point, BEFORE any restore on the cold-join factory path) so any
    /// restore-delivered event sits buffered in this unbounded channel;
    /// [`Self::run`] takes it to spawn the dispatcher AFTER registering the
    /// Policy B/D listeners. `None` only on the relocation path (the
    /// inherited dispatcher already owns the receiver).
    task_completed_rx: Option<mpsc::UnboundedReceiver<TaskCompletedEvent>>,
    /// Receiver end of the #520 per-task narration channel. Captured at
    /// construction (the sender is installed into `cluster_state` at the
    /// SAME point, BEFORE any restore — so the bootstrap baseline's
    /// transitions buffer here as the catch-up batch). [`Self::run`] takes
    /// it, narrates the buffered baseline as ONE summary line, then drains
    /// each subsequent LIVE transition per-event. Set by BOTH constructors
    /// (the observer is the operator narrator on every path); the primary /
    /// secondary install no such sender, so this channel is observer-only.
    task_state_change_rx: Option<mpsc::UnboundedReceiver<TaskStateChangeEvent>>,
    /// AE-3 recovery-cadence state: the LAST-SEEN `(declared-observer bit,
    /// [`StateDigest`])` of each peer that has broadcast one, keyed by
    /// sender-id. The timer-driven recovery arm intersects this with the
    /// live roster (`current_primary` ∪ alive secondaries) and checks
    /// whether the replica is still behind any of them — the C9 quiesce
    /// signal — to decide whether to NOTE the divergence to the disciplined
    /// pull driver (`pull_coordinator`). The declared-observer bit rides
    /// each peer's digest frame and is stored for the roster intersection.
    /// Updated on every inbound `StateDigest` frame.
    peer_digests: std::collections::HashMap<String, (bool, StateDigest)>,
    /// Outbound snapshot-stream driver: serves `RequestSnapshotStream`
    /// pulls one bounded package per run-loop wakeup — see
    /// `crate::snapshot_stream`. The loop's wake arm drains it; the
    /// inbound request arm feeds it.
    snapshot_streams: crate::snapshot_stream::SnapshotStreamResponder,
    /// Settled-CRDT spill driver: sweeps join-fixed-point ledger
    /// entries to the node-local spill file on a cadence — see
    /// `crate::settled_spill`. The run loop owns its one arm; the
    /// observer's mirror holds the same multi-GB terminal ledger every
    /// member does, so it spills like every other role.
    settled_spill: crate::settled_spill::SettledSpillDriver,
    /// Inbound snapshot-stream progress (per responder): lets this
    /// observer's own pulls (bootstrap recovery, reactive digest,
    /// AE-3 recovery cadence) RESUME an interrupted stream instead of
    /// re-pulling from scratch.
    inbound_snapshots: crate::snapshot_stream::InboundSnapshotStreams,
    /// Disciplined anti-entropy PULL driver (the #491 storm-killer): the
    /// single-flight probe→select→pull FSM. BOTH the reactive digest-receive
    /// path AND the AE-3 recovery tick feed it `note_behind` instead of the
    /// eager `reconcile_against_peer` / `plan_recovery_pull` immediate pull;
    /// the run loop's pull arm drives its timers + translates its directives
    /// into `send_to`. See `crate::pull_coordinator`.
    pull_coordinator: crate::pull_coordinator::PullCoordinator,
    /// Rate limit for the digest tick's fault WARNs (empty mesh registry /
    /// failed broadcast). The tick fires every ~20s; during an outage the
    /// SAME fault recurs every tick, so the gate emits at most once per
    /// interval (with a suppressed count) instead of one WARN per tick —
    /// while guaranteeing the fault is never SILENT (the run_20260610
    /// heal-never-engages wedge was undiagnosable because the
    /// empty-registry broadcast "succeeded" and the Err arm logged DEBUG).
    ae_digest_warn: WarnThrottle,
    /// Same gate for the AE-3 recovery tick's fault WARNs.
    ae_recovery_warn: WarnThrottle,
    /// The shared reconnection-note slot (the wake-stream piggyback seam,
    /// see [`WakeNoteSlot`]). ONE slot per observer: the
    /// [`LostVisibilityReporter`] writes into it on a logged-loss regain;
    /// every wake-stream emitter in this process (the narrator's caller,
    /// the periodic reporter, the failure policies, the coordinator's own
    /// important emits) flushes it right after emitting.
    wake_note: WakeNoteSlot,
    /// The transport-recovery port (BUG-B reconnect). `Some` on the
    /// relocated submitter→observer path, whose
    /// [`dynrunner_transport_tunnel::TunneledPeerTransport`] reaches the
    /// compute mesh over per-secondary `-R` reverse tunnels and has no dial
    /// path / no QUIC reconnect ticker — so on lost visibility the observer
    /// triggers this port to rebuild the dropped `-R` (the provider layer
    /// owns the ssh). `None` on a path whose transport heals its own links
    /// (the late-joiner's `PeerNetwork` QUIC reconnect ticker), where there
    /// is nothing for the observer to drive. The observer NEVER owns ssh —
    /// it calls `reconnect(roster)`; see [`crate::observer::reconnect`].
    reconnector: ReconnectorHandle,
    /// The upload-action port for setup-task UPLOADS (#336 P1). The
    /// submitter→observer is the framework auto-staging upload affinity, so
    /// the observer's in-process setup executor consults this when an
    /// assigned setup task carries an [`dynrunner_core::UploadFileRef`].
    /// Carried across relocation on the [`ObserverHandoff`]; `None` on a
    /// cold-join observer (it never submitted, so holds no source files). A
    /// no-ref setup task no-op-succeeds regardless. See
    /// [`crate::upload_action`].
    upload_action: crate::upload_action::UploadActionHandle,
    /// Single-flight latch for the spawned reconnect call. A reconnect
    /// over a roster with dead peers can run for MINUTES (each dead
    /// id's rebuild burns its ssh establishment budget); the ~60s
    /// lost-visibility cadence must not pile a fresh detached
    /// `reconnect(roster)` on top of a still-running one — overlapping
    /// calls raced each other's release+rebind on the SAME tunnel
    /// ports (specimen 1 of run_20260611_200548). `Rc<Cell<_>>`: the
    /// coordinator and its spawned task live on one `LocalSet` thread.
    reconnect_in_flight: std::rc::Rc<std::cell::Cell<bool>>,
    /// The respawn EXECUTION provider this PROCESS hosts (`Some` only on
    /// the relocated-submitter path — the provider rides the
    /// [`ObserverHandoff`] across the demotion; cold-join observers host
    /// none). The run loop's respawn-execution arm drives it on the
    /// promoted primary's `RespawnSpawnRequest` / `RespawnRevokeRequest`
    /// frames and replies with the outcome. ZERO authority is involved:
    /// the observer never decides a respawn — it executes the
    /// primary's already-budgeted decision against the provider only
    /// this process physically holds.
    respawn_provider: Option<Arc<dyn crate::primary::respawn::SecondarySpawner>>,
    /// The job-ledger consult port (`Some` only on the relocated-submitter
    /// path that hosts the `SlurmJobManager`). The lost-visibility
    /// escalation seam consults it once per wake-loss cadence emit; two
    /// consecutive empty-queue results render the cluster-empty terminal
    /// verdict + teardown + non-zero exit. `None` on a cold-join observer
    /// — it cannot teardown a cluster it did not submit, so it keeps the
    /// never-terminal report-and-retry behaviour. See
    /// [`crate::observer::job_ledger`] + [`crate::observer::cluster_gone`].
    job_ledger: JobLedgerProbeHandle,
    /// Idempotency state for the respawn-execution arm, keyed by the
    /// primary-minted replacement id: `InFlight` absorbs re-sent
    /// requests while the provider call runs; `Done` caches the outcome
    /// so a duplicate request (the lost-result replay) re-sends the
    /// SAME result instead of re-submitting. Bounded by the respawn
    /// budget (`max_total` ids per run).
    respawn_exec: std::collections::HashMap<String, RespawnExecState>,
    /// Sender half of the respawn-exec outbox: the detached provider
    /// task posts its outcome here; the run loop's outbox arm (owning
    /// `&mut self`) records it and replies to the primary. Cloned into
    /// each spawned execution task.
    respawn_exec_tx: mpsc::UnboundedSender<RespawnExecOutcome>,
    /// Receiver half, taken by `run` for the loop's outbox arm (the
    /// `task_completed_rx` take-once shape).
    respawn_exec_rx: Option<mpsc::UnboundedReceiver<RespawnExecOutcome>>,
    /// The operator's SIGUSR2 graceful-abort trigger. `Some` when the
    /// entry path pre-armed it (the late-joiner arms BEFORE its bootstrap
    /// rendezvous so a pre-seat signal is latched instead of killing the
    /// process — see [`crate::graceful_abort_trigger`]); `None`
    /// → [`Self::run`] arms at loop start (every other path's behaviour).
    /// Either way the run loop consumes the SAME trigger, so a buffered
    /// pre-seat delivery is serviced exactly like a post-seat one.
    graceful_abort_trigger: Option<GracefulAbortTrigger>,
}

/// Per-replacement-id state of the observer-side respawn execution arm.
/// `InFlight` = the provider call is running (re-sent requests are
/// absorbed; the completion will reply). `Done` = outcome cached for
/// duplicate-request replay.
enum RespawnExecState {
    InFlight,
    Done(Result<(), String>),
}

/// One completed provider call, posted from the detached execution task
/// to the run loop's respawn-exec outbox arm (which owns `&mut self`
/// for the dedup-map update and the reply send) — the same
/// task-posts/loop-sends shape as the announcer outbox.
struct RespawnExecOutcome {
    kind: RespawnExecKind,
    new_secondary_id: String,
    result: Result<(), String>,
}

#[derive(Clone, Copy)]
enum RespawnExecKind {
    Spawn,
    Revoke,
}

/// The observer's exit decision after a cluster-gone job-ledger consult —
/// the disambiguated outcome the run loop acts on.
///
/// The streak double-check (`squeue` empty twice) only establishes the
/// cluster is GONE; the authoritative `sacct` consult then resolves WHY,
/// which is what determines the EXIT CODE (the #532 false-FAIL fix). A
/// cluster that emptied because the run COMPLETED cleanly must exit `Done`
/// (success), not FAILED.
enum ClusterGoneDecision {
    /// The double-check has not yet concluded the cluster is gone (one
    /// empty consult, a `Present`/`ProbeFailed` reset, etc.). Keep
    /// observing — no exit.
    KeepWatching,
    /// The cluster is gone BECAUSE the run completed cleanly (the
    /// authoritative accounting shows every job COMPLETED, exit 0). The
    /// missing `RunComplete` was lost to the dropped observer leg, not
    /// absent because the run failed — the run loop exits
    /// [`ObserverTerminal::Done`] (success). `note` is the operator-facing
    /// line (honest about the observer's stale counts).
    CompletedClean { note: String },
    /// The cluster is gone and the authoritative state is a real failure
    /// (or could not confirm a clean completion) — the run loop exits the
    /// structured non-zero [`RunError::FatalPolicyExit`] with `reason`.
    Failed { reason: String },
}

impl<I> ObserverCoordinator<I>
where
    I: Identifier,
{
    /// Cold-join constructor: build a fresh observer over a mesh client +
    /// inbox and a (possibly already-restored) cluster state.
    ///
    /// Setup ORDERING (load-bearing for Policy B/D — a restore-delivered
    /// event must reach the listeners): the caller installs the
    /// task_completed sender and restores the bootstrap snapshot into
    /// `cluster_state` BEFORE constructing (see [`build_cold_join_observer`]),
    /// so any restore-delivered `TaskCompletedEvent` sits buffered in the
    /// unbounded channel; [`Self::run`] then registers the failure-policy
    /// listeners and spawns the dispatcher, which drains the buffered
    /// events on first poll. No dispatcher is spawned here.
    pub fn new(
        client: MeshClient<I>,
        inbox: RoleInbox<I>,
        cluster_state: ClusterState<I>,
        config: ObserverConfig,
    ) -> Self {
        let node_id = config.node_id.clone();
        Self::with_pieces(
            client,
            inbox,
            cluster_state,
            node_id,
            config,
            HashSet::new(),
            None,
            HashSet::new(),
            // Cold-join observers run over a real `PeerNetwork`, whose QUIC
            // reconnect ticker + dial path re-establish links on their own
            // — there is no out-of-band `-R` for the observer to rebuild.
            None,
        )
    }

    /// Relocation constructor: continue from a handed-off context (the
    /// relocation wave constructs the [`ObserverHandoff`]). The panik
    /// receiver and the narration seed carry across so the observer resumes
    /// without dropping a live event.
    ///
    /// # Dispatcher / listener reconciliation (the inherited-vs-own tension)
    ///
    /// The inherited `task_completed_dispatcher_handle` was spawned by the
    /// PRIMARY with the PRIMARY's listener vector baked in at spawn time, so
    /// the observer's Policy B (invalid_task fatal-exit) and Policy D (error
    /// aggregation) listeners CANNOT be grafted onto it. Resolution: mirror
    /// the cold-join [`Self::new`] ordering — install a FRESH
    /// `task_completed` channel on the moved-in `cluster_state` (which
    /// REPLACES the inherited sender, so subsequent apply-path events route
    /// to the observer's own receiver) and set `task_completed_rx = Some`,
    /// so [`Self::run`] registers the observer's OWN Policy B/D listeners and
    /// spawns the observer's OWN dispatcher. The inherited primary dispatcher
    /// handle is carried in `inherited_task_completed_dispatcher` ONLY so the
    /// observer's single-teardown ABORTS it cleanly (it is otherwise
    /// orphaned — its sender was just replaced). No applies happen during
    /// this cutover window (the observer is not yet in its run loop), so no
    /// event is lost between the sender swap and the dispatcher spawn.
    ///
    /// (Events that were mid-flight inside the primary's dispatcher at
    /// teardown are recovered later by the CRDT track's restore-emit over the
    /// converged state — this is noted, NOT depended on here.)
    pub fn from_handoff(handoff: ObserverHandoff<I>) -> Self {
        let ObserverHandoff {
            client,
            inbox,
            mut cluster_state,
            node_id,
            deadlines,
            started_phases,
            panik_signal_rx,
            task_completed_dispatcher_handle,
            lifecycle_dispatcher_handle,
            holdings,
            reconnector,
            upload_action,
            graceful_abort_trigger,
            respawn_provider,
            job_ledger,
        } = handoff;
        // Respawn-exec outbox (see the field docs): created at
        // construction so the tx lives on `self` for the inbound
        // handlers; `run` takes the rx for its outbox arm.
        let (respawn_exec_tx, respawn_exec_rx) = mpsc::unbounded_channel::<RespawnExecOutcome>();
        // Install a FRESH task-completed channel on the moved-in
        // `cluster_state`, REPLACING the inherited primary sender (see the
        // reconciliation note above). The receiver feeds the observer's own
        // dispatcher, which `run` spawns with the Policy B/D listeners.
        let (task_tx, task_rx) = mpsc::unbounded_channel::<TaskCompletedEvent>();
        cluster_state.install_task_completed_sender(task_tx);
        // Install the #520 per-task narration channel on the moved-in
        // `cluster_state` (the observer is the operator narrator). On the
        // relocation path the moved-in ledger is already converged and no
        // post-construction restore runs, so this channel buffers nothing as
        // baseline — `run`'s drain finds it empty and the baseline summary
        // reflects the converged counts.
        let (state_change_tx, state_change_rx) =
            mpsc::unbounded_channel::<TaskStateChangeEvent>();
        cluster_state.install_task_state_change_sender(state_change_tx);
        // Attach the resource-holdings announcer's role-change hook on the
        // moved-in (already-converged) ledger. The relocation path does no
        // post-attach restore, so no initial trigger is dropped here; the
        // hook still fires on every later `PrimaryChanged`. Attaching at
        // construction (vs. in `run`) keeps the announcer wiring uniform
        // across both constructors — `run` only ever spawns from the stored
        // handle.
        let announcer_handle =
            attach_observer_announcer(&mut cluster_state, holdings, node_id.clone());
        // Recognition→routing publish — THE production wiring for the
        // run_20260612_045106 zombie: the relocated submitter's observer
        // process is exactly where a respawned secondary's
        // Primary-addressed setup frames land, and this attach is what
        // lets the mesh relay them to the promoted primary. The moved-in
        // `cluster_state` is already converged (the relocation applied
        // `PrimaryChanged`), so the attach SEEDS the view immediately.
        crate::process::attach_primary_recognition(&mut cluster_state, client.role_holder_view());
        let snapshot_streams = crate::snapshot_stream::SnapshotStreamResponder::new(&node_id);
        // Settled-CRDT spill: the handoff state may already carry an
        // inherited settled base (the demoted primary's segments); this
        // attaches the observer's OWN writer segment for new settles.
        let settled_spill =
            crate::settled_spill::SettledSpillDriver::start("observer", &mut cluster_state);
        let inbound_snapshots = crate::snapshot_stream::InboundSnapshotStreams::new(&node_id);
        let pull_coordinator = crate::pull_coordinator::PullCoordinator::new(&node_id);
        Self {
            client,
            inbox,
            cluster_state,
            config: ObserverConfig {
                node_id,
                ..deadlines
            },
            started_phases,
            // The inherited primary dispatcher is now orphaned (its sender
            // was replaced above); carry it ONLY so single-teardown aborts
            // it. The observer's own dispatcher is spawned by `run` from
            // `task_completed_rx`.
            inherited_task_completed_dispatcher: Some(task_completed_dispatcher_handle),
            lifecycle_dispatcher_handle: Some(lifecycle_dispatcher_handle),
            panik_signal_rx,
            announcer_handle: Some(announcer_handle),
            task_completed_rx: Some(task_rx),
            task_state_change_rx: Some(state_change_rx),
            peer_digests: std::collections::HashMap::new(),
            snapshot_streams,
            settled_spill,
            inbound_snapshots,
            pull_coordinator,
            ae_digest_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            ae_recovery_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            wake_note: WakeNoteSlot::default(),
            reconnector,
            // The upload-action port the relocating primary held — the
            // submitter→observer is the framework auto-staging upload
            // affinity, so the observer keeps the uploader across demotion
            // and executes upload setup tasks in-process (#336 P1). `None`
            // on a backend with no uploader wired.
            upload_action,
            reconnect_in_flight: std::rc::Rc::new(std::cell::Cell::new(false)),
            // The provider this process kept across its demotion (None
            // when the run's respawn policy is disabled).
            respawn_provider,
            // The job-ledger consult port the relocating primary held —
            // the SAME `SlurmJobManager` this process submitted the cohort
            // from, kept across the demotion. `None` on a backend with no
            // ledger (the cold-join path constructs via `with_pieces`).
            job_ledger,
            respawn_exec: std::collections::HashMap::new(),
            respawn_exec_tx,
            respawn_exec_rx: Some(respawn_exec_rx),
            // Carry the relocating primary's pre-armed SIGUSR2 trigger
            // across, so the observer's run loop consumes the SAME stream
            // (latched pre-relocation deliveries surface on its first poll;
            // the relocated observer keeps responding to operator SIGUSR2).
            // `None` when the primary was never injected one — the observer's
            // own `run`-start arm then takes over (the cold-join behaviour).
            graceful_abort_trigger,
        }
    }

    /// Shared cold-join construction body. No dispatcher is spawned here —
    /// [`Self::run`] is the single wiring point that registers the Policy
    /// B/D listeners and spawns the dispatcher (the install-sender →
    /// restore → register → spawn ordering is realised across the
    /// [`build_cold_join_observer`] factory + `run`).
    #[allow(clippy::too_many_arguments)]
    fn with_pieces(
        client: MeshClient<I>,
        inbox: RoleInbox<I>,
        mut cluster_state: ClusterState<I>,
        node_id: String,
        config: ObserverConfig,
        started_phases: HashSet<dynrunner_core::PhaseId>,
        panik_signal_rx: Option<oneshot::Receiver<PanikSignal>>,
        holdings: HashSet<String>,
        reconnector: ReconnectorHandle,
    ) -> Self {
        // Install the task_completed sender HERE so the dispatcher channel
        // exists before any caller-side apply/restore. Events the apply
        // path emits after this point are buffered (unbounded) until `run`
        // spawns the dispatcher; the cold-join factory restores AFTER this
        // install so restore-delivered events are captured too.
        let (task_tx, task_rx) = mpsc::unbounded_channel::<TaskCompletedEvent>();
        cluster_state.install_task_completed_sender(task_tx);
        // Install the #520 per-task narration channel HERE too — BEFORE the
        // cold-join factory's bootstrap restore, so every baseline
        // transition the restore fires buffers in this unbounded channel as
        // the catch-up batch. `run` drains it into ONE baseline summary line
        // (never narrating the 66k-task bootstrap mirror as 66k changes),
        // then narrates each subsequent LIVE transition per-event.
        let (state_change_tx, state_change_rx) =
            mpsc::unbounded_channel::<TaskStateChangeEvent>();
        cluster_state.install_task_state_change_sender(state_change_tx);
        // Attach the resource-holdings announcer's role-change hook HERE,
        // BEFORE the cold-join factory's snapshot restore runs. The
        // restore's `primary_epoch > local` branch fires
        // `fire_role_change_hooks` from inside `cluster_state.restore`; with
        // the hook registered first, that fire pushes the initial
        // `AnnounceTrigger` into the registered channel (mirrors the
        // attach-then-restore ordering the late-joiner pinned). `run` takes
        // the handle to spawn the announcer task, which drains the queued
        // trigger first. (Attaching here rather than in `run` is the fix for
        // the dropped initial announce: with attach-in-`run`, the factory's
        // restore had already fired the hook before any announcer existed.)
        let announcer_handle =
            attach_observer_announcer(&mut cluster_state, holdings, node_id.clone());
        // Recognition→routing publish (the same attach-at-construction
        // shape as the announcer hook above): the role-change hook
        // publishes `role_table.primary` into the mesh's
        // `RoleHolderView`, so this process's INGRESS relay can forward
        // a directed `Primary` frame toward the recognized holder. The
        // cold-join factory's restore fires the hook post-attach,
        // seeding the view from the snapshot's primary fact.
        crate::process::attach_primary_recognition(&mut cluster_state, client.role_holder_view());
        // Respawn-exec outbox — created unconditionally (the arm is
        // inert without a provider; requests then get an error reply).
        let (respawn_exec_tx, respawn_exec_rx) = mpsc::unbounded_channel::<RespawnExecOutcome>();
        let snapshot_streams = crate::snapshot_stream::SnapshotStreamResponder::new(&node_id);
        // Settled-CRDT spill: attach this observer's writer segment.
        let settled_spill =
            crate::settled_spill::SettledSpillDriver::start("observer", &mut cluster_state);
        let inbound_snapshots = crate::snapshot_stream::InboundSnapshotStreams::new(&node_id);
        let pull_coordinator = crate::pull_coordinator::PullCoordinator::new(&node_id);
        Self {
            client,
            inbox,
            cluster_state,
            config: ObserverConfig { node_id, ..config },
            started_phases,
            inherited_task_completed_dispatcher: None,
            lifecycle_dispatcher_handle: None,
            panik_signal_rx,
            announcer_handle: Some(announcer_handle),
            task_completed_rx: Some(task_rx),
            task_state_change_rx: Some(state_change_rx),
            peer_digests: std::collections::HashMap::new(),
            snapshot_streams,
            settled_spill,
            inbound_snapshots,
            pull_coordinator,
            ae_digest_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            ae_recovery_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            wake_note: WakeNoteSlot::default(),
            reconnector,
            // Cold-join observers (late-joiner consoles) never submitted, so
            // they hold no source files and host no upload action (#336 P1).
            // The submitter→observer arrives via `from_handoff`, which carries
            // the relocating primary's `upload_action` across.
            upload_action: None,
            reconnect_in_flight: std::rc::Rc::new(std::cell::Cell::new(false)),
            // Cold-join observers (late-joiner consoles) host no respawn
            // provider; the submitter/observer process is the only
            // provider host and always arrives via `from_handoff`.
            respawn_provider: None,
            // Cold-join observers host no job ledger either — they
            // submitted no jobs and cannot teardown a cluster they did not
            // launch, so they keep the never-terminal report-and-retry
            // behaviour. Only the relocated submitter (via `from_handoff`)
            // carries a ledger.
            job_ledger: None,
            respawn_exec: std::collections::HashMap::new(),
            respawn_exec_tx,
            respawn_exec_rx: Some(respawn_exec_rx),
            graceful_abort_trigger: None,
        }
    }

    /// Wire (or replace) the transport-recovery port AFTER construction —
    /// the observer-side mirror of
    /// `PrimaryCoordinator::set_tunnel_reconnector`. Used by the
    /// gateway-mode late-joiner, whose `ssh -L` local-forward registry
    /// only exists once its tunnels are established (after the factory's
    /// snapshot restore but before `run`): the transport's own QUIC/WSS
    /// reconnect ticker redials `127.0.0.1:<local_port>`, which heals
    /// ONLY if the underlying `ssh -L` child is rebuilt out-of-band —
    /// exactly what the lost-visibility trigger drives through this port.
    pub fn set_tunnel_reconnector(
        &mut self,
        reconnector: Arc<dyn crate::observer::TunnelReconnector>,
    ) {
        self.reconnector = Some(reconnector);
    }

    /// Wire (or replace) the upload-action port AFTER construction (#336 P1)
    /// — the upload-executor sibling of [`Self::set_tunnel_reconnector`].
    /// The submitter→observer is the framework auto-staging upload affinity;
    /// this is the port its in-process setup executor uses to perform an
    /// assigned upload setup task's file upload. Normally carried across the
    /// relocation handoff; this setter exists for the wiring paths that
    /// inject it post-construction (mirroring the reconnector). Absence
    /// leaves the executor with no uploader — an upload-ref setup task then
    /// fails as a wiring error (a no-ref task no-op-succeeds). See
    /// [`crate::upload_action`].
    pub fn set_upload_action(&mut self, action: Arc<dyn crate::upload_action::UploadAction>) {
        self.upload_action = Some(action);
    }

    /// Wire (or replace) the job-ledger consult port AFTER construction —
    /// the cluster-empty-verdict sibling of [`Self::set_tunnel_reconnector`].
    /// Only the process that hosts the job ledger (the relocated submitter)
    /// has one; absence keeps the never-terminal report-and-retry behaviour
    /// (the cold-join observer cannot teardown a cluster it did not submit).
    /// See [`crate::observer::job_ledger`].
    pub fn set_job_ledger_probe(
        &mut self,
        probe: Arc<dyn crate::observer::job_ledger::JobLedgerProbe>,
    ) {
        self.job_ledger = Some(probe);
    }

    /// Inject a pre-armed operator graceful-abort trigger AFTER
    /// construction — the SIGUSR2 sibling of
    /// [`Self::set_tunnel_reconnector`]. Used by the late-joiner entry
    /// path, which arms the trigger at process entry (BEFORE its
    /// bootstrap rendezvous, so a signal received pre-seat is latched
    /// rather than killing the process via the kernel's default
    /// disposition) and hands the SAME trigger here for the run loop to
    /// consume. Without an injection, [`Self::run`] arms at loop start.
    pub fn set_graceful_abort_trigger(&mut self, trigger: GracefulAbortTrigger) {
        self.graceful_abort_trigger = Some(trigger);
    }

    /// Read-only access to the replicated ledger (tests / result getters).
    pub fn cluster_state(&self) -> &ClusterState<I> {
        &self.cluster_state
    }

    /// The registered upload-action port (#336 P1). Read by the setup-task
    /// executor twin (`observer::setup_exec`) to perform an assigned upload
    /// setup task's file upload. `None` when no uploader was wired (a
    /// cold-join observer / a backend with no upload action). See
    /// [`crate::upload_action`].
    pub(crate) fn upload_action(&self) -> &crate::upload_action::UploadActionHandle {
        &self.upload_action
    }

    /// This observer's own peer id. Read by the setup-task executor twin
    /// (`observer::setup_exec`) to stamp its `SetupTerminal` report; a thin
    /// accessor so the sibling module need not reach into the private
    /// `config` field.
    pub(crate) fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Tasks the cluster recorded as successfully completed, read off the
    /// observer's moved-in (converged) `cluster_state`. Same CRDT reader
    /// (`outcome_counts().succeeded`) the primary's `completed_count` routes
    /// through — so the post-run accounting the PyO3 boundary reads is
    /// identical in shape across a relocation. (The relocation wave's
    /// [`crate::primary::RunOutcome`] re-sources the counts through this
    /// surface after the submitter binding is consumed.)
    pub fn completed_count(&self) -> usize {
        self.cluster_state.outcome_counts().succeeded
    }

    /// Tasks the cluster recorded as terminally failed (any failure class).
    /// Sums the three failure buckets off `cluster_state.outcome_counts()`,
    /// matching the primary's `failed_count` semantics.
    pub fn failed_count(&self) -> usize {
        let o = self.cluster_state.outcome_counts();
        o.fail_retry + o.fail_oom + o.fail_final
    }

    /// Tasks left without a recorded outcome. ALWAYS 0 for an observer: it
    /// relinquished authority and never dispatched, so there is nothing it
    /// could strand. Present so the post-run accounting surface mirrors the
    /// primary's (`completed`/`failed`/`stranded`) across a relocation.
    pub fn stranded_count(&self) -> usize {
        0
    }

    /// Emit a run-terminal reason on the [`IMPORTANT_TARGET`] channel.
    ///
    /// The observer's own LOCAL exit arms (fatal-policy, panik) reach this
    /// when the run ends WITHOUT a CRDT terminal the narrator could project
    /// — the connected case is already covered (the primary broadcasts
    /// `RunComplete` / `RunAborted` and the narrator emits the summary on
    /// `IMPORTANT_TARGET`). Visibility loss does NOT reach this (it is not a
    /// terminal — see [`crate::observer::lost_visibility`]). One emit site,
    /// one line per local terminal arm, so "every run-terminal reason
    /// reaches the important channel" holds on both the connected and the
    /// partitioned path.
    fn emit_terminal_reason_important(&self, reason: &str) {
        tracing::error!(
            target: IMPORTANT_TARGET,
            "run terminated — {reason}",
        );
        // A wake-stream host: a parked reconnection note rides it.
        self.wake_note.flush_after_host();
    }

    /// The observer's OWN egress edge: resolve `Destination::Primary`
    /// against `current_primary()` (no bootstrap-primary fallback — an
    /// observer never dialled one) and QUEUE the frame onto the mesh client.
    /// NO loopback delivery: the observer is never the primary, so a
    /// resolved self-id is a no-op drop (it would only happen if the ledger
    /// named the observer primary, which never occurs).
    ///
    /// The resolve HEAD stays AT the coordinator (H1): an observer has NO
    /// bootstrap-primary fallback (it never dialled one), so `Primary`
    /// resolves against `current_primary()` alone. `None` ⇒ no route to the
    /// primary, surfaced as `Err`. The TAIL collapses onto a single queued
    /// [`MeshClient::send`]: we map the resolved [`SendTarget`] back to the
    /// role-bearing [`Destination`] (NEVER the role-erased `SendTarget` on
    /// the wire — dirty-D2), STAMP it on the frame (the C3 routing target
    /// the receiver's mesh-pump demuxes against its slots), and queue it.
    /// The mesh-pump (not this coordinator) does loopback-vs-remote; an
    /// observer never loops back, so the resolved self case is the same
    /// best-effort drop the original held.
    pub(crate) async fn send_to(
        &mut self,
        dst: Destination,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        let target = resolve_destination(
            dst.clone(),
            self.cluster_state.current_primary(),
            None,
            &self.config.node_id,
        )
        .ok_or_else(|| {
            "Destination::Primary unresolvable: no current primary in the role table \
             (an observer has no bootstrap primary link) — no route to the primary"
                .to_string()
        })?;
        // Two `Destination`s: the routing send-target the mesh-pump dispatches
        // by, and the C3 stamp the RECEIVER's pump demuxes against its slots.
        // They differ ONLY for a remote `Destination::Primary` (the C2
        // egress collapse). See `routing_target_for` for the single owner
        // of the routing-target-vs-stamp dual semantic — the same helper
        // that the secondary's edge calls.
        //
        // SELF-RESOLVED PRIMARY (observer-specific): an observer is never
        // the primary, so a `SendTarget::Loopback` for `Destination::Primary`
        // is a logic-impossible case here; drop it best-effort rather than
        // self-addressing the observer's own slot via the helper's
        // Loopback-stays-bare-Primary branch. This is a coordinator-local
        // policy on top of the shared routing-target collapse; it does NOT
        // belong inside `routing_target_for` (the secondary genuinely
        // loopbacks Primary on a promoted-self send and must NOT be
        // dropped).
        if matches!(&dst, Destination::Primary) && matches!(&target, SendTarget::Loopback) {
            tracing::debug!(
                "observer send resolved to self (impossible for a zero-authority \
                 observer); dropping"
            );
            return Ok(());
        }
        let send_target: Destination = routing_target_for(&dst, &target);
        // The C3 stamp is ALWAYS the role-bearing intent `dst` — what the
        // receiver demuxes to a slot. Only the routing send-target carries the
        // resolved host (for a remote, id-less `Destination::Primary`).
        self.client.send(send_target, msg.with_target(dst))
    }

    /// Send the GRACEFUL-ABORT command to the primary — the ONE
    /// management command a zero-authority observer may send. Builds the
    /// typed [`DistributedMessage::GracefulAbortRequest`] and routes it
    /// through the observer's own `Destination::Primary` egress edge; the
    /// primary originates the replicated `GracefulAbortRequested` sticky
    /// latch on receipt (idempotent — re-sends NoOp at the apply).
    ///
    /// Best-effort by design: a send failure (no current primary known /
    /// mesh-pump gone) is WARNed and the operator simply re-triggers once
    /// visibility returns — the request carries no state of its own, the
    /// LATCH (primary-originated, CRDT-replicated) is the durable fact.
    ///
    /// Triggered by the operator channel (`SIGUSR2` to the observer
    /// process — see the run loop's graceful-abort arm); `pub` so an
    /// embedding driver/test can invoke it directly.
    pub async fn request_graceful_abort(&mut self) {
        tracing::warn!(
            target: IMPORTANT_TARGET,
            "graceful abort triggered on the observer; sending \
             GracefulAbortRequest to the primary"
        );
        // A wake-stream host: a parked reconnection note rides it.
        self.wake_note.flush_after_host();
        let msg = DistributedMessage::GracefulAbortRequest {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
        };
        if let Err(e) = self.send_to(Destination::Primary, msg).await {
            tracing::warn!(
                target: IMPORTANT_TARGET,
                error = %e,
                "graceful-abort request could not be sent (no route to the \
                 primary); re-trigger once visibility returns"
            );
        }
    }

    /// Drive the observer until it OBSERVES a run terminal (the primary's
    /// RunComplete/RunAborted, or its own local panik). THIN DRIVER: a
    /// `select!` loop whose arms delegate to named per-concern methods + the
    /// inline reporter / failure-policy / panik arms. Returns the observed
    /// run terminal; a local policy abort (the invalid-task monitor)
    /// surfaces as `Err`. A loss of the observer's own visibility is NEVER a
    /// terminal — it is reported + retried (BUG-B).
    pub async fn run(&mut self) -> Result<ObserverTerminal, RunError>
    where
        Self: 'static,
    {
        // Role-tag the whole observer run future so every event this task
        // emits is attributed to the observer role and routed to the
        // per-role full log (`observer.log`). A relocated submitter steps
        // down into this standalone observer on the SAME host as the
        // compute peer it handed the primary role to; this span keeps the
        // relocated submitter's events in `observer.log`, distinct from the
        // promoted peer's `primary.log` and any host `secondary.log`. See
        // `dynrunner_core::role_span`.
        let span = tracing::info_span!(
            dynrunner_core::OBSERVER_ROLE_SPAN,
            kind = "observer",
            node = %self.config.node_id
        );
        async move { self.run_inner().await }.instrument(span).await
    }

    /// Original `run` body, factored out so the public `run` wrapper can
    /// role-tag the whole future (see [`Self::run`]). Behaviour-identical
    /// to the pre-span body; the wrapper adds only the role span.
    async fn run_inner(&mut self) -> Result<ObserverTerminal, RunError>
    where
        Self: 'static,
    {
        // ── Pre-loop wiring (single concern per block) ──

        // Policy B: invalid_task fatal-exit monitor. The signal rides a
        // dedicated unbounded mpsc consumed by the run loop's fatal-exit
        // arm; the policy never calls `std::process::exit`.
        let (fatal_exit_tx, mut fatal_exit_rx) = mpsc::unbounded_channel::<String>();
        let (invalid_task_listener, invalid_task_driver) = windowed_failure_collector(
            InvalidTaskMonitorPolicy::new(fatal_exit_tx).with_wake_note(self.wake_note.clone()),
        );
        // Policy D: rolling error aggregation (importance-channel emit,
        // never exits).
        let (aggregation_listener, aggregation_driver) = windowed_failure_collector(
            ErrorAggregationPolicy::new().with_wake_note(self.wake_note.clone()),
        );

        // Spawn the task-completed dispatcher with the Policy B/D listeners
        // AFTER building them — the load-bearing ordering (install sender →
        // restore → register → spawn): the sender was installed at
        // construction (so restore-delivered events are buffered), and the
        // listeners are registered into the dispatcher's vector here, before
        // it first polls. BOTH constructors install a fresh sender + set
        // `task_completed_rx = Some`: cold-join via `with_pieces`, relocation
        // via `from_handoff` (which REPLACES the inherited primary sender and
        // carries the orphaned primary dispatcher handle only for teardown).
        // So the observer always spawns its OWN dispatcher with the Policy
        // B/D listeners here.
        // Respawn-exec outbox receiver (the take-once shape of
        // `task_completed_rx`): drained by the loop's outbox arm below.
        let mut respawn_exec_rx = self
            .respawn_exec_rx
            .take()
            .expect("respawn_exec_rx is set by both constructors and taken once by run");
        let dispatcher_task = self.task_completed_rx.take().map(|task_rx| {
            let listeners: Vec<Box<dyn TaskCompletedListener>> =
                vec![invalid_task_listener, aggregation_listener];
            tokio::task::spawn_local(run_task_completed_dispatcher(task_rx, listeners))
        });
        // #520 per-task narration channel receiver (the same take-once
        // shape): drained by the loop's narration arm below. The buffered
        // baseline (bootstrap restore's transitions) is drained ONCE at loop
        // entry into the one-line summary before the live arm engages.
        let mut task_state_change_rx = self
            .task_state_change_rx
            .take()
            .expect("task_state_change_rx is set by both constructors and taken once by run");

        // Drive each policy's window timer.
        let (invalid_cancel_tx, invalid_cancel_rx) = oneshot::channel::<()>();
        let invalid_driver_task =
            tokio::task::spawn_local(run_collector(invalid_task_driver, async move {
                let _ = invalid_cancel_rx.await;
            }));
        let (aggregation_cancel_tx, aggregation_cancel_rx) = oneshot::channel::<()>();
        let aggregation_driver_task =
            tokio::task::spawn_local(run_collector(aggregation_driver, async move {
                let _ = aggregation_cancel_rx.await;
            }));

        // Observer announcer. The role-change hook was attached at
        // CONSTRUCTION (before the cold-join factory's snapshot restore), so
        // the restore's role-change fire already pushed the initial
        // `AnnounceTrigger` into the handle's channel — the task drains it on
        // first poll. Here we only build the production cross-task sender and
        // spawn the task from the stored handle. The hook also fires on every
        // later `PrimaryChanged`.
        let announcer_handle = self
            .announcer_handle
            .take()
            .expect("announcer_handle is set by both constructors");
        let (announcer_outbox_tx, mut announcer_outbox_rx) =
            mpsc::channel::<AnnouncerOutboxItem<I>>(8);
        let announcer_sender =
            PeerMeshAnnouncerSender::new(self.config.node_id.clone(), announcer_outbox_tx);
        let announcer_task = tokio::task::spawn_local(run_observer_announcer(
            announcer_handle.rx,
            announcer_handle.holdings,
            announcer_handle.peer_id,
            announcer_sender,
            announcer_handle.primary_epoch_mirror,
        ));

        // Periodic-stats reporter (B, 10-min) + idle detector (C, 1-min).
        // The reporter task owns its own cadences with Skip + immediate-
        // tick-consume (see `run_reporter`); the loop publishes a fresh
        // CRDT projection each iteration into the shared cell.
        let snapshot_source =
            SharedSnapshotSource::new(StatsSnapshot::from_cluster_state(&self.cluster_state));
        let snapshot_publisher = snapshot_source.clone();
        let (reporter_cancel_tx, reporter_cancel_rx) = oneshot::channel::<()>();
        // The wake-stream outage seam: the run loop forwards the loss
        // policy's EndedOutage signal (a LOGGED outage regained
        // visibility) to the reporter task, which owns the late-run /
        // skip-one grid bookkeeping; the shared note slot lets the
        // reporter's emissions host the parked reconnection note.
        let (outage_tx, outage_rx) = mpsc::unbounded_channel::<EndedOutage>();
        let reporter_task = tokio::task::spawn_local(run_reporter(
            snapshot_source,
            TokioClock,
            outage_rx,
            self.wake_note.clone(),
            async move {
                let _ = reporter_cancel_rx.await;
            },
        ));

        // Bootstrap recovery REQUEST half (§6): fire one snapshot-stream
        // request to `Destination::Primary` at entry, gated on a known
        // primary, best-effort. The REPLY half (the packages) is folded
        // into the loop's recv arm. The stream is tracked per the
        // primary's id so an interrupted bootstrap stream RESUMES from
        // its cursor on the recovery cadence.
        if let Some(primary_id) = self.cluster_state.current_primary().map(str::to_owned) {
            let (stream_id, resume_after) = self.inbound_snapshots.request_params(&primary_id);
            let req = DistributedMessage::RequestSnapshotStream {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                stream_id,
                resume_after,
                // Bootstrap recovery pulls the FULL ledger (empty =
                // all-ranges, the P0 full stream): an observer entering the
                // loop has no range digest of its own to compute a delta. P1
                // narrows STEADY-STATE anti-entropy pulls, not bootstrap.
                task_ranges: Vec::new(),
                is_observer: true,
                can_be_primary: false,
            };
            if let Err(e) = self.send_to(Destination::Primary, req).await {
                tracing::warn!(
                    error = %e,
                    "observer bootstrap snapshot request failed; relying on \
                     anti-entropy / late snapshot heal + reconnect"
                );
            }
        }

        // Capture the loop result BEFORE the single-teardown so an
        // `Err`/`?`-propagated early return still routes through cleanup
        // (cp the late-joiner inner-async-block idiom — NO
        // cleanup-before-each-return).
        let loop_result: Result<ObserverTerminal, RunError> = async {
            // ── Loop-local state ──
            let mut narrator =
                RunNarrator::with_started_phases(std::mem::take(&mut self.started_phases));
            // #520 per-task narrator + the arm-after-catch-up bootstrap
            // guard (owner directive). The channel buffered every transition
            // the bootstrap restore fired BEFORE this loop; drain that batch
            // NON-blockingly here and narrate it as ONE baseline summary line
            // (the converged mirror's per-state partition), NEVER as N
            // per-task changes. From the next line on, the select! arm
            // narrates each LIVE transition individually. A `Disconnected`
            // here is impossible — `self.cluster_state` still holds the
            // sender — so `try_recv` only ever returns `Empty` at the batch
            // end.
            let mut task_narrator = ObserverTaskNarrator::default();
            let mut baseline_transitions = 0usize;
            while task_state_change_rx.try_recv().is_ok() {
                baseline_transitions += 1;
            }
            task_narrator.narrate_baseline(baseline_transitions, self.cluster_state.counts());
            // The report-lost-and-keep-observing state machine (BUG-B): the
            // observer's loss of its OWN transport view is reported + retried,
            // NEVER a run verdict — see [`crate::observer::lost_visibility`].
            // It shares the wake-note slot so a logged-loss regain parks the
            // reconnection note every wake emitter can host.
            let mut visibility_reporter = LostVisibilityReporter::new(self.wake_note.clone());
            let mut primary_last_seen = Instant::now();
            let mut transport_closed = false;
            // LAST-RESORT fleet-death presumption (see `fleet_death.rs`):
            // tracks the one bounded terminal the never-fatal
            // report-and-retry machinery above deliberately never renders.
            // Fed at top-of-loop from the observer's OWN present-tense
            // evidence — its transport leg view + the last-received-
            // ANYTHING clock below — never from the (possibly stale) CRDT
            // snapshot.
            let mut fleet_death =
                FleetDeathDetector::new(self.config.fleet_death_presumption);
            // Cluster-empty terminal verdict (see `cluster_gone.rs`): when
            // this process hosts the job ledger (the relocated-submitter
            // path), a long lost-visibility episode triggers a squeue
            // consult for the run's job ids; two consecutive empty results
            // PROVE the cluster is gone (a ground truth the fleet-death
            // PRESUMPTION need not be used for). Driven once per wake-loss
            // cadence emit — keyed on `last_consulted_wake_emit` advancing
            // — so the double-check is one wake-loss interval apart and no
            // new timer is introduced. Inert (never consulted) when the
            // observer hosts no ledger.
            let mut cluster_gone = ClusterGoneDetector::new();
            // The wake-loss emit instant the job ledger was last consulted
            // for. The consult fires when the reporter's wake-emit instant
            // ADVANCES past this (a fresh 5-min-threshold / 10-min-recurrence
            // emit), reusing the existing wake-loss cadence.
            let mut last_consulted_wake_emit: Option<tokio::time::Instant> = None;
            // When the observer last received ANYTHING from ANY member
            // (every inbound frame, regardless of type/sender). Seeded at
            // loop entry so the presumption clock starts from "now", not
            // from process epoch. A `tokio::time::Instant` (not the file's
            // std `Instant`) so paused-clock tests drive the presumption
            // window in virtual time, matching the detector's clock.
            let mut last_inbound_at = tokio::time::Instant::now();

            // Visibility re-check poll tick: the same cadence as before, but
            // it is NO LONGER a death timer — it only re-drives the loop so
            // the top-of-loop visibility check re-evaluates (and the
            // lost-visibility recurrence report fires) even when no inbound
            // frame arrives. The immediate tick (fires at t=0) is consumed so
            // the cadence starts one full interval out; the tick carries no
            // work.
            let mut visibility_recheck_tick = tokio::time::interval(self.config.fleet_dead_timeout);
            visibility_recheck_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = visibility_recheck_tick.tick().await;

            // Anti-entropy tick (item 3): per-node-jittered cadence; Skip +
            // immediate-tick-consume so a converged mesh's digest traffic
            // starts one period out.
            let mut ae_tick =
                tokio::time::interval(anti_entropy::tick_period(&self.config.node_id));
            ae_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = ae_tick.tick().await;

            // AE-3 snapshot-recovery tick (D-C / C9): an INDEPENDENT timer,
            // distinct from the digest broadcast above, on the SAME proven
            // per-node-jittered period (bounded by `peer_timeout` so the
            // recovery probe re-pulls at least as often as the named-primary
            // silence-report threshold). Each tick
            // re-pulls a snapshot from a rotating known peer iff still behind
            // its last-seen digest — this is what re-converges a WARN-dropped
            // steady-state decode. Skip + immediate-tick-consume mirrors the
            // digest cadence so the first recovery probe is one period out.
            let recovery_period =
                anti_entropy::tick_period(&self.config.node_id).min(self.config.peer_timeout);
            let mut recovery_tick = tokio::time::interval(recovery_period);
            recovery_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = recovery_tick.tick().await;

            // Panik receiver (BUG-4): consumed by a live arm. `None` →
            // a never-firing arm.
            let mut panik_rx = self.panik_signal_rx.take();

            // Operator graceful-abort channel: SIGUSR2 to the observer
            // process (the cleanest existing operator seam — the sibling of
            // the panik watcher's SIGTERM arm; both ride tokio's unix
            // signal registry). Each delivery sends one
            // `GracefulAbortRequest` to the primary; re-sending the signal
            // re-sends the request (idempotent at the primary's latch), so
            // a request lost to a failover window is operator-recoverable.
            // The trigger module owns the SIGUSR2 stream: an entry path
            // that pre-armed it (the late-joiner, whose bootstrap window
            // must survive the signal) injected it via
            // `set_graceful_abort_trigger` and its latched pre-seat
            // delivery fires this arm's first poll; otherwise arm NOW (the
            // pre-injection behaviour of every other path). Registration
            // failure degrades to a parked arm inside the trigger — the
            // embedding driver can still call `request_graceful_abort`
            // directly.
            let mut graceful_abort_signal = self
                .graceful_abort_trigger
                .take()
                .unwrap_or_else(GracefulAbortTrigger::arm);

            loop {
                // 1. Narrate (item 9/14): emit pending phase / summary
                //    BEFORE the terminal early-returns so the completing
                //    iteration emits the summary first. A narrated
                //    iteration is a wake-stream HOST: the pending
                //    reconnection note (if any) rides it.
                if narrator.observe(&self.cluster_state) {
                    self.wake_note.flush_after_host();
                }
                // Keep the reporter's cell fresh with the live projection.
                snapshot_publisher.publish(StatsSnapshot::from_cluster_state(&self.cluster_state));

                // 2. Observed-terminal block (top-of-loop). The observer
                //    terminates ONLY on an OBSERVED run-terminal (the
                //    primary's RunComplete/RunAborted, or its own panik); a
                //    loss of the observer's OWN transport visibility is fed to
                //    the lost-visibility reporter (report-and-retry, NEVER an
                //    exit). Ordering is load-bearing — see the module header.
                if let Some(terminal) = self.evaluate_exit(transport_closed) {
                    return Ok(terminal);
                }
                // Feed the reporter the current visibility; on a due
                // reconnect attempt (the first lost loop, then once per
                // ~60s recurrence) actively trigger a `-R` tunnel rebuild
                // for the roster the observer expects to reach. The reporter
                // owns the cadence; the coordinator owns the action (the
                // reconnect port). Visibility flips back to Visible once the
                // SECONDARY's bootstrap-redial supervisor re-dials through
                // the rebuilt tunnel and re-folds the wire (an owned
                // mechanism in transport-quic, not an assumed side effect).
                let outcome = visibility_reporter.observe(
                    &self.current_visibility(primary_last_seen),
                    tokio::time::Instant::now(),
                );
                if outcome.directive == RetryDirective::ReconnectDue {
                    self.trigger_reconnect();
                    // One recovery cycle driven — the fleet-death attempt
                    // floor counts these (recovery must have been TRIED,
                    // and failed, before the presumption may trip).
                    fleet_death.note_reconnect_attempt();
                }
                // A LOGGED outage just regained visibility: forward the
                // ended-outage signal to the periodic reporter, which owns
                // the late-run decision (did a grid occurrence elapse while
                // down?) + the skip-one bookkeeping. Best-effort: a closed
                // channel means the reporter task is already torn down.
                if let Some(ended) = outcome.ended_logged_outage {
                    let _ = outage_tx.send(ended);
                }
                // LAST-RESORT fleet-death presumption (after the
                // report-and-retry machinery above has had its turn —
                // ordering is the rc-B contract). The inputs are the
                // observer's OWN present-tense evidence: zero live
                // transport legs + the last-received-anything clock —
                // NEVER the CRDT snapshot, whose membership ledger freezes
                // alive-looking when a fleet dies without originating
                // `PeerRemoved` (the asm-dataset LMU "still shows 10 live
                // members … running autonomously" forever-spin). On the
                // verdict: one loud wake-stream terminal reason (the same
                // single emit site every local terminal uses) + a
                // structured non-zero exit (`FatalPolicyExit` — the
                // documented home of deliberate policy aborts, which the
                // PyO3 boundary RAISES).
                if let FleetDeathVerdict::PresumedDead { reason } = fleet_death.observe(
                    self.client.peer_count() == 0,
                    last_inbound_at,
                    tokio::time::Instant::now(),
                ) {
                    let err = RunError::FatalPolicyExit { reason };
                    self.emit_terminal_reason_important(&err.to_string());
                    return Err(err);
                }

                // Cluster-empty terminal verdict (the relocated-submitter
                // ground truth). Consult the hosted job ledger ONCE per
                // wake-loss cadence emit — keyed on the reporter's wake-emit
                // instant advancing past the last consulted one, so it
                // reuses the existing 5-min-threshold / 10-min-recurrence
                // cadence (no new timer) and the two-consecutive-empty
                // double-check is one wake-loss interval apart. Conditional
                // on hosting a ledger (`Some`): a cold-join observer keeps
                // the never-terminal report-and-retry behaviour.
                //
                // Once the double-check concludes the cluster is GONE, the
                // consult disambiguates WHY via SLURM accounting (the #532
                // false-FAIL fix): a clean COMPLETED framework shutdown — the
                // run reached its terminal but the `RunComplete` verdict was
                // lost to the dropped observer leg — exits `Done` (success),
                // NEVER FAILED; a real crash/scancel/OOM (or an unreadable
                // authoritative state) keeps the structured non-zero
                // `FatalPolicyExit`. Both route the single wake-stream
                // terminal-reason emit + the single-teardown below, exactly
                // like the fleet-death exit above.
                let wake_emit = visibility_reporter.wake_emit_instant();
                if wake_emit.is_some() && wake_emit != last_consulted_wake_emit {
                    last_consulted_wake_emit = wake_emit;
                    match self.consult_cluster_gone(&mut cluster_gone).await {
                        Some(ClusterGoneDecision::CompletedClean { note }) => {
                            // The cluster is gone BECAUSE the run completed
                            // cleanly — report success, not a false FAILED.
                            self.emit_terminal_reason_important(&note);
                            return Ok(ObserverTerminal::Done);
                        }
                        Some(ClusterGoneDecision::Failed { reason }) => {
                            let err = RunError::FatalPolicyExit { reason };
                            self.emit_terminal_reason_important(&err.to_string());
                            return Err(err);
                        }
                        // The double-check has not (yet) proven the cluster
                        // gone, or this observer hosts no ledger — keep
                        // observing.
                        Some(ClusterGoneDecision::KeepWatching) | None => {}
                    }
                }

                // 3. Await events.
                tokio::select! {
                    // Inbound mesh frame — the mesh-pump has already demuxed
                    // it to the observer's slot, so this inbox carries only
                    // frames addressed to the observer.
                    maybe = self.inbox.recv() => {
                        match maybe {
                            Some(msg) => {
                                // ANY inbound frame is fleet life — feed the
                                // last-received-anything clock the
                                // fleet-death presumption derives from.
                                last_inbound_at = tokio::time::Instant::now();
                                self.on_inbound(msg, &mut primary_last_seen).await;
                            }
                            None => {
                                // Latch + disable this arm; the
                                // closed-transport-with-peers clean-exit
                                // decision is made at top-of-loop. A `None` is
                                // the role's teardown signal (every write end
                                // of the slot's inbound dropped).
                                transport_closed = true;
                            }
                        }
                    }
                    // Visibility re-check poll tick: no work — it only
                    // re-drives the loop so the top-of-loop visibility check
                    // re-evaluates + the lost-visibility recurrence report
                    // fires even with zero inbound traffic.
                    _ = visibility_recheck_tick.tick() => {}
                    // Wake-stream loss cadence (the 5-minute mark, then one
                    // repeat per 10 minutes while still down). A PERSISTENT
                    // deadline: `wake_deadline()` derives from STORED
                    // instants (the loss instant, then the last wake emit),
                    // so this `sleep_until` — though rebuilt every iteration
                    // — always targets the same absolute instant and fires
                    // under constant sibling activity (the watchdog law; a
                    // relative `sleep` here would be reset by every other
                    // arm and never fire). `None` (visible) parks the arm.
                    // Cancel-safe: `sleep_until` holds no state beyond its
                    // target instant.
                    _ = async {
                        match visibility_reporter.wake_deadline() {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    } => {
                        // Re-check the LIVE visibility before logging: the
                        // transport may have healed between the last observe
                        // and this deadline — then the episode was a blip
                        // SHORTER than the threshold and must produce
                        // nothing (the next top-of-loop observe clears the
                        // loss state). Only a STILL-down connection at the
                        // mark is "continuously down for 5 minutes"; a
                        // still-DEGRADED state at the mark drives the
                        // degraded wake cadence (the reporter dispatches on
                        // its own episode state — the two are mutually
                        // exclusive).
                        if matches!(
                            self.current_visibility(primary_last_seen),
                            Visibility::Lost { .. } | Visibility::Degraded { .. }
                        ) {
                            visibility_reporter.on_wake_deadline(tokio::time::Instant::now());
                        }
                    }
                    // Snapshot-stream production arm: ONE bounded package
                    // per wakeup (the driver re-enqueues its own token
                    // while the stream has more), so serving a behind
                    // peer's pull interleaves with every other arm
                    // instead of serializing the mirror monolithically
                    // on the loop. Cancel-safe: a single mpsc recv.
                    stream_id = self.snapshot_streams.next_wake() => {
                        if let Some((dst, frame)) = self.snapshot_streams.emit_next(
                            &stream_id,
                            &self.cluster_state,
                            timestamp_now(),
                        ) && let Err(e) = self.send_to(dst, frame).await
                        {
                            tracing::warn!(
                                stream_id = %stream_id,
                                error = %e,
                                "observer: snapshot-stream package send failed; dropping \
                                 stream (the requester resumes from its cursor)"
                            );
                            // The direct leg to the requester dropped; signal
                            // it via a PullFail (relayed INDIRECTLY) so its
                            // pull driver falls to the next target.
                            if let Some((requester, requester_is_observer)) =
                                self.snapshot_streams.abort_stream(&stream_id)
                            {
                                let (fail_dst, fail) = crate::pull_coordinator::pull_fail::<I>(
                                    &self.config.node_id,
                                    timestamp_now(),
                                    &requester,
                                    requester_is_observer,
                                    &stream_id,
                                );
                                let _ = self.send_to(fail_dst, fail).await;
                            }
                        }
                    }
                    // Disciplined-pull WAKE arm (#491 storm-killer): drives
                    // the `pull_coordinator`'s probe/selection/rebalance
                    // timers off its PERSISTENT `wake_deadline` (an absolute
                    // instant from STORED state — the window end / re-probe /
                    // rebalance deadline), NOT a relative sleep, so it fires
                    // under constant sibling-arm activity. `None` (Idle) parks
                    // the arm. Cancel-safe: `sleep_until` consumes nothing and
                    // the deadline is recomputed each iteration from the
                    // coordinator's stored state.
                    _ = async {
                        match self.pull_coordinator.wake_deadline() {
                            Some(due) => {
                                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await
                            }
                            None => std::future::pending().await,
                        }
                    } => {
                        for directive in self.pull_coordinator.tick(std::time::Instant::now()) {
                            self.drive_pull_directive(directive).await;
                        }
                    }
                    // Settled-CRDT spill arm: cadence sweep / write
                    // completion (see `crate::settled_spill`). Cancel-safe;
                    // bounded per-wakeup work.
                    event = self.settled_spill.next_event() => {
                        self.settled_spill.handle(event, &mut self.cluster_state);
                    }
                    // Anti-entropy tick (item 3): broadcast our digest.
                    _ = ae_tick.tick() => {
                        self.on_anti_entropy_tick().await;
                    }
                    // AE-3 recovery tick (D-C / C9): re-pull from a rotating
                    // known peer iff still behind its last-seen digest.
                    _ = recovery_tick.tick() => {
                        self.on_recovery_tick().await;
                    }
                    // Panik arm (BUG-4): consumed. On fire, announce
                    // self-departure + return the Panik terminal.
                    signal = recv_panik(&mut panik_rx) => {
                        return Ok(self.on_panik(signal).await);
                    }
                    // Operator graceful-abort trigger (SIGUSR2). Each
                    // delivery sends one typed GracefulAbortRequest to the
                    // primary. `recv() == None` (signal stream closed) parks
                    // the trigger for the rest of the run — never a
                    // hot-loop. Cancel-safety: `GracefulAbortTrigger::recv`
                    // is cancel-safe (see its doc); a sibling arm winning
                    // drops and rebuilds the recv future without losing a
                    // queued signal.
                    sig = graceful_abort_signal.recv() => {
                        if sig.is_some() {
                            self.request_graceful_abort().await;
                        }
                    }
                    // Announcer outbox drain: the announcer task posts a
                    // ready send; this arm owns the transport `&mut self`.
                    Some(item) = announcer_outbox_rx.recv() => {
                        let outcome = self.send_to(Destination::Primary, item.msg).await;
                        let _ = item.reply.send(outcome);
                    }
                    // Respawn-exec outbox drain: a detached provider call
                    // finished; this arm owns `&mut self` for the
                    // idempotency-cache update + the result reply to the
                    // primary. Never `None`: `self.respawn_exec_tx` keeps
                    // one sender alive for the coordinator's lifetime.
                    Some(outcome) = respawn_exec_rx.recv() => {
                        self.on_respawn_exec_complete(outcome).await;
                    }
                    // #520 per-task narration arm: one LIVE transition the
                    // CRDT merge applied (live broadcast OR snapshot restore
                    // — path-independent) → one operator wake-line at the
                    // spec-fixed level. Never `None`: `self.cluster_state`
                    // holds the sender for the coordinator's lifetime.
                    // Cancel-safe: a single mpsc recv. A narrated transition
                    // is a wake-stream HOST — flush the parked reconnection
                    // note right after, exactly like `RunNarrator::observe`.
                    Some(change) = task_state_change_rx.recv() => {
                        if task_narrator.narrate_live(&change) {
                            self.wake_note.flush_after_host();
                        }
                    }
                    // Policy B fatal-exit (item 13): the invalid_task
                    // monitor signalled; the OBSERVER exits non-zero. Typed as
                    // a structured `FatalPolicyExit` (NOT a generic `Other`) so
                    // the PyO3 boundary RAISES — a policy abort must surface
                    // non-zero, never be log-and-swallowed.
                    Some(reason) = fatal_exit_rx.recv() => {
                        let err = RunError::FatalPolicyExit {
                            reason: format!("invalid_task monitor — {reason}"),
                        };
                        self.emit_terminal_reason_important(&err.to_string());
                        return Err(err);
                    }
                }
            }
        }
        .await;

        // ── Single-teardown (item 17): every spawned task torn down once,
        //    on every Ok/Err/Panik path, via cancel-then-abort-then-await
        //    so a follow-on dispatcher run starts quiesced. ──
        let _ = reporter_cancel_tx.send(());
        reporter_task.abort();
        let _ = reporter_task.await;

        announcer_task.abort();
        let _ = announcer_task.await;

        let _ = invalid_cancel_tx.send(());
        invalid_driver_task.abort();
        let _ = invalid_driver_task.await;
        let _ = aggregation_cancel_tx.send(());
        aggregation_driver_task.abort();
        let _ = aggregation_driver_task.await;

        // The run-loop dispatcher (cold-join: built with the policy
        // listeners) and the inherited dispatcher (relocation), each torn
        // down once if present.
        if let Some(handle) = dispatcher_task {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(handle) = self.inherited_task_completed_dispatcher.take() {
            handle.abort();
            let _ = handle.await;
        }
        // The inherited peer-lifecycle dispatcher (relocation only).
        if let Some(handle) = self.lifecycle_dispatcher_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        loop_result
    }

    /// Top-of-loop OBSERVED-terminal block (the single exit decision
    /// point). The observer terminates ONLY on a terminal it OBSERVED — the
    /// primary's `RunAborted` / `RunComplete` verdict, or the
    /// closed-transport-with-peers clean tail. It carries ZERO authority
    /// over the run, so it NEVER returns an `Err`: a loss of the observer's
    /// own transport visibility is handled separately by the
    /// [`LostVisibilityReporter`] (report-and-retry, NEVER an exit — BUG-B).
    /// Returns `Some(terminal)` for a clean observed exit, `None` to keep
    /// observing.
    fn evaluate_exit(&self, transport_closed: bool) -> Option<ObserverTerminal> {
        // 1. Aborted FIRST (BUG-1): never narrate/exit as completed. This is
        //    the PRIMARY's verdict, broadcast cluster-wide and converged into
        //    the observer's CRDT — the run's authority, faithfully relayed.
        if let Some(reason) = self.cluster_state.run_aborted() {
            return Some(ObserverTerminal::Aborted {
                reason: reason.to_string(),
            });
        }
        // 2. Complete → exit. Also the PRIMARY's verdict. With the
        //    replicated graceful-abort latch set, the composed fact
        //    `run_complete ∧ graceful_abort` IS the graceful-abort verdict
        //    (distinct from a clean success — work was deliberately left
        //    unscheduled — and from the hard abort above); otherwise a
        //    plain clean exit 0.
        //
        //    MID-TRANSFER HOLD: a snapshot STREAM delivers the run
        //    latches on its HEAD package, BEFORE the task bulk — exiting
        //    the moment the latch lands would report a complete run off
        //    a half-merged mirror (zero counts). While a partially-
        //    applied inbound stream is live, keep observing; the stream's
        //    `done` (or its responder stalling past the idle TTL — the
        //    double-fault edge) releases the exit. The hard-abort exit
        //    above is NOT held: an abort is an operator/fatal verdict
        //    where leaving promptly beats complete stats.
        if self.cluster_state.run_complete()
            && !self
                .inbound_snapshots
                .mid_transfer(crate::snapshot_stream::STREAM_IDLE_TTL)
        {
            if self.cluster_state.graceful_abort_requested() {
                return Some(ObserverTerminal::GracefulAbort);
            }
            return Some(ObserverTerminal::Done);
        }
        // 3. Closed transport with peers present → clean exit 0. Read off
        //    the mesh client's pump-published `MembershipView` (≤1-cycle
        //    stale, monotone-toward-truth — it republishes the whole live set
        //    each pump cycle, so it can never MISS a remove). This is the
        //    role's teardown tail: the inbound closed but the wire still has
        //    peers (a clean shutdown), so the observer rides out cleanly. A
        //    stale-HIGH count only delays this by one cycle; a closed
        //    transport with ZERO peers is NOT an exit — it falls through to
        //    the lost-visibility reporter (report-and-retry), never a strand.
        if transport_closed && self.client.peer_count() > 0 {
            return Some(ObserverTerminal::Done);
        }
        None
    }

    /// Compute the observer's CURRENT visibility into the run for the
    /// [`LostVisibilityReporter`]. Visibility is LOST when the observer can
    /// see NO peer (fleet empty — its `-R` setup tunnel dropped, or the
    /// gateway link blipped) OR a NAMED primary has been silent past the
    /// configured threshold with no terminal. Neither is the cluster dying:
    /// the compute mesh runs autonomously over its direct links. This is a
    /// PURE classification (no exit, no `Err`); the reporter owns the
    /// report-and-retry cadence.
    fn current_visibility(&self, primary_last_seen: Instant) -> Visibility {
        // Fleet empty: the observer has no reachable peer (its own transport
        // view collapsed). The transport's role-blind reconnect ticker is
        // already redialling underneath; the observer just reports + waits.
        if self.client.peer_count() == 0 {
            return Visibility::Lost {
                reason: format!(
                    "no reachable peer (the observer's transport view is empty for \
                     ≥{:.1}s)",
                    self.config.fleet_dead_timeout.as_secs_f64()
                ),
                mesh_liveness: self.mesh_liveness(),
            };
        }
        // Named-primary silence: a primary is NAMED but its keepalives /
        // re-points stopped reaching this observer past `peer_timeout`. The
        // half-open-link case the empty-fleet check cannot see. Still not a
        // death — the primary is reachable from its own mesh; the observer
        // lost ITS path to the primary's signals.
        if let Some(primary) = self.cluster_state.current_primary()
            && primary_last_seen.elapsed() > self.config.peer_timeout
        {
            // DATA-PLANE evidence veto (the late-joined-observer keepalive
            // blackout, owner logs 2026-06-11): authenticated frames ARE
            // arriving and applying from the roster's legs — the same
            // ingest that feeds the periodic stats — while the
            // PRIMARY-KEEPALIVE class is not. "Connection down with no
            // successful reconnect+sync" would be FALSE (sync is
            // succeeding), so the verdict is the DEGRADED addressing-gap
            // state: honest narration, no loss episode, no wake-LOSS /
            // cluster-gone escalation; the freshness-filtered primary-leg
            // redial cadence stays alive (see [`Visibility::Degraded`]).
            // The freshness window is the SAME `peer_timeout` the silence
            // judgment uses — one threshold, two sides of one verdict.
            if let Some(arrival) = self.freshest_data_plane_arrival()
                && arrival.elapsed() < self.config.peer_timeout
            {
                return Visibility::Degraded {
                    reason: format!(
                        "named primary {primary} keepalive-silent toward this observer \
                         for {:.0}s while the data plane is live (last frame {:.1}s ago)",
                        primary_last_seen.elapsed().as_secs_f64(),
                        arrival.elapsed().as_secs_f64()
                    ),
                };
            }
            return Visibility::Lost {
                reason: format!(
                    "named primary {primary} silent for {:.0}s (no keepalive / re-point \
                     reaching this observer)",
                    primary_last_seen.elapsed().as_secs_f64()
                ),
                mesh_liveness: self.mesh_liveness(),
            };
        }
        Visibility::Visible
    }

    /// Freshest data-plane delivery instant across the observer's known
    /// roster legs ([`Self::known_peer_roster`]) — the max of each leg's
    /// delivery clock ([`Self::leg_last_delivery`]). `None` when no
    /// roster leg has ever delivered. The positive evidence the
    /// [`Visibility::Degraded`] classification keys on: frames arriving
    /// from ANY member prove the observer's connection to the run is
    /// alive, whatever the primary-keepalive class is doing.
    fn freshest_data_plane_arrival(&self) -> Option<std::time::Instant> {
        self.known_peer_roster()
            .iter()
            .filter_map(|id| self.leg_last_delivery(id))
            .max()
    }

    /// Consult the hosted job ledger for the cluster-gone terminal — and,
    /// once the cluster is proven gone, DISAMBIGUATE whether it left
    /// CLEANLY (a completed framework shutdown) or in FAILURE.
    ///
    /// Single concern at this seam: cross the
    /// [`crate::observer::job_ledger::JobLedgerProbe`] port to learn the
    /// run's authoritative terminal disposition when its jobs have left the
    /// queue, and turn it into the observer's exit decision. The observer
    /// never names squeue/sacct — the provider layer owns the queries.
    ///
    /// Two ordered probes (cheap-then-authoritative):
    ///  1. `jobs_still_queued` (a `squeue` probe) feeds the
    ///     [`ClusterGoneDetector`]'s two-consecutive-empty double-check.
    ///     Until that renders [`ClusterGoneVerdict::Gone`] there is no
    ///     decision (`None`).
    ///  2. ONLY when GONE: `run_terminal_outcome` (the authoritative
    ///     `sacct` consult) classifies WHY the queue emptied. `squeue` going
    ///     empty is AMBIGUOUS — every job COMPLETED-exit-0 (a clean
    ///     framework shutdown) and a crash/scancel/OOM both leave the queue
    ///     — so the gone-cluster cannot be authored as FAILED on the
    ///     empty-queue evidence alone (the #532 false-FAIL: a clean 66k run
    ///     whose `RunComplete` was lost to the dropped observer leg exited 1).
    ///
    /// CONDITIONAL on hosting a ledger: a cold-join observer (no probe
    /// wired) returns `None` and keeps the never-terminal report-and-retry
    /// behaviour (it cannot teardown a cluster it did not submit).
    ///
    /// Disposition mapping (the optimistic COMPLETED is gated on a POSITIVE
    /// all-completed reading; everything else stays the conservative FAILED,
    /// so the genuine-failure path is preserved):
    ///  - [`ClusterTerminalOutcome::Completed`] ⇒
    ///    [`ClusterGoneDecision::CompletedClean`] (the caller exits `Done`).
    ///  - [`ClusterTerminalOutcome::Failed`] / `Indeterminate` ⇒
    ///    [`ClusterGoneDecision::Failed`] (the caller exits `FatalPolicyExit`).
    ///
    /// On either decision the caller drives the existing pipeline-guard
    /// teardown (the #451 clean-completion job sweep + tunnel teardown — the
    /// SAME path the normal exit's `cancel_all_jobs` runs); only the EXIT
    /// CODE + the operator-facing line differ between the two.
    ///
    /// Each reason/note carries the observer's last-known run state from its
    /// converged CRDT (the last narrated phase / counts) so the operator
    /// learns what the run was doing — what is DERIVABLE, distinct from the
    /// authoritative disposition.
    async fn consult_cluster_gone(
        &self,
        detector: &mut ClusterGoneDetector,
    ) -> Option<ClusterGoneDecision> {
        let probe = self.job_ledger.as_ref()?;
        let last_known = self.last_known_run_state();
        // Cheap streak probe first: no authoritative consult until the
        // double-check has proven the cluster gone.
        let ClusterGoneVerdict::Gone { reason } =
            detector.observe(probe.jobs_still_queued().await, &last_known)
        else {
            return Some(ClusterGoneDecision::KeepWatching);
        };
        // Cluster proven GONE — now read the AUTHORITATIVE terminal state to
        // disambiguate clean-completion from failure.
        match probe.run_terminal_outcome().await {
            ClusterTerminalOutcome::Completed => Some(ClusterGoneDecision::CompletedClean {
                note: format!(
                    "run completed cleanly — all of the run's SLURM jobs reached \
                     COMPLETED (exit 0) per the authoritative accounting (sacct), so \
                     the cluster left the queue because the run FINISHED, not because \
                     it failed. The observer's connection dropped before the primary's \
                     RunComplete verdict reached it, so its last converged ledger is \
                     STALE: {last_known}. The authoritative final counts are on the \
                     gateway/primary host; the observer reports the run as COMPLETED \
                     (exit 0)."
                ),
            }),
            ClusterTerminalOutcome::Failed => Some(ClusterGoneDecision::Failed {
                reason: format!(
                    "{reason} The authoritative accounting (sacct) shows at least one \
                     of the run's jobs reached a FAILURE terminal (FAILED / CANCELLED \
                     / TIMEOUT / NODE_FAIL / OUT_OF_MEMORY), so the run is treated as \
                     FAILED."
                ),
            }),
            ClusterTerminalOutcome::Indeterminate => Some(ClusterGoneDecision::Failed {
                reason: format!(
                    "{reason} The authoritative accounting (sacct) could not confirm a \
                     clean completion for every job (it was unreadable, or returned no \
                     terminal state for part of the cohort), so — conservatively — the \
                     run is treated as FAILED. If the run actually completed cleanly, \
                     consult the gateway/primary host for the run's verdict."
                ),
            }),
        }
    }

    /// A human description of the run's last-known state from the
    /// observer's converged CRDT, for the cluster-empty verdict line. The
    /// primary's verdict (`RunComplete` / `RunAborted`) is checked FIRST at
    /// top-of-loop, so reaching the consult means neither converged — this
    /// reports the phase progress the observer last saw, which is what is
    /// DERIVABLE about a run whose cluster left the queue without a
    /// completion verdict of its own.
    fn last_known_run_state(&self) -> String {
        let counts = self.cluster_state.outcome_counts();
        let done = counts.succeeded;
        let failed = counts.fail_retry + counts.fail_oom + counts.fail_final;
        match self.cluster_state.current_primary() {
            Some(primary) => format!(
                "no run-terminal converged (last recognized primary {primary}); \
                 {done} task(s) completed, {failed} failed in the observer's last \
                 converged ledger"
            ),
            None => format!(
                "no run-terminal converged and no primary is named in the last \
                 converged ledger; {done} task(s) completed, {failed} failed"
            ),
        }
    }

    /// CRDT-derived evidence of whether the compute mesh is still alive,
    /// for the [`LostVisibilityReporter`]'s reassurance gate. This is the
    /// ONLY signal that distinguishes an ssh-link blip (mesh fine, banner
    /// may reassure) from an all-nodes teardown (mesh dead, banner must
    /// stay neutral) — the observer's own transport `peer_count()` cannot.
    ///
    /// Reads the SAME positive CRDT liveness signal the AE-3 recovery tick
    /// and the reconnect roster use ([`crate::cluster_state::ClusterState::alive_secondary_members`]):
    /// peers that POSITIVELY advertised worker-secondary capacity AND are
    /// still live members of the last converged snapshot. A non-zero count
    /// is positive evidence the mesh survived; zero (the membership ledger
    /// emptied, or no roster ever landed) is [`MeshLiveness::Unknown`] —
    /// the observer holds nothing that confirms autonomy.
    fn mesh_liveness(&self) -> MeshLiveness {
        let alive_count = self.cluster_state.alive_secondary_members().count();
        if alive_count > 0 {
            MeshLiveness::KnownAlive { alive_count }
        } else {
            MeshLiveness::Unknown
        }
    }

    /// Trigger a transport-path rebuild for the observer's expected roster
    /// (BUG-B reconnect). Single concern at this seam: the observer hands
    /// the [`crate::observer::reconnect::TunnelReconnector`] the ids it
    /// expects to reach and lets the provider layer rebuild the dropped
    /// `-R` tunnels — the observer NEVER names ssh. No-op when no
    /// reconnector is wired (a transport that heals its own links) or when
    /// the roster is empty (nothing known to reach yet).
    ///
    /// The roster is the SAME `current_primary ∪ alive secondaries` set the
    /// AE-3 recovery tick targets — the peers whose `-R` tunnels carry the
    /// observer's link to the run. The port call is SPAWNED detached on the
    /// LocalSet so the observer loop never blocks on the (info-file-polling,
    /// ssh-handshaking) rebuild; the observer keeps observing + narrating,
    /// and its visibility flips back to `Visible` once the SECONDARY's
    /// bootstrap-redial supervisor re-dials over the rebuilt tunnel and the
    /// pump republishes membership (the re-dial is an owned transport-quic
    /// mechanism, not an assumed side effect). A failed rebuild is retried
    /// on the next lost-visibility cadence tick — but a tunnel whose child
    /// is still ALIVE is a no-op (the liveness gate), never a rebuild
    /// against its own healthy forward.
    fn trigger_reconnect(&self) {
        let Some(reconnector) = self.reconnector.clone() else {
            return;
        };
        // SINGLE-FLIGHT: a reconnect over a roster with dead peers can run
        // for minutes (each dead id's rebuild burns its ssh establishment
        // budget). Piling a fresh detached call onto a still-running one
        // every ~60s cadence tick made the overlapping calls race each
        // other's release+rebind on the SAME tunnel ports — the next
        // cadence tick after completion retries instead.
        if self.reconnect_in_flight.get() {
            tracing::debug!(
                "observer reconnect still in flight; skipping this cadence tick \
                 (the next tick after it completes retries)"
            );
            return;
        }
        let mut roster: HashSet<String> = self.known_peer_roster();
        // PER-LEG honesty: the reconnector's contract is "the ids I have
        // LOST". Run-level visibility can be lost for reasons unrelated to
        // a given leg (a silent named primary, dead sibling peers), so the
        // roster is filtered by each leg's OWN delivery freshness — a peer
        // whose frames are arriving is not lost, and handing it over would
        // make the provider's half-dead escalation force-rebuild a WORKING
        // tunnel every K ticks (tearing down the only live legs — the
        // run_20260611_200548 probe churn). The judgment keys on the
        // inbox's per-peer ingest clocks, never the aggregate verdict.
        let delivering: Vec<String> = roster
            .iter()
            .filter(|id| self.leg_recently_delivered(id))
            .cloned()
            .collect();
        for id in &delivering {
            roster.remove(id);
        }
        if roster.is_empty() {
            tracing::debug!(
                excluded_delivering = ?delivering,
                "observer reconnect due but every roster leg is delivering \
                 (or no roster is known yet); nothing to rebuild this tick"
            );
            return;
        }
        let peer_ids: Vec<String> = roster.into_iter().collect();
        tracing::info!(
            peers = peer_ids.len(),
            excluded_delivering = ?delivering,
            "observer triggering tunnel rebuild for lost roster (BUG-B reconnect)"
        );
        self.reconnect_in_flight.set(true);
        let in_flight = std::rc::Rc::clone(&self.reconnect_in_flight);
        tokio::task::spawn_local(async move {
            // Reset on EVERY exit of the spawned call — completion,
            // cancellation (LocalSet teardown), or a panicking
            // reconnector — so the latch can never stick shut.
            struct ResetOnDrop(std::rc::Rc<std::cell::Cell<bool>>);
            impl Drop for ResetOnDrop {
                fn drop(&mut self) {
                    self.0.set(false);
                }
            }
            let _reset = ResetOnDrop(in_flight);
            reconnector.reconnect(&peer_ids).await;
        });
    }

    /// The observer's known-peer roster — `current_primary ∪ alive
    /// worker-secondary members` — the ONE set the reconnect trigger,
    /// the AE-3 recovery tick, and the data-plane freshness read all key
    /// on (one roster definition, three consumers).
    fn known_peer_roster(&self) -> HashSet<String> {
        let mut roster: HashSet<String> = self
            .cluster_state
            .alive_secondary_members()
            .map(str::to_owned)
            .collect();
        if let Some(p) = self.cluster_state.current_primary() {
            roster.insert(p.to_owned());
        }
        roster
    }

    /// When did `peer_id`'s leg last DELIVER a frame — the freshest of
    /// the inbox's per-peer ingest clocks (slot delivery + the transport
    /// arrival edge). `None` if the leg never delivered. The ONE
    /// per-leg delivery clock both the reconnect-roster freshness filter
    /// and the data-plane evidence read consume.
    fn leg_last_delivery(&self, peer_id: &str) -> Option<std::time::Instant> {
        [
            self.inbox.last_ingest_from(peer_id),
            self.inbox.last_transport_arrival_from(peer_id),
        ]
        .into_iter()
        .flatten()
        .max()
    }

    /// Whether `peer_id`'s leg has DELIVERED a frame recently — the
    /// per-leg liveness the reconnect roster keys on. Reads the leg's
    /// delivery clock ([`Self::leg_last_delivery`]) against the same
    /// [`ObserverConfig::peer_timeout`] silence threshold the
    /// named-primary visibility check uses: a leg silent past it is
    /// rebuild-eligible (covers the wired-but-blackholed wire, which
    /// `has_peer` would wrongly call healthy); anything fresher is a
    /// working leg the reconnector must not touch.
    fn leg_recently_delivered(&self, peer_id: &str) -> bool {
        self.leg_last_delivery(peer_id)
            .is_some_and(|t| t.elapsed() < self.config.peer_timeout)
    }

    /// Dispatch one inbound mesh frame. Apply-only: the observer mirrors
    /// CRDT mutations, refreshes primary-liveness on recognised signals,
    /// heals from snapshots, and reconciles digests. It NEVER originates a
    /// mutation or re-broadcasts.
    async fn on_inbound(&mut self, msg: DistributedMessage<I>, primary_last_seen: &mut Instant) {
        match msg {
            DistributedMessage::ClusterMutation { mutations, .. } => {
                self.on_cluster_mutation(mutations, primary_last_seen);
            }
            DistributedMessage::Keepalive {
                secondary_id,
                emitter_role,
                ..
            } => {
                self.on_keepalive(&secondary_id, emitter_role, primary_last_seen);
            }
            DistributedMessage::SnapshotStreamPackage {
                sender_id,
                stream_id,
                cursor,
                payload,
                done,
                ..
            } => {
                self.on_snapshot_stream_package(
                    &sender_id,
                    &stream_id,
                    cursor.as_deref(),
                    &payload,
                    done,
                    primary_last_seen,
                );
            }
            DistributedMessage::StateDigest {
                sender_id,
                digest,
                sender_is_observer,
                ..
            } => {
                self.on_state_digest(&sender_id, sender_is_observer, &digest)
                    .await;
            }
            // Pull-model PROBE from a behind peer: answer with our inbox
            // depth + `ahead`. Direct-neighbours-only (the ingress never
            // re-broadcast this inbound `All`); never relayed onward.
            DistributedMessage::PullProbe {
                sender_id, digest, ..
            } => {
                self.handle_pull_probe(&sender_id, &digest).await;
            }
            // Pull-model PROBE REPLY → the single-flight pull driver.
            DistributedMessage::PullProbeReply {
                sender_id,
                requester,
                inbox_size,
                ahead,
                range_digest,
                ..
            } => {
                self.handle_pull_probe_reply(
                    &sender_id,
                    &requester,
                    inbox_size,
                    ahead,
                    range_digest,
                )
                .await;
            }
            // Pull-model FAIL (chosen target's direct leg to us dropped,
            // delivered INDIRECTLY via the relay) → fall to next target.
            DistributedMessage::PullFail {
                requester,
                stream_id,
                ..
            } => {
                self.handle_pull_fail(&requester, &stream_id).await;
            }
            DistributedMessage::RequestSnapshotStream {
                sender_id,
                stream_id,
                resume_after,
                task_ranges,
                is_observer,
                ..
            } => {
                self.answer_snapshot_request(
                    &sender_id,
                    is_observer,
                    &stream_id,
                    resume_after.as_deref(),
                    &task_ranges,
                );
            }
            // Respawn EXECUTION requests from the primary (the decision
            // holder): this process physically hosts the provider, so it
            // executes and replies — zero authority involved. See the
            // `respawn_provider` field doc.
            DistributedMessage::RespawnSpawnRequest {
                new_secondary_id,
                primary_endpoint,
                primary_pubkey_pem,
                dead_member_id,
                ..
            } => {
                self.on_respawn_spawn_request(
                    new_secondary_id,
                    primary_endpoint,
                    primary_pubkey_pem,
                    dead_member_id,
                )
                .await;
            }
            DistributedMessage::RespawnRevokeRequest {
                new_secondary_id, ..
            } => {
                self.on_respawn_revoke_request(new_secondary_id).await;
            }
            // The primary directed a `TaskKind::Setup` task to THIS observer's
            // in-process executor (framework auto-staging affinity = the
            // submitter, which runs as a standalone observer after a
            // bootstrap relocation). The observer runs it poolless and reports
            // the terminal to the primary (it holds zero authority — the
            // primary originates the CRDT terminal). Body + report live in
            // `observer::setup_exec`; this is the one-line delegate, the twin
            // of the secondary's router arm.
            DistributedMessage::SetupAssignment { task_hash, .. } => {
                self.execute_setup_assignment(task_hash).await;
            }
            // Every other frame is irrelevant to a zero-authority
            // observer (it owns no dispatch / setup / election concern).
            // Silently ignored — the observer only consumes the
            // replication / liveness frames above.
            _ => {}
        }
    }

    /// Execute one spawn request against the hosted provider —
    /// idempotently. The primary-minted `new_secondary_id` is the
    /// dedup key: `InFlight` absorbs a re-sent request (its result
    /// reply is already on the way once the provider finishes); `Done`
    /// replays the cached outcome (the lost-result case — the primary
    /// re-sends until a result lands). A fresh id marks `InFlight` and
    /// drives the provider on a detached LocalSet task (the loop never
    /// blocks on sbatch/tunnel work), whose outcome returns through the
    /// respawn-exec outbox arm. No provider hosted → an immediate error
    /// reply (a misdirected request — only the submitter process holds
    /// a provider) so the primary's budget/logging sees the failure
    /// instead of retrying forever.
    async fn on_respawn_spawn_request(
        &mut self,
        new_secondary_id: String,
        primary_endpoint: String,
        primary_pubkey_pem: String,
        dead_member_id: Option<String>,
    ) {
        match self.respawn_exec.get(&new_secondary_id) {
            Some(RespawnExecState::InFlight) => {
                tracing::debug!(
                    target: "dynrunner_respawn",
                    new_secondary_id = %new_secondary_id,
                    "duplicate spawn request while execution is in flight; absorbed",
                );
                return;
            }
            Some(RespawnExecState::Done(result)) => {
                tracing::debug!(
                    target: "dynrunner_respawn",
                    new_secondary_id = %new_secondary_id,
                    "duplicate spawn request after completion; replaying the cached outcome",
                );
                let cached = result.clone();
                self.send_respawn_result(RespawnExecKind::Spawn, &new_secondary_id, &cached)
                    .await;
                return;
            }
            None => {}
        }
        let Some(provider) = self.respawn_provider.clone() else {
            tracing::warn!(
                target: "dynrunner_respawn",
                new_secondary_id = %new_secondary_id,
                "spawn request received but this observer hosts no respawn \
                 provider (only the submitter process does); replying with an \
                 error",
            );
            self.send_respawn_result(
                RespawnExecKind::Spawn,
                &new_secondary_id,
                &Err("this observer hosts no respawn provider".to_string()),
            )
            .await;
            return;
        };
        self.respawn_exec
            .insert(new_secondary_id.clone(), RespawnExecState::InFlight);
        tracing::info!(
            target: "dynrunner_respawn",
            new_secondary_id = %new_secondary_id,
            event = "respawn_exec_started",
            "executing the primary's spawn request against the hosted provider",
        );
        let spec = crate::primary::respawn::SecondarySpawnSpec {
            new_secondary_id: new_secondary_id.clone(),
            primary_endpoint,
            primary_pubkey_pem,
            dead_member_id,
        };
        let tx = self.respawn_exec_tx.clone();
        tokio::task::spawn_local(async move {
            let result = provider.spawn(spec).await.map_err(|e| e.to_string());
            // A closed outbox means the observer is winding down; the
            // provider's own orphan-safety (job id on its `job_ids`
            // ledger → run-teardown scancel sweep) covers the job.
            let _ = tx.send(RespawnExecOutcome {
                kind: RespawnExecKind::Spawn,
                new_secondary_id,
                result,
            });
        });
    }

    /// Handle one revoke request from a primary. The per-replacement
    /// revoke surface was retired in favour of the slurm-authoritative
    /// quantity gate (#543); no current primary emits this frame. A frame
    /// that lands here is a partial-upgrade leftover — reply Ok so the
    /// sender's retry stops, and proceed without driving the provider.
    /// The provider's run-teardown sweep remains the reclamation backstop
    /// for any over-allocation.
    async fn on_respawn_revoke_request(&mut self, new_secondary_id: String) {
        tracing::debug!(
            target: "dynrunner_respawn",
            new_secondary_id = %new_secondary_id,
            "revoke request received but the revoke surface was retired; \
             replying Ok without driving the provider",
        );
        self.send_respawn_result(RespawnExecKind::Revoke, &new_secondary_id, &Ok(()))
            .await;
    }

    /// Record a finished provider call and reply its outcome to the
    /// primary. Spawn outcomes are cached in the idempotency map so a
    /// duplicate request replays the SAME result (one id can never
    /// double-submit); revoke outcomes are not cached (the provider is
    /// idempotent for revokes).
    async fn on_respawn_exec_complete(&mut self, outcome: RespawnExecOutcome) {
        let RespawnExecOutcome {
            kind,
            new_secondary_id,
            result,
        } = outcome;
        if let RespawnExecKind::Spawn = kind {
            self.respawn_exec.insert(
                new_secondary_id.clone(),
                RespawnExecState::Done(result.clone()),
            );
        }
        self.send_respawn_result(kind, &new_secondary_id, &result)
            .await;
    }

    /// Send one respawn result frame to the primary (whichever host
    /// currently holds the role — a failover between request and result
    /// re-resolves; an unmatched result at a successor primary is
    /// logged-and-ignored there, and the successor's own retry replays
    /// against this observer's outcome cache). A send failure (no
    /// current primary visible) is WARNed and NOT retried here: the
    /// primary's request-side retry is the replay driver, and a
    /// re-sent request replies again from the cache.
    async fn send_respawn_result(
        &mut self,
        kind: RespawnExecKind,
        new_secondary_id: &str,
        result: &Result<(), String>,
    ) {
        let error = result.as_ref().err().cloned();
        let msg = match kind {
            RespawnExecKind::Spawn => DistributedMessage::RespawnSpawnResult {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_secondary_id: new_secondary_id.to_owned(),
                error,
            },
            RespawnExecKind::Revoke => DistributedMessage::RespawnRevokeResult {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
                new_secondary_id: new_secondary_id.to_owned(),
                error,
            },
        };
        if let Err(e) = self.send_to(Destination::Primary, msg).await {
            tracing::warn!(
                target: "dynrunner_respawn",
                new_secondary_id,
                error = %e,
                "could not send the respawn result to the primary (no route); \
                 the primary's request retry will trigger a replay from the \
                 outcome cache",
            );
        }
    }

    /// Apply-only CRDT mirror (item 1) + incremental-`PrimaryChanged`
    /// liveness refresh (§2 event 2). Apply EACH mutation through the PURE
    /// `cluster_state.apply` — never `handle_cluster_mutation`. A batch
    /// that re-points `current_primary()` to a new value refreshes
    /// `primary_last_seen` (the newly-named primary is live by
    /// construction).
    fn on_cluster_mutation(
        &mut self,
        mutations: Vec<ClusterMutation<I>>,
        primary_last_seen: &mut Instant,
    ) {
        let before = self.cluster_state.current_primary().map(str::to_owned);
        for m in mutations {
            // Prune the departed peer's AE-3 recovery digest so the
            // `peer_digests` store stays bounded by the LIVE roster (the
            // recovery tick already excludes a departed id from routing via
            // the roster intersection; this stops the store growing without
            // bound over the run's lifetime). Mirrors the CRDT's own
            // sticky-`Dead` peer_state removal: once `PeerRemoved` lands the
            // id never re-enters the live roster, so dropping its last-seen
            // digest is safe (a respawn requires a fresh id).
            if let ClusterMutation::PeerRemoved { id, .. } = &m {
                self.peer_digests.remove(id);
            }
            self.cluster_state.apply(m);
        }
        let after = self.cluster_state.current_primary().map(str::to_owned);
        if after.is_some() && after != before {
            *primary_last_seen = Instant::now();
        }
    }

    /// Primary-liveness refresh on a recognised primary keepalive (§2
    /// event 1). Mirrors the secondary's recognition rule: a `Primary`-role
    /// keepalive whose originator IS the current primary refreshes the
    /// clock; a `Primary` keepalive from a non-current id, or any
    /// `Secondary`-role keepalive, does NOT.
    fn on_keepalive(
        &mut self,
        secondary_id: &str,
        emitter_role: KeepaliveRole,
        primary_last_seen: &mut Instant,
    ) {
        if emitter_role == KeepaliveRole::Primary
            && self.cluster_state.current_primary() == Some(secondary_id)
        {
            *primary_last_seen = Instant::now();
        }
    }

    /// Bootstrap-recovery REPLY half + late/anti-entropy snapshot heal
    /// (§6) with the BUG-5 liveness refresh. Decode one stream package +
    /// `restore()` (idempotent lattice — each package is a valid partial
    /// snapshot) and refresh `primary_last_seen` if it re-points
    /// `current_primary()` (a package that newly names a primary is a
    /// liveness assertion; the fact rides the stream's HEAD package). A
    /// decode failure is WARN-and-ignore in steady state — NOT fatal —
    /// and does not advance the resume cursor, so the recovery cadence
    /// re-pulls from before the bad span.
    fn on_snapshot_stream_package(
        &mut self,
        sender_id: &str,
        stream_id: &str,
        cursor: Option<&str>,
        payload: &str,
        done: bool,
        primary_last_seen: &mut Instant,
    ) {
        match crate::cluster_state::decode_stream_payload::<I>(payload) {
            Ok(snap) => {
                let before = self.cluster_state.current_primary().map(str::to_owned);
                self.cluster_state.restore(snap);
                self.inbound_snapshots
                    .note_package(sender_id, stream_id, cursor, done);
                let after = self.cluster_state.current_primary().map(str::to_owned);
                if after.is_some() && after != before {
                    *primary_last_seen = Instant::now();
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    stream_id = %stream_id,
                    "observer: failed to decode SnapshotStreamPackage; ignoring (the next \
                     live broadcast / anti-entropy pull heals from the last good cursor)"
                );
            }
        }
        // End the disciplined pull's in-flight cycle on the terminal package
        // (whether or not THIS package decoded) so the driver returns to
        // Idle: a converged observer goes quiescent, a still-behind one (a
        // WARN-dropped package) re-probes on its next divergence detection
        // (the recovery tick) rather than waiting out the rebalance. A NoOp
        // for a bootstrap-stream `done`.
        if done {
            self.pull_coordinator.on_pull_done(stream_id);
        }
    }

    /// Anti-entropy receive side (item 3): compare the peer's digest
    /// against ours and, iff behind, NOTE the divergence to the disciplined
    /// PULL driver (`pull_coordinator`) instead of firing an eager immediate
    /// pull at the sender. `note_behind` is IDEMPOTENT (a NoOp while a
    /// probe→pull cycle is in flight = single-flight), so a behind observer
    /// under churn initiates pulls at the cooldown rate, not one per inbound
    /// digest. The cold trigger's `Probe` directive is driven onto the wire;
    /// selection + pull happen on the pull arm's timers. The peer-digest
    /// store is kept (it still bounds against the live roster and the
    /// recovery tick reads "are we behind any known peer"); the eager pull
    /// is replaced.
    async fn on_state_digest(
        &mut self,
        sender_id: &str,
        sender_is_observer: bool,
        peer: &StateDigest,
    ) {
        // Record this peer's last-seen digest + declared role (the recovery
        // tick's still-behind-any-known-peer signal reads it).
        self.peer_digests
            .insert(sender_id.to_string(), (sender_is_observer, *peer));
        let local = self.cluster_state.digest();
        if !local.is_behind(peer) {
            // Converged on this peer's digest — nothing to pull.
            return;
        }
        if let Some(directive) = self
            .pull_coordinator
            .note_behind(std::time::Instant::now())
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Translate ONE [`crate::pull_coordinator::PullDirective`] into this
    /// observer's `send_to` edge — the role-owned wire-touch half of the
    /// disciplined pull (FSM + selection in `pull_coordinator`; frame
    /// construction + role-typing in `pull_coordinator::pull_probe` /
    /// `pull_request`; this method only owns `send_to`). The observer
    /// declares `is_observer: true`, `can_be_primary: false` on its pulls.
    async fn drive_pull_directive(&mut self, directive: crate::pull_coordinator::PullDirective) {
        match directive {
            crate::pull_coordinator::PullDirective::Probe => {
                let digest = self.cluster_state.digest();
                let frame = crate::pull_coordinator::pull_probe::<I>(
                    &self.config.node_id,
                    timestamp_now(),
                    digest,
                );
                if let Err(e) = self.send_to(Destination::All, frame).await
                    && let Some(suppressed) = self.ae_recovery_warn.permit()
                {
                    tracing::warn!(
                        error = %e,
                        suppressed_since_last_warn = suppressed,
                        "observer pull-probe broadcast failed; the pull driver re-probes"
                    );
                }
            }
            crate::pull_coordinator::PullDirective::PullFrom {
                target_id,
                target_is_observer,
                target_range_digest,
            } => {
                // P1: narrow to the buckets divergent from the chosen
                // responder (compare its piggybacked range digest to ours).
                let task_ranges = crate::pull_coordinator::divergent_ranges_for_pull(
                    &self.cluster_state.tasks_range_digest(),
                    &target_range_digest,
                );
                let (dst, frame, stream_id) = crate::pull_coordinator::pull_request::<I>(
                    &self.config.node_id,
                    true,
                    false,
                    &target_id,
                    target_is_observer,
                    task_ranges,
                    &mut self.inbound_snapshots,
                    timestamp_now(),
                );
                if self.send_to(dst, frame).await.is_ok() {
                    self.pull_coordinator.note_pull_stream(&stream_id);
                }
            }
        }
    }

    /// Answer an inbound `PullProbe`: reply with this observer's inbox depth
    /// + the responder-side `ahead` bit. Direct-only reply.
    async fn handle_pull_probe(&mut self, prober_id: &str, prober_digest: &StateDigest) {
        let local = self.cluster_state.digest();
        let ahead = crate::pull_coordinator::probe_reply_ahead(&local, prober_digest);
        // P1: piggyback this observer's task-ledger range digest so the
        // prober computes the divergent buckets (an observer holds the same
        // replicated ledger, so it can serve a delta like any peer).
        let range_digest = self.cluster_state.tasks_range_digest();
        let (dst, frame) = crate::pull_coordinator::pull_probe_reply::<I>(
            &self.config.node_id,
            timestamp_now(),
            prober_id,
            false,
            self.inbox.depth() as u64,
            ahead,
            range_digest,
        );
        let _ = self.send_to(dst, frame).await;
    }

    /// Record an inbound `PullProbeReply` into the pull driver (a usable
    /// reply may resolve the pull target via the first-answer fallback).
    async fn handle_pull_probe_reply(
        &mut self,
        responder_id: &str,
        requester: &str,
        inbox_size: u64,
        ahead: bool,
        range_digest: Box<dynrunner_protocol_primary_secondary::RangeDigest>,
    ) {
        if requester != self.config.node_id {
            return;
        }
        let reply = crate::pull_coordinator::ProbeReply {
            responder_id,
            responder_is_observer: false,
            inbox_size,
            ahead,
            range_digest,
        };
        if let Some(directive) = self
            .pull_coordinator
            .on_probe_reply(std::time::Instant::now(), &reply)
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Record an inbound `PullFail` and fall to the next pull target.
    async fn handle_pull_fail(&mut self, requester: &str, stream_id: &str) {
        if requester != self.config.node_id {
            return;
        }
        if let Some(directive) = self
            .pull_coordinator
            .on_fail(std::time::Instant::now(), stream_id)
        {
            self.drive_pull_directive(directive).await;
        }
    }

    /// Serve a peer's anti-entropy snapshot pull from this observer's
    /// converged mirror.
    ///
    /// READ-ONLY gossip, in-contract for a zero-authority observer: the
    /// answer is a STREAM of the mirror it already holds — no mutation is
    /// originated (unlike the secondary's responder, NO `PeerJoined` is
    /// emitted; membership recording stays a compute-role concern) and
    /// nothing is re-broadcast. Serving is LOAD-BEARING for the
    /// relocation handoff: after the submitter relocates, this observer
    /// can be the ONLY replica holding the `PrimaryChanged` fact (the
    /// live broadcast is one-shot), and its own digest broadcasts
    /// (`on_anti_entropy_tick`) are what prove behind peers divergent — a
    /// mute responder would advertise data nobody can pull (the
    /// run_20260610_185621 leaderless wedge).
    ///
    /// The handler only REGISTERS (or resumes) the stream; the run
    /// loop's stream arm produces one bounded package per wakeup, each
    /// typed off the requester's self-declared role (the same
    /// `is_observer` field every snapshot responder records) via the
    /// shared reply policy ([`anti_entropy::reply_destination`]):
    /// `Observer(id)` for an observer requester, `Secondary(id)`
    /// otherwise.
    fn answer_snapshot_request(
        &mut self,
        requester: &str,
        requester_is_observer: bool,
        stream_id: &str,
        resume_after: Option<&str>,
        task_ranges: &[u16],
    ) {
        self.snapshot_streams.accept_request(
            &self.cluster_state,
            requester,
            requester_is_observer,
            stream_id,
            resume_after,
            task_ranges,
        );
    }

    /// Anti-entropy tick (item 3): broadcast our digest to the mesh.
    ///
    /// FAULTS ARE LOUD (rate-limited via [`Self::ae_digest_warn`]): a
    /// broadcast over an EMPTY mesh registry returns `Ok` while reaching
    /// nobody — in the demoted-submitter topology this is exactly the
    /// heal-never-engages wedge (the secondaries' re-dialed bootstrap
    /// wires sit unregistered on the accept loop until they speak), and
    /// it was previously fully silent: no Err, and the Err arm only ever
    /// logged DEBUG. Both fault shapes now WARN, naming the registry
    /// state against the replicated roster so the divergence (CRDT knows
    /// N peers, the wire reaches 0) is visible to the operator.
    async fn on_anti_entropy_tick(&mut self) {
        if self.client.peer_count() == 0 {
            // The roster the replicated ledger believes exists — the WARN
            // names both sides of the divergence.
            let roster_secondaries = self.cluster_state.alive_secondary_members().count();
            let roster_primary = self.cluster_state.current_primary().map(str::to_owned);
            if let Some(suppressed) = self.ae_digest_warn.permit() {
                tracing::warn!(
                    roster_secondaries,
                    roster_primary = ?roster_primary,
                    suppressed_since_last_warn = suppressed,
                    "anti-entropy digest has no peers to reach: the mesh \
                     registry is EMPTY while the replicated roster names the \
                     peers above — the digest heal cannot engage until a peer \
                     (re-)registers (a re-dialed bootstrap wire registers on \
                     its first inbound frame)"
                );
            }
        }
        let digest = self.cluster_state.digest();
        // The standalone observer declares `is_observer: true` so a behind
        // peer that pulls a snapshot FROM this observer types the pull
        // `Observer(id)` and hits this process's observer-slot ingress demux
        // (instead of mis-typing `Secondary` and tripping the role-miss fan
        // WARN once per cadence on every behind peer).
        let msg = anti_entropy::digest_broadcast::<I>(
            &self.config.node_id,
            timestamp_now(),
            digest,
            true,
        );
        if let Err(e) = self.send_to(Destination::All, msg).await
            && let Some(suppressed) = self.ae_digest_warn.permit()
        {
            tracing::warn!(
                error = %e,
                suppressed_since_last_warn = suppressed,
                "observer anti-entropy digest broadcast failed; next tick retries"
            );
        }
    }

    /// AE-3 recovery-cadence tick (D-C / C9): the TIMER-driven half of the
    /// disciplined pull, distinct from the digest broadcast above. With ZERO
    /// inbound traffic, NOTE divergence to the `pull_coordinator` IFF the
    /// local replica is still behind ANY known peer's last-seen digest. This
    /// is what makes a WARN-dropped steady-state decode (the non-latching
    /// `on_cluster_snapshot` arm) recover: the next tick re-notes until
    /// convergence, and `note_behind`'s single-flight + the pull driver's
    /// 30s re-probe / target selection subsume the old responder rotation.
    /// The role owns only the `send_to` edge (via `drive_pull_directive`) +
    /// the peer-digest store + the roster intersection; the convergence
    /// detection lives in [`StateDigest::is_behind`] and the
    /// single-flight/selection in [`crate::pull_coordinator`].
    async fn on_recovery_tick(&mut self) {
        // Known-peer roster ([`Self::known_peer_roster`]): the live peers
        // we both KNOW and hold a last-seen digest for (a departed peer's
        // digest is excluded — we never resurrect a route to a dead peer).
        let roster: HashSet<String> = self.known_peer_roster();
        let local = self.cluster_state.digest();
        // Behind ANY known live peer's last-seen digest? (the C9 quiesce
        // signal: if converged with all of them, nothing to do this tick.)
        let behind_any = self
            .peer_digests
            .iter()
            .filter(|(id, _)| roster.contains(id.as_str()))
            .any(|(_, (_, d))| local.is_behind(d));
        if !behind_any {
            // Converged with every known peer (or none known yet) — quiesce.
            return;
        }
        // The TIMER-driven half of the storm-killer: NOTE the divergence to
        // the disciplined pull driver instead of firing an immediate
        // rotating `plan_recovery_pull`. Idempotent (single-flight) — a
        // recovery tick while a probe→pull cycle is already in flight is a
        // NoOp, so the recovery cadence can never stack a second pull. The
        // pull driver's own 30s re-probe / target selection subsumes the
        // old responder-rotation entirely.
        if let Some(directive) = self
            .pull_coordinator
            .note_behind(std::time::Instant::now())
        {
            // A planned probe over an EMPTY mesh registry can only no-route
            // — name the registry state (rate-limited).
            if self.client.peer_count() == 0
                && let Some(suppressed) = self.ae_recovery_warn.permit()
            {
                tracing::warn!(
                    suppressed_since_last_warn = suppressed,
                    "AE-3 recovery: a known peer's digest proves this replica \
                     behind but the mesh registry is EMPTY — the pull probe \
                     cannot reach anyone until a peer (re-)registers"
                );
            }
            self.drive_pull_directive(directive).await;
        }
    }

    /// Panik arm body (BUG-4 / §7): announce self-departure (observability
    /// only — peers are NOT terminated) then return the Panik terminal so
    /// the boundary exits 137.
    async fn on_panik(&mut self, signal: PanikSignal) -> ObserverTerminal {
        let matched_path = signal.matched_path;
        // The canonical source-attributed reason (owned by `panik_watcher`):
        // a SIGTERM names the sender pid so the operator sees a HOST signal,
        // not a policy abort. Same single owner the secondary teardown uses.
        let reason = panik_watcher::panik_reason(&matched_path, signal.sender_pid);
        tracing::error!(
            matched_path = %matched_path.display(),
            reason = %reason,
            "observer panik signal observed; announcing self-departure and exiting 137"
        );
        // Surface the terminal reason on the important channel too: a panik
        // exit is a run terminal the narrator never projects (no CRDT
        // RunComplete / RunAborted lands), so without this it is silent
        // there.
        self.emit_terminal_reason_important(&format!("{reason} — exiting 137"));
        // Self-authored departure: apply locally + broadcast. Peers LOG it
        // and mark this node Dead. It does NOT cancel cluster work.
        let mutation = ClusterMutation::<I>::PeerRemoved {
            id: self.config.node_id.clone(),
            cause: RemovalCause::SelfDeparture(BoundedString::from(reason)),
            // Kills THIS node's current membership incarnation.
            member_gen: self.cluster_state.peer_member_gen(&self.config.node_id),
        };
        self.cluster_state.apply(mutation.clone());
        let msg = DistributedMessage::ClusterMutation {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            mutations: vec![mutation],
        };
        if let Err(e) = self.send_to(Destination::All, msg).await {
            tracing::warn!(
                error = %e,
                "observer panik self-departure broadcast failed; exiting locally anyway"
            );
        }
        ObserverTerminal::Panik { matched_path }
    }
}

/// Cold-join observer factory: build the watcher, take its signal rx,
/// install the task_completed sender, restore the bootstrap snapshot(s),
/// and hand back a ready coordinator. The setup ORDERING is the one
/// pinned by the spec: install sender → restore snapshot → (listeners +
/// dispatcher happen in `run`). This helper wires the panik watcher whose
/// signal the run loop's panik arm consumes.
pub fn build_cold_join_observer<I>(
    client: MeshClient<I>,
    inbox: RoleInbox<I>,
    cluster_state: ClusterState<I>,
    config: ObserverConfig,
    snapshots: Vec<crate::cluster_state::ClusterStateSnapshot<I>>,
    live_gossip: Vec<DistributedMessage<I>>,
    holdings: HashSet<String>,
) -> ObserverCoordinator<I>
where
    I: Identifier + 'static,
{
    // Spawn the panik watcher (observer-role: SIGTERM listening OFF) and
    // take its signal rx for the run loop's panik arm. The watcher's
    // signal_tx lives on the watcher task; `take_signal_rx` hands us the
    // receiver the run loop's panik arm consumes. Keep the watcher handle
    // alive (its `Drop` aborts the task) by parking it in a detached task —
    // it self-terminates once it fires (oneshot consumed) or the process
    // exits.
    let mut watcher = panik_watcher::spawn_panik_watcher(PanikWatcherConfig {
        paths: config.panik_watcher_paths.clone(),
        poll_interval: config.panik_watcher_poll_interval,
        listen_for_sigterm: false,
    });
    let panik_signal_rx = watcher.take_signal_rx();
    tokio::task::spawn_local(async move {
        let _watcher = watcher; // hold across the park so Drop doesn't fire
        std::future::pending::<()>().await;
    });

    let node_id = config.node_id.clone();
    // `with_pieces` installs the task_completed sender AND attaches the
    // resource-holdings announcer's role-change hook. Construct FIRST, then
    // restore the bootstrap snapshot(s) into the now-wired ledger so:
    //   (a) restore-delivered `TaskCompleted` events are buffered in the
    //       dispatcher channel, and
    //   (b) the restore's role-change fire pushes the initial
    //       `AnnounceTrigger` into the announcer channel (the announcer hook
    //       is registered, so the initial holdings announce is NOT dropped).
    // The full load-bearing ordering is: install sender + attach announcer →
    // restore → register listeners + spawn (in `run`). `restore` is an
    // idempotent lattice merge; applying each responder's snapshot unions
    // them.
    let mut coordinator = ObserverCoordinator::with_pieces(
        client,
        inbox,
        cluster_state,
        node_id,
        config,
        HashSet::new(),
        panik_signal_rx,
        holdings,
        // No reconnector at construction: a cold-join observer whose
        // addresses are DIRECTLY reachable heals through the transport's
        // own QUIC/WSS reconnect ticker. The gateway-mode late-joiner,
        // whose dial targets are `ssh -L` local-forward endpoints that the
        // ticker alone cannot resurrect, wires its registry afterwards via
        // `set_tunnel_reconnector`.
        None,
    );
    for snap in snapshots {
        coordinator.cluster_state.restore(snap);
    }
    // Live gossip buffered during the bootstrap window (pre-stream it was
    // warn-dropped, losing one-shot facts until anti-entropy healed them).
    // Applied AFTER the restores: by the CRDT join's commutativity +
    // idempotence this reaches exactly the state concurrent application
    // would, so no fact observed during the window is lost.
    for frame in live_gossip {
        if let DistributedMessage::ClusterMutation { mutations, .. } = frame {
            for mutation in mutations {
                coordinator.cluster_state.apply(mutation);
            }
        }
    }
    coordinator
}

/// Await a panik signal from an optional receiver. A `None` receiver
/// yields a never-completing future so the panik `select!` arm stays
/// structurally present but never fires (the panik-disabled mode). A
/// dropped sender (watcher aborted) also never completes (the arm goes
/// inert), which is the correct "no panik" behaviour.
async fn recv_panik(rx: &mut Option<oneshot::Receiver<PanikSignal>>) -> PanikSignal {
    match rx {
        Some(r) => match r.await {
            Ok(signal) => signal,
            Err(_) => {
                // Sender dropped (watcher gone): the arm must go inert, not
                // fire. Take the receiver so it is not polled again, then
                // pend forever.
                rx.take();
                std::future::pending().await
            }
        },
        None => std::future::pending().await,
    }
}

/// Wall-clock timestamp for outbound frames (digest / snapshot request /
/// self-departure). Seconds since the UNIX epoch, the same `f64` shape
/// every other wire frame carries.
fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests;
