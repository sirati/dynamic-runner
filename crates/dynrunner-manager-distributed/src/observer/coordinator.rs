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
    ClusterMutation, Destination, DistributedMessage, KeepaliveRole, PeerId, RemovalCause,
    SendTarget, StateDigest, resolve_destination,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;

use crate::anti_entropy::{self, RequesterIdentity};
use crate::cluster_state::ClusterState;
use crate::observer::announcer::{AnnouncerOutboxItem, PeerMeshAnnouncerSender};
use crate::observer::failure_response::{ErrorAggregationPolicy, InvalidTaskMonitorPolicy};
use crate::observer::fleet_death::{FleetDeathDetector, FleetDeathVerdict};
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
use crate::run_narrator::RunNarrator;
use crate::task_completed::{
    TaskCompletedEvent, TaskCompletedListener, run_collector, run_task_completed_dispatcher,
    windowed_failure_collector,
};
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
    /// AE-3 recovery-cadence state: the LAST-SEEN [`StateDigest`] of each
    /// peer that has broadcast one, keyed by sender-id. The timer-driven
    /// recovery arm intersects this with the live roster (`current_primary`
    /// ∪ alive secondaries) and asks [`anti_entropy::plan_recovery_pull`]
    /// whether the replica is still behind any of them — the C9 quiesce
    /// signal. Updated on every inbound `StateDigest` frame.
    peer_digests: std::collections::HashMap<String, StateDigest>,
    /// AE-3 responder rotation cursor (the different-responder-on-malformed
    /// rotation). Advanced by [`anti_entropy::plan_recovery_pull`] on each
    /// tick that has a candidate, so a malformed-snapshot responder is not
    /// retried on the immediately-following tick.
    recovery_cursor: usize,
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
        } = handoff;
        // Install a FRESH task-completed channel on the moved-in
        // `cluster_state`, REPLACING the inherited primary sender (see the
        // reconciliation note above). The receiver feeds the observer's own
        // dispatcher, which `run` spawns with the Policy B/D listeners.
        let (task_tx, task_rx) = mpsc::unbounded_channel::<TaskCompletedEvent>();
        cluster_state.install_task_completed_sender(task_tx);
        // Attach the resource-holdings announcer's role-change hook on the
        // moved-in (already-converged) ledger. The relocation path does no
        // post-attach restore, so no initial trigger is dropped here; the
        // hook still fires on every later `PrimaryChanged`. Attaching at
        // construction (vs. in `run`) keeps the announcer wiring uniform
        // across both constructors — `run` only ever spawns from the stored
        // handle.
        let announcer_handle =
            attach_observer_announcer(&mut cluster_state, holdings, node_id.clone());
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
            peer_digests: std::collections::HashMap::new(),
            recovery_cursor: 0,
            ae_digest_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            ae_recovery_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            wake_note: WakeNoteSlot::default(),
            reconnector,
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
            peer_digests: std::collections::HashMap::new(),
            recovery_cursor: 0,
            ae_digest_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            ae_recovery_warn: WarnThrottle::new(AE_FAULT_WARN_INTERVAL),
            wake_note: WakeNoteSlot::default(),
            reconnector,
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

    /// Read-only access to the replicated ledger (tests / result getters).
    pub fn cluster_state(&self) -> &ClusterState<I> {
        &self.cluster_state
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
    async fn send_to(
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
        // They differ ONLY for a remote `Destination::Primary`: it is id-less,
        // so the mesh cannot route it by host — `Mesh::dispatch`'s Primary arm
        // can only `deliver_local`, and with no local primary slot it returns
        // the C3-seam `Err` and the frame is DROPPED. So mirror the secondary's
        // `(Primary, Peer(id)) → Secondary(id)` collapse (`resource.rs:119`):
        // ROUTE under the resolved host's id so the mesh delivers it over the
        // wire, but STAMP `Destination::Primary` so the receiver's pump demuxes
        // to its primary slot.
        let send_target: Destination = match (&dst, &target) {
            (Destination::Primary, SendTarget::Peer(id)) => Destination::Secondary(id.clone()),
            (_, SendTarget::Peer(_) | SendTarget::Broadcast) => dst.clone(),
            // The observer is never the primary, so a self-resolved
            // Destination::Primary is a logic-impossible case; drop it
            // best-effort rather than self-addressing.
            (_, SendTarget::Loopback) => {
                tracing::debug!(
                    "observer send resolved to self (impossible for a zero-authority \
                     observer); dropping"
                );
                return Ok(());
            }
        };
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
        let dispatcher_task = self.task_completed_rx.take().map(|task_rx| {
            let listeners: Vec<Box<dyn TaskCompletedListener>> =
                vec![invalid_task_listener, aggregation_listener];
            tokio::task::spawn_local(run_task_completed_dispatcher(task_rx, listeners))
        });

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

        // Bootstrap recovery REQUEST half (§6): fire one snapshot request
        // to `Destination::Primary` at entry, gated on a known primary,
        // best-effort. The REPLY half is folded into the loop's recv arm.
        if self.cluster_state.current_primary().is_some() {
            let req = DistributedMessage::RequestClusterSnapshot {
                target: None,
                sender_id: self.config.node_id.clone(),
                timestamp: timestamp_now(),
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
            // Registration failure (exotic runtimes) degrades to a parked
            // arm — the embedding driver can still call
            // `request_graceful_abort` directly.
            let mut graceful_abort_signal =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())
                    .map_err(|e| {
                        tracing::warn!(
                            error = %e,
                            "SIGUSR2 graceful-abort trigger could not be \
                             registered; the signal channel is disabled for \
                             this observer"
                        );
                    })
                    .ok();

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
                        // mark is "continuously down for 5 minutes".
                        if matches!(
                            self.current_visibility(primary_last_seen),
                            Visibility::Lost { .. }
                        ) {
                            visibility_reporter.on_wake_deadline(tokio::time::Instant::now());
                        }
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
                    // the arm for the rest of the run — never a hot-loop.
                    // Cancel-safety: `Signal::recv` is cancel-safe (tokio
                    // docs); a sibling arm winning drops and rebuilds the
                    // recv future without losing a queued signal.
                    sig = async {
                        match graceful_abort_signal.as_mut() {
                            Some(stream) => stream.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        match sig {
                            Some(()) => self.request_graceful_abort().await,
                            None => graceful_abort_signal = None,
                        }
                    }
                    // Announcer outbox drain: the announcer task posts a
                    // ready send; this arm owns the transport `&mut self`.
                    Some(item) = announcer_outbox_rx.recv() => {
                        let outcome = self.send_to(Destination::Primary, item.msg).await;
                        let _ = item.reply.send(outcome);
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
        if self.cluster_state.run_complete() {
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
        let mut roster: HashSet<String> = self
            .cluster_state
            .alive_secondary_members()
            .map(str::to_owned)
            .collect();
        if let Some(p) = self.cluster_state.current_primary() {
            roster.insert(p.to_owned());
        }
        if roster.is_empty() {
            tracing::debug!(
                "observer reconnect due but no known roster yet; nothing to rebuild this tick"
            );
            return;
        }
        let peer_ids: Vec<String> = roster.into_iter().collect();
        tracing::info!(
            peers = peer_ids.len(),
            "observer triggering tunnel rebuild for lost roster (BUG-B reconnect)"
        );
        tokio::task::spawn_local(async move {
            reconnector.reconnect(&peer_ids).await;
        });
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
            DistributedMessage::ClusterSnapshot { snapshot_json, .. } => {
                self.on_cluster_snapshot(&snapshot_json, primary_last_seen);
            }
            DistributedMessage::StateDigest {
                sender_id, digest, ..
            } => {
                self.on_state_digest(&sender_id, &digest).await;
            }
            DistributedMessage::RequestClusterSnapshot {
                sender_id,
                is_observer,
                ..
            } => {
                self.answer_snapshot_request(&sender_id, is_observer).await;
            }
            // Every other frame is irrelevant to a zero-authority
            // observer (it owns no dispatch / setup / election concern).
            // Silently ignored — the observer only consumes the
            // replication / liveness frames above.
            _ => {}
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
    /// (§6) with the BUG-5 liveness refresh. Decode + `restore()`
    /// (idempotent lattice) and refresh `primary_last_seen` if the snapshot
    /// re-points `current_primary()` (a snapshot that newly names a primary
    /// is a liveness assertion). A decode failure is WARN-and-ignore in
    /// steady state — NOT fatal.
    fn on_cluster_snapshot(&mut self, snapshot_json: &str, primary_last_seen: &mut Instant) {
        match serde_json::from_str(snapshot_json) {
            Ok(snap) => {
                let before = self.cluster_state.current_primary().map(str::to_owned);
                self.cluster_state.restore(snap);
                let after = self.cluster_state.current_primary().map(str::to_owned);
                if after.is_some() && after != before {
                    *primary_last_seen = Instant::now();
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "observer: failed to decode ClusterSnapshot frame; ignoring (the next \
                     live broadcast / anti-entropy pull heals)"
                );
            }
        }
    }

    /// Anti-entropy receive side (item 3): compare the peer's digest
    /// against ours and pull a snapshot if behind. Uses the role-agnostic
    /// `reconcile_against_peer` as-is; the observer owns only the `send_to`
    /// edge.
    async fn on_state_digest(&mut self, sender_id: &str, peer: &StateDigest) {
        // Record this peer's last-seen digest for the AE-3 recovery cadence
        // (the C9 quiesce signal: the timer arm pulls iff still behind one
        // of these). Reactive single-frame reconciliation still runs below.
        self.peer_digests.insert(sender_id.to_string(), *peer);
        let local = self.cluster_state.digest();
        let requester = RequesterIdentity {
            node_id: &self.config.node_id,
            is_observer: true,
            can_be_primary: false,
        };
        let Some((dst, req)) = anti_entropy::reconcile_against_peer::<I>(
            &local,
            peer,
            sender_id,
            &requester,
            timestamp_now(),
        ) else {
            // Converged on this peer's digest — nothing to pull.
            return;
        };
        if let Err(e) = self.send_to(dst, req).await {
            tracing::warn!(
                error = %e,
                "observer anti-entropy snapshot pull failed; will retry on the next \
                 digest divergence"
            );
        }
    }

    /// Serve a peer's anti-entropy snapshot pull from this observer's
    /// converged mirror.
    ///
    /// READ-ONLY gossip, in-contract for a zero-authority observer: the
    /// reply is a serialization of the mirror it already holds — no
    /// mutation is originated (unlike the secondary's responder, NO
    /// `PeerJoined` is emitted; membership recording stays a compute-role
    /// concern) and nothing is re-broadcast. Serving is LOAD-BEARING for
    /// the relocation handoff: after the submitter relocates, this
    /// observer can be the ONLY replica holding the `PrimaryChanged` fact
    /// (the live broadcast is one-shot), and its own digest broadcasts
    /// (`on_anti_entropy_tick`) are what prove behind peers divergent — a
    /// mute responder would advertise data nobody can pull (the
    /// run_20260610_185621 leaderless wedge).
    ///
    /// The reply destination is typed off the requester's self-declared
    /// role (the same `is_observer` field the snapshot responders record):
    /// `Observer(id)` for an observer requester, `Secondary(id)` otherwise.
    async fn answer_snapshot_request(&mut self, requester: &str, requester_is_observer: bool) {
        // Serialize-once per state generation (#367): the cache inside
        // `ClusterState` keys the reply bytes on the anti-entropy
        // digest, so a burst of pulls against an unchanged ledger does
        // not re-serialize ~100 MB per request.
        let snapshot_json = match self.cluster_state.snapshot_json() {
            Ok(json) => json,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "observer: snapshot serialization failed; cannot answer the pull"
                );
                return;
            }
        };
        let reply = DistributedMessage::ClusterSnapshot {
            target: None,
            sender_id: self.config.node_id.clone(),
            timestamp: timestamp_now(),
            snapshot_json: (*snapshot_json).clone(),
        };
        let dst = if requester_is_observer {
            Destination::Observer(PeerId::from(requester.to_string()))
        } else {
            Destination::Secondary(PeerId::from(requester.to_string()))
        };
        if let Err(e) = self.send_to(dst, reply).await {
            tracing::warn!(
                requester = %requester,
                error = %e,
                "observer: failed to deliver ClusterSnapshot answer; the \
                 requester's next digest round retries"
            );
        }
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
        let msg =
            anti_entropy::digest_broadcast::<I>(&self.config.node_id, timestamp_now(), digest);
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

    /// AE-3 recovery-cadence tick (D-C / C9): the TIMER-driven snapshot
    /// recovery, distinct from the digest broadcast above. With ZERO
    /// inbound traffic, re-pull a fresh snapshot from a rotating known peer
    /// IFF the local replica is still behind that peer's last-seen digest.
    /// This is what makes a WARN-dropped steady-state decode (the
    /// non-latching `on_cluster_snapshot` arm) recover: the next tick re-
    /// pulls until convergence. The role owns only the `send_to` edge +
    /// the peer-digest store + the roster intersection; the convergence
    /// detection, the C9 quiesce, and the responder rotation live in
    /// [`anti_entropy::plan_recovery_pull`].
    async fn on_recovery_tick(&mut self) {
        // Known-peer roster = current_primary ∪ alive secondaries. Intersect
        // it with the peers that have actually broadcast a digest, so the
        // recovery arm only ever targets a peer we both KNOW and have a
        // last-seen digest for. A digest from a peer no longer in the roster
        // (departed) is excluded — we never resurrect a route to a dead peer.
        let mut roster: HashSet<String> = self
            .cluster_state
            .alive_secondary_members()
            .map(str::to_owned)
            .collect();
        if let Some(p) = self.cluster_state.current_primary() {
            roster.insert(p.to_owned());
        }
        let peer_digests: Vec<(String, StateDigest)> = self
            .peer_digests
            .iter()
            .filter(|(id, _)| roster.contains(id.as_str()))
            .map(|(id, d)| (id.clone(), *d))
            .collect();
        let local = self.cluster_state.digest();
        let requester = RequesterIdentity {
            node_id: &self.config.node_id,
            is_observer: true,
            can_be_primary: false,
        };
        let Some((dst, req)) = anti_entropy::plan_recovery_pull::<I>(
            &local,
            &peer_digests,
            &mut self.recovery_cursor,
            &requester,
            timestamp_now(),
        ) else {
            // C9: converged with every known peer (or none known yet) —
            // quiesce, no pull this tick.
            return;
        };
        // A planned pull over an EMPTY mesh registry can only no-route —
        // name the registry state (rate-limited; same gate as the Err arm
        // below, so one tick emits at most one recovery-fault WARN).
        if self.client.peer_count() == 0
            && let Some(suppressed) = self.ae_recovery_warn.permit()
        {
            tracing::warn!(
                pull_target = ?dst,
                suppressed_since_last_warn = suppressed,
                "AE-3 recovery pull has no peers to reach: the mesh registry \
                 is EMPTY while a known peer's digest proves this replica \
                 behind — recovery cannot engage until the peer (re-)registers"
            );
        }
        if let Err(e) = self.send_to(dst, req).await
            && let Some(suppressed) = self.ae_recovery_warn.permit()
        {
            tracing::warn!(
                error = %e,
                suppressed_since_last_warn = suppressed,
                "observer AE-3 recovery snapshot pull failed; the next recovery \
                 tick rotates to a different responder"
            );
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
