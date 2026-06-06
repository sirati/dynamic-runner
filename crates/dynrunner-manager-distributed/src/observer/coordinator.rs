//! The standalone observer coordinator.
//!
//! # Single concern
//!
//! Own the lifecycle of a ZERO-AUTHORITY observer node: hold the
//! replicated [`ClusterState`] mirror, apply (never originate) every
//! mutation that flows through the mesh, narrate the run for the operator,
//! and exit on the run's terminal — or, when the run is stranded, on one
//! of the strand backstops. The observer is its OWN role/component: it has
//! no scheduler, no worker pool, no dispatch authority, and originates no
//! `PrimaryChanged`. It is NOT a [`crate::SecondaryCoordinator`] in a
//! mode — there is no `is_observer` flag anywhere on this type.
//!
//! # Module boundary
//!
//! Callers (the PyO3 observer dispatcher, the relocation wave that builds
//! an [`ObserverHandoff`]) construct via [`ObserverCoordinator::new`]
//! (cold-join) or [`ObserverCoordinator::from_handoff`] (relocation) and
//! drive the single [`ObserverCoordinator::run`] loop. Everything else —
//! the backstop clocks, the apply-only mirror, the send wrapper, the
//! teardown discipline — is private. The observer COMPOSES the
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
//! # Top-of-loop ordering invariant (load-bearing — see the spec §9)
//!
//! Each iteration, BEFORE awaiting events:
//!   1. narrate (emit any pending phase / summary line),
//!   2. `run_aborted()` ⇒ exit 1 (checked FIRST),
//!   3. `run_complete()` ⇒ exit 0,
//!   4. transport-closed with peers present ⇒ exit 0,
//!   5. zero peers ⇒ accumulate fleet-dead grace ⇒ `Err` on expiry,
//!   6. named primary silent past `peer_timeout` ⇒ `Err`.
//!
//! This guarantees `run_complete`/`run_aborted` always win over any
//! strand; a closed-transport-with-peers is clean; only
//! zero-peers-no-RunComplete becomes the fleet-dead strand; the
//! primary-silence backstop catches the half-open-peer case the
//! fleet-dead grace cannot see.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use dynrunner_core::{BoundedString, Identifier};
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, KeepaliveRole, RemovalCause, SendTarget,
    StateDigest, resolve_destination,
};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::anti_entropy::{self, RequesterIdentity};
use crate::cluster_state::ClusterState;
use crate::observer::announcer::{AnnouncerOutboxItem, PeerMeshAnnouncerSender};
use crate::observer::failure_response::{ErrorAggregationPolicy, InvalidTaskMonitorPolicy};
use crate::observer::lifecycle::{AnnouncerHandle, attach_observer_announcer};
use crate::observer::reporting::{SharedSnapshotSource, StatsSnapshot, TokioClock, run_reporter};
use crate::observer::run_observer_announcer;
use crate::process::{MeshClient, RoleInbox};
use crate::panik_watcher::{self, PanikSignal, PanikWatcherConfig};
use crate::primary::RunError;
use crate::run_narrator::RunNarrator;
use crate::task_completed::{
    TaskCompletedEvent, TaskCompletedListener, run_collector, run_task_completed_dispatcher,
    windowed_failure_collector,
};

/// Configuration for a standalone observer. Carries only the values the
/// observer's own concerns read: the node identity, the strand-backstop
/// thresholds, the setup-promote deadline + its gate, and the panik
/// trigger inputs. It carries NO scheduler / worker / dispatch fields —
/// an observer has none of those concerns.
#[derive(Debug, Clone)]
pub struct ObserverConfig {
    /// This observer's own peer-id — the bootstrap-RPC return address and
    /// the `local_id` the send edge resolves loopback against (the
    /// observer never loops back, but the resolver still needs it).
    pub node_id: String,
    /// Fleet-dead grace (§1). The window after the LAST peer leaves the
    /// mesh before the observer exits a stranded run. Doubles as the
    /// poll-tick cadence so a fully-silent observer still re-checks.
    pub fleet_dead_timeout: Duration,
    /// Primary-silence threshold (§2). The window a NAMED primary may be
    /// silent (no `Primary` keepalive, no `PrimaryChanged` re-point)
    /// before the observer exits a stranded run.
    pub peer_timeout: Duration,
    /// Setup-promote deadline (§3). The window a setup-defer observer
    /// waits for the promoted secondary to seed the ledger before
    /// exiting via [`RunError::SetupDeadlineExpired`].
    pub setup_promote_deadline: Duration,
    /// Whether this observer is in setup-defer mode. Combined with a
    /// live `cluster_state.task_count() == 0`, this defines
    /// `setup_pending` LIVE (R2): the setup-promote deadline arm is inert
    /// the moment the ledger is seeded.
    pub required_setup_on_promote: bool,
    /// Panik trigger paths (sentinel files). Empty disables the file
    /// trigger.
    pub panik_watcher_paths: Vec<std::path::PathBuf>,
    /// Panik poll cadence.
    pub panik_watcher_poll_interval: Duration,
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
    /// The strand-backstop + setup deadline thresholds.
    pub deadlines: ObserverConfig,
    /// The phases the pre-relocation emitter already announced as started
    /// — the `RunNarrator::with_started_phases` seed so the observer does
    /// not re-announce them but still narrates post-relocation starts.
    pub started_phases: HashSet<dynrunner_core::PhaseId>,
    /// Whether the relocated node was in setup-defer mode. `setup_pending`
    /// is recomputed LIVE (= `required_setup_on_promote && task_count==0`)
    /// at both arm and fire — this is NOT a frozen `setup_pending` bool.
    pub required_setup_on_promote: bool,
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
    /// The setup-deadline elapsed, recorded for the GIL-side tail when the
    /// deadline genuinely expired (mirrors the primary's outcome slot).
    setup_deadline_elapsed: Option<Duration>,
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
        let required_setup = config.required_setup_on_promote;
        Self::with_pieces(
            client,
            inbox,
            cluster_state,
            node_id,
            config,
            HashSet::new(),
            required_setup,
            None,
            HashSet::new(),
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
            required_setup_on_promote,
            panik_signal_rx,
            task_completed_dispatcher_handle,
            lifecycle_dispatcher_handle,
            holdings,
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
                required_setup_on_promote,
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
            setup_deadline_elapsed: None,
            task_completed_rx: Some(task_rx),
            peer_digests: std::collections::HashMap::new(),
            recovery_cursor: 0,
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
        required_setup_on_promote: bool,
        panik_signal_rx: Option<oneshot::Receiver<PanikSignal>>,
        holdings: HashSet<String>,
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
            config: ObserverConfig {
                node_id,
                required_setup_on_promote,
                ..config
            },
            started_phases,
            inherited_task_completed_dispatcher: None,
            lifecycle_dispatcher_handle: None,
            panik_signal_rx,
            announcer_handle: Some(announcer_handle),
            setup_deadline_elapsed: None,
            task_completed_rx: Some(task_rx),
            peer_digests: std::collections::HashMap::new(),
            recovery_cursor: 0,
        }
    }

    /// The setup-deadline elapsed, if the deadline genuinely expired. Read
    /// by the GIL-side tail to surface [`RunError::SetupDeadlineExpired`].
    pub fn setup_deadline_elapsed(&self) -> Option<Duration> {
        self.setup_deadline_elapsed
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

    /// Build the STRUCTURED strand-collapse error for a strand backstop
    /// (fleet-dead / primary-silence): the run loop is exiting with tasks the
    /// CRDT ledger left non-terminal. Typed as [`RunError::ClusterCollapsed`]
    /// (NOT a generic `Other`) so the PyO3 boundary's uniform terminal
    /// mapping RAISES on it — a strand must surface non-zero, never be
    /// log-and-swallowed (the §14/§15 fleet-collapse contract). Carries the
    /// CRDT-converged per-class breakdown + the stranded count (`task_count -
    /// terminal`), the same shape the primary's `ClusterCollapsed` renders.
    /// `detail` names which backstop fired for the operator log.
    fn strand_collapsed(&self, detail: String) -> RunError {
        let outcome = self.cluster_state.outcome_counts();
        let stranded = self
            .cluster_state
            .task_count()
            .saturating_sub(outcome.total_terminal());
        tracing::error!(stranded, %detail, "observer strand — surfacing ClusterCollapsed");
        RunError::ClusterCollapsed { stranded, outcome }
    }

    /// LIVE `setup_pending` predicate (R2): `required_setup_on_promote &&
    /// task_count() == 0`. Recomputed at BOTH the deadline arm and fire so
    /// the moment discovery seeds the ledger (first `TaskAdded`) the arm
    /// goes inert. NEVER a frozen bool.
    fn setup_pending(&self) -> bool {
        self.config.required_setup_on_promote && self.cluster_state.task_count() == 0
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

    /// Drive the observer until the run terminates or a strand backstop
    /// fires. THIN DRIVER: a `select!` loop whose arms delegate to named
    /// per-concern methods + the inline reporter / failure-policy / panik
    /// arms. Returns the run terminal; the strand backstops surface as
    /// `Err`.
    pub async fn run(&mut self) -> Result<ObserverTerminal, RunError>
    where
        Self: 'static,
    {
        // ── Pre-loop wiring (single concern per block) ──

        // Policy B: invalid_task fatal-exit monitor. The signal rides a
        // dedicated unbounded mpsc consumed by the run loop's fatal-exit
        // arm; the policy never calls `std::process::exit`.
        let (fatal_exit_tx, mut fatal_exit_rx) = mpsc::unbounded_channel::<String>();
        let (invalid_task_listener, invalid_task_driver) =
            windowed_failure_collector(InvalidTaskMonitorPolicy::new(fatal_exit_tx));
        // Policy D: rolling error aggregation (importance-channel emit,
        // never exits).
        let (aggregation_listener, aggregation_driver) =
            windowed_failure_collector(ErrorAggregationPolicy::new());

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
        let invalid_driver_task = tokio::task::spawn_local(run_collector(
            invalid_task_driver,
            async move {
                let _ = invalid_cancel_rx.await;
            },
        ));
        let (aggregation_cancel_tx, aggregation_cancel_rx) = oneshot::channel::<()>();
        let aggregation_driver_task = tokio::task::spawn_local(run_collector(
            aggregation_driver,
            async move {
                let _ = aggregation_cancel_rx.await;
            },
        ));

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
        let reporter_task = tokio::task::spawn_local(run_reporter(
            snapshot_source,
            TokioClock,
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
                    "observer bootstrap snapshot request failed; relying on backstops + \
                     anti-entropy / late snapshot heal"
                );
            }
        }

        // Capture the loop result BEFORE the single-teardown so an
        // `Err`/`?`-propagated early return still routes through cleanup
        // (cp the late-joiner inner-async-block idiom — NO
        // cleanup-before-each-return).
        let loop_result: Result<ObserverTerminal, RunError> = async {
            // ── Backstop clocks ──
            let mut narrator = RunNarrator::with_started_phases(std::mem::take(&mut self.started_phases));
            let mut fleet_dead_since: Option<Instant> = None;
            let mut primary_last_seen = Instant::now();
            let mut transport_closed = false;

            // Fleet-dead poll tick (§1): same cadence as the grace; the
            // immediate tick (fires at t=0) is consumed so the cadence
            // starts one full interval out and the tick carries no work —
            // it only re-drives the loop so the top-of-loop check
            // re-evaluates.
            let mut fleet_dead_tick = tokio::time::interval(self.config.fleet_dead_timeout);
            fleet_dead_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = fleet_dead_tick.tick().await;

            // Anti-entropy tick (item 3): per-node-jittered cadence; Skip +
            // immediate-tick-consume so a converged mesh's digest traffic
            // starts one period out.
            let mut ae_tick = tokio::time::interval(anti_entropy::tick_period(&self.config.node_id));
            ae_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = ae_tick.tick().await;

            // AE-3 snapshot-recovery tick (D-C / C9): an INDEPENDENT timer,
            // distinct from the digest broadcast above, on the SAME proven
            // per-node-jittered period (bounded by `peer_timeout` so a
            // configured period never exceeds the strand window). Each tick
            // re-pulls a snapshot from a rotating known peer iff still behind
            // its last-seen digest — this is what re-converges a WARN-dropped
            // steady-state decode. Skip + immediate-tick-consume mirrors the
            // digest cadence so the first recovery probe is one period out.
            let recovery_period = anti_entropy::tick_period(&self.config.node_id)
                .min(self.config.peer_timeout);
            let mut recovery_tick = tokio::time::interval(recovery_period);
            recovery_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let _ = recovery_tick.tick().await;

            // Setup-promote deadline (§3): anchored at loop entry. A
            // one-shot consumed latch ensures the fire path runs at most
            // once. The arm is LIVE-gated on `setup_pending()` so it goes
            // inert the moment the ledger is seeded.
            let setup_loop_start = Instant::now();
            let setup_deadline_at = setup_loop_start + self.config.setup_promote_deadline;
            let mut setup_deadline_consumed = false;

            // Panik receiver (BUG-4): consumed by a live arm. `None` →
            // a never-firing arm.
            let mut panik_rx = self.panik_signal_rx.take();

            loop {
                // 1. Narrate (item 9/14): emit pending phase / summary
                //    BEFORE the terminal early-returns so the completing
                //    iteration emits the summary first.
                narrator.observe(&self.cluster_state);
                // Keep the reporter's cell fresh with the live projection.
                snapshot_publisher.publish(StatsSnapshot::from_cluster_state(&self.cluster_state));

                // 2. Terminal backstop block (top-of-loop). Ordering is
                //    load-bearing — see the module + spec §9.
                if let Some(terminal) = self.evaluate_exit(
                    transport_closed,
                    &mut fleet_dead_since,
                    primary_last_seen,
                )? {
                    return Ok(terminal);
                }

                // 3. Await events.
                tokio::select! {
                    // Inbound mesh frame — the mesh-pump has already demuxed
                    // it to the observer's slot, so this inbox carries only
                    // frames addressed to the observer.
                    maybe = self.inbox.recv() => {
                        match maybe {
                            Some(msg) => {
                                self.on_inbound(msg, &mut primary_last_seen).await;
                            }
                            None => {
                                // Latch + disable this arm; the exit
                                // decision is made at top-of-loop (§4). A
                                // `None` is the role's teardown signal (every
                                // write end of the slot's inbound dropped).
                                transport_closed = true;
                            }
                        }
                    }
                    // Fleet-dead poll tick (§1): no work — just re-drives.
                    _ = fleet_dead_tick.tick() => {}
                    // Anti-entropy tick (item 3): broadcast our digest.
                    _ = ae_tick.tick() => {
                        self.on_anti_entropy_tick().await;
                    }
                    // AE-3 recovery tick (D-C / C9): re-pull from a rotating
                    // known peer iff still behind its last-seen digest.
                    _ = recovery_tick.tick() => {
                        self.on_recovery_tick().await;
                    }
                    // Setup-promote deadline (§3): LIVE-recompute at fire.
                    _ = tokio::time::sleep_until(setup_deadline_at.into()),
                        if self.setup_pending() && !setup_deadline_consumed =>
                    {
                        setup_deadline_consumed = true;
                        if self.setup_pending() {
                            let elapsed = setup_loop_start.elapsed();
                            self.setup_deadline_elapsed = Some(elapsed);
                            return Err(RunError::SetupDeadlineExpired { elapsed });
                        }
                        // A TaskAdded landed in the same tick — the gate is
                        // now inert; fall back into the loop.
                    }
                    // Panik arm (BUG-4): consumed. On fire, announce
                    // self-departure + return the Panik terminal.
                    signal = recv_panik(&mut panik_rx) => {
                        return Ok(self.on_panik(signal).await);
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
                        return Err(RunError::FatalPolicyExit {
                            reason: format!("invalid_task monitor — {reason}"),
                        });
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

    /// Top-of-loop terminal backstop block (the single exit decision
    /// point). Ordering is load-bearing (spec §9). Returns `Some(terminal)`
    /// for a clean exit, `Err` for a strand, `None` to keep looping.
    fn evaluate_exit(
        &self,
        transport_closed: bool,
        fleet_dead_since: &mut Option<Instant>,
        primary_last_seen: Instant,
    ) -> Result<Option<ObserverTerminal>, RunError> {
        // 2. Aborted FIRST (§5 / BUG-1): never narrate/exit as completed.
        if let Some(reason) = self.cluster_state.run_aborted() {
            return Ok(Some(ObserverTerminal::Aborted {
                reason: reason.to_string(),
            }));
        }
        // 3. Complete → clean exit 0.
        if self.cluster_state.run_complete() {
            return Ok(Some(ObserverTerminal::Done));
        }
        // 4. Closed transport with peers present → clean exit 0 (§4-int).
        //    Read off the mesh client's pump-published `MembershipView`
        //    (≤1-cycle stale, monotone-toward-truth — it republishes the
        //    whole live set each pump cycle, so it can never MISS a remove).
        //    This is a clean-EXIT permit, NEVER a death trigger: a
        //    stale-HIGH count only delays a clean exit by one cycle, and a
        //    stale-LOW count falls through to the timeout-gated grace at
        //    step 5 (which a ≤1-cycle blip cannot drive to fire — the count
        //    corrects within one cycle and clears the grace at line below).
        //    So no strand decision keys on a live-vs-stale distinction.
        let peer_count = self.client.peer_count();
        if transport_closed && peer_count > 0 {
            return Ok(Some(ObserverTerminal::Done));
        }
        // 5. Fleet-dead grace (§1). Capture-once on first emptiness; clear
        //    on any non-empty observation (a partial recovery resets the
        //    grace). Fires `Err` when the grace elapses. This also OWNS the
        //    closed-transport-with-zero-peers strand (§4-int): a closed
        //    transport does not short-circuit to Ok here — it falls through.
        if peer_count == 0 {
            let since = *fleet_dead_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= self.config.fleet_dead_timeout {
                return Err(self.strand_collapsed(format!(
                    "fleet-dead: every peer left the mesh and no RunComplete was \
                     broadcast within {:.1}s",
                    self.config.fleet_dead_timeout.as_secs_f64()
                )));
            }
        } else {
            *fleet_dead_since = None;
        }
        // 6. Primary-silence backstop (§2): only when a primary is NAMED
        //    and it has been silent past `peer_timeout` with no RunComplete.
        //    Independent of `peer_count()` (un-pruned for an apply-only
        //    observer that never sends).
        if let Some(primary) = self.cluster_state.current_primary()
            && primary_last_seen.elapsed() > self.config.peer_timeout
            && !self.cluster_state.run_complete()
        {
            let detail = format!(
                "primary-silence: current primary {primary} silent for {:.0}s with no \
                 RunComplete",
                primary_last_seen.elapsed().as_secs_f64()
            );
            return Err(self.strand_collapsed(detail));
        }
        Ok(None)
    }

    /// Dispatch one inbound mesh frame. Apply-only: the observer mirrors
    /// CRDT mutations, refreshes primary-liveness on recognised signals,
    /// heals from snapshots, and reconciles digests. It NEVER originates a
    /// mutation or re-broadcasts.
    async fn on_inbound(
        &mut self,
        msg: DistributedMessage<I>,
        primary_last_seen: &mut Instant,
    ) {
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

    /// Anti-entropy tick (item 3): broadcast our digest to the mesh.
    async fn on_anti_entropy_tick(&mut self) {
        let digest = self.cluster_state.digest();
        let msg = anti_entropy::digest_broadcast::<I>(&self.config.node_id, timestamp_now(), digest);
        if let Err(e) = self.send_to(Destination::All, msg).await {
            tracing::debug!(
                error = %e,
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
        if let Err(e) = self.send_to(dst, req).await {
            tracing::warn!(
                error = %e,
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
        let is_sigterm = panik_watcher::is_sigterm_signal(&matched_path);
        let reason = if is_sigterm {
            "panik SIGTERM (per-host)".to_string()
        } else {
            format!("panik file: {}", matched_path.display())
        };
        tracing::error!(
            matched_path = %matched_path.display(),
            reason = %reason,
            "observer panik signal observed; announcing self-departure and exiting 137"
        );
        // Self-authored departure: apply locally + broadcast. Peers LOG it
        // and mark this node Dead. It does NOT cancel cluster work.
        let mutation = ClusterMutation::<I>::PeerRemoved {
            id: self.config.node_id.clone(),
            cause: RemovalCause::SelfDeparture(BoundedString::from(reason)),
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

    let required_setup = config.required_setup_on_promote;
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
        required_setup,
        panik_signal_rx,
        holdings,
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
