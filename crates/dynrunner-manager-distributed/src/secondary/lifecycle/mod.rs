//! Typed secondary lifecycle state machine (skeleton — UNWIRED).
//!
//! This module owns the [`SecondaryLifecycle`] type: the explicit,
//! type-level encoding of a secondary's progression from "just a peer on
//! the mesh, no primary contact yet" to "operational task-runner" to a
//! terminal outcome. It exists to replace the ~15 scattered bools/Options
//! on [`super::SecondaryCoordinator`] with one state value whose variants
//! make the system's capability invariants *unrepresentable to violate*.
//!
//! # Status: defined but not yet wired
//!
//! Every type here is currently `#[allow(dead_code)]`. This leaf only
//! introduces the shapes; a later leaf moves the coordinator's flat
//! fields into these states and routes handlers through the enum. Until
//! then nothing constructs or reads a `SecondaryLifecycle` value, so the
//! behaviour of the running coordinator is byte-for-byte unchanged.
//!
//! # Capability invariants (the WHY — enforced by construction once wired)
//!
//! The states form a forward progression `Connecting → AwaitingPrimary →
//! Configuring → Operational`, with five terminal absorbing states. The
//! invariant each state encodes:
//!
//! - **`AwaitingPrimary` cannot spawn workers and cannot accept a
//!   `TaskAssignment`** — by construction, in a later wiring leaf: the
//!   worker pool lives *only* inside [`ConfiguringState`]/[`OperationalState`],
//!   so there is no pool to spawn into before `Configuring`, and the
//!   `TaskAssignment` handler arm (written against `&mut ConfiguringState`
//!   / `&mut OperationalState`) is simply unreachable while the lifecycle
//!   is `AwaitingPrimary`. No runtime `if not configured { reject }`
//!   guard is needed — the type makes the bad call uncompilable.
//! - **Workers spawn on the `AwaitingPrimary → Configuring` transition.**
//!   `initialize_workers` runs as the entry action of `Configuring` (after
//!   the primary has announced itself, before the InitialAssignment
//!   dispatch). If the primary never announces, the lifecycle never leaves
//!   `AwaitingPrimary` and no worker pool is ever built.
//! - **Election and keepalive live ONLY in `Operational`.** The
//!   [`super::election::ElectionState`] sub-machine and the
//!   primary-liveness keepalive are fields of [`OperationalState`], so a
//!   `run_election_tick` / `send_keepalive` written as `impl
//!   OperationalState` cannot fire before the lifecycle reaches
//!   `Operational`. A `Configuring` secondary advancing past the short
//!   election deadline therefore stays election-quiet by construction.
//! - **Two timeout horizons, owned by the states they govern.** The long
//!   `unconfigured_deadline` governs the pre-`Operational` span
//!   (`AwaitingPrimary` + `Configuring`); the short election deadline
//!   (`keepalive_interval × keepalive_miss_threshold`) is computed *only*
//!   inside [`OperationalState`], so it physically cannot fire while the
//!   secondary is still unconfigured.
//!
//! # Mesh connectivity is orthogonal (see [`MeshFormation`])
//!
//! Forming the peer mesh is NOT one of the config states above and is NOT
//! gated behind configuration. It is a sibling sub-concern carried across
//! every non-terminal state — modelled exactly the way
//! [`super::election::ElectionState`] is modelled *within* `Operational`,
//! but at the outer level so it spans `Connecting → Operational`. See
//! [`MeshFormation`] for the full rationale.
//!
//! # Generic parameters
//!
//! `SecondaryLifecycle<M, I>` mirrors the two generics the carried state
//! data genuinely needs:
//!
//! - `M`: the [`ManagerEndpoint`] the worker pool talks to — required
//!   because the carried `pool` is a real
//!   [`WorkerPool<M, I>`](dynrunner_manager_local::pool::WorkerPool), not a
//!   placeholder.
//! - `I`: the cluster [`Identifier`] — required by `WorkerPool<M, I>`,
//!   `PendingFirstBind<I>`, and the queued `DistributedMessage<I>`.
//!
//! The plan's migration sketch wrote a single `<I>`; the binding rule is
//! "mirror the real field types, never invent a placeholder where a real
//! type exists", and the real `pool` type forces `M` in as well, so the
//! machine is parameterized over both. The coordinator's other two
//! generics (`Tr` transport, `S` scheduler, `E` estimator) are NOT needed:
//! none of the fields migrated into a state is typed over them.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::{Identifier, WorkerId};
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;

use super::ClusterStateRefreshFn;
use super::PendingFirstBind;
use super::election::{ElectionState, ElectionTickActions};
use super::primary_link::PrimaryLink;

/// The typed secondary lifecycle.
///
/// One value of this type replaces the coordinator's scattered
/// configuration latches (`setup_phase_completed`, `transfer_complete`,
/// `pre_staged_mode`, the `Option<Receiver>` take-once gates, `fatal_exit`,
/// …). The forward span is `Connecting → AwaitingPrimary → Configuring →
/// Operational`; `Operational` is resumable across a `SetupPending`
/// excursion (the caller re-enters and finds the lifecycle already
/// `Operational`, preserving the fire-once consumption of the take-once
/// channels). The five terminal variants are absorbing.
///
/// **Observer is a role, not a state.** A pure observer / late-joiner
/// constructs [`Operational`](SecondaryLifecycle::Operational) directly
/// with an empty pool (replacing the old
/// `restore_from_snapshot_and_skip_setup` bool poke); its non-candidacy is
/// a capability gate on the role, not a parallel lifecycle variant.
///
/// See the module-level docs for the capability invariants each variant
/// encodes (no worker-spawn / no `TaskAssignment` before `Configuring`;
/// election + keepalive only in `Operational`; the long vs short timeout
/// horizons).
#[allow(dead_code)]
pub(in crate::secondary) enum SecondaryLifecycle<M: ManagerEndpoint, I: Identifier> {
    /// Just starting: no primary peer known and no `PrimaryChanged`
    /// applied yet. The peer mesh IS being established here (see
    /// [`MeshFormation`], carried alongside this variant in the wired
    /// coordinator) — only worker-spawn, task-acceptance, election, and
    /// keepalive are unavailable.
    Connecting {
        /// When this secondary entered the lifecycle. Governs the long
        /// `unconfigured_deadline` (a property of the pre-`Operational`
        /// span), never the short election deadline.
        since: Instant,
    },

    /// Mesh-joining as far as it can, handshake in flight, but no primary
    /// has announced itself yet. **Cannot spawn workers, cannot accept a
    /// `TaskAssignment`, runs no election, sends no keepalive** — there is
    /// no pool in this variant and no Operational state data to write to.
    /// Bounded by the long `unconfigured_deadline`.
    AwaitingPrimary {
        /// Entry instant; subject to `unconfigured_deadline`.
        since: Instant,
        /// Whether the setup handshake (`send_welcome` /
        /// `send_cert_exchange`) has been emitted to the (now-known) mesh
        /// primary peer. A one-shot guard so re-entry does not re-send.
        handshake_sent: bool,
    },

    /// The primary has announced itself. Workers are spawned on entry to
    /// this state (before the InitialAssignment dispatch); the secondary
    /// is receiving `PeerInfo` / `InitialAssignment` / `TransferComplete`.
    /// Still pre-`Operational`, so the long `unconfigured_deadline` (not
    /// the short election deadline) governs, and election/keepalive remain
    /// unavailable.
    ///
    /// Boxed: the heavy state-data variants are kept behind an indirection
    /// so they do not inflate the size of the cheap variants
    /// (`Connecting`/`AwaitingPrimary`/terminal) the lifecycle spends most
    /// of its life as.
    Configuring(Box<ConfiguringState<M, I>>),

    /// Fully configured and running tasks. Resumable across a
    /// `SetupPending` yield/resume. Carries the worker pool, the nested
    /// [`ElectionState`] sub-machine, primary-liveness tracking, peer
    /// keepalives, the primary link, and the pending/active task
    /// collections. The short election deadline is computed from inside
    /// this state's data and so cannot fire earlier.
    ///
    /// Boxed for the same reason as [`Configuring`](Self::Configuring): the
    /// largest state-data variant is kept behind an indirection.
    Operational(Box<OperationalState<M, I>>),

    /// Terminal: this node won (or was named in) a promotion and is now
    /// the primary; the co-located parked `PrimaryCoordinator` has been
    /// activated. (`promote_activation_tx` is consumed at the
    /// `Operational → Promoted` transition.)
    Promoted,

    /// Terminal: the run reached a normal completion (RunComplete observed
    /// / clean drain-down). Maps the old `RunOutcome::Done`.
    Done,

    /// Terminal: the replicated ledger recorded `RunAborted`. Maps the old
    /// `RunOutcome::Aborted`.
    Aborted,

    /// Terminal: the panik watcher fired (sentinel file / SIGTERM); workers
    /// have been hard-killed. Maps the old `RunOutcome::PanikShutdown`.
    Panik,

    /// Terminal: an unrecoverable local fault was latched (the read of the
    /// old `fatal_exit` write-latch transitions here). The run exits
    /// non-zero.
    Failed,
}

/// State data for [`SecondaryLifecycle::Configuring`].
///
/// Carries the worker pool (spawned on entry to this state) plus the
/// setup-discovery flags the configuration phase reads. The pre-staged /
/// file-based / discovery-done flags are *carried forward* into
/// [`OperationalState`] when configuration completes (they are
/// "Configuring → Operational data" in the plan's migration map), so the
/// resolver and the `SetupPending` discriminator keep their values across
/// the `enter_operational()` boundary.
#[allow(dead_code)]
pub(in crate::secondary) struct ConfiguringState<M: ManagerEndpoint, I: Identifier> {
    /// The local worker pool, built by `initialize_workers` on entry to
    /// this state. Real [`WorkerPool<M, I>`] — there is no pool in any
    /// earlier state, which is what makes a pre-`Configuring` worker-spawn
    /// unrepresentable.
    pub(in crate::secondary) pool: WorkerPool<M, I>,

    /// Mirrors the coordinator's `transfer_complete` latch — set when the
    /// primary's `TransferComplete` arrives. One of the "got_*" config
    /// signals that gate the `Configuring → Operational` transition.
    pub(in crate::secondary) transfer_complete: bool,

    /// Pre-staged source mode, from `InitialAssignment.pre_staged_mode`.
    /// Carried forward into [`OperationalState`] (it feeds the
    /// `SetupPending` discriminator and the dispatch-resolver hash choice).
    pub(in crate::secondary) pre_staged_mode: bool,

    /// Whether dispatched items are real files, from
    /// `InitialAssignment.uses_file_based_items`. Carried forward into
    /// [`OperationalState`].
    pub(in crate::secondary) uses_file_based_items: bool,

    /// One-shot latch for the setup-discovery `SetupPending` yield.
    /// Carried forward into [`OperationalState`] so the yield fires at most
    /// once per node across re-entry.
    pub(in crate::secondary) setup_discovery_done: bool,
}

/// State data for [`SecondaryLifecycle::Operational`].
///
/// Carries everything the running task loop owns. The nested
/// [`ElectionState`] is the orthogonal-within-`Operational` sub-concern
/// (election can only run here); the short election deadline is computed
/// from `primary_last_seen` + the keepalive config that lives alongside it,
/// so it cannot fire before this state is reached.
#[allow(dead_code)]
pub(in crate::secondary) struct OperationalState<M: ManagerEndpoint, I: Identifier> {
    /// The local worker pool, carried in from [`ConfiguringState`] (or
    /// constructed empty for a pure observer / late-joiner that lands here
    /// directly).
    pub(in crate::secondary) pool: WorkerPool<M, I>,

    /// The failover-election sub-machine. Orthogonal *within* `Operational`
    /// the way [`MeshFormation`] is orthogonal across all states — it is a
    /// nested concern, not a sibling lifecycle variant. Election ticks are
    /// `impl`-reachable only through this field, so they cannot run
    /// pre-`Operational`.
    pub(in crate::secondary) election: ElectionState,

    /// Last time any message was seen from the primary (F2 failover
    /// detection). `None` until the first primary message. Drives the short
    /// election deadline, which is therefore physically pre-`Operational`-
    /// unreachable.
    pub(in crate::secondary) primary_last_seen: Option<Instant>,

    /// Peer-keepalive tracking: `peer_id -> last_seen` (epoch seconds).
    /// Peer-liveness, distinct from primary-liveness (`primary_last_seen`).
    pub(in crate::secondary) peer_keepalives: HashMap<String, f64>,

    /// Routing target + per-worker request rate limiting for the
    /// secondary→primary link.
    pub(in crate::secondary) primary_link: PrimaryLink,

    /// This node's OWN in-flight worker assignments: `file_hash ->
    /// worker_id`. Own-worker management, not authority.
    pub(in crate::secondary) active_tasks: HashMap<String, WorkerId>,

    /// Deferred peer messages queued from sync handlers, flushed onto the
    /// transport at the top of each loop iteration.
    pub(in crate::secondary) pending_peer_messages: Vec<(String, DistributedMessage<I>)>,

    /// Worker IDs queued for respawn at the next processing tick (broken
    /// pipe observed without a `WorkerEvent::Disconnected`).
    pub(in crate::secondary) pending_worker_restarts: HashSet<WorkerId>,

    /// Tasks deferred because the target worker's per-type subprocess is
    /// mid-respawn (respawn-HOLD, #58). Keyed by `WorkerId`.
    pub(in crate::secondary) pending_first_bind: HashMap<WorkerId, PendingFirstBind<I>>,

    /// Pre-staged source mode, carried forward from [`ConfiguringState`].
    pub(in crate::secondary) pre_staged_mode: bool,

    /// File-based-items flag, carried forward from [`ConfiguringState`].
    pub(in crate::secondary) uses_file_based_items: bool,

    /// Setup-discovery fire-once latch, carried forward from
    /// [`ConfiguringState`].
    pub(in crate::secondary) setup_discovery_done: bool,
}

/// Peer-mesh formation progress — the orthogonal sub-concern.
///
/// **This is NOT a config state and is NOT gated behind setup completion.**
/// Establishing/maintaining the peer mesh is a capability available in
/// every non-terminal lifecycle variant: an unconfigured secondary begins
/// dialing known peers / accepting connections immediately, as far as it
/// can. `MeshFormation` is therefore *carried across* the config FSM —
/// it begins in `Connecting`/`AwaitingPrimary` and continues unchanged
/// into `Configuring`/`Operational`.
///
/// It is modelled as a sibling sub-state of the config machine exactly the
/// way [`super::election::ElectionState`] is modelled *within*
/// `Operational` — a nested concern with its own progress + latch — except
/// `MeshFormation` lives at the *outer* level because mesh connectivity
/// spans `Connecting → Operational`, whereas election is confined to
/// `Operational`. Worker-spawn, task-acceptance, election, and keepalive
/// are the config/`Operational`-gated capabilities; "form/maintain the
/// mesh" is not among them.
#[allow(dead_code)]
pub(in crate::secondary) struct MeshFormation {
    /// One-shot watchdog deadline for "did the peer mesh form?". Set to
    /// `now + watchdog` when the per-peer dials kick off with ≥1 peer;
    /// cleared on the first tick after it passes. `None` means we haven't
    /// reached the dial step, there were no peers (single-secondary), or
    /// the watchdog has already fired.
    pub(in crate::secondary) peer_mesh_check_at: Option<Instant>,

    /// Number of peers the transport was asked to dial. Used to phrase the
    /// watchdog WARN ("0 of N reachable") and to suppress the watchdog
    /// when the peer list is empty.
    pub(in crate::secondary) peer_dial_count: u32,

    /// One-shot guard: has `MeshReady` already been emitted to the primary?
    /// The primary defers `PromotePrimary` until every secondary reports,
    /// so this enforces "exactly once per secondary".
    pub(in crate::secondary) mesh_ready_sent: bool,

    /// The `degraded` latch: set true once the watchdog deadline elapsed
    /// with zero connected peers. A degraded mesh is NOT fatal — task
    /// dispatch over the direct primary link still works; only the
    /// peer-mesh-dependent paths (failover election, peer-keepalive
    /// broadcasts) fail-loud-or-skip on this flag.
    pub(in crate::secondary) degraded: bool,
}

/// The take-once runtime latches surrendered at the single
/// [`SecondaryLifecycle::enter_operational`] boundary.
///
/// These are the `Option<Receiver>` / `Option<callback>` slots the
/// coordinator builds at construction and `take()`s once when it first
/// reaches `process_tasks` (see the matching fields on
/// [`super::SecondaryCoordinator`]: `lifecycle_rx`, `task_completed_rx`,
/// `announcer_outbox_rx`, `panik_signal_rx`, `fatal_exit_signal_rx`,
/// `command_rx`, `on_cluster_state_refresh`). The plan's migration map
/// makes the `Configuring → Operational` transition the ONE place they
/// are consumed: the coordinator fills this carrier by `take()`-ing each
/// `Option`, hands it to `enter_operational`, and gets the unwrapped
/// values back to drive the operational `select!` loop.
///
/// **Fire-once by construction.** A `SetupPending` excursion re-enters
/// `run_until_setup_or_done` and finds the lifecycle already
/// `Operational`, so `enter_operational` is never called twice; and even
/// if it were, the coordinator's `Option::take()` yields `None` on the
/// second pass. Modelling the latches as a move-in / move-out carrier (NOT
/// fields of [`OperationalState`]) keeps them where they belong — local to
/// the operational loop, not part of the resumable state data — exactly as
/// the skeleton scoped [`OperationalState`].
///
/// `promote_activation_tx` is deliberately NOT here: it is consumed at the
/// later `Operational → Promoted` transition (see
/// [`SecondaryLifecycle::enter_promoted`]), not at this boundary.
#[allow(dead_code)]
pub(in crate::secondary) struct OperationalLatches<I: Identifier> {
    /// Peer-lifecycle dispatcher receiver (`PeerJoined`/`PeerRemoved`).
    pub(in crate::secondary) lifecycle_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>>,

    /// Task-completion dispatcher receiver.
    pub(in crate::secondary) task_completed_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::task_completed::TaskCompletedEvent>>,

    /// Observer-announcer outbox receiver. `None` outside an attached
    /// observer wiring.
    pub(in crate::secondary) announcer_outbox_rx:
        Option<tokio::sync::mpsc::Receiver<crate::observer::announcer::AnnouncerOutboxItem<I>>>,

    /// Panik-watcher signal receiver. `None` when no panik paths were set.
    pub(in crate::secondary) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,

    /// Externally-armed fatal-exit signal receiver. `None` when no
    /// run-loop-external policy was attached.
    pub(in crate::secondary) fatal_exit_signal_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<String>>,

    /// PyO3 `PrimaryHandle` command-channel receiver (transferred to the
    /// co-located primary, never drained by the secondary itself).
    pub(in crate::secondary) command_rx:
        Option<tokio::sync::mpsc::Receiver<crate::primary::PrimaryCommand<I>>>,

    /// Periodic cluster-state refresh callback. `None` when no consumer
    /// registered.
    pub(in crate::secondary) on_cluster_state_refresh: Option<ClusterStateRefreshFn<I>>,
}

#[allow(dead_code)]
// `M: 'static` mirrors the bound on `WorkerPool::new()` (and on the
// `SecondaryCoordinator` impl): the empty-pool late-joiner construction in
// `operational_observer` calls it, and the carried `WorkerPool<M, I>`
// requires it anyway.
impl<M: ManagerEndpoint + 'static, I: Identifier> SecondaryLifecycle<M, I> {
    /// Construct the initial state. The lifecycle begins as `Connecting`
    /// the moment the coordinator starts; `since` anchors the long
    /// `unconfigured_deadline` that governs the whole pre-`Operational`
    /// span (`Connecting` + `AwaitingPrimary` + `Configuring`).
    pub(in crate::secondary) fn connecting(now: Instant) -> Self {
        SecondaryLifecycle::Connecting { since: now }
    }

    /// `Connecting → AwaitingPrimary`. The peer mesh keeps forming (the
    /// orthogonal [`MeshFormation`] sub-concern is unaffected by this
    /// transition); the secondary is now actively trying to reach a
    /// primary but none has announced itself yet. The pre-config deadline
    /// anchor (`since`) is carried forward unchanged — `AwaitingPrimary`
    /// shares the long `unconfigured_deadline` horizon with `Connecting`,
    /// it does not restart it. `handshake_sent` starts `false`; the
    /// capability to actually emit the welcome/cert-exchange handshake is
    /// gated on this state (see [`Self::mark_handshake_sent`]).
    ///
    /// Returns `self` unchanged if called from any other variant: the
    /// transition is only valid out of `Connecting`. A future wiring leaf
    /// calls this exactly once, so the identity arm is defensive rather than
    /// a reachable path.
    pub(in crate::secondary) fn enter_awaiting_primary(self) -> Self {
        match self {
            SecondaryLifecycle::Connecting { since } => SecondaryLifecycle::AwaitingPrimary {
                since,
                handshake_sent: false,
            },
            other => other,
        }
    }

    /// `AwaitingPrimary → Configuring`: the primary has announced itself.
    ///
    /// This is THE worker-spawn boundary. The caller runs
    /// `initialize_workers` immediately before this transition (after the
    /// announce, before the InitialAssignment dispatch) and moves the
    /// resulting [`WorkerPool`] **into** the returned `Configuring` state.
    /// Because the pool lives only inside `ConfiguringState` /
    /// [`OperationalState`], there is structurally no pool to spawn into in
    /// `AwaitingPrimary`: a pre-`Configuring` worker-spawn is
    /// unrepresentable, and if the primary never announces the lifecycle
    /// never leaves `AwaitingPrimary` and no pool is ever built.
    ///
    /// `pre_staged_mode` / `uses_file_based_items` are seeded from the
    /// primary's `InitialAssignment` and carried forward into
    /// [`OperationalState`] at the next boundary. `transfer_complete` and
    /// `setup_discovery_done` start `false` — they are the `got_*` config
    /// signals that gate the `Configuring → Operational` transition.
    pub(in crate::secondary) fn enter_configuring(
        self,
        pool: WorkerPool<M, I>,
        pre_staged_mode: bool,
        uses_file_based_items: bool,
    ) -> Self {
        match self {
            SecondaryLifecycle::AwaitingPrimary { .. } => {
                SecondaryLifecycle::Configuring(Box::new(ConfiguringState {
                    pool,
                    transfer_complete: false,
                    pre_staged_mode,
                    uses_file_based_items,
                    setup_discovery_done: false,
                }))
            }
            other => other,
        }
    }

    /// `Configuring → Operational`: configuration completed (the `got_*`
    /// trio landed) and the secondary is now a running task-runner.
    ///
    /// This is the SINGLE fire-once boundary at which the take-once runtime
    /// latches ([`OperationalLatches`]) are consumed — the coordinator
    /// surrenders its `Option<Receiver>` slots here and gets the unwrapped
    /// handles back (`(Self, OperationalLatches)`) to drive the operational
    /// `select!` loop. A `SetupPending` excursion re-enters and finds the
    /// lifecycle already `Operational`, so this transition (and thus the
    /// consumption) happens at most once per node.
    ///
    /// The [`ConfiguringState`]'s `pool` and the three carried-forward
    /// config flags (`pre_staged_mode` / `uses_file_based_items` /
    /// `setup_discovery_done`) move **into** [`OperationalState`]; the
    /// operational runtime values the caller supplies — the
    /// [`ElectionState`] sub-machine, `primary_last_seen`,
    /// `peer_keepalives`, the [`PrimaryLink`], `active_tasks`, and the
    /// pending collections — are moved in alongside. Only once they live in
    /// `OperationalState` are `run_election_tick` / `send_keepalive`
    /// reachable (they are `impl OperationalState`), so neither can fire
    /// pre-`Operational`.
    ///
    /// Returns `self` unchanged (passing the latches straight back) if
    /// called from any non-`Configuring` variant: the transition is only
    /// valid out of `Configuring`.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::secondary) fn enter_operational(
        self,
        latches: OperationalLatches<I>,
        election: ElectionState,
        primary_last_seen: Option<Instant>,
        peer_keepalives: HashMap<String, f64>,
        primary_link: PrimaryLink,
        active_tasks: HashMap<String, WorkerId>,
        pending_peer_messages: Vec<(String, DistributedMessage<I>)>,
        pending_worker_restarts: HashSet<WorkerId>,
        pending_first_bind: HashMap<WorkerId, PendingFirstBind<I>>,
    ) -> (Self, OperationalLatches<I>) {
        match self {
            SecondaryLifecycle::Configuring(cfg) => {
                let ConfiguringState {
                    pool,
                    transfer_complete: _,
                    pre_staged_mode,
                    uses_file_based_items,
                    setup_discovery_done,
                } = *cfg;
                let next = SecondaryLifecycle::Operational(Box::new(OperationalState {
                    pool,
                    election,
                    primary_last_seen,
                    peer_keepalives,
                    primary_link,
                    active_tasks,
                    pending_peer_messages,
                    pending_worker_restarts,
                    pending_first_bind,
                    pre_staged_mode,
                    uses_file_based_items,
                    setup_discovery_done,
                }));
                (next, latches)
            }
            other => (other, latches),
        }
    }

    /// Construct `Operational` directly for a pure observer / late-joiner.
    ///
    /// An observer joins a running cluster, restores a snapshot, and skips
    /// the welcome/cert-exchange/wait-for-setup handshake entirely (the old
    /// `restore_from_snapshot_and_skip_setup` bool poke). It therefore
    /// lands in `Operational` from the start with an **empty** pool — it
    /// runs no workers. Observer non-candidacy is a capability gate on the
    /// role (the election machine refuses to enter `Candidate`), NOT a
    /// parallel lifecycle variant, so this is a direct `Operational`
    /// construction rather than a new state.
    ///
    /// Consumes [`OperationalLatches`] at the same single fire-once
    /// boundary as [`Self::enter_operational`]; returns the unwrapped
    /// handles for the operational loop.
    pub(in crate::secondary) fn operational_observer(
        latches: OperationalLatches<I>,
        primary_link: PrimaryLink,
    ) -> (Self, OperationalLatches<I>) {
        let state = SecondaryLifecycle::Operational(Box::new(OperationalState {
            pool: WorkerPool::new(),
            election: ElectionState::Normal,
            primary_last_seen: None,
            peer_keepalives: HashMap::new(),
            primary_link,
            active_tasks: HashMap::new(),
            pending_peer_messages: Vec::new(),
            pending_worker_restarts: HashSet::new(),
            pending_first_bind: HashMap::new(),
            // A late-joiner is never the pre-stage discovery node and
            // its dispatched items follow the historical file-based
            // contract until an `InitialAssignment` says otherwise.
            pre_staged_mode: false,
            uses_file_based_items: true,
            setup_discovery_done: false,
        }));
        (state, latches)
    }

    /// `Operational → Promoted` (terminal): this node won (or was named in)
    /// a promotion and is now the primary.
    ///
    /// `promote_activation_tx` — the one-shot gate to the co-located parked
    /// [`crate::primary::PrimaryCoordinator`] — is consumed HERE, at this
    /// transition (NOT at [`Self::enter_operational`]). Firing it wakes the
    /// parked primary into its seeded resume with the secondary's
    /// continuously-mirrored cluster snapshot. The two promotion paths that
    /// reach `Promoted` (own-election win + peer-named) both route through
    /// this single `take()`-and-fire boundary, so activation is fire-once.
    /// `None` when no co-located primary was composed (Rust-only tests,
    /// legacy single-`run()` callers): the gate is then a no-op and only
    /// the `PromotePrimary { new = self }` broadcast fires (the caller owns
    /// that broadcast — it is not this transition's concern).
    ///
    /// Returns the `Promoted` terminal plus the (taken) activation gate so
    /// the caller can fire it; modelling the gate as move-out keeps the
    /// fire-once `take()` at this one boundary.
    pub(in crate::secondary) fn enter_promoted(
        self,
        promote_activation_tx: Option<
            tokio::sync::oneshot::Sender<crate::cluster_state::ClusterStateSnapshot<I>>,
        >,
    ) -> (
        Self,
        Option<tokio::sync::oneshot::Sender<crate::cluster_state::ClusterStateSnapshot<I>>>,
    ) {
        (SecondaryLifecycle::Promoted, promote_activation_tx)
    }

    /// `* → Done` (terminal): the run reached a normal completion
    /// (RunComplete observed / clean drain-down). Maps the old
    /// `RunOutcome::Done`.
    pub(in crate::secondary) fn enter_done(self) -> Self {
        SecondaryLifecycle::Done
    }

    /// `* → Aborted` (terminal): the replicated ledger recorded
    /// `RunAborted`. Maps the old `RunOutcome::Aborted`.
    pub(in crate::secondary) fn enter_aborted(self) -> Self {
        SecondaryLifecycle::Aborted
    }

    /// `* → Panik` (terminal): the panik watcher fired (sentinel file /
    /// SIGTERM) and workers have been hard-killed. Maps the old
    /// `RunOutcome::PanikShutdown`.
    pub(in crate::secondary) fn enter_panik(self) -> Self {
        SecondaryLifecycle::Panik
    }

    /// `* → Failed` (terminal): an unrecoverable local fault was latched
    /// (the read of the old `fatal_exit` write-latch transitions here). The
    /// run exits non-zero.
    pub(in crate::secondary) fn enter_failed(self) -> Self {
        SecondaryLifecycle::Failed
    }

    /// Whether the lifecycle has reached `Operational` or a terminal
    /// variant — i.e. the old `setup_phase_completed` latch, recovered as a
    /// projection of the typed state rather than a separate bool. Used by
    /// the re-entry guard so a `SetupPending` re-entry skips the handshake.
    pub(in crate::secondary) fn setup_phase_completed(&self) -> bool {
        !matches!(
            self,
            SecondaryLifecycle::Connecting { .. }
                | SecondaryLifecycle::AwaitingPrimary { .. }
                | SecondaryLifecycle::Configuring(_)
        )
    }

    /// Whether the lifecycle is in a terminal (absorbing) variant.
    pub(in crate::secondary) fn is_terminal(&self) -> bool {
        matches!(
            self,
            SecondaryLifecycle::Promoted
                | SecondaryLifecycle::Done
                | SecondaryLifecycle::Aborted
                | SecondaryLifecycle::Panik
                | SecondaryLifecycle::Failed
        )
    }

    /// `&mut` access to the operational state, iff the lifecycle has
    /// reached `Operational`. The handlers that own worker dispatch,
    /// election, and keepalive are written against this — they are
    /// reachable ONLY through this accessor, so they are unrepresentable
    /// while the lifecycle is `Connecting` / `AwaitingPrimary` /
    /// `Configuring` (those variants carry no [`OperationalState`]).
    pub(in crate::secondary) fn operational_mut(&mut self) -> Option<&mut OperationalState<M, I>> {
        match self {
            SecondaryLifecycle::Operational(state) => Some(state),
            _ => None,
        }
    }

    /// `&mut` access to the configuring state, iff the lifecycle is
    /// `Configuring`. The config-phase handlers (`InitialAssignment` /
    /// `TransferComplete` ingestion) are written against this and are thus
    /// unrepresentable before the primary announces (in `Connecting` /
    /// `AwaitingPrimary`).
    pub(in crate::secondary) fn configuring_mut(&mut self) -> Option<&mut ConfiguringState<M, I>> {
        match self {
            SecondaryLifecycle::Configuring(state) => Some(state),
            _ => None,
        }
    }
}

/// Capability invariants that exist ONLY in `AwaitingPrimary`.
///
/// `AwaitingPrimary` carries no [`WorkerPool`] and no
/// [`OperationalState`], so it has neither a `spawn`-target nor a
/// `TaskAssignment` handler — those live on [`ConfiguringState`] /
/// [`OperationalState`] and are unrepresentable here by construction. The
/// only capability available pre-announce (beyond mesh formation, which is
/// the orthogonal [`MeshFormation`] sub-concern) is emitting the setup
/// handshake exactly once.
#[allow(dead_code)]
impl<M: ManagerEndpoint, I: Identifier> SecondaryLifecycle<M, I> {
    /// One-shot guard for the setup handshake (`send_welcome` /
    /// `send_cert_exchange`). Returns `true` and flips the latch the first
    /// time it is called in `AwaitingPrimary`; subsequent calls (and any
    /// call from another variant) return `false` so re-entry does not
    /// re-send. The handshake is the ONLY primary-facing action available
    /// before the primary announces — there is no worker-spawn and no
    /// task-acceptance capability in this variant to accompany it.
    pub(in crate::secondary) fn mark_handshake_sent(&mut self) -> bool {
        match self {
            SecondaryLifecycle::AwaitingPrimary { handshake_sent, .. } if !*handshake_sent => {
                *handshake_sent = true;
                true
            }
            _ => false,
        }
    }
}

#[allow(dead_code)]
impl<M: ManagerEndpoint, I: Identifier> ConfiguringState<M, I> {
    /// Record the primary's `TransferComplete`. One of the `got_*` config
    /// signals the `Configuring → Operational` transition gates on; this is
    /// the WRITE site, the transition is the READ site.
    pub(in crate::secondary) fn mark_transfer_complete(&mut self) {
        self.transfer_complete = true;
    }

    /// Whether the config-completion signal has landed. Read by the
    /// `Configuring → Operational` transition decision.
    pub(in crate::secondary) fn is_transfer_complete(&self) -> bool {
        self.transfer_complete
    }

    /// Borrow the worker pool. The pool exists ONLY from `Configuring`
    /// onward — this accessor is the structural reason a pre-`Configuring`
    /// worker operation cannot be expressed.
    pub(in crate::secondary) fn pool_mut(&mut self) -> &mut WorkerPool<M, I> {
        &mut self.pool
    }
}

/// Capability invariants that exist ONLY in `Operational`.
///
/// Election and keepalive are `impl OperationalState` — they read state
/// (`election`, `primary_last_seen`, `peer_keepalives`, `primary_link`)
/// that lives only here, so a `run_election_tick` / `send_keepalive`
/// written against `&mut OperationalState` is physically unreachable
/// before the lifecycle reaches `Operational`. A `Configuring` secondary
/// advancing past the short election deadline therefore stays
/// election-quiet by construction.
#[allow(dead_code)]
impl<M: ManagerEndpoint, I: Identifier> OperationalState<M, I> {
    /// Borrow the worker pool for task dispatch. The `TaskAssignment`
    /// handler is written against `&mut OperationalState`, so it is
    /// unrepresentable in any pre-`Operational` variant (none of which
    /// carry an `OperationalState`).
    pub(in crate::secondary) fn pool_mut(&mut self) -> &mut WorkerPool<M, I> {
        &mut self.pool
    }

    /// Record that a primary-side message was just seen (the canonical
    /// "primary is alive" signal): refresh `primary_last_seen` and reset
    /// the [`PrimaryLink`] health window. This is the primary-liveness half
    /// of the role-tagged keepalive split — distinct from peer-liveness,
    /// which is recorded into `peer_keepalives`.
    pub(in crate::secondary) fn record_primary_message(&mut self, now: Instant) {
        self.primary_last_seen = Some(now);
        self.primary_link.record_recv_success();
    }

    /// Record peer-liveness for `peer_id` (peer-keepalive tracking). The
    /// peer-liveness half of the role-tagged split: a `Secondary`-tagged
    /// keepalive always lands here, even when its id matches the current
    /// primary, so a multi-role host is tracked both as the primary and as
    /// a live peer without collision.
    pub(in crate::secondary) fn record_peer_keepalive(&mut self, peer_id: String, at: f64) {
        self.peer_keepalives.insert(peer_id, at);
    }

    /// The short election deadline horizon — computed from the keepalive
    /// cadence the caller passes (`keepalive_interval ×
    /// keepalive_miss_threshold`). It is defined as a method ON
    /// `OperationalState` precisely so it cannot be evaluated, and thus
    /// cannot fire, before the lifecycle reaches `Operational`; the long
    /// `unconfigured_deadline` is the only horizon that governs the
    /// pre-`Operational` span.
    pub(in crate::secondary) fn election_deadline(
        &self,
        keepalive_interval: std::time::Duration,
        keepalive_miss_threshold: u32,
    ) -> std::time::Duration {
        keepalive_interval * keepalive_miss_threshold
    }

    /// Whether the failover election is in its quiescent `Normal` state.
    /// Election ticks only advance from here; this is the entry predicate
    /// the operational tick consults.
    pub(in crate::secondary) fn election_is_normal(&self) -> bool {
        self.election.is_normal()
    }

    /// Take the queued deferred peer messages, leaving the field empty.
    /// Flushed onto the transport at the top of each operational loop
    /// iteration — a capability that exists only here because the queue
    /// lives only in `OperationalState`.
    pub(in crate::secondary) fn drain_pending_peer_messages(
        &mut self,
    ) -> Vec<(String, DistributedMessage<I>)> {
        std::mem::take(&mut self.pending_peer_messages)
    }

    /// The local-build half of one election tick: advance the failover
    /// state machine and collect the peer messages to flush, with NO
    /// transport contact. This is the part of `run_election_tick` that
    /// reads/writes only `OperationalState`-resident data (`election`,
    /// `peer_keepalives`, `primary_last_seen`, `primary_link`). It lives on
    /// `impl OperationalState` — and so is unreachable before the lifecycle
    /// is `Operational` — exactly as the plan requires of `run_election_tick`.
    ///
    /// The transport-coupled remainder (resolving destinations, actually
    /// sending the returned [`ElectionTickActions`]) couples to the
    /// coordinator's `Tr` and is owned by the wiring leaf (D-#124, which
    /// owns `coordinator.rs`); this additive leaf does not touch it. Today
    /// the failover decision logic still lives on the coordinator, so this
    /// method returns the empty action set — the wiring leaf relocates the
    /// decision body here when it migrates `election/coordinator.rs`.
    pub(in crate::secondary) fn collect_election_actions(&mut self) -> ElectionTickActions<I> {
        // Decision body relocated by the wiring leaf (D-#124). Until then
        // an Operational tick produces no election traffic — the same
        // election-quiet default the `Normal` state has always had.
        ElectionTickActions::default()
    }
}
