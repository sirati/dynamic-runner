//! Typed secondary lifecycle state machine.
//!
//! This module owns the [`SecondaryLifecycle`] type: the explicit,
//! type-level encoding of a secondary's progression from "just a peer on
//! the mesh, no primary contact yet" to "operational task-runner" to a
//! terminal outcome. It replaces the scattered bools/Options on
//! [`super::SecondaryCoordinator`] with one state value whose variants
//! make the system's capability invariants hard to violate.
//!
//! # Single source of truth for the per-secondary terminal
//!
//! The terminal variants [`Done`](SecondaryLifecycle::Done) /
//! [`Aborted`](SecondaryLifecycle::Aborted) /
//! [`Panik`](SecondaryLifecycle::Panik) /
//! [`Failed`](SecondaryLifecycle::Failed) carry the per-secondary terminal
//! payload (the abort/panik reason, the panik `matched_path`). They are
//! the ONE place that records how *this secondary* ended; the coordinator
//! drives the matching `enter_*` transition at each terminal site in
//! `process_tasks`, and both the run-loop teardown and the PyO3 boundary
//! read the terminal back via [`SecondaryLifecycle::terminal`] (projected
//! to the public [`super::SecondaryTerminal`]). The per-run control signal
//! `run_until_setup_or_done` returns ([`super::RunOutcome`]) is the
//! orthogonal yield-vs-reached-terminal axis and carries no terminal
//! payload — it never duplicates the terminal semantics that live here.
//!
//! # Capability invariants (the WHY — enforced by construction)
//!
//! The states form a forward progression `Connecting → AwaitingPrimary →
//! Configuring → Operational`, with four terminal absorbing states
//! (`Done`/`Aborted`/`Panik`/`Failed`). The invariant each state encodes:
//!
//! - **`AwaitingPrimary` cannot spawn workers and cannot accept a
//!   `TaskAssignment`** — by construction for the spawn target, by an
//!   expect-contract for the handler. The worker pool lives *only* inside
//!   [`ConfiguringState`]/[`OperationalState`], so there is structurally no
//!   pool to spawn into before `Configuring`. The `TaskAssignment` handler
//!   reaches the pool / operational state through the
//!   `op_mut()` / `pool_mut()` accessors, which `#[track_caller] .expect(…)`
//!   that the carrying variant is present; that contract holds because
//!   dispatch is routed to run only after `enter_operational`, never while
//!   the lifecycle is `AwaitingPrimary`. No runtime
//!   `if not configured { reject }` guard is needed — a stray pre-config
//!   dispatch is a loud panic at the accessor, not a silent bad state.
//! - **Workers spawn on the `AwaitingPrimary → Configuring` transition.**
//!   `initialize_workers` runs as the entry action of `Configuring` (after
//!   the primary has announced itself, before the InitialAssignment
//!   dispatch). If the primary never announces, the lifecycle never leaves
//!   `AwaitingPrimary` and no worker pool is ever built.
//! - **Election and keepalive live ONLY in `Operational`.** The
//!   [`super::election::ElectionState`] sub-machine and the
//!   primary-liveness tracking (`primary_last_seen`, `peer_keepalives`,
//!   `primary_link`) are fields of [`OperationalState`]. The election and
//!   keepalive BEHAVIOUR stays on the coordinator (it needs coordinator-
//!   level `cluster_state`/transport, not just `OperationalState` data),
//!   but it reaches that state through `op_mut()`, which is `None` in every
//!   pre-`Operational` variant — so an election tick / keepalive emission
//!   cannot fire before the lifecycle reaches `Operational`. A
//!   `Configuring` secondary advancing past the short election deadline
//!   therefore stays election-quiet by construction.
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
//! The binding rule is "mirror the real field types, never invent a
//! placeholder where a real type exists": the carried `pool` is a real
//! [`WorkerPool<M, I>`], which forces `M` in alongside `I`, so the machine
//! is parameterized over both. The coordinator's other generics (`Tr`
//! transport, `S` scheduler, `E` estimator) are NOT needed: no field
//! carried by a state is typed over them.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;

use dynrunner_core::{Identifier, WorkerId};
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::DistributedMessage;

use super::PendingFirstBind;
use super::SecondaryTerminal;
use super::election::ElectionState;
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
/// channels). The four terminal variants
/// (`Done`/`Aborted`/`Panik`/`Failed`) are absorbing and carry the
/// per-secondary terminal payload (see the module docs).
///
/// **No promotion state.** A secondary is never promoted: when its host
/// becomes the primary the host's co-located primary coordinator is BUILT
/// ON DEMAND (the coordinator-level `primary_activator`, invoked from the
/// apply hook for a failover-self election win and from the setup FSM for a
/// bootstrap transfer), while this secondary stays `Operational`. There is
/// no `Operational → Promoted` lifecycle transition.
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
pub(in crate::secondary) enum SecondaryLifecycle<M: ManagerEndpoint, I: Identifier> {
    /// Just starting: no primary peer known and no `PrimaryChanged`
    /// applied yet. The peer mesh IS being established here (see
    /// [`MeshFormation`], carried alongside this variant in the wired
    /// coordinator) — only worker-spawn, task-acceptance, election, and
    /// keepalive are unavailable. The long `unconfigured_deadline` that
    /// bounds the pre-`Operational` span is applied as a relative
    /// `tokio::time::timeout` at the `run_until_setup_or_done` orchestration
    /// boundary, so this variant carries no entry-instant anchor.
    Connecting,

    /// Mesh-joining as far as it can, handshake in flight, but no primary
    /// has announced itself yet. **Cannot spawn workers, cannot accept a
    /// `TaskAssignment`, runs no election, sends no keepalive** — there is
    /// no pool in this variant and no Operational state data to write to.
    /// Bounded by the long `unconfigured_deadline` (applied relatively at
    /// the orchestration boundary).
    AwaitingPrimary {
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

    /// Terminal: the run reached a normal completion (RunComplete observed
    /// / clean drain-down). Projects to [`SecondaryTerminal::Done`].
    Done,

    /// Terminal: the replicated ledger recorded `RunAborted` (the failure
    /// twin of RunComplete). Carries the abort `reason` for the boundary
    /// log; projects to [`SecondaryTerminal::Aborted`], which the PyO3
    /// boundary maps to `exit(1)`.
    Aborted {
        /// The cluster-wide abort reason carried from the broadcast.
        reason: String,
    },

    /// Terminal: the panik watcher fired (sentinel file / SIGTERM) and
    /// workers have been hard-killed. Carries the first matched panik file
    /// path and the human-readable reason; projects to
    /// [`SecondaryTerminal::Panik`], which the PyO3 boundary maps to
    /// `exit(137)`.
    Panik {
        /// The first panik file that existed (PyO3 boundary shutdown-cause
        /// log).
        matched_path: PathBuf,
        /// The human-readable reason (`"panik file: <path>"` shape).
        reason: String,
    },

    /// Terminal: an unrecoverable local fault was latched (the read of the
    /// `fatal_exit` write-latch transitions here). The run loop returns
    /// `Err(reason)`; this terminal records the per-secondary
    /// internal-failure outcome and carries the same `reason`.
    Failed {
        /// The fatal-exit reason the run loop propagates as its `Err`.
        reason: String,
    },
}

/// State data for [`SecondaryLifecycle::Configuring`].
///
/// Carries the worker pool (spawned on entry to this state) plus the
/// setup-discovery flags the configuration phase reads. The pre-staged /
/// file-based / discovery-done flags are *carried forward* into
/// [`OperationalState`] when configuration completes, so the resolver and
/// the `SetupPending` discriminator keep their values across the
/// `enter_operational()` boundary.
pub(in crate::secondary) struct ConfiguringState<M: ManagerEndpoint, I: Identifier> {
    /// The local worker pool, built by `initialize_workers` on entry to
    /// this state. Real [`WorkerPool<M, I>`] — there is no pool in any
    /// earlier state, which is what makes a pre-`Configuring` worker-spawn
    /// unrepresentable.
    pub(in crate::secondary) pool: WorkerPool<M, I>,

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

    /// This node's OWN in-flight worker assignments: `file_hash ->
    /// worker_id`. Populated during the `InitialAssignment` dispatch,
    /// which runs in `Configuring` (`wait_for_setup` →
    /// `handle_initial_assignment`), BEFORE the `Configuring → Operational`
    /// transition — so the map must exist from `Configuring` onward and is
    /// carried forward into [`OperationalState`] at `enter_operational`.
    /// Own-worker management, not authority.
    pub(in crate::secondary) active_tasks: HashMap<String, WorkerId>,
}

/// State data for [`SecondaryLifecycle::Operational`].
///
/// Carries everything the running task loop owns. The nested
/// [`ElectionState`] is the orthogonal-within-`Operational` sub-concern
/// (election can only run here); the short election deadline is computed
/// from `primary_last_seen` + the keepalive config that lives alongside it,
/// so it cannot fire before this state is reached.
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

    /// Peer-keepalive tracking: `peer_id -> last_seen` (LOCAL receipt-time
    /// monotonic `Instant`, recorded the moment THIS node receives the
    /// peer's keepalive — NOT the sender's wire wall-clock timestamp).
    /// Keying on a monotonic receipt `Instant` (mirroring `primary_last_seen`
    /// and the primary's `secondary_keepalives`) makes peer-liveness immune to
    /// a coordinated host suspend/resume: `CLOCK_MONOTONIC` does not accrue
    /// suspend time, so a wall-clock jump cannot mass-prune every peer at once.
    /// The next received keepalive resets the anchor (reset-on-receipt).
    /// Peer-liveness, distinct from primary-liveness (`primary_last_seen`).
    pub(in crate::secondary) peer_keepalives: HashMap<String, Instant>,

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

    /// Co-located primary LOOPBACK receiver (channel CH1) — RESUMABLE
    /// per-run terminal-signal state, NOT a fire-once latch.
    ///
    /// On a promoted / multi-role host the co-located
    /// [`crate::primary::PrimaryCoordinator`] is NOT a mesh peer of itself,
    /// so its `RunComplete` (and every other `Destination::All` broadcast)
    /// reaches this co-located secondary's `process_tasks` loop ONLY through
    /// this loopback — the QUIC mesh never closes, so the `recv_peer()==None`
    /// break cannot fire. This receiver is therefore the SOLE path by which a
    /// promoted secondary observes its own primary's clean completion.
    ///
    /// It lives on [`OperationalState`] (alongside `active_tasks` and the
    /// other resumable per-run state) precisely BECAUSE a `SetupPending`
    /// yield/re-entry is a real second consumption: the operational loop
    /// `take`s it into a loop-local at each entry and restores it here before
    /// the `SetupPending` return, so re-entry re-attaches it from this home.
    /// Were it a fire-once [`OperationalLatches`] member, the re-entry would
    /// find `None`, the loopback arm would park on `pending()` forever, and
    /// the promoted secondary would never observe `RunComplete` (per-run
    /// container/process leak). Seeded from the coordinator's
    /// `colocated_loopback_inbound_rx` slot at the `process_tasks`
    /// loop-local-take site (`coordinator-slot, else OperationalState`),
    /// the same single idiom as `panik_signal_rx`; `None` on every
    /// non-co-located path (the drain arm parks on `pending()`) and for the
    /// pure-observer construction.
    pub(in crate::secondary) colocated_loopback_inbound_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<I>>>,

    /// Panik-watcher signal receiver — RESUMABLE per-run terminal-signal
    /// state, NOT a fire-once latch.
    ///
    /// The watcher resolves this `oneshot` the moment it observes any
    /// configured sentinel path OR a delivered SIGTERM (the SLURM
    /// time-limit / `scancel` → `kill -TERM` cascade). It is the SOLE
    /// in-loop path by which a graceful operator-initiated shutdown reaches
    /// the operational `select!`: the panik arm turns it into worker
    /// teardown + the `Panik` terminal + exit(137), letting the secondary
    /// release its resources before the kernel `SIGKILL`s at the grace
    /// deadline.
    ///
    /// It lives on [`OperationalState`] (alongside `active_tasks` and the
    /// other resumable per-run state) precisely BECAUSE a `SetupPending`
    /// yield/re-entry is a real second consumption: a REGULAR (non-observer)
    /// secondary registers panik AND can be the pre-staged discovery node,
    /// so it both reaches the panik arm and takes the `SetupPending`
    /// excursion. The operational loop `take`s it into a loop-local at each
    /// entry and restores it here before the `SetupPending` return, so
    /// re-entry re-attaches it from this home. Were it a fire-once
    /// [`OperationalLatches`] member, the re-entry would find `None`, the
    /// panik arm would park on `pending()` forever, and a SIGTERM delivered
    /// after the discovery yield would NOT be acted on by the secondary loop
    /// (the watcher's oneshot cannot reach the dead arm) — graceful
    /// shutdown is lost and teardown falls back to the kernel `SIGKILL`.
    /// Seeded from the coordinator's `panik_signal_rx` slot at the
    /// `process_tasks` loop-local-take site
    /// (`coordinator-slot, else OperationalState`), the same single idiom as
    /// `colocated_loopback_inbound_rx`; `None` when no panik paths were set
    /// (the arm parks on `pending()`) and for the pure-observer
    /// construction.
    pub(in crate::secondary) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,
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
    /// The primary defers its `PrimaryChanged` announcement until every
    /// secondary reports, so this enforces "exactly once per secondary".
    pub(in crate::secondary) mesh_ready_sent: bool,

    /// The `degraded` latch: set true once the watchdog deadline elapsed
    /// with zero connected peers. A degraded mesh is NOT fatal — task
    /// dispatch over the direct primary link still works; only the
    /// peer-mesh-dependent paths (failover election, peer-keepalive
    /// broadcasts) fail-loud-or-skip on this flag.
    pub(in crate::secondary) degraded: bool,
}

impl Default for MeshFormation {
    /// The pre-dial resting state, identical to the flat-field defaults
    /// the coordinator's `new()` used to set: no watchdog armed, zero
    /// dials attempted, `MeshReady` not yet emitted, not degraded. The
    /// orthogonal mesh sub-concern starts here in `Connecting` and
    /// evolves as `connect_to_peers` fires and the watchdog runs.
    fn default() -> Self {
        Self {
            peer_mesh_check_at: None,
            peer_dial_count: 0,
            mesh_ready_sent: false,
            degraded: false,
        }
    }
}

/// The take-once runtime latches surrendered at the single
/// [`SecondaryLifecycle::enter_operational`] boundary.
///
/// These are the `Option<Receiver>` slots the coordinator builds at
/// construction and `take()`s once when it first reaches `process_tasks`
/// (see the matching fields on [`super::SecondaryCoordinator`]:
/// `announcer_outbox_rx`, `fatal_exit_signal_rx`).
/// The `Configuring → Operational` transition is the ONE place they are
/// consumed: the coordinator fills this carrier by `take()`-ing each
/// `Option`, hands it to `enter_operational`, and gets the unwrapped values
/// back to drive the operational `select!` loop.
///
/// **Neither the co-located loopback inbound receiver NOR the panik-watcher
/// signal receiver is here.** Both used to ride this carrier, but a
/// `SetupPending` yield/re-entry is a real SECOND consumption, and each is a
/// SOLE terminal-signal path for a NON-observer pre-staged node:
///
/// - The loopback receiver: on a promoted node the co-located primary is not
///   a mesh peer of itself, so its `RunComplete` reaches the secondary only
///   via the loopback. A `None`-on-re-entry would park the loopback arm on
///   `pending()` forever and leak the run.
/// - The panik receiver: a REGULAR (non-observer) secondary registers panik
///   AND can be the pre-staged discovery node, so a SIGTERM delivered after
///   the discovery yield must still be acted on. A `None`-on-re-entry would
///   park the panik arm on `pending()` forever, dropping graceful shutdown
///   to a kernel `SIGKILL`.
///
/// Both are therefore resumable per-run state on
/// [`OperationalState::colocated_loopback_inbound_rx`] /
/// [`OperationalState::panik_signal_rx`], `take`-n into a loop-local and
/// restored before the `SetupPending` return so re-entry re-attaches them.
///
/// The `lifecycle_rx` / `task_completed_rx` receivers are deliberately NOT
/// here: they were already `take()`-n earlier (in
/// `run_until_setup_or_done_inner`) to spawn their dispatcher tasks, and
/// `command_rx` is transferred to the co-located primary — none of the
/// three is ever drained by this loop, so the carrier only ferries the
/// receivers the operational `select!` actually polls.
///
/// **Fire-once by construction — and that is exactly why the loopback and
/// panik receivers are elsewhere.** A `SetupPending` excursion re-enters
/// `run_until_setup_or_done` and finds the lifecycle already `Operational`,
/// so `enter_operational` is never called twice; and even if it were, the
/// coordinator's `Option::take()` yields `None` on the second pass. For the
/// two members carried here that `None` is benign — NOT because the
/// capability is "optional", but because both are OBSERVER-ONLY
/// registrations (`attach_observer_announcer` / the observer's invalid-task
/// `register_fatal_exit_signal_rx`),
/// and an observer / late-joiner lands directly in `Operational` via
/// `restore_from_snapshot_and_skip_setup` — it NEVER takes the
/// `SetupPending` excursion, so its arms are never re-parked on re-entry.
/// Modelling these two as a move-in / move-out carrier (NOT fields of
/// [`OperationalState`]) keeps them where they belong — local to the
/// operational loop, not part of the resumable state data. The loopback
/// inbound receiver and the panik signal receiver are the exceptions: each
/// is the SOLE terminal-signal path for a NON-observer node that DOES take
/// the `SetupPending` excursion (the loopback on a promoted node, panik on a
/// pre-staged regular secondary), so a `None`-on-re-entry is FATAL — they
/// are resumable per-run state on [`OperationalState`], not members of this
/// fire-once carrier.
///
/// `primary_activator` is deliberately NOT here: it stays a
/// coordinator-level slot, `take()`-n at the activation site when this node
/// is named primary and the co-located primary is built on demand (the
/// secondary itself never leaves `Operational`), not at this boundary.
pub(in crate::secondary) struct OperationalLatches<I: Identifier> {
    /// Observer-announcer outbox receiver. `None` outside an attached
    /// observer wiring.
    pub(in crate::secondary) announcer_outbox_rx:
        Option<tokio::sync::mpsc::Receiver<crate::observer::announcer::AnnouncerOutboxItem<I>>>,

    /// Externally-armed fatal-exit signal receiver. `None` when no
    /// run-loop-external policy was attached.
    pub(in crate::secondary) fatal_exit_signal_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
}

impl<I: Identifier> OperationalLatches<I> {
    /// An all-`None` latch carrier.
    ///
    /// Used by the pure-observer / late-joiner construction
    /// ([`SecondaryLifecycle::operational_observer`] via
    /// `restore_from_snapshot_and_skip_setup`): that path builds the
    /// `Operational` state shell BEFORE the operational loop and must NOT
    /// consume the coordinator's real take-once receivers — those are
    /// surrendered at the single `process_tasks`-entry boundary, uniform
    /// with the normal path. Passing this empty carrier and discarding the
    /// returned one keeps the real `Option` fields intact on the
    /// coordinator for that single consumption site.
    pub(in crate::secondary) fn empty() -> Self {
        Self {
            announcer_outbox_rx: None,
            fatal_exit_signal_rx: None,
        }
    }
}

// `M: 'static` mirrors the bound on `WorkerPool::new()` (and on the
// `SecondaryCoordinator` impl): the empty-pool late-joiner construction in
// `operational_observer` calls it, and the carried `WorkerPool<M, I>`
// requires it anyway.
impl<M: ManagerEndpoint + 'static, I: Identifier> SecondaryLifecycle<M, I> {
    /// Construct the initial state: the lifecycle begins as `Connecting`
    /// the moment the coordinator starts. The long `unconfigured_deadline`
    /// that governs the pre-`Operational` span is applied relatively at the
    /// orchestration boundary, so no entry-instant is carried.
    pub(in crate::secondary) fn connecting() -> Self {
        SecondaryLifecycle::Connecting
    }

    /// `Connecting → AwaitingPrimary`. The peer mesh keeps forming (the
    /// orthogonal [`MeshFormation`] sub-concern is unaffected by this
    /// transition); the secondary is now actively trying to reach a
    /// primary but none has announced itself yet. `handshake_sent` starts
    /// `false`; the capability to actually emit the welcome/cert-exchange
    /// handshake is gated on this state (see [`Self::mark_handshake_sent`]).
    ///
    /// Returns `self` unchanged if called from any other variant: the
    /// transition is only valid out of `Connecting`. The coordinator calls
    /// this exactly once (at the top of `run_until_setup_or_done`), so the
    /// identity arm is defensive rather than a reachable path.
    pub(in crate::secondary) fn enter_awaiting_primary(self) -> Self {
        match self {
            SecondaryLifecycle::Connecting => SecondaryLifecycle::AwaitingPrimary {
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
    /// [`OperationalState`] at the next boundary. `setup_discovery_done`
    /// starts `false`. The real `Configuring → Operational` gate is the
    /// local `got_peer_info / got_assignment / got_transfer` trio tracked
    /// in `wait_for_setup` — the single source of truth, not a field on
    /// this state.
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
                    pre_staged_mode,
                    uses_file_based_items,
                    setup_discovery_done: false,
                    active_tasks: HashMap::new(),
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
    /// `OperationalState` can the coordinator's election-tick / keepalive
    /// behaviour reach them (via `op_mut()`, which is `None` pre-
    /// `Operational`), so neither can fire pre-`Operational`.
    ///
    /// Returns `self` unchanged (passing the latches straight back) if
    /// called from any non-`Configuring` variant: the transition is only
    /// valid out of `Configuring`.
    ///
    /// The two resumable terminal-signal receivers
    /// ([`OperationalState::colocated_loopback_inbound_rx`] and
    /// [`OperationalState::panik_signal_rx`]) are NOT seeded here: both start
    /// `None` on this construction and are seeded from their coordinator slots
    /// at the `process_tasks` loop-local-take site (with a fallback to the
    /// restored `OperationalState` value on re-entry). That single take site,
    /// rather than this transition, is the seed/restore home for both because
    /// panik is also registered on the observer's already-`Operational` path
    /// (which never reaches this `Configuring` arm), so seeding it only here
    /// would drop the observer's receiver — and sourcing the loopback the same
    /// way keeps ONE idiom for both. Unlike the fire-once
    /// [`OperationalLatches`], both must survive re-entry — see the take site
    /// and the matching `OperationalState` fields.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::secondary) fn enter_operational(
        self,
        latches: OperationalLatches<I>,
        election: ElectionState,
        primary_last_seen: Option<Instant>,
        peer_keepalives: HashMap<String, Instant>,
        primary_link: PrimaryLink,
        pending_peer_messages: Vec<(String, DistributedMessage<I>)>,
        pending_worker_restarts: HashSet<WorkerId>,
        pending_first_bind: HashMap<WorkerId, PendingFirstBind<I>>,
    ) -> (Self, OperationalLatches<I>) {
        match self {
            SecondaryLifecycle::Configuring(cfg) => {
                let ConfiguringState {
                    pool,
                    pre_staged_mode,
                    uses_file_based_items,
                    setup_discovery_done,
                    // The initial-assignment dispatch (run in `Configuring`)
                    // already populated `active_tasks`; carry it forward
                    // rather than overwrite it with an empty map.
                    active_tasks,
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
                    // Both resumable terminal-signal receivers start `None` on
                    // the lifecycle: the live receiver is held on its
                    // coordinator slot until the `process_tasks`
                    // loop-local-take site seeds it (see
                    // [`OperationalState::colocated_loopback_inbound_rx`] /
                    // [`OperationalState::panik_signal_rx`]).
                    colocated_loopback_inbound_rx: None,
                    panik_signal_rx: None,
                }));
                (next, latches)
            }
            // Already `Operational` (a `SetupPending` re-entry): a no-op on the
            // state — the receivers already living in `OperationalState`
            // (restored before the yield) are preserved.
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
            // A pure observer / late-joiner has no co-located primary, so no
            // loopback inbound; the operational loop's drain arm parks on
            // `pending()`.
            colocated_loopback_inbound_rx: None,
            // `panik_signal_rx` starts `None` on this observer shell: the
            // observer DOES register a panik receiver, but (like the normal
            // path) the live receiver stays on the coordinator slot until the
            // `process_tasks` loop-local-take site seeds it. The observer
            // lands here already-`Operational` and never takes the
            // `SetupPending` excursion, so the take site sees the coordinator
            // slot `Some` on its single entry — no restore is needed.
            panik_signal_rx: None,
        }));
        (state, latches)
    }

    /// `* → Done` (terminal): the run reached a normal completion
    /// (RunComplete observed / clean drain-down). The single site that
    /// records a normal per-secondary finish; projects to
    /// [`SecondaryTerminal::Done`].
    pub(in crate::secondary) fn enter_done(self) -> Self {
        SecondaryLifecycle::Done
    }

    /// `* → Aborted` (terminal): the replicated ledger recorded
    /// `RunAborted`. Carries the cluster-wide abort `reason`; projects to
    /// [`SecondaryTerminal::Aborted`] (`exit(1)` at the PyO3 boundary).
    pub(in crate::secondary) fn enter_aborted(self, reason: String) -> Self {
        SecondaryLifecycle::Aborted { reason }
    }

    /// `* → Panik` (terminal): the panik watcher fired (sentinel file /
    /// SIGTERM) and workers have been hard-killed. Carries the matched
    /// panik file path and the reason; projects to
    /// [`SecondaryTerminal::Panik`] (`exit(137)` at the PyO3 boundary).
    pub(in crate::secondary) fn enter_panik(self, matched_path: PathBuf, reason: String) -> Self {
        SecondaryLifecycle::Panik {
            matched_path,
            reason,
        }
    }

    /// `* → Failed` (terminal): an unrecoverable local fault was latched
    /// (the read of the `fatal_exit` write-latch transitions here). The run
    /// loop returns `Err(reason)`; this terminal records the per-secondary
    /// internal-failure outcome with the same `reason`.
    pub(in crate::secondary) fn enter_failed(self, reason: String) -> Self {
        SecondaryLifecycle::Failed { reason }
    }

    /// Whether the lifecycle has reached `Operational` or a terminal
    /// variant — i.e. the old `setup_phase_completed` latch, recovered as a
    /// projection of the typed state rather than a separate bool. Used by
    /// the re-entry guard so a `SetupPending` re-entry skips the handshake.
    pub(in crate::secondary) fn setup_phase_completed(&self) -> bool {
        !matches!(
            self,
            SecondaryLifecycle::Connecting
                | SecondaryLifecycle::AwaitingPrimary { .. }
                | SecondaryLifecycle::Configuring(_)
        )
    }

    /// Project the terminal variant to the public [`SecondaryTerminal`]
    /// boundary type, or `None` if the lifecycle has not reached a terminal.
    ///
    /// This is the SINGLE crossing point from the module-private lifecycle
    /// terminal to the public boundary: the run-loop teardown and the PyO3
    /// exit-code decision both read the per-secondary outcome through here,
    /// so the terminal semantics are defined once (on the lifecycle) and
    /// merely projected — never duplicated onto `RunOutcome`.
    pub(in crate::secondary) fn terminal(&self) -> Option<SecondaryTerminal> {
        match self {
            SecondaryLifecycle::Done => Some(SecondaryTerminal::Done),
            SecondaryLifecycle::Aborted { reason } => Some(SecondaryTerminal::Aborted {
                reason: reason.clone(),
            }),
            SecondaryLifecycle::Panik {
                matched_path,
                reason,
            } => Some(SecondaryTerminal::Panik {
                matched_path: matched_path.clone(),
                reason: reason.clone(),
            }),
            SecondaryLifecycle::Failed { reason } => Some(SecondaryTerminal::Failed {
                reason: reason.clone(),
            }),
            _ => None,
        }
    }

    /// `&mut` access to the operational state, iff the lifecycle has
    /// reached `Operational`; `None` in every pre-`Operational` / terminal
    /// variant (those carry no [`OperationalState`]). The handlers that own
    /// worker dispatch, election, and keepalive are written against this and
    /// reach it through the coordinator's `op_mut()`, which `.expect(…)`s
    /// the operational variant is present — an expect-contract honoured by
    /// routing dispatch to run only after `enter_operational`, not a
    /// compile-time guarantee that the call is unrepresentable.
    pub(in crate::secondary) fn operational_mut(&mut self) -> Option<&mut OperationalState<M, I>> {
        match self {
            SecondaryLifecycle::Operational(state) => Some(state),
            _ => None,
        }
    }

    /// `&` (shared) access to the operational state, iff the lifecycle
    /// has reached `Operational`. The read-only sibling of
    /// [`Self::operational_mut`]: the read-only paths that may run before
    /// the loop is fully operational (the mesh watchdog's
    /// keepalive-active worker count, the keepalive emitter's
    /// active-worker tally) borrow the pool / counts through this without
    /// asserting `Operational`.
    pub(in crate::secondary) fn operational_ref(&self) -> Option<&OperationalState<M, I>> {
        match self {
            SecondaryLifecycle::Operational(state) => Some(state),
            _ => None,
        }
    }

    /// `&mut` access to the worker pool from EITHER state that carries it
    /// (`Configuring` or `Operational`). `None` in `Connecting` /
    /// `AwaitingPrimary` (no pool spawned yet) and in terminal states.
    ///
    /// This is the accessor for the handlers that legitimately run in
    /// BOTH the configuration and the operational phase and touch only
    /// the pool — notably the shared `report_unresolvable_task` fail-loud
    /// guard, reached from `handle_initial_assignment` (Configuring) AND
    /// from the operational `TaskAssignment` dispatch (Operational). The
    /// pool exists from `Configuring` onward, so a state-agnostic pool
    /// borrow is exactly "pool exists" — which is the same structural
    /// guarantee `op_mut`/`configuring_mut` give for their own state's
    /// full surface, narrowed to the one field both states share.
    pub(in crate::secondary) fn pool_mut(&mut self) -> Option<&mut WorkerPool<M, I>> {
        match self {
            SecondaryLifecycle::Configuring(cfg) => Some(&mut cfg.pool),
            SecondaryLifecycle::Operational(op) => Some(&mut op.pool),
            _ => None,
        }
    }

    /// `&` (shared) sibling of [`Self::pool_mut`]: the worker pool from
    /// `Configuring` or `Operational`. `None` pre-`Configuring` and in
    /// terminal states. Used by the read-only sampler hooks, which fire
    /// from both the initial-assignment (Configuring) and operational
    /// dispatch sites and need the worker's cgroup-leaf path off the pool
    /// without asserting a specific state.
    pub(in crate::secondary) fn pool_ref(&self) -> Option<&WorkerPool<M, I>> {
        match self {
            SecondaryLifecycle::Configuring(cfg) => Some(&cfg.pool),
            SecondaryLifecycle::Operational(op) => Some(&op.pool),
            _ => None,
        }
    }

    /// `&mut` access to the OWN-worker `active_tasks` map from whichever
    /// state carries it (`Configuring` or `Operational`). It is first
    /// populated by the `InitialAssignment` dispatch in `Configuring` and
    /// carried forward into `Operational`, so own-worker management
    /// touches it across both states. `None` pre-`Configuring` / terminal.
    pub(in crate::secondary) fn active_tasks_mut(
        &mut self,
    ) -> Option<&mut HashMap<String, WorkerId>> {
        match self {
            SecondaryLifecycle::Configuring(cfg) => Some(&mut cfg.active_tasks),
            SecondaryLifecycle::Operational(op) => Some(&mut op.active_tasks),
            _ => None,
        }
    }

    /// Read the pre-staged-source-mode flag from whichever state carries
    /// it (`Configuring` or `Operational` — it is seeded in `Configuring`
    /// and carried forward into `Operational`). `false` before
    /// `Configuring` (no `InitialAssignment` seen yet), which matches the
    /// historical pre-InitialAssignment default.
    pub(in crate::secondary) fn pre_staged_mode(&self) -> bool {
        match self {
            SecondaryLifecycle::Configuring(cfg) => cfg.pre_staged_mode,
            SecondaryLifecycle::Operational(op) => op.pre_staged_mode,
            _ => false,
        }
    }

    /// Read the file-based-items flag from whichever state carries it.
    /// Defaults to `true` before `Configuring` (the historical
    /// pre-`InitialAssignment` contract: items are real files until an
    /// `InitialAssignment` says otherwise).
    pub(in crate::secondary) fn uses_file_based_items(&self) -> bool {
        match self {
            SecondaryLifecycle::Configuring(cfg) => cfg.uses_file_based_items,
            SecondaryLifecycle::Operational(op) => op.uses_file_based_items,
            _ => true,
        }
    }

    /// Read the setup-discovery fire-once latch from whichever state
    /// carries it. `false` before `Configuring`.
    pub(in crate::secondary) fn setup_discovery_done(&self) -> bool {
        match self {
            SecondaryLifecycle::Configuring(cfg) => cfg.setup_discovery_done,
            SecondaryLifecycle::Operational(op) => op.setup_discovery_done,
            _ => false,
        }
    }

    /// Set the pre-staged-source-mode flag on the carrying state. Written
    /// from `wait_for_setup`'s `InitialAssignment` handler, which runs in
    /// `Configuring`. A no-op pre-`Configuring` / terminal (no state to
    /// carry it) — defensive; the only caller runs in `Configuring`.
    pub(in crate::secondary) fn set_pre_staged_mode(&mut self, on: bool) {
        match self {
            SecondaryLifecycle::Configuring(cfg) => cfg.pre_staged_mode = on,
            SecondaryLifecycle::Operational(op) => op.pre_staged_mode = on,
            _ => {}
        }
    }

    /// Set the file-based-items flag on the carrying state. Same
    /// `Configuring`-phase write site / no-op-otherwise disposition as
    /// [`Self::set_pre_staged_mode`].
    pub(in crate::secondary) fn set_uses_file_based_items(&mut self, on: bool) {
        match self {
            SecondaryLifecycle::Configuring(cfg) => cfg.uses_file_based_items = on,
            SecondaryLifecycle::Operational(op) => op.uses_file_based_items = on,
            _ => {}
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

/// The one operational-only capability whose state lives entirely on
/// [`OperationalState`].
///
/// Election and keepalive BEHAVIOUR stays on the coordinator (it needs
/// coordinator-level `cluster_state`/transport, not just `OperationalState`
/// data), so there are no election/keepalive methods here — only the
/// deferred-peer-message queue, which is owned wholly by `OperationalState`
/// and so is unreachable before the lifecycle reaches `Operational`.
impl<M: ManagerEndpoint, I: Identifier> OperationalState<M, I> {
    /// Take the queued deferred peer messages, leaving the field empty.
    /// Flushed onto the transport at the top of each operational loop
    /// iteration — a capability that exists only here because the queue
    /// lives only in `OperationalState`.
    pub(in crate::secondary) fn drain_pending_peer_messages(
        &mut self,
    ) -> Vec<(String, DistributedMessage<I>)> {
        std::mem::take(&mut self.pending_peer_messages)
    }
}
