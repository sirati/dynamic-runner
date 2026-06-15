//! `SecondaryCoordinator` — the state-machine that joins the
//! distributed manager mesh as a non-primary participant.
//!
//! # Sub-module layout
//!
//! - [`types`] — public boundary types: `RunOutcome` (per-run control
//!   signal), `SecondaryTerminal` (per-secondary terminal projection),
//!   `SecondaryConfig`, `PeerCertInfo`.
//! - [`coordinator`] — inherent-impl methods on
//!   `SecondaryCoordinator` (constructor, listener registration,
//!   observer-announcer attachment, mode flags, the `run` entry
//!   points).
//! - operational state-machine: [`dispatch`], [`election`],
//!   [`peer`], [`processing`], [`resource`], [`primary`],
//!   [`primary_link`], [`retry_budget`], [`setup`], [`staging`],
//!   [`wire`]. Each owns one concern of the running coordinator.
//!
//! This file holds the `SecondaryCoordinator` struct definition
//! itself plus its two internal support types (`PrimaryInFlightItem`,
//! `FailedTaskEntry`). The struct is the central type of the module
//! — its fields span the full state surface of one secondary in
//! flight — and a per-field split would force every operational
//! handler to thread the relevant subset through method arguments.

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_protocol_manager_worker::ManagerEndpoint;
// Named directly by the `setup_frame_backlog` field (the run-config
// backstop's frame buffer) and re-exported into the module namespace for
// the `#[cfg(test)]` child modules that reach it via `use super::super::*`.
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterState;
use crate::process::{MeshClient, PromotionSignal, RoleInbox};
use crate::zip_extract::ExtractionCache;

use self::lifecycle::{MeshFormation, SecondaryLifecycle};

mod affine_exec;
pub mod control;
mod coordinator;
pub(crate) mod custom_message;
mod dispatch;
// The duplicate-assignment wire marker is emitter-owned (the router's
// TaskAssignment arm emits it) but consumed by the primary's TaskFailed
// classifier; re-export ONLY the constant so `mod dispatch` stays
// private (same pattern as `resource::NO_FAULT_PREEMPT_WIRE_MESSAGE`,
// which rides its module's existing `pub(crate)` visibility).
pub(crate) use dispatch::TASK_ALREADY_HELD_WIRE_MESSAGE;
// Pre-start fence markers (#530a + #530b) — emitter-owned (the router's
// TaskAssignment fence arms emit them), consumed by the primary's
// `handle_task_failed` classifier. Same visibility shape as
// `TASK_ALREADY_HELD_WIRE_MESSAGE`.
pub(crate) use dispatch::{
    TASK_STALE_ADDRESSEE_GEN_WIRE_MESSAGE, TASK_SUPPLANTED_BY_LIVE_HOLDER_WIRE_MESSAGE,
};
mod election;
mod lifecycle;
// `pub(crate)` only so the `cascade_drain_done` pool-cascade primitive
// (the module's single `pub(crate)` item) is reachable by the
// symmetric `crate::primary::hydrate` path. Every other item in the
// module stays `pub(in crate::secondary)`.
pub(crate) mod origination;
mod peer;
mod primary_link;
mod processing;
pub(crate) mod resource;
mod sampler_hooks;
mod setup;
mod setup_deadline;
mod setup_exec;
mod staging;
mod stats;
mod types;
mod wait_marks;
mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

pub use control::SecondaryControlCommand;
pub use primary_link::DEFAULT_PRIMARY_SILENCE_BACKSTOP;
pub use types::{
    FinalizeRunConfigFn, PeerCertInfo, RunOutcome, SecondaryConfig, SecondaryTerminal,
    StagingDispatchContext,
};

/// A task DEFERRED on this secondary because the target worker's
/// per-type subprocess is mid-respawn (the dispatch arm observed
/// [`dynrunner_manager_local::EnsureWorkerOutcome::RespawnInProgress`]).
///
/// Respawn-HOLD (#58): instead of dropping the task or busy-re-bouncing
/// it to the authority while the only worker for that type is
/// respawning, the dispatch arm stashes the resolved task here keyed by
/// `WorkerId`. The `WorkerEvent::Ready` handler picks it up and calls
/// `assign_task` once the slot is observably Idle with the new type
/// bound — no drop, no tight retry loop. If the worker dies before
/// Ready (`WorkerEvent::Disconnected`), the task never ran and the
/// secondary reports it back to the authority as a backpressure-shaped
/// `TaskFailed` so the authority requeues + re-dispatches it.
///
/// Carries everything `assign_task` needs: the resolved [`TaskInfo`],
/// the wire-side `file_hash` (the `active_tasks` key + the recovery
/// wire message's `task_hash`), the scheduler's estimated resource
/// usage, and the `predecessor_outputs` the dispatch arm destructured
/// off the inbound `TaskAssignment` (forwarded verbatim so the
/// dependent worker observes the same shape a same-type fast-path
/// assignment would have produced).
///
/// Unlike the pre-demolition `PendingFirstBind`, there is no
/// `BindSource` discriminator: the secondary is never the authority, so
/// the loss-recovery path is unconditionally "report to the primary
/// role". The authority-self-assign recovery leg the discriminator
/// selected is gone with the authority mirror.
#[derive(Debug, Clone)]
pub(super) struct PendingFirstBind<I: Identifier> {
    pub(super) binary: TaskInfo<I>,
    pub(super) file_hash: String,
    pub(super) estimated: dynrunner_core::ResourceMap,
    pub(super) predecessor_outputs: std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>,
}

/// A work task `B` QUEUED behind this secondary's local SecondaryAffine import
/// (#497 P4).
///
/// When a dispatch assignment for `B` (Phase 5) finds `B` gates on a
/// SecondaryAffine task `I` whose import is not yet locally done on this node,
/// the dependent is parked here (in `OperationalState::affine_running[I]`)
/// until the single per-secondary import for `I` finishes. On the import's
/// success the executor drains the queued dependents and emits one
/// `LocalDependencyReleased` per dependent (→ the primary originates
/// `TaskAssigned` → `B` `InFlight`); on failure it emits one `TaskFailed` per
/// dependent (re-routable per #495).
///
/// Carries EVERYTHING the assignment release (the router's `assign_task`)
/// needs, mirroring [`PendingFirstBind`]: the resolved [`TaskInfo`] for `B`,
/// its wire-side `work_hash` (the `active_tasks` key + the release/queue wire
/// frames' `task_hash`), the chosen `worker_id` slot (pinned onto the
/// originated `TaskAssigned`), the scheduler's `estimated` usage, and the
/// `predecessor_outputs` forwarded verbatim so the dependent worker observes
/// the same shape a non-gated assignment would have produced.
#[derive(Debug, Clone)]
pub(super) struct PendingAffineDependent<I: Identifier> {
    pub(super) work_hash: String,
    pub(super) worker_id: dynrunner_core::WorkerId,
    // The resolved binary + scheduler estimate + predecessor outputs are
    // forwarded VERBATIM to the release dispatch
    // (`dispatch_released_affine_dependent` → `assign_resolved_task`) when the
    // import completes (#497 P5 — same "carries everything assign needs"
    // contract as `PendingFirstBind`).
    pub(super) binary: TaskInfo<I>,
    pub(super) estimated: dynrunner_core::ResourceMap,
    pub(super) predecessor_outputs: std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>,
}

/// Outcome of [`SecondaryCoordinator::ensure_affine_import`] (#497 P4) — what
/// the dispatch router (Phase 5) does with the work task `B` after gating it
/// on its SecondaryAffine dependency `I`.
///
/// The three outcomes are mutually exclusive by construction (the node-local
/// `affine_done` / `affine_running` sets partition the affine hash into
/// done / in-flight / not-yet-seen):
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AffineGateOutcome {
    /// `I` is already locally imported (`affine_hash ∈ affine_done`): the
    /// caller releases `B` STRAIGHT to `InFlight` — no queue, no import, no
    /// `QueuedAfterLocalDependency` report.
    AlreadyDone,
    /// `I`'s import is already in flight (`affine_hash ∈ affine_running`):
    /// `B` was APPENDED to the existing queue and reported
    /// `QueuedAfterLocalDependency`; NO second import was started. `B`
    /// releases when the single in-flight import finishes.
    QueuedBehindRun,
    /// `B` is the FIRST dependent on `I`: it inserted the queue, reported
    /// `QueuedAfterLocalDependency`, and EXACTLY ONE import run was spawned.
    StartedRun,
}

/// The secondary coordinator: connects to primary, manages local workers.
///
/// Unlike `LocalManager` which runs a 5-phase pipeline, the secondary receives
/// individual task assignments from the primary and dispatches them to local
/// workers. It reports completions back and requests more work.
///
/// Generic over:
/// - `M`: manager endpoint for worker communication
/// - `S`: scheduler
/// - `E`: memory estimator
/// - `I`: identifier type
///
/// The coordinator holds NO transport: it reaches the one mesh ONLY
/// through its [`MeshClient`] (egress) + [`RoleInbox`] (ingress), both
/// minted together with the coordinator's `Arc<RoleSlot>` by
/// `Mesh::register_local_role` and handed in at construction. The
/// transport (and the role-demux that resolves a `Destination` to a
/// loopback-or-remote send) lives in the `Node`'s `Mesh`; the
/// coordinator never names it. The manager addresses by typed
/// `Destination`, the egress edge ([`Self::send_to`]) stamps the
/// resolved role-bearing target on the frame, and the mesh decides
/// loopback-vs-remote. The promotion re-route is implicit:
/// `current_primary()` updates on every `PrimaryChanged`, so the next
/// `Destination::Primary` resolves to the new holder.
pub struct SecondaryCoordinator<M, S, E, I>
where
    M: ManagerEndpoint,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    config: SecondaryConfig,

    /// Egress capability over the one mesh. Every operational send goes
    /// through the [`Self::send_to`] egress edge, which resolves a typed
    /// [`dynrunner_protocol_primary_secondary::Destination`] to a concrete
    /// host by reading this coordinator's own role facts, stamps the
    /// role-bearing target on the frame, and hands it to
    /// [`MeshClient::send`] — QUEUED, drained by the mesh-pump, which
    /// decides loopback-vs-remote against the live slot set. The manager
    /// never names a `primary_transport`/`peer_transport` and never
    /// branches on transport-locality. `peer_count`/`has_peer` (if ever
    /// needed) read the pump-published membership view off this client.
    client: MeshClient<I>,

    /// Ingress stream over the one mesh. Every inbound frame addressed to
    /// THIS role's slot arrives via [`RoleInbox::recv`] — the mesh-pump
    /// has already demuxed the wire frame to this slot by its stamped
    /// role-bearing target, so the coordinator receives only frames meant
    /// for it. `None` from `recv()` is the role's teardown signal (every
    /// write end of the slot's inbound dropped).
    inbox: RoleInbox<I>,

    /// Promotion signal egress — the C4 seam. On a self-named
    /// `PrimaryChanged` (an election win via `fire_local_promotion`, or a
    /// transferred primary) the secondary FIRES a [`PromotionSignal`] here
    /// instead of building a primary itself (SUPREME-LAW #3: the secondary
    /// NEVER constructs a primary). The matching receiver lives on the
    /// `Node`, which builds the snapshot-seeded `PrimaryCoordinator` on the
    /// signal. Installed before `run` via
    /// [`Self::register_promotion_signal`] (mirror of
    /// [`Self::register_panik_signal_rx`]). `None` for a coordinator with
    /// no node-wiring (Rust-only unit fixtures that drive promotion through
    /// direct method calls and assert on the CRDT identity advance instead
    /// of a built primary); the fire site is then a best-effort no-op.
    promotion_tx: Option<tokio::sync::mpsc::UnboundedSender<PromotionSignal<I>>>,

    /// The peer-id of the primary this secondary dialled at bootstrap
    /// (the conventional `"primary"`), set via
    /// [`Self::set_bootstrap_primary_id`] alongside the mesh-link
    /// registration. It is the edge's cold-cache fallback when resolving
    /// [`dynrunner_protocol_primary_secondary::Destination::Primary`]
    /// before any `PrimaryChanged` warms `cluster_state.current_primary()`
    /// — the by-id resolution that lets setup frames route to the
    /// dialled primary during the pre-announcement window. `None` for a
    /// secondary with no bootstrap primary link (e.g. channel-only unit
    /// fixtures that never send to the primary before a `PrimaryChanged`).
    bootstrap_primary_id: Option<String>,

    scheduler: S,
    estimator: E,

    // Certificate info for peer connections (set before run)
    peer_cert_info: Option<PeerCertInfo>,

    /// This node's OWN liveness-beacon listener UDP port, set by the run
    /// boundary after it binds the listener (`set_liveness_port`).
    /// Advertised in this node's `CertExchange.liveness_port` so peers
    /// know where to beacon it once it becomes primary. `None` when no
    /// listener was bound (channel-only fixtures, or a deployment without
    /// the beacon).
    liveness_port: Option<u16>,

    /// The runtime→beacon-thread bridge: the current primary's liveness
    /// `SocketAddr`. The coordinator PUBLISHES into it (whenever the
    /// resolved primary or the peer-address view changes); the dedicated
    /// beacon thread READS it each tick. Decouples the beacon (which must
    /// survive runtime CPU-starvation) from the coordinator's mesh/role
    /// state. A clone is handed to `LivenessBeacon::spawn` at the run
    /// boundary; default (empty) until the first publish.
    beacon_target: crate::liveness::BeaconTarget,

    /// The transport-INDEPENDENT view of the CURRENT PRIMARY's liveness:
    /// `node_id -> last beacon receipt Instant`, published by this node's
    /// [`crate::liveness::LivenessListener`] per decoded datagram. The
    /// failover-detector (`run_election_tick` / `record_promotion_vote`)
    /// reads the current primary's entry and UNIONs it with the mesh-frame
    /// legs: a CPU-starved-but-alive primary whose mesh keepalive froze (its
    /// runtime starved by a co-located build) but whose dedicated-thread
    /// beacon still flows is NOT declared dead → no spurious failover. A
    /// clone of the listener's view is installed at the run boundary
    /// (`set_beacon_liveness`); default (empty) for channel-only fixtures,
    /// where the union degrades to mesh-frame-only (its prior behaviour).
    beacon_liveness: crate::liveness::BeaconLiveness,

    /// Node-scoped peer→liveness-address book, captured from `PeerInfo`
    /// (`PeerConnectionInfo.ipv4`/`liveness_port`). The secondary WRITES it
    /// (rebuilt on each `PeerInfo`) and reads it when republishing
    /// `beacon_target` (resolving `current_primary()` → its beacon address).
    /// A SHARED cell (not a plain map) so the co-located promoted
    /// `PrimaryCoordinator` can READ the same book to build its
    /// PRIMARY→secondaries beacon set — the promoted primary observes no
    /// `PeerInfo` itself (it is built from the address-less CRDT), so this
    /// is its only source of its secondaries' raw beacon addresses.
    peer_liveness_addrs: crate::liveness::PeerLivenessAddrs,

    /// Test-only counter: number of `WorkerEvent::TaskCompleted` events
    /// this secondary's OWN workers fired (i.e. tasks actually
    /// dispatched to and executed by this node's worker pool). Distinct
    /// from the cluster-wide terminal set, which is read off the
    /// replicated CRDT (`cluster_state.outcome_counts()`) — the
    /// secondary holds NO per-node completed/failed/total counter.
    /// Pinned by the peer-repoll-on-primary-changed regression test to
    /// assert post-fix distribution across secondaries: pre-fix the
    /// promoted secondary's pool burns through small workloads before
    /// any peer re-polls (production keepalive default = 5s), so peer
    /// `local_tasks_run` stays 0; post-fix every secondary's idle
    /// workers retry against the freshly-identified primary inside
    /// the `PrimaryChanged` apply tick and pick up work.
    #[cfg(test)]
    local_tasks_run: usize,

    // ZIP extraction cache
    extraction_cache: ExtractionCache,

    /// The typed secondary lifecycle. Replaces the scattered
    /// configuration latches (`setup_phase_completed`,
    /// `transfer_complete`, `pre_staged_mode`, `uses_file_based_items`)
    /// and the operational-only state (`pool`,
    /// `active_tasks`, `peer_keepalives`, `primary_last_seen`,
    /// `election`, `pending_peer_messages`, `primary_link`,
    /// `pending_worker_restarts`, `pending_first_bind`) with one state
    /// value whose variants make the system's capability invariants
    /// unrepresentable to violate: no worker pool exists before
    /// `Configuring`, and the `TaskAssignment` / election / keepalive
    /// handlers are reachable only through the `OperationalState`
    /// accessor. See `lifecycle/mod.rs` for the full invariant set.
    pub(in crate::secondary) lifecycle: SecondaryLifecycle<M, I>,

    /// Peer-mesh-formation progress — the orthogonal sub-concern carried
    /// ACROSS the lifecycle's config states (it begins forming on the
    /// unconfigured peer and continues unchanged into `Operational`). It
    /// is NOT a config state and is NOT gated behind configuration: an
    /// unconfigured secondary joins the mesh as far as it can. Modelled
    /// as a sibling field of the lifecycle FSM rather than one of its
    /// variants — see [`MeshFormation`].
    pub(in crate::secondary) mesh: MeshFormation,

    /// Set by handlers that detect an unrecoverable local fault.
    /// The main `process_tasks` loop checks this once per iteration
    /// AFTER the deferred-message flush; if `Some`, the loop returns
    /// `Err(reason)` and the secondary's `run()` propagates that out
    /// so the process exits non-zero.
    ///
    /// One-concern wiring: handlers only WRITE this; the main loop
    /// only READS. Avoids `break` from inside a sub-handler — every
    /// flag-setter stays cancel-safe and the loop owns its own exit
    /// condition.
    pub(super) fatal_exit: Option<String>,

    /// THIS node held the primary role and was DEPOSED — a later applied
    /// `PrimaryChanged` named a DIFFERENT peer while `current_primary()`
    /// still named this node. Written ONLY by the primary-identity apply
    /// seam (`on_primary_identity_advanced`): latched `true` on the
    /// deposing advance, cleared the moment any applied `PrimaryChanged`
    /// names this node again (a re-election with peer agreement, or a
    /// relocation back).
    ///
    /// Read by the failover election's lone-survivor fast path
    /// (`run_election_tick`): a deposed ex-primary may NOT take the
    /// in-tick `failover_quorum(0) == 1` self-promotion — its own
    /// deposition is evidence the fleet elected around it (its view of
    /// the mesh is suspect, e.g. an asymmetric dead leg), so re-candidacy
    /// requires POSITIVE peer agreement (a real `PromotionConfirm`). The
    /// production primary ping-pong (asm-dataset @2212c136) was a deposed
    /// half-partitioned ex-primary metronomically re-asserting itself
    /// through exactly that fast path.
    pub(super) deposed_primary: bool,

    /// "Peer mesh did not form" sentinel. Set true by
    /// `check_peer_mesh_watchdog` when the 30s deadline elapses with
    /// zero connected peers. The watchdog used to make this fatal,
    /// stranding every remaining task in the run; the failure is now
    /// a degraded state instead — task dispatch over WSS still works,
    /// only the peer-mesh-dependent paths (failover election, peer
    /// keepalive broadcasts) fail-loud-or-skip on this flag.
    ///
    /// Read by:
    ///   - the failover election entry in `run_election_tick`: a
    ///     primary-silent transition without a quorum-capable peer
    ///     mesh sets `fatal_exit` (degraded run can't elect a new
    ///     primary, so the only safe move is to bail clearly instead
    ///     of self-promoting solo).
    ///   - the inter-secondary keepalive paths
    ///     (`send_keepalive`'s broadcast, `check_peer_timeouts`):
    ///     skip the cycle. With zero peers these are no-ops anyway,
    ///     but the explicit guard documents the contract and avoids
    ///     a surprise the day a future peer-transport variant
    ///     buffers messages even when nothing is connected.
    ///
    /// Replicated mirror of the cluster ledger. Maintained by applying
    /// every `DistributedMessage::ClusterMutation` observed on the mesh.
    /// Read-only authority-wise on this node — the secondary never
    /// originates a terminal mutation. The authority (the live primary,
    /// or this node's same-node primary once promoted) owns
    /// origination. The secondary DOES originate the one non-authority
    /// mutation the unified model keeps on this side: the panik
    /// self-departure `PeerRemoved` (via
    /// `origination::apply_and_broadcast_mutations`).
    pub(super) cluster_state: ClusterState<I>,

    /// Outbound snapshot-stream driver: serves `RequestSnapshotStream`
    /// pulls (late joiners, behind peers) one bounded package per
    /// process-loop wakeup — see `crate::snapshot_stream`. The loop's
    /// wake arm drains it; the router's request arm feeds it.
    pub(super) snapshot_streams: crate::snapshot_stream::SnapshotStreamResponder,
    /// Settled-CRDT spill driver: sweeps join-fixed-point ledger
    /// entries to the node-local spill file on a cadence (one
    /// `spawn_blocking` write in flight, durable-then-evict) — see
    /// `crate::settled_spill`. The process loop owns its one arm; the
    /// PROMOTION capture pairs the fat snapshot with this store's
    /// read-only base (`settled_base_clone`) so the promoted primary
    /// inherits the settled slice without replay.
    pub(super) settled_spill: crate::settled_spill::SettledSpillDriver,
    /// Inbound snapshot-stream progress (per responder): lets this
    /// node's own anti-entropy pulls RESUME an interrupted stream
    /// (same stream id + cursor) instead of re-pulling from scratch.
    pub(super) inbound_snapshots: crate::snapshot_stream::InboundSnapshotStreams,
    /// Disciplined anti-entropy PULL driver (the #491 storm-killer): the
    /// single-flight probe→select→pull FSM. The digest-receive path feeds
    /// it `note_behind` instead of the eager per-digest immediate pull; the
    /// operational loop's pull arm drives its timers + translates its
    /// directives into `send_to`. See `crate::pull_coordinator`.
    pub(super) pull_coordinator: crate::pull_coordinator::PullCoordinator,

    /// Peer-lifecycle dispatcher channel receiver, paired with the
    /// `lifecycle_tx` installed on `cluster_state` at construction.
    /// Taken out at `run_until_setup_or_done`'s first entry and
    /// handed to
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`] inside
    /// the LocalSet running the secondary's operational loop.
    pub(super) lifecycle_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>>,
    /// Consumers of peer-lifecycle events; same single-shot
    /// `mem::take`-at-run semantics as on `PrimaryCoordinator`. See
    /// `PrimaryCoordinator::peer_lifecycle_listeners` for the
    /// rationale.
    pub(super) peer_lifecycle_listeners: Vec<Box<dyn crate::peer_lifecycle::LifecycleListener>>,

    /// Handle to the peer-lifecycle dispatcher task spawned at
    /// `run_until_setup_or_done`'s first entry. `Some` between spawn
    /// and the `cleanup_lifecycle_dispatcher` abort+await at run
    /// exit; `None` outside an active run. Mirrors the same field on
    /// `PrimaryCoordinator` — see that doc for the leaked-dispatcher
    /// failure mode this guards against.
    pub(super) lifecycle_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Task-completion dispatcher channel receiver, paired with the
    /// `task_completed_tx` installed on `cluster_state` at
    /// construction. Same single-shot / `mem::take`-at-first-entry
    /// semantics as `lifecycle_rx`.
    pub(super) task_completed_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<crate::task_completed::TaskCompletedEvent>>,

    /// Consumers of task-completion events; same single-shot
    /// `mem::take`-at-run semantics as on `PrimaryCoordinator`. See
    /// `PrimaryCoordinator::task_completed_listeners` for the
    /// rationale.
    pub(super) task_completed_listeners: Vec<Box<dyn crate::task_completed::TaskCompletedListener>>,

    /// The upload-action port for setup-task UPLOADS (#336 P1). Consulted
    /// by this secondary's in-process setup executor when an assigned setup
    /// task carries an [`dynrunner_core::UploadFileRef`] (a consumer setup
    /// task whose source-owning affinity is THIS compute node). `None` on a
    /// secondary that hosts no upload setup task (the common case — the
    /// framework auto-staging affinity is the submitter→observer, not a
    /// compute secondary); a no-ref setup task no-op-succeeds regardless.
    /// Set before `run` via [`Self::set_upload_action`]. See
    /// [`crate::upload_action`].
    pub(super) upload_action: crate::upload_action::UploadActionHandle,

    /// The import-action port for SecondaryAffine per-secondary IMPORTS (#497
    /// P4). Consulted by this secondary's run-once affine executor
    /// ([`affine_exec`]) when an assigned work task gates on a SecondaryAffine
    /// dependency whose import is not yet locally done on THIS node. `None` on
    /// a secondary whose work tasks never gate on an import; a registered
    /// importer runs the per-secondary import AT MOST ONCE per affine hash,
    /// gating ALL that node's workers' dependent tasks behind the single run.
    /// Set before `run` via [`Self::set_import_action`]. See
    /// [`crate::affine_action`].
    pub(super) import_action: crate::affine_action::ImportActionHandle<I>,

    /// The OPTIONAL per-(gate,node) satisfied probe (#537). Consulted by
    /// this secondary's run-once affine executor ([`affine_exec`]) BEFORE
    /// the run-once latch — when the probe returns `true`, the gate's hash
    /// enters `affine_done` immediately and the dependent dispatches on
    /// the `AlreadyDone` path with no `QueuedAfterLocalDependency` /
    /// `LocalDependencyReleased` frames and no [`tokio::task::spawn_local`].
    /// `None` (the default) leaves the executor with today's behaviour
    /// bit-for-bit. Set before `run` via
    /// [`Self::set_affine_satisfied_probe`]. See
    /// [`crate::affine_satisfied`].
    pub(super) affine_satisfied_probe:
        crate::affine_satisfied::AffineSatisfiedProbeHandle<I>,

    /// Handle to the task-completion dispatcher task. Mirrors
    /// `lifecycle_dispatcher_handle` — same Drop-vs-explicit cleanup
    /// rationale.
    pub(super) task_completed_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Worker custom-message dispatcher channel SENDER, paired with
    /// `worker_message_rx`. The worker-event bridge
    /// (`processing/worker_event.rs`, the `WorkerEvent::CustomMessage`
    /// arm) enqueues one [`crate::worker_messages::WorkerCustomMessage`]
    /// per inbound worker custom frame; the dispatcher task drains and
    /// fans out to the registered listeners OFF the operational loop
    /// (the consumer's `worker_message_listener` runs Python). The
    /// causal fence's pre-terminal flush
    /// (`processing/process_tasks.rs`) enqueues
    /// [`crate::worker_messages::WorkerMessageItem::FlushBarrier`]
    /// tokens on the same channel — FIFO order is what makes the
    /// barrier ack an ordering proof.
    pub(super) worker_message_tx:
        tokio::sync::mpsc::UnboundedSender<crate::worker_messages::WorkerMessageItem>,

    /// Worker custom-message dispatcher channel receiver. Same
    /// single-shot / `mem::take`-at-first-entry semantics as
    /// `lifecycle_rx` / `task_completed_rx`.
    pub(super) worker_message_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::worker_messages::WorkerMessageItem>,
    >,

    /// Consumers of worker custom-message events; same single-shot
    /// `mem::take`-at-run semantics as `task_completed_listeners`.
    pub(super) worker_message_listeners:
        Vec<Box<dyn crate::worker_messages::WorkerMessageListener>>,

    /// Handle to the worker-message dispatcher task. Mirrors
    /// `task_completed_dispatcher_handle` — same Drop-vs-explicit
    /// cleanup rationale.
    pub(super) worker_message_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

    /// Secondary control-plane ingress SENDER (cloned to external
    /// surfaces via [`Self::secondary_control_sender`] — today the
    /// PyO3 `SecondaryHandle.send_to_worker`). Commands land on
    /// `secondary_control_rx` and are drained by a dedicated
    /// `process_tasks` select arm, so external callers act on this
    /// node's workers WITHOUT touching the pool from a foreign task
    /// (the dispatch-decoupling law).
    pub(super) secondary_control_tx:
        tokio::sync::mpsc::UnboundedSender<control::SecondaryControlCommand>,

    /// Secondary control-plane ingress receiver. Taken into a
    /// loop-local at `process_tasks` entry (the same take-once
    /// discipline as `fatal_exit_signal_rx`).
    pub(super) secondary_control_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<control::SecondaryControlCommand>>,

    /// Off-loop SecondaryAffine-import completion SENDER (#497 P5). Cloned
    /// into each detached `spawn_local` import task by
    /// [`Self::drive_affine_import`]; the task computes the classified
    /// outcome and posts one
    /// [`affine_exec::AffineImportComplete`](crate::secondary::affine_exec)
    /// per import. Mirrors the worker-completion mechanism (the pool's
    /// `event_tx` cloned into each worker monitor task): the import runs OFF
    /// the coordinator loop so a multi-GB `nix-store --import` never blocks it,
    /// and the completion lands back on the loop's `select!` arm via this
    /// channel. Unbounded for the same reason as the other dispatcher channels
    /// — the producing import task must never block; the volume is bounded by
    /// the number of distinct per-secondary imports (one send per import).
    pub(super) affine_import_tx:
        tokio::sync::mpsc::UnboundedSender<affine_exec::AffineImportComplete>,

    /// Off-loop SecondaryAffine-import completion receiver. Taken into a
    /// loop-local at `process_tasks` entry (the same take-once discipline as
    /// `secondary_control_rx`); the operational `select!` arm drains it and
    /// runs the on-loop release ([`Self::complete_affine_import`]).
    pub(super) affine_import_rx:
        Option<tokio::sync::mpsc::UnboundedReceiver<affine_exec::AffineImportComplete>>,

    /// Announcer-outbox sender. Cloned out via
    /// [`Self::attach_observer_announcer`] into the
    /// [`crate::observer::announcer::PeerMeshAnnouncerSender`] held
    /// by the spawned announcer task. The matching receiver is
    /// drained by the operational `select!` arm in `process_tasks`,
    /// which dequeues each [`crate::observer::announcer::AnnouncerOutboxItem`]
    /// and forwards it onto `send_to(Destination::Primary, msg)`,
    /// returning the outcome through the item's `reply` oneshot.
    ///
    /// `None` outside an active observer wiring — non-observer
    /// secondaries (and observer coordinators whose caller hasn't
    /// attached the announcer) never construct the outbox, so the
    /// select arm parks on `pending()` instead of polling a dead
    /// channel.
    pub(super) announcer_outbox_tx:
        Option<tokio::sync::mpsc::Sender<crate::observer::announcer::AnnouncerOutboxItem<I>>>,

    /// Announcer-outbox receiver, paired with `announcer_outbox_tx`.
    /// Built in [`Self::attach_observer_announcer`] (so non-observer
    /// secondaries don't allocate a channel they'll never use). Taken
    /// out at `process_tasks`' first entry into the drain arm and
    /// held locally for the duration of the loop — same shape as
    /// `command_rx`/`matcher_trigger_rx` on the primary. `None`
    /// outside the attached-observer window or once the loop has
    /// taken ownership.
    pub(super) announcer_outbox_rx:
        Option<tokio::sync::mpsc::Receiver<crate::observer::announcer::AnnouncerOutboxItem<I>>>,

    /// Panik-watcher signal receiver — the PRE-RUN REGISTRATION SLOT only.
    /// Installed via [`Self::register_panik_signal_rx`] before
    /// `run_until_setup_or_done` (typically from the PyO3 wrapper which
    /// spawns [`crate::panik_watcher::spawn_panik_watcher`] at `run()` start
    /// and threads the receiver into the inner coordinator). `None` when the
    /// operator did not pass any panik-file paths (and SIGTERM listening is
    /// off) — the `process_tasks` select! arm parks on `pending().await` and
    /// never fires in that case.
    ///
    /// `take`-n ONCE at the first `process_tasks` entry (normal OR observer)
    /// into the loop-local panik arm and moved into
    /// [`super::lifecycle::OperationalState::panik_signal_rx`], its RESUMABLE
    /// home. This coordinator slot is therefore `None` from the first entry
    /// onward; the live receiver lives on `OperationalState` thereafter.
    /// Re-attaching the `Option` from this struct field on every iteration
    /// would race the take/put with the arm's cancel-on-fire semantics,
    /// hence the loop owns it across `select!` iterations.
    pub(super) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,

    /// #571 — setup-phase tunnel-gave-up signal. Fires `Ok(())` if the
    /// background bootstrap bring-up dial exhausts its deadline without
    /// ever connecting (the tunnel never appeared, the submitter crashed /
    /// SSH dropped / gateway rebooted mid-run).
    ///
    /// Installed via [`Self::register_tunnel_gave_up_rx`] before
    /// `run_until_setup_or_done` (the pyo3 wrapper extracts it from the
    /// `SecondaryMeshBundle` returned by
    /// `transport_factory::dial_secondary_mesh`). `None` when no receiver
    /// was registered (channel-only fixtures, observer paths) — the
    /// `wait_for_setup` arm parks on `pending()`.
    ///
    /// Consumed ONCE in `wait_for_setup`: on fire the arm logs to
    /// `IMPORTANT_TARGET` ("setup-phase tunnel-wait deadline expired") and
    /// returns `Err(...)` so the secondary exits non-zero and releases
    /// its SLURM allocation. `None`-s itself at the `wait_for_setup` entry
    /// so only the setup phase observes it (post-`Operational` the signal
    /// is irrelevant — the tunnel connected).
    pub(super) tunnel_gave_up_rx: Option<tokio::sync::oneshot::Receiver<()>>,

    /// Externally-armed fatal-exit signal. Installed via
    /// [`Self::register_fatal_exit_signal_rx`] before
    /// `run_until_setup_or_done`. A run-loop-external policy (the
    /// observer's invalid_task monitor — a windowed-failure-collector
    /// action running on the task-completed dispatcher task, which holds
    /// no `&mut self` to write `fatal_exit` directly) sends a reason
    /// string here; the `process_tasks` select! arm receives it and
    /// latches `self.fatal_exit`, exiting the run non-zero. Mirrors
    /// `panik_signal_rx` exactly: `None` when no such policy was attached
    /// (regular secondaries, Rust-only tests) and the arm parks on
    /// `pending().await`.
    ///
    /// An mpsc (not a oneshot) receiver because the sender side is a
    /// `CollectorPolicy` action that fires best-effort `send`; the
    /// receiver consumes the first message and the run exits, so only the
    /// first send is ever observed. Taken out for the duration of
    /// `process_tasks` so the arm's `await` owns it across iterations,
    /// same discipline as `panik_signal_rx`.
    pub(super) fatal_exit_signal_rx: Option<tokio::sync::mpsc::UnboundedReceiver<String>>,

    /// Lifecycle hook invoked when this node owns the authoritative
    /// primary pool and a phase reaches `Drained`. The PyO3 wrapper
    /// installs a GIL-reacquiring closure that calls Python's
    /// `task.on_phase_end(phase_id, completed, failed)`.
    ///
    /// R4 SEAM: the secondary holds NO authority, so it has no
    /// phase-machine to fire this from. The fire site is the
    /// authoritative `PrimaryCoordinator`, which owns `on_phase_end` +
    /// the phase machine; pyo3 registers the lifecycle hook on the
    /// PRIMARY, not the secondary. Kept here only as the wiring anchor
    /// R4 re-homes.
    #[allow(dead_code)] // TODO(R4): re-home lifecycle registration to PrimaryCoordinator
    pub(super) on_phase_end: Option<crate::primary::OnPhaseEnd>,

    /// Lifecycle hook invoked when this node owns the authoritative
    /// primary pool and a phase flips Blocked → Active. Sibling of
    /// `on_phase_end`; same R4-seam disposition.
    #[allow(dead_code)] // TODO(R4): re-home lifecycle registration to PrimaryCoordinator
    pub(super) on_phase_start: Option<crate::primary::OnPhaseStart>,

    /// The consumer's run-config finalize policy — re-derives the per-type
    /// worker `cmd_args` from the delivered `forwarded_argv` and swaps them
    /// into the worker-command source the factory reads. Installed via
    /// [`Self::register_finalize_run_config`] BEFORE `run`; `Some` on the
    /// run-config-bearing consumer path (the pyo3 wrapper supplies a closure
    /// that re-parses Python's argparse + rebuilds the cmd_args under the
    /// GIL). Fired ONCE at the `AwaitingPrimary → Configuring` transition,
    /// BEFORE [`Self::initialize_workers`] reads the cmd_args at worker
    /// spawn, so the swapped command is live for the initial pool. `None` only
    /// for callers that register no closure at all (legacy Rust-only fixtures /
    /// out-of-tree direct drivers), which skips the seam. The `args=` consumer
    /// path (compiler_suit) registers an IDENTITY finalizer (Some) — the seam
    /// fires but is a faithful no-op (byte-identical rebuild).
    pub(super) finalize_run_config: Option<super::FinalizeRunConfigFn>,

    /// Latch set true by [`Self::store_pushed_run_config`] the first time an
    /// inbound `RunConfig` lands (a primary PUSH or a `RequestRunConfig`
    /// answer). Drives the finalize backstop: at the
    /// `AwaitingPrimary → Configuring` transition, if the push has NOT yet
    /// landed, the secondary actively requests the run-config in-band before
    /// firing the finalize, so the per-type `cmd_args` are derived from the
    /// delivered argv rather than the empty boot CLI. An EMPTY pushed argv is
    /// a valid landing (compiler_suit-shape), so emptiness cannot be the
    /// discriminator — this dedicated bool is.
    pub(super) forwarded_argv_was_pushed: bool,

    /// Setup frames the run-config backstop pulled off the inbox while
    /// bounded-waiting for the `RunConfig` answer. The backstop must drain the
    /// inbox to find the answer, but the SETUP frames it encounters
    /// (PeerInfo / InitialAssignment / TransferComplete) belong to
    /// `wait_for_setup`'s own progress loop; buffering them here and draining
    /// this backlog before each fresh `inbox.recv()` keeps the backstop from
    /// stealing frames the setup loop needs (no frame loss, no duplicated
    /// setup-handling logic). Empty in the common path (the push lands before
    /// the first setup frame, so the backstop never recvs).
    pub(super) setup_frame_backlog: std::collections::VecDeque<DistributedMessage<I>>,

    /// The re-armable pre-`Operational` deadline (the
    /// `unconfigured_deadline` made structural — see
    /// [`setup_deadline::SetupDeadline`]). The orchestration
    /// (`run_until_setup_or_done_inner`) arms it at setup entry and
    /// sleeps against a clone; `wait_for_setup` EXTENDS it on every frame
    /// whose sender is the primary, so the deadline measures PRIMARY
    /// SILENCE (the dead-primary detection it exists for), never
    /// slow-fleet assembly (the asm-dataset LMU 15-secondary fleet
    /// death).
    pub(super) setup_deadline: setup_deadline::SetupDeadline,

    /// SETUP-PHASE failover election state (#420 face (c)). `None` in the
    /// normal setup wait; `Some` once a `wait_for_setup` secondary whose
    /// primary has gone permanently silent (formed mesh, no primary frames for
    /// half the unconfigured deadline) ARMS a failover election WITHOUT
    /// transitioning its lifecycle to `Operational`.
    ///
    /// Why a coordinator field and not the lifecycle's `OperationalState`: the
    /// LOSERS of a setup-phase election must STAY in `wait_for_setup` so the
    /// elected primary's re-sent setup trio (PeerInfo / InitialAssignment /
    /// TransferComplete — its `PromotedDestination` arm re-runs the full
    /// pre-loop chain) completes their handshake and spawns their workers (the
    /// relocation-handoff path is the precedent). A permanent transition to
    /// `Operational` would no-op `enter_configuring_on_first_primary_frame`
    /// (it fires only from `AwaitingPrimary`), so a loser that went Operational
    /// would DROP the re-sent trio and sit worker-less forever. Parking the
    /// election state HERE keeps the lifecycle in its setup variant — only the
    /// WINNER leaves setup (via `fire_local_promotion` → `PromotionSignal` →
    /// the Node builds the primary). The election LOGIC is unchanged: every
    /// election method reads its four fields through the op-OR-setup accessors
    /// ([`Self::election_state`] etc.), so there is ONE election code path with
    /// the state owned by whichever regime drives it.
    pub(in crate::secondary) setup_election: Option<election::SetupElection<I>>,

    /// Throttle for the recurring "primary silent past the election threshold
    /// but zero membership evidence" WARN the setup-election arm emits while
    /// it declines to arm (a never-welcomed node keeps hitting the arm path
    /// every keepalive tick once the silence threshold is crossed — loud
    /// while the fault persists, never per tick). Lives next to the election
    /// state it narrates.
    pub(in crate::secondary) setup_election_seedless_warn: crate::warn_throttle::WarnThrottle,

    /// The shared own-tick-health authority (`crate::own_tick_health`): the
    /// SAME primitive the primary's heartbeat sweep consumes. The
    /// keepalive-arm tick (and the setup-phase election tick) feeds each
    /// tick's instant to it; a lagged tick means THIS node's runtime was
    /// frozen/starved, so every silence age it would measure — the
    /// primary-silence backstop (leg B), the peer-keepalive reaper, the
    /// setup-election arm — is inflated by its OWN stall, not the peer's
    /// silence. The authority re-bases its trustworthy floor on the lag, and
    /// every silence judgment reads `now - trustworthy_anchor(last_evidence)`
    /// so the starved window contributes ZERO silence: peers are judged from
    /// fresh, post-lag evidence (a genuine death is detected one healthy
    /// cadence window later — correctness over speed).
    pub(in crate::secondary) own_tick_health: crate::own_tick_health::OwnTickHealth,

    /// Cross-thread / cross-runtime ingress for the `PrimaryHandle`
    /// PyO3 surface (when the handle was minted from a
    /// `PySecondaryCoordinator`).
    ///
    /// R4 SEAM: the secondary no longer drains this channel — the
    /// externally-issued `PrimaryCommand`s are authority mutations whose
    /// only correct owner is the `PrimaryCoordinator`. Kept here only as
    /// the wiring anchor R4 re-homes (so the PyO3 `command_sender()`
    /// clone keeps a stable type until then).
    #[allow(dead_code)] // TODO(R4): re-home the command channel to PrimaryCoordinator
    pub(super) command_rx: Option<tokio::sync::mpsc::Receiver<crate::primary::PrimaryCommand<I>>>,

    /// Sender side of the secondary's command channel, cloned to
    /// consumers via `command_sender()`. Same R4-seam disposition as
    /// `command_rx`.
    #[allow(dead_code)] // TODO(R4): re-home the command channel to PrimaryCoordinator
    pub(super) command_tx: tokio::sync::mpsc::Sender<crate::primary::PrimaryCommand<I>>,

    /// Per-task memory-profile sampler. `Some` iff
    /// [`SecondaryConfig::output_dir`] was set when the secondary's
    /// `run_until_setup_or_done` started — sampler construction
    /// defers to the post-`initialize_workers` step because
    /// `MemProfileSampler::spawn` requires a running tokio runtime
    /// (the coordinator's caller may not be inside one when
    /// `new()` runs).
    ///
    /// Owns one background tokio task that ticks at the configured
    /// `sample_interval` (1 s by default), reads each active worker's
    /// cgroup memory stats, and writes zstd-framed JSONL through the
    /// sampler's writers. Drained + joined via
    /// [`Self::shutdown_sampler_if_present`] at the start of every
    /// terminal teardown sequence — BEFORE the pool's
    /// `SubcgroupHandle::drop` rmdir's the leaf cgroups the sampler
    /// would otherwise still be sampling from.
    ///
    /// Mirrors the same field on
    /// [`dynrunner_manager_local::manager::LocalManager`].
    pub(super) sampler: Option<dynrunner_manager_local::memprofile::MemProfileSampler>,

    /// The consumer's run configuration — the byte-identical token
    /// sequence the framework forwards onto a joining / respawned /
    /// promoted node's command line. A NODE-LOCAL launch constant
    /// seeded from `config.forwarded_argv` at construction; NOT
    /// replicated lattice data, so it never touches `cluster_state`.
    ///
    /// A SHARED handle (single source of truth) so the three readers
    /// observe the SAME delivered value with exactly one writer
    /// ([`Self::store_pushed_run_config`]):
    ///   * the `RequestRunConfig` responder (the router's
    ///     `DistributedMessage::RequestRunConfig` arm) reads it READ-ONLY
    ///     and unicasts it back to a requesting peer — available on this
    ///     secondary role so a cold-start fetch is answerable before any
    ///     primary exists / promotes;
    ///   * the finalize-run-config fire feeds the delivered argv to the
    ///     consumer's reparse closure;
    ///   * the promotion recipe (built in the pyo3 wrapper) reads it at
    ///     promotion time so a node promoted to primary threads the
    ///     DELIVERED argv (post-push) into its `PrimaryConfig.forwarded_argv`,
    ///     not the stale boot copy.
    ///
    /// `Arc<Mutex<_>>` rather than the plain `Vec` it replaces because the
    /// promotion recipe is a standalone closure that cannot borrow the
    /// coordinator: it captures a clone of THIS handle and reads it at the
    /// promotion instant (which is always after the push has landed). Empty
    /// for a run with no forwarded args.
    pub(super) forwarded_argv: std::sync::Arc<std::sync::Mutex<Vec<String>>>,

    /// The run-config dispatch flags (`pre_staged_mode` /
    /// `uses_file_based_items`) the primary stamped into this secondary's
    /// `InitialAssignment`. A NODE-LOCAL run constant (NOT replicated lattice
    /// data), so it lives on the coordinator, never in `cluster_state`.
    ///
    /// SINGLE source of truth with exactly one writer
    /// ([`Self::set_staging_dispatch_context`], fired from the
    /// `InitialAssignment` handler) and two readers:
    ///   * the dispatch resolver ([`Self::resolve_for_dispatch`]) — chooses
    ///     whether to do filesystem IO / content-hash verification on the
    ///     wire `local_path`;
    ///   * the promotion recipe (built in the pyo3 wrapper) — reads it at the
    ///     promotion instant and threads it into the promoted
    ///     `PrimaryConfig.uses_file_based_items` / `.source_pre_staged_root`
    ///     so the relocated primary's own `InitialAssignment` re-stamps the
    ///     SAME flags the submitter primary did (without it, a promoted
    ///     primary stamps the defaults and the worker re-requires a StageFile
    ///     for a no-file / bind-mounted item — the relocate-staging bug).
    ///
    /// `Arc<Mutex<_>>` for the same reason `forwarded_argv` is: the promotion
    /// recipe is a standalone closure that cannot borrow the coordinator, so
    /// it captures a clone of this handle and reads it at promotion time
    /// (always after the `InitialAssignment` landed). Seeded `Default`
    /// (file-based, not pre-staged) — the historical pre-`InitialAssignment`
    /// contract — until the handler overwrites it.
    pub(super) staging_dispatch_context:
        std::sync::Arc<std::sync::Mutex<types::StagingDispatchContext>>,

    /// Buffered-terminal-replay queue — the reporting concern's retain
    /// buffer for a terminal-bearing primary-bound report that has not
    /// yet been CONFIRMED delivered at the authority, for either of two
    /// retention reasons ([`resource::RetainedSendState`]):
    ///   * `NoRoute` — the send was ABSORBED on a transient no-route
    ///     (nothing was ever queued);
    ///   * `AwaitingAck` — the send returned `Ok` but the primary's
    ///     app-level `TerminalAck { delivery_seq }` has not landed
    ///     (#352): on a blackholed-but-live QUIC leg `write_all` buffers
    ///     locally and returns `Ok` without delivering, and the route is
    ///     not pruned until the 60s idle timeout — so transport success
    ///     proves nothing; only the ack does.
    ///
    /// `send_to_primary` absorbs a no-route `Err` into `Ok(())` so a
    /// primary-loss never fatals / false-failovers a voter (the absorb is
    /// a failover SIGNAL, not a run-fatal error). Pre-replay the absorbed
    /// frame was genuinely LOST: a `TaskComplete` / `TaskFailed` cleared
    /// here LOCALLY (e.g. the replacement sweep clears `active_tasks`
    /// before reporting) but never reached the authority, so the
    /// primary's in-flight entry strands forever (phantom-busy; the phase
    /// barrier wedges). This buffer is that fix: every TERMINAL-bearing
    /// (`DistributedMessage::requires_delivery_ack`) report is RETAINED
    /// here from the send until its ack.
    ///
    /// Scope: ONLY terminal-bearing reports are buffered — keepalives /
    /// stats / capacity `TaskRequest`s are legitimately droppable (a
    /// missed one is re-emitted next tick) and never land here. The gate
    /// is at the send chokepoint in `send_to_primary` (see
    /// `resource.rs::send_to_primary`); every other primary-bound send
    /// kind flows through unchanged.
    ///
    /// Drained FIFO, retrying FOREVER until acked, on TWO triggers: the
    /// operational loop's replay WAKE arm (`process_tasks` parks on
    /// `next_report_replay_due`, the buffer-wide minimum of the
    /// per-entry `next_due` deadlines — it fires when an entry is due,
    /// never per tick) AND the primary-link-recovery edge
    /// (`record_primary_message`, where Suspecting/Voting/Candidate →
    /// Normal; drains via `drain_report_replays_now`, overriding the
    /// schedule for a prompt route-restored retry). A drain re-sends
    /// the DUE entries only — due-ness is each entry's own capped
    /// exponential backoff schedule (`resource::replay_backoff_delay`):
    /// a fresh no-route retention is due immediately, a sent entry one
    /// `delivery_ack_timeout` after its send, and every replay pushes
    /// the slot out (`ack_timeout` → 2× → 4× … capped) so a
    /// never-acking destination costs one frame per cap, not one per
    /// loop tick (the production replay flood). The ONLY drop site is
    /// `ack_delivery` (an inbound `TerminalAck` matching the entry's
    /// `delivery_seq` exactly).
    ///
    /// Each re-send carries the SAME `delivery_seq` and re-retains at
    /// the back (never drop, never reorder within a single drain). A
    /// re-delivery to a NEW primary after failover works automatically —
    /// `send_to_primary` routes `Destination::Primary` to
    /// `current_primary()` at the egress edge, so the retained frame
    /// follows the role. The authority dedupes a duplicate landing
    /// (hash-keyed `completed_tasks` / `failed_tasks`; idempotent
    /// backpressure requeue gated on `free_slot_on_terminal`'s held-hash
    /// match) and ACKS every landing including dedup-dropped duplicates,
    /// so an at-most-once-effective re-delivery is safe even if the
    /// original send had in fact reached an old primary.
    ///
    /// Lives on the coordinator (NOT `OperationalState`) because the
    /// reporting concern that owns `send_to_primary` lives here, and the
    /// buffer is its private mechanism — a drain is a no-op outside an
    /// operational run (no terminal can be produced there), so it needs
    /// no lifecycle gating.
    pub(in crate::secondary) pending_report_replays: Vec<resource::RetainedReport<I>>,

    /// Rate-limit anchor for the replay drain's aggregated "re-sent"
    /// INFO line (`resource::note_replays_for_log`): the instant of the
    /// last emit, `None` before the first. Together with
    /// `replay_log_suppressed` this caps the line at one per
    /// [`resource::REPORT_REPLAY_LOG_WINDOW`] — the production replay
    /// flood logged the per-pass variant 19,437 times in ~5 minutes.
    /// (The per-seq replay-attempt tally for the #366 permanent-failure
    /// escalation lives ON each `resource::RetainedReport` entry, which
    /// is updated in place across replays — no side table.)
    pub(in crate::secondary) replay_log_last_emit: Option<std::time::Instant>,

    /// Re-sends tallied since the last aggregated drain-log emit (the
    /// suppressed count the next emitted line carries). Diagnostic
    /// bookkeeping only — never read by routing, liveness, or the
    /// replay decision itself.
    pub(in crate::secondary) replay_log_suppressed: usize,

    /// Per-secondary monotonic `delivery_seq` counter (#352), owned by
    /// the `send_to_primary` stamping chokepoint: every confirmable
    /// primary-bound report (terminal-bearing, or an IMPORTANT custom
    /// message — F5) is stamped with the next value on its first
    /// send (replays keep their original stamp). Matched by the
    /// primary's echoed `TerminalAck { seq }` to drop the corresponding
    /// `pending_report_replays` entry. Starts at 1 so a `0` never
    /// appears on the wire (`Some(0)` would be valid but a non-zero
    /// floor makes a default-initialised stamp visibly distinct in
    /// logs).
    pub(in crate::secondary) next_delivery_seq: u64,

    /// Per-origin monotonic custom-message sequence (F5), owned by the
    /// [`custom_message`] send seam: every IMPORTANT consumer custom
    /// message this secondary ORIGINATES is stamped with the next
    /// value, so `(secondary_id, msg_seq)` is the cluster-wide
    /// idempotency key the primary's CRDT inbox dedups transport
    /// replays by. Droppables are UNSEQUENCED (`msg_seq = 0`): they are
    /// lost-by-design on no-route/failover, so they must not occupy a
    /// slot in this identity space — the terminal-ordering gate counts
    /// it (a task terminal's `msgs_posted_through` stamp, read here by
    /// `send_to_primary`'s terminal-stamping step), and the dense
    /// important-only space is what makes the CRDT's contiguous-prefix
    /// watermark exact. Distinct from `next_delivery_seq` (the #352
    /// retention/ack key): `msg_seq` identifies the MESSAGE,
    /// `delivery_seq` one retention-buffer entry. Starts at 1 so the
    /// per-origin handled watermark's "all of `1..=w` handled"
    /// contiguous-prefix walk has a fixed base.
    pub(in crate::secondary) next_custom_msg_seq: u64,

    /// How long a sent terminal waits for its `TerminalAck` before the
    /// drain treats it as no-route-equivalent and replays it. Seeded
    /// from [`resource::DEFAULT_DELIVERY_ACK_TIMEOUT`] (see its doc for
    /// the 15s justification against the 60s QUIC idle timeout); tests
    /// drive it sub-second directly. Delivery bookkeeping only — never
    /// an input to the failover-health probe.
    pub(in crate::secondary) delivery_ack_timeout: std::time::Duration,

    /// Per-iteration `select!`-arm accounting for the secondary's
    /// `process_tasks` operational loop. The co-located topology runs the
    /// promoted primary AND this secondary on ONE runtime, so the production
    /// ingest wedge could in principle present on either loop's arms; this is
    /// the secondary's twin of the primary's `op_loop_arm_stats`. Published at
    /// `process_tasks` entry, observation-only (never read by control flow),
    /// `None` until the loop runs. See [`crate::oploop_instrumentation`].
    pub(in crate::secondary) op_loop_arm_stats:
        Option<std::sync::Arc<crate::oploop_instrumentation::OpLoopArmStats>>,

    /// Optional shared bridge to the off-runtime [`crate::runtime_watchdog`]
    /// (set at node bootstrap via [`Self::set_op_loop_arm_stats_cell`]). The
    /// `process_tasks` loop publishes its live arm stats into this cell on
    /// entry and clears them on exit, so the single watchdog can dump this
    /// loop's hot arm at a freeze — the co-located twin of the primary's cell.
    /// See [`crate::oploop_instrumentation::OpLoopArmStatsCell`].
    pub(in crate::secondary) op_loop_arm_stats_cell:
        Option<crate::oploop_instrumentation::OpLoopArmStatsCell>,

    /// Cadence state for the periodic collection-stats line — the
    /// accumulation-visibility twin of `op_loop_arm_stats` for the
    /// unbounded-by-design collections this coordinator holds (the
    /// replicated custom-message inbox mirror, the confirmable-report
    /// replay buffer, the role inbox). Driven once per keepalive tick
    /// (`observe_collection_stats`); policy + thresholds live in
    /// [`crate::collection_stats`].
    pub(in crate::secondary) collection_stats: crate::collection_stats::CollectionStatsEmitter,
}
