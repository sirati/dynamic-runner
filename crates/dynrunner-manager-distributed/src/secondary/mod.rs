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
// Re-exported into the module namespace for the `#[cfg(test)]` child
// modules that reach it via `use super::super::*` (the production code in
// this module no longer names `DistributedMessage` directly).
#[cfg(test)]
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterState;
use crate::process::{MeshClient, PromotionSignal, RoleInbox};
use crate::zip_extract::ExtractionCache;

use self::lifecycle::{MeshFormation, SecondaryLifecycle};

mod coordinator;
mod dispatch;
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
mod staging;
mod types;
mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

pub use primary_link::DEFAULT_PRIMARY_SILENCE_BACKSTOP;
pub use types::{PeerCertInfo, RunOutcome, SecondaryConfig, SecondaryTerminal};

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
    promotion_tx: Option<tokio::sync::mpsc::UnboundedSender<PromotionSignal>>,

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
    /// `transfer_complete`, `pre_staged_mode`, `uses_file_based_items`,
    /// `setup_discovery_done`) and the operational-only state (`pool`,
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
    /// origination. The secondary DOES originate the two non-authority
    /// mutations the unified model keeps on this side: the
    /// `ingest_setup_discovery` `PhaseDepsSet + TaskAdded` batch and the
    /// panik self-departure `PeerRemoved` (both via
    /// `origination::apply_and_broadcast_mutations`).
    pub(super) cluster_state: ClusterState<I>,

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
    /// failure mode this guards against. The re-entrant
    /// `RunOutcome::SetupPending` yield path deliberately does NOT
    /// clean up: the caller will re-enter and the dispatcher is
    /// still useful (and the receiver has already been moved into
    /// the task, so it can't be re-spawned).
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

    /// Handle to the task-completion dispatcher task. Mirrors
    /// `lifecycle_dispatcher_handle` — same Drop-vs-explicit cleanup
    /// rationale, same re-entrant SetupPending non-cleanup
    /// discipline.
    pub(super) task_completed_dispatcher_handle: Option<tokio::task::JoinHandle<()>>,

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
    /// home: a `SetupPending` re-entry is a real second consumption, and on a
    /// regular (non-observer) pre-staged secondary this is the SOLE in-loop
    /// path by which a SIGTERM (or sentinel file) delivered after the
    /// discovery yield reaches the graceful-shutdown cascade, so it must
    /// survive the yield. This coordinator slot is therefore `None` from the
    /// first entry onward; the live receiver lives on `OperationalState`
    /// thereafter. Re-attaching the `Option` from this struct field on every
    /// iteration would race the take/put with the arm's cancel-on-fire
    /// semantics, hence the loop owns it across `select!` iterations.
    pub(super) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,

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
}
