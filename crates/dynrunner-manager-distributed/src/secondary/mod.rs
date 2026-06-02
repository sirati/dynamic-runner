//! `SecondaryCoordinator` — the state-machine that joins the
//! distributed manager mesh as a non-primary participant.
//!
//! # Sub-module layout
//!
//! - [`types`] — public boundary types: `RunOutcome`,
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

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use dynrunner_core::{Identifier, TaskInfo, WorkerId};
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterState;
use crate::zip_extract::ExtractionCache;

use self::primary_link::PrimaryLink;

mod coordinator;
mod dispatch;
mod election;
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

pub use types::{PeerCertInfo, RunOutcome, SecondaryConfig};

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
    pub(super) predecessor_outputs:
        std::collections::BTreeMap<String, dynrunner_core::TaskOutputs>,
}

/// The secondary coordinator: connects to primary, manages local workers.
///
/// Unlike `LocalManager` which runs a 5-phase pipeline, the secondary receives
/// individual task assignments from the primary and dispatches them to local
/// workers. It reports completions back and requests more work.
///
/// Generic over:
/// - `Tr`: the unified transport (a `PeerTransport<I>`). One opaque
///   handle: the manager addresses by [`Address`] and never branches
///   on transport locality. Routing (local-vs-remote primary AND the
///   promotion re-route) lives entirely inside the transport — see
///   `dynrunner_transport_tunnel::UnifiedSecondaryTransport`.
/// - `M`: manager endpoint for worker communication
/// - `S`: scheduler
/// - `E`: memory estimator
/// - `I`: identifier type
pub struct SecondaryCoordinator<Tr, M, S, E, I>
where
    Tr: PeerTransport<I>,
    M: ManagerEndpoint,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    config: SecondaryConfig,

    /// The single opaque transport handle. Every operational send is
    /// an [`Address`]-addressed `transport.send(..)`; every inbound
    /// frame arrives via `transport.recv_peer()` (which merges the
    /// uplink + mesh streams internally). The manager never names a
    /// `primary_transport`/`peer_transport`, never reads
    /// `peer_count()` for routing, and never branches on
    /// transport-locality — the unified transport owns all of that.
    transport: Tr,

    scheduler: S,
    estimator: E,

    // Certificate info for peer connections (set before run)
    peer_cert_info: Option<PeerCertInfo>,

    // Workers
    pool: WorkerPool<M, I>,

    // Task tracking: file_hash -> worker_id (this node's OWN in-flight
    // worker assignments — own-worker management, not authority).
    active_tasks: HashMap<String, WorkerId>,

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
    /// the PromotePrimary dispatch tick and pick up work.
    #[cfg(test)]
    local_tasks_run: usize,

    // State
    transfer_complete: bool,

    // ZIP extraction cache
    extraction_cache: ExtractionCache,

    /// Pre-staged source mode flag — set from the
    /// `InitialAssignment.pre_staged_mode` the primary sends. When
    /// true, file resolution skips the extraction-cache hash check
    /// and trusts `src_network/<local_path>` directly. Off until
    /// the InitialAssignment lands, which is fine: no TaskAssignment
    /// can arrive before InitialAssignment.
    pre_staged_mode: bool,

    /// Whether dispatched task items are backed by real files (the
    /// historical contract). Set from
    /// `InitialAssignment.uses_file_based_items`. When false, the
    /// extraction-cache resolution is skipped entirely and the
    /// wire's `local_path` is passed through to the worker as an
    /// opaque identifier — no `stat()`, no hash, no `.exists()`
    /// check. Defaults to TRUE before InitialAssignment lands so
    /// the historical behaviour remains in place.
    uses_file_based_items: bool,

    // Peer keepalive tracking: peer_id -> last_seen timestamp
    peer_keepalives: HashMap<String, f64>,

    // Primary keepalive tracking for failover detection (F2). `None` until
    // the first primary message arrives. Updated on every primary message,
    // not just `Keepalive`, so an actively-routing primary doesn't get
    // falsely declared dead.
    primary_last_seen: Option<Instant>,

    // Failover election state (F2). Defaults to Normal until the primary
    // misses keepalives.
    election: election::ElectionState,

    // Deferred peer messages to send (queued from sync handlers)
    pending_peer_messages: Vec<(String, DistributedMessage<I>)>,

    /// Routing target + per-worker request rate limiting for the
    /// secondary→primary link. Single owner of "where do operational
    /// sends go?" and "is this worker's request allowed to fire yet?"
    /// — see `primary_link.rs` for the boundary contract.
    pub(in crate::secondary) primary_link: PrimaryLink,

    /// One-shot watchdog deadline for "did the peer mesh form?".
    /// Set to `now + 30s` when `wait_for_setup` kicks off the per-peer
    /// dials with at least one peer in the list; cleared on first
    /// keepalive tick after the deadline passes (after the watchdog
    /// has logged its result). `None` means either we haven't reached
    /// the dial step yet, the peer list was empty (single-secondary
    /// runs), or the watchdog has already fired.
    ///
    /// Without this, the per-peer "QUIC to peer X timed out, trying
    /// WSS" / "WSS to peer X also failed" lines are scattered across
    /// the log with no single signal that the secondary is now
    /// running primary-only — operators have to grep + count to
    /// realise. Cohort 4 (tokenizer) hit exactly this: 5 secondaries,
    /// each printed 4 dial-failure lines, and silence after that;
    /// the actual "0 peers connected ⇒ degraded" state was implied.
    peer_mesh_check_at: Option<Instant>,
    /// Number of peers we asked the transport to dial. Used by the
    /// watchdog to phrase the WARN ("0 of N peers reachable") and to
    /// suppress the watchdog when peers is empty (single-secondary).
    peer_dial_count: u32,
    /// One-shot guard: have we already emitted `MeshReady` to the
    /// primary? The primary defers `PromotePrimary` until every
    /// secondary has reported, so duplicate sends would over-count
    /// (harmless on the receiving end today, but the contract is
    /// "exactly once per secondary"). Toggled true the first time
    /// `report_mesh_ready_if_needed` decides the mesh has settled
    /// (mesh formed, watchdog elapsed, or no peers to dial).
    mesh_ready_sent: bool,

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
    /// Distinct from `peer_mesh_check_at`: the watchdog field tracks
    /// the in-flight deadline (cleared when mesh forms OR watchdog
    /// fires). `peer_mesh_degraded` is the post-fire latch that
    /// callers consult to decide whether peer-mesh-dependent
    /// behaviour is available.
    pub(super) peer_mesh_degraded: bool,

    /// Replicated mirror of the cluster ledger. Maintained by applying
    /// every `DistributedMessage::ClusterMutation` the primary
    /// broadcasts. Read-only on this node — only the originator may
    /// produce mutations (Phase L will move the originator-side logic
    /// onto whichever node currently holds the primary role).
    pub(super) cluster_state: ClusterState<I>,

    /// Worker IDs queued for respawn at the next `process_tasks`
    /// tick. Populated by code paths that observe a worker
    /// subprocess as dead WITHOUT a corresponding
    /// `WorkerEvent::Disconnected` arriving on the pool's event
    /// channel:
    ///
    ///   - `handle_primary_task_request`'s self-assign Err arm
    ///     (the local worker's pipe is broken when we try to write
    ///     the next task to it),
    ///   - `dispatch_message`'s peer-assign Err arm (same shape on
    ///     the peer-assigned path).
    ///
    /// In both cases the worker subprocess has voluntarily exited
    /// — typically because `NonRecoverableError` in the task
    /// handler causes the runtime to send the error response,
    /// then the framework's restart-on-next-assignment contract
    /// (see `dynamic_runner.worker.runtime.NonRecoverableError`
    /// docstring) kicks in. The `assign_task` write subsequently
    /// fails on Broken pipe and the worker_id ends up here.
    ///
    /// `process_tasks` drains the set at the end of each tick and
    /// calls `pool.restart_worker(wid, factory, _)` for each
    /// entry, then re-issues a `TaskRequest` so the fresh worker
    /// pulls fresh work from the primary's pool. Idempotent on
    /// duplicate entries — the worker either restarted at the
    /// last drain (set was emptied) or is still queued (no-op
    /// already in flight).
    pub(super) pending_worker_restarts: HashSet<WorkerId>,

    /// Tasks DEFERRED because the target worker's per-type subprocess
    /// is mid-respawn (respawn-HOLD, #58). Keyed by `WorkerId`; the
    /// `WorkerEvent::Ready` handler picks the entry up and assigns it
    /// once the slot is Idle with the new type bound. See
    /// [`PendingFirstBind`] for the full contract.
    pub(super) pending_first_bind: HashMap<WorkerId, PendingFirstBind<I>>,

    /// Re-entry guard for `run_until_setup_or_done`. The first call
    /// runs `initialize_workers`, the setup-handshake (`send_welcome`,
    /// `send_cert_exchange`, `wait_for_setup`) and then enters
    /// `process_tasks`. If `process_tasks` returns early with
    /// `RunOutcome::SetupPending`, the caller (the PyO3 wrapper) runs
    /// Python discovery and re-enters this same method to resume.
    /// On that second entry the per-secondary setup phase must NOT
    /// run again — `initialize_workers` would race against the
    /// already-spawned worker pool and `wait_for_setup` would block
    /// on wire messages that have already been consumed. This flag
    /// is set the moment setup completes successfully and gates the
    /// setup block on every subsequent entry.
    ///
    /// Always false on the pre-seeded (`required_setup_on_promote =
    /// false`) and failover paths; the existing `run` wrapper only
    /// calls `run_until_setup_or_done` once, so the flag transition
    /// is `false → true (mid-call) → (method returns Done)` for
    /// those callers.
    pub(super) setup_phase_completed: bool,

    /// Peer-lifecycle dispatcher channel receiver, paired with the
    /// `lifecycle_tx` installed on `cluster_state` at construction.
    /// Taken out at `run_until_setup_or_done`'s first entry and
    /// handed to
    /// [`crate::peer_lifecycle::run_peer_lifecycle_dispatcher`] inside
    /// the LocalSet running the secondary's operational loop.
    pub(super) lifecycle_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::peer_lifecycle::PeerLifecycleEvent>,
    >,
    /// Consumers of peer-lifecycle events; same single-shot
    /// `mem::take`-at-run semantics as on `PrimaryCoordinator`. See
    /// `PrimaryCoordinator::peer_lifecycle_listeners` for the
    /// rationale.
    pub(super) peer_lifecycle_listeners:
        Vec<Box<dyn crate::peer_lifecycle::LifecycleListener>>,

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
    pub(super) task_completed_rx: Option<
        tokio::sync::mpsc::UnboundedReceiver<crate::task_completed::TaskCompletedEvent>,
    >,

    /// Consumers of task-completion events; same single-shot
    /// `mem::take`-at-run semantics as on `PrimaryCoordinator`. See
    /// `PrimaryCoordinator::task_completed_listeners` for the
    /// rationale.
    pub(super) task_completed_listeners:
        Vec<Box<dyn crate::task_completed::TaskCompletedListener>>,

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
    /// and forwards it onto `transport.send(Address::Role(Role::Primary),
    /// msg)`, returning the outcome through the item's `reply`
    /// oneshot.
    ///
    /// `None` outside an active observer wiring — non-observer
    /// secondaries (and observer coordinators whose caller hasn't
    /// attached the announcer) never construct the outbox, so the
    /// select arm parks on `pending()` instead of polling a dead
    /// channel.
    pub(super) announcer_outbox_tx: Option<
        tokio::sync::mpsc::Sender<
            crate::observer::announcer::AnnouncerOutboxItem<I>,
        >,
    >,

    /// Announcer-outbox receiver, paired with `announcer_outbox_tx`.
    /// Built in [`Self::attach_observer_announcer`] (so non-observer
    /// secondaries don't allocate a channel they'll never use). Taken
    /// out at `process_tasks`' first entry into the drain arm and
    /// held locally for the duration of the loop — same shape as
    /// `command_rx`/`matcher_trigger_rx` on the primary. `None`
    /// outside the attached-observer window or once the loop has
    /// taken ownership.
    pub(super) announcer_outbox_rx: Option<
        tokio::sync::mpsc::Receiver<
            crate::observer::announcer::AnnouncerOutboxItem<I>,
        >,
    >,

    /// Panik-watcher signal receiver. Installed via
    /// [`Self::register_panik_signal_rx`] before `run_until_setup_or_done`
    /// (typically from the PyO3 wrapper which spawns
    /// [`crate::panik_watcher::spawn_panik_watcher`] at `run()` start
    /// and threads the receiver into the inner coordinator). `None`
    /// when the operator did not pass any panik-file paths — the
    /// `process_tasks` select! arm parks on `pending().await` and
    /// never fires in that case.
    ///
    /// Taken out for the duration of `process_tasks` so the arm's
    /// `await` can own the receiver across `select!` iterations
    /// (re-attaching `Option` from a struct field on every iteration
    /// would race the take/put with cancel-on-arm-fire semantics).
    pub(super) panik_signal_rx:
        Option<tokio::sync::oneshot::Receiver<crate::panik_watcher::PanikSignal>>,

    /// Lifecycle hook the PyO3 wrapper registers (a GIL-reacquiring
    /// closure calling Python's `task.on_phase_end(phase_id, completed,
    /// failed)`).
    ///
    /// The secondary holds NO authority and runs no phase machine — the
    /// fire site is the co-located authoritative `PrimaryCoordinator`,
    /// which owns `on_phase_end` and the phase machine. This field is
    /// the registration ANCHOR: the PyO3 secondary wrapper still accepts
    /// the closure (keeping the `register_phase_lifecycle_callbacks`
    /// pre-run contract stable for callers minting a handle from a
    /// secondary), but it is consumed only once the composed-primary
    /// runtime hands it to the co-located primary at activation. Until
    /// that runtime wiring lands the field is write-only on this side.
    #[allow(dead_code)] // consumed by the co-located PrimaryCoordinator under composition
    pub(super) on_phase_end:
        Option<crate::primary::OnPhaseEnd>,

    /// Phase-start sibling of `on_phase_end`; same registration-anchor
    /// disposition (fired by the co-located primary, not the secondary).
    #[allow(dead_code)] // consumed by the co-located PrimaryCoordinator under composition
    pub(super) on_phase_start:
        Option<crate::primary::OnPhaseStart>,

    /// Cross-thread / cross-runtime ingress for the `PrimaryHandle`
    /// PyO3 surface (when the handle was minted from a
    /// `PySecondaryCoordinator`).
    ///
    /// Externally-issued `PrimaryCommand`s are authority mutations whose
    /// only correct owner is the co-located `PrimaryCoordinator`; the
    /// secondary never drains this channel. The field is the
    /// registration ANCHOR keeping the PyO3 `command_sender()` clone a
    /// stable type — the composed-primary runtime hands the receiver to
    /// the primary's command loop. Write-only on this side until then.
    #[allow(dead_code)] // consumed by the co-located PrimaryCoordinator under composition
    pub(super) command_rx:
        Option<tokio::sync::mpsc::Receiver<crate::primary::PrimaryCommand<I>>>,

    /// Sender side of the secondary's command channel, cloned to
    /// consumers via `command_sender()`. Same registration-anchor
    /// disposition as `command_rx`.
    #[allow(dead_code)] // consumed by the co-located PrimaryCoordinator under composition
    pub(super) command_tx:
        tokio::sync::mpsc::Sender<crate::primary::PrimaryCommand<I>>,

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
