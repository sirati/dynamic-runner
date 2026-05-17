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

use dynrunner_core::{
    ErrorType, Identifier, MessageReceiver, MessageSender, PhaseId, TaskInfo, WorkerId,
};
use dynrunner_manager_local::pool::WorkerPool;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};
use dynrunner_scheduler_api::{PendingPool, ResourceEstimator, Scheduler};

use crate::cluster_state::ClusterState;
use crate::zip_extract::ExtractionCache;

use self::primary_link::PrimaryLink;

mod command_channel;
mod coordinator;
mod dispatch;
mod election;
mod peer;
mod primary;
mod primary_link;
mod processing;
mod resource;
mod retry_budget;
mod setup;
mod staging;
mod types;
mod wire;

#[cfg(test)]
mod test_helpers;

#[cfg(test)]
mod tests;

pub use types::{PeerCertInfo, RunOutcome, SecondaryConfig};

pub(super) struct PrimaryInFlightItem<I: Identifier> {
    pub(super) phase_id: PhaseId,
    pub(super) target_secondary_id: String,
    pub(super) binary: TaskInfo<I>,
}

/// One entry in the secondary's `primary_failed` ledger. Carries
/// both the original `TaskInfo` (so the secondary can re-inject the
/// binary on a retry pass without re-fetching it from the cluster
/// state) and the last-observed `ErrorType` (so the per-class
/// outcome breakdown — fail_retry/fail_oom/fail_final — can be
/// computed locally at log time).
///
/// On retry, the entry is overwritten with the new pass's
/// `ErrorType`; the binary is unchanged.
#[derive(Debug, Clone)]
pub(super) struct FailedTaskEntry<I: Identifier> {
    pub(super) binary: TaskInfo<I>,
    // Kept for diagnostic / future routing; not currently read by the
    // retry path which surfaces it via the FailedTask wire message.
    #[allow(dead_code)]
    pub(super) error_type: ErrorType,
}

/// The secondary coordinator: connects to primary, manages local workers.
///
/// Unlike `LocalManager` which runs a 5-phase pipeline, the secondary receives
/// individual task assignments from the primary and dispatches them to local
/// workers. It reports completions back and requests more work.
///
/// Generic over:
/// - `PT`: primary transport (e.g. WSS connection or channel)
/// - `P`: peer transport (e.g. `PeerNetwork` or `NoPeerTransport`)
/// - `M`: manager endpoint for worker communication
/// - `S`: scheduler
/// - `E`: memory estimator
/// - `I`: identifier type
pub struct SecondaryCoordinator<PT, P, M, S, E, I>
where
    PT: MessageSender<DistributedMessage<I>> + MessageReceiver<DistributedMessage<I>>,
    P: PeerTransport<I>,
    M: ManagerEndpoint,
    S: Scheduler<I>,
    E: ResourceEstimator<I>,
    I: Identifier,
{
    config: SecondaryConfig,
    primary_transport: PT,
    peer_transport: P,
    scheduler: S,
    estimator: E,

    // Certificate info for peer connections (set before run)
    peer_cert_info: Option<PeerCertInfo>,

    // Workers
    pool: WorkerPool<M, I>,

    // Task tracking: file_hash -> worker_id
    active_tasks: HashMap<String, WorkerId>,
    completed_tasks: HashSet<String>,

    /// Test-only counter: number of `WorkerEvent::TaskCompleted` events
    /// this secondary's OWN workers fired (i.e. tasks actually
    /// dispatched to and executed by this node's worker pool). Distinct
    /// from `completed_tasks` (which is the cluster-wide set of all
    /// task hashes any secondary observed terminal). Pinned by the
    /// peer-repoll-on-primary-changed regression test to assert
    /// post-fix distribution across secondaries: pre-fix the promoted
    /// secondary's pool burns through small workloads before any peer
    /// re-polls (production keepalive default = 5s), so peer
    /// `local_tasks_run` stays 0; post-fix every secondary's idle
    /// workers retry against the freshly-identified primary inside
    /// the PromotePrimary dispatch tick and pick up work.
    #[cfg(test)]
    local_tasks_run: usize,

    // State
    transfer_complete: bool,
    is_primary: bool,

    /// Wall-clock instant of the most recent `is_primary: false → true`
    /// transition. `None` while this secondary has never been promoted;
    /// set whenever a `PromotePrimary` (dispatch path) or a failover
    /// election (election path) flips `is_primary` to true.
    ///
    /// Read by the **alive-demoted natural-quiesce** branch in
    /// `process_tasks` to enforce a minimum-elapsed-time gate
    /// (`PROMOTED_PRIMARY_QUIESCE_GRACE`) before declaring the cluster
    /// done. Rationale: that branch fires on a CRDT-derived predicate
    /// (`task_count() > 0 && pending == 0 && in_flight == 0`) that is
    /// **incomplete-mirror-prone** in the immediate post-promotion
    /// window — a freshly promoted secondary may hold only the
    /// fraction of `TaskAdded` broadcasts the demoted primary had
    /// flushed at promotion time. Without a settle period the local
    /// view "5 of 10 tasks added, all 5 already terminal" satisfies
    /// the predicate and the branch broadcasts `RunComplete` while
    /// the demoted primary is still publishing the other 5
    /// (asm-dataset-nix T11: 5/10 phase_build tasks stranded).
    ///
    /// This is a **time-based bandage**, documented as such: the
    /// structurally clean fix (a wire signal from the demoted primary
    /// that it has finished publishing) does not exist in the
    /// protocol today. The grace is small enough not to defeat the
    /// branch's original asm-tokenizer LMU 2-of-235 deadlock-break
    /// purpose (loopback round-trips on a healthy cluster complete
    /// in single-digit ms; 2 s gives orders-of-magnitude headroom)
    /// while still finite so the branch eventually fires in the
    /// LMU scenario it was added to fix.
    ///
    /// Not reset on a subsequent re-hydration (the post-bootstrap
    /// `ClusterSnapshot` arm in `dispatch_message`): later snapshot
    /// arrivals are exactly the events the grace exists to wait for,
    /// resetting would defeat the purpose.
    pub(in crate::secondary) promoted_at: Option<Instant>,

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

    /// Sticky flag for "the primary's transport returned None on
    /// `recv()`". Used as the `if` guard on the `primary_transport.recv`
    /// arm of `process_tasks`'s `select!` so the persistently-None
    /// future doesn't hot-loop. Once true, we drive failover via the
    /// election state machine and route work via
    /// `primary_link.current_primary()` once a peer wins the election.
    ///
    /// Pre-fix the recv arm bare-broke the loop on `None` and the
    /// secondary exited cleanly with `completed=0` — losing every
    /// task the primary peer was about to dispatch. Dataset
    /// reported this on the dev-box-primary scenario where the local
    /// transport closed at the phase boundary.
    primary_disconnected: bool,

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

    // primary state (populated on promotion).
    // `primary_pending` is `None` until this secondary is promoted; on
    // the became-primary transition `populate_primary_from_cluster_state`
    // rebuilds it from the continuously-replicated `cluster_state` mirror.
    // No wire round-trip: the cluster ledger has been kept in sync via
    // `ClusterMutation` broadcasts since the run started.
    primary_pending: Option<PendingPool<I>>,
    primary_completed: HashSet<String>,
    /// Per-item ledger for every primary dispatch that hasn't
    /// terminated yet. Keyed by the same task hash used in
    /// `completed_tasks` / `active_tasks`. Stores `phase_id` (drives
    /// the pool's `on_item_finished` counter), `target_secondary_id`
    /// (used by the backpressure path to mark the right peer), and
    /// the full `binary` (used to `pool.requeue` on a rejection so
    /// the task isn't silently dropped from the pool).
    primary_in_flight: HashMap<String, PrimaryInFlightItem<I>>,

    /// Per-task ledger of Recoverable failures observed on the
    /// primary path. Mirrors the live primary's `failed_tasks` set, but
    /// keyed to the secondary that's currently acting as primary.
    /// Populated by `note_primary_item_failed` whenever a Recoverable
    /// failure terminates a dispatch slot for a task this node
    /// dispatched; drained by `primary_drain_check_and_retry`
    /// when the main pass quiesces (pool empty + no in-flight + no
    /// active local tasks) and re-injected into `primary_pending` via
    /// `pool.reinject(item)` for one more attempt.
    ///
    /// Stores the binary alongside the hash so the re-injection step
    /// has the full `TaskInfo` to put back into the pool — `primary_in_flight`
    /// already kept this shape for rejection-recovery; the failed
    /// ledger keeps the same shape for retry-injection.
    ///
    /// Each entry now also carries the last-observed `ErrorType` so
    /// the post-restructure `succeeded/fail_retry/fail_oom/fail_final`
    /// log surface can partition failures by class. Pre-restructure
    /// the secondary lost OOM-vs-final classification at this
    /// boundary (the binary was carried, the failure class was not).
    ///
    /// Closes the gap demotion introduced: post-demotion the local
    /// primary's `run_retry_passes` is a no-op, so without this
    /// primary-side ledger every Recoverable failure became
    /// terminal at the cluster level.
    primary_failed: HashMap<String, FailedTaskEntry<I>>,

    /// Retry budget for the primary-side re-injection loop. Owns
    /// both the attempt counter (originally `retry_max_passes`) and
    /// the optional SLURM-wallclock deadline (read once at
    /// construction from `$SLURM_JOB_END_TIME`). Consulted via
    /// `RetryBudget::should_retry()` from `primary_drain_check_and_retry`
    /// and the two drain-down exit predicates in `processing.rs`;
    /// bumped via `record_attempt()` once per completed re-injection
    /// cycle. See `retry_budget.rs` for the dual-axis design.
    primary_retry_budget: retry_budget::RetryBudget,

    /// One-shot guard for the budget-exhausted WARN emitted by
    /// `primary_drain_check_and_retry`. The drain-check fires
    /// every keepalive tick (and synchronously after every
    /// `note_primary_item_failed`); without this flag the warning would
    /// duplicate every tick for the rest of the run. Pure logging
    /// hygiene — the actual failure count lives in
    /// `primary_failed`.
    exhaustion_warning_emitted: bool,

    /// Per-peer backpressure backoff for the primary path.
    /// Mirrors `PrimaryCoordinator::backpressured_secondaries` — when
    /// a peer rejects a `TaskAssignment` with the wire signal
    /// "No idle worker available", record the peer with an expiry
    /// timestamp; until expiry, `handle_primary_task_request` skips
    /// re-dispatching to that peer (binary stays in the pool for
    /// another candidate). Cleared when the peer reports an actual
    /// `TaskComplete` (proves it's healthy) or when the backoff
    /// window expires naturally.
    backpressured_secondaries: HashMap<String, Instant>,

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

    /// Set true by the `PromotePrimary { required_setup: true }` arm
    /// in `dispatch.rs` when this secondary is promoted into the
    /// setup-secondary role (the submitter deferred all run-setup work
    /// to us — no `TaskAdded` batch was pre-seeded on the cluster
    /// ledger). The outer process-tasks loop yields back to the PyO3
    /// wrapper when this is true so Python's `task.discover_items` can
    /// run on this node (which has the staged source filesystem
    /// bind-mounted locally). The wrapper then calls
    /// `ingest_setup_discovery`, which seeds the ledger with
    /// `PhaseDepsSet` + `TaskAdded` mutations, broadcasts them to
    /// every peer, clears this flag, and hydrates the primary pool
    /// from the now-populated `cluster_state`.
    ///
    /// Never set on the pre-seeded promotion path (`required_setup:
    /// false` from the local submitter — local did discovery and the
    /// ledger is pre-seeded) nor on the failover-election path (the
    /// ledger has content from the CRDT broadcasts that ran during
    /// the live-primary phase). The wire-level `required_setup` flag
    /// is the only discriminator; "ledger empty" is NOT a proxy
    /// because failover-at-startup can legitimately observe an
    /// empty ledger.
    pub(super) setup_pending: bool,

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
    /// and forwards it onto `peer_transport.send(Address::Role(Role::Primary),
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

    /// Lifecycle hook invoked when this secondary owns the primary
    /// pool (post-promotion) and a phase reaches `Drained`. Mirrors
    /// `PrimaryCoordinator::on_phase_end` exactly; the PyO3 wrapper
    /// installs a GIL-reacquiring closure that calls Python's
    /// `task.on_phase_end(phase_id, completed, failed)`. `None` when
    /// the caller did not supply a hook OR when this secondary never
    /// gets promoted — in both cases the fire-site silently no-ops, so
    /// non-promoted secondaries are unaffected by the field's
    /// existence.
    pub(super) on_phase_end:
        Option<crate::primary::OnPhaseEnd>,

    /// Per-phase completion counters fed to `on_phase_end`. Incremented
    /// inside `note_primary_item_completed` (the single chokepoint
    /// where a primary-dispatched item terminates successfully on this
    /// secondary's pool); mirrors the same field on
    /// `PrimaryCoordinator`. Stays empty when this secondary is never
    /// promoted (the increment site is also the only writer).
    pub(super) primary_phase_completed:
        std::collections::HashMap<dynrunner_core::PhaseId, u32>,

    /// Per-phase failure counters fed to `on_phase_end`. Sibling of
    /// `primary_phase_completed` for the failure path.
    pub(super) primary_phase_failed:
        std::collections::HashMap<dynrunner_core::PhaseId, u32>,

    /// Phases that have already had `on_phase_start` fired through this
    /// secondary's lifecycle bridge. Mirrors
    /// `PrimaryCoordinator::phase_started_emitted` — the pool's state
    /// machine doesn't track "did we observe this transition", so the
    /// coordinator does the bookkeeping. Stays empty when no lifecycle
    /// callback is installed.
    pub(super) primary_phase_started_emitted:
        std::collections::HashSet<dynrunner_core::PhaseId>,

    /// Lifecycle hook invoked when this secondary owns the primary
    /// pool (post-promotion) and a phase flips Blocked → Active.
    /// Mirrors `PrimaryCoordinator::on_phase_start`. Same `None`
    /// semantics as `on_phase_end`.
    pub(super) on_phase_start:
        Option<crate::primary::OnPhaseStart>,

    /// Cross-thread / cross-runtime ingress for the
    /// `PrimaryHandle` PyO3 surface (when the handle was minted from
    /// a `PySecondaryCoordinator`). Each handler is co-located with
    /// the coordinator's per-mutation semantics under
    /// `secondary/primary/{fail_permanent,reinject_task,
    /// update_preferred_secondaries,spawn_tasks}.rs`; the receiver is
    /// read inside `process_tasks`' `select!` arm and the sender is
    /// cloned out via `command_sender()` before
    /// `run_until_setup_or_done` enters.
    ///
    /// Mirrors the `command_rx` / `command_tx` pair on
    /// `PrimaryCoordinator`. Held as `Option` so the operational loop
    /// can take the receiver out for the duration of the
    /// select-driven phase (Rust's borrow checker won't let us hold a
    /// `&mut Receiver` inside the same `&mut self` that the per-arm
    /// handlers need) and put it back when the loop exits. Outside
    /// the loop, the option is `Some` so cloned senders keep working
    /// across `SetupPending` re-entries.
    pub(super) command_rx:
        Option<tokio::sync::mpsc::Receiver<crate::primary::PrimaryCommand<I>>>,

    /// Sender side of the secondary's command channel, cloned to
    /// consumers via `command_sender()`. Stored on `Self` so the
    /// lifetime is tied to the coordinator — when the coordinator is
    /// dropped, all cloned senders return `SendError` on subsequent
    /// `send()` calls and the PyO3 side surfaces that as a Python
    /// exception.
    pub(super) command_tx:
        tokio::sync::mpsc::Sender<crate::primary::PrimaryCommand<I>>,

    /// Per-task reinject counter, paired with
    /// `SecondaryConfig::unfulfillable_reinject_max_per_task`. Lazily
    /// initialised on first reinject for a hash; counts DOWN from the
    /// configured cap (so 0 means "exhausted, refuse"). The map is
    /// keyed by task hash, not task_id, because external-control
    /// callers use the hash as the canonical identifier (mirroring the
    /// rest of the wire protocol).
    ///
    /// Independent of the primary's same-name counter: at promotion
    /// the freshly-promoted secondary starts with a fresh `HashMap`,
    /// so the budget effectively resets across the demotion boundary.
    /// Documented in `secondary/primary/reinject_task.rs`.
    pub(super) unfulfillable_reinject_remaining: HashMap<String, u32>,
}
