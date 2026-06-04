//! Tests for the primary coordinator. Fixtures live in
//! [`super::test_helpers`]; each sub-module here holds a test family.
//!
//! Sub-modules:
//! - [`basic`] — happy-path single/multi-secondary dispatch.
//! - [`retry`] — recoverable-failure retry passes.
//! - [`setup_promote`] — setup-promote / pre-seeded-counter pathways.
//! - [`e2e`] — end-to-end primary + real secondary scenarios.
//! - [`promotion`] — primary-promotion + mesh-ready gates.
//! - [`stranded`] — stranded-task / cluster-collapse accounting.
//! - [`initial_assignment`] — initial round-robin + dispatch ordering.
//! - [`coordinator_setup`] — mint id, slurm-mgr stash, welcome handling.
//! - [`preferred_secondaries`] — preferred-secondary validation.
//! - [`wire`] — `wire_local_path` pre-staged-prefix stripping.
//! - [`worker_lifecycle`] — P1 slot-typestate / no-reassign-before-terminal.
//! - [`dispatch_decoupling`] — dispatch is a parked recheck woken by a
//!   `WorkerMgmtSignal::TasksAdded`; positive + negative-control +
//!   is_idle-advisory + coalesce.
//! - [`relocate_e2e`] — running channel-mesh proof of the full bootstrap
//!   hand-off: submitter relocates to a primary-capable secondary whose
//!   on-demand `PrimaryCoordinator` dispatches the residual workload;
//!   submitter observes + exits on `RunComplete`. Plus the no-capable-peer
//!   → submitter-stays-primary case.

mod basic;
mod coordinator_setup;
mod dispatch_decoupling;
mod e2e;
mod hydrate;
mod initial_assignment;
mod oom_bucket;
mod phase_decision;
mod phase_ordering;
mod preferred_secondaries;
mod promotion;
mod relocate_e2e;
mod relocate_observe;
mod result_data_plumbing;
mod retry;
mod select_bootstrap;
mod setup_promote;
mod stranded;
mod wire;
mod worker_lifecycle;

// Shared imports re-exported with `pub(super)` so each test sub-module
// can `use super::*` and pick them up without restating the full list.
// The `#[allow(unused_imports)]` covers files that legitimately don't
// touch a given import; the cost of curating per-file is high and the
// alternative is duplicated `use` lines in 11 sibling files.
#[allow(unused_imports)]
pub(super) use super::test_helpers::{
    FakeWorkerFactory, FixedEstimator, SlowFakeWorkerFactory, TestId, fake_secondary,
    fake_secondary_with_addrs, make_binary, make_relative_binary, setup_test,
};
#[allow(unused_imports)]
pub(super) use super::*;
#[allow(unused_imports)]
pub(super) use crate::secondary::{SecondaryConfig, SecondaryCoordinator};
#[allow(unused_imports)]
pub(super) use dynrunner_core::TaskInfo;
#[allow(unused_imports)]
pub(super) use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
#[allow(unused_imports)]
pub(super) use dynrunner_scheduler::ResourceStealingScheduler;
#[allow(unused_imports)]
pub(super) use dynrunner_transport_channel::ChannelPeerTransport;
#[allow(unused_imports)]
pub(super) use std::collections::HashMap;
#[allow(unused_imports)]
pub(super) use std::time::Duration;
#[allow(unused_imports)]
pub(super) use tokio::sync::mpsc as tokio_mpsc;

/// Phase 4b: tests that don't care about phase lifecycle pass an empty
/// dep map and no-op closures. Centralised here so individual tests
/// stay focused on the wire-flow they actually exercise.
pub(super) fn noop_phase_args() -> (
    HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>,
    OnPhaseStart,
    OnPhaseEnd,
) {
    (HashMap::new(), Box::new(|_| {}), Box::new(|_, _, _| {}))
}

/// Shared `PrimaryConfig` starting point for the in-process coordinator
/// tests. Returns `PrimaryConfig::default()` with the one deviation the
/// in-process harness shares almost universally baked in:
///
/// - `mesh_ready_timeout: 5s` (default 60s) — the in-process channel mesh
///   settles immediately, so the production 60s wait only slows tests.
///
/// Everything else is `Default`. Tests spread `..test_primary_config()` and
/// override only the fields they actually exercise (commonly
/// `num_secondaries`, `connect_timeout`, `peer_timeout`). A test that wants
/// the production `mesh_ready_timeout` (or any other non-default deviation)
/// overrides it explicitly, same as any other field.
pub(super) fn test_primary_config() -> PrimaryConfig {
    PrimaryConfig {
        mesh_ready_timeout: Duration::from_secs(5),
        ..PrimaryConfig::default()
    }
}

/// Build the channel-backed mesh transport a real secondary holds when
/// driven against a primary in-process: the secondary reaches the primary
/// as an ordinary mesh peer keyed by `"primary"` (folded in via
/// [`ChannelPeerTransport::register_primary_link`]), the channel analog of
/// how the QUIC bootstrap wire folds into `PeerNetwork`.
///
/// Returns the two channel ends the primary plugs into its own
/// `ChannelPeerTransport` — `pri_to_sec_tx` goes in the primary's
/// `outgoing[secondary_id]`, `sec_to_pri_rx` is forwarded into the
/// primary's inbound — plus the secondary's ready-to-use transport.
/// Single source of the mesh wiring so the per-factory spawn helpers below
/// stay focused on config + worker factory.
fn channel_mesh_secondary_ends(
    secondary_id: &str,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>, // primary→secondary
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>, // secondary→primary
    ChannelPeerTransport<TestId>,
) {
    // primary→secondary: feeds the secondary transport's inbound.
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    // secondary→primary: the secondary's outbound to the folded primary
    // link; the primary forwards `sec_to_pri_rx` into its own inbound.
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();

    let mut transport =
        ChannelPeerTransport::from_raw_channels(secondary_id.into(), HashMap::new(), pri_to_sec_rx);
    transport.register_primary_link("primary".into(), sec_to_pri_tx);

    (pri_to_sec_tx, sec_to_pri_rx, transport)
}

/// Wire up a real SecondaryCoordinator as a tokio task, connected to the
/// primary via a channel-backed mesh. Returns the secondary's channel ends
/// that should be plugged into the primary's `ChannelPeerTransport`.
pub(super) fn spawn_real_secondary(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>, // primary→secondary
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>, // secondary→primary
    tokio::task::JoinHandle<usize>,                          // returns completed count
) {
    spawn_real_secondary_with_src_network(secondary_id, num_workers, max_resources, None)
}

pub(super) fn spawn_real_secondary_with_src_network(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    src_network: Option<std::path::PathBuf>,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio::task::JoinHandle<usize>,
) {
    let (pri_to_sec_tx, sec_to_pri_rx, transport) = channel_mesh_secondary_ends(&secondary_id);

    let handle = tokio::task::spawn_local(async move {
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
            src_network,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            oom_retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            unconfigured_deadline: Duration::from_secs(600),
            is_observer: false,
            can_be_primary: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        // The egress edge resolves `Destination::Primary` to the
        // in-process primary's id (`"primary"`) while the role table is
        // cold — matching the folded primary mesh-link's key.
        secondary.set_bootstrap_primary_id("primary".to_string());
        let mut factory = FakeWorkerFactory;
        secondary.run(&mut factory).await.unwrap();
        secondary.local_tasks_run_for_test()
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}

/// Like [`spawn_real_secondary`] but the worker factory is a
/// [`SlowFakeWorkerFactory`] driven by per-`relative_path` substring
/// markers. Used by the phase-lifecycle ordering tests to keep one
/// item in-flight while a sibling completes so the cascade has a
/// chance to misfire `on_phase_end`.
pub(super) fn spawn_real_secondary_slow(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    slow_markers: Vec<(String, Duration)>,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio::task::JoinHandle<usize>,
) {
    let (pri_to_sec_tx, sec_to_pri_rx, transport) = channel_mesh_secondary_ends(&secondary_id);

    let handle = tokio::task::spawn_local(async move {
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            keepalive_interval: Duration::from_secs(60),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            oom_retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            unconfigured_deadline: Duration::from_secs(600),
            is_observer: false,
            can_be_primary: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        secondary.set_bootstrap_primary_id("primary".to_string());
        let mut factory = SlowFakeWorkerFactory::with_markers(slow_markers);
        secondary.run(&mut factory).await.unwrap();
        secondary.local_tasks_run_for_test()
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}

/// The result handle a [`spawn_real_secondary_primary_capable`] secondary
/// hands back: the count of tasks its CO-LOCATED primary credited to the
/// replicated ledger (`completed_count()`), captured the instant
/// `run_activated` returns. `None` means the on-demand activator never
/// fired (this node was never named primary). This is the per-host primary
/// attribution the relocation e2e asserts on — proof that the CHOSEN peer's
/// own authority dispatched + drove the run to completion, not merely that
/// cluster totals reconcile.
pub(super) type ActivatedPrimaryResult = std::rc::Rc<std::cell::Cell<Option<usize>>>;

/// Like [`spawn_real_secondary`] but PRIMARY-CAPABLE: the secondary joins
/// with `can_be_primary = true` and registers a [`PrimaryActivator`] that,
/// the moment a bootstrap `PrimaryChanged { reason: Transferred, new =
/// self }` names it, builds a real [`PrimaryCoordinator`] over a
/// `ChannelPeerTransport` ON DEMAND and `spawn_local`s its `run_activated`.
///
/// This is the channel-mesh analog of the pyo3 runtime's activator
/// (`managers/secondary/run.rs`): same closure SHAPE (build transport →
/// `PrimaryCoordinator::new` → `register_colocated_loopback` → transfer
/// command/phase wiring → `spawn_local(run_activated(snapshot))`) but the
/// transport is a net-new `ChannelPeerTransport` instead of the QUIC
/// `MeshHandleTransport` — TRANSPORT⊥ROLES holds, the secondary never names
/// the primary's transport type.
///
/// Mesh topology (caller supplies `peer_outboxes` = an outbox to EVERY
/// other peer, keyed by id):
///   * The SECONDARY transport's `outgoing` = `peer_outboxes` (so its
///     pre-relocation `Destination::Primary` reaches the submitter keyed
///     `"primary"`, and post-relocation a peer secondary's frames it must
///     relay route over the mesh); its inbound is `inbound_rx` (the wire
///     traffic this peer receives).
///   * The co-located PRIMARY transport's `outgoing` = a CLONE of
///     `peer_outboxes` (so `Destination::Secondary(other)` /
///     `Destination::All` reach the other secondaries + the
///     submitter-observer keyed `"primary"`); its inbound is CH2 — the
///     demuxed primary-facing frames the secondary's `handle_inbound`
///     forwards while `current_primary() == self`. Own-secondary delivery
///     is the egress-edge loopback (CH1), NOT a transport leg.
///
/// `keepalive_interval` is short in the relocation harness so peer
/// keepalives flow fast — the two secondaries recognise each other as alive
/// (`alive_secondary_count` ⇒ full mesh), each reports `MeshReady`, and the
/// submitter's `wait_for_mesh_ready` releases its hand-off fork promptly.
/// `slow_markers` (a [`SlowFakeWorkerFactory`] marker set) gives every task
/// a small per-task latency so the workload stays IN-FLIGHT across the
/// relocation window — the residual tasks the submitter's one-per-worker
/// initial assignment didn't place are then dispatched by the CHOSEN peer's
/// on-demand primary, making the dispatch attribution deterministic rather
/// than a "submitter finished everything in the fast-channel gap before it
/// could relocate" race.
///
/// Returns the secondary's own-work `JoinHandle` plus the
/// [`ActivatedPrimaryResult`] cell.
#[allow(clippy::type_complexity)]
pub(super) fn spawn_real_secondary_primary_capable(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    keepalive_interval: Duration,
    slow_markers: Vec<(String, Duration)>,
    inbound_rx: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    peer_outboxes: HashMap<String, tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>>,
) -> (tokio::task::JoinHandle<usize>, ActivatedPrimaryResult) {
    // CH1 (primary→secondary loopback) + CH2 (secondary→primary inbound):
    // the two co-located-composition channels, identical in shape to the
    // pyo3 wrapper's. CH1's rx drains in the secondary's operational loop;
    // CH2's rx is the on-demand primary transport's inbound.
    let (loopback_to_secondary_tx, loopback_to_secondary_rx) = tokio_mpsc::unbounded_channel();
    let (primary_inbound_tx, primary_inbound_rx) = tokio_mpsc::unbounded_channel();

    let activated_result: ActivatedPrimaryResult = std::rc::Rc::new(std::cell::Cell::new(None));
    let activated_result_for_closure = activated_result.clone();

    let handle = tokio::task::spawn_local(async move {
        // The SECONDARY transport: every other peer is an ordinary mesh
        // member in `outgoing`, the wire inbound is `inbound_rx`. The
        // submitter (keyed `"primary"`) is one such member, folded in the
        // same way `register_primary_link` does — but since the caller
        // already put it in `peer_outboxes`, we just hand the whole table
        // to `from_raw_channels`.
        let mut transport = ChannelPeerTransport::from_raw_channels(
            secondary_id.clone(),
            peer_outboxes.clone(),
            inbound_rx,
        );
        // `local_id` keying aside, mark the submitter link as the primary
        // link so `register_primary_link`'s contract (the folded primary is
        // a directed member) is honoured symmetrically with the bare
        // `spawn_real_secondary` path. The outbox is already present; this
        // re-insert is idempotent.
        if let Some(to_primary) = peer_outboxes.get("primary") {
            transport.register_primary_link("primary".into(), to_primary.clone());
        }

        let config = SecondaryConfig {
            secondary_id: secondary_id.clone(),
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            keepalive_interval,
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes: 1,
            oom_retry_max_passes: 1,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            unconfigured_deadline: Duration::from_secs(600),
            is_observer: false,
            // Opt-in primary capability: advertised in `SecondaryWelcome`,
            // recorded by the submitter as `PeerJoined { can_be_primary:
            // true }`, so `select_bootstrap_primary` may relocate to it.
            can_be_primary: true,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        secondary.set_bootstrap_primary_id("primary".to_string());

        // Co-located composition wiring (secondary side), the pre-run
        // one-shot registrations the pyo3 wrapper performs: CH2's sender
        // (the demux target the egress `Loopback` arm + `handle_inbound`
        // forward into) and CH1's receiver (the loopback the operational
        // loop drains).
        secondary.register_colocated_primary_inbound(primary_inbound_tx);
        secondary.register_colocated_loopback_inbound(loopback_to_secondary_rx);

        // Build the ON-DEMAND primary-activator. Same closure SHAPE as the
        // pyo3 reference; the transport is a `ChannelPeerTransport` over the
        // captured peer outboxes (remote send) + CH2 (demuxed inbound).
        let primary_node_id = secondary_id.clone();
        let primary_peer_outboxes = peer_outboxes;
        let activator: crate::secondary::PrimaryActivator<TestId> = Box::new(move |snapshot| {
            let primary_transport = ChannelPeerTransport::from_raw_channels(
                primary_node_id.clone(),
                primary_peer_outboxes,
                primary_inbound_rx,
            );
            let primary_config = PrimaryConfig {
                node_id: primary_node_id,
                ..PrimaryConfig::default()
            };
            let mut primary = PrimaryCoordinator::new(
                primary_config,
                primary_transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // Own-secondary loopback (CH1): `Destination::Secondary(own_id)`
            // + the own-secondary leg of every `Destination::All` broadcast
            // reach the co-located secondary through this sender.
            primary.register_colocated_loopback(loopback_to_secondary_tx);
            tokio::task::spawn_local(async move {
                let outcome = primary.run_activated(snapshot).await;
                // Capture the per-host attribution: the count THIS primary's
                // own replicated ledger credited, read the instant the run
                // finalizes. Recorded even on error so a hang vs a failed
                // run are distinguishable (a hang never reaches here).
                activated_result_for_closure.set(Some(primary.completed_count()));
                if let Err(e) = outcome {
                    tracing::error!(error = %e, "co-located primary (channel-mesh) failed");
                }
            })
        });
        secondary.register_primary_activator(activator);

        let mut factory = SlowFakeWorkerFactory::with_markers(slow_markers);
        secondary.run(&mut factory).await.unwrap();
        // Join the on-demand-built primary (if this node was activated) so
        // the run is fully wound down — and the `activated_result` cell is
        // set — before this handle resolves. `None` on a node never named
        // primary. The activated future never errors out of the join (its
        // body swallows the `run_activated` Err after recording the count).
        if let Some(primary_handle) = secondary.take_activated_primary_handle() {
            let _ = primary_handle.await;
        }
        secondary.local_tasks_run_for_test()
    });

    (handle, activated_result)
}

#[allow(clippy::type_complexity)]
pub(super) fn spawn_real_secondary_flaky(
    secondary_id: String,
    num_workers: u32,
    max_resources: dynrunner_core::ResourceMap,
    flaky: super::test_helpers::FlakyWorkerFactory,
    retry_max_passes: u32,
) -> (
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    // Returns the secondary's OWN-worker run count. The authoritative
    // retry-cascade counters (completed / failed-residual / passes-used)
    // live on the PRIMARY now — retry tests read them via the primary's
    // `completed_count()` / `failed_count()` / `retry_passes_used_for_test()`
    // before dropping the primary, not from this secondary handle.
    tokio::task::JoinHandle<usize>,
) {
    let (pri_to_sec_tx, sec_to_pri_rx, transport) = channel_mesh_secondary_ends(&secondary_id);

    let handle = tokio::task::spawn_local(async move {
        let config = SecondaryConfig {
            secondary_id,
            num_workers,
            max_resources,
            hostname: "test-host".into(),
            // Tight keepalive so the keepalive-tick backstop fires
            // quickly enough that tests don't hit the default 60s
            // wait if any code path needs the periodic drain-check
            // (the synchronous one in `note_primary_item_failed` is
            // the primary trigger — this is just defensive).
            keepalive_interval: Duration::from_millis(50),
            src_network: None,
            src_tmp: None,
            peer_timeout: Duration::from_secs(120),
            keepalive_miss_threshold: 3,
            retry_max_passes,
            // Mirror Recoverable retries: the existing fixture
            // callers want one budget value passed in for both
            // channels; the new `oom_retry_max_passes` knob is
            // unit-tested in `secondary/tests` separately.
            oom_retry_max_passes: retry_max_passes,
            primary_link_failure_threshold: 5,
            primary_link_failure_window: Duration::from_secs(30),
            unconfigured_deadline: Duration::from_secs(600),
            is_observer: false,
            can_be_primary: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
        };
        let mut secondary = SecondaryCoordinator::new(
            config,
            transport,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        secondary.set_bootstrap_primary_id("primary".to_string());
        let mut factory = flaky;
        secondary.run(&mut factory).await.unwrap();
        secondary.local_tasks_run_for_test()
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}
