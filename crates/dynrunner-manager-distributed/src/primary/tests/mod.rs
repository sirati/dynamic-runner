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
mod result_data_plumbing;
mod retry;
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
