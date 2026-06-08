//! Tests for the primary coordinator. Fixtures live in
//! [`super::test_helpers`]; each sub-module here holds a test family.
//!
//! Sub-modules:
//! - [`basic`] — happy-path single/multi-secondary dispatch.
//! - [`retry`] — recoverable-failure retry passes.
//! - [`setup_promote`] — pre-seeded-counter exit pathway.
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
mod crdt_convergence;
mod dispatch_decoupling;
mod e2e;
mod hydrate;
mod initial_assignment;
mod node_gates;
mod oom_bucket;
mod phase_decision;
mod phase_end_raise;
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
    FakeWorkerFactory, FixedEstimator, PrimaryMeshKeepalive, SlowFakeWorkerFactory, TestId,
    build_test_primary, build_test_promote_recipe, fake_secondary, fake_secondary_with_addrs,
    make_binary, make_relative_binary, seed_operational_ledger, setup_test,
};
#[allow(unused_imports)]
pub(super) use super::*;
#[allow(unused_imports)]
pub(super) use crate::process::SeedSource;
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

/// Yield enough times for the production mesh-pump (spawned by
/// [`build_test_primary`]) to drain a coordinator's QUEUED egress onto the
/// wire and for the transport to deliver it.
///
/// A direct-handler test (e.g. `primary.handle_welcome(..).await` then a
/// synchronous `try_recv` drain of the secondary's inbox) must call this
/// BETWEEN the handler and the drain: `MeshClient::send` is QUEUED (M4), so
/// the handler's broadcast sits on the pump's egress queue until the pump
/// task is scheduled. Yielding hands control to the pump (and the channel
/// transport's send) so the frame reaches the wire before the test reads it —
/// the test-side counterpart of the queued-egress model.
pub(super) async fn settle_pump() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// Phase 4b: tests that don't care about phase lifecycle pass an empty
/// dep map and no-op closures. Centralised here so individual tests
/// stay focused on the wire-flow they actually exercise.
pub(super) fn noop_phase_args() -> (
    HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>,
    OnPhaseStart,
    OnPhaseEnd,
) {
    (HashMap::new(), Box::new(|_| {}), Box::new(|_, _, _, _| {}))
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
    transport.register_primary_link("setup".into(), sec_to_pri_tx);

    (pri_to_sec_tx, sec_to_pri_rx, transport)
}

/// Run a real `SecondaryCoordinator` against the PRODUCTION mesh-pump
/// ([`crate::process::pump::run_pump`]) over `transport`, returning the
/// secondary's own-worker run count when it exits.
///
/// This is the secondary half of the real-`Node` e2e harness: the
/// coordinator holds ONLY a `MeshClient` (egress, queued) + `RoleInbox`
/// (ingress); the pump owns the `Mesh` and concurrently drains the egress
/// onto the wire AND demuxes inbound wire frames onto the secondary slot —
/// the exact production turn C-NODE built. We do NOT use a test-double pump
/// (the `PENDING-C-NODE` sequential stub starved non-ping-pong handshakes);
/// this is the true concurrent pump.
///
/// The coordinator's `run` and the pump race on one `tokio::select!`: when
/// the secondary's run completes we read its count and return — dropping the
/// pump future (the transport's inbound closes when the primary side drops,
/// which the pump observes as teardown anyway).
async fn run_secondary_node(
    config: SecondaryConfig,
    transport: ChannelPeerTransport<TestId>,
    factory: impl dynrunner_manager_local::WorkerFactory<dynrunner_transport_channel::ChannelManagerEnd>,
) -> usize {
    run_secondary_node_reading(config, transport, factory, |s| s.local_tasks_run_for_test()).await
}

/// As [`run_secondary_node`] but the caller supplies a `reader` closure run
/// on `&SecondaryCoordinator` AFTER the run exits, so a test can collect more
/// than the own-worker count (e.g. the replicated `cluster_state` counts) off
/// the same coordinator without a second harness.
async fn run_secondary_node_reading<R>(
    config: SecondaryConfig,
    transport: ChannelPeerTransport<TestId>,
    mut factory: impl dynrunner_manager_local::WorkerFactory<
        dynrunner_transport_channel::ChannelManagerEnd,
    >,
    reader: impl FnOnce(
        &SecondaryCoordinator<
            dynrunner_transport_channel::ChannelManagerEnd,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
    ) -> R,
) -> R {
    use crate::process::{LocalRole, Mesh};
    use dynrunner_protocol_primary_secondary::address::PeerId;

    let mut mesh = Mesh::new(transport);
    let secondary_id = config.secondary_id.clone();
    let (_slot, client, inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from(secondary_id.as_str()));
    let mut secondary = SecondaryCoordinator::new(
        config,
        client,
        inbox,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // The egress edge resolves `Destination::Primary` to the in-process
    // primary's id (`"primary"`) while the role table is cold — matching the
    // folded primary mesh-link's key.
    secondary.set_bootstrap_primary_id("setup".to_string());

    // Publish the live membership BEFORE the secondary's first send so its
    // no-route failover probe (`client.has_peer("primary")`) reads the folded
    // primary link as connected from the very first welcome — the pump
    // republishes every cycle, but the secondary's run may issue its first
    // send before the pump task is first scheduled.
    mesh.publish_membership();

    // The production pump OWNS the mesh; it needs a control receiver even
    // though this single-secondary harness performs no register/retag.
    let (_control, control_rx) = crate::process::pump::control_channel::<TestId>();
    let pump = crate::process::pump::run_pump(mesh, control_rx);
    tokio::pin!(pump);

    // Race the secondary's run against the pump in an inner scope so the run
    // future (which borrows `&mut secondary`) is fully DROPPED before we read
    // off `&secondary` (no overlapping borrow).
    {
        let run = secondary.run(&mut factory);
        tokio::pin!(run);
        tokio::select! {
            r = &mut run => { r.unwrap(); }
            // The pump exits first only if the wire closes before the
            // secondary finishes — a harness/teardown race, not a clean run.
            _ = &mut pump => {}
        }
    }
    reader(&secondary)
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
            primary_silence_backstop: Duration::from_secs(120),
            unconfigured_deadline: Duration::from_secs(600),
            can_be_primary: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
            forwarded_argv: Vec::new(),
        };
        run_secondary_node(config, transport, FakeWorkerFactory).await
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
            primary_silence_backstop: Duration::from_secs(120),
            unconfigured_deadline: Duration::from_secs(600),
            can_be_primary: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
            forwarded_argv: Vec::new(),
        };
        run_secondary_node(
            config,
            transport,
            SlowFakeWorkerFactory::with_markers(slow_markers),
        )
        .await
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
            primary_silence_backstop: Duration::from_secs(120),
            unconfigured_deadline: Duration::from_secs(600),
            can_be_primary: false,
            resource_check_interval: Duration::from_millis(100),
            log_oom_watcher: false,
            promoted_primary_quiesce_grace: Duration::from_millis(100),
            unfulfillable_reinject_max_per_task: None,
            mem_manager_reserved_bytes: None,
            output_dir: None,
            memuse_log_path: None,
            forwarded_argv: Vec::new(),
        };
        run_secondary_node(config, transport, flaky).await
    });

    (pri_to_sec_tx, sec_to_pri_rx, handle)
}
