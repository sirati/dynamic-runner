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
    FakeWorkerFactory, FixedEstimator, NoPeers, SlowFakeWorkerFactory, TestId, fake_secondary,
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
pub(super) use dynrunner_transport_channel::{ChannelPeerTransport, ChannelPrimaryTransportEnd};
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

/// Wire up a real SecondaryCoordinator as a tokio task, connected to the
/// primary via channels. Returns the secondary's channel ends that should
/// be plugged into the primary's ChannelTransport.
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
    // This helper ran a full SecondaryCoordinator against the primary
    // over a channel UPLINK (the secondary reached the primary via
    // `ChannelPrimaryTransportEnd`). Post-uplink deletion the secondary
    // holds its mesh `PeerTransport` directly and reaches the primary as
    // a mesh peer by id — so this helper must wire the channel link as a
    // channel-backed mesh connection (primary folded into the secondary's
    // mesh, symmetric to how the QUIC bootstrap wire now folds into
    // `PeerNetwork`). Building that channel-backed mesh harness is the
    // channel→mesh fold leaf's job, NOT this uplink-deletion leaf. Left
    // unimplemented so the owning leaf supplies the real-API harness; the
    // primary-side tests that drive a real secondary through this helper
    // are `#[ignore]`d until then.
    let _ = (secondary_id, num_workers, max_resources, src_network);
    unimplemented!(
        "spawn_real_secondary over a channel link: the secondary must join the primary's mesh \
         (primary as a mesh peer reached by id) via a channel-backed mesh stub — owned by the \
         channel-mesh-fold leaf, not the uplink-deletion leaf"
    )
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
    // See `spawn_real_secondary_with_src_network` — the channel-uplink
    // secondary-against-primary harness is the channel→mesh fold leaf's
    // job, not this uplink-deletion leaf. Callers are `#[ignore]`d.
    let _ = (secondary_id, num_workers, max_resources, slow_markers);
    unimplemented!(
        "spawn_real_secondary_slow over a channel link: needs the channel-backed mesh harness — \
         owned by the channel-mesh-fold leaf, not the uplink-deletion leaf"
    )
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
    // See `spawn_real_secondary_with_src_network` — the channel-uplink
    // secondary-against-primary harness is the channel→mesh fold leaf's
    // job, not this uplink-deletion leaf. Callers are `#[ignore]`d.
    let _ = (
        secondary_id,
        num_workers,
        max_resources,
        flaky,
        retry_max_passes,
    );
    unimplemented!(
        "spawn_real_secondary_flaky over a channel link: needs the channel-backed mesh harness — \
         owned by the channel-mesh-fold leaf, not the uplink-deletion leaf"
    )
}
