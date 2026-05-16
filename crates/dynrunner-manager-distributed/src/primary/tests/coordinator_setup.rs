//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


/// `handle_welcome` originates `ClusterMutation::PeerJoined` for every
/// secondary it admits, in addition to the existing connection-state
/// bookkeeping. The mutation rides the canonical
/// `apply_and_broadcast_cluster_mutations` path (local apply + wire
/// broadcast), so we can assert both surfaces from one fixture:
///
/// 1. The broadcast envelope shows up on the connected secondary's
///    inbound channel (the in-process `ChannelSecondaryTransportEnd`
///    broadcast fan-out delivers to every entry in `outgoing`, and the
///    welcomed `sec-0` is one of them).
/// 2. The widened `apply_peer_joined` rule has run locally on the
///    primary's `cluster_state` — observable via the public
///    `cluster_state_for_test().role_table().observers` projection for
///    observer welcomes.
///
/// The first variant fires with `is_observer = false`, so the
/// observer-set projection MUST stay empty post-welcome — the
/// `apply_peer_joined` rule only inserts into `observers` when the
/// incoming flag is `true`. The broadcast must still go out (peer_state
/// insertion is `Applied`, so `apply_locally_for_broadcast` keeps the
/// mutation in the broadcast batch).
fn make_test_primary_config(num_secondaries: u32) -> PrimaryConfig {
    PrimaryConfig {
        node_id: "primary".into(),
        num_secondaries,
        connect_timeout: Duration::from_secs(5),
        peer_timeout: Duration::from_secs(5),
        keepalive_interval: Duration::from_secs(5),
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: true,
        required_setup_on_promote: false,
        max_concurrent_per_type: std::collections::HashMap::new(),
        retry_max_passes: 1,
        fleet_dead_timeout: std::time::Duration::from_secs(30),
        mesh_ready_timeout: std::time::Duration::from_secs(5),
        mass_death_grace: std::time::Duration::ZERO,
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
    }
}

#[test]
fn mint_secondary_id_returns_sequential() {
    let (transport, _ends) = setup_test(1);
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        make_test_primary_config(1),
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // num_secondaries = 1, so the initial id `secondary-0` is reserved
    // for the prep phase; the first respawn returns `secondary-1` and
    // each subsequent mint advances the counter by one.
    let first = primary.mint_secondary_id();
    let second = primary.mint_secondary_id();
    assert_eq!(first, "secondary-1");
    assert_eq!(second, "secondary-2");
}

#[test]
fn mint_secondary_id_starts_at_num_secondaries() {
    let (transport, _ends) = setup_test(4);
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        make_test_primary_config(4),
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // num_secondaries = 4 reserves `secondary-0..secondary-3`; the
    // first mint must land at `secondary-4`.
    let first = primary.mint_secondary_id();
    assert_eq!(first, "secondary-4");
}

#[test]
fn set_slurm_job_manager_stores_arc() {
    use std::sync::Arc;

    let (transport, _ends) = setup_test(1);
    let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
        make_test_primary_config(1),
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    // No parking yet → accessor returns `None`.
    assert!(primary.slurm_job_manager().is_none());

    // Park a marker payload (the type itself doesn't matter here —
    // `manager-distributed` only stores the `Arc` opaquely; downcasting
    // back is the respawn caller's responsibility).
    #[derive(Debug, PartialEq, Eq)]
    struct Marker(u32);
    let marker: Arc<dyn std::any::Any + Send + Sync> = Arc::new(Marker(0x42));
    primary.set_slurm_job_manager(marker);

    // Round-trip via the accessor: the parked Arc must be retrievable
    // and downcast cleanly to the original concrete type.
    let parked = primary
        .slurm_job_manager()
        .expect("parked Arc must be present after set_slurm_job_manager");
    let downcast = parked
        .clone()
        .downcast::<Marker>()
        .expect("parked Arc must downcast to the original concrete type");
    assert_eq!(*downcast, Marker(0x42));
}

/// Assemble the welcome envelope the primary receives on the
/// `SecondaryWelcome` arm of `dispatch_message`. Centralised so the
/// three handle_welcome tests stay focused on what they're asserting
/// (broadcast shape, observer projection, non-observer broadcast).
fn welcome_msg(secondary_id: &str, is_observer: bool) -> DistributedMessage<TestId> {
    DistributedMessage::SecondaryWelcome {
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        resources: vec![dynrunner_core::ResourceAmount {
            kind: dynrunner_core::ResourceKind::memory(),
            amount: 1024 * 1024 * 1024,
        }],
        worker_count: 1,
        hostname: "test".into(),
        is_observer,
    }
}

/// Drain the welcomed secondary's inbox and return every
/// `ClusterMutation::PeerJoined` mutation observed across all envelopes,
/// in arrival order. Other envelope variants are ignored — the primary's
/// broadcast is unfiltered (the test fixture wires it to a single
/// secondary, so noise frames are rare but possible if future
/// `handle_welcome` work adds adjacent fan-outs).
fn drain_peer_joined(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            for m in mutations {
                if let dynrunner_protocol_primary_secondary::ClusterMutation::PeerJoined {
                    peer_id,
                    is_observer,
                } = m
                {
                    out.push((peer_id, is_observer));
                }
            }
        }
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn handle_welcome_emits_peer_joined_for_accepted_secondary() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(1);
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
            PrimaryCoordinator::new(
                make_test_primary_config(1),
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

        let (id, mut to_sec_rx, _outgoing) = secondary_ends.remove(0);
        assert_eq!(id, "sec-0");

        // Drive the handler directly — no run-loop indirection so the
        // assertion is on the handler itself, not the surrounding setup
        // pipeline.
        primary.handle_welcome(welcome_msg(&id, false)).await;

        let observed = drain_peer_joined(&mut to_sec_rx);
        assert_eq!(
            observed,
            vec![(id.clone(), false)],
            "handle_welcome must originate exactly one PeerJoined \
             carrying the welcomed secondary's id; got {:?}",
            observed
        );
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn handle_welcome_emits_peer_joined_with_is_observer_flag_from_welcome() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // Branch 1: is_observer = true. The widened apply rule must
        // both broadcast the mutation AND project the welcomed id into
        // `role_table.observers` locally.
        {
            let (transport, mut secondary_ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
                PrimaryCoordinator::new(
                    make_test_primary_config(1),
                    transport,
                    NoPeers,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
            let (id, mut to_sec_rx, _outgoing) = secondary_ends.remove(0);
            primary.handle_welcome(welcome_msg(&id, true)).await;

            let observed = drain_peer_joined(&mut to_sec_rx);
            assert_eq!(
                observed,
                vec![(id.clone(), true)],
                "observer welcome must carry is_observer=true on the \
                 originated PeerJoined; got {:?}",
                observed
            );
            assert!(
                primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains(&id),
                "is_observer=true welcome must project into role_table.observers \
                 via the local apply path; observers={:?}",
                primary.cluster_state_for_test().role_table().observers
            );
        }

        // Branch 2: is_observer = false. The broadcast still goes out
        // (the apply rule inserts a non-observer peer_state entry and
        // returns Applied — the broadcast batch keeps it), but the
        // observer projection must stay empty.
        {
            let (transport, mut secondary_ends) = setup_test(1);
            let mut primary: PrimaryCoordinator<_, _, _, _, TestId> =
                PrimaryCoordinator::new(
                    make_test_primary_config(1),
                    transport,
                    NoPeers,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
            let (id, mut to_sec_rx, _outgoing) = secondary_ends.remove(0);
            primary.handle_welcome(welcome_msg(&id, false)).await;

            let observed = drain_peer_joined(&mut to_sec_rx);
            assert_eq!(
                observed,
                vec![(id.clone(), false)],
                "non-observer welcome must carry is_observer=false on \
                 the originated PeerJoined; got {:?}",
                observed
            );
            assert!(
                !primary
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains(&id),
                "is_observer=false welcome must NOT populate \
                 role_table.observers (ratchet-up-only rule); observers={:?}",
                primary.cluster_state_for_test().role_table().observers
            );
        }
    }).await;
}

/// Pins the peer-lifecycle dispatcher cleanup contract: every `run()`
/// exit (Ok happy-path here; the Err path goes through the same
/// `cleanup_lifecycle_dispatcher` call in the outer wrapper) must
/// take + abort + join the dispatcher's `JoinHandle`. The observable
/// post-condition is `lifecycle_dispatcher_handle == None`.
///
/// Without the cleanup, the spawned dispatcher task would stay
/// blocked on its input channel forever (the channel's sender lives
/// on `cluster_state`, which the coordinator still owns post-run),
/// leaking a tokio task per `run()` invocation. This test mirrors the
/// single-secondary happy-path fixture so the assertion exercises the
/// same end-of-run boundary every other test relies on.
#[tokio::test(flavor = "current_thread")]
async fn lifecycle_dispatcher_joinhandle_aborted_on_run_exit() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, secondary_ends) = setup_test(1);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 1,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-run: no dispatcher spawned yet.
        assert!(
            !primary.lifecycle_dispatcher_handle_present_for_test(),
            "dispatcher handle must be None before run() spawns it"
        );

        let binaries = vec![make_binary("a", 50)];
        for (id, rx, tx) in secondary_ends {
            tokio::task::spawn_local(fake_secondary(id, 1, 1024 * 1024 * 1024, rx, tx));
        }

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        // Post-run: cleanup_lifecycle_dispatcher must have taken the
        // handle out of `self`. A surviving `Some` would mean the
        // dispatcher was not aborted+joined and is leaking against
        // the still-alive `cluster_state` sender.
        assert!(
            !primary.lifecycle_dispatcher_handle_present_for_test(),
            "lifecycle_dispatcher_handle must be None after run() exits — \
             cleanup_lifecycle_dispatcher should have taken + aborted + \
             joined the handle"
        );
    }).await;
}
