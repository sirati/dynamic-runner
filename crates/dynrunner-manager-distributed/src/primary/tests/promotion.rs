//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;


/// Multi-secondary mesh-ready gate: the primary must NOT issue
/// `PromotePrimary` until every connected secondary has reported
/// `MeshReady`. Pre-fix the promotion fired ~750µs after cert-
/// exchange completed; the promoted secondary then became
/// authoritative against a still-forming peer mesh, and every
/// pre-mesh-formation peer-broadcast routed into the void for up
/// to 30s. This test pins the new ordering: wire `PromotePrimary`
/// arrives at every fake secondary AFTER all of them have sent
/// their own `MeshReady`. Implementation uses a per-secondary
/// `tokio::sync::oneshot` to gate the MeshReady send so the test
/// can drive the order deterministically.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_held_until_every_secondary_reports_mesh_ready() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const N_SECONDARIES: u32 = 3;
            let (transport, secondary_ends) = setup_test(N_SECONDARIES);

            // Per-secondary oneshot triggers. Test drives them in
            // order to enforce: the primary doesn't fire
            // PromotePrimary until ALL three have flipped.
            let mut mesh_triggers: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
            // Per-secondary observation: did this secondary see
            // PromotePrimary BEFORE it was allowed to send
            // MeshReady? (true = bug present)
            let mut promote_seen_pre_mesh_observers: Vec<
                tokio::sync::oneshot::Receiver<bool>,
            > = Vec::new();

            for (id, rx, tx) in secondary_ends {
                let (mesh_tx, mesh_rx) = tokio::sync::oneshot::channel::<()>();
                let (obs_tx, obs_rx) = tokio::sync::oneshot::channel::<bool>();
                mesh_triggers.push(mesh_tx);
                promote_seen_pre_mesh_observers.push(obs_rx);
                tokio::task::spawn_local(gated_mesh_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    rx,
                    tx,
                    mesh_rx,
                    obs_tx,
                ));
            }

            let config = PrimaryConfig {
                node_id: "primary".into(),
                num_secondaries: N_SECONDARIES,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                keepalive_interval: Duration::from_secs(5),
                keepalive_miss_threshold: 3,
                source_pre_staged_root: None,
                uses_file_based_items: true,
                required_setup_on_promote: false,
                max_concurrent_per_type: std::collections::HashMap::new(),
                retry_max_passes: 1,
                oom_retry_max_passes: 1,
                fleet_dead_timeout: std::time::Duration::from_secs(30),
                // Generous timeout so the test can fire triggers
                // sequentially without racing the deadline.
                mesh_ready_timeout: std::time::Duration::from_secs(10),
                mass_death_grace: std::time::Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
                unfulfillable_reinject_max_per_task: None,
                setup_promote_deadline: std::time::Duration::from_secs(600),
            };

            let mut primary = PrimaryCoordinator::new(
                config,
                transport,
                NoPeers,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..6)
                .map(|i| make_binary(&format!("bin_{i}"), 100))
                .collect();

            // Drive the primary's coordination pipeline on a child
            // task so the test body can release MeshReady triggers
            // in sequence and observe the gate.
            let primary_handle = tokio::task::spawn_local(async move {
                let (deps, ops, ope) = noop_phase_args();
                primary.run(binaries, deps, ops, ope).await.unwrap();
                primary.completed_count()
            });

            // Release MeshReady triggers one at a time. Between
            // each release, yield enough times for the primary's
            // wait loop to observe the freshly-arrived
            // MeshReady. The primary must NOT have advanced past
            // `wait_for_mesh_ready` until all three triggers have
            // fired — otherwise the per-secondary "did I see
            // PromotePrimary before being allowed to MeshReady?"
            // observer would have reported true for some of them.
            for trigger in mesh_triggers {
                trigger.send(()).expect("trigger send");
                // Yield repeatedly so the primary task gets a
                // chance to dequeue & process the MeshReady. A
                // single `yield_now` isn't enough on a
                // current_thread runtime when the primary is
                // mid-message, so spam it.
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
            }

            // Collect the per-secondary observations. None of
            // them should have seen PromotePrimary before being
            // allowed to send MeshReady.
            for (i, obs) in promote_seen_pre_mesh_observers.into_iter().enumerate() {
                let saw = obs.await.expect("observer recv");
                assert!(
                    !saw,
                    "secondary {i} observed PromotePrimary BEFORE its own \
                     MeshReady was allowed to send — primary's \
                     wait_for_mesh_ready step is not gating PromotePrimary"
                );
            }

            let completed = primary_handle.await.unwrap();
            assert_eq!(completed, 6, "all 6 tasks should complete");
        })
        .await;
}

/// Fake secondary that defers `MeshReady` until the test fires
/// `mesh_trigger`. Reports via `observer` whether it saw
/// `PromotePrimary` arrive before its `MeshReady` was permitted to
/// send (true = bug). Otherwise behaves like `fake_secondary`.
async fn gated_mesh_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    mesh_trigger: tokio::sync::oneshot::Receiver<()>,
    observer: tokio::sync::oneshot::Sender<bool>,
) {
    use dynrunner_protocol_primary_secondary::MessageType;

    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            resources: vec![dynrunner_core::ResourceAmount {
                kind: dynrunner_core::ResourceKind::memory(),
                amount: ram_bytes,
            }],
            worker_count: num_workers,
            hostname: "test-host".into(),
            is_observer: false,
        })
        .unwrap();

    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            sender_id: secondary_id.clone(),
            timestamp: 0.0,
            secondary_id: secondary_id.clone(),
            public_cert_pem: "FAKE_CERT".into(),
            ipv4_address: Some("127.0.0.1".into()),
            ipv6_address: None,
            quic_port: 5000,
        })
        .unwrap();

    // Race: receive the trigger to send MeshReady against
    // observing PromotePrimary on the inbound path. If
    // PromotePrimary arrives first, the gate failed.
    let mut mesh_trigger_opt = Some(mesh_trigger);
    let mut observer_opt = Some(observer);
    let mut mesh_sent = false;
    let mut promote_seen_pre_mesh = false;

    loop {
        // While we're still pre-MeshReady, race the trigger
        // against an inbound PromotePrimary. After MeshReady has
        // been sent, the trigger arm is removed and we fall back
        // to a normal recv loop.
        if !mesh_sent {
            let trigger = mesh_trigger_opt.as_mut().unwrap();
            tokio::select! {
                _ = trigger => {
                    outgoing_to_primary
                        .send(DistributedMessage::MeshReady {
                            sender_id: secondary_id.clone(),
                            timestamp: 0.0,
                            secondary_id: secondary_id.clone(),
                            peer_count: 0,
                        })
                        .unwrap();
                    mesh_sent = true;
                    mesh_trigger_opt = None;
                    if let Some(obs) = observer_opt.take() {
                        let _ = obs.send(promote_seen_pre_mesh);
                    }
                }
                msg = incoming_from_primary.recv() => match msg {
                    Some(m) => {
                        if matches!(m.msg_type(), MessageType::PromotePrimary) {
                            promote_seen_pre_mesh = true;
                        }
                        handle_inbound_for_gated_secondary(
                            &secondary_id,
                            &outgoing_to_primary,
                            ram_bytes,
                            m,
                        );
                    }
                    None => break,
                },
            }
        } else {
            match incoming_from_primary.recv().await {
                Some(m) => handle_inbound_for_gated_secondary(
                    &secondary_id,
                    &outgoing_to_primary,
                    ram_bytes,
                    m,
                ),
                None => break,
            }
        }
    }
}

fn handle_inbound_for_gated_secondary(
    secondary_id: &str,
    outgoing: &tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    ram_bytes: u64,
    msg: DistributedMessage<TestId>,
) {
    match msg {
        DistributedMessage::PeerInfo { .. } => {}
        DistributedMessage::InitialAssignment {
            zip_files,
            workers_ready,
            ..
        } => {
            // Pair each binary with the worker the primary's
            // `assign_initial` placed it on (positional alignment of
            // `workers_ready[i]` and `zip_files[0].binaries[i]` is
            // `perform_initial_assignment`'s contract). Always
            // emitting `worker_id=0` worked pre-demotion because the
            // primary's kickstart re-dispatch eventually cleared
            // every worker's `current_task` regardless of which one
            // a TaskComplete was attributed to. Post-demotion the
            // primary stops dispatching after `PromotePrimary`, so a
            // mis-attributed TaskComplete leaves the OTHER worker
            // permanently mid-dispatch and `active_workers > 0`
            // forever — operational_loop never terminates.
            let entries: Vec<_> = zip_files
                .iter()
                .flat_map(|zf| zf.binaries.iter())
                .collect();
            for (idx, entry) in entries.iter().enumerate() {
                let worker_id = workers_ready
                    .get(idx)
                    .map(|w| w.worker_id)
                    .unwrap_or(0);
                let _ = outgoing.send(DistributedMessage::TaskComplete {
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    task_hash: entry.hash.clone(),
                    result_data: None,
                });
                let _ = outgoing.send(DistributedMessage::TaskRequest {
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    available_resources: vec![dynrunner_core::ResourceAmount {
                        kind: dynrunner_core::ResourceKind::memory(),
                        amount: ram_bytes,
                    }],
                });
            }
        }
        DistributedMessage::TransferComplete { .. } => {}
        DistributedMessage::TaskAssignment { file_hash, .. } => {
            let _ = outgoing.send(DistributedMessage::TaskComplete {
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                task_hash: file_hash,
                result_data: None,
            });
            let _ = outgoing.send(DistributedMessage::TaskRequest {
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                available_resources: vec![dynrunner_core::ResourceAmount {
                    kind: dynrunner_core::ResourceKind::memory(),
                    amount: ram_bytes,
                }],
            });
        }
        _ => {}
    }
}

/// End-to-end pin for the "peer ipv4/ipv6 addresses reach the dialer"
/// plumbing: spin up a primary against two channel-transport
/// secondaries, have each advertise BOTH families in CertExchange, and
/// inspect the `PeerInfo` broadcast that lands at one of them. The
/// peers vector must carry the OTHER secondary's ipv4 AND ipv6 — pre-
/// fix `peer_setup::send_peer_lists` hardcoded `ipv6: None`, which
/// produced empty happy-eyeballs candidate sets on dual-stack hosts
/// where ipv4 was administratively blocked between compute nodes.
///
/// The test snoops `PeerInfo` by intercepting the second secondary's
/// inbound channel: a forwarder task drains the channel, copies any
/// `PeerInfo` into a `oneshot` for assertion, then forwards every
/// message to the real fake-secondary task so the lifecycle
/// (PeerInfo → InitialAssignment → TaskAssignment → TaskComplete)
/// completes and `primary.run` returns.
#[tokio::test(flavor = "current_thread")]
async fn peer_info_broadcast_carries_both_ipv4_and_ipv6() {
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, mut secondary_ends) = setup_test(2);

        let config = PrimaryConfig {
            node_id: "primary".into(),
            num_secondaries: 2,
            connect_timeout: Duration::from_secs(5),
            peer_timeout: Duration::from_secs(5),
            keepalive_interval: Duration::from_secs(5),
            keepalive_miss_threshold: 3,
            source_pre_staged_root: None,
            uses_file_based_items: true,
            required_setup_on_promote: false,
            max_concurrent_per_type: std::collections::HashMap::new(),
            retry_max_passes: 1,
            oom_retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: std::time::Duration::from_secs(600),
        };

        let mut primary = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        let binaries = vec![make_binary("a", 50)];

        // Two secondaries, each advertising a distinct ipv4 + ipv6.
        // sec-0 → (10.0.0.1, 2001:db8::1)
        // sec-1 → (10.0.0.2, 2001:db8::2)
        // The assertion below pulls the PeerInfo sec-1 receives and
        // looks up sec-0's entry — that's the entry whose addresses
        // were in flight through `handle_cert_exchange` →
        // `SecondaryConnectionState` → `send_peer_lists`.
        let addrs: Vec<(String, String)> = vec![
            ("10.0.0.1".into(), "2001:db8::1".into()),
            ("10.0.0.2".into(), "2001:db8::2".into()),
        ];

        // Snoop the second secondary's primary→secondary channel: a
        // forwarder task copies any `PeerInfo` into a oneshot before
        // re-forwarding every message to the actual fake-secondary
        // task. Without the forward step, the fake never sees
        // InitialAssignment / TransferComplete and `primary.run`
        // hangs on `wait_for_peer_connections` budgeting → timeout.
        let (peer_info_tx, peer_info_rx) = tokio::sync::oneshot::channel();
        let mut peer_info_tx = Some(peer_info_tx);

        // Pull sec-1 out first so we can wrap its inbound channel.
        // `secondary_ends` is ordered sec-0, sec-1.
        let (sec1_id, sec1_inbound, sec1_outbound) = secondary_ends.remove(1);
        let (sec0_id, sec0_inbound, sec0_outbound) = secondary_ends.remove(0);

        // sec-0: vanilla fake_secondary_with_addrs.
        let (sec0_ipv4, sec0_ipv6) = addrs[0].clone();
        tokio::task::spawn_local(fake_secondary_with_addrs(
            sec0_id.clone(),
            1,
            1024 * 1024 * 1024,
            Some(sec0_ipv4),
            Some(sec0_ipv6),
            sec0_inbound,
            sec0_outbound,
        ));

        // sec-1: forwarder + fake.
        let (sec1_inner_tx, sec1_inner_rx) = tokio_mpsc::unbounded_channel();
        let (sec1_ipv4, sec1_ipv6) = addrs[1].clone();
        tokio::task::spawn_local(fake_secondary_with_addrs(
            sec1_id.clone(),
            1,
            1024 * 1024 * 1024,
            Some(sec1_ipv4),
            Some(sec1_ipv6),
            sec1_inner_rx,
            sec1_outbound,
        ));

        tokio::task::spawn_local(async move {
            let mut rx = sec1_inbound;
            while let Some(msg) = rx.recv().await {
                if let DistributedMessage::PeerInfo { peers, .. } = &msg
                    && let Some(tx) = peer_info_tx.take()
                {
                    let _ = tx.send(peers.clone());
                }
                if sec1_inner_tx.send(msg).is_err() {
                    break;
                }
            }
        });

        let (deps, ops, ope) = noop_phase_args();
        primary.run(binaries, deps, ops, ope).await.unwrap();

        let peers = peer_info_rx.await.expect("PeerInfo never delivered");

        let sec0_peer = peers
            .iter()
            .find(|p| p.secondary_id == "sec-0")
            .expect("sec-0 missing from PeerInfo");
        assert_eq!(
            sec0_peer.ipv4.as_deref(),
            Some("10.0.0.1"),
            "primary dropped ipv4 from peer broadcast"
        );
        assert_eq!(
            sec0_peer.ipv6.as_deref(),
            Some("2001:db8::1"),
            "primary dropped ipv6 from peer broadcast — happy-eyeballs \
             dialer would race only ipv4 candidates and fail on \
             clusters where ipv4 is administratively blocked between \
             compute nodes"
        );
    })
    .await;
}

/// Regression: `promote_primary` flips `self.demoted` to true
/// and from that point `dispatch_to_idle_workers` is a no-op on the
/// scheduler — i.e. the local primary stops handing out work as
/// soon as it has handed authority off to the primary.
///
/// Without this contract the local primary and the promoted secondary
/// would both run dispatch in parallel against the same pool, racing
/// for workers and creating duplicate assignments / inconsistent
/// ledger state. See `demoted` doc on `PrimaryCoordinator` for the
/// full rationale.
#[tokio::test(flavor = "current_thread")]
async fn promote_primary_demotes_local_and_disables_dispatch() {
    use crate::state::{SecondaryConnection, SecondaryConnectionState};
    use dynrunner_scheduler_api::PendingPool;

    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let (transport, _ends) = setup_test(1);
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
            oom_retry_max_passes: 1,
            fleet_dead_timeout: std::time::Duration::from_secs(30),
            mesh_ready_timeout: std::time::Duration::from_secs(5),
            mass_death_grace: std::time::Duration::ZERO,
            mass_death_min_count: 2,
            source_dir: None,
            unfulfillable_reinject_max_per_task: None,
            setup_promote_deadline: std::time::Duration::from_secs(600),
        };
        let mut primary: PrimaryCoordinator<_, _, _, _, TestId> = PrimaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );

        // Pre-conditions: a registered secondary, a single idle
        // virtual worker bound to it, and a pool with one queued
        // binary that `dispatch_to_idle_workers` would otherwise
        // pick up. We bypass `run()` because we want to drive
        // `promote_primary` and `dispatch_to_idle_workers`
        // in isolation.
        let phase = dynrunner_core::PhaseId::from("default");
        let mut pool = PendingPool::<TestId>::new(
            [phase.clone()],
            std::collections::HashMap::new(),
        )
        .expect("default-phase pool");
        let bin = make_binary("solo", 50);
        pool.extend([bin.clone()]).expect("valid extend");
        primary.pending = Some(pool);
        primary.phase_completed.insert(phase.clone(), 0);
        primary.phase_failed.insert(phase, 0);
        primary.all_binaries = vec![bin];
        primary.total_tasks = 1;

        let conn = SecondaryConnection::new("sec-0".into())
            .receive_welcome(1, vec![], "host".into(), 0, None, false)
            .receive_cert_exchange(String::new(), None, None, 0)
            .begin_peer_discovery()
            .peers_ready()
            .assignments_sent();
        primary.secondaries.insert(
            "sec-0".into(),
            SecondaryConnectionState::Operational(conn),
        );
        primary.workers.push(RemoteWorkerState {
            worker_id: 0,
            secondary_id: "sec-0".into(),
            resource_budgets: dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]),
            current_task: None,
            estimated_resources: dynrunner_core::ResourceMap::new(),
            is_idle: true,
        });

        assert!(!primary.demoted, "fresh primary is not demoted");

        // Promote: should set `demoted = true` and emit a
        // `PromotePrimary` to the secondary (we don't observe the
        // wire here; the demotion flag is the contract under test).
        primary.promote_primary().await.unwrap();
        assert!(primary.demoted, "promote_primary must demote local");
        assert_eq!(
            primary.primary_id.as_deref(),
            Some("sec-0"),
            "promote_primary records the routing target"
        );

        // The pool still has its queued binary; the worker is
        // still idle. Pre-fix `dispatch_to_idle_workers` would
        // happily take the binary from the pool and assign it.
        // Post-fix it must early-return without touching pool
        // state — since the primary now owns dispatch.
        let pool_len_before = primary.pool().len();
        let view_before = primary.pool().view_for_worker(0, None).len();
        assert_eq!(pool_len_before, 1);
        assert_eq!(view_before, 1);
        assert!(primary.workers[0].is_idle);
        assert!(primary.workers[0].current_task.is_none());

        primary.dispatch_to_idle_workers().await.unwrap();

        assert_eq!(
            primary.pool().len(),
            pool_len_before,
            "dispatch_to_idle_workers must not take from pool when demoted"
        );
        assert!(
            primary.workers[0].is_idle,
            "worker must remain idle when local primary is demoted"
        );
        assert!(
            primary.workers[0].current_task.is_none(),
            "worker must not be assigned a task when local primary is demoted"
        );
    }).await;
}
