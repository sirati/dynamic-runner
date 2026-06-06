//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

use crate::primary::wire::compute_task_hash;

/// One advertised-memory resource amount (in bytes), the live welcome
/// shape: a single `memory` `ResourceAmount`.
fn mem(bytes: u64) -> Vec<dynrunner_core::ResourceAmount> {
    vec![dynrunner_core::ResourceAmount {
        kind: dynrunner_core::ResourceKind::memory(),
        amount: bytes,
    }]
}

/// A promotion that inherits N `Pending` tasks must dispatch ALL of them —
/// the F1 regression guard. Pre-F1, `run_pipeline` rebuilt the pool from the
/// (empty) `binaries` run-arg, clobbering the hydrate-built pool to empty and
/// zeroing `total_tasks`; the counter exit (`0 + 0 >= 0`) then tripped on the
/// first operational-loop iteration and broadcast `RunComplete` with ZERO
/// tasks dispatched (the silent false-complete bug). Post-F1 the unified init
/// keys on `SeedSource::PromotionSnapshot`: it originates nothing and
/// `hydrate_from_cluster_state` is the SOLE pool builder, so the N inherited
/// Pending tasks are all dispatched and completed — `completed == N`,
/// `stranded == 0`, NO premature `RunComplete`.
#[tokio::test(flavor = "current_thread")]
async fn mid_run_failover_dispatches_all_inherited_pending() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const N: usize = 5;

            // --- Live primary: seed N Pending tasks + one SecondaryCapacity
            // for sec-0, then snapshot its converged ledger (the payload a
            // promotion carries). ---
            let snapshot = {
                let (transport, _ends) = setup_test(1);
                let (mut live, _mesh) = build_test_primary(
                    PrimaryConfig::default(),
                    transport,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );
                let cs = live.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::new(),
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 2,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                for i in 0..N {
                    let task = make_binary(&format!("t-{i}"), 100);
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: compute_task_hash(&task),
                        task,
                    });
                }
                assert_eq!(cs.task_count(), N, "live ledger holds N Pending tasks");
                live.cluster_state_for_test().snapshot()
            };

            // --- Promoted primary: restore + hydrate + reconstruct via the
            // production promotion construction primitive. ---
            let (transport, secondary_ends) = setup_test(1);
            let config = PrimaryConfig {
                num_secondaries: 1,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };
            let (mut promoted, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            promoted.seed_from_promotion_snapshot(snapshot);

            // PRE-RUN invariant: hydrate built the pool + total_tasks from the
            // inherited CRDT. The F1-broken `run_pipeline` would clobber these
            // to empty/0 inside the run; here they are correct BEFORE the run.
            assert_eq!(
                promoted.pool().len(),
                N,
                "all N inherited Pending tasks hydrated into the pool"
            );
            assert_eq!(
                promoted.total_tasks, N,
                "total_tasks hydrated from the inherited ledger"
            );

            // --- Drive the promoted primary's run on the inherited ledger
            // against a fake secondary that answers TaskRequests + completes
            // every dispatched task. ---
            for (id, rx, tx) in secondary_ends {
                tokio::task::spawn_local(fake_secondary(id, 2, 1024 * 1024 * 1024, rx, tx));
            }

            let (_deps, ops, ope) = noop_phase_args();
            let outcome = promoted
                .run_consuming(SeedSource::PromotionSnapshot, ops, ope)
                .await
                .expect("promoted run must not error");

            match outcome {
                PrimaryRunOutcome::Local {
                    result,
                    completed,
                    failed,
                    stranded,
                } => {
                    assert!(result.is_ok(), "run completed cleanly: {result:?}");
                    assert_eq!(
                        completed, N,
                        "every inherited Pending task was dispatched + completed \
                         (pre-F1 false-completed with 0 dispatched)"
                    );
                    assert_eq!(failed, 0, "no task failed");
                    assert_eq!(
                        stranded, 0,
                        "no inherited task was stranded by a premature RunComplete"
                    );
                }
                PrimaryRunOutcome::Relocated { .. } => {
                    panic!("promoted primary should run to a Local outcome, not relocate")
                }
            }
        })
        .await;
}

/// Multi-secondary mesh-ready gate: the primary must NOT issue its
/// bootstrap primary announcement (`ClusterMutation::PrimaryChanged
/// { new = primary }`) until every connected secondary has reported
/// `MeshReady`. Pre-fix the announcement fired ~750µs after cert-
/// exchange completed; the newly-named primary then became
/// authoritative against a still-forming peer mesh, and every
/// pre-mesh-formation peer-broadcast routed into the void for up
/// to 30s. This test pins the new ordering: the `PrimaryChanged`
/// frame arrives at every fake secondary AFTER all of them have sent
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
            // order to enforce: the primary doesn't announce
            // PrimaryChanged until ALL three have flipped.
            let mut mesh_triggers: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
            // Per-secondary observation: did this secondary see the
            // primary's `PrimaryChanged` announcement BEFORE it was
            // allowed to send MeshReady? (true = bug present)
            let mut promote_seen_pre_mesh_observers: Vec<tokio::sync::oneshot::Receiver<bool>> =
                Vec::new();

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
                num_secondaries: N_SECONDARIES,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                // Generous timeout so the test can fire triggers
                // sequentially without racing the deadline.
                mesh_ready_timeout: std::time::Duration::from_secs(10),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
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
                primary
                .run(SeedSource::ColdStart { binaries, phase_deps: deps }, ops, ope)
                .await
                .unwrap();
                primary.completed_count()
            });

            // Release MeshReady triggers one at a time. Between
            // each release, yield enough times for the primary's
            // wait loop to observe the freshly-arrived
            // MeshReady. The primary must NOT have advanced past
            // `wait_for_mesh_ready` until all three triggers have
            // fired — otherwise the per-secondary "did I see the
            // PrimaryChanged announcement before being allowed to
            // MeshReady?" observer would have reported true for some
            // of them.
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
            // them should have seen the PrimaryChanged announcement
            // before being allowed to send MeshReady.
            for (i, obs) in promote_seen_pre_mesh_observers.into_iter().enumerate() {
                let saw = obs.await.expect("observer recv");
                assert!(
                    !saw,
                    "secondary {i} observed the PrimaryChanged announcement \
                     BEFORE its own MeshReady was allowed to send — primary's \
                     wait_for_mesh_ready step is not gating the announcement"
                );
            }

            let completed = primary_handle.await.unwrap();
            assert_eq!(completed, 6, "all 6 tasks should complete");
        })
        .await;
}

/// Fake secondary that defers `MeshReady` until the test fires
/// `mesh_trigger`. Reports via `observer` whether it saw the primary's
/// `PrimaryChanged` announcement arrive before its `MeshReady` was
/// permitted to send (true = bug). Otherwise behaves like
/// `fake_secondary`.
async fn gated_mesh_secondary(
    secondary_id: String,
    num_workers: u32,
    ram_bytes: u64,
    mut incoming_from_primary: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    outgoing_to_primary: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    mesh_trigger: tokio::sync::oneshot::Receiver<()>,
    observer: tokio::sync::oneshot::Sender<bool>,
) {
    use dynrunner_protocol_primary_secondary::ClusterMutation;

    /// The primary's bootstrap promotion announcement is a
    /// `ClusterMutation` carrying `PrimaryChanged` — detect it to gate.
    fn is_primary_changed(m: &DistributedMessage<TestId>) -> bool {
        matches!(
                m,
                DistributedMessage::ClusterMutation {
        target: _, mutations, .. }
                    if mutations
                        .iter()
                        .any(|mu| matches!(mu, ClusterMutation::PrimaryChanged { .. }))
            )
    }

    outgoing_to_primary
        .send(DistributedMessage::SecondaryWelcome {
            target: None,
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
            can_be_primary: false,
        })
        .unwrap();

    outgoing_to_primary
        .send(DistributedMessage::CertExchange {
            target: None,
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
    // observing the PrimaryChanged announcement on the inbound path.
    // If PrimaryChanged arrives first, the gate failed.
    let mut mesh_trigger_opt = Some(mesh_trigger);
    let mut observer_opt = Some(observer);
    let mut mesh_sent = false;
    let mut promote_seen_pre_mesh = false;

    loop {
        // While we're still pre-MeshReady, race the trigger
        // against an inbound PrimaryChanged. After MeshReady has
        // been sent, the trigger arm is removed and we fall back
        // to a normal recv loop.
        if !mesh_sent {
            let trigger = mesh_trigger_opt.as_mut().unwrap();
            tokio::select! {
                _ = trigger => {
                    outgoing_to_primary
                        .send(DistributedMessage::MeshReady {
                            target: None,
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
                        if is_primary_changed(&m) {
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
            // primary stops dispatching after relinquishing via
            // `PrimaryChanged`, so a
            // mis-attributed TaskComplete leaves the OTHER worker
            // permanently mid-dispatch and `active_workers > 0`
            // forever — operational_loop never terminates. (The
            // primary stops dispatching once it relinquishes the role
            // via `PrimaryChanged`.)
            let entries: Vec<_> = zip_files.iter().flat_map(|zf| zf.binaries.iter()).collect();
            for (idx, entry) in entries.iter().enumerate() {
                let worker_id = workers_ready.get(idx).map(|w| w.worker_id).unwrap_or(0);
                let _ = outgoing.send(DistributedMessage::TaskComplete {
                    target: None,
                    sender_id: secondary_id.into(),
                    timestamp: 0.0,
                    secondary_id: secondary_id.into(),
                    worker_id,
                    task_hash: entry.hash.clone(),
                    result_data: None,
                });
                let _ = outgoing.send(DistributedMessage::TaskRequest {
                    target: None,
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
                target: None,
                sender_id: secondary_id.into(),
                timestamp: 0.0,
                secondary_id: secondary_id.into(),
                worker_id: 0,
                task_hash: file_hash,
                result_data: None,
            });
            let _ = outgoing.send(DistributedMessage::TaskRequest {
                target: None,
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
    local
        .run_until(async {
            let (transport, mut secondary_ends) = setup_test(2);

            let config = PrimaryConfig {
                num_secondaries: 2,
                connect_timeout: Duration::from_secs(5),
                peer_timeout: Duration::from_secs(5),
                ..test_primary_config()
            };

            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
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
                    // Snoop at the WIRE level (before any local-delivery
                    // target-clear): the primary's PeerInfo broadcast is
                    // stamped `Destination::All` by the egress, so accept ANY
                    // target here (the routing header is not what we assert).
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
            primary
                .run(SeedSource::ColdStart { binaries, phase_deps: deps }, ops, ope)
                .await
                .unwrap();

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

/// The deepest failover-completeness guard: a retry reset
/// (`Failed { attempt: n } → Pending { attempt: n+1 }`) MUST survive a
/// `restore()` of a peer snapshot still holding the stale
/// `Failed { attempt: n }`.
///
/// Pre-fix, the retry-bucket reinjected a failed task into the LOCAL pool
/// but emitted NO CRDT mutation, so the ledger stayed `Failed`; even a
/// naive new `Failed → Pending` apply-arm would NOT survive anti-entropy,
/// because `restore` routes every task through the band-first join and
/// `Failed` (Terminal band) out-ranks `Pending` (NonTerminal band) BEFORE
/// version is consulted — the reset would be reverted to `Failed` on every
/// heal, orphaning the in-flight retry = lost work. Option (i) puts the
/// per-task `attempt` generation at the TOP of the join key (above band),
/// so the attempt-(n+1) `Pending` dominates the attempt-n `Failed` across
/// EVERY merge path including restore/anti-entropy.
///
/// NEGATIVE-CONTROL CONFIRMATION: this test FAILS against a naive
/// no-attempt arm (where the reset would be `Pending { attempt: 0 }`
/// against `Failed { attempt: 0 }` — equal attempt, so band dominates and
/// the restore reverts to `Failed`), and PASSES under the attempt-at-top
/// design (the load-bearing step-5 assertion below). It was confirmed
/// fail-naive by temporarily neutralizing the attempt-ordering (forcing
/// the reset attempt to 0): step 5 then reverts to `Failed` as predicted.
#[tokio::test(flavor = "current_thread")]
async fn retry_reset_survives_anti_entropy_heal() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // --- Build primary P1 and seed one task hash H as
            // Failed { attempt: 0 } (TaskAdded then TaskFailed Recoverable). ---
            let (transport, _ends) = setup_test(1);
            let (mut p1, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let task = make_binary("h", 100);
            let h = compute_task_hash(&task);
            let cs = p1.cluster_state_mut_for_test();
            cs.apply(ClusterMutation::TaskAdded {
                hash: h.clone(),
                task,
            });
            cs.apply(ClusterMutation::TaskFailed {
                hash: h.clone(),
                kind: dynrunner_core::ErrorType::Recoverable,
                error: "transient".into(),
                version: Default::default(),
                attempt: 0,
            });
            assert_eq!(
                p1.cluster_state_for_test().task_state(&h).unwrap().attempt(),
                0,
                "seed: H is at the cold generation"
            );

            // --- Capture the stale peer view (P2's snapshot before the
            // reset reaches it: H is still Failed { attempt: 0 }). ---
            let stale = p1.cluster_state_for_test().snapshot();

            // --- Originate the retry reset on P1: TaskRetried bumps
            // attempt 0 → 1 and crosses Failed → Pending. ---
            p1.cluster_state_mut_for_test()
                .apply(ClusterMutation::TaskRetried {
                    hash: h.clone(),
                    attempt: 1,
                    version: Default::default(),
                });
            {
                let st = p1.cluster_state_for_test().task_state(&h).unwrap();
                assert!(
                    matches!(st, crate::cluster_state::TaskState::Pending { .. }),
                    "after TaskRetried, H is Pending"
                );
                assert_eq!(st.attempt(), 1, "the reset minted the next generation");
            }

            // --- Heal: P1 restores the STALE peer snapshot (models the
            // anti-entropy pull of P2's view INTO P1 — both sides are
            // detected behind because the tasks_hash diverges). ---
            p1.cluster_state_mut_for_test().restore(stale.clone());

            // --- THE load-bearing assertion: the attempt-1 Pending
            // out-ranks the attempt-0 Failed across the band boundary, so
            // restore's merge does NOT revert the reset. ---
            {
                let st = p1.cluster_state_for_test().task_state(&h).unwrap();
                assert!(
                    matches!(st, crate::cluster_state::TaskState::Pending { .. }),
                    "the retry reset SURVIVES the stale-Failed restore \
                     (attempt dominates band) — a naive no-attempt arm \
                     would revert to Failed here"
                );
                assert_eq!(st.attempt(), 1, "H stays at the reset generation");
            }

            // --- Symmetric direction: a fresh P2 holding the stale
            // Failed { attempt: 0 } restores P1's reset snapshot and must
            // ALSO converge to Pending { attempt: 1 }. ---
            let reset_snap = p1.cluster_state_for_test().snapshot();
            let (transport2, _ends2) = setup_test(1);
            let (mut p2, _mesh2) = build_test_primary(
                test_primary_config(),
                transport2,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // P2 starts from the stale Failed view, then heals from P1.
            p2.cluster_state_mut_for_test().restore(stale);
            assert_eq!(
                p2.cluster_state_for_test().task_state(&h).unwrap().attempt(),
                0,
                "P2 begins at the stale generation"
            );
            p2.cluster_state_mut_for_test().restore(reset_snap);
            {
                let st = p2.cluster_state_for_test().task_state(&h).unwrap();
                assert!(
                    matches!(st, crate::cluster_state::TaskState::Pending { .. }),
                    "P2 converges to the reset (attempt dominates)"
                );
                assert_eq!(st.attempt(), 1, "both replicas converge to Pending{{1}}");
            }
        })
        .await;
}

/// A promotion that inherits an `InFlight` task whose type is capped must
/// reserve the per-type concurrency slot on hydrate, so the eventual
/// terminal release (`free_slot_on_terminal`'s `saturating_sub`) is
/// symmetric. Pre-fix `seed_inflight` inserted the ledger entry without a
/// `reserve_type_slot`, so the counter sat at 0; the inherited task's
/// completion then fired `saturating_sub` against 0 (clamped, no underflow
/// panic) and the cap desynced — every subsequent dispatch saw a phantom
/// free slot and over-dispatched past `max_concurrent_per_type`. This test
/// pins the reservation (counter == 1 after hydrate) AND the symmetric
/// release (back to 0 after the broadcast completion, NOT a stuck-low
/// counter).
#[tokio::test(flavor = "current_thread")]
async fn promoted_inflight_reserves_per_type_slot_for_symmetric_release() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let cap_type = dynrunner_core::TypeId::from("default");

            let (transport, _ends) = setup_test(1);
            let config = PrimaryConfig {
                num_secondaries: 1,
                max_concurrent_per_type: HashMap::from([(cap_type.clone(), 4)]),
                ..test_primary_config()
            };
            let (mut promoted, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let task = make_binary("inflight-cap", 100);
            {
                let cs = promoted.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "secondary-0".into(),
                    worker_count: 1,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "inflight-cap".into(),
                    task: task.clone(),
                });
                cs.apply(ClusterMutation::TaskAssigned {
                    hash: "inflight-cap".into(),
                    secondary: "secondary-0".into(),
                    worker: 0,
                    version: Default::default(),
                    attempt: 0,
                });
            }

            promoted.hydrate_from_cluster_state();

            // The inherited InFlight task reserved its type slot — symmetric
            // with the live `commit_assignment` path.
            assert_eq!(
                promoted.in_flight_per_type_for_test(&cap_type),
                1,
                "inherited InFlight task must reserve its per-type slot on hydrate"
            );

            // The broadcast completion releases the slot through
            // `free_slot_on_terminal`; the reserved counter drops back to 0.
            let msg = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "secondary-0".into(),
                timestamp: 0.0,
                secondary_id: "secondary-0".into(),
                worker_id: 0,
                task_hash: "inflight-cap".into(),
                result_data: None,
            };
            promoted.handle_task_complete(msg, &mut None).await;

            assert_eq!(
                promoted.in_flight_per_type_for_test(&cap_type),
                0,
                "terminal release must return the per-type counter to 0 \
                 (reservation + release are symmetric — no stuck-low cap desync)"
            );
        })
        .await;
}

/// A promotion-hydrate must rebuild `all_binaries` (the OOM/retry candidate
/// source `retry_bucket` filters against) from the inherited CRDT. Pre-fix
/// the seeded path left it empty, so a promoted primary's retry bucket had
/// NO candidate source. Every ledger entry carries its `TaskInfo` regardless
/// of state, so the rebuilt universe spans Pending + InFlight + terminal.
#[test]
fn promoted_hydrate_rebuilds_all_binaries_candidate_source() {
    let (transport, _ends) = setup_test(1);
    let (mut promoted, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    {
        let cs = promoted.cluster_state_mut_for_test();
        // A spread of states: Pending, InFlight, and terminal Completed —
        // all three must contribute to the candidate universe.
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "secondary-0".into(),
            worker_count: 1,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        for name in ["pend-1", "pend-2"] {
            let t = make_binary(name, 100);
            cs.apply(ClusterMutation::TaskAdded {
                hash: name.into(),
                task: t,
            });
        }
        let inflight = make_binary("inflight-1", 100);
        cs.apply(ClusterMutation::TaskAdded {
            hash: "inflight-1".into(),
            task: inflight,
        });
        cs.apply(ClusterMutation::TaskAssigned {
            hash: "inflight-1".into(),
            secondary: "secondary-0".into(),
            worker: 0,
            version: Default::default(),
            attempt: 0,
        });
        let done = make_binary("done-1", 100);
        cs.apply(ClusterMutation::TaskAdded {
            hash: "done-1".into(),
            task: done,
        });
        cs.apply(ClusterMutation::TaskCompleted {
            hash: "done-1".into(),
            result_data: None,
            attempt: 0,
        });
    }

    // Pre-condition: the freshly-built coordinator has an empty universe.
    assert!(
        promoted.all_binaries.is_empty(),
        "all_binaries starts empty before hydrate"
    );

    promoted.hydrate_from_cluster_state();

    // The candidate universe spans EVERY ledger entry (4 tasks across
    // three state classes), so the retry bucket has a source to filter.
    assert_eq!(
        promoted.all_binaries.len(),
        4,
        "all_binaries must rebuild from every CRDT entry regardless of state"
    );
    let names: std::collections::HashSet<&str> = promoted
        .all_binaries
        .iter()
        .map(|t| t.task_id.as_str())
        .collect();
    assert!(names.contains("pend-1"));
    assert!(names.contains("inflight-1"));
    assert!(names.contains("done-1"));
}

/// A promotion-hydrate must advance `next_secondary_id` past the highest id
/// already in the inherited roster, so a respawn after promotion never mints
/// an id that collides with one the pre-failover primary already minted.
/// Pre-fix `next_secondary_id` reset to `config.num_secondaries` on
/// promotion, colliding with any respawned id above that floor.
#[test]
fn promoted_hydrate_advances_next_secondary_id_past_roster_max() {
    let (transport, _ends) = setup_test(1);
    let config = PrimaryConfig {
        num_secondaries: 2,
        ..test_primary_config()
    };
    let (mut promoted, _mesh) = build_test_primary(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    {
        let cs = promoted.cluster_state_mut_for_test();
        // Roster from a run that had already respawned past the bootstrap
        // range: ids secondary-0, secondary-1 (bootstrap) + secondary-5
        // (a respawn the pre-failover primary minted).
        for id in ["secondary-0", "secondary-1", "secondary-5"] {
            cs.apply(ClusterMutation::SecondaryCapacity {
                secondary: id.into(),
                worker_count: 1,
                resources: mem(8 * 1024 * 1024 * 1024),
            });
        }
    }

    promoted.hydrate_from_cluster_state();

    // The next mint must exceed the highest known id (secondary-5 → 6),
    // NOT reset to the `num_secondaries` floor (2) where it would collide
    // with secondary-2..secondary-5.
    assert_eq!(
        promoted.next_secondary_id, 6,
        "next_secondary_id must be max(known roster id) + 1, not the \
         num_secondaries floor"
    );
    assert_eq!(
        promoted.mint_secondary_id(),
        "secondary-6",
        "the first respawn after promotion mints a collision-free id"
    );
}

/// A promotion-hydrate must seed `phase_started_emitted` from the inherited
/// CRDT (a phase is "started" iff its rollup `has_any`), so a promoted
/// primary does NOT re-fire `on_phase_start` for a phase that already started
/// pre-failover. Seeded ONLY on the promote path
/// (`seed_from_promotion_snapshot`); the cold path's freshly-`Pending` CRDT
/// must NOT suppress the legitimate first fire.
#[test]
fn promoted_hydrate_seeds_phase_started_emitted_so_on_phase_start_not_refired() {
    let (transport, _ends) = setup_test(1);
    let (mut live, _mesh) = build_test_primary(
        PrimaryConfig::default(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    {
        let cs = live.cluster_state_mut_for_test();
        // A started phase "build" (≥1 ledger entry) and a never-reached
        // phase "ship" (no entries).
        let mut t = make_binary("b-1", 100);
        t.phase_id = dynrunner_core::PhaseId::from("build");
        cs.apply(ClusterMutation::TaskAdded {
            hash: "b-1".into(),
            task: t,
        });
    }
    let snapshot = live.cluster_state_for_test().snapshot();

    let (transport2, _ends2) = setup_test(1);
    let (mut promoted, _mesh2) = build_test_primary(
        PrimaryConfig::default(),
        transport2,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    promoted.seed_from_promotion_snapshot(snapshot);

    // "build" is seeded as already-started (so `on_phase_start` won't
    // re-fire); "ship" (no ledger entry) is NOT, so a later fire there is
    // still legitimate.
    assert!(
        promoted
            .phase_started_emitted
            .contains(&dynrunner_core::PhaseId::from("build")),
        "an already-started phase must be seeded so on_phase_start does not re-fire"
    );
    assert!(
        !promoted
            .phase_started_emitted
            .contains(&dynrunner_core::PhaseId::from("ship")),
        "a never-reached phase must NOT be marked started"
    );
}
