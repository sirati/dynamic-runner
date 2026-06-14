//! Production replay: the deposed-primary zombie split-brain (asm-dataset
//! run_20260610_221140).
//!
//! The epoch-9 primary declared "cluster routing collapsed" and broadcast
//! the `RunAborted` terminal verdict at 20:26:44. TWO MINUTES LATER the
//! DEPOSED epoch-2 primary — which had kept running its primary pipeline
//! after epoch 3+ existed — logged `primary finished succeeded=165 …`
//! and exited rc=0 with totals DIVERGENT from the abort verdict: one run,
//! two contradictory verdicts from two primaries.
//!
//! These tests pin the three stand-down seams:
//!
//! - the anti-entropy `ClusterSnapshot` pull-reply is INGESTIBLE on a
//!   primary (pre-fix it fell through the unhandled-type catch-all, so the
//!   pull issued by `handle_state_digest` could never converge a zombie),
//!   and ingesting a higher-epoch snapshot fires the BUG-6 demote signal;
//! - a primary that observes the replicated `RunAborted` latch STANDS
//!   DOWN: it adopts the verdict (non-zero, the abort reason) and never
//!   authors `RunComplete` / "run complete:";
//! - a primary that loses primary RECOGNITION (the CRDT register names
//!   another holder at a higher epoch) must not author ANY clean verdict
//!   at run end — the exit is a structured `Deposed`, not rc=0.

use super::*;

use dynrunner_protocol_primary_secondary::PrimaryChangeReason;

/// The anti-entropy `ClusterSnapshot` reply (the pull `handle_state_digest`
/// requests when a peer's digest proves this node behind) must be RESTORED
/// into the primary's `cluster_state`: the register adopts the higher
/// epoch + holder, the sticky `run_aborted` verdict latches, and the BUG-6
/// displaced hook fires the demote signal. Pre-fix the primary's
/// `dispatch_message` had NO snapshot-reply arm — the frame fell
/// through the catch-all, so a dead-leg-starved zombie that DID hear a
/// digest could request the snapshot but never converge on its reply.
#[tokio::test(flavor = "current_thread")]
async fn cluster_snapshot_reply_is_ingested_and_fires_demote() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // The node-owned demote channel (BUG-6): the displaced hook
            // must fire on the restore-driven self→other flip exactly as
            // on a directly-applied PrimaryChanged.
            let (demote_tx, mut demote_rx) = tokio_mpsc::unbounded_channel();
            primary.register_demote_on_displaced(demote_tx);

            // This primary believes it holds authority.
            let own_id = primary.config.node_id.clone();
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PrimaryChanged {
                    new: own_id,
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });
            assert!(
                demote_rx.try_recv().is_err(),
                "naming SELF is not a displacement"
            );

            // A peer's converged snapshot: epoch 9 names another holder and
            // carries the sticky RunAborted verdict.
            let mut ahead = crate::cluster_state::ClusterState::<TestId>::new();
            ahead.apply(ClusterMutation::PrimaryChanged {
                new: "usurper".into(),
                epoch: 9,
                reason: PrimaryChangeReason::Election,
            });
            ahead.apply(ClusterMutation::RunAborted {
                reason: "cluster routing collapsed (replayed verdict)".into(),
                counts: Default::default(),
            });
            for reply in
                crate::snapshot_stream::stream_frames_for_test(&ahead, "sec-0", "prim/0")
            {
                primary
                    .dispatch_message(reply, &mut None)
                    .await
                    .expect("snapshot package ingest ok");
            }

            let state = primary.cluster_state_for_test();
            assert_eq!(
                state.current_primary(),
                Some("usurper"),
                "the snapshot reply must be restored — the higher-epoch \
                 register wins (pre-fix the frame was dropped unhandled)"
            );
            assert_eq!(state.primary_epoch(), 9);
            assert_eq!(
                state.run_aborted(),
                Some("cluster routing collapsed (replayed verdict)"),
                "the sticky run-terminal verdict must latch from the snapshot"
            );
            assert!(
                demote_rx.try_recv().is_ok(),
                "the restore-driven self→other flip must fire the BUG-6 \
                 demote signal so run_consuming drops the zombie pipeline"
            );
        })
        .await;
}

/// A primary whose replicated ledger carries the cluster's `RunAborted`
/// verdict (here: present before the operational loop entered — the
/// snapshot-carried / zombie-heard-late shape) must STAND DOWN: adopt the
/// verdict as a structured non-zero exit, author NO `RunComplete`, and
/// never log "run complete:". Production: the deposed epoch-2 primary
/// ran 2 more minutes and exited rc=0 with divergent totals.
#[tokio::test(flavor = "current_thread")]
async fn replicated_abort_verdict_stands_primary_down_without_clean_finish() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let (pri_to_sec_tx, sec_to_pri_rx, _sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            seed_operational_ledger(&mut primary, binaries, HashMap::new());

            // The cluster's terminal verdict is already resident in the
            // replicated state this primary holds (the anti-entropy /
            // zombie-heard-a-peer shape).
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::RunAborted {
                    reason: "cluster routing collapsed (replayed verdict)".into(),
                    counts: Default::default(),
                });

            let (_deps, on_start, on_end) = noop_phase_args();
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                primary.run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end),
            )
            .await
            .expect("the run must stand down promptly on the latched verdict");

            match &result {
                Err(RunError::AbortedByClusterVerdict { reason }) => {
                    assert!(
                        reason.contains("replayed verdict"),
                        "the adopted verdict must carry the cluster's abort \
                         reason; got: {reason}"
                    );
                }
                other => panic!(
                    "a primary observing the replicated RunAborted latch must \
                     adopt it and stand down (NOT author a clean finish); \
                     got {other:?}"
                ),
            }
            assert!(
                !primary.cluster_state_for_test().run_complete(),
                "a stood-down primary must NEVER author RunComplete over an \
                 aborted run (the production zombie's rc=0 'primary finished')"
            );
        })
        .await;
}

/// A primary that loses primary RECOGNITION mid-run (the replicated
/// register adopts a higher-epoch holder — here learned only at the very
/// end, the dead-leg-starved production shape) must NOT author a clean
/// verdict at run end: no `RunComplete`, no "run complete:" — a
/// structured `Deposed` exit instead. Production: the deposed epoch-2
/// primary exited rc=0 with `primary finished succeeded=165 fail_final=108`
/// against the cluster's 153/120 abort verdict.
#[tokio::test(flavor = "current_thread")]
async fn deposed_primary_authors_no_clean_verdict_at_run_end() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let secondary_id = "sec-0".to_string();
            let max_res = dynrunner_core::ResourceMap::from([(
                dynrunner_core::ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            let (pri_to_sec_tx, sec_to_pri_rx, _sec_handle) =
                spawn_real_secondary(secondary_id.clone(), 2, max_res);

            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            // Injection handle: the "usurper" peer's announcement reaching
            // this primary's inbound late in the run.
            let inject_tx = incoming_tx.clone();
            let mut outgoing = HashMap::new();
            outgoing.insert(secondary_id.clone(), pri_to_sec_tx);
            tokio::task::spawn_local(async move {
                let mut rx = sec_to_pri_rx;
                while let Some(msg) = rx.recv().await {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
            });

            let transport =
                ChannelPeerTransport::from_raw_channels("setup".into(), outgoing, incoming_rx);
            let config = PrimaryConfig {
                connect_timeout: Duration::from_secs(10),
                peer_timeout: Duration::from_secs(10),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            let binaries: Vec<TaskInfo<TestId>> = (0..3)
                .map(|i| make_binary(&format!("bin_{i}"), 50 + i * 10))
                .collect();
            seed_operational_ledger(&mut primary, binaries, HashMap::new());

            // On the (single) phase end — i.e. with every task already
            // terminal, microseconds before the run's exit check — a
            // higher-epoch `PrimaryChanged` from the cluster's REAL primary
            // finally reaches this node's inbound. The finalize drain
            // ingests it before any verdict is authored.
            let on_start: OnPhaseStart = Box::new(|_p| {});
            let mut injected = false;
            let on_end: OnPhaseEnd = Box::new(move |_p, _c, _f, _outputs| {
                if injected {
                    return;
                }
                injected = true;
                let _ = inject_tx.send(DistributedMessage::ClusterMutation {
                    target: None,
                    sender_id: "usurper".into(),
                    timestamp: 0.0,
                    mutations: vec![ClusterMutation::PrimaryChanged {
                        new: "usurper".into(),
                        epoch: 99,
                        reason: PrimaryChangeReason::Election,
                    }],
                });
            });

            let result = tokio::time::timeout(
                Duration::from_secs(30),
                primary.run(SeedSource::PromotionSnapshot { kind: crate::process::BootstrapKind::Failover }, on_start, on_end),
            )
            .await
            .expect("the run must finish promptly");

            match &result {
                Err(RunError::Deposed {
                    current_primary, ..
                }) => {
                    assert_eq!(
                        current_primary, "usurper",
                        "the deposed exit must name the recognized holder"
                    );
                }
                other => panic!(
                    "a primary without current recognition must not author a \
                     clean verdict — the production zombie's rc=0 'primary \
                     finished' over a divergent ledger; got {other:?}"
                ),
            }
            let state = primary.cluster_state_for_test();
            assert!(
                !state.run_complete(),
                "a deposed primary must NOT author RunComplete (the verdict \
                 belongs to the epoch-99 holder)"
            );
            assert!(
                state.run_aborted().is_none(),
                "a deposed primary must not author a RunAborted verdict either \
                 — it holds no authority to conclude the run"
            );
        })
        .await;
}
