#![cfg(test)]

    //! Tests pinning Step 10's setup-promote behaviour on the
    //! `SecondaryCoordinator`. Each test drives `dispatch_message` or
    //! `record_promotion_confirm` directly so the assertions are on
    //! the state-machine outcome, not on a select! loop's timing.

    use super::super::test_helpers::{election_config, FakeWorkerFactory, FixedEstimator, RecordingPeer, TestId};
    use super::super::*;
    use dynrunner_core::{PhaseId, SoftPreferredSecondaries, TaskInfo, TypeId};
    use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;
    use tokio::sync::mpsc as tokio_mpsc;

    /// Build a SecondaryCoordinator backed by a `RecordingPeer`. The
    /// `(secondary, primary_rx, peer_log)` triple is the same shape
    /// the existing watchdog / R1 helpers use: primary-side mpsc on
    /// the LHS, peer-side recorder on the RHS. Each test inspects
    /// whichever it cares about.
    ///
    /// Uses `election_config` so the keepalive / death-threshold
    /// constants match the surrounding test ecosystem; the four
    /// setup-promote tests don't drive election timing but reusing
    /// the helper keeps the construction site identical to the R1
    /// tests for grep-affinity.
    // SecondaryCoordinator's six type parameters mean any return
    // shape that mentions it qualifies as "complex"; a type alias
    // would mirror the same shape and is not warranted for a
    // one-off test helper.
    #[allow(clippy::type_complexity)]
    fn make_secondary_with_recording_peer(
        secondary_id: &str,
        peer_count: usize,
    ) -> (
        SecondaryCoordinator<
            ChannelPrimaryTransportEnd<TestId>,
            RecordingPeer<TestId>,
            dynrunner_transport_channel::ChannelManagerEnd,
            ResourceStealingScheduler,
            FixedEstimator,
            TestId,
        >,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) {
        let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (_pri_to_sec_tx, pri_to_sec_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let recorder = RecordingPeer::<TestId>::new(peer_count);
        let peer_log = recorder.log_handle();
        let sec = SecondaryCoordinator::new(
            election_config(secondary_id),
            transport,
            recorder,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        );
        (sec, sec_to_pri_rx, peer_log)
    }

    /// Same shape as `make_binary` at the top of `tests.rs` but kept
    /// local so the module compiles even if the top-level helper's
    /// signature drifts. `/tmp/...` paths so the dispatch.rs
    /// resolvable-task guard doesn't reject relative paths under
    /// `src_network=None`.
    fn make_binary(name: &str, phase: &str) -> TaskInfo<TestId> {
        TaskInfo {
            path: PathBuf::from(format!("/tmp/{name}")),
            size: 100,
            identifier: TestId(name.into()),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: None,
            task_depends_on: vec![],
            preferred_secondaries: SoftPreferredSecondaries::default(),
            resolved_path: None,
        }
    }

    /// Count `ClusterMutation` envelopes carrying a `TaskAdded`. A
    /// single broadcast may batch many `TaskAdded` mutations into one
    /// envelope; the assertions in test 1 want the count of ADDED
    /// items across all envelopes, so flatten before counting.
    fn count_task_added_mutations(
        log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) -> usize {
        log.borrow()
            .iter()
            .flat_map(|msg| match msg {
                DistributedMessage::ClusterMutation { mutations, .. } => mutations.iter(),
                _ => [].iter(),
            })
            .filter(|m| matches!(m, ClusterMutation::TaskAdded { .. }))
            .count()
    }

    /// Reason 2: setup-promote. `PromotePrimary { required_setup: true }`
    /// arrives → `setup_pending` latches true and the pool is NOT
    /// hydrated yet (cluster_state is empty by contract — submitter
    /// deferred everything to us). After `ingest_setup_discovery` runs:
    ///   - the cluster ledger holds the discovered tasks,
    ///   - the primary pool is hydrated (size == discovered count),
    ///   - `setup_pending` clears,
    ///   - two `TaskAdded` mutations + one `PhaseDepsSet` were
    ///     broadcast to peers.
    #[tokio::test(flavor = "current_thread")]
    async fn test_setup_promote_runs_discovery_then_seeds_then_populates() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 0);

                // Pre-condition: no cluster_state content, setup_pending
                // false. This is the wire-state contract for the
                // setup-promote path.
                assert_eq!(sec.cluster_state.counts().pending, 0);
                assert!(!sec.setup_pending);

                let promote = DistributedMessage::PromotePrimary {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    new_primary_id: "sec-a".into(),
                    epoch: 1,
                    required_setup: true,
                };
                sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
                    .await
                    .expect("PromotePrimary handler succeeds");

                // Post-handler: latched setup_pending; we ARE primary
                // but NO hydration yet — the pool stays unbuilt because
                // there's nothing to populate from.
                assert!(sec.is_primary, "we are now primary");
                assert!(
                    sec.setup_pending,
                    "required_setup=true must latch setup_pending"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    0,
                    "pool stays unbuilt until ingest_setup_discovery feeds the ledger"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    0,
                    "promotion alone broadcasts no TaskAdded — discovery hasn't run yet"
                );

                // Now drive the wrapper's contract: call
                // ingest_setup_discovery with two mock binaries and an
                // empty phase_deps map (single default phase, no edges).
                let binaries = vec![
                    make_binary("bin-a", "default"),
                    make_binary("bin-b", "default"),
                ];
                let phase_deps: HashMap<PhaseId, Vec<PhaseId>> = HashMap::new();
                sec.ingest_setup_discovery(binaries, phase_deps)
                    .await
                    .expect("ingest_setup_discovery succeeds");

                // Post-ingest: ledger has 2 items, pool hydrated to 2,
                // setup_pending cleared, 2 TaskAdded broadcasts went to
                // peers (plus one PhaseDepsSet envelope which the
                // count helper skips).
                assert_eq!(
                    sec.cluster_state.task_count(),
                    2,
                    "cluster ledger holds the two discovered binaries"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    2,
                    "primary pool hydrated to the discovered count"
                );
                assert!(
                    !sec.setup_pending,
                    "setup_pending clears after successful ingest"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    2,
                    "two TaskAdded mutations broadcast to peers"
                );
            })
            .await;
    }

    /// Reason 1: pre-seeded bootstrap (`required_setup_on_promote =
    /// false` — submitter did discovery + pre-seeded the local
    /// ledger before sending PromotePrimary). `required_setup` is
    /// false (the new field defaults to false on missing-wire-field
    /// shapes too via `#[serde(default)]`, so this is byte-
    /// compatible with pre-Step-10 senders). The handler hydrates
    /// the pool from cluster_state at promotion time; no discovery,
    /// no broadcasts.
    #[tokio::test(flavor = "current_thread")]
    async fn test_pre_seeded_promote_does_not_run_discovery() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 0);

                // Pre-seed cluster_state as if the live submitter's
                // `seed_cluster_state` broadcast had landed: 1 task +
                // empty phase_deps. Mirrors
                // `promotion_hydrates_primary_tasks_from_cluster_state`
                // in election.rs but here we drive the DISPATCH path
                // (PromotePrimary wire arrival), not the
                // record_promotion_confirm path.
                sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                    deps: HashMap::new(),
                });
                sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
                    hash: "hash_bin1".into(),
                    task: make_binary("bin1", "default"),
                });
                assert_eq!(sec.cluster_state.task_count(), 1);
                let broadcasts_before = peer_log.borrow().len();

                let promote = DistributedMessage::PromotePrimary {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    new_primary_id: "sec-a".into(),
                    epoch: 1,
                    required_setup: false,
                };
                sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
                    .await
                    .expect("PromotePrimary handler succeeds");

                assert!(sec.is_primary);
                assert!(
                    !sec.setup_pending,
                    "required_setup=false must NOT latch setup_pending"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    1,
                    "pool hydrates from the pre-seeded ledger at promotion time"
                );
                let broadcasts_after = peer_log.borrow().len();
                let new_task_added = count_task_added_mutations(&peer_log);
                assert_eq!(
                    new_task_added, 0,
                    "pre-seeded promotion path must NOT originate new TaskAdded broadcasts"
                );
                assert_eq!(
                    broadcasts_before, broadcasts_after,
                    "no peer-side traffic from a pre-seeded promotion"
                );
            })
            .await;
    }

    /// Reason 3: failover after primary loss. The election state
    /// machine flips us to Promoted via `record_promotion_confirm`
    /// (the existing scenario covered by election.rs's
    /// `promotion_hydrates_primary_tasks_from_cluster_state`). Pin
    /// the setup-pending discriminator: same shape as the legacy
    /// bootstrap test — pool hydrates from cluster_state, no
    /// discovery, no new broadcasts.
    #[tokio::test(flavor = "current_thread")]
    async fn test_failover_election_does_not_run_discovery() {
        use super::super::election::ElectionState;
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 1);
                // One peer present so the election quorum math
                // (peer_count=1 → quorum=2) works.
                sec.peer_keepalives.insert("sec-b".into(), 0.0);

                // Pre-seed cluster_state as if the live primary's
                // pre-failure broadcasts had landed via CRDT
                // replication: 1 task + empty phase_deps.
                sec.cluster_state.apply(ClusterMutation::<TestId>::PhaseDepsSet {
                    deps: HashMap::new(),
                });
                sec.cluster_state.apply(ClusterMutation::<TestId>::TaskAdded {
                    hash: "hash_bin1".into(),
                    task: make_binary("bin1", "default"),
                });
                let broadcasts_before = peer_log.borrow().len();

                // Drive the candidate-to-promoted path: pretend we're
                // already Candidate (e.g. our self-promotion vote went
                // out a tick earlier), then a peer confirms. Quorum =
                // 2; with ourselves + sec-b that promotes us.
                sec.election = ElectionState::Candidate {
                    round: 1,
                    confirms: std::collections::HashSet::from(["sec-a".to_string()]),
                    started: std::time::Instant::now(),
                };
                let promoted =
                    sec.record_promotion_confirm("sec-b".into(), "sec-a".into(), 1);
                assert!(promoted, "self + one peer confirm = quorum");

                assert!(sec.is_primary);
                assert!(matches!(sec.election, ElectionState::Promoted));
                assert!(
                    !sec.setup_pending,
                    "failover election must NOT latch setup_pending — the wire \
                     flag is the only discriminator and election bypasses it \
                     entirely (record_promotion_confirm goes straight to \
                     populate_primary_from_cluster_state)"
                );
                assert_eq!(
                    sec.primary_pending_len(),
                    1,
                    "failover hydrates the pool from the CRDT-replicated ledger"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    0,
                    "failover election originates no new TaskAdded broadcasts"
                );
                assert_eq!(
                    peer_log.borrow().len(),
                    broadcasts_before,
                    "election machinery does not piggyback peer broadcasts on \
                     record_promotion_confirm (PromotionVote went out one tick \
                     earlier in the Suspecting transition, which this test \
                     bypassed by jumping straight to Candidate)"
                );
            })
            .await;
    }

    /// Reason-3-edge-case the wire flag EXISTS to disambiguate:
    /// failover-at-startup against an empty ledger.
    ///
    /// A late-arriving secondary boots; the live primary just elected
    /// some peer as new primary and that peer's `PromotePrimary`
    /// broadcast lands on us before any `ClusterMutation` snapshot
    /// has replicated. Cluster_state is empty for the same reason as
    /// the setup-promote path — but this is NOT setup-promote, it's
    /// failover. The discriminator is the wire flag (`required_setup:
    /// false`), NOT "ledger empty". If the handler used the empty-
    /// ledger heuristic instead, it would wrongly latch setup_pending
    /// and the wrapper would call `discover_items` from cold-start —
    /// duplicating work AND racing the actual snapshot fetch.
    ///
    /// This test pins that `required_setup: false` with an empty
    /// ledger leaves setup_pending false. The pool stays unbuilt at
    /// this exact tick (nothing to hydrate from) but that's the
    /// correct behaviour: the subsequent snapshot RPC + CRDT replay
    /// will populate the ledger, and the next
    /// `populate_primary_from_cluster_state` call (e.g. on the next
    /// drain check) will hydrate the pool.
    #[tokio::test(flavor = "current_thread")]
    async fn test_failover_at_startup_does_not_redo_discovery() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("sec-a", 0);

                // Pre-condition: empty cluster_state. Same as the
                // setup-promote test's pre-condition; the wire flag is
                // what differs.
                assert_eq!(sec.cluster_state.task_count(), 0);
                assert!(!sec.setup_pending);
                let broadcasts_before = peer_log.borrow().len();

                // Sender_id deliberately names a peer (not "primary")
                // to make the failover origin explicit in the test
                // shape. The handler doesn't read sender_id for the
                // setup_pending decision — only `required_setup` —
                // but a future maintainer reading this test sees
                // "peer-originated PromotePrimary against empty
                // ledger" and the assertion makes sense.
                let promote = DistributedMessage::PromotePrimary {
                    sender_id: "sec-b".into(),
                    timestamp: 0.0,
                    new_primary_id: "sec-a".into(),
                    epoch: 1,
                    required_setup: false,
                };
                sec.dispatch_message(promote, &mut None, &mut FakeWorkerFactory)
                    .await
                    .expect("PromotePrimary handler succeeds");

                assert!(sec.is_primary);
                assert!(
                    !sec.setup_pending,
                    "EMPTY-ledger PromotePrimary with required_setup=false must \
                     NOT latch setup_pending — the wire flag, not ledger \
                     emptiness, is the discriminator"
                );
                assert_eq!(
                    count_task_added_mutations(&peer_log),
                    0,
                    "failover-at-startup originates no TaskAdded broadcasts (no \
                     local discovery)"
                );
                assert_eq!(
                    peer_log.borrow().len(),
                    broadcasts_before,
                    "no peer-side traffic emitted by the failover-at-startup \
                     promotion handler"
                );
                // Sanity: a brief delay then re-check that setup_pending
                // hasn't been flipped by some deferred path. The
                // existing dispatch contract is synchronous, so this
                // is belt-and-suspenders against a future change that
                // moved the latch into a deferred task.
                tokio::time::sleep(Duration::from_millis(10)).await;
                assert!(
                    !sec.setup_pending,
                    "setup_pending stays false across a brief wait — no \
                     deferred flip"
                );
            })
            .await;
    }
