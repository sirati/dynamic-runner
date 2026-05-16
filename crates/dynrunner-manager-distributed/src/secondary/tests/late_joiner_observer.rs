#![cfg(test)]

    use super::super::test_helpers::{election_config, FixedEstimator, NoPeers, TestId};
    use super::super::*;
    use dynrunner_core::TaskInfo;
    use dynrunner_protocol_primary_secondary::DistributedMessage;
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;
    use tokio::sync::mpsc as tokio_mpsc;

    /// Construct a 3-node-mesh-analogous joiner: a single
    /// `SecondaryCoordinator` configured as observer
    /// (`is_observer=true`, `num_workers=0`). The "rest of the cluster"
    /// shows up purely as the snapshot the test hands it. The
    /// `NoPeers` peer transport (peer_count=0) is what
    /// `make_secondary` uses elsewhere; the late-joiner code path
    /// the test cares about (restore + skip-setup) runs to
    /// completion regardless of peer reachability — peer membership
    /// is asserted on the role-table side, not the transport side.
    fn make_observer_secondary(
        observer_id: &str,
    ) -> SecondaryCoordinator<
        ChannelPrimaryTransportEnd<TestId>,
        NoPeers,
        dynrunner_transport_channel::ChannelManagerEnd,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    > {
        let (sec_to_pri_tx, _sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
        let (_pri_to_sec_tx, pri_to_sec_rx) =
            tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
        let transport = ChannelPrimaryTransportEnd {
            tx: sec_to_pri_tx,
            rx: pri_to_sec_rx,
        };
        let mut config = election_config(observer_id);
        config.is_observer = true;
        config.num_workers = 0;
        SecondaryCoordinator::new(
            config,
            transport,
            NoPeers,
            ResourceStealingScheduler::memory(),
            FixedEstimator(100),
        )
    }

    /// Build a synthetic `ClusterStateSnapshot<TestId>` carrying two
    /// pending tasks, a designated `current_primary`, primary_epoch=7,
    /// and one observer id. The same shape the wire frame's
    /// `snapshot_json` decodes to.
    fn make_synthetic_snapshot()
    -> crate::cluster_state::ClusterStateSnapshot<TestId> {
        use crate::cluster_state::TaskState;
        let mut tasks = HashMap::new();
        let mk_pending = |path: &str, ident: &str| TaskState::Pending {
            task: TaskInfo {
                path: PathBuf::from(path),
                size: 100,
                identifier: TestId(ident.into()),
                phase_id: dynrunner_core::PhaseId::from("default"),
                type_id: dynrunner_core::TypeId::from("default"),
                affinity_id: None,
                payload: serde_json::Value::Null,
                task_id: None,
                task_depends_on: vec![],
                preferred_secondaries: dynrunner_core::SoftPreferredSecondaries::default(),
                resolved_path: None,
            },
        };
        tasks.insert(
            "task-1".to_string(),
            mk_pending("/tmp/task-1", "task-1"),
        );
        tasks.insert(
            "task-2".to_string(),
            mk_pending("/tmp/task-2", "task-2"),
        );
        let mut observers = HashSet::new();
        observers.insert("observer-peer".to_string());
        crate::cluster_state::ClusterStateSnapshot {
            tasks,
            current_primary: Some("primary-peer".to_string()),
            primary_epoch: 7,
            phase_deps: HashMap::new(),
            observers,
            peer_holdings: HashMap::new(),
        }
    }

    /// `restore_from_snapshot_and_skip_setup` is the load-bearing
    /// API: a single call must (a) install the snapshot's task
    /// ledger, observers, and current_primary into the coordinator's
    /// `cluster_state`, AND (b) latch `setup_phase_completed=true`
    /// so the next `run_until_setup_or_done` call skips the
    /// welcome / cert-exchange / wait-for-setup phases.
    #[test]
    fn restore_installs_snapshot_and_latches_setup_completed() {
        let mut sec = make_observer_secondary("observer-1");

        // Pre-condition: every field this test asserts is at its
        // freshly-constructed default. Pinning the pre-conditions
        // catches "the field was already true / non-empty before
        // restore" regressions that would otherwise silently make
        // the post-condition asserts pass for the wrong reason.
        assert!(!sec.setup_phase_completed);
        assert_eq!(sec.cluster_state.task_count(), 0);
        assert!(sec.cluster_state.current_primary().is_none());
        assert!(sec.cluster_state.role_table().observers.is_empty());

        sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

        // Latch is set — `run_until_setup_or_done` will skip the
        // entire `!setup_phase_completed` setup block on its next
        // call.
        assert!(sec.setup_phase_completed);

        // Task ledger merged in: two pending tasks survive.
        assert_eq!(sec.cluster_state.task_count(), 2);

        // current_primary and primary_epoch reflect the snapshot's
        // authority — the joiner's role cache (read via the
        // PeerTransport hook registered in `new()`) now knows
        // who's primary, so Address::Role(Role::Primary) dispatches
        // resolve immediately rather than failing with
        // "role-table cache empty".
        assert_eq!(
            sec.cluster_state.current_primary(),
            Some("primary-peer"),
        );
        assert_eq!(sec.cluster_state.primary_epoch(), 7);

        // Observer set merged — Step 7's election filter will skip
        // `observer-peer` from `lowest_alive` candidate selection
        // even before the next live PeerInfo broadcast lands.
        let observers = &sec.cluster_state.role_table().observers;
        assert!(observers.contains("observer-peer"));
        assert_eq!(observers.len(), 1);
    }

    /// The same `restore` call applied twice is a no-op the second
    /// time — `ClusterState::restore` is documented as idempotent /
    /// CRDT-merge. Pins that the wrapper preserves the underlying
    /// idempotency (i.e. the wrapper doesn't toggle the latch back
    /// or re-broadcast).
    #[test]
    fn restore_is_idempotent_on_second_call() {
        let mut sec = make_observer_secondary("observer-1");
        sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());
        let tasks_after_first = sec.cluster_state.task_count();
        let epoch_after_first = sec.cluster_state.primary_epoch();

        // Second call with the SAME snapshot — the merge rules
        // (`primary_epoch > self.primary_epoch` gate, observer-set
        // "only when local empty" gate) make this a no-op.
        sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

        assert!(sec.setup_phase_completed, "latch stays true");
        assert_eq!(sec.cluster_state.task_count(), tasks_after_first);
        assert_eq!(sec.cluster_state.primary_epoch(), epoch_after_first);
    }

    /// Observer config sanity: an observer's `is_observer=true` flag
    /// reaches the coordinator's `config` so downstream consumers
    /// (the election filter at `election.rs::run_election_tick`'s
    /// `we_lead` branch, the dispatch defensive reject at
    /// `dispatch.rs::handle_promote_primary`) see the flag.
    #[test]
    fn observer_config_propagated_to_coordinator() {
        let sec = make_observer_secondary("observer-1");
        assert!(
            sec.config.is_observer,
            "observer flag must be readable on the coordinator's config — \
             election + dispatch defensive paths both consult it"
        );
        assert_eq!(
            sec.config.num_workers, 0,
            "observer's num_workers must be 0 (no work to take on)"
        );
    }

    /// After restore, the coordinator's `run_until_setup_or_done`
    /// MUST NOT touch `primary_transport` (no welcome /
    /// cert-exchange / wait-for-setup), and MUST NOT spawn workers
    /// via the factory. The cleanest pin is: drive the run loop
    /// briefly under a short tokio::time::timeout — the call must
    /// return `Ok(Done)` only when the cluster reports RunComplete;
    /// for this test we just need to assert it ENTERS the
    /// processing branch (the welcome handshake would have errored
    /// out on the disconnected primary channel within milliseconds).
    ///
    /// We assert the easier shape: with `setup_phase_completed=true`
    /// pre-set, a `run_until_setup_or_done` future advances past
    /// the setup block. The `FakeWorkerFactory` is wired in case
    /// some future code path under processing_loop pulls on it; with
    /// num_workers=0 nothing pulls on the factory today, but the
    /// wiring keeps the test resilient.
    #[tokio::test(flavor = "current_thread")]
    async fn run_after_restore_skips_setup_handshake() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut sec = make_observer_secondary("observer-1");
                sec.restore_from_snapshot_and_skip_setup(make_synthetic_snapshot());

                let mut factory = super::super::test_helpers::FakeWorkerFactory;
                // The run loop polls peer/primary/timers; without
                // any RunComplete broadcast it would block forever
                // in `process_tasks`. Wrap in a short timeout: a
                // timeout (not an Err) is the success signal here —
                // it means we entered `process_tasks` (the setup
                // block would have errored on the disconnected
                // primary channel and returned an Err well within
                // 100ms). If `Err` comes back, the setup-skip
                // latch failed and the welcome attempted to send /
                // recv on the dead primary transport.
                let outcome = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    sec.run_until_setup_or_done(&mut factory),
                )
                .await;
                match outcome {
                    Err(_elapsed) => {
                        // Expected: we're in the processing loop,
                        // waiting on peer / primary / timer events.
                    }
                    Ok(Err(e)) => panic!(
                        "run_until_setup_or_done returned Err — setup-skip \
                         latch did not take effect (welcome attempted on \
                         a dead primary): {e}"
                    ),
                    Ok(Ok(RunOutcome::Done)) => {
                        // Acceptable but unusual: processing-loop's
                        // run-complete exit may fire if the snapshot
                        // carried RunComplete mutation, which it
                        // doesn't in `make_synthetic_snapshot`. If
                        // a future fixture toggles it, this branch
                        // still asserts the right behaviour.
                    }
                    Ok(Ok(RunOutcome::SetupPending)) => panic!(
                        "observer must never see SetupPending — only \
                         PromotePrimary{{required_setup=true}} causes it, \
                         which an observer rejects"
                    ),
                }
            })
            .await;
    }
