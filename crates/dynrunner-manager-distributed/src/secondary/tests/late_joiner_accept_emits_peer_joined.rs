#![cfg(test)]

    use super::super::test_helpers::{election_config, FixedEstimator, RecordingPeer, TestId};
    use super::super::*;
    use dynrunner_protocol_primary_secondary::{ClusterMutation, DistributedMessage};
    use dynrunner_scheduler::ResourceStealingScheduler;
    use dynrunner_transport_channel::ChannelPrimaryTransportEnd;
    use tokio::sync::mpsc as tokio_mpsc;

    /// Mirror of the `setup_promote_discriminator` fixture: build a
    /// SecondaryCoordinator backed by a `RecordingPeer` so the test can
    /// inspect what got broadcast over the peer mesh, plus a hand-held
    /// primary-side receiver so the test can confirm the unicast
    /// snapshot reply does NOT mis-fire on `primary_transport`.
    // SecondaryCoordinator's six type parameters make this triple
    // "complex" by clippy's measure; a one-off test helper.
    #[allow(clippy::type_complexity)]
    fn make_secondary_with_recording_peer(
        secondary_id: &str,
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
        let recorder = RecordingPeer::<TestId>::new(1);
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

    /// Find the first `ClusterMutation::PeerJoined` mutation in the
    /// recorded peer-bus traffic. The `RecordingPeer` collates both
    /// `broadcast` and `send_to_peer` envelopes into one log; the
    /// snapshot reply uses `send_to_peer` and the originator's
    /// `apply_and_broadcast_mutations` uses `broadcast` — both land in
    /// the same vector, so the filter walks any-arm-any-frame.
    fn find_peer_joined(
        log: &std::rc::Rc<std::cell::RefCell<Vec<DistributedMessage<TestId>>>>,
    ) -> Option<(String, bool)> {
        log.borrow().iter().find_map(|msg| match msg {
            DistributedMessage::ClusterMutation { mutations, .. } => {
                mutations.iter().find_map(|m| match m {
                    ClusterMutation::PeerJoined {
                        peer_id,
                        is_observer,
                    } => Some((peer_id.clone(), *is_observer)),
                    _ => None,
                })
            }
            _ => None,
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_late_joiner_accept_emits_peer_joined_observer_true() {
        let _ = tracing_subscriber::fmt::try_init();
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (mut sec, _pri_rx, peer_log) =
                    make_secondary_with_recording_peer("responder");

                // Pre-condition pins: the observer set is empty before
                // the snapshot RPC arrives. Without this the post-state
                // assert could be tautologically satisfied by an
                // unrelated population of `observers`.
                assert!(
                    sec.cluster_state.role_table().observers.is_empty(),
                    "observer set must be empty pre-accept; got {:?}",
                    sec.cluster_state.role_table().observers
                );

                // Drive the late-joiner accept path: the joiner's
                // `RequestClusterSnapshot` arriving on the responder's
                // dispatch loop.
                let req = DistributedMessage::RequestClusterSnapshot {
                    sender_id: "late-observer-1".into(),
                    timestamp: 0.0,
                };
                sec.dispatch_message(req)
                    .await
                    .expect("RequestClusterSnapshot handler succeeds");

                // (1) The originator-side apply landed locally: the
                // joiner shows up in the observer projection. This
                // exercises the widened `apply_peer_joined` rule's
                // `is_observer = true` branch via the canonical
                // `apply_and_broadcast_mutations` path.
                assert!(
                    sec.cluster_state
                        .role_table()
                        .observers
                        .contains("late-observer-1"),
                    "late-joiner must be projected into role_table.observers \
                     via the originator-side apply; observers={:?}",
                    sec.cluster_state.role_table().observers
                );

                // (2) The mutation was broadcast over the peer mesh so
                // every other cluster member converges. `find_peer_joined`
                // scans the recorder for the exact envelope shape.
                let observed = find_peer_joined(&peer_log).expect(
                    "RequestClusterSnapshot accept must originate one \
                     PeerJoined mutation on the peer bus",
                );
                assert_eq!(
                    observed,
                    ("late-observer-1".to_string(), true),
                    "late-joiner PeerJoined must carry the joiner's id \
                     and is_observer=true (current design treats every \
                     late-joiner as an observer); got {:?}",
                    observed
                );
            })
            .await;
    }
