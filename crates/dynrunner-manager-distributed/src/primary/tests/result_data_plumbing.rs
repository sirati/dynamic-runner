//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

/// Pin the plumbing contract: `result_data` bytes attached to a
/// `DistributedMessage::TaskComplete` arriving on the primary must
/// land verbatim on the broadcast `ClusterMutation::TaskCompleted`.
/// Pre-P3b the primary destructured with `..` and hardcoded
/// `result_data: None`, which silently dropped every byte that the
/// producer worker had attached.
#[tokio::test(flavor = "current_thread")]
async fn primary_handle_task_complete_forwards_result_data_to_cluster_mutation() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(1);
            let (sec_id, mut to_sec_rx, _incoming_tx) =
                secondary_ends.into_iter().next().unwrap();

            let config = crate::primary::PrimaryConfig {
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
                fleet_dead_timeout: Duration::from_secs(30),
                mesh_ready_timeout: Duration::from_secs(5),
                mass_death_grace: Duration::ZERO,
                mass_death_min_count: 2,
                source_dir: None,
                unfulfillable_reinject_max_per_task: None,
                setup_promote_deadline: Duration::from_secs(600),
            };
            let mut primary: crate::primary::PrimaryCoordinator<_, _, _, _, TestId> =
                crate::primary::PrimaryCoordinator::new(
                    config,
                    transport,
                    NoPeers,
                    ResourceStealingScheduler::memory(),
                    FixedEstimator(100),
                );

            // Seed a TaskAdded so the CRDT apply for TaskCompleted is
            // not NoOp'd by the `apply_locally_for_broadcast` filter.
            // Without this the post-handle broadcast set is empty and
            // we never get to inspect the bytes — the test would pass
            // for the wrong reason.
            let bin = make_binary("payload-task", 100);
            let hash = crate::primary::wire::compute_task_hash(&bin);
            primary
                .apply_and_broadcast_cluster_mutations(vec![ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task: bin,
                }])
                .await;
            // Drain the TaskAdded broadcast from the secondary queue so
            // the subsequent assertion only sees the TaskCompleted
            // broadcast under test.
            while let Ok(_msg) = to_sec_rx.try_recv() {}

            let payload: Vec<u8> = b"keyed-output-bytes".to_vec();
            let msg = DistributedMessage::TaskComplete {
                sender_id: sec_id.clone(),
                timestamp: 0.0,
                secondary_id: sec_id.clone(),
                worker_id: 0,
                task_hash: hash.clone(),
                result_data: Some(payload.clone()),
            };
            primary.handle_task_complete(msg, &mut None).await;

            // The broadcast lands on the per-secondary outgoing channel.
            let received = to_sec_rx
                .try_recv()
                .expect("primary must have broadcast a ClusterMutation after handle_task_complete");
            match received {
                DistributedMessage::ClusterMutation { mutations, .. } => {
                    let completed = mutations
                        .iter()
                        .find_map(|m| match m {
                            ClusterMutation::TaskCompleted { hash: h, result_data }
                                if h == &hash =>
                            {
                                Some(result_data.clone())
                            }
                            _ => None,
                        })
                        .expect(
                            "ClusterMutation batch must include a TaskCompleted for this hash",
                        );
                    assert_eq!(
                        completed,
                        Some(payload),
                        "result_data bytes must survive the worker->secondary->primary \
                         destructure-and-reconstruct chain into the broadcast mutation; \
                         drop here means P3b plumbing regressed at \
                         primary/task/complete.rs"
                    );
                }
                other => panic!(
                    "expected ClusterMutation broadcast, got {:?}",
                    other.msg_type()
                ),
            }
        })
        .await;
}
