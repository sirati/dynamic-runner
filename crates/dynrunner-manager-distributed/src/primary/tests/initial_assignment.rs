//! Tests grouped by feature. Shared imports + helpers live in
//! [`super`] (`tests/mod.rs`); each sub-file re-exports via `use super::*`.

use super::*;

fn make_remote_worker(worker_id: u32, secondary_id: &str, busy: bool) -> RemoteWorkerState<TestId> {
    let state = if busy {
        let task = make_binary("placeholder", 0);
        let task_hash = crate::primary::wire::compute_task_hash(&task);
        crate::primary::SlotState::Assigned {
            task_hash,
            task,
            estimated: dynrunner_core::ResourceMap::new(),
        }
    } else {
        crate::primary::SlotState::Idle
    };
    RemoteWorkerState {
        worker_id,
        secondary_id: secondary_id.into(),
        resource_budgets: dynrunner_core::ResourceMap::new(),
        state,
    }
}

#[test]
fn dispatch_order_equal_load_preserves_worker_id_order() {
    let workers = vec![
        make_remote_worker(0, "A", false),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", false),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![0, 1, 2, 3]);
}

#[test]
fn dispatch_order_prefers_less_loaded_secondary() {
    // A has 2 busy + 2 idle (load 2). B has 0 busy + 2 idle (load 0).
    // B's idle workers must come before A's even though A's worker_ids
    // are lower — the pre-fix iteration order would have given A first
    // dibs on tail-of-phase items.
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", true),
        make_remote_worker(2, "A", false),
        make_remote_worker(3, "A", false),
        make_remote_worker(4, "B", false),
        make_remote_worker(5, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![4, 5, 2, 3]);
}

#[test]
fn dispatch_order_excludes_busy_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "A", false),
        make_remote_worker(2, "B", true),
        make_remote_worker(3, "B", false),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert_eq!(order, vec![1, 3]);
}

#[test]
fn dispatch_order_empty_workers() {
    let workers: Vec<RemoteWorkerState<TestId>> = vec![];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

#[test]
fn dispatch_order_no_idle_workers() {
    let workers = vec![
        make_remote_worker(0, "A", true),
        make_remote_worker(1, "B", true),
    ];
    let order = super::lifecycle::dispatch_order(&workers);
    assert!(order.is_empty());
}

/// T-#33: initial assignment is round-robin across secondaries AND
/// secondary iteration order is deterministic (sorted by name).
///
/// Setup: 3 secondaries × 1 worker × 3 binaries. With contiguous-
/// per-secondary order (pre-fix) the assignment was still
/// one-per-secondary in this exact-fit case, but the SECONDARY-ID
/// ORDER of which-secondary-got-which-binary was HashMap-random.
/// Post-fix the binaries land in sec-0, sec-1, sec-2 order.
///
/// More important regression case: tasks ≪ total_workers. With
/// pre-fix (contiguous), 3 secondaries × 2 workers × 3 tasks would
/// have given the first secondary 2 tasks and one other secondary
/// 1 task — the third got nothing. Post-fix all three each receive
/// exactly 1. We exercise that exact case here to pin the actual
/// behaviour change, not just the determinism gain.
#[tokio::test(flavor = "current_thread")]
async fn initial_assignment_is_round_robin_and_name_sorted() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, secondary_ends) = setup_test(3);

            let config = PrimaryConfig {
                num_secondaries: 3,
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

            // 3 tasks, 3 secondaries × 2 workers = 6 worker slots.
            // The pre-fix contiguous-per-secondary order would have
            // given two secondaries all 3 tasks and one secondary 0.
            // Post-fix every secondary gets exactly 1.
            let binaries = vec![
                make_binary("a", 50),
                make_binary("b", 50),
                make_binary("c", 50),
            ];

            // Per-secondary initial-assignment count, captured by
            // intercepting each secondary's primary→secondary channel.
            // Forwarder counts InitialAssignment binaries before
            // re-forwarding every message to the real fake-secondary,
            // so the lifecycle still completes via TaskComplete +
            // TaskRequest cycles.
            let counts: Arc<Mutex<std::collections::BTreeMap<String, usize>>> =
                Arc::new(Mutex::new(std::collections::BTreeMap::new()));

            for (id, sec_inbound, sec_outbound) in secondary_ends {
                let (inner_tx, inner_rx) = tokio_mpsc::unbounded_channel();
                let counts_for_secondary = Arc::clone(&counts);
                let id_for_forwarder = id.clone();
                tokio::task::spawn_local(async move {
                    let mut rx = sec_inbound;
                    while let Some(msg) = rx.recv().await {
                        if let DistributedMessage::InitialAssignment {
                            target: _,
                            zip_files,
                            ..
                        } = &msg
                        {
                            let n: usize = zip_files.iter().map(|zf| zf.binaries.len()).sum();
                            counts_for_secondary
                                .lock()
                                .unwrap()
                                .insert(id_for_forwarder.clone(), n);
                        }
                        if inner_tx.send(msg).is_err() {
                            break;
                        }
                    }
                });

                tokio::task::spawn_local(fake_secondary(
                    id,
                    2,
                    1024 * 1024 * 1024,
                    inner_rx,
                    sec_outbound,
                ));
            }

            let (deps, ops, ope) = noop_phase_args();
            primary.run(binaries, deps, ops, ope).await.unwrap();

            assert_eq!(primary.completed_count(), 3);
            assert_eq!(primary.failed_count(), 0);

            // Each of the 3 secondaries must have received exactly 1
            // binary in its InitialAssignment. Pre-fix the
            // contiguous-per-secondary layout produced something like
            // {sec-X: 2, sec-Y: 1, sec-Z: 0} where X/Y/Z were
            // HashMap-random; the secondary that got 0 then had to
            // wait for the operational TaskRequest cycle to receive
            // any work at all.
            let final_counts = counts.lock().unwrap().clone();
            assert_eq!(
                final_counts.len(),
                3,
                "every secondary must receive an InitialAssignment \
                 (even an empty one) so wait_for_setup unblocks; \
                 captured: {:?}",
                final_counts
            );
            for sid in &["sec-0", "sec-1", "sec-2"] {
                let n = final_counts
                    .get(*sid)
                    .copied()
                    .expect("expected secondary missing from captured InitialAssignment");
                assert_eq!(
                    n, 1,
                    "{sid} expected exactly 1 initial-assignment binary, \
                     got {n}. Pre-fix this would fail because contiguous-\
                     per-secondary ordering plus HashMap-random iteration \
                     order gave 2 tasks to one secondary and 0 to another. \
                     Captured: {:?}",
                    final_counts
                );
            }
        })
        .await;
}
