use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dynrunner_core::{ErrorType, TaskInfo};
use dynrunner_protocol_primary_secondary::ClusterMutation;
use dynrunner_scheduler::ResourceStealingScheduler;

use crate::fulfillability_matcher::{FulfillabilityMatcher, MatcherBatch, MatcherTriggerEvent};
use crate::primary::test_helpers::{FixedEstimator, TestId, make_binary, setup_test};
use crate::primary::wire::compute_task_hash;
use crate::primary::{PrimaryCommand, PrimaryConfig, PrimaryCoordinator};
use dynrunner_transport_channel::ChannelPeerTransport;

/// Capturing matcher: records every `(hash, reason)` pair it is
/// asked about so the tests can assert which tasks the pipeline
/// surfaced. `accept_set` controls which hashes return `true`;
/// every other call returns `false`. Per-call counter exposes the
/// total invocation count for the burst-coalescing test.
struct CapturingMatcher {
    captured: Arc<Mutex<Vec<(String, String)>>>,
    accept_set: HashSet<String>,
    calls: Arc<AtomicUsize>,
}
impl FulfillabilityMatcher<TestId> for CapturingMatcher {
    fn should_reinject(
        &self,
        hash: &str,
        _task: &TaskInfo<TestId>,
        reason: &str,
        _holdings: &HashMap<String, HashSet<String>>,
    ) -> bool {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.captured
            .lock()
            .unwrap()
            .push((hash.to_string(), reason.to_string()));
        self.accept_set.contains(hash)
    }
}

/// Matcher that panics on the panicking-task's hash and behaves
/// normally otherwise. Pairs with the exception-isolation test.
struct PanickyOnHashMatcher {
    panic_for: String,
    accept_set: HashSet<String>,
    captured: Arc<Mutex<Vec<String>>>,
}
impl FulfillabilityMatcher<TestId> for PanickyOnHashMatcher {
    fn should_reinject(
        &self,
        hash: &str,
        _task: &TaskInfo<TestId>,
        _reason: &str,
        _holdings: &HashMap<String, HashSet<String>>,
    ) -> bool {
        self.captured.lock().unwrap().push(hash.to_string());
        if hash == self.panic_for {
            panic!("intentional matcher panic for hash {hash}");
        }
        self.accept_set.contains(hash)
    }
}

/// Same shape as command_channel::tests::make_coordinator — built
/// in this file to avoid leaking the helper across modules.
fn make_coordinator() -> PrimaryCoordinator<
    ChannelPeerTransport<TestId>,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let (transport, _secondary_ends) = setup_test(0);
    let config = PrimaryConfig {
        num_secondaries: 0,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_millis(100),
        uses_file_based_items: false,
        retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        ..PrimaryConfig::default()
    };
    PrimaryCoordinator::new(
        config,
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Seed `cluster_state` with one task per `(name, state)` entry.
/// Each entry lands in the requested terminal/non-terminal state
/// via the apply path so the dispatcher sees the same shape the
/// production CRDT does.
fn seed_tasks(
    coordinator: &mut PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    entries: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut hashes = HashMap::new();
    for (name, state) in entries {
        let binary = make_binary(name, 100);
        let hash = compute_task_hash(&binary);
        coordinator.cluster_state.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: binary.clone(),
        });
        match *state {
            "Pending" => {}
            "Unfulfillable" => {
                coordinator
                    .cluster_state
                    .apply(ClusterMutation::TaskFailed {
                        hash: hash.clone(),
                        kind: ErrorType::Unfulfillable {
                            reason: format!("missing-resource-for-{name}").into(),
                        },
                        error: format!("unfulfillable {name}"),

                        version: Default::default(),
                    });
            }
            "Failed_NonRecoverable" => {
                coordinator
                    .cluster_state
                    .apply(ClusterMutation::TaskFailed {
                        hash: hash.clone(),
                        kind: ErrorType::NonRecoverable,
                        error: format!("nonrecoverable {name}"),

                        version: Default::default(),
                    });
            }
            "Completed" => {
                coordinator
                    .cluster_state
                    .apply(ClusterMutation::TaskCompleted {
                        hash: hash.clone(),
                        result_data: None,
                    });
            }
            other => panic!("unsupported seed state: {other}"),
        }
        hashes.insert((*name).to_string(), hash);
    }
    hashes
}

/// Pool init helper so `apply_reinject_task` has the phase pre-
/// registered when an auto-fire lands.
fn init_pool(
    coordinator: &mut PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
) {
    let mut phase_set = std::collections::HashSet::new();
    phase_set.insert(dynrunner_core::PhaseId::from("default"));
    coordinator.pending = Some(
        dynrunner_scheduler_api::PendingPool::new(phase_set, HashMap::new()).expect("pool init"),
    );
}

/// Drain the command channel until empty, returning the
/// `(hash, _reply)` pairs of every `ReinjectTask` command seen.
fn drain_reinject_commands(
    coordinator: &mut PrimaryCoordinator<
        ChannelPeerTransport<TestId>,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
) -> Vec<String> {
    let mut hashes = Vec::new();
    let rx = coordinator.command_rx.as_mut().expect("command_rx present");
    while let Ok(cmd) = rx.try_recv() {
        if let PrimaryCommand::ReinjectTask { hash, .. } = cmd {
            hashes.push(hash);
        }
    }
    hashes
}

/// State-filter regression: the matcher fires ONLY for
/// `TaskState::Unfulfillable { .. }` entries. Running / Pending /
/// Failed{NonRecoverable} / Completed do NOT trigger the matcher
/// at all.
#[tokio::test(flavor = "current_thread")]
async fn matcher_fires_only_on_unfulfillable_tasks() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            init_pool(&mut coordinator);
            let hashes = seed_tasks(
                &mut coordinator,
                &[
                    ("pending", "Pending"),
                    ("unfulfillable", "Unfulfillable"),
                    ("failed", "Failed_NonRecoverable"),
                    ("completed", "Completed"),
                ],
            );
            let captured = Arc::new(Mutex::new(Vec::new()));
            let matcher = CapturingMatcher {
                captured: captured.clone(),
                accept_set: HashSet::new(),
                calls: Arc::new(AtomicUsize::new(0)),
            };
            coordinator.set_fulfillability_matcher(Box::new(matcher));

            coordinator
                .invoke_fulfillability_matcher_batch(MatcherBatch {
                    holdings: HashMap::new(),
                })
                .await;

            let seen = captured.lock().unwrap().clone();
            assert_eq!(
                seen.len(),
                1,
                "matcher must be invoked exactly once (the single Unfulfillable task), got {seen:?}"
            );
            assert_eq!(seen[0].0, hashes["unfulfillable"]);
        })
        .await;
}

/// End-to-end: matcher returns true → auto-fire enqueues
/// `ReinjectTask` for that hash on the coordinator's own command
/// channel. The downstream `apply_reinject_task` handler is the
/// single chokepoint that flips the state; this test pins the
/// FIRE side of the boundary.
#[tokio::test(flavor = "current_thread")]
async fn matcher_true_fires_reinject_task_command() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            init_pool(&mut coordinator);
            let hashes = seed_tasks(
                &mut coordinator,
                &[("a", "Unfulfillable"), ("b", "Unfulfillable")],
            );
            let mut accept_set = HashSet::new();
            accept_set.insert(hashes["a"].clone());
            let captured = Arc::new(Mutex::new(Vec::new()));
            let matcher = CapturingMatcher {
                captured: captured.clone(),
                accept_set,
                calls: Arc::new(AtomicUsize::new(0)),
            };
            coordinator.set_fulfillability_matcher(Box::new(matcher));

            coordinator
                .invoke_fulfillability_matcher_batch(MatcherBatch {
                    holdings: HashMap::new(),
                })
                .await;

            let fired = drain_reinject_commands(&mut coordinator);
            assert_eq!(
                fired,
                vec![hashes["a"].clone()],
                "only the accepted hash should have been auto-fired"
            );
        })
        .await;
}

/// Per-task panic isolation: a matcher that panics on one task
/// must NOT take down the loop; the other Unfulfillable tasks in
/// the same batch still get checked, and `false`-returns produce
/// no `ReinjectTask` fire.
#[tokio::test(flavor = "current_thread")]
async fn matcher_exception_swallowed_and_other_tasks_continue() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            init_pool(&mut coordinator);
            let hashes = seed_tasks(
                &mut coordinator,
                &[
                    ("panic", "Unfulfillable"),
                    ("ok-accept", "Unfulfillable"),
                    ("ok-reject", "Unfulfillable"),
                ],
            );
            let mut accept_set = HashSet::new();
            accept_set.insert(hashes["ok-accept"].clone());
            let captured = Arc::new(Mutex::new(Vec::new()));
            let matcher = PanickyOnHashMatcher {
                panic_for: hashes["panic"].clone(),
                accept_set,
                captured: captured.clone(),
            };
            coordinator.set_fulfillability_matcher(Box::new(matcher));

            coordinator
                .invoke_fulfillability_matcher_batch(MatcherBatch {
                    holdings: HashMap::new(),
                })
                .await;

            let seen = captured.lock().unwrap().clone();
            // All three tasks were asked (the panic happens
            // inside the matcher body — the recording precedes
            // the panic; the catch_unwind isolates the panic and
            // the next task still runs).
            assert_eq!(
                seen.len(),
                3,
                "all three Unfulfillable tasks should have been asked, got {seen:?}"
            );
            let fired = drain_reinject_commands(&mut coordinator);
            assert_eq!(
                fired,
                vec![hashes["ok-accept"].clone()],
                "only the non-panicking-and-accepted hash should have fired"
            );
        })
        .await;
}

/// Burst-coalescing regression: 50 trigger events arrive within
/// the idle window; the pipeline collapses them into one batch
/// and the matcher fires exactly once per Unfulfillable task.
///
/// Pins the contract that the matcher invocation rate is bounded
/// by the number of Unfulfillable tasks, not by the holdings-
/// update event volume.
#[tokio::test(flavor = "current_thread")]
async fn matcher_batched_per_holdings_update_burst() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Exercise the drain helper standalone: 50 sends in
            // quick succession collapse into one batch.
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<MatcherTriggerEvent>();
            for i in 0..50u32 {
                let mut h = HashMap::new();
                h.insert(
                    format!("peer-{}", i % 3),
                    HashSet::from([format!("outpath-{i}")]),
                );
                tx.send(MatcherTriggerEvent { holdings: h }).unwrap();
            }
            drop(tx);

            let batch = crate::fulfillability_matcher::drain_matcher_batch(
                &mut rx,
                Duration::from_millis(50),
            )
            .await
            .expect("burst should yield a batch");
            // Snapshot is the LATEST event in the burst.
            assert!(
                batch.holdings.contains_key("peer-1")
                    || batch.holdings.contains_key("peer-2")
                    || batch.holdings.contains_key("peer-0")
            );

            // Now invoke the matcher walk with this batch and
            // confirm it fires exactly once per Unfulfillable
            // task (here: 2 tasks → 2 calls, regardless of the
            // 50 input events).
            let mut coordinator = make_coordinator();
            init_pool(&mut coordinator);
            let _hashes = seed_tasks(
                &mut coordinator,
                &[("u-a", "Unfulfillable"), ("u-b", "Unfulfillable")],
            );
            let calls = Arc::new(AtomicUsize::new(0));
            let matcher = CapturingMatcher {
                captured: Arc::new(Mutex::new(Vec::new())),
                accept_set: HashSet::new(),
                calls: calls.clone(),
            };
            coordinator.set_fulfillability_matcher(Box::new(matcher));
            coordinator.invoke_fulfillability_matcher_batch(batch).await;
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "matcher should fire once per Unfulfillable task in the batch, \
                 not once per input event"
            );
        })
        .await;
}
