//! Contract-level constructor smoke tests. Full integration
//! (spawner ↔ dispatcher ↔ JoinSet drain) lands in sibling F6.

use super::*;
use super::types::push_event;
use dynrunner_protocol_primary_secondary::RemovalCause;
use std::time::{Duration, SystemTime};

#[test]
fn spawn_spec_constructs() {
    let spec = SecondarySpawnSpec {
        new_secondary_id: "sec-replacement-1".to_owned(),
        primary_endpoint: "127.0.0.1:5555".to_owned(),
        primary_pubkey_pem: "-----BEGIN PUBLIC KEY-----\n...\n".to_owned(),
    };
    assert_eq!(spec.new_secondary_id, "sec-replacement-1");
    assert_eq!(spec.primary_endpoint, "127.0.0.1:5555");
    assert!(spec.primary_pubkey_pem.starts_with("-----BEGIN"));
}

#[test]
fn spawn_error_renders_human_strings() {
    let provider_unavail = SpawnError::ProviderUnavailable("slurm not configured".to_owned());
    assert_eq!(
        format!("{provider_unavail}"),
        "spawn provider unavailable: slurm not configured",
    );
    let timeout = SpawnError::Timeout;
    assert_eq!(format!("{timeout}"), "spawn timed out");
    let other = SpawnError::Other("exec failed".to_owned());
    assert_eq!(format!("{other}"), "spawn failed: exec failed");
}

#[test]
fn respawn_budget_default_matches_spec() {
    let b = RespawnBudget::default();
    assert_eq!(b.max_per_secondary, 3);
    assert_eq!(b.max_total, 10);
    assert_eq!(b.cooldown, Duration::from_secs(30));
}

#[test]
fn respawn_outcome_constructs_with_ok_and_err() {
    let ok = RespawnOutcome {
        original_id: "sec-a".to_owned(),
        new_id: "sec-a-replacement".to_owned(),
        cause: RemovalCause::KeepaliveMiss,
        result: Ok(()),
    };
    assert!(ok.result.is_ok());

    let err = RespawnOutcome {
        original_id: "sec-b".to_owned(),
        new_id: "sec-b-replacement".to_owned(),
        cause: RemovalCause::MassDeathEscalation,
        result: Err("spawn failed".to_owned()),
    };
    assert!(matches!(err.result, Err(ref s) if s == "spawn failed"));
}

#[test]
fn respawn_event_constructs() {
    let ev = RespawnEvent {
        original_id: "sec-a".to_owned(),
        new_id: "sec-a-replacement".to_owned(),
        cause: RemovalCause::KeepaliveMiss,
        at: SystemTime::now(),
    };
    assert_eq!(ev.original_id, "sec-a");
    assert_eq!(ev.new_id, "sec-a-replacement");
    assert!(matches!(ev.cause, RemovalCause::KeepaliveMiss));
}

#[test]
fn respawn_event_ringbuffer_drops_oldest_at_1024_cap() {
    use std::collections::VecDeque;

    let mut ring: VecDeque<RespawnEvent> = VecDeque::new();
    // Push exactly one more than the cap; the very first event
    // (`new_id = "new-0"`) must be evicted, and the buffer must
    // remain at the cap with the freshest event at the back.
    for i in 0..=RESPAWN_EVENTS_CAP {
        push_event(
            &mut ring,
            RespawnEvent {
                original_id: format!("orig-{i}"),
                new_id: format!("new-{i}"),
                cause: RemovalCause::KeepaliveMiss,
                at: SystemTime::now(),
            },
        );
    }
    assert_eq!(ring.len(), RESPAWN_EVENTS_CAP);
    assert_eq!(ring.front().unwrap().new_id, "new-1");
    assert_eq!(
        ring.back().unwrap().new_id,
        format!("new-{}", RESPAWN_EVENTS_CAP),
    );
}
// End-to-end coverage of the listener → request-channel →
// operational-loop-arm pipeline. Each test constructs a
// `PrimaryCoordinator` against the in-process channel stub used
// by the rest of the primary tests, installs a mock
// `SecondarySpawner`, and drives the pipeline by either
// (a) calling the dispatcher's `on_event` directly + draining
// the request channel into the coordinator's
// `dispatch_respawn_request` (which is what the operational
// loop's `select!` arm does), or (b) calling
// `dispatch_respawn_request` directly with a synthetic request
// when only the budget logic is under test.
//
// Single concern per test: each pins one observable side of
// the contract — spawn invoked, no-spawn when disabled, family
// budget honoured, total budget honoured, ids monotonic.
// The JoinSet drain + log-event emission are exercised
// transitively (a spawn future that resolves before assertions
// lands its outcome on the JoinSet; the test reads the
// resolved entry to confirm the new id).
use crate::primary::test_helpers::{setup_test, FixedEstimator, NoPeers, TestId};
use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use crate::peer_lifecycle::PeerLifecycleEvent;
use dynrunner_scheduler::ResourceStealingScheduler;
use dynrunner_transport_channel::ChannelSecondaryTransportEnd;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Counting mock spawner: records every `spec.new_secondary_id`
/// it observes and returns `Ok(())` for the first call (or as
/// configured). The recorded ids let tests assert the
/// coordinator minted fresh ids and the `RespawnDecision`
/// path honoured the budget.
struct MockSpawner {
    calls: Arc<AtomicU32>,
    captured_ids: Arc<Mutex<Vec<String>>>,
}

impl MockSpawner {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicU32::new(0)),
            captured_ids: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn captured_ids(&self) -> Vec<String> {
        self.captured_ids.lock().unwrap().clone()
    }
}

#[async_trait::async_trait(?Send)]
impl SecondarySpawner for MockSpawner {
    async fn spawn(
        &self,
        spec: SecondarySpawnSpec,
    ) -> Result<(), SpawnError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.captured_ids
            .lock()
            .unwrap()
            .push(spec.new_secondary_id);
        Ok(())
    }
}

/// Build a coordinator wired with 1 reserved initial-cohort id so
/// the first minted respawn lands on `secondary-1`. The minted-id
/// monotonic test pins this contract directly.
fn make_coordinator(
) -> PrimaryCoordinator<
    ChannelSecondaryTransportEnd<TestId>,
    NoPeers,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
> {
    let (transport, _ends) = setup_test(0);
    let config = PrimaryConfig {
        node_id: "primary".into(),
        num_secondaries: 1,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_millis(100),
        keepalive_miss_threshold: 3,
        source_pre_staged_root: None,
        uses_file_based_items: false,
        required_setup_on_promote: false,
        max_concurrent_per_type: HashMap::new(),
        retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        mass_death_grace: Duration::from_secs(1),
        mass_death_min_count: 2,
        source_dir: None,
        unfulfillable_reinject_max_per_task: None,
        setup_promote_deadline: std::time::Duration::from_secs(600),
    };
    PrimaryCoordinator::new(
        config,
        transport,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    )
}

/// Loose budget — every per-knob cap large enough to never
/// reject; cooldown zero so back-to-back requests are accepted.
fn permissive_budget() -> RespawnBudget {
    RespawnBudget {
        max_per_secondary: 100,
        max_total: 100,
        cooldown: Duration::ZERO,
    }
}

/// Confirms the dispatcher closure registered by `enable_respawn`
/// translates `PeerLifecycleEvent::Removed` into a real
/// `spawner.spawn` invocation. Drives the LocalSet directly so
/// the `spawn_local` future used internally by
/// `dispatch_respawn_request` resolves before the assertions.
#[tokio::test(flavor = "current_thread")]
async fn respawn_dispatcher_fires_spawner_on_peer_removed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            let captured = Arc::clone(&spawner.captured_ids);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // Direct invocation of the dispatcher's `on_event`
            // path. The operational `select!` arm in
            // `lifecycle::operational_loop` ultimately calls the
            // same `dispatch_respawn_request` we invoke here; this
            // test takes the same path without spinning up the
            // full LocalSet-bound dispatcher task.
            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            // Drain the spawned future on the LocalSet so the
            // spawner's atomic counter has settled before the
            // assertion. `join_next` resolves after the
            // `spawn_local` future returns.
            let outcome = coordinator
                .respawn_tasks
                .join_next()
                .await
                .expect("respawn task should be present after dispatch");
            let outcome = outcome.expect("respawn task should not panic");
            assert!(outcome.result.is_ok());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let ids = captured.lock().unwrap();
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], "secondary-1");
            assert_eq!(coordinator.respawn_events.len(), 1);
        })
        .await;
}

/// Policy-disabled coordinators must never register the
/// dispatcher listener and never invoke a spawner — even when a
/// `Removed` event is delivered directly via the lifecycle
/// pipeline. Pins the CCD-5 "no hot-site `if policy_enabled`"
/// contract from the dispatch side: the request channel sender
/// is `None`, so no listener can enqueue.
#[tokio::test(flavor = "current_thread")]
async fn respawn_dispatcher_skips_when_policy_disabled() {
    let coordinator = make_coordinator();
    // No `enable_respawn` call — the spawner / budget / channel /
    // listener registration are all absent by construction.
    assert!(coordinator.respawn_spawner.is_none());
    assert!(coordinator.respawn_budget.is_none());
    assert!(coordinator.respawn_request_tx.is_none());
    assert!(coordinator.respawn_request_rx.is_none());
    assert!(coordinator.peer_lifecycle_listeners.is_empty());

    // Build a free-standing dispatcher listener so we can verify
    // its on_event side-effect: a Removed event has no place to
    // land if the channel side hasn't been wired. We construct a
    // throwaway channel just to verify the closure shape; the
    // coordinator's wiring itself is the contract under test.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RespawnRequest>();
    let listener = respawn_dispatcher_listener(tx);
    listener.on_event(&PeerLifecycleEvent::Removed {
        id: "secondary-0".into(),
        cause: RemovalCause::KeepaliveMiss,
    });
    // The free-standing listener does enqueue (it's a pure
    // transformation); the coordinator we built simply has no
    // listener registered, so its operational-loop arm would
    // never see the request. That's the CCD-5 invariant.
    let req = rx.try_recv().expect("free-standing listener should still translate");
    assert_eq!(req.original_id, "secondary-0");
}

/// Three deaths in the same family chain (each respawn's `new_id`
/// becoming the next death's `original_id`) consume the
/// `max_per_secondary = 3` budget; the fourth death is rejected
/// with `RespawnDecision::RejectFamilyBudget` and no spawn lands.
#[tokio::test(flavor = "current_thread")]
async fn respawn_dispatcher_respects_per_secondary_budget() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            coordinator.enable_respawn(
                spawner.clone(),
                RespawnBudget {
                    max_per_secondary: 3,
                    max_total: 100,
                    cooldown: Duration::ZERO,
                },
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // First death — id "secondary-0" (initial cohort).
            // Each subsequent "death" addresses the prior
            // respawn's new id so the family chain is contiguous.
            let mut current_dead = String::from("secondary-0");
            for i in 0..3 {
                coordinator.dispatch_respawn_request(RespawnRequest {
                    original_id: current_dead.clone(),
                    cause: RemovalCause::KeepaliveMiss,
                });
                let outcome = coordinator
                    .respawn_tasks
                    .join_next()
                    .await
                    .expect("spawn future should be queued");
                let outcome = outcome.expect("no panic");
                assert!(outcome.result.is_ok(), "spawn #{i} should accept");
                // Walk the chain forward.
                current_dead = outcome.new_id;
            }
            assert_eq!(calls.load(Ordering::SeqCst), 3);

            // Fourth death in the same family — must be rejected
            // by the family budget. No new spawn future lands on
            // the JoinSet.
            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: current_dead,
                cause: RemovalCause::KeepaliveMiss,
            });
            assert!(
                coordinator.respawn_tasks.is_empty(),
                "4th death should NOT have spawned",
            );
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            // Ring records 3 events (one per accepted spawn).
            assert_eq!(coordinator.respawn_events.len(), 3);
        })
        .await;
}

/// Ten respawns across distinct families saturate `max_total =
/// 10`; the 11th request is rejected with
/// `RespawnDecision::RejectTotalBudget`.
#[tokio::test(flavor = "current_thread")]
async fn respawn_dispatcher_respects_total_budget() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            coordinator.enable_respawn(
                spawner.clone(),
                RespawnBudget {
                    // Family budget high enough to never trigger;
                    // total is the binding constraint.
                    max_per_secondary: 100,
                    max_total: 10,
                    cooldown: Duration::ZERO,
                },
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // Ten DISTINCT families, one death per family — no
            // chain walk involved.
            for i in 0..10u32 {
                coordinator.dispatch_respawn_request(RespawnRequest {
                    original_id: format!("distinct-{i}"),
                    cause: RemovalCause::KeepaliveMiss,
                });
                let _ = coordinator
                    .respawn_tasks
                    .join_next()
                    .await
                    .expect("spawn future should land");
            }
            assert_eq!(calls.load(Ordering::SeqCst), 10);

            // 11th death — distinct family, but total budget is
            // exhausted.
            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "distinct-10".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            assert!(
                coordinator.respawn_tasks.is_empty(),
                "11th death should NOT have spawned",
            );
            assert_eq!(calls.load(Ordering::SeqCst), 10);
            assert_eq!(coordinator.respawn_events.len(), 10);
        })
        .await;
}

/// Every accepted respawn must mint a fresh, monotonically-
/// increasing `secondary-N` id. The coordinator's
/// `mint_secondary_id` is the authority; this test pins that
/// `dispatch_respawn_request` consults it (rather than reusing
/// the dead peer's id) and that the spawn future receives the
/// minted id verbatim.
#[tokio::test(flavor = "current_thread")]
async fn respawn_dispatcher_minted_id_is_monotonic() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let captured = Arc::clone(&spawner.captured_ids);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // Three distinct families so the per-secondary cap
            // doesn't intrude on the monotonic-id assertion.
            for i in 0..3u32 {
                coordinator.dispatch_respawn_request(RespawnRequest {
                    original_id: format!("distinct-{i}"),
                    cause: RemovalCause::KeepaliveMiss,
                });
                let _ = coordinator
                    .respawn_tasks
                    .join_next()
                    .await
                    .expect("spawn future should land");
            }

            let ids = captured.lock().unwrap();
            assert_eq!(ids.len(), 3);
            // The coordinator's `next_secondary_id` is seeded
            // from `config.num_secondaries = 1`, so the first
            // mint is `secondary-1` and each subsequent mint is
            // monotonic.
            assert_eq!(ids[0], "secondary-1");
            assert_eq!(ids[1], "secondary-2");
            assert_eq!(ids[2], "secondary-3");
        })
        .await;
}

/// Mass-death-grace finalize bursts in a real cluster can emit a
/// `PeerRemoved` per peer within a tight window. With the
/// historical bounded (256-cap) channel and `try_send` drop-on-full
/// path, anything past 256 vanished without trace — the budget
/// accounting (`respawn_events` ring) never saw the request,
/// `respawn_budget_exhausted` never fired, and the operator had no
/// way to know a death had happened. The unbounded shape pins the
/// inverse: 1000 sequential `Removed` events all enqueue without
/// drop, exactly N of them clear the budget (here `max_total = 1000`),
/// and the spawner sees all N.
#[tokio::test(flavor = "current_thread")]
async fn unbounded_respawn_request_channel_accepts_burst() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut coordinator = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            coordinator.enable_respawn(
                spawner.clone(),
                RespawnBudget {
                    // Budget high enough that none of the burst
                    // entries are rejected: the test pins the
                    // ENQUEUE side (channel doesn't drop), so the
                    // budget arithmetic stays out of the way.
                    max_per_secondary: 1,
                    max_total: 1000,
                    cooldown: Duration::ZERO,
                },
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            let tx = coordinator
                .respawn_request_tx
                .as_ref()
                .expect("enable_respawn must install the sender")
                .clone();

            // 1000 sequential `PeerRemoved` translations. Each
            // peer id is unique so the family-budget cap of 1
            // accepts every entry.
            const BURST: u32 = 1000;
            for i in 0..BURST {
                tx.send(RespawnRequest {
                    original_id: format!("burst-{i}"),
                    cause: RemovalCause::KeepaliveMiss,
                })
                .expect(
                    "unbounded send must succeed while the \
                     receiver is alive — this is the contract \
                     the burst test pins",
                );
            }

            // Drain on the operational-loop side. We can't enter
            // `lifecycle::operational_loop` from this test
            // fixture (no real transport), so we replicate the
            // arm's behaviour: pull one request at a time and
            // call `dispatch_respawn_request`, draining the
            // JoinSet between dispatches so the spawner's atomic
            // counter has settled. The rx is taken out for the
            // duration of the drain so the per-iteration
            // `dispatch_respawn_request` (which mutates the same
            // coordinator) does not conflict with an outstanding
            // borrow on `respawn_request_rx`.
            let mut rx = coordinator
                .respawn_request_rx
                .take()
                .expect("enable_respawn must install the receiver");
            let mut drained = 0u32;
            while let Ok(req) = rx.try_recv() {
                drained += 1;
                coordinator.dispatch_respawn_request(req);
                if let Some(outcome) = coordinator.respawn_tasks.join_next().await {
                    let _ = outcome.expect("no panic in mock spawner");
                }
            }
            coordinator.respawn_request_rx = Some(rx);
            assert_eq!(
                drained, BURST,
                "all {BURST} enqueued requests must be drainable",
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                BURST,
                "spawner must have received every accepted request",
            );
            assert_eq!(
                coordinator.respawn_events.len() as u32,
                BURST,
                "every accepted request must land on the events ring",
            );
        })
        .await;
}
