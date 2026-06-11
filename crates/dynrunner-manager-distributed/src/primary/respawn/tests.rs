//! Contract-level constructor smoke tests. Full integration
//! (spawner ↔ dispatcher ↔ JoinSet drain) lands in sibling F6.

use super::*;
use dynrunner_protocol_primary_secondary::RemovalCause;
use std::time::Duration;

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
        cause: RemovalCause::KeepaliveMiss,
        result: Err("spawn failed".to_owned()),
    };
    assert!(matches!(err.result, Err(ref s) if s == "spawn failed"));
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
use crate::peer_lifecycle::PeerLifecycleEvent;
use crate::primary::test_helpers::{
    FixedEstimator, PrimaryMeshKeepalive, TestId, build_test_primary, setup_test,
};
use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use dynrunner_scheduler::ResourceStealingScheduler;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Counting mock spawner: records every `spec.new_secondary_id`
/// it observes and returns `Ok(())` for the first call (or as
/// configured). The recorded ids let tests assert the
/// coordinator minted fresh ids and the `RespawnDecision`
/// path honoured the budget. `revoke` calls are recorded the
/// same way so the re-admission reconciliation tests can pin
/// exactly which replacements were revoked.
struct MockSpawner {
    calls: Arc<AtomicU32>,
    captured_ids: Arc<Mutex<Vec<String>>>,
    revoked_ids: Arc<Mutex<Vec<String>>>,
}

impl MockSpawner {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicU32::new(0)),
            captured_ids: Arc::new(Mutex::new(Vec::new())),
            revoked_ids: Arc::new(Mutex::new(Vec::new())),
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
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.captured_ids
            .lock()
            .unwrap()
            .push(spec.new_secondary_id);
        Ok(())
    }

    async fn revoke(&self, new_secondary_id: &str) -> Result<(), SpawnError> {
        self.revoked_ids
            .lock()
            .unwrap()
            .push(new_secondary_id.to_owned());
        Ok(())
    }
}

/// Build a coordinator wired with 1 reserved initial-cohort id so
/// the first minted respawn lands on `secondary-1`. The minted-id
/// monotonic test pins this contract directly.
fn make_coordinator() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    PrimaryMeshKeepalive,
) {
    let (transport, _ends) = setup_test(0);
    let config = PrimaryConfig {
        num_secondaries: 1,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(1),
        keepalive_interval: Duration::from_millis(100),
        uses_file_based_items: false,
        retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(1),
        mesh_ready_timeout: Duration::from_secs(1),
        ..PrimaryConfig::default()
    };
    build_test_primary(
        config,
        transport,
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
            let (mut coordinator, _mesh) = make_coordinator();
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
            assert_eq!(coordinator.cluster_state.respawn_events().len(), 1);
        })
        .await;
}

/// Under the replicated graceful-abort freeze a departing secondary must
/// NOT be replaced: the fleet is draining DOWN by design, so
/// `dispatch_respawn_request` suppresses the request BEFORE the budget —
/// no spawner call, no respawn task, no ledger entry (a drain departure
/// never consumes budget either).
#[tokio::test(flavor = "current_thread")]
async fn respawn_suppressed_under_graceful_abort() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );
            // Latch the replicated dispatch freeze (the fact a drained
            // secondary departs under).
            coordinator
                .cluster_state
                .apply(dynrunner_protocol_primary_secondary::ClusterMutation::GracefulAbortRequested);

            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "secondary-0".into(),
                cause: RemovalCause::SelfDeparture(dynrunner_core::BoundedString::from(
                    "graceful abort: local work drained".to_string(),
                )),
            });

            assert!(
                coordinator.respawn_tasks.is_empty(),
                "no respawn future may be spawned under the graceful-abort freeze"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0, "spawner never invoked");
            assert!(
                coordinator.cluster_state.respawn_events().is_empty(),
                "a suppressed request must not consume replicated respawn budget"
            );
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
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // `make_coordinator` spawns the production mesh-pump
            // (`build_test_primary`), which `spawn_local`s — so this test, like
            // its siblings, must run inside a `LocalSet`.
            let (coordinator, _mesh) = make_coordinator();
            // No `enable_respawn` call — the spawner / budget / channel /
            // listener registration are all absent by construction.
            assert!(coordinator.respawn_spawner.is_none());
            assert!(coordinator.respawn_budget.is_none());
            assert!(coordinator.respawn_lifecycle_tx.is_none());
            assert!(coordinator.respawn_lifecycle_rx.is_none());
            assert!(coordinator.peer_lifecycle_listeners.is_empty());

            // Build a free-standing dispatcher listener so we can verify
            // its on_event side-effect: a Removed event has no place to
            // land if the channel side hasn't been wired. We construct a
            // throwaway channel just to verify the closure shape; the
            // coordinator's wiring itself is the contract under test.
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PeerLifecycleEvent>();
            let listener = respawn_dispatcher_listener(tx);
            let removed = PeerLifecycleEvent::Removed {
                id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            };
            listener.on_event(&removed);
            // The free-standing listener does enqueue (it's a pure
            // forwarder); the coordinator we built simply has no
            // listener registered, so its operational-loop arm would
            // never see the event. That's the CCD-5 invariant.
            let event = rx
                .try_recv()
                .expect("free-standing listener should still forward");
            assert_eq!(event, removed);
        })
        .await;
}

/// F7-ζ: with the respawn policy disabled (`respawn_budget == None`),
/// `dispatch_respawn_request` early-returns BEFORE any ledger write, so the
/// REPLICATED respawn ledger is never touched — the grow-only SET stays
/// empty and contributes nothing to a snapshot/digest. This pins that a
/// `None` budget never originates a replicated event.
#[tokio::test(flavor = "current_thread")]
async fn disabled_policy_writes_nothing_to_replicated_ledger() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            // No `enable_respawn` — `respawn_budget` is `None`.
            assert!(coordinator.respawn_budget.is_none());
            assert!(coordinator.cluster_state.respawn_events().is_empty());

            // Dispatch a request directly (what the operational-loop arm
            // does). With no budget, it must drop the request and write
            // nothing to the replicated ledger.
            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            assert!(
                coordinator.cluster_state.respawn_events().is_empty(),
                "a disabled respawn policy must never write to the replicated ledger",
            );
            assert!(
                coordinator.respawn_tasks.is_empty(),
                "a disabled respawn policy must never spawn",
            );
        })
        .await;
}

/// Respawn-vs-re-admission: a removal that is RE-ADMITTED (the same
/// incarnation provably returned — the frame-ingest seam flipped its
/// membership back to `Alive` at the next generation) must CANCEL the
/// queued-not-launched respawn for that peer. The request sits on the
/// unbounded channel until the operational loop drains it; the dispatch
/// decision point is therefore where the queued stage is cancellable,
/// and the replicated membership is the fact it consults. Because the
/// ledger entry (the budget spend) is only written on an ACCEPTED
/// dispatch, a canceled queued respawn never consumes budget — the
/// "refund" is structural.
///
/// A genuine death is intact: the same peer removed AGAIN (membership
/// `Dead` at dispatch time) spawns a replacement.
///
/// (The LAUNCHED stage needs no cancellation: an accepted respawn comes
/// up under a freshly-minted `secondary-N` id — `mint_secondary_id`,
/// pinned by `respawn_dispatcher_minted_id_is_monotonic`, with the
/// failover-monotonic `next_secondary_id` fold over capacity +
/// respawn-ledger ids — so it can never duplicate the re-admitted
/// peer's identity; it joins as an ordinary extra secondary.)
#[tokio::test(flavor = "current_thread")]
async fn queued_respawn_canceled_when_peer_readmitted_before_dispatch() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use dynrunner_protocol_primary_secondary::ClusterMutation;

            let (mut coordinator, _mesh) = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // The member joins, then is (falsely) removed — the removal
            // is what enqueued the respawn request the loop will drain.
            coordinator.cluster_state.apply(ClusterMutation::PeerJoined {
                peer_id: "sec-x".into(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 0,
            });
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-x".into(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                });
            // RE-ADMISSION lands BEFORE the queued request is drained:
            // the generation-advancing PeerJoined the frame-ingest seam
            // originates.
            coordinator.cluster_state.apply(ClusterMutation::PeerJoined {
                peer_id: "sec-x".into(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 1,
            });

            // The operational loop now drains the (stale) queued request.
            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "sec-x".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            assert!(
                coordinator.respawn_tasks.is_empty(),
                "no replacement may be spawned for a re-admitted (alive) peer"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0, "spawner never invoked");
            assert!(
                coordinator.cluster_state.respawn_events().is_empty(),
                "a canceled queued respawn must not consume replicated budget"
            );

            // Genuine death of the re-admitted incarnation: removal at
            // the CURRENT generation → membership Dead at dispatch time →
            // the replacement spawns.
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-x".into(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 1,
                });
            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "sec-x".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            let outcome = coordinator
                .respawn_tasks
                .join_next()
                .await
                .expect("genuine death must spawn a replacement")
                .expect("no panic");
            assert!(outcome.result.is_ok());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(coordinator.cluster_state.respawn_events().len(), 1);
        })
        .await;
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
            let (mut coordinator, _mesh) = make_coordinator();
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
            // Ledger records 3 events (one per accepted spawn).
            assert_eq!(coordinator.cluster_state.respawn_events().len(), 3);
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
            let (mut coordinator, _mesh) = make_coordinator();
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
            assert_eq!(coordinator.cluster_state.respawn_events().len(), 10);
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
            let (mut coordinator, _mesh) = make_coordinator();
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
/// accounting (the replicated `respawn_events` ledger) never saw the request,
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
            let (mut coordinator, _mesh) = make_coordinator();
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
                .respawn_lifecycle_tx
                .as_ref()
                .expect("enable_respawn must install the sender")
                .clone();

            // 1000 sequential `PeerRemoved` lifecycle events. Each
            // peer id is unique so the family-budget cap of 1
            // accepts every entry.
            const BURST: u32 = 1000;
            for i in 0..BURST {
                tx.send(PeerLifecycleEvent::Removed {
                    id: format!("burst-{i}"),
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
            // arm's behaviour: pull one event at a time and
            // call `dispatch_respawn_lifecycle`, draining the
            // JoinSet between dispatches so the spawner's atomic
            // counter has settled. The rx is taken out for the
            // duration of the drain so the per-iteration
            // `dispatch_respawn_lifecycle` (which mutates the same
            // coordinator) does not conflict with an outstanding
            // borrow on `respawn_lifecycle_rx`.
            let mut rx = coordinator
                .respawn_lifecycle_rx
                .take()
                .expect("enable_respawn must install the receiver");
            let mut drained = 0u32;
            while let Ok(event) = rx.try_recv() {
                drained += 1;
                coordinator.dispatch_respawn_lifecycle(event);
                if let Some(outcome) = coordinator.respawn_tasks.join_next().await {
                    let _ = outcome.expect("no panic in mock spawner");
                }
            }
            coordinator.respawn_lifecycle_rx = Some(rx);
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
                coordinator.cluster_state.respawn_events().len() as u32,
                BURST,
                "every accepted request must land on the replicated ledger",
            );
        })
        .await;
}

/// Production replay of the already-submitted-replacement edge
/// (original-re-admitted-FIRST interleaving): a member is removed, its
/// replacement is dispatched (sbatch submitted — here the mock spawner
/// stands in for the SLURM provider), and THEN the member's
/// authenticated frames re-admit it (the membership-generation bump
/// the frame-ingest seam originates). The still-pending replacement is
/// a resource squatter: the pipeline must revoke it through the
/// spawner port (`scancel` in the SLURM provider) and clear its
/// pending-replacement bookkeeping.
///
/// The event path is the production one: the lifecycle listener
/// `enable_respawn` registered forwards both the `Removed` and the
/// `Added` events onto the respawn lifecycle channel; the test drains
/// that channel into `dispatch_respawn_lifecycle` exactly as the
/// operational-loop arm does.
#[tokio::test(flavor = "current_thread")]
async fn replacement_revoked_when_original_readmitted_before_replacement_joins() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use dynrunner_protocol_primary_secondary::ClusterMutation;

            let (mut coordinator, _mesh) = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let revoked = Arc::clone(&spawner.revoked_ids);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // Membership ground truth: the member joins, then is
            // (falsely) removed — `is_peer_alive` must be false at
            // dispatch time or the queued-stage cancellation gate
            // (the prior fix) would absorb the request before the
            // launched-stage edge under test is ever reached.
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-x".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-x".into(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                });

            // The death reaches the pipeline through the REGISTERED
            // listener (the same one the peer-lifecycle dispatcher
            // fires in production), then the channel drain mirrors
            // the operational-loop arm.
            let removed = PeerLifecycleEvent::Removed {
                id: "sec-x".into(),
                cause: RemovalCause::KeepaliveMiss,
            };
            for listener in &coordinator.peer_lifecycle_listeners {
                listener.on_event(&removed);
            }
            let mut rx = coordinator
                .respawn_lifecycle_rx
                .take()
                .expect("enable_respawn must install the receiver");
            let event = rx.try_recv().expect("listener must forward the removal");
            coordinator.dispatch_respawn_lifecycle(event);
            let outcome = coordinator
                .respawn_tasks
                .join_next()
                .await
                .expect("the death must spawn a replacement")
                .expect("no panic");
            assert!(outcome.result.is_ok());
            assert_eq!(outcome.new_id, "secondary-1");
            // The replacement is now pending-until-join, keyed by its
            // minted id.
            assert_eq!(
                coordinator.pending_replacements.get("secondary-1"),
                Some(&"sec-x".to_string()),
                "an accepted dispatch must track the replacement as pending",
            );

            // RE-ADMISSION: the frame-ingest seam originates the
            // generation-advancing PeerJoined; its apply emits the
            // `Added` lifecycle event the listener forwards.
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-x".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 1,
                });
            let readmitted = PeerLifecycleEvent::Added {
                id: "sec-x".into(),
                is_observer: false,
            };
            for listener in &coordinator.peer_lifecycle_listeners {
                listener.on_event(&readmitted);
            }
            let event = rx
                .try_recv()
                .expect("listener must forward the re-admission join");
            coordinator.dispatch_respawn_lifecycle(event);

            // The revoke runs detached on the LocalSet (the loop arm
            // must not await a gateway round-trip); yield until it
            // lands.
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                revoked.lock().unwrap().clone(),
                vec!["secondary-1".to_string()],
                "the still-pending replacement must be revoked when its \
                 original re-admits",
            );
            assert!(
                coordinator.pending_replacements.is_empty(),
                "revocation must clear the pending-replacement bookkeeping",
            );
            coordinator.respawn_lifecycle_rx = Some(rx);
        })
        .await;
}

/// The replacement-welcomed-FIRST interleaving: the replacement joins
/// the membership (its `PeerJoined` applies before any re-admission of
/// the original). It is then the legitimate occupant — bookkeeping is
/// cleared WITHOUT a revoke, and a LATER re-admission of the original
/// must not revoke it either (a welcomed member is ordinary fleet
/// capacity; both run side by side under distinct `secondary-N` ids).
#[tokio::test(flavor = "current_thread")]
async fn replacement_join_clears_bookkeeping_and_later_readmission_revokes_nothing() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use dynrunner_protocol_primary_secondary::ClusterMutation;

            let (mut coordinator, _mesh) = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let revoked = Arc::clone(&spawner.revoked_ids);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-x".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-x".into(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                });
            coordinator.dispatch_respawn_lifecycle(PeerLifecycleEvent::Removed {
                id: "sec-x".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            let outcome = coordinator
                .respawn_tasks
                .join_next()
                .await
                .expect("the death must spawn a replacement")
                .expect("no panic");
            assert_eq!(outcome.new_id, "secondary-1");
            assert!(coordinator.pending_replacements.contains_key("secondary-1"));

            // The replacement WELCOMES first: its PeerJoined applies
            // and the forwarded `Added` clears the bookkeeping.
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "secondary-1".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            coordinator.dispatch_respawn_lifecycle(PeerLifecycleEvent::Added {
                id: "secondary-1".into(),
                is_observer: false,
            });
            assert!(
                coordinator.pending_replacements.is_empty(),
                "the joined replacement must leave the pending bookkeeping",
            );

            // The original re-admits LATER: nothing is pending for it
            // any more, so nothing may be revoked.
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-x".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 1,
                });
            coordinator.dispatch_respawn_lifecycle(PeerLifecycleEvent::Added {
                id: "sec-x".into(),
                is_observer: false,
            });
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert!(
                revoked.lock().unwrap().is_empty(),
                "a welcomed replacement is the legitimate occupant; a later \
                 re-admission of its original must not revoke it",
            );
        })
        .await;
}
