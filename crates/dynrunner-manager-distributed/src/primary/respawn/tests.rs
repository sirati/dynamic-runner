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
        dead_member_id: Some("secondary-0".to_owned()),
    };
    assert_eq!(spec.new_secondary_id, "sec-replacement-1");
    assert_eq!(spec.primary_endpoint, "127.0.0.1:5555");
    assert!(spec.primary_pubkey_pem.starts_with("-----BEGIN"));
    assert_eq!(spec.dead_member_id.as_deref(), Some("secondary-0"));
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
    FixedEstimator, MockSpawner, PrimaryMeshKeepalive, TestId, build_test_primary, setup_test,
};
use crate::primary::{PrimaryConfig, PrimaryCoordinator};
use dynrunner_scheduler::ResourceStealingScheduler;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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

/// A respawn must carry the DEAD member's id on the spec as
/// `dead_member_id`, so the (provider-agnostic) coordinator names who
/// died and lets the provider resolve the SLURM node to exclude from
/// SLURM's own vocabulary. The coordinator no longer holds a node map —
/// it always names the dead member; an unresolvable id is the
/// provider's best-effort omit, not the coordinator's concern.
#[tokio::test(flavor = "current_thread")]
async fn respawn_spec_names_dead_member_for_exclusion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _mesh) = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            let outcome = coordinator
                .respawn_tasks
                .join_next()
                .await
                .expect("respawn task present")
                .expect("respawn task should not panic");
            assert!(outcome.result.is_ok());
            // The minted replacement is `secondary-1`; its spec must name
            // the dead member it stands in for.
            assert_eq!(
                spawner.dead_member_for("secondary-1").as_deref(),
                Some("secondary-0"),
                "the replacement spec must name the dead member to exclude",
            );
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
            // A closed gate (no replacement pending) — a `Removed` is
            // always relevant, so it must still forward.
            let gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let listener = respawn_dispatcher_listener(tx, gate);
            let removed = PeerLifecycleEvent::Removed {
                id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            };
            listener.on_event(&removed);
            // The free-standing listener does enqueue (a death is always a
            // spawn trigger); the coordinator we built simply has no
            // listener registered, so its operational-loop arm would
            // never see the event. That's the CCD-5 invariant.
            let event = rx
                .try_recv()
                .expect("free-standing listener should still forward a removal");
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

/// #467: a removed member's respawn replacement has already SEATED
/// (operational, a live member) by the time the original re-admits, so
/// BOTH hold a SLURM job to run-end — the account-quota double-occupancy
/// #399 does NOT heal (its revoke path only covers a queued / not-yet-
/// joined replacement). The re-admission seam must mark the SEATED
/// replacement for graceful wind-down, and that wind-down's deliberate
/// self-departure must NOT spawn yet another replacement (else the fix
/// self-defeats / loops).
///
/// Revert-confirm: without `schedule_seated_replacement_winddown` no
/// `WindDownRequested` is recorded — the replacement stays seated to
/// run-end (the second assertion below fails). Without the
/// `SelfDeparture` respawn-admission guard the replacement's departure
/// re-spawns (the third assertion's respawn_events bump / respawn_tasks
/// non-empty fires).
#[tokio::test(flavor = "current_thread")]
async fn seated_replacement_winds_down_on_readmission_and_its_departure_does_not_respawn() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use dynrunner_protocol_primary_secondary::{
                ClusterMutation, DistributedMessage, KeepaliveRole,
            };

            let (mut coordinator, _mesh) = make_coordinator();
            let spawner = Arc::new(MockSpawner::new());
            let calls = Arc::clone(&spawner.calls);
            coordinator.enable_respawn(
                spawner.clone(),
                permissive_budget(),
                "tcp://127.0.0.1:5555".into(),
                "-----BEGIN PUBLIC KEY-----\nFAKE\n".into(),
            );

            // ── Stage 1: the original joins, is (falsely) removed, and a
            // replacement is spawned + SEATS (becomes a live member). ──
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
            let replacement = outcome.new_id.clone();
            assert_eq!(replacement, "secondary-1");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "one replacement spawned");
            assert_eq!(coordinator.cluster_state.respawn_events().len(), 1);

            // The replacement SEATS: its PeerJoined applies (live member)
            // and the forwarded `Added` clears the node-local pending
            // bookkeeping — the exact post-seat state in which #399 holds
            // nothing for it any more.
            coordinator.cluster_state.apply(ClusterMutation::PeerJoined {
                peer_id: replacement.clone(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 0,
            });
            coordinator.dispatch_respawn_lifecycle(PeerLifecycleEvent::Added {
                id: replacement.clone(),
                is_observer: false,
            });
            assert!(
                coordinator.pending_replacements.is_empty(),
                "the seated replacement is no longer tracked as pending",
            );
            assert!(
                coordinator.cluster_state.is_peer_alive(&replacement),
                "the replacement is a live (seated) member",
            );

            // ── Stage 2: the ORIGINAL re-admits via the real frame-ingest
            // seam (an authenticated keepalive from the removed-but-alive
            // member). This is the production entry path — it bumps the
            // original's membership generation AND must now schedule the
            // seated replacement's wind-down. ──
            let readmit_frame: DistributedMessage<TestId> = DistributedMessage::Keepalive {
                target: None,
                sender_id: "sec-x".into(),
                timestamp: 1.0,
                secondary_id: "sec-x".into(),
                active_workers: 0,
                emitter_role: KeepaliveRole::Secondary,
            };
            coordinator.maybe_readmit_sender(&readmit_frame).await;

            // (a) The re-admitted original is alive again and continues.
            assert!(
                coordinator.cluster_state.is_peer_alive("sec-x"),
                "the re-admitted original is a live member again",
            );
            // (b) THE LOAD-BEARING POSITIVE: the seated replacement is
            // marked for wind-down at its CURRENT incarnation generation.
            let replacement_gen = coordinator.cluster_state.peer_member_gen(&replacement);
            assert!(
                coordinator
                    .cluster_state
                    .wind_down_requested(&replacement, replacement_gen),
                "the seated replacement must be scheduled to wind down once \
                 its original re-admits (the #467 double-occupancy heal)",
            );
            // The re-admitted original must NOT be wound down — it is the
            // process that was wrongly removed; only the replacement stands
            // down.
            let original_gen = coordinator.cluster_state.peer_member_gen("sec-x");
            assert!(
                !coordinator
                    .cluster_state
                    .wind_down_requested("sec-x", original_gen),
                "the re-admitted original must never be wound down",
            );

            // ── Stage 3: THE LOAD-BEARING NEGATIVE (owner-pinned). The
            // replacement, at its next quiescence, gracefully departs via a
            // self-authored `PeerRemoved { SelfDeparture }`. That departure
            // must NOT spawn yet another replacement — drive the resulting
            // lifecycle `Removed` through the SAME dispatch path the
            // operational loop uses and assert zero new respawn. ──
            let respawn_events_before = coordinator.cluster_state.respawn_events().len();
            let calls_before = calls.load(Ordering::SeqCst);
            coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: replacement.clone(),
                    cause: RemovalCause::SelfDeparture(
                        dynrunner_core::BoundedString::from(
                            "graceful abort: local work drained".to_string(),
                        ),
                    ),
                    member_gen: replacement_gen,
                });
            coordinator.dispatch_respawn_lifecycle(PeerLifecycleEvent::Removed {
                id: replacement.clone(),
                cause: RemovalCause::SelfDeparture(dynrunner_core::BoundedString::from(
                    "graceful abort: local work drained".to_string(),
                )),
            });
            // Let any (erroneously) spawned future settle before asserting.
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert!(
                coordinator.respawn_tasks.is_empty(),
                "a deliberate self-departure (the wound-down replacement) \
                 must NOT spawn a replacement",
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                calls_before,
                "the spawner must not be invoked for the wound-down \
                 replacement's self-departure",
            );
            assert_eq!(
                coordinator.cluster_state.respawn_events().len(),
                respawn_events_before,
                "the self-departure must not record a new respawn event \
                 (no budget consumed) — closing the wind-down→respawn loop",
            );
        })
        .await;
}

// ═══════════════════════════════════════════════════════════════════
// Remote-execution backend (the relocated/promoted-primary topology):
// the respawn DECISION runs on a primary with NO local provider; the
// EXECUTION is delegated over the mesh to the provider-host observer
// process ("setup"). These tests stand up TWO real mesh processes —
// a promoted primary at "promoted-1" and a provider-hosting observer
// at "setup" — over paired channel transports, with both production
// mesh-pumps running, and replay the production sequence end-to-end.
// ═══════════════════════════════════════════════════════════════════

use crate::observer::{ObserverConfig, ObserverCoordinator, ObserverHandoff};
use crate::process::{LocalRole, Mesh};
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::{
    ClusterMutation, Destination, DistributedMessage, MessageType,
};
use dynrunner_transport_channel::ChannelPeerTransport;
use std::collections::HashMap as StdHashMap;
use tokio::sync::mpsc as tokio_mpsc;

/// Two-process rig: the promoted primary (decision) and the
/// provider-host observer (execution), wired over paired channel
/// transports with both mesh-pumps live.
struct RemoteRig {
    coordinator: PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    /// The observer-hosted provider (the execution side's MockSpawner).
    provider: Arc<MockSpawner>,
    /// Raw inject into the observer process's inbound wire — used to
    /// replay a DUPLICATE request (the lost-result re-send) without
    /// waiting out the stub's 10s re-send window.
    obs_inject_tx: tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// Keepalives: pumps + control handles + the observer's run task.
    _pri_keepalive: PrimaryMeshKeepalive,
    _obs_control: crate::process::MeshControlHandle<TestId>,
    _obs_pump: tokio::task::JoinHandle<()>,
    _obs_run: tokio::task::JoinHandle<()>,
}

/// Build the two connected processes. The PRIMARY side is built by the
/// standard `build_test_primary` (node id "promoted-1") over a channel
/// transport whose single peer is "setup"; the OBSERVER side mirrors
/// the relocated submitter: an `ObserverCoordinator::from_handoff`
/// carrying the respawn provider (the process-owned half), its
/// `current_primary` naming the promoted node, and its OWN production
/// run loop driving the respawn-execution arm.
async fn build_remote_rig() -> RemoteRig {
    // Paired transports: pri("promoted-1") <-> obs("setup").
    let (to_pri_tx, to_pri_rx) = tokio_mpsc::unbounded_channel();
    let (to_obs_tx, to_obs_rx) = tokio_mpsc::unbounded_channel();
    let pri_transport = ChannelPeerTransport::<TestId>::from_raw_channels(
        "promoted-1".into(),
        StdHashMap::from([("setup".to_string(), to_obs_tx.clone())]),
        to_pri_rx,
    );
    let obs_transport = ChannelPeerTransport::<TestId>::from_raw_channels(
        "setup".into(),
        StdHashMap::from([("promoted-1".to_string(), to_pri_tx)]),
        to_obs_rx,
    );

    // Primary process (the promoted decision holder).
    let config = PrimaryConfig {
        node_id: "promoted-1".into(),
        num_secondaries: 1,
        connect_timeout: Duration::from_secs(1),
        peer_timeout: Duration::from_secs(60),
        keepalive_interval: Duration::from_secs(60),
        uses_file_based_items: false,
        retry_max_passes: 0,
        fleet_dead_timeout: Duration::from_secs(60),
        mesh_ready_timeout: Duration::from_secs(1),
        ..PrimaryConfig::default()
    };
    let (coordinator, pri_keepalive) = build_test_primary(
        config,
        pri_transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    // Observer process (the provider host). Mirrors `from_handoff`:
    // the provider rides the handoff across the submitter's demotion.
    let mut obs_mesh = Mesh::new(obs_transport);
    let (obs_slot, obs_client, obs_inbox) =
        obs_mesh.register_local_role(LocalRole::Observer, PeerId::from("setup"));
    obs_mesh.publish_membership();
    let (obs_control, obs_control_rx) = crate::process::pump::control_channel::<TestId>();
    let obs_pump = tokio::task::spawn_local(async move {
        let _slot = obs_slot;
        crate::process::pump::run_pump(obs_mesh, obs_control_rx).await;
    });

    let mut obs_state = crate::cluster_state::ClusterState::<TestId>::new();
    obs_state.apply(ClusterMutation::PrimaryChanged {
        new: "promoted-1".into(),
        epoch: 1,
        reason: Default::default(),
    });
    let provider = Arc::new(MockSpawner::new());
    let inherited_dispatcher =
        tokio::task::spawn_local(async { std::future::pending::<()>().await });
    let lifecycle_dispatcher =
        tokio::task::spawn_local(async { std::future::pending::<()>().await });
    let handoff = ObserverHandoff {
        client: obs_client,
        inbox: obs_inbox,
        cluster_state: obs_state,
        node_id: "setup".into(),
        deadlines: ObserverConfig {
            node_id: "setup".into(),
            fleet_dead_timeout: Duration::from_secs(60),
            peer_timeout: Duration::from_secs(60),
            panik_watcher_paths: Vec::new(),
            panik_watcher_poll_interval: Duration::from_secs(60),
            fleet_death_presumption: ObserverConfig::DEFAULT_FLEET_DEATH_PRESUMPTION,
        },
        started_phases: std::collections::HashSet::new(),
        panik_signal_rx: None,
        task_completed_dispatcher_handle: inherited_dispatcher,
        lifecycle_dispatcher_handle: lifecycle_dispatcher,
        holdings: std::collections::HashSet::new(),
        reconnector: None,
        upload_action: None,
        respawn_provider: Some(provider.clone() as Arc<dyn SecondarySpawner>),
        graceful_abort_trigger: None,
        job_ledger: None,
    };
    let mut observer = ObserverCoordinator::from_handoff(handoff);
    let obs_run = tokio::task::spawn_local(async move {
        let _ = observer.run().await;
    });

    RemoteRig {
        coordinator,
        provider,
        obs_inject_tx: to_obs_tx,
        _pri_keepalive: pri_keepalive,
        _obs_control: obs_control,
        _obs_pump: obs_pump,
        _obs_run: obs_run,
    }
}

/// Pump the primary's inbox through `dispatch_message` (what the
/// operational loop's inbox arm does) until a respawn outcome lands on
/// the JoinSet or the deadline passes. Returns the drained inbound
/// message types alongside the outcome for frame-level assertions.
async fn pump_until_respawn_outcome(
    coordinator: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    deadline: Duration,
) -> (Option<RespawnOutcome>, Vec<MessageType>) {
    let mut seen = Vec::new();
    let started = tokio::time::Instant::now();
    loop {
        while let Some(msg) = coordinator.inbox.try_recv() {
            seen.push(msg.msg_type());
            coordinator
                .dispatch_message(msg, &mut None)
                .await
                .expect("dispatch_message must not error in this rig");
        }
        if let Some(joined) = coordinator.respawn_tasks.try_join_next() {
            return (Some(joined.expect("respawn task must not panic")), seen);
        }
        if started.elapsed() > deadline {
            return (None, seen);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// TRACE-REPLAY of the production shape that sat inert: a PROMOTED
/// primary (relocated off the submitter — NO local provider) with the
/// on-secondary-death policy sees a member removal. Pre-fix,
/// `enable_respawn` was never called on this topology, so the respawn
/// arm was structurally parked (`respawn_lifecycle_rx = None`) and the
/// `respawn_request` counter sat at 0 forever. Now: the replicated
/// `RespawnPolicySet` re-arms the decision at promotion-snapshot
/// hydrate; the removal flows listener → channel → dispatch (the
/// production path); the EXECUTION crosses the mesh to the observer's
/// provider; the outcome flows back; the replicated ledger records the
/// spend.
#[tokio::test(flavor = "current_thread")]
async fn promoted_primary_respawns_through_observer_hosted_provider() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut rig = build_remote_rig().await;

            // The promotion snapshot a failed-over secondary inherits:
            // policy caps + the member that is about to die.
            let mut origin = crate::cluster_state::ClusterState::<TestId>::new();
            origin.apply(ClusterMutation::RespawnPolicySet {
                max_per_secondary: 100,
                max_total: 100,
                cooldown_ms: 0,
            });
            origin.apply(ClusterMutation::PeerJoined {
                peer_id: "secondary-0".into(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 0,
            });
            rig.coordinator
                .seed_from_promotion_snapshot(origin.snapshot());

            // The structural fact that was FALSE in production: the
            // promoted primary's respawn arm is ARMED (this is exactly
            // why respawn_request stayed 0 — the arm had no receiver).
            assert!(
                rig.coordinator.respawn_lifecycle_rx.is_some(),
                "promotion hydrate must re-arm the respawn lifecycle arm",
            );
            assert!(
                rig.coordinator.respawn_budget.is_some(),
                "promotion hydrate must restore the budget caps",
            );
            assert!(
                rig.coordinator.respawn_spawner.is_some(),
                "promotion hydrate must install the remote execution backend",
            );

            // The member dies — replayed through the PRODUCTION path:
            // replicated removal → registered lifecycle listener →
            // respawn channel → the operational-loop dispatch.
            rig.coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: "secondary-0".into(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                });
            let removed = PeerLifecycleEvent::Removed {
                id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            };
            for listener in &rig.coordinator.peer_lifecycle_listeners {
                listener.on_event(&removed);
            }
            let mut rx = rig
                .coordinator
                .respawn_lifecycle_rx
                .take()
                .expect("armed above");
            let event = rx.try_recv().expect("listener must forward the removal");
            rig.coordinator.dispatch_respawn_lifecycle(event);
            rig.coordinator.respawn_lifecycle_rx = Some(rx);

            // The spend lands on the replicated ledger AT DISPATCH —
            // the counter that sat at 0 in production is non-zero the
            // moment the decision accepts.
            assert_eq!(
                rig.coordinator.cluster_state.respawn_events().len(),
                1,
                "the accepted respawn must be on the replicated ledger",
            );

            // EXECUTION crosses the mesh: the observer's arm drives the
            // provider and the outcome flows back to complete the
            // primary's spawn future.
            let (outcome, seen) =
                pump_until_respawn_outcome(&mut rig.coordinator, Duration::from_secs(10)).await;
            let outcome = outcome.expect("the remote round trip must complete");
            assert_eq!(outcome.new_id, "secondary-1");
            assert!(
                outcome.result.is_ok(),
                "observer-side provider success must come back Ok: {:?}",
                outcome.result,
            );
            assert_eq!(rig.provider.call_count(), 1, "provider executed exactly once");
            assert_eq!(rig.provider.captured_ids(), vec!["secondary-1".to_string()]);
            assert!(
                seen.contains(&MessageType::RespawnSpawnResult),
                "the outcome must arrive as a typed RespawnSpawnResult frame; saw {seen:?}",
            );
        })
        .await;
}

/// Idempotency at the execution host: a RE-SENT spawn request for the
/// same replacement id (the lost-result replay — the stub re-sends the
/// SAME id until a result lands) must NOT double-submit. The observer's
/// arm dedupes on the id and replays the cached outcome instead.
#[tokio::test(flavor = "current_thread")]
async fn duplicate_spawn_request_replays_cached_outcome_without_resubmitting() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut rig = build_remote_rig().await;
            rig.coordinator.enable_respawn_remote(permissive_budget());

            rig.coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "secondary-0".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            let (outcome, _) =
                pump_until_respawn_outcome(&mut rig.coordinator, Duration::from_secs(10)).await;
            assert!(outcome.expect("first round trip").result.is_ok());
            assert_eq!(rig.provider.call_count(), 1);

            // Replay the EXACT request bytes into the observer process
            // (what the stub's re-send window would do after 10s).
            let dup = DistributedMessage::RespawnSpawnRequest {
                target: None,
                sender_id: "promoted-1".into(),
                timestamp: 0.0,
                new_secondary_id: "secondary-1".into(),
                primary_endpoint: String::new(),
                primary_pubkey_pem: String::new(),
                dead_member_id: None,
            }
            .with_target(Destination::Observer(PeerId::from("setup")));
            rig.obs_inject_tx.send(dup).expect("observer wire open");

            // The duplicate must produce a REPLAYED result frame at the
            // primary (the lost-result heal) and NO second submission.
            let started = tokio::time::Instant::now();
            let mut replayed = false;
            while started.elapsed() < Duration::from_secs(10) && !replayed {
                while let Some(msg) = rig.coordinator.inbox.try_recv() {
                    if msg.msg_type() == MessageType::RespawnSpawnResult {
                        replayed = true;
                    }
                    rig.coordinator
                        .dispatch_message(msg, &mut None)
                        .await
                        .expect("dispatch ok");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(replayed, "the cached outcome must be replayed to the primary");
            assert_eq!(
                rig.provider.call_count(),
                1,
                "one replacement id must never submit twice",
            );
        })
        .await;
}

/// Remote revocation parity: a member re-admitted while its
/// replacement is still pending revokes the squatter THROUGH THE MESH —
/// the same `SecondarySpawner::revoke` contract the local provider
/// honours, executed by the observer-hosted provider.
#[tokio::test(flavor = "current_thread")]
async fn readmission_revokes_pending_replacement_through_observer_provider() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut rig = build_remote_rig().await;
            rig.coordinator.enable_respawn_remote(permissive_budget());

            // Member joins, dies, replacement dispatched + completed.
            rig.coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-x".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 0,
                });
            rig.coordinator
                .cluster_state
                .apply(ClusterMutation::PeerRemoved {
                    id: "sec-x".into(),
                    cause: RemovalCause::KeepaliveMiss,
                    member_gen: 0,
                });
            rig.coordinator.dispatch_respawn_request(RespawnRequest {
                original_id: "sec-x".into(),
                cause: RemovalCause::KeepaliveMiss,
            });
            let (outcome, _) =
                pump_until_respawn_outcome(&mut rig.coordinator, Duration::from_secs(10)).await;
            let outcome = outcome.expect("spawn round trip");
            assert!(outcome.result.is_ok());
            assert!(
                rig.coordinator
                    .pending_replacements
                    .contains_key("secondary-1"),
                "the replacement must be pending-until-join",
            );

            // RE-ADMISSION: the original returns alive at the next
            // generation; the reconciliation revokes the squatter via
            // the REMOTE provider.
            rig.coordinator
                .cluster_state
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-x".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                    member_gen: 1,
                });
            rig.coordinator
                .dispatch_respawn_lifecycle(PeerLifecycleEvent::Added {
                    id: "sec-x".into(),
                    is_observer: false,
                });

            // The revoke runs detached; pump the primary's inbox (the
            // revoke RESULT must complete the stub's waiter) until the
            // observer-side provider records the revocation.
            let started = tokio::time::Instant::now();
            while started.elapsed() < Duration::from_secs(10)
                && rig.provider.revoked_ids.lock().unwrap().is_empty()
            {
                while let Some(msg) = rig.coordinator.inbox.try_recv() {
                    rig.coordinator
                        .dispatch_message(msg, &mut None)
                        .await
                        .expect("dispatch ok");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                rig.provider.revoked_ids.lock().unwrap().clone(),
                vec!["secondary-1".to_string()],
                "the pending replacement must be revoked on the provider host",
            );
            assert!(
                rig.coordinator.pending_replacements.is_empty(),
                "revocation must clear the pending bookkeeping",
            );
        })
        .await;
}

/// The seed originators replicate the enabled policy caps (the
/// promoted primary's re-arm source). A coordinator WITHOUT the policy
/// replicates nothing — every replica keeps `None` ("respawn off").
#[tokio::test(flavor = "current_thread")]
async fn seed_origination_replicates_enabled_policy_caps() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Enabled: the relocated-seed originator (the mesh-always
            // submitter's production path) carries the caps.
            let (mut enabled, _mesh) = make_coordinator();
            enabled.enable_respawn(
                Arc::new(MockSpawner::new()),
                RespawnBudget {
                    max_per_secondary: 5,
                    max_total: 20,
                    cooldown: Duration::from_secs(45),
                },
                "tcp://127.0.0.1:5555".into(),
                "PEM".into(),
            );
            enabled.originate_relocated_seed(std::collections::HashMap::new());
            let policy = enabled
                .cluster_state
                .respawn_policy()
                .expect("enabled policy must be replicated with the seed");
            assert_eq!(policy.max_per_secondary, 5);
            assert_eq!(policy.max_total, 20);
            assert_eq!(policy.cooldown_ms, 45_000);

            // Disabled: nothing is replicated.
            let (mut disabled, _mesh2) = make_coordinator();
            disabled.originate_relocated_seed(std::collections::HashMap::new());
            assert_eq!(disabled.cluster_state.respawn_policy(), None);
        })
        .await;
}
