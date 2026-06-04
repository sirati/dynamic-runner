//! Unit tests for the submitter's bootstrap hand-off + observer tail
//! ([`PrimaryCoordinator::relocate_primary_to`] +
//! [`PrimaryCoordinator::run_as_observer`]) and the relinquished-case
//! result getters.
//!
//! These exercise the manager-layer mechanism directly (the pyo3
//! coordinator runtimes can't be unit-tested — libpython linking), over
//! a `ChannelPeerTransport` whose `has_peer` / `peer_count` reflect real
//! `outgoing` membership and whose broadcasts fan out to per-secondary
//! receivers a test can drain.

use super::*;
use dynrunner_core::ErrorType;
use dynrunner_protocol_primary_secondary::{KeepaliveRole, PeerId, PeerTransport};

use crate::primary::lifecycle::RelocationOutcome;
use crate::primary::wire::compute_task_hash;

/// Build a submitter coordinator whose mesh transport confirms exactly
/// the secondaries `sec-0 .. sec-{confirmed_peers-1}` (the
/// `ChannelPeerTransport` keys `has_peer` / `peer_count` off its
/// `outgoing` membership, which `setup_test` populates with those ids),
/// returning the coordinator and the per-secondary end handles so a test
/// can drain the broadcasts the submitter fans out.
#[allow(clippy::type_complexity)]
fn coordinator_with_confirmed_peers(
    confirmed_peers: u32,
) -> (
    PrimaryCoordinator<ChannelPeerTransport<TestId>, ResourceStealingScheduler, FixedEstimator, TestId>,
    Vec<(
        String,
        tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio::sync::mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (transport, ends) = setup_test(confirmed_peers);
    let coordinator = PrimaryCoordinator::new(
        PrimaryConfig {
            num_secondaries: confirmed_peers.max(1),
            // Short so the dead-fleet grace fires fast in the
            // peer-count==0 observer test.
            fleet_dead_timeout: std::time::Duration::from_millis(50),
            ..Default::default()
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (coordinator, ends)
}

/// Build an observer coordinator with `confirmed_peers` resident mesh
/// peers AND short `peer_timeout` / `fleet_dead_timeout`, so the
/// primary-silence backstop fires fast in tests. The resident `outgoing`
/// entries keep `peer_count() > 0` (the SIGTERM'd-remote shape: the
/// connection table is non-empty because the observer never sends and so
/// never prunes the dead link), while a `recv_peer()` that is never fed
/// stays pending — exactly the half-open-peer strand. Returns the
/// per-secondary ends so a test can feed primary keepalives / mutations
/// into the observer's inbound via the shared `incoming_tx`.
#[allow(clippy::type_complexity)]
fn observer_with_short_timeouts(
    confirmed_peers: u32,
    peer_timeout: std::time::Duration,
) -> (
    PrimaryCoordinator<ChannelPeerTransport<TestId>, ResourceStealingScheduler, FixedEstimator, TestId>,
    Vec<(
        String,
        tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio::sync::mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
) {
    let (transport, ends) = setup_test(confirmed_peers);
    let coordinator = PrimaryCoordinator::new(
        PrimaryConfig {
            num_secondaries: confirmed_peers.max(1),
            peer_timeout,
            // The fleet-dead poll is the wake source that re-drives the
            // top-of-loop silence check; keep it short and below
            // `peer_timeout` so the backstop re-evaluates promptly.
            fleet_dead_timeout: std::time::Duration::from_millis(20),
            ..Default::default()
        },
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    (coordinator, ends)
}

/// Collect every `PrimaryChanged { new, epoch }` the submitter fanned
/// out to the given secondary-end receiver.
fn drain_primary_changes(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation { mutations, .. } = msg {
            for m in mutations {
                if let ClusterMutation::PrimaryChanged { new, epoch, .. } = m {
                    out.push((new, epoch));
                }
            }
        }
    }
    out
}

/// `relocate_primary_to(chosen)` on a confirmed, non-observer candidate:
/// originates `PrimaryChanged { chosen, epoch+1 }` over the mesh, steps
/// the submitter down (drops its own `Role::Primary` via the local apply),
/// and — crucially — does NOT call `activate_local_primary` (so it never
/// sets `primary_id = self`).
#[tokio::test(flavor = "current_thread")]
async fn relocate_originates_primary_changed_and_does_not_pin_self() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, mut ends) = coordinator_with_confirmed_peers(1);

            // Pre-state: the submitter is the bootstrap primary WITHOUT a
            // self-announce — the relocate path does no submitter epoch-1
            // self-announce, so `primary_epoch` starts at 0 (the bootstrap
            // pin is not a `PrimaryChanged`).
            assert_eq!(coordinator.cluster_state_for_test().primary_epoch(), 0);

            let outcome = coordinator
                .relocate_primary_to(PeerId::from("sec-0"))
                .await
                .expect("relocate succeeds");
            assert_eq!(
                outcome,
                RelocationOutcome::Relocated,
                "a confirmed, non-observer candidate must relocate, not fall back"
            );

            // Wire announce: exactly one PrimaryChanged naming the CHOSEN
            // peer at epoch 1 (= primary_epoch()+1 = 0+1) fanned out over
            // the mesh.
            let (_id, rx, _tx) = &mut ends[0];
            assert_eq!(
                drain_primary_changes(rx),
                vec![("sec-0".to_string(), 1)],
                "relocate must broadcast PrimaryChanged{{ chosen, epoch+1 }}"
            );

            // The submitter applied its own broadcast: current_primary is
            // now the chosen peer at epoch 1 (it dropped Role::Primary).
            assert_eq!(
                coordinator.cluster_state_for_test().current_primary(),
                Some("sec-0"),
                "the submitter's own apply must repoint current_primary to the chosen peer"
            );
            assert_eq!(coordinator.cluster_state_for_test().primary_epoch(), 1);

            // The bootstrap pin is GONE: relocate must NOT set
            // primary_id = self (that is `activate_local_primary`'s job,
            // which the relocate path must never call).
            assert_eq!(
                coordinator.primary_id, None,
                "relocate_primary_to must NOT set primary_id = self \
                 (it does not call activate_local_primary)"
            );
        })
        .await;
}

/// Vanished-candidate fallback: when the chosen peer is NOT a confirmed
/// mesh peer at origination time (`has_peer == false`), relocate falls
/// back to `activate_local_primary` — the submitter STAYS primary
/// (`primary_id = self`, current_primary = self) and signals the caller
/// to run the normal operational path.
#[tokio::test(flavor = "current_thread")]
async fn relocate_falls_back_to_local_on_vanished_candidate() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Zero confirmed peers → has_peer("sec-0") is false.
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(0);

            let outcome = coordinator
                .relocate_primary_to(PeerId::from("sec-0"))
                .await
                .expect("relocate succeeds via fallback");
            assert_eq!(
                outcome,
                RelocationOutcome::FellBackToLocal,
                "an unconfirmed candidate must fall back to local primary"
            );

            // Fell back to activate_local_primary: the submitter stayed
            // primary (pinned its own id, named itself current_primary).
            assert_eq!(
                coordinator.primary_id,
                Some("primary".to_string()),
                "the fallback must activate the submitter as the local primary"
            );
            assert_eq!(
                coordinator.cluster_state_for_test().current_primary(),
                Some("primary"),
                "the fallback must leave the submitter as current_primary"
            );
        })
        .await;
}

/// A confirmed candidate that has nonetheless landed in
/// `role_table().observers` between selection and origination is rejected
/// by the defensive observer cut → fall back to local primary.
#[tokio::test(flavor = "current_thread")]
async fn relocate_falls_back_when_candidate_became_observer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(1);
            // sec-0 IS a confirmed mesh peer (has_peer true) but joined as
            // an observer, so it is in role_table().observers.
            coordinator
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".into(),
                    is_observer: true,
                    can_be_primary: false,
                });
            assert!(
                coordinator
                    .cluster_state_for_test()
                    .role_table()
                    .observers
                    .contains("sec-0"),
                "fixture precondition: sec-0 is an observer"
            );

            let outcome = coordinator
                .relocate_primary_to(PeerId::from("sec-0"))
                .await
                .expect("relocate succeeds via fallback");
            assert_eq!(
                outcome,
                RelocationOutcome::FellBackToLocal,
                "a candidate in role_table().observers must fall back to local primary"
            );
            assert_eq!(
                coordinator.primary_id,
                Some("primary".to_string()),
                "the observer-cut fallback must activate the submitter as local primary"
            );
        })
        .await;
}

/// The observer tail returns `Ok(())` the instant
/// `cluster_state.run_complete()` is true — checked at the TOP of the
/// loop, so a `RunComplete` already applied (e.g. during the hand-off
/// window) returns immediately without blocking on a recv.
#[tokio::test(flavor = "current_thread")]
async fn observer_returns_on_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(1);
            // The authoritative primary declared the run over; the
            // submitter has already applied it.
            coordinator
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::RunComplete);

            coordinator
                .run_as_observer()
                .await
                .expect("observer tail returns Ok on run_complete");
        })
        .await;
}

/// The observer tail's dead-fleet grace (RESIDUAL-RISK-#3): with zero
/// confirmed peers (`peer_count() == 0`) and no `RunComplete`, the
/// observer exits with a stranded-fleet error after `fleet_dead_timeout`,
/// rather than hanging forever. Gated purely on the role-blind peer count,
/// never on a pool read.
#[tokio::test(flavor = "current_thread")]
async fn observer_exits_on_dead_fleet() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Zero peers → peer_count() == 0; fleet_dead_timeout is 50ms.
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(0);

            let result = coordinator.run_as_observer().await;
            assert!(
                result.is_err(),
                "an observer whose fleet is entirely gone with no RunComplete \
                 must exit on the fleet-dead grace, not hang"
            );
            assert!(
                result.unwrap_err().contains("fleet-dead"),
                "the dead-fleet exit must surface a fleet-dead error"
            );
        })
        .await;
}

/// E4b plumbing: in the relinquished case the result getters the PyO3
/// boundary reads (`completed` / `failed`) come from the replicated
/// ledger (`cluster_state.outcome_counts()`), NOT the now-empty local
/// pool / per-node sets. Seed terminal task states directly into the
/// replicated `cluster_state` (the only source an observer has — its pool
/// is empty) and assert the counts come back off the ledger.
#[tokio::test(flavor = "current_thread")]
async fn relinquished_result_getters_read_replicated_ledger() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(1);

            // The observer's local pool / completed_tasks / failed_tasks
            // sets are empty (it relinquished authority and never
            // dispatched). The replicated ledger is the source of truth.
            assert!(
                coordinator.completed_tasks.is_empty() && coordinator.failed_tasks.is_empty(),
                "fixture precondition: the relinquished node's local sets are empty"
            );

            // Two succeeded, one failed — replicated into cluster_state
            // exactly as the chosen primary's broadcasts would feed an
            // observer's mirror.
            let ok_a = make_binary("ok-a", 10);
            let ok_b = make_binary("ok-b", 20);
            let bad = make_binary("bad", 30);
            for b in [&ok_a, &ok_b, &bad] {
                coordinator
                    .cluster_state_mut_for_test()
                    .apply(ClusterMutation::TaskAdded {
                        hash: compute_task_hash(b),
                        task: b.clone(),
                    });
            }
            for b in [&ok_a, &ok_b] {
                coordinator
                    .cluster_state_mut_for_test()
                    .apply(ClusterMutation::TaskCompleted {
                        hash: compute_task_hash(b),
                        result_data: None,
                    });
            }
            coordinator
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TaskFailed {
                    hash: compute_task_hash(&bad),
                    kind: ErrorType::NonRecoverable,
                    error: "boom".into(),
                });

            // The getters the PyO3 boundary reads (run.rs:497-498) route
            // through `cluster_state.outcome_counts()`, so they report the
            // replicated tally even though the local sets are empty.
            assert_eq!(
                coordinator.completed_count(),
                2,
                "completed_count must read the replicated ledger, not the empty local set"
            );
            assert_eq!(
                coordinator.failed_count(),
                1,
                "failed_count must read the replicated ledger, not the empty local set"
            );
        })
        .await;
}

/// THE BUG (asm-dataset-nix, on-cluster): the relocated primary dies
/// abnormally (SIGTERM / exit 137 / dropped mesh-send handle) with NO
/// clean `RunComplete`. The dead connection stays RESIDENT in the
/// transport's `outgoing` table (the apply-only observer never sends, so
/// the send-failure prune never runs), so `peer_count() > 0` and the
/// dead-fleet arm never arms; `recv_peer()` blocks forever on an inbox
/// that will never deliver. Pre-fix the observer hung here. The
/// primary-silence backstop must terminate it: with a primary NAMED, the
/// connection resident (`peer_count() > 0`), inbound silent, and no
/// `RunComplete`, the observer exits `Err` within `peer_timeout` rather
/// than hanging.
#[tokio::test(flavor = "current_thread")]
async fn observer_exits_on_silent_primary_with_resident_peer() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // One resident peer → peer_count() == 1 (NOT zero): the
                // dead-fleet grace can NEVER arm. peer_timeout is short so
                // the silence backstop fires fast.
                let (mut coordinator, _ends) = observer_with_short_timeouts(
                    1,
                    std::time::Duration::from_millis(80),
                );
                assert!(
                    coordinator.transport_mut_for_test().peer_count() > 0,
                    "fixture precondition: the dead primary's connection stays \
                     resident, so peer_count() > 0 and the dead-fleet arm cannot fire"
                );

                // A primary IS named (the relocated primary, now dead).
                // The observer applied this PrimaryChanged at hand-off.
                coordinator
                    .cluster_state_mut_for_test()
                    .apply(ClusterMutation::PrimaryChanged {
                        new: "sec-0".into(),
                        epoch: 1,
                        reason:
                            dynrunner_protocol_primary_secondary::PrimaryChangeReason::Transferred,
                    });
                assert_eq!(
                    coordinator.cluster_state_for_test().current_primary(),
                    Some("sec-0"),
                    "fixture precondition: a primary is named"
                );

                // No RunComplete is ever applied; no frame is ever fed to
                // the inbound (recv_peer stays pending forever). The only
                // exit is the primary-silence backstop.
                let result = coordinator.run_as_observer().await;
                assert!(
                    result.is_err(),
                    "an observer whose named primary went silent (resident peer, \
                     no RunComplete) must exit on the primary-silence backstop, not hang"
                );
                let err = result.unwrap_err();
                assert!(
                    err.contains("stranded") && err.contains("sec-0"),
                    "the silence exit must name the silent primary and say stranded: {err}"
                );
            })
            .await;
    })
    .await
    .expect("the observer must terminate via the silence backstop, NOT hang");
}

/// The operator's run narration is emitted by the OBSERVER process
/// reading the CRDT: `run_as_observer` itself produces the
/// IMPORTANT_TARGET narrative (phase started/complete transitions plus a
/// single one-shot completion summary) from the replicated `ClusterState`
/// before it returns on `run_complete()`. The narrative is driven purely
/// from the CRDT mirror, so it is independent of which node holds the
/// primary — exactly the property that matters after a relocation moves
/// the primary to a different process.
///
/// The CRDT is pre-driven through a two-phase chain (`build` → `compile`)
/// with mixed outcomes (2 succeeded, 1 failed-final) and `RunComplete`
/// already applied, exactly the shape an observer's mirror holds when the
/// relocated primary's broadcasts have all landed. With `run_complete()`
/// true the loop emits the full narrative on its first iteration and then
/// returns Ok.
#[tokio::test(flavor = "current_thread")]
async fn run_as_observer_narrates_phases_and_one_completion_summary() {
    use crate::test_capture::{ImportantCapture, important_only};
    use dynrunner_core::PhaseId;
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    /// A `make_binary` re-tagged with an explicit phase + task_id.
    fn phased(phase: &str, id: &str) -> TaskInfo<TestId> {
        let mut t = make_binary(id, 100);
        t.phase_id = PhaseId::from(phase);
        t.task_id = id.to_string();
        t
    }

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut coordinator, _ends) = coordinator_with_confirmed_peers(1);

            // Two-phase chain: compile depends on build. build has one
            // (completed) task; compile has two (one completed, one
            // failed-final). All terminal ⇒ both phases dispatchable AND
            // complete; mixed outcomes ⇒ succeeded=2, fail_final=1.
            let toolchain = phased("build", "toolchain");
            let ok = phased("compile", "ok");
            let bad = phased("compile", "bad");
            {
                let cs = coordinator.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PhaseDepsSet {
                    deps: HashMap::from([(
                        PhaseId::from("compile"),
                        vec![PhaseId::from("build")],
                    )]),
                });
                for b in [&toolchain, &ok, &bad] {
                    cs.apply(ClusterMutation::TaskAdded {
                        hash: compute_task_hash(b),
                        task: b.clone(),
                    });
                }
                for b in [&toolchain, &ok] {
                    cs.apply(ClusterMutation::TaskCompleted {
                        hash: compute_task_hash(b),
                        result_data: None,
                    });
                }
                cs.apply(ClusterMutation::TaskFailed {
                    hash: compute_task_hash(&bad),
                    kind: ErrorType::NonRecoverable,
                    error: "boom".into(),
                });
                // The authoritative primary declared the run over.
                cs.apply(ClusterMutation::RunComplete);
            }

            // Capture importance-target events, held across the `.await`
            // via `set_default` (current_thread + LocalSet keep the
            // observer loop on this thread). See
            // `phase_ordering.rs::connected_event_precedes_first_phase_start…`.
            let capture = ImportantCapture::default();
            let subscriber =
                Registry::default().with(capture.clone().with_filter(important_only()));
            let _guard = tracing::subscriber::set_default(subscriber);

            coordinator
                .run_as_observer()
                .await
                .expect("observer tail returns Ok on run_complete");

            let events = capture.events();

            // Both phases narrated as started AND complete.
            let started: std::collections::HashSet<&str> = events
                .iter()
                .filter(|e| e.message.contains("starting job phase"))
                .filter_map(|e| e.fields.get("phase").map(String::as_str))
                .collect();
            assert_eq!(
                started,
                std::collections::HashSet::from(["build", "compile"]),
                "the observer must narrate both phases starting: {events:?}"
            );
            let done: std::collections::HashSet<&str> = events
                .iter()
                .filter(|e| e.message.contains("phase complete"))
                .filter_map(|e| e.fields.get("phase").map(String::as_str))
                .collect();
            assert_eq!(
                done,
                std::collections::HashSet::from(["build", "compile"]),
                "the observer must narrate both phases completing: {events:?}"
            );

            // Exactly one completion summary, with the correct partition.
            let summary: Vec<_> = events
                .iter()
                .filter(|e| e.message.contains("run complete"))
                .collect();
            assert_eq!(
                summary.len(),
                1,
                "exactly one run-complete summary from the observer: {events:?}"
            );
            assert_eq!(
                summary[0].fields.get("succeeded").map(String::as_str),
                Some("2"),
                "summary succeeded count: {events:?}"
            );
            assert_eq!(
                summary[0].fields.get("fail_final").map(String::as_str),
                Some("1"),
                "summary fail_final count: {events:?}"
            );
        })
        .await;
}

/// A LEGITIMATE FAILOVER must NOT trip the silence backstop. The
/// relocated primary dies, a surviving secondary re-elects (a
/// `PrimaryChanged` to the new primary), and the new primary emits
/// `Primary` keepalives that refresh `primary_last_seen`; the observer
/// rides through and exits `Ok` on the new primary's eventual
/// `RunComplete`. Feeds the refresh + RunComplete over the real inbound
/// so the recv-arm's keepalive-surfacing + mutation-apply path is what is
/// exercised (not a direct cluster_state poke).
#[tokio::test(flavor = "current_thread")]
async fn observer_rides_through_failover_and_exits_on_run_complete() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // peer_timeout 120ms; the refresh + RunComplete land well
                // inside it, and the backstop would otherwise fire by then
                // — so a green Ok proves the refresh actually reset the
                // clock (a non-default value: not the 300s default).
                let (mut coordinator, ends) = observer_with_short_timeouts(
                    1,
                    std::time::Duration::from_millis(120),
                );
                // The original relocated primary, now dead.
                coordinator
                    .cluster_state_mut_for_test()
                    .apply(ClusterMutation::PrimaryChanged {
                        new: "sec-0".into(),
                        epoch: 1,
                        reason:
                            dynrunner_protocol_primary_secondary::PrimaryChangeReason::Transferred,
                    });

                // The shared inbound sender every secondary end carries
                // (setup_test feeds all ends' `incoming_tx` into the
                // transport's single `incoming_rx`). The new primary's
                // failover signals ride this into recv_peer.
                let inbound = ends[0].2.clone();

                // Drive the failover sequence from a spawned task so the
                // observer loop is concurrently running its recv: (1) a
                // `PrimaryChanged` re-electing sec-1 as the new primary —
                // applied via the recv arm, refreshes primary_last_seen;
                // (2) a `Primary` keepalive from sec-1 — surfaced + refreshes
                // again; (3) `RunComplete` — the happy-path exit.
                tokio::task::spawn_local(async move {
                    // Let the observer enter its loop and (had we not fired
                    // failover) approach the silence deadline.
                    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::PrimaryChanged {
                                new: "sec-1".into(),
                                epoch: 2,
                                reason:
                                    dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
                            }],
                        })
                        .expect("inbound open");
                    inbound
                        .send(DistributedMessage::Keepalive {
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            secondary_id: "sec-1".into(),
                            active_workers: 0,
                            emitter_role: KeepaliveRole::Primary,
                        })
                        .expect("inbound open");
                    // Past the ORIGINAL 120ms deadline: if the refresh did
                    // not reset the clock the observer would already have
                    // exited Err; reaching RunComplete proves it rode
                    // through.
                    tokio::time::sleep(std::time::Duration::from_millis(90)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                coordinator
                    .run_as_observer()
                    .await
                    .expect("a legitimate failover must NOT trip the silence backstop; \
                             the observer rides through and exits Ok on RunComplete");
                assert_eq!(
                    coordinator.cluster_state_for_test().current_primary(),
                    Some("sec-1"),
                    "the failover re-elected sec-1 as the new primary"
                );
            })
            .await;
    })
    .await
    .expect("the failover-ride-through test must terminate");
}
