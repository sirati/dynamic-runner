//! Behaviour tests for the standalone [`ObserverCoordinator`].
//!
//! These re-create the observer-behaviour contract Wave 0 removed from
//! `relocate_observe.rs` / `crdt_convergence.rs`, now targeting the
//! standalone coordinator: run-complete / run-aborted / panik exits, the
//! three strand backstops (fleet-dead, primary-silence, setup-promote
//! deadline), CRDT narration, snapshot recovery, and the BUG-1/4/5/7
//! fixes. Each test builds the observer via [`ObserverCoordinator::new`]
//! over a real [`ChannelPeerTransport`] (or a minimal feed), drives the
//! single `run()` loop, and asserts on its terminal / `Err` / emitted
//! narration. The relocation/handoff e2e is a LATER wave's concern.

use std::collections::HashMap;
use std::time::Duration;

use dynrunner_core::{ErrorType, PhaseId, TaskInfo, TypeId};
use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
use dynrunner_protocol_primary_secondary::{
    DistributedMessage, KeepaliveRole, PrimaryChangeReason,
};
use dynrunner_transport_channel::ChannelPeerTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::{ObserverConfig, ObserverCoordinator, ObserverTerminal};
use crate::cluster_state::ClusterState;

/// Minimal serializable identifier for the observer tests.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId(String);

/// A `TaskInfo` in `phase` with id `id` and the given fully-qualified
/// `(dep_phase, dep_task_id)` prerequisites.
fn task(phase: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<TestId> {
    TaskInfo {
        path: std::path::PathBuf::from(format!("/tmp/{id}")),
        size: 1,
        identifier: TestId(id.to_string()),
        phase_id: PhaseId::from(phase),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: id.to_string(),
        task_depends_on: deps
            .iter()
            .map(|(dp, dt)| dynrunner_core::TaskDep {
                task_id: (*dt).to_string(),
                phase_id: PhaseId::from(*dp),
                inherit_outputs: false,
            })
            .collect(),
        preferred_secondaries: Default::default(),
        resolved_path: None,
    }
}

fn add(state: &mut ClusterState<TestId>, t: &TaskInfo<TestId>) {
    state.apply(ClusterMutation::TaskAdded {
        hash: t.task_id.clone(),
        task: t.clone(),
    });
}

fn complete(state: &mut ClusterState<TestId>, hash: &str) {
    state.apply(ClusterMutation::TaskCompleted {
        hash: hash.to_string(),
        result_data: None,
    });
}

/// Default observer config with short backstop windows so wall-clock
/// tests finish quickly. `fleet_dead_timeout` doubles as the loop's
/// re-check cadence, so it is kept small to drive the silence backstop too.
fn observer_config(node_id: &str) -> ObserverConfig {
    ObserverConfig {
        node_id: node_id.to_string(),
        fleet_dead_timeout: Duration::from_millis(50),
        peer_timeout: Duration::from_secs(300),
        setup_promote_deadline: Duration::from_secs(600),
        required_setup_on_promote: false,
        panik_watcher_paths: Vec::new(),
        panik_watcher_poll_interval: Duration::from_secs(60),
    }
}

/// Live peer receivers that must be kept alive for the duration of a test:
/// the router PRUNES a peer outbox whose receiver was dropped (a closed
/// channel) on the first failed send, which would silently drop
/// `peer_count()` to zero and trip the fleet-dead grace. Tests bind this so
/// the dummy peers stay "connected" for as long as the observer runs.
type PeerKeepalive = Vec<mpsc::UnboundedReceiver<DistributedMessage<TestId>>>;

/// Build a `ChannelPeerTransport` with `peer_count` dummy peer outboxes
/// and an inbound receiver fed by the returned sender. The peers are keyed
/// `peer-{i}` (never `"primary"`), so `Destination::Primary` is
/// unrouteable unless the test also wires a primary-keyed outbox. The
/// returned [`PeerKeepalive`] MUST be held by the test (see its doc) so the
/// dummy peers are not pruned on a failed send. `recv_peer` pends until the
/// returned `inbound_tx` is fed.
fn transport_with_peers(
    node_id: &str,
    peer_count: usize,
) -> (
    ChannelPeerTransport<TestId>,
    mpsc::UnboundedSender<DistributedMessage<TestId>>,
    PeerKeepalive,
) {
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let mut outgoing = HashMap::new();
    let mut keepalive = Vec::new();
    for i in 0..peer_count {
        let (peer_tx, peer_rx) = mpsc::unbounded_channel();
        outgoing.insert(format!("peer-{i}"), peer_tx);
        keepalive.push(peer_rx);
    }
    let transport = ChannelPeerTransport::from_raw_channels(node_id.into(), outgoing, inbound_rx);
    (transport, inbound_tx, keepalive)
}

/// Run a closure with an `ImportantCapture` installed as the default
/// subscriber (held across the await on a current-thread + LocalSet
/// runtime), returning the captured events.
fn capture_events() -> crate::test_capture::ImportantCapture {
    crate::test_capture::ImportantCapture::default()
}

/// `run_complete` already applied ⇒ the observer returns `Done` (exit 0)
/// immediately, without arming any backstop.
#[tokio::test(flavor = "current_thread")]
async fn observer_returns_on_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            cs.apply(ClusterMutation::RunComplete);
            let mut observer = ObserverCoordinator::new(transport, cs, observer_config("obs"));
            let terminal = observer.run().await.expect("Ok on run_complete");
            assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
        })
        .await;
}

/// Fleet-dead grace (§1): zero peers + no RunComplete ⇒ exit `Err`
/// (fleet-dead) after `fleet_dead_timeout`, never hang.
#[tokio::test(flavor = "current_thread")]
async fn observer_exits_on_dead_fleet() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (transport, _inbound, _peers) = transport_with_peers("obs", 0);
                let cs = ClusterState::<TestId>::new();
                let mut observer =
                    ObserverCoordinator::new(transport, cs, observer_config("obs"));
                let err = observer
                    .run()
                    .await
                    .expect_err("dead fleet with no RunComplete must exit Err");
                assert!(
                    err.to_string().contains("fleet-dead"),
                    "dead-fleet exit must surface a fleet-dead error: {err}"
                );
            })
            .await;
    })
    .await
    .expect("the dead-fleet observer must terminate, not hang");
}

/// Primary-silence backstop (§2): a NAMED primary goes silent (resident
/// peer, no RunComplete) ⇒ exit `Err` (stranded) within `peer_timeout`,
/// never hang. peer_count == 1 so the fleet-dead arm can never fire.
#[tokio::test(flavor = "current_thread")]
async fn observer_exits_on_silent_primary_with_resident_peer() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });
                let mut config = observer_config("obs");
                // Short silence threshold; fleet-dead cadence (= re-check
                // tick) smaller still so the loop re-evaluates the silence
                // backstop before peer_timeout's deadline.
                config.peer_timeout = Duration::from_millis(80);
                config.fleet_dead_timeout = Duration::from_millis(30);
                let mut observer = ObserverCoordinator::new(transport, cs, config);
                let err = observer
                    .run()
                    .await
                    .expect_err("silent named primary must exit Err");
                let msg = err.to_string();
                assert!(
                    msg.contains("stranded") && msg.contains("sec-0"),
                    "the silence exit must name the silent primary and say stranded: {msg}"
                );
            })
            .await;
    })
    .await
    .expect("the silent-primary observer must terminate via the backstop, not hang");
}

/// The observer narrates phases + exactly one completion summary from the
/// CRDT (items 9/14). Two-phase chain (build → compile), mixed outcomes
/// (2 succeeded, 1 failed-final), RunComplete applied ⇒ both phases
/// narrated started + complete and one run-complete summary.
#[tokio::test(flavor = "current_thread")]
async fn observer_narrates_phases_and_one_completion_summary() {
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            cs.apply(ClusterMutation::PhaseDepsSet {
                deps: HashMap::from([(PhaseId::from("compile"), vec![PhaseId::from("build")])]),
            });
            let toolchain = task("build", "toolchain", &[]);
            let ok = task("compile", "ok", &[]);
            let bad = task("compile", "bad", &[]);
            for b in [&toolchain, &ok, &bad] {
                add(&mut cs, b);
            }
            complete(&mut cs, "toolchain");
            complete(&mut cs, "ok");
            cs.apply(ClusterMutation::TaskFailed {
                hash: "bad".to_string(),
                kind: ErrorType::NonRecoverable,
                error: "boom".into(),
            });
            cs.apply(ClusterMutation::RunComplete);

            let capture = capture_events();
            let subscriber = Registry::default()
                .with(capture.clone().with_filter(crate::test_capture::important_only()));
            let _guard = tracing::subscriber::set_default(subscriber);

            let mut observer = ObserverCoordinator::new(transport, cs, observer_config("obs"));
            observer.run().await.expect("Ok on run_complete");

            let events = capture.events();
            let started: std::collections::HashSet<&str> = events
                .iter()
                .filter(|e| e.message.contains("starting job phase"))
                .filter_map(|e| e.fields.get("phase").map(String::as_str))
                .collect();
            assert_eq!(
                started,
                std::collections::HashSet::from(["build", "compile"]),
                "both phases must narrate started: {events:?}"
            );
            let done: std::collections::HashSet<&str> = events
                .iter()
                .filter(|e| e.message.contains("phase complete"))
                .filter_map(|e| e.fields.get("phase").map(String::as_str))
                .collect();
            assert_eq!(
                done,
                std::collections::HashSet::from(["build", "compile"]),
                "both phases must narrate complete: {events:?}"
            );
            let summary: Vec<_> = events
                .iter()
                .filter(|e| e.message.contains("run complete"))
                .collect();
            assert_eq!(summary.len(), 1, "exactly one completion summary: {events:?}");
            assert_eq!(
                summary[0].fields.get("succeeded").map(String::as_str),
                Some("2")
            );
            assert_eq!(
                summary[0].fields.get("fail_final").map(String::as_str),
                Some("1")
            );
        })
        .await;
}

/// A legitimate failover must NOT trip the silence backstop: the relocated
/// primary dies, a surviving secondary re-elects (PrimaryChanged), the new
/// primary emits a `Primary` keepalive (refreshes `primary_last_seen`),
/// and the observer rides through, exiting `Ok` on the new primary's
/// RunComplete. Drives the refresh + RunComplete over the real inbound so
/// the recv-arm's liveness + apply path is exercised.
#[tokio::test(flavor = "current_thread")]
async fn observer_rides_through_failover_and_exits_on_run_complete() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (transport, inbound, _peers) = transport_with_peers("obs", 1);
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "sec-0".into(),
                    epoch: 1,
                    reason: PrimaryChangeReason::Transferred,
                });
                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_millis(120);
                // Re-check cadence well under peer_timeout so the backstop
                // WOULD fire by ~120ms if the refresh didn't reset it.
                config.fleet_dead_timeout = Duration::from_millis(40);
                let mut observer = ObserverCoordinator::new(transport, cs, config);

                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::PrimaryChanged {
                                new: "sec-1".into(),
                                epoch: 2,
                                reason: PrimaryChangeReason::Election,
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
                    // Past the ORIGINAL 120ms deadline: reaching RunComplete
                    // proves the refresh reset the clock.
                    tokio::time::sleep(Duration::from_millis(90)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "sec-1".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("a legitimate failover must NOT trip the silence backstop");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                assert_eq!(
                    observer.cluster_state().current_primary(),
                    Some("sec-1"),
                    "the failover re-elected sec-1"
                );
            })
            .await;
    })
    .await
    .expect("the failover-ride-through observer must terminate");
}

/// Bootstrap recovery (§6 / item 2): an empty observer with a named
/// primary recovers from a `ClusterSnapshot` reply fed over the inbound,
/// restoring the completed-task count + the RunComplete latch ⇒ exit `Ok`.
#[tokio::test(flavor = "current_thread")]
async fn observer_recovers_from_snapshot_reply() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Donor snapshot: two completed tasks + RunComplete.
                let snapshot_json = {
                    let mut donor = ClusterState::<TestId>::new();
                    for name in ["t1", "t2"] {
                        let t = task("p", name, &[]);
                        add(&mut donor, &t);
                        complete(&mut donor, name);
                    }
                    donor.apply(ClusterMutation::RunComplete);
                    serde_json::to_string(&donor.snapshot()).expect("snapshot serializes")
                };

                // Observer transport: a `"promoted-sec"`-keyed outbox so the
                // recovery RPC's Destination::Primary resolves + sends, plus
                // an inbound we feed the ClusterSnapshot reply into.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, _to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });

                // Pre-feed the snapshot reply so the loop's recv arm picks
                // it up immediately on entry.
                inbound_tx
                    .send(DistributedMessage::ClusterSnapshot {
                        sender_id: "promoted-sec".into(),
                        timestamp: 0.0,
                        snapshot_json,
                    })
                    .unwrap();

                let mut observer = ObserverCoordinator::new(transport, cs, observer_config("obs"));
                assert_eq!(
                    observer.cluster_state().outcome_counts().succeeded,
                    0,
                    "pre-recovery the observer's ledger is empty"
                );
                let terminal = observer.run().await.expect("Ok after recovery");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                assert_eq!(
                    observer.cluster_state().outcome_counts().succeeded,
                    2,
                    "recovery must restore the completed-task count"
                );
            })
            .await;
    })
    .await
    .expect("the recovery observer must terminate");
}

/// Negative control (item 2): with NO snapshot reply + setup pending, the
/// observer still terminates — via the setup-promote-deadline backstop.
/// The recovery request is fire-and-forget; a missing reply cannot
/// deadlock. peer_count == 1 so fleet-dead never arms.
#[tokio::test(flavor = "current_thread")]
async fn observer_no_reply_still_terminates_via_deadline() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<DistributedMessage<TestId>>();
                let (to_primary_tx, _to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);
                // Hold inbound open but never feed it.
                let _inbound_tx = inbound_tx;

                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });

                let mut config = observer_config("obs");
                // Setup-defer + empty ledger ⇒ setup_pending() true.
                config.required_setup_on_promote = true;
                config.setup_promote_deadline = Duration::from_millis(150);
                // Far out so the deadline arm is the one that fires
                // (peer_count == 1 means fleet-dead never arms anyway).
                config.peer_timeout = Duration::from_secs(60);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let mut observer = ObserverCoordinator::new(transport, cs, config);
                let err = observer
                    .run()
                    .await
                    .expect_err("no reply + setup pending must exit via the deadline arm");
                assert!(
                    matches!(err, crate::primary::RunError::SetupDeadlineExpired { .. }),
                    "the deadline exit must be SetupDeadlineExpired: {err}"
                );
                assert!(
                    observer.setup_deadline_elapsed().is_some(),
                    "the elapsed must be recorded for the GIL-side tail"
                );
            })
            .await;
    })
    .await
    .expect("the no-reply observer must terminate via the deadline");
}

/// BUG-1: `run_aborted` ⇒ non-zero exit (Aborted terminal), checked
/// BEFORE `run_complete` so an aborted run never exits as completed.
#[tokio::test(flavor = "current_thread")]
async fn observer_run_aborted_exits_non_zero() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
            let mut cs = ClusterState::<TestId>::new();
            // Both aborted AND complete latched: aborted must win.
            cs.apply(ClusterMutation::RunAborted {
                reason: "duplicate task id in initial batch".into(),
            });
            cs.apply(ClusterMutation::RunComplete);
            let mut observer = ObserverCoordinator::new(transport, cs, observer_config("obs"));
            let terminal = observer.run().await.expect("aborted is a terminal, not an Err");
            match terminal {
                ObserverTerminal::Aborted { reason } => {
                    assert!(reason.contains("duplicate"), "carries the abort reason: {reason}");
                }
                other => panic!("aborted must win over complete: got {other:?}"),
            }
        })
        .await;
}

/// BUG-4: a working panik arm. A sentinel panik file triggers the watcher;
/// the run loop's panik arm consumes the signal and returns the Panik
/// terminal (→ exit 137 at the boundary). Built via the cold-join factory
/// so the real watcher is wired.
#[tokio::test(flavor = "current_thread")]
async fn observer_panik_arm_returns_panik_terminal() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let panik_path = tmp.path().join("observer.panik");
                let (transport, _inbound, _peers) = transport_with_peers("obs", 1);
                let mut config = observer_config("obs");
                config.panik_watcher_paths = vec![panik_path.clone()];
                config.panik_watcher_poll_interval = Duration::from_millis(20);
                // Keep the strand backstops far out so the panik arm is the
                // only exit path.
                config.fleet_dead_timeout = Duration::from_secs(60);
                config.peer_timeout = Duration::from_secs(60);

                let mut observer = super::build_cold_join_observer(
                    transport,
                    ClusterState::<TestId>::new(),
                    config,
                    Vec::new(),
                    std::collections::HashSet::new(),
                );

                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    std::fs::write(&panik_path, b"stop").unwrap();
                });

                let terminal = observer.run().await.expect("panik is a terminal, not an Err");
                match terminal {
                    ObserverTerminal::Panik { matched_path } => {
                        assert!(
                            matched_path.ends_with("observer.panik"),
                            "panik terminal carries the matched path: {matched_path:?}"
                        );
                    }
                    other => panic!("panik file must drive the Panik terminal: got {other:?}"),
                }
            })
            .await;
    })
    .await
    .expect("the panik observer must terminate via the panik arm");
}

/// BUG-5: `primary_last_seen` is refreshed when a restore-driven snapshot
/// re-points `current_primary`. A snapshot that newly names a primary is a
/// liveness assertion; without the refresh the silence backstop would fire
/// against a primary the observer only just learned of via snapshot.
///
/// Setup: peer_timeout short; the observer starts with NO named primary
/// (so the silence backstop is gated off initially). A snapshot naming
/// `promoted-sec` AND carrying RunComplete arrives just before the
/// peer_timeout window — the observer applies it, refreshes the clock, and
/// exits `Ok` on RunComplete rather than firing the silence backstop. The
/// proof is the green exit: had the restore NOT refreshed the clock, the
/// silence window measured from the (named) primary would already be blown
/// by the time the loop re-checks.
#[tokio::test(flavor = "current_thread")]
async fn observer_refreshes_primary_clock_on_restore_repoint() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let snapshot_json = {
                    let mut donor = ClusterState::<TestId>::new();
                    let t = task("p", "t1", &[]);
                    add(&mut donor, &t);
                    complete(&mut donor, "t1");
                    donor.apply(ClusterMutation::PrimaryChanged {
                        new: "promoted-sec".into(),
                        epoch: 3,
                        reason: PrimaryChangeReason::Election,
                    });
                    // NOT complete yet: we want the loop to keep running on
                    // the named primary so the silence backstop is the
                    // hazard the refresh must defuse, then a later
                    // RunComplete provides the clean exit.
                    serde_json::to_string(&donor.snapshot()).expect("snapshot serializes")
                };

                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, _to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                // Start with NO named primary so the silence backstop is
                // gated off until the restore re-points it.
                let cs = ClusterState::<TestId>::new();
                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_millis(80);
                config.fleet_dead_timeout = Duration::from_millis(30);

                let mut observer = ObserverCoordinator::new(transport, cs, config);

                tokio::task::spawn_local(async move {
                    // Land the re-pointing snapshot well into the run (past
                    // one re-check tick). It names promoted-sec → arms the
                    // silence backstop, and the BUG-5 refresh resets the
                    // clock to "now".
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    inbound_tx
                        .send(DistributedMessage::ClusterSnapshot {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            snapshot_json,
                        })
                        .expect("inbound open");
                    // Past the ORIGINAL 80ms window measured from t=0: if
                    // the restore had not refreshed the clock the backstop
                    // would already have fired Err. A later RunComplete
                    // gives the clean exit.
                    tokio::time::sleep(Duration::from_millis(70)).await;
                    inbound_tx
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("the restore-driven re-point must refresh the clock, not strand");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                assert_eq!(
                    observer.cluster_state().current_primary(),
                    Some("promoted-sec"),
                    "the snapshot re-pointed the primary"
                );
            })
            .await;
    })
    .await
    .expect("the restore-refresh observer must terminate");
}

/// R2/BUG-7: the setup-promote deadline uses a LIVE `setup_pending`. When a
/// `TaskAdded` seeds the ledger BEFORE the deadline, the arm goes inert and
/// the observer does NOT exit via the deadline — it rides on to RunComplete.
/// This pins that `setup_pending` is recomputed live (not frozen at entry).
#[tokio::test(flavor = "current_thread")]
async fn observer_setup_deadline_uses_live_setup_pending() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let (transport, inbound, _peers) = transport_with_peers("obs", 1);
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });
                let mut config = observer_config("obs");
                config.required_setup_on_promote = true;
                config.setup_promote_deadline = Duration::from_millis(150);
                config.peer_timeout = Duration::from_secs(60);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let mut observer = ObserverCoordinator::new(transport, cs, config);

                tokio::task::spawn_local(async move {
                    // Seed the ledger BEFORE the 150ms deadline ⇒ task_count
                    // > 0 ⇒ setup_pending() goes false ⇒ the deadline arm is
                    // inert. Then complete the run.
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    let t = task("p", "seed", &[]);
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::TaskAdded {
                                hash: t.task_id.clone(),
                                task: t,
                            }],
                        })
                        .expect("inbound open");
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::TaskCompleted {
                                hash: "seed".into(),
                                result_data: None,
                            }],
                        })
                        .expect("inbound open");
                    // Past the 150ms deadline window: if the gate were frozen
                    // the observer would already have exited Err.
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("a seeded ledger must make the deadline arm inert (live gate)");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                assert!(
                    observer.setup_deadline_elapsed().is_none(),
                    "the deadline must NOT have fired"
                );
            })
            .await;
    })
    .await
    .expect("the live-setup-pending observer must terminate");
}
