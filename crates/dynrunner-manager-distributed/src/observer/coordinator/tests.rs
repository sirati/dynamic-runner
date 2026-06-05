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
        preferred_version: Default::default(),
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
///
/// The narration is captured through the observer's OWN narration seam —
/// a [`RunNarrator`] driven synchronously over the converged ledger the
/// observer's `run()` exited on (its `cluster_state()`) — NOT by scraping
/// the process-global tracing dispatcher. The observer's `run()` loop
/// narrates by seeding `RunNarrator::with_started_phases(self.started_phases)`
/// (empty for `new()`) and calling `observe()` against `self.cluster_state`;
/// a single `observe()` over a pre-converged ledger reproduces exactly the
/// lines that loop emits. Capturing that synchronous drive — inside a
/// `with_default` closure with no `.await` between subscriber install and
/// emission, the proven non-flaky idiom from `run_narrator.rs` — makes the
/// importance assertion independent of the `tracing` per-callsite `Interest`
/// cache, which is process-global and concurrently re-poisoned by sibling
/// tests that install a `fmt::try_init` global subscriber (a thread-local
/// `set_default` held across `run().await` races that shared cache).
#[tokio::test(flavor = "current_thread")]
async fn observer_narrates_phases_and_one_completion_summary() {
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
                version: Default::default(),
            });
            cs.apply(ClusterMutation::RunComplete);

            // Drive the real observer to its terminal so the narration is
            // asserted over the ledger `run()` actually converged + exited on.
            let mut observer = ObserverCoordinator::new(transport, cs, observer_config("obs"));
            let terminal = observer.run().await.expect("Ok on run_complete");
            assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");

            // Re-derive the observer's narration synchronously over its
            // converged ledger, capturing through the narrator's own emit
            // path under a thread-local subscriber (no await between install
            // and emit → the per-callsite Interest is evaluated under THIS
            // subscriber, immune to the cross-test global cache poisoning).
            let events = crate::test_capture::capture_important(|| {
                crate::run_narrator::RunNarrator::new().observe(observer.cluster_state());
            });

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

/// The shared inbound + bookkeeping a [`build_test_handoff`] hands back.
struct HandoffTestRig {
    handoff: super::ObserverHandoff<ChannelPeerTransport<TestId>, TestId>,
    /// Feed mesh frames the observer's run loop applies.
    inbound: mpsc::UnboundedSender<DistributedMessage<TestId>>,
    /// Count of events the INHERITED (primary) dispatcher received. The
    /// `from_handoff` reconciliation REPLACES the inherited sender, so this
    /// must stay 0 for any event emitted after construction — proof the
    /// inherited dispatcher was superseded by the observer's own fresh one.
    inherited_event_count: std::rc::Rc<std::cell::Cell<usize>>,
    /// Held so the single dummy peer is not pruned for the test's lifetime.
    _peers: PeerKeepalive,
}

/// Build an [`super::ObserverHandoff`] over a moved-in transport +
/// cluster_state, mirroring what the relocated submitter's
/// `into_observer_handoff` produces: the cluster_state already carries an
/// installed task-completed sender (the inherited primary fabric), and two
/// dummy dispatcher tasks stand in for the inherited dispatcher handles. The
/// inherited task-completed dispatcher counts every event it receives into
/// `inherited_event_count`.
fn build_test_handoff(
    node_id: &str,
    cluster_state: ClusterState<TestId>,
    config: ObserverConfig,
) -> HandoffTestRig {
    let (transport, inbound, peers) = transport_with_peers(node_id, 1);

    // The inherited primary fabric: a task-completed channel already
    // installed on the moved-in cluster_state, with a dummy dispatcher
    // counting events on its receiver. `from_handoff` REPLACES this sender
    // with a fresh one (so this dispatcher is orphaned + receives nothing
    // further) and carries the handle only to abort it via single-teardown.
    let mut cluster_state = cluster_state;
    let (inherited_tx, mut inherited_rx) =
        mpsc::unbounded_channel::<crate::task_completed::TaskCompletedEvent>();
    cluster_state.install_task_completed_sender(inherited_tx);
    let inherited_event_count = std::rc::Rc::new(std::cell::Cell::new(0usize));
    let count_for_task = inherited_event_count.clone();
    let inherited_task_completed_dispatcher = tokio::task::spawn_local(async move {
        while inherited_rx.recv().await.is_some() {
            count_for_task.set(count_for_task.get() + 1);
        }
    });
    // A dummy peer-lifecycle dispatcher handle (no observer consumer; carried
    // only so single-teardown aborts it).
    let lifecycle_dispatcher_handle =
        tokio::task::spawn_local(async { std::future::pending::<()>().await });

    let handoff = super::ObserverHandoff {
        transport,
        cluster_state,
        node_id: node_id.to_string(),
        deadlines: config.clone(),
        started_phases: std::collections::HashSet::new(),
        required_setup_on_promote: config.required_setup_on_promote,
        panik_signal_rx: None,
        task_completed_dispatcher_handle: inherited_task_completed_dispatcher,
        lifecycle_dispatcher_handle,
        holdings: std::collections::HashSet::new(),
    };
    HandoffTestRig {
        handoff,
        inbound,
        inherited_event_count,
        _peers: peers,
    }
}

/// A relocation hands off transport + cluster_state BY VALUE: the observer
/// resumes over the moved-in mesh (peer set intact, no re-dial) and the
/// moved-in ledger, and exits cleanly on a `RunComplete` already present in
/// that ledger. Pins the core relocation mechanic at the observer seam +
/// proves the post-run accounting is re-sourced from the moved-in ledger.
#[tokio::test(flavor = "current_thread")]
async fn from_handoff_resumes_moved_in_state_and_exits_on_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The moved-in ledger already carries the run's terminal +
            // accounting (two completions) — exactly what the submitter's
            // converged cluster_state would hold at relocation.
            let mut cs = ClusterState::<TestId>::new();
            for id in ["a", "b"] {
                let t = task("p", id, &[]);
                add(&mut cs, &t);
                complete(&mut cs, id);
            }
            cs.apply(ClusterMutation::RunComplete);

            let rig = build_test_handoff("obs", cs, observer_config("obs"));
            let mut observer = ObserverCoordinator::from_handoff(rig.handoff);

            let terminal = observer.run().await.expect("Ok on the moved-in run_complete");
            assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");

            // Post-run accounting is re-sourced from the observer's moved-in
            // (converged) ledger — the surface the relocated `PrimaryRunOutcome`
            // reads after the submitter binding is consumed.
            assert_eq!(observer.completed_count(), 2, "completions off the moved-in ledger");
            assert_eq!(observer.failed_count(), 0);
            assert_eq!(observer.stranded_count(), 0, "an observer strands nothing");
        })
        .await;
}

/// `from_handoff` reconciliation: the observer installs a FRESH task-completed
/// channel on the moved-in cluster_state, REPLACING the inherited primary
/// sender. Proof: an inbound `TaskCompleted` applied AFTER `from_handoff`
/// (through the observer's run loop) routes to the FRESH sender — the inherited
/// (orphaned) primary dispatcher receives ZERO post-handoff events, even
/// though it was live and counting before the swap. (The observer's own fresh
/// dispatcher carries the Policy B/D listeners; the inherited handle is
/// carried only so single-teardown aborts it.)
#[tokio::test(flavor = "current_thread")]
async fn from_handoff_fresh_sender_supersedes_inherited_dispatcher() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut cs = ClusterState::<TestId>::new();
                let t = task("p", "x", &[]);
                add(&mut cs, &t);

                let rig = build_test_handoff("obs", cs, observer_config("obs"));
                let inherited_count = rig.inherited_event_count.clone();
                let inbound = rig.inbound.clone();
                let mut observer = ObserverCoordinator::from_handoff(rig.handoff);

                // Apply a TaskCompleted AFTER from_handoff (so it routes via
                // the FRESH sender), then complete the run. The observer's run
                // loop applies these inbound frames.
                tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "p".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::TaskCompleted {
                                hash: "x".into(),
                                result_data: None,
                            }],
                        })
                        .expect("inbound open");
                    inbound
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "p".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                });

                let terminal = observer.run().await.expect("Ok on RunComplete");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                // The completion was observed on the FRESH ledger.
                assert_eq!(observer.completed_count(), 1, "completion applied via fresh sender");
                // The INHERITED dispatcher received NOTHING after the swap —
                // its sender was replaced by the fresh one in from_handoff.
                assert_eq!(
                    inherited_count.get(),
                    0,
                    "the inherited primary dispatcher must be superseded by the \
                     observer's own fresh dispatcher (0 post-handoff events)"
                );
            })
            .await;
    })
    .await
    .expect("the fresh-sender observer must terminate (not hang)");
}

/// Announcer-ordering fix: `build_cold_join_observer` attaches the
/// resource-holdings announcer's role-change hook BEFORE it restores the
/// bootstrap snapshot, so the restore's `PrimaryChanged` apply fires the
/// hook into the registered channel and the INITIAL holdings announce is
/// NOT dropped. Pre-fix the announcer was attached only in `run`, AFTER
/// the factory's restore had already fired the (then-unregistered) hook,
/// so the first announce never went out.
///
/// Proof: a cold-join observer with non-empty `holdings`, restoring a
/// snapshot that names a primary (so the restore re-points
/// `current_primary` and fires the role-change hook), broadcasts exactly
/// the restore-driven `PeerResourceHoldingsUpdated` carrying those
/// holdings to `Destination::Primary` — captured on the primary-keyed
/// outbox — before the run completes.
#[tokio::test(flavor = "current_thread")]
async fn cold_join_announces_initial_holdings_after_restore() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Donor snapshot: names a primary (epoch 4) so the restore's
                // `primary_epoch > local` branch fires the role-change hook.
                // NOT complete — the loop must keep running so the announcer
                // task gets a turn to drain its trigger onto the outbox.
                let snapshot = {
                    let mut donor = ClusterState::<TestId>::new();
                    donor.apply(ClusterMutation::PrimaryChanged {
                        new: "promoted-sec".into(),
                        epoch: 4,
                        reason: PrimaryChangeReason::Election,
                    });
                    donor.snapshot()
                };

                // Observer transport: a `"promoted-sec"`-keyed outbox so the
                // announce's `Destination::Primary` resolves + sends, plus an
                // inbound we feed RunComplete into after capturing the announce.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, mut to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                let holdings: std::collections::HashSet<String> =
                    ["/nix/store/aaa".to_string(), "/nix/store/bbb".to_string()]
                        .into_iter()
                        .collect();

                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_secs(60);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let mut observer = super::build_cold_join_observer(
                    transport,
                    ClusterState::<TestId>::new(),
                    config,
                    vec![snapshot],
                    holdings,
                );

                // Drain frames to the primary until the restore-driven
                // holdings announce arrives, then complete the run. The
                // observer also fires a bootstrap `RequestClusterSnapshot` to
                // the named primary at loop entry (§6); skip it — only the
                // `PeerResourceHoldingsUpdated` announce is under test.
                tokio::task::spawn_local(async move {
                    loop {
                        let frame = to_primary_rx.recv().await.expect(
                            "the restore-driven initial announce must reach the primary outbox",
                        );
                        match frame {
                            DistributedMessage::RequestClusterSnapshot { .. } => continue,
                            DistributedMessage::ClusterMutation { mutations, .. } => {
                                assert_eq!(mutations.len(), 1, "one mutation per announce");
                                match &mutations[0] {
                                    ClusterMutation::PeerResourceHoldingsUpdated {
                                        peer_id,
                                        holdings,
                                        epoch,
                                    } => {
                                        assert_eq!(peer_id, "obs");
                                        assert_eq!(
                                            holdings,
                                            &vec![
                                                "/nix/store/aaa".to_string(),
                                                "/nix/store/bbb".to_string()
                                            ],
                                            "the initial announce carries the cold-join holdings"
                                        );
                                        assert_eq!(
                                            *epoch, 4,
                                            "the announce stamps the restored primary_epoch"
                                        );
                                    }
                                    other => panic!(
                                        "expected PeerResourceHoldingsUpdated; got {other:?}"
                                    ),
                                }
                                break;
                            }
                            other => panic!("unexpected frame to primary: got {other:?}"),
                        }
                    }
                    // Now finish the run.
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
                    .expect("Ok after the announce + RunComplete");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
            })
            .await;
    })
    .await
    .expect("the cold-join-announce observer must terminate");
}

/// D-C / D3 (§5.3): a steady-state WARN-DROPPED snapshot decode keeps the
/// anti-entropy LIVE — the AE-3 recovery cadence RE-PULLS a fresh snapshot
/// and the observer converges + exits `Done` within ≤ one cadence period.
///
/// ISOLATION of the TIMER-driven recovery (not the reactive digest arm):
/// the ahead digest arrives EXACTLY ONCE. The observer's reactive
/// `on_state_digest` issues the FIRST snapshot pull; the driver answers it
/// with a MALFORMED snapshot (WARN-dropped, not fatal). After that there is
/// NO further inbound digest, so the reactive arm is exhausted — the ONLY
/// thing that can re-pull is the timer-driven AE-3 recovery cadence, which
/// fires off the RECORDED peer digest. The driver answers that SECOND pull
/// with the GOOD snapshot. Without the recovery cadence this test HANGS
/// (no second pull ever comes) and trips the 5s timeout — that is the
/// fail-before / pass-after proof that the cadence (not the reactive arm)
/// drives convergence here.
#[tokio::test(flavor = "current_thread")]
async fn warn_dropped_decode_is_repulled_and_converges_via_recovery() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The good donor snapshot the recovery re-pull heals from:
                // one completed task + RunComplete.
                let good_snapshot_json = {
                    let mut donor = ClusterState::<TestId>::new();
                    let t = task("p", "t1", &[]);
                    add(&mut donor, &t);
                    complete(&mut donor, "t1");
                    donor.apply(ClusterMutation::RunComplete);
                    serde_json::to_string(&donor.snapshot()).expect("snapshot serializes")
                };
                // The (single) digest the named primary broadcasts — ahead of
                // the observer's empty ledger, so the observer is `is_behind`.
                let ahead_digest = {
                    let mut donor = ClusterState::<TestId>::new();
                    let t = task("p", "t1", &[]);
                    add(&mut donor, &t);
                    complete(&mut donor, "t1");
                    donor.digest()
                };

                // `promoted-sec`-keyed outbox so BOTH the reactive pull
                // (Destination::Secondary(promoted-sec)) and the recovery pull
                // resolve + send; we capture each RequestClusterSnapshot.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, mut to_primary_rx) = mpsc::unbounded_channel();
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

                let mut config = observer_config("obs");
                // Short peer_timeout bounds the recovery period small (the
                // recovery interval is tick_period.min(peer_timeout)); the
                // driver keeps the primary clock fresh so the SILENCE backstop
                // never fires within the test.
                config.peer_timeout = Duration::from_millis(50);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let mut observer = ObserverCoordinator::new(transport, cs, config);

                let inbound_for_driver = inbound_tx.clone();
                tokio::task::spawn_local(async move {
                    // Keep the primary-silence clock alive throughout (a
                    // Primary keepalive every 20ms < peer_timeout 50ms), so a
                    // strand never fires — ONLY the recovery path can heal.
                    let inbound_ka = inbound_for_driver.clone();
                    let keepalive_pump = tokio::task::spawn_local(async move {
                        loop {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            if inbound_ka
                                .send(DistributedMessage::Keepalive {
                                    sender_id: "promoted-sec".into(),
                                    timestamp: 0.0,
                                    secondary_id: "promoted-sec".into(),
                                    active_workers: 0,
                                    emitter_role: KeepaliveRole::Primary,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    });

                    // Feed the ahead digest EXACTLY ONCE — this is the only
                    // event that drives the reactive pull; the recorded digest
                    // is what the timer-driven recovery later re-pulls off.
                    inbound_for_driver
                        .send(DistributedMessage::StateDigest {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            digest: ahead_digest,
                        })
                        .expect("inbound open");

                    // Two pulls are NOT timer-driven: the at-entry bootstrap
                    // request (fired synchronously before the loop, to the
                    // named primary) and the ONE reactive pull the single
                    // inbound digest triggers. Answer BOTH of those with a
                    // MALFORMED snapshot (WARN-dropped — the observer stays
                    // behind). Since no further digest ever arrives, EVERY
                    // pull after those two can ONLY be the TIMER-driven AE-3
                    // recovery cadence (it re-pulls off the recorded digest);
                    // answer those with the GOOD snapshot → converges. This
                    // is the isolation: without the recovery cadence there is
                    // no third pull and the test hangs to its 5s timeout.
                    let mut non_timer_pulls_left = 2u8;
                    while let Some(frame) = to_primary_rx.recv().await {
                        if let DistributedMessage::RequestClusterSnapshot { .. } = frame {
                            let reply = if non_timer_pulls_left > 0 {
                                non_timer_pulls_left -= 1;
                                "{ this is not valid snapshot json".to_string()
                            } else {
                                good_snapshot_json.clone()
                            };
                            inbound_for_driver
                                .send(DistributedMessage::ClusterSnapshot {
                                    sender_id: "promoted-sec".into(),
                                    timestamp: 0.0,
                                    snapshot_json: reply,
                                })
                                .expect("inbound open");
                        }
                    }
                    keepalive_pump.abort();
                });

                let terminal = observer
                    .run()
                    .await
                    .expect("a WARN-dropped decode must NOT strand; recovery re-pulls + heals");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                assert_eq!(
                    observer.cluster_state().outcome_counts().succeeded,
                    1,
                    "the recovery re-pull restored the completed-task count"
                );
                drop(inbound_tx);
            })
            .await;
    })
    .await
    .expect("the WARN-drop-then-recovery observer must terminate");
}

/// C9 quiesce (§5.3): the recovery cadence does NOT pull when the observer
/// is converged with every known peer it has heard a digest from. Setup: a
/// known primary broadcasts a digest the observer is ALREADY converged with
/// (the observer's ledger matches it), so `plan_recovery_pull` returns
/// `None` every tick. Proof: across a window spanning multiple recovery
/// ticks, ZERO `RequestClusterSnapshot` frames are emitted by the recovery
/// cadence (only the at-entry bootstrap request, which we account for).
#[tokio::test(flavor = "current_thread")]
async fn recovery_cadence_quiesces_when_converged() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The observer and the primary share the SAME ledger shape, so
                // the digest the primary broadcasts is one the observer is NOT
                // behind — the C9 quiesce case.
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                let (to_primary_tx, mut to_primary_rx) = mpsc::unbounded_channel();
                let mut outgoing = HashMap::new();
                outgoing.insert("promoted-sec".to_string(), to_primary_tx);
                let transport =
                    ChannelPeerTransport::from_raw_channels("obs".into(), outgoing, inbound_rx);

                // Observer ledger: a named primary + one completed task.
                let mut cs = ClusterState::<TestId>::new();
                cs.apply(ClusterMutation::PrimaryChanged {
                    new: "promoted-sec".into(),
                    epoch: 2,
                    reason: PrimaryChangeReason::Election,
                });
                let t = task("p", "t1", &[]);
                add(&mut cs, &t);
                complete(&mut cs, "t1");
                // The converged digest the primary will echo back (identical
                // to the observer's own ⇒ not is_behind ⇒ quiesce).
                let converged_digest = cs.digest();

                let mut config = observer_config("obs");
                config.peer_timeout = Duration::from_millis(40);
                config.fleet_dead_timeout = Duration::from_secs(60);

                let mut observer = ObserverCoordinator::new(transport, cs, config);

                let inbound_for_driver = inbound_tx.clone();
                let recovery_requests = std::rc::Rc::new(std::cell::Cell::new(0usize));
                let recovery_requests_drv = recovery_requests.clone();
                tokio::task::spawn_local(async move {
                    // Keep the silence clock fresh so the silence backstop
                    // never fires; feed the converged digest so the observer
                    // records a known-peer digest it is NOT behind.
                    let inbound_ka = inbound_for_driver.clone();
                    let pump = tokio::task::spawn_local(async move {
                        loop {
                            tokio::time::sleep(Duration::from_millis(15)).await;
                            let _ = inbound_ka.send(DistributedMessage::Keepalive {
                                sender_id: "promoted-sec".into(),
                                timestamp: 0.0,
                                secondary_id: "promoted-sec".into(),
                                active_workers: 0,
                                emitter_role: KeepaliveRole::Primary,
                            });
                            let _ = inbound_ka.send(DistributedMessage::StateDigest {
                                sender_id: "promoted-sec".into(),
                                timestamp: 0.0,
                                digest: converged_digest,
                            });
                        }
                    });

                    // Count any RequestClusterSnapshot the recovery cadence
                    // emits over a multi-tick window. A drain task tallies
                    // them; with C9 quiesce there is only the at-entry
                    // bootstrap request (1), never a recovery re-pull.
                    let count_task = tokio::task::spawn_local(async move {
                        while let Some(frame) = to_primary_rx.recv().await {
                            if let DistributedMessage::RequestClusterSnapshot { .. } = frame {
                                recovery_requests_drv
                                    .set(recovery_requests_drv.get() + 1);
                            }
                        }
                    });

                    // Let several recovery ticks (period ≈ 40ms) elapse, then
                    // complete the run cleanly.
                    tokio::time::sleep(Duration::from_millis(180)).await;
                    pump.abort();
                    inbound_for_driver
                        .send(DistributedMessage::ClusterMutation {
                            sender_id: "promoted-sec".into(),
                            timestamp: 0.0,
                            mutations: vec![ClusterMutation::RunComplete],
                        })
                        .expect("inbound open");
                    // Give the drain a beat to observe any straggler frame.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    count_task.abort();
                });

                let terminal = observer.run().await.expect("Ok on RunComplete");
                assert!(matches!(terminal, ObserverTerminal::Done), "got {terminal:?}");
                // The ONLY snapshot request is the at-entry bootstrap one;
                // the converged recovery cadence emitted ZERO re-pulls.
                assert!(
                    recovery_requests.get() <= 1,
                    "a converged observer's recovery cadence must quiesce \
                     (≤1 request = the at-entry bootstrap only), got {}",
                    recovery_requests.get()
                );
                drop(inbound_tx);
            })
            .await;
    })
    .await
    .expect("the converged-quiesce observer must terminate");
}

/// L2a: a departed peer's AE-3 recovery digest is PRUNED from
/// `peer_digests` when its `PeerRemoved` mutation is applied, so the store
/// stays bounded by the live roster over the run's lifetime. A still-live
/// peer's entry is untouched.
#[tokio::test(flavor = "current_thread")]
async fn peer_digests_pruned_on_peer_removed() {
    use dynrunner_protocol_primary_secondary::RemovalCause;
    use dynrunner_protocol_primary_secondary::StateDigest;

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, inbound_tx, _keepalive) = transport_with_peers("obs", 1);
            let cs = ClusterState::<TestId>::new();
            let mut observer = ObserverCoordinator::new(transport, cs, observer_config("obs"));

            // Two recorded last-seen digests, one of which is about to depart.
            observer
                .peer_digests
                .insert("departing-sec".to_string(), StateDigest::default());
            observer
                .peer_digests
                .insert("live-sec".to_string(), StateDigest::default());

            let mut primary_last_seen = std::time::Instant::now();
            observer.on_cluster_mutation(
                vec![ClusterMutation::PeerRemoved {
                    id: "departing-sec".to_string(),
                    cause: RemovalCause::KeepaliveMiss,
                }],
                &mut primary_last_seen,
            );

            assert!(
                !observer.peer_digests.contains_key("departing-sec"),
                "the departed peer's recovery digest must be pruned"
            );
            assert!(
                observer.peer_digests.contains_key("live-sec"),
                "a still-live peer's recovery digest must be retained"
            );
            assert_eq!(
                observer.peer_digests.len(),
                1,
                "only the live peer's entry remains"
            );
            drop(inbound_tx);
        })
        .await;
}
