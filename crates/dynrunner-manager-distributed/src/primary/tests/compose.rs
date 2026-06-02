//! Tests for `PrimaryCoordinator::compose_with_local_secondary` — the
//! authoritative-primary stand-up over an mpsc loopback to a co-located
//! `SecondaryCoordinator` (Phase 2A).
//!
//! These prove the four composition hazards the plan flags as HIGHEST
//! risk, all deterministically (synchronous state/transport assertions
//! or a bounded, instant-fake-worker LocalSet flow — never an unbounded
//! real loop raced against a wall-clock timeout):
//!
//!   (a) `send_to(local_secondary_id, ..)` routes to the loopback
//!       secondary's inbound and does NOT echo back to the primary;
//!   (b) the local secondary's hydrated in-flight work is owned as
//!       remote-`InFlight` (`pre_owned_in_flight`), never double-counted
//!       as a local-active worker;
//!   (c) a co-located (non-promoted) secondary observes/applies but
//!       NEVER originates `RunComplete`;
//!   (d) compose → hydrate → dispatch one task: the hydrated pool item
//!       is dispatched to the local secondary over the loopback, with
//!       both a primary and a secondary coordinator coexisting on one
//!       `LocalSet` without deadlock.

use super::*;

use dynrunner_core::{PhaseId, ResourceKind, TypeId};
use dynrunner_protocol_primary_secondary::{MessageType, SecondaryTransport};

/// Local-node secondary id used as the loopback registration key across
/// these tests — the load-bearing detail of 2A: the co-located
/// secondary is addressed exactly like any remote one.
const LOCAL_SEC: &str = "local-sec";

/// Build a composed primary plus the secondary-side loopback endpoints.
/// Mirrors the pyo3 in-process wiring (`distributed/run.rs:279-357,554`)
/// reduced to a single loopback secondary: a `(pri→sec, sec→pri)` mpsc
/// pair, a `sec→pri` forwarder into the primary's single `incoming_rx`,
/// and the `pri→sec` writer registered under the node's own
/// secondary_id (done inside the constructor).
///
/// Returns:
///   * the composed `PrimaryCoordinator`;
///   * `pri_to_sec_rx` — the secondary side's inbound (what the
///     co-located `SecondaryCoordinator` would read from);
///   * `incoming_probe_rx` — a tap on the primary's aggregated inbound,
///     so a test can assert that a `send_to(LOCAL_SEC, ..)` did NOT echo
///     back to the primary.
///   * `sec_to_pri_tx` — the secondary→primary writer, so a test can
///     inject sec→pri frames (e.g. a `TaskRequest`) the forwarder
///     delivers to the primary.
#[allow(clippy::type_complexity)]
fn compose_primary() -> (
    PrimaryCoordinator<
        ChannelSecondaryTransportEnd<TestId>,
        NoPeers,
        ResourceStealingScheduler,
        FixedEstimator,
        TestId,
    >,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
    tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
) {
    // pri→sec: the primary writes here; the loopback secondary reads it.
    let (pri_to_sec_tx, pri_to_sec_rx) = tokio_mpsc::unbounded_channel();
    // sec→pri: the loopback secondary writes here; the forwarder feeds
    // the primary's `incoming_rx`.
    let (sec_to_pri_tx, sec_to_pri_rx) = tokio_mpsc::unbounded_channel();
    // The primary's aggregated inbound, plus a probe tap so the test can
    // assert "no echo to the primary" — anything that reaches the
    // primary's inbound also lands on the probe.
    let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
    let (probe_tx, probe_rx) = tokio_mpsc::unbounded_channel();

    // sec→pri forwarder (the caller's concern per the constructor's
    // boundary note). Tees each inbound frame to the probe so the test
    // observes the primary's true inbound stream.
    tokio::task::spawn_local(async move {
        let mut rx: tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>> =
            sec_to_pri_rx;
        while let Some(msg) = rx.recv().await {
            let _ = probe_tx.send(msg.clone());
            if incoming_tx.send(msg).is_err() {
                break;
            }
        }
    });

    let primary = PrimaryCoordinator::compose_with_local_secondary(
        PrimaryConfig::default(),
        LOCAL_SEC.to_string(),
        pri_to_sec_tx,
        incoming_rx,
        NoPeers,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );

    (primary, pri_to_sec_rx, probe_rx, sec_to_pri_tx)
}

fn dep_binary(name: &str, phase: &str) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t
}

/// (a) `transport.send_to(LOCAL_SEC, ..)` reaches the loopback
/// secondary's inbound and does NOT echo back to the primary's inbound.
#[tokio::test(flavor = "current_thread")]
async fn loopback_send_routes_to_secondary_inbound_without_echo() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut pri_to_sec_rx, mut probe_rx, _sec_to_pri_tx) =
                compose_primary();

            let assignment = DistributedMessage::TaskRequest {
                sender_id: "primary".into(),
                timestamp: 0.0,
                secondary_id: LOCAL_SEC.into(),
                worker_id: 0,
                available_resources: vec![],
            };

            primary
                .transport_mut_for_test()
                .send_to(LOCAL_SEC, assignment.clone())
                .await
                .expect("send_to local secondary id must succeed");

            // The loopback secondary's inbound received exactly the
            // frame the primary addressed to its id.
            let got = pri_to_sec_rx
                .try_recv()
                .expect("loopback secondary inbound must carry the routed frame");
            assert_eq!(got.msg_type(), MessageType::TaskRequest);

            // No echo: the primary's aggregated inbound (probe tap) saw
            // nothing — the loopback is unidirectional pri→sec.
            assert!(
                probe_rx.try_recv().is_err(),
                "send_to(local_sec) must NOT echo back to the primary's inbound"
            );
        })
        .await;
}

/// (b) A hydrated in-flight task owned by the local secondary is tracked
/// as remote-`InFlight` (in `pre_owned_in_flight`, phase counter == 1),
/// and is NOT double-counted as a local-active worker.
#[tokio::test(flavor = "current_thread")]
async fn local_inflight_not_double_counted_as_local_active() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, _pri_to_sec_rx, _probe_rx, _sec_to_pri_tx) =
                compose_primary();

            let task = dep_binary("inflight-local", "work");
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "inflight-local".into(),
                    task: task.clone(),
                });
                // In-flight on THIS node's own secondary (the loopback
                // secondary) — the case the double-count hazard targets.
                cs.apply(ClusterMutation::TaskAssigned {
                    hash: "inflight-local".into(),
                    secondary: LOCAL_SEC.into(),
                    worker: 0,
                });
            }

            primary.hydrate_from_cluster_state();

            let phase = PhaseId::from("work");
            // Owned as remote-in-flight by the primary ledger.
            assert_eq!(
                primary.pre_owned_in_flight_len_for_test(),
                1,
                "local secondary's in-flight task must be owned via pre_owned_in_flight"
            );
            assert_eq!(
                primary.pool().in_flight(&phase),
                1,
                "phase in-flight counter must read exactly 1"
            );
            // NOT re-offered as a queued/dispatchable pool item.
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "the in-flight task must not appear as a queued item"
            );
            // NOT double-counted as a local-active worker: the composed
            // primary registered no worker for it (it was inherited, not
            // dispatched here), so no `RemoteWorkerState` holds it.
            assert_eq!(
                primary.active_workers_for_test(),
                0,
                "the inherited in-flight task must not show up as a local-active worker"
            );
        })
        .await;
}

/// (c) A co-located, NON-promoted `SecondaryCoordinator` driven over the
/// loopback through one full task lifecycle observes and applies cluster
/// state but NEVER originates `RunComplete` — every secondary-side
/// `RunComplete` broadcast is gated behind `is_primary`, which the
/// loopback secondary never sets. Deterministic: the fake worker
/// completes the single task instantly and the secondary exits cleanly
/// when the loopback closes; bounded by a timeout that only trips on a
/// genuine hang.
///
/// This also exercises the "two coordinators on one `LocalSet`" hazard —
/// a composed `PrimaryCoordinator` object coexists with the live
/// secondary task on the same `LocalSet`.
#[tokio::test(flavor = "current_thread")]
async fn loopback_secondary_never_originates_run_complete() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let max_res = dynrunner_core::ResourceMap::from([(
                ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);

            // Real co-located secondary on this LocalSet, wired to the
            // loopback. `spawn_real_secondary` builds it with
            // `is_primary = false`; it stays a plain secondary.
            let (pri_to_sec_tx, mut sec_to_pri_rx, sec_handle) =
                spawn_real_secondary(LOCAL_SEC.to_string(), 1, max_res);

            // A composed primary object coexists on the same LocalSet
            // (proving two coordinators don't deadlock at construction /
            // hydration time). Its loopback is independent of the
            // hand-driven frames below; it simply must not interfere.
            let (_composed, _r1, _r2, _r3) = compose_primary();

            // Drive the secondary through the minimal setup handshake it
            // expects from a primary (PeerInfo + InitialAssignment +
            // TransferComplete), then hand it ONE task assignment and
            // close the loopback so it drains and exits.
            pri_to_sec_tx
                .send(DistributedMessage::PeerInfo {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    peers: vec![],
                })
                .unwrap();
            pri_to_sec_tx
                .send(DistributedMessage::InitialAssignment {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    secondary_id: LOCAL_SEC.into(),
                    zip_files: vec![],
                    workers_ready: vec![],
                    staged_files: vec![],
                    pre_staged_mode: false,
                    uses_file_based_items: false,
                })
                .unwrap();
            pri_to_sec_tx
                .send(DistributedMessage::TransferComplete {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    total_files: 0,
                    total_bytes: 0,
                })
                .unwrap();

            let task = dep_binary("c-task", "work");
            pri_to_sec_tx
                .send(DistributedMessage::TaskAssignment {
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    secondary_id: LOCAL_SEC.into(),
                    worker_id: 0,
                    zip_file: None,
                    binary_info:
                        crate::primary::wire::binary_to_distributed(&task),
                    local_path: task.path.to_string_lossy().into_owned(),
                    file_hash: "c-task".into(),
                    predecessor_outputs: Default::default(),
                })
                .unwrap();

            // Capture the secondary's sec→pri stream. We keep the
            // loopback sender alive until the secondary has reported its
            // one task complete (closing it earlier would break the
            // secondary's loop before the worker's async completion
            // event lands), then drop it so the secondary drains and
            // exits cleanly (single-secondary, no peer mesh → "primary
            // close = end of run"). Every step is bounded by a timeout
            // that only trips on a genuine hang; the fake worker
            // completes the single task instantly, so progress is
            // deterministic, not a wall-clock race.
            let mut saw_task_complete = false;
            let mut kept_sender = Some(pri_to_sec_tx);
            let collect = async {
                while let Some(msg) = sec_to_pri_rx.recv().await {
                    match msg.msg_type() {
                        MessageType::TaskComplete => {
                            saw_task_complete = true;
                            // Let the secondary finish: drop the loopback
                            // sender so its `primary_transport.recv()`
                            // returns None and it exits.
                            kept_sender = None;
                        }
                        MessageType::ClusterMutation => {
                            if let DistributedMessage::ClusterMutation {
                                mutations,
                                ..
                            } = &msg
                            {
                                assert!(
                                    !mutations.iter().any(|m| matches!(
                                        m,
                                        ClusterMutation::RunComplete
                                    )),
                                    "a non-promoted loopback secondary must NEVER \
                                     originate a RunComplete broadcast"
                                );
                            }
                        }
                        _ => {}
                    }
                }
            };
            tokio::time::timeout(Duration::from_secs(10), collect)
                .await
                .expect("loopback secondary must drain and exit, not hang");

            let completed = tokio::time::timeout(Duration::from_secs(10), sec_handle)
                .await
                .expect("secondary task must finish")
                .expect("secondary join must not panic");

            assert!(
                saw_task_complete,
                "the loopback secondary must report its one task complete"
            );
            assert_eq!(completed, 1, "the loopback secondary ran exactly one task");
        })
        .await;
}

/// (d) compose → hydrate → dispatch one task to the local secondary. A
/// single hydrated `Pending` pool item is dispatched by the primary's
/// real dispatch path (`dispatch_to_idle_workers`) to a worker owned by
/// the local secondary; the resulting `TaskAssignment` is routed over
/// the loopback (addressed to the node's own secondary_id) and is NOT
/// echoed back to the primary.
#[tokio::test(flavor = "current_thread")]
async fn compose_hydrate_dispatches_one_task_over_loopback() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut pri_to_sec_rx, mut probe_rx, _sec_to_pri_tx) =
                compose_primary();

            // One genuinely pending task in the replicated ledger.
            let task = dep_binary("d-task", "work");
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: "d-task".into(),
                    task: task.clone(),
                });
            }
            primary.hydrate_from_cluster_state();
            assert_eq!(primary.pool().iter().count(), 1, "one pending pool item");

            // Register one idle worker owned by the local secondary (the
            // composed primary skips the welcome/initial-assignment
            // handshake that would normally register it — that live
            // wiring is 2B's concern; here we seed it to drive dispatch).
            let max_res = dynrunner_core::ResourceMap::from([(
                ResourceKind::memory(),
                1024 * 1024 * 1024u64,
            )]);
            primary.register_idle_worker_for_test(LOCAL_SEC.to_string(), 0, max_res);

            // Drive the real dispatch path.
            primary
                .dispatch_to_idle_workers()
                .await
                .expect("dispatch must succeed");

            // The task assignment routed to the local secondary over the
            // loopback.
            let routed = pri_to_sec_rx
                .try_recv()
                .expect("a TaskAssignment must reach the loopback secondary");
            match routed {
                DistributedMessage::TaskAssignment {
                    secondary_id,
                    binary_info,
                    ..
                } => {
                    assert_eq!(secondary_id, LOCAL_SEC);
                    assert_eq!(
                        binary_info.task_id, "d-task",
                        "the dispatched assignment must carry the hydrated task"
                    );
                }
                other => panic!("expected TaskAssignment, got {:?}", other.msg_type()),
            }

            // No echo back to the primary's own inbound.
            assert!(
                probe_rx.try_recv().is_err(),
                "the dispatch must not echo back to the primary's inbound"
            );

            // The pool item is now in-flight (taken from the queue), not
            // re-offered.
            assert_eq!(
                primary.pool().iter().count(),
                0,
                "the dispatched task must leave the queued pool"
            );
            assert_eq!(
                primary.active_workers_for_test(),
                1,
                "the local worker now holds the dispatched task"
            );
        })
        .await;
}
