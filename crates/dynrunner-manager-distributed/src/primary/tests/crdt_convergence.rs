//! CRDT convergence-robustness coverage.
//!
//! Primary-side behaviors, all additive to the DELIVERY layer (the
//! apply/merge/snapshot algebra is untouched):
//!
//!   (a) `rebroadcast_full_roster` re-emits the FULL per-secondary roster
//!       post-mesh, healing a failover-promoted secondary that inherited
//!       an INCOMPLETE `secondary_capacities` mirror (so it rebuilds the
//!       full worker roster + correct `alive_remote_secondary_count`, no
//!       premature fleet-dead).
//!   (b) the primary ANSWERS `RequestClusterSnapshot` (unicasts a
//!       `ClusterSnapshot` of its complete ledger + originates the
//!       requester's `PeerJoined`).

use super::*;

use crate::primary::wire::compute_task_hash;
use crate::state::{SecondaryConnection, SecondaryConnectionState};
use dynrunner_protocol_primary_secondary::{MessageType, RemovalCause};

/// One advertised-memory resource amount (bytes), mirroring the live
/// welcome shape (a single `memory` `ResourceAmount`).
fn mem(bytes: u64) -> Vec<dynrunner_core::ResourceAmount> {
    vec![dynrunner_core::ResourceAmount {
        kind: dynrunner_core::ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Insert an Operational secondary into the primary's connection table
/// carrying the advertised `(worker_count, ram)` + capability flags —
/// the `self.secondaries` shape `rebroadcast_full_roster` iterates.
fn insert_operational_secondary(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    secondary_id: &str,
    worker_count: u32,
    ram_bytes: u64,
) {
    let conn = SecondaryConnection::new(secondary_id.into())
        .receive_welcome(
            worker_count,
            mem(ram_bytes),
            "host".into(),
            0,
            None,
            false,
            false,
        )
        .receive_cert_exchange(String::new(), None, None, 0)
        .begin_peer_discovery()
        .peers_ready()
        .assignments_sent();
    primary.secondaries.insert(
        secondary_id.into(),
        SecondaryConnectionState::Operational(conn),
    );
}

/// Drain the single `ClusterMutation` batch a primary's
/// `rebroadcast_full_roster` shipped to a secondary's inbox.
fn drain_first_cluster_mutation(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<ClusterMutation<TestId>> {
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::ClusterMutation {
            target: _,
            mutations,
            ..
        } = msg
        {
            return mutations;
        }
    }
    Vec::new()
}

/// (a) Mid-run failover with a partial roster. Two worker-secondaries
/// (sec-0, sec-1) connect to a complete primary; a promotion-bound sec-0
/// inherited an INCOMPLETE mirror (it missed sec-1's `SecondaryCapacity`
/// + `PeerJoined`). Pre-rebroadcast its reconstructed roster undercounts
/// (only sec-0's workers; `alive_remote_secondary_count` == 0). Running
/// the complete primary's `rebroadcast_full_roster` ships the full roster;
/// applying it to sec-0's mirror heals BOTH secondaries, so a promotion
/// reconstructs the FULL worker roster and the correct
/// `alive_remote_secondary_count` — no premature fleet-dead.
#[tokio::test(flavor = "current_thread")]
async fn rebroadcast_full_roster_heals_partial_promoted_mirror() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // The COMPLETE primary: both secondaries connected, both
            // capacity records present in its mirror (as `handle_welcome`
            // would have originated them). Capture its rebroadcast batch
            // over the `sec-0` wire.
            let (transport, mut ends) = setup_test(2);
            let mut sec0_inbox = ends.remove(0).1; // sec-0's primary→secondary rx
            let (mut complete, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            insert_operational_secondary(&mut complete, "sec-0", 2, 8 * 1024 * 1024 * 1024);
            insert_operational_secondary(&mut complete, "sec-1", 3, 8 * 1024 * 1024 * 1024);
            // The complete primary's own mirror already holds both records
            // (set-once apply), mirroring the post-welcome state.
            {
                let cs = complete.cluster_state_mut_for_test();
                for (id, n) in [("sec-0", 2u32), ("sec-1", 3u32)] {
                    cs.apply(ClusterMutation::PeerJoined {
                        peer_id: id.into(),
                        is_observer: false,
                        can_be_primary: false,
                        cap_version: Default::default(),
                    });
                    cs.apply(ClusterMutation::SecondaryCapacity {
                        secondary: id.into(),
                        worker_count: n,
                        resources: mem(8 * 1024 * 1024 * 1024),
                    });
                }
            }

            // Re-emit the full roster. This is a PURE re-emission — the
            // records are already in the primary's mirror, so it must NOT
            // route through the NoOp-filtering originator path (which would
            // drop the whole batch). Assert the wire batch carries BOTH
            // secondaries' records.
            complete.rebroadcast_full_roster().await;
            settle_pump().await;
            let batch = drain_first_cluster_mutation(&mut sec0_inbox);
            let cap_ids: std::collections::HashSet<&str> = batch
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::SecondaryCapacity { secondary, .. } => {
                        Some(secondary.as_str())
                    }
                    _ => None,
                })
                .collect();
            assert!(
                cap_ids.contains("sec-0") && cap_ids.contains("sec-1"),
                "rebroadcast must carry BOTH secondaries' capacity records, got {cap_ids:?}"
            );

            // The promotion-bound sec-0: an INCOMPLETE inherited mirror —
            // it has its OWN records but missed sec-1's. Model it as the
            // promoted primary's coordinator.
            let (t2, _e2) = setup_test(0);
            let (mut promoted, _mesh) = build_test_primary(
                test_primary_config(),
                t2,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            {
                let cs = promoted.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-0".into(),
                    is_observer: false,
                    can_be_primary: true,
                    cap_version: Default::default(),
                });
                cs.apply(ClusterMutation::SecondaryCapacity {
                    secondary: "sec-0".into(),
                    worker_count: 2,
                    resources: mem(8 * 1024 * 1024 * 1024),
                });
                // sec-1's records are DENIED — the desync.
            }

            // Pre-heal: the reconstructed worker roster undercounts (only
            // sec-0's 2 slots), and `alive_remote_secondary_count` is 0
            // (sec-1 not yet a known alive worker-secondary). A promotion
            // here would arm fleet-dead prematurely.
            promoted.reconstruct_workers_from_cluster_state();
            assert_eq!(
                promoted.alive_worker_count_for_test(),
                2,
                "pre-heal the partial mirror rebuilds only sec-0's slots"
            );
            assert_eq!(
                promoted
                    .cluster_state_for_test()
                    .alive_remote_secondary_count(),
                1,
                "pre-heal only sec-0 is known so the remote-secondary count undercounts \
                 (sec-1 missing)"
            );

            // Apply the rebroadcast batch the complete primary shipped —
            // the heal. The idempotent lattice absorbs sec-0's own
            // already-present records (NoOp) and adds sec-1's.
            promoted
                .handle_cluster_mutation(DistributedMessage::ClusterMutation {
                    target: None,
                    sender_id: "primary".into(),
                    timestamp: 0.0,
                    mutations: batch,
                })
                .await;

            // Post-heal: a fresh promotion reconstructs the FULL roster
            // (sec-0's 2 + sec-1's 3 = 5 slots) and the correct
            // `alive_remote_secondary_count` (both worker-secondaries are
            // now known + alive) — no premature fleet-dead.
            promoted.reconstruct_workers_from_cluster_state();
            assert_eq!(
                promoted.alive_worker_count_for_test(),
                5,
                "post-heal the full roster reconstructs ALL advertised slots"
            );
            assert_eq!(
                promoted
                    .cluster_state_for_test()
                    .alive_remote_secondary_count(),
                2,
                "post-heal both worker-secondaries are known + alive"
            );
        })
        .await;
}

/// (a') The `RosterReemit` Departed re-emit path. `rebroadcast_full_roster`
/// catches a reconnecting node's LIVENESS view up by re-emitting a
/// `PeerRemoved { cause: RosterReemit }` for every id the `capabilities`
/// 2P-set holds as `Departed` — iterating `departed_capability_ids()` (the
/// authoritative tombstone view), NOT `self.secondaries` (which already
/// dropped the departed peer). This pins that a Departed-tombstoned id is
/// re-emitted while live ids are NOT (live ids ride the `PeerJoined` /
/// `SecondaryCapacity` records, never a `PeerRemoved`).
#[tokio::test(flavor = "current_thread")]
async fn rebroadcast_full_roster_reemits_departed_tombstones() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let mut sec_inbox = ends.remove(0).1;
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // One LIVE secondary in the connection table (rides PeerJoined +
            // SecondaryCapacity, never a PeerRemoved).
            insert_operational_secondary(&mut primary, "sec-live", 2, 8 * 1024 * 1024 * 1024);

            // Seed the capabilities 2P-set with a Departed tombstone for a
            // peer that joined and then departed (the apply path that writes
            // the tombstone). `sec-live` also gets its membership record so
            // the live-vs-departed distinction is exercised end to end.
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-live".into(),
                    is_observer: false,
                    can_be_primary: false,
                    cap_version: Default::default(),
                });
                cs.apply(ClusterMutation::PeerJoined {
                    peer_id: "sec-gone".into(),
                    is_observer: false,
                    can_be_primary: false,
                    cap_version: Default::default(),
                });
                cs.apply(ClusterMutation::PeerRemoved {
                    id: "sec-gone".into(),
                    cause: RemovalCause::KeepaliveMiss,
                });
            }
            // Precondition: the 2P-set holds sec-gone as the ONLY Departed
            // tombstone (sec-live is still Advertised).
            let departed: std::collections::HashSet<&str> = primary
                .cluster_state_for_test()
                .departed_capability_ids()
                .collect();
            assert_eq!(
                departed,
                std::iter::once("sec-gone").collect(),
                "only sec-gone is a Departed tombstone"
            );

            primary.rebroadcast_full_roster().await;
            settle_pump().await;
            let batch = drain_first_cluster_mutation(&mut sec_inbox);

            // The batch re-emits a `PeerRemoved { cause: RosterReemit }` for
            // the Departed-tombstoned id, and for NO live id.
            let reemitted: std::collections::HashSet<&str> = batch
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::PeerRemoved {
                        id,
                        cause: RemovalCause::RosterReemit,
                    } => Some(id.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                reemitted,
                std::iter::once("sec-gone").collect(),
                "rebroadcast must re-emit a RosterReemit PeerRemoved for the Departed id only"
            );
            assert!(
                !reemitted.contains("sec-live"),
                "a live secondary must never be re-emitted as a PeerRemoved"
            );
            // The live secondary still rides its membership record (proves
            // the live roster path is untouched by the Departed re-emit).
            let joined: std::collections::HashSet<&str> = batch
                .iter()
                .filter_map(|m| match m {
                    ClusterMutation::PeerJoined { peer_id, .. } => Some(peer_id.as_str()),
                    _ => None,
                })
                .collect();
            assert!(
                joined.contains("sec-live"),
                "the live secondary must still ride a PeerJoined membership record"
            );
        })
        .await;
}

/// (c) The primary answers `RequestClusterSnapshot`: it unicasts a
/// `ClusterSnapshot` of its complete ledger back to the requester AND
/// originates the requester's `PeerJoined` (carrying its declared role +
/// capability). Pre-fix only the secondary router answered; a request
/// addressed at the primary fell through the catch-all and timed out.
#[tokio::test(flavor = "current_thread")]
async fn primary_answers_request_cluster_snapshot() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One existing secondary slot so the requester's unicast reply
            // + the broadcast PeerJoined have somewhere to land.
            let (transport, mut ends) = setup_test(1);
            // setup_test keys the single secondary as "sec-0"; we re-key
            // the requester onto that outbox by sending the request with
            // sender_id == "sec-0".
            let mut requester_inbox = ends.remove(0).1;
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed the primary's complete ledger: one task.
            let task = make_binary("task-x", 100);
            let hash = compute_task_hash(&task);
            {
                let cs = primary.cluster_state_mut_for_test();
                cs.apply(ClusterMutation::TaskAdded {
                    hash: hash.clone(),
                    task,
                });
            }

            // A late-joining WORKER (is_observer=false, can_be_primary=true)
            // requests a snapshot.
            primary
                .handle_request_cluster_snapshot(DistributedMessage::RequestClusterSnapshot {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 0.0,
                    is_observer: false,
                    can_be_primary: true,
                })
                .await;

            // The requester receives a unicast `ClusterSnapshot` whose
            // payload restores into the seeded ledger (the task survives).
            let mut got_snapshot = false;
            let mut got_peer_joined = false;
            settle_pump().await;
            while let Ok(msg) = requester_inbox.try_recv() {
                match msg.msg_type() {
                    MessageType::ClusterSnapshot => {
                        if let DistributedMessage::ClusterSnapshot {
                            target: _,
                            snapshot_json,
                            ..
                        } = msg
                        {
                            let snap: crate::cluster_state::ClusterStateSnapshot<TestId> =
                                serde_json::from_str(&snapshot_json).expect("snapshot decodes");
                            let mut restored = crate::cluster_state::ClusterState::<TestId>::new();
                            restored.restore(snap);
                            assert!(
                                restored.task_state(&hash).is_some(),
                                "the snapshot must carry the primary's seeded task"
                            );
                            got_snapshot = true;
                        }
                    }
                    MessageType::ClusterMutation => {
                        // The originated PeerJoined for the requester rides
                        // a broadcast ClusterMutation batch.
                        if let DistributedMessage::ClusterMutation {
                            target: _,
                            mutations,
                            ..
                        } = msg
                        {
                            got_peer_joined |= mutations.iter().any(|m| {
                                matches!(
                                    m,
                                    ClusterMutation::PeerJoined { peer_id, can_be_primary, .. }
                                        if peer_id == "sec-0" && *can_be_primary
                                )
                            });
                        }
                    }
                    _ => {}
                }
            }
            assert!(got_snapshot, "primary must unicast a ClusterSnapshot reply");
            assert!(
                got_peer_joined,
                "primary must originate the requester's PeerJoined (with its declared capability)"
            );
            // The requester's PeerJoined landed in the primary's own mirror
            // too (canonical apply-and-broadcast).
            assert!(
                primary.cluster_state_for_test().can_be_primary("sec-0"),
                "the requester's declared can_be_primary must be recorded"
            );
        })
        .await;
}

/// (d) The primary answers `RequestRunConfig` as a PURE responder: it
/// unicasts exactly ONE `RunConfig` carrying its node-local
/// `forwarded_argv` verbatim, and originates ZERO membership/authority
/// frames (no `PeerJoined` / `ClusterMutation`, no `SecondaryWelcome`) —
/// distinct from the snapshot responder above, which DOES originate a
/// `PeerJoined`. The replicated ledger + the connection (quorum) table are
/// left untouched: answering for the run-config is read-only peer gossip,
/// NOT primary authority (the work-split is preserved).
#[tokio::test(flavor = "current_thread")]
async fn primary_answers_request_run_config_purely() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // One existing secondary slot so the requester's unicast reply
            // has somewhere to land. Keyed "sec-0" by setup_test; the
            // request rides sender_id == "sec-0" so the reply re-keys onto it.
            let (transport, mut ends) = setup_test(1);
            let mut requester_inbox = ends.remove(0).1;
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Seed the node-local run-config directly (production seeding
            // from the pyo3 kwarg is B3; this test seeds the field itself).
            let seeded = vec![
                "--jobs".to_string(),
                "8".to_string(),
                "--log-oom-watcher".to_string(),
            ];
            primary.forwarded_argv = seeded.clone();

            // Fingerprints BEFORE: a pure responder leaves the replicated
            // ledger AND the quorum/connection table untouched.
            let digest_before = primary.cluster_state_for_test().digest();
            let quorum_before = primary.secondaries.len();

            primary
                .handle_request_run_config(DistributedMessage::RequestRunConfig {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 0.0,
                })
                .await;

            settle_pump().await;

            let mut run_configs: Vec<Vec<String>> = Vec::new();
            let mut saw_cluster_mutation = false;
            let mut saw_welcome = false;
            while let Ok(msg) = requester_inbox.try_recv() {
                match msg.msg_type() {
                    MessageType::RunConfig => {
                        if let DistributedMessage::RunConfig { forwarded_argv, .. } = msg {
                            run_configs.push(forwarded_argv);
                        }
                    }
                    MessageType::ClusterMutation => saw_cluster_mutation = true,
                    MessageType::SecondaryWelcome => saw_welcome = true,
                    _ => {}
                }
            }

            assert_eq!(
                run_configs.len(),
                1,
                "primary must unicast exactly one RunConfig reply; got {run_configs:?}"
            );
            assert_eq!(
                run_configs[0], seeded,
                "the RunConfig must carry the seeded forwarded_argv token-for-token"
            );
            assert!(
                !saw_cluster_mutation,
                "pure responder must NOT originate any ClusterMutation (no PeerJoined)"
            );
            assert!(
                !saw_welcome,
                "pure responder must NOT send a SecondaryWelcome"
            );

            // Replicated ledger + quorum table unchanged.
            assert_eq!(
                primary.cluster_state_for_test().digest(),
                digest_before,
                "pure responder must NOT mutate the replicated ledger \
                 (roster / capacity / CRDT convergence state)"
            );
            assert_eq!(
                primary.secondaries.len(),
                quorum_before,
                "pure responder must NOT touch the connection (quorum) table"
            );
        })
        .await;
}

/// (e) Empty pre-seed → empty argv (graceful): a primary that never had a
/// run-config threaded in still answers, with an empty `forwarded_argv`.
#[tokio::test(flavor = "current_thread")]
async fn primary_answers_request_run_config_empty_preseed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let mut requester_inbox = ends.remove(0).1;
            let (mut primary, _mesh) = build_test_primary(
                test_primary_config(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            // No seeding: forwarded_argv defaults empty.

            primary
                .handle_request_run_config(DistributedMessage::RequestRunConfig {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 0.0,
                })
                .await;
            settle_pump().await;

            let mut run_configs: Vec<Vec<String>> = Vec::new();
            while let Ok(msg) = requester_inbox.try_recv() {
                if let DistributedMessage::RunConfig { forwarded_argv, .. } = msg {
                    run_configs.push(forwarded_argv);
                }
            }
            assert_eq!(
                run_configs.len(),
                1,
                "primary must still answer with one RunConfig on empty pre-seed; got {run_configs:?}"
            );
            assert!(
                run_configs[0].is_empty(),
                "empty pre-seed must answer with an empty forwarded_argv; got {:?}",
                run_configs[0]
            );
        })
        .await;
}

/// (f) PROMOTED-primary answerability: a node promoted from secondary builds
/// its `PrimaryCoordinator` from a `PrimaryConfig` whose `forwarded_argv` is
/// the SAME copy it fetched over the mesh (the pyo3
/// `build_promoted_primary_recipe` threads the secondary's node-local argv
/// into the promoted `PrimaryConfig.forwarded_argv`). This test models that
/// recipe's output — seeding the run-config via the CONFIG (the production
/// `forwarded_argv: …` field on `PrimaryConfig`, NOT a direct post-construct
/// field write) — and proves the config → coordinator → responder chain:
/// the promoted node answers `RequestRunConfig` with the fetched argv
/// byte-identically, so it is indistinguishable from the original submitter
/// (no split-brain).
#[tokio::test(flavor = "current_thread")]
async fn promoted_primary_answers_request_run_config_with_fetched_argv() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let mut requester_inbox = ends.remove(0).1;

            // The argv a cold-start secondary fetched over the mesh. On
            // promotion the pyo3 recipe builds the new primary's
            // `PrimaryConfig` with exactly this in `forwarded_argv`.
            let fetched = vec![
                "--jobs".to_string(),
                "8".to_string(),
                "--name-regex".to_string(),
                "foo.*".to_string(),
            ];
            // Seed via the CONFIG field — the production promotion path —
            // rather than the post-construct `primary.forwarded_argv = …`
            // write the pure-responder test (d) uses.
            let config = PrimaryConfig {
                forwarded_argv: fetched.clone(),
                ..test_primary_config()
            };
            let (mut primary, _mesh) = build_test_primary(
                config,
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            primary
                .handle_request_run_config(DistributedMessage::RequestRunConfig {
                    target: None,
                    sender_id: "sec-0".into(),
                    timestamp: 0.0,
                })
                .await;
            settle_pump().await;

            let mut run_configs: Vec<Vec<String>> = Vec::new();
            while let Ok(msg) = requester_inbox.try_recv() {
                if let DistributedMessage::RunConfig { forwarded_argv, .. } = msg {
                    run_configs.push(forwarded_argv);
                }
            }
            assert_eq!(
                run_configs.len(),
                1,
                "promoted primary must unicast exactly one RunConfig reply; got {run_configs:?}"
            );
            assert_eq!(
                run_configs[0], fetched,
                "the promoted primary must answer with the argv it fetched, \
                 byte-identical to the original submitter (no split-brain)"
            );
        })
        .await;
}
