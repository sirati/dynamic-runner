//! Production replay: the requeue-vs-complete race (asm-dataset
//! run_20260610_221140, task_hash 2fc5c3cadbae9bc8).
//!
//! A task completed on a survivor worker during the primary-less failover
//! window; its confirmable terminal was blackholed and replayed. The
//! promoted primary inherited the stale `InFlight` occupancy, and the
//! lost-completion heuristic (#322 inherited-slot reconciliation) raced the
//! late-delivered terminal — BOTH won: the ALREADY-COMPLETED task was
//! requeued, re-assigned 25 s later, and re-ran to a SECOND terminal.
//!
//! These tests replay the production interleavings and pin the invariant:
//! a task hash with a CRDT-resident terminal is NEVER re-queued and NEVER
//! re-assigned, in EITHER order of (terminal ingestion, idle
//! re-confirmation):
//!
//! - terminal-then-requeue-check: the terminal lands first (as a received
//!   `ClusterMutation::TaskCompleted` — the delivery path that does not
//!   free the inherited slot — or as snapshot-restore residue); the
//!   worker's later `TaskRequest` must NOT requeue the completed task.
//! - requeue-then-terminal: the idle re-confirmation wins the race and the
//!   requeue legitimately fires (the heuristic cannot yet know); when the
//!   replayed wire terminal then lands, the queued task must be RECLAIMED
//!   from the pool so it is never dispatched a second time.

use super::*;

use std::time::Instant;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, TaskDep, TypeId};

use crate::primary::wire::compute_task_hash;

/// One advertised-memory resource amount (in bytes) for a secondary
/// capacity record / task request. Mirrors the live welcome shape: a
/// single `memory` `ResourceAmount`. (Same fixture as `hydrate.rs`.)
fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Drain every `TaskAssignment` `task_id` queued on the primary→secondary
/// wire (non-blocking). `task_id == name` for `dep_binary`, so the
/// re-dispatch assertions compare against the task name. (Same fixture as
/// `hydrate.rs`.)
fn drain_assigned_task_ids(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::TaskAssignment {
            target: _,
            binary_info,
            ..
        } = msg
        {
            ids.push(binary_info.task_id);
        }
    }
    ids
}

/// Build a `TaskInfo` with an explicit phase + dependency list. (Same
/// fixture as `hydrate.rs`.)
fn dep_binary(name: &str, phase: &str, depends_on: &[&str]) -> TaskInfo<TestId> {
    let mut t = make_binary(name, 100);
    t.phase_id = PhaseId::from(phase);
    t.type_id = TypeId::from("default");
    t.task_depends_on = depends_on
        .iter()
        .map(|d| TaskDep {
            task_id: (*d).to_string(),
            phase_id: PhaseId::from(phase),
            inherit_outputs: false,
        })
        .collect();
    t
}

/// The worker's post-`PrimaryChanged` idle re-confirmation frame. (Same
/// fixture as `hydrate.rs`.)
fn task_request_for(secondary: &str, worker: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        target: None,
        sender_id: secondary.into(),
        timestamp: 0.0,
        secondary_id: secondary.into(),
        worker_id: worker,
        available_resources: mem(8 * 1024 * 1024 * 1024),
    }
}

/// Seed the promoted-primary fixture: a single survivor secondary `sec-0`
/// with one worker whose pre-failover task `lost` is replicated `InFlight`
/// on `(sec-0, worker 0)`, then hydrate — producing the inherited
/// (unconfirmed-occupancy) slot + in-flight ledger entry + pool in-flight
/// counter the production failover produced. Returns the task + its hash.
fn seed_inherited_inflight(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
) -> (TaskInfo<TestId>, String) {
    let lost = dep_binary("lost", "work", &[]);
    let lost_hash = compute_task_hash(&lost);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::SecondaryCapacity {
            secondary: "sec-0".into(),
            worker_count: 1,
            resources: mem(8 * 1024 * 1024 * 1024),
        });
        cs.apply(ClusterMutation::TaskAdded {
            hash: lost_hash.clone(),
            task: lost.clone(),
        });
        cs.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: lost_hash.clone(),
            secondary: "sec-0".into(),
            worker: 0,
            version: Default::default(),
        });
    }
    primary.hydrate_from_cluster_state();
    assert!(
        primary.slot_is_inherited_for_test("sec-0", 0),
        "fixture: the reconstructed slot is INHERITED (unconfirmed occupancy)"
    );
    assert_eq!(primary.in_flight_len_for_test(), 1);
    (lost, lost_hash)
}

/// ORDER 1 (terminal-then-requeue-check), CRDT-mutation delivery: the
/// late terminal reaches the promoted primary as a received
/// `ClusterMutation::TaskCompleted` (a concurrent/deposed primary's
/// broadcast — the very thing the same incident's Face-2 zombie was
/// emitting). Ingesting it must settle the inherited slot + ledger entry
/// + pool accounting, so the worker's subsequent idle re-confirmation has
/// NOTHING to requeue and the completed task is never re-assigned.
#[tokio::test(flavor = "current_thread")]
async fn crdt_delivered_terminal_settles_inherited_slot_and_vetoes_requeue() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let (_lost, lost_hash) = seed_inherited_inflight(&mut primary);

            // The replayed terminal finally lands — as a replicated-ledger
            // mutation, NOT a wire TaskComplete frame.
            let mutation_frame = DistributedMessage::ClusterMutation {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                mutations: vec![ClusterMutation::TaskCompleted {
                    hash: lost_hash.clone(),
                    result_data: None,
                    attempt: 0,
                }],
            };
            primary
                .dispatch_message(mutation_frame, &mut None)
                .await
                .expect("mutation ingest ok");
            settle_pump().await;

            // Terminal ingestion settled the LOCAL execution caches: the
            // phantom-busy inherited slot is freed, the in-flight ledger
            // entry is dropped, and the pool's phase accounting drained.
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "a CRDT-delivered terminal must free the inherited slot \
                 holding its hash (production: the slot stayed phantom-busy \
                 and the reconciliation later requeued the completed task)"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "the in-flight ledger entry must be settled by the terminal"
            );
            assert!(
                primary.pool().is_run_complete(),
                "the pool's phase accounting must drain on the terminal \
                 (in-flight counter decremented, nothing queued)"
            );

            // The worker's live idle re-confirmation arrives one tick later
            // (production: same second). It must NOT requeue / re-assign
            // the completed task.
            primary
                .dispatch_message(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("task request ok");
            settle_pump().await;

            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&lost_hash),
                    Some(crate::cluster_state::TaskState::Completed { .. })
                ),
                "the CRDT terminal is sticky — never regressed to Pending"
            );
            let assigned = drain_assigned_task_ids(&mut ends[0].1);
            assert!(
                !assigned.contains(&"lost".to_string()),
                "an ALREADY-COMPLETED task must never be re-assigned \
                 (production re-executed 2fc5c3… to a second terminal); \
                 assigned: {assigned:?}"
            );
            assert_eq!(primary.in_flight_len_for_test(), 0);
        })
        .await;
}

/// ORDER 1 variant, snapshot-restore residue: the CRDT already records the
/// terminal (applied directly to the ledger — the shape a snapshot
/// restore/merge leaves behind, where NO per-mutation ingest hook ran), the
/// inherited slot still holds the hash. The requeue heuristic itself must
/// consult the terminal ledger and VETO: free the slot WITHOUT returning
/// the completed work to the pool.
#[tokio::test(flavor = "current_thread")]
async fn reconcile_heuristic_consults_terminal_ledger_and_vetoes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let (_lost, lost_hash) = seed_inherited_inflight(&mut primary);

            // Terminal lands as restore residue: ledger-only, no ingest hook.
            primary
                .cluster_state_mut_for_test()
                .apply(ClusterMutation::TaskCompleted {
                    hash: lost_hash.clone(),
                    result_data: None,
                    attempt: 0,
                });
            // The slot is still phantom-busy (restore runs no per-hash
            // settle) — exactly the state the heuristic fires on.
            assert!(primary.slot_is_inherited_for_test("sec-0", 0));

            // The worker's idle re-confirmation: the heuristic must veto
            // the requeue (the terminal ledger already accounts the hash)
            // and settle the slot instead.
            primary
                .dispatch_message(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("task request ok");
            settle_pump().await;

            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&lost_hash),
                    Some(crate::cluster_state::TaskState::Completed { .. })
                ),
                "the terminal must survive the reconciliation"
            );
            let assigned = drain_assigned_task_ids(&mut ends[0].1);
            assert!(
                !assigned.contains(&"lost".to_string()),
                "the vetoed requeue must not re-assign the completed task; \
                 assigned: {assigned:?}"
            );
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "the veto still frees the phantom-busy slot (the worker is \
                 live and idle — it must be able to take fresh work)"
            );
            assert_eq!(
                primary.in_flight_len_for_test(),
                0,
                "the inherited ledger entry is settled, not requeued"
            );
            assert!(
                primary.pool().is_run_complete(),
                "the completed task must NOT sit queued in the pool"
            );
        })
        .await;
}

/// ORDER 2 (requeue-then-terminal) — the PRODUCTION interleaving: the idle
/// re-confirmation wins the race, so the heuristic legitimately requeues
/// (it cannot yet know the completion exists); the worker is skipped for
/// dispatch this tick (production: the task sat QUEUED for 25 s), and THEN
/// the replayed wire `TaskComplete` lands. The terminal must reclaim the
/// queued task from the pool so the later idle request never re-assigns it.
#[tokio::test(flavor = "current_thread")]
async fn replayed_wire_terminal_reclaims_requeued_task_from_pool() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, mut ends) = setup_test(1);
            let (mut primary, _mesh) = build_test_primary(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let (_lost, lost_hash) = seed_inherited_inflight(&mut primary);

            // Production shape: the requeue did NOT immediately re-dispatch
            // to the requesting worker (the task sat queued 25 s). Model the
            // dispatch-shape gate with a backpressure window on sec-0 so the
            // reconcile fires but the same-tick assignment is skipped.
            primary.backpressured_secondaries.insert(
                "sec-0".into(),
                Instant::now() + Duration::from_secs(60),
            );

            // 20:24:13 — the idle re-confirmation: the heuristic requeues
            // the inherited task (legitimate at this instant — the lost
            // completion is still unknown).
            primary
                .dispatch_message(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("task request ok");
            settle_pump().await;
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&lost_hash),
                    Some(crate::cluster_state::TaskState::Pending { .. })
                ),
                "fixture: the reconciliation requeued the task (InFlight → \
                 Pending) — the race's first leg"
            );
            assert!(
                drain_assigned_task_ids(&mut ends[0].1).is_empty(),
                "fixture: the backpressure window held the re-dispatch (the \
                 production 25 s queue residence)"
            );

            // 20:24:13 (same second) — the replayed wire terminal finally
            // delivers.
            let replayed_terminal = DistributedMessage::TaskComplete {
                target: None,
                sender_id: "sec-0".into(),
                timestamp: 0.0,
                secondary_id: "sec-0".into(),
                worker_id: 0,
                task_hash: lost_hash.clone(),
                result_data: None,
                delivery_seq: Some(2847),
            };
            primary
                .dispatch_message(replayed_terminal, &mut None)
                .await
                .expect("terminal ingest ok");
            settle_pump().await;

            // The terminal must supersede the requeue: CRDT converges to
            // Completed AND the queued copy is reclaimed from the pool.
            assert!(
                matches!(
                    primary.cluster_state_for_test().task_state(&lost_hash),
                    Some(crate::cluster_state::TaskState::Completed { .. })
                ),
                "the late terminal wins the CRDT join over the requeued Pending"
            );
            assert!(
                primary.completed_tasks.contains(&lost_hash),
                "the completion is accounted"
            );
            assert!(
                primary.pool().is_run_complete(),
                "the requeued copy must be RECLAIMED from the pool by the \
                 terminal — leaving it queued is what re-executed 2fc5c3… \
                 at 20:24:38"
            );

            // 20:24:38 — the worker re-polls once its backoff clears. The
            // reclaimed task must NOT be re-assigned.
            primary.backpressured_secondaries.clear();
            primary
                .dispatch_message(task_request_for("sec-0", 0), &mut None)
                .await
                .expect("task request ok");
            settle_pump().await;
            let assigned = drain_assigned_task_ids(&mut ends[0].1);
            assert!(
                !assigned.contains(&"lost".to_string()),
                "an ALREADY-COMPLETED task must never be re-assigned after \
                 its erroneous requeue (the production second execution); \
                 assigned: {assigned:?}"
            );
        })
        .await;
}
