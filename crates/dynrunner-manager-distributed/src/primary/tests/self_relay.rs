//! Self-relay gating: an unassignable `TaskRequest` is relayed to
//! `Destination::Primary` ONLY when the current primary is a REMOTE peer
//! (the demoted-ex-primary forward-on case). When this node IS the
//! current primary, relaying resolves to SELF — on a co-located host the
//! egress `SendTarget::Loopback` delivers a LIVE frame into the
//! own-secondary's inbound, which demuxes it straight back into this
//! primary's inbound: an unthrottled self-feeding `TaskRequest` cycle.
//! `handle_task_request` therefore PARKS such a request instead of
//! relaying it. Parking is liveness-safe: the worker re-attempts on its
//! next backoff-throttled tick and the `TasksAdded` recheck re-nudges it
//! when work arrives.
//!
//! Three invariants:
//!   1. NO SELF-LOOP — a live/co-located primary (`current_primary() ==
//!      self`) with an EMPTY pool that cannot assign an own-worker
//!      `TaskRequest` emits ZERO frames into the co-located loopback (the
//!      egress edge the self-relay would feed). Under the pre-fix code
//!      this relayed a frame into the loopback (which, wired end-to-end,
//!      loops); the assertion on egress count == 0 is the fail-before.
//!   2. NO LIVENESS REGRESSION — same co-located primary, worker parked
//!      on an empty pool, THEN a task is added and the `TasksAdded`
//!      recheck runs: the parked worker IS assigned that task. Parking
//!      strands nothing.
//!   3. DEMOTED RELAY PRESERVED — a node whose `current_primary()` is a
//!      REMOTE peer (≠ self) that cannot assign an own-worker
//!      `TaskRequest` DOES relay it to `Destination::Primary` (forwarded
//!      to the real, remote primary).

use super::*;

use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind, ResourceMap, TypeId};

use crate::primary::wire::compute_task_hash;
use crate::worker_signal::{WorkerMgmtSignal, drain_worker_signal_batch};

type TestPrimary = PrimaryCoordinator<
    ChannelPeerTransport<TestId>,
    ResourceStealingScheduler,
    FixedEstimator,
    TestId,
>;

/// A `TaskRequest` from `secondary_id`'s worker `worker_id`, advertising
/// 1 GiB free memory — the same wire shape the sibling dispatch tests use.
fn task_request(secondary_id: &str, worker_id: u32) -> DistributedMessage<TestId> {
    DistributedMessage::TaskRequest {
        sender_id: secondary_id.into(),
        timestamp: 0.0,
        secondary_id: secondary_id.into(),
        worker_id,
        available_resources: vec![ResourceAmount {
            kind: ResourceKind::memory(),
            amount: 1024 * 1024 * 1024u64,
        }],
    }
}

/// Re-point `current_primary` via a `PrimaryChanged` apply (epoch 1 beats
/// the cold `None`/epoch-0 bootstrap). `new == "primary"` names this
/// coordinator itself (its `node_id`); any other id names a remote peer.
fn set_current_primary(primary: &mut TestPrimary, new: &str) {
    primary
        .cluster_state_mut_for_test()
        .apply(ClusterMutation::PrimaryChanged {
            new: new.into(),
            epoch: 1,
            reason: dynrunner_protocol_primary_secondary::PrimaryChangeReason::Election,
        });
}

fn one_gib() -> ResourceMap {
    ResourceMap::from([(ResourceKind::memory(), 1024 * 1024 * 1024u64)])
}

/// (1) NO SELF-LOOP. A co-located primary that IS the current primary
/// (`current_primary() == self`) holds a co-located loopback sender and
/// one idle worker, but its pool is EMPTY. An own-worker `TaskRequest`
/// cannot be assigned (no pending work) — and MUST NOT be relayed to
/// `Destination::Primary`, because that resolves to self and the egress
/// `SendTarget::Loopback` would deliver a LIVE frame into the loopback
/// (the head of the self-feeding cycle). Assert ZERO frames in the
/// loopback receiver.
///
/// FAIL-BEFORE: the pre-fix relay arm fired unconditionally on
/// `!assigned`, so `send_to(Destination::Primary, msg)` resolved to
/// `SendTarget::Loopback` and pushed exactly one frame here. The
/// `loopback_frames == 0` assertion fails under the old code.
#[tokio::test(flavor = "current_thread")]
async fn live_primary_empty_pool_does_not_relay_unassignable_request_into_loopback() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let mut primary: TestPrimary = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Co-located composition: the own-secondary loopback the
            // egress `SendTarget::Loopback` arm feeds. This is the channel
            // the self-relay would push into.
            let (loopback_tx, mut loopback_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            primary.register_colocated_loopback(loopback_tx);

            // This node IS the current primary.
            set_current_primary(&mut primary, "primary");
            assert!(
                primary.current_primary_is_self(),
                "fixture must put this node in the live-primary state"
            );

            // Empty CRDT ⇒ empty pool after hydrate; one idle worker.
            primary.hydrate_from_cluster_state();
            primary.register_idle_worker_for_test("sec-0".into(), 0, one_gib());

            // Own-worker request the primary cannot satisfy (no work).
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();

            // The defining assertion: nothing was relayed into the
            // loopback. Pre-fix this carried the self-relayed TaskRequest.
            let mut loopback_frames = 0usize;
            while loopback_rx.try_recv().is_ok() {
                loopback_frames += 1;
            }
            assert_eq!(
                loopback_frames, 0,
                "a live/co-located primary must NOT relay an unassignable own-worker \
                 TaskRequest into the co-located loopback (the self-feeding cycle); \
                 got {loopback_frames} frame(s)"
            );

            // The worker is left parked (still idle), not stranded — proved
            // by test (2).
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "the unassignable worker stays idle/parked on the live primary"
            );
        })
        .await;
}

/// (2) NO LIVENESS REGRESSION. A co-located live primary with an EMPTY
/// "work"-phase pool and one idle worker: the worker requests work and
/// PARKS (no work, no relay — per test 1). THEN a task is added to the
/// pool and the `TasksAdded` recheck runs (exactly the
/// dispatch-decoupling wake path the operational loop drives). The parked
/// worker IS assigned the task. Parking does not strand the worker.
///
/// The secondary channel ends are held LIVE (`_ends`) for the duration so
/// the recheck's `TaskAssignment` wire send to `sec-0` reaches a live
/// receiver; dropping them would fail the send into the rollback arm
/// (slot back to idle) — a transport artefact, not the parking behaviour
/// under test.
#[tokio::test(flavor = "current_thread")]
async fn parked_worker_is_assigned_when_task_arrives_via_tasks_added_recheck() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (transport, _ends) = setup_test(1);
            let mut primary: TestPrimary = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );
            let (loopback_tx, _loopback_rx) =
                tokio_mpsc::unbounded_channel::<DistributedMessage<TestId>>();
            primary.register_colocated_loopback(loopback_tx);
            set_current_primary(&mut primary, "primary");

            // EMPTY "work"-phase pool + one idle worker (the
            // `worker_lifecycle` seeding shape: a directly-built pool whose
            // phase set makes a later-extended task immediately
            // dispatchable, without re-hydrating — which would wipe the
            // registered worker).
            let phase = PhaseId::from("work");
            let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
                [phase.clone()],
                HashMap::new(),
            )
            .expect("work-phase pool");
            primary.pending = Some(pool);
            primary.phase_completed.insert(phase.clone(), 0);
            primary.phase_failed.insert(phase, 0);
            primary.register_idle_worker_for_test("sec-0".into(), 0, one_gib());

            // The worker requests; the pool is empty ⇒ it parks (idle).
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();
            assert!(
                primary.slot_is_idle_for_test("sec-0", 0),
                "worker parks on the empty pool"
            );

            // Now real work arrives: extend the pool with one task and
            // drive the parked recheck the way the operational loop does
            // (emit TasksAdded → drain the batch → react).
            let mut t = make_binary("t", 100);
            t.phase_id = PhaseId::from("work");
            t.type_id = TypeId::from("default");
            let hash_t = compute_task_hash(&t);
            primary.pool_mut().extend(vec![t]).expect("valid extend");

            let (wm_tx, mut wm_rx) = tokio_mpsc::unbounded_channel::<WorkerMgmtSignal>();
            primary
                .cluster_state_mut_for_test()
                .install_worker_mgmt_sender(wm_tx);
            primary
                .cluster_state_mut_for_test()
                .emit_worker_mgmt(WorkerMgmtSignal::TasksAdded);
            let batch = drain_worker_signal_batch(&mut wm_rx, Duration::from_millis(50))
                .await
                .expect("emit must produce a batch");
            primary.react_to_worker_signal_batch(batch).await;

            // The parked worker took the newly-arrived task.
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash_t),
                "the TasksAdded recheck must assign the arrived task to the parked worker — \
                 parking on the empty pool does not strand it"
            );
        })
        .await;
}

/// (3) DEMOTED RELAY PRESERVED. A node whose `current_primary()` is a
/// REMOTE peer (a demoted ex-primary: authority moved elsewhere) receives
/// an own-worker `TaskRequest` it cannot assign (empty pool). It MUST
/// still relay the request to `Destination::Primary` — which now resolves
/// to the remote primary's id (`SendTarget::Peer`), NOT a loopback. The
/// relayed frame lands in the remote primary's inbox.
#[tokio::test(flavor = "current_thread")]
async fn demoted_primary_relays_unassignable_request_to_remote_primary() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // Build the transport with a routable outbox to the remote
            // primary so the relay (a `send_to_peer`) is observable.
            let (incoming_tx, incoming_rx) = tokio_mpsc::unbounded_channel();
            let (to_remote_tx, mut to_remote_rx) = tokio_mpsc::unbounded_channel();
            let mut outgoing = HashMap::new();
            outgoing.insert("remote-primary".to_string(), to_remote_tx);
            // Keep the inbound alive for the lifetime of the test.
            let _incoming_tx = incoming_tx;
            let transport =
                ChannelPeerTransport::from_raw_channels("primary".into(), outgoing, incoming_rx);
            let mut primary: TestPrimary = PrimaryCoordinator::new(
                PrimaryConfig::default(),
                transport,
                ResourceStealingScheduler::memory(),
                FixedEstimator(100),
            );

            // Authority has moved to a REMOTE peer: this node is demoted.
            set_current_primary(&mut primary, "remote-primary");
            assert!(
                !primary.current_primary_is_self(),
                "fixture must put this node in the demoted state (remote primary)"
            );

            // Empty pool, one idle worker: the request cannot be assigned
            // locally, so it falls through to the relay arm.
            primary.hydrate_from_cluster_state();
            primary.register_idle_worker_for_test("sec-0".into(), 0, one_gib());
            primary
                .handle_task_request(task_request("sec-0", 0))
                .await
                .unwrap();

            // The relay fired: the remote primary's inbox holds the
            // forwarded TaskRequest.
            let relayed = to_remote_rx.try_recv().expect(
                "demoted primary must relay the unassignable TaskRequest to the remote primary",
            );
            assert!(
                matches!(
                    relayed,
                    DistributedMessage::TaskRequest { worker_id: 0, .. }
                ),
                "the relayed frame must be the own-worker TaskRequest; got {relayed:?}"
            );
            assert!(
                to_remote_rx.try_recv().is_err(),
                "exactly one relay frame is forwarded"
            );
        })
        .await;
}
