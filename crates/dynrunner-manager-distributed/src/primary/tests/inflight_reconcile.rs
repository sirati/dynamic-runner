//! #518 cross-member double-execution reconciliation — the primary half.
//!
//! The production sequence replayed VERBATIM (the false-dead → requeue →
//! cross-member double-run → re-admission reconcile arc):
//!
//!   1. A live member A (sec-0) is running task T (in-flight ledger holder
//!      = sec-0, its slot Assigned, CRDT `InFlight`).
//!   2. A is FALSELY declared dead (its keepalive lagged under CPU load):
//!      `requeue_dead_secondary("sec-0")` requeues T to `Pending`, DELETES
//!      A's ledger entry, and removes A from membership. A is NOT told to
//!      stop and keeps running T.
//!   3. The dispatch recheck re-dispatches the requeued T to a DIFFERENT
//!      live member B (sec-1): the ledger now keys T to sec-1. #517's
//!      same-worker bounce never fires (B's slot WAS idle), so T executes
//!      on BOTH A and B (cross-member double-execution).
//!   4. A re-contacts the primary (a frame from removed A). The
//!      re-admission seam re-admits A AND pulls its actual in-flight
//!      roster (`RequestInFlightRoster`). A answers `InFlightRoster`
//!      naming T as authoritatively running on its worker — A is the
//!      source of truth for what its workers run.
//!   5. The primary reconciles: it recognises T is authoritatively on A,
//!      RE-SEATS the ledger onto A, and WITHDRAWS the duplicate copy from
//!      B (`WithdrawTask`). T no longer double-runs: the original keeps it,
//!      B's copy is withdrawn.
//!
//! Pinned here:
//!   * the reconcile re-seats the ledger holder A→sec-0 and seats A's slot
//!     (Inherited provenance);
//!   * a `WithdrawTask` for T lands on B's (sec-1's) wire end;
//!   * NO failure accounting (the withdraw is the requeue-inverse, never a
//!     terminal — `failed_tasks` untouched);
//!   * REVERT-CONFIRM: without the roster reconcile the ledger stays on B
//!     (the pre-fix tolerate-double-exec state), and no withdraw is sent.

use super::*;

use crate::primary::wire::compute_task_hash;
use dynrunner_core::{PhaseId, ResourceAmount, ResourceKind};
use dynrunner_protocol_primary_secondary::{InFlightRosterEntry, RemovalCause};

fn mem(bytes: u64) -> Vec<ResourceAmount> {
    vec![ResourceAmount {
        kind: ResourceKind::memory(),
        amount: bytes,
    }]
}

/// Register a member in the CRDT (PeerJoined at `member_gen` +
/// SecondaryCapacity), the live-welcome shape.
fn register_member(
    primary: &mut PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    id: &str,
    member_gen: u64,
) {
    let cs = primary.cluster_state_mut_for_test();
    cs.apply(ClusterMutation::PeerJoined {
        peer_id: id.into(),
        is_observer: false,
        can_be_primary: true,
        cap_version: Default::default(),
        member_gen,
    });
    cs.apply(ClusterMutation::SecondaryCapacity {
        secondary: id.into(),
        worker_count: 1,
        resources: mem(8 * 1024 * 1024 * 1024),
    });
}

/// The re-admitted member's in-flight roster answer (#518), mirroring the
/// secondary emitter (`secondary/dispatch/inflight_roster.rs`): names the
/// task its worker is ACTUALLY running, stamped with the member's current
/// membership generation.
fn roster_from(
    member_id: &str,
    member_gen: u64,
    entries: Vec<(&str, u32, &str)>,
) -> DistributedMessage<TestId> {
    DistributedMessage::InFlightRoster {
        target: None,
        sender_id: member_id.into(),
        timestamp: 0.0,
        secondary_id: member_id.into(),
        member_gen,
        entries: entries
            .into_iter()
            .map(|(hash, worker_id, task_name)| InFlightRosterEntry {
                hash: hash.into(),
                worker_id,
                task_id: TestId(task_name.into()),
            })
            .collect(),
    }
}

/// Drain every `WithdrawTask` queued on a wire end, returning the
/// withdrawn `task_hash`es.
fn withdrawn_hashes(
    rx: &mut tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
) -> Vec<String> {
    let mut hashes = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if let DistributedMessage::WithdrawTask { task_hash, .. } = msg {
            hashes.push(task_hash);
        }
    }
    hashes
}

/// Build a 2-member primary (sec-0 = A, sec-1 = B) and drive the
/// production false-dead → requeue → cross-member redispatch arc up to the
/// double-run state: T's ledger holder is B (sec-1), while A (sec-0) is the
/// authoritative original still running it. Returns the primary, the wire
/// ends, and T's hash.
#[allow(clippy::type_complexity)]
async fn primary_with_cross_member_duplicate() -> (
    PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>,
    Vec<(
        String,
        tokio_mpsc::UnboundedReceiver<DistributedMessage<TestId>>,
        tokio_mpsc::UnboundedSender<DistributedMessage<TestId>>,
    )>,
    String,
    PrimaryMeshKeepalive,
) {
    let (transport, ends) = setup_test(2);
    let (mut primary, mesh) = build_test_primary(
        test_primary_config(),
        transport,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    let phase = PhaseId::from("default");
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::PhaseDepsSet {
            deps: HashMap::from([(phase.clone(), vec![])]),
        });
    }
    // The pool the requeue path returns the falsely-dead member's task into.
    let pool = dynrunner_scheduler_api::PendingPool::<TestId>::new(
        [phase.clone()],
        HashMap::new(),
    )
    .expect("default-phase pool");
    primary.pending = Some(pool);
    register_member(&mut primary, "sec-0", 0);
    register_member(&mut primary, "sec-1", 0);

    // (1) A (sec-0) is running T: seat it on sec-0's worker + the CRDT
    // InFlight fact the live dispatch would have written.
    let task = make_binary("cross-member-task", 100);
    let hash = compute_task_hash(&task);
    let staged = primary.stage_in_flight_for_test("sec-0".into(), 0, task.clone());
    assert_eq!(staged, hash);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAdded {
            hash: hash.clone(),
            task: task.clone(),
            def_id: None,
        });
        cs.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: hash.clone(),
            secondary: "sec-0".into(),
            worker: 0,
            version: Default::default(),
        });
    }
    assert_eq!(
        primary.in_flight.get(&hash).map(|e| e.secondary_id.as_str()),
        Some("sec-0"),
        "fixture precondition: T's ledger holder is the original A (sec-0)"
    );

    // (2) A is FALSELY declared dead: T requeues, A's ledger entry is
    // deleted, A is removed from membership. (A keeps running T — the
    // primary cannot tell it to stop.)
    let dead = super::super::heartbeat::DeadSecondary {
        secondary_id: "sec-0".into(),
        last_keepalive: std::time::Instant::now(),
    };
    primary
        .requeue_dead_secondary(dead, RemovalCause::KeepaliveMiss)
        .await
        .unwrap();
    assert!(
        !primary.in_flight.contains_key(&hash),
        "the false death deletes A's ledger entry (T is requeued to Pending)"
    );

    // (3) The dispatch recheck re-dispatches the requeued T onto B (sec-1):
    // seat it on sec-1's worker + the CRDT InFlight fact. The ledger now
    // attributes T to B — the cross-member duplicate state (#517's bounce
    // never fires, because B's slot WAS idle).
    let restaged = primary.stage_in_flight_for_test("sec-1".into(), 0, task.clone());
    assert_eq!(restaged, hash);
    {
        let cs = primary.cluster_state_mut_for_test();
        cs.apply(ClusterMutation::TaskAssigned {
            attempt: 0,
            hash: hash.clone(),
            secondary: "sec-1".into(),
            worker: 0,
            version: Default::default(),
        });
    }
    assert_eq!(
        primary.in_flight.get(&hash).map(|e| e.secondary_id.as_str()),
        Some("sec-1"),
        "fixture: the requeued copy's ledger holder is now B (sec-1) — the \
         cross-member double-run state"
    );

    // A re-contacts: a frame from removed sec-0 arrives. The REAL
    // re-admission seam re-admits A (PeerJoined at gen+1), rebuilds its
    // worker roster (`react_to_capacity_growth`), and pulls its in-flight
    // roster (`RequestInFlightRoster` to A's wire end). Drive the genuine
    // seam so the worker roster + the gen gate match production.
    let frame_from_a = DistributedMessage::<TestId>::Keepalive {
        target: None,
        sender_id: "sec-0".into(),
        timestamp: 0.0,
        secondary_id: "sec-0".into(),
        active_workers: 1,
        emitter_role: dynrunner_protocol_primary_secondary::KeepaliveRole::Secondary,
    };
    primary.maybe_readmit_sender(&frame_from_a).await;
    assert!(
        primary.cluster_state_for_test().is_peer_alive("sec-0"),
        "the re-admission seam must restore A to live membership"
    );
    assert_eq!(
        primary.cluster_state_for_test().peer_member_gen("sec-0"),
        1,
        "re-admission bumps A's membership generation to dead_gen + 1"
    );

    (primary, ends, hash, mesh)
}

/// THE replay: A re-admits and reports T as authoritatively running on its
/// worker; the primary re-seats the ledger onto A and WITHDRAWS the
/// duplicate copy from B. T no longer double-runs.
#[tokio::test(flavor = "current_thread")]
async fn readmission_roster_reseats_ledger_and_withdraws_cross_member_duplicate() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash, _mesh) =
                primary_with_cross_member_duplicate().await;

            let failed_before = primary.failed_tasks.len();
            // Let the mesh-pump drain the fixture's requeue/re-admission
            // egress, then clear B's wire end so the post-reconcile capture
            // sees only the withdraw.
            settle_pump().await;
            let _ = withdrawn_hashes(&mut ends[1].1);

            // (4)+(5) A's re-admission roster answer drives the reconcile.
            primary
                .handle_inflight_roster(roster_from(
                    "sec-0",
                    1,
                    vec![(&hash, 0, "cross-member-task")],
                ))
                .await;
            settle_pump().await;

            // The ledger is RE-SEATED onto the authoritative original A.
            assert_eq!(
                primary.in_flight.get(&hash).map(|e| e.secondary_id.as_str()),
                Some("sec-0"),
                "the reconcile must re-seat the ledger onto the authoritative \
                 holder (the re-admitted original A)"
            );
            // A's slot now holds T (Inherited occupancy — reconciled from A's
            // own report, settled by A's eventual terminal).
            assert!(
                primary.slot_holds_hash_for_test("sec-0", 0, &hash),
                "A's slot must hold T after the re-seat"
            );
            assert!(
                primary.slot_is_inherited_for_test("sec-0", 0),
                "the re-seated occupancy is Inherited (A's report, not a fresh \
                 dispatch)"
            );

            // A `WithdrawTask` for T lands on B's (sec-1's) wire end.
            assert_eq!(
                withdrawn_hashes(&mut ends[1].1),
                vec![hash.clone()],
                "the duplicate copy on B must be withdrawn"
            );
            // No withdraw was sent to A (the original is never the loser).
            assert!(
                withdrawn_hashes(&mut ends[0].1).is_empty(),
                "the re-admitted original must never be told to withdraw"
            );

            // NO failure accounting: the withdraw is the requeue-inverse.
            assert_eq!(
                primary.failed_tasks.len(),
                failed_before,
                "withdrawing a duplicate must never burn failure budget"
            );
        })
        .await;
}

/// REVERT-CONFIRM: WITHOUT the roster reconcile, the cross-member
/// duplicate persists — the ledger stays on B and no withdraw is sent (the
/// pre-fix tolerate-double-exec state). This pins that the reconcile is
/// what closes the double-run, not the fixture.
#[tokio::test(flavor = "current_thread")]
async fn without_reconcile_the_cross_member_duplicate_persists() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (primary, mut ends, hash, _mesh) =
                primary_with_cross_member_duplicate().await;

            // No `handle_inflight_roster` call: the duplicate is untouched.
            assert_eq!(
                primary.in_flight.get(&hash).map(|e| e.secondary_id.as_str()),
                Some("sec-1"),
                "without the reconcile the ledger stays on B — the duplicate \
                 persists (the pre-fix double-run)"
            );
            assert!(
                withdrawn_hashes(&mut ends[1].1).is_empty(),
                "without the reconcile no withdraw is ever sent — both members \
                 keep running T"
            );
        })
        .await;
}

/// A roster whose member generation is STALE (the reporter was re-removed
/// after sending the roster) is ignored — the primary must not re-seat the
/// ledger onto a member the cluster has since removed again.
#[tokio::test(flavor = "current_thread")]
async fn stale_generation_roster_is_ignored() {
    let _ = tracing_subscriber::fmt::try_init();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut primary, mut ends, hash, _mesh) =
                primary_with_cross_member_duplicate().await;
            let _ = withdrawn_hashes(&mut ends[1].1);

            // A's live membership generation is 1 (re-admitted); a roster
            // stamped gen 0 crossed a re-removal in flight — ignore it.
            primary
                .handle_inflight_roster(roster_from(
                    "sec-0",
                    0,
                    vec![(&hash, 0, "cross-member-task")],
                ))
                .await;

            assert_eq!(
                primary.in_flight.get(&hash).map(|e| e.secondary_id.as_str()),
                Some("sec-1"),
                "a stale-generation roster must NOT re-seat the ledger"
            );
            assert!(
                withdrawn_hashes(&mut ends[1].1).is_empty(),
                "a stale-generation roster must send no withdraw"
            );
        })
        .await;
}
