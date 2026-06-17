//! RCA disco-prune edge C: the dispatch-pipeline's "skip this worker"
//! predicate (`should_skip_worker_for_dispatch`) must SKIP a worker whose
//! secondary the transport cannot DELIVER to RIGHT NOW.
//!
//! A silently-dropped secondary (raw mesh leg disconnected) lingers `Alive`
//! in every CRDT membership view until the slow keepalive→consensus removal
//! converges. The bug: the primary keeps DISPATCHING new `TaskAssignment`s to
//! it through that whole lag window, stranding the work on a dead leg. The fix
//! gates dispatch on the transport's LIVE deliverability set
//! (`MeshClient::has_route`, pump-published from a live transport read), which
//! flips `false` the instant the secondary has NO path at all (no direct wire
//! AND no relay forwarder) — independent of the consensus removal.
//!
//! `has_route`, NOT `has_peer`: a member reachable via relay through a
//! connected forwarder is still a valid dispatch target (the live
//! mid-run-joiner-via-sibling-relay path in `midrun_join` — covered there over
//! a relay-capable transport). These unit checks use a NON-relay-capable
//! `ControllableMembershipPeer`, where `has_route` collapses to `has_peer`, so
//! "absent from the connected set" == "no route" — the direct-wire facet of
//! the gate.
//!
//! Two invariants pinned here:
//!   * a member present in the roster but with NO delivery route is skipped; a
//!     member with a route is NOT skipped — on BOTH the proactive
//!     (`bypass_backpressure=false`) and reactive/recheck
//!     (`bypass_backpressure=true`) dispatch paths;
//!   * the colocated SELF secondary (same node as this primary, id ==
//!     `PrimaryConfig.node_id`) has NO transport route to itself by
//!     construction (the mesh forbids self-links; its frames resolve as
//!     in-process loopback), so `has_route(node_id)` is always `false` — it
//!     must be EXEMPTED and stay dispatchable.
//!
//! Deterministic: a `ControllableMembershipPeer` whose connected-id cell the
//! test seeds, then `publish_membership()` snapshots it into the
//! `MembershipView` the coordinator's detached `MeshClient` reads. No pump,
//! no clock.

use super::*;

use std::collections::HashSet;

use dynrunner_core::{ResourceKind, ResourceMap};
use dynrunner_protocol_primary_secondary::address::PeerId;

use crate::primary::test_helpers::ControllableMembershipPeer;
use crate::process::{LocalRole, Mesh};

type TestPrimary = PrimaryCoordinator<ResourceStealingScheduler, FixedEstimator, TestId>;

/// One advertised-memory budget (bytes) for a registered worker slot.
fn budget(bytes: u64) -> ResourceMap {
    ResourceMap::from([(ResourceKind::memory(), bytes)])
}

/// The pieces a gate test drives: the coordinator under test, the mesh (held
/// so the test can re-`publish_membership`), the shared connected-id cell the
/// transport projects, and the role slot (held alive — there is no pump in
/// these SYNC checks to own it).
struct GateHarness {
    primary: TestPrimary,
    mesh: Mesh<TestId, ControllableMembershipPeer<TestId>>,
    connected: std::rc::Rc<std::cell::RefCell<HashSet<String>>>,
    _slot: std::sync::Arc<crate::process::RoleSlot<TestId>>,
}

/// Build a primary over a `ControllableMembershipPeer` whose live connected-id
/// set is `connected`. The primary's own peer id is `"setup"` (=
/// `test_primary_config().node_id`), so a worker registered under `"setup"`
/// models the colocated self-secondary.
fn primary_with_controllable_membership() -> GateHarness {
    let connected: std::rc::Rc<std::cell::RefCell<HashSet<String>>> =
        std::rc::Rc::new(std::cell::RefCell::new(HashSet::new()));
    let transport = ControllableMembershipPeer::<TestId>::new(connected.clone());
    let mut mesh = Mesh::new(transport);
    let (slot, client, inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("setup"));
    let (_demote_tx, demote_rx) = tokio_mpsc::unbounded_channel();
    let primary = PrimaryCoordinator::new(
        test_primary_config(),
        client,
        inbox,
        demote_rx,
        ResourceStealingScheduler::memory(),
        FixedEstimator(100),
    );
    GateHarness {
        primary,
        mesh,
        connected,
        _slot: slot,
    }
}

/// Edge C core: a roster member with a LIVE transport leg is dispatchable; the
/// SAME member with its leg dropped (absent from the connected set, but still
/// `Alive` in the CRDT — the lag window the bug lives in) is SKIPPED.
#[test]
fn dispatch_skips_secondary_with_no_live_transport_leg() {
    let GateHarness {
        mut primary,
        mesh,
        connected,
        _slot,
    } = primary_with_controllable_membership();

    // Two remote secondaries, one worker each. worker idx 0 → "sec-live",
    // worker idx 1 → "sec-gone".
    primary.register_idle_worker_for_test("sec-live".into(), 0, budget(1024));
    primary.register_idle_worker_for_test("sec-gone".into(), 1, budget(1024));

    // Both legs UP: the transport's live connected set carries both ids.
    connected
        .borrow_mut()
        .extend(["sec-live".to_string(), "sec-gone".to_string()]);
    mesh.publish_membership();

    assert!(
        !primary.should_skip_worker_for_dispatch(0, false),
        "a member with a live transport leg must be dispatchable"
    );
    assert!(
        !primary.should_skip_worker_for_dispatch(1, false),
        "a member with a live transport leg must be dispatchable"
    );

    // "sec-gone"'s raw mesh leg drops: the transport re-reads its live
    // connected set (now WITHOUT it) and republishes. Its CRDT membership is
    // untouched (no PeerRemoved applied) — exactly the silent-drop lag window.
    connected.borrow_mut().remove("sec-gone");
    mesh.publish_membership();

    assert!(
        !primary.should_skip_worker_for_dispatch(0, false),
        "the still-connected member stays dispatchable"
    );
    assert!(
        primary.should_skip_worker_for_dispatch(1, false),
        "a member with NO live transport leg must be skipped for dispatch, \
         even while it lingers Alive in the CRDT pre-consensus-removal"
    );

    // The gate holds on BOTH dispatch entry paths: the predicate is the single
    // choke point both `dispatch_to_idle_workers` (proactive) and
    // `handle_task_request` (reactive) call. The `bypass_backpressure=true`
    // path a `TasksAdded` recheck takes must STILL skip a dead leg (a freed
    // backpressure slot on a disconnected secondary is not a real target).
    assert!(
        primary.should_skip_worker_for_dispatch(1, true),
        "the transport-liveness skip must NOT be lifted by bypass_backpressure \
         (the reactive/recheck dispatch path must also refuse a dead leg)"
    );
    assert!(
        !primary.should_skip_worker_for_dispatch(0, true),
        "the live member stays dispatchable on the bypass path too"
    );

    drop(mesh);
}

/// Edge C self-exemption: the colocated self-secondary (id == node_id) has NO
/// transport leg to itself by construction, so `has_peer(node_id)` is ALWAYS
/// `false`. It must be EXEMPTED from the transport-liveness gate, or the
/// primary would refuse to feed its own in-process workers.
#[test]
fn dispatch_does_not_skip_colocated_self_secondary() {
    let GateHarness {
        mut primary,
        mesh,
        connected,
        _slot,
    } = primary_with_controllable_membership();

    // The self/in-process secondary shares the primary's node id ("setup").
    primary.register_idle_worker_for_test("setup".into(), 0, budget(1024));

    // The connected set is EMPTY of "setup" — the mesh never lists itself
    // (self-links forbidden; loopback bypasses the transport connection
    // table), so has_peer(self) is false. Publish so the view reflects it.
    assert!(
        !connected.borrow().contains("setup"),
        "the self id is never in the transport connected set (self-link \
         forbidden)"
    );
    mesh.publish_membership();

    assert!(
        !primary.should_skip_worker_for_dispatch(0, false),
        "the colocated self-secondary must NOT be skipped despite \
         has_peer(self)==false — it dispatches over in-process loopback, not \
         a transport leg"
    );

    drop(mesh);
}
