//! Routing tests for the `process/` mesh: the broadcast origin-exclusion
//! (BUG-1), directed loopback-vs-remote dispatch, the queued client send
//! (M4), and the in-place retag (H5). Shares the fixtures in the parent
//! [`super`] test module.

use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};

use super::super::mesh::Mesh;
use super::super::role::LocalRole;
use super::{
    TestId, abort_request_frame, frame, frame_to, sender_of, transport_with_remotes, welcome_frame,
};

/// `broadcast` excludes the ORIGINATING ROLE but includes the OTHER local
/// roles AND the remote connections (BUG-1: exclude the origin ROLE, never
/// the origin PEER). Here a same-host secondary's broadcast reaches the
/// same-host primary slot (the §14 fix) and every remote — but not the
/// secondary's own inbox.
#[tokio::test]
async fn broadcast_excludes_origin_role_includes_siblings_and_remotes() {
    let (transport, mut receivers) = transport_with_remotes("host-a", &["remote-1", "remote-2"]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));
    let (_s_slot, _s_client, mut secondary_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));

    // The secondary broadcasts an `All` frame.
    mesh.broadcast(LocalRole::Secondary, frame("sec-broadcast"))
        .await;

    // The same-host PRIMARY slot receives it (§14: the local primary's
    // death clock would be refreshed by its own host's secondary keepalive).
    assert_eq!(
        sender_of(&primary_inbox.try_recv().expect("primary got the broadcast")),
        "sec-broadcast"
    );
    // The originating SECONDARY does NOT receive its own broadcast.
    assert!(
        secondary_inbox.try_recv().is_none(),
        "originating role is excluded from its own broadcast"
    );
    // Every REMOTE peer received it (the peer is never excluded).
    for id in ["remote-1", "remote-2"] {
        let rx = receivers.get_mut(id).expect("remote receiver");
        assert_eq!(
            sender_of(&rx.try_recv().expect("remote got the broadcast")),
            "sec-broadcast"
        );
    }
}

/// An `All`-target `dispatch` fans to EVERY local slot + remotes minus the
/// origin — the same fan as `broadcast`, reached through the directed
/// `dispatch` entry point.
#[tokio::test]
async fn dispatch_all_fans_like_broadcast() {
    let (transport, mut receivers) = transport_with_remotes("host-a", &["remote-1"]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));
    let (_o_slot, _o_client, mut observer_inbox) =
        mesh.register_local_role(LocalRole::Observer, PeerId::from("host-a"));

    mesh.dispatch(LocalRole::Primary, Destination::All, frame("pri-all"))
        .await
        .expect("All dispatch succeeds");

    // Observer (a non-origin local role) and the remote both got it; the
    // origin primary did not.
    assert_eq!(
        sender_of(&observer_inbox.try_recv().expect("observer got All")),
        "pri-all"
    );
    assert!(primary_inbox.try_recv().is_none(), "origin excluded");
    let rx = receivers.get_mut("remote-1").unwrap();
    assert_eq!(
        sender_of(&rx.try_recv().expect("remote got All")),
        "pri-all"
    );
}

/// A directed `dispatch` to a SAME-HOST secondary loopbacks to the local
/// secondary slot; a directed `dispatch` to a REMOTE host id goes over the
/// wire — never excluding the origin (directed delivery is not a fan).
#[tokio::test]
async fn dispatch_directed_loopback_vs_remote() {
    let (transport, mut receivers) = transport_with_remotes("host-a", &["remote-1"]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_s_slot, _s_client, mut secondary_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));

    // Directed to the local host's secondary → loopback.
    mesh.dispatch(
        LocalRole::Primary,
        Destination::Secondary(PeerId::from("host-a")),
        frame("to-local-sec"),
    )
    .await
    .expect("loopback dispatch succeeds");
    assert_eq!(
        sender_of(
            &secondary_inbox
                .try_recv()
                .expect("local secondary loopback")
        ),
        "to-local-sec"
    );

    // Directed to a remote secondary id → over the wire.
    mesh.dispatch(
        LocalRole::Primary,
        Destination::Secondary(PeerId::from("remote-1")),
        frame("to-remote-sec"),
    )
    .await
    .expect("remote dispatch succeeds");
    let rx = receivers.get_mut("remote-1").unwrap();
    assert_eq!(
        sender_of(&rx.try_recv().expect("remote secondary over wire")),
        "to-remote-sec"
    );
    // The local secondary did NOT also get the remote-addressed frame.
    assert!(secondary_inbox.try_recv().is_none());
}

/// A `MeshClient::send` is QUEUED (M4): it enqueues a `LocalDispatch` the
/// pump later applies via `apply_local_dispatch`. Draining the queue and
/// applying it routes loopback-vs-remote against the live slots.
#[tokio::test]
async fn mesh_client_send_is_queued_and_applied_by_pump() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_s_slot, s_client, _s_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));
    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    // The secondary queues a directed send to the local primary; nothing
    // is delivered synchronously.
    s_client
        .send(Destination::Primary, frame("queued-to-primary"))
        .expect("queue accepts");
    assert!(
        primary_inbox.try_recv().is_none(),
        "send is queued, not synchronous (M4)"
    );

    // The pump drains the queue and applies it against the live slots.
    let item = mesh.next_local_dispatch().await.expect("one queued item");
    mesh.apply_local_dispatch(item).await.expect("apply routes");
    assert_eq!(
        sender_of(
            &primary_inbox
                .try_recv()
                .expect("primary got the queued send")
        ),
        "queued-to-primary"
    );
}

/// The submitter primary→observer RETAG flips the slot's role IN PLACE
/// (H5) — the SAME `Arc`/channel — and the slot keeps delivering through
/// its original inbox under the new role, with no drop+recreate and no
/// delivery gap. (The C1 `Process` owns re-pointing the mesh's `Weak` at
/// the retagged slot; this asserts the retag primitive the swap relies
/// on: identity + channel survive, only the role flips.)
#[tokio::test]
async fn retag_in_place_keeps_identity_and_channel() {
    let (transport, _r) = transport_with_remotes("submitter", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    // Register the submitter's PRIMARY slot; a loopback lands.
    let (slot, _client, mut inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("submitter"));
    assert!(mesh.deliver_local(LocalRole::Primary, frame("as-primary")));
    assert_eq!(
        sender_of(&inbox.try_recv().expect("primary delivery")),
        "as-primary"
    );

    // Retag primary → observer IN PLACE: same `Arc`, same `peer_id`, same
    // inbound channel — only the role flips.
    slot.set_role(LocalRole::Observer);
    assert_eq!(slot.role(), LocalRole::Observer);
    assert_eq!(slot.peer_id(), &PeerId::from("submitter"));

    // The slot's ORIGINAL channel still delivers under the new role — the
    // stable channel across the retag (no delivery gap).
    slot.deliver(frame("as-observer"))
        .expect("retagged slot's channel is live");
    assert_eq!(
        sender_of(&inbox.try_recv().expect("retagged slot keeps its channel")),
        "as-observer"
    );
}

/// `Mesh::retag_local_role` (D-RETAG) moves the mesh's `Weak` from the OLD
/// role field to the NEW one AND flips the slot's role — the same `Arc` /
/// channel survives, so a frame directed to the NEW role now reaches the
/// retagged slot's inbox, the OLD role field is empty, and `set_role` took
/// effect. This is what the submitter primary→observer swap needs: C0 keys
/// slots by FIELD, so `set_role` alone would leave the `Weak` in the wrong
/// field. We assert the field move BEHAVIORALLY through `deliver_local`
/// (the mesh delivers off the field, so it is the field state's witness).
#[tokio::test]
async fn retag_local_role_moves_weak_and_keeps_channel() {
    let (transport, _r) = transport_with_remotes("submitter", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (slot, _client, mut inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("submitter"));
    let arc_before = std::sync::Arc::as_ptr(&slot);

    // Before the retag: the mesh delivers a Primary-directed frame to the
    // slot; the Observer field is empty.
    assert!(mesh.deliver_local(LocalRole::Primary, frame("pre-retag")));
    assert_eq!(
        sender_of(&inbox.try_recv().expect("primary pre-retag")),
        "pre-retag"
    );
    assert!(
        !mesh.deliver_local(LocalRole::Observer, frame("no-observer-yet")),
        "no observer slot is registered before the retag"
    );

    // Retag primary → observer: the mesh moves the `Weak` to the observer
    // field and flips the slot's role.
    mesh.retag_local_role(LocalRole::Primary, LocalRole::Observer);

    // The slot's role flipped, and it is the SAME `Arc` (identity + channel
    // preserved — the `Node`'s `RoleEntry` still holds it).
    assert_eq!(slot.role(), LocalRole::Observer, "set_role took effect");
    assert_eq!(
        std::sync::Arc::as_ptr(&slot),
        arc_before,
        "same Arc, no recreate"
    );

    // The OLD field is now empty: a Primary-directed delivery finds no slot.
    assert!(
        !mesh.deliver_local(LocalRole::Primary, frame("stale-primary")),
        "the Weak moved off the primary field"
    );
    // The NEW field carries the slot: an Observer-directed delivery reaches
    // the SAME inbox (stable channel across the retag, no delivery gap).
    assert!(mesh.deliver_local(LocalRole::Observer, frame("now-observer")));
    assert_eq!(
        sender_of(&inbox.try_recv().expect("observer post-retag, same channel")),
        "now-observer"
    );
}

/// `route_incoming` demuxes a DIRECTED inbound frame to the one local slot
/// its stamped `target` names — a pure `role → slot` table, never a
/// content classifier and never a same-host comparison. A frame targeting
/// `Secondary(host)` reaches ONLY the secondary slot; the primary slot
/// (a different local role) does not.
#[tokio::test]
async fn route_incoming_directed_delivers_to_target_slot() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_s_slot, _s_client, mut secondary_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));
    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    // An inbound frame stamped for the local secondary.
    mesh.route_incoming(frame_to(
        "wire",
        Destination::Secondary(PeerId::from("host-a")),
    ))
    .await;

    assert_eq!(
        sender_of(
            &secondary_inbox
                .try_recv()
                .expect("secondary got the directed frame")
        ),
        "wire"
    );
    assert!(
        primary_inbox.try_recv().is_none(),
        "a directed Secondary frame must not reach the primary slot"
    );
}

/// `route_incoming` with an `All` target fans to EVERY live local slot —
/// and NEVER re-emits to the wire (an inbound frame already crossed to this
/// peer; re-broadcasting would be the banned re-fan, dirty-D3 / BUG-8).
/// The remote receiver stays empty.
#[tokio::test]
async fn route_incoming_all_fans_to_local_slots_not_wire() {
    let (transport, mut receivers) = transport_with_remotes("host-a", &["remote-1"]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));
    let (_s_slot, _s_client, mut secondary_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));

    mesh.route_incoming(frame_to("wire", Destination::All)).await;

    // Both local slots got it (no origin to exclude — the sender is remote).
    assert_eq!(
        sender_of(&primary_inbox.try_recv().expect("primary got All")),
        "wire"
    );
    assert_eq!(
        sender_of(&secondary_inbox.try_recv().expect("secondary got All")),
        "wire"
    );
    // The wire was NOT touched — no re-broadcast (the raw mpsc receiver is
    // empty: `try_recv` yields `Err(Empty)`).
    assert!(
        receivers.get_mut("remote-1").unwrap().try_recv().is_err(),
        "an inbound All frame is never re-fanned to remotes"
    );
}

/// `route_incoming` on an UNSTAMPED frame (`target == None`) does NOT drop
/// it: it `warn`s (the diagnostic for a production egress that forgot to
/// stamp) and falls back to the documented safe default — fan to every live
/// local slot. A raw-frame test double that injects an unstamped frame must
/// still have it delivered, never swallowed or panicked. Here the lone local
/// primary slot receives the unstamped frame.
#[tokio::test]
async fn route_incoming_none_fans_safely_never_drops() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);
    let (_p_slot, _p_client, mut inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    // `frame(..)` carries no stamped target — the unstamped shape. The safe
    // default fans it to every live local slot rather than dropping it.
    mesh.route_incoming(frame("unstamped")).await;
    assert_eq!(
        sender_of(
            &inbox
                .try_recv()
                .expect("unstamped frame fanned to the primary slot")
        ),
        "unstamped"
    );
}

/// `route_incoming` must NOT silently drop a DIRECTED frame whose named
/// role has no live slot on this host: the frame already crossed the wire
/// to the CORRECT host — the role tag is only the SENDER's (possibly
/// stale) belief of which role lives here. The production shape: a behind
/// secondary addresses the relocated submitter as
/// `Destination::Secondary("setup")` (the digest sender's id), but the
/// submitter swapped its primary into a standalone OBSERVER — pre-fix the
/// `RequestClusterSnapshot` died at the absent-secondary demux and the
/// only ahead replica was unreachable. The no-drop fallback fans to every
/// live local slot (the same documented safe default as the unstamped
/// arm); the role's own handler decides relevance.
#[tokio::test]
async fn route_incoming_directed_role_miss_falls_back_to_live_slots() {
    let (transport, _r) = transport_with_remotes("setup", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    // The relocated submitter hosts ONLY an observer.
    let (_o_slot, _o_client, mut observer_inbox) =
        mesh.register_local_role(LocalRole::Observer, PeerId::from("setup"));

    // An inbound frame addressed to this host under a role it no longer
    // (or never) hosts.
    mesh.route_incoming(frame_to(
        "sec-1",
        Destination::Secondary(PeerId::from("setup")),
    ))
    .await;

    assert_eq!(
        sender_of(&observer_inbox.try_recv().expect(
            "REVERT-CHECK: a directed frame for an absent local role must fall \
             back to the live slots, never silently drop"
        )),
        "sec-1"
    );
}

/// THE slotless-window bug: an ingress frame arriving while this process has
/// ZERO live local slots (the transient promotion / role-swap window — the
/// coordinator slot torn down and not yet recreated) must NOT vanish. The
/// fan reaches nobody, so the mesh HOLDS the frame; the instant the next
/// slot registers it is replayed to that slot. The production shape: a
/// `RequestClusterSnapshot` fanned mid-promotion at zero slots (prod + e2e
/// WARN evidence) — pre-fix the only ahead replica's reply was lost.
#[tokio::test]
async fn route_incoming_holds_frame_while_slotless_then_replays_on_register() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    // ZERO live local slots: the swap tore the coordinator slot down and the
    // replacement has not registered yet. A directed frame arrives.
    mesh.route_incoming(frame_to(
        "snap-req",
        Destination::Secondary(PeerId::from("host-a")),
    ))
    .await;

    // The replacement coordinator registers its slot — the slotless window
    // closes. The held frame must replay to the freshly-registered slot.
    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    assert_eq!(
        sender_of(&primary_inbox.try_recv().expect(
            "a frame received while slotless must be HELD and replayed to the \
             next registered slot, never silently dropped"
        )),
        "snap-req"
    );
}

/// The slotless hold covers the `All` fan path too: an `All`-targeted frame
/// arriving at zero slots is held and replayed on the next slot register
/// (the fan-to-nobody hole is identical regardless of frame target — the
/// fix is generic, not per-kind).
#[tokio::test]
async fn route_incoming_all_held_while_slotless_then_replayed() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    mesh.route_incoming(frame_to("all-while-slotless", Destination::All)).await;

    let (_s_slot, _s_client, mut secondary_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));

    assert_eq!(
        sender_of(
            &secondary_inbox
                .try_recv()
                .expect("an All frame held while slotless must replay on register")
        ),
        "all-while-slotless"
    );
}

/// A frame is HELD only when the fan reaches ZERO live slots: while ≥1 slot
/// is live the existing fan delivers it and nothing is buffered, so a later
/// registration does NOT re-deliver a stale copy (the hold must not double
/// up the steady path).
#[tokio::test]
async fn route_incoming_does_not_hold_when_a_slot_is_live() {
    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    let (_p_slot, _p_client, mut primary_inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    // A live slot exists: the fan delivers immediately, nothing is held.
    mesh.route_incoming(frame_to("delivered-now", Destination::All)).await;
    assert_eq!(
        sender_of(&primary_inbox.try_recv().expect("delivered to the live slot")),
        "delivered-now"
    );

    // Registering a SECOND slot must not replay a phantom held copy.
    let (_s_slot, _s_client, mut secondary_inbox) =
        mesh.register_local_role(LocalRole::Secondary, PeerId::from("host-a"));
    assert!(
        secondary_inbox.try_recv().is_none(),
        "no frame was held while a slot was live, so registration replays nothing"
    );
    assert!(
        primary_inbox.try_recv().is_none(),
        "the already-delivered frame is not re-delivered on a later register"
    );
}

/// The hold buffer is BOUNDED: once it is full, admitting a newer held frame
/// evicts the OLDEST with a WARN naming its kind (never a silent drop). Here
/// we overrun the bound by one while slotless, then register a slot: every
/// held frame EXCEPT the evicted oldest replays, in arrival order, and the
/// overflow WARN fired.
#[tokio::test]
async fn slotless_hold_overflow_drops_oldest_with_warn() {
    use tracing_subscriber::layer::SubscriberExt;
    let capture = crate::test_capture::TargetCapture::for_target(
        "dynrunner_manager_distributed::process::mesh::routing",
    );
    let subscriber = tracing_subscriber::Registry::default().with(capture.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let (transport, _r) = transport_with_remotes("host-a", &[]);
    let mut mesh = Mesh::<TestId, _>::new(transport);

    // Overrun the bound by one while slotless: CAPACITY + 1 frames, tagged
    // by arrival index so we can assert which survived.
    let capacity = super::super::mesh::SLOTLESS_HOLD_CAPACITY;
    for i in 0..=capacity {
        mesh.route_incoming(frame_to(
            &format!("f{i}"),
            Destination::Secondary(PeerId::from("host-a")),
        ))
        .await;
    }

    // The overflow evicted the OLDEST (`f0`) with a WARN.
    let overflow_warns = capture
        .events()
        .into_iter()
        .filter(|e| {
            e.level == tracing::Level::WARN
                && e.event.message.contains("slotless-hold buffer full")
        })
        .count();
    assert_eq!(
        overflow_warns, 1,
        "exactly one overflow eviction WARN must fire (oldest dropped, never silent)"
    );

    // Register a slot; the surviving CAPACITY frames replay in arrival order,
    // and `f0` (the evicted oldest) is absent.
    let (_p_slot, _p_client, mut inbox) =
        mesh.register_local_role(LocalRole::Primary, PeerId::from("host-a"));

    let mut replayed = Vec::new();
    while let Some(msg) = inbox.try_recv() {
        replayed.push(sender_of(&msg).to_string());
    }
    let expected: Vec<String> = (1..=capacity).map(|i| format!("f{i}")).collect();
    assert_eq!(
        replayed, expected,
        "the surviving held frames replay in arrival order; the evicted oldest is gone"
    );
}

// ─── Ingress relay toward the recognized role holder ──────────────────
//
// Production replay (run_20260612_045106): the submitter primary
// relocated to a compute peer long ago; a RESPAWNED replacement
// secondary dials the original setup endpoint (the bootstrap URL its
// respawn provider hands it) and its `SecondaryWelcome` —
// `target = Primary` — lands at the SETUP process, where no Primary
// slot will EVER register again. Pre-fix the frame died in the local
// fan/hold ("HOLDING for replay" at a process whose Primary slot never
// comes); the member joined membership but stayed unassignable for the
// whole run. The fix: a directed role-miss is RELAYED toward the
// role's recognized holder (the coordinator-published
// `RoleHolderView`) before any local fan/hold.

use std::collections::HashMap;

use dynrunner_transport_channel::ChannelPeerTransport;
use tokio::sync::mpsc;

use super::super::mesh_client::RoleInbox;
use super::super::role_slot::RoleSlot;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};

/// One end of a hand-wired multi-process topology: a mesh whose
/// transport inbound is fed by `in_tx` (the test's "wire into this
/// process") and whose outgoing table the caller wired to the OTHER
/// processes' inbound senders.
struct WiredProcess {
    mesh: Mesh<TestId, ChannelPeerTransport<TestId>>,
    in_tx: mpsc::UnboundedSender<DistributedMessage<TestId>>,
}

/// Build one process of a hand-wired topology. `outgoing` maps each
/// remote peer-id to THAT process's inbound sender, so a
/// `send_to_peer` here genuinely arrives at the remote mesh's ingress
/// — a real multi-process pipe, not a drained stub.
fn wired_process(
    local_id: &str,
    outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<TestId>>>,
) -> WiredProcess {
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let transport = ChannelPeerTransport::from_raw_channels(local_id.to_string(), outgoing, in_rx);
    WiredProcess {
        mesh: Mesh::<TestId, _>::new(transport),
        in_tx,
    }
}

/// Register a role on a wired process, returning the slot Arc (held
/// for liveness) and the inbox. Publishes nothing.
fn register(
    p: &mut WiredProcess,
    role: LocalRole,
    host: &str,
) -> (std::sync::Arc<RoleSlot<TestId>>, RoleInbox<TestId>) {
    let (slot, client, inbox) = p.mesh.register_local_role(role, PeerId::from(host));
    drop(client);
    (slot, inbox)
}

/// THE production replay (RED pre-fix): the relocated-away setup
/// process (observer slot only, NO Primary slot, recognition warm —
/// its observer's `PrimaryChanged` hook published the promoted holder)
/// receives a respawned secondary's `SecondaryWelcome` stamped
/// `target = Primary`. The frame must be RELAYED to the compute peer
/// hosting the Primary slot and reach the promoted primary's inbox —
/// pre-fix it was held at the setup process forever (the hold's
/// Primary slot never registers there) and the member zombied.
#[tokio::test]
async fn primary_addressed_frame_at_setup_process_relays_to_promoted_primary() {
    // Compute peer (the promoted primary's host).
    let mut compute = wired_process("secondary-0", HashMap::new());
    let (_p_slot, mut primary_inbox) = register(&mut compute, LocalRole::Primary, "secondary-0");

    // Setup process: observer slot ONLY; its outgoing wire reaches the
    // compute peer's ingress.
    let mut setup = wired_process(
        "setup",
        HashMap::from([("secondary-0".to_string(), compute.in_tx.clone())]),
    );
    let (o_slot, o_client, _o_inbox) = setup
        .mesh
        .register_local_role(LocalRole::Observer, PeerId::from("setup"));
    let _o_slot = o_slot;
    // The observer's recognition published the promoted holder (what
    // `attach_primary_recognition`'s role-change hook does on the
    // relocation's `PrimaryChanged`).
    o_client
        .role_holder_view()
        .publish_primary(Some("secondary-0"), 1);

    // The respawned secondary's welcome arrives at the SETUP ingress
    // (it dialed the original bootstrap endpoint).
    setup
        .in_tx
        .send(welcome_frame("secondary-4").with_target(Destination::Primary))
        .expect("setup inbound open");
    assert!(setup.mesh.recv_dial_and_route().await);

    // The relay must have forwarded it to the compute peer, whose
    // ingress demuxes it to the live Primary slot. Bounded wait so a
    // regression FAILS fast instead of hanging on a held frame.
    let handled = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        compute.mesh.recv_dial_and_route(),
    )
    .await
    .expect(
        "REGRESSION: the Primary-addressed welcome must be RELAYED to the \
         promoted primary's host, not held at the setup process forever",
    );
    assert!(handled);
    let got = primary_inbox
        .try_recv()
        .expect("the promoted primary's inbox must receive the relayed welcome");
    assert_eq!(got.sender_id(), "secondary-4");
    assert!(matches!(
        got,
        DistributedMessage::SecondaryWelcome { .. }
    ));
}

/// The relay is frame-KIND-agnostic (second confirmed production
/// victim: `GracefulAbortRequest`): ANY directed `Primary` frame at a
/// process with no Primary slot relays toward the recognized holder —
/// the decision reads only the routing stamp, never the content.
#[tokio::test]
async fn relay_is_frame_kind_agnostic() {
    let mut compute = wired_process("secondary-0", HashMap::new());
    let (_p_slot, mut primary_inbox) = register(&mut compute, LocalRole::Primary, "secondary-0");

    let mut setup = wired_process(
        "setup",
        HashMap::from([("secondary-0".to_string(), compute.in_tx.clone())]),
    );
    let (o_slot, o_client, _o_inbox) = setup
        .mesh
        .register_local_role(LocalRole::Observer, PeerId::from("setup"));
    let _o_slot = o_slot;
    o_client
        .role_holder_view()
        .publish_primary(Some("secondary-0"), 1);

    setup
        .in_tx
        .send(abort_request_frame("operator").with_target(Destination::Primary))
        .expect("setup inbound open");
    assert!(setup.mesh.recv_dial_and_route().await);

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        compute.mesh.recv_dial_and_route(),
    )
    .await
    .expect("a GracefulAbortRequest must relay exactly like the welcome (kind-agnostic)");
    let got = primary_inbox
        .try_recv()
        .expect("the primary's inbox must receive the relayed abort request");
    assert!(matches!(
        got,
        DistributedMessage::GracefulAbortRequest { .. }
    ));
}

/// Loop safety: two processes with DIVERGENT stale holder views (each
/// believes the other hosts the Primary) must not ping-pong a frame
/// forever. The relay ring lets each process relay a given frame at
/// most ONCE: a → b (a's first sight), b → a (b's first sight), then
/// `a` REVISITS the frame, the ring refuses a second relay, and the
/// frame comes to REST in a's documented local fan (its live observer
/// slot) — no third wire hop.
#[tokio::test]
async fn stale_holder_views_cannot_cycle_a_frame() {
    // Bidirectional wiring: a.outgoing[b] → b.in, b.outgoing[a] → a.in.
    let (a_in_tx, a_in_rx) = mpsc::unbounded_channel();
    let (b_in_tx, b_in_rx) = mpsc::unbounded_channel();
    let a_tr = ChannelPeerTransport::from_raw_channels(
        "proc-a".to_string(),
        HashMap::from([("proc-b".to_string(), b_in_tx)]),
        a_in_rx,
    );
    let b_tr = ChannelPeerTransport::from_raw_channels(
        "proc-b".to_string(),
        HashMap::from([("proc-a".to_string(), a_in_tx.clone())]),
        b_in_rx,
    );
    let mut a = Mesh::<TestId, _>::new(a_tr);
    let mut b = Mesh::<TestId, _>::new(b_tr);

    // Neither process hosts a Primary slot; both have a live observer.
    let (_a_slot, a_client, mut a_observer_inbox) =
        a.register_local_role(LocalRole::Observer, PeerId::from("proc-a"));
    let (_b_slot, b_client, mut b_observer_inbox) =
        b.register_local_role(LocalRole::Observer, PeerId::from("proc-b"));
    // DIVERGENT stale views: each names the OTHER as the holder.
    a_client
        .role_holder_view()
        .publish_primary(Some("proc-b"), 1);
    b_client
        .role_holder_view()
        .publish_primary(Some("proc-a"), 1);

    // A Primary-addressed frame arrives at a.
    a_in_tx
        .send(frame("wanderer").with_target(Destination::Primary))
        .expect("a inbound open");

    // Each hop is bounded so a regression (nothing arriving at the next
    // mesh) FAILS fast instead of hanging the suite.
    const HOP: std::time::Duration = std::time::Duration::from_secs(1);
    // Hop 1: a relays toward its believed holder (b).
    assert!(
        tokio::time::timeout(HOP, a.recv_dial_and_route())
            .await
            .expect("hop 1 must reach b")
    );
    // Hop 2: b's view points back at a — its ring is fresh, so it
    // relays once more.
    assert!(
        tokio::time::timeout(HOP, b.recv_dial_and_route())
            .await
            .expect("hop 2 must reach back to a")
    );
    // Revisit: a has ALREADY relayed this frame — the ring refuses, and
    // the frame rests in a's local fan (the live observer slot).
    assert!(
        tokio::time::timeout(HOP, a.recv_dial_and_route())
            .await
            .expect("the revisit must be handled")
    );

    assert_eq!(
        sender_of(
            &a_observer_inbox
                .try_recv()
                .expect("the frame must come to REST at the revisited process")
        ),
        "wanderer"
    );
    // No third relay: b's transport inbound is empty — the cycle is dead.
    assert!(
        b.transport_mut().try_recv_peer().is_none(),
        "a revisited process must never relay the same frame again (no cycle)"
    );
    assert!(b_observer_inbox.try_recv().is_none());
}

/// Step (3) of the role-miss order is UNCHANGED when the holder is
/// genuinely unknown: a cold recognition view (no `PrimaryChanged`
/// published) must keep the documented fan-to-live-slots default and
/// touch no wire.
#[tokio::test]
async fn cold_holder_view_keeps_the_local_fan_default() {
    let (transport, mut receivers) = transport_with_remotes("setup", &["secondary-0"]);
    let mut mesh = Mesh::<TestId, _>::new(transport);
    let (_o_slot, _o_client, mut observer_inbox) =
        mesh.register_local_role(LocalRole::Observer, PeerId::from("setup"));
    // NO publish: the view is cold.

    mesh.route_incoming(frame_to("early-bird", Destination::Primary))
        .await;

    assert_eq!(
        sender_of(
            &observer_inbox
                .try_recv()
                .expect("holder unknown ⇒ the live-slot fan keeps the frame local")
        ),
        "early-bird"
    );
    assert!(
        receivers
            .get_mut("secondary-0")
            .unwrap()
            .try_recv()
            .is_err(),
        "a cold view must never relay (no guessed holder, no wire traffic)"
    );
}
