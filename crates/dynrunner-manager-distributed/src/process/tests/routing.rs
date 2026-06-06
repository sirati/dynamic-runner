//! Routing tests for the `process/` mesh: the broadcast origin-exclusion
//! (BUG-1), directed loopback-vs-remote dispatch, the queued client send
//! (M4), and the in-place retag (H5). Shares the fixtures in the parent
//! [`super`] test module.

use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};

use super::super::mesh::Mesh;
use super::super::role::LocalRole;
use super::{TestId, frame, sender_of, transport_with_remotes};

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
    assert_eq!(sender_of(&rx.try_recv().expect("remote got All")), "pri-all");
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
        sender_of(&secondary_inbox.try_recv().expect("local secondary loopback")),
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
        sender_of(&primary_inbox.try_recv().expect("primary got the queued send")),
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
