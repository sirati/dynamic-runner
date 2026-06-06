//! [`RoleSlot`] — the mesh-addressable inbound endpoint + identity of one
//! local role.
//!
//! # Concern
//!
//! A `RoleSlot` is the single thing the mesh needs to deliver a frame to
//! one local coordinator: its inbound [`mpsc::Sender`] (where a delivered
//! frame goes) plus its identity (`peer_id` + the current [`LocalRole`]).
//! Nothing else — it does NOT know the mesh, the remote connections, or
//! the other roles.
//!
//! # Lifecycle (the ownership split that IS the teardown mechanism)
//!
//! The owning [`super::Process`] holds the `Arc<RoleSlot>`; the
//! [`super::Mesh`] holds a `Weak<RoleSlot>`. Two distinct lifecycle
//! events ride this split:
//!
//! - **Role DEATH / teardown** (clarification H4): the `Process` drops
//!   its `Arc` (a role is wound down — e.g. the old primary applying a
//!   remote `PrimaryChanged`). The mesh's `Weak` then fails to upgrade,
//!   so the next delivery to that slot self-prunes it. There is no
//!   unregister handshake — dropping the `Arc` IS the teardown.
//! - **In-place RETAG** (clarification H5): the submitter primary becomes
//!   the observer WITHOUT dropping the slot — the SAME `Arc`/channel
//!   stays alive and its [`LocalRole`] is flipped atomically via
//!   [`RoleSlot::set_role`]. The inbound `Sender` is the "stable channel"
//!   across the retag; there is no delivery gap and no drop+recreate.
//!
//! Because the retag is a lock-free atomic store and death is the `Arc`
//! drop, the two never collide: a retag is a mutation of a live slot, a
//! death is the disappearance of one.
//!
//! # Boundary
//!
//! Lives in `manager-distributed`. Carries a `DistributedMessage<I>`
//! inbound `Sender` (the existing wire frame the mesh already delivers —
//! NOT a new envelope) and a [`LocalRole`]; it never names a transport
//! type. The per-frame explicit `target` field is a later (C3) concern —
//! this slot's inbound carries the bare frame today.

use std::sync::atomic::{AtomicU8, Ordering};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::DistributedMessage;
use dynrunner_protocol_primary_secondary::address::PeerId;
use tokio::sync::mpsc;

use super::role::LocalRole;

/// The mesh-addressable inbound endpoint + identity of one local role.
///
/// Held as `Arc<RoleSlot<I>>` by the owning process and as
/// `Weak<RoleSlot<I>>` by the mesh (see the module docs for the
/// death-vs-retag lifecycle). The `role` is a lock-free atomic so the
/// submitter primary→observer retag mutates it in place without dropping
/// the channel (H5).
pub struct RoleSlot<I: Identifier> {
    /// The current local role this slot hosts, stored as a stable
    /// [`LocalRole::as_u8`] discriminant so it can be retagged in place.
    role: AtomicU8,
    /// The host peer-id this slot's coordinator runs on. Immutable: a
    /// retag changes the role, never the host.
    peer_id: PeerId,
    /// Where a frame delivered to this role goes. The receive end is the
    /// role's [`super::RoleInbox`]; this is the "stable channel" that
    /// survives a retag.
    inbound: mpsc::UnboundedSender<DistributedMessage<I>>,
}

impl<I: Identifier> RoleSlot<I> {
    /// Construct a slot for `role` at `peer_id` feeding `inbound`.
    ///
    /// Minted together with its [`super::MeshClient`] + [`super::RoleInbox`]
    /// inside [`super::Mesh::register_local_role`] so the trio can never
    /// mismatch (clarification M3); call sites do not build a slot
    /// standalone in production.
    pub fn new(
        role: LocalRole,
        peer_id: PeerId,
        inbound: mpsc::UnboundedSender<DistributedMessage<I>>,
    ) -> Self {
        Self {
            role: AtomicU8::new(role.as_u8()),
            peer_id,
            inbound,
        }
    }

    /// The current local role. Read with `Acquire` so a retag published
    /// by [`RoleSlot::set_role`] is observed by a concurrent reader.
    pub fn role(&self) -> LocalRole {
        // The atomic only ever holds a value written by `set_role` (a
        // valid `LocalRole::as_u8`), so the decode is total in practice;
        // a foreign value would be a memory-safety bug elsewhere, and
        // defaulting it silently would hide that — fall back to Observer
        // (the apply-only, least-authority role) rather than panic on the
        // delivery hot path.
        LocalRole::from_u8(self.role.load(Ordering::Acquire)).unwrap_or(LocalRole::Observer)
    }

    /// Retag this slot's role IN PLACE (clarification H5).
    ///
    /// The submitter primary→observer handoff calls this: the `Arc`, the
    /// `peer_id`, and the inbound `Sender` are all preserved — only the
    /// role flips. `Release` so the new role is published to a concurrent
    /// [`RoleSlot::role`] reader. This is NOT teardown (which is the `Arc`
    /// drop); a retagged slot is the same live endpoint under a new role.
    pub fn set_role(&self, role: LocalRole) {
        self.role.store(role.as_u8(), Ordering::Release);
    }

    /// The host peer-id this slot's coordinator runs on.
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Hand a frame to this role's inbound stream.
    ///
    /// `Err` iff the receive end ([`super::RoleInbox`]) was dropped — the
    /// "this role's coordinator loop is gone" signal the mesh treats the
    /// same as a failed `Weak`-upgrade (collect-then-prune). A live slot
    /// with a dropped inbound is a transient between coordinator exit and
    /// `Arc` drop; the mesh prunes on either. The frame is unrecoverable
    /// on failure (its only consumer is gone), so the error is a small
    /// reason string — the mesh only inspects `is_err`.
    pub fn deliver(&self, frame: DistributedMessage<I>) -> Result<(), String> {
        self.inbound
            .send(frame)
            .map_err(|_| "role inbound receiver dropped".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_protocol_primary_secondary::KeepaliveRole;

    #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    struct TestId(String);

    fn frame(sender: &str) -> DistributedMessage<TestId> {
        DistributedMessage::Keepalive {
            target: None,
            sender_id: sender.to_string(),
            timestamp: 1.0,
            secondary_id: sender.to_string(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Secondary,
        }
    }

    /// A fresh slot reports its construction role + host, and delivers a
    /// frame to the paired receiver.
    #[test]
    fn new_slot_delivers_to_inbound() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let slot = RoleSlot::<TestId>::new(LocalRole::Secondary, PeerId::from("host-a"), tx);

        assert_eq!(slot.role(), LocalRole::Secondary);
        assert_eq!(slot.peer_id(), &PeerId::from("host-a"));

        slot.deliver(frame("host-a")).expect("inbound is live");
        let got = rx.try_recv().expect("frame delivered");
        match got {
            DistributedMessage::Keepalive { sender_id, .. } => assert_eq!(sender_id, "host-a"),
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    /// `set_role` flips the role IN PLACE — the SAME inbound `Sender`
    /// keeps delivering across the retag (H5: stable channel). The
    /// submitter primary→observer handoff relies on exactly this: no
    /// drop, no delivery gap.
    #[test]
    fn set_role_retags_in_place_keeping_channel() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let slot = RoleSlot::<TestId>::new(LocalRole::Primary, PeerId::from("submitter"), tx);
        assert_eq!(slot.role(), LocalRole::Primary);

        // Deliver before the retag.
        slot.deliver(frame("before")).expect("inbound live pre-retag");

        // Retag primary → observer in place.
        slot.set_role(LocalRole::Observer);
        assert_eq!(slot.role(), LocalRole::Observer);
        assert_eq!(
            slot.peer_id(),
            &PeerId::from("submitter"),
            "retag preserves the host id"
        );

        // The same channel still delivers after the retag.
        slot.deliver(frame("after")).expect("inbound live post-retag");

        let first = rx.try_recv().expect("pre-retag frame");
        let second = rx.try_recv().expect("post-retag frame");
        for (msg, expect) in [(first, "before"), (second, "after")] {
            match msg {
                DistributedMessage::Keepalive { sender_id, .. } => assert_eq!(sender_id, expect),
                other => panic!("unexpected frame: {other:?}"),
            }
        }
    }

    /// Once the receive end is dropped, `deliver` returns the frame back
    /// as `Err` — the "coordinator loop gone" signal the mesh prunes on.
    #[test]
    fn deliver_errors_when_inbound_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel();
        let slot = RoleSlot::<TestId>::new(LocalRole::Observer, PeerId::from("obs"), tx);
        drop(rx);
        let returned = slot.deliver(frame("orphan"));
        assert!(returned.is_err(), "delivery to a dropped inbox must fail");
    }
}
