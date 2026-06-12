//! Role-demux routing for [`Mesh`]: directed loopback-vs-remote
//! dispatch, the origin-excluded `All` fan, the ingress demux, and the
//! in-place submitter→observer retag.
//!
//! # Concern
//!
//! ONE concern: given an origin role + a [`Destination`] (or a received
//! frame's stamped target), decide WHICH local slot(s) and/or remote
//! connection a frame reaches — and move a live slot between role fields
//! when the submitter-primary relocates. The §14 fix lives in [`Mesh::broadcast`]'s
//! role-keyed (NEVER peer-keyed) exclusion; the no-re-broadcast invariant
//! lives in [`Mesh::route_incoming`] (an inbound frame is never re-fanned to
//! remotes).

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::address::Destination;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerTransport};

use super::super::role::LocalRole;
use super::Mesh;

impl<I: Identifier, Tr: PeerTransport<I>> Mesh<I, Tr> {
    /// Deliver a frame to ONE local role slot (loopback).
    ///
    /// Upgrades the target role's `Weak`; on success hands the frame to
    /// the slot's inbound. On upgrade FAILURE (the role's `Arc` was
    /// dropped — teardown, H4) or a dropped inbound, self-prunes that
    /// slot's `Weak` and returns `false` — NEVER panics, NEVER prunes
    /// while iterating (clarification BUG-2). Returns `true` iff the frame
    /// reached a live slot.
    pub fn deliver_local(&mut self, target: LocalRole, frame: DistributedMessage<I>) -> bool {
        let slot = match target {
            LocalRole::Primary => &self.primary,
            LocalRole::Secondary => &self.secondary,
            LocalRole::Observer => &self.observer,
        };
        let Some(weak) = slot else {
            return false;
        };
        let prune = match weak.upgrade() {
            Some(arc) => arc.deliver(frame).is_err(),
            None => true,
        };
        if prune {
            self.clear_slot(target);
            return false;
        }
        true
    }

    /// Route a directed frame from `origin` to `target`: loopback to a
    /// local slot when the target role's host is local, else remote by id.
    /// [`Destination::All`] fans (origin-excluded — see [`Self::broadcast`]);
    /// a directed delivery NEVER excludes the origin. `origin` is carried
    /// only for the `All` fan's origin-exclusion (clarification BUG-1).
    pub async fn dispatch(
        &mut self,
        origin: LocalRole,
        target: Destination,
        frame: DistributedMessage<I>,
    ) -> Result<(), String> {
        match &target {
            Destination::All => {
                self.broadcast(origin, frame).await;
                Ok(())
            }
            Destination::Primary => {
                // Primary is id-less on the wire: a local primary is the
                // loopback target. A REMOTE primary needs the resolved host
                // id carried on the frame — a C3 seam (no `target` field
                // yet); until then C2's egress collapse resolves
                // `Destination::Primary` to a concrete id BEFORE dispatch,
                // so this arm is unreachable in the wired system. Surface
                // it loudly rather than silently drop.
                if self.deliver_local(LocalRole::Primary, frame) {
                    return Ok(());
                }
                Err(
                    "Mesh::dispatch: remote Destination::Primary requires the resolved \
                     host id (C3 frame target)"
                        .to_string(),
                )
            }
            Destination::Secondary(id) | Destination::Observer(id) => {
                let role = LocalRole::from_destination(&target)
                    .expect("Secondary/Observer always carry a role");
                if self.is_local_host(id) && self.deliver_local(role, frame.clone()) {
                    return Ok(());
                }
                self.transport.send_to_peer(id.as_str(), frame).await
            }
        }
    }

    /// Fan a frame to every remote connection AND every local slot EXCEPT
    /// the originating role/slot (clarification BUG-1).
    ///
    /// The exclusion keys on the `origin` ROLE — NEVER on the originating
    /// PEER. A same-peer secondary's `All` frame therefore still reaches
    /// the local primary slot (the §14 fix: the local primary's death
    /// clock is refreshed by its own host's secondary keepalive). Local
    /// upgrade failures self-prune (collect-then-prune, BUG-2).
    pub async fn broadcast(&mut self, origin: LocalRole, frame: DistributedMessage<I>) {
        // Remote fan: the transport broadcasts to every connection
        // role-blind. The same-peer self is not a remote connection, so
        // no peer is wrongly excluded here.
        let _ = self.transport.broadcast(frame.clone()).await;

        // Local fan: every local slot except the originating role.
        self.fan_local(Some(origin), frame);
    }

    /// Route ONE frame received off the wire to the right LOCAL slot(s),
    /// reading the frame's resolved [`Destination`] target (the C3 field
    /// stamped by the sender's egress) — a pure `role → slot` table, NEVER
    /// a content classifier and NEVER an `if host == local_id` test.
    ///
    /// This is the mesh-pump's INGRESS demux. It is local-only: an inbound
    /// frame has already crossed the wire to THIS peer, so it is NEVER
    /// re-fanned to remotes (the no-re-broadcast invariant, dirty-D3 /
    /// BUG-8). The `origin`-exclusion of [`Self::broadcast`] does not apply
    /// — the originator is a remote peer, not a local role.
    ///
    /// - directed `Some(Primary|Secondary|Observer)` → [`Self::deliver_local`]
    ///   to that one role's slot. When that role has NO live local slot the
    ///   frame is NOT dropped: it already crossed the wire to the correct
    ///   HOST, and the role tag is only the SENDER's (possibly stale) belief
    ///   of which role lives here — e.g. a behind secondary addressing the
    ///   relocated submitter as `Secondary(setup)` while the submitter
    ///   swapped its primary into a standalone OBSERVER. Fall back to the
    ///   documented safe default (fan to every live local slot, WARN) so the
    ///   receiving role's own handler decides relevance instead of the mesh
    ///   silently eating the frame.
    /// - `Some(All)` → local fan to every live slot.
    /// - `None` (transitional: a frame stamped before the egress rewire
    ///   lands) → a LOUD `debug_assert!` + `warn`, then the documented safe
    ///   default: fan to every live LOCAL slot so NO local coordinator
    ///   misses a frame. We never silently drop. Once every egress edge
    ///   stamps `Some(resolved)` (the next coordinator-rewire wave), this
    ///   arm is unreachable and the `debug_assert!` guards that invariant.
    pub fn route_incoming(&mut self, frame: DistributedMessage<I>) {
        match frame.target() {
            Some(Destination::All) => self.fan_local_or_hold(frame),
            Some(dst) => {
                // `from_destination` is total over the non-`All` directed
                // variants; the `All` arm is handled above.
                let role = LocalRole::from_destination(dst)
                    .expect("non-All directed Destination always carries a role");
                // Role-miss no-drop fallback: a directed frame for a role
                // with no LIVE local slot reflects the sender's stale role
                // knowledge of this host (the host-level addressing is
                // already satisfied — the frame arrived here). Fan it to
                // the live slots instead of dropping. The presence check is
                // upfront so the hot path (slot present) pays no clone; the
                // transient "slot present but its inbox just closed" prune
                // inside `deliver_local` keeps its existing semantics.
                let role_has_live_slot = self
                    .slot_for(role)
                    .map(|weak| weak.upgrade().is_some())
                    .unwrap_or(false);
                if role_has_live_slot {
                    self.deliver_local(role, frame);
                } else {
                    if let Some(suppressed) = self.ingress_fallback_warn.permit() {
                        tracing::warn!(
                            kind = ?frame.msg_type(),
                            target = ?dst,
                            suppressed_since_last_warn = suppressed,
                            "mesh ingress: directed frame names a role with no live \
                             local slot (stale sender-side role knowledge); fanning \
                             to every live local slot — or HOLDING it for replay if \
                             this process is momentarily slotless — rather than \
                             dropping it"
                        );
                    }
                    self.fan_local_or_hold(frame);
                }
            }
            None => {
                // A frame arriving unstamped is either a real production
                // egress bug OR a test double that injects raw wire frames
                // without going through a coordinator's stamping egress. The
                // documented safe default — fan to every live local slot —
                // handles BOTH without dropping the frame, so we WARN (the
                // diagnostic for the production-bug case) rather than panic
                // (which a debug_assert would, killing legitimate raw-frame
                // test doubles). Production egress still stamps every edge;
                // this arm is the no-drop backstop, not the happy path.
                if let Some(suppressed) = self.ingress_fallback_warn.permit() {
                    tracing::warn!(
                        kind = ?frame.msg_type(),
                        suppressed_since_last_warn = suppressed,
                        "mesh ingress: frame has no routing target (pre-stamp \
                         transitional); fanning to every local slot — or HOLDING it \
                         for replay if this process is momentarily slotless — rather \
                         than dropping it"
                    );
                }
                self.fan_local_or_hold(frame);
            }
        }
    }

    /// Deliver a frame to every LIVE local slot, optionally excluding one
    /// role. Collect-then-prune over a stale `Weak` (never prune during
    /// the upgrade walk — BUG-2). This is the local half of both the
    /// egress `All`-fan (`exclude = Some(origin)`, BUG-1) and the ingress
    /// fan (`exclude = None`). It NEVER touches the wire — the remote half
    /// of an egress broadcast is the caller's separate concern.
    ///
    /// Returns the number of LIVE slots the frame reached. The ingress
    /// caller ([`Self::route_incoming`]) reads a `0` as "this process is
    /// momentarily slotless" and HOLDS the frame instead of dropping it; the
    /// egress broadcast ignores the count (a same-host fan reaching only the
    /// wire is legitimate).
    fn fan_local(&mut self, exclude: Option<LocalRole>, frame: DistributedMessage<I>) -> usize {
        let mut to_prune: Vec<LocalRole> = Vec::new();
        let mut delivered = 0usize;
        for role in [
            LocalRole::Primary,
            LocalRole::Secondary,
            LocalRole::Observer,
        ] {
            if Some(role) == exclude {
                continue;
            }
            if let Some(weak) = self.slot_for(role) {
                match weak.upgrade() {
                    Some(arc) => {
                        if arc.deliver(frame.clone()).is_err() {
                            to_prune.push(role);
                        } else {
                            delivered += 1;
                        }
                    }
                    None => to_prune.push(role),
                }
            }
        }
        for role in to_prune {
            self.clear_slot(role);
        }
        delivered
    }

    /// The ingress fan with the slotless no-drop guarantee: fan to every live
    /// local slot, and if NONE was live, HOLD the frame so the next slot
    /// registration replays it ([`Self::hold_slotless`] /
    /// [`Self::drain_slotless_hold`]). Used by every [`Self::route_incoming`]
    /// fan path (directed role-miss, `All`, unstamped) so the
    /// transient-zero-slots window during a promotion / role swap can never
    /// silently eat a frame. The egress broadcast does NOT route through here
    /// — its zero-local-slot fan is legitimate (the frame went to the wire).
    fn fan_local_or_hold(&mut self, frame: DistributedMessage<I>) {
        if self.fan_local(None, frame.clone()) == 0 {
            self.hold_slotless(frame);
        }
    }

    /// Buffer an ingress frame that arrived while this process had ZERO live
    /// local slots (a transient promotion / role-swap window), to be replayed
    /// by [`Self::drain_slotless_hold`] when the next slot registers.
    ///
    /// Bounded ([`super::SLOTLESS_HOLD_CAPACITY`]): on overflow the OLDEST
    /// held frame is evicted with a (throttled) WARN naming its kind/target —
    /// never a silent drop. The hold itself also WARNs (throttled) so the
    /// operator sees the slotless window without one WARN per frame at wire
    /// rate during a long swap.
    fn hold_slotless(&mut self, frame: DistributedMessage<I>) {
        if self.slotless_hold.len() == super::SLOTLESS_HOLD_CAPACITY
            && let Some(evicted) = self.slotless_hold.pop_front()
        {
            tracing::warn!(
                kind = ?evicted.msg_type(),
                target = ?evicted.target(),
                capacity = super::SLOTLESS_HOLD_CAPACITY,
                "mesh ingress: slotless-hold buffer full — dropping the OLDEST \
                 held frame to admit a newer one (the swap window outran the \
                 buffer bound)"
            );
        }
        let kind = frame.msg_type();
        self.slotless_hold.push_back(frame);
        if let Some(suppressed) = self.slotless_hold_warn.permit() {
            tracing::warn!(
                kind = ?kind,
                held = self.slotless_hold.len(),
                suppressed_since_last_warn = suppressed,
                "mesh ingress: directed frame arrived with NO live local slot \
                 (transient promotion / role-swap window); HOLDING it for replay \
                 when the next slot registers rather than dropping it"
            );
        }
    }

    /// Replay every held ingress frame through the live local fan, in arrival
    /// order, then clear the buffer. Called the instant a slot REGISTERS
    /// ([`super::Mesh::register_local_role`]) — the only event that ends a
    /// slotless window (a retag merely MOVES an already-live slot between
    /// fields, so the process was never slotless and the buffer is empty
    /// there). A frame that STILL finds zero live slots (a pathological race
    /// where the just-registered slot's `Arc` already died) is re-held by
    /// `fan_local_or_hold`; the drain takes a snapshot first, so this is a
    /// single bounded pass, never an unbounded loop.
    pub(super) fn drain_slotless_hold(&mut self) {
        if self.slotless_hold.is_empty() {
            return;
        }
        let held: Vec<DistributedMessage<I>> = self.slotless_hold.drain(..).collect();
        for frame in held {
            self.fan_local_or_hold(frame);
        }
    }

    /// Re-point a live local slot from the `old` role's field to the `new`
    /// role's field, atomically with the slot's in-place
    /// [`RoleSlot::set_role`] retag (clarification D-RETAG / H5).
    ///
    /// C0 keys slots by FIELD (`primary`/`secondary`/`observer`), not by
    /// the slot's live `role()`. So a bare `slot.set_role(new)` would leave
    /// this mesh demuxing `new`-role frames to a `None` field and
    /// `old`-role frames to the now-retagged slot. This method moves the
    /// `Weak` from the `old` field to the `new` field AND flips the slot's
    /// role, in one step, so the demux stays correct across the
    /// submitter-primary→observer handoff. The SAME `Arc`/channel is
    /// preserved — the [`super::RoleInbox`] drains uninterrupted (stable
    /// channel, no delivery gap).
    ///
    /// `old == new` is a no-op. If no slot occupies the `old` field
    /// (already torn down or never registered), the move is a no-op — there
    /// is nothing live to retag — and the `new` field is left as-is. The
    /// caller (the [`super::Node`] swap) holds the matching `Arc` and is
    /// the authority on whether a retag is appropriate.
    pub fn retag_local_role(&mut self, old: LocalRole, new: LocalRole) {
        if old == new {
            return;
        }
        let Some(weak) = self.take_slot(old) else {
            return;
        };
        // Flip the live slot's role in place (the stable-channel retag),
        // then re-point the mesh `Weak` into the `new` field. A pre-existing
        // `Weak` in the `new` field is REPLACED — its `Arc`, once the caller
        // drops it, simply never upgrades again (the same semantics as a
        // second `register_local_role`).
        if let Some(arc) = weak.upgrade() {
            arc.set_role(new);
        }
        self.set_slot(new, weak);
    }
}
