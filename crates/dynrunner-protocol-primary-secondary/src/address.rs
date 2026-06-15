//! Typed addressing primitives for the primary/secondary protocol.
//!
//! # Concern
//!
//! This module owns the *addressing vocabulary* — the pure-data
//! [`Destination`] enum that describes **who** a message is for,
//! decoupled from **how** the transport gets it there, plus the
//! egress-edge [`resolve_destination`] that turns a [`Destination`]
//! into a concrete [`SendTarget`] the `PeerId`-only transport can act
//! on. The transport itself never resolves a role; resolution is the
//! coordinator edge's job (transport ⊥ roles).
//!
//! [`RoleTable`] and [`RoleChangeHookRegistrar`] live here too because
//! they are the *replicated role-state* shape — they describe **who
//! holds which role** in a form the manager edge can read to resolve
//! [`Destination::Primary`] without depending on the
//! manager-distributed crate (which owns the actual replication
//! mechanics via `ClusterState`). The registrar trait is the boundary
//! `ClusterState` implements. No transport subscribes to it: the edge
//! reads the table to resolve, the transport sees only a [`PeerId`].

/// Opaque peer identifier — the host peer-id of a node in the mesh.
///
/// A newtype over the peer-id string representation (the same `String`
/// carried by [`Destination::Secondary`] / [`Destination::Observer`]
/// and [`RoleTable::primary`]). It exists so the typed [`Destination`]
/// vocabulary never traffics in bare `String`s: a peer-id is a distinct
/// domain value, not "any string", and the type system should say so.
///
/// # Why a newtype rather than `String`
///
/// The mesh addresses hosts by id, and several distinct string-shaped
/// concepts coexist in this protocol (peer ids, role names, message
/// types). A newtype makes "this is a peer-id" unambiguous at every API
/// boundary and lets a misuse (passing, say, a role name where a host id
/// is expected) fail to compile rather than mis-route at runtime.
///
/// # Contract
///
/// - Cheap to clone (`String` inside); usable directly as a `HashMap`
///   key (`Hash + Eq`), which the mesh connection/keepalive tables need.
/// - `Display` / [`PeerId::as_str`] / `AsRef<str>` expose the underlying
///   id for logging and for transports that still key tables by `&str`.
/// - `From<String>` / `From<&str>` make migrating a call site that holds
///   a raw id a zero-friction wrap.
/// - **Serde transparent**: the wire/JSON form is exactly the inner
///   string, so a `PeerId` is interchangeable on the wire with the bare
///   id strings the protocol carries today — no envelope churn when call
///   sites migrate from `String` to `PeerId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct PeerId(String);

impl PeerId {
    /// Borrow the underlying peer-id string. The escape hatch for
    /// transports that still key their tables by `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the underlying `String`.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for PeerId {
    fn from(s: String) -> Self {
        PeerId(s)
    }
}

impl From<&str> for PeerId {
    fn from(s: &str) -> Self {
        PeerId(s.to_string())
    }
}

impl AsRef<str> for PeerId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Typed destination for a mesh send — the one addressing vocabulary
/// the transport/router layer will resolve against.
///
/// # Resolution contract (honored later by the transport/router layer)
///
/// This enum is **pure data**: it names *who* a frame is for. It does
/// **not** resolve anything itself — resolution (id lookup, role demux,
/// loopback short-circuit, broadcast fan-out) is the transport/router
/// layer's job and lands in a later leaf. The contract that layer must
/// honor:
///
/// - [`Destination::Primary`] resolves via `current_primary()` / the
///   role table to the **host peer-id** the primary runs on, and the
///   `Router` routes to that host. The primary is *this variant* — there
///   is no `"primary"` string literal; the literal id is never spoken.
/// - [`Destination::Secondary`] / [`Destination::Observer`] carry the
///   target **host peer-id** AND, on a multi-role host (one peer running
///   several coordinators), select **which receiving coordinator** the
///   frame is demuxed to by its role — replacing the ad-hoc
///   primary-facing check. The id selects the host; the variant selects
///   the role at that host.
/// - [`Destination::All`] is the cluster broadcast — fan out to every
///   peer (subsuming the legacy mesh / all-secondaries scopes).
/// - **Loopback is implicit**: when a resolved host id equals the local
///   peer-id, the transport delivers locally rather than over the wire.
///   Call sites never special-case "is this me?" — they address by
///   destination and the resolver short-circuits.
///
/// Serde derives carry `Destination` for any frame that needs to record
/// an intended destination; resolution remains a send-time decision, not
/// a wire-format concern.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Destination {
    /// Whichever host currently holds the primary role, resolved via
    /// `current_primary()` / the role table to that host's peer-id.
    Primary,
    /// The secondary coordinator on the host with this peer-id (role
    /// demux selects the secondary at a multi-role host).
    Secondary(PeerId),
    /// The observer coordinator on the host with this peer-id (role
    /// demux selects the observer at a multi-role host).
    Observer(PeerId),
    /// Cluster broadcast — every peer.
    All,
}

/// The concrete dispatch a resolved [`Destination`] maps to at the
/// egress edge — what the coordinator hands the `PeerId`-only transport.
///
/// This is the output of [`resolve_destination`]: the role→peer
/// resolution that used to live *inside* the transport (the
/// `Address::Role(Primary)` → role-cache lookup arm) now produces one
/// of these three transport-agnostic outcomes at the coordinator
/// boundary, so the transport never sees a role.
///
/// - [`SendTarget::Peer`] — deliver to this concrete host peer-id via
///   the transport's by-id send.
/// - [`SendTarget::Broadcast`] — fan out to the whole mesh via the
///   transport's broadcast.
/// - [`SendTarget::Loopback`] — the resolved host id equals the local
///   id; deliver to the same-peer coordinator without a wire hop. The
///   edge owns the loopback delivery (the implicit "resolved host id ==
///   local id" rule), NOT the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendTarget {
    /// Resolved to the local node — deliver in-process, no wire hop.
    Loopback,
    /// Resolved to a concrete remote host peer-id.
    Peer(PeerId),
    /// The cluster broadcast.
    Broadcast,
}

/// Resolve a typed [`Destination`] to a concrete [`SendTarget`] at the
/// egress edge.
///
/// # Single concern
///
/// This is THE role→peer resolution, lifted out of the transport. The
/// coordinator supplies the role facts it owns — `current_primary` (from
/// its `ClusterState` role table), `bootstrap_primary` (the id of the
/// peer dialled at bootstrap, known before any `PrimaryChanged` warms
/// `current_primary`), and `local_id` (this host's peer-id) — and gets
/// back a transport-agnostic [`SendTarget`]. The transport then sees
/// only a `PeerId` (or a broadcast); it never resolves a role.
///
/// # Resolution rules
///
/// - [`Destination::Primary`] resolves to `current_primary ??
///   bootstrap_primary` (by-id, cold-cache-safe — before any
///   `PrimaryChanged` is applied, `current_primary` is `None` and the
///   bootstrap-dialled primary is the holder). `None` only when BOTH are
///   absent (a node with no current primary and no bootstrap link — the
///   honest "no route to primary" the caller surfaces as `Err`, matching
///   the prior cold-cache hard error).
/// - [`Destination::Secondary`] / [`Destination::Observer`] resolve to
///   the carried host peer-id directly (the id IS the host). Egress
///   resolution is identical for both variants — the secondary/observer
///   distinction selects the receiving coordinator at the INGRESS edge,
///   not the outbound target.
/// - [`Destination::All`] is the broadcast.
/// - Any resolved host id equal to `local_id` short-circuits to
///   [`SendTarget::Loopback`] (the implicit loopback rule) so call sites
///   never special-case "is this me?".
pub fn resolve_destination(
    dst: Destination,
    current_primary: Option<&str>,
    bootstrap_primary: Option<&str>,
    local_id: &str,
) -> Option<SendTarget> {
    let to_target = |host: &str| {
        if host == local_id {
            SendTarget::Loopback
        } else {
            SendTarget::Peer(PeerId::from(host))
        }
    };
    match dst {
        Destination::Primary => {
            let host = current_primary.or(bootstrap_primary)?;
            Some(to_target(host))
        }
        Destination::Secondary(id) | Destination::Observer(id) => Some(to_target(id.as_str())),
        Destination::All => Some(SendTarget::Broadcast),
    }
}

/// Project a resolved [`SendTarget`] back into the role-bearing
/// [`Destination`] the mesh-pump's `dispatch` routes by — the EGRESS
/// COLLAPSE that keeps the routing-target and the C3 frame stamp distinct.
///
/// # Two `Destination`s: routing-target vs C3 stamp
///
/// A coordinator's egress produces TWO role-bearing destinations from one
/// `Destination` intent:
///
/// - The **routing-target** the local mesh-pump's `Mesh::dispatch`
///   matches on to pick loopback-vs-wire. The receiver-demuxes-by-stamp /
///   sender-dispatches-by-routing-target asymmetry means a REMOTE
///   `Destination::Primary` must carry the resolved host id at this
///   layer; otherwise the pump cannot route it (its Primary arm is
///   loopback-only). Hence the collapse:
///   `(Destination::Primary, SendTarget::Peer(id)) → Destination::Secondary(id)`.
///   For SendTarget::Loopback the routing-target stays bare
///   `Destination::Primary` (so the pump enters its Primary arm and calls
///   `deliver_local(LocalRole::Primary)` against the same-host primary
///   slot — collapsing to `Destination::Secondary(local_id)` would land
///   the loopback in the Secondary slot, the wrong coordinator).
/// - The **C3 stamp** on the frame, which the RECEIVER's `route_incoming`
///   reads to demux to one of its own slots. The stamp is always the
///   original role-bearing intent (`Destination::Primary` for a primary
///   send, etc.), so the receiver picks the right LocalRole regardless of
///   the routing-target the sender used to reach it.
///
/// # Why centralized here
///
/// Two coordinator edges (`SecondaryCoordinator::send_to`,
/// `ObserverCoordinator::send_to`) used to do this collapse inline (#551
/// uncovered that the dispatch-failure path was silently dropping
/// loopback races; the collapse itself was correct, but it was duplicated
/// — the same imports + match pattern at the same call-site level in two
/// files, an anti-spec). Lifting it here keeps ONE owner of the
/// "routing-target vs C3 stamp" dual semantic, so future variant
/// additions (e.g. the C3-frame-target rewire) need only one update.
///
/// # `SendTarget::Broadcast`
///
/// `Destination::All` is its own routing-target (broadcast fan); the
/// collapse is a no-op for it. The caller is responsible for stamping
/// `Destination::All` on the frame.
pub fn routing_target_for(intent: &Destination, resolved: &SendTarget) -> Destination {
    match (intent, resolved) {
        // The REMOTE-primary collapse: route under the resolved host id
        // so the mesh delivers it by-id over the wire; the C3 stamp
        // stays `Destination::Primary` (handled by the caller via
        // `msg.with_target(intent)`) so the receiver's pump demuxes to
        // its primary slot.
        (Destination::Primary, SendTarget::Peer(id)) => Destination::Secondary(id.clone()),
        // Loopback Primary keeps the bare intent so dispatch's Primary
        // arm fires (loopback to LocalRole::Primary). All other
        // intent/resolved combinations route under the original intent
        // (which already carries the host id for Secondary/Observer or
        // is the broadcast for All).
        _ => intent.clone(),
    }
}

/// Replicated role bookkeeping. The authoritative owner is the
/// downstream `ClusterState`; the manager edge reads it to resolve
/// [`Destination::Primary`] (the transport never does).
///
/// `observers` is reserved for the future observer story; no current
/// mutation populates it. Keeping the field present here means the
/// table shape stays stable when observer roles are tracked.
///
/// `can_be_primary` is the SEPARATE, EXPLICIT, first-class per-peer
/// capability set: the authoritative "may this peer ever host the
/// primary role" property. It is NOT deduced from membership / liveness
/// / observer status and is NOT a transport property (the transport
/// edge never reads it) — it is set explicitly by a peer at join (riding
/// `ClusterMutation::PeerJoined { can_be_primary }`, the exact twin of
/// `is_observer` → `observers`) and updatable at runtime by a client via
/// `ClusterMutation::SetCanBePrimary`. Membership in this set is the
/// single source of truth for primary capability.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleTable {
    pub primary: Option<String>,
    pub observers: std::collections::HashSet<String>,
    pub can_be_primary: std::collections::HashSet<String>,
}

/// Boundary trait that downstream replicated-state owners implement
/// so edge consumers can subscribe to [`RoleTable`] mutations without
/// depending on the manager crate's concrete `ClusterState`. The
/// single implementer in production is
/// `dynrunner_manager_distributed::ClusterState`; test fixtures may
/// also implement it.
///
/// The trait deliberately exposes only **hook registration** — not
/// table read access. Readers go through `ClusterState::role_table()`
/// directly. This way the trait stays a one-method boundary that
/// doesn't leak the replicated-state internals to the protocol crate.
pub trait RoleChangeHookRegistrar {
    /// Register a callback fired (synchronously, from inside the
    /// state's apply loop) AFTER any [`RoleTable`] mutation. The
    /// callback observes the *post*-mutation table.
    ///
    /// Hooks accumulate; implementers must NOT clear prior registrants.
    fn register_role_change_hook(&mut self, hook: Box<dyn Fn(&RoleTable) + Send + Sync + 'static>);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `PeerId` serializes transparently (as the bare inner string) and
    /// round-trips through serde unchanged. The transparent form is the
    /// contract that lets a `PeerId` replace a raw id `String` on the
    /// wire with no envelope churn — pin it so a future derive change
    /// that adds a wrapper object trips this test.
    #[test]
    fn peer_id_serde_round_trip_is_transparent() {
        let id = PeerId::from("node-3");
        let json = serde_json::to_string(&id).expect("serialize PeerId");
        assert_eq!(json, "\"node-3\"", "PeerId must serialize as a bare string");
        let back: PeerId = serde_json::from_str(&json).expect("deserialize PeerId");
        assert_eq!(id, back);
    }

    /// `PeerId` is usable as a `HashMap` key — the mesh
    /// connection/keepalive tables depend on `Hash + Eq`. Distinct ids
    /// occupy distinct slots; equal ids collide as expected.
    #[test]
    fn peer_id_is_hash_map_key() {
        use std::collections::HashMap;
        let mut map: HashMap<PeerId, u32> = HashMap::new();
        map.insert(PeerId::from("a"), 1);
        map.insert(PeerId::from("b"), 2);
        // Re-inserting an equal key overwrites rather than adding.
        map.insert(PeerId::from("a".to_string()), 10);

        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&PeerId::from("a")), Some(&10));
        assert_eq!(map.get(&PeerId::from("b")), Some(&2));
        assert_eq!(map.get(&PeerId::from("missing")), None);
    }

    /// `PeerId` exposes its inner string via `as_str` / `AsRef` /
    /// `Display` identically, and the `From` conversions agree.
    #[test]
    fn peer_id_accessors_agree() {
        let id = PeerId::from("worker-9");
        assert_eq!(id.as_str(), "worker-9");
        assert_eq!(AsRef::<str>::as_ref(&id), "worker-9");
        assert_eq!(id.to_string(), "worker-9");
        assert_eq!(PeerId::from("worker-9".to_string()), id);
        assert_eq!(id.clone().into_string(), "worker-9");
    }

    /// `Destination` round-trips through serde for every variant and
    /// preserves structural equality. Pins the `Serialize/Deserialize`
    /// + `PartialEq` derives the transport/router layer will rely on.
    #[test]
    fn destination_serde_round_trip() {
        let cases = [
            Destination::Primary,
            Destination::Secondary(PeerId::from("sec-1")),
            Destination::Observer(PeerId::from("obs-1")),
            Destination::All,
        ];
        for dest in cases {
            let json = serde_json::to_string(&dest).expect("serialize Destination");
            let back: Destination = serde_json::from_str(&json).expect("deserialize Destination");
            assert_eq!(dest, back);
        }
    }

    /// `Destination::Primary` resolves to `current_primary` when warm
    /// (post-`PrimaryChanged`), and to `bootstrap_primary` while the
    /// role table is still cold — the cold-cache-safe by-id resolution
    /// that lets a secondary route setup frames to the dialled primary
    /// before any announcement lands.
    #[test]
    fn resolve_primary_warm_then_cold() {
        // Warm: current_primary wins over bootstrap.
        assert_eq!(
            resolve_destination(Destination::Primary, Some("promoted"), Some("boot"), "sec"),
            Some(SendTarget::Peer(PeerId::from("promoted")))
        );
        // Cold: no current_primary → fall back to the bootstrap id.
        assert_eq!(
            resolve_destination(Destination::Primary, None, Some("boot"), "sec"),
            Some(SendTarget::Peer(PeerId::from("boot")))
        );
        // Neither known → unresolvable (the honest "no route to primary").
        assert_eq!(
            resolve_destination(Destination::Primary, None, None, "sec"),
            None
        );
    }

    /// A `Destination::Primary` (or a directly-addressed peer) that
    /// resolves to the local id short-circuits to loopback — the
    /// implicit "resolved host id == local id" rule.
    #[test]
    fn resolve_loopback_when_resolved_is_local() {
        assert_eq!(
            resolve_destination(Destination::Primary, Some("me"), Some("boot"), "me"),
            Some(SendTarget::Loopback)
        );
        assert_eq!(
            resolve_destination(Destination::Secondary(PeerId::from("me")), None, None, "me"),
            Some(SendTarget::Loopback)
        );
    }

    /// `Secondary(id)` and `Observer(id)` resolve identically at egress
    /// — to the carried host id (the role distinction is an ingress-demux
    /// concern, not an outbound-target one). `All` is the broadcast.
    #[test]
    fn resolve_secondary_observer_and_all() {
        let s = resolve_destination(
            Destination::Secondary(PeerId::from("peer-b")),
            Some("primary"),
            Some("primary"),
            "peer-a",
        );
        let o = resolve_destination(
            Destination::Observer(PeerId::from("peer-b")),
            Some("primary"),
            Some("primary"),
            "peer-a",
        );
        assert_eq!(s, Some(SendTarget::Peer(PeerId::from("peer-b"))));
        assert_eq!(s, o, "Secondary and Observer resolve identically at egress");
        assert_eq!(
            resolve_destination(Destination::All, Some("primary"), None, "peer-a"),
            Some(SendTarget::Broadcast)
        );
    }

    /// `routing_target_for` collapses a REMOTE `Destination::Primary` to
    /// `Destination::Secondary(id)` (the id-bearing routing-target the
    /// mesh-pump can route by host) but leaves a LOOPBACK
    /// `Destination::Primary` as bare `Destination::Primary` (so the
    /// pump's Primary arm fires and loopbacks to `LocalRole::Primary`,
    /// not the wrong-slot Secondary). The receiver-demuxes-by-stamp /
    /// sender-dispatches-by-routing-target asymmetry the helper exists
    /// to enforce.
    #[test]
    fn routing_target_for_remote_primary_collapses_loopback_stays_bare() {
        let id = PeerId::from("primary-host");
        // REMOTE primary: collapse to id-bearing routing-target.
        assert_eq!(
            routing_target_for(&Destination::Primary, &SendTarget::Peer(id.clone())),
            Destination::Secondary(id.clone())
        );
        // LOOPBACK primary: stays bare so dispatch's Primary arm fires.
        // Collapsing this to Destination::Secondary(local_id) would land
        // the loopback in the wrong-role slot.
        assert_eq!(
            routing_target_for(&Destination::Primary, &SendTarget::Loopback),
            Destination::Primary
        );
        // BROADCAST is unchanged.
        assert_eq!(
            routing_target_for(&Destination::All, &SendTarget::Broadcast),
            Destination::All
        );
        // Secondary/Observer carry their id; the helper is a no-op.
        let pb = PeerId::from("peer-b");
        assert_eq!(
            routing_target_for(
                &Destination::Secondary(pb.clone()),
                &SendTarget::Peer(pb.clone())
            ),
            Destination::Secondary(pb.clone())
        );
        assert_eq!(
            routing_target_for(
                &Destination::Observer(pb.clone()),
                &SendTarget::Peer(pb.clone())
            ),
            Destination::Observer(pb)
        );
    }

    /// Distinct `Destination` variants — and same-variant destinations
    /// carrying different peer-ids — compare unequal; the resolver and
    /// any dedup logic downstream relies on this.
    #[test]
    fn destination_variants_distinct() {
        assert_ne!(Destination::Primary, Destination::All);
        assert_ne!(
            Destination::Secondary(PeerId::from("a")),
            Destination::Observer(PeerId::from("a"))
        );
        assert_ne!(
            Destination::Secondary(PeerId::from("a")),
            Destination::Secondary(PeerId::from("b"))
        );
        assert_eq!(
            Destination::Secondary(PeerId::from("a")),
            Destination::Secondary(PeerId::from("a"))
        );
    }
}
