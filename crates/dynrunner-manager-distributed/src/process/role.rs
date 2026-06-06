//! [`LocalRole`] — which of the three local coordinator roles a slot or
//! frame is for.
//!
//! # Concern
//!
//! This type owns ONE thing: naming the local role (primary / secondary /
//! observer) that a [`super::role_slot::RoleSlot`] hosts and that a
//! directed frame is demuxed to. It is the manager-layer half of the
//! routing vocabulary whose other half is the protocol crate's
//! [`Destination`]: a `Destination` names *who on the wire* (id-bearing,
//! serializable), a `LocalRole` names *which local coordinator*. They are
//! the SAME vocabulary viewed from the two sides of the role-demux, so
//! `LocalRole` derives directly from `Destination` ([`LocalRole::from_destination`])
//! and projects back to one ([`LocalRole::to_destination`]) — there is no
//! third, parallel role match anywhere (clarification L2).
//!
//! # Boundary
//!
//! `LocalRole` lives in `manager-distributed` (role-aware), NEVER in a
//! transport crate (SUPREME-LAW #5: transport ⊥ roles). It depends only
//! on the protocol crate's [`Destination`]/[`PeerId`] — pure data, no
//! transport, no slot, no mesh internals. A caller of any other `process`
//! type that needs to talk about a role uses this enum and never has to
//! reach into the slot, mesh, or client internals.

use dynrunner_protocol_primary_secondary::address::{Destination, PeerId};

/// One of the three local coordinator roles a peer's process can host.
///
/// A single OS-process is at most one primary + one secondary + one
/// observer (the multi-role host is the result of a promotion, never a
/// bootstrap configuration). This enum is how the mesh demux picks which
/// local coordinator a directed frame is delivered to, and how a slot
/// records its own identity.
///
/// The discriminant values are STABLE and small (`u8`-representable) so a
/// slot can store its role in a lock-free [`std::sync::atomic::AtomicU8`]
/// and retag it in place (clarification H5) — see
/// [`LocalRole::from_u8`] / [`LocalRole::as_u8`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LocalRole {
    Primary,
    Secondary,
    Observer,
}

impl LocalRole {
    /// The stable `u8` discriminant for atomic storage. Paired with
    /// [`LocalRole::from_u8`]; the two are the only encode/decode of the
    /// role into the slot's [`std::sync::atomic::AtomicU8`].
    pub fn as_u8(self) -> u8 {
        match self {
            LocalRole::Primary => 0,
            LocalRole::Secondary => 1,
            LocalRole::Observer => 2,
        }
    }

    /// Decode a role from its stable `u8` discriminant.
    ///
    /// `None` for any value the slot never stores — the slot only ever
    /// writes [`LocalRole::as_u8`] outputs, so a foreign value is a
    /// programming error the caller surfaces rather than silently
    /// coercing to a wrong role.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(LocalRole::Primary),
            1 => Some(LocalRole::Secondary),
            2 => Some(LocalRole::Observer),
            _ => None,
        }
    }

    /// Project this role onto the wire-addressing [`Destination`] for a
    /// frame bound to the role at host `peer_id`.
    ///
    /// `Primary` is id-less on the wire ([`Destination::Primary`] resolves
    /// via the role table), so the `peer_id` is ignored for it — that is
    /// the protocol crate's contract, mirrored here so there is ONE role
    /// vocabulary, not a manager-side parallel match.
    pub fn to_destination(self, peer_id: PeerId) -> Destination {
        match self {
            LocalRole::Primary => Destination::Primary,
            LocalRole::Secondary => Destination::Secondary(peer_id),
            LocalRole::Observer => Destination::Observer(peer_id),
        }
    }

    /// Read the local role a directed [`Destination`] selects.
    ///
    /// - [`Destination::Primary`] → [`LocalRole::Primary`].
    /// - [`Destination::Secondary`] → [`LocalRole::Secondary`].
    /// - [`Destination::Observer`] → [`LocalRole::Observer`].
    /// - [`Destination::All`] → `None`: the broadcast is NOT a single
    ///   role; it fans to every local slot + remotes (the fan, not a
    ///   directed delivery, is the mesh's job).
    pub fn from_destination(dst: &Destination) -> Option<Self> {
        match dst {
            Destination::Primary => Some(LocalRole::Primary),
            Destination::Secondary(_) => Some(LocalRole::Secondary),
            Destination::Observer(_) => Some(LocalRole::Observer),
            Destination::All => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `u8` round-trip is total over the three roles and rejects
    /// foreign discriminants — the contract the slot's atomic relies on.
    #[test]
    fn u8_round_trip_total_over_roles() {
        for role in [
            LocalRole::Primary,
            LocalRole::Secondary,
            LocalRole::Observer,
        ] {
            assert_eq!(LocalRole::from_u8(role.as_u8()), Some(role));
        }
        assert_eq!(LocalRole::from_u8(3), None);
        assert_eq!(LocalRole::from_u8(255), None);
    }

    /// `to_destination` / `from_destination` agree with the protocol
    /// crate's [`Destination`] so a single role vocabulary spans both
    /// sides of the demux. `Primary` is id-less; `Secondary`/`Observer`
    /// carry the host id.
    #[test]
    fn destination_projection_round_trips() {
        let id = PeerId::from("host-7");
        assert_eq!(
            LocalRole::Primary.to_destination(id.clone()),
            Destination::Primary
        );
        assert_eq!(
            LocalRole::Secondary.to_destination(id.clone()),
            Destination::Secondary(id.clone())
        );
        assert_eq!(
            LocalRole::Observer.to_destination(id.clone()),
            Destination::Observer(id.clone())
        );

        assert_eq!(
            LocalRole::from_destination(&Destination::Primary),
            Some(LocalRole::Primary)
        );
        assert_eq!(
            LocalRole::from_destination(&Destination::Secondary(id.clone())),
            Some(LocalRole::Secondary)
        );
        assert_eq!(
            LocalRole::from_destination(&Destination::Observer(id)),
            Some(LocalRole::Observer)
        );
    }

    /// `All` is not a single local role — it is the broadcast fan, which
    /// the mesh handles distinctly from a directed delivery.
    #[test]
    fn all_destination_is_not_a_single_role() {
        assert_eq!(LocalRole::from_destination(&Destination::All), None);
    }
}
