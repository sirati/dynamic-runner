//! Role-based addressing primitives for the primary/secondary protocol.
//!
//! # Concern
//!
//! This module owns the *addressing vocabulary* — the pure-data enums
//! that describe **who** a message is for, decoupled from **how** the
//! transport gets it there. It is the single source of truth that every
//! transport (`channel`, `quic`, future kinds) will resolve against once
//! the transport-unification refactor lands its role-resolution cache
//! (Step 2) and `Role(_)` dispatch (Step 3).
//!
//! # Design decisions
//!
//! - **Runtime enum, not type-system parameter.** The address is
//!   fundamentally a runtime fact: the primary changes mid-run on
//!   handover, so a `T: TargetsPrimary`-style marker can't express
//!   "send to whichever node *currently* holds the primary role".
//!   Trait methods also can't carry payload-dependent type parameters
//!   without HKT contortions, so the address must be a plain value.
//!
//! - **`Role::Self_` with trailing underscore.** `self` is a reserved
//!   keyword, and the variant is genuinely needed: a primary running in
//!   single-process mode (worker pool colocated with the dispatcher)
//!   must be able to address itself without the call site having to
//!   case-distinguish on local peer id. The transport layer resolves
//!   `Self_` to a loopback path.
//!
//! - **Variants are intentionally open.** Adding `Role::Observer` (or
//!   `Role::Coordinator` for multi-primary topologies) later costs only
//!   call-site coverage of the new variant — the enum itself stays
//!   binary-compatible at the protocol wire format because addressing
//!   is a *send-time* decision, not a wire field.
//!
//! - **`Scope::AllSecondaries` is a migration aid, not a long-term
//!   target.** It exists so the legacy "primary fans out to every
//!   secondary" code paths can be ported to `PeerTransport::send`
//!   one-by-one without changing semantics. New code should prefer
//!   `Scope::Mesh`, which fans out to every member regardless of role.

/// Role-based addressing primitive. Resolved at send time from a
/// transport-local cache that mirrors the cluster_state's RoleTable
/// (populated in Step 2).
///
/// `Self` means "the local node's own address" — used by senders that
/// need to loopback (e.g., a primary sending TaskAssigned to its own
/// worker pool in single-process mode).
///
/// Variants are open to extension as the protocol grows (e.g., a
/// future `Coordinator` role for multi-primary setups).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Role {
    Primary,
    Self_,
}

/// Broadcast scope. `Mesh` is the default — fan out to every member.
/// `AllSecondaries` excludes the current primary; useful for legacy
/// primary→secondary broadcasts during migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    Mesh,
    AllSecondaries,
}

/// First-class addressing for PeerTransport sends. The trait method
/// `PeerTransport::send(Address, msg)` is the single entry point that
/// every call site should eventually migrate to. Today it default-
/// impls via the existing `send_to_peer` / `broadcast`; Step 3 lands
/// real `Role(_)` dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    /// Specific peer by id. Direct routing via existing `send_to_peer`.
    Peer(String),
    /// Role-based: resolves to whichever peer the sender's local
    /// cache says holds that role. Wrapped in `RoleAddressed`
    /// envelope at send time so receivers can detect misaddress and
    /// relay-with-hint (Step 4).
    Role(Role),
    /// Fan out to a scope of peers.
    Broadcast(Scope),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each variant round-trips through `clone()` with structural
    /// equality preserved. Pins the `Clone + PartialEq` derives so a
    /// future refactor that adds non-`Clone` payload trips this test
    /// rather than silently breaking call sites.
    #[test]
    fn address_variants_clone_and_eq() {
        let peer = Address::Peer("worker-7".to_string());
        let role = Address::Role(Role::Primary);
        let role_self = Address::Role(Role::Self_);
        let bcast_mesh = Address::Broadcast(Scope::Mesh);
        let bcast_secondaries = Address::Broadcast(Scope::AllSecondaries);

        assert_eq!(peer, peer.clone());
        assert_eq!(role, role.clone());
        assert_eq!(role_self, role_self.clone());
        assert_eq!(bcast_mesh, bcast_mesh.clone());
        assert_eq!(bcast_secondaries, bcast_secondaries.clone());
    }

    /// Distinct roles must compare unequal — the resolver downstream
    /// (Step 2) relies on this to key its cache.
    #[test]
    fn role_primary_ne_self() {
        assert_ne!(Role::Primary, Role::Self_);
        assert_ne!(
            Address::Role(Role::Primary),
            Address::Role(Role::Self_)
        );
    }

    /// Distinct peer ids must produce distinct addresses; this is the
    /// minimum contract `send_to_peer` dispatch relies on.
    #[test]
    fn address_peer_distinguishes_ids() {
        assert_ne!(
            Address::Peer("a".to_string()),
            Address::Peer("b".to_string())
        );
    }

    /// `Scope` variants are distinct — guards against an accidental
    /// merge during refactor.
    #[test]
    fn scope_variants_distinct() {
        assert_ne!(Scope::Mesh, Scope::AllSecondaries);
        assert_ne!(
            Address::Broadcast(Scope::Mesh),
            Address::Broadcast(Scope::AllSecondaries)
        );
    }
}
