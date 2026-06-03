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
//! [`RoleTable`] and [`RoleChangeHookRegistrar`] live here too because
//! they are the role-vocabulary's *replicated state* shape — they
//! describe **who holds which role** in a form that transports can
//! mirror without depending on the manager-distributed crate (which
//! owns the actual replication mechanics via `ClusterState`). The
//! registrar trait is the boundary `ClusterState` implements; the
//! `PeerTransport` impl on each node uses it to subscribe a write-
//! through cache.
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
///
/// **Serde derives** carry `Role` into the `RoleAddressed` /
/// `RoleMisaddressHint` wire frames (Step 3) — the receiver decodes
/// `intended_role` from the envelope and checks it against its own
/// role-table cache. `rename_all = "snake_case"` keeps the JSON
/// representation in lockstep with the rest of the protocol crate
/// (`msg_type`, `MessageType` variants), so a future cross-language
/// peer sees `"primary"` / `"self_"` rather than `"Primary"` /
/// `"Self_"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// Replicated role bookkeeping. The authoritative owner is the
/// downstream `ClusterState`; transports keep a write-through cache
/// populated via [`RoleChangeHookRegistrar::register_role_change_hook`].
///
/// `observers` is reserved for the future observer story (Steps 7–9
/// of the unification refactor); no current mutation populates it.
/// Keeping the field present here means the cache + registrar API
/// shapes stay stable when `Role::Observer` lands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleTable {
    pub primary: Option<String>,
    pub observers: std::collections::HashSet<String>,
}

/// Boundary trait that downstream replicated-state owners implement
/// so transports can attach a write-through cache without depending
/// on the manager crate's concrete `ClusterState`. The single
/// implementer in production is `dynrunner_manager_distributed::
/// ClusterState`; test fixtures may also implement it.
///
/// The trait deliberately exposes only **hook registration** — not
/// table read access. Readers go through the transport's cache
/// (lock-free hot path) or through `ClusterState::role_table()`
/// directly. This way the trait stays a one-method boundary that
/// doesn't leak the replicated-state internals to the protocol
/// crate.
pub trait RoleChangeHookRegistrar {
    /// Register a callback fired (synchronously, from inside the
    /// state's apply loop) AFTER any [`RoleTable`] mutation. The
    /// callback observes the *post*-mutation table.
    ///
    /// Hooks accumulate; implementers must NOT clear prior
    /// registrants. Today the only registrant is the `PeerTransport`
    /// write-through cache (one per node).
    fn register_role_change_hook(&mut self, hook: Box<dyn Fn(&RoleTable) + Send + Sync + 'static>);
}

// ── Shared write-through cache plumbing for PeerTransport impls ──
//
// Every `PeerTransport` impl that owns a role cache uses the same
// `Arc<RwLock<HashMap<Role, String>>>` shape, the same hook body,
// and the same read helper. Pulling the trio into the protocol
// crate prevents drift between the QUIC, channel, and future
// `EitherPeerTransport` paths: a bug fixed here lands everywhere.

/// Shared `Role → peer_id` write-through cache, populated by the
/// hook installed via [`install_role_change_hook`] and read on the
/// send hot path via [`read_role_cache`].
///
/// `Arc<RwLock<...>>` so the hook (`Send + Sync + 'static` closure
/// stored on the registrar / `ClusterState`) can mutate the same
/// map the transport reads. The map stays small (at most
/// `Role`-cardinality entries) so RwLock contention is negligible;
/// the registration happens once at coordinator construct time and
/// the read path is cache-style — lock, clone the `String`,
/// release.
pub type RoleCache = std::sync::Arc<std::sync::RwLock<std::collections::HashMap<Role, String>>>;

/// Construct an empty role cache. Convenience constructor so
/// transport impls don't need to thread the `Arc::new(RwLock::new(
/// HashMap::new()))` boilerplate at every `new`-style entry point.
pub fn new_role_cache() -> RoleCache {
    std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Seed `Role::Self_` with the transport's `local_id`. The role-
/// table hook (`install_role_change_hook`) never touches `Self_`
/// — it's not a replicated fact, it's a strictly local "who am I"
/// answer — so transports populate it themselves at construction.
///
/// The receiver-side `RoleAddressed` decision (Step 4) uses
/// `read_role_cache(.., Role::Self_)` as the holder lookup when an
/// envelope's `intended_role == Self_`; without this seed the
/// receiver would observe a cache-cold `Self_` and drop the
/// envelope as Case C, even though `Self_` is by definition
/// always-resolved-to-self.
///
/// Idempotent on subsequent calls. On lock poisoning we recover
/// the inner — same rationale as the rest of the cache helpers.
pub fn seed_self_role(cache: &RoleCache, local_id: &str) {
    let mut guard = match cache.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.insert(Role::Self_, local_id.to_string());
}

/// Install a hook on the registrar that writes through to the
/// cache. The hook captures a strong `Arc<RwLock<_>>` handle to the
/// cache; both the transport and the hook hold one, so the cache
/// outlives whichever side drops first. When the registrar is
/// dropped the hook is dropped — releasing the registrar's
/// `Arc<RoleCache>` handle — and the transport's handle keeps the
/// cache alive for as long as the transport itself lives.
///
/// `Role::Primary` is the only role today (per `Role` enum:
/// `Self_` is a sender-side intent, not a holder). Observers will
/// land here when Step 7+ populates `RoleTable::observers`.
pub fn install_role_change_hook(cache: RoleCache, registrar: &mut dyn RoleChangeHookRegistrar) {
    registrar.register_role_change_hook(Box::new(move |table: &RoleTable| {
        let mut guard = match cache.write() {
            Ok(g) => g,
            // Lock poisoning means a hook panicked mid-write on a
            // prior call. Recovering the inner map is the right
            // call: we'd rather take the latest write-through over
            // a stale prior value than block the apply loop on an
            // unrecoverable state.
            Err(p) => p.into_inner(),
        };
        guard.remove(&Role::Primary);
        if let Some(ref id) = table.primary {
            guard.insert(Role::Primary, id.clone());
        }
    }));
}

/// Lock the cache and clone out the holder id for `role`. Wraps
/// the `RwLock::read` + `Option::cloned` dance so every transport
/// impl produces the same observable result. On lock poisoning we
/// recover the inner — same rationale as the hook write path.
pub fn read_role_cache(cache: &RoleCache, role: &Role) -> Option<String> {
    let guard = match cache.read() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.get(role).cloned()
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
        assert_ne!(Address::Role(Role::Primary), Address::Role(Role::Self_));
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
