//! Receiver-side handling of [`DistributedMessage::RoleAddressed`]
//! envelopes (Step 4 of the transport-unification refactor).
//!
//! # Concern
//!
//! When a peer receives a `RoleAddressed { intended_role, payload,
//! sender_id, attempts }` envelope, it must decide one of four
//! actions, based purely on (a) its own role-table cache, (b) the
//! envelope's `intended_role`, and (c) the `attempts` safety bound:
//!
//! - **Case A** — receiver IS the intended role: unwrap the payload
//!   and treat it as if it had been directly addressed.
//! - **Case B** — receiver is NOT the intended role, but its cache
//!   names a different holder: forward the (re-wrapped) envelope to
//!   that holder AND send a [`DistributedMessage::RoleMisaddressHint`]
//!   back to `sender_id` so the originator's cache warms.
//! - **Case C** — receiver does not know who holds the role at all:
//!   drop. Returning to the sender would amplify the disagreement.
//! - **Case D** — `attempts >= [`MAX_ROLE_RELAY_ATTEMPTS`]`: drop.
//!   Bounds relay storms when the cluster is in disagreement about
//!   who holds the role.
//!
//! # Module boundary
//!
//! This module owns the decision: given inputs, what should the
//! transport do? It is intentionally pure (no I/O, no async, no
//! transport-specific state). Transports plug it into their
//! `recv_peer` paths, then act on [`RoleAddressedAction`] using
//! their existing `send_to_peer` primitive. Keeping the decision
//! out of the transport impls also keeps the QUIC and channel
//! impls in lockstep — a bug fixed here lands everywhere.
//!
//! Hint application (the symmetric receiver side: a peer that
//! receives a `RoleMisaddressHint` must update its own role cache)
//! lives in [`apply_role_misaddress_hint`], also pure: takes the
//! cache handle and the (role, holder_id) pair and writes through.
//! Senders never surface the hint to the application layer — see
//! the design rationale in the plan: senders that issued an
//! `Address::Role(_)` send are not awaiting a hint reply; the hint
//! is fire-and-forget cache-warming.

use dynrunner_core::Identifier;

use crate::address::{Role, RoleCache, read_role_cache};
use crate::messages::{DistributedMessage, timestamp_now};

/// Maximum number of times a `RoleAddressed` envelope may be relayed
/// before the receiver drops it. With each Case-B relay the
/// `attempts` field is incremented by one; a receiver observing
/// `attempts >= MAX_ROLE_RELAY_ATTEMPTS` drops without relaying
/// further. Picked at 3 so two consecutive misroutes still resolve
/// (originator → A → B → holder is at most attempts=2 on the
/// final delivery; the cap stops a fourth hop), while a partition
/// where every peer disagrees on who holds the role bounds the
/// total mesh traffic at 3× the originating send.
pub const MAX_ROLE_RELAY_ATTEMPTS: u8 = 3;

/// One decision the transport must execute after receiving a
/// `RoleAddressed` envelope.
///
/// The decision is computed by [`decide_role_addressed`] from
/// `(local_id, cache_holder_for_role, envelope, attempts)`. The
/// transport then performs at most two `send_to_peer` calls
/// (Case B) or surfaces a payload to the application layer
/// (Case A) or logs-and-drops (Cases C / D).
pub enum RoleAddressedAction<I> {
    /// Receiver IS the intended role-holder; the unwrapped payload
    /// should be returned from `recv_peer` so the caller's normal
    /// dispatch path handles it as if it had been directly
    /// addressed (Case A).
    Unwrap(DistributedMessage<I>),
    /// Receiver knows a different peer holds the role. Transport
    /// should:
    ///   1. `send_to_peer(forward_to, forwarded)` — forward the
    ///      payload, re-wrapped with `attempts + 1`.
    ///   2. `send_to_peer(hint_to, hint)` — fire-and-forget cache-
    ///      warming so the originating sender stops misrouting on
    ///      the next send (Case B).
    ///
    /// `hint` is boxed so the Relay variant size stays close to
    /// `Unwrap` (carries one `DistributedMessage`); leaving both
    /// payloads inline doubled the variant to ~712 bytes and tripped
    /// clippy::large_enum_variant. `forwarded` remains inline because
    /// trimming one payload is already enough to close the size gap.
    Relay {
        forward_to: String,
        forwarded: DistributedMessage<I>,
        hint_to: String,
        hint: Box<DistributedMessage<I>>,
    },
    /// Drop and log. Covers Case C (no known holder for the role)
    /// and Case D (`attempts` exceeded the safety bound). The
    /// `reason` is a static string so log lines stay grep-able
    /// without per-call allocation.
    Drop { reason: &'static str },
}

/// Decide what the receiver should do with one `RoleAddressed`
/// envelope.
///
/// Parameters
/// - `local_id`: the receiver's own peer id.
/// - `cache_holder_for_role`: snapshot of the receiver's role-table
///   cache for `intended_role` (cloned once before this call so the
///   decision is pure and stays lock-free).
/// - `sender_id`, `intended_role`, `payload`, `attempts`: the
///   destructured envelope fields. Caller owns the envelope and
///   passes the inner payload by box-move; on Case-A unwrap we
///   return `*payload` to the caller; on Case-B we re-wrap.
/// - `now_wire`: unix-epoch timestamp for the outbound envelopes
///   (forwarded `RoleAddressed` + `RoleMisaddressHint`). Passed in
///   so tests can drive deterministic timestamps and so the decision
///   stays free of `SystemTime::now` calls.
///
/// # Returns
///
/// One of [`RoleAddressedAction::Unwrap`] (Case A),
/// [`RoleAddressedAction::Relay`] (Case B), or
/// [`RoleAddressedAction::Drop`] (Cases C / D).
///
/// # Panics
///
/// Never — the function is total over its input.
pub fn decide_role_addressed<I: Identifier>(
    local_id: &str,
    cache_holder_for_role: Option<String>,
    sender_id: String,
    intended_role: Role,
    payload: Box<DistributedMessage<I>>,
    attempts: u8,
    now_wire: f64,
) -> RoleAddressedAction<I> {
    // Case D: safety bound on relay-hop count. Check BEFORE Case A
    // would also be valid (a receiver that IS the role doesn't relay
    // anyway, so the bound is moot there), but ordering Case D
    // before A would silently drop a hot-path local-delivery when
    // the cluster is in mass-disagreement — which is exactly the
    // scenario where prompt delivery to the actual holder matters
    // most. So: only apply the bound when we're about to *relay*
    // (Case B candidate). For Case A we always unwrap; for Case C
    // we always drop (no holder to relay to anyway).
    let receiver_is_holder = cache_holder_for_role.as_deref() == Some(local_id);
    if receiver_is_holder {
        // Case A.
        return RoleAddressedAction::Unwrap(*payload);
    }
    let Some(holder) = cache_holder_for_role else {
        // Case C: no known holder. Drop without bouncing back —
        // returning a hint to the originator would just amplify
        // the disagreement (the originator already thought we held
        // the role).
        return RoleAddressedAction::Drop {
            reason: "RoleAddressed received but receiver has no cached holder for the role",
        };
    };
    // Case D candidate: about to relay, but attempts cap reached.
    if attempts >= MAX_ROLE_RELAY_ATTEMPTS {
        return RoleAddressedAction::Drop {
            reason: "RoleAddressed dropped: attempts cap reached (relay storm guard)",
        };
    }
    // Case B: relay-and-hint.
    let forwarded = DistributedMessage::RoleAddressed {
        sender_id: sender_id.clone(),
        timestamp: now_wire,
        intended_role: intended_role.clone(),
        payload,
        attempts: attempts.saturating_add(1),
    };
    let hint = DistributedMessage::RoleMisaddressHint {
        sender_id: local_id.to_string(),
        timestamp: now_wire,
        role: intended_role,
        holder_id: holder.clone(),
    };
    RoleAddressedAction::Relay {
        forward_to: holder,
        forwarded,
        hint_to: sender_id,
        hint: Box::new(hint),
    }
}

/// Convenience wrapper that reads the cache and computes the
/// decision in one call. Used by transports that hold a
/// [`RoleCache`] directly. Equivalent to
/// `decide_role_addressed(local_id, read_role_cache(cache,
/// &intended_role), …)`.
pub fn decide_role_addressed_with_cache<I: Identifier>(
    local_id: &str,
    cache: &RoleCache,
    sender_id: String,
    intended_role: Role,
    payload: Box<DistributedMessage<I>>,
    attempts: u8,
) -> RoleAddressedAction<I> {
    let holder = read_role_cache(cache, &intended_role);
    decide_role_addressed(
        local_id,
        holder,
        sender_id,
        intended_role,
        payload,
        attempts,
        timestamp_now(),
    )
}

/// Apply a `RoleMisaddressHint` to a role-table cache. Writes
/// `holder_id` into the cache under `role`. Idempotent.
///
/// Used by the transport's `recv_peer` when it intercepts a
/// `RoleMisaddressHint` envelope: the hint is purely cache-warming,
/// the application layer never sees it.
///
/// On lock poisoning we recover the inner — same rationale as the
/// other cache helpers in `address.rs`: a poisoned guard means a
/// prior hook panicked mid-write; we prefer the freshest write-
/// through over blocking the recv loop indefinitely.
pub fn apply_role_misaddress_hint(cache: &RoleCache, role: Role, holder_id: String) {
    let mut guard = match cache.write() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    guard.insert(role, holder_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address::new_role_cache;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    fn keepalive(sender: &str) -> DistributedMessage<TestId> {
        DistributedMessage::Keepalive {
            sender_id: sender.into(),
            timestamp: 1.0,
            secondary_id: sender.into(),
            active_workers: 0,
        }
    }

    /// Case A: receiver IS the cached holder of `intended_role` →
    /// unwrap.
    #[test]
    fn decides_unwrap_when_receiver_is_holder() {
        let decision = decide_role_addressed::<TestId>(
            "B",
            Some("B".to_string()),
            "A".to_string(),
            Role::Primary,
            Box::new(keepalive("A")),
            0,
            1.0,
        );
        match decision {
            RoleAddressedAction::Unwrap(inner) => {
                assert!(matches!(inner, DistributedMessage::Keepalive { .. }));
            }
            _ => panic!("expected Unwrap"),
        }
    }

    /// Case B: receiver knows the role belongs to someone else →
    /// relay-and-hint. Forwarded envelope has attempts+1; hint
    /// addresses the original sender with the right (role, holder).
    #[test]
    fn decides_relay_and_hint_when_holder_is_other_peer() {
        let decision = decide_role_addressed::<TestId>(
            "B",
            Some("C".to_string()),
            "A".to_string(),
            Role::Primary,
            Box::new(keepalive("A")),
            0,
            2.0,
        );
        match decision {
            RoleAddressedAction::Relay {
                forward_to,
                forwarded,
                hint_to,
                hint,
            } => {
                assert_eq!(forward_to, "C");
                assert_eq!(hint_to, "A");
                match forwarded {
                    DistributedMessage::RoleAddressed {
                        sender_id,
                        intended_role,
                        attempts,
                        ..
                    } => {
                        assert_eq!(sender_id, "A", "forwarded preserves originator");
                        assert_eq!(intended_role, Role::Primary);
                        assert_eq!(attempts, 1, "relay increments attempts");
                    }
                    _ => panic!("forwarded must be RoleAddressed"),
                }
                match *hint {
                    DistributedMessage::RoleMisaddressHint {
                        sender_id,
                        role,
                        holder_id,
                        ..
                    } => {
                        assert_eq!(sender_id, "B", "hint sender is the receiver");
                        assert_eq!(role, Role::Primary);
                        assert_eq!(holder_id, "C");
                    }
                    _ => panic!("hint must be RoleMisaddressHint"),
                }
            }
            _ => panic!("expected Relay"),
        }
    }

    /// Case C: receiver does not know any holder → drop.
    #[test]
    fn decides_drop_when_no_known_holder() {
        let decision = decide_role_addressed::<TestId>(
            "B",
            None,
            "A".to_string(),
            Role::Primary,
            Box::new(keepalive("A")),
            0,
            1.0,
        );
        assert!(matches!(decision, RoleAddressedAction::Drop { .. }));
    }

    /// Case D: attempts cap reached → drop even though a holder is
    /// cached. Bounds relay storms when the cluster is in
    /// disagreement.
    #[test]
    fn decides_drop_when_attempts_cap_reached() {
        let decision = decide_role_addressed::<TestId>(
            "B",
            Some("C".to_string()),
            "A".to_string(),
            Role::Primary,
            Box::new(keepalive("A")),
            MAX_ROLE_RELAY_ATTEMPTS,
            1.0,
        );
        assert!(matches!(decision, RoleAddressedAction::Drop { .. }));
    }

    /// Just below the cap still relays — pins the off-by-one
    /// boundary so a future tweak of `MAX_ROLE_RELAY_ATTEMPTS`
    /// surfaces here rather than as a missed envelope in production.
    #[test]
    fn decides_relay_at_cap_minus_one() {
        let decision = decide_role_addressed::<TestId>(
            "B",
            Some("C".to_string()),
            "A".to_string(),
            Role::Primary,
            Box::new(keepalive("A")),
            MAX_ROLE_RELAY_ATTEMPTS - 1,
            1.0,
        );
        match decision {
            RoleAddressedAction::Relay { forwarded, .. } => {
                if let DistributedMessage::RoleAddressed { attempts, .. } = forwarded {
                    assert_eq!(attempts, MAX_ROLE_RELAY_ATTEMPTS);
                }
            }
            _ => panic!("expected Relay just below cap"),
        }
    }

    /// `apply_role_misaddress_hint` writes through to the cache and
    /// is idempotent.
    #[test]
    fn apply_hint_writes_to_cache() {
        let cache = new_role_cache();
        apply_role_misaddress_hint(&cache, Role::Primary, "C".to_string());
        assert_eq!(
            read_role_cache(&cache, &Role::Primary),
            Some("C".to_string())
        );
        // Idempotent overwrite.
        apply_role_misaddress_hint(&cache, Role::Primary, "D".to_string());
        assert_eq!(
            read_role_cache(&cache, &Role::Primary),
            Some("D".to_string())
        );
    }
}
