//! Per-target WARN rate-limiting for the relay dispatch path.
//!
//! # Single concern
//!
//! Decide WHEN a recurring relay-path WARN for a given `(kind, target)`
//! may emit again, and account for how many were suppressed in between.
//! The caller owns WHAT to log (the message + its structured fields);
//! this gate owns only the edge + the min-interval window + the
//! suppressed count.
//!
//! # Why a separate primitive (not `dynrunner-manager-distributed`'s
//! `WarnThrottle`)
//!
//! `WarnThrottle` reads the ambient `tokio::time::Instant::now()` and is
//! a SINGLE throttle; its ~15 callers drive it under `start_paused`
//! test time. The relay [`Router`](crate::relay::router::Router) lives a
//! crate upstream and is deliberately CLOCK-INJECTED — every entry point
//! takes a [`Clocks`](crate::relay::router::Clocks) snapshot so
//! `tokio::time::pause`'d transport tests drive the cooldown gate without
//! the Router ever touching the system clock. It also needs PER-TARGET
//! throttling, not a single instance. Those two constraints (injected
//! `std::time::Instant` + keyed) are why this is a sibling primitive
//! rather than a reuse: the suppressed-count idea is shared, but the
//! clock model and the keying differ.
//!
//! # Two faults the rate-limit addresses (owner #log-format A+B)
//!
//!   * (A) the route-state flip WARN ("peer unroutable: …") re-fires on a
//!     FLAPPING link (one emit per flip → a partition that oscillates
//!     storms the operator stream),
//!   * (B) the per-MESSAGE relay-forward-failure WARNs (connection
//!     closed / target missing / backoff send failed) fire once per
//!     message, so a burst of traffic toward a freshly-dead peer emits a
//!     WARN per message.
//!
//! Both are genuinely actionable on their FIRST occurrence (the operator
//! must learn a peer just went unroutable / a forward just failed) but
//! pure noise on repeat for the same condition. The gate keeps the first
//! and one-per-window thereafter, naming the suppressed count.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Minimum spacing between two emitted WARNs for the same `(kind,
/// target)`. Picked to match the relay state's `BLACKLIST_TTL`-scale
/// cadence: a genuinely-down peer re-warns roughly twice a minute rather
/// than per message / per flip, while a one-off failure still emits at
/// once (the first occurrence always passes).
pub(super) const RELAY_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// The recurring relay-path WARN conditions the gate rate-limits, keyed
/// alongside the target peer so a fault toward one peer never suppresses
/// the SAME fault toward a different peer. Each variant is one fault
/// class; the message text lives at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum RelayWarnKind {
    /// The route to a target just flipped to no-route (every connected
    /// forwarder blacklisted, no direct link) — the (A) flip WARN.
    PeerUnroutable,
    /// A direct forward toward the target failed: its connection closed.
    DirectForwardClosed,
    /// A direct forward target was unexpectedly missing from connections.
    DirectForwardMissing,
    /// A relay forward via a forwarder failed: the forwarder's connection
    /// closed.
    RelayForwardClosed,
    /// A relay forward's chosen forwarder was unexpectedly missing.
    RelayForwardMissing,
    /// A dead-end backoff toward the predecessor failed: connection closed.
    BackoffPredecessorClosed,
    /// A dead-end backoff's predecessor was not in connections.
    BackoffPredecessorMissing,
    /// A dead-end relay had an empty path: no predecessor to back off to.
    BackoffEmptyPath,
    /// A backoff-driven relay retry send failed.
    RetrySendFailed,
    /// A backoff propagation toward the predecessor failed.
    BackoffPropagationFailed,
    /// The sync try-recv path could not forward a relay envelope.
    SyncDropForward,
}

/// Per-`(kind, target)` minimum-interval WARN gate with a suppressed
/// counter. One instance lives on the
/// [`Router`](crate::relay::router::Router); each entry point passes the
/// injected `now` so the gate never reads the clock itself.
#[derive(Debug, Default)]
pub(super) struct RelayWarnGate {
    /// Per-`(kind, target)`: when its last WARN emitted (`None` until the
    /// first) and how many occurrences were suppressed since.
    entries: HashMap<(RelayWarnKind, String), WarnSlot>,
}

/// One throttle slot: last emit + suppressed-since-last count.
#[derive(Debug, Default)]
struct WarnSlot {
    last_emit: Option<Instant>,
    suppressed: u64,
}

impl RelayWarnGate {
    /// Report one occurrence of `kind` toward `target` at `now`. Returns
    /// `Some(suppressed_since_last_emit)` when the caller should emit the
    /// WARN NOW (the first occurrence always emits; later ones once per
    /// [`RELAY_WARN_INTERVAL`]), or `None` when this occurrence is
    /// suppressed. The returned count lets the emitted line name how many
    /// repeats it stands in for.
    pub(super) fn admit(&mut self, kind: RelayWarnKind, target: &str, now: Instant) -> Option<u64> {
        let slot = self
            .entries
            .entry((kind, target.to_string()))
            .or_default();
        match slot.last_emit {
            Some(last) if now.duration_since(last) < RELAY_WARN_INTERVAL => {
                slot.suppressed += 1;
                None
            }
            _ => {
                let suppressed = slot.suppressed;
                slot.suppressed = 0;
                slot.last_emit = Some(now);
                Some(suppressed)
            }
        }
    }

    /// Drop slots whose last emit is older than [`RELAY_WARN_INTERVAL`]
    /// AND that hold no pending suppressed count — they carry no state a
    /// future `admit` could not reconstruct from a fresh slot (a long-idle
    /// `(kind, target)` emits immediately with a zero suppressed count
    /// either way). Keeps the map bounded by recent fault activity rather
    /// than the lifetime set of peers, called from the Router's existing
    /// TTL sweep.
    pub(super) fn prune(&mut self, now: Instant) {
        self.entries.retain(|_, slot| {
            slot.suppressed > 0
                || slot
                    .last_emit
                    .is_some_and(|t| now.duration_since(t) < RELAY_WARN_INTERVAL)
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// First occurrence of a `(kind, target)` emits immediately; within
    /// the window everything is suppressed and counted; past the window
    /// the next occurrence emits carrying the suppressed count.
    #[test]
    fn first_emits_then_suppresses_until_window_elapses() {
        let mut gate = RelayWarnGate::default();
        let t0 = Instant::now();
        assert_eq!(
            gate.admit(RelayWarnKind::PeerUnroutable, "sec-a", t0),
            Some(0),
            "the first occurrence always emits"
        );
        assert_eq!(
            gate.admit(
                RelayWarnKind::PeerUnroutable,
                "sec-a",
                t0 + Duration::from_secs(1)
            ),
            None
        );
        assert_eq!(
            gate.admit(
                RelayWarnKind::PeerUnroutable,
                "sec-a",
                t0 + Duration::from_secs(2)
            ),
            None
        );
        assert_eq!(
            gate.admit(
                RelayWarnKind::PeerUnroutable,
                "sec-a",
                t0 + RELAY_WARN_INTERVAL + Duration::from_secs(1)
            ),
            Some(2),
            "past the window: emit, naming the 2 suppressed occurrences"
        );
    }

    /// Distinct targets and distinct kinds are throttled independently —
    /// a flood toward sec-a never suppresses the first WARN toward sec-b
    /// or a different fault toward sec-a.
    #[test]
    fn distinct_keys_throttle_independently() {
        let mut gate = RelayWarnGate::default();
        let t0 = Instant::now();
        assert_eq!(
            gate.admit(RelayWarnKind::PeerUnroutable, "sec-a", t0),
            Some(0)
        );
        // Same kind, different target: still a first occurrence.
        assert_eq!(
            gate.admit(RelayWarnKind::PeerUnroutable, "sec-b", t0),
            Some(0)
        );
        // Different kind, same target: still a first occurrence.
        assert_eq!(
            gate.admit(RelayWarnKind::RelayForwardClosed, "sec-a", t0),
            Some(0)
        );
    }

    /// An idle key does not bank credit: an emit is gated only on the
    /// time since the LAST emit, so a long-idle slot emits immediately on
    /// its next occurrence with a zero suppressed count.
    #[test]
    fn idle_key_emits_immediately_with_zero_suppressed() {
        let mut gate = RelayWarnGate::default();
        let t0 = Instant::now();
        assert_eq!(
            gate.admit(RelayWarnKind::PeerUnroutable, "sec-a", t0),
            Some(0)
        );
        assert_eq!(
            gate.admit(
                RelayWarnKind::PeerUnroutable,
                "sec-a",
                t0 + Duration::from_secs(600)
            ),
            Some(0)
        );
    }

    /// `prune` drops an idle, fully-drained slot but keeps one that still
    /// holds a pending suppressed count (it must survive to name those
    /// repeats on the next emit).
    #[test]
    fn prune_drops_idle_drained_slots_keeps_pending() {
        let mut gate = RelayWarnGate::default();
        let t0 = Instant::now();
        // Idle, drained: emitted once, nothing suppressed since.
        assert_eq!(gate.admit(RelayWarnKind::PeerUnroutable, "idle", t0), Some(0));
        // Pending: emitted, then suppressed one within the window.
        assert_eq!(
            gate.admit(RelayWarnKind::PeerUnroutable, "pending", t0),
            Some(0)
        );
        assert_eq!(
            gate.admit(
                RelayWarnKind::PeerUnroutable,
                "pending",
                t0 + Duration::from_secs(1)
            ),
            None
        );

        gate.prune(t0 + RELAY_WARN_INTERVAL + Duration::from_secs(1));

        assert!(
            !gate
                .entries
                .contains_key(&(RelayWarnKind::PeerUnroutable, "idle".to_string())),
            "idle drained slot pruned"
        );
        assert!(
            gate.entries
                .contains_key(&(RelayWarnKind::PeerUnroutable, "pending".to_string())),
            "slot with a pending suppressed count survives"
        );
    }
}
