//! Router state types, TTL constants, and prune helpers.
//!
//! Pulled out of the dispatcher so `dispatcher.rs` only carries the
//! `Router<I>` struct and its public/private methods. Everything here
//! is pure data + cheap O(N) helpers — no I/O, no transport
//! interaction.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::messages::DistributedMessage;
use crate::relay::OutgoingRelay;

/// How long an outgoing-relay state entry survives without an update
/// before the periodic sweep prunes it. Picked larger than any
/// realistic forwarding round-trip across a multi-hop mesh; smaller
/// than the 30s peer-keepalive miss threshold so a dead-letter
/// state doesn't outlive the peer it was waiting on.
pub(super) const RELAY_STATE_TTL: Duration = Duration::from_secs(20);

/// How long a per-target forwarder failure stays in the blacklist
/// before subsequent relays will retry that forwarder again. Picked
/// long enough that we don't hammer a confirmed-dead path on every
/// outbound message, but short enough that a re-established direct
/// link recovers without a whole-process restart.
pub(super) const BLACKLIST_TTL: Duration = Duration::from_secs(120);

/// How long after the last observed relay relationship with a peer a
/// fresh redial signal stays suppressed. Tuned to roughly match the
/// per-peer keepalive miss-detection window so the redial fires on
/// the first send/recv after a partition heals or after the partition
/// has had time to be observed end-to-end without storming the
/// transport's dial path.
pub const REDIAL_COOLDOWN: Duration = Duration::from_secs(30);

/// `tracing` target used for every log line emitted on the relay
/// dispatch path. Operators filter via
/// `RUST_LOG=dynrunner_relay=info` to see relay events independent of
/// which crate the code lives in.
pub const RELAY_LOG_TARGET: &str = "dynrunner_relay";

/// Log-message constants for the relay-path events that integration
/// tests pin against (silent-reconnect coverage, originator-drop
/// coverage). Exposed so tests can match the literal production
/// string instead of carrying a substring copy that would silently
/// drift on rephrase. Other relay-path log messages stay inline —
/// extract here only when a test needs to assert exact content.
pub const MSG_RELAY_ENGAGED: &str =
    "peer relay engaged: direct link unreachable, forwarding via peer";
pub const MSG_DIRECT_RESTORED: &str = "peer direct link restored";
pub const MSG_DROPPED_AT_ORIGINATOR: &str = "relay dropped: all paths exhausted at originator";

/// Per-call clock snapshot. `now` is the monotonic clock used for
/// TTL/cooldown arithmetic; `wire` is the unix-epoch wall-clock value
/// embedded in outgoing wire envelopes for cross-machine correlation.
/// The transport supplies both — the Router never reads the system
/// clock directly so tests can drive it with `tokio::time::pause`'d
/// and explicit-timestamp values.
#[derive(Debug, Clone, Copy)]
pub struct Clocks {
    pub now: Instant,
    pub wire: f64,
}

/// Per-target observed route — drives the transition log so steady-
/// state sends don't generate noise. Carries the redial-cooldown
/// timestamp so the gate trips exactly on the first observation of an
/// active relay relationship (or first re-observation after the
/// cooldown elapsed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRouteState {
    pub via: RouteVia,
    /// Last time we observed an active relay relationship with this
    /// peer — set by `send_to_peer` on Relay outcomes (sender side)
    /// AND by `process_inbound` on Relay envelopes addressed to us
    /// (receiver side). `None` means we have never observed a relay
    /// relationship since the route was created or the peer was
    /// re-discovered.
    pub last_observed_relay_at: Option<Instant>,
}

/// What we observed about the route to one peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteVia {
    Direct,
    Relay { forwarder: String },
}

/// Successful outcome of `send_to_peer`. `redial_target` is `Some`
/// iff this send tripped the redial cooldown gate — the transport
/// should attempt a background dial against the named peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    Direct,
    Relayed {
        forwarder: String,
        redial_target: Option<String>,
    },
    NoRoute,
}

/// What `process_inbound` / `process_inbound_sync` did with one
/// inbound message.
///
/// Both arms carry a `redial_target` because the receiver-side
/// observation (a `Relay` envelope addressed to us) both delivers a
/// payload to the user AND signals a partition with the relay's
/// originator — collapsing the two into one variant would force a
/// `(Option<msg>, Option<redial>)` shape with three impossible
/// combinations.
#[derive(Debug)]
pub enum InboundOutcome<I> {
    /// Caller's recv loop should yield this to the user. If
    /// `redial_target` is `Some`, the transport should ALSO kick a
    /// background dial against that peer (receiver-side observation
    /// of an active relay relationship — same signal as
    /// [`SendOutcome::Relayed::redial_target`]).
    ///
    /// `msg` is boxed to keep the enum stack-size small: `Handled`
    /// is 24 bytes and the unboxed `DistributedMessage` blew the
    /// enum out to ~356 bytes (clippy::large_enum_variant). Boxing
    /// the heavy variant shrinks the common-case copy.
    Deliver {
        msg: Box<DistributedMessage<I>>,
        redial_target: Option<String>,
    },
    /// Router consumed the message internally (forward / backoff /
    /// stale-drop). Caller continues receiving. `redial_target`
    /// reserved for future router-internal observations that don't
    /// produce a deliverable payload (currently always `None`).
    Handled { redial_target: Option<String> },
}

/// Errors surfaced through `send_to_peer`. Recoverable conditions
/// (closed channels, missing forwarders) are surfaced as `Err` so the
/// transport propagates the failure to its caller; the alternative —
/// converting them to `SendOutcome::NoRoute` — would silently drop
/// messages. `NoRoute` is reserved for "no path exists right now,
/// neither direct nor any forwarder" which is a different condition.
#[derive(Debug)]
pub enum RoutingError {
    /// Direct send was selected but the per-peer channel record had
    /// already been removed (dropped between blanket connection
    /// listing and the send). Caller likely wants to surface as a
    /// fatal "no connection".
    NoConnection { peer: String },
    /// The dispatcher returned `Err`, signalling the underlying
    /// connection is dead. Router has already removed the channel
    /// from `connections`; caller propagates the failure upward so
    /// retry / reroute happens at a higher layer if appropriate.
    DispatchFailed { peer: String, context: String },
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::NoConnection { peer } => {
                write!(f, "no connection to peer '{peer}'")
            }
            RoutingError::DispatchFailed { peer, context } => {
                write!(f, "dispatch to peer '{peer}' failed: {context}")
            }
        }
    }
}

impl std::error::Error for RoutingError {}

/// Drop entries from `outgoing_relays` whose `last_used_at` is older
/// than `RELAY_STATE_TTL`. Cheap (O(N)).
pub(super) fn prune_stale<I>(
    outgoing_relays: &mut HashMap<(String, u64), OutgoingRelay<I>>,
    now: Instant,
) {
    outgoing_relays.retain(|_, st| {
        now.duration_since(st.last_used_at) <= RELAY_STATE_TTL
    });
}

/// Drop blacklist entries older than `BLACKLIST_TTL` so a recovered
/// direct link gets re-tried.
pub(super) fn prune_blacklist(
    failed_forwarders: &mut HashMap<(String, String), Instant>,
    now: Instant,
) {
    failed_forwarders
        .retain(|_, t| now.duration_since(*t) < BLACKLIST_TTL);
}

/// Build the per-target blacklist for a routing decision: the set
/// of forwarder peer ids whose `(target, peer)` entry exists and is
/// still within the TTL. Caller passes this into the routing
/// helpers so steady-state `Direct` checks remain unaffected (the
/// target itself is never blacklisted).
pub(super) fn blacklist_for(
    failed_forwarders: &HashMap<(String, String), Instant>,
    target: &str,
    now: Instant,
) -> HashSet<String> {
    failed_forwarders
        .iter()
        .filter(|((t, _), ts)| {
            t == target && now.duration_since(**ts) < BLACKLIST_TTL
        })
        .map(|((_, peer), _)| peer.clone())
        .collect()
}
