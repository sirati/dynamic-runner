//! Peer-mesh routing dispatcher.
//!
//! A [`Router<I>`] owns the routing decision tree (direct / relay /
//! redial) for one node. Transports delegate every send and every
//! inbound dispatch to it via [`Router::send_to_peer`] /
//! [`Router::process_inbound`] / [`Router::process_inbound_sync`],
//! supplying a per-peer connection map keyed by id whose values
//! implement [`OutboundChannel<I>`]. The Router never holds transport
//! state and never dials peers — it returns a [`SendOutcome`] /
//! [`InboundOutcome`] that signals when the transport should kick a
//! redial; the transport implements that side of the boundary.
//!
//! All log lines on the relay path use `target: RELAY_LOG_TARGET`
//! ("dynrunner_relay") so an operator running with
//! `RUST_LOG=dynrunner_relay=info` sees the relay-path events
//! regardless of which crate the code happens to live in.
//!
//! # Concurrency
//!
//! Caller invariant: at most one in-flight `send_to_peer` /
//! `process_inbound` / `process_inbound_sync` call per Router. The
//! `&mut self` shape of every public method enforces this
//! structurally; the field-level split-borrow pattern inside each
//! method (e.g. `outgoing_relays.get_mut(&key)` held alongside
//! `&self.failed_forwarders`) only works because no concurrent
//! borrow exists. Future maintainers wanting reentrant-safe Router
//! must add a lock at the entry points.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use dynrunner_core::Identifier;

use crate::messages::DistributedMessage;
use crate::relay::channel::OutboundChannel;
use crate::relay::{forward_step, handle_backoff, route_send, BackoffDecision, OutgoingRelay,
    RouteDecision};

/// How long an outgoing-relay state entry survives without an update
/// before the periodic sweep prunes it. Picked larger than any
/// realistic forwarding round-trip across a multi-hop mesh; smaller
/// than the 30s peer-keepalive miss threshold so a dead-letter
/// state doesn't outlive the peer it was waiting on.
const RELAY_STATE_TTL: Duration = Duration::from_secs(20);

/// How long a per-target forwarder failure stays in the blacklist
/// before subsequent relays will retry that forwarder again. Picked
/// long enough that we don't hammer a confirmed-dead path on every
/// outbound message, but short enough that a re-established direct
/// link recovers without a whole-process restart.
const BLACKLIST_TTL: Duration = Duration::from_secs(120);

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
    Deliver {
        msg: DistributedMessage<I>,
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
fn prune_stale<I>(
    outgoing_relays: &mut HashMap<(String, u64), OutgoingRelay<I>>,
    now: Instant,
) {
    outgoing_relays.retain(|_, st| {
        now.duration_since(st.last_used_at) <= RELAY_STATE_TTL
    });
}

/// Drop blacklist entries older than `BLACKLIST_TTL` so a recovered
/// direct link gets re-tried.
fn prune_blacklist(
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
fn blacklist_for(
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

/// Single owner of peer-mesh routing decisions for one node.
///
/// The transport supplies a per-peer connection map (with values that
/// implement [`OutboundChannel<I>`]) and a [`Clocks`] snapshot on each
/// call. Router never reads the system clock and never holds
/// transport-specific state — so test drivers can simulate partitions
/// by mutating the connection map directly, and `tokio::time::pause`'d
/// scenarios drive the cooldown gate without races.
pub struct Router<I: Identifier> {
    self_id: String,
    /// Monotonic counter for relay ids generated by this node as
    /// originator. Forwarders never allocate — they preserve the
    /// originator's id.
    next_relay_id: u64,
    /// In-flight relays we've sent (originator) or forwarded
    /// (forwarder). Keyed by `(original_sender, relay_id)` so two
    /// originators using overlapping monotonic counters don't
    /// collide. The TTL sweep prunes entries whose `last_used_at` is
    /// older than `RELAY_STATE_TTL` so silent successes don't leak
    /// memory.
    outgoing_relays: HashMap<(String, u64), OutgoingRelay<I>>,
    /// Cross-relay blacklist of forwarders that recently bounced a
    /// relay back. Keyed by `(target, forwarder)`: a forwarder F that
    /// failed to deliver to target T1 is NOT also blacklisted for
    /// target T2 — the partition that broke F→T1 may not affect F→T2.
    /// Entries expire after `BLACKLIST_TTL` so a direct link
    /// re-established in the meantime gets re-tried rather than
    /// shadowed forever.
    failed_forwarders: HashMap<(String, String), Instant>,
    /// Per-target route observation: drives the only logging on the
    /// relay path (direct→relay, relay→relay, relay→direct) AND
    /// carries `last_observed_relay_at`, the redial-cooldown gate's
    /// single source of truth for "when did we last observe an active
    /// relay relationship with this peer?". Direct-only traffic does
    /// NOT touch the timestamp; only Relay outcomes (send-side) and
    /// Relay envelopes addressed to us (recv-side) do.
    route_state: HashMap<String, PeerRouteState>,
}

impl<I: Identifier> Router<I> {
    /// Construct an empty Router for the node identified by `self_id`.
    pub fn new(self_id: String) -> Self {
        Self {
            self_id,
            next_relay_id: 0,
            outgoing_relays: HashMap::new(),
            failed_forwarders: HashMap::new(),
            route_state: HashMap::new(),
        }
    }

    /// Sweep TTL'd state. Idempotent and cheap; safe to call from
    /// every entry point. Pre-fix-baseline: only `send_to_peer` would
    /// prune outgoing_relays, so a node that only *forwarded* (never
    /// *originated* sends) grew the relay map without bound. Calling
    /// this from every entry point bounds memory by the union of
    /// recent activity regardless of which side of the duplex was
    /// last touched.
    pub fn prune(&mut self, now: Instant) {
        prune_stale(&mut self.outgoing_relays, now);
        prune_blacklist(&mut self.failed_forwarders, now);
    }

    /// Originate a send to `target`. Routes via direct or relay,
    /// dispatches via `connections`, commits state on success, and
    /// emits the redial signal when the cooldown gate trips.
    ///
    /// On dispatch failure (channel closed) the dead channel is
    /// removed from `connections` and the failure is propagated as
    /// `RoutingError::DispatchFailed`.
    ///
    /// Caller invariant: at most one in-flight call per Router
    /// (`&mut self` enforces this structurally).
    pub fn send_to_peer<C: OutboundChannel<I>>(
        &mut self,
        target: &str,
        msg: DistributedMessage<I>,
        connections: &mut HashMap<String, C>,
        clocks: Clocks,
    ) -> Result<SendOutcome, RoutingError> {
        let relay_id = self.next_relay_id;
        let blacklist = blacklist_for(&self.failed_forwarders, target, clocks.now);
        let decision = route_send(
            connections,
            &self.self_id,
            target,
            relay_id,
            msg,
            clocks.wire,
            &blacklist,
        );
        match decision {
            RouteDecision::Direct(direct) => {
                let send_res = connections
                    .get(target)
                    .map(|chan| chan.dispatch(direct))
                    .ok_or_else(|| RoutingError::NoConnection {
                        peer: target.to_string(),
                    })?;
                if send_res.is_err() {
                    connections.remove(target);
                    return Err(RoutingError::DispatchFailed {
                        peer: target.to_string(),
                        context: "direct send: connection closed".to_string(),
                    });
                }
                self.observe_direct(target);
                Ok(SendOutcome::Direct)
            }
            RouteDecision::Relay {
                via,
                wrapped,
                bookkeeping,
            } => {
                let send_res = connections
                    .get(&via)
                    .map(|chan| chan.dispatch(wrapped))
                    .ok_or_else(|| RoutingError::NoConnection {
                        peer: via.clone(),
                    })?;
                if send_res.is_err() {
                    connections.remove(&via);
                    return Err(RoutingError::DispatchFailed {
                        peer: via.clone(),
                        context: format!(
                            "relay forwarder connection closed during send to '{target}'"
                        ),
                    });
                }
                self.next_relay_id = self.next_relay_id.wrapping_add(1);
                self.outgoing_relays
                    .insert((self.self_id.clone(), relay_id), bookkeeping);
                let redial_target =
                    self.observe_relay(target, &via, clocks.now);
                Ok(SendOutcome::Relayed {
                    forwarder: via,
                    redial_target,
                })
            }
            RouteDecision::NoRoute => Ok(SendOutcome::NoRoute),
        }
    }

    /// Async-safe inbound dispatch. Handles `Relay` / `RelayBackoff`
    /// envelopes internally (forward via `connections`, retry on
    /// backoff, drop on dead-end). Returns
    /// [`InboundOutcome::Deliver`] for non-routing variants and for
    /// `Relay` envelopes addressed to us; `Relay`-for-us
    /// additionally bumps `last_observed_relay_at[original_sender]`
    /// (receiver-side observation) and emits `redial_target` if the
    /// cooldown gate trips.
    pub fn process_inbound<C: OutboundChannel<I>>(
        &mut self,
        msg: DistributedMessage<I>,
        connections: &mut HashMap<String, C>,
        clocks: Clocks,
    ) -> InboundOutcome<I> {
        match msg {
            DistributedMessage::Relay {
                sender_id,
                timestamp,
                target_id,
                relay_id,
                path,
                inner,
            } => {
                if target_id == self.self_id {
                    // Receiver-side observation: the original sender
                    // is reaching us via relay, so we have an active
                    // relay relationship with them. Bump the cooldown
                    // gate against `sender_id` (the wire envelope's
                    // sender_id is the originator, not the immediate
                    // predecessor — see route_send/forward_step which
                    // preserve sender_id end-to-end), and emit
                    // redial_target if the gate trips.
                    //
                    // We do NOT touch `route_state[sender_id].via`:
                    // `via` represents OUR outbound route to the peer,
                    // which is only knowable from our send-side
                    // experience. Their→us being broken does not
                    // imply ours→them is broken (asymmetric
                    // partition); overwriting `via` here would
                    // produce a spurious direct→relay warn the next
                    // time we send to them while our direct link
                    // still works. The path argument is unused for
                    // the same reason.
                    let _ = path;
                    let redial_target =
                        self.observe_relay_recv(&sender_id, clocks.now);
                    return InboundOutcome::Deliver {
                        msg: *inner,
                        redial_target,
                    };
                }
                let blacklist =
                    blacklist_for(&self.failed_forwarders, &target_id, clocks.now);
                let decision = forward_step(
                    connections,
                    &self.self_id,
                    &target_id,
                    relay_id,
                    &path,
                    timestamp,
                    &sender_id,
                    inner,
                    &blacklist,
                );
                self.apply_forward_decision(
                    decision,
                    sender_id,
                    relay_id,
                    target_id,
                    path,
                    connections,
                    clocks,
                );
                InboundOutcome::Handled {
                    redial_target: None,
                }
            }
            DistributedMessage::RelayBackoff {
                sender_id: failed_via,
                timestamp: _,
                original_sender,
                relay_id,
            } => {
                self.handle_inbound_backoff(
                    original_sender,
                    relay_id,
                    failed_via,
                    connections,
                    clocks,
                );
                InboundOutcome::Handled {
                    redial_target: None,
                }
            }
            other => InboundOutcome::Deliver {
                msg: other,
                redial_target: None,
            },
        }
    }

    /// Sync inbound dispatch for fast-path try-recv loops.
    ///
    /// Cannot forward `Relay` envelopes addressed to others, because
    /// forwarding requires both state mutation AND outbound dispatch
    /// against the connections map; preserved-from-pre-refactor
    /// behavior is to drop them with a warn. `RelayBackoff` is
    /// dropped silently for the same reason.
    ///
    /// `Relay` envelopes addressed to us are unwrapped and delivered,
    /// AND the receiver-side cooldown gate fires — bookkeeping is
    /// pure state mutation, no outbound dispatch needed, so nothing
    /// in the sync constraint excludes it. (Pre-A3.M1 this branch
    /// suppressed the redial signal, leaving a latent gap if a
    /// consumer ever drove recv via try_recv only.)
    pub fn process_inbound_sync(
        &mut self,
        msg: DistributedMessage<I>,
        clocks: Clocks,
    ) -> InboundOutcome<I> {
        match msg {
            DistributedMessage::Relay {
                sender_id,
                target_id,
                inner,
                ..
            } if target_id == self.self_id => {
                let redial_target = self.observe_relay_recv(&sender_id, clocks.now);
                InboundOutcome::Deliver {
                    msg: *inner,
                    redial_target,
                }
            }
            DistributedMessage::Relay { target_id, .. } => {
                tracing::warn!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target_id,
                    "try_recv path dropped relay: cannot forward synchronously, use recv_peer"
                );
                InboundOutcome::Handled {
                    redial_target: None,
                }
            }
            DistributedMessage::RelayBackoff { .. } => InboundOutcome::Handled {
                redial_target: None,
            },
            other => InboundOutcome::Deliver {
                msg: other,
                redial_target: None,
            },
        }
    }

    /// Inspect the per-peer route observation map.
    ///
    /// Exposed for cross-crate tests (`dynrunner-transport-channel`'s
    /// `tests/mesh_partition.rs`) so partition / heal scenarios can
    /// assert on `last_observed_relay_at` directly without round-
    /// tripping through `SendOutcome`. Gated behind the `test-utils`
    /// feature so production builds cannot accidentally depend on
    /// internal routing state — flip the feature on in dev-deps,
    /// never in runtime deps.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn route_state(&self) -> &HashMap<String, PeerRouteState> {
        &self.route_state
    }

    // ── private helpers ──

    /// Observe a Direct outcome for `target`. Updates `route_state`
    /// (logging the transition if it changed) but DOES NOT touch
    /// `last_observed_relay_at` — the cooldown gate is only driven
    /// by Relay outcomes.
    fn observe_direct(&mut self, target: &str) {
        let prev = self.route_state.get(target).cloned();
        let new_via = RouteVia::Direct;
        match prev.as_ref().map(|s| &s.via) {
            None => {}
            Some(RouteVia::Direct) => {}
            Some(RouteVia::Relay { forwarder }) => {
                tracing::info!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    from = %forwarder,
                    "peer direct link restored"
                );
            }
        }
        // Keep last_observed_relay_at — Direct doesn't touch it.
        let last_observed_relay_at =
            prev.and_then(|s| s.last_observed_relay_at);
        self.route_state.insert(
            target.to_string(),
            PeerRouteState {
                via: new_via,
                last_observed_relay_at,
            },
        );
    }

    /// Observe a Relay outcome for `target` via forwarder `forwarder`
    /// at time `now`. Updates `route_state`, logs the transition if it
    /// changed, and applies the redial-cooldown gate to determine
    /// whether to emit a `redial_target`. Returns `Some(target)` iff
    /// the cooldown tripped on this observation.
    fn observe_relay(
        &mut self,
        target: &str,
        forwarder: &str,
        now: Instant,
    ) -> Option<String> {
        let prev = self.route_state.get(target).cloned();
        let new_via = RouteVia::Relay {
            forwarder: forwarder.to_string(),
        };
        // Transition log fires only on a STATE CHANGE — the first
        // observation of a peer's route is silent (matches the
        // pre-Router behavior of `observe_transition` in
        // relay/mod.rs, where `(None, _) => {}` is the silent arm).
        match prev.as_ref().map(|s| &s.via) {
            None => {}
            Some(RouteVia::Direct) => {
                tracing::warn!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    relay = %forwarder,
                    "peer relay engaged: direct link unreachable, forwarding via peer"
                );
            }
            Some(RouteVia::Relay { forwarder: old }) if old != forwarder => {
                tracing::info!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target,
                    from = %old,
                    to = %forwarder,
                    "peer relay forwarder changed"
                );
            }
            Some(RouteVia::Relay { .. }) => {}
        }
        let prev_observed = prev.as_ref().and_then(|s| s.last_observed_relay_at);
        let trip = match prev_observed {
            None => true,
            Some(t) => now.duration_since(t) >= REDIAL_COOLDOWN,
        };
        let redial_target = if trip {
            Some(target.to_string())
        } else {
            None
        };
        self.route_state.insert(
            target.to_string(),
            PeerRouteState {
                via: new_via,
                last_observed_relay_at: Some(now),
            },
        );
        redial_target
    }

    /// Receiver-side relay observation: a `Relay` envelope addressed
    /// to us arrived from `peer`. Bumps `last_observed_relay_at[peer]`
    /// and applies the redial-cooldown gate, but does NOT touch
    /// `route_state[peer].via` — `via` is OUR outbound route, only
    /// knowable from our send-side experience. Their→us being broken
    /// does not imply ours→them is broken (asymmetric partitions are
    /// possible in principle), so this path stays silent on logging
    /// and lets the next outgoing send observe the true outbound route.
    fn observe_relay_recv(&mut self, peer: &str, now: Instant) -> Option<String> {
        let prev = self.route_state.get(peer).cloned();
        let prev_observed = prev.as_ref().and_then(|s| s.last_observed_relay_at);
        let trip = match prev_observed {
            None => true,
            Some(t) => now.duration_since(t) >= REDIAL_COOLDOWN,
        };
        let redial_target = if trip {
            Some(peer.to_string())
        } else {
            None
        };
        let via = prev
            .as_ref()
            .map(|s| s.via.clone())
            .unwrap_or(RouteVia::Direct);
        self.route_state.insert(
            peer.to_string(),
            PeerRouteState {
                via,
                last_observed_relay_at: Some(now),
            },
        );
        redial_target
    }

    /// Apply the forward-step decision: deliver direct, send a new
    /// Relay envelope (recording bookkeeping for backoff), or send a
    /// `RelayBackoff` to our predecessor on dead-end.
    fn apply_forward_decision<C: OutboundChannel<I>>(
        &mut self,
        decision: RouteDecision<I>,
        sender_id: String,
        relay_id: u64,
        target_id: String,
        path: Vec<String>,
        connections: &mut HashMap<String, C>,
        clocks: Clocks,
    ) {
        match decision {
            RouteDecision::Direct(inner_unwrapped) => {
                let send_res = connections
                    .get(&target_id)
                    .map(|chan| chan.dispatch(inner_unwrapped));
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) => {
                        connections.remove(&target_id);
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            "relay forward failed: target connection closed"
                        );
                    }
                    None => {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            "relay forward target unexpectedly missing"
                        );
                    }
                }
            }
            RouteDecision::Relay {
                via,
                wrapped,
                bookkeeping,
            } => {
                let send_res =
                    connections.get(&via).map(|chan| chan.dispatch(wrapped));
                match send_res {
                    Some(Ok(())) => {
                        self.outgoing_relays
                            .insert((sender_id.clone(), relay_id), bookkeeping);
                    }
                    Some(Err(_)) => {
                        connections.remove(&via);
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            next = %via,
                            "relay forward failed: forwarder connection closed"
                        );
                    }
                    None => {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            next = %via,
                            "relay forward target unexpectedly missing"
                        );
                    }
                }
            }
            RouteDecision::NoRoute => {
                if let Some(predecessor) = path.last() {
                    let backoff = DistributedMessage::RelayBackoff {
                        sender_id: self.self_id.clone(),
                        timestamp: clocks.wire,
                        original_sender: sender_id.clone(),
                        relay_id,
                    };
                    if let Some(chan) = connections.get(predecessor) {
                        if chan.dispatch(backoff).is_err() {
                            tracing::warn!(
                                target: RELAY_LOG_TARGET,
                                target_peer = %target_id,
                                predecessor = %predecessor,
                                "relay backoff send failed: predecessor connection closed"
                            );
                        }
                    } else {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_id,
                            predecessor = %predecessor,
                            "relay backoff send failed: predecessor not in connections"
                        );
                    }
                } else {
                    tracing::warn!(
                        target: RELAY_LOG_TARGET,
                        target_peer = %target_id,
                        "dropping relay: empty path on dead-end (no predecessor to back off to)"
                    );
                }
            }
        }
    }

    /// Handle an inbound `RelayBackoff` for `(original_sender,
    /// relay_id)`: look up our state, record the failed forwarder in
    /// the per-target blacklist for future relays, ask the routing
    /// helper what to do, and apply.
    fn handle_inbound_backoff<C: OutboundChannel<I>>(
        &mut self,
        original_sender: String,
        relay_id: u64,
        failed_via: String,
        connections: &mut HashMap<String, C>,
        clocks: Clocks,
    ) {
        let key = (original_sender.clone(), relay_id);
        let state = match self.outgoing_relays.get_mut(&key) {
            Some(s) => s,
            None => {
                // Stale or duplicate backoff — our state was already
                // pruned (TTL) or we never had this relay. Silent.
                return;
            }
        };
        // Record per-target so subsequent relays don't keep paying
        // the same dead-end. Keyed by (target, forwarder) so the
        // failure of `failed_via` to deliver to `state.target`
        // doesn't blacklist it for any other destination.
        self.failed_forwarders
            .insert((state.target.clone(), failed_via.clone()), clocks.now);
        let blacklist =
            blacklist_for(&self.failed_forwarders, &state.target, clocks.now);
        let decision = handle_backoff(
            state,
            connections,
            &self.self_id,
            relay_id,
            &failed_via,
            clocks.wire,
            &blacklist,
        );
        match decision {
            BackoffDecision::Retry { via, wrapped } => {
                let send_res =
                    connections.get(&via).map(|chan| chan.dispatch(wrapped));
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        connections.remove(&via);
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %state.target,
                            next = %via,
                            "relay retry send failed; further retries will fire on next backoff"
                        );
                    }
                }
            }
            BackoffDecision::PropagateBackoff { to, msg } => {
                let target_for_log = state.target.clone();
                let send_res = connections.get(&to).map(|chan| chan.dispatch(msg));
                self.outgoing_relays.remove(&key);
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        tracing::warn!(
                            target: RELAY_LOG_TARGET,
                            target_peer = %target_for_log,
                            predecessor = %to,
                            "relay backoff propagation failed: predecessor connection closed"
                        );
                    }
                }
            }
            BackoffDecision::Drop => {
                let target_for_log = state.target.clone();
                self.outgoing_relays.remove(&key);
                tracing::warn!(
                    target: RELAY_LOG_TARGET,
                    target_peer = %target_for_log,
                    relay_id,
                    original_sender = %original_sender,
                    "relay dropped: all paths exhausted at originator"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::testing::{new_log, DispatchedRecord, RecordingChannel};

    /// Trivial Identifier impl so we can build messages without
    /// pulling in the real cluster types.
    fn keepalive(sender: &str) -> DistributedMessage<()> {
        DistributedMessage::Keepalive {
            sender_id: sender.into(),
            timestamp: 1.0,
            secondary_id: sender.into(),
            active_workers: 0,
        }
    }

    /// Build a connection map populated with a `RecordingChannel` per
    /// id, sharing one log buffer.
    fn conns_with_log(
        ids: &[&str],
        log: &Rc<RefCell<Vec<DispatchedRecord<()>>>>,
    ) -> HashMap<String, RecordingChannel<()>> {
        ids.iter()
            .map(|id| (id.to_string(), RecordingChannel::new(id.to_string(), log.clone())))
            .collect()
    }

    fn clocks_at(now: Instant, wire: f64) -> Clocks {
        Clocks { now, wire }
    }

    use std::cell::RefCell;
    use std::rc::Rc;

    // ── send_to_peer ──

    #[test]
    fn send_to_peer_direct_when_target_reachable() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b", "c"], &log);
        let mut router = Router::<()>::new("a".into());
        let outcome = router
            .send_to_peer(
                "b",
                keepalive("a"),
                &mut conns,
                clocks_at(Instant::now(), 1.0),
            )
            .expect("send ok");
        assert_eq!(outcome, SendOutcome::Direct);
        let entries = log.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].addressee, "b");
        assert!(matches!(entries[0].msg, DistributedMessage::Keepalive { .. }));
        // Direct path must NOT have set last_observed_relay_at.
        assert!(router
            .route_state
            .get("b")
            .and_then(|s| s.last_observed_relay_at)
            .is_none());
    }

    #[test]
    fn send_to_peer_relays_via_lowest_and_emits_redial_on_first_observation() {
        let log = new_log::<()>();
        // Target d not in our connections; b is the lowest non-self.
        let mut conns = conns_with_log(&["b", "c"], &log);
        let mut router = Router::<()>::new("a".into());
        let now = Instant::now();
        let outcome = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(now, 1.0))
            .expect("send ok");
        match outcome {
            SendOutcome::Relayed {
                forwarder,
                redial_target,
            } => {
                assert_eq!(forwarder, "b");
                assert_eq!(redial_target.as_deref(), Some("d"));
            }
            other => panic!("expected Relayed: {other:?}"),
        }
        let entries = log.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].addressee, "b", "envelope went to forwarder");
        match &entries[0].msg {
            DistributedMessage::Relay {
                target_id,
                sender_id,
                relay_id,
                path,
                ..
            } => {
                assert_eq!(target_id, "d");
                assert_eq!(sender_id, "a");
                assert_eq!(*relay_id, 0);
                assert_eq!(path, &vec!["a".to_string()]);
            }
            other => panic!("expected Relay envelope: {other:?}"),
        }
        assert_eq!(
            router
                .route_state
                .get("d")
                .expect("route_state populated for relay target")
                .last_observed_relay_at,
            Some(now),
            "last_observed_relay_at recorded"
        );
    }

    #[test]
    fn send_to_peer_no_route_when_alone() {
        let log = new_log::<()>();
        let mut conns: HashMap<String, RecordingChannel<()>> = HashMap::new();
        let mut router = Router::<()>::new("a".into());
        let outcome = router
            .send_to_peer(
                "b",
                keepalive("a"),
                &mut conns,
                clocks_at(Instant::now(), 1.0),
            )
            .expect("send ok");
        assert!(matches!(outcome, SendOutcome::NoRoute));
        assert!(log.borrow().is_empty());
        let _ = log;
    }

    #[test]
    fn send_to_peer_dispatch_failure_drops_channel() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        // Simulate b's pipe being dead.
        conns.get("b").unwrap().disable();
        let mut router = Router::<()>::new("a".into());
        let err = router
            .send_to_peer(
                "b",
                keepalive("a"),
                &mut conns,
                clocks_at(Instant::now(), 1.0),
            )
            .expect_err("dispatch failure");
        assert!(matches!(err, RoutingError::DispatchFailed { .. }));
        assert!(!conns.contains_key("b"), "dead channel evicted from map");
    }

    // ── redial cooldown gate ──

    #[test]
    fn relay_redial_signal_suppressed_within_cooldown() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        let mut router = Router::<()>::new("a".into());
        let t0 = Instant::now();
        // First relay observation trips the gate.
        let out1 = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
            .unwrap();
        assert!(
            matches!(out1, SendOutcome::Relayed { redial_target: Some(ref id), .. } if id == "d")
        );
        // Second observation 5s later → gate suppresses.
        let out2 = router
            .send_to_peer(
                "d",
                keepalive("a"),
                &mut conns,
                clocks_at(t0 + Duration::from_secs(5), 2.0),
            )
            .unwrap();
        assert!(
            matches!(out2, SendOutcome::Relayed { redial_target: None, .. }),
            "second relay within cooldown emits no redial: {out2:?}"
        );
    }

    #[test]
    fn relay_redial_signal_re_fires_after_cooldown() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        let mut router = Router::<()>::new("a".into());
        let t0 = Instant::now();
        let _ = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
            .unwrap();
        // Past cooldown → fresh signal.
        let out = router
            .send_to_peer(
                "d",
                keepalive("a"),
                &mut conns,
                clocks_at(t0 + REDIAL_COOLDOWN + Duration::from_secs(1), 2.0),
            )
            .unwrap();
        assert!(
            matches!(out, SendOutcome::Relayed { redial_target: Some(ref id), .. } if id == "d")
        );
    }

    // ── process_inbound: forwarder path ──

    #[test]
    fn process_inbound_forwards_relay_via_next_hop() {
        // Forwarder c sees a Relay from a targeted at z. c has direct
        // links to {a, b, d}; pick the lowest non-{path,target,self} = b.
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["a", "b", "d"], &log);
        let mut router = Router::<()>::new("c".into());
        let inbound = DistributedMessage::Relay {
            sender_id: "a".into(),
            timestamp: 1.0,
            target_id: "z".into(),
            relay_id: 7,
            path: vec!["a".into()],
            inner: Box::new(keepalive("a")),
        };
        let outcome =
            router.process_inbound(inbound, &mut conns, clocks_at(Instant::now(), 1.0));
        assert!(matches!(
            outcome,
            InboundOutcome::Handled {
                redial_target: None
            }
        ));
        let entries = log.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].addressee, "b");
        match &entries[0].msg {
            DistributedMessage::Relay {
                relay_id,
                target_id,
                path,
                sender_id,
                ..
            } => {
                assert_eq!(*relay_id, 7);
                assert_eq!(target_id, "z");
                assert_eq!(sender_id, "a");
                assert_eq!(path, &vec!["a".to_string(), "c".to_string()]);
            }
            other => panic!("expected forwarded Relay: {other:?}"),
        }
        // Forwarder bookkeeping recorded for backoff.
        assert!(router
            .outgoing_relays
            .contains_key(&("a".to_string(), 7)));
    }

    // ── process_inbound: receiver-side relay observation ──

    #[test]
    fn process_inbound_relay_for_self_emits_redial() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        let mut router = Router::<()>::new("a".into());
        // d sends a Relay envelope targeted at a, immediately
        // forwarded by b (path=[d, b], from a's view).
        let inbound = DistributedMessage::Relay {
            sender_id: "d".into(),
            timestamp: 1.0,
            target_id: "a".into(),
            relay_id: 3,
            path: vec!["d".into(), "b".into()],
            inner: Box::new(keepalive("d")),
        };
        let now = Instant::now();
        let outcome = router.process_inbound(inbound, &mut conns, clocks_at(now, 1.0));
        match outcome {
            InboundOutcome::Deliver { msg, redial_target } => {
                assert!(matches!(msg, DistributedMessage::Keepalive { .. }));
                assert_eq!(redial_target.as_deref(), Some("d"));
            }
            other => panic!("expected Deliver with redial target d: {other:?}"),
        }
        // Receiver-side observation must have written
        // last_observed_relay_at against the originator.
        assert_eq!(
            router
                .route_state
                .get("d")
                .and_then(|s| s.last_observed_relay_at),
            Some(now)
        );
        // No outbound dispatch — we delivered, didn't forward.
        assert!(log.borrow().is_empty());
    }

    #[test]
    fn process_inbound_relay_for_self_preserves_existing_direct_via() {
        // A1.M1 regression: receiver-side relay observation must NOT
        // overwrite route_state[sender].via if we already observed a
        // Direct route to the sender. Asymmetric partitions are
        // possible in principle (their→us broken, ours→them works);
        // the next outbound send must NOT log a spurious
        // direct→relay warn.
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b", "d"], &log);
        let mut router = Router::<()>::new("a".into());
        // Establish a Direct route to d via a real send.
        let _ = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(Instant::now(), 1.0))
            .unwrap();
        match &router.route_state.get("d").expect("route_state for d").via {
            RouteVia::Direct => {}
            other => panic!("expected Direct, got {other:?}"),
        }
        // Now d sends a relay envelope addressed to us via b.
        let inbound = DistributedMessage::Relay {
            sender_id: "d".into(),
            timestamp: 1.0,
            target_id: "a".into(),
            relay_id: 3,
            path: vec!["d".into(), "b".into()],
            inner: Box::new(keepalive("d")),
        };
        let _ = router.process_inbound(inbound, &mut conns, clocks_at(Instant::now(), 2.0));
        // via must remain Direct: their inbound being relayed says
        // nothing about our outbound.
        match &router.route_state.get("d").expect("route_state for d").via {
            RouteVia::Direct => {}
            other => panic!(
                "via should remain Direct after recv-relay-for-self, got {other:?}"
            ),
        }
        assert!(router
            .route_state
            .get("d")
            .and_then(|s| s.last_observed_relay_at)
            .is_some());
    }

    #[test]
    fn process_inbound_relay_for_self_redial_suppressed_within_cooldown() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        let mut router = Router::<()>::new("a".into());
        let t0 = Instant::now();
        let envelope = || DistributedMessage::Relay {
            sender_id: "d".into(),
            timestamp: 1.0,
            target_id: "a".into(),
            relay_id: 3,
            path: vec!["d".into(), "b".into()],
            inner: Box::new(keepalive("d")),
        };
        let _ = router.process_inbound(envelope(), &mut conns, clocks_at(t0, 1.0));
        let outcome = router.process_inbound(
            envelope(),
            &mut conns,
            clocks_at(t0 + Duration::from_secs(5), 2.0),
        );
        match outcome {
            InboundOutcome::Deliver { redial_target, .. } => {
                assert!(redial_target.is_none(), "second observation suppressed");
            }
            other => panic!("expected Deliver: {other:?}"),
        }
    }

    // ── process_inbound: backoff retry & propagate ──

    #[test]
    fn process_inbound_backoff_retries_via_next_candidate() {
        // Originator a sent to d via b (relay_id 0). Backoff arrives;
        // a retries via c.
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b", "c"], &log);
        let mut router = Router::<()>::new("a".into());
        let _ = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(Instant::now(), 1.0))
            .unwrap();
        log.borrow_mut().clear();
        let backoff = DistributedMessage::RelayBackoff {
            sender_id: "b".into(),
            timestamp: 2.0,
            original_sender: "a".into(),
            relay_id: 0,
        };
        let outcome =
            router.process_inbound(backoff, &mut conns, clocks_at(Instant::now(), 2.0));
        assert!(matches!(
            outcome,
            InboundOutcome::Handled {
                redial_target: None
            }
        ));
        let entries = log.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].addressee, "c", "retry went to next candidate");
        assert!(matches!(entries[0].msg, DistributedMessage::Relay { .. }));
        // Failed_via b is now blacklisted for target d.
        assert!(router
            .failed_forwarders
            .contains_key(&("d".to_string(), "b".to_string())));
    }

    #[test]
    fn process_inbound_backoff_propagates_when_forwarder_exhausted() {
        // Forwarder c received a relay from a for target z; c picked
        // d. Now d's backoff returns and c has no other candidates
        // (a is in path, c is self, d is tried). c propagates
        // backoff to predecessor a.
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["a", "d"], &log);
        let mut router = Router::<()>::new("c".into());
        let inbound = DistributedMessage::Relay {
            sender_id: "a".into(),
            timestamp: 1.0,
            target_id: "z".into(),
            relay_id: 9,
            path: vec!["a".into()],
            inner: Box::new(keepalive("a")),
        };
        let _ = router.process_inbound(inbound, &mut conns, clocks_at(Instant::now(), 1.0));
        log.borrow_mut().clear();
        let backoff = DistributedMessage::RelayBackoff {
            sender_id: "d".into(),
            timestamp: 2.0,
            original_sender: "a".into(),
            relay_id: 9,
        };
        let outcome =
            router.process_inbound(backoff, &mut conns, clocks_at(Instant::now(), 2.0));
        assert!(matches!(
            outcome,
            InboundOutcome::Handled {
                redial_target: None
            }
        ));
        let entries = log.borrow();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].addressee, "a", "backoff propagated to predecessor");
        match &entries[0].msg {
            DistributedMessage::RelayBackoff {
                sender_id,
                relay_id,
                original_sender,
                ..
            } => {
                assert_eq!(sender_id, "c");
                assert_eq!(*relay_id, 9);
                assert_eq!(original_sender, "a");
            }
            other => panic!("expected RelayBackoff: {other:?}"),
        }
        // Local state for the relay we propagated must be removed.
        assert!(!router
            .outgoing_relays
            .contains_key(&("a".to_string(), 9)));
    }

    #[test]
    fn process_inbound_backoff_drops_when_originator_exhausted() {
        // Originator a sent to d via b (only candidate). Backoff
        // returns; no other candidates → drop. No further dispatch,
        // local state cleared.
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        let mut router = Router::<()>::new("a".into());
        let _ = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(Instant::now(), 1.0))
            .unwrap();
        log.borrow_mut().clear();
        let backoff = DistributedMessage::RelayBackoff {
            sender_id: "b".into(),
            timestamp: 2.0,
            original_sender: "a".into(),
            relay_id: 0,
        };
        let _ = router.process_inbound(backoff, &mut conns, clocks_at(Instant::now(), 2.0));
        assert!(log.borrow().is_empty(), "originator drop emits nothing");
        assert!(!router
            .outgoing_relays
            .contains_key(&("a".to_string(), 0)));
    }

    // ── process_inbound: non-routing pass-through ──

    #[test]
    fn process_inbound_non_routing_delivers() {
        let log = new_log::<()>();
        let mut conns: HashMap<String, RecordingChannel<()>> = HashMap::new();
        let mut router = Router::<()>::new("a".into());
        let outcome = router.process_inbound(
            keepalive("b"),
            &mut conns,
            clocks_at(Instant::now(), 1.0),
        );
        match outcome {
            InboundOutcome::Deliver { msg, redial_target } => {
                assert!(matches!(msg, DistributedMessage::Keepalive { .. }));
                assert!(redial_target.is_none());
            }
            other => panic!("expected Deliver: {other:?}"),
        }
        assert!(log.borrow().is_empty());
    }

    // ── process_inbound_sync ──

    #[test]
    fn process_inbound_sync_delivers_relay_for_self_and_emits_redial() {
        // A3.M1: sync path now mirrors the async path for
        // Relay-for-self — receiver-side bookkeeping is pure state
        // mutation (no outbound dispatch), so the sync constraint
        // does not exclude it. A consumer driving recv via try_recv
        // only must NOT silently lose the redial safety net.
        let mut router = Router::<()>::new("a".into());
        let inbound = DistributedMessage::Relay {
            sender_id: "d".into(),
            timestamp: 1.0,
            target_id: "a".into(),
            relay_id: 3,
            path: vec!["d".into(), "b".into()],
            inner: Box::new(keepalive("d")),
        };
        let now = Instant::now();
        let outcome = router.process_inbound_sync(inbound, clocks_at(now, 1.0));
        match outcome {
            InboundOutcome::Deliver { msg, redial_target } => {
                assert!(matches!(msg, DistributedMessage::Keepalive { .. }));
                assert_eq!(redial_target.as_deref(), Some("d"));
            }
            other => panic!("expected Deliver: {other:?}"),
        }
        assert_eq!(
            router
                .route_state
                .get("d")
                .and_then(|s| s.last_observed_relay_at),
            Some(now)
        );
    }

    #[test]
    fn process_inbound_sync_drops_relay_for_others() {
        let mut router = Router::<()>::new("a".into());
        let inbound = DistributedMessage::Relay {
            sender_id: "d".into(),
            timestamp: 1.0,
            target_id: "z".into(),
            relay_id: 3,
            path: vec!["d".into()],
            inner: Box::new(keepalive("d")),
        };
        let outcome = router.process_inbound_sync(inbound, clocks_at(Instant::now(), 1.0));
        assert!(matches!(
            outcome,
            InboundOutcome::Handled {
                redial_target: None
            }
        ));
    }

    #[test]
    fn process_inbound_sync_drops_backoff() {
        let mut router = Router::<()>::new("a".into());
        let inbound = DistributedMessage::RelayBackoff {
            sender_id: "b".into(),
            timestamp: 1.0,
            original_sender: "a".into(),
            relay_id: 0,
        };
        let outcome = router.process_inbound_sync(inbound, clocks_at(Instant::now(), 1.0));
        assert!(matches!(
            outcome,
            InboundOutcome::Handled {
                redial_target: None
            }
        ));
    }

    // ── prune ──

    #[test]
    fn prune_evicts_stale_outgoing_relays() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b"], &log);
        let mut router = Router::<()>::new("a".into());
        let t0 = Instant::now();
        let _ = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
            .unwrap();
        assert!(router
            .outgoing_relays
            .contains_key(&("a".to_string(), 0)));
        // Past TTL.
        router.prune(t0 + RELAY_STATE_TTL + Duration::from_secs(1));
        assert!(!router
            .outgoing_relays
            .contains_key(&("a".to_string(), 0)));
    }

    #[test]
    fn prune_evicts_stale_blacklist() {
        let log = new_log::<()>();
        let mut conns = conns_with_log(&["b", "c"], &log);
        let mut router = Router::<()>::new("a".into());
        let t0 = Instant::now();
        let _ = router
            .send_to_peer("d", keepalive("a"), &mut conns, clocks_at(t0, 1.0))
            .unwrap();
        // Backoff inserts blacklist entry under (target=d, peer=b).
        let backoff = DistributedMessage::RelayBackoff {
            sender_id: "b".into(),
            timestamp: 2.0,
            original_sender: "a".into(),
            relay_id: 0,
        };
        let _ = router.process_inbound(backoff, &mut conns, clocks_at(t0, 2.0));
        assert!(router
            .failed_forwarders
            .contains_key(&("d".to_string(), "b".to_string())));
        // Past blacklist TTL.
        router.prune(t0 + BLACKLIST_TTL + Duration::from_secs(1));
        assert!(!router
            .failed_forwarders
            .contains_key(&("d".to_string(), "b".to_string())));
    }
}
