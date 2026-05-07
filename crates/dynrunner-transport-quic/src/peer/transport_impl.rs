//! `PeerTransport` impl for `PeerNetwork`. The inherent methods stay in
//! `mod.rs` so this file is purely the trait-glue layer.
//!
//! Routing decisions live in
//! [`dynrunner_protocol_primary_secondary::relay`]: this file
//! consumes [`RouteDecision`] and [`BackoffDecision`] and applies
//! them via the per-peer mpsc channels.

use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime};

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    forward_step, handle_backoff, observe_transition, route_send, BackoffDecision,
    DistributedMessage, OutgoingRelay, PeerConnectionInfo, PeerTransport, RouteDecision,
};

use super::PeerNetwork;

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

fn timestamp_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Drop entries from `outgoing_relays` whose `last_used_at` is older
/// than `RELAY_STATE_TTL`. Cheap (O(N)).
fn prune_stale<I>(
    outgoing_relays: &mut std::collections::HashMap<
        (String, u64),
        OutgoingRelay<I>,
    >,
) {
    let now = SystemTime::now();
    outgoing_relays.retain(|_, st| {
        now.duration_since(st.last_used_at)
            .map(|d| d <= RELAY_STATE_TTL)
            .unwrap_or(true)
    });
}

/// Drop blacklist entries older than `BLACKLIST_TTL` so a recovered
/// direct link gets re-tried.
fn prune_blacklist(
    failed_forwarders: &mut std::collections::HashMap<(String, String), Instant>,
) {
    let now = Instant::now();
    failed_forwarders
        .retain(|_, t| now.duration_since(*t) < BLACKLIST_TTL);
}

/// Build the per-target blacklist for a routing decision: the set
/// of forwarder peer ids whose `(target, peer)` entry exists and is
/// still within the TTL. Caller passes this into the routing
/// helpers so steady-state `Direct` checks remain unaffected (the
/// target itself is never blacklisted).
fn blacklist_for(
    failed_forwarders: &std::collections::HashMap<(String, String), Instant>,
    target: &str,
) -> HashSet<String> {
    let now = Instant::now();
    failed_forwarders
        .iter()
        .filter(|((t, _), ts)| {
            t == target && now.duration_since(**ts) < BLACKLIST_TTL
        })
        .map(|((_, peer), _)| peer.clone())
        .collect()
}

impl<I: Identifier> PeerNetwork<I> {
    /// Single-call TTL sweep: prunes both `outgoing_relays` (entries
    /// whose forwarding round-trip exceeded `RELAY_STATE_TTL`) and
    /// `failed_forwarders` (per-target blacklist entries past
    /// `BLACKLIST_TTL`). Called from every public entry point on the
    /// transport so memory is bounded by the union of recent activity
    /// regardless of which side of the duplex was last touched —
    /// pre-fix only `send_to_peer` pruned `outgoing_relays`, so a
    /// node that only *forwarded* (never *originated* sends) grew
    /// the relay map without bound.
    fn prune_relay_state(&mut self) {
        prune_stale(&mut self.outgoing_relays);
        prune_blacklist(&mut self.failed_forwarders);
    }
}

impl<I: Identifier> PeerTransport<I> for PeerNetwork<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.drain_new_connections();
        self.prune_relay_state();
        let mut errors = Vec::new();
        for (peer_id, tx) in &self.connections {
            if tx.send(msg.clone()).is_err() {
                errors.push(peer_id.clone());
            }
        }
        for peer_id in &errors {
            self.connections.remove(peer_id);
            tracing::warn!(peer = %peer_id, "peer disconnected during broadcast");
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        self.drain_new_connections();
        self.prune_relay_state();
        let now = timestamp_secs();
        let relay_id = self.next_relay_id;
        let blacklist = blacklist_for(&self.failed_forwarders, peer_id);
        let decision = route_send(
            &self.connections,
            &self.peer_id,
            peer_id,
            relay_id,
            msg,
            now,
            &blacklist,
        );
        match decision {
            RouteDecision::Direct(direct) => {
                let send_res = self
                    .connections
                    .get(peer_id)
                    .map(|tx| tx.send(direct))
                    .ok_or_else(|| format!("no connection to peer '{peer_id}'"))?;
                if send_res.is_err() {
                    self.connections.remove(peer_id);
                    return Err(format!(
                        "direct send to peer '{peer_id}' failed: connection closed"
                    ));
                }
                observe_transition(&mut self.route_state, peer_id, peer_id);
                Ok(())
            }
            RouteDecision::Relay {
                via,
                wrapped,
                bookkeeping,
            } => {
                let send_res = self
                    .connections
                    .get(&via)
                    .map(|tx| tx.send(wrapped))
                    .ok_or_else(|| format!("relay forwarder '{via}' not connected"))?;
                if send_res.is_err() {
                    self.connections.remove(&via);
                    return Err(format!(
                        "relay forwarder '{via}' connection closed during send to '{peer_id}'"
                    ));
                }
                self.next_relay_id = self.next_relay_id.wrapping_add(1);
                self.outgoing_relays
                    .insert((self.peer_id.clone(), relay_id), bookkeeping);
                observe_transition(&mut self.route_state, peer_id, &via);
                Ok(())
            }
            RouteDecision::NoRoute => Err(format!(
                "no route to peer '{peer_id}': direct unreachable and no forwarder available"
            )),
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.drain_new_connections();
        self.prune_relay_state();
        loop {
            tokio::select! {
                msg = self.incoming_rx.recv() => {
                    self.drain_new_connections();
                    self.prune_relay_state();
                    let msg = msg?;
                    match msg {
                        DistributedMessage::Relay {
                            sender_id,
                            timestamp,
                            target_id,
                            relay_id,
                            path,
                            inner,
                        } => {
                            if target_id == self.peer_id {
                                return Some(*inner);
                            }
                            // No inline prune: `recv_peer`'s top-of-loop
                            // `prune_relay_state` already swept both
                            // maps before we landed on this branch.
                            let blacklist = blacklist_for(
                                &self.failed_forwarders,
                                &target_id,
                            );
                            let decision = forward_step(
                                &self.connections,
                                &self.peer_id,
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
                            );
                            continue;
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
                            );
                            continue;
                        }
                        other => return Some(other),
                    }
                }
                accepted = self.new_conn_rx.recv() => {
                    if let Some(accepted) = accepted {
                        if !self.connections.contains_key(&accepted.peer_id) {
                            tracing::info!(peer = %accepted.peer_id, "incoming peer registered (during recv)");
                            self.connections.insert(accepted.peer_id, accepted.outgoing_tx);
                        }
                    }
                }
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.drain_new_connections();
        self.prune_relay_state();
        loop {
            let msg = self.incoming_rx.try_recv().ok()?;
            match msg {
                DistributedMessage::Relay {
                    target_id, inner, ..
                } if target_id == self.peer_id => return Some(*inner),
                DistributedMessage::Relay { target_id, .. } => {
                    // Forwarding requires the async path so we can
                    // await the per-peer mpsc send (and queue a
                    // RelayBackoff if we have no candidate). Drop
                    // here with a warn — `try_recv_peer` is a
                    // best-effort fast path that the secondary code
                    // doesn't currently use for relay forwarding.
                    tracing::warn!(
                        target = %target_id,
                        "try_recv_peer dropped relay: cannot forward synchronously, use recv_peer"
                    );
                    continue;
                }
                DistributedMessage::RelayBackoff { .. } => {
                    // Same reason: backoff handling needs the async
                    // path so we can re-send via mpsc.
                    continue;
                }
                other => return Some(other),
            }
        }
    }

    fn peer_count(&self) -> usize {
        self.connections.len()
    }

    async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        // Inherent method spawns per-peer dial tasks and returns
        // immediately; the trait stays async because other PeerTransport
        // impls (channel, no-op) keep their async signatures.
        PeerNetwork::connect_to_peers(self, peers);
    }
}

impl<I: Identifier> PeerNetwork<I> {
    /// Apply the forward-step decision: deliver direct, send a
    /// new Relay envelope (recording bookkeeping for backoff), or
    /// send a `RelayBackoff` to our predecessor on dead-end.
    fn apply_forward_decision(
        &mut self,
        decision: RouteDecision<I>,
        sender_id: String,
        relay_id: u64,
        target_id: String,
        path: Vec<String>,
    ) {
        match decision {
            RouteDecision::Direct(inner_unwrapped) => {
                let send_res = self
                    .connections
                    .get(&target_id)
                    .map(|tx| tx.send(inner_unwrapped));
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) => {
                        self.connections.remove(&target_id);
                        tracing::warn!(
                            target = %target_id,
                            "relay forward failed: target connection closed"
                        );
                    }
                    None => {
                        tracing::warn!(
                            target = %target_id,
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
                let send_res = self.connections.get(&via).map(|tx| tx.send(wrapped));
                match send_res {
                    Some(Ok(())) => {
                        self.outgoing_relays
                            .insert((sender_id.clone(), relay_id), bookkeeping);
                    }
                    Some(Err(_)) => {
                        self.connections.remove(&via);
                        tracing::warn!(
                            target = %target_id,
                            next = %via,
                            "relay forward failed: forwarder connection closed"
                        );
                    }
                    None => {
                        tracing::warn!(
                            target = %target_id,
                            next = %via,
                            "relay forward target unexpectedly missing"
                        );
                    }
                }
            }
            RouteDecision::NoRoute => {
                // Send RelayBackoff to predecessor (last entry in
                // path from our view).
                if let Some(predecessor) = path.last() {
                    let backoff = DistributedMessage::RelayBackoff {
                        sender_id: self.peer_id.clone(),
                        timestamp: timestamp_secs(),
                        original_sender: sender_id.clone(),
                        relay_id,
                    };
                    if let Some(tx) = self.connections.get(predecessor) {
                        if tx.send(backoff).is_err() {
                            tracing::warn!(
                                target = %target_id,
                                predecessor = %predecessor,
                                "relay backoff send failed: predecessor connection closed"
                            );
                        }
                    } else {
                        tracing::warn!(
                            target = %target_id,
                            predecessor = %predecessor,
                            "relay backoff send failed: predecessor not in connections"
                        );
                    }
                } else {
                    tracing::warn!(
                        target = %target_id,
                        "dropping relay: empty path on dead-end (no predecessor to back off to)"
                    );
                }
            }
        }
    }

    /// Handle an inbound `RelayBackoff` for `(original_sender,
    /// relay_id)`: look up our state, record the failed forwarder
    /// in the per-target blacklist for future relays, ask the
    /// routing helper what to do, and apply.
    fn handle_inbound_backoff(
        &mut self,
        original_sender: String,
        relay_id: u64,
        failed_via: String,
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
            .insert((state.target.clone(), failed_via.clone()), Instant::now());
        // No inline prune: `recv_peer`'s top-of-loop sweep already
        // ran before we entered the RelayBackoff branch that landed
        // us here, and the entry we just inserted carries `now()` so
        // it's the freshest in the map.
        let blacklist = blacklist_for(&self.failed_forwarders, &state.target);
        let now = timestamp_secs();
        let decision = handle_backoff(
            state,
            &self.connections,
            &self.peer_id,
            relay_id,
            &failed_via,
            now,
            &blacklist,
        );
        match decision {
            BackoffDecision::Retry { via, wrapped } => {
                let send_res = self.connections.get(&via).map(|tx| tx.send(wrapped));
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        self.connections.remove(&via);
                        tracing::warn!(
                            target = %state.target,
                            next = %via,
                            "relay retry send failed; further retries will fire on next backoff"
                        );
                    }
                }
            }
            BackoffDecision::PropagateBackoff { to, msg } => {
                let target_for_log = state.target.clone();
                let send_res = self.connections.get(&to).map(|tx| tx.send(msg));
                self.outgoing_relays.remove(&key);
                match send_res {
                    Some(Ok(())) => {}
                    Some(Err(_)) | None => {
                        tracing::warn!(
                            target = %target_for_log,
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
                    target = %target_for_log,
                    relay_id,
                    original_sender = %original_sender,
                    "relay dropped: all paths exhausted at originator"
                );
            }
        }
    }
}
