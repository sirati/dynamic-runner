//! `PeerTransport` impl for `PeerNetwork`. The inherent methods stay in
//! `mod.rs` so this file is purely the trait-glue layer.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    observe_transition, route_send, DistributedMessage, PeerConnectionInfo, PeerTransport,
    RouteDecision,
};

use super::PeerNetwork;

fn timestamp_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

impl<I: Identifier> PeerTransport<I> for PeerNetwork<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.drain_new_connections();
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
        let now = timestamp_secs();
        let decision = route_send(&self.connections, &self.peer_id, peer_id, msg, now);
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
            RouteDecision::Relay { via, wrapped } => {
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
        loop {
            tokio::select! {
                msg = self.incoming_rx.recv() => {
                    self.drain_new_connections();
                    let msg = msg?;
                    if let DistributedMessage::Relay {
                        sender_id,
                        timestamp,
                        target_id,
                        path,
                        inner,
                    } = msg
                    {
                        if target_id == self.peer_id {
                            return Some(*inner);
                        }
                        // Forward via path-aware exclusion. Drop with
                        // a warn if no candidate exists — backtracking
                        // ("ask previous to choose another") is a
                        // separate stateful protocol left for follow-up.
                        match dynrunner_protocol_primary_secondary::relay::forward_step(
                            &self.connections,
                            &self.peer_id,
                            &target_id,
                            &path,
                            timestamp,
                            &sender_id,
                            inner,
                        ) {
                            Some((next, forwarded)) => {
                                let send_res = self
                                    .connections
                                    .get(&next)
                                    .map(|tx| tx.send(forwarded));
                                match send_res {
                                    Some(Ok(())) => {}
                                    Some(Err(_)) => {
                                        self.connections.remove(&next);
                                        tracing::warn!(
                                            target = %target_id,
                                            next = %next,
                                            "relay forward failed: forwarder connection closed"
                                        );
                                    }
                                    None => {
                                        tracing::warn!(
                                            target = %target_id,
                                            next = %next,
                                            "relay forward target unexpectedly missing"
                                        );
                                    }
                                }
                            }
                            None => {
                                tracing::warn!(
                                    target = %target_id,
                                    path = ?path,
                                    "dropping relay: no forwarder candidate outside path"
                                );
                            }
                        }
                        continue;
                    }
                    return Some(msg);
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
        let msg = self.incoming_rx.try_recv().ok()?;
        // Synchronous unwrap for relay-targeting-self only; deferred
        // forwarding requires the full async path in `recv_peer`. A
        // misrouted relay arriving via try_recv falls through unwrapped
        // to the caller, who will see an unexpected `Relay` variant —
        // the swallow-with-warn below mirrors the same dead-end
        // handling on a synchronous best-effort path.
        if let DistributedMessage::Relay {
            target_id, inner, ..
        } = msg
        {
            if target_id == self.peer_id {
                return Some(*inner);
            }
            tracing::warn!(
                target = %target_id,
                "try_recv_peer dropped relay: cannot forward synchronously, use recv_peer"
            );
            return None;
        }
        Some(msg)
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
