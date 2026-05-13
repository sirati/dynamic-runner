//! `PeerTransport` impl for `PeerNetwork`.
//!
//! Routing decisions live in
//! [`dynrunner_protocol_primary_secondary::Router`]; this file is a
//! thin adapter that supplies the QUIC connection map to Router on
//! every entry point and hands off Router's redial signal to
//! `PeerNetwork::spawn_redial`. The QUIC-specific bits — accept-loop
//! drain (`drain_new_connections`), the `tokio::select!` arm for
//! `new_conn_rx` in `recv_peer` — stay here because they are QUIC's
//! mechanics, not routing decisions.

use std::sync::Arc;
use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    install_role_change_hook, read_role_cache, Clocks, DistributedMessage, InboundOutcome,
    PeerConnectionInfo, PeerTransport, Role, RoleChangeHookRegistrar, SendOutcome,
};

use super::PeerNetwork;

fn timestamp_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Snapshot the `Clocks` pair Router consumes: monotonic `now` for
/// TTL/cooldown arithmetic, unix-epoch `wire` for outbound envelope
/// timestamps. Centralised so the four entry points stay in lockstep.
fn now_clocks() -> Clocks {
    Clocks {
        now: Instant::now(),
        wire: timestamp_secs(),
    }
}

impl<I: Identifier> PeerTransport<I> for PeerNetwork<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.drain_new_connections();
        // Memory hygiene: even when a node only broadcasts (no
        // `send_to_peer` / `recv_peer` traffic), routing state
        // accumulated by past relay activity needs sweeping.
        self.router.prune(Instant::now());
        let mut errors = Vec::new();
        for (peer_id, tx) in &self.connections {
            if tx.send(msg.clone()).is_err() {
                errors.push(peer_id.clone());
            }
        }
        for peer_id in &errors {
            self.connections.remove(peer_id);
            // Engage the reconnect tracker on detection so the
            // first 5s retry pulse already has the peer in its
            // tracking set. Idempotent on already-tracked peers
            // (returns false). On first observation, kick a
            // redial immediately rather than waiting for the
            // next periodic tick — the user-directed contract is
            // "reconnect immediately, then every 5 seconds".
            let first_observation =
                self.reconnect_tracker.observe_disconnect(peer_id);
            if first_observation {
                self.spawn_redial(peer_id);
            }
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        self.drain_new_connections();
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        let outcome = self
            .router
            .send_to_peer(peer_id, msg, &mut self.connections, clocks)
            .map_err(|e| e.to_string())?;
        match outcome {
            SendOutcome::Direct => Ok(()),
            SendOutcome::Relayed { redial_target, .. } => {
                if let Some(id) = redial_target {
                    self.spawn_redial(&id);
                }
                Ok(())
            }
            // Preserve the pre-Router `RouteDecision::NoRoute` Err
            // mapping so callers (e.g. secondary-side keepalive that
            // matches on `Err`) continue to observe a fatal "no route"
            // rather than a silent success.
            SendOutcome::NoRoute => Err(format!(
                "no route to peer '{peer_id}': direct unreachable and no forwarder available"
            )),
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.drain_new_connections();
        let mut clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            // The reconnect tick is conditionally polled: take()
            // the receiver out for the duration of the select so
            // tokio's borrow checker is happy, restore it on each
            // arm. Single-secondary test fixtures that construct
            // PeerNetwork without `start()` leave
            // `reconnect_tick_rx = None`; that branch resolves to
            // `pending::<Option<()>>().await` and never fires.
            let mut tick_rx = self.reconnect_tick_rx.take();
            tokio::select! {
                msg = self.incoming_rx.recv() => {
                    self.reconnect_tick_rx = tick_rx;
                    self.drain_new_connections();
                    clocks = now_clocks();
                    self.router.prune(clocks.now);
                    let msg = msg?;
                    match self.router.process_inbound(
                        msg,
                        &mut self.connections,
                        clocks,
                    ) {
                        InboundOutcome::Deliver { msg, redial_target } => {
                            if let Some(id) = redial_target {
                                self.spawn_redial(&id);
                            }
                            return Some(msg);
                        }
                        InboundOutcome::Handled { redial_target } => {
                            if let Some(id) = redial_target {
                                self.spawn_redial(&id);
                            }
                            continue;
                        }
                    }
                }
                accepted = self.new_conn_rx.recv() => {
                    self.reconnect_tick_rx = tick_rx;
                    if let Some(accepted) = accepted {
                        if !self.connections.contains_key(&accepted.peer_id) {
                            // Same observe_reconnect-before-register
                            // ordering as drain_new_connections so
                            // operator log shows resolution
                            // (with attempts+elapsed) immediately
                            // before "incoming peer registered".
                            self.reconnect_tracker
                                .observe_reconnect(&accepted.peer_id);
                            tracing::info!(
                                peer = %accepted.peer_id,
                                "incoming peer registered (during recv)"
                            );
                            self.connections
                                .insert(accepted.peer_id, accepted.outgoing_tx);
                        }
                    }
                }
                _ = async {
                    match tick_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending::<Option<()>>().await,
                    }
                } => {
                    self.reconnect_tick_rx = tick_rx;
                    // Periodic reconnect-tick. The tracker
                    // reconciles against the authoritative cluster
                    // list (peer_dial_info) so a peer that dropped
                    // out of `connections` via any path — not just
                    // the broadcast disconnect detector — gets
                    // picked up here. `spawn_redial` deduplicates
                    // against `connections` so duplicate dials on
                    // a freshly-restored peer are harmless.
                    self.process_reconnect_tick();
                }
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        self.drain_new_connections();
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            let msg = self.incoming_rx.try_recv().ok()?;
            match self.router.process_inbound_sync(msg, clocks) {
                InboundOutcome::Deliver { msg, redial_target } => {
                    if let Some(id) = redial_target {
                        self.spawn_redial(&id);
                    }
                    return Some(msg);
                }
                InboundOutcome::Handled { redial_target } => {
                    if let Some(id) = redial_target {
                        self.spawn_redial(&id);
                    }
                    continue;
                }
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

    fn register_with_cluster_state(&self, registrar: &mut dyn RoleChangeHookRegistrar) {
        // Same write-through-cache plumbing as the channel transport
        // — both delegate to the protocol-crate helper so the hook
        // body never drifts between transport kinds. The Arc clone
        // is what the hook captures; the transport's own handle
        // keeps the cache alive for as long as PeerNetwork lives.
        install_role_change_hook(Arc::clone(&self.role_cache), registrar);
    }

    fn peer_for_role(&self, role: &Role) -> Option<String> {
        read_role_cache(&self.role_cache, role)
    }

    fn local_id(&self) -> &str {
        // `PeerNetwork.peer_id` is already the local node's id —
        // surfaced through the trait so the protocol crate's `send`
        // default impl can populate `RoleAddressed.sender_id`
        // (Step 3) without the transport-specific call sites
        // needing to know about role envelopes.
        &self.peer_id
    }
}
