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

use std::time::Instant;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    Clocks, DistributedMessage, InboundOutcome, PeerConnectionInfo, PeerId, PeerTransport,
    SendOutcome,
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
        // Send-failure prune is now the BACKUP detector: the
        // AUTHORITATIVE one is the reader/writer-exit signal
        // (`disconnect_rx` arm below, driven by the QUIC IDLE_TIMEOUT
        // on a blackholed link). A failed `tx.send` still means the
        // writer task is gone, so we observe the disconnect — but
        // through the SAME generation-checked disposition
        // (`handle_peer_disconnect`) as the authoritative path, so the
        // prune cannot delete a freshly-reconnected entry whose channel
        // differs from the dead one. We capture the dead `tx` (not just
        // the id) precisely for that `same_channel` check.
        let mut dead: Vec<(
            String,
            tokio::sync::mpsc::UnboundedSender<DistributedMessage<I>>,
        )> = Vec::new();
        for (peer_id, tx) in &self.connections {
            if tx.send(msg.clone()).is_err() {
                dead.push((peer_id.clone(), tx.clone()));
            }
        }
        for (peer_id, dead_tx) in &dead {
            self.handle_peer_disconnect(peer_id, dead_tx);
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
        // One-shot gate on the reconnect-tick arm. Unlike the other five
        // arms — each of which keeps a network-held sender clone alive, so
        // `recv()` never resolves `None` while the network lives — the
        // tick arm's sole production sender is MOVED into the spawned 5 s
        // tick task (`peer/mod.rs`). If that task ever ends (panic, or the
        // runtime tearing it down), `reconnect_tick_rx` closes and a closed
        // `UnboundedReceiver::recv()` resolves `None` SYNCHRONOUSLY on every
        // poll, making this arm always-ready: the inner `loop` then re-polls
        // the `select!` without ever parking, burning ~100% CPU (the
        // lifetime-heat half of the livelock RCA — see
        // `tests/recv_tick_closed_spins.rs`). Flipping this bool on the first
        // `None` disables the arm (`, if !tick_closed`) for the rest of this
        // `recv_peer` call, mirroring the `transport_closed` one-shot gate the
        // operational loop uses for its own inbound arm. The reconnect cadence
        // is lost for the remainder of the call (the tick task is gone), but
        // redials still fire on the authoritative disconnect arm + Router's
        // own redial pulse — correctness is preserved, only the periodic
        // backstop is dropped, and the WARN names the regression.
        let mut tick_closed = false;
        loop {
            // All three select arms poll cancel-safe channel
            // receivers via disjoint-field borrows of `self`. The
            // futures are dropped before the winning arm's body
            // runs, so each body has unrestricted `&mut self`. No
            // take/restore of `reconnect_tick_rx` is needed (and
            // doing it was the pre-fix bug: an outer caller that
            // dropped the `recv_peer` future mid-poll destroyed
            // the taken-out receiver and silently disabled the
            // periodic reconnect tick — see the comment on
            // `PeerNetwork::reconnect_tick_rx`).
            let delivered_from_router = tokio::select! {
                msg = self.incoming_rx.recv() => {
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
                            // Unbox once at the router/role-layer boundary
                            // so the by-value role-layer signature stays
                            // unchanged.
                            Some(*msg)
                        }
                        InboundOutcome::Handled { redial_target } => {
                            if let Some(id) = redial_target {
                                self.spawn_redial(&id);
                            }
                            None
                        }
                    }
                }
                accepted = self.new_conn_rx.recv() => {
                    if let Some(accepted) = accepted
                        && !self.connections.contains_key(&accepted.peer_id)
                    {
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
                    None
                }
                disconnected = self.disconnect_rx.recv() => {
                    // AUTHORITATIVE disconnect: a per-connection
                    // reader/writer supervisor exited (peer gone, or
                    // the QUIC IDLE_TIMEOUT fired because keep-alive
                    // PINGs stopped being acked on a blackholed link).
                    // Run the SAME generation-checked prune+redial
                    // disposition the broadcast send-failure fallback
                    // uses, so a stale signal for a torn-down
                    // connection cannot delete a freshly-reconnected
                    // entry. `recv()` yields `None` only if every
                    // `disconnect_tx` clone dropped — impossible while
                    // the network lives (it holds one in
                    // `self.disconnect_tx`), so this never spuriously
                    // closes.
                    if let Some(d) = disconnected {
                        self.handle_peer_disconnect(&d.peer_id, &d.outgoing_tx);
                    }
                    None
                }
                tick = self.reconnect_tick_rx.recv(), if !tick_closed => {
                    match tick {
                        Some(()) => {
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
                        None => {
                            // The 5 s tick task ended, closing the channel.
                            // A closed receiver resolves `None` forever and
                            // synchronously, so leaving the arm enabled would
                            // hot-loop the `select!` at ~100% CPU. Gate it off
                            // for the rest of this call (see the `tick_closed`
                            // declaration above). One WARN names the
                            // regression so the lost reconnect backstop is
                            // visible in the operator log.
                            tick_closed = true;
                            tracing::warn!(
                                "reconnect-tick channel closed (the 5s tick task ended); \
                                 disabling the tick arm for the remainder of this recv_peer \
                                 to avoid a busy-spin — periodic redials are now driven only \
                                 by the disconnect arm + Router's redial pulse"
                            );
                        }
                    }
                    None
                }
                redialed = self.bootstrap_redial_rx.recv() => {
                    // Defect (b): a dropped submitter-bootstrap wire was
                    // re-dialed off-loop (the `-R` tunnel came back); re-
                    // fold the fresh client into the mesh under `&mut self`
                    // via the SAME `register_primary_link` install path —
                    // fan-in `mesh_writer` + inbound forwarder. This re-
                    // arms the redial for the NEXT drop, so the link stays
                    // restorable for the life of the run. Restores ONLY the
                    // transport pipe: no failover input is touched (the
                    // secondary→primary app-layer liveness window is
                    // unchanged; see `bootstrap_redial`).
                    //
                    // `recv()` never yields `None` while the network lives
                    // (the held `bootstrap_redial_tx` clone keeps the
                    // channel open), so this only fires on a real re-dial.
                    if let Some(r) = redialed {
                        self.fold_primary_link(r.target, r.client);
                    }
                    None
                }
                queued = self.proxy_rx.recv() => {
                    // Mesh-send proxy drain (see `mesh_send.rs` +
                    // `MeshSendHandle`). A promoted-host primary's
                    // send-proxy queued a remote send here; forward it
                    // through THIS network's own relay-aware send path
                    // so the router's relay / blacklist / redial logic
                    // applies uniformly. The forwarding lives entirely
                    // in the transport — no manager-visible mesh drain.
                    //
                    // `recv()` never yields `None` while the network
                    // lives: `self.mesh_send_tx` keeps a sender clone,
                    // so the channel cannot close from under us even if
                    // every handed-out `MeshSendHandle` is dropped.
                    // Errors from the inner send are best-effort
                    // (logged, not surfaced) — the same contract the
                    // mesh's own callers get; a dead peer is the router's
                    // concern, not the queued sender's.
                    if let Some(item) = queued {
                        match item {
                            super::MeshSend::ToPeer(peer_id, msg) => {
                                if let Err(e) = self.send_to_peer(&peer_id, msg).await {
                                    tracing::debug!(
                                        peer = %peer_id,
                                        error = %e,
                                        "mesh-send proxy: forwarded unicast had no route"
                                    );
                                }
                            }
                            super::MeshSend::Broadcast(msg) => {
                                if let Err(e) = self.broadcast(msg).await {
                                    tracing::debug!(
                                        error = %e,
                                        "mesh-send proxy: forwarded broadcast failed"
                                    );
                                }
                            }
                        }
                    }
                    None
                }
            };
            if let Some(msg) = delivered_from_router {
                return Some(msg);
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
                    return Some(*msg);
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
        // Pure membership cardinality: the count of connections in the
        // mesh. The transport does NOT know which connection is the
        // primary, so it cannot (and must not) special-case it — any
        // "exclude the primary" mesh-health/quorum policy is a role
        // concern resolved at the coordinator edge (the secondary's
        // election quorum reads `live_peer_ids`, which excludes the
        // current primary at the edge). Symmetric with `broadcast`,
        // which now fans out to every connection uniformly.
        self.connections.len()
    }

    fn has_peer(&self, id: &PeerId) -> bool {
        // Real per-id membership: a peer is a member iff it has a live
        // connection entry in the QUIC connection table. The bootstrap-
        // primary link folded in via `register_primary_link` IS such an
        // entry, so `has_peer(primary)` is `true` once registered — the
        // primary is reachable as a plain mesh peer from the secondary's
        // side, which is the correct membership answer. Drained-but-not-
        // yet-registered accept-loop connections are not counted here —
        // the table is the single source of truth for "reachable right
        // now".
        self.connections.contains_key(id.as_str())
    }

    fn connected_ids(&self) -> Vec<PeerId> {
        // Live enumeration off the same `connections` table that backs
        // `peer_count`/`has_peer` — the single source of truth. Role-blind:
        // the folded bootstrap primary appears as an ordinary id.
        self.connections
            .keys()
            .map(|k| PeerId::from(k.as_str()))
            .collect()
    }

    async fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        // Inherent method spawns per-peer dial tasks and returns
        // immediately; the trait stays async because other PeerTransport
        // impls (channel, no-op) keep their async signatures.
        PeerNetwork::connect_to_peers(self, peers);
    }
}
