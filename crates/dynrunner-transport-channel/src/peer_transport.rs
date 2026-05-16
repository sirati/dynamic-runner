//! Channel-based [`PeerTransport`] implementation.
//!
//! Owns a per-peer inbox + outbox table and delegates all routing
//! decisions to [`Router`]. Tests simulate partitions by mutating
//! `outgoing` through `disconnect_from` / `connect_to`.

use std::collections::HashMap;
use std::sync::Arc;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{
    apply_role_misaddress_hint, decide_role_addressed_with_cache, install_role_change_hook,
    read_role_cache, Clocks, DistributedMessage, InboundOutcome, PeerConnectionInfo, PeerTransport,
    Role, RoleAddressedAction, RoleCache, RoleChangeHookRegistrar, Router, SendOutcome,
};
use tokio::sync::mpsc;

use crate::now_clocks;

/// Channel-based [`PeerTransport`]. Each instance owns one inbox
/// (mpsc receiver), a dictionary of outboxes (mpsc senders, one per
/// directly-reachable peer), and a [`Router`] that owns ALL routing
/// decisions — direct vs relay, blacklist, redial-cooldown gate.
/// Adjacency is set up by [`peer_mesh`] (all-to-all) or
/// [`peer_mesh_with_adjacency`] (caller-supplied undirected links);
/// tests simulate partitions by mutating `outgoing` through
/// [`ChannelPeerTransport::disconnect_from`] /
/// [`ChannelPeerTransport::connect_to`].
///
/// `last_outcome` exposes the most recent `Router::send_to_peer`
/// outcome for test assertions (the channel transport doesn't dial,
/// so the redial signal is observable here rather than producing
/// background work). It is `pub` deliberately — tests assert against
/// it directly instead of bypassing the [`PeerTransport`] trait, per
/// the "abstractions the test path circumvents are wrong" rule.
pub struct ChannelPeerTransport<I: Identifier> {
    /// Local peer-id. Surfaced via
    /// [`PeerTransport::local_id`] so the trait's `send` default
    /// impl can populate the `sender_id` field of the
    /// `RoleAddressed` envelope it constructs for
    /// `Address::Role(_)` dispatches (Step 3). The Router also
    /// holds this id (for relay-path bookkeeping); duplicating it
    /// at the transport level is cheap (`String`, populated once at
    /// mesh construction) and saves a layer of indirection on the
    /// `local_id` hot path.
    pub(crate) local_id: String,
    pub(crate) incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    pub(crate) outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    /// Peer-mesh routing dispatcher. Owns ALL routing state (in-flight
    /// relays, blacklist, per-peer route observation, monotonic
    /// relay-id counter). The transport itself never inspects routing
    /// state.
    pub router: Router<I>,
    /// Most recent `Router::send_to_peer` outcome — exposed for test
    /// assertions per `M5` of the relay-routing plan. Not part of the
    /// stable public API; production callers should ignore it.
    pub last_outcome: Option<SendOutcome>,
    /// Write-through cache of `Role → peer_id`. Populated by the
    /// hook registered via
    /// [`PeerTransport::register_with_cluster_state`]; read by
    /// [`PeerTransport::peer_for_role`] on the send hot path. Empty
    /// until registration runs — transports that never register
    /// observe `None` for every lookup.
    pub(crate) role_cache: RoleCache,
}

impl<I: Identifier + Clone> PeerTransport<I> for ChannelPeerTransport<I> {
    async fn broadcast(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        for tx in self.outgoing.values() {
            // Closed senders are tolerated — the peer simply went away.
            let _ = tx.send(msg.clone());
        }
        Ok(())
    }

    async fn send_to_peer(
        &mut self,
        peer_id: &str,
        msg: DistributedMessage<I>,
    ) -> Result<(), String> {
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        let outcome = self
            .router
            .send_to_peer(peer_id, msg, &mut self.outgoing, clocks)
            .map_err(|e| e.to_string())?;
        self.last_outcome = Some(outcome.clone());
        // Channel transport does not dial — partition heal in tests is
        // manual via `connect_to`. The redial signal carried inside
        // `SendOutcome::Relayed` is observable through `last_outcome`
        // for assertions; nothing else acts on it here.
        match outcome {
            SendOutcome::NoRoute => Err(format!(
                "no route to peer '{peer_id}': direct unreachable and no forwarder available"
            )),
            SendOutcome::Direct | SendOutcome::Relayed { .. } => Ok(()),
        }
    }

    async fn recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        let mut clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            let msg = self.incoming_rx.recv().await?;
            clocks = now_clocks();
            self.router.prune(clocks.now);
            let delivered = match self
                .router
                .process_inbound(msg, &mut self.outgoing, clocks)
            {
                // `msg` is `Box<DistributedMessage<I>>`; unbox to feed
                // the by-value role-layer entry point.
                InboundOutcome::Deliver { msg, .. } => *msg,
                InboundOutcome::Handled { .. } => continue,
            };
            match self.handle_role_layer(delivered, clocks) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn try_recv_peer(&mut self) -> Option<DistributedMessage<I>> {
        let clocks = now_clocks();
        self.router.prune(clocks.now);
        loop {
            let msg = self.incoming_rx.try_recv().ok()?;
            let delivered = match self.router.process_inbound_sync(msg, clocks) {
                InboundOutcome::Deliver { msg, .. } => *msg,
                InboundOutcome::Handled { .. } => continue,
            };
            match self.handle_role_layer(delivered, clocks) {
                Some(payload) => return Some(payload),
                None => continue,
            }
        }
    }

    fn peer_count(&self) -> usize {
        self.outgoing.len()
    }

    async fn connect_to_peers(&mut self, _peers: &[PeerConnectionInfo]) {
        // No-op: peers are pre-wired via `peer_mesh` /
        // `peer_mesh_with_adjacency`. Test drivers simulate partition
        // heal via `connect_to` directly.
    }

    fn register_with_cluster_state(&self, registrar: &mut dyn RoleChangeHookRegistrar) {
        // Hand the shared cache handle to the protocol-crate helper
        // that installs the hook on the registrar. The hook captures
        // a clone of the `Arc<RwLock<_>>` and writes through to the
        // same map `peer_for_role` reads. See
        // `install_role_change_hook` for the lock-poisoning recovery
        // rationale.
        install_role_change_hook(Arc::clone(&self.role_cache), registrar);
    }

    fn peer_for_role(&self, role: &Role) -> Option<String> {
        read_role_cache(&self.role_cache, role)
    }

    fn local_id(&self) -> &str {
        &self.local_id
    }
}

impl<I: Identifier> ChannelPeerTransport<I> {
    /// Role-layer interceptor: handle [`DistributedMessage::RoleAddressed`]
    /// (Case A unwrap / Case B relay-and-hint / Cases C, D drop) and
    /// [`DistributedMessage::RoleMisaddressHint`] (silent cache-warm).
    ///
    /// Returns `Some(payload)` when the caller should yield a message
    /// to the application layer (Case A or "not a role-layer
    /// envelope"); returns `None` when the envelope was consumed
    /// internally (Cases B/C/D, or hint absorbed) — caller loops.
    ///
    /// The relay-and-hint sends bypass `PeerTransport::send_to_peer`
    /// and go straight to `self.router.send_to_peer`: the trait method
    /// would update `last_outcome` and surface NoRoute as a hard
    /// error, neither of which is appropriate for transport-internal
    /// bookkeeping sends. NoRoute on a Case-B relay falls back to a
    /// warn-and-drop — the originator's `attempts` counter and the
    /// next periodic role-table refresh together bound the failure
    /// horizon.
    fn handle_role_layer(
        &mut self,
        msg: DistributedMessage<I>,
        clocks: Clocks,
    ) -> Option<DistributedMessage<I>> {
        match msg {
            DistributedMessage::RoleAddressed {
                sender_id,
                intended_role,
                payload,
                attempts,
                ..
            } => {
                let decision = decide_role_addressed_with_cache(
                    &self.local_id,
                    &self.role_cache,
                    sender_id,
                    intended_role,
                    payload,
                    attempts,
                );
                match decision {
                    RoleAddressedAction::Unwrap(inner) => Some(inner),
                    RoleAddressedAction::Relay {
                        forward_to,
                        forwarded,
                        hint_to,
                        hint,
                    } => {
                        // Both sends are fire-and-forget at the
                        // role-routing layer. Router::send_to_peer
                        // emits its own warn on NoRoute / dispatch
                        // failure; we add a target-tagged warn to
                        // make role-relay failures distinguishable
                        // from raw direct-send failures in operator
                        // logs.
                        if let Err(e) = self.router.send_to_peer(
                            &forward_to,
                            forwarded,
                            &mut self.outgoing,
                            clocks,
                        ) {
                            tracing::warn!(
                                forward_to = %forward_to,
                                error = %e,
                                "RoleAddressed relay forward failed",
                            );
                        }
                        if let Err(e) = self.router.send_to_peer(
                            &hint_to,
                            // Unbox once at the dispatch boundary so the
                            // by-value send_to_peer signature stays
                            // unchanged.
                            *hint,
                            &mut self.outgoing,
                            clocks,
                        ) {
                            tracing::warn!(
                                hint_to = %hint_to,
                                error = %e,
                                "RoleMisaddressHint send failed",
                            );
                        }
                        None
                    }
                    RoleAddressedAction::Drop { reason } => {
                        tracing::warn!(reason, "RoleAddressed dropped");
                        None
                    }
                }
            }
            DistributedMessage::RoleMisaddressHint {
                role, holder_id, ..
            } => {
                // Cache-warming only — never surfaced to the
                // application layer (per Step 4 design rationale:
                // senders that issued an Address::Role(_) send are
                // not awaiting a hint reply).
                apply_role_misaddress_hint(&self.role_cache, role, holder_id);
                None
            }
            other => Some(other),
        }
    }

    /// Remove a peer's outbox so a subsequent send finds no direct
    /// channel — the Router will then route via relay (or no-route)
    /// just as if the underlying network link had broken. Used by
    /// partition tests; idempotent on already-disconnected peers.
    pub fn disconnect_from(&mut self, peer_id: &str) {
        self.outgoing.remove(peer_id);
    }

    /// Re-add a peer's outbox so a subsequent send can again take the
    /// direct path — used by partition-heal tests. Overwrites any
    /// existing entry.
    pub fn connect_to(
        &mut self,
        peer_id: String,
        sender: mpsc::UnboundedSender<DistributedMessage<I>>,
    ) {
        self.outgoing.insert(peer_id, sender);
    }
}
