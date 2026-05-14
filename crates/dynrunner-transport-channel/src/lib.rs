use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{
    apply_role_misaddress_hint, decide_role_addressed_with_cache, install_role_change_hook,
    new_role_cache, read_role_cache, seed_self_role, Clocks, DistributedMessage, InboundOutcome,
    PeerConnectionInfo, PeerTransport, Role, RoleAddressedAction, RoleCache,
    RoleChangeHookRegistrar, Router, SecondaryTransport, SendOutcome,
};
use dynrunner_protocol_manager_worker::{Command, Response};
use tokio::sync::mpsc;

/// Unix-epoch wall-clock seconds for the wire-side `Clocks::wire`
/// envelope timestamp. Local-clock TTL/cooldown decisions inside Router
/// use the monotonic `Instant::now()` carried alongside it.
fn timestamp_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Snapshot the `Clocks` pair Router consumes — kept centralised so
/// every entry point stays in lockstep with the QUIC transport's
/// equivalent helper (cf. `transport-quic/src/peer/transport_impl.rs`).
fn now_clocks() -> Clocks {
    Clocks {
        now: Instant::now(),
        wire: timestamp_secs(),
    }
}

// ── Manager ↔ Runner channel transport ──

/// Manager-side endpoint backed by tokio mpsc channels.
pub struct ChannelManagerEnd {
    cmd_tx: mpsc::UnboundedSender<Command>,
    resp_rx: mpsc::UnboundedReceiver<Response>,
}

/// Runner-side endpoint backed by tokio mpsc channels.
pub struct ChannelRunnerEnd {
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    resp_tx: mpsc::UnboundedSender<Response>,
}

/// Create a pair of channel endpoints for in-process testing.
pub fn channel_pair() -> (ChannelManagerEnd, ChannelRunnerEnd) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (resp_tx, resp_rx) = mpsc::unbounded_channel();
    (
        ChannelManagerEnd { cmd_tx, resp_rx },
        ChannelRunnerEnd { cmd_rx, resp_tx },
    )
}

impl MessageSender<Command> for ChannelManagerEnd {
    async fn send(&mut self, msg: Command) -> Result<(), String> {
        self.cmd_tx.send(msg).map_err(|e| e.to_string())
    }
}

impl MessageReceiver<Response> for ChannelManagerEnd {
    async fn recv(&mut self) -> Option<Response> {
        self.resp_rx.recv().await
    }

}

impl MessageReceiver<Command> for ChannelRunnerEnd {
    async fn recv(&mut self) -> Option<Command> {
        self.cmd_rx.recv().await
    }
}

impl MessageSender<Response> for ChannelRunnerEnd {
    async fn send(&mut self, msg: Response) -> Result<(), String> {
        self.resp_tx.send(msg).map_err(|e| e.to_string())
    }
}

// ── Primary ↔ Secondary channel transport ──

/// Channel-based transport for the primary side of distributed coordination.
///
/// Holds per-secondary outgoing senders and a single incoming receiver
/// that aggregates messages from all secondaries.
pub struct ChannelSecondaryTransportEnd<I: Identifier> {
    pub outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    pub incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelSecondaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.incoming_rx.recv().await
    }
}

impl<I: Identifier> SecondaryTransport<I> for ChannelSecondaryTransportEnd<I> {
    async fn send_to(&mut self, secondary_id: &str, msg: DistributedMessage<I>) -> Result<(), String> {
        if let Some(tx) = self.outgoing.get(secondary_id) {
            tx.send(msg).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn broadcast(
        &mut self,
        msg: DistributedMessage<I>,
    ) -> Result<(), Vec<(String, String)>> {
        let mut errors = Vec::new();
        for (secondary_id, tx) in &self.outgoing {
            if let Err(e) = tx.send(msg.clone()) {
                errors.push((secondary_id.clone(), e.to_string()));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Channel-based transport for the secondary side of distributed coordination.
///
/// Sends to the primary and receives from it via unbounded mpsc channels.
pub struct ChannelPrimaryTransportEnd<I: Identifier> {
    pub tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    pub rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        self.tx.send(msg).map_err(|e| e.to_string())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for ChannelPrimaryTransportEnd<I> {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        self.rx.recv().await
    }
}

// ── Peer-to-peer channel transport (for multi-secondary in-process tests) ──

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
    local_id: String,
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    outgoing: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
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
    role_cache: RoleCache,
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
                InboundOutcome::Deliver { msg, .. } => msg,
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
                InboundOutcome::Deliver { msg, .. } => msg,
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
                            hint,
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

/// Build the full set of unordered pairs `(a, b)` where `a < b` in
/// input order, given a peer-id list. The shared subroutine behind
/// the all-to-all [`peer_mesh`] adjacency.
fn all_undirected_pairs(ids: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(ids.len() * ids.len().saturating_sub(1) / 2);
    for (i, a) in ids.iter().enumerate() {
        for b in &ids[i + 1..] {
            out.push((a.clone(), b.clone()));
        }
    }
    out
}

/// Wire up an all-to-all peer mesh for the given ids and return one
/// [`ChannelPeerTransport`] per id, in input order. Each transport's
/// outbox map contains every other peer's inbox sender.
///
/// Delegates to [`peer_mesh_with_adjacency`] with the full set of
/// unordered pairs so a single adjacency wiring path serves both
/// fully-connected and partial-mesh test fixtures.
pub fn peer_mesh<I: Identifier>(peer_ids: &[String]) -> Vec<ChannelPeerTransport<I>> {
    let links = all_undirected_pairs(peer_ids);
    peer_mesh_with_adjacency(peer_ids, &links)
}

/// Wire up a partial peer mesh defined by an explicit adjacency list
/// and return one [`ChannelPeerTransport`] per id, in `peer_ids`
/// input order. Each `(a, b)` link is **undirected**: a's outbox gains
/// b's sender AND b's outbox gains a's sender.
///
/// # Panics
///
/// - Panics if a link references an id not present in `peer_ids` —
///   silently dropping it would half-wire a partition the test
///   thought was symmetric.
/// - Panics if the same unordered pair appears more than once
///   (including the directed-style `(a, b) + (b, a)` form). A
///   misconfigured fixture that accidentally lists a link twice would
///   half-wire a partition test exactly the way a real bug would, so
///   we refuse to construct the mesh rather than mask the typo.
pub fn peer_mesh_with_adjacency<I: Identifier>(
    peer_ids: &[String],
    links: &[(String, String)],
) -> Vec<ChannelPeerTransport<I>> {
    // Allocate inbox + outbox-sender for each peer.
    let mut inboxes: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>> = HashMap::new();
    let mut receivers: HashMap<String, mpsc::UnboundedReceiver<DistributedMessage<I>>> =
        HashMap::new();
    for id in peer_ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inboxes.insert(id.clone(), tx);
        receivers.insert(id.clone(), rx);
    }

    // Build the per-peer outgoing tables from the undirected adjacency
    // list. We key by the canonical (lo, hi) ordering so a duplicate
    // — whether listed twice in the same direction or once each
    // direction — surfaces as a panic rather than silent re-insert.
    let mut outgoing: HashMap<String, HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>> =
        peer_ids
            .iter()
            .map(|id| (id.clone(), HashMap::new()))
            .collect();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for (a, b) in links {
        assert!(a != b, "peer_mesh_with_adjacency: self-link '{a}' is not allowed");
        assert!(
            inboxes.contains_key(a),
            "peer_mesh_with_adjacency: link references unknown peer '{a}'"
        );
        assert!(
            inboxes.contains_key(b),
            "peer_mesh_with_adjacency: link references unknown peer '{b}'"
        );
        let canonical = if a <= b {
            (a.clone(), b.clone())
        } else {
            (b.clone(), a.clone())
        };
        assert!(
            seen.insert(canonical.clone()),
            "peer_mesh_with_adjacency: duplicate link {canonical:?} (adjacency is undirected; \
             list each unordered pair at most once)"
        );
        outgoing
            .get_mut(a)
            .expect("peer table allocated above")
            .insert(b.clone(), inboxes[b].clone());
        outgoing
            .get_mut(b)
            .expect("peer table allocated above")
            .insert(a.clone(), inboxes[a].clone());
    }

    let mut transports = Vec::with_capacity(peer_ids.len());
    for id in peer_ids {
        let incoming_rx = receivers
            .remove(id)
            .expect("inbox was inserted above for every id");
        let outgoing_for_peer = outgoing
            .remove(id)
            .expect("outgoing table allocated for every id");
        let role_cache = new_role_cache();
        // Self_ is a strictly local fact — the role-table hook
        // populates Primary only. Seeding at construction so the
        // receiver-side RoleAddressed handling (Step 4) treats
        // `intended_role == Self_` envelopes as Case A (local
        // unwrap) rather than Case C (no cached holder → drop).
        seed_self_role(&role_cache, id);
        transports.push(ChannelPeerTransport {
            local_id: id.clone(),
            incoming_rx,
            outgoing: outgoing_for_peer,
            router: Router::new(id.clone()),
            last_outcome: None,
            role_cache,
        });
    }
    transports
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        manager
            .send(Command::ProcessTask { relative_path: "test/bin".into(), payload: None, resolved_path: None, })
            .await
            .unwrap();

        let cmd = runner.recv().await.unwrap();
        match cmd {
            Command::ProcessTask { relative_path, .. } => {
                assert_eq!(relative_path, "test/bin");
            }
            _ => panic!("expected ProcessTask"),
        }
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let (mut manager, mut runner) = channel_pair();

        runner
            .send(Response::Done {
                result_data: Some(b"2:5".to_vec()),
            })
            .await
            .unwrap();

        let resp = manager.recv().await.unwrap();
        match resp {
            Response::Done { result_data } => {
                assert_eq!(result_data.unwrap(), b"2:5");
            }
            _ => panic!("expected Done"),
        }
    }

    #[tokio::test]
    async fn stop_command() {
        let (mut manager, mut runner) = channel_pair();

        manager.send(Command::Stop).await.unwrap();

        let cmd = runner.recv().await.unwrap();
        assert!(matches!(cmd, Command::Stop));
    }

    #[tokio::test]
    async fn multiple_responses() {
        let (mut manager, mut runner) = channel_pair();

        runner.send(Response::Ready).await.unwrap();
        runner
            .send(Response::PhaseUpdate {
                phase_name: "ANGR_1".into(),
            })
            .await
            .unwrap();
        runner.send(Response::Keepalive).await.unwrap();

        let r1 = manager.recv().await.unwrap();
        assert!(matches!(r1, Response::Ready));
        let r2 = manager.recv().await.unwrap();
        assert!(matches!(r2, Response::PhaseUpdate { .. }));
        let r3 = manager.recv().await.unwrap();
        assert!(matches!(r3, Response::Keepalive));
    }

    #[tokio::test]
    async fn runner_disconnect_returns_none() {
        let (manager, mut runner) = channel_pair();

        // Drop the manager end
        drop(manager);

        // Runner should get a send error
        let result = runner.send(Response::Ready).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn manager_disconnect_returns_none() {
        let (mut manager, runner) = channel_pair();

        // Drop the runner end
        drop(runner);

        // Manager recv should return None (disconnected)
        let resp = manager.recv().await;
        assert!(resp.is_none());
    }

    /// `peer_mesh` wires N transports with all-to-all senders. A broadcast
    /// from one peer should reach every other peer's inbox; nothing should
    /// loop back to the sender.
    #[tokio::test]
    async fn peer_mesh_broadcasts_to_all_others() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        struct TestId(String);

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<TestId>(&ids);

        assert_eq!(transports.len(), 3);
        assert_eq!(transports[0].peer_count(), 2);
        assert_eq!(transports[1].peer_count(), 2);
        assert_eq!(transports[2].peer_count(), 2);

        let msg = DistributedMessage::Keepalive {
            sender_id: "a".into(),
            timestamp: 1.0,
            secondary_id: "a".into(),
            active_workers: 0,
        };
        transports[0].broadcast(msg).await.unwrap();

        // a does not receive its own broadcast
        assert!(transports[0].try_recv_peer().is_none());
        // b and c do
        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_some());
    }

    /// `send_to_peer` reaches exactly one inbox.
    #[tokio::test]
    async fn peer_mesh_send_to_specific_peer() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        struct TestId(String);

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<TestId>(&ids);

        let msg = DistributedMessage::Keepalive {
            sender_id: "a".into(),
            timestamp: 1.0,
            secondary_id: "a".into(),
            active_workers: 0,
        };
        transports[0].send_to_peer("b", msg).await.unwrap();

        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_none());
        assert!(transports[0].try_recv_peer().is_none());
    }

    // ── PeerTransport::send default-impl contract tests ──
    //
    // These pin the Step 1 default impl so Step 3 (which will replace
    // the Role/AllSecondaries error arms with real dispatch) has a
    // regression net. Each test exercises exactly one Address variant
    // through the trait's default body — the channel transport itself
    // does not override `send`, so what we observe here is the protocol-
    // crate default routing through `send_to_peer` / `broadcast`.

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct SendTestId(String);

    fn keepalive(sender: &str) -> DistributedMessage<SendTestId> {
        DistributedMessage::Keepalive {
            sender_id: sender.into(),
            timestamp: 1.0,
            secondary_id: sender.into(),
            active_workers: 0,
        }
    }

    /// `send(Address::Peer(id), msg)` routes through the default impl
    /// to `send_to_peer` and reaches exactly that peer.
    #[tokio::test]
    async fn send_address_peer_reaches_recipient() {
        use dynrunner_protocol_primary_secondary::Address;

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        transports[0]
            .send(Address::Peer("b".to_string()), keepalive("a"))
            .await
            .unwrap();

        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_none());
        assert!(transports[0].try_recv_peer().is_none());
    }

    /// `send(Address::Broadcast(Scope::Mesh), msg)` routes through the
    /// default impl to `broadcast` and fans out to every other peer.
    #[tokio::test]
    async fn send_address_broadcast_mesh_fans_out() {
        use dynrunner_protocol_primary_secondary::{Address, Scope};

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        transports[0]
            .send(Address::Broadcast(Scope::Mesh), keepalive("a"))
            .await
            .unwrap();

        assert!(transports[0].try_recv_peer().is_none());
        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_some());
    }

    /// Post-Step 3: `send(Address::Role(Role::Primary), msg)`
    /// against a cold role-table cache returns an `Err` that names
    /// "Role" and the missing cache state. Pins the cache-cold
    /// contract for `Role::Primary`; `Role::Self_` has its own
    /// cache-seeded behavior (Step 4 — see
    /// [`role_self_cache_populated_at_init`]) and is covered there.
    /// Pre-Step-3 this test asserted "not yet supported"; the
    /// assertion shifted to the new contract when the real
    /// dispatch landed.
    #[tokio::test]
    async fn send_address_role_returns_err() {
        use dynrunner_protocol_primary_secondary::{Address, Role};

        let ids = vec!["a".to_string(), "b".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        let err = transports[0]
            .send(Address::Role(Role::Primary), keepalive("a"))
            .await
            .expect_err("Role(Primary) with cold cache must error");
        assert!(
            err.contains("Role"),
            "error must reference Role; got: {err}"
        );
        assert!(
            err.contains("cache"),
            "error must reference cache state; got: {err}"
        );

        // No message must have been delivered to any peer's inbox.
        assert!(transports[0].try_recv_peer().is_none());
        assert!(transports[1].try_recv_peer().is_none());
    }

    /// Post-Step 5: `send(Address::Broadcast(Scope::AllSecondaries), msg)`
    /// fans out via the default impl's `broadcast` delegation. From a
    /// primary caller's vantage (the only Step-5 caller), every
    /// peer-mesh member is by definition a secondary, so `AllSecondaries`
    /// and `Mesh` produce the same wire effect today; the Scope variant
    /// is preserved for the future case of a secondary broadcasting
    /// only-to-non-primary peers (which would override the default
    /// impl with a per-impl `outgoing.iter().filter(|id| id !=
    /// primary_holder)` walk).
    #[tokio::test]
    async fn send_address_broadcast_all_secondaries_fans_out() {
        use dynrunner_protocol_primary_secondary::{Address, Scope};

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        transports[0]
            .send(Address::Broadcast(Scope::AllSecondaries), keepalive("a"))
            .await
            .unwrap();

        // Same delivery pattern as `Scope::Mesh`: peer 0 keeps nothing,
        // peers 1 and 2 both received.
        assert!(transports[0].try_recv_peer().is_none());
        assert!(transports[1].try_recv_peer().is_some());
        assert!(transports[2].try_recv_peer().is_some());
    }

    // ── Step 2: role-table write-through cache tests ──
    //
    // These exercise the channel transport's `register_with_cluster_state`
    // + `peer_for_role` pair through a minimal in-test registrar (the
    // trait owner — `ClusterState` — lives in a downstream crate; we
    // mimic only the part of its contract the transport actually
    // touches). Step 3 will couple this to live `ClusterState` flow;
    // for now the registrar fires the hook directly so the cache's
    // write-through path is the single thing under test.

    /// Minimal in-test `RoleChangeHookRegistrar` implementation that
    /// holds onto the registered hook and exposes a `fire` method
    /// driving it against an arbitrary `RoleTable`. Strictly enough
    /// to test the transport's cache plumbing without taking a
    /// dev-dep on the cluster-state crate.
    #[derive(Default)]
    struct TestRegistrar {
        hooks: Vec<Box<dyn Fn(&dynrunner_protocol_primary_secondary::RoleTable) + Send + Sync>>,
    }

    impl TestRegistrar {
        fn fire(&self, table: &dynrunner_protocol_primary_secondary::RoleTable) {
            for h in &self.hooks {
                h(table);
            }
        }
    }

    impl RoleChangeHookRegistrar for TestRegistrar {
        fn register_role_change_hook(
            &mut self,
            hook: Box<
                dyn Fn(&dynrunner_protocol_primary_secondary::RoleTable) + Send + Sync + 'static,
            >,
        ) {
            self.hooks.push(hook);
        }
    }

    /// After `register_with_cluster_state` runs and the registrar
    /// fires with a `RoleTable { primary: Some(id), .. }`, the
    /// transport's `peer_for_role(Role::Primary)` returns the same
    /// id. Pins the basic write-through path.
    #[tokio::test]
    async fn peer_transport_role_cache_populates_via_hook() {
        use dynrunner_protocol_primary_secondary::{Role, RoleTable};

        let ids = vec!["a".to_string(), "b".to_string()];
        let transports = peer_mesh::<SendTestId>(&ids);
        let transport = &transports[0];

        assert_eq!(
            transport.peer_for_role(&Role::Primary),
            None,
            "cache empty before registration"
        );

        let mut registrar = TestRegistrar::default();
        transport.register_with_cluster_state(&mut registrar);

        // Still None until the hook actually fires — registration
        // alone does not seed; the authoritative table has to send
        // a `RoleTable` snapshot through.
        assert_eq!(transport.peer_for_role(&Role::Primary), None);

        let table = RoleTable {
            primary: Some("sec-7".to_string()),
            ..Default::default()
        };
        registrar.fire(&table);

        assert_eq!(
            transport.peer_for_role(&Role::Primary),
            Some("sec-7".to_string()),
        );
    }

    /// A subsequent `PrimaryChanged` (modelled here as a second
    /// registrar.fire with a different holder) overwrites the
    /// cache. Pins the overwrite contract — Step 3's dispatch will
    /// silently misroute if the cache holds a stale id across a
    /// promotion.
    #[tokio::test]
    async fn peer_transport_role_cache_overwrites_on_subsequent_promote() {
        use dynrunner_protocol_primary_secondary::{Role, RoleTable};

        let ids = vec!["a".to_string(), "b".to_string()];
        let transports = peer_mesh::<SendTestId>(&ids);
        let transport = &transports[0];

        let mut registrar = TestRegistrar::default();
        transport.register_with_cluster_state(&mut registrar);

        registrar.fire(&RoleTable {
            primary: Some("first-leader".to_string()),
            ..Default::default()
        });
        assert_eq!(
            transport.peer_for_role(&Role::Primary),
            Some("first-leader".to_string()),
        );

        registrar.fire(&RoleTable {
            primary: Some("second-leader".to_string()),
            ..Default::default()
        });
        assert_eq!(
            transport.peer_for_role(&Role::Primary),
            Some("second-leader".to_string()),
            "second fire must overwrite first leader",
        );

        // Clearing the primary (e.g. an unset table) clears the
        // cache entry — `peer_for_role` returns None again. This
        // is the contract the protocol-crate helper enforces by
        // `remove(&Role::Primary)` ahead of the conditional insert.
        registrar.fire(&RoleTable {
            primary: None,
            ..Default::default()
        });
        assert_eq!(transport.peer_for_role(&Role::Primary), None);
    }

    // ── Step 3: Address::Role(_) dispatch ──
    //
    // These exercise the protocol-crate default `send` impl's role
    // arm through the channel transport, which now overrides
    // `local_id` (so `RoleAddressed.sender_id` carries a meaningful
    // value) but does not override `send`. The default impl resolves
    // the role through `peer_for_role`, wraps in `RoleAddressed`, and
    // calls `send_to_peer` — exactly the Step 3 contract.

    /// `send(Address::Role(Role::Primary), msg)` with a populated
    /// role cache routes the envelope to the cached holder. Post-
    /// Step 4 the receiver unwraps the envelope when its own cache
    /// agrees on the holder (Case A); the inner payload — not the
    /// wrapper — is what reaches `try_recv_peer`. The wire-frame
    /// shape (`RoleAddressed { sender_id, attempts: 0, … }`) is
    /// pinned by the codec round-trip tests in `codec_tests.rs`
    /// (the only place that observes the wrapper, since both
    /// transports now unwrap on receipt).
    #[tokio::test]
    async fn send_role_primary_routes_via_cache() {
        use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

        let ids = vec!["A".to_string(), "B".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        // Populate BOTH A's and B's caches so Role::Primary -> "B".
        // A's cache drives the send-time route (envelope ships to B);
        // B's cache drives the recv-time decision (Case A unwrap).
        let mut registrar_a = TestRegistrar::default();
        let mut registrar_b = TestRegistrar::default();
        transports[0].register_with_cluster_state(&mut registrar_a);
        transports[1].register_with_cluster_state(&mut registrar_b);
        for r in [&registrar_a, &registrar_b] {
            r.fire(&RoleTable {
                primary: Some("B".to_string()),
                ..Default::default()
            });
        }

        // Sanity: cache populated.
        assert_eq!(
            transports[0].peer_for_role(&Role::Primary),
            Some("B".to_string())
        );

        let inner = keepalive("A");
        transports[0]
            .send(Address::Role(Role::Primary), inner.clone())
            .await
            .expect("Role(Primary) send must succeed with populated cache");

        // Case A: B's cache agrees on the holder, so the recv-side
        // intercept unwraps the envelope. B sees the inner payload
        // — not the RoleAddressed wrapper.
        let received = transports[1].try_recv_peer().expect("B must receive");
        assert_eq!(received.sender_id(), inner.sender_id());
        assert_eq!(received.msg_type(), inner.msg_type());
        // No stray loopback to A.
        assert!(
            transports[0].try_recv_peer().is_none(),
            "sender must not loopback the envelope"
        );
    }

    /// `send(Address::Role(_), msg)` with an empty cache returns an
    /// `Err` whose message names "Role" and "cache" — the contract
    /// the trait's default impl documents. No message reaches any
    /// peer in this case.
    #[tokio::test]
    async fn send_role_unresolved_returns_err() {
        use dynrunner_protocol_primary_secondary::{Address, Role};

        let ids = vec!["A".to_string(), "B".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        // Cache deliberately NOT populated.
        let err = transports[0]
            .send(Address::Role(Role::Primary), keepalive("A"))
            .await
            .expect_err("cold cache must error");
        assert!(
            err.contains("Role"),
            "error must reference Role; got: {err}"
        );
        assert!(
            err.contains("cache"),
            "error must reference cache; got: {err}"
        );
        // No message must have reached any peer.
        assert!(transports[0].try_recv_peer().is_none());
        assert!(transports[1].try_recv_peer().is_none());
    }

    /// Pins the `local_id` plumbing end-to-end: A's id propagates
    /// from the `peer_mesh` constructor through the transport's
    /// `local_id` override into the `RoleAddressed.sender_id` wire
    /// field, and through Step 4's Case-A unwrap path the
    /// receiver's recv loop returns the inner payload unmodified.
    /// Wire-shape detail of the wrapper's sender_id is covered by
    /// the codec round-trip tests; the assertion here is the
    /// observable end-to-end behavior under Case A (B's cache
    /// agrees it holds Primary, so it unwraps).
    #[tokio::test]
    async fn send_role_envelope_round_trips_inner_payload() {
        use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

        let ids = vec!["A".to_string(), "B".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        let mut registrar_a = TestRegistrar::default();
        let mut registrar_b = TestRegistrar::default();
        transports[0].register_with_cluster_state(&mut registrar_a);
        transports[1].register_with_cluster_state(&mut registrar_b);
        for r in [&registrar_a, &registrar_b] {
            r.fire(&RoleTable {
                primary: Some("B".to_string()),
                ..Default::default()
            });
        }

        let inner = keepalive("A");
        transports[0]
            .send(Address::Role(Role::Primary), inner.clone())
            .await
            .unwrap();

        let received = transports[1].try_recv_peer().expect("B must receive");
        assert_eq!(received.sender_id(), "A");
        assert_eq!(received.msg_type(), inner.msg_type());
    }

    // ── Step 4: receiver-side relay-and-hint + Role::Self_ seeding ──
    //
    // These pin the four cases (A/B/C/D) of `decide_role_addressed`
    // through the channel transport's `recv_peer` integration plus the
    // construction-time seed of `Role::Self_` into the role cache.
    //
    // The recv-side intercept lives in `ChannelPeerTransport::
    // handle_role_layer` (which calls `decide_role_addressed_with_cache`
    // in the protocol crate). The decision module's own unit tests
    // (`crates/dynrunner-protocol-primary-secondary/src/role_routing.rs`)
    // exercise the pure decision; these tests exercise the wired
    // path end-to-end.

    /// Case A: a `RoleAddressed { intended_role: Primary }` envelope
    /// addressed to a peer whose cache agrees that it holds Primary
    /// is unwrapped — `recv_peer` returns the inner payload, not the
    /// wrapper. The Step-3 sender-side wire shape is unchanged; what
    /// changes here is the recv-side intercept.
    #[tokio::test]
    async fn role_addressed_case_a_unwraps_to_inner_payload() {
        use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

        let ids = vec!["A".to_string(), "B".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        // BOTH caches agree Primary=B; B's recv hits Case A.
        let mut reg_a = TestRegistrar::default();
        let mut reg_b = TestRegistrar::default();
        transports[0].register_with_cluster_state(&mut reg_a);
        transports[1].register_with_cluster_state(&mut reg_b);
        for r in [&reg_a, &reg_b] {
            r.fire(&RoleTable {
                primary: Some("B".to_string()),
                ..Default::default()
            });
        }

        let inner = keepalive("A");
        transports[0]
            .send(Address::Role(Role::Primary), inner.clone())
            .await
            .unwrap();

        let received = transports[1].try_recv_peer().expect("B receives");
        // Unwrapped: NOT the RoleAddressed wrapper, just the inner.
        assert!(
            matches!(received, DistributedMessage::Keepalive { .. }),
            "Case A must yield the inner payload, not the wrapper",
        );
        assert_eq!(received.sender_id(), inner.sender_id());
        // No additional traffic — no relay, no hint.
        assert!(transports[0].try_recv_peer().is_none());
    }

    /// Case B: A sends `Address::Role(Primary)` with its cache
    /// saying Primary=B; B's cache says Primary=C; C's cache also
    /// agrees Primary=C. Assert:
    ///   1. C receives the forwarded envelope with `attempts=1`
    ///      (Case A then unwraps for C, so C's `try_recv_peer`
    ///      yields the inner). To pin `attempts=1` we observe at
    ///      C's cache state pre-send vs. post-send is not enough,
    ///      so we additionally make C's cache empty for Primary so
    ///      the forwarded envelope lands at C as Case C (drop) and
    ///      we can intercept via C's `try_recv_peer` returning
    ///      nothing. But that loses the attempts-1 assertion.
    ///
    /// Compromise: configure C's cache to agree (Primary=C) so the
    /// payload reaches C unwrapped (Case A at C); the cache-warming
    /// hint must arrive at A (decoded into A's role cache as
    /// Primary=C). Then we verify A's Primary cache was updated
    /// from B to C — pinning the hint round-trip.
    ///
    /// To pin `attempts=1` on the forwarded envelope we additionally
    /// inspect the protocol-crate's `decide_role_addressed` unit
    /// test (which checks the attempts field directly).
    #[tokio::test]
    async fn role_addressed_case_b_relays_and_hints() {
        use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

        let ids = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        // A thinks Primary=B; B thinks Primary=C; C thinks Primary=C.
        let mut reg_a = TestRegistrar::default();
        let mut reg_b = TestRegistrar::default();
        let mut reg_c = TestRegistrar::default();
        transports[0].register_with_cluster_state(&mut reg_a);
        transports[1].register_with_cluster_state(&mut reg_b);
        transports[2].register_with_cluster_state(&mut reg_c);
        reg_a.fire(&RoleTable {
            primary: Some("B".to_string()),
            ..Default::default()
        });
        reg_b.fire(&RoleTable {
            primary: Some("C".to_string()),
            ..Default::default()
        });
        reg_c.fire(&RoleTable {
            primary: Some("C".to_string()),
            ..Default::default()
        });
        // Sanity: caches set as designed.
        assert_eq!(
            transports[0].peer_for_role(&Role::Primary),
            Some("B".to_string())
        );

        let inner = keepalive("A");
        transports[0]
            .send(Address::Role(Role::Primary), inner.clone())
            .await
            .unwrap();

        // B receives the wrapper, intercepts it (Case B): forwards
        // to C AND sends a hint back to A. Both internal sends
        // happen synchronously inside B's recv loop. Drive B's
        // recv with try_recv_peer; it should return None because
        // Case B never yields to the application layer.
        assert!(
            transports[1].try_recv_peer().is_none(),
            "B intercepts the envelope at recv-time; nothing surfaces to caller",
        );

        // C receives the forwarded envelope and (Case A on C)
        // unwraps it — try_recv_peer yields the inner.
        let at_c = transports[2].try_recv_peer().expect("C receives forwarded");
        assert!(
            matches!(at_c, DistributedMessage::Keepalive { .. }),
            "C must unwrap (Case A): {at_c:?}",
        );
        assert_eq!(at_c.sender_id(), inner.sender_id());

        // A receives the misaddress-hint and absorbs it into its
        // cache; nothing surfaces to A's caller.
        assert!(
            transports[0].try_recv_peer().is_none(),
            "hint is consumed at recv-time; never surfaced",
        );
        assert_eq!(
            transports[0].peer_for_role(&Role::Primary),
            Some("C".to_string()),
            "A's cache must be updated from B to C by the hint",
        );
    }

    /// Case C: receiver has no cached holder for the role → drop,
    /// no relay, no hint.
    #[tokio::test]
    async fn role_addressed_case_c_no_holder_drops() {
        use dynrunner_protocol_primary_secondary::{Address, Role, RoleTable};

        let ids = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        // A thinks Primary=B; B has no Primary in cache; C has no
        // Primary in cache. (Step 4's Role::Self_ seed populates
        // each cache with Self_=self, but Primary stays empty
        // without a hook fire.)
        let mut reg_a = TestRegistrar::default();
        transports[0].register_with_cluster_state(&mut reg_a);
        reg_a.fire(&RoleTable {
            primary: Some("B".to_string()),
            ..Default::default()
        });

        transports[0]
            .send(Address::Role(Role::Primary), keepalive("A"))
            .await
            .unwrap();

        // B intercepts the envelope; no cached holder → drop.
        assert!(
            transports[1].try_recv_peer().is_none(),
            "B should consume the envelope at recv-time (Case C drop)",
        );
        // C must not have received any relay.
        assert!(
            transports[2].try_recv_peer().is_none(),
            "no relay forwarded — no known holder to relay to",
        );
        // A must not have received any hint.
        assert!(
            transports[0].try_recv_peer().is_none(),
            "no hint sent back — Case C is a silent drop",
        );
        // A's cache stays at its pre-send value: Primary=B.
        assert_eq!(
            transports[0].peer_for_role(&Role::Primary),
            Some("B".to_string()),
            "Case C drops silently — sender's cache stays at the stale value",
        );
    }

    /// Case D: a forwarded envelope already at the relay-hop cap
    /// (`MAX_ROLE_RELAY_ATTEMPTS`) must NOT be forwarded further,
    /// even if the receiver knows a different holder. We bypass the
    /// sender path here (`PeerTransport::send` only ever emits
    /// `attempts=0`) and feed the envelope through B's inbox
    /// directly by sending it from A using `send_to_peer` — A's
    /// direct send carries whatever envelope we wrap manually.
    #[tokio::test]
    async fn role_addressed_case_d_max_attempts_drops() {
        use dynrunner_protocol_primary_secondary::{
            Role, RoleTable, MAX_ROLE_RELAY_ATTEMPTS,
        };

        let ids = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let mut transports = peer_mesh::<SendTestId>(&ids);

        // Configure B's cache so it WOULD be a Case-B candidate
        // (Primary=C != self). Without the attempts cap, B would
        // forward to C and hint back to A.
        let mut reg_b = TestRegistrar::default();
        transports[1].register_with_cluster_state(&mut reg_b);
        reg_b.fire(&RoleTable {
            primary: Some("C".to_string()),
            ..Default::default()
        });

        // Hand-craft an envelope at the cap. Sent A→B via direct
        // send_to_peer so it lands at B's inbox unaltered.
        let envelope = DistributedMessage::RoleAddressed {
            sender_id: "A".into(),
            timestamp: 1.0,
            intended_role: Role::Primary,
            payload: Box::new(keepalive("A")),
            attempts: MAX_ROLE_RELAY_ATTEMPTS,
        };
        transports[0].send_to_peer("B", envelope).await.unwrap();

        // B intercepts, Case D drops — nothing forwarded, no hint.
        assert!(transports[1].try_recv_peer().is_none());
        assert!(transports[2].try_recv_peer().is_none());
        assert!(transports[0].try_recv_peer().is_none());
    }

    /// Step 4 cache-init fix: `Role::Self_` must be populated with
    /// the local peer id immediately at construction time, before
    /// any `register_with_cluster_state` runs. Without this seed,
    /// a `RoleAddressed { intended_role: Self_ }` envelope would
    /// fall into Case C at the receiver (no cached holder → drop),
    /// which contradicts the role's semantics ("the receiver IS by
    /// definition the holder of Self_").
    #[tokio::test]
    async fn role_self_cache_populated_at_init() {
        use dynrunner_protocol_primary_secondary::Role;

        let ids = vec!["A".to_string(), "B".to_string()];
        let transports = peer_mesh::<SendTestId>(&ids);

        // Self_ resolves to local_id with no hook ever fired.
        assert_eq!(
            transports[0].peer_for_role(&Role::Self_),
            Some("A".to_string()),
        );
        assert_eq!(
            transports[1].peer_for_role(&Role::Self_),
            Some("B".to_string()),
        );

        // Primary, by contrast, stays cold until a hook fires.
        assert_eq!(transports[0].peer_for_role(&Role::Primary), None);
        assert_eq!(transports[1].peer_for_role(&Role::Primary), None);
    }
}
