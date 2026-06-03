//! Peer-to-peer network transport between secondaries.
//!
//! Each secondary runs a [`PeerNetwork`] that:
//! 1. Starts a local QUIC+WSS server for incoming peer connections
//! 2. Connects to other peers using their cert/address from the PeerInfo message
//! 3. Broadcasts messages to all connected peers
//! 4. Receives messages from any peer into a single channel

use std::collections::HashMap;
use std::net::SocketAddr;

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, PeerConnectionInfo, Router};
use tokio::sync::mpsc;

use crate::certs::CertPair;
use crate::transport::QuicListener;
use crate::wss::WssListener;

mod accept;
mod dial;
mod either;
mod handler;
mod mesh_send;
mod no_peer;
mod reconnect;
mod transport_impl;
mod util;

#[cfg(test)]
mod tests;

pub use either::EitherPeerTransport;
pub use mesh_send::MeshSendHandle;
pub use no_peer::NoPeerTransport;

use mesh_send::MeshSend;

/// A peer connection accepted by this node's server.
pub(super) struct AcceptedPeer<I: Identifier> {
    pub(super) peer_id: String,
    pub(super) outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
}

/// Peer-to-peer network transport for secondary coordinators.
///
/// Manages bidirectional connections to all peer secondaries. Uses QUIC (UDP)
/// with WSS (TCP) fallback, same as the primary-secondary transport.
pub struct PeerNetwork<I: Identifier> {
    /// Our secondary ID.
    peer_id: String,
    /// Our certificate for QUIC server.
    cert: CertPair,
    /// The port we're listening on.
    port: u16,
    /// Per-peer outgoing channels, keyed by peer_id.
    connections: HashMap<String, mpsc::UnboundedSender<DistributedMessage<I>>>,
    /// Incoming messages from all peers.
    incoming_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    /// Sender side (kept for spawning new connection handlers).
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    /// New connections from the accept loop that need registration.
    new_conn_rx: mpsc::UnboundedReceiver<AcceptedPeer<I>>,
    /// Sender side for accept loop AND per-peer outgoing-dial tasks
    /// (see `connect_to_peers`). Cloning this sender lets a spawned
    /// dial task hand off a successful connection through the same
    /// registration channel the accept loop uses, so callers don't
    /// have to await per-peer dials and miss tokio::select! tick
    /// budgets while connect_to_peers drains.
    new_conn_tx: mpsc::UnboundedSender<AcceptedPeer<I>>,
    /// Peer-mesh routing dispatcher. Owns ALL routing state
    /// (in-flight relays, blacklist, per-peer route observation,
    /// monotonic relay-id counter) and produces the redial signal
    /// the transport acts on via `spawn_redial`. The transport
    /// itself never inspects routing state.
    pub(super) router: Router<I>,
    /// Cached dial info per peer id, populated wholesale on each
    /// `connect_to_peers` call (replacement, not extension): peers
    /// dropped from the new authoritative list have their cached
    /// dial info removed so subsequent redial signals for those
    /// peers find no dial info and silently no-op. Used by
    /// `spawn_redial` when the Router emits a redial target after a
    /// relay observation.
    pub(super) peer_dial_info: HashMap<String, PeerConnectionInfo>,
    /// Periodic reconnect-tick receiver. The 5s ticker spawned in
    /// `start()` fires `()` here; `recv_peer`'s tokio::select! arm
    /// pulls the tick and drives the reconnect state machine.
    ///
    /// Held as a plain receiver (not `Option<…>`) so `recv_peer` can
    /// poll it via the disjoint-field borrow `self.reconnect_tick_rx
    /// .recv()` inside `tokio::select!`. The prior `Option` + per-
    /// arm `.take()`/restore dance was not cancel-safe: if the outer
    /// caller's `select!` dropped the `recv_peer` future while the
    /// inner select was pending, the local taken-out receiver was
    /// destroyed together with the dropped future and
    /// `reconnect_tick_rx` stayed `None` forever, silently disabling
    /// the periodic reconnect tick for the lifetime of the
    /// coordinator. `UnboundedReceiver::recv()` is itself cancel-
    /// safe, so polling the field in place preserves the contract.
    pub(super) reconnect_tick_rx: mpsc::UnboundedReceiver<()>,
    /// Test-only handle to the reconnect-tick sender. Production
    /// builds drop the sender into the ticker task spawned in
    /// `start()`; the test backdoor keeps a clone so regression
    /// tests can inject synthetic ticks without waiting for the
    /// real 5s cadence (see `peer/tests.rs::
    /// recv_peer_tick_survives_outer_drop`). Gated to `cfg(test)`
    /// so the production struct layout is unchanged.
    #[cfg(test)]
    pub(super) reconnect_tick_tx_for_test: mpsc::UnboundedSender<()>,
    /// Per-peer reconnect-attempt state. See `reconnect.rs` for
    /// the milestone schedule and the disconnect/reconnect event
    /// semantics. Visibility limited to the `peer` submodule so
    /// the tracker stays an implementation detail; callers don't
    /// see (or depend on) the milestone schedule directly.
    pub(super) reconnect_tracker: reconnect::ReconnectTracker,

    /// Receiver for the cloneable mesh-send proxy (see
    /// [`MeshSendHandle`]). A co-located parked `PrimaryCoordinator`
    /// holds a clone of the matching sender and queues its remote sends
    /// here; `recv_peer`'s drain arm forwards each through this
    /// network's own relay-aware `send_to_peer` / `broadcast` so the
    /// router applies uniformly. `None`-yielding is impossible while the
    /// network lives (the network keeps its own sender clone in
    /// `mesh_send_tx`), so the drain arm only fires on real items.
    proxy_rx: mpsc::UnboundedReceiver<MeshSend<I>>,

    /// Network-held clone of the proxy sender. Kept so
    /// [`Self::mesh_send_handle`] can mint additional cloneable handles
    /// at any time AND so `proxy_rx` never observes a spurious close
    /// while the network is alive (the handed-out handles may all be
    /// dropped — e.g. a never-promoted parked primary — without closing
    /// the drain).
    mesh_send_tx: mpsc::UnboundedSender<MeshSend<I>>,
}

impl<I: Identifier> PeerNetwork<I> {
    /// Create a new peer network: generate a certificate and start listening.
    pub async fn start(peer_id: &str) -> Result<Self, String> {
        let cert = CertPair::generate(peer_id)?;

        // Bind QUIC (UDP)
        let quic_listener = QuicListener::bind(&cert).await?;
        let port = quic_listener.port();

        // Bind WSS (TCP) on the same port
        let wss_addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        let wss_listener = WssListener::bind(wss_addr).await?;

        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (new_conn_tx, new_conn_rx) = mpsc::unbounded_channel();

        // Spawn QUIC accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            tokio::task::spawn_local(async move {
                accept::quic_accept_loop::<I>(quic_listener, incoming_tx, new_conn_tx).await;
            });
        }

        // Spawn WSS accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            tokio::task::spawn_local(async move {
                accept::wss_accept_loop::<I>(wss_listener, incoming_tx, new_conn_tx).await;
            });
        }

        // Periodic reconnect ticker: every RECONNECT_TICK
        // (5s, per peer/reconnect.rs), the tick task sends ()
        // through `tick_tx`. PeerNetwork's recv_peer pulls the
        // tick and runs `reconnect_tracker.tick()` to issue a
        // redial for every peer currently in the disconnect
        // tracker. The cadence is decoupled from the keepalive
        // interval and from the Router-driven redial pulse — they
        // coexist; `spawn_redial` deduplicates against
        // `connections` so a freshly-restored peer doesn't get a
        // second dial.
        let (reconnect_tick_tx, reconnect_tick_rx) = mpsc::unbounded_channel();
        #[cfg(test)]
        let reconnect_tick_tx_for_test = reconnect_tick_tx.clone();
        {
            let tick_tx = reconnect_tick_tx;
            tokio::task::spawn_local(async move {
                let mut interval = tokio::time::interval(reconnect::RECONNECT_TICK);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Skip the first immediate tick: `Interval::tick()`
                // resolves immediately on first call which would
                // ping the tracker before any peers are tracked.
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if tick_tx.send(()).is_err() {
                        break;
                    }
                }
            });
        }

        tracing::info!(peer_id, port, "peer network listening (QUIC/UDP + WSS/TCP)");

        let (mesh_send_tx, proxy_rx) = mpsc::unbounded_channel();
        Ok(Self {
            peer_id: peer_id.to_string(),
            cert,
            port,
            connections: HashMap::new(),
            incoming_rx,
            incoming_tx,
            new_conn_rx,
            new_conn_tx,
            router: Router::new(peer_id.to_string()),
            peer_dial_info: HashMap::new(),
            reconnect_tick_rx,
            #[cfg(test)]
            reconnect_tick_tx_for_test,
            reconnect_tracker: reconnect::ReconnectTracker::new(),
            proxy_rx,
            mesh_send_tx,
        })
    }

    /// Fold the secondary's dialed primary connection — the bootstrap
    /// wire — into THIS mesh as a routable member keyed by `primary_id`,
    /// in BOTH directions. After this call the bootstrap wire is just
    /// another mesh connection: "the tunnel is just a way of joining the
    /// mesh".
    ///
    /// Takes the whole [`crate::NetworkClient`] by value (the mesh now
    /// owns the wire; there is no separate `uplink` leg). Both
    /// directions are wired onto the one uniform mesh path:
    ///
    /// - **Outbound:** [`crate::NetworkClient::mesh_writer`] mints a
    ///   fan-in send handle into the wire; it is inserted into
    ///   [`Self::connections`] so [`PeerTransport::send_to_peer`] /
    ///   observable via [`PeerTransport::has_peer`] resolve over the
    ///   existing connection (no second wire is opened).
    /// - **Inbound:** a forwarder task drains the client's `recv()` into
    ///   this network's single [`Self::incoming_tx`] fan-in — the same
    ///   sink the accept loops + outgoing-dial handlers feed — so primary
    ///   frames surface through [`PeerTransport::recv_peer`] like any
    ///   other peer's. The task exits when the wire closes (`recv()` →
    ///   `None`) or `incoming_tx` is dropped. It lives on the same
    ///   `LocalSet` as the rest of the mesh's reader/writer tasks.
    ///
    /// The connection goes in the same [`Self::connections`] table every
    /// directed send + the router read from, so routing to the primary
    /// uses the one uniform path. The transport keeps no notion of
    /// "which connection is the primary": the primary is a plain mesh
    /// peer here, indistinguishable from any other. Any "exclude the
    /// primary" policy (quorum, mesh-health) is a role concern resolved
    /// at the coordinator edge, not in the transport.
    pub fn register_primary_link(&mut self, primary_id: String, client: crate::NetworkClient<I>) {
        use dynrunner_core::MessageReceiver;

        tracing::info!(primary = %primary_id, "folded primary bootstrap wire into the mesh");
        // Outbound: fan-in send handle into the existing wire.
        self.connections
            .insert(primary_id.clone(), client.mesh_writer());

        // Inbound: drive the wire's inbound into the mesh's single
        // fan-in, so the primary's frames arrive via `recv_peer` like
        // every other peer's — no per-role uplink recv arm.
        let incoming_tx = self.incoming_tx.clone();
        tokio::task::spawn_local(async move {
            let mut client = client;
            while let Some(msg) = client.recv().await {
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
            tracing::debug!("primary bootstrap-wire inbound forwarder done");
        });
    }

    /// Mint a cloneable [`MeshSendHandle`] over this network's mesh.
    ///
    /// The co-located parked `PrimaryCoordinator`'s send-proxy holds the
    /// returned handle to reach remote secondaries over the SAME mesh
    /// this `PeerNetwork` owns (the secondary's `EitherPeerTransport::Real`
    /// arm) — without aliasing this network's `connections` ownership.
    /// Sends queued on the handle are drained + dispatched (relay-aware)
    /// inside [`Self::recv_peer`]. See the [`mesh_send`] module docs.
    pub fn mesh_send_handle(&self) -> MeshSendHandle<I> {
        MeshSendHandle::new(self.mesh_send_tx.clone())
    }

    /// The port this peer network is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The public certificate PEM for sharing with other peers.
    pub fn cert_pem(&self) -> &str {
        &self.cert.cert_pem
    }

    /// The certificate DER for QUIC client connections.
    pub fn cert_der(&self) -> &rustls::pki_types::CertificateDer<'static> {
        &self.cert.cert_der
    }

    /// Initiate connections to all peers from the peer list received
    /// from primary. **Non-blocking**: spawns one task per peer to do
    /// the actual QUIC/WSS dial, then returns immediately. Successful
    /// dials register through `new_conn_tx` (the same channel the
    /// accept loop uses for incoming connections); failed dials log
    /// and exit silently. Callers can observe completion via
    /// `peer_count()` (which calls `drain_new_connections` first) or
    /// by simply going on with their work — incoming peer messages
    /// route through `recv_peer` regardless.
    ///
    /// Why non-blocking: the previous shape (await each per-peer
    /// dial sequentially) blocked `wait_for_setup`'s `tokio::select!`
    /// for up to 10s × num_peers when the per-peer QUIC handshake
    /// timed out. That's fatal on clusters where compute nodes can't
    /// reach each other directly (most institutional SLURM setups —
    /// firewalled / NAT'd compute fabric): all peer dials hit their
    /// 10s timeout, the secondary's keepalive ticker can't fire from
    /// inside the blocked select arm, and the primary declares the
    /// secondary dead before peer-setup returns.
    ///
    /// Per-peer dial uses happy-eyeballs: when both `ipv4` and `ipv6`
    /// are set in [`PeerConnectionInfo`], QUIC and WSS attempts run in
    /// parallel across both families inside [`dial::dial_peer`] and
    /// the first connected socket wins. Single-family peers race
    /// against just the one address (no overhead). Total per-peer
    /// budget is ≤ 20s, same as the pre-happy-eyeballs sequential
    /// QUIC-then-WSS shape.
    pub fn connect_to_peers(&mut self, peers: &[PeerConnectionInfo]) {
        // Cache dial info wholesale: peers dropped from this
        // authoritative list have their cached entry removed so
        // subsequent redial signals against retired peers find no
        // dial info and silently no-op. The primary's `PeerInfo`
        // broadcast is the source of truth — chunked / additive
        // callers would observe peers leaking after a membership
        // change, so we replace rather than extend.
        self.peer_dial_info.clear();
        self.peer_dial_info.extend(
            peers
                .iter()
                .filter(|p| p.secondary_id != self.peer_id)
                .map(|p| (p.secondary_id.clone(), p.clone())),
        );

        for peer_info in peers {
            if peer_info.secondary_id == self.peer_id {
                continue; // Skip self
            }

            if self.connections.contains_key(&peer_info.secondary_id) {
                continue; // Already connected (from accept loop)
            }

            // Lower-id-dials: only the secondary whose id sorts
            // lexicographically lower than the peer's initiates the
            // dial; the higher-id node relies on its accept loop to
            // receive the inbound connection.
            //
            // Without this asymmetry, both sides race to dial each
            // other on `connect_to_peers`. Each side then sees TWO
            // candidate connections to the same peer in its
            // `new_conn_rx` queue — one from its own outbound dial,
            // one accepted from the peer's outbound dial. The
            // existing `drain_new_connections` dedup keeps whichever
            // arrives first and DROPS the duplicate's
            // `AcceptedPeer.outgoing_tx`. Dropping that sender
            // tears down the duplicate's WSS pipe via the
            // writer/reader cleanup chain — which is the SAME WSS
            // pipe the OTHER side may have chosen to KEEP. The peer
            // then sees its kept-connection's writer die, and its
            // `connections[us]` becomes a dead `outgoing_tx`. The
            // failure surfaces as "peer disconnected during
            // broadcast" warns at the next keepalive tick, on what
            // is otherwise a healthy fleet — both consumer teams
            // hit this on Krater (tokenizer cohort-5 dispatch lost
            // its entire peer mesh ~10s after promotion; dataset's
            // 8-secondary K=2 run had 3-of-8 secondaries lose all
            // peers and sit idle while the others were saturated).
            //
            // Lower-id-dials makes the connection asymmetric and
            // eliminates the duplicate scenario: there is at most
            // one WSS pipe per pair. The accept loop on the
            // higher-id side handles the inbound dial as before.
            if self.peer_id.as_str() > peer_info.secondary_id.as_str() {
                tracing::debug!(
                    self_id = %self.peer_id,
                    peer = %peer_info.secondary_id,
                    "skipping dial: peer has lower id, expecting them to initiate"
                );
                continue;
            }

            self.spawn_dial_task(peer_info.clone());
        }
    }

    /// Drain any newly accepted incoming connections and register them.
    fn drain_new_connections(&mut self) {
        while let Ok(accepted) = self.new_conn_rx.try_recv() {
            if !self.connections.contains_key(&accepted.peer_id) {
                // Clear reconnect-tracker state for this peer
                // BEFORE inserting into `connections` so the
                // operator log shape is:
                //   "peer reconnected (attempts=N elapsed=Ms)"
                // then
                //   "incoming peer registered"
                // (resolution preceded by the reconnect-tracker
                // disposition the operator was tracking).
                self.reconnect_tracker.observe_reconnect(&accepted.peer_id);
                tracing::info!(peer = %accepted.peer_id, "incoming peer registered");
                self.connections
                    .insert(accepted.peer_id, accepted.outgoing_tx);
            }
        }
    }

    /// Drive the reconnect state machine. Called from `recv_peer`'s
    /// select arm on every reconnect-ticker pulse (5s). Reconciles
    /// the tracker against the authoritative cluster list:
    /// - any peer in `peer_dial_info` that is NOT in `connections`
    ///   becomes tracked (observe_disconnect, idempotent on
    ///   already-tracked peers);
    /// - any peer in `connections` is cleared from the tracker
    ///   (observe_reconnect, idempotent on absence);
    /// - then `tracker.tick()` bumps attempt counters, emits any
    ///   crossed-milestone WARNs, and returns the list of peers to
    ///   redial.
    ///
    /// `spawn_redial` itself deduplicates against `connections`,
    /// so this method is safe even if a redial from a prior tick
    /// is still in flight when the next tick fires.
    fn process_reconnect_tick(&mut self) {
        let cluster_peers: Vec<String> = self.peer_dial_info.keys().cloned().collect();
        for peer_id in &cluster_peers {
            if self.connections.contains_key(peer_id) {
                self.reconnect_tracker.observe_reconnect(peer_id);
            } else {
                self.reconnect_tracker.observe_disconnect(peer_id);
            }
        }
        let to_dial = self.reconnect_tracker.tick();
        for peer_id in to_dial {
            self.spawn_redial(&peer_id);
        }
    }

    /// Background-dial `peer_id` if we have its cached connection
    /// info AND we own the dial-side of the lower-id-dials rule.
    /// Called whenever the [`Router`] emits a redial signal — i.e. on
    /// the first observation of an active relay relationship with
    /// `peer_id` (or first re-observation past `REDIAL_COOLDOWN`).
    /// Silent on attempt and silent on failure: the only operator-
    /// visible signal that the peer is reachable again is the
    /// Router's `Relay → Direct` info log on the next send through
    /// the restored peer.
    fn spawn_redial(&self, peer_id: &str) {
        if self.connections.contains_key(peer_id) {
            return; // already directly connected (mid-heal: prior dial
            // landed but the cooldown timestamp is still hot).
            // Skipping avoids a duplicate WSS pipe whose sender
            // would later be discarded by drain_new_connections.
        }
        let Some(peer_info) = self.peer_dial_info.get(peer_id).cloned() else {
            return; // peer no longer in the authoritative cluster list
        };
        if self.peer_id.as_str() > peer_id {
            return; // higher-id side: lower-id-dials rule, accept loop handles inbound
        }
        self.spawn_dial_task(peer_info);
    }

    /// Spawn a per-peer outgoing dial task: race QUIC/WSS via
    /// happy-eyeballs, hand off the resulting connection through
    /// `new_conn_tx` so the caller's next `drain_new_connections`
    /// picks it up. Single dispatch shape used by both the initial-
    /// dial path (`connect_to_peers`) and the redial path
    /// (`spawn_redial`); failed dials exit silently in both cases.
    fn spawn_dial_task(&self, peer_info: PeerConnectionInfo) {
        let incoming_tx = self.incoming_tx.clone();
        let new_conn_tx = self.new_conn_tx.clone();
        tokio::task::spawn_local(async move {
            let peer_id = peer_info.secondary_id.clone();
            let Some(connection) = dial::dial_peer(&peer_id, &peer_info).await else {
                return;
            };
            let outgoing_tx =
                handler::spawn_outgoing_handler(peer_id.clone(), connection, incoming_tx);
            let _ = new_conn_tx.send(AcceptedPeer {
                peer_id,
                outgoing_tx,
            });
        });
    }
}
