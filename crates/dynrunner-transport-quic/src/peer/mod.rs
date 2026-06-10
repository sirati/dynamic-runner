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
mod bootstrap_redial;
mod dial;
mod handler;
mod mesh_send;
mod reconnect;
mod transport_impl;
mod util;

#[cfg(test)]
mod tests;

pub use mesh_send::MeshSendHandle;

use mesh_send::MeshSend;

/// What one `connect_to_peers` sweep decided, per disposition — the
/// single source for the sweep-summary log line and the unit tests
/// that pin the dispositions. A sweep that spawns ZERO dials is a
/// legitimate, structural outcome (every listed peer has a lower id,
/// so this node awaits their inbound dials), and before this summary
/// existed it was INVISIBLE at operator level: the coordinator logged
/// "kicking off peer dials peers=N" and then nothing, ever — the #362
/// production shape.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct DialSweepSummary {
    /// Peers in the list excluding self.
    pub(super) listed: usize,
    /// Dial tasks actually spawned this sweep.
    pub(super) spawned: usize,
    /// Skipped: already in `connections` (accept loop or earlier dial).
    pub(super) already_connected: usize,
    /// Skipped by the lower-id-dials rule: these peers' ids sort LOWER
    /// than ours, so THEY dial US — this node never dials them and its
    /// mesh leg to each depends entirely on their inbound succeeding.
    pub(super) awaiting_inbound: Vec<String>,
    /// Peers that were in the previous authoritative dial list but are
    /// absent from this one: their cached dial info AND any redial
    /// tracking stop now (membership-replacement semantics).
    pub(super) dropped_from_list: Vec<String>,
}

/// RAII abort-detector for a spawned dial task (#362). Armed at task
/// entry, defused once `dial_peer` returned (every return path of
/// which logs its outcome). If the task is dropped mid-dial (LocalSet
/// teardown cancels detached tasks) or unwinds on a panic, `Drop` runs
/// with the guard still armed and emits the WARN — so a dial that was
/// SPAWNED can never conclude tracelessly: it logs an outcome, or it
/// logs that it was aborted before reaching one.
struct DialTaskGuard {
    peer_id: String,
    attempt: dial::DialAttempt,
    armed: bool,
}

impl DialTaskGuard {
    fn new(peer_id: String, attempt: dial::DialAttempt) -> Self {
        Self {
            peer_id,
            attempt,
            armed: true,
        }
    }

    fn defuse(&mut self) {
        self.armed = false;
    }
}

impl Drop for DialTaskGuard {
    fn drop(&mut self) {
        if self.armed {
            tracing::warn!(
                peer = %self.peer_id,
                attempt = %self.attempt,
                "peer dial task aborted before reaching an outcome \
                 (task cancelled mid-dial or panicked) — the dial \
                 produced neither a success nor a failure"
            );
        }
    }
}

/// A peer connection accepted by this node's server.
pub(super) struct AcceptedPeer<I: Identifier> {
    pub(super) peer_id: String,
    pub(super) outgoing_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
}

/// A peer connection whose reader/writer supervisor has exited — the
/// AUTHORITATIVE disconnect detector. Mirrors [`AcceptedPeer`]: the
/// reader/writer tasks hold no `&mut self`, so the supervisor cannot
/// touch `connections` directly; it hands the disconnect back to
/// `recv_peer` through the disconnect-signal channel (the close-side
/// analog of `new_conn_tx`), which runs the prune+redial disposition.
///
/// `outgoing_tx` is a clone of the writer's own channel sender, carried
/// for the generation check in
/// [`super::PeerNetwork::handle_peer_disconnect`]: the entry is pruned
/// only if `connections[peer_id]` is STILL this exact channel, so a
/// late disconnect signal from a torn-down connection cannot delete a
/// freshly-reconnected entry for the same peer.
pub(super) struct DisconnectedPeer<I: Identifier> {
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
    /// Reader/writer-supervisor disconnect signals that need the
    /// prune+redial disposition. The close-side analog of
    /// `new_conn_rx`: a per-connection supervisor (accept side in
    /// `accept.rs`, outgoing side in `handler.rs`) fires a
    /// [`DisconnectedPeer`] here the instant its reader OR writer task
    /// exits, and `recv_peer`'s `select!` drains it and runs
    /// `handle_peer_disconnect`. This is the AUTHORITATIVE liveness
    /// detector — the QUIC `IDLE_TIMEOUT` (see `certs.rs`) errors a
    /// blackholed read, the reader task exits, and the disconnect
    /// surfaces here without waiting for an outbound-send failure.
    disconnect_rx: mpsc::UnboundedReceiver<DisconnectedPeer<I>>,
    /// Sender side for the disconnect-signal channel, cloned into every
    /// per-connection supervisor (accept loops + outgoing-dial tasks),
    /// mirroring how `new_conn_tx` is cloned into the same places.
    disconnect_tx: mpsc::UnboundedSender<DisconnectedPeer<I>>,
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
    /// [`MeshSendHandle`]). An on-demand same-host `PrimaryCoordinator`
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
    /// dropped — e.g. a host that is never promoted, so no same-host
    /// primary is ever constructed — without closing the drain).
    mesh_send_tx: mpsc::UnboundedSender<MeshSend<I>>,

    /// Re-dialed bootstrap-wire handoff (defect (b)). When a folded
    /// submitter-bootstrap wire closes (the `-R` tunnel dropped), an
    /// off-loop supervisor re-dials it INDEFINITELY and sends the fresh
    /// client here; `recv_peer`'s drain arm re-folds it via
    /// [`Self::register_primary_link`] under `&mut self`. This restores
    /// ONLY the transport pipe — it feeds NO failover input (the
    /// secondary→primary app-layer liveness window stays the sole
    /// failover signal; see [`bootstrap_redial`]). `None`-yielding is
    /// impossible while the network lives (the held `bootstrap_redial_tx`
    /// clone keeps the channel open), so the arm only fires on real
    /// re-dials.
    bootstrap_redial_rx: mpsc::UnboundedReceiver<bootstrap_redial::BootstrapRedial<I>>,
    /// Network-held clone of the bootstrap-redial sender, handed to each
    /// folded wire's redial supervisor. Kept on the struct so
    /// `bootstrap_redial_rx` never observes a spurious close on the path
    /// where no bootstrap wire was ever folded (an observer/late-joiner
    /// that has no submitter bootstrap link).
    bootstrap_redial_tx: mpsc::UnboundedSender<bootstrap_redial::BootstrapRedial<I>>,
}

impl<I: Identifier> PeerNetwork<I> {
    /// Create a new peer network: generate a certificate and start listening.
    ///
    /// `bind_port` is the numeric port BOTH listeners bind (QUIC on UDP,
    /// WSS on TCP — the same-numeric-port convention below). `None` (and
    /// `Some(0)`) keeps the historical behaviour: the OS picks an
    /// ephemeral port for the QUIC bind and WSS follows it. A concrete
    /// port exists for deployments where the port was advertised BEFORE
    /// this network started — e.g. the SLURM wrapper pre-allocates a free
    /// port host-side, records it in the late-joiner's
    /// `connection_info/<id>.info` file, and hands it to the in-container
    /// secondary via `--secondary-quic-port`; binding anything else makes
    /// the recorded port a dead address for every peer that dials it.
    pub async fn start(peer_id: &str, bind_port: Option<u16>) -> Result<Self, String> {
        let cert = CertPair::generate(peer_id)?;

        // Bind QUIC (UDP). `bind_port` None → port 0 → the OS picks, which
        // is exactly what `QuicListener::bind` (the no-port convenience)
        // does — the None path is unchanged from the historical behaviour.
        let quic_bind: SocketAddr =
            (std::net::Ipv4Addr::UNSPECIFIED, bind_port.unwrap_or(0)).into();
        let quic_listener = QuicListener::bind_addr(&cert, quic_bind).await?;
        let port = quic_listener.port();

        // Bind WSS (TCP) on the same port
        let wss_addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        let wss_listener = WssListener::bind(wss_addr).await?;

        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (new_conn_tx, new_conn_rx) = mpsc::unbounded_channel();
        let (disconnect_tx, disconnect_rx) = mpsc::unbounded_channel();

        // Spawn QUIC accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            let disconnect_tx = disconnect_tx.clone();
            tokio::task::spawn_local(async move {
                accept::quic_accept_loop::<I>(
                    quic_listener,
                    incoming_tx,
                    new_conn_tx,
                    disconnect_tx,
                )
                .await;
            });
        }

        // Spawn WSS accept loop
        {
            let incoming_tx = incoming_tx.clone();
            let new_conn_tx = new_conn_tx.clone();
            let disconnect_tx = disconnect_tx.clone();
            tokio::task::spawn_local(async move {
                accept::wss_accept_loop::<I>(wss_listener, incoming_tx, new_conn_tx, disconnect_tx)
                    .await;
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
        let (bootstrap_redial_tx, bootstrap_redial_rx) = mpsc::unbounded_channel();
        Ok(Self {
            peer_id: peer_id.to_string(),
            cert,
            port,
            connections: HashMap::new(),
            incoming_rx,
            incoming_tx,
            new_conn_rx,
            new_conn_tx,
            disconnect_rx,
            disconnect_tx,
            router: Router::new(peer_id.to_string()),
            peer_dial_info: HashMap::new(),
            reconnect_tick_rx,
            #[cfg(test)]
            reconnect_tick_tx_for_test,
            reconnect_tracker: reconnect::ReconnectTracker::new(),
            proxy_rx,
            mesh_send_tx,
            bootstrap_redial_rx,
            bootstrap_redial_tx,
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
    pub fn register_primary_link(
        &mut self,
        primary_id: String,
        dial_addr: std::net::SocketAddr,
        client: crate::NetworkClient<I>,
    ) {
        let target = bootstrap_redial::BootstrapDialTarget {
            addr: dial_addr,
            primary_id,
        };
        self.fold_primary_link(target, client);
    }

    /// Fold (or RE-fold) a bootstrap wire into the mesh and arm its
    /// re-dial. Shared by the initial [`Self::register_primary_link`] and
    /// the `recv_peer` re-fold of a re-dialed wire (defect (b)), so the
    /// mesh-install + redial-arming logic lives in exactly one place.
    ///
    /// `target` is the FIXED `localhost:<tunnel_port>` dial address +
    /// primary id, retained so the inbound forwarder can re-dial through
    /// the (rebuilt) tunnel after a drop.
    pub(in crate::peer) fn fold_primary_link(
        &mut self,
        target: bootstrap_redial::BootstrapDialTarget,
        client: crate::NetworkClient<I>,
    ) {
        use dynrunner_core::MessageReceiver;

        tracing::info!(primary = %target.primary_id, "folded primary bootstrap wire into the mesh");
        // Outbound: fan-in send handle into the existing wire.
        self.connections
            .insert(target.primary_id.clone(), client.mesh_writer());

        // Inbound: drive the wire's inbound into the mesh's single
        // fan-in, so the primary's frames arrive via `recv_peer` like
        // every other peer's — no per-role uplink recv arm.
        //
        // App-layer-liveness independence (honest-liveness invariant):
        // the bootstrap-primary entry deliberately does NOT feed the
        // QUIC disconnect-signal channel — only the dial/accept mesh
        // handlers do. The secondary→primary link's liveness is owned
        // by the APPLICATION layer (the secondary's primary-link
        // failure window in
        // `dynrunner_manager_distributed::secondary::primary_link`,
        // re-checked on every keepalive tick), keyed on
        // `DistributedMessage` arrival, NOT on QUIC packet liveness. A
        // primary that is alive-at-QUIC (PINGs still acked) but
        // wedged-at-app (no `DistributedMessage`s) MUST still be
        // declared dead — and it is, because no primary message arrives
        // and `should_arm_failover` trips on the time axis regardless
        // of QUIC. Conversely the QUIC `IDLE_TIMEOUT` (60s) is set
        // ABOVE that 30s failure window precisely so the QUIC layer
        // never closes a healthy-but-quiet bootstrap wire out from
        // under the app layer. The two liveness detectors are
        // orthogonal by construction; do not couple them by routing the
        // forwarder's exit through `handle_peer_disconnect`.
        //
        // Defect (b): when the forwarder exits because the WIRE CLOSED
        // (`recv()` → None — the `-R` tunnel dropped), arm the bootstrap
        // re-dial supervisor so the link is restored after the tunnel is
        // rebuilt. This restores ONLY the transport pipe; it feeds no
        // failover input, preserving the honest-liveness decoupling
        // above. When the forwarder exits because `incoming_tx` closed
        // (the network is tearing down), do NOT re-dial.
        let incoming_tx = self.incoming_tx.clone();
        let redial_tx = self.bootstrap_redial_tx.clone();
        tokio::task::spawn_local(async move {
            let mut client = client;
            let mut wire_closed = false;
            loop {
                match client.recv().await {
                    Some(msg) => {
                        if incoming_tx.send(msg).is_err() {
                            // Network tearing down — not a wire drop.
                            break;
                        }
                    }
                    None => {
                        // The bootstrap wire itself closed (tunnel down).
                        wire_closed = true;
                        break;
                    }
                }
            }
            tracing::debug!("primary bootstrap-wire inbound forwarder done");
            if wire_closed {
                // Re-dial INDEFINITELY off-loop; the fresh client comes
                // back through `redial_tx` for re-fold under `&mut self`.
                tokio::task::spawn_local(bootstrap_redial::redial_bootstrap_wire(
                    target, redial_tx,
                ));
            }
        });
    }

    /// Mint a cloneable [`MeshSendHandle`] over this network's mesh.
    ///
    /// The on-demand same-host `PrimaryCoordinator`'s send-proxy holds the
    /// returned handle to reach remote secondaries over the SAME mesh
    /// this `PeerNetwork` owns (the secondary's peer mesh)
    /// — without aliasing this network's `connections` ownership.
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
        let summary = self.connect_to_peers_inner(peers);
        // Sweep-summary narration (#362): ONE operator line per sweep
        // naming every disposition, so a sweep that spawned zero dials
        // names WHY (e.g. "awaiting_inbound=[a, b]" — the lower-id-dials
        // rule made the dials structurally someone else's) instead of
        // going silent after the coordinator's "received peer list".
        tracing::info!(
            listed = summary.listed,
            spawned = summary.spawned,
            already_connected = summary.already_connected,
            awaiting_inbound = ?summary.awaiting_inbound,
            "peer-dial sweep: spawned dial tasks for the listed peers \
             (awaiting_inbound peers have LOWER ids and dial us — this \
             node never dials them)"
        );
        if !summary.dropped_from_list.is_empty() {
            // Membership shrink is operator-significant: every dropped
            // peer's redial tracking stops NOW, silently, forever — if
            // the new list is wrong (e.g. a freshly promoted primary
            // broadcast a partial roster), this is the only trace.
            tracing::warn!(
                dropped = ?summary.dropped_from_list,
                "authoritative peer list no longer contains previously \
                 tracked peers; their cached dial info and redial \
                 tracking stop here"
            );
        }
    }

    /// The sweep itself, returning the per-disposition summary the
    /// public wrapper logs (and the unit tests pin). Kept separate so
    /// the narration has exactly one emission point.
    fn connect_to_peers_inner(&mut self, peers: &[PeerConnectionInfo]) -> DialSweepSummary {
        // Cache dial info wholesale: peers dropped from this
        // authoritative list have their cached entry removed so
        // subsequent redial signals against retired peers find no
        // dial info and no-op (loudly, per `spawn_redial`'s skip
        // narration). The primary's `PeerInfo` broadcast is the
        // source of truth — chunked / additive callers would observe
        // peers leaking after a membership change, so we replace
        // rather than extend. Dropped peers are surfaced on the
        // summary so the replacement is never a silent shrink.
        let mut summary = DialSweepSummary {
            dropped_from_list: self
                .peer_dial_info
                .keys()
                .filter(|known| !peers.iter().any(|p| p.secondary_id == known.as_str()))
                .cloned()
                .collect(),
            ..Default::default()
        };
        summary.dropped_from_list.sort();
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
            summary.listed += 1;

            if self.connections.contains_key(&peer_info.secondary_id) {
                summary.already_connected += 1;
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
            if !self.dials_outbound_to(&peer_info.secondary_id) {
                tracing::debug!(
                    self_id = %self.peer_id,
                    peer = %peer_info.secondary_id,
                    "skipping dial: peer has lower id, expecting them to initiate"
                );
                summary
                    .awaiting_inbound
                    .push(peer_info.secondary_id.clone());
                continue;
            }

            summary.spawned += 1;
            self.spawn_dial_task(peer_info.clone(), dial::DialAttempt::Initial);
        }
        summary.awaiting_inbound.sort();
        summary
    }

    /// THE lower-id-dials predicate — the single owner of the dial-side
    /// ownership rule: this node dials `peer_id` iff our id sorts
    /// lexicographically LOWER than the peer's. The higher-id side
    /// NEVER dials; its mesh leg to the peer exists only if the peer's
    /// inbound dial lands on our accept loop. Consulted by the initial
    /// sweep (`connect_to_peers`), the redial path (`spawn_redial`),
    /// and the reconnect-summary narration (`process_reconnect_tick`),
    /// so all three agree by construction.
    fn dials_outbound_to(&self, peer_id: &str) -> bool {
        self.peer_id.as_str() < peer_id
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
        let outcome = self.reconnect_tracker.tick();

        // Address-carrying dial-failure summary: the tracker owns the
        // count-throttle (which peers crossed a summary boundary this
        // tick); THIS edge owns the dialed address, resolved from the
        // authoritative `peer_dial_info`. Emitting here — not in the
        // tracker — keeps the timing tracker free of any dial-address
        // knowledge. The WARN surfaces the address an operator must
        // sanity-check (a container-internal / bridge addr that no peer
        // can route to is the canonical mesh-never-forms cause).
        for summary in outcome.dial_summaries {
            let dialed = self
                .peer_dial_info
                .get(&summary.peer_id)
                .map(|info| dial::format_dial_targets(&dial::candidate_addrs(info)))
                // A summary for a peer dropped from the authoritative
                // list between tick start and here is vanishingly rare
                // (membership churn) and not worth suppressing the WARN
                // over — the count alone is still operator-useful.
                .unwrap_or_else(|| "<unknown>".to_string());
            // Dial-side truthfulness (#362): on the higher-id side this
            // node NEVER dials (lower-id-dials rule), so "dialing
            // address" would be a lie that sends the operator chasing
            // local dial failures that do not exist. Name the real
            // dependency instead: the missing leg can only come from
            // the PEER's inbound dial — its logs hold the failure.
            if self.dials_outbound_to(&summary.peer_id) {
                tracing::warn!(
                    peer = %summary.peer_id,
                    addr = %dialed,
                    consecutive_failed_dials = summary.attempts,
                    "peer unreachable; dialing address — verify it is peer-routable"
                );
            } else {
                tracing::warn!(
                    peer = %summary.peer_id,
                    peer_advertised_addr = %dialed,
                    ticks_disconnected = summary.attempts,
                    "peer leg missing; this node NEVER dials it (lower-id-dials \
                     rule — the peer's id sorts lower, so IT dials US): check \
                     THAT peer's logs for dial failures toward this node and \
                     verify this node's advertised address is peer-routable"
                );
            }
        }

        for peer_id in outcome.to_dial {
            self.spawn_redial(&peer_id);
        }
    }

    /// Background-dial `peer_id` if we have its cached connection
    /// info AND we own the dial-side of the lower-id-dials rule.
    /// Called whenever the [`Router`] emits a redial signal — i.e. on
    /// the first observation of an active relay relationship with
    /// `peer_id` (or first re-observation past `REDIAL_COOLDOWN`) —
    /// and on every reconnect tick for a tracked-disconnected peer.
    ///
    /// Attempt narration rides the spawned dial itself (`dial_peer`
    /// logs start + outcome with the redial's attempt count); the
    /// skip branches log at DEBUG (they fire every 5s tick for a
    /// tracked peer, so operator-level visibility for the skip facts
    /// belongs to the throttled summary in `process_reconnect_tick`).
    /// A heal is still narrated where it always was: the tracker's
    /// `peer reconnected` INFO + the registration INFO.
    fn spawn_redial(&self, peer_id: &str) {
        if self.connections.contains_key(peer_id) {
            // already directly connected (mid-heal: prior dial landed
            // but the cooldown timestamp is still hot). Skipping avoids
            // a duplicate WSS pipe whose sender would later be
            // discarded by drain_new_connections.
            tracing::debug!(peer = %peer_id, "redial skipped: already connected");
            return;
        }
        let Some(peer_info) = self.peer_dial_info.get(peer_id).cloned() else {
            // peer no longer in the authoritative cluster list
            tracing::debug!(
                peer = %peer_id,
                "redial skipped: peer not in the authoritative dial list"
            );
            return;
        };
        if !self.dials_outbound_to(peer_id) {
            // higher-id side: lower-id-dials rule, accept loop handles
            // inbound. The throttled `peer leg missing` summary names
            // this structural fact at WARN level.
            tracing::debug!(
                peer = %peer_id,
                "redial skipped: lower-id-dials rule (the peer dials us)"
            );
            return;
        }
        let attempt = dial::DialAttempt::Redial {
            attempt: self.reconnect_tracker.attempts_for(peer_id).unwrap_or(0),
        };
        self.spawn_dial_task(peer_info, attempt);
    }

    /// Spawn a per-peer outgoing dial task: race QUIC/WSS via
    /// happy-eyeballs, hand off the resulting connection through
    /// `new_conn_tx` so the caller's next `drain_new_connections`
    /// picks it up. Single dispatch shape used by both the initial-
    /// dial path (`connect_to_peers`) and the redial path
    /// (`spawn_redial`).
    ///
    /// No-silent-vanish contract (#362): the spawned task's
    /// `JoinHandle` is detached, so a panicked or cancelled dial task
    /// would otherwise produce NO outcome line at all — neither
    /// success nor failure — and the dial would just vanish (the
    /// "spawned but never concluded" shape). The [`DialTaskGuard`]
    /// makes that impossible-or-loud: it is defused only after
    /// `dial_peer` returned (which itself logs every outcome), so a
    /// task dropped mid-dial (LocalSet teardown) or unwound by a
    /// panic emits the guard's WARN from its `Drop`. A success whose
    /// registration channel is already closed is likewise named, not
    /// swallowed.
    fn spawn_dial_task(&self, peer_info: PeerConnectionInfo, attempt: dial::DialAttempt) {
        let incoming_tx = self.incoming_tx.clone();
        let new_conn_tx = self.new_conn_tx.clone();
        let disconnect_tx = self.disconnect_tx.clone();
        tokio::task::spawn_local(async move {
            let peer_id = peer_info.secondary_id.clone();
            let mut guard = DialTaskGuard::new(peer_id.clone(), attempt);
            let outcome = dial::dial_peer(&peer_id, &peer_info, attempt).await;
            // `dial_peer` has logged the outcome (success or failure with
            // reasons) — the dial CONCLUDED, so the abort guard stands
            // down regardless of which it was.
            guard.defuse();
            let Some(connection) = outcome else {
                return;
            };
            let outgoing_tx = handler::spawn_outgoing_handler(
                peer_id.clone(),
                connection,
                incoming_tx,
                disconnect_tx,
            );
            if new_conn_tx
                .send(AcceptedPeer {
                    peer_id: peer_id.clone(),
                    outgoing_tx,
                })
                .is_err()
            {
                // Registration sink closed (network tearing down): the
                // freshly dialed connection is dropped on the floor.
                // Name it — a silently discarded SUCCESS is the most
                // confusing flavor of silent branch.
                tracing::warn!(
                    peer = %peer_id,
                    "dialed peer connection established but the registration \
                     channel is closed (network tearing down); dropping it"
                );
            }
        });
    }

    /// Single prune+redial disposition for a detected disconnect,
    /// shared by the AUTHORITATIVE reader/writer-exit detector
    /// (`disconnect_rx` arm in `recv_peer`) and the send-failure
    /// fallback (`broadcast` in `transport_impl.rs`). Both observe the
    /// SAME dead `outgoing_tx`, so both route through here — no
    /// duplicated disposition logic, no second redial path.
    ///
    /// Generation check: the entry is removed only if
    /// `connections[peer_id]` is STILL the dead channel
    /// (`same_channel`). A redial that already replaced the entry owns
    /// a fresh channel, so a late/duplicate disconnect signal for the
    /// stale connection is a no-op and cannot delete the live one.
    ///
    /// On a genuine prune, engage the reconnect tracker; the first
    /// observation kicks an immediate redial (then the 5s ticker takes
    /// over), matching the existing send-failure contract.
    pub(super) fn handle_peer_disconnect(
        &mut self,
        peer_id: &str,
        dead_tx: &mpsc::UnboundedSender<DistributedMessage<I>>,
    ) {
        match self.connections.get(peer_id) {
            Some(current) if current.same_channel(dead_tx) => {}
            // Either already pruned, or a redial replaced the entry with
            // a fresh channel — leave the live connection untouched.
            _ => return,
        }
        self.connections.remove(peer_id);
        let first_observation = self.reconnect_tracker.observe_disconnect(peer_id);
        if first_observation {
            self.spawn_redial(peer_id);
        }
    }
}
