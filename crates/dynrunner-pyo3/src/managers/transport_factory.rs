//! Transport-backend factory — the ONE place in the pyo3 managers
//! that names a transport backend.
//!
//! # Single concern: backend opacity
//!
//! The mesh invariant (`one-mesh`, invariant 1) is that the transport
//! backend — SSH-tunnelled WSS / QUIC / in-process channel — is OPAQUE
//! below the mesh: managers and coordinators only ever hold a
//! `Tr: PeerTransport` and address peers by id. Before this module the
//! four pyo3 manager `run.rs` constructors each named a concrete
//! backend (`NetworkServer`, `NetworkClient`, `PeerNetwork`,
//! `EitherPeerTransport`, `NoPeerTransport`, `TunneledPeerTransport`,
//! `ChannelPeerTransport`) and read
//! backend-only scalars off it (`.port()`, `.cert_pem()`), so the
//! backend choice leaked across the manager boundary.
//!
//! This factory closes that leak. Per run-mode it constructs the
//! backend, performs the backend-specific bootstrap (bind / dial+retry
//! / peer-overlay selection / bootstrap-wire fold), and hands the
//! manager back an **opaque** `impl PeerTransport` plus the
//! backend-derived values the manager still needs (the respawn trust
//! anchor, the `PeerCertInfo` a secondary/observer ships in its
//! `CertExchange`, and the optional mesh-send capability the manager
//! borrows). No caller of this module names a backend.
//!
//! # What is NOT this module's concern
//!
//! The mesh-level COMPOSITION — the channel-fold's channel mesh, the
//! `register_primary_link` bootstrap-wire fold, the
//! `set_bootstrap_primary_id` egress hint — is the secondary/distributed
//! coordinator's concern and stays in `run.rs`. This factory only owns
//! BACKEND SELECTION/NAMING: it builds the backend objects and exposes
//! the seams (bind, dial, fold, mesh-send) the composition wires, never
//! naming a backend type at any caller.

use dynrunner_core::{Identifier, SETUP_NODE_ID};
use dynrunner_manager_distributed::PeerCertInfo;
use dynrunner_transport_quic::{
    EitherPeerTransport, MeshSendHandle, NetworkClient, NetworkServer, NoPeerTransport, PeerNetwork,
};
use dynrunner_transport_tunnel::{InboundTap, SharedOutgoing, TunneledPeerTransport};

/// Opaque guard holding the bound mesh listener alive.
///
/// Its accept loops were `spawn_local`-ed inside the bind and must
/// outlive the run; the manager binds this to a `let _guard` in the
/// `LocalSet` scope so the listeners stay up, never naming the backend
/// listener type.
pub(crate) struct MeshListenerGuard(
    // Held purely for its liveness: the server keeps the bound
    // QUIC/WSS listeners (and the `spawn_local`-ed accept loops that
    // feed the transport) alive until this guard is dropped at the end
    // of the run. Never read after construction.
    #[allow(dead_code)] NetworkServer,
);

/// The bootstrap submitter primary's mesh-join transport plus the
/// backend-derived values the manager threads onward.
pub(crate) struct PrimaryMeshBundle<I: Identifier> {
    /// The opaque mesh transport the `PrimaryCoordinator` holds by value.
    pub transport: TunneledPeerTransport<I>,
    /// The primary's listen endpoint (`127.0.0.1:<port>`) — the respawn
    /// trust anchor threaded through `enable_respawn`.
    pub respawn_endpoint: String,
    /// The primary's public cert PEM — the matching trust anchor.
    pub respawn_pubkey_pem: String,
    /// Held by the manager to keep the accept loops alive for the run.
    pub listener_guard: MeshListenerGuard,
}

/// Bind the submitter primary's mesh-join transport on `127.0.0.1:port`.
///
/// Builds the `TunneledPeerTransport` first (it OWNS the inbound demux),
/// then binds the `NetworkServer` wiring its accept loops to the
/// transport's inbound + registration sinks. Returns the opaque
/// transport, the respawn trust anchor read off the bound server, and
/// the live server (held in the bundle).
///
/// Must run inside the coordinator's `LocalSet` (the server's accept
/// loops are `spawn_local`-ed).
pub(crate) async fn bind_primary_mesh<I: Identifier>(
    port: u16,
) -> Result<PrimaryMeshBundle<I>, String> {
    // The transport OWNS the real inbound demux (the relocated
    // `NetworkServer::recv` `select!`); the accept loops feed it via the
    // inbound + registration sinks. `shared_outgoing` is unused on this
    // path (the accept loops register each secondary's writer via the
    // registration sink, not a direct insert).
    let (transport, _shared_outgoing, inbound, registration) =
        TunneledPeerTransport::<I>::new(SETUP_NODE_ID.into());

    let bind_addr: std::net::SocketAddr = format!("127.0.0.1:{}", port)
        .parse()
        .map_err(|e| format!("invalid bind addr: {e}"))?;
    // The submitter mesh's QUIC cert names the submitter host (the same
    // id its TunneledPeerTransport registered above + secondaries dial it
    // by), so a QUIC-dialing secondary's `connect(addr, SETUP_NODE_ID)`
    // certificate-name check passes.
    let server =
        NetworkServer::bind::<I>(bind_addr, SETUP_NODE_ID, inbound, registration).await?;

    // The primary's listen endpoint + cert PEM are the trust anchors
    // threaded through `enable_respawn`. Endpoint format mirrors the
    // QUIC-listen surface (`127.0.0.1:<port>`).
    let respawn_endpoint = format!("127.0.0.1:{}", server.port());
    let respawn_pubkey_pem = server.cert_pem().to_string();

    Ok(PrimaryMeshBundle {
        transport,
        respawn_endpoint,
        respawn_pubkey_pem,
        listener_guard: MeshListenerGuard(server),
    })
}

/// A secondary's mesh transport plus the backend-derived values the
/// composition in `run.rs` threads onward.
pub(crate) struct SecondaryMeshBundle<I: Identifier> {
    /// The opaque mesh transport the `SecondaryCoordinator` holds by
    /// value, with the bootstrap primary wire already folded in.
    pub transport: EitherPeerTransport<I>,
    /// The `PeerCertInfo` this secondary ships in its `CertExchange`,
    /// built from the backend's cert PEM + QUIC port (so the manager
    /// never reads `.cert_pem()` / `.port()` itself).
    pub peer_cert_info: PeerCertInfo,
    /// Cloneable mesh-send capability — `Some` only when a REAL peer
    /// mesh exists (a `Disabled` overlay has no remote secondaries and
    /// thus no failover). The manager reads `is_some()` as the
    /// primary-capability marker.
    pub mesh_send: Option<MeshSendHandle<I>>,
}

/// Inputs to [`dial_secondary_mesh`] — the secondary's mesh-dial
/// parameters grouped so the backend-construction signature stays tight.
pub(crate) struct SecondaryDialParams<'a> {
    /// The resolved bootstrap-primary `SocketAddr` to dial.
    pub addr: std::net::SocketAddr,
    /// Overall dial budget; the retry loop gives up past this.
    pub connect_timeout: std::time::Duration,
    /// Delay between dial attempts.
    pub retry_delay: std::time::Duration,
    /// Select the firewalled (no inter-compute dialing) fabric.
    pub disable_peer_overlay: bool,
    /// This secondary's logical id (the peer-mesh cert CN).
    pub secondary_id: &'a str,
    /// Peer-id the folded bootstrap wire is keyed under (the
    /// conventional `"primary"`).
    pub bootstrap_primary_id: String,
    /// Detected IPv4 address advertised in the `PeerCertInfo`.
    pub ipv4_address: Option<String>,
    /// Detected IPv6 address advertised in the `PeerCertInfo`.
    pub ipv6_address: Option<String>,
}

/// Dial the bootstrap primary and build the secondary's mesh transport.
///
/// Encapsulates every backend-naming step on the secondary path: the
/// WSS dial + retry loop, the peer-overlay selection (real `PeerNetwork`
/// vs the firewalled `NoPeerTransport`), reading the backend's cert /
/// port into a `PeerCertInfo`, extracting the mesh-send capability, and
/// folding the dialed bootstrap wire into the mesh under the primary's
/// peer-id (`register_primary_link`).
///
/// Must run inside the coordinator's `LocalSet`.
pub(crate) async fn dial_secondary_mesh<I: Identifier>(
    params: SecondaryDialParams<'_>,
) -> Result<SecondaryMeshBundle<I>, String> {
    let SecondaryDialParams {
        addr,
        connect_timeout,
        retry_delay,
        disable_peer_overlay,
        secondary_id,
        bootstrap_primary_id,
        ipv4_address,
        ipv6_address,
    } = params;
    // Connect to primary via WSS, retrying until the configured timeout.
    let start = std::time::Instant::now();
    let mut attempt = 0u32;
    let client = loop {
        attempt += 1;
        let elapsed = start.elapsed();
        if elapsed > connect_timeout {
            tracing::error!(
                addr = %addr,
                attempts = attempt,
                "failed to connect to primary after {:.0}s",
                connect_timeout.as_secs_f64()
            );
            return Err(format!(
                "failed to connect to primary at {addr} after {:.0}s ({attempt} attempts)",
                connect_timeout.as_secs_f64()
            ));
        }
        match NetworkClient::connect_wss_only(addr).await {
            Ok(c) => {
                tracing::info!(
                    addr = %addr,
                    elapsed_s = elapsed.as_secs_f64(),
                    attempts = attempt,
                    "connected to primary"
                );
                break c;
            }
            Err(e) => {
                let remaining = connect_timeout.saturating_sub(elapsed);
                if remaining > retry_delay {
                    tracing::info!(
                        attempt,
                        error = %e,
                        "connection failed, retrying in {:.0}s...",
                        retry_delay.as_secs_f64()
                    );
                    tokio::time::sleep(retry_delay).await;
                } else {
                    tracing::error!(addr = %addr, error = %e, "failed to connect to primary");
                    return Err(format!("failed to connect to primary at {addr}: {e}"));
                }
            }
        }
    };

    // Pick the peer transport at runtime: real `PeerNetwork` for normal
    // clusters, `NoPeerTransport` for clusters that firewall
    // inter-compute-node networking (LMU SLURM, etc.) where every peer
    // dial would time out anyway. The identity passed to
    // `PeerNetwork::start` is BOTH the CN baked into this secondary's
    // QUIC certificate AND the `peer_id` other secondaries pass to
    // quinn's `connect(addr, server_name)` to validate that cert — the
    // primary distributes peer info keyed by `secondary_id` (the logical
    // id), so the cert CN must match the logical id.
    let (mut transport, cert_pem, port): (EitherPeerTransport<I>, String, u16) =
        if disable_peer_overlay {
            tracing::info!("peer overlay disabled by config; using NoPeerTransport");
            (
                EitherPeerTransport::Disabled(NoPeerTransport),
                String::new(),
                0,
            )
        } else {
            let pn = PeerNetwork::<I>::start(secondary_id).await.map_err(|e| {
                tracing::error!(error = %e, "failed to start peer network");
                format!("peer network start failed: {e}")
            })?;
            let cert_pem = pn.cert_pem().to_string();
            let port = pn.port();
            (EitherPeerTransport::Real(Box::new(pn)), cert_pem, port)
        };

    // Cloneable mesh-send capability (`Some` only when a real peer mesh
    // exists). Taken BEFORE the bootstrap wire fold so the handle reflects
    // the live `PeerNetwork`.
    let mesh_send = transport.mesh_send_handle();

    // Fold the dialed primary bootstrap wire into the mesh as a
    // directed-routable member keyed by the primary's peer-id: BOTH the
    // outbound fan-in writer AND the wire's inbound move onto the one
    // uniform mesh path, so the primary's frames arrive via `recv_peer`
    // like every other peer's — there is no separate `uplink` leg. "The
    // tunnel is just a way of joining the mesh."
    transport.register_primary_link(bootstrap_primary_id, client);

    let peer_cert_info = PeerCertInfo {
        public_cert_pem: cert_pem,
        ipv4_address,
        ipv6_address,
        quic_port: port,
    };

    Ok(SecondaryMeshBundle {
        transport,
        peer_cert_info,
        mesh_send,
    })
}

/// Stand up the observer late-joiner's peer transport.
///
/// The CN baked into the cert MUST match `observer_id` because every
/// dialing peer validates the SAN against the logical id. The bootstrap
/// rendezvous (`join_running_cluster`) runs on the returned transport in
/// `run.rs` — it is a `PeerTransport` trait method and names no backend.
///
/// Returns the bare `PeerNetwork`: the standalone, apply-only
/// `ObserverCoordinator` holds the mesh transport directly and ships no
/// `PeerCertInfo` (it ignores cert-exchange frames), so the
/// secondary-style cert bundle the late-joiner used to build is gone.
///
/// Must run inside the coordinator's `LocalSet`.
pub(crate) async fn observer_mesh<I: Identifier>(
    observer_id: &str,
) -> Result<PeerNetwork<I>, String> {
    PeerNetwork::<I>::start(observer_id)
        .await
        .map_err(|e| format!("failed to start peer network: {e}"))
}

/// The in-process distributed manager's primary mesh transport plus the
/// channel sinks the per-secondary forwarders wire into.
pub(crate) struct InProcessPrimaryBundle<I: Identifier> {
    /// The opaque mesh transport the in-process `PrimaryCoordinator`
    /// holds by value.
    pub transport: TunneledPeerTransport<I>,
    /// Writer table: the per-secondary `pri_to_sec_tx` is inserted here
    /// so `send_to_peer(sec_id, ..)` reaches each secondary (no accept
    /// loops in-process — the direct insert IS the registration).
    pub shared_outgoing: SharedOutgoing<I>,
    /// The transport's single inbound sink: each per-secondary forwarder
    /// task feeds `sec_to_pri` frames into it (the in-process analogue of
    /// a QUIC/WSS accept loop's reader task).
    pub inbound: InboundTap<I>,
}

/// Build the in-process distributed manager's primary mesh transport.
///
/// Post-collapse this is the ONE transport the in-process primary holds;
/// the manager wires the per-secondary channel writers into
/// `shared_outgoing` and feeds `sec_to_pri` frames into `inbound`.
pub(crate) fn inprocess_primary_mesh<I: Identifier>() -> InProcessPrimaryBundle<I> {
    let (transport, shared_outgoing, inbound, _registration) =
        TunneledPeerTransport::<I>::new(SETUP_NODE_ID.into());
    InProcessPrimaryBundle {
        transport,
        shared_outgoing,
        inbound,
    }
}

/// Build an in-process distributed-manager secondary's channel-backed
/// mesh transport with the primary folded in as an ordinary mesh peer.
///
/// Inbound is the primary→secondary channel; the outbound primary link
/// is the secondary→primary channel folded under `bootstrap_primary_id`
/// (`register_primary_link`) — no per-role uplink leg.
pub(crate) fn inprocess_secondary_mesh<I: Identifier>(
    secondary_id: String,
    bootstrap_primary_id: String,
    pri_to_sec_rx: tokio::sync::mpsc::UnboundedReceiver<
        dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    >,
    sec_to_pri_tx: tokio::sync::mpsc::UnboundedSender<
        dynrunner_protocol_primary_secondary::DistributedMessage<I>,
    >,
) -> dynrunner_transport_channel::ChannelPeerTransport<I> {
    let mut transport = dynrunner_transport_channel::ChannelPeerTransport::<I>::from_raw_channels(
        secondary_id,
        std::collections::HashMap::new(),
        pri_to_sec_rx,
    );
    transport.register_primary_link(bootstrap_primary_id, sec_to_pri_tx);
    transport
}
