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
//! `TunneledPeerTransport`,
//! `ChannelPeerTransport`) and read
//! backend-only scalars off it (`.port()`, `.cert_pem()`), so the
//! backend choice leaked across the manager boundary.
//!
//! This factory closes that leak. Per run-mode it constructs the
//! backend, performs the backend-specific bootstrap (bind / dial+retry
//! / bootstrap-wire fold), and hands the
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
    MeshSendHandle, NetworkClient, NetworkServer, PeerNetwork,
};
use dynrunner_transport_tunnel::TunneledPeerTransport;

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
    pub transport: PeerNetwork<I>,
    /// The `PeerCertInfo` this secondary ships in its `CertExchange`,
    /// built from the backend's cert PEM + QUIC port (so the manager
    /// never reads `.cert_pem()` / `.port()` itself).
    pub peer_cert_info: PeerCertInfo,
    /// Cloneable mesh-send capability over the secondary's peer mesh.
    /// The manager reads `is_some()` as the primary-capability marker.
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
/// WSS dial + retry loop, starting the real `PeerNetwork`, reading the
/// backend's cert / port into a `PeerCertInfo`, extracting the mesh-send
/// capability, and folding the dialed bootstrap wire into the mesh under
/// the primary's peer-id (`register_primary_link`).
///
/// Must run inside the coordinator's `LocalSet`.
pub(crate) async fn dial_secondary_mesh<I: Identifier>(
    params: SecondaryDialParams<'_>,
) -> Result<SecondaryMeshBundle<I>, String> {
    let SecondaryDialParams {
        addr,
        connect_timeout,
        retry_delay,
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

    // Start the secondary's peer mesh. The identity passed to
    // `PeerNetwork::start` is BOTH the CN baked into this secondary's
    // QUIC certificate AND the `peer_id` other secondaries pass to
    // quinn's `connect(addr, server_name)` to validate that cert — the
    // primary distributes peer info keyed by `secondary_id` (the logical
    // id), so the cert CN must match the logical id.
    let mut transport = PeerNetwork::<I>::start(secondary_id).await.map_err(|e| {
        tracing::error!(error = %e, "failed to start peer network");
        format!("peer network start failed: {e}")
    })?;
    let cert_pem = transport.cert_pem().to_string();
    let port = transport.port();

    // Cloneable mesh-send capability. Taken BEFORE the bootstrap wire fold
    // so the handle reflects the live `PeerNetwork`.
    let mesh_send = Some(transport.mesh_send_handle());

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
// The in-process `--multi-computer local` manager no longer constructs its
// mesh here: it builds the full N+1-node all-to-all mpsc mesh directly via the
// EXISTING `dynrunner_transport_channel::peer_mesh` primitive (every node — the
// setup peer + each secondary — is a first-class `ChannelPeerTransport`
// member). The old STAR builders (a `TunneledPeerTransport` primary + per-
// secondary `from_raw_channels` legs folded only the primary in) are gone:
// under mesh-always the setup peer relocates onto a secondary, so every node
// needs all-to-all reach, which the STAR could not provide.
