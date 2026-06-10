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
    /// Initial delay between dial attempts; doubled per refused attempt,
    /// capped at [`MAX_DIAL_RETRY_DELAY`].
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

/// Cap on the bring-up dial's backoff delay. The delay starts at the
/// configured `retry_delay` (default 1s) and doubles per transient
/// failure up to this cap, so a slow tunnel keeps a visible WARN
/// cadence without hammering the port.
const MAX_DIAL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Classify a failed bring-up dial attempt: `true` iff the error is
/// connection-refused/reset-class — the canonical "the submitter's `-R`
/// reverse tunnel is still establishing (or dropped mid-handshake)"
/// race, which is worth retrying until the deadline. Auth/protocol/
/// handshake errors return `false` and fail fast: retrying cannot fix
/// them and would bury the real cause for the whole dial budget.
///
/// Matches BOTH the kernel error text and the raw errno because the
/// backend stringifies `tungstenite` errors (production shape:
/// `"IO error: Connection refused (os error 111)"`) and the text half
/// is locale/wording-sensitive across std versions.
fn is_transient_dial_error(error: &str) -> bool {
    const TRANSIENT_NEEDLES: [&str; 6] = [
        "Connection refused",
        "os error 111", // ECONNREFUSED
        "Connection reset",
        "os error 104", // ECONNRESET
        "Connection aborted",
        "os error 103", // ECONNABORTED
    ];
    TRANSIENT_NEEDLES.iter().any(|n| error.contains(n))
}

/// Drive one bring-up dial to success or a hard verdict: retry
/// transient (refused/reset-class) failures with capped exponential
/// backoff until `deadline`, fail fast on anything else, and — on
/// deadline exhaustion — fail with the full attempt history plus the
/// last underlying error (so a never-listening tunnel is
/// distinguishable from a first-attempt death; the production
/// fire-drill of run_20260610_130030 was misdiagnosed exactly because
/// the old exhaustion message carried neither).
///
/// Every attempt is additionally hard-bounded by the time remaining to
/// the deadline (`connect_wss` itself has no handshake timeout, so a
/// stalled accept could otherwise outlive the budget).
///
/// Generic over the dial future so the retry policy is unit-testable
/// without a live backend; [`dial_secondary_mesh`] passes the real
/// `NetworkClient::connect_wss_only`.
async fn dial_until_deadline<T, F, Fut>(
    addr: std::net::SocketAddr,
    deadline: std::time::Duration,
    initial_delay: std::time::Duration,
    mut dial: F,
) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let start = std::time::Instant::now();
    // An operator-raised fixed `retry_delay` above the cap is honoured
    // (the cap only bounds the doubling, never lowers the configured
    // floor).
    let max_delay = MAX_DIAL_RETRY_DELAY.max(initial_delay);
    let mut delay = initial_delay;
    let mut attempts = 0u32;
    // The most recent transient failure, kept so deadline exhaustion
    // reports the real underlying error (e.g. the refused dial), not a
    // synthetic timeout shell.
    let mut last_error: Option<String> = None;
    loop {
        let budget = deadline.saturating_sub(start.elapsed());
        // (A zero/expired budget BEFORE any attempt — `last_error` still
        // `None` — falls through and gives the dial exactly one
        // zero-budget chance, so a degenerate deadline still reports a
        // real outcome.)
        if budget.is_zero()
            && let Some(error) = last_error
        {
            tracing::error!(
                addr = %addr,
                attempts,
                elapsed_s = start.elapsed().as_secs_f64(),
                last_error = %error,
                "failed to connect to primary: retried every refused dial until the \
                 deadline; the primary tunnel never started listening"
            );
            return Err(format!(
                "failed to connect to primary at {addr} after {:.0}s ({attempts} attempts; \
                 every connection-refused/reset failure was retried until the deadline — \
                 the primary's reverse tunnel never started listening): last error: {error}",
                start.elapsed().as_secs_f64()
            ));
        }
        attempts += 1;
        let error = match tokio::time::timeout(budget, dial()).await {
            Ok(Ok(connected)) => {
                tracing::info!(
                    addr = %addr,
                    elapsed_s = start.elapsed().as_secs_f64(),
                    attempts,
                    "connected to primary"
                );
                return Ok(connected);
            }
            Ok(Err(e)) => e,
            // The attempt itself consumed the rest of the budget (a
            // stalled TCP connect / WS handshake): deadline exhaustion,
            // verbatim — never routed through the transient classifier.
            Err(_) => {
                let history = match &last_error {
                    Some(e) => format!("; last completed attempt's error: {e}"),
                    None => String::new(),
                };
                tracing::error!(
                    addr = %addr,
                    attempts,
                    elapsed_s = start.elapsed().as_secs_f64(),
                    "failed to connect to primary: final dial attempt still pending at the deadline"
                );
                return Err(format!(
                    "failed to connect to primary at {addr} after {:.0}s ({attempts} attempts; \
                     the final dial attempt was still pending when the deadline expired{history})",
                    start.elapsed().as_secs_f64()
                ));
            }
        };
        if !is_transient_dial_error(&error) {
            tracing::error!(
                addr = %addr,
                attempts,
                error = %error,
                "failed to connect to primary (non-transient error; not retrying)"
            );
            return Err(format!(
                "failed to connect to primary at {addr}: {error} \
                 (non-transient error on attempt {attempts}; not retrying)"
            ));
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        let pause = delay.min(remaining);
        tracing::warn!(
            addr = %addr,
            attempt = attempts,
            error = %error,
            retry_in_s = pause.as_secs_f64(),
            deadline_remaining_s = remaining.as_secs_f64(),
            "primary tunnel not listening yet — the submitter's reverse tunnel may still \
             be establishing; retrying until the deadline"
        );
        last_error = Some(error);
        tokio::time::sleep(pause).await;
        delay = (delay * 2).min(max_delay);
    }
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
    // Connect to primary via WSS, retrying transient failures until the
    // configured deadline (`connect_timeout`, default 600s — the same
    // window as the submitter's secondary-welcome wait, so both sides
    // give up together).
    let client = dial_until_deadline(addr, connect_timeout, retry_delay, || {
        NetworkClient::connect_wss_only(addr)
    })
    .await?;

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
    //
    // `addr` is retained (it is `Copy`) so the wire is RE-dialable: when
    // the `-R` tunnel drops, the bootstrap-redial supervisor re-dials this
    // same `localhost:<tunnel_port>` address indefinitely and re-folds the
    // fresh client (defect (b)). It is the FIXED tunnel address — never a
    // LAN addr — because the submitter is unroutable except through the
    // tunnel.
    transport.register_primary_link(bootstrap_primary_id, addr, client);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifier::RunnerIdentifier;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    /// The EXACT error shape production emitted in run_20260610_130030
    /// (tungstenite `Error::Io` Display through `connect_wss`'s
    /// `e.to_string()`); the retry tests drive the policy with it so the
    /// classifier is exercised on the real wire shape, not a synthetic one.
    const PRODUCTION_REFUSED: &str = "IO error: Connection refused (os error 111)";

    fn test_addr() -> std::net::SocketAddr {
        "127.0.0.1:1".parse().unwrap()
    }

    /// Refused/reset-class strings (text AND errno halves) are transient;
    /// auth/protocol/handshake strings are not.
    #[test]
    fn classifier_separates_refused_class_from_protocol_class() {
        assert!(is_transient_dial_error(PRODUCTION_REFUSED));
        assert!(is_transient_dial_error(
            "IO error: Connection reset by peer (os error 104)"
        ));
        assert!(is_transient_dial_error(
            "IO error: Connection aborted (os error 103)"
        ));
        assert!(!is_transient_dial_error(
            "WebSocket protocol error: Handshake not finished"
        ));
        assert!(!is_transient_dial_error("HTTP error: 401 Unauthorized"));
        assert!(!is_transient_dial_error("IO error: unexpected end of file"));
    }

    /// A dial refused N times then accepted (the tunnel finishing its
    /// concurrent establishment) connects instead of dying — the core
    /// retry-works guarantee.
    #[tokio::test(start_paused = true)]
    async fn retries_refused_until_listener_appears() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = calls.clone();
        let result: Result<u32, String> = dial_until_deadline(
            test_addr(),
            Duration::from_secs(600),
            Duration::from_secs(1),
            move || {
                let counter = counter.clone();
                async move {
                    let n = counter.get() + 1;
                    counter.set(n);
                    if n <= 3 {
                        Err(PRODUCTION_REFUSED.to_string())
                    } else {
                        Ok(7)
                    }
                }
            },
        )
        .await;
        assert_eq!(result, Ok(7));
        assert_eq!(calls.get(), 4, "three refused attempts then success");
    }

    /// Deadline exhaustion fails with the attempt history AND the last
    /// underlying error — the production fire-drill was misdiagnosed as a
    /// first-dial death precisely because the old message carried neither.
    #[tokio::test(start_paused = true)]
    async fn deadline_exhaustion_reports_attempt_history() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = calls.clone();
        let result: Result<u32, String> = dial_until_deadline(
            test_addr(),
            Duration::from_secs(10),
            Duration::from_secs(1),
            move || {
                let counter = counter.clone();
                async move {
                    counter.set(counter.get() + 1);
                    Err(PRODUCTION_REFUSED.to_string())
                }
            },
        )
        .await;
        let err = result.unwrap_err();
        assert!(
            calls.get() > 1,
            "must have retried before the deadline (attempts: {})",
            calls.get()
        );
        assert!(
            err.contains(&format!("({} attempts", calls.get())),
            "error must carry the attempt count: {err}"
        );
        assert!(
            err.contains("last error: IO error: Connection refused (os error 111)"),
            "error must carry the last underlying error: {err}"
        );
        assert!(
            err.contains("127.0.0.1:1") && err.contains("after 10s"),
            "error must carry the target and the elapsed budget: {err}"
        );
    }

    /// A non-refused-class error (auth/protocol) fails fast with the
    /// original error — retrying cannot fix it and would bury the cause
    /// for the whole dial budget.
    #[tokio::test]
    async fn non_transient_error_fails_fast() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = calls.clone();
        let result: Result<u32, String> = dial_until_deadline(
            test_addr(),
            Duration::from_secs(600),
            Duration::from_secs(1),
            move || {
                let counter = counter.clone();
                async move {
                    counter.set(counter.get() + 1);
                    Err("WebSocket protocol error: Handshake not finished".to_string())
                }
            },
        )
        .await;
        let err = result.unwrap_err();
        assert_eq!(calls.get(), 1, "must not retry a non-transient error");
        assert!(
            err.contains("WebSocket protocol error: Handshake not finished")
                && err.contains("not retrying"),
            "fail-fast error must carry the original error verbatim: {err}"
        );
    }

    /// Wire-shape mirror: a REAL refused dial through the REAL backend
    /// (`connect_wss_only` against a port nothing listens on) produces an
    /// error the classifier recognises as transient. Pins the cross-crate
    /// Display shape the string classifier depends on.
    #[tokio::test]
    async fn real_refused_dial_error_is_classified_transient() {
        // Reserve a port, then free it so nothing listens there.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let err = NetworkClient::<RunnerIdentifier>::connect_wss_only(addr)
            .await
            .err()
            .expect("dialing a port with no listener must fail");
        assert!(
            is_transient_dial_error(&err),
            "real refused error not classified transient: {err}"
        );
    }

    /// End-to-end through the real backend: a dial racing a listener that
    /// only appears after several refused attempts connects before the
    /// deadline (the production tunnel race, replayed).
    #[tokio::test]
    async fn connects_when_listener_appears_within_deadline() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = probe.local_addr().unwrap();
                drop(probe);
                // The "submitter": brings the listener up only after the
                // secondary has already started dialing.
                let server = tokio::task::spawn_local(async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let listener = dynrunner_transport_quic::WssListener::bind(addr)
                        .await
                        .expect("test listener bind");
                    let _conn = listener.accept().await.expect("test accept");
                });
                let client = dial_until_deadline(
                    addr,
                    Duration::from_secs(30),
                    Duration::from_millis(50),
                    || NetworkClient::<RunnerIdentifier>::connect_wss_only(addr),
                )
                .await;
                assert!(
                    client.is_ok(),
                    "dial must succeed once the listener appears: {:?}",
                    client.err()
                );
                server.await.expect("server task");
            })
            .await;
    }
}
