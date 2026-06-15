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
//! # Execution context: the mesh runtime
//!
//! Each constructor here runs INSIDE the construct closure of
//! `MeshHost::on_dedicated_thread` — on the dedicated mesh runtime's
//! `LocalSet` — because tokio IO resources register with the creating
//! runtime's driver and the backends `spawn_local` their accept/dial
//! tasks at construction. The transport never leaves that thread; only
//! the `Send` halves of each bundle cross back to the coordinator
//! runtime. Nothing in this module may touch Python (the mesh runtime
//! never blocks on the GIL).
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

/// The bootstrap submitter primary's mesh-join transport plus the
/// backend-derived values the manager threads onward.
pub(crate) struct PrimaryMeshBundle<I: Identifier> {
    /// The opaque mesh transport the mesh runtime's pump owns for the run.
    pub transport: TunneledPeerTransport<I>,
    /// The primary's listen endpoint (`127.0.0.1:<port>`) — the respawn
    /// trust anchor threaded through `enable_respawn`.
    pub respawn_endpoint: String,
    /// The primary's public cert PEM — the matching trust anchor.
    pub respawn_pubkey_pem: String,
}

/// Bind the submitter primary's mesh-join transport on `127.0.0.1:port`.
///
/// Builds the `TunneledPeerTransport` first (it OWNS the inbound demux),
/// then binds the `NetworkServer` wiring its accept loops to the
/// transport's inbound + registration sinks. Returns the opaque
/// transport and the respawn trust anchors read off the bound server;
/// the live server itself is PARKED on the constructing `LocalSet` (a
/// run-length holder task) so its accept loops stay up until the mesh
/// runtime is stopped — no caller holds a backend listener type.
///
/// Must run inside the mesh runtime's `LocalSet` (the construct closure
/// of `MeshHost::on_dedicated_thread`): the server's accept loops are
/// `spawn_local`-ed and its sockets register with the creating runtime's
/// IO driver.
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

    // Park the bound server for the LIFETIME of the constructing
    // runtime's `LocalSet` (the mesh runtime): the server keeps the bound
    // QUIC/WSS listeners — and the `spawn_local`-ed accept loops that feed
    // the transport — alive until the mesh runtime is stopped, which drops
    // this holder task. Same parking pattern as the liveness listener.
    tokio::task::spawn_local(async move {
        let _server = server;
        std::future::pending::<()>().await;
    });

    Ok(PrimaryMeshBundle {
        transport,
        respawn_endpoint,
        respawn_pubkey_pem,
    })
}

/// A secondary's mesh transport plus the backend-derived values the
/// composition in `run.rs` threads onward.
pub(crate) struct SecondaryMeshBundle<I: Identifier> {
    /// The opaque mesh transport the `SecondaryCoordinator` holds by
    /// value. The bootstrap primary wire folds in ASYNCHRONOUSLY — the
    /// background bring-up dial hands it through the network's fold
    /// channel whenever it lands (`bootstrap_fold_handle`).
    pub transport: PeerNetwork<I>,
    /// The `PeerCertInfo` this secondary ships in its `CertExchange`,
    /// built from the backend's cert PEM + QUIC port (so the manager
    /// never reads `.cert_pem()` / `.port()` itself).
    pub peer_cert_info: PeerCertInfo,
    /// Cloneable mesh-send capability over the secondary's peer mesh.
    /// The manager reads `is_some()` as the primary-capability marker.
    pub mesh_send: Option<MeshSendHandle<I>>,
    /// Fires `Ok(())` if the background bring-up dial exhausts its
    /// deadline without ever connecting — the tunnel never appeared.
    ///
    /// The receiver is registered on the
    /// `SecondaryCoordinator` via
    /// `register_tunnel_gave_up_rx` so `wait_for_setup` can exit early
    /// (log to `IMPORTANT_TARGET` + return a typed error) instead of
    /// squatting until the outer `unconfigured_deadline` fires. Fires at
    /// most ONCE; never fires if the dial succeeds. Dropped unfired on
    /// successful dial so the receiver side returns `Err(RecvError)` —
    /// the `wait_for_setup` arm selects on `Option` and parks on
    /// `pending()` once the option is taken.
    pub tunnel_gave_up_rx: tokio::sync::oneshot::Receiver<()>,
}

/// Inputs to [`dial_secondary_mesh`] — the secondary's mesh-dial
/// parameters grouped so the backend-construction signature stays tight.
/// Fully OWNED (no borrows) so the whole struct moves into the mesh
/// runtime's `Send + 'static` construct closure.
pub(crate) struct SecondaryDialParams {
    /// ALL resolved bootstrap-primary `SocketAddr`s (the full resolver
    /// output for the primary URL, not just its first entry). The
    /// factory derives the per-attempt candidate order — v4 first,
    /// loopback family-completed — via [`dial_candidates`]; see there
    /// for why a single pre-picked address cannot carry a partial
    /// `-R` bind.
    pub addrs: Vec<std::net::SocketAddr>,
    /// Overall dial budget; the BACKGROUND retry loop gives up past this
    /// (loud ERROR, no process exit — the coordinator's
    /// `unconfigured_deadline` owns the node's fate).
    pub connect_timeout: std::time::Duration,
    /// Initial delay between dial attempts; doubled per refused attempt,
    /// capped at [`MAX_DIAL_RETRY_DELAY`].
    pub retry_delay: std::time::Duration,
    /// This secondary's logical id (the peer-mesh cert CN).
    pub secondary_id: String,
    /// Peer-id the folded bootstrap wire is keyed under (the
    /// conventional `"primary"`).
    pub bootstrap_primary_id: String,
    /// Detected IPv4 address advertised in the `PeerCertInfo`.
    pub ipv4_address: Option<String>,
    /// Detected IPv6 address advertised in the `PeerCertInfo`.
    pub ipv6_address: Option<String>,
    /// Port this secondary's OWN mesh listeners bind (QUIC UDP + WSS
    /// TCP, same number — the `PeerNetwork::start` convention). `None`
    /// keeps the historical OS-picked ephemeral bind. The SLURM wrapper
    /// pre-allocates this port host-side, records it as `quic_port=` in
    /// the late-joiner's `connection_info/<id>.info` file, and delivers
    /// it in-container via `--secondary-quic-port` — so the bind here is
    /// what makes the recorded port actually dialable (a late-joining
    /// observer dials exactly this `ip:port`).
    pub quic_bind_port: Option<u16>,
    /// Optional sender side of the
    /// [`dynrunner_transport_quic::PeerNetwork::notify_persistent_dial_failures`]
    /// channel (#542 cause-B). The factory installs it on the freshly
    /// constructed `PeerNetwork` BEFORE the bundle returns + the network
    /// is consumed into the mesh-host, mirroring how the observer
    /// late-joiner wires its OWN dial-failure trigger (the per-leg
    /// forward-recovery for `-R` tunnels). On THIS path the rx side is
    /// parked on the secondary's
    /// [`super::secondary::new::PromotePrimaryRecipe`] and installed on
    /// the promoted primary via `set_persistent_dial_failure_rx` so the
    /// primary's operational loop's arm consumes
    /// `DIAL_SUMMARY_THRESHOLD`-crossed peer ids and originates a
    /// `PeerRemoved` for any that name a `role_table.observers` entry —
    /// the cause-B prune that ends the recurring 60s "peer unreachable"
    /// WARN. `None` for callers that don't wire the cause-B prune
    /// (channel-only fixtures, the late-joiner observer path that has
    /// its own subscriber on the same channel).
    pub dial_failure_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
}

/// Cap on the bring-up dial's backoff delay. The delay starts at the
/// configured `retry_delay` (default 1s) and doubles per transient
/// failure up to this cap, so a slow tunnel keeps a visible WARN
/// cadence without hammering the port.
const MAX_DIAL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// PURE: the per-attempt dial-candidate list from the resolver's
/// output — deduplicated, IPv4 before IPv6 (the tunnel's primary
/// family, resolver order preserved within each family), and, when
/// the target is loopback, COMPLETED with BOTH loopback families on
/// the same port.
///
/// The completion is the dial-side half of the partial-`-R`-bind
/// defense: sshd binds the remote forward per address family and
/// reports success when EITHER lands (OpenSSH
/// `channel_setup_fwd_listener_tcpip`), so a transient v4 collision
/// on the worker leaves a `[::1]`-only listener. The container's
/// `/etc/hosts` may resolve `localhost` to only one family — the
/// resolver output must never constrain the dial to the family that
/// happened to lose the bind race, and both loopback addresses always
/// exist on the host. Non-loopback targets (Standard gateway mode)
/// get ordering + dedup only — never an invented address.
fn dial_candidates(resolved: &[std::net::SocketAddr]) -> Vec<std::net::SocketAddr> {
    let mut all: Vec<std::net::SocketAddr> = resolved.to_vec();
    if let Some(port) = resolved
        .iter()
        .find(|a| a.ip().is_loopback())
        .map(|a| a.port())
    {
        all.push((std::net::Ipv4Addr::LOCALHOST, port).into());
        all.push((std::net::Ipv6Addr::LOCALHOST, port).into());
    }
    let mut out: Vec<std::net::SocketAddr> = Vec::new();
    for addr in all
        .iter()
        .filter(|a| a.is_ipv4())
        .chain(all.iter().filter(|a| a.is_ipv6()))
    {
        if !out.contains(addr) {
            out.push(*addr);
        }
    }
    out
}

/// One bring-up dial ATTEMPT across the candidate list: try each
/// address in order, first success wins — returning WHICH address
/// connected (the re-dialable bootstrap address
/// `register_primary_link` retains). When every candidate fails, the
/// combined per-candidate error string feeds
/// [`is_transient_dial_error`], whose substring match retries the
/// attempt iff ANY candidate failed refused/reset-class (that
/// family's listener may still be coming up) — a v4 squatter's
/// protocol error therefore cannot mask a v6 listener that is still
/// establishing.
async fn dial_candidates_once<T, F, Fut>(
    candidates: &[std::net::SocketAddr],
    mut dial_one: F,
) -> Result<(T, std::net::SocketAddr), String>
where
    F: FnMut(std::net::SocketAddr) -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let mut errors: Vec<String> = Vec::new();
    for &addr in candidates {
        match dial_one(addr).await {
            Ok(connected) => return Ok((connected, addr)),
            Err(e) => errors.push(format!("{addr}: {e}")),
        }
    }
    Err(errors.join("; "))
}

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
/// without a live backend; [`dial_secondary_mesh`] passes a
/// [`dial_candidates_once`] sweep over the real
/// `NetworkClient::connect_wss_only`. `target` is the display label
/// for logs/errors (the candidate list rendered once at the call
/// site).
async fn dial_until_deadline<T, F, Fut>(
    target: &str,
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
                target = %target,
                attempts,
                elapsed_s = start.elapsed().as_secs_f64(),
                last_error = %error,
                "failed to connect to primary: retried every refused dial until the \
                 deadline; the primary tunnel never started listening"
            );
            return Err(format!(
                "failed to connect to primary at {target} after {:.0}s ({attempts} attempts; \
                 every connection-refused/reset failure was retried until the deadline — \
                 the primary's reverse tunnel never started listening): last error: {error}",
                start.elapsed().as_secs_f64()
            ));
        }
        attempts += 1;
        let error = match tokio::time::timeout(budget, dial()).await {
            Ok(Ok(connected)) => {
                tracing::info!(
                    target = %target,
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
                    target = %target,
                    attempts,
                    elapsed_s = start.elapsed().as_secs_f64(),
                    "failed to connect to primary: final dial attempt still pending at the deadline"
                );
                return Err(format!(
                    "failed to connect to primary at {target} after {:.0}s ({attempts} attempts; \
                     the final dial attempt was still pending when the deadline expired{history})",
                    start.elapsed().as_secs_f64()
                ));
            }
        };
        if !is_transient_dial_error(&error) {
            tracing::error!(
                target = %target,
                attempts,
                error = %error,
                "failed to connect to primary (non-transient error; not retrying)"
            );
            return Err(format!(
                "failed to connect to primary at {target}: {error} \
                 (non-transient error on attempt {attempts}; not retrying)"
            ));
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        let pause = delay.min(remaining);
        tracing::warn!(
            target = %target,
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
/// Encapsulates every backend-naming step on the secondary path:
/// starting the real `PeerNetwork` (the node's own acceptors come up
/// FIRST), reading the backend's cert / port into a `PeerCertInfo`,
/// extracting the mesh-send capability, and spawning the BACKGROUND
/// WSS dial + retry loop that folds the bootstrap wire into the mesh
/// under the primary's peer-id whenever it lands
/// (`bootstrap_fold_handle` — the same fold path a re-dialed wire
/// takes). Returns WITHOUT waiting on the bootstrap dial: the
/// coordinator's setup-wait owns the entire pre-primary wait
/// (run_20260611_005927).
///
/// Must run inside the mesh runtime's `LocalSet` (the construct closure
/// of `MeshHost::on_dedicated_thread`): the mesh's listeners/dials
/// register with the creating runtime's IO driver, and the background
/// bring-up dial is `spawn_local`-ed.
pub(crate) async fn dial_secondary_mesh<I: Identifier>(
    params: SecondaryDialParams,
) -> Result<SecondaryMeshBundle<I>, String> {
    let SecondaryDialParams {
        addrs,
        connect_timeout,
        retry_delay,
        secondary_id,
        bootstrap_primary_id,
        ipv4_address,
        ipv6_address,
        quic_bind_port,
        dial_failure_tx,
    } = params;
    // Per-attempt candidate order: v4 first, loopback family-completed
    // (the dial-side half of the partial-`-R`-bind defense — see
    // `dial_candidates`).
    let candidates = dial_candidates(&addrs);
    if candidates.is_empty() {
        return Err("no resolved address to dial for the bootstrap primary".to_string());
    }
    let target = candidates
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    // Start the secondary's peer mesh FIRST — before any bootstrap dial.
    // The node's own acceptors (QUIC UDP + WSS TCP) must be up from t≈0
    // so a peer that knows this node's recorded port (an observer, a
    // relocated primary) can dial IN and heal it even while the bootstrap
    // target is dead — the run_20260611_005927 structural fix (pre-fix,
    // the blocking dial below parked the whole node deaf for up to
    // `connect_timeout`). The identity passed to `PeerNetwork::start` is
    // BOTH the CN baked into this secondary's QUIC certificate AND the
    // `peer_id` other secondaries pass to quinn's
    // `connect(addr, server_name)` to validate that cert — the primary
    // distributes peer info keyed by `secondary_id` (the logical id), so
    // the cert CN must match the logical id. `quic_bind_port` (when the
    // operator/wrapper pre-allocated one) pins BOTH mesh listeners to the
    // advertised port; `None` keeps the OS-picked bind.
    let mut transport = PeerNetwork::<I>::start(&secondary_id, quic_bind_port)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to start peer network");
            format!("peer network start failed: {e}")
        })?;
    // #542 cause-B: install the persistent-dial-failure tx on the
    // freshly minted `PeerNetwork` BEFORE it is consumed into the
    // mesh-host (the same SHAPE the observer late-joiner uses for its
    // `-R` per-leg forward-recovery sink — see
    // `observer_late_joiner/run.rs`). `None` leaves the channel
    // un-subscribed, preserving the historical no-op default.
    if let Some(tx) = dial_failure_tx {
        transport.notify_persistent_dial_failures(tx);
    }
    let cert_pem = transport.cert_pem().to_string();
    let port = transport.port();

    // Cloneable mesh-send capability. Taken BEFORE the bootstrap wire fold
    // so the handle reflects the live `PeerNetwork`.
    let mesh_send = Some(transport.mesh_send_handle());

    // Tunnel-gave-up signal: the background dial fires this if it exhausts
    // its deadline without connecting (the tunnel never appeared). The
    // receiver is registered on the `SecondaryCoordinator` so
    // `wait_for_setup` can exit early — log to `IMPORTANT_TARGET` + return
    // a typed error — instead of squatting until the outer
    // `unconfigured_deadline` fires. The sender is consumed inside the
    // `spawn_local` below (dropped on success, fired on deadline
    // exhaustion), so the receiver side never needs to distinguish
    // success-vs-timeout: it fires `Ok(())` only on timeout, and a
    // successful dial lets it drop, making the coordinator's arm yield
    // `Err(RecvError)` — handled by parking on `pending()`.
    let (tunnel_gave_up_tx, tunnel_gave_up_rx) = tokio::sync::oneshot::channel::<()>();

    // Dial the bootstrap primary IN THE BACKGROUND: WSS, retrying
    // transient failures until `connect_timeout` (default 80% of
    // `unconfigured_deadline_secs` — sized so the coordinator's
    // `wait_for_setup` arm receives the give-up signal before the outer
    // `unconfigured_deadline` itself fires, giving the operator a clear
    // IMPORTANT_TARGET log and a clean non-zero exit rather than a silent
    // squat to the full 10-minute outer horizon). Each attempt sweeps the
    // candidate list in order; the FIRST address that accepts carries the
    // run (a v6-only partial `-R` bind connects over `[::1]`).
    //
    // The dial no longer gates the node's existence: the coordinator
    // enters its setup-wait immediately (beaconing + healable + retrying
    // its welcome handshake on a capped backoff), and the wire folds in
    // WHENEVER the dial lands — handed through the `PeerNetwork`'s fold
    // channel (`bootstrap_fold_handle`), the same path a re-dialed wire
    // takes, which retains the connected address so the wire stays
    // RE-dialable after a drop ("the tunnel is just a way of joining the
    // mesh"). On deadline exhaustion the dial fires `tunnel_gave_up_tx`
    // so the coordinator can exit early and release the SLURM allocation.
    let fold = transport.bootstrap_fold_handle();
    tokio::task::spawn_local(async move {
        match dial_until_deadline(&target, connect_timeout, retry_delay, || {
            dial_candidates_once(&candidates, |addr| {
                NetworkClient::<I>::connect_wss_only(addr)
            })
        })
        .await
        {
            Ok((client, connected_addr)) => {
                tracing::info!(
                    addr = %connected_addr,
                    "bootstrap primary dial connected; folding the wire into the mesh"
                );
                fold.fold(bootstrap_primary_id, connected_addr, client);
                // `tunnel_gave_up_tx` is dropped here (success path),
                // making the receiver's await yield `Err(RecvError)` —
                // the coordinator's arm treats this as "no give-up signal"
                // and parks on `pending()` for the rest of setup.
            }
            Err(error) => {
                // The tunnel never appeared within the dial deadline. Signal
                // the coordinator so `wait_for_setup` can exit cleanly
                // instead of squatting until the outer `unconfigured_deadline`
                // fires. The error detail is captured here; the coordinator
                // owns the IMPORTANT_TARGET emit (it has the secondary-id,
                // the configured horizon, and the context to narrate cleanly).
                tracing::error!(
                    error = %error,
                    "bootstrap bring-up dial deadline exhausted (tunnel never appeared); \
                     signalling coordinator to exit setup cleanly"
                );
                // Best-effort: the coordinator may have already exited (e.g.
                // it received a RunComplete/RunAborted during setup). The
                // `Err` is intentionally discarded.
                let _ = tunnel_gave_up_tx.send(());
            }
        }
    });

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
        tunnel_gave_up_rx,
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
/// `ObserverCoordinator` holds no transport and ships no `PeerCertInfo`
/// (it ignores cert-exchange frames), so the secondary-style cert bundle
/// the late-joiner used to build is gone.
///
/// Must run inside the mesh runtime's `LocalSet` (the construct closure
/// of `MeshHost::on_dedicated_thread`); the bootstrap rendezvous
/// (`join_running_cluster`) that follows is transport IO and runs there
/// too, before the transport is handed to the pump.
pub(crate) async fn observer_mesh<I: Identifier>(
    observer_id: &str,
) -> Result<PeerNetwork<I>, String> {
    // Ephemeral bind (`None`): the late-joiner DIALS the recorded
    // ports of existing peers; nobody dials the late-joiner from a
    // pre-advertised record, so it has no fixed port to honour.
    //
    // JOINING mode: the late-joiner is unknown to the running fleet, so a
    // seed whose id sorts BELOW it would never dial it under the
    // lower-id-dials rule and that leg would park forever. `start_joining`
    // makes this node dial every seed regardless of id order; crossed dials
    // (a lower-id seed later learning the joiner via roster) converge via
    // the accept-side grace-window dedup.
    PeerNetwork::<I>::start_joining(observer_id, None)
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
            &test_addr().to_string(),
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
            &test_addr().to_string(),
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
            &test_addr().to_string(),
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
                    &addr.to_string(),
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

    /// `dial_candidates` pure semantics: v4 before v6, dedup, and a
    /// loopback target is completed with BOTH loopback families on the
    /// same port — the dial-side half of the partial-`-R`-bind defense.
    #[test]
    fn dial_candidates_orders_v4_first_and_completes_loopback_families() {
        let v4: std::net::SocketAddr = "127.0.0.1:42655".parse().unwrap();
        let v6: std::net::SocketAddr = "[::1]:42655".parse().unwrap();

        // Resolver handed back only v6 loopback (container /etc/hosts
        // quirk): the v4 loopback is completed AND ordered first.
        assert_eq!(dial_candidates(&[v6]), vec![v4, v6]);
        // v4-only input: v6 completed second.
        assert_eq!(dial_candidates(&[v4]), vec![v4, v6]);
        // Both (v6 first from the resolver): reordered v4-first, deduped.
        assert_eq!(dial_candidates(&[v6, v4]), vec![v4, v6]);

        // Non-loopback targets (Standard gateway mode) are never
        // augmented with invented addresses — ordering + dedup only.
        let lan4: std::net::SocketAddr = "10.0.0.5:7000".parse().unwrap();
        let lan6: std::net::SocketAddr = "[fd00::5]:7000".parse().unwrap();
        assert_eq!(dial_candidates(&[lan6, lan4, lan6]), vec![lan4, lan6]);
    }

    /// `dial_candidates_once` sweep semantics: first success wins and
    /// reports WHICH address connected; an earlier candidate's failure
    /// (e.g. a v4 squatter's protocol error) does not stop the sweep;
    /// all-fail joins the per-candidate errors so the transient
    /// classifier sees every family's failure class.
    #[tokio::test]
    async fn dial_candidates_once_sweeps_in_order_and_reports_connected_addr() {
        let v4: std::net::SocketAddr = "127.0.0.1:42655".parse().unwrap();
        let v6: std::net::SocketAddr = "[::1]:42655".parse().unwrap();
        let candidates = vec![v4, v6];

        // v4 refused, v6 accepts: the sweep succeeds via v6 and says so.
        let (value, connected) = dial_candidates_once(&candidates, |addr| async move {
            if addr == v6 { Ok(42u32) } else { Err(PRODUCTION_REFUSED.to_string()) }
        })
        .await
        .expect("the v6 fallback must carry the attempt");
        assert_eq!(value, 42);
        assert_eq!(connected, v6);

        // Both refused: the combined error carries BOTH candidates and
        // classifies transient (retry until the tunnel listens).
        let err = dial_candidates_once(&candidates, |_addr| async move {
            Err::<u32, _>(PRODUCTION_REFUSED.to_string())
        })
        .await
        .expect_err("all-fail must surface the combined error");
        assert!(err.contains("127.0.0.1:42655") && err.contains("[::1]:42655"), "{err}");
        assert!(is_transient_dial_error(&err), "combined refusal stays transient: {err}");

        // v4 squatter (protocol error) + v6 refused (listener still
        // coming up): the refused half keeps the combined error
        // transient so the retry loop keeps waiting for the tunnel.
        let err = dial_candidates_once(&candidates, |addr| async move {
            if addr == v4 {
                Err::<u32, _>("WebSocket protocol error: Handshake not finished".to_string())
            } else {
                Err(PRODUCTION_REFUSED.to_string())
            }
        })
        .await
        .expect_err("all-fail must surface the combined error");
        assert!(
            is_transient_dial_error(&err),
            "a v4 squatter must not mask a v6 listener still establishing: {err}"
        );
    }

    /// THE production fix, end-to-end through the real backend: a
    /// listener that exists ONLY on `[::1]` (the partial `-R` bind that
    /// sshd reports as success) is reached by the candidate sweep —
    /// pre-fix the v4-only dial spun on Connection refused until the
    /// 10-minute deadline.
    #[tokio::test]
    async fn dual_family_dial_succeeds_against_v6_only_listener() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let listener =
                    match dynrunner_transport_quic::WssListener::bind("[::1]:0".parse().unwrap())
                        .await
                    {
                        Ok(l) => l,
                        Err(e) => {
                            eprintln!("skipping: no IPv6 loopback in this environment ({e})");
                            return;
                        }
                    };
                let port = listener.local_addr().port();
                let server = tokio::task::spawn_local(async move {
                    let _conn = listener.accept().await.expect("test accept");
                });

                // The resolver shape a worker container produces for
                // `localhost:<port>` when /etc/hosts maps localhost to
                // 127.0.0.1 only — the EXACT pre-fix dead end.
                let resolved: Vec<std::net::SocketAddr> =
                    vec![format!("127.0.0.1:{port}").parse().unwrap()];
                let candidates = dial_candidates(&resolved);
                let target = candidates
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let (_client, connected) = dial_until_deadline(
                    &target,
                    Duration::from_secs(30),
                    Duration::from_millis(50),
                    || {
                        dial_candidates_once(&candidates, |addr| {
                            NetworkClient::<RunnerIdentifier>::connect_wss_only(addr)
                        })
                    },
                )
                .await
                .expect("the v6 loopback fallback must reach a v6-only listener");
                assert!(
                    connected.is_ipv6(),
                    "must have connected over [::1], got {connected}"
                );
                server.await.expect("server task");
            })
            .await;
    }

    /// Symmetric guard: a v4-only listener (the common healthy shape)
    /// still connects via the first candidate — the fallback adds v6,
    /// it never penalises v4.
    #[tokio::test]
    async fn dual_family_dial_still_succeeds_against_v4_only_listener() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let listener = dynrunner_transport_quic::WssListener::bind(
                    "127.0.0.1:0".parse().unwrap(),
                )
                .await
                .expect("v4 loopback bind");
                let port = listener.local_addr().port();
                let server = tokio::task::spawn_local(async move {
                    let _conn = listener.accept().await.expect("test accept");
                });

                let resolved: Vec<std::net::SocketAddr> =
                    vec![format!("127.0.0.1:{port}").parse().unwrap()];
                let candidates = dial_candidates(&resolved);
                let (_client, connected) = dial_until_deadline(
                    "test-target",
                    Duration::from_secs(30),
                    Duration::from_millis(50),
                    || {
                        dial_candidates_once(&candidates, |addr| {
                            NetworkClient::<RunnerIdentifier>::connect_wss_only(addr)
                        })
                    },
                )
                .await
                .expect("the v4 path must be unaffected by the fallback");
                assert!(connected.is_ipv4(), "v4 candidate dials first: {connected}");
                server.await.expect("server task");
            })
            .await;
    }

    /// Neither family listening: the candidate sweep keeps the #357
    /// retry/deadline semantics intact — every all-refused attempt is
    /// retried until the deadline, and exhaustion reports the attempt
    /// history with the combined per-candidate error.
    #[tokio::test(start_paused = true)]
    async fn dual_family_dial_keeps_retry_deadline_semantics_when_nothing_listens() {
        let v4: std::net::SocketAddr = "127.0.0.1:42655".parse().unwrap();
        let v6: std::net::SocketAddr = "[::1]:42655".parse().unwrap();
        let candidates = vec![v4, v6];
        // Reference, so the `move` closure (which must own `counter`)
        // does not move the Vec the returned futures borrow.
        let candidates = &candidates;
        let calls = Rc::new(Cell::new(0u32));
        let counter = calls.clone();
        let result: Result<(u32, std::net::SocketAddr), String> = dial_until_deadline(
            "127.0.0.1:42655, [::1]:42655",
            Duration::from_secs(10),
            Duration::from_secs(1),
            move || {
                counter.set(counter.get() + 1);
                dial_candidates_once(candidates, |_addr| async move {
                    Err::<u32, _>(PRODUCTION_REFUSED.to_string())
                })
            },
        )
        .await;
        let err = result.unwrap_err();
        assert!(calls.get() > 1, "must retry all-refused sweeps: {}", calls.get());
        assert!(
            err.contains(&format!("({} attempts", calls.get())),
            "exhaustion carries the attempt count: {err}"
        );
        assert!(
            err.contains("127.0.0.1:42655") && err.contains("[::1]:42655"),
            "exhaustion carries the per-candidate failures: {err}"
        );
    }

    /// THE run_20260611_005927 structural pin: the secondary's mesh —
    /// its OWN acceptors, the coordinator behind them, the digest
    /// beacon, the heal arms — must come up WITHOUT waiting on the
    /// bootstrap dial. Pre-fix, `dial_secondary_mesh` parked the whole
    /// node inside `dial_until_deadline` for up to `connect_timeout`
    /// (default 600s — the production fleet's full mute window): no
    /// acceptor bound, no coordinator, heal-unreachable. Post-fix the
    /// factory returns promptly (the dial churns in the BACKGROUND) and
    /// the dial still lands once the listener appears — handing the wire
    /// to the `PeerNetwork`'s fold channel.
    ///
    /// REVERT-CHECK: with the blocking dial restored, the 5s bound on
    /// `dial_secondary_mesh` trips (the dial waits ~30s for a listener
    /// that only appears after the factory must have returned).
    #[tokio::test]
    async fn mesh_comes_up_before_the_bootstrap_dial_lands() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Reserve a bootstrap port, then free it: NOTHING listens
                // there while the factory runs (the dead-tunnel boot shape).
                let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let bootstrap_addr = probe.local_addr().unwrap();
                drop(probe);

                let bundle = tokio::time::timeout(
                    Duration::from_secs(5),
                    dial_secondary_mesh::<RunnerIdentifier>(SecondaryDialParams {
                        addrs: vec![bootstrap_addr],
                        connect_timeout: Duration::from_secs(30),
                        retry_delay: Duration::from_millis(50),
                        secondary_id: "sec-early".to_string(),
                        bootstrap_primary_id: "primary".to_string(),
                        ipv4_address: Some("127.0.0.1".to_string()),
                        ipv6_address: None,
                        quic_bind_port: None,
                        dial_failure_tx: None,
                    }),
                )
                .await
                .expect(
                    "dial_secondary_mesh must return promptly with the \
                     bootstrap target down — the bring-up dial is a \
                     background concern; blocking here is the \
                     run_20260611_005927 mute-node park",
                )
                .expect("factory bundle");

                // The node's own mesh is LIVE before any bootstrap wire
                // exists: its advertised WSS port accepts a dial — the
                // heal-reachability half (an observer holding the primary
                // fact can reach this node NOW).
                let mesh_addr: std::net::SocketAddr =
                    format!("127.0.0.1:{}", bundle.peer_cert_info.quic_port)
                        .parse()
                        .unwrap();
                NetworkClient::<RunnerIdentifier>::connect_wss_only(mesh_addr)
                    .await
                    .expect("the node's own acceptor must be up pre-bootstrap-dial");

                // The background dial LANDS once the bootstrap listener
                // appears: bring it up on the reserved addr and observe
                // the accept.
                let listener = dynrunner_transport_quic::WssListener::bind(bootstrap_addr)
                    .await
                    .expect("bootstrap listener bind");
                tokio::time::timeout(Duration::from_secs(10), listener.accept())
                    .await
                    .expect("the background bring-up dial must land once the listener appears")
                    .expect("bootstrap accept");
                drop(bundle);
            })
            .await;
    }

    /// Allocate a port free on BOTH protocols (TCP and UDP) — the same
    /// shape the SLURM wrapper's host-side pre-allocation produces for
    /// `--secondary-quic-port`. Mirrors the helper in
    /// `dynrunner-transport-quic`'s `peer/tests/bind_port.rs` (test-only;
    /// the crates share no test-support crate to host it).
    fn alloc_dual_free_port() -> u16 {
        for _ in 0..16 {
            let tcp = std::net::TcpListener::bind("0.0.0.0:0").expect("probe tcp bind");
            let port = tcp.local_addr().expect("probe tcp addr").port();
            if std::net::UdpSocket::bind(("0.0.0.0", port)).is_ok() {
                return port;
            }
        }
        panic!("could not find a port free on both TCP and UDP in 16 attempts");
    }

    /// #355 end-to-end through the factory: `SecondaryDialParams::
    /// quic_bind_port` threads into `PeerNetwork::start`, so the
    /// advertised `PeerCertInfo.quic_port` — the SAME number the SLURM
    /// wrapper recorded as `quic_port=` in the late-joiner's
    /// connection-info file — is a port the mesh actually LISTENS on: a
    /// cert-less WSS dial to it connects, which is exactly the
    /// late-joiner's fallback dial leg. Pre-fix the mesh bound an
    /// ephemeral port and the recorded one was dead
    /// (`JoinError::NoReachablePeer`).
    #[tokio::test]
    async fn dial_secondary_mesh_binds_the_preallocated_port() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // The "submitter" bootstrap listener the secondary dials.
                let bootstrap = dynrunner_transport_quic::WssListener::bind(
                    "127.0.0.1:0".parse().unwrap(),
                )
                .await
                .expect("bootstrap bind");
                let bootstrap_addr = bootstrap.local_addr();
                tokio::task::spawn_local(async move {
                    let _conn = bootstrap.accept().await.expect("bootstrap accept");
                    // Hold the folded wire open for the test's lifetime.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                });

                // Wrapper-style host-side pre-allocation.
                let mesh_port = alloc_dual_free_port();

                let bundle = dial_secondary_mesh::<RunnerIdentifier>(SecondaryDialParams {
                    addrs: vec![bootstrap_addr],
                    connect_timeout: Duration::from_secs(30),
                    retry_delay: Duration::from_millis(50),
                    secondary_id: "sec-0".to_string(),
                    bootstrap_primary_id: "primary".to_string(),
                    ipv4_address: Some("127.0.0.1".to_string()),
                    ipv6_address: None,
                    quic_bind_port: Some(mesh_port),
                    dial_failure_tx: None,
                })
                .await
                .expect("dial_secondary_mesh");

                assert_eq!(
                    bundle.peer_cert_info.quic_port, mesh_port,
                    "the advertised quic_port must be the pre-allocated one, \
                     not an ephemeral pick"
                );
                // The recorded port is DIALABLE — the late-joiner's
                // cert-less WSS leg, against the live mesh.
                let mesh_addr: std::net::SocketAddr =
                    format!("127.0.0.1:{mesh_port}").parse().unwrap();
                NetworkClient::<RunnerIdentifier>::connect_wss_only(mesh_addr)
                    .await
                    .expect("a WSS dial to the pre-allocated port must connect");
                drop(bundle);
            })
            .await;
    }

    /// #571 — `tunnel_gave_up_rx` fires when the background dial
    /// exhausts its deadline (the tunnel never appeared). Deterministic
    /// under `tokio::time::pause` — no real sleep.
    ///
    /// Shape: a port with nothing listening, `connect_timeout=100ms` → the
    /// signal fires within the deadline; the receiver resolves `Ok(())`.
    #[tokio::test(start_paused = true)]
    async fn tunnel_gave_up_rx_fires_on_deadline_exhaustion() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                // Nothing listens on this address.
                let bootstrap_addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
                let bundle = dial_secondary_mesh::<RunnerIdentifier>(SecondaryDialParams {
                    addrs: vec![bootstrap_addr],
                    connect_timeout: Duration::from_millis(100),
                    retry_delay: Duration::from_millis(10),
                    secondary_id: "sec-0".to_string(),
                    bootstrap_primary_id: "primary".to_string(),
                    ipv4_address: Some("127.0.0.1".to_string()),
                    ipv6_address: None,
                    quic_bind_port: None,
                    dial_failure_tx: None,
                })
                .await
                .expect("dial_secondary_mesh must return promptly (dial is background)");

                // `dial_until_deadline` tracks its own deadline via
                // `std::time::Instant` (not tokio time), so advancing the
                // tokio clock has no effect on when it exits.  Resume real
                // time so the 100 ms `connect_timeout` can expire naturally
                // and `tunnel_gave_up_tx` fires through the spawn_local.
                tokio::time::resume();
                tokio::time::timeout(
                    Duration::from_millis(500),
                    bundle.tunnel_gave_up_rx,
                )
                .await
                .expect("tunnel_gave_up_rx must resolve within the timeout")
                .expect("tunnel_gave_up_rx must fire Ok(()) on dial deadline exhaustion");
            })
            .await;
    }

    /// #571 regression: a successful dial does NOT fire `tunnel_gave_up_rx`.
    ///
    /// Shape: a real listener appears before `connect_timeout` → the
    /// background dial connects, drops `tunnel_gave_up_tx`, and the
    /// receiver yields `Err(RecvError)` (the coordinator treats this as
    /// "no give-up signal" and parks on `pending()`).
    #[tokio::test]
    async fn tunnel_gave_up_rx_does_not_fire_on_successful_dial() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let listener = dynrunner_transport_quic::WssListener::bind(
                    "127.0.0.1:0".parse().unwrap(),
                )
                .await
                .expect("test listener bind");
                let addr = listener.local_addr();

                // Accept in background — just enough to not leave the
                // dial hanging on the WS handshake.
                tokio::task::spawn_local(async move {
                    let _conn = listener.accept().await.expect("test accept");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                });

                let bundle = dial_secondary_mesh::<RunnerIdentifier>(SecondaryDialParams {
                    addrs: vec![addr],
                    connect_timeout: Duration::from_secs(30),
                    retry_delay: Duration::from_millis(50),
                    secondary_id: "sec-ok".to_string(),
                    bootstrap_primary_id: "primary".to_string(),
                    ipv4_address: Some("127.0.0.1".to_string()),
                    ipv6_address: None,
                    quic_bind_port: None,
                    dial_failure_tx: None,
                })
                .await
                .expect("dial_secondary_mesh");

                // Wait for the background dial to land (success path).
                // The background spawn folds the wire then drops
                // `tunnel_gave_up_tx`.
                tokio::time::sleep(Duration::from_millis(500)).await;

                // The receiver must yield Err(RecvError) — NOT Ok(()).
                let result = tokio::time::timeout(
                    Duration::from_millis(50),
                    bundle.tunnel_gave_up_rx,
                )
                .await;
                // Either timed-out (tx not yet dropped) OR RecvError
                // (tx dropped on success). Both are correct — we must
                // NOT see Ok(()).
                if let Ok(inner) = result {
                    assert!(
                        inner.is_err(),
                        "successful dial must NOT fire tunnel_gave_up_rx (got Ok(()))"
                    );
                }
                // Timed-out: the tx is alive (connected) but not yet
                // dropped — also correct; the receiver is parked.
            })
            .await;
    }
}
