//! `drive_fetch_run_config` — the backend-opaque dial + fetch
//! implementation behind the `fetch_run_config` pyfunction.
//!
//! Resolves the primary URL, dials it through `transport_factory`
//! (folding the primary into the mesh), then drives
//! [`PeerTransport::fetch_run_config`] on a current-thread tokio
//! `LocalSet` (the dial path's accept loops are `spawn_local`-ed, so the
//! whole fetch must run inside a `LocalSet`). Mirrors the observer
//! late-joiner's `run_observer_late_joiner` runtime shape.

use pyo3::prelude::*;

use dynrunner_protocol_primary_secondary::PeerTransport;

use crate::config::distributed::DistributedConfig;
use crate::identifier::RunnerIdentifier;
use crate::managers::transport_factory;
use crate::network::{detect_ipv4, detect_ipv6};

/// Dial the bootstrap primary and pull the cluster-wide `forwarded_argv`.
///
/// See the module / pyfunction docs for the contract. This function holds
/// the runtime + `LocalSet` scaffolding and the two boundary maps (dial
/// error / fetch error → `PyResult`).
pub(crate) fn drive_fetch_run_config(
    py: Python<'_>,
    primary_url: String,
    secondary_id: String,
    distributed_config: DistributedConfig,
) -> PyResult<Vec<String>> {
    // Dial budget AND fetch budget both ride the unconfigured-deadline
    // (audit D4): a still-starting primary must be waited out across the
    // WHOLE cold-start window, not the 10s rendezvous budget. The dial
    // retry loop (`dial_secondary_mesh`) gives up past `connect_timeout`;
    // once the link is up the responder answers in milliseconds, so the
    // same generous budget on the fetch is slack, not a rendezvous.
    let unconfigured_deadline = distributed_config.unconfigured_deadline();
    let retry_delay = distributed_config.connect_retry_delay();
    let disable_peer_overlay = distributed_config.disable_peer_overlay();

    py.detach(|| -> PyResult<Vec<String>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "failed to create tokio runtime: {e}"
                ))
            })?;
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(async move {
            // Resolve the primary URL to a SocketAddr. Same acceptance as
            // the secondary dispatcher: "tcp://host:port", "ws://host:port",
            // "wss://host:port", or bare "host:port" where host may be an
            // IP or a DNS name (SLURM gateways hand out the FQDN).
            let addr_str = primary_url
                .strip_prefix("tcp://")
                .or_else(|| primary_url.strip_prefix("ws://"))
                .or_else(|| primary_url.strip_prefix("wss://"))
                .unwrap_or(&primary_url);
            let addr: std::net::SocketAddr = match tokio::net::lookup_host(addr_str).await {
                Ok(mut iter) => match iter.next() {
                    Some(a) => a,
                    None => {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "fetch_run_config: DNS lookup returned no addresses for primary URL \
                             {primary_url}"
                        )));
                    }
                },
                Err(e) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "fetch_run_config: failed to resolve primary URL {primary_url}: {e}"
                    )));
                }
            };

            // Dial the bootstrap primary through the backend-opaque
            // factory: the real WSS dial + retry loop, the peer-overlay
            // selection, and the bootstrap-wire fold under the primary's
            // peer-id ("primary") that makes the primary an ordinary mesh
            // member. We reuse the secondary's exact dial machinery — the
            // run-config fetch is just a different RPC on the same dialed
            // mesh. The cert info / mesh-send handle the bundle also
            // carries are unused here (this is a read-only fetch, not a
            // join), but the factory builds them as a unit.
            let mesh_bundle = transport_factory::dial_secondary_mesh::<RunnerIdentifier>(
                transport_factory::SecondaryDialParams {
                    addr,
                    // Dial budget ≥ unconfigured-deadline (audit D4).
                    connect_timeout: unconfigured_deadline,
                    retry_delay,
                    disable_peer_overlay,
                    secondary_id: &secondary_id,
                    bootstrap_primary_id: "primary".to_string(),
                    ipv4_address: Some(detect_ipv4(None)),
                    ipv6_address: detect_ipv6(None),
                },
            )
            .await
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
            let mut transport = mesh_bundle.transport;

            // Drive the fetch RPC on the folded mesh. `secondary_id` is the
            // unicast return address the primary's reply routes back to
            // (the CN it registered the dialed connection under). The fetch
            // sends an UNWELCOMED RequestRunConfig — no welcome / cert
            // exchange (the real join happens later inside the spliced
            // run()). Budget ≥ unconfigured-deadline; on exhaustion the
            // error propagates and the shim exits non-zero.
            let argv = transport
                .fetch_run_config(&secondary_id, unconfigured_deadline)
                .await
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "fetch_run_config: {e}"
                    ))
                })?;

            Ok(argv)
        }))
    })
}
