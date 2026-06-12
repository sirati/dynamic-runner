//! Gateway-mode seed acquisition for the late-joiner observer
//! (`--observer-join-from-peer-info-dir <gateway-side dir> --gateway
//! <host>`): the desktop-shaped bootstrap where the `.info` files live
//! on the gateway and the recorded addresses are compute-node-internal.
//!
//! # Module boundary
//!
//! This file owns ONLY the orchestration glue + the ONE seed-rewrite
//! seam. The machinery is reused wholesale from its owners:
//!
//! - gateway file machinery: [`dynrunner_gateway::SshGateway`] (the
//!   same ControlMaster gateway the SLURM pipeline drives) +
//!   [`dynrunner_slurm::fetch_peer_info_dir_v2`] mirror the remote
//!   `connection_info/` dir to a local tempdir, which the UNCHANGED
//!   local reader (`read_peer_info_dir_v2`) then consumes;
//! - tunnel machinery: [`dynrunner_slurm::LocalForwardTunnels`] brings
//!   up one `127.0.0.1:<local> → <compute>:<quic_port>` forward per
//!   seed peer — concurrently, each registered ON the connected
//!   gateway's ControlMaster (`ssh -O forward`; bounded direct
//!   `ssh -N -L` dials when the master is gone) and gated on the
//!   local port actually LISTENing (per-peer registry, same-port
//!   rebuild, half-dead escalation — the `-R` path's lifecycle
//!   policy);
//! - seed building: the sibling [`super::helpers::records_to_seed`]
//!   stays byte-identical; [`rewrite_seed_for_local_forwards`] below is
//!   the SINGLE place a fetched record's dial target is replaced by its
//!   local tunnel endpoint.
//!
//! The reconnect story: the registry's
//! [`dynrunner_slurm::LocalForwardTunnelReconnector`] is handed to the
//! caller, who wires it onto the standalone `ObserverCoordinator` via
//! `set_tunnel_reconnector` — the observer's lost-visibility trigger
//! then rebuilds dropped `ssh -L` children on their ORIGINAL local
//! ports, and the transport's own 5s reconnect ticker re-dials the
//! unchanged `127.0.0.1:<local_port>` endpoints.
//!
//! # Known residual (live-run scope)
//!
//! A peer that joins the cluster AFTER this bootstrap (a respawned
//! secondary) has no tunnel and no rewritten dial info on this
//! observer; the reconnector names such ids loudly and skips them.
//! Mid-run tunnel discovery (re-fetching the info dir on unknown ids)
//! is deliberately not built until a consumer run shows it matters.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use pyo3::prelude::*;

use dynrunner_gateway::traits::Gateway;
use dynrunner_gateway::{SshConfig, SshGateway};
use dynrunner_protocol_primary_secondary::PeerConnectionInfo;
use dynrunner_slurm::{
    ForwardTarget, LocalForwardTunnels, fetch_peer_info_dir_v2, read_peer_info_dir_v2,
};

use super::helpers::{map_read_dir_error, records_to_seed};

/// The live gateway-mode runtime the join rides on: the connected
/// gateway (ControlMaster; disconnected at teardown) and the tunnel
/// registry (children reaped at teardown; also the reconnector's
/// substrate). Returned alongside the rewritten seed so the run loop
/// owns the teardown ordering.
pub(super) struct GatewayJoinRuntime {
    pub(super) gateway: SshGateway,
    pub(super) tunnels: Arc<LocalForwardTunnels>,
}

impl GatewayJoinRuntime {
    /// Tear the gateway-mode runtime down: reap every `ssh -L` child,
    /// then disconnect the gateway master. Failures are logged, never
    /// propagated — teardown runs on both the success and error exits.
    pub(super) async fn teardown(mut self) {
        self.tunnels.teardown().await;
        if let Err(e) = self.gateway.disconnect().await {
            tracing::warn!(error = %e, "gateway disconnect at late-joiner teardown failed");
        }
    }
}

/// THE seed-rewrite seam: substitute each entry's dial target with its
/// local tunnel endpoint (`127.0.0.1:<local_port>`). The cert is
/// CLEARED because a TCP forward cannot carry QUIC/UDP — a cert-less
/// entry makes `dial_peer` skip the QUIC race and dial WSS directly on
/// the same forwarded port (the production wrapper omits the cert
/// anyway, so this is the already-proven dial shape). An entry with no
/// established tunnel keeps its original (gateway-internal) address and
/// is named loudly — `join_running_cluster` fans to every seed, so a
/// dead record must not brick the peers that DID tunnel.
pub(super) fn rewrite_seed_for_local_forwards(
    seed: &mut [PeerConnectionInfo],
    endpoints: &HashMap<String, u16>,
) {
    for entry in seed.iter_mut() {
        if let Some(&local_port) = endpoints.get(&entry.secondary_id) {
            entry.ipv4 = Some("127.0.0.1".into());
            entry.ipv6 = None;
            entry.port = local_port;
            entry.cert = String::new();
        } else {
            tracing::warn!(
                peer_id = %entry.secondary_id,
                target = %format!(
                    "{}:{}",
                    entry.ipv4.as_deref().or(entry.ipv6.as_deref()).unwrap_or("<no-addr>"),
                    entry.port
                ),
                "no local-forward tunnel for this seed entry; keeping its \
                 recorded (gateway-internal) address, which is unlikely to be \
                 dialable from this host"
            );
        }
    }
}

/// Gateway-mode bootstrap acquisition, one straight line with a loud
/// failure at every step: connect the gateway → mirror the remote
/// `.info` dir to a tempdir → read it with the unchanged local reader
/// → build the seed → establish the per-peer `ssh -L` tunnels →
/// rewrite the seed's dial targets to the local endpoints.
pub(super) async fn acquire_gateway_seed(
    cfg: SshConfig,
    remote_dir: &Path,
) -> PyResult<(Vec<PeerConnectionInfo>, GatewayJoinRuntime)> {
    let remote_dir_str = remote_dir.to_string_lossy().into_owned();

    // 1. Connect (loud: unreachable gateway names the host + cause).
    let mut gateway = SshGateway::new(cfg.clone());
    gateway.connect().await.map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "observer late-joiner: failed to connect to gateway {}: {e}",
            cfg.host
        ))
    })?;

    // From here on, a failure must disconnect the gateway before
    // surfacing — wrap the rest and tear down on error.
    let result = acquire_over_connected_gateway(&gateway, &cfg, &remote_dir_str).await;
    match result {
        Ok((seed, tunnels)) => Ok((seed, GatewayJoinRuntime { gateway, tunnels })),
        Err(e) => {
            if let Err(de) = gateway.disconnect().await {
                tracing::warn!(error = %de, "gateway disconnect after failed bootstrap also failed");
            }
            Err(e)
        }
    }
}

/// Steps 2–6 over an already-connected gateway. Split out so the
/// error-path gateway disconnect in [`acquire_gateway_seed`] has a
/// single wrap point (and so the fetch/tunnel/rewrite sequence is
/// testable over any `Gateway` impl if a fixture ever needs it).
async fn acquire_over_connected_gateway(
    gateway: &SshGateway,
    cfg: &SshConfig,
    remote_dir: &str,
) -> PyResult<(Vec<PeerConnectionInfo>, Arc<LocalForwardTunnels>)> {
    // 2. Mirror the gateway-side dir (loud: missing dir / zero .info
    //    files / per-file download failures each name their step).
    let mirror = tempfile::tempdir().map_err(|e| {
        pyo3::exceptions::PyOSError::new_err(format!(
            "observer late-joiner: failed to create local mirror tempdir: {e}"
        ))
    })?;
    fetch_peer_info_dir_v2(gateway, remote_dir, mirror.path())
        .await
        .map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("observer late-joiner: {e}"))
        })?;

    // 3. Read the mirror with the UNCHANGED local reader + 4. build
    //    the seed — identical code path (and error shapes) to the
    //    local no-gateway mode.
    let records = read_peer_info_dir_v2(mirror.path()).map_err(map_read_dir_error)?;
    let mut seed = records_to_seed(&records);
    if seed.is_empty() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "observer late-joiner: gateway-side peer-info dir produced zero usable \
             seed entries after v2 filtering — refusing to enter \
             join_running_cluster with an empty seed (would hang on the \
             connect-budget)",
        ));
    }

    // 5. Establish one `ssh -L` per seed peer. The forward target is
    //    the record's legacy-URI host (the same compute-node name the
    //    `-R` path sshes to — resolvable on the gateway) + its mesh
    //    port. Only records that produced a seed entry are tunneled
    //    (same secondary_id/quic_port required-set).
    let targets: Vec<ForwardTarget> = records
        .iter()
        .filter_map(|r| {
            Some(ForwardTarget {
                peer_id: r.secondary_id.clone()?,
                host: r.legacy_uri.host.clone(),
                port: r.quic_port?,
            })
        })
        .collect();
    //    Each forward registers on the connected gateway's
    //    ControlMaster (one real auth session, zero sshd sessions;
    //    the registry falls back to bounded direct dials if the
    //    master dies), so the whole cohort establishes concurrently
    //    in one listen-gate window instead of ~3s × N sequentially.
    let tunnels = Arc::new(LocalForwardTunnels::new(
        cfg.clone(),
        gateway.control_path().map(str::to_owned),
    ));
    let endpoints = tunnels.establish(&targets).await.map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("observer late-joiner: {e}"))
    })?;

    // 6. THE rewrite seam.
    rewrite_seed_for_local_forwards(&mut seed, &endpoints);
    Ok((seed, tunnels))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, ipv4: &str, port: u16, cert: &str) -> PeerConnectionInfo {
        PeerConnectionInfo {
            secondary_id: id.into(),
            cert: cert.into(),
            ipv4: Some(ipv4.into()),
            ipv6: Some("fd00::1".into()),
            port,
            is_observer: false,
            liveness_port: None,
        }
    }

    /// The rewrite seam: a tunneled entry's dial target becomes its
    /// local endpoint — ipv4 pinned to loopback, ipv6 dropped (the
    /// forward listens on 127.0.0.1 only), port replaced, cert cleared
    /// (WSS-only through the TCP pipe). Identity/role fields are
    /// untouched.
    #[test]
    fn rewrite_substitutes_local_endpoint_and_forces_wss() {
        let mut seed = vec![entry("secondary-0", "10.0.0.7", 51200, "CERT")];
        let endpoints = HashMap::from([("secondary-0".to_string(), 15001u16)]);
        rewrite_seed_for_local_forwards(&mut seed, &endpoints);
        assert_eq!(seed[0].ipv4.as_deref(), Some("127.0.0.1"));
        assert_eq!(seed[0].ipv6, None);
        assert_eq!(seed[0].port, 15001);
        assert_eq!(
            seed[0].cert, "",
            "cert must be cleared: QUIC/UDP cannot ride a TCP forward"
        );
        assert_eq!(seed[0].secondary_id, "secondary-0");
        assert!(!seed[0].is_observer);
    }

    /// An entry whose tunnel failed keeps its recorded address (and is
    /// WARNed about) — the fanned-out join still reaches the tunneled
    /// survivors, mirroring `LocalForwardTunnels::establish`'s
    /// per-peer tolerance.
    #[test]
    fn rewrite_keeps_untunneled_entry_unchanged() {
        let mut seed = vec![
            entry("secondary-0", "10.0.0.7", 51200, "CERT-0"),
            entry("secondary-1", "10.0.0.8", 51201, "CERT-1"),
        ];
        let endpoints = HashMap::from([("secondary-0".to_string(), 15001u16)]);
        rewrite_seed_for_local_forwards(&mut seed, &endpoints);
        // Tunneled: rewritten.
        assert_eq!(seed[0].port, 15001);
        // Untunneled: byte-identical (field-by-field — the wire struct
        // deliberately derives no PartialEq).
        let expected = entry("secondary-1", "10.0.0.8", 51201, "CERT-1");
        assert_eq!(seed[1].secondary_id, expected.secondary_id);
        assert_eq!(seed[1].cert, expected.cert);
        assert_eq!(seed[1].ipv4, expected.ipv4);
        assert_eq!(seed[1].ipv6, expected.ipv6);
        assert_eq!(seed[1].port, expected.port);
        assert_eq!(seed[1].is_observer, expected.is_observer);
        assert_eq!(seed[1].liveness_port, expected.liveness_port);
    }
}
