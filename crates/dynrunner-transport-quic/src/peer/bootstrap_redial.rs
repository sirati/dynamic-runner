//! Bootstrap-wire re-dial: keep the secondary→submitter link restorable
//! after the `-R` tunnel drops.
//!
//! # The defect this fixes (defect (b))
//!
//! The secondary dials the submitter primary's bootstrap wire ONCE at
//! startup over the per-secondary `ssh -R` reverse tunnel
//! (`connect_wss_only(localhost:<tunnel_port>)`), then folds that wire
//! into the mesh via [`PeerNetwork::register_primary_link`]. The dialed
//! address was DISCARDED, and the inbound forwarder exited SILENTLY when
//! the wire closed — so when the `-R` tunnel dropped and the observer
//! later rebuilt it, NOBODY re-dialed QUIC/WSS through the fresh tunnel.
//! The submitter→observer stayed blind forever. The submitter's transport
//! is inbound-only (no dial path), and the secondary's generic peer
//! reconnect ticker only redials peers in `peer_dial_info` — which the
//! bootstrap wire was never entered into. This module restores ONLY the
//! re-dial of the bootstrap pipe.
//!
//! # Why a SEPARATE loop, not the generic `peer_dial_info` ticker
//!
//! Routing the bootstrap wire through the generic
//! [`PeerNetwork::process_reconnect_tick`] / `spawn_dial_task` path was
//! rejected for two hard reasons, both load-bearing:
//!
//! 1. **Honest-liveness invariant (mod.rs `register_primary_link` doc).**
//!    The generic outgoing handler wires the connection's reader/writer
//!    exit into the QUIC `disconnect_tx` channel, which feeds the
//!    mesh prune+redial disposition. The bootstrap-primary entry
//!    DELIBERATELY does NOT feed that channel — the secondary→primary
//!    link's app-layer liveness (the `should_arm_failover` failure
//!    window, keyed on `DistributedMessage` arrival) is the SOLE failover
//!    signal, decoupled from QUIC packet liveness by design. A redial that
//!    introduced a QUIC-liveness edge into failover arming would violate
//!    that invariant. This module's redial restores the TRANSPORT PIPE
//!    ONLY: it emits no event any failover input consumes — it dials,
//!    re-folds, done. `should_arm_failover` / mesh-degraded / election
//!    inputs are untouched.
//!
//! 2. **Re-fold semantics.** A restored bootstrap wire must re-enter the
//!    mesh via [`PeerNetwork::register_primary_link`] (fan-in `mesh_writer`
//!    plus inbound forwarder into the single `incoming_tx`), NOT the
//!    generic `spawn_outgoing_handler` + `new_conn_tx` registration. And
//!    the dial is `connect_wss_only` against the FIXED
//!    `localhost:<tunnel_port>` tunnel address — never a
//!    `PeerConnectionInfo`-advertised LAN addr (the submitter is
//!    unroutable except through the tunnel).
//!
//! # Shape
//!
//! [`BootstrapDialTarget`] carries the fixed dial address + the primary's
//! peer id. When a folded wire's inbound forwarder observes close, it
//! spawns [`redial_bootstrap_wire`], which dials with capped backoff
//! INDEFINITELY (the tunnel may be down for minutes; the observer link is
//! restorable at any time, so there is no give-up). On success it sends
//! the fresh [`crate::NetworkClient`] back to `recv_peer` through the
//! [`BootstrapRedial`] channel, where `&mut self` re-folds it via
//! `register_primary_link` — which arms the next forwarder, so the cycle
//! self-perpetuates for the life of the run.

use std::net::SocketAddr;
use std::time::Duration;

use dynrunner_core::Identifier;
use tokio::sync::mpsc;

use crate::NetworkClient;

/// Initial backoff between bootstrap re-dial attempts.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Cap on the bootstrap re-dial backoff. Modest so the link is restored
/// promptly once the tunnel comes back, but slow enough that a tunnel
/// down for minutes does not churn the dial path. The retry NEVER gives
/// up (the observer link is restorable at any time).
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// The fixed dial target for re-establishing the bootstrap wire. Retained
/// from the secondary's original startup dial: the `localhost:<tunnel_port>`
/// the `ssh -R` reverse tunnel terminates at (NOT a LAN address — the
/// submitter is reachable only through the tunnel). Cloneable so the
/// re-fold can stash a fresh copy for the NEXT drop.
#[derive(Clone)]
pub(super) struct BootstrapDialTarget {
    /// The fixed `localhost:<tunnel_port>` address to re-dial.
    pub(super) addr: SocketAddr,
    /// The peer id the bootstrap wire is folded under (the conventional
    /// `"primary"`); re-fold keys `connections` by this.
    pub(super) primary_id: String,
}

/// A freshly re-dialed bootstrap wire handed back to `recv_peer` for
/// re-fold under `&mut self`. Carries the new client AND the (cloned)
/// dial target so the re-fold can re-arm the redial for the NEXT drop —
/// the cycle is self-perpetuating.
pub(super) struct BootstrapRedial<I: Identifier> {
    pub(super) target: BootstrapDialTarget,
    pub(super) client: NetworkClient<I>,
}

/// Re-dial the bootstrap wire with capped backoff, INDEFINITELY, and hand
/// the fresh client back to `recv_peer` for re-fold.
///
/// Spawned by the folded wire's inbound forwarder the instant the wire
/// closes. Loops `connect_wss_only(target.addr)` — the SAME WSS-only dial
/// the secondary used at startup — with exponential backoff capped at
/// [`MAX_BACKOFF`], never giving up: the `-R` tunnel may be down for
/// minutes while the observer rebuilds it, and the link is restorable at
/// any time. On the first success the fresh [`NetworkClient`] is sent
/// through `redial_tx`; the receiving `recv_peer` arm re-folds it via
/// `register_primary_link`, which spawns the next forwarder and re-arms
/// this redial for the subsequent drop.
///
/// Emits NO failover-consumed event: the only output is the transport
/// pipe handed to the loop. The app-layer liveness window keeps running
/// orthogonally on whatever frames do (or do not) arrive.
pub(super) async fn redial_bootstrap_wire<I: Identifier>(
    target: BootstrapDialTarget,
    redial_tx: mpsc::UnboundedSender<BootstrapRedial<I>>,
) {
    tracing::info!(
        primary = %target.primary_id,
        addr = %target.addr,
        "bootstrap wire closed — re-dialing the submitter link through the (rebuilt) tunnel"
    );
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match NetworkClient::<I>::connect_wss_only(target.addr).await {
            Ok(client) => {
                tracing::info!(
                    primary = %target.primary_id,
                    addr = %target.addr,
                    "bootstrap wire re-dialed; handing back for re-fold into the mesh"
                );
                // If the receiver (the network's recv loop) is gone the
                // run is tearing down — drop the client and stop.
                let _ = redial_tx.send(BootstrapRedial { target, client });
                return;
            }
            Err(e) => {
                tracing::debug!(
                    primary = %target.primary_id,
                    addr = %target.addr,
                    error = %e,
                    backoff_secs = backoff.as_secs_f64(),
                    "bootstrap re-dial attempt failed; retrying after backoff (never gives up)"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}
