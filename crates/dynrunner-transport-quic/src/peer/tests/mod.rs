//! Tests for the QUIC peer overlay.
//!
//! Layout:
//! - [`bind_port`]: `PeerNetwork::start` bind-port contract (#355) —
//!   an explicit port pins both listeners (QUIC/UDP + WSS/TCP);
//!   `None` keeps the OS-picked ephemeral behaviour.
//! - [`cert_parsing`]: pure PEM→DER bridge tests.
//! - [`two_peers`]: basic peer exchange + dial tie-break
//!   (`higher_id_does_not_dial_lower_id`).
//! - [`recv_lifetime`]: `recv_peer_tick_survives_outer_drop` —
//!   future-drop hardening for the reconnect tick arm.
//! - [`reader_exit_disconnect`]: reader/writer-exit is the
//!   authoritative disconnect detector + the `same_channel`
//!   generation check that keeps a stale signal from pruning a
//!   freshly-reconnected entry.
//! - [`dial_failure_summary`]: the operator-visible per-peer
//!   dial-failure summary emitted from `process_reconnect_tick` —
//!   carries the dialed address + consecutive-failure count, throttled
//!   to the summary threshold/recurrence boundaries.
//! - [`broadcast_miss`]: broadcast honesty (#363) — a known
//!   (`peer_dial_info`) but unconnected peer that misses a broadcast
//!   is named by a WARN, once per peer per outage.
//! - [`dial_sweep`]: the `connect_to_peers` sweep-summary dispositions
//!   (#362) — spawned / already-connected / awaiting-inbound (lower-id
//!   rule) / dropped-from-list — plus the higher-id side's truthful
//!   "peer leg missing, this node never dials it" summary WARN.
//! - [`formation_retry`]: mesh-formation retry — a leg whose INITIAL
//!   dial never landed (peer unreachable during a startup-load window)
//!   stays tracked by the reconnect reconciliation and establishes the
//!   moment the peer becomes reachable, with no further membership
//!   event (run_20260611_200548 replay, transport half).
//! - [`ingest_edges`]: ingest-edge clock recording over a real wire —
//!   the read loop stamps ARRIVAL without anyone driving `recv_peer`
//!   (the starved-pump honesty), DRAINED only on the actual pull.
//! - [`late_joiner_forward`]: desktop-shaped late-joiner bootstrap —
//!   the RED repro (compute-internal address unreachable from this
//!   host ⇒ loud bounded `NoReachablePeer`), the GREEN contract
//!   (join + snapshot RPC succeed through a local TCP forward
//!   endpoint with a cert-less, WSS-only rewritten seed entry), and
//!   the production frame shape (the bootstrap window accepts a
//!   snapshot reply stamped with the Phase-C role-typed routing
//!   target, amid stamped broadcast gossip).
//! - [`log_capture`]: shared tracing capture layer + `pump_b_until`
//!   used by the silent-reconnect + dial-failure-summary scenarios;
//!   kept here because they observe the framework log trace.
//! - [`silent_reconnect`]: the canonical 3-peer partition→heal
//!   scenario (~450 lines, intentionally one file: the multi-phase
//!   setup/partition/drain/heal pump cannot be cleanly chopped
//!   without scattering the test's invariants).
//! - [`persistent_dial_failure_trigger`]: the per-leg forward-recovery
//!   trigger (#419) — a dial-owned peer that keeps failing past the
//!   dial-summary boundary publishes its id on the
//!   `notify_persistent_dial_failures` sink (throttled to the summary
//!   cadence); silent for connected and non-dial-owned peers.
//! - [`member_leg_redial`]: the half-open member↔member leg heal
//!   (run_20260610_221140 / BUG 3.3) — the non-dial-owner's
//!   `RedialRequest` nudges the dial owner to force-prune + re-dial,
//!   relay covers directed sends meanwhile; plus the
//!   genuine-departure stop (roster replacement forgets tracking).
//! - [`accept_replace_rejoin`]: the rejoin-exile heal (#416 /
//!   run_20260611_123632) — a removed-but-alive peer that redials is
//!   re-admitted because a fresh authenticated inbound REPLACES the
//!   stale half-open `connections` entry on the accept side; the
//!   lower-id-dials dedup is preserved on the dial-owner side, and the
//!   replacement is generation-checked.
//!
//! The shared [`TestId`] is defined here so every sub-module gets
//! the same `Identifier` impl via `super::TestId`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct TestId(pub(crate) String);

/// Allocate a port that is currently free on BOTH protocols (TCP and
/// UDP) — the same shape the SLURM wrapper's host-side pre-allocation
/// produces. Retries a handful of OS-picked candidates so a UDP
/// squatter on a TCP-free port can't flake the test. Shared by
/// [`bind_port`] (the explicit-port contract) and [`formation_retry`]
/// (the peer-becomes-reachable replay).
pub(crate) fn alloc_dual_free_port() -> u16 {
    for _ in 0..16 {
        let tcp = std::net::TcpListener::bind("0.0.0.0:0").expect("probe tcp bind");
        let port = tcp.local_addr().expect("probe tcp addr").port();
        if std::net::UdpSocket::bind(("0.0.0.0", port)).is_ok() {
            // Both binds succeeded; release them for the network to claim.
            return port;
        }
    }
    panic!("could not find a port free on both TCP and UDP in 16 attempts");
}

mod accept_replace_rejoin;
mod bind_port;
mod bootstrap_redial;
mod broadcast_miss;
mod cert_parsing;
mod dial_failure_summary;
mod dial_sweep;
mod formation_retry;
mod ingest_edges;
mod late_joiner_forward;
mod log_capture;
mod member_leg_redial;
mod oversize_snapshot_chunking;
mod persistent_dial_failure_trigger;
mod primary_link;
mod reader_exit_disconnect;
mod recv_lifetime;
mod recv_tick_closed_spins;
mod silent_reconnect;
mod two_peers;
