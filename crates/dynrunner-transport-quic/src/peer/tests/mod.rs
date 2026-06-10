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
//! - [`dial_sweep`]: the `connect_to_peers` sweep-summary dispositions
//!   (#362) — spawned / already-connected / awaiting-inbound (lower-id
//!   rule) / dropped-from-list — plus the higher-id side's truthful
//!   "peer leg missing, this node never dials it" summary WARN.
//! - [`log_capture`]: shared tracing capture layer + `pump_b_until`
//!   used by the silent-reconnect + dial-failure-summary scenarios;
//!   kept here because they observe the framework log trace.
//! - [`silent_reconnect`]: the canonical 3-peer partition→heal
//!   scenario (~450 lines, intentionally one file: the multi-phase
//!   setup/partition/drain/heal pump cannot be cleanly chopped
//!   without scattering the test's invariants).
//!
//! The shared [`TestId`] is defined here so every sub-module gets
//! the same `Identifier` impl via `super::TestId`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct TestId(pub(crate) String);

mod bind_port;
mod bootstrap_redial;
mod cert_parsing;
mod dial_failure_summary;
mod dial_sweep;
mod log_capture;
mod primary_link;
mod reader_exit_disconnect;
mod recv_lifetime;
mod recv_tick_closed_spins;
mod silent_reconnect;
mod two_peers;
