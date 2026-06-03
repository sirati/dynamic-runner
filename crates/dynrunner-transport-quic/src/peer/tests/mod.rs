//! Tests for the QUIC peer overlay.
//!
//! Layout:
//! - [`cert_parsing`]: pure PEM→DER bridge tests.
//! - [`two_peers`]: basic peer exchange + dial tie-break
//!   (`higher_id_does_not_dial_lower_id`).
//! - [`recv_lifetime`]: `recv_peer_tick_survives_outer_drop` —
//!   future-drop hardening for the reconnect tick arm.
//! - [`either`]: `NoPeerTransport` + `EitherPeerTransport::Disabled`
//!   parity, plus `Real`-variant round-trip.
//! - [`log_capture`]: shared tracing capture layer + `pump_b_until`
//!   used only by the silent-reconnect scenario; kept here because
//!   no other scenario observes the relay log trace.
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

mod cert_parsing;
mod either;
mod log_capture;
mod primary_link;
mod recv_lifetime;
mod silent_reconnect;
mod two_peers;
