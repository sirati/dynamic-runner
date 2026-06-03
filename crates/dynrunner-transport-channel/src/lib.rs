//! In-process channel transports for the runtime's three boundaries:
//!
//! - [`manager_runner`][crate::manager_runner]: tokio-mpsc pair for
//!   the manager↔worker `Command`/`Response` protocol.
//! - [`secondary_transport`][crate::secondary_transport]: primary↔
//!   secondary `SecondaryTransport` end + matching primary-end on
//!   the secondary side.
//! - [`peer_transport`][crate::peer_transport]: `PeerTransport` over
//!   a mesh of in-process peers backed by the same `Router` the QUIC
//!   transport uses. Role-blind (transport ⊥ roles) — it routes by
//!   `PeerId` only.
//! - [`mesh`][crate::mesh]: builders that wire up either a full or
//!   partial peer mesh in one call.
//!
//! Tests are split into three sibling files by concern; see the
//! `cfg(test)` block at the bottom.
//!
//! `Clocks` helpers are defined here because every entry point
//! that calls into `Router` snapshots `(Instant::now(), wire_secs)`
//! through the same pair of utilities; the QUIC transport keeps an
//! equivalent helper in `transport-quic/src/peer/transport_impl.rs`.

use std::time::Instant;

use dynrunner_protocol_primary_secondary::Clocks;

pub mod manager_runner;
pub mod mesh;
pub mod peer_transport;
pub mod secondary_transport;

pub use manager_runner::{ChannelManagerEnd, ChannelRunnerEnd, channel_pair};
pub use mesh::{peer_mesh, peer_mesh_with_adjacency};
pub use peer_transport::ChannelPeerTransport;
pub use secondary_transport::{ChannelPrimaryTransportEnd, ChannelSecondaryTransportEnd};

/// Unix-epoch wall-clock seconds for the wire-side `Clocks::wire`
/// envelope timestamp. Local-clock TTL/cooldown decisions inside Router
/// use the monotonic `Instant::now()` carried alongside it.
fn timestamp_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Snapshot the `Clocks` pair Router consumes — kept centralised so
/// every entry point stays in lockstep with the QUIC transport's
/// equivalent helper (cf. `transport-quic/src/peer/transport_impl.rs`).
pub(crate) fn now_clocks() -> Clocks {
    Clocks {
        now: Instant::now(),
        wire: timestamp_secs(),
    }
}

// Test files split by concern. The role-routing test family was
// removed when the channel transport was de-roled (transport ⊥ roles):
// the transport no longer carries a role cache or intercepts
// `RoleAddressed`, so there is no channel-side role layer to test.
#[cfg(test)]
mod tests_manager_runner;
#[cfg(test)]
mod tests_peer_basics;
