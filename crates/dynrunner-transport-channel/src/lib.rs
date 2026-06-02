//! In-process channel transports for the runtime's three boundaries:
//!
//! - [`manager_runner`][crate::manager_runner]: tokio-mpsc pair for
//!   the manager↔worker `Command`/`Response` protocol.
//! - [`secondary_transport`][crate::secondary_transport]: primary↔
//!   secondary `SecondaryTransport` end + matching primary-end on
//!   the secondary side.
//! - [`peer_transport`][crate::peer_transport]: `PeerTransport` over
//!   a mesh of in-process peers with the same `Router` + role-cache
//!   machinery the QUIC transport uses.
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

pub mod either_primary;
pub mod manager_runner;
pub mod mesh;
pub mod peer_transport;
pub mod secondary_transport;

pub use either_primary::EitherPrimaryTransport;
pub use manager_runner::{channel_pair, ChannelManagerEnd, ChannelRunnerEnd};
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

// Test files split by concern. `tests_role_routing` sits slightly
// above the 500-line guideline (~530 lines) because every test in
// it shares one `TestRegistrar` fixture + cache-state invariants
// that span all four receiver-side cases; partitioning further
// would scatter the per-case assertions across files for no
// maintainability gain.
#[cfg(test)]
mod tests_either_primary;
#[cfg(test)]
mod tests_manager_runner;
#[cfg(test)]
mod tests_peer_basics;
#[cfg(test)]
mod tests_role_routing;
