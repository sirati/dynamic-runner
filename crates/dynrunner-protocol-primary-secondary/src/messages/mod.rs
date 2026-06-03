//! Wire-format message types for the primary/secondary protocol.
//!
//! Submodule layout:
//!   - [`message_type`] — the `MessageType` discriminator enum.
//!   - [`peer_info`] — setup-phase peer descriptor structs
//!     (`WorkerReadyInfo`, `PeerConnectionInfo`).
//!   - [`binary_info`] — per-task wire descriptors
//!     (`DistributedBinaryInfo`, `ZipFileAssignment`,
//!     `ZipBinaryEntry`, `StagedFileRecord`) and their serde-default
//!     helpers.
//!   - [`distributed`] — the top-level `DistributedMessage<I>` enum
//!     plus accessor impls.

pub mod accessors;
pub mod binary_info;
pub mod distributed;
pub mod message_type;
pub mod peer_info;

pub use binary_info::{DistributedBinaryInfo, StagedFileRecord, ZipBinaryEntry, ZipFileAssignment};
pub use distributed::{DistributedMessage, KeepaliveRole};
pub use message_type::MessageType;
pub use peer_info::{PeerConnectionInfo, WorkerReadyInfo};

/// Unix-epoch wall-clock seconds for use as a wire-frame `timestamp`
/// field. Single source for envelope construction inside this crate
/// (e.g. the `RoleAddressed` / `RoleMisaddressHint` envelopes wrapped
/// by `PeerTransport::send`).
///
/// Manager-side `wire.rs` helpers (`secondary/wire.rs`,
/// `primary/wire.rs`) and the per-transport `timestamp_secs` helpers
/// in `transport-quic` / `transport-channel` predate this one and
/// remain in place. Consolidating them is a separate cleanup
/// (single concern, easily grep-driven) — keeping this helper
/// scoped to envelope construction inside the protocol crate avoids
/// a four-call-site refactor riding along with Step 3.
pub fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
