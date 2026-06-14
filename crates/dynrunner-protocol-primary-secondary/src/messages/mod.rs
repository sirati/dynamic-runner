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
pub mod illegal_assignment;
pub mod message_type;
pub mod peer_info;
pub mod range_digest;
pub mod state_digest;

pub use binary_info::{DistributedBinaryInfo, StagedFileRecord, ZipBinaryEntry, ZipFileAssignment};
pub use distributed::{DistributedMessage, KeepaliveRole};
pub use illegal_assignment::AssignedTaskRef;
pub use message_type::MessageType;
pub use peer_info::{PeerConnectionInfo, WorkerReadyInfo};
pub use range_digest::{RANGE_COUNT, RangeDigest};
pub use state_digest::StateDigest;

/// Unix-epoch wall-clock seconds for use as a wire-frame `timestamp`
/// field. Single source for envelope construction inside this crate
/// (e.g. the `RequestSnapshotStream` envelope built by
/// `PeerTransport::join_running_cluster`).
///
/// Manager-side `wire.rs` helpers (`secondary/wire.rs`,
/// `primary/wire.rs`) and the per-transport `timestamp_secs` helpers
/// in `transport-quic` / `transport-channel` predate this one and
/// remain in place. Consolidating them is a separate cleanup
/// (single concern, easily grep-driven).
pub fn timestamp_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}
