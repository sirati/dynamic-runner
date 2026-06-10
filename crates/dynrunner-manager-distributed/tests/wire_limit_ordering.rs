//! Pins the cross-crate payload-limit ordering (#366).
//!
//! The three caps live in three crates with no shared dependency edge
//! that could host a single compile-time assert (the transport crate
//! deliberately does not depend on the worker-IPC protocol crate), so
//! the full chain is pinned here — this crate depends on all three.
//!
//! The invariant: each layer is strictly larger than the one below, so
//! a payload that passes an inner gate can NEVER be dropped by an
//! outer one:
//!
//!   publish API per-value cap (16 MiB)
//!     < worker→manager IPC frame guard (64 MiB)
//!     < mesh wire frame limit (96 MiB)
//!
//! If any constant changes, this test forces the change to be made
//! coherently across the chain. See
//! `dynrunner_transport_quic::framing::MAX_WIRE_FRAME_BYTES` for the
//! full rationale of each layer's headroom.

use dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES;
use dynrunner_protocol_manager_worker::MAX_RESPONSE_FRAME_BYTES;
use dynrunner_transport_quic::MAX_WIRE_FRAME_BYTES;

// The chain is between constants — pin it at compile time.
const _: () = assert!(INLINE_VALUE_HARD_CAP_BYTES < MAX_RESPONSE_FRAME_BYTES);
const _: () = assert!(MAX_RESPONSE_FRAME_BYTES < MAX_WIRE_FRAME_BYTES);

/// Named, counted mirror of the const asserts above (the compile-time
/// pins are the enforcement; this makes the pin visible in test
/// output). The const blocks fail THIS test binary's compilation if
/// the chain is broken.
#[test]
fn wire_limit_ordering() {
    const { assert!(INLINE_VALUE_HARD_CAP_BYTES < MAX_RESPONSE_FRAME_BYTES) };
    const { assert!(MAX_RESPONSE_FRAME_BYTES < MAX_WIRE_FRAME_BYTES) };
}
