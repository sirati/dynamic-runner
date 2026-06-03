//! Codec round-trip / wire-shape tests, decomposed by concern:
//!
//!   - [`roundtrip`] — basic per-variant encode/decode round-trips
//!     (Keepalive, SecondaryWelcome, TaskAssignment, TaskFailed,
//!     PeerInfo).
//!   - [`frame`] — `decode_frame` partial-input handling,
//!     `msg_type` / `sender_id` accessors, and the
//!     "every-variant" round-trip sweep.
//!   - [`binary_info`] — `DistributedBinaryInfo` wire-shape tests
//!     (flattened identifier, phase tags, legacy default-decoding,
//!     empty-field omission).
//!   - [`stage_promote`] — `StageFile` and `PromotePrimary`
//!     (incl. `required_setup` backcompat).

use super::*;
use crate::messages::*;
use dynrunner_core::{ErrorType, ResourceKind};
use serde::{Deserialize, Serialize};

mod binary_info;
mod cluster_mutation;
mod frame;
mod roundtrip;
mod stage_promote;

/// Test identifier matching the tokenizer's wire format.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TestId {
    binary_name: String,
    platform: String,
    compiler: String,
    version: String,
    opt_level: String,
}

fn test_id(name: &str) -> TestId {
    TestId {
        binary_name: name.into(),
        platform: "x86_64".into(),
        compiler: "gcc".into(),
        version: "12.0".into(),
        opt_level: "O2".into(),
    }
}
