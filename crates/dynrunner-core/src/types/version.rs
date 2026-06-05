//! [`TaskVersion`] — the primary-stamped monotone per-task metadata
//! version that makes the non-lattice CRDT fields convergent.
//!
//! Single concern: the data shape of the version tag. The originating
//! primary stamps it on every metadata-bearing mutation and on every
//! authoritative reset; the comparison/merge logic that consumes it
//! lives in the `cluster_state` convergence comparators, not here.

use serde::{Deserialize, Serialize};

/// Primary-stamped monotone per-task metadata version. The originating
/// primary stamps `(current primary_epoch, per-task seq)` on every
/// metadata-bearing mutation and on every authoritative reset
/// (requeue / reinject). Total order = lexicographic on
/// `(primary_epoch, seq)`.
///
/// `Default` is `(0, 0)`, the strict minimum, so a legacy (pre-field)
/// sender's record — which decodes to the default via
/// `#[serde(default)]` — never dominates a versioned one.
/// Same-version-per-run is the norm (one primary, one epoch); `seq`
/// disambiguates two updates from the same primary within an epoch and
/// makes a reset strictly supersede the state it resets.
///
/// `primary_epoch` is `u64` (the source epoch is `u64`; narrowing to a
/// smaller width would be a silent lossy cast on the monotone tiebreak).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize, Hash,
)]
pub struct TaskVersion {
    pub primary_epoch: u64,
    pub seq: u32,
}
