//! Wire-canonical task content hash.
//!
//! Single concern: deterministic `String` hash recipe used as the key
//! in every cluster ledger, retry bucket, command-channel address
//! space, and pool predicate. Hoisted to `dynrunner-core` so both the
//! distributed primary path and the local manager path (which also
//! processes `PrimaryCommand` via its own command channel) compute the
//! same value without crossing a manager → manager dependency edge.
//!
//! Module boundary:
//!   * Owns: the canonical hash recipe + the `String` shape every
//!     consumer treats as opaque.
//!   * Does NOT own: any per-backend bookkeeping. Callers store their
//!     own `HashMap<String, _>` keyed on the returned value.
//!
//! Stability contract: the recipe (`{path, identifier}` hashed via
//! `std::collections::hash_map::DefaultHasher`, formatted as a 16-char
//! lowercase hex string) is observable on the wire (CRDT ledger keys,
//! `TaskHash` Python bytes). Any change here is a breaking wire change
//! for live runs that mix old + new binaries.

use crate::{Identifier, TaskInfo};

/// Compute the wire-canonical content hash for a task. Deterministic
/// across runs given identical `(path, identifier)` inputs; opaque to
/// callers (treat as an arbitrary string id).
///
/// Used as the key in (a) the CRDT cluster ledger, (b) every primary
/// command-channel address (FailPermanent, ReinjectTask,
/// UpdatePreferredSecondaries, SpawnTasks duplicate detection), and
/// (c) the local manager's `task_by_hash` mirror that backs the same
/// command channel on the local backend.
pub fn compute_task_hash<I: Identifier>(binary: &TaskInfo<I>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    binary.path.hash(&mut hasher);
    binary.identifier.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
