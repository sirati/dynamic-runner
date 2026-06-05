//! `StateDigest` — a compact, order-independent fingerprint of a
//! replicated cluster ledger, exchanged periodically for anti-entropy.
//!
//! Single concern: a fixed-size summary (per-field counts + `u64`
//! content hashes) of a `ClusterState`, plus the pure field-by-field
//! "is the local replica missing data the peer holds?" comparison the
//! anti-entropy cadence uses to decide *whether* to pull a snapshot.
//!
//! The digest carries NO task payloads and NO identifier-typed data —
//! every member is a `u64` hash or a `usize`/`u64` scalar — so the frame
//! is `I`-erased exactly like the wire envelope it rides in. The hashes
//! summarise SET/MAP membership (XOR-fold of per-entry hashes), never
//! iteration order, so two replicas that converged to the same state
//! always produce byte-identical digests regardless of insertion order.
//!
//! This type holds NO merge logic and NO knowledge of the CRDT lattice.
//! It is a read-only projection (built by `ClusterState::digest`) plus a
//! monotone comparison; the actual reconciliation is the existing
//! snapshot RPC + `ClusterState::restore`. The digest only answers
//! "when to pull".

use serde::{Deserialize, Serialize};

/// Compact fingerprint of a replicated `ClusterState`, exchanged on the
/// anti-entropy cadence. Every field pairs a COUNT (how many entries the
/// replica holds for that part of the ledger) with a `u64` content HASH
/// (an order-independent fold of the entries' identities/values), so a
/// receiver can tell — per field — both whether the sender holds MORE
/// entries and whether the same-count sets DIVERGE.
///
/// Determinism + order-independence: each map/set hash is the XOR-fold of
/// a per-entry hash, so it is invariant under iteration order and
/// re-computing it on a converged replica yields the same value. The
/// scalar fields (`primary_epoch`, the run latches) are carried verbatim.
///
/// Wire-compat: the whole struct is `#[serde(default)]`-friendly via
/// per-field defaults so a future field added here decodes as its zero
/// value on a peer that predates it (a missing count/hash reads as "this
/// peer holds nothing for that field", the conservative
/// never-claims-to-be-ahead shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StateDigest {
    /// Number of task entries in the ledger.
    #[serde(default)]
    pub tasks_count: u64,
    /// Order-independent fold over the task ledger: XOR of a per-entry
    /// hash that combines the task's wire hash key with its
    /// state-rank, so a replica whose entry advanced to a stronger
    /// terminal (e.g. `Pending` → `Completed` for the same key) produces
    /// a different fold even at the same count.
    #[serde(default)]
    pub tasks_hash: u64,
    /// Number of per-secondary capacity records.
    #[serde(default)]
    pub secondary_capacities_count: u64,
    /// XOR-fold over the per-secondary capacity record keys (the
    /// secondary ids). Capacity is set-once/static, so the key-set
    /// identity is sufficient to detect a missing entry.
    #[serde(default)]
    pub secondary_capacities_hash: u64,
    /// Number of keyed task-output cache entries.
    #[serde(default)]
    pub task_outputs_count: u64,
    /// XOR-fold over the keyed-output cache keys (per-key first-write-
    /// wins, so the key-set identity detects a missing entry).
    #[serde(default)]
    pub task_outputs_hash: u64,
    /// Number of phases in the static dependency graph.
    #[serde(default)]
    pub phase_deps_count: u64,
    /// Replicated primary epoch (monotone scalar; higher wins).
    #[serde(default)]
    pub primary_epoch: u64,
    /// Sticky-monotone run-completion latch.
    #[serde(default)]
    pub run_complete: bool,
    /// Sticky-monotone run-abort latch (carried as a presence bit — the
    /// reason string is not needed to detect divergence).
    #[serde(default)]
    pub run_aborted: bool,
}

impl StateDigest {
    /// Does `other` (a peer's digest) hold ledger data this local digest
    /// is MISSING — i.e. would pulling+`restore()`ing the peer's snapshot
    /// change local state? This is the anti-entropy detector: the cadence
    /// pulls a snapshot iff this returns `true`.
    ///
    /// Per-field rule, faithful to the monotone `restore()` lattice:
    ///
    /// - Count-bearing fields (`tasks`, `secondary_capacities`,
    ///   `task_outputs`, `phase_deps`): the peer is ahead iff it holds
    ///   strictly MORE entries (`peer.count > local.count`) OR holds the
    ///   SAME number but a DIFFERENT fold (`count ==`, `hash !=`) — same-
    ///   count-different-members means the peer holds at least one entry the
    ///   local replica lacks (and the merge is a no-op for the entries they
    ///   share, so pulling is always safe). A peer with FEWER entries is
    ///   behind, not ahead, so it never triggers a local pull (the peer's
    ///   own digest round will pull from us).
    /// - `primary_epoch`: ahead iff strictly higher (the `restore` epoch
    ///   merge is `>` wins).
    /// - `run_complete` / `run_aborted`: ahead iff the peer's latch is set
    ///   while ours is not (`false → true` ratchet; the reason string is
    ///   irrelevant to the detector).
    ///
    /// The detector compares ONLY the monotone, snapshot-healable ledger
    /// fields. The membership/role sets (`observers`, `can_be_primary`, the
    /// alive-member set) are DELIBERATELY EXCLUDED — carried by neither the
    /// digest nor this comparison — for two independent reasons:
    ///
    /// 1. They are non-monotone-via-removal and NOT snapshot-healable. The
    ///    live apply path REMOVES ids (`PeerRemoved` drops a peer from
    ///    `can_be_primary`/`observers`; `SetCanBePrimary(false)` removes),
    ///    but `ClusterState::restore` is additive/sticky: it replaces the
    ///    role sets only when the local set is empty (else keeps local) and
    ///    inserts alive entries only into VACANT membership slots (a local
    ///    `Dead` id is sticky and never resurrected). So a node holding a
    ///    stale extra id can never reconcile it by pulling — flagging these
    ///    would produce a PERMANENT no-op pull loop (a pull every cadence
    ///    tick that `restore()` cannot heal).
    /// 2. They converge via their OWN paths, not anti-entropy. Additions
    ///    flow over the live `PeerJoined`/`PeerRemoved`/`SetCanBePrimary`
    ///    broadcasts plus the post-mesh `rebroadcast_full_roster` re-emit.
    ///    And the alive-member divergence is INTENTIONAL per the honest-
    ///    liveness design — each node owns its own liveness view, so two
    ///    nodes legitimately disagree on whether a peer is alive; anti-
    ///    entropy must NEVER force-converge that (it must never resurrect a
    ///    peer a node correctly buried as dead).
    ///
    /// (Removing a stale role-set id thus relies on deliver-once live
    /// broadcast + the roster re-emit, not anti-entropy. Closing that gap —
    /// a node that missed a single `PeerRemoved`/`SetCanBePrimary(false)`
    /// while disconnected — would need a tombstone or version-vector in the
    /// CRDT algebra, out of this delivery-layer detector's scope.)
    ///
    /// Symmetric quiescence: when the two replicas are converged every
    /// compared field is equal and this returns `false`, so a steady-state
    /// digest exchange triggers ZERO pulls — the self-quiescing property.
    pub fn is_behind(&self, other: &StateDigest) -> bool {
        field_behind(self.tasks_count, self.tasks_hash, other.tasks_count, other.tasks_hash)
            || field_behind(
                self.secondary_capacities_count,
                self.secondary_capacities_hash,
                other.secondary_capacities_count,
                other.secondary_capacities_hash,
            )
            || field_behind(
                self.task_outputs_count,
                self.task_outputs_hash,
                other.task_outputs_count,
                other.task_outputs_hash,
            )
            // `phase_deps` is static (set-once); a count-only compare
            // suffices and there is no separate fold to carry.
            || other.phase_deps_count > self.phase_deps_count
            || other.primary_epoch > self.primary_epoch
            || (other.run_complete && !self.run_complete)
            || (other.run_aborted && !self.run_aborted)
    }
}

/// A single count+hash field: the peer is ahead iff it holds strictly
/// more entries, or the same number with a divergent fold (so it holds at
/// least one entry the local replica lacks). See [`StateDigest::is_behind`].
fn field_behind(local_count: u64, local_hash: u64, peer_count: u64, peer_hash: u64) -> bool {
    peer_count > local_count || (peer_count == local_count && peer_hash != local_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_digests_are_not_behind() {
        let d = StateDigest {
            tasks_count: 3,
            tasks_hash: 0xABCD,
            primary_epoch: 2,
            ..Default::default()
        };
        assert!(!d.is_behind(&d));
    }

    #[test]
    fn more_tasks_on_peer_is_behind() {
        let local = StateDigest {
            tasks_count: 1,
            tasks_hash: 0x11,
            ..Default::default()
        };
        let peer = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x22,
            ..Default::default()
        };
        assert!(local.is_behind(&peer));
        // The peer with fewer entries is NOT behind the lesser local.
        assert!(!peer.is_behind(&local));
    }

    #[test]
    fn same_count_divergent_fold_is_behind_both_ways() {
        // Equal counts, different folds: each side may hold an entry the
        // other lacks, so both pull (idempotent restore reconciles).
        let a = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x1,
            ..Default::default()
        };
        let b = StateDigest {
            tasks_count: 2,
            tasks_hash: 0x2,
            ..Default::default()
        };
        assert!(a.is_behind(&b));
        assert!(b.is_behind(&a));
    }

    #[test]
    fn higher_epoch_and_set_latches_are_behind() {
        let local = StateDigest::default();
        let peer = StateDigest {
            primary_epoch: 5,
            run_complete: true,
            run_aborted: true,
            ..Default::default()
        };
        assert!(local.is_behind(&peer));
        // Latch ratchet is one-directional: a set-local vs unset-peer is
        // NOT behind.
        assert!(!peer.is_behind(&local));
    }
}
