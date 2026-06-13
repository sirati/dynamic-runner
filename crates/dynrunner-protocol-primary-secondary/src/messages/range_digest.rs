//! `RangeDigest` — a Merkle-lite, range-scoped fingerprint of the task
//! ledger, the P1 "resume from last good state" DELTA on top of the
//! scalar [`StateDigest`](crate::StateDigest) task fold.
//!
//! # Single concern
//!
//! The scalar [`StateDigest::tasks_hash`](crate::StateDigest::tasks_hash)
//! XOR-fold tells a behind node THAT its task ledger diverges from a peer,
//! never WHICH entries. This type splits the task keyspace into
//! [`RANGE_COUNT`] fixed buckets and carries a per-bucket count + XOR-fold,
//! so two replicas can compare bucket-by-bucket and pull ONLY the divergent
//! buckets — a one-task churn re-pulls ~one bucket, not the whole 34027-key
//! ledger.
//!
//! # Stability + replica-independence (the correctness backbone)
//!
//! Each task key is assigned to a bucket by a HASH-PREFIX of the key (the
//! manager-side `cluster_state` owns the exact `range_index`), NOT by its
//! sorted position: a sorted-index split would shift every key's bucket
//! when the ledgers differ in count — precisely the divergence case — so
//! the same key would land in different buckets on two replicas and the
//! comparison would be meaningless. A hash-prefix split puts the SAME key
//! in the SAME bucket on every replica, independent of what other keys
//! exist, so the per-bucket fold is directly comparable across replicas.
//!
//! Each bucket's `fold` is the XOR of the SAME per-entry term that builds
//! [`StateDigest::tasks_hash`](crate::StateDigest::tasks_hash) (the
//! manager folds `hash_one((key, hashable_join_key(state)))` into bucket
//! `range_index(key)`). Because every entry's term lands in exactly one
//! bucket and XOR is associative + commutative, the cross-bucket fold
//! reconstructs the scalar exactly:
//! `XOR(folds) == tasks_hash` and `sum(counts) == tasks_count`. The
//! manager-side `range_digest_folds_match_scalar` test pins this invariant.
//!
//! This type holds NO merge logic and NO `I`-typed payload — every member
//! is a `u64`/`u32` summary — so it is identifier-erased exactly like
//! [`StateDigest`](crate::StateDigest) and rides the wire inline.

use serde::{Deserialize, Serialize};

/// Number of fixed task-keyspace buckets. A behind node re-pulls the
/// divergent buckets, so this trades wire size (a `RangeDigest` is
/// `RANGE_COUNT × (u64 + u32)` ≈ 3 KiB at 256) against delta granularity
/// (a single churned key narrows the pull to `1/RANGE_COUNT` of the
/// ledger). 256 keeps a 34027-key ledger at ~133 keys/bucket — a one-task
/// change re-pulls ~133 keys, three orders of magnitude below the full
/// snapshot, while the per-reply wire cost stays a few KiB on the direct
/// leg and ONLY when the requester is behind.
pub const RANGE_COUNT: usize = 256;

/// A range-scoped (Merkle-lite) fingerprint of the task ledger: a per-bucket
/// count + XOR-fold over the [`RANGE_COUNT`] hash-prefix buckets. Paired
/// with the requester's own `RangeDigest`, [`RangeDigest::divergent_ranges`]
/// yields exactly the buckets that differ, which the requester stamps on its
/// `RequestSnapshotStream` so the responder streams only those keys.
///
/// Wire-compat: `#[serde(default)]` decodes a pre-field peer as the
/// all-zero digest (every bucket count 0, fold 0). A requester that
/// receives an all-zero `RangeDigest` (a legacy responder, or a responder
/// that genuinely holds no tasks) computes "every non-empty local bucket
/// diverges" — but the SAFE fallback is the empty divergent-set the
/// `RequestSnapshotStream` reads as "all ranges" (a full P0 stream), so a
/// legacy peer never corrupts the delta; it just degrades to a full pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeDigest {
    /// Per-bucket entry count, indexed by `range_index(key)`. Boxed so the
    /// `RangeDigest` is cheap to move and the `[u32; 256]`/`[u64; 256]`
    /// pair does not bloat every enclosing frame's stack footprint.
    #[serde(default = "zero_counts", with = "serde_arrays")]
    pub counts: [u32; RANGE_COUNT],
    /// Per-bucket XOR-fold of the per-entry task term, indexed by
    /// `range_index(key)`. `XOR(folds) == StateDigest::tasks_hash`.
    #[serde(default = "zero_folds", with = "serde_arrays")]
    pub folds: [u64; RANGE_COUNT],
}

impl Default for RangeDigest {
    fn default() -> Self {
        Self {
            counts: zero_counts(),
            folds: zero_folds(),
        }
    }
}

fn zero_counts() -> [u32; RANGE_COUNT] {
    [0u32; RANGE_COUNT]
}

fn zero_folds() -> [u64; RANGE_COUNT] {
    [0u64; RANGE_COUNT]
}

impl RangeDigest {
    /// The buckets in which `peer` holds task data this local digest is
    /// MISSING — the per-bucket image of [`StateDigest::is_behind`]'s task
    /// rule (`field_behind`): a bucket is divergent iff the peer holds
    /// strictly MORE entries there OR the SAME number with a DIFFERENT fold
    /// (same-count-different-members ⇒ the peer holds ≥1 entry the local
    /// replica lacks in that bucket). Returns the divergent bucket indices,
    /// ascending — the set the requester stamps on its
    /// `RequestSnapshotStream` so the responder streams ONLY those keys.
    ///
    /// Buckets where the local replica is AHEAD (more entries, or a
    /// divergent fold the OTHER way) are NOT returned: the responder cannot
    /// help there, and the local replica's own pull from someone else (or
    /// the peer's pull from us) heals the reverse direction — exactly the
    /// one-directional `field_behind` semantics, applied per bucket. A
    /// converged ledger yields an EMPTY set (every bucket equal), so a
    /// quiesced node pulls nothing.
    pub fn divergent_ranges(&self, peer: &RangeDigest) -> Vec<u16> {
        let mut out = Vec::new();
        for r in 0..RANGE_COUNT {
            let local_count = self.counts[r];
            let local_fold = self.folds[r];
            let peer_count = peer.counts[r];
            let peer_fold = peer.folds[r];
            // The exact per-bucket image of `field_behind`: the peer is
            // ahead in this bucket iff it holds strictly more entries, or
            // the same number with a divergent fold.
            if peer_count > local_count || (peer_count == local_count && peer_fold != local_fold) {
                out.push(r as u16);
            }
        }
        out
    }
}

/// `serde` adapter for the fixed-size arrays: `serde` has no blanket
/// `[T; N]` impl for N > 32, so the count/fold arrays serialize as
/// sequences. Kept tiny + local (the only fixed-array fields on the wire).
mod serde_arrays {
    use super::RANGE_COUNT;
    use serde::de::{Error, SeqAccess, Visitor};
    use serde::ser::SerializeTuple;
    use serde::{Deserializer, Serializer};
    use std::fmt;
    use std::marker::PhantomData;

    pub(super) fn serialize<S, T>(arr: &[T; RANGE_COUNT], ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: serde::Serialize,
    {
        let mut tup = ser.serialize_tuple(RANGE_COUNT)?;
        for v in arr.iter() {
            tup.serialize_element(v)?;
        }
        tup.end()
    }

    pub(super) fn deserialize<'de, D, T>(de: D) -> Result<[T; RANGE_COUNT], D::Error>
    where
        D: Deserializer<'de>,
        T: serde::Deserialize<'de> + Default + Copy,
    {
        struct ArrVisitor<T>(PhantomData<T>);
        impl<'de, T> Visitor<'de> for ArrVisitor<T>
        where
            T: serde::Deserialize<'de> + Default + Copy,
        {
            type Value = [T; RANGE_COUNT];
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "an array of {RANGE_COUNT} elements")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut out = [T::default(); RANGE_COUNT];
                let mut n = 0usize;
                while let Some(v) = seq.next_element::<T>()? {
                    if n >= RANGE_COUNT {
                        return Err(A::Error::invalid_length(n + 1, &self));
                    }
                    out[n] = v;
                    n += 1;
                }
                if n != RANGE_COUNT {
                    return Err(A::Error::invalid_length(n, &self));
                }
                Ok(out)
            }
        }
        de.deserialize_tuple(RANGE_COUNT, ArrVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_with(buckets: &[(usize, u32, u64)]) -> RangeDigest {
        let mut d = RangeDigest::default();
        for &(r, c, f) in buckets {
            d.counts[r] = c;
            d.folds[r] = f;
        }
        d
    }

    /// A converged pair (byte-identical range digests) diverges in NO
    /// bucket — the self-quiescing property at the bucket granularity.
    #[test]
    fn converged_digests_have_no_divergent_ranges() {
        let d = digest_with(&[(0, 3, 0xABCD), (5, 1, 0x11), (200, 7, 0xDEAD)]);
        assert!(d.divergent_ranges(&d).is_empty());
    }

    /// A bucket where the peer holds MORE entries is divergent (the
    /// count-ahead case); a bucket where the LOCAL holds more is NOT (the
    /// one-directional `field_behind` semantics, per bucket).
    #[test]
    fn more_entries_in_peer_bucket_is_divergent_one_directionally() {
        let local = digest_with(&[(3, 1, 0x1)]);
        let peer = digest_with(&[(3, 2, 0x2)]);
        assert_eq!(local.divergent_ranges(&peer), vec![3]);
        // The reverse: local is ahead in bucket 3, peer cannot help.
        assert!(peer.divergent_ranges(&local).is_empty());
    }

    /// Equal count, divergent fold in a bucket (the `count==`, `hash!=`
    /// case — equal-count-different-content WITHIN a bucket) is detected
    /// both ways: each side may hold an entry the other lacks in that
    /// bucket, so both pull (the idempotent restore reconciles).
    #[test]
    fn equal_count_divergent_fold_in_bucket_is_divergent_both_ways() {
        let a = digest_with(&[(7, 2, 0x1)]);
        let b = digest_with(&[(7, 2, 0x2)]);
        assert_eq!(a.divergent_ranges(&b), vec![7]);
        assert_eq!(b.divergent_ranges(&a), vec![7]);
    }

    /// Several divergent buckets are returned ascending; equal buckets
    /// between them are skipped.
    #[test]
    fn multiple_divergent_buckets_returned_ascending() {
        let local = digest_with(&[(1, 1, 0x1), (10, 2, 0x2), (250, 5, 0x5)]);
        let peer = digest_with(&[
            (1, 1, 0x1),    // equal — skipped
            (10, 3, 0x9),   // more entries — divergent
            (250, 5, 0x99), // equal count, divergent fold — divergent
        ]);
        assert_eq!(local.divergent_ranges(&peer), vec![10, 250]);
    }

    /// Wire round-trip preserves every bucket (the fixed-array serde
    /// adapter is symmetric).
    #[test]
    fn json_round_trip_preserves_buckets() {
        let d = digest_with(&[(0, 9, 0xFEED), (128, 3, 0xC0DE), (255, 1, 0x42)]);
        let json = serde_json::to_string(&d).expect("serialize");
        let back: RangeDigest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.counts, d.counts);
        assert_eq!(back.folds, d.folds);
    }
}
