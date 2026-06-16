//! Per-secondary AFFINE bitvector — the replicated CRDT state that models,
//! per `(secondary_id, affine_id)`, the completion of a
//! `TaskKind::SecondaryAffine` def on that secondary.
//!
//! Single concern: WHERE the per-secondary affine cell state lives, its
//! compact representation, its per-cell convergent merge, and the
//! state-query helpers the affine SCHEDULER (AF-sched) reads. It owns NO
//! scheduling: it does not decide WHICH secondary an affine def goes to, nor
//! place per-secondary queues — it only records and converges the cells the
//! scheduler reads/writes through the typed API below.
//!
//! ## Cell value
//! Each cell is one [`AffineCell`] — `NotDone(00) / Queued(01) / Failed(10) /
//! Done(11)`. The compact store packs the two bits per cell (four cells per
//! byte), indexed by the DENSE affine-id (see `task_def_store::AffineId`); all
//! cells start `NotDone`.
//!
//! ## Merge lattice (the load-bearing part)
//! The cell is **NOT** a pure grow-only / max-join value. The idle-steal does
//! a `Queued → NotDone` reset (it un-queues a locality claim when the
//! schedulable unit moves to another secondary), so there is NO partial order
//! under which every legal transition only moves UP — a value max-join would
//! make the steal's reset LOSE to the Queued it is undoing and never converge.
//!
//! Convergence is instead a **per-cell LAST-WRITER-WINS** lattice on a
//! `generation: u64` stamp:
//!
//! * the LATTICE is, per cell, the set `{(generation, value)}` ordered by
//!   `generation` (with a deterministic value tiebreak at equal generation);
//!   the JOIN keeps the entry with the strictly-greater generation, and at an
//!   equal generation the higher [`AffineCell`] discriminant
//!   (`Done > Failed > Queued > NotDone`) — a total order, so the join is
//!   commutative + associative + idempotent ⇒ it CONVERGES under anti-entropy
//!   regardless of delivery order.
//! * the generation is PRIMARY-MONOTONE: every cell-setting mutation is
//!   stamped at the origination choke point from a monotone counter, and a
//!   promoted primary RESUMES the counter PAST every observed cell generation
//!   (the `resume_affine_gen_floor` failover seam), so a later write (incl. the
//!   steal's reset) always carries a strictly-greater stamp than the write it
//!   supersedes. This is why the steal's `01 → 00` reset CONVERGES: its
//!   generation is `> ` the Queued's generation, so the LWW join keeps the
//!   reset.
//! * FAILOVER: the bitvector is REPLICATED (snapshot + digest + restore), so a
//!   promoted primary INHERITS every cell + its generation; resuming the gen
//!   counter past `max(observed generation)` keeps the LWW total order intact
//!   across the epoch boundary.
//!
//! ### Q1 (failed-stickiness) — RECOMMENDED DEFAULT, FLAGGED
//! `Failed(10)` is **NOT sticky-terminal** under this lattice: a later
//! higher-generation `Queued`/`Done` overrides a `Failed` (a dependent
//! re-routes/retries on another secondary, OR the secondary itself retries the
//! affine build). This is the recommended default the owner left open
//! (design Q1). It is a PURE consequence of LWW-on-generation: nothing special-
//! cases `Failed`. Should the owner later decide `Failed` IS sticky, the change
//! is localized to [`SecondaryAffineBits::merge_cell`] / [`Self::set_cell`]
//! (clamp a `Failed` cell against an incoming non-`Done`), NOT a wire change.

use std::collections::HashMap;

use dynrunner_protocol_primary_secondary::AffineCell;

use super::task_def_store::AffineId;

/// Pack an [`AffineCell`] into its 2-bit code (the wire/bitvector encoding).
fn cell_to_bits(cell: AffineCell) -> u8 {
    match cell {
        AffineCell::NotDone => 0b00,
        AffineCell::Queued => 0b01,
        AffineCell::Failed => 0b10,
        AffineCell::Done => 0b11,
    }
}

/// Unpack a 2-bit code into its [`AffineCell`]. Total over the 4 codes.
fn bits_to_cell(bits: u8) -> AffineCell {
    match bits & 0b11 {
        0b00 => AffineCell::NotDone,
        0b01 => AffineCell::Queued,
        0b10 => AffineCell::Failed,
        _ => AffineCell::Done,
    }
}

/// Total-order rank for the equal-generation tiebreak (descending:
/// `Done`, `Failed`, `Queued`, `NotDone`). Only consulted when two writes
/// carry the SAME generation (which the single-primary monotone stamp makes
/// vanishingly rare); it exists solely to keep the merge a deterministic,
/// commutative join.
fn cell_rank(cell: AffineCell) -> u8 {
    cell_to_bits(cell)
}

/// One secondary's affine cells: the COMPACT packed-bits value vector
/// (four cells per byte, indexed by affine-id) plus a SPARSE per-cell
/// generation map. Only a cell that has been written away from the default
/// `NotDone` carries a generation entry — the steady state (almost every cell
/// `NotDone` at generation 0) costs ~2 bits/cell, the design's compactness
/// requirement, while LWW still has a stamp for every NON-default cell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SecondaryAffineBits {
    /// Packed 2-bit cells, indexed by affine-id (`affine_id / 4` byte,
    /// `(affine_id % 4) * 2` bit offset). Grows on demand; an out-of-range
    /// affine-id reads as the default `NotDone` (a not-yet-written cell).
    bits: Vec<u8>,
    /// Per-cell LWW stamp, keyed by affine-id. ABSENT ⇒ generation 0 (the
    /// never-written default cell). Sparse — only non-default cells appear.
    generations: HashMap<u32, u64>,
}

impl SecondaryAffineBits {
    /// The cell value for `affine_id` (default `NotDone` for an unwritten one).
    fn cell(&self, affine_id: u32) -> AffineCell {
        let byte = (affine_id / 4) as usize;
        let Some(&packed) = self.bits.get(byte) else {
            return AffineCell::NotDone;
        };
        let shift = (affine_id % 4) * 2;
        bits_to_cell((packed >> shift) & 0b11)
    }

    /// The LWW generation stamp for `affine_id` (0 for an unwritten cell).
    fn generation(&self, affine_id: u32) -> u64 {
        self.generations.get(&affine_id).copied().unwrap_or(0)
    }

    /// Write `cell` at `affine_id` with stamp `generation`, growing the packed
    /// vector as needed. The single packed-bits write seam.
    fn write(&mut self, affine_id: u32, cell: AffineCell, generation: u64) {
        let byte = (affine_id / 4) as usize;
        if byte >= self.bits.len() {
            self.bits.resize(byte + 1, 0);
        }
        let shift = (affine_id % 4) * 2;
        let mask = 0b11u8 << shift;
        self.bits[byte] = (self.bits[byte] & !mask) | (cell_to_bits(cell) << shift);
        self.generations.insert(affine_id, generation);
    }

    /// LWW apply of an incoming `(cell, generation)` at `affine_id`. Returns
    /// `true` iff the local cell CHANGED (the strictly-greater generation, or
    /// equal generation with a strictly-higher value rank, wins). The SINGLE
    /// per-cell convergent join — `set_cell` (live apply) and `merge`
    /// (snapshot/anti-entropy) both route through it, so apply == merge by
    /// construction.
    fn merge_cell(&mut self, affine_id: u32, cell: AffineCell, generation: u64) -> bool {
        let local_gen = self.generation(affine_id);
        let wins = generation > local_gen
            || (generation == local_gen && cell_rank(cell) > cell_rank(self.cell(affine_id)));
        if wins {
            self.write(affine_id, cell, generation);
        }
        wins
    }

    /// The highest generation stamp across this secondary's cells (0 if none) —
    /// the per-secondary input to the failover gen-floor resume.
    fn max_generation(&self) -> u64 {
        self.generations.values().copied().max().unwrap_or(0)
    }
}

/// The affine state sub-store ClusterState owns through a `Box` (one heap
/// allocation): the REPLICATED per-secondary bitvector plus the NODE-LOCAL
/// per-cell LWW generation stamp counter. Boxed so the whole AF-id concern
/// adds exactly ONE pointer to `ClusterState`'s already-large inline footprint
/// (the struct is held by value across `.await` in the operational futures, so
/// inline growth costs stack — the boxing keeps the future-size impact flat).
///
/// Custom `Clone`: the bitvector is replicated (cloned), the gen counter is
/// node-local (RESET — a cloned replica originates nothing inherited from the
/// source, same contract as `task_seq`).
#[derive(Debug, Default)]
pub(crate) struct AffineState {
    bitvector: AffineBitvector,
    /// Node-local monotone LWW stamp source (see the `ClusterState` field doc).
    next_cell_gen: u64,
}

impl Clone for AffineState {
    fn clone(&self) -> Self {
        Self {
            bitvector: self.bitvector.clone(),
            // Node-local — reset on clone (a cloned replica cold-starts its
            // stamp counter; the replicated signal is the per-cell generation
            // already in the cloned bitvector).
            next_cell_gen: 0,
        }
    }
}

impl AffineState {
    /// The replicated bitvector (read seam for the digest / snapshot / queries).
    pub(crate) fn bitvector(&self) -> &AffineBitvector {
        &self.bitvector
    }

    /// Mutable bitvector (the apply / restore-merge write seam).
    pub(crate) fn bitvector_mut(&mut self) -> &mut AffineBitvector {
        &mut self.bitvector
    }

    /// Mint the next node-local LWW stamp (the originator's per-cell stamp
    /// source). Bumps the monotone counter.
    pub(crate) fn next_cell_generation(&mut self) -> u64 {
        let g = self.next_cell_gen;
        self.next_cell_gen += 1;
        g
    }

    /// Re-anchor the stamp counter PAST `floor` (the failover gen-floor resume).
    /// Monotone — never lowers.
    pub(crate) fn resume_cell_gen_floor(&mut self, floor: u64) {
        self.next_cell_gen = self.next_cell_gen.max(floor);
    }
}

/// The replicated per-secondary affine bitvector: `secondary_id → cells`.
/// REPLICATED CRDT state (like `tasks` / `capabilities`) — carried fully by
/// `Clone`, folded into the anti-entropy digest, and round-tripped through
/// snapshot/restore. Unlike the content-addressed `definitions` (whose content
/// the `tasks` fold already implies), this state is GENUINELY independent
/// replicated mutation state, so it IS summarised in the digest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AffineBitvector {
    secondaries: HashMap<String, SecondaryAffineBits>,
}

impl AffineBitvector {
    /// The cell for `(secondary, affine_id)` — `NotDone` for an unwritten one.
    /// The primary read AF-sched's locality ranking consumes.
    ///
    /// AF-sched is the consumer (the locality ranker); ADDITIVE in AF-id, so it
    /// is `#[allow(dead_code)]` until that leaf wires the scheduler — the method
    /// is real + tested, just not yet called outside this crate's tests.
    #[allow(dead_code)]
    pub(crate) fn cell(&self, secondary: &str, affine_id: AffineId) -> AffineCell {
        self.secondaries
            .get(secondary)
            .map_or(AffineCell::NotDone, |b| b.cell(affine_id.0))
    }

    /// LWW-apply a cell write for `(secondary, affine_id)`. Returns `true` iff
    /// local state CHANGED. The single seam the live apply arms call; the
    /// snapshot restore routes through [`Self::merge`], which calls the same
    /// per-cell join.
    pub(crate) fn set_cell(
        &mut self,
        secondary: &str,
        affine_id: AffineId,
        cell: AffineCell,
        generation: u64,
    ) -> bool {
        self.secondaries
            .entry(secondary.to_string())
            .or_default()
            .merge_cell(affine_id.0, cell, generation)
    }

    /// Merge another bitvector into this one per the per-cell LWW lattice —
    /// the snapshot/anti-entropy restore seam. Idempotent + order-insensitive
    /// (the per-cell join is commutative/associative/idempotent).
    pub(crate) fn merge(&mut self, other: &AffineBitvector) {
        for (secondary, bits) in &other.secondaries {
            let local = self.secondaries.entry(secondary.clone()).or_default();
            for (&affine_id, &generation) in &bits.generations {
                local.merge_cell(affine_id, bits.cell(affine_id), generation);
            }
        }
    }

    /// The highest cell generation observed across ALL secondaries (0 if the
    /// bitvector is empty) — the failover-resume floor input: a promoted
    /// primary resumes its cell-generation stamp counter PAST this so a later
    /// write always out-stamps every inherited cell (the LWW total order is
    /// preserved across the epoch boundary).
    pub(crate) fn max_generation(&self) -> u64 {
        self.secondaries
            .values()
            .map(SecondaryAffineBits::max_generation)
            .max()
            .unwrap_or(0)
    }

    // ── digest fold inputs ──

    /// Number of `(secondary, affine_id)` cells that have been WRITTEN away
    /// from the default (i.e. carry a generation stamp). The digest count half.
    pub(crate) fn written_cell_count(&self) -> u64 {
        self.secondaries
            .values()
            .map(|b| b.generations.len() as u64)
            .sum()
    }

    /// Order-independent fold input: each written cell's
    /// `(secondary, affine_id, cell-bits, generation)`. The digest XOR-folds
    /// these so a same-count divergence (a cell at a different value/generation)
    /// is detected and the snapshot pull's LWW restore heals it.
    pub(crate) fn digest_entries(&self) -> impl Iterator<Item = (&str, u32, u8, u64)> {
        self.secondaries.iter().flat_map(|(secondary, bits)| {
            bits.generations.iter().map(move |(&affine_id, &generation)| {
                (secondary.as_str(), affine_id, cell_to_bits(bits.cell(affine_id)), generation)
            })
        })
    }

    /// Clone the wire-portable contents for a snapshot: `secondary →
    /// [(affine_id, cell, generation)]`. A plain owned form so the snapshot
    /// struct carries no in-crate type. The restore rebuilds an
    /// [`AffineBitvector`] and [`Self::merge`]s it.
    pub(crate) fn to_wire(&self) -> HashMap<String, Vec<(u32, AffineCell, u64)>> {
        self.secondaries
            .iter()
            .map(|(secondary, bits)| {
                let cells = bits
                    .generations
                    .iter()
                    .map(|(&affine_id, &generation)| {
                        (affine_id, bits.cell(affine_id), generation)
                    })
                    .collect();
                (secondary.clone(), cells)
            })
            .collect()
    }

    /// Rebuild a bitvector from the wire form (the restore side of
    /// [`Self::to_wire`]). The caller [`Self::merge`]s it into local state.
    pub(crate) fn from_wire(wire: HashMap<String, Vec<(u32, AffineCell, u64)>>) -> Self {
        let mut out = Self::default();
        for (secondary, cells) in wire {
            let bits = out.secondaries.entry(secondary).or_default();
            for (affine_id, cell, generation) in cells {
                bits.write(affine_id, cell, generation);
            }
        }
        out
    }

    // ── AF-sched state-query helpers ──

    /// The set of secondaries on which EVERY affine-id in `affine_ids` is
    /// `Done(11)` — AF-sched's "which secondaries are fully warmed for this
    /// task's affine prereqs" query (the best affine-rank source). An empty
    /// `affine_ids` matches every secondary that has any cell state (a task
    /// with no affine deps is trivially satisfied everywhere — AF-sched treats
    /// the empty case at its own layer; here it falls out as "all-of-none").
    ///
    /// AF-sched consumer; `#[allow(dead_code)]` until that leaf (see `cell`).
    #[allow(dead_code)]
    pub(crate) fn secondaries_with_all_done(
        &self,
        affine_ids: &[AffineId],
    ) -> Vec<String> {
        self.secondaries
            .iter()
            .filter(|(_, bits)| {
                affine_ids
                    .iter()
                    .all(|id| bits.cell(id.0) == AffineCell::Done)
            })
            .map(|(secondary, _)| secondary.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(n: u32) -> AffineId {
        AffineId(n)
    }

    #[test]
    fn cell_round_trips_through_packed_bits() {
        for cell in [AffineCell::NotDone, AffineCell::Queued, AffineCell::Failed, AffineCell::Done] {
            assert_eq!(bits_to_cell(cell_to_bits(cell)), cell);
        }
    }

    #[test]
    fn unwritten_cell_is_not_done() {
        let bv = AffineBitvector::default();
        assert_eq!(bv.cell("s1", aid(0)), AffineCell::NotDone);
        assert_eq!(bv.cell("s1", aid(999)), AffineCell::NotDone);
    }

    #[test]
    fn set_cell_writes_and_packs_multiple_in_one_byte() {
        let mut bv = AffineBitvector::default();
        // Four affine-ids share byte 0 (ids 0..=3).
        assert!(bv.set_cell("s", aid(0), AffineCell::Queued, 1));
        assert!(bv.set_cell("s", aid(1), AffineCell::Done, 1));
        assert!(bv.set_cell("s", aid(2), AffineCell::Failed, 1));
        assert!(bv.set_cell("s", aid(3), AffineCell::NotDone, 1));
        assert_eq!(bv.cell("s", aid(0)), AffineCell::Queued);
        assert_eq!(bv.cell("s", aid(1)), AffineCell::Done);
        assert_eq!(bv.cell("s", aid(2)), AffineCell::Failed);
        assert_eq!(bv.cell("s", aid(3)), AffineCell::NotDone);
    }

    #[test]
    fn lww_higher_generation_wins() {
        let mut bv = AffineBitvector::default();
        assert!(bv.set_cell("s", aid(5), AffineCell::Queued, 1));
        // A higher-generation Done wins.
        assert!(bv.set_cell("s", aid(5), AffineCell::Done, 2));
        assert_eq!(bv.cell("s", aid(5)), AffineCell::Done);
        // A LOWER-generation Failed LOSES (NoOp) — stale write rejected.
        assert!(!bv.set_cell("s", aid(5), AffineCell::Failed, 1));
        assert_eq!(bv.cell("s", aid(5)), AffineCell::Done);
    }

    #[test]
    fn steal_unqueue_converges_via_higher_generation() {
        // The load-bearing case: Queued(gen 3) then the steal's NotDone(gen 4)
        // must WIN (a value max-join would keep Queued and never converge).
        let mut bv = AffineBitvector::default();
        bv.set_cell("s", aid(0), AffineCell::Queued, 3);
        assert!(bv.set_cell("s", aid(0), AffineCell::NotDone, 4));
        assert_eq!(bv.cell("s", aid(0)), AffineCell::NotDone);
    }

    #[test]
    fn failed_is_not_sticky_q1_default() {
        // Q1 default: Failed(gen 1) is overridden by a later Queued(gen 2) and
        // a later Done(gen 3) — failed retries/re-routes are allowed.
        let mut bv = AffineBitvector::default();
        bv.set_cell("s", aid(0), AffineCell::Failed, 1);
        assert!(bv.set_cell("s", aid(0), AffineCell::Queued, 2));
        assert_eq!(bv.cell("s", aid(0)), AffineCell::Queued);
        assert!(bv.set_cell("s", aid(0), AffineCell::Done, 3));
        assert_eq!(bv.cell("s", aid(0)), AffineCell::Done);
    }

    #[test]
    fn merge_is_commutative_and_idempotent() {
        // Two replicas with divergent same-cell writes converge to the SAME
        // value regardless of merge direction (LWW total order).
        let mut a = AffineBitvector::default();
        a.set_cell("s", aid(0), AffineCell::Queued, 2);
        a.set_cell("s", aid(1), AffineCell::Done, 5);
        let mut b = AffineBitvector::default();
        b.set_cell("s", aid(0), AffineCell::NotDone, 3); // steal reset, higher gen
        b.set_cell("s", aid(2), AffineCell::Failed, 1);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab, ba, "merge is commutative");

        // Cell 0: b's gen-3 NotDone beats a's gen-2 Queued.
        assert_eq!(ab.cell("s", aid(0)), AffineCell::NotDone);
        assert_eq!(ab.cell("s", aid(1)), AffineCell::Done);
        assert_eq!(ab.cell("s", aid(2)), AffineCell::Failed);

        // Idempotent: re-merging changes nothing.
        let mut ab2 = ab.clone();
        ab2.merge(&b);
        ab2.merge(&a);
        assert_eq!(ab2, ab);
    }

    #[test]
    fn wire_round_trips() {
        let mut bv = AffineBitvector::default();
        bv.set_cell("s1", aid(0), AffineCell::Done, 4);
        bv.set_cell("s2", aid(7), AffineCell::Queued, 2);
        let rebuilt = AffineBitvector::from_wire(bv.to_wire());
        assert_eq!(rebuilt, bv);
    }

    #[test]
    fn secondaries_with_all_done() {
        let mut bv = AffineBitvector::default();
        bv.set_cell("s1", aid(0), AffineCell::Done, 1);
        bv.set_cell("s1", aid(1), AffineCell::Done, 1);
        bv.set_cell("s2", aid(0), AffineCell::Done, 1);
        bv.set_cell("s2", aid(1), AffineCell::Queued, 1); // not done
        let mut all = bv.secondaries_with_all_done(&[aid(0), aid(1)]);
        all.sort();
        assert_eq!(all, vec!["s1".to_string()]);
    }

    #[test]
    fn max_generation_tracks_highest() {
        let mut bv = AffineBitvector::default();
        assert_eq!(bv.max_generation(), 0);
        bv.set_cell("s1", aid(0), AffineCell::Queued, 3);
        bv.set_cell("s2", aid(0), AffineCell::Done, 9);
        assert_eq!(bv.max_generation(), 9);
    }

    #[test]
    fn digest_count_tracks_written_cells() {
        let mut bv = AffineBitvector::default();
        assert_eq!(bv.written_cell_count(), 0);
        bv.set_cell("s1", aid(0), AffineCell::Queued, 1);
        bv.set_cell("s1", aid(1), AffineCell::Done, 1);
        bv.set_cell("s2", aid(0), AffineCell::Failed, 1);
        assert_eq!(bv.written_cell_count(), 3);
    }
}
