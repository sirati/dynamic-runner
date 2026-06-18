//! Per-secondary CELL bitvector — the KIND-BLIND replicated CRDT state that
//! models, per `(secondary_id, cell_id)`, the completion of a per-secondary
//! def on that secondary. The cell substrate is SHARED by every per-secondary
//! scheduling kind that needs a 2-bit completion cell — today
//! `TaskKind::SecondaryAffine` (the affine scheduler) and
//! `TaskKind::SecondaryEagerPrep` (the idle-filler) — neither of which this
//! layer knows about: it only stores + converges cells indexed by a dense
//! [`SecondaryCellId`].
//!
//! Single concern: WHERE the per-secondary cell state lives, its compact
//! representation, its per-cell convergent merge, and the state-query helpers
//! the per-secondary SCHEDULERS read. It owns NO scheduling: it does not decide
//! WHICH secondary a def goes to, nor place per-secondary queues — it only
//! records and converges the cells the schedulers read/write through the typed
//! API below.
//!
//! ## Cell value
//! Each cell is one [`SecondaryCell`] — `NotDone(00) / Queued(01) / Failed(10) /
//! Done(11)`. The compact store packs the two bits per cell (four cells per
//! byte), indexed by the DENSE cell-id (see `task_def_store::SecondaryCellId`); all
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
//!   equal generation the higher [`SecondaryCell`] discriminant
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
//! * SINGLE-ORIGINATOR INVARIANT (load-bearing — do not break): the monotone
//!   stamp is sound ONLY because the PRIMARY is the SOLE originator of every
//!   cell mutation — all four cell writes (Queued / NotDone-reset / Done /
//!   Failed) originate in primary code and pass through the one broadcast choke
//!   that stamps the generation. Secondaries and the snapshot/anti-entropy
//!   restore path only RECEIVE + merge cells; they NEVER originate one with an
//!   independently-sourced generation. If a future change ever lets a secondary
//!   (or any non-primary path) ORIGINATE a cell write, two writers would mint
//!   colliding/independent stamps and the gen-LWW total order would no longer be
//!   causally correct (a stale write could out-stamp a live one) — that change
//!   MUST route the new origination through the same monotone choke (or replace
//!   the lattice). The cell-ordering correctness of the affine bounce-recovery
//!   reset depends on this.
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
//! WOULD BE localized to [`SecondaryCellBits::merge_cell`] / [`Self::set_cell`]
//! — adding a clamp of a `Failed` cell against an incoming non-`Done` (NO such
//! clamp exists now; this is purely WHERE it would go), NOT a wire change.

use std::collections::HashMap;

use dynrunner_protocol_primary_secondary::SecondaryCell;

use super::task_def_store::SecondaryCellId;

/// Pack an [`SecondaryCell`] into its 2-bit code (the wire/bitvector encoding).
fn cell_to_bits(cell: SecondaryCell) -> u8 {
    match cell {
        SecondaryCell::NotDone => 0b00,
        SecondaryCell::Queued => 0b01,
        SecondaryCell::Failed => 0b10,
        SecondaryCell::Done => 0b11,
    }
}

/// Unpack a 2-bit code into its [`SecondaryCell`]. Total over the 4 codes.
fn bits_to_cell(bits: u8) -> SecondaryCell {
    match bits & 0b11 {
        0b00 => SecondaryCell::NotDone,
        0b01 => SecondaryCell::Queued,
        0b10 => SecondaryCell::Failed,
        _ => SecondaryCell::Done,
    }
}

/// Total-order rank for the equal-generation tiebreak (descending:
/// `Done`, `Failed`, `Queued`, `NotDone`). Only consulted when two writes
/// carry the SAME generation (which the single-primary monotone stamp makes
/// vanishingly rare); it exists solely to keep the merge a deterministic,
/// commutative join.
fn cell_rank(cell: SecondaryCell) -> u8 {
    cell_to_bits(cell)
}

/// One secondary's affine cells: the COMPACT packed-bits value vector
/// (four cells per byte, indexed by affine-id) plus a SPARSE per-cell
/// generation map. Only a cell that has been written away from the default
/// `NotDone` carries a generation entry — the steady state (almost every cell
/// `NotDone` at generation 0) costs ~2 bits/cell, the design's compactness
/// requirement, while LWW still has a stamp for every NON-default cell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SecondaryCellBits {
    /// Packed 2-bit cells, indexed by affine-id (`affine_id / 4` byte,
    /// `(affine_id % 4) * 2` bit offset). Grows on demand; an out-of-range
    /// affine-id reads as the default `NotDone` (a not-yet-written cell).
    bits: Vec<u8>,
    /// Per-cell LWW stamp, keyed by affine-id. ABSENT ⇒ generation 0 (the
    /// never-written default cell). Sparse — only non-default cells appear.
    generations: HashMap<u32, u64>,
}

impl SecondaryCellBits {
    /// The cell value for `affine_id` (default `NotDone` for an unwritten one).
    fn cell(&self, affine_id: u32) -> SecondaryCell {
        let byte = (affine_id / 4) as usize;
        let Some(&packed) = self.bits.get(byte) else {
            return SecondaryCell::NotDone;
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
    fn write(&mut self, affine_id: u32, cell: SecondaryCell, generation: u64) {
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
    fn merge_cell(&mut self, affine_id: u32, cell: SecondaryCell, generation: u64) -> bool {
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
pub(crate) struct SecondaryCellState {
    bitvector: SecondaryCellBitvector,
    /// Node-local monotone LWW stamp source (see the `ClusterState` field doc).
    next_cell_gen: u64,
}

impl Clone for SecondaryCellState {
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

impl SecondaryCellState {
    /// The replicated bitvector (read seam for the digest / snapshot / queries).
    pub(crate) fn bitvector(&self) -> &SecondaryCellBitvector {
        &self.bitvector
    }

    /// Mutable bitvector (the apply / restore-merge write seam).
    pub(crate) fn bitvector_mut(&mut self) -> &mut SecondaryCellBitvector {
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
pub(crate) struct SecondaryCellBitvector {
    secondaries: HashMap<String, SecondaryCellBits>,
}

impl SecondaryCellBitvector {
    /// The cell for `(secondary, affine_id)` — `NotDone` for an unwritten one.
    /// The primary read AF-sched's locality ranking consumes (via
    /// `ClusterState::affine_state` → `primary::affine_dispatch`).
    pub(crate) fn cell(&self, secondary: &str, affine_id: SecondaryCellId) -> SecondaryCell {
        self.secondaries
            .get(secondary)
            .map_or(SecondaryCell::NotDone, |b| b.cell(affine_id.0))
    }

    /// LWW-apply a cell write for `(secondary, affine_id)`. Returns `true` iff
    /// local state CHANGED. The single seam the live apply arms call; the
    /// snapshot restore routes through [`Self::merge`], which calls the same
    /// per-cell join.
    pub(crate) fn set_cell(
        &mut self,
        secondary: &str,
        affine_id: SecondaryCellId,
        cell: SecondaryCell,
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
    pub(crate) fn merge(&mut self, other: &SecondaryCellBitvector) {
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
            .map(SecondaryCellBits::max_generation)
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
    /// [`SecondaryCellBitvector`] and [`Self::merge`]s it.
    pub(crate) fn to_wire(&self) -> HashMap<String, Vec<(u32, SecondaryCell, u64)>> {
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
    pub(crate) fn from_wire(wire: HashMap<String, Vec<(u32, SecondaryCell, u64)>>) -> Self {
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
    /// Part of the AF-sched read API; the current rank policy reads per-cell
    /// state via [`Self::cell`] (counting `Done` OR `Queued`, not just the
    /// all-`Done` subset), leaving this all-`Done` query for a future
    /// locality-aware caller — `#[allow(dead_code)]` until then (real + tested).
    #[allow(dead_code)]
    pub(crate) fn secondaries_with_all_done(
        &self,
        affine_ids: &[SecondaryCellId],
    ) -> Vec<String> {
        self.secondaries
            .iter()
            .filter(|(_, bits)| {
                affine_ids
                    .iter()
                    .all(|id| bits.cell(id.0) == SecondaryCell::Done)
            })
            .map(|(secondary, _)| secondary.clone())
            .collect()
    }

    /// The subset of `cell_ids` that are NON-TERMINAL on `secondary` — i.e. a
    /// cell still worth running there: `NotDone(00)` (never started) or
    /// `Failed(10)` (failed, non-sticky under Q1 so retryable). EXCLUDES
    /// `Queued(01)` (a run is in flight / claimed here) and `Done(11)` (already
    /// completed here). The read query the eager-prep idle-filler consumes to
    /// pick a per-secondary speculative-prep candidate: only a non-terminal cell
    /// is dispatchable, and a `Queued`/`Done` cell must be skipped so the prep
    /// runs at most once-per-secondary (the same per-secondary run-once
    /// authority the affine dispatch reads off `Done`). KIND-BLIND — the
    /// substrate does not know which kind a cell belongs to; the caller passes
    /// only the cell-ids it owns.
    pub(crate) fn non_terminal_cells_for(
        &self,
        secondary: &str,
        cell_ids: &[SecondaryCellId],
    ) -> Vec<SecondaryCellId> {
        let Some(bits) = self.secondaries.get(secondary) else {
            // A secondary with no written cells: every cell reads the default
            // `NotDone`, so all are non-terminal (placeable).
            return cell_ids.to_vec();
        };
        cell_ids
            .iter()
            .copied()
            .filter(|id| {
                matches!(
                    bits.cell(id.0),
                    SecondaryCell::NotDone | SecondaryCell::Failed
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(n: u32) -> SecondaryCellId {
        SecondaryCellId(n)
    }

    #[test]
    fn cell_round_trips_through_packed_bits() {
        for cell in [SecondaryCell::NotDone, SecondaryCell::Queued, SecondaryCell::Failed, SecondaryCell::Done] {
            assert_eq!(bits_to_cell(cell_to_bits(cell)), cell);
        }
    }

    #[test]
    fn unwritten_cell_is_not_done() {
        let bv = SecondaryCellBitvector::default();
        assert_eq!(bv.cell("s1", aid(0)), SecondaryCell::NotDone);
        assert_eq!(bv.cell("s1", aid(999)), SecondaryCell::NotDone);
    }

    #[test]
    fn set_cell_writes_and_packs_multiple_in_one_byte() {
        let mut bv = SecondaryCellBitvector::default();
        // Four affine-ids share byte 0 (ids 0..=3).
        assert!(bv.set_cell("s", aid(0), SecondaryCell::Queued, 1));
        assert!(bv.set_cell("s", aid(1), SecondaryCell::Done, 1));
        assert!(bv.set_cell("s", aid(2), SecondaryCell::Failed, 1));
        assert!(bv.set_cell("s", aid(3), SecondaryCell::NotDone, 1));
        assert_eq!(bv.cell("s", aid(0)), SecondaryCell::Queued);
        assert_eq!(bv.cell("s", aid(1)), SecondaryCell::Done);
        assert_eq!(bv.cell("s", aid(2)), SecondaryCell::Failed);
        assert_eq!(bv.cell("s", aid(3)), SecondaryCell::NotDone);
    }

    #[test]
    fn lww_higher_generation_wins() {
        let mut bv = SecondaryCellBitvector::default();
        assert!(bv.set_cell("s", aid(5), SecondaryCell::Queued, 1));
        // A higher-generation Done wins.
        assert!(bv.set_cell("s", aid(5), SecondaryCell::Done, 2));
        assert_eq!(bv.cell("s", aid(5)), SecondaryCell::Done);
        // A LOWER-generation Failed LOSES (NoOp) — stale write rejected.
        assert!(!bv.set_cell("s", aid(5), SecondaryCell::Failed, 1));
        assert_eq!(bv.cell("s", aid(5)), SecondaryCell::Done);
    }

    #[test]
    fn steal_unqueue_converges_via_higher_generation() {
        // The load-bearing case: Queued(gen 3) then the steal's NotDone(gen 4)
        // must WIN (a value max-join would keep Queued and never converge).
        let mut bv = SecondaryCellBitvector::default();
        bv.set_cell("s", aid(0), SecondaryCell::Queued, 3);
        assert!(bv.set_cell("s", aid(0), SecondaryCell::NotDone, 4));
        assert_eq!(bv.cell("s", aid(0)), SecondaryCell::NotDone);
    }

    #[test]
    fn failed_is_not_sticky_q1_default() {
        // Q1 default: Failed(gen 1) is overridden by a later Queued(gen 2) and
        // a later Done(gen 3) — failed retries/re-routes are allowed.
        let mut bv = SecondaryCellBitvector::default();
        bv.set_cell("s", aid(0), SecondaryCell::Failed, 1);
        assert!(bv.set_cell("s", aid(0), SecondaryCell::Queued, 2));
        assert_eq!(bv.cell("s", aid(0)), SecondaryCell::Queued);
        assert!(bv.set_cell("s", aid(0), SecondaryCell::Done, 3));
        assert_eq!(bv.cell("s", aid(0)), SecondaryCell::Done);
    }

    #[test]
    fn merge_is_commutative_and_idempotent() {
        // Two replicas with divergent same-cell writes converge to the SAME
        // value regardless of merge direction (LWW total order).
        let mut a = SecondaryCellBitvector::default();
        a.set_cell("s", aid(0), SecondaryCell::Queued, 2);
        a.set_cell("s", aid(1), SecondaryCell::Done, 5);
        let mut b = SecondaryCellBitvector::default();
        b.set_cell("s", aid(0), SecondaryCell::NotDone, 3); // steal reset, higher gen
        b.set_cell("s", aid(2), SecondaryCell::Failed, 1);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        assert_eq!(ab, ba, "merge is commutative");

        // Cell 0: b's gen-3 NotDone beats a's gen-2 Queued.
        assert_eq!(ab.cell("s", aid(0)), SecondaryCell::NotDone);
        assert_eq!(ab.cell("s", aid(1)), SecondaryCell::Done);
        assert_eq!(ab.cell("s", aid(2)), SecondaryCell::Failed);

        // Idempotent: re-merging changes nothing.
        let mut ab2 = ab.clone();
        ab2.merge(&b);
        ab2.merge(&a);
        assert_eq!(ab2, ab);
    }

    #[test]
    fn wire_round_trips() {
        let mut bv = SecondaryCellBitvector::default();
        bv.set_cell("s1", aid(0), SecondaryCell::Done, 4);
        bv.set_cell("s2", aid(7), SecondaryCell::Queued, 2);
        let rebuilt = SecondaryCellBitvector::from_wire(bv.to_wire());
        assert_eq!(rebuilt, bv);
    }

    #[test]
    fn secondaries_with_all_done() {
        let mut bv = SecondaryCellBitvector::default();
        bv.set_cell("s1", aid(0), SecondaryCell::Done, 1);
        bv.set_cell("s1", aid(1), SecondaryCell::Done, 1);
        bv.set_cell("s2", aid(0), SecondaryCell::Done, 1);
        bv.set_cell("s2", aid(1), SecondaryCell::Queued, 1); // not done
        let mut all = bv.secondaries_with_all_done(&[aid(0), aid(1)]);
        all.sort();
        assert_eq!(all, vec!["s1".to_string()]);
    }

    #[test]
    fn max_generation_tracks_highest() {
        let mut bv = SecondaryCellBitvector::default();
        assert_eq!(bv.max_generation(), 0);
        bv.set_cell("s1", aid(0), SecondaryCell::Queued, 3);
        bv.set_cell("s2", aid(0), SecondaryCell::Done, 9);
        assert_eq!(bv.max_generation(), 9);
    }

    #[test]
    fn non_terminal_cells_for_returns_notdone_and_failed_only() {
        // The eager-prep filler's candidate query: NotDone + Failed are
        // non-terminal (placeable / retryable); Queued (in flight here) + Done
        // (already ran here) are EXCLUDED so the prep runs at most once.
        let mut bv = SecondaryCellBitvector::default();
        bv.set_cell("s", aid(0), SecondaryCell::NotDone, 1);
        bv.set_cell("s", aid(1), SecondaryCell::Queued, 1);
        bv.set_cell("s", aid(2), SecondaryCell::Failed, 1);
        bv.set_cell("s", aid(3), SecondaryCell::Done, 1);
        // aid(4) is never written ⇒ reads the default NotDone ⇒ non-terminal.
        let ids = [aid(0), aid(1), aid(2), aid(3), aid(4)];
        let got = bv.non_terminal_cells_for("s", &ids);
        assert_eq!(
            got,
            vec![aid(0), aid(2), aid(4)],
            "exactly NotDone + Failed (incl. the unwritten default), order-preserved"
        );

        // A secondary with NO written cells: every queried cell is the default
        // NotDone ⇒ all are non-terminal (placeable).
        let fresh = bv.non_terminal_cells_for("fresh-sec", &ids);
        assert_eq!(fresh, ids.to_vec(), "all cells placeable on a fresh secondary");
    }

    #[test]
    fn digest_count_tracks_written_cells() {
        let mut bv = SecondaryCellBitvector::default();
        assert_eq!(bv.written_cell_count(), 0);
        bv.set_cell("s1", aid(0), SecondaryCell::Queued, 1);
        bv.set_cell("s1", aid(1), SecondaryCell::Done, 1);
        bv.set_cell("s2", aid(0), SecondaryCell::Failed, 1);
        assert_eq!(bv.written_cell_count(), 3);
    }
}
