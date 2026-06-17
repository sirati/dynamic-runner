//! Apply rules + origination seams for the AF-id affine state layer:
//! the affine-id agreement (`SecondaryCellRegistered`) and the
//! per-secondary bitvector cell writes (`SecondaryAffine{Finished,Queued,
//! Failed,Unqueued}`).
//!
//! Single concern: translating the affine CRDT mutations into bitvector /
//! def-store writes, and supplying the originator + failover seams the
//! scheduler (AF-sched) and the failover path call. It owns NO scheduling.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::SecondaryCell;

use super::task_def_store::{SecondaryCellId, DefBijectionError};
use super::{ApplyOutcome, ClusterState};

impl<I: Identifier> ClusterState<I> {
    /// Apply `SecondaryCellRegistered`: bind a cell-bearing def's content
    /// `hash` to its CRDT-agreed dense cell-id. KIND-BLIND (affine + eager-prep
    /// both register here). Delegates the bijection-enforced placement to the
    /// def store (the cell-id twin of the def-id `intern_at`). A bijection
    /// violation (a converged registry never produces one) is logged LOUD and
    /// NoOps — the loud-but-safe drop, exactly like the def-id `TaskAdded` arm.
    pub(super) fn apply_secondary_cell_registered(
        &mut self,
        hash: &str,
        cell_id: u32,
    ) -> ApplyOutcome {
        // Idempotent on a same-id re-add (at-least-once / snapshot replay):
        // already-bound ⇒ NoOp; a fresh binding ⇒ Applied.
        let already = self.definitions.cell_id_for_hash(hash) == Some(SecondaryCellId(cell_id));
        match self.definitions.intern_cell_at(SecondaryCellId(cell_id), hash) {
            Ok(_) if already => ApplyOutcome::NoOp,
            Ok(_) => {
                // Stamp the cell-id INLINE onto the live `TaskState`'s def so
                // the binding is self-describing in a snapshot. The snapshot
                // serializes THIS per-task def by value (NOT the def store's
                // registry, which it discards + rebuilds per-task via
                // `register_restored_def`), so a snapshot-promoted / relocated
                // primary's ONLY carrier of the cell binding is the def's own
                // `affine_id` field. Without this, the restored def resolves
                // `cell_id_for_hash == None`, the dependent's affine deps read
                // EMPTY in `affine_placement_for`, the per-secondary import is
                // never placed, and the run stalls with the import phase held
                // open (the secondary-affine --mode local stall). The
                // `TaskState`'s def is a separate `Arc` from the store slot, so
                // stamping the store slot alone would fork an unstamped per-task
                // copy into the snapshot — the stamp MUST happen here.
                if let Some(state) = self.tasks.get_mut(hash) {
                    std::sync::Arc::make_mut(state.def_mut()).affine_id = Some(SecondaryCellId(cell_id));
                }
                ApplyOutcome::Applied
            }
            Err(err) => {
                Self::log_cell_bijection_violation(&err);
                ApplyOutcome::NoOp
            }
        }
    }

    /// Apply a per-cell bitvector write (the shared body the four named cell
    /// mutations route through — no per-variant duplication, KIND-BLIND).
    /// Per-cell LWW on `generation`: returns `Applied` iff the local cell
    /// CHANGED, else `NoOp` (a stale/equal generation is idempotent under
    /// at-least-once + snapshot replay).
    pub(super) fn apply_secondary_cell_write(
        &mut self,
        secondary: &str,
        cell_id: u32,
        cell: SecondaryCell,
        generation: u64,
    ) -> ApplyOutcome {
        if self
            .affine
            .bitvector_mut()
            .set_cell(secondary, SecondaryCellId(cell_id), cell, generation)
        {
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }

    fn log_cell_bijection_violation(err: &DefBijectionError) {
        tracing::error!(
            target: "dynrunner_cluster_state",
            ?err,
            "SecondaryCellRegistered cell-id BIJECTION violation — the \
             wire-carried (cell_id, hash) contradicts an established binding \
             (a converged content-addressed registry never produces one). \
             Dropping the registration."
        );
        debug_assert!(false, "cell-id bijection violation: {err:?}");
    }

    // ── ORIGINATION seams (the primary stamps the LWW generation here) ──

    /// Mint the next affine cell-generation stamp from the node-local monotone
    /// counter — the originator's per-cell LWW stamp source (the affine twin of
    /// the version counter). The primary calls this once per cell-setting
    /// mutation it originates, so a later write always out-stamps the one it
    /// supersedes (incl. the steal's reset).
    pub(crate) fn next_affine_cell_generation(&mut self) -> u64 {
        self.affine.next_cell_generation()
    }

    /// PRIMARY-side cell-id reservation for a cell-bearing def's content
    /// `hash` (idempotent on hash) — the broadcast-stamp seam: the originator
    /// reserves the agreed cell-id here, then emits the matching
    /// `SecondaryCellRegistered`, so the wire and the originator's own apply
    /// converge on the same id. KIND-BLIND (affine + eager-prep). The cell twin
    /// of `allocate_def_id`.
    pub(crate) fn allocate_cell_id(&mut self, hash: &str) -> SecondaryCellId {
        self.definitions.alloc_for_cell_hash(hash)
    }

    // ── FAILOVER resume seam ──

    /// Re-anchor the node-local affine cell-generation stamp counter PAST every
    /// inherited cell generation — the failover-safety seam a promoted primary
    /// fires at the `PrimaryChanged` epoch advance (the affine twin of
    /// `resume_def_alloc_floor`). Monotone: never lowers. Also re-anchors the
    /// affine-ID allocator past every observed affine-id (the in-memory store's
    /// own `next_affine_id` already tracks that), so a promoted primary never
    /// re-mints a live affine-id NOR a live cell generation.
    pub(crate) fn resume_affine_cell_gen_floor(&mut self) {
        let floor = self.affine.bitvector().max_generation().saturating_add(1);
        self.affine.resume_cell_gen_floor(floor);
        let id_floor = self.definitions.next_cell_id_floor();
        self.definitions.resume_cell_alloc_floor(id_floor);
    }

    // ── AF-sched state-query helpers ──
    //
    // The query surface AF-sched reads to rank secondaries by affine locality +
    // map affine deps to cells.

    /// The affine cell for `(secondary, affine_id)` — AF-sched's locality-rank
    /// input (`Done`/`Queued` count better). `NotDone` for an unwritten cell.
    /// Consumed by `primary::affine_dispatch`'s `cell_of` reads.
    pub(crate) fn affine_state(&self, secondary: &str, affine_id: SecondaryCellId) -> SecondaryCell {
        self.affine.bitvector().cell(secondary, affine_id)
    }

    /// The secondaries on which EVERY affine-id in `affine_ids` is `Done` —
    /// AF-sched's "fully warmed for this task's affine prereqs" query. Part of
    /// the AF-sched read API; the current rank policy reads per-cell state via
    /// [`Self::affine_state`] (so it counts `Done` OR `Queued`, not just the
    /// all-`Done` subset), leaving this all-`Done` query for a future
    /// locality-aware caller — `#[allow(dead_code)]` until then (real + tested).
    #[allow(dead_code)]
    pub(crate) fn secondaries_with_all_done(&self, affine_ids: &[SecondaryCellId]) -> Vec<String> {
        self.affine.bitvector().secondaries_with_all_done(affine_ids)
    }

    /// The affine-id bound to a `SecondaryAffine` def's content `hash`, if any —
    /// the seam AF-sched uses to map an affine dep (resolved by content hash) to
    /// its bitvector cell index. Consumed by `affine_placement_for` +
    /// `affine_terminal_mutation`.
    pub(crate) fn affine_id_for_hash(&self, hash: &str) -> Option<SecondaryCellId> {
        self.definitions.cell_id_for_hash(hash)
    }

    // ── eager-prep cell-substrate queries (#638) ──
    //
    // The read surface the eager-prep idle-filler dispatch leaf consumes. They
    // are kind-blind reads over the SAME cell substrate the affine queries
    // above use — only the candidate SET differs (the def store reports which
    // cell-ids are eager-prep), so the substrate gains zero eager-prep
    // branches.

    /// Every cell-id whose def is a `SecondaryEagerPrep` — the filler's
    /// candidate universe (re-derived from the def kinds, no duplicated set).
    pub(crate) fn eager_prep_cell_ids(&self) -> Vec<SecondaryCellId> {
        self.definitions.eager_prep_cell_ids()
    }

    /// The content hash a cell-id is bound to — the filler maps a chosen
    /// eager-prep cell back to its def hash to reconstruct the dispatch
    /// `TaskInfo`. KIND-BLIND (the same `cell_id → hash` binding affine uses).
    pub(crate) fn hash_for_cell_id(&self, cell_id: SecondaryCellId) -> Option<&str> {
        self.definitions.cell_hash_for_id(cell_id)
    }

    /// The subset of `cell_ids` still non-terminal (NotDone/Failed) on
    /// `secondary` — the filler's "which of my eager-prep cells are still worth
    /// running here" query. Delegates to the kind-blind bitvector read.
    pub(crate) fn non_terminal_cells_for(
        &self,
        secondary: &str,
        cell_ids: &[SecondaryCellId],
    ) -> Vec<SecondaryCellId> {
        self.affine.bitvector().non_terminal_cells_for(secondary, cell_ids)
    }
}
