//! Apply rules + origination seams for the AF-id affine state layer:
//! the affine-id agreement (`SecondaryAffineRegistered`) and the
//! per-secondary bitvector cell writes (`SecondaryAffine{Finished,Queued,
//! Failed,Unqueued}`).
//!
//! Single concern: translating the affine CRDT mutations into bitvector /
//! def-store writes, and supplying the originator + failover seams the
//! scheduler (AF-sched) and the failover path call. It owns NO scheduling.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::AffineCell;

use super::task_def_store::{AffineId, DefBijectionError};
use super::{ApplyOutcome, ClusterState};

impl<I: Identifier> ClusterState<I> {
    /// Apply `SecondaryAffineRegistered`: bind a `SecondaryAffine` def's
    /// content `hash` to its CRDT-agreed dense affine-id. Delegates the
    /// bijection-enforced placement to the def store (the affine twin of the
    /// def-id `intern_at`). A bijection violation (a converged registry never
    /// produces one) is logged LOUD and NoOps — the loud-but-safe drop, exactly
    /// like the def-id `TaskAdded` arm.
    pub(super) fn apply_secondary_affine_registered(
        &mut self,
        hash: &str,
        affine_id: u32,
    ) -> ApplyOutcome {
        // Idempotent on a same-id re-add (at-least-once / snapshot replay):
        // already-bound ⇒ NoOp; a fresh binding ⇒ Applied.
        let already = self.definitions.affine_id_for_hash(hash) == Some(AffineId(affine_id));
        match self.definitions.intern_affine_at(AffineId(affine_id), hash) {
            Ok(_) if already => ApplyOutcome::NoOp,
            Ok(_) => ApplyOutcome::Applied,
            Err(err) => {
                Self::log_affine_bijection_violation(&err);
                ApplyOutcome::NoOp
            }
        }
    }

    /// Apply a per-cell affine bitvector write (the shared body the four named
    /// cell mutations route through — no per-variant duplication). Per-cell LWW
    /// on `generation`: returns `Applied` iff the local cell CHANGED, else
    /// `NoOp` (a stale/equal generation is idempotent under at-least-once +
    /// snapshot replay).
    pub(super) fn apply_secondary_affine_cell(
        &mut self,
        secondary: &str,
        affine_id: u32,
        cell: AffineCell,
        generation: u64,
    ) -> ApplyOutcome {
        if self
            .affine
            .bitvector_mut()
            .set_cell(secondary, AffineId(affine_id), cell, generation)
        {
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::NoOp
        }
    }

    fn log_affine_bijection_violation(err: &DefBijectionError) {
        tracing::error!(
            target: "dynrunner_cluster_state",
            ?err,
            "SecondaryAffineRegistered affine-id BIJECTION violation — the \
             wire-carried (affine_id, hash) contradicts an established binding \
             (a converged content-addressed registry never produces one). \
             Dropping the registration."
        );
        debug_assert!(false, "affine-id bijection violation: {err:?}");
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

    /// PRIMARY-side affine-id reservation for a `SecondaryAffine` def's content
    /// `hash` (idempotent on hash) — the broadcast-stamp seam: the originator
    /// reserves the agreed affine-id here, then emits the matching
    /// `SecondaryAffineRegistered`, so the wire and the originator's own apply
    /// converge on the same id. The affine twin of `allocate_def_id`.
    pub(crate) fn allocate_affine_id(&mut self, hash: &str) -> AffineId {
        self.definitions.alloc_for_affine_hash(hash)
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
        let id_floor = self.definitions.next_affine_id_floor();
        self.definitions.resume_affine_alloc_floor(id_floor);
    }

    // ── AF-sched state-query helpers ──
    //
    // The query surface AF-sched reads to rank secondaries by affine locality +
    // map affine deps to cells.

    /// The affine cell for `(secondary, affine_id)` — AF-sched's locality-rank
    /// input (`Done`/`Queued` count better). `NotDone` for an unwritten cell.
    /// Consumed by `primary::affine_dispatch`'s `cell_of` reads.
    pub(crate) fn affine_state(&self, secondary: &str, affine_id: AffineId) -> AffineCell {
        self.affine.bitvector().cell(secondary, affine_id)
    }

    /// The secondaries on which EVERY affine-id in `affine_ids` is `Done` —
    /// AF-sched's "fully warmed for this task's affine prereqs" query. Part of
    /// the AF-sched read API; the current rank policy reads per-cell state via
    /// [`Self::affine_state`] (so it counts `Done` OR `Queued`, not just the
    /// all-`Done` subset), leaving this all-`Done` query for a future
    /// locality-aware caller — `#[allow(dead_code)]` until then (real + tested).
    #[allow(dead_code)]
    pub(crate) fn secondaries_with_all_done(&self, affine_ids: &[AffineId]) -> Vec<String> {
        self.affine.bitvector().secondaries_with_all_done(affine_ids)
    }

    /// The affine-id bound to a `SecondaryAffine` def's content `hash`, if any —
    /// the seam AF-sched uses to map an affine dep (resolved by content hash) to
    /// its bitvector cell index. Consumed by `affine_placement_for` +
    /// `affine_terminal_mutation`.
    pub(crate) fn affine_id_for_hash(&self, hash: &str) -> Option<AffineId> {
        self.definitions.affine_id_for_hash(hash)
    }
}
