//! The dispatch CONSUMER of the per-secondary EAGER-PREP primitive (#638) —
//! the operational leaf that makes `TaskKind::SecondaryEagerPrep` work run as
//! the LAST-resort idle filler on the shared per-secondary cell substrate.
//!
//! ## The one concern
//! When an idle worker has nothing else to do — its global-pool view is empty,
//! its secondary's affine queue is empty, AND the affine idle-steal found no
//! donor — speculatively run ONE eager-prep task on its secondary, claiming the
//! per-secondary cell so the prep runs at most once-per-secondary. This is a
//! phase-AGNOSTIC, queue-LESS, CELL-DRIVEN filler (design model A): there is NO
//! eager-prep queue data structure — readiness IS the per-secondary cell. The
//! candidate universe is the def store's eager-prep cell-ids; the per-secondary
//! cell is the run-once authority.
//!
//! ## Module boundary (CLAUDE.md design-first)
//! Owner: the primary. The ONE seam it crosses is the proactive
//! `dispatch_to_idle_workers_chunk` per-worker assignment site, where it slots
//! in AFTER the affine idle-steal as the LAST dispatch precedence. It consumes
//! the kind-blind cell substrate (`eager_prep_cell_ids`, `non_terminal_cells_for`,
//! `hash_for_cell_id`, the `SecondaryCell{Queued,Unqueued}` mutations, and the
//! shared `dispatch_one_assignment` + run-once guards) — it adds NO pool branch
//! (eager-prep is never a pool item) and NO new terminal handler (an eager-prep
//! worker terminal flows through the SAME kind-blind per-secondary cell terminal
//! path the affine import uses, since the cell substrate is kind-blind). This
//! leaf only picks a candidate, claims its cell, and dispatches it.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{ClusterMutation, SecondaryCell};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::lifecycle::dispatch::DispatchOutcome;
use super::PrimaryCoordinator;
use crate::cluster_state::SecondaryCellId;

/// Pick an index into a `len`-element candidate list from a varying `seed` —
/// the spread the idle filler uses to cover ALL non-terminal eager-prep cells
/// over many runs (the dispatch seeds it from a wall-clock nanos source, which
/// varies tick to tick). A pure function so the coverage property is unit-
/// testable without a clock or a `rand` dependency (the codebase deliberately
/// avoids `rand` — see `affine_scheduler`'s deterministic hash-spread). `len`
/// must be > 0 (the caller only calls with a non-empty candidate list).
fn pick_index(seed: u64, len: usize) -> usize {
    debug_assert!(len > 0, "pick_index requires a non-empty candidate list");
    (seed % len as u64) as usize
}

/// A wall-clock nanosecond seed for the spread pick — a cheap, dependency-free
/// varying source (the codebase avoids `rand`). Monotonicity is irrelevant: we
/// only need it to VARY tick to tick so the filler covers every candidate over
/// many runs.
fn spread_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// LAST-resort eager-prep idle filler for the idle worker at `worker_idx`:
    /// the caller's precondition is that this worker has NOTHING else to do —
    /// its global pool view is empty AND the affine pop + affine idle-steal both
    /// found nothing. Speculatively run ONE eager-prep task on its secondary.
    /// Returns `true` iff a prep was committed (the caller advances the budget
    /// snapshot); `false` leaves the worker idle this tick (no candidate, or an
    /// uncommittable one — harmless, a later tick retries).
    ///
    /// Queue-LESS (model A): the candidate set is derived live from the def
    /// store's eager-prep cell-ids filtered to NON-TERMINAL on this secondary
    /// (the per-secondary cell IS the readiness + run-once authority), then a
    /// spread pick chooses one. No data structure to drain, nothing to cancel:
    /// when real work arrives this filler simply is not reached (it is last in
    /// precedence), so a RUNNING prep is never interrupted (Q1 leave-running).
    pub(crate) async fn try_eager_prep_fill_for_worker(&mut self, worker_idx: usize) -> bool {
        let secondary = self.workers[worker_idx].secondary_id.clone();

        // Candidate universe: every eager-prep cell-id, narrowed to the ones
        // still NON-TERMINAL (NotDone/Failed) on THIS secondary — a Queued cell
        // is in flight here and a Done cell already ran here, so neither is a
        // candidate (the per-secondary run-once authority). An empty result =>
        // nothing to speculatively prep here.
        let all_ids = self.cluster_state.eager_prep_cell_ids();
        if all_ids.is_empty() {
            return false;
        }
        let candidates = self
            .cluster_state
            .non_terminal_cells_for(&secondary, &all_ids);
        if candidates.is_empty() {
            return false;
        }

        // Spread pick over the non-terminal candidates (covers all over many
        // runs; the seed varies tick to tick).
        let chosen = candidates[pick_index(spread_seed(), candidates.len())];
        self.dispatch_eager_prep_cell(worker_idx, &secondary, chosen)
            .await
    }

    /// Claim the chosen eager-prep cell `Queued` on `secondary` and dispatch its
    /// task to `worker_idx` through the shared `dispatch_one_assignment` seam.
    /// On a non-committed outcome, reset the cell `Queued → NotDone` (the shared
    /// `SecondaryCellUnqueued` reset) so a later tick re-picks it. The terminal
    /// (Done/Failed) is written by the SAME kind-blind per-secondary cell
    /// terminal path the affine import's worker terminal uses — this leaf owns
    /// only the claim + dispatch + non-commit rollback.
    async fn dispatch_eager_prep_cell(
        &mut self,
        worker_idx: usize,
        secondary: &str,
        cell_id: SecondaryCellId,
    ) -> bool {
        let Some(hash) = self
            .cluster_state
            .hash_for_cell_id(cell_id)
            .map(str::to_string)
        else {
            return false;
        };
        let Some(task) = self.cluster_state.task_info_for_hash(&hash) else {
            // Settled / gone: the cell binding outlived the def. Drop quietly
            // (a fresh registration would re-bind it).
            return false;
        };

        // PER-SECONDARY RUN-ONCE / DISPATCH-ONCE GUARD (the SAME guard the affine
        // import dispatch applies, reused not re-spelled): the cell is `Done` on
        // THIS secondary (it already ran here — the run-once authority) or a slot
        // on THIS secondary already holds the hash (a concurrent dispatch is in
        // flight here). Either way, do not dispatch again — a sibling idle worker
        // racing the same pick is absorbed here.
        let cell_done =
            self.cluster_state.affine_state(secondary, cell_id) == SecondaryCell::Done;
        if cell_done || self.secondary_has_slot_holding_hash(secondary, &hash) {
            return false;
        }

        // Claim the cell `Queued` (the per-secondary in-flight claim; the cell
        // generation is stamped at the broadcast choke). NOT a pool take — an
        // eager-prep task is never a pool item, so there is no bucket accounting
        // here (the modular win: zero pool branches).
        self.apply_and_broadcast_cluster_mutations(vec![ClusterMutation::SecondaryCellQueued {
            secondary: secondary.to_string(),
            cell_id: cell_id.0,
            generation: 0,
        }])
        .await;

        let estimated = self.estimator.estimate(&task);
        match self
            .dispatch_one_assignment(worker_idx, std::sync::Arc::new(task), estimated)
            .await
        {
            DispatchOutcome::Committed => true,
            DispatchOutcome::CommitRefused(_) | DispatchOutcome::SendFailed(_) => {
                // The prep never reached the worker — its cell must NOT stay
                // `Queued` (which would wedge it un-runnable here forever, no
                // terminal coming) and must NOT go `Failed` (it never ran).
                // Reset `Queued → NotDone` (the shared cell un-queue) so a later
                // tick re-picks it. No pool requeue: an eager-prep dispatch took
                // nothing out of a bucket.
                self.apply_and_broadcast_cluster_mutations(vec![
                    ClusterMutation::SecondaryCellUnqueued {
                        secondary: secondary.to_string(),
                        cell_id: cell_id.0,
                        generation: 0,
                    },
                ])
                .await;
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_index_covers_all_candidates_over_many_seeds() {
        // The spread pick must reach EVERY candidate index over many runs (the
        // varying-seed property), so no eager-prep cell is starved.
        for len in 1..=8usize {
            let mut seen = vec![false; len];
            for seed in 0..(len as u64 * 50) {
                let idx = pick_index(seed, len);
                assert!(idx < len, "index in range");
                seen[idx] = true;
            }
            assert!(
                seen.iter().all(|&s| s),
                "every candidate index covered for len={len}"
            );
        }
    }

    #[test]
    fn pick_index_stays_in_range_for_large_seeds() {
        // A wall-clock nanos seed is large; the modulo must keep the index in
        // bounds regardless.
        assert_eq!(pick_index(u64::MAX, 3), (u64::MAX % 3) as usize);
        assert!(pick_index(u64::MAX, 3) < 3);
    }
}
