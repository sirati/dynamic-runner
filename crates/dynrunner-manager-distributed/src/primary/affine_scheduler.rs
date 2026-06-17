//! Primary-LOCAL per-secondary affine scheduling — the locality layer that
//! makes `TaskKind::SecondaryAffine` work tasks RUN again on top of the AF-id
//! replicated bitvector (`cluster_state::affine_state`).
//!
//! ## The one concern
//! Decide, for a work task whose non-affine deps are satisfied, WHICH secondary
//! it (and its still-not-done affine prereqs) should be QUEUED on, maintain the
//! resulting per-secondary queues, hand the next queued unit to a worker on a
//! secondary, and rebalance idle secondaries by stealing a whole schedulable
//! unit from the longest queue. This module owns NONE of:
//!   * the dependency graph / non-affine dep resolution (the `PendingPool` owns
//!     that — the caller asks the pool for ready work, exactly as today);
//!   * the bitvector cell state, its merge, or its generation stamp (the AF-id
//!     `cluster_state::affine_state` layer owns that — this module READS cells
//!     and EMITS cell mutations through the typed seams);
//!   * the wire / broadcast (the caller routes the emitted mutations through
//!     `apply_and_broadcast_cluster_mutations`, where the generation is
//!     stamped at the single choke point).
//!
//! ## The model (design 2026-06-16, `affine-rescheduling-design.md`)
//! Each secondary has a LOCAL ordered queue of [`QueuedUnit`]s. A unit is one
//! affine prereq OR one work task; a work task is always appended AFTER its own
//! still-not-done affine prereqs, so the prereqs run first on that secondary.
//!
//! * **affine-as-dependency-only**: an affine task is NEVER placed on a queue on
//!   its own — only as a prereq dragged in by a work task's placement (or moved
//!   wholesale by the idle-steal). The global queue (the `PendingPool`) holds
//!   only the work tasks.
//! * **rank selection** ([`AffineScheduler::select_secondary`]): a work task is
//!   routed to one of the two LOWEST-combined-rank secondaries (affine-rank =
//!   how many of THIS task's affine deps are already done/queued there, more =
//!   better/lower; queue-rank = shorter per-secondary queue = better/lower),
//!   tie-broken DETERMINISTICALLY by the task hash (no `rand`).
//! * **HINT, not a guarantee**: the per-secondary queue is a locality HINT. The
//!   work task stays in the global pool; pulling it from EITHER source re-runs
//!   the SAME append (re-derives + re-queues any missing affine prereq), so a
//!   partial steal that strands a work task without its prereqs is HARMLESS.
//! * **idle-steal** ([`AffineScheduler::steal_for`]): an idle secondary with an
//!   empty queue (and an empty global pool — the caller's precondition) MOVES
//!   the longest queue's PREFIX up to and including the first work task to
//!   itself; each moved affine cell is reset `Queued → NotDone` on the SOURCE
//!   ONLY IF currently `Queued` (the design's only `01 → 00` transition), and
//!   the destination append re-marks the still-not-done ones `Queued`.
//! * **failover-rebuild** ([`AffineScheduler::rebuild`]): the queues are LOCAL,
//!   so a promoted primary discards them and re-runs `place` over the pending
//!   work pool against the inherited (replicated) bitvector — the locality
//!   claims survive because the bitvector (incl. `Queued`) is replicated.

use std::collections::HashMap;

use dynrunner_protocol_primary_secondary::{AffineCell, ClusterMutation};

use crate::cluster_state::AffineId;

/// One placed item on a secondary's local queue: either an affine prereq (its
/// affine-id + content hash, so the dispatch path can resolve the task body)
/// or a work task (its content hash). The hash is the dispatch key — the
/// caller resolves it back to the `Arc<TaskInfo>` through the pool / def store
/// exactly as the global dispatch path does.
///
/// The placement/pop/steal/rebuild surface (this enum, [`WorkPlacement`], and
/// the matching [`AffineScheduler`] methods) is consumed by the operational
/// dispatch LEAF (`primary::affine_dispatch`) — the per-secondary-first pop +
/// idle-steal + placement-trigger call sites + the failover rebuild. The
/// terminal→bitvector seam ([`PrimaryCoordinator::affine_terminal_mutation`]) is
/// wired live into the completion / failure paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueuedUnit {
    /// An affine prereq: run this affine def on the owning secondary. Carries
    /// the affine-id (the bitvector cell key) and the content hash (the
    /// dispatch key).
    Affine { affine_id: AffineId, hash: String },
    /// A work task gated on the affine prereqs queued before it.
    Work { hash: String },
}

impl QueuedUnit {
    /// Whether this unit is a work task — the idle-steal prefix terminator
    /// ("up to AND INCLUDING the first work task").
    fn is_work(&self) -> bool {
        matches!(self, QueuedUnit::Work { .. })
    }
}

/// A work task's placement request: its content hash (the dispatch + tie-break
/// key) plus its affine deps as `(affine_id, hash)` pairs. The caller builds
/// this by resolving the task's `task_depends_on` whose prereq is a
/// `TaskKind::SecondaryAffine` def to its affine-id via
/// `cluster_state::affine_id_for_hash`. A non-affine dep never appears here
/// (it was already satisfied by the pool's ordinary dep check before placement).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkPlacement {
    /// The work task's content hash — the dispatch key and the deterministic
    /// tie-break source for the two-lowest-rank choice.
    pub(crate) hash: String,
    /// This task's affine prereqs: `(affine_id, prereq content hash)`. Empty ⇒
    /// no affine deps; the task is placed with no prereqs appended (the
    /// all-done case falls out as the best affine rank everywhere).
    pub(crate) affine_deps: Vec<(AffineId, String)>,
}

/// The primary-LOCAL per-secondary affine queues + the placement/steal policy.
/// Holds NO replicated state — every cell read/write crosses the typed AF-id
/// seam supplied by the caller (`cell_of`) / returned as mutations.
///
/// The placement / pop / steal / rebuild surface is consumed by the operational
/// dispatch path (the per-secondary-first assignment + idle-steal call sites)
/// and the failover rebuild; the terminal→bitvector seam
/// (`affine_terminal_mutation`) is wired into the live completion / failure
/// paths.
#[derive(Debug, Default)]
pub(crate) struct AffineScheduler {
    /// `secondary_id → ordered local queue`. A missing entry is an empty queue.
    queues: HashMap<String, Vec<QueuedUnit>>,
    /// Content hashes of WORK tasks already PLACED onto a secondary's queue —
    /// the once-per-work-task idempotency guard for the placement trigger (the
    /// `place` policy always appends, so a re-run must not double-queue an
    /// already-placed work task). Co-located with the queues it guards (it is
    /// reset together with them by [`Self::clear`] on failover rebuild). The
    /// CONSUMER records a placed work hash via [`Self::record_placed_work`]
    /// (returns `false` if already placed — the gate) and undoes it via
    /// [`Self::unrecord_placed_work`].
    placed_work: std::collections::HashSet<String>,
}

impl AffineScheduler {
    /// Drop every local queue AND the placement-idempotency guard — the
    /// failover seam (the queues + guard are local and not replicated; a
    /// promoted primary [`Self::rebuild`]s them from the inherited bitvector).
    pub(crate) fn clear(&mut self) {
        self.queues.clear();
        self.placed_work.clear();
    }

    /// Record `work_hash` as PLACED (the placement trigger queued its unit) and
    /// report whether it was newly recorded (`true`) or already present
    /// (`false`) — the once-per-work-task idempotency gate. A `false` return
    /// means a prior placement already queued this work task; the caller skips
    /// re-placing it (the `place` policy would otherwise double-append).
    pub(crate) fn record_placed_work(&mut self, work_hash: &str) -> bool {
        self.placed_work.insert(work_hash.to_string())
    }

    /// Undo a [`Self::record_placed_work`] (the caller recorded a work hash but
    /// then could not place it — e.g. no secondary to select), so a later pass
    /// retries the placement.
    pub(crate) fn unrecord_placed_work(&mut self, work_hash: &str) {
        self.placed_work.remove(work_hash);
    }

    /// This secondary's current queue length (0 for an unseen secondary) — the
    /// queue-rank input + the "is the per-secondary queue empty?" idle-steal
    /// precondition.
    pub(crate) fn queue_len(&self, secondary: &str) -> usize {
        self.queues.get(secondary).map_or(0, Vec::len)
    }

    /// The whole current queue for a secondary (empty slice for an unseen one)
    /// — the read seam this module's unit tests assert layout against.
    #[cfg(test)]
    pub(crate) fn queue(&self, secondary: &str) -> &[QueuedUnit] {
        self.queues.get(secondary).map_or(&[], Vec::as_slice)
    }

    /// Select the secondary a work task should be placed on, by the design's
    /// combined RANK over the candidate `secondaries`:
    ///
    ///   * **affine rank** — secondaries ordered by how many of `placement`'s
    ///     affine deps are already `Done` OR `Queued` there (MORE = lower rank);
    ///   * **queue rank** — secondaries ordered by per-secondary queue length
    ///     (SHORTER = lower rank); equal lengths share a rank.
    ///   * **combined = affine rank + queue rank**; the two LOWEST-combined-rank
    ///     secondaries are the candidates, and the task hash deterministically
    ///     picks BETWEEN them (no `rand` — the hash spreads placements while
    ///     staying replayable on failover rebuild).
    ///
    /// `cell_of(secondary, affine_id)` is the caller-supplied AF-id bitvector
    /// read (`cluster_state::affine_state`). Returns `None` only when
    /// `secondaries` is empty.
    pub(crate) fn select_secondary<F>(
        &self,
        secondaries: &[String],
        placement: &WorkPlacement,
        cell_of: F,
    ) -> Option<String>
    where
        F: Fn(&str, AffineId) -> AffineCell,
    {
        if secondaries.is_empty() {
            return None;
        }

        // affine SCORE per secondary: count deps already Done/Queued there
        // (higher score = better locality). Rank is the dense position in the
        // score order (best score = rank 0); equal scores share a rank.
        let affine_score = |sec: &str| -> usize {
            placement
                .affine_deps
                .iter()
                .filter(|(aid, _)| {
                    matches!(cell_of(sec, *aid), AffineCell::Done | AffineCell::Queued)
                })
                .count()
        };
        let affine_rank = Self::dense_rank(secondaries, |sec| {
            // Higher score = lower (better) rank, so rank by the NEGATED score
            // (via descending sort key).
            std::cmp::Reverse(affine_score(sec))
        });
        let queue_rank = Self::dense_rank(secondaries, |sec| self.queue_len(sec));

        // Two lowest combined ranks, tie-broken deterministically by
        // (combined, secondary name) so the candidate pair is stable across a
        // failover rebuild given the same inputs.
        let mut ranked: Vec<(usize, &String)> = secondaries
            .iter()
            .map(|sec| (affine_rank[sec] + queue_rank[sec], sec))
            .collect();
        ranked.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));

        let top: Vec<&String> = ranked.iter().take(2).map(|(_, s)| *s).collect();
        // Deterministic-but-spread choice between the (at most) two candidates:
        // the task hash's byte sum parity. With a single candidate it returns
        // it; with two it spreads by the hash.
        let pick = Self::hash_spread(&placement.hash) % top.len();
        Some(top[pick].clone())
    }

    /// Place a work task on secondary `S`: append `S`'s STILL-not-done affine
    /// prereqs (each emitting a `SecondaryAffineQueued` so the cell goes
    /// `→ Queued`), then append the work task after them. A prereq already
    /// `Done`/`Queued`/`Failed` on `S` is NOT re-appended (and emits nothing) —
    /// the all-done case falls out as "append only the work task".
    ///
    /// Returns the cell mutations the caller must originate (generation is
    /// stamped at the broadcast choke). `cell_of` is the AF-id bitvector read.
    pub(crate) fn place<I, F>(
        &mut self,
        secondary: &str,
        placement: &WorkPlacement,
        cell_of: F,
    ) -> Vec<ClusterMutation<I>>
    where
        F: Fn(&str, AffineId) -> AffineCell,
    {
        let queue = self.queues.entry(secondary.to_string()).or_default();
        let mut mutations = Vec::new();
        for (aid, hash) in &placement.affine_deps {
            // Only a NOT-done cell is (re-)queued; Done/Queued/Failed are left
            // as-is (Queued already claims this secondary; Done is satisfied;
            // Failed is non-sticky and re-routes via the dependent, not a
            // blind re-queue here).
            if cell_of(secondary, *aid) == AffineCell::NotDone {
                queue.push(QueuedUnit::Affine {
                    affine_id: *aid,
                    hash: hash.clone(),
                });
                mutations.push(ClusterMutation::SecondaryAffineQueued {
                    secondary: secondary.to_string(),
                    affine_id: aid.0,
                    generation: 0,
                });
            }
        }
        queue.push(QueuedUnit::Work {
            hash: placement.hash.clone(),
        });
        mutations
    }

    /// Pop the next queued unit for a worker on secondary `S` (per-secondary
    /// queue FIRST — the global-pool fallback is the CALLER's concern, since the
    /// global pool is the `PendingPool`, not this module). `None` ⇒ `S`'s queue
    /// is empty (the caller then tries the global pool, then the idle-steal).
    pub(crate) fn pop_next(&mut self, secondary: &str) -> Option<QueuedUnit> {
        let queue = self.queues.get_mut(secondary)?;
        if queue.is_empty() {
            return None;
        }
        Some(queue.remove(0))
    }

    /// Re-push a previously-[`Self::pop_next`]'d `unit` to the FRONT of `S`'s
    /// queue — the exact inverse of `pop_next`, for the caller's dispatch
    /// rollback: a popped unit whose worker dispatch then fails (commit refused
    /// / send failed) is returned to the head so it is retried before any other
    /// queued unit, preserving the queue order. A pure FIFO-front queue
    /// primitive (no cell read / mutation): the cell stays whatever `place` /
    /// `steal_for` last set it to, because the unit was popped, not dequeued
    /// from the bitvector's perspective — its `Queued` claim never changed.
    pub(crate) fn requeue_front(&mut self, secondary: &str, unit: QueuedUnit) {
        self.queues
            .entry(secondary.to_string())
            .or_default()
            .insert(0, unit);
    }

    /// Idle-steal for secondary `S` (the caller's precondition: BOTH the global
    /// pool AND `S`'s own queue are empty). Find the secondary `T` with the
    /// LONGEST queue (deterministic name tie-break) and MOVE its prefix — every
    /// unit UP TO AND INCLUDING the first work task (the whole schedulable unit:
    /// the affine prereqs and their dependent work task) — onto `S`.
    ///
    /// For each MOVED affine unit, reset `T`'s cell `Queued → NotDone` (emit
    /// `SecondaryAffineUnqueued`) ONLY IF it is currently `Queued`
    /// (Done/Failed/NotDone untouched — `T` relinquishes only its queued
    /// CLAIM), then RE-mark the still-not-done ones `Queued` for `S` (emit
    /// `SecondaryAffineQueued`). This is the design's only `01 → 00` site.
    ///
    /// Returns the emitted cell mutations (empty when no donor with a non-empty
    /// queue exists, i.e. nothing to steal). `cell_of` is the AF-id bitvector
    /// read.
    pub(crate) fn steal_for<I, F>(&mut self, secondary: &str, cell_of: F) -> Vec<ClusterMutation<I>>
    where
        F: Fn(&str, AffineId) -> AffineCell,
    {
        // Pick the donor T: longest queue, name tie-break, never S itself, must
        // be non-empty.
        let donor = self
            .queues
            .iter()
            .filter(|(t, q)| t.as_str() != secondary && !q.is_empty())
            .max_by(|a, b| a.1.len().cmp(&b.1.len()).then_with(|| b.0.cmp(a.0)))
            .map(|(t, _)| t.clone());
        let Some(donor) = donor else {
            return Vec::new();
        };

        // Split off the prefix [.. first work task INCLUSIVE] from T.
        let donor_queue = self.queues.get_mut(&donor).expect("donor present");
        let cut = donor_queue
            .iter()
            .position(QueuedUnit::is_work)
            .map_or(donor_queue.len(), |i| i + 1);
        let prefix: Vec<QueuedUnit> = donor_queue.drain(..cut).collect();

        // Reset each MOVED affine cell on T (01 → 00 only-if-01).
        let mut mutations = Vec::new();
        for unit in &prefix {
            if let QueuedUnit::Affine { affine_id, .. } = unit
                && cell_of(&donor, *affine_id) == AffineCell::Queued
            {
                mutations.push(ClusterMutation::SecondaryAffineUnqueued {
                    secondary: donor.clone(),
                    affine_id: affine_id.0,
                    generation: 0,
                });
            }
        }

        // Append the moved prefix to S, re-marking still-not-done affine cells
        // Queued for S. The cell read for S must see T's reset already — but the
        // reset is on T's cell, and S's cell is independent, so reading S's
        // current cell is correct (the reset above does not touch S).
        let dest = self.queues.entry(secondary.to_string()).or_default();
        for unit in prefix {
            if let QueuedUnit::Affine { affine_id, .. } = &unit
                && cell_of(secondary, *affine_id) == AffineCell::NotDone
            {
                mutations.push(ClusterMutation::SecondaryAffineQueued {
                    secondary: secondary.to_string(),
                    affine_id: affine_id.0,
                    generation: 0,
                });
            }
            dest.push(unit);
        }
        mutations
    }

    /// Failover rebuild: discard every local queue, then re-run [`Self::place`]
    /// over `placements` (the pending work pool, in a deterministic order the
    /// caller supplies) against the inherited replicated bitvector via
    /// `select_secondary` + `place`. Returns the accumulated cell mutations.
    ///
    /// The locality claims survive failover because the bitvector — including
    /// `Queued` — is replicated; re-placing each task lands it on a secondary
    /// whose claims it can see, reproducing the pre-failover layout up to the
    /// deterministic tie-break. A cell already `Queued` for the chosen secondary
    /// is left as-is by `place` (no duplicate mutation), so re-running on an
    /// already-converged bitvector is a near-no-op.
    pub(crate) fn rebuild<I, F>(
        &mut self,
        secondaries: &[String],
        placements: &[WorkPlacement],
        cell_of: F,
    ) -> Vec<ClusterMutation<I>>
    where
        F: Fn(&str, AffineId) -> AffineCell + Copy,
    {
        self.clear();
        let mut mutations = Vec::new();
        for placement in placements {
            if let Some(sec) = self.select_secondary(secondaries, placement, cell_of) {
                mutations.extend(self.place::<I, F>(&sec, placement, cell_of));
            }
        }
        mutations
    }

    // ── pure helpers ──

    /// Dense rank of each secondary by a `key` (LOWER key = lower/better rank,
    /// rank 0 = best); equal keys SHARE a rank (the design's "ties same rank").
    fn dense_rank<K, F>(secondaries: &[String], key: F) -> HashMap<String, usize>
    where
        K: Ord,
        F: Fn(&str) -> K,
    {
        let mut keyed: Vec<(K, &String)> =
            secondaries.iter().map(|s| (key(s), s)).collect();
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        let mut ranks = HashMap::new();
        let mut rank = 0usize;
        let mut prev: Option<&K> = None;
        for (i, (k, sec)) in keyed.iter().enumerate() {
            if let Some(p) = prev
                && *p != *k
            {
                rank = i;
            }
            ranks.insert((*sec).clone(), rank);
            prev = Some(k);
        }
        ranks
    }

    /// Deterministic spread key from a content hash (byte sum) — the `rand`
    /// substitute the design calls for. Stable per hash, so a failover rebuild
    /// reproduces the same two-candidate choice.
    fn hash_spread(hash: &str) -> usize {
        hash.bytes().fold(0usize, |acc, b| acc.wrapping_add(b as usize))
    }
}

// ── coordinator-facing seam ──
//
// The thin bridge between the pure [`AffineScheduler`] policy above and the
// `PrimaryCoordinator`'s replicated state: build a [`WorkPlacement`] from a
// task's deps (resolving affine prereqs through the AF-id seams), and map a
// worker terminal back onto the bitvector (the design's point 7). The
// `cell_of` closure the placement/steal/rebuild APIs take is built here from
// `cluster_state::affine_state`.

use dynrunner_core::{Identifier, TaskInfo};
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};

use super::wire::compute_task_hash;
use super::PrimaryCoordinator;

impl<S: Scheduler<I>, E: ResourceEstimator<I>, I: Identifier> PrimaryCoordinator<S, E, I> {
    /// Build the [`WorkPlacement`] for `task`: its content hash + the subset of
    /// its `task_depends_on` that resolve to a `TaskKind::SecondaryAffine` def
    /// (i.e. whose prereq hash binds to an affine-id). A non-affine dep is
    /// dropped here — the pool's ordinary dep check already governs it; only
    /// affine deps drive per-secondary locality. The single seam translating a
    /// task's string-identity deps into affine-id placement input.
    ///
    /// Consumed by the dispatch leaf's placement trigger + the failover rebuild
    /// (`primary::affine_dispatch`).
    pub(crate) fn affine_placement_for(&self, task: &TaskInfo<I>) -> WorkPlacement {
        let mut affine_deps = Vec::new();
        for dep in &task.task_depends_on {
            if let Some(hash) = self.cluster_state.task_hash_for_dep(&dep.phase_id, &dep.task_id)
            {
                let hash = hash.to_string();
                if let Some(aid) = self.cluster_state.affine_id_for_hash(&hash) {
                    affine_deps.push((aid, hash));
                }
            }
        }
        WorkPlacement {
            hash: compute_task_hash(task),
            affine_deps,
        }
    }

    /// Map a worker terminal for `task_hash` on `secondary` onto the affine
    /// bitvector (design point 7): if the hash binds to an affine-id, return the
    /// `SecondaryAffine{Finished|Failed}` cell mutation the caller originates
    /// (generation stamped at the broadcast choke). `None` ⇒ the hash is not an
    /// affine def (an ordinary work task), so nothing affine-side happens. The
    /// secondary ran the affine task like any other and reported its terminal;
    /// this is the ONLY affine-specific reaction to that terminal.
    pub(crate) fn affine_terminal_mutation(
        &self,
        secondary: &str,
        task_hash: &str,
        succeeded: bool,
    ) -> Option<ClusterMutation<I>> {
        let affine_id = self.cluster_state.affine_id_for_hash(task_hash)?;
        Some(if succeeded {
            ClusterMutation::SecondaryAffineFinished {
                secondary: secondary.to_string(),
                affine_id: affine_id.0,
                generation: 0,
            }
        } else {
            ClusterMutation::SecondaryAffineFailed {
                secondary: secondary.to_string(),
                affine_id: affine_id.0,
                generation: 0,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A trivial concrete `Identifier` for the mutation type parameter (the
    // affine cell mutations are identifier-generic but carry no identifier
    // payload). `String` satisfies the blanket `Identifier` bound.
    type TaskId = String;
    type Mutation = ClusterMutation<TaskId>;

    fn aid(n: u32) -> AffineId {
        AffineId(n)
    }

    /// A cell map for the `cell_of` closure: `(secondary, affine_id) → cell`.
    #[derive(Default, Clone)]
    struct Cells(HashMap<(String, u32), AffineCell>);
    impl Cells {
        fn set(&mut self, sec: &str, aid: AffineId, cell: AffineCell) {
            self.0.insert((sec.to_string(), aid.0), cell);
        }
        fn of(&self) -> impl Fn(&str, AffineId) -> AffineCell + Copy + '_ {
            move |sec: &str, a: AffineId| {
                self.0
                    .get(&(sec.to_string(), a.0))
                    .copied()
                    .unwrap_or(AffineCell::NotDone)
            }
        }
    }

    fn work(hash: &str, deps: &[(AffineId, &str)]) -> WorkPlacement {
        WorkPlacement {
            hash: hash.to_string(),
            affine_deps: deps
                .iter()
                .map(|(a, h)| (*a, h.to_string()))
                .collect(),
        }
    }

    fn secs(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn place_appends_not_done_prereqs_then_work_and_emits_queued() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        let p = work("w1", &[(aid(0), "a0"), (aid(1), "a1")]);
        let muts: Vec<Mutation> = sched.place("s1", &p, cells.of());
        // Both prereqs not-done ⇒ both queued + appended before the work task.
        assert_eq!(
            sched.queue("s1"),
            &[
                QueuedUnit::Affine { affine_id: aid(0), hash: "a0".into() },
                QueuedUnit::Affine { affine_id: aid(1), hash: "a1".into() },
                QueuedUnit::Work { hash: "w1".into() },
            ]
        );
        assert_eq!(muts.len(), 2);
        assert!(matches!(
            &muts[0],
            ClusterMutation::SecondaryAffineQueued { secondary, affine_id: 0, .. } if secondary == "s1"
        ));
    }

    #[test]
    fn place_skips_done_prereq_no_mutation() {
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        cells.set("s1", aid(0), AffineCell::Done);
        let p = work("w1", &[(aid(0), "a0"), (aid(1), "a1")]);
        let muts: Vec<Mutation> = sched.place("s1", &p, cells.of());
        // a0 done ⇒ not re-appended; a1 not-done ⇒ appended; work last.
        assert_eq!(
            sched.queue("s1"),
            &[
                QueuedUnit::Affine { affine_id: aid(1), hash: "a1".into() },
                QueuedUnit::Work { hash: "w1".into() },
            ]
        );
        assert_eq!(muts.len(), 1);
    }

    #[test]
    fn place_all_done_appends_only_work() {
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        cells.set("s1", aid(0), AffineCell::Done);
        let p = work("w1", &[(aid(0), "a0")]);
        let muts: Vec<Mutation> = sched.place("s1", &p, cells.of());
        assert_eq!(sched.queue("s1"), &[QueuedUnit::Work { hash: "w1".into() }]);
        assert!(muts.is_empty());
    }

    #[test]
    fn select_prefers_secondary_with_deps_done_or_queued() {
        let sched = AffineScheduler::default();
        let mut cells = Cells::default();
        // s1 has both deps done; s2/s3 have none. Equal empty queues.
        cells.set("s1", aid(0), AffineCell::Done);
        cells.set("s1", aid(1), AffineCell::Queued);
        let p = work("w1", &[(aid(0), "a0"), (aid(1), "a1")]);
        let chosen = sched
            .select_secondary(&secs(&["s1", "s2", "s3"]), &p, cells.of())
            .unwrap();
        // s1 is the unique best affine rank AND ties queue rank ⇒ lowest
        // combined; it must be one of the top-2 and (here) the strict best.
        assert_eq!(chosen, "s1");
    }

    #[test]
    fn select_balances_by_queue_length() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        // No affine locality anywhere ⇒ affine rank ties; pure queue-length
        // balance. Make s1 long, s2 short.
        for i in 0..5 {
            sched.place::<TaskId, _>("s1", &work(&format!("pre{i}"), &[]), cells.of());
        }
        let p = work("w1", &[]);
        let chosen = sched
            .select_secondary(&secs(&["s1", "s2"]), &p, cells.of())
            .unwrap();
        // s2 (empty) outranks s1 (len 5) on queue rank, affine ties ⇒ s2 wins.
        assert_eq!(chosen, "s2");
    }

    #[test]
    fn pop_next_is_per_secondary_fifo() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        assert_eq!(
            sched.pop_next("s1"),
            Some(QueuedUnit::Affine { affine_id: aid(0), hash: "a0".into() })
        );
        assert_eq!(sched.pop_next("s1"), Some(QueuedUnit::Work { hash: "w1".into() }));
        assert_eq!(sched.pop_next("s1"), None);
    }

    #[test]
    fn steal_moves_whole_unit_and_unqueues_only_01_on_source() {
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        // Donor s1: place w1 with two prereqs (both not-done → both queued).
        let muts: Vec<Mutation> =
            sched.place("s1", &work("w1", &[(aid(0), "a0"), (aid(1), "a1")]), cells.of());
        // Mirror the place mutations into the cell map so steal sees Queued.
        for m in muts {
            if let ClusterMutation::SecondaryAffineQueued { secondary, affine_id, .. } = m {
                cells.set(&secondary, aid(affine_id), AffineCell::Queued);
            }
        }
        // s2 idle steals from s1.
        let steal: Vec<Mutation> = sched.steal_for("s2", cells.of());
        // s1 drained, s2 holds the whole unit.
        assert!(sched.queue("s1").is_empty());
        assert_eq!(
            sched.queue("s2"),
            &[
                QueuedUnit::Affine { affine_id: aid(0), hash: "a0".into() },
                QueuedUnit::Affine { affine_id: aid(1), hash: "a1".into() },
                QueuedUnit::Work { hash: "w1".into() },
            ]
        );
        // Two Unqueued on s1 (both were 01) + two Queued on s2.
        let unqueued = steal
            .iter()
            .filter(|m| matches!(m, ClusterMutation::SecondaryAffineUnqueued { secondary, .. } if secondary == "s1"))
            .count();
        let queued = steal
            .iter()
            .filter(|m| matches!(m, ClusterMutation::SecondaryAffineQueued { secondary, .. } if secondary == "s2"))
            .count();
        assert_eq!(unqueued, 2);
        assert_eq!(queued, 2);
    }

    #[test]
    fn steal_leaves_done_and_failed_source_cells_untouched() {
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        // Manually build s1's queue with an affine unit whose source cell is
        // Done (e.g. a re-queued prereq that completed elsewhere then moved).
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        // Source cell a0 on s1 is DONE, not Queued ⇒ steal must NOT emit Unqueued.
        cells.set("s1", aid(0), AffineCell::Done);
        let steal: Vec<Mutation> = sched.steal_for("s2", cells.of());
        let unqueued = steal
            .iter()
            .filter(|m| matches!(m, ClusterMutation::SecondaryAffineUnqueued { .. }))
            .count();
        assert_eq!(unqueued, 0, "Done source cell must not be unqueued");
    }

    #[test]
    fn steal_no_donor_is_noop() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        let steal: Vec<Mutation> = sched.steal_for("s2", cells.of());
        assert!(steal.is_empty());
        assert!(sched.queue("s2").is_empty());
    }

    #[test]
    fn steal_picks_longest_queue() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        // s1: one work unit. s2: two work units (longer prefix? prefix is up to
        // first work). Longest TOTAL queue is the donor selector.
        sched.place::<TaskId, _>("s1", &work("a", &[]), cells.of());
        sched.place::<TaskId, _>("s2", &work("b", &[]), cells.of());
        sched.place::<TaskId, _>("s2", &work("c", &[]), cells.of());
        sched.steal_for::<TaskId, _>("s3", cells.of());
        // s2 was longer ⇒ donor; its first unit (work "b") moved to s3.
        assert_eq!(sched.queue("s3"), &[QueuedUnit::Work { hash: "b".into() }]);
        assert_eq!(sched.queue("s2"), &[QueuedUnit::Work { hash: "c".into() }]);
        assert_eq!(sched.queue("s1"), &[QueuedUnit::Work { hash: "a".into() }]);
    }

    #[test]
    fn hint_property_partial_steal_replace_redrives_deps() {
        // The HINT property: a work task pulled from a secondary WITHOUT its
        // affine deps queued there re-derives them on a fresh place — no wedge.
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        // Place w1 + prereq on s1, then pop ONLY the work task (simulating a
        // partial steal that left the work task without its prereq).
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        let _prereq = sched.pop_next("s1"); // drop the prereq (steal it away)
        let stranded = sched.pop_next("s1"); // the work task, now prereq-less here
        assert_eq!(stranded, Some(QueuedUnit::Work { hash: "w1".into() }));
        // Re-placing the same work runs the IDENTICAL procedure: a0 still
        // not-done on the chosen secondary ⇒ re-queued. No special detection.
        let muts: Vec<Mutation> =
            sched.place("s2", &work("w1", &[(aid(0), "a0")]), cells.of());
        assert_eq!(
            sched.queue("s2"),
            &[
                QueuedUnit::Affine { affine_id: aid(0), hash: "a0".into() },
                QueuedUnit::Work { hash: "w1".into() },
            ]
        );
        assert_eq!(muts.len(), 1);
    }

    #[test]
    fn rebuild_is_deterministic_and_reproduces_layout() {
        let cells = Cells::default();
        let placements = vec![
            work("w1", &[(aid(0), "a0")]),
            work("w2", &[(aid(1), "a1")]),
            work("w3", &[]),
        ];
        let secondaries = secs(&["s1", "s2"]);

        let mut a = AffineScheduler::default();
        a.rebuild::<TaskId, _>(&secondaries, &placements, cells.of());
        let mut b = AffineScheduler::default();
        // A pre-failover scheduler with stale queues rebuilds to the SAME layout.
        b.place::<TaskId, _>("s1", &work("garbage", &[]), cells.of());
        b.rebuild::<TaskId, _>(&secondaries, &placements, cells.of());

        for sec in &secondaries {
            assert_eq!(a.queue(sec), b.queue(sec), "rebuild deterministic for {sec}");
        }
        // Every work task is placed somewhere exactly once.
        let total_work: usize = secondaries
            .iter()
            .map(|s| a.queue(s).iter().filter(|u| u.is_work()).count())
            .sum();
        assert_eq!(total_work, 3);
    }

    #[test]
    fn clear_drops_all_queues() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[]), cells.of());
        sched.clear();
        assert!(sched.queue("s1").is_empty());
        assert_eq!(sched.queue_len("s1"), 0);
    }
}
