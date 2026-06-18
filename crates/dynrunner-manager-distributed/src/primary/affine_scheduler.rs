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

use dynrunner_protocol_primary_secondary::{SecondaryCell, ClusterMutation};

use crate::cluster_state::SecondaryCellId;

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
    Affine { affine_id: SecondaryCellId, hash: String },
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
    pub(crate) affine_deps: Vec<(SecondaryCellId, String)>,
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
    /// PER-SECONDARY blocked WORK tasks (#652 concern B): `(secondary, work
    /// hash) → the set of affine import cells still NOT `Done` on that
    /// secondary`. A work task whose import is not yet imported on its chosen
    /// secondary is NOT enqueued onto that secondary's queue (which would
    /// spin-requeue on every `TasksAdded`); it WAITS here until every pending
    /// import cell flips `Done` ([`Self::on_cell_finished`] empties its set and
    /// returns it for enqueue). The edge is PER-SECONDARY because the same
    /// affine work runs on multiple secondaries — a block on `(S, W)` is
    /// independent of `(S', W)`.
    ///
    /// Co-located with `queues` + `placed_work` (the same scheduling-state
    /// concern, reset together by [`Self::clear`] on failover rebuild). The
    /// pool's global-`task_id`-keyed `blocked` map cannot express the
    /// `(import D, secondary S)` readiness this needs (#652 FLAG #3), and the
    /// work task STAYS in the pool bucket as its phase-drain token while blocked
    /// here — this map is a pure per-secondary scheduling overlay, never the
    /// phase-drain authority.
    blocked_per_secondary:
        HashMap<(String, String), std::collections::HashSet<SecondaryCellId>>,
}

impl AffineScheduler {
    /// Drop every local queue AND the placement-idempotency guard — the
    /// failover seam (the queues + guard are local and not replicated; a
    /// promoted primary [`Self::rebuild`]s them from the inherited bitvector).
    pub(crate) fn clear(&mut self) {
        self.queues.clear();
        self.placed_work.clear();
        self.blocked_per_secondary.clear();
    }

    /// Block work `work_hash` on `secondary` until every cell in
    /// `pending_imports` is `Done` there (#652 concern B). Records the
    /// per-secondary blocked entry; the work is NOT enqueued onto the
    /// secondary's queue (the caller dispatches the missing import on-demand and
    /// returns — the work re-enters the queue via [`Self::on_cell_finished`]
    /// when its imports complete). An empty `pending_imports` is a no-op (the
    /// caller should enqueue directly via `place` in that all-`Done` case).
    /// Idempotent: re-blocking the same `(secondary, work_hash)` overwrites with
    /// the current pending set (the freshest gate read).
    pub(crate) fn block_until_import(
        &mut self,
        secondary: &str,
        work_hash: &str,
        pending_imports: Vec<SecondaryCellId>,
    ) {
        if pending_imports.is_empty() {
            return;
        }
        self.blocked_per_secondary.insert(
            (secondary.to_string(), work_hash.to_string()),
            pending_imports.into_iter().collect(),
        );
    }

    /// Bitvector-cell-`Finished` handler (#652 concern B): an affine import's
    /// cell just flipped `Done` on `secondary`. Return + UNBLOCK every work on
    /// THIS secondary that was waiting on `affine_id` — the caller re-enqueues
    /// each onto the secondary's queue via `place` so it RE-POPS + re-gates.
    ///
    /// Why unblock EVERY waiter (not only the now-fully-`Done` ones): a
    /// multi-import work (e.g. `[base, delta]`) waits on the FIRST not-`Done`
    /// import in list order — `dispatch_affine_import_on_demand` kicked only
    /// `base`. When `base` flips `Done` the work must re-pop so the gate sees
    /// `delta` is now the first not-`Done` import and kicks IT (then re-blocks
    /// the work on `delta`). Re-enqueuing on EACH cell-finish drives that
    /// list-order progression one import per real `Finished` EVENT — never a
    /// spin (the work leaves the queue again immediately on the re-block, and
    /// every cycle makes forward progress on exactly one import). A work whose
    /// imports are now ALL `Done` re-pops `Ready` and dispatches. Removing the
    /// entry keeps the map free of stale blocks; the re-pop re-blocks it on its
    /// remaining imports if any.
    pub(crate) fn on_cell_finished(
        &mut self,
        secondary: &str,
        affine_id: SecondaryCellId,
    ) -> Vec<String> {
        let mut freed = Vec::new();
        self.blocked_per_secondary.retain(|(sec, work_hash), pending| {
            if sec == secondary && pending.contains(&affine_id) {
                freed.push(work_hash.clone());
                false // unblock — the caller re-enqueues for a re-pop + re-gate
            } else {
                true
            }
        });
        freed
    }

    /// Bitvector-cell-`Failed` / dead-secondary drain (#652 concern B's
    /// import-FAIL + dead-secondary edges): remove + RETURN every blocked work
    /// on `secondary` (optionally filtered to those waiting on `affine_id`) so
    /// the caller can make a fresh route/terminalize decision RIGHT NOW — never
    /// leaving the work stranded until the 5-min reconcile. `affine_id = None`
    /// drains the whole secondary (dead-secondary); `Some(aid)` drains only the
    /// works waiting on that import (one import failed). The returned works are
    /// no longer blocked here; the caller is responsible for re-placing or
    /// terminalizing them.
    pub(crate) fn drain_blocked_on(
        &mut self,
        secondary: &str,
        affine_id: Option<SecondaryCellId>,
    ) -> Vec<String> {
        let mut drained = Vec::new();
        self.blocked_per_secondary.retain(|(sec, work_hash), pending| {
            if sec != secondary {
                return true;
            }
            let hit = match affine_id {
                Some(aid) => pending.contains(&aid),
                None => true,
            };
            if hit {
                drained.push(work_hash.clone());
                false
            } else {
                true
            }
        });
        drained
    }

    /// 5-min reconcile sweep for the per-secondary blocked map (#652 concern C):
    /// the ORPHAN net for affine-blocked work, the complement of the pool's
    /// `reconcile_blocked`. For each `(secondary, work_hash, pending imports)`,
    /// the entry is ORPHANED — its unblock event will never come — when EITHER:
    ///   * its `secondary` is no longer `reachable` (the dead-secondary drain
    ///     should already have caught this; reconcile is the backstop for a
    ///     miss), OR
    ///   * a pending import has no terminal coming to flip its cell — it is
    ///     neither `Done` (an already-fired `Finished` was lost) nor GENUINELY
    ///     in flight. A `Queued` cell counts as in-flight ONLY when its import is
    ///     actually running on a live worker slot of that secondary
    ///     (`import_in_flight`). A `Queued` cell with NO holding slot is a STALE
    ///     claim — its holder died SILENTLY (no `TaskFailed` bounce emitted, so
    ///     the normal backpressure-arm reset never fired), and no terminal will
    ///     ever flip it `Done`: the M1 (#656) silent-loss orphan.
    ///
    /// An orphaned entry is REMOVED and its `work_hash` RETURNED so the caller
    /// routes the work to the general-queue head (clearing its placement-dedup so
    /// a fresh affine placement re-derives it). A blocked entry whose secondary
    /// is reachable AND every pending import is `Done` or `Queued`-with-a-live-
    /// holding-slot there is healthy (its unblock is still coming) and is left
    /// untouched.
    ///
    /// `reachable(secondary)` is the live-roster predicate; `cell_of(secondary,
    /// affine_id)` is the bitvector read; `import_in_flight(secondary,
    /// affine_id)` answers "is the import for this cell genuinely running on a
    /// live worker slot of this secondary?" (the caller composes it from
    /// `hash_for_cell_id` + `secondary_has_slot_holding_hash` — this module owns
    /// no slot/replicated state, so it never learns the import hash).
    pub(crate) fn reconcile_per_secondary_blocked<R, F, L>(
        &mut self,
        reachable: R,
        cell_of: F,
        import_in_flight: L,
    ) -> Vec<String>
    where
        R: Fn(&str) -> bool,
        F: Fn(&str, SecondaryCellId) -> SecondaryCell,
        L: Fn(&str, SecondaryCellId) -> bool,
    {
        let mut orphaned = Vec::new();
        self.blocked_per_secondary.retain(|(sec, work_hash), pending| {
            let unreachable = !reachable(sec);
            let no_terminal_coming = pending.iter().any(|aid| match cell_of(sec, *aid) {
                SecondaryCell::Done => false, // already finished — no terminal needed
                // `Queued` is "unblock coming" ONLY if a live slot is actually
                // running the import; a `Queued` cell with no holding slot is a
                // silently-lost holder (M1) → no terminal will ever come.
                SecondaryCell::Queued => !import_in_flight(sec, *aid),
                // NotDone / Failed: no terminal coming to flip it `Done`.
                _ => true,
            });
            if unreachable || no_terminal_coming {
                orphaned.push(work_hash.clone());
                false // remove the orphan
            } else {
                true // healthy — unblock still coming
            }
        });
        orphaned
    }

    /// Whether `(secondary, work_hash)` is currently blocked-on-import — the
    /// read seam the concern-B tests assert against.
    #[cfg(test)]
    pub(crate) fn is_blocked_on_import(&self, secondary: &str, work_hash: &str) -> bool {
        self.blocked_per_secondary
            .contains_key(&(secondary.to_string(), work_hash.to_string()))
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

    /// Whether `work_hash` is currently recorded in the placement-dedup guard —
    /// the read seam the requeue-recovery test asserts against (the guard is SET
    /// after placement, CLEARED by `requeue_affine_aware`).
    #[cfg(test)]
    pub(crate) fn is_work_placed(&self, work_hash: &str) -> bool {
        self.placed_work.contains(work_hash)
    }

    /// This secondary's current queue length (0 for an unseen secondary) — the
    /// queue-rank input + the "is the per-secondary queue empty?" idle-steal
    /// precondition.
    pub(crate) fn queue_len(&self, secondary: &str) -> usize {
        self.queues.get(secondary).map_or(0, Vec::len)
    }

    /// DIAGNOSTIC (raw, scheduler-side): every work hash recorded PLACED
    /// (`placed_work`) yet sitting in NO secondary's queue as a `Work` unit and
    /// NOT `block_until_import`-blocked here — the candidate set the coordinator
    /// further narrows (it drops still-in-flight hashes the ledger holds) into
    /// the genuinely-stranded affine-dep-work signal. Unbounded: the caller owns
    /// the in-flight filter AND the log bound, so count and per-hash list can
    /// never disagree about "stranded". Owned here because `placed_work`,
    /// `queues`, and `blocked_per_secondary` are all this module's private state;
    /// the in-flight (ledger) exclusion is deliberately NOT here (no ledger
    /// knowledge crosses into the scheduler).
    pub(crate) fn placed_but_unqueued_hashes_all(&self) -> Vec<String> {
        self.placed_unqueued_iter().cloned().collect()
    }

    /// The shared filter behind both diagnostics: each `placed_work` hash that
    /// sits in NO secondary's queue as a `Work` unit AND is not legitimately
    /// `block_until_import`-blocked here (a blocked work is intentionally absent
    /// from the queue — `on_cell_finished` re-enqueues it when its imports
    /// complete — so it is NOT stranded). One owner so the count and the per-hash
    /// list can never disagree about what "stranded" means. Both exclusions read
    /// this module's own private state (`queues`, `blocked_per_secondary`); the
    /// orthogonal in-flight (ledger) exclusion is applied by the coordinator,
    /// which owns the ledger — this iterator carries no ledger knowledge.
    fn placed_unqueued_iter(&self) -> impl Iterator<Item = &String> {
        self.placed_work.iter().filter(|hash| {
            let queued_somewhere = self
                .queues
                .values()
                .flatten()
                .any(|unit| matches!(unit, QueuedUnit::Work { hash: h } if h == *hash));
            let blocked_on_import = self
                .blocked_per_secondary
                .keys()
                .any(|(_secondary, work_hash)| work_hash == *hash);
            !queued_somewhere && !blocked_on_import
        })
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
        F: Fn(&str, SecondaryCellId) -> SecondaryCell,
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
                    matches!(cell_of(sec, *aid), SecondaryCell::Done | SecondaryCell::Queued)
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
    /// prereqs (each emitting a `SecondaryCellQueued` so the cell goes
    /// `→ Queued`), then append the work task after them. A prereq already
    /// `Done`/`Queued`/`Failed` on `S` is NOT re-appended (and emits nothing) —
    /// the all-done case falls out as "append only the work task".
    ///
    /// Returns the cell mutations the caller must originate (generation is
    /// stamped at the broadcast choke). `cell_of` is the AF-id bitvector read.
    ///
    /// The LIVE placement seam: a `Queued` cell is left as-is (its import is
    /// already claimed AND its node-local unit is alive on this primary's
    /// queue). The failover rebuild instead re-arms a `Queued` cell's LOST unit
    /// via [`Self::place_core`] with a real `import_held` predicate; `place`
    /// supplies the always-held predicate, so a `Queued` cell is never restored
    /// here (its unit already exists).
    /// Lazy-import model (c): placement enqueues ONLY the WORK unit and emits NO
    /// cell mutation. The affine import is NOT a queued/scheduled unit — it is a
    /// DEPENDENCY the work drags in ON-DEMAND when the work actually COMMITS on a
    /// secondary (the dispatch path's `StrandedHere` arm dispatches the import
    /// then, claiming the cell `Queued`). Enqueuing the import ahead of a
    /// committed work is exactly what created the eager-import-then-steal strand
    /// (`importing_nodes != building_nodes`) and the steal-dependent reroute;
    /// deriving it from the work's commitment dissolves both, and the import runs
    /// per-secondary run-once where the bitvector cell is the readiness authority.
    /// `cell_of` is unread here (no placement-time cell claim) but retained on the
    /// signature so the placement / rebuild call sites stay uniform.
    pub(crate) fn place<I, F>(
        &mut self,
        secondary: &str,
        placement: &WorkPlacement,
        cell_of: F,
    ) -> Vec<ClusterMutation<I>>
    where
        F: Fn(&str, SecondaryCellId) -> SecondaryCell,
    {
        let _ = &cell_of;
        self.queues
            .entry(secondary.to_string())
            .or_default()
            .push(QueuedUnit::Work {
                hash: placement.hash.clone(),
            });
        Vec::new()
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
    /// `SecondaryCellUnqueued`) ONLY IF it is currently `Queued`
    /// (Done/Failed/NotDone untouched — `T` relinquishes only its queued
    /// CLAIM), then RE-mark the still-not-done ones `Queued` for `S` (emit
    /// `SecondaryCellQueued`). This is the design's only `01 → 00` site.
    ///
    /// Returns the emitted cell mutations (empty when no donor with a non-empty
    /// queue exists, i.e. nothing to steal). `cell_of` is the AF-id bitvector
    /// read.
    ///
    /// ## Steal-aware eligibility (the (b)+(c) combination)
    ///
    /// `can_steal_work(donor, work_hash)` returns `false` for a work whose affine
    /// import is currently IN FLIGHT on the donor. Under lazy import (c) a work's
    /// import is in flight on a secondary precisely BECAUSE that work committed
    /// there (its pop triggered the on-demand import via `StrandedHere`), so the
    /// work BELONGS there — stealing it to an idle peer would leave the running
    /// import with no dependent (`importing_nodes != building_nodes`). Such a
    /// donor's head unit is skipped in favour of the next-longest donor whose head
    /// work has no in-flight import. This is well-defined ONLY under (c): the
    /// eager model dispatched the import ahead of any commitment, so the same
    /// guard there wrongly pinned works to the first importing secondary and broke
    /// the reroute path; under (c) an import is in flight only for a committed
    /// work, and the failure-reroute is gate-driven (a Failed cell → `Reroute`),
    /// independent of the steal. If no donor is eligible, nothing is stolen.
    pub(crate) fn steal_for<I, F, C>(
        &mut self,
        secondary: &str,
        cell_of: F,
        can_steal_work: C,
    ) -> Vec<ClusterMutation<I>>
    where
        F: Fn(&str, SecondaryCellId) -> SecondaryCell,
        C: Fn(&str, &str) -> bool,
    {
        // Pick the donor T: longest queue, name tie-break, never S itself, must
        // be non-empty AND CONTAIN A WORK unit whose import is steal-eligible (no
        // in-flight import on T). A queue with NO work unit is NOT a donor (#652
        // concern B): under lazy import a queue normally holds only work units,
        // but a refused on-demand import dispatch can leave a BARE `Affine`
        // import unit requeued at its front. That import is per-secondary —
        // committed to T because a dependent there triggered it — so it must
        // NEVER migrate to S (stealing it would dispatch the import on a
        // secondary whose dependent's earlier deps are not yet met — e.g. a
        // delta whose base is still `NotDone` on S). The eligibility +
        // has-work filter is an ADDED filter; the longest/name `max_by`
        // tie-break is unchanged.
        let donor = self
            .queues
            .iter()
            .filter(|(t, q)| {
                t.as_str() != secondary
                    && !q.is_empty()
                    && match q.iter().find(|u| u.is_work()) {
                        Some(QueuedUnit::Work { hash }) => can_steal_work(t, hash),
                        // No work unit (only a bare requeued import) → NOT a
                        // donor: nothing schedulable to steal, and the import
                        // must stay on its own secondary.
                        _ => false,
                    }
            })
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
                && cell_of(&donor, *affine_id) == SecondaryCell::Queued
            {
                mutations.push(ClusterMutation::SecondaryCellUnqueued {
                    secondary: donor.clone(),
                    cell_id: affine_id.0,
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
                && cell_of(secondary, *affine_id) == SecondaryCell::NotDone
            {
                mutations.push(ClusterMutation::SecondaryCellQueued {
                    secondary: secondary.to_string(),
                    cell_id: affine_id.0,
                    generation: 0,
                });
            }
            dest.push(unit);
        }
        mutations
    }

    /// Failover rebuild: discard every local queue, then re-run
    /// [`Self::place_core`] over `placements` (the pending work pool, in a
    /// deterministic order the caller supplies) against the inherited replicated
    /// bitvector via `select_secondary`. Returns the accumulated cell mutations.
    ///
    /// The locality claims survive failover because the bitvector — including
    /// `Queued` — is replicated; re-placing each work lands it on a secondary
    /// whose claims it can see, reproducing the pre-failover layout up to the
    /// deterministic tie-break.
    ///
    /// Under lazy import (c) the rebuild re-derives ONLY the work units — there is
    /// no enqueued import unit to reconstruct. A `Queued` cell on the inherited
    /// bitvector means an import was in flight on its secondary when the prior
    /// primary was lost; it is NOT re-queued here. If that running import
    /// terminals, its cell flips `Done`/`Failed` and the rebuilt work gates
    /// normally; if its holder also died, the work re-derives a fresh import
    /// on-demand when it next commits on a secondary (the `StrandedHere` arm,
    /// which reads the live cell). So the lost prior import is never double-run
    /// and never strands a dependent — the reconstruct-the-stranded-import-unit
    /// step the eager model needed (and its `import_held` discriminator) is gone.
    pub(crate) fn rebuild<I, F>(
        &mut self,
        secondaries: &[String],
        placements: &[WorkPlacement],
        cell_of: F,
    ) -> Vec<ClusterMutation<I>>
    where
        F: Fn(&str, SecondaryCellId) -> SecondaryCell + Copy,
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
        self.affine_placement_from_parts(compute_task_hash(task), &task.task_depends_on)
    }

    /// Build the [`WorkPlacement`] for a ledger entry DIRECTLY from its
    /// `(content-hash key, &TaskState)` — the clone-free placement seam the
    /// `TasksAdded` recheck (`place_dependency_satisfied_affine_tasks`) uses.
    /// Identical output to [`Self::affine_placement_for`] for the same logical
    /// task: the iteration's `hash` key IS the task's content hash (so the
    /// re-derive via `compute_task_hash` is skipped), and the def's compact
    /// dep refs resolve to the SAME string deps a full `task_to_info` rebuild
    /// would produce — fed through the SAME `affine_placement_from_parts`
    /// resolution. The win is purely cost: it never materializes the full
    /// [`TaskInfo`] clone the placement never reads (the placement needs only
    /// the hash + the affine subset of the deps), so a recheck over the live
    /// ledger pays O(live-ledger) pointer reads + per-task dep-ref resolution
    /// instead of O(live-ledger) whole-`TaskInfo` clones.
    pub(crate) fn affine_placement_for_state(
        &self,
        hash: &str,
        state: &crate::cluster_state::TaskState<I>,
    ) -> WorkPlacement {
        let deps = self.cluster_state.resolve_dep_refs(&state.def().task_depends_on);
        self.affine_placement_from_parts(hash.to_string(), &deps)
    }

    /// Shared placement-construction core: resolve the affine subset of `deps`
    /// to `(affine_id, prereq-hash)` pairs and pair with `hash`. The SINGLE
    /// resolution recipe both the `TaskInfo`-based and the state-based seams
    /// delegate to, so the two never drift.
    fn affine_placement_from_parts(
        &self,
        hash: String,
        deps: &[dynrunner_core::TaskDep],
    ) -> WorkPlacement {
        let mut affine_deps = Vec::new();
        for dep in deps {
            if let Some(prereq_hash) =
                self.cluster_state.task_hash_for_dep(&dep.phase_id, &dep.task_id)
            {
                let prereq_hash = prereq_hash.to_string();
                if let Some(aid) = self.cluster_state.affine_id_for_hash(&prereq_hash) {
                    affine_deps.push((aid, prereq_hash));
                }
            }
        }
        WorkPlacement { hash, affine_deps }
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
            ClusterMutation::SecondaryCellFinished {
                secondary: secondary.to_string(),
                cell_id: affine_id.0,
                generation: 0,
            }
        } else {
            ClusterMutation::SecondaryCellFailed {
                secondary: secondary.to_string(),
                cell_id: affine_id.0,
                generation: 0,
            }
        })
    }

    /// Map a per-secondary affine import's BACKPRESSURE bounce onto the
    /// bitvector: reset `(secondary, affine_id)` `Queued → NotDone` (the
    /// `SecondaryCellUnqueued` mutation — the SAME `01 → 00` cell reset
    /// `steal_for` emits when a donor relinquishes its queued claim). `None` ⇒
    /// the hash is not an affine def (an ordinary work task), so nothing
    /// affine-side happens — symmetric with [`Self::affine_terminal_mutation`].
    ///
    /// This is the NON-terminal twin of `affine_terminal_mutation`: a bounce
    /// means the import never ran here, so its cell must NOT go `Failed` (which
    /// would mislead the readiness gate into a `Reroute`/`Unsatisfiable`) and
    /// must NOT stay `Queued` (which would wedge the dependent `InFlightHere`
    /// forever — the import's terminal already left as the swallowed bounce).
    /// Resetting to `NotDone` lets the dependent's next pop read `StrandedHere`
    /// and re-derive the import on-demand.
    pub(crate) fn affine_unqueue_mutation(
        &self,
        secondary: &str,
        task_hash: &str,
    ) -> Option<ClusterMutation<I>> {
        let affine_id = self.cluster_state.affine_id_for_hash(task_hash)?;
        Some(ClusterMutation::SecondaryCellUnqueued {
            secondary: secondary.to_string(),
            cell_id: affine_id.0,
            generation: 0,
        })
    }

    /// Re-queue a recovered work `binary` into the pending pool, affine-aware:
    /// the SINGLE requeue seam every requeue-of-recovered-work path uses (the
    /// backpressure-bounce arm and the dead-secondary recovery), so the
    /// affine-dependent-work recovery is owned in ONE place and the two call
    /// sites read no affine internals.
    ///
    /// The recovery this owns (the #646 twin for affine-DEPENDENT WORK): an
    /// affine-dep work task is withheld from the GLOBAL worker view by
    /// `has_affine_dep` and dispatches ONLY through the per-secondary affine
    /// queue, gated once-per-task by the affine scheduler's `placed_work`
    /// dedup. A plain `pool.requeue` returns the binary to its bucket (correct
    /// — the pool item is its ready-state and the placement candidate
    /// `place_dependency_satisfied_affine_tasks` iterates), but leaves
    /// `placed_work` STILL recorded, so the next placement pass SKIPS
    /// re-deriving its queue unit: the task is hidden in the global pool, absent
    /// from every affine queue, and `placed_work` blocks re-placement —
    /// permanently unassignable. Clearing `placed_work` here (the affine twin of
    /// the import bounce's `Queued → NotDone` cell reset) lets the SAME
    /// same-tick `TasksAdded` recheck re-run `place_dependency_satisfied_affine_tasks`
    /// → re-derive + re-queue the unit onto a rank-selected live-route secondary
    /// → `try_affine_pop` dispatch it. No pool-routing change: the binary stays
    /// a pool item exactly as before; only the local, non-replicated placement
    /// guard is cleared (so the fix is failover-safe by construction — a
    /// promoted primary rebuilds `placed_work` with the queues). A
    /// non-affine-dep work task takes the unchanged `pool.requeue`.
    pub(crate) fn requeue_affine_aware(&mut self, binary: std::sync::Arc<TaskInfo<I>>) {
        if self.pool().has_affine_dep(&binary) {
            let work_hash = compute_task_hash(&binary);
            self.affine_scheduler.unrecord_placed_work(&work_hash);
        }
        self.pool_mut().requeue(binary);
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

    fn aid(n: u32) -> SecondaryCellId {
        SecondaryCellId(n)
    }

    /// A cell map for the `cell_of` closure: `(secondary, affine_id) → cell`.
    #[derive(Default, Clone)]
    struct Cells(HashMap<(String, u32), SecondaryCell>);
    impl Cells {
        fn set(&mut self, sec: &str, aid: SecondaryCellId, cell: SecondaryCell) {
            self.0.insert((sec.to_string(), aid.0), cell);
        }
        fn of(&self) -> impl Fn(&str, SecondaryCellId) -> SecondaryCell + Copy + '_ {
            move |sec: &str, a: SecondaryCellId| {
                self.0
                    .get(&(sec.to_string(), a.0))
                    .copied()
                    .unwrap_or(SecondaryCell::NotDone)
            }
        }
    }

    fn work(hash: &str, deps: &[(SecondaryCellId, &str)]) -> WorkPlacement {
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
    fn place_enqueues_only_work_no_import_prefix() {
        // Lazy-import model (c): placement enqueues ONLY the work unit and emits
        // NO `SecondaryCellQueued` — the import is dispatched ON-DEMAND when the
        // work commits on a secondary (the dispatch path's `StrandedHere` arm),
        // never enqueued ahead of a committed work. This holds regardless of the
        // deps' cell state (both not-done here).
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        let p = work("w1", &[(aid(0), "a0"), (aid(1), "a1")]);
        let muts: Vec<Mutation> = sched.place("s1", &p, cells.of());
        assert_eq!(sched.queue("s1"), &[QueuedUnit::Work { hash: "w1".into() }]);
        assert!(muts.is_empty(), "no placement-time cell claim under lazy import");
    }

    #[test]
    fn place_enqueues_only_work_regardless_of_done_dep() {
        // A `Done` dep changes nothing at placement under (c): still just the
        // work, no mutation. (The cell drives readiness at dispatch, not here.)
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        cells.set("s1", aid(0), SecondaryCell::Done);
        let p = work("w1", &[(aid(0), "a0"), (aid(1), "a1")]);
        let muts: Vec<Mutation> = sched.place("s1", &p, cells.of());
        assert_eq!(sched.queue("s1"), &[QueuedUnit::Work { hash: "w1".into() }]);
        assert!(muts.is_empty());
    }

    #[test]
    fn place_all_done_appends_only_work() {
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        cells.set("s1", aid(0), SecondaryCell::Done);
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
        cells.set("s1", aid(0), SecondaryCell::Done);
        cells.set("s1", aid(1), SecondaryCell::Queued);
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
        // Under lazy import (c) the queue holds only work units; FIFO across two
        // placed works on the same secondary.
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        sched.place::<TaskId, _>("s1", &work("w2", &[(aid(0), "a0")]), cells.of());
        assert_eq!(sched.pop_next("s1"), Some(QueuedUnit::Work { hash: "w1".into() }));
        assert_eq!(sched.pop_next("s1"), Some(QueuedUnit::Work { hash: "w2".into() }));
        assert_eq!(sched.pop_next("s1"), None);
    }

    #[test]
    fn steal_moves_work_unit_no_import_in_queue() {
        // Lazy import (c): the queue holds only the work, so the idle-steal moves
        // just the work unit and emits NO cell mutations (there is no enqueued
        // import claim to unqueue/re-queue). The import re-derives on the new
        // home when the work commits there (the `StrandedHere` on-demand path).
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0"), (aid(1), "a1")]), cells.of());
        let steal: Vec<Mutation> = sched.steal_for("s2", cells.of(), |_, _| true);
        assert!(sched.queue("s1").is_empty());
        assert_eq!(sched.queue("s2"), &[QueuedUnit::Work { hash: "w1".into() }]);
        assert!(
            steal.is_empty(),
            "no enqueued import claim ⇒ steal emits no cell mutations under lazy import"
        );
    }

    #[test]
    fn steal_no_donor_is_noop() {
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        let steal: Vec<Mutation> = sched.steal_for("s2", cells.of(), |_, _| true);
        assert!(steal.is_empty());
        assert!(sched.queue("s2").is_empty());
    }

    #[test]
    fn steal_skips_ineligible_donor_picks_next() {
        // (b)+(c): a donor whose head work is steal-INELIGIBLE (its import is in
        // flight there) is skipped; the next-longest eligible donor is chosen.
        // s1 (len 2, INELIGIBLE) must be passed over for s2 (len 1, eligible).
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        sched.place::<TaskId, _>("s1", &work("w1b", &[(aid(0), "a0")]), cells.of());
        sched.place::<TaskId, _>("s2", &work("w2", &[(aid(0), "a0")]), cells.of());
        // w1 (s1's head) is ineligible; everything else eligible.
        let can_steal = |_donor: &str, work_hash: &str| work_hash != "w1";
        let _steal: Vec<Mutation> = sched.steal_for("s3", cells.of(), can_steal);
        // s1's ineligible head kept it; s2 (eligible) donated its work to s3.
        assert_eq!(sched.queue("s3"), &[QueuedUnit::Work { hash: "w2".into() }]);
        assert!(sched.queue("s2").is_empty());
        assert_eq!(sched.queue("s1").len(), 2, "the ineligible donor is untouched");
    }

    #[test]
    fn steal_all_ineligible_is_noop() {
        // Every donor's head work is ineligible (imports in flight) ⇒ nothing is
        // stolen; the idle worker stays idle (the committed works run on their own
        // secondaries).
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        sched.place::<TaskId, _>("s2", &work("w2", &[(aid(0), "a0")]), cells.of());
        let _steal: Vec<Mutation> = sched.steal_for("s3", cells.of(), |_, _| false);
        assert!(sched.queue("s3").is_empty(), "no eligible donor ⇒ no steal");
        assert_eq!(sched.queue("s1").len(), 1);
        assert_eq!(sched.queue("s2").len(), 1);
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
        sched.steal_for::<TaskId, _, _>("s3", cells.of(), |_, _| true);
        // s2 was longer ⇒ donor; its first unit (work "b") moved to s3.
        assert_eq!(sched.queue("s3"), &[QueuedUnit::Work { hash: "b".into() }]);
        assert_eq!(sched.queue("s2"), &[QueuedUnit::Work { hash: "c".into() }]);
        assert_eq!(sched.queue("s1"), &[QueuedUnit::Work { hash: "a".into() }]);
    }

    #[test]
    fn hint_property_partial_steal_replace_redrives_deps() {
        // The HINT property under lazy import (c): the queue carries only the
        // work; a steal/pop that moves it re-derives the IMPORT on-demand at the
        // new home (the dispatch path's `StrandedHere` arm, not the queue). Here
        // we assert the pure-scheduler half: re-placing a stolen work simply
        // re-enqueues the work on the new secondary (no import unit), and the
        // import is derived later from the work's commitment.
        let mut sched = AffineScheduler::default();
        let cells = Cells::default();
        sched.place::<TaskId, _>("s1", &work("w1", &[(aid(0), "a0")]), cells.of());
        let stranded = sched.pop_next("s1"); // steal the work away from s1
        assert_eq!(stranded, Some(QueuedUnit::Work { hash: "w1".into() }));
        // Re-place on s2: just the work, no mutation. The import re-derives when
        // the work commits on s2 (cell a0 NotDone there → on-demand dispatch).
        let muts: Vec<Mutation> =
            sched.place("s2", &work("w1", &[(aid(0), "a0")]), cells.of());
        assert_eq!(sched.queue("s2"), &[QueuedUnit::Work { hash: "w1".into() }]);
        assert!(muts.is_empty());
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

        // Lazy import (c): rebuild re-derives only work units against the
        // inherited bitvector — no import reconstruction, no held discriminator.
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
    fn place_enqueues_only_work_for_every_cell_state() {
        // Lazy import (c): placement is cell-state-INDEPENDENT — it always
        // enqueues just the work and emits no mutation, for NotDone / Queued /
        // Done deps alike. (The old `place_core` import-reconstruction + the
        // `import_held` discriminator are gone: the import is derived on-demand at
        // dispatch, never reconstructed at placement.)
        for state in [SecondaryCell::NotDone, SecondaryCell::Queued, SecondaryCell::Done] {
            let mut sched = AffineScheduler::default();
            let mut cells = Cells::default();
            cells.set("s1", aid(0), state);
            let p = work("w1", &[(aid(0), "a0")]);
            let muts: Vec<Mutation> = sched.place("s1", &p, cells.of());
            assert_eq!(
                sched.queue("s1"),
                &[QueuedUnit::Work { hash: "w1".into() }],
                "placement enqueues only the work for cell {state:?}"
            );
            assert!(muts.is_empty(), "no placement-time mutation for cell {state:?}");
        }
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

    #[test]
    fn block_until_import_records_and_on_cell_finished_unblocks() {
        // #652 B: block a work on its not-Done import cells; on_cell_finished
        // returns + unblocks every work waiting on that import on that secondary.
        let mut sched = AffineScheduler::default();
        sched.block_until_import("s1", "w1", vec![aid(0), aid(1)]);
        assert!(sched.is_blocked_on_import("s1", "w1"));
        // A cell-finish for an import this work does NOT wait on (different sec)
        // returns nothing.
        assert!(sched.on_cell_finished("s2", aid(0)).is_empty());
        assert!(sched.is_blocked_on_import("s1", "w1"));
        // The first import finishing unblocks the work (re-pop drives the next).
        let freed = sched.on_cell_finished("s1", aid(0));
        assert_eq!(freed, vec!["w1".to_string()]);
        assert!(!sched.is_blocked_on_import("s1", "w1"), "unblocked on cell-finish");
    }

    #[test]
    fn block_until_import_empty_pending_is_noop() {
        let mut sched = AffineScheduler::default();
        sched.block_until_import("s1", "w1", vec![]);
        assert!(!sched.is_blocked_on_import("s1", "w1"), "empty pending → no block");
    }

    #[test]
    fn drain_blocked_on_filters_by_affine_id_or_whole_secondary() {
        // #652 B import-FAIL + dead-secondary edges.
        let mut sched = AffineScheduler::default();
        sched.block_until_import("s1", "w1", vec![aid(0)]);
        sched.block_until_import("s1", "w2", vec![aid(1)]);
        sched.block_until_import("s2", "w3", vec![aid(0)]);
        // Filtered to aid(0) on s1 → only w1.
        let drained = sched.drain_blocked_on("s1", Some(aid(0)));
        assert_eq!(drained, vec!["w1".to_string()]);
        assert!(!sched.is_blocked_on_import("s1", "w1"));
        assert!(sched.is_blocked_on_import("s1", "w2"), "w2 (aid 1) untouched");
        // Whole-secondary drain (dead s1) → w2.
        let drained = sched.drain_blocked_on("s1", None);
        assert_eq!(drained, vec!["w2".to_string()]);
        assert!(sched.is_blocked_on_import("s2", "w3"), "other secondary untouched");
    }

    #[test]
    fn clear_drops_blocked_map() {
        let mut sched = AffineScheduler::default();
        sched.block_until_import("s1", "w1", vec![aid(0)]);
        sched.clear();
        assert!(!sched.is_blocked_on_import("s1", "w1"), "clear drops the blocked map");
    }

    #[test]
    fn reconcile_per_secondary_blocked_returns_orphans_keeps_healthy() {
        // #652 C: a blocked entry on an UNREACHABLE secondary is an orphan; a
        // blocked entry whose import cell is Queued (with a live holding slot) /
        // Done on a reachable secondary is healthy (its unblock is still
        // coming); a blocked entry whose cell is NotDone (no terminal coming) is
        // an orphan.
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        // healthy: cell Queued on reachable s1.
        cells.set("s1", aid(0), SecondaryCell::Queued);
        sched.block_until_import("s1", "healthy", vec![aid(0)]);
        // orphan-A: dead (unreachable) secondary s_dead.
        sched.block_until_import("s_dead", "orphan_dead", vec![aid(0)]);
        // orphan-B: reachable s1 but cell NotDone (no terminal coming).
        sched.block_until_import("s1", "orphan_lost", vec![aid(1)]);

        let reachable = |s: &str| s == "s1";
        // The healthy entry's Queued import IS in flight on a live slot.
        let import_in_flight = |s: &str, a: SecondaryCellId| s == "s1" && a == aid(0);
        let mut orphans =
            sched.reconcile_per_secondary_blocked(reachable, cells.of(), import_in_flight);
        orphans.sort();
        assert_eq!(orphans, vec!["orphan_dead".to_string(), "orphan_lost".to_string()]);
        // The healthy one is kept; the orphans are removed.
        assert!(sched.is_blocked_on_import("s1", "healthy"));
        assert!(!sched.is_blocked_on_import("s_dead", "orphan_dead"));
        assert!(!sched.is_blocked_on_import("s1", "orphan_lost"));
    }

    #[test]
    fn reconcile_orphans_queued_cell_with_no_holding_slot() {
        // #656 M1 (silent-loss): a `Queued` import cell on a REACHABLE secondary
        // whose holder died SILENTLY (no `TaskFailed` bounce → the cell stays
        // `Queued` with NO live slot running the import) is an ORPHAN — no
        // terminal will ever flip it `Done`. The reconcile must drain its blocked
        // dependent so the work re-routes, rather than treating the bare `Queued`
        // claim as "unblock coming" forever.
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        cells.set("s1", aid(0), SecondaryCell::Queued);
        sched.block_until_import("s1", "silent_loss", vec![aid(0)]);

        let reachable = |_: &str| true;
        // No live slot holds the import — the silent-loss case.
        let import_in_flight = |_: &str, _: SecondaryCellId| false;
        let orphans =
            sched.reconcile_per_secondary_blocked(reachable, cells.of(), import_in_flight);
        assert_eq!(orphans, vec!["silent_loss".to_string()]);
        assert!(!sched.is_blocked_on_import("s1", "silent_loss"));
    }

    #[test]
    fn reconcile_keeps_queued_cell_with_a_holding_slot() {
        // #656 M1 (no false orphan): the SAME `Queued` cell, but its import IS
        // genuinely in flight on a live slot — a legitimately in-progress import.
        // The dependent's unblock is still coming, so the entry must be KEPT (not
        // orphaned). This is the discriminator that distinguishes the silent-loss
        // case above from a healthy in-flight import.
        let mut sched = AffineScheduler::default();
        let mut cells = Cells::default();
        cells.set("s1", aid(0), SecondaryCell::Queued);
        sched.block_until_import("s1", "in_flight", vec![aid(0)]);

        let reachable = |_: &str| true;
        let import_in_flight = |s: &str, a: SecondaryCellId| s == "s1" && a == aid(0);
        let orphans =
            sched.reconcile_per_secondary_blocked(reachable, cells.of(), import_in_flight);
        assert!(orphans.is_empty(), "an in-flight Queued import is not orphaned");
        assert!(sched.is_blocked_on_import("s1", "in_flight"));
    }
}
