//! CRDT-derived statistics snapshot.
//!
//! # Single concern
//!
//! Read a `&ClusterState<I>` once and project it into the flat
//! [`StatsSnapshot`] the reporter formats + diffs. Everything here is a
//! pure function of the replicated ledger: there is no authority, no
//! pool, and no wall-clock — exactly the inputs a zero-authority
//! observer holds.
//!
//! # Why recompute here (not read pool accessors)
//!
//! An observer holds NO `PendingPool` — it carries only the replicated
//! `ClusterState`. So "waiting-on-deps" and "ready-in-queue", which on
//! the primary are pool-state reads, are RECOMPUTED here from the CRDT:
//! the `Pending` task set and each task's `task_depends_on` for the
//! per-task dep check, and the per-phase dispatchability read off
//! [`ClusterState::phase_rollups`] — the single owning accessor in
//! `dynrunner-manager-distributed` that encodes the phase state machine
//! (a phase is dispatchable once every phase it depends on has fully
//! terminated). Reading the rollup keeps this file from re-deriving the
//! dispatchability walk; the per-task dep resolution (a dep is satisfied
//! once its prereq is terminal) stays here because it is a TASK-level,
//! not phase-level, predicate.

use std::collections::{HashMap, HashSet};

use dynrunner_core::Identifier;

use crate::{ClusterState, TaskState};

/// One flat projection of the CRDT at an instant. Every field is a
/// non-negative count except `per_secondary_in_flight`, which the idle
/// detector consumes. The reporter diffs successive snapshots and
/// applies the >0-and-changed inclusion rule against this shape.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// `OutcomeSummary::succeeded` — unique-hash completion count.
    pub succeeded: usize,
    /// `OutcomeSummary::setup_succeeded` — succeeded setup-kind tasks. A
    /// SUCCESS-LIKE terminal in its OWN line, NEVER folded into
    /// `succeeded` (the setup-task counter contract: a succeeded setup
    /// task never inflates the worker-work success count).
    pub setup_succeeded: usize,
    /// `OutcomeSummary::fail_retry` — last-observed `Recoverable`.
    pub fail_retry: usize,
    /// `OutcomeSummary::fail_oom` — last-observed memory `ResourceExhausted`.
    pub fail_oom: usize,
    /// `NonRecoverable` / non-memory `ResourceExhausted`, with BOTH the
    /// discrete `Unfulfillable` AND the discrete `InvalidTask` sets
    /// NETTED OUT — see the trap note at the assignment in
    /// `from_cluster_state`. Reported as a line disjoint from both
    /// `unfulfillable` and `invalid_task`.
    pub fail_final: usize,
    /// `StateCounts::unfulfillable` — the DISCRETE `TaskState::Unfulfillable`
    /// count. Read from `counts()`, NOT `outcome_counts()`: the latter
    /// folds Unfulfillable into `fail_final`, which would double-count
    /// it here. This is its OWN reported line.
    pub unfulfillable: usize,
    /// `StateCounts::invalid_task` — the DISCRETE `TaskState::InvalidTask`
    /// count (terminal, non-reinjectable structural failures: missing
    /// dep / duplicate id). Read from `counts()`, NOT `outcome_counts()`,
    /// for exactly the same reason as `unfulfillable`: `outcome_counts()`
    /// folds `InvalidTask` into `fail_final` (see the accessor's
    /// `InvalidTask => fail_final += 1` arm), so reading it from the
    /// outcome bucket and then reporting a discrete line would
    /// double-count. This is its OWN reported line, disjoint from
    /// `fail_final`.
    pub invalid_task: usize,
    /// Tasks in `TaskState::InFlight` — currently executing somewhere.
    pub in_flight: usize,
    /// `StateCounts::queued_after_local_dependency` — work tasks assigned
    /// to a secondary but WAITING on that secondary's local SecondaryAffine
    /// import (#497). Read from `counts()`: a NON-TERMINAL live state kept
    /// disjoint from `in_flight` (the task is committed but not yet
    /// running). Its OWN reported line so the operator sees work parked
    /// behind a per-secondary import.
    pub queued_after_local_dependency: usize,
    /// secondary-id → number of `TaskState::QueuedAfterLocalDependency`
    /// tasks WAITING on that secondary's local import (#497). Only
    /// secondaries with ≥1 queued task appear — so the observable
    /// projection NAMES the secondary `S` parking work behind its import.
    pub per_secondary_queued_after_local_dep: HashMap<String, usize>,
    /// `Pending` tasks with at least one UNsatisfied `task_depends_on`
    /// prereq (the prereq is not yet terminal). Recomputed from the
    /// CRDT; the pool's `blocked` map analog.
    pub waiting_on_deps: usize,
    /// `TaskState::Blocked` count — cascade-paused dependents of an
    /// `Unfulfillable` prereq. A SEPARATE category from `waiting_on_deps`
    /// per owner-decision C-2.
    pub blocked: usize,
    /// `Pending` tasks whose deps are all satisfied AND whose phase is
    /// dispatchable (every dep-phase fully terminated). The CRDT analog
    /// of the pool's queued-in-active-phase count.
    pub ready_in_queue: usize,
    /// secondary-id → number of `TaskState::InFlight` tasks it is
    /// running. Only secondaries with ≥1 in-flight task appear; the
    /// idle detector folds this against its accumulated known-secondary
    /// set.
    pub per_secondary_in_flight: HashMap<String, usize>,
    /// The LIVE peer-secondary roster — `alive_secondary_members()`
    /// (worker_count > 0 ∧ `is_peer_alive`). The idle detector PRUNES its
    /// accumulated gates against this so a secondary that has DEPARTED
    /// (`PeerRemoved` ⇒ `peer_state = Dead`) — e.g. a scancelled
    /// ex-primary, whose sticky `SecondaryCapacity` record keeps it in
    /// `known_secondaries()` but NOT in the alive set — stops being
    /// reported as a perpetually-idle ghost. A pure CRDT-liveness fact,
    /// available to a zero-authority observer.
    pub alive_secondaries: HashSet<String>,
    /// Occupancy NUMERATOR: distinct secondaries with ≥1 `InFlight`
    /// task. Identically `per_secondary_in_flight.len()` (that map keys
    /// exactly the secondaries with ≥1 in-flight task), surfaced as its
    /// own field so the reporter need not know the map's "busy" meaning.
    pub busy_secondaries: usize,
    /// Occupancy DENOMINATOR: total known secondaries —
    /// `ClusterState::known_secondaries().count()` (the CRDT roster of
    /// every secondary with a replicated capacity record).
    pub total_secondaries: usize,
    /// Occupancy NUMERATOR: distinct `(secondary, worker)` pairs with an
    /// `InFlight` task — the count of worker slots currently executing.
    /// Collected as a set while iterating the `InFlight` entries so a
    /// secondary running N tasks on N distinct workers contributes N
    /// (not 1).
    pub busy_workers: usize,
    /// Occupancy DENOMINATOR: total advertised worker slots across every
    /// secondary — `ClusterState::total_worker_count()` (sum of each
    /// secondary's replicated `worker_count`).
    pub total_workers: usize,
    /// #575 averaged aggregated resource stats — each field is the
    /// arithmetic mean across the compute secondaries that have
    /// emitted at least one `SecondaryResourceSampleRecord` (the
    /// `live_compute_resource_samples` accessor's filter). `None`
    /// when NO secondary has yet emitted a sample (the freshly-
    /// promoted-primary / cold-start window before the 5-minute
    /// cadence has fired); the reporter's per-field render arm omits
    /// a `None` metric without consuming its baseline (the next emit
    /// then trips the zero-baseline include).
    ///
    /// Per-field independence: each field is fed by the SAME accessor
    /// iteration and folded INDEPENDENTLY; a secondary that emitted a
    /// sample contributes to every field, but the 25% threshold
    /// against the last-printed baseline is decided per field by the
    /// reporter (the snapshot only carries the averaged values; the
    /// inclusion gate lives in `format.rs`).
    pub avg_mem_p10_bytes: Option<u64>,
    pub avg_mem_p30_bytes: Option<u64>,
    pub avg_mem_p50_bytes: Option<u64>,
    pub avg_mem_p70_bytes: Option<u64>,
    pub avg_mem_p90_bytes: Option<u64>,
    pub avg_mem_avg_bytes: Option<u64>,
    pub avg_total_free_memory_bytes: Option<u64>,
    pub avg_total_swap_used_bytes: Option<u64>,
    pub avg_total_free_swap_bytes: Option<u64>,
    pub avg_cpu_utilization_milli: Option<u32>,
    /// #589 loop-health: arithmetic mean of every alive compute
    /// secondary's `oploop_iters_per_sec_milli` over secondaries whose
    /// LATEST sample carries a NON-ZERO value (a zero is the cold-
    /// start / no-signal sentinel — pre-#589 secondaries decode to
    /// zero via serde-default, and a freshly-spawned loop's first emit
    /// is zero by the tracker's seed-then-return-Default contract).
    /// `None` when NO live secondary has a non-zero reading. Excluding
    /// zeros from the denominator keeps the fleet average from being
    /// dragged toward the cold-start sentinel by a single just-
    /// respawned secondary. Necessary-but-not-sufficient is the host
    /// CPU% line ALONE; pairing the host axis with this loop-rate axis
    /// catches the #586 oom_sweep class (low host CPU + low iter-rate).
    pub avg_oploop_iters_per_sec_milli: Option<u64>,
    /// #589 loop-health: the SINGLE alive compute secondary with the
    /// HIGHEST `dominant_arm_pct_milli` across the fleet — paired with
    /// its arm name. `None` when no live secondary has a non-empty
    /// dominant-arm name (every alive secondary is either pre-#589
    /// (serde-default empty string) or cold-start (the tracker's seed
    /// emit). Max-by-pct because the operator's signal is "is ANY
    /// secondary's loop monopolised by a non-inbox arm" — the fleet
    /// average would average a hot secondary down to noise.
    pub dominant_arm: Option<DominantArm>,
    /// #589 loop-health: the fleet maximum of every alive compute
    /// secondary's `max_unacked_for_secs` — the longest unACKed
    /// confirmable report age ANYWHERE in the fleet at the latest emit
    /// each secondary broadcast. `None` when no live secondary has a
    /// non-zero reading (steady-state, no buffered replays). Max
    /// because the blackholed-but-live leg (#352) class is a per-
    /// secondary problem — the fleet average dilutes it to invisibility.
    pub max_unacked_for_secs: Option<u32>,
}

/// #589 loop-health: the fleet's single hottest arm, by share of the
/// originating secondary's emit-window iteration deltas. The (name,
/// pct) pair the format layer renders as "dominant arm:
/// `oom_sweep`:55.0%". Lives on the snapshot (not as two separate
/// `Option<String>` / `Option<u32>` fields) so the format layer
/// renders ONE atomic line — a non-empty name without a pct, or vice
/// versa, would be a structural lie the type system rules out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DominantArm {
    /// The arm name from the winning secondary (e.g. `"oom_sweep"`).
    pub arm_name: String,
    /// That arm's share of the secondary's emit-window iteration
    /// deltas, in milli-percent (`55_000` = 55.0%).
    pub pct_milli: u32,
}

impl StatsSnapshot {
    /// Project a `&ClusterState` into a flat snapshot. `O(n)` over the
    /// ledger (a handful of single passes; not hot-path code — runs on
    /// a 10-minute cadence).
    ///
    /// # Production caller is the live-feed seam
    ///
    /// This is the projection the observer run loop publishes into the
    /// reporter's `SharedSnapshotSource`. The producer is the
    /// `ObserverCoordinator` run loop, which calls this on each iteration
    /// and pushes the result into the reporter's snapshot source via
    /// `SharedSnapshotSource::publish` (the reporter's own test suite also
    /// exercises it directly).
    /// Skip-predicate for the 10-min periodic report: returns `true` iff
    /// every field that DIFFERS between `self` and `prev` is in the
    /// skip-eligible set — i.e. the only movement since the last
    /// announcement is routine throughput OR a high-frequency
    /// resource-stat reading (#575). A `true` return tells the driver
    /// to elide this 10-minute emission (the 1-hour safety net will
    /// print the accumulated delta later); a `false` return means at
    /// least one non-eligible field moved and the report must run on
    /// the normal cadence.
    ///
    /// Skip-eligible set:
    ///   * Throughput counters: `succeeded`, `fail_retry`, `fail_oom`,
    ///     `fail_final` (owner-decision 2026-06-15).
    ///   * #575 averaged resource stats: `avg_mem_p10/p30/p50/p70/p90/avg`,
    ///     `avg_total_free_memory`, `avg_total_swap_used`,
    ///     `avg_total_free_swap`, `avg_cpu_utilization_milli` — these
    ///     are continuous measurements with a per-field 25% inclusion
    ///     gate inside `format.rs`; a resource-stat-only change must
    ///     NOT force a 10-min print (high-frequency noise; brief #575).
    ///     When the OUTER predicate fires and the print does happen
    ///     (because something OUTSIDE the skip set changed, OR the
    ///     1-hour safety net fires, OR a force-print was signalled),
    ///     `format.rs::render_report` then runs the 25% gate on each
    ///     resource line to decide actual line inclusion.
    ///   * #589 loop-health: `avg_oploop_iters_per_sec_milli`,
    ///     `dominant_arm`, `max_unacked_for_secs` — same rationale.
    ///     The 25%-threshold-against-last-printed gate in `format.rs`
    ///     decides whether each loop-health line is actually included
    ///     when an outside-the-skip-set field forces the print.
    ///
    /// Subset semantics (not strict equality): an all-equal snapshot
    /// (diff = ∅) trivially satisfies "all changes are in the eligible
    /// set" and returns `true` — the spec elides such ticks too (an
    /// empty report has nothing wake-worthy; the operator uses SIGUSR1
    /// to force a heartbeat read).
    ///
    /// Scope rationale (owner-decision 2026-06-15): `unfulfillable`,
    /// `invalid_task`, and `setup_succeeded` are EXCLUDED — they are
    /// exceptional-flow categories (structural failures, capability-loss
    /// cascades, setup-task outcomes), not routine throughput, and the
    /// operator wants those changes promptly. The maps
    /// (`per_secondary_in_flight`, `per_secondary_queued_after_local_dep`)
    /// and the roster (`alive_secondaries`) are also excluded so a peer
    /// joining/leaving or a task moving between secondaries is never
    /// skipped.
    pub fn diff_subset_of_skip_eligible(&self, prev: &Self) -> bool {
        // Destructure once so a future field addition forces a compile
        // error here — every snapshot field must be classified
        // "skip-eligible counter" or "must-print-on-change".
        let Self {
            succeeded: _cur_succeeded,
            setup_succeeded,
            fail_retry: _cur_fail_retry,
            fail_oom: _cur_fail_oom,
            fail_final: _cur_fail_final,
            unfulfillable,
            invalid_task,
            in_flight,
            queued_after_local_dependency,
            per_secondary_queued_after_local_dep,
            waiting_on_deps,
            blocked,
            ready_in_queue,
            per_secondary_in_flight,
            alive_secondaries,
            busy_secondaries,
            total_secondaries,
            busy_workers,
            total_workers,
            // #575 resource-stat averages are SKIP-ELIGIBLE — see the
            // method docs. Bound with `_` prefix so the destructure
            // stays exhaustive (a future resource field is a compile
            // error here until classified), but skipped from the
            // equality comparisons below: a resource-only change
            // never trips this predicate.
            avg_mem_p10_bytes: _,
            avg_mem_p30_bytes: _,
            avg_mem_p50_bytes: _,
            avg_mem_p70_bytes: _,
            avg_mem_p90_bytes: _,
            avg_mem_avg_bytes: _,
            avg_total_free_memory_bytes: _,
            avg_total_swap_used_bytes: _,
            avg_total_free_swap_bytes: _,
            avg_cpu_utilization_milli: _,
            // #589 loop-health fields are SKIP-ELIGIBLE for the same
            // reason as the #575 resource averages: continuous
            // measurements with a per-field 25% inclusion gate in
            // `format.rs`. A loop-health-only change must NOT force a
            // 10-min print; the operator gets the change on the next
            // outside-the-skip-set movement OR on the 1-hour safety
            // net. Bound with `_` prefix so the destructure stays
            // exhaustive (a future loop-health field is a compile
            // error here until classified).
            avg_oploop_iters_per_sec_milli: _,
            dominant_arm: _,
            max_unacked_for_secs: _,
        } = self;
        setup_succeeded == &prev.setup_succeeded
            && unfulfillable == &prev.unfulfillable
            && invalid_task == &prev.invalid_task
            && in_flight == &prev.in_flight
            && queued_after_local_dependency == &prev.queued_after_local_dependency
            && per_secondary_queued_after_local_dep
                == &prev.per_secondary_queued_after_local_dep
            && waiting_on_deps == &prev.waiting_on_deps
            && blocked == &prev.blocked
            && ready_in_queue == &prev.ready_in_queue
            && per_secondary_in_flight == &prev.per_secondary_in_flight
            && alive_secondaries == &prev.alive_secondaries
            && busy_secondaries == &prev.busy_secondaries
            && total_secondaries == &prev.total_secondaries
            && busy_workers == &prev.busy_workers
            && total_workers == &prev.total_workers
    }

    pub fn from_cluster_state<I: Identifier>(state: &ClusterState<I>) -> Self {
        let outcome = state.outcome_counts();
        let counts = state.counts();

        // task_id → terminal? Built once so the per-task dep walk is
        // O(deps) rather than O(ledger) per dep. Terminal == the pool's
        // resolution rule: Completed | Failed | Unfulfillable. Blocked,
        // Pending and InFlight are NOT terminal (a dep on any of them is
        // unsatisfied).
        let mut terminal_task_ids: HashSet<&str> = HashSet::new();
        for (_, st) in state.tasks_iter() {
            if st.is_terminal() {
                let id = task_id_of(st);
                if !id.is_empty() {
                    terminal_task_ids.insert(id);
                }
            }
        }
        // SETTLED (spilled) entries are terminal by construction; their
        // ids come off the slim index (the fat iterator above no longer
        // yields them).
        for (_, entry) in state.settled_entries() {
            if !entry.task_id.is_empty() {
                terminal_task_ids.insert(entry.task_id.as_str());
            }
        }

        // Per-phase derived view (has_any / has_live / dispatchable),
        // recomputed from the CRDT by the single owning accessor in
        // `dynrunner-manager-distributed`. The `dispatchable` bit drives
        // the ready-in-queue derivation below; reading it from the
        // accessor removes the previously-duplicated `phase_has_live`
        // loop + `phase_dispatchable` walk that this file used to carry.
        let phase_rollups = state.phase_rollups();

        let mut waiting_on_deps = 0usize;
        let mut ready_in_queue = 0usize;
        let mut per_secondary_in_flight: HashMap<String, usize> = HashMap::new();
        // Distinct `(secondary, worker)` slots currently executing — the
        // worker-occupancy numerator. A set (not a sum of the per-
        // secondary counts) so that if the CRDT ever showed two InFlight
        // entries pinned to the SAME slot mid-reorder the slot still
        // counts once.
        let mut busy_worker_slots: HashSet<(&str, dynrunner_core::WorkerId)> = HashSet::new();
        // secondary → number of work tasks WAITING on that secondary's
        // local SecondaryAffine import (#497). The observable projection of
        // the `QueuedAfterLocalDependency` state: NAMES the parking secondary.
        let mut per_secondary_queued_after_local_dep: HashMap<String, usize> = HashMap::new();

        for (_, st) in state.tasks_iter() {
            match st {
                TaskState::InFlight {
                    secondary, worker, ..
                } => {
                    *per_secondary_in_flight
                        .entry(secondary.clone())
                        .or_insert(0) += 1;
                    busy_worker_slots.insert((secondary.as_str(), *worker));
                }
                // A work task queued behind secondary `S`'s local import —
                // tallied PER `S` so the observer reports which secondary is
                // parking work behind its per-secondary import (#497).
                TaskState::QueuedAfterLocalDependency { secondary, .. } => {
                    *per_secondary_queued_after_local_dep
                        .entry(secondary.clone())
                        .or_insert(0) += 1;
                }
                TaskState::Pending { .. } => {
                    let def = st.def();
                    let deps_satisfied = def
                        .task_depends_on
                        .iter()
                        .all(|d| terminal_task_ids.contains(d.task_id.as_str()));
                    if !deps_satisfied {
                        waiting_on_deps += 1;
                    } else if phase_rollups
                        .get(&def.phase_id)
                        .is_some_and(|r| r.dispatchable)
                    {
                        ready_in_queue += 1;
                    }
                    // else: deps satisfied but the phase is still gated
                    // on an upstream phase — not "ready", not
                    // "waiting-on-deps"; intentionally surfaced by
                    // neither category (mirrors the pool, where such an
                    // item sits in a Blocked-phase bucket).
                }
                _ => {}
            }
        }

        // #575 aggregated resource stats — average each field across
        // EVERY compute secondary that has emitted a sample. The fold
        // is a single pass over `live_compute_resource_samples()`; a
        // secondary that has not yet emitted is excluded from the
        // denominator for ALL fields (per-secondary atomicity — a
        // present record contributes to every field).
        //
        // #589 loop-health is folded in the same pass with PER-FIELD
        // denominators (zero values are the cold-start / wire-elided
        // sentinel): a freshly-respawned secondary or a pre-#589
        // peer contributes the present #575 fields but is excluded
        // from the loop-health denominators until it has a non-zero
        // reading. Dominant arm is max-by-pct (not averaged) — a hot
        // secondary's signal would average down to noise across an
        // otherwise-quiet fleet.
        let (resource_sum, resource_n, loop_health_sum) = state
            .live_compute_resource_samples()
            .fold(
                (
                    ResourceFieldSums::default(),
                    0u64,
                    LoopHealthFieldSums::default(),
                ),
                |(mut acc, n, mut lh), (_id, record)| {
                    acc.mem_p10 = acc.mem_p10.saturating_add(record.mem_p10_bytes as u128);
                    acc.mem_p30 = acc.mem_p30.saturating_add(record.mem_p30_bytes as u128);
                    acc.mem_p50 = acc.mem_p50.saturating_add(record.mem_p50_bytes as u128);
                    acc.mem_p70 = acc.mem_p70.saturating_add(record.mem_p70_bytes as u128);
                    acc.mem_p90 = acc.mem_p90.saturating_add(record.mem_p90_bytes as u128);
                    acc.mem_avg = acc.mem_avg.saturating_add(record.mem_avg_bytes as u128);
                    acc.total_free_memory = acc
                        .total_free_memory
                        .saturating_add(record.total_free_memory_bytes as u128);
                    acc.total_swap_used = acc
                        .total_swap_used
                        .saturating_add(record.total_swap_used_bytes as u128);
                    acc.total_free_swap = acc
                        .total_free_swap
                        .saturating_add(record.total_free_swap_bytes as u128);
                    acc.cpu_utilization_milli = acc
                        .cpu_utilization_milli
                        .saturating_add(record.cpu_utilization_milli as u128);
                    // #589 loop-health fold-in:
                    //   - iter-rate: arithmetic mean over secondaries
                    //     with a NON-ZERO reading (zero = cold-start /
                    //     pre-#589 / wire-elided sentinel).
                    //   - dominant arm: max-by-pct over secondaries
                    //     with a NON-EMPTY name.
                    //   - max-unacked: fleet max over secondaries with
                    //     a NON-ZERO reading.
                    if record.oploop_iters_per_sec_milli > 0 {
                        lh.iters_sum = lh
                            .iters_sum
                            .saturating_add(record.oploop_iters_per_sec_milli as u128);
                        lh.iters_n += 1;
                    }
                    if !record.dominant_arm_name.is_empty()
                        && record.dominant_arm_pct_milli > lh.dominant_pct_max
                    {
                        lh.dominant_pct_max = record.dominant_arm_pct_milli;
                        lh.dominant_name = Some(record.dominant_arm_name.clone());
                    }
                    if record.max_unacked_for_secs > lh.unacked_max {
                        lh.unacked_max = record.max_unacked_for_secs;
                        lh.unacked_seen = true;
                    }
                    (acc, n + 1, lh)
                },
            );
        let avg_mem_p10_bytes = avg_u64_from_sum(resource_sum.mem_p10, resource_n);
        let avg_mem_p30_bytes = avg_u64_from_sum(resource_sum.mem_p30, resource_n);
        let avg_mem_p50_bytes = avg_u64_from_sum(resource_sum.mem_p50, resource_n);
        let avg_mem_p70_bytes = avg_u64_from_sum(resource_sum.mem_p70, resource_n);
        let avg_mem_p90_bytes = avg_u64_from_sum(resource_sum.mem_p90, resource_n);
        let avg_mem_avg_bytes = avg_u64_from_sum(resource_sum.mem_avg, resource_n);
        let avg_total_free_memory_bytes =
            avg_u64_from_sum(resource_sum.total_free_memory, resource_n);
        let avg_total_swap_used_bytes =
            avg_u64_from_sum(resource_sum.total_swap_used, resource_n);
        let avg_total_free_swap_bytes =
            avg_u64_from_sum(resource_sum.total_free_swap, resource_n);
        let avg_cpu_utilization_milli =
            avg_u32_from_sum(resource_sum.cpu_utilization_milli, resource_n);
        // #589 loop-health aggregates from the per-field denominators.
        let avg_oploop_iters_per_sec_milli =
            avg_u64_from_sum(loop_health_sum.iters_sum, loop_health_sum.iters_n);
        let dominant_arm =
            loop_health_sum
                .dominant_name
                .map(|arm_name| DominantArm {
                    arm_name,
                    pct_milli: loop_health_sum.dominant_pct_max,
                });
        let max_unacked_for_secs = if loop_health_sum.unacked_seen {
            Some(loop_health_sum.unacked_max)
        } else {
            None
        };

        StatsSnapshot {
            succeeded: outcome.succeeded,
            setup_succeeded: outcome.setup_succeeded,
            fail_retry: outcome.fail_retry,
            fail_oom: outcome.fail_oom,
            // `outcome_counts().fail_final` FOLDS BOTH the discrete
            // `TaskState::Unfulfillable` AND the discrete
            // `TaskState::InvalidTask` sets into itself (see the
            // accessor: `Unfulfillable => fail_final += 1` and
            // `InvalidTask => fail_final += 1`). Since `unfulfillable`
            // and `invalid_task` are each reported as their OWN line,
            // subtract BOTH here so the three lines are disjoint and a
            // single failed task is not double-counted across two
            // metrics. This is the complement of the documented
            // `counts()`-vs-`outcome_counts()` trap: read
            // `counts().{unfulfillable,invalid_task}` for the discrete
            // lines AND net them back out of `fail_final`.
            fail_final: outcome
                .fail_final
                .saturating_sub(counts.unfulfillable)
                .saturating_sub(counts.invalid_task),
            unfulfillable: counts.unfulfillable,
            invalid_task: counts.invalid_task,
            in_flight: counts.in_flight,
            queued_after_local_dependency: counts.queued_after_local_dependency,
            per_secondary_queued_after_local_dep,
            waiting_on_deps,
            blocked: counts.blocked,
            ready_in_queue,
            // Occupancy numerators are derived from the live InFlight
            // entries collected above; the denominators are read from
            // the D1 replicated-capacity accessors. Both are pure
            // functions of the CRDT (D1 capacity × D2 InFlight) — no
            // authority, no pool, no primary-local state.
            busy_secondaries: per_secondary_in_flight.len(),
            total_secondaries: state.known_secondaries().count(),
            busy_workers: busy_worker_slots.len(),
            total_workers: state.total_worker_count() as usize,
            per_secondary_in_flight,
            alive_secondaries: state.alive_secondary_members().map(String::from).collect(),
            avg_mem_p10_bytes,
            avg_mem_p30_bytes,
            avg_mem_p50_bytes,
            avg_mem_p70_bytes,
            avg_mem_p90_bytes,
            avg_mem_avg_bytes,
            avg_total_free_memory_bytes,
            avg_total_swap_used_bytes,
            avg_total_free_swap_bytes,
            avg_cpu_utilization_milli,
            avg_oploop_iters_per_sec_milli,
            dominant_arm,
            max_unacked_for_secs,
        }
    }
}

/// Per-field running aggregates of the #589 loop-health digest values,
/// folded across `live_compute_resource_samples()`. Lives here (not on
/// the public `StatsSnapshot`) — a fold-local intermediate, never
/// reported. Mirrors [`ResourceFieldSums`] for the loop-health axis;
/// the field shapes differ because loop-health uses MAX (dominant arm,
/// unacked) and a separately-denominated MEAN (iter-rate excludes
/// zero/cold-start emitters from the denominator) — neither matches
/// the #575 "average over a fixed denominator" rule.
#[derive(Debug, Default, Clone)]
struct LoopHealthFieldSums {
    iters_sum: u128,
    /// Iter-rate is averaged ONLY across secondaries with a non-zero
    /// reading (zero = cold-start / pre-#589 / wire-elided sentinel).
    /// Folding cold-starts into the denominator would drag the fleet
    /// avg toward zero after every respawn — useless to the operator.
    iters_n: u64,
    dominant_pct_max: u32,
    dominant_name: Option<String>,
    unacked_max: u32,
    /// `true` once any secondary's `max_unacked_for_secs` exceeded the
    /// running max (i.e. was non-zero) — the "saw a real reading"
    /// witness for the `Option<u32>` denominator. A non-zero
    /// `unacked_max` alone would conflate "every secondary at zero"
    /// (steady-state, no signal) with "no secondary emitted at all";
    /// this flag disambiguates.
    unacked_seen: bool,
}

/// Per-field running u128 sums of the #575 resource-stat averages,
/// folded across `live_compute_resource_samples()`. Lives here (not
/// on the public `StatsSnapshot`) — a fold-local intermediate, never
/// reported.
#[derive(Debug, Default, Clone, Copy)]
struct ResourceFieldSums {
    mem_p10: u128,
    mem_p30: u128,
    mem_p50: u128,
    mem_p70: u128,
    mem_p90: u128,
    mem_avg: u128,
    total_free_memory: u128,
    total_swap_used: u128,
    total_free_swap: u128,
    cpu_utilization_milli: u128,
}

/// `Some(sum / n)` saturating back into `u64`, OR `None` when `n == 0`.
fn avg_u64_from_sum(sum: u128, n: u64) -> Option<u64> {
    if n == 0 {
        return None;
    }
    Some((sum / n as u128).min(u64::MAX as u128) as u64)
}

/// `Some(sum / n)` saturating back into `u32`, OR `None` when `n == 0`.
fn avg_u32_from_sum(sum: u128, n: u64) -> Option<u32> {
    if n == 0 {
        return None;
    }
    Some((sum / n as u128).min(u32::MAX as u128) as u32)
}

// These helpers are reached only through `from_cluster_state`, the live
// CRDT-projection feed (producer: `observer_late_joiner/run.rs`).

fn task_id_of<I>(state: &TaskState<I>) -> &str {
    state.def().task_id.as_str()
}
