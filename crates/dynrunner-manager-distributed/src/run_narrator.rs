//! Process-independent operator run-narration over the replicated CRDT.
//!
//! # Concern
//!
//! Single concern: turn the evolving [`ClusterState`] into the operator's
//! "important" (LLM-wake-worthy) run narrative — phase-started,
//! phase-complete, the per-phase progress milestones (task-spawning,
//! error- / OOM-retry-pass start), and the one-shot run-complete /
//! run-aborted summary — by DIFFING the replicated ledger, not by hooking
//! any one process's authority. Every line is emitted at the
//! [`dynrunner_core::IMPORTANT_TARGET`] tracing target, exactly the marker
//! the primary's [`crate::primary::important_events`] siblings use.
//!
//! # Milestones are DERIVED, not a fact
//!
//! There is no narrator-specific replicated milestone fact. The three
//! per-phase progress milestones are derived from the COMPLETE converged
//! CRDT, exactly as a promoted primary would derive them: task-spawning off
//! the same `has_any && dispatchable` phase edge as the "starting job phase"
//! line, and the two retry-pass-start milestones off the PRESENCE of a
//! positive count in the replicated grow-only
//! [`ClusterState::retry_passes_used`] map — once per `(phase, bucket)`, not
//! per count-increment, so the milestone derives identically whether a node
//! watched the count climb live or was fed the already-converged value (a
//! pass that opened on a remote promoted primary is surfaced here purely via
//! replication).
//!
//! # Why narrate from the CRDT, not from the primary
//!
//! After a bootstrap relocation the operator's process steps down to an
//! observer (the relocation observer tail) and the new
//! primary lives on a DIFFERENT node. A narrative emitted by the primary
//! then goes to that other node's stdout — invisible to the operator who
//! launched the job. The CRDT, by contrast, is replicated to every node:
//! the observer holds a continuously-coherent mirror. So the narrative
//! the operator must see is derived HERE, from `ClusterState`, in the
//! observer's own process — process-independent by construction.
//!
//! # Sibling to `StatsSnapshot`, not a reuse of it
//!
//! The pyo3 `StatsSnapshot` reporter is the same idea (project the CRDT
//! for the operator) but lives in the LEAF `dynrunner-pyo3` crate, which
//! DEPENDS on this one — there is no reverse edge to reuse. The shared
//! logic is lifted to the CRDT-accessor layer instead:
//! [`ClusterState::phase_rollups`] owns the phase state machine and BOTH
//! this narrator and `StatsSnapshot::from_cluster_state` consume it.
//!
//! # Idempotency
//!
//! `observe()` is called repeatedly against a monotonically-advancing
//! ledger. Each event is emitted at most once via the `HashSet::insert`
//! edge pattern (mirroring the primary's `phase_started_emitted.insert`
//! and [`crate::primary::important_events`]): the started/done edge-sets
//! accumulate the phases already announced, and the run-complete /
//! run-aborted summary is gated on a single `completion_emitted` latch so
//! it fires exactly once across the whole observer tail.

use std::collections::HashSet;

use dynrunner_core::{IMPORTANT_TARGET, Identifier, PhaseId, TerminalOutcomeCounts, narrate_routed};

use crate::ClusterState;
use crate::cluster_state::{CustomMsgState, DiscoveryDebt};
use crate::primary::retry_bucket::BucketKind;

/// The terminal-summary count source for the narrator: the verdict's CARRIED
/// counts (`terminal_outcome()`), the primary's authoritative finalized
/// partition latched ATOMICALLY with the `run_complete`/`run_aborted` latch.
///
/// Callers reach the terminal branches ONLY when a latch is set, and the
/// counts ride the SAME mutation as that latch, so `terminal_outcome()` is
/// `Some` here by construction — this is the #513 fix: the narrator no longer
/// re-folds its own (possibly unconverged) ledger via `outcome_counts()`,
/// which is exactly what let an observer narrate a false "0 failed-final"
/// success on a RunComplete observed before the per-task terminals merged.
///
/// The `unwrap_or_else` fallback to the local fold is purely DEFENSIVE (a
/// latch with no carried counts cannot occur on any current path — apply
/// latches both together); it guarantees the narrator never panics and, in
/// that impossible case, degrades to the pre-fix read rather than crashing.
fn terminal_outcome_or_local<I: Identifier>(state: &ClusterState<I>) -> TerminalOutcomeCounts {
    state
        .terminal_outcome()
        .unwrap_or_else(|| state.outcome_counts().into())
}

/// Stateful, pure projection that diffs the replicated [`ClusterState`]
/// against its accumulated edge-sets and emits the operator's run
/// narrative idempotently.
///
/// Holds only accumulated edge-sets — no authority, no pool, no
/// wall-clock. Construct once before the observer loop and call
/// [`Self::observe`] each iteration; the accumulated sets make repeated
/// calls against an unchanged (or monotonically-advanced) ledger
/// idempotent.
pub struct RunNarrator {
    /// Phases for which the "starting job phase" line has been emitted.
    started_phases: HashSet<PhaseId>,
    /// Phases for which the "phase complete" line has been emitted.
    done_phases: HashSet<PhaseId>,
    /// `(phase, bucket)` keys for which the retry-pass-start milestone has
    /// been emitted, derived from the replicated grow-only-MAX
    /// [`ClusterState::retry_passes_used`]. A pure PRESENCE edge-set — the
    /// exact twin of `started`/`done` — not a count diff: the milestone fires
    /// ONCE the moment a `(phase, bucket)` key first appears with a positive
    /// count (`retry_passes_used` ≥ 1) and never again for that key, no matter
    /// how high the count climbs. Presence (not per-increment) is the only
    /// failover-consistent derivation: a live primary watching the count step
    /// 1→2→3 and a promoted/observing node fed the already-converged count 3
    /// both see the SAME presence and so emit the SAME single line — whereas a
    /// count diff would make the live primary emit three lines and the
    /// promoted node only one, deriving DIFFERENT narration from the one
    /// converged CRDT.
    retry_passes_emitted: HashSet<(PhaseId, BucketKind)>,
    /// Whether the one-shot "setup phase started" line has fired (#508) —
    /// the once-only edge the moment the converged ledger first shows ≥1
    /// setup-kind task. Mirrors the `started`/`done` phase edge-sets: a pure
    /// PRESENCE latch over [`ClusterState::setup_progress`]'s `total`, so it
    /// derives identically whether a node watched the setup tasks land live
    /// or was fed the already-converged ledger after a relocation.
    setup_started_emitted: bool,
    /// Whether the one-shot "setup complete" line has fired (#508) — the
    /// once-only edge the moment every planned setup task is terminal
    /// (`complete == total`, `total > 0`). The setup block runs BEFORE the
    /// phase block in [`Self::observe`], so this all-done line precedes the
    /// dependent phases' "starting job phase" narration.
    setup_done_emitted: bool,
    /// The last setup `complete` count the aggregate progress line emitted
    /// (#508). The aggregate "setup: N/M complete" line fires once per
    /// observe sweep ONLY when `complete` advanced past this value — the
    /// anti-spam cadence (#393): the operator gets progress without one line
    /// per setup task (staged uploads can be many). `None` until the first
    /// aggregate emit. NOT a CRDT mirror — purely the local
    /// already-narrated watermark, the exact role `started`/`done` play.
    setup_progress_emitted: Option<usize>,
    /// Whether the one-shot run-complete / run-aborted summary has fired.
    /// The terminal outcomes are mutually exclusive and share this single
    /// latch so at most one terminal line is ever emitted.
    completion_emitted: bool,
    /// Whether the one-shot mid-run "graceful abort requested" line has
    /// fired — the operator-wake announcement of the replicated
    /// `graceful_abort_requested` dispatch-freeze latch (the terminal
    /// graceful summary below shares `completion_emitted` instead; this
    /// latch covers the REQUEST edge, which lands long before the drain
    /// terminal).
    graceful_abort_announced: bool,
    /// Whether the first [`Self::observe`] has run and SEEDED the
    /// membership / primary baseline. The cold fleet forming — and, on a
    /// relocation, the already-converged roster the observer inherits — is
    /// NOT a wake event, so the FIRST observe records the current
    /// remote-secondary roster and the current primary identity WITHOUT
    /// emitting; only genuine POST-establishment transitions (a departure, a
    /// rejoin, a primary CHANGE, a primary leaving the mesh) narrate. The
    /// phase / retry / completion blocks above are deliberately NOT gated on
    /// this latch — a phase starting or a retry pass opening IS a wake event
    /// on first appearance; only the failover / degradation block below is
    /// baseline-seeded.
    failover_seeded: bool,
    /// The currently-known-live REMOTE worker-secondary roster — every
    /// [`ClusterState::alive_secondary_members`] id that is NOT the recognised
    /// primary (an `id != current_primary` cut owned HERE, purely for
    /// narration framing: the primary's OWN co-located worker-secondary is
    /// never narrated as a peer — its departure is the primary-left event
    /// below, not a secondary departure). Maintained across observes so a set-difference against the
    /// freshly-read live set yields the departures (peer-lost) and the
    /// post-establishment joins (peer-rejoined). The membership ledger is
    /// STICKY (a `PeerRemoved` id is `Dead` forever and can never re-`Alive`),
    /// so a departed id never re-enters this set under the same id — the
    /// set-difference therefore narrates each transition exactly once with no
    /// flicker-damping needed.
    live_remote_secondaries: HashSet<String>,
    /// The last observed recognised primary as `(id, epoch)`. Seeded silently
    /// on the first observe (the initial establishment is not a wake event);
    /// thereafter a differing `(id, epoch)` is a genuine failover and emits
    /// the "primary failed over" line exactly once per new `(id, epoch)`.
    /// `None` means no primary has been recognised yet (pre-`PrimaryChanged`).
    last_primary: Option<(String, u64)>,
    /// Recognised-primary ids for which the "primary left the mesh — failover
    /// in progress" line has been emitted, keyed by the departed primary's
    /// id. Idempotent per departed-primary-id: the line fires the moment the
    /// recognised `current_primary` is no longer a live member
    /// ([`ClusterState::is_peer_alive`] false) and never again for that id.
    /// A dead id is sticky-`Dead` (never resurrects), so once-per-id is exact.
    primary_lost_emitted: HashSet<String>,
    /// `(secondary_id, member_gen)` pairs the "wind-down requested" WARN has
    /// fired for. The replicated
    /// [`dynrunner_protocol_primary_secondary::ClusterMutation::WindDownRequested`]
    /// set is grow-only, so each pair narrates exactly once via this edge-set
    /// — the per-incarnation sibling of `graceful_abort_announced`. The line
    /// is an operator-wake event because a SLURM job slot is about to be
    /// released intentionally (the directed secondary self-departs once its
    /// in-flight work drains, releasing its slot), and that resource loss
    /// has no other operator signal.
    wind_down_announced: HashSet<(String, u64)>,
    /// Whether the one-shot "discovery owed" INFO line has fired — the mode-2
    /// startup-boundary announcement of the replicated
    /// [`crate::cluster_state::DiscoveryDebt::Owed`] lattice value. Once the
    /// debt is settled the [`Self::discovery_settled_announced`] twin fires
    /// the matching closing line; both are sticky bools (the lattice is
    /// monotone, no de-narration on revert). A run that never declares debt
    /// (`Undeclared`) keeps both silent — exactly the precedent shape of
    /// `setup_started_emitted`/`setup_done_emitted` for a setup-free run.
    discovery_owed_announced: bool,
    /// Whether the one-shot "discovery settled" INFO line has fired — the
    /// `Settled` lattice TOP, the mode-2 startup-completion edge. May fire
    /// without [`Self::discovery_owed_announced`] having fired first when the
    /// observer's first observe already sees a `Settled` debt (the empty-
    /// corpus path latches `Settled` immediately alongside `RunComplete`),
    /// so the narration is a PRESENCE latch on the value, not a transition
    /// diff: an `Owed` observed first emits `discovery_owed_announced`, a
    /// `Settled` observed first emits `discovery_settled_announced` only;
    /// neither retroactively emits the other (the operator's question is
    /// "is discovery done?", not "did this observer watch every step?").
    discovery_settled_announced: bool,
    /// `(origin, seq)` keys for which the "custom message posted" INFO line
    /// has fired — the F5 inbox landing edge, narrating the topic the
    /// consumer handler is about to dispatch on. Per-key edge-set; mirrors
    /// `primary_lost_emitted` exactly.
    ///
    /// Posted is the only F5 lifecycle edge surfaced from state alone in
    /// this narrator (#568): the matching `Handled`/`Failed` terminal lines
    /// require label-preserving observation that the `compact_custom_watermark`
    /// apply rule strips at the SAME apply that lands the terminal (see
    /// `apply_custom.rs::compact_custom_watermark` and the doc-comment at
    /// `apply_custom.rs`'s `custom_message_state` accessor — "the watermark
    /// erases the Handled/Failed label"). The audit's terminal arms (Gap B
    /// and Gap D-Handled) are therefore queued for the event-driven channel
    /// follow-up #570; the state-derived narrator only surfaces the
    /// lifecycle edge whose label IS in the converged mirror — the
    /// `Unhandled` snapshot the [`ClusterState::custom_message_entries`]
    /// iterator yields.
    custom_posted_emitted: HashSet<(String, u64)>,
}

impl RunNarrator {
    /// Construct with the started-phases edge-set pre-seeded from phases
    /// already announced by another emitter in THIS process (the
    /// pre-relocation submitter's `fire_initial_phase_starts`), so the
    /// narrator does not re-announce them but still emits phases that
    /// first become dispatchable post-relocation. The relocation observer
    /// tail seeds from `phase_started_emitted`; the empty-seed [`Self::new`]
    /// is the cold-join / test constructor.
    pub(crate) fn with_started_phases(started_phases: HashSet<PhaseId>) -> Self {
        Self {
            started_phases,
            done_phases: HashSet::new(),
            retry_passes_emitted: HashSet::new(),
            setup_started_emitted: false,
            setup_done_emitted: false,
            setup_progress_emitted: None,
            completion_emitted: false,
            graceful_abort_announced: false,
            failover_seeded: false,
            live_remote_secondaries: HashSet::new(),
            last_primary: None,
            primary_lost_emitted: HashSet::new(),
            wind_down_announced: HashSet::new(),
            discovery_owed_announced: false,
            discovery_settled_announced: false,
            custom_posted_emitted: HashSet::new(),
        }
    }

    /// Empty-seed narrator (no phase pre-announced). The cold-join path
    /// seeds an empty set through [`Self::with_started_phases`] directly;
    /// this delegating constructor is the test entry point (the production
    /// observer always calls `with_started_phases`). Delegates so field
    /// initialisation has one source of truth.
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_started_phases(HashSet::new())
    }

    /// Diff `state` against the accumulated edge-sets and emit any newly
    /// reached narrative events. Idempotent per edge.
    ///
    /// Ordering within a single call: per-phase transitions (started,
    /// then complete) first, then the one-shot run summary last — so an
    /// iteration that simultaneously observes the final phase completing
    /// AND `run_complete()` emits the phase line before the summary.
    ///
    /// Returns whether ≥1 narrative event was emitted this call — the
    /// caller's seam for the wake-stream piggyback (a narrated iteration
    /// is a wake-stream HOST: the observer loop flushes the pending
    /// reconnection note right after a `true`). The narrator itself stays
    /// free of any wake-policy knowledge.
    pub(crate) fn observe<I: Identifier>(&mut self, state: &ClusterState<I>) -> bool {
        let mut emitted = false;
        // Setup-task phase milestones FIRST (#508): the setup phase is the
        // dependency root — its tasks gate the dependent work phases — so its
        // started / progress / all-done lines must narrate BEFORE the phase
        // block below emits the dependents' "starting job phase". The
        // all-done edge in particular has to precede the first dependent
        // phase the setup unblocks.
        emitted |= self.narrate_setup(state);
        // Phase transitions, read off the single owning phase-state
        // accessor. Two edges per phase:
        //   * STARTED: owns ≥1 task AND its formal boundary is open
        //     (every phase-dep has had `PhaseEnded` applied — the I1
        //     invariant, `ClusterState::phase_boundary_open`).
        //   * COMPLETE: owns ≥1 task, has no live task left, the formal
        //     start has already been emitted for it, and its formal
        //     boundary is open (I2).
        //
        // Evaluated in TWO PASSES against the same rollup snapshot, with
        // all completes emitted BEFORE any starts within this `observe()`
        // call: semantically a phase's `PhaseEnded` enables its
        // successors' formal starts (I1 reads the predecessors'
        // `PhaseEnded`), so complete-before-start matches the boundary
        // semantics even when one observe simultaneously sees a
        // predecessor reach all-terminal AND a successor newly cross its
        // open-boundary start edge. Pre-fix the loop iterated
        // `HashMap<&PhaseId, PhaseRollup>` in iter order and emitted both
        // edges interleaved per phase — so the operator's log could show
        // `successor start` BEFORE `predecessor complete` when they
        // landed in the same sweep (cosmetic, no causal violation, but
        // misleading). Sorted by `PhaseId` within each pass for a
        // deterministic order across observe calls (a `HashMap` iter is
        // otherwise per-build).
        let rollups = state.phase_rollups();
        let mut ordered: Vec<(&PhaseId, crate::cluster_state::PhaseRollup)> =
            rollups.into_iter().collect();
        ordered.sort_by(|a, b| a.0.cmp(b.0));

        // PASS 1: complete edges. The complete predicate ANDs the start
        // edge having already been observed (`started_phases.contains`)
        // and the boundary being open (`phase_boundary_open`) — closes
        // V-A2: pre-fix the `has_any && !has_live` arm carried NO
        // start-fired check and NO boundary check, so a barrier=False
        // phase whose tasks all completed FAST while its predecessor
        // was still draining false-narrated `phase complete`.
        for (phase, rollup) in &ordered {
            if rollup.has_any
                && !rollup.has_live
                && self.started_phases.contains(*phase)
                && state.phase_boundary_open(phase)
                && self.done_phases.insert((*phase).clone())
            {
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    phase = %phase,
                    "phase complete",
                );
                emitted = true;
            }
        }

        // PASS 2: start edges. The start predicate is the strict
        // boundary predicate (`phase_boundary_open`) — closes V-A1b:
        // pre-fix this gated on `rollup.dispatchable` (the WEAKER
        // "every transitive dep has no live task" predicate computed
        // from `phase_rollups`), which can flip true before the
        // predecessor's `PhaseEnded` lands. The narrator's
        // "starting job phase" line is the operator-facing twin of the
        // primary's same-named line from `fire_initial_phase_starts`,
        // so the gates must match — both now consult
        // `phase_boundary_open`.
        for (phase, rollup) in &ordered {
            if rollup.has_any
                && state.phase_boundary_open(phase)
                && self.started_phases.insert((*phase).clone())
            {
                // REUSE of the exact phrase the primary emits at
                // `fire_initial_phase_starts` (coordinator.rs) so the
                // operator reads ONE consistent line pre- and
                // post-relocation.
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    phase = %phase,
                    "starting job phase",
                );
                // PhaseTaskSpawning milestone, derived on the SAME
                // start edge the "starting job phase" line fires on — the
                // CRDT-side twin of the milestone the removed projection
                // emitted from `fire_initial_phase_starts` (which
                // originated `PhaseTaskSpawning` on the same
                // `phase_started_emitted.insert` edge as that line).
                // Sharing the `started_phases` edge keeps the two lines
                // on one once-per-phase guard, exactly as the authority
                // did.
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    phase = %phase,
                    "phase preparation / task spawning",
                );
                // #337: per-phase work partition — how many of this phase's
                // tasks are real work vs already-done skips. Counts come from
                // the SHARED `phase_task_partition` ClusterState accessor (the
                // single owner of this projection), NOT a narrator-local
                // ledger re-walk. On the same once-per-phase `started_phases`
                // edge, so it fires exactly once per phase.
                let p = state.phase_task_partition(phase);
                let (to_run, skipped) = (p.to_run, p.skipped);
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    phase = %phase,
                    to_run = to_run,
                    skipped = skipped,
                    "phase {phase}: {to_run} to run, {skipped} skipped (already done)",
                );
                // Running OVERALL across every phase started so far. DERIVED
                // by summing `phase_task_partition` over the started-phases
                // edge-set rather than accumulating into a mutable field, so
                // it is failover-consistent and re-derivable on a narrator
                // restart (a mutable accumulator would be observer-only state
                // that could desync from the ledger — the exact antipattern
                // this feature avoids). Emitted on the same once-per-phase
                // edge; each newly-started phase advances the running total.
                // Unlike the per-phase line (whose phase just spawned, so it
                // owns no terminal work yet), the overall spans EARLIER
                // phases whose tasks have since terminated — the partition's
                // done / failed buckets keep those honest instead of
                // re-counting every ever-planned task as still "to run".
                let overall = self
                    .started_phases
                    .iter()
                    .map(|p| state.phase_task_partition(p))
                    .fold(
                        crate::cluster_state::PhaseTaskPartition::default(),
                        |acc, p| acc + p,
                    );
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    to_run = overall.to_run,
                    done = overall.done,
                    failed = overall.failed,
                    skipped = overall.skipped,
                    "overall: {} to run, {} done, {} failed, {} skipped (already done)",
                    overall.to_run,
                    overall.done,
                    overall.failed,
                    overall.skipped,
                );
                emitted = true;
            }
        }

        // Retry-pass-start milestones, derived off the replicated grow-only
        // `retry_passes_used` map. The milestone marks that a `(phase, bucket)`
        // retry pass OPENED for that phase at all — a once-per-`(phase, bucket)`
        // PRESENCE edge, mirroring the started/done edge-sets exactly: the
        // first time a key is observed with a positive count (≥ 1) it is
        // inserted and the milestone emitted, and never again for that key.
        // Presence (not the per-increment count step) is the only
        // failover-consistent derivation — a live primary watching the count
        // climb 1→2→3 and a promoted/observing node fed the already-converged
        // count 3 both observe the same presence and emit the SAME single line,
        // so everything derives identically from the one converged CRDT.
        // `BucketKind` selects the operator wording.
        for (key, used) in state.retry_passes_used() {
            if used >= 1 && self.retry_passes_emitted.insert(key.clone()) {
                let (phase, bucket) = key;
                match bucket {
                    BucketKind::Recoverable => tracing::info!(
                        target: IMPORTANT_TARGET,
                        phase = %phase,
                        "error-retry-pass start",
                    ),
                    BucketKind::Oom => tracing::info!(
                        target: IMPORTANT_TARGET,
                        phase = %phase,
                        "OOM-retry-pass start",
                    ),
                }
                emitted = true;
            }
        }

        // Failover / degradation transitions, derived off the replicated
        // membership + primary CRDT facts. Like the phase/retry milestones
        // these are pure CRDT projections — narrated HERE, in the observer's
        // process, so a relocated primary's failover reaches the operator's
        // `--important-stdio-only` stdout regardless of which node now hosts
        // the primary (the primary-side promotion / relocation emit goes to
        // the PROMOTING node's stdout, never the operator's). Placed BEFORE
        // the one-shot terminal summary: a failover is a mid-run transition,
        // the summary is the run's end.
        emitted |= self.narrate_failover(state);

        // Per-incarnation wind-down directives (mid-run edge, #568): each
        // landed `(secondary_id, member_gen)` pair narrates ONCE — the
        // grow-only set + per-pair edge-set is the per-peer sibling of the
        // global `graceful_abort_announced` latch below. WARN class: a SLURM
        // job slot is about to be released intentionally (the directed
        // secondary self-departs once its in-flight work drains) and the
        // operator's `--important-stdio` stream is the only signal for that
        // resource loss.
        emitted |= self.narrate_wind_down(state);

        // Discovery-debt startup boundaries (mid-run edges, #568): a one-shot
        // INFO pair latched on the replicated three-state lattice. `Owed`
        // narrates the mode-2 "awaiting compute-peer primary to seed task
        // ledger" announcement; `Settled` narrates the "task ledger fully
        // seeded" closing line. Neither retroactively emits the other — see
        // the field docs (`Settled`-first on first observe is the empty-corpus
        // path).
        emitted |= self.narrate_discovery_debt(state);

        // F5 custom-message LANDING edge (mid-run, #568): per-`(origin, seq)`
        // INFO line carrying the topic the consumer handler will dispatch on.
        // Terminal lines (Handled/Failed) are not surfaced here — see
        // `narrate_custom_messages` for the label-erasure reason and the
        // queued follow-up (#570).
        emitted |= self.narrate_custom_messages(state);

        // One-shot graceful-abort REQUEST announcement (mid-run edge): the
        // replicated dispatch-freeze latch landed in the mirror. Narrated
        // here — in the observer's process — so the operator's
        // `--important-stdio` stdout sees the wind-down begin regardless of
        // which node hosts the primary. The drain TERMINAL is the summary
        // below; this is the request edge.
        if !self.graceful_abort_announced && state.graceful_abort_requested() {
            self.graceful_abort_announced = true;
            let c = state.counts();
            tracing::warn!(
                target: IMPORTANT_TARGET,
                in_flight = c.in_flight,
                pending = c.pending,
                blocked = c.blocked,
                "graceful abort requested — dispatch frozen; running tasks \
                 will complete and the fleet will drain"
            );
            emitted = true;
        }

        // One-shot run summary, gated on the sticky run-complete /
        // run-aborted latches AND the local `completion_emitted` bool so
        // it fires exactly once across the entire observer tail. The two
        // outcomes are mutually exclusive: `run_complete` is the
        // happy-path terminal, `run_aborted` the failure twin; check
        // aborted first so an aborted run never narrates as completed.
        if !self.completion_emitted {
            if let Some(reason) = state.run_aborted() {
                self.completion_emitted = true;
                // Narrate the verdict's CARRIED counts (the primary's
                // finalized partition, latched ATOMICALLY with the abort) —
                // NOT a re-fold of this node's local ledger via
                // `outcome_counts()`. The latch and the counts arrive on the
                // SAME mutation, so reaching this branch (`run_aborted` set)
                // guarantees the counts are in hand: no partial-ledger
                // re-derivation, no convergence wait. `c` stays live for the
                // observability-only `in_flight`/`blocked` fields.
                let o = terminal_outcome_or_local(state);
                let c = state.counts();
                tracing::error!(
                    target: IMPORTANT_TARGET,
                    succeeded = o.succeeded,
                    setup_succeeded = o.setup_succeeded,
                    fail_retry = o.fail_retry,
                    fail_oom = o.fail_oom,
                    fail_final = o.fail_final,
                    in_flight = c.in_flight,
                    blocked = c.blocked,
                    reason = %reason,
                    "run aborted — shutting down",
                );
                emitted = true;
            } else if state.run_complete() && state.graceful_abort_requested() {
                // The composed graceful-abort verdict (run_complete ∧
                // graceful latch): distinct from the clean success below —
                // `unscheduled` names the deliberately-unrun residue — and
                // from the hard abort above (nothing failed; the wind-down
                // was requested). Checked BEFORE the plain-complete branch
                // so a graceful run never narrates as a clean success.
                self.completion_emitted = true;
                // Carried verdict counts (atomic with the latch) — see the
                // abort branch above. `unscheduled` is the LIVE residue
                // (pending + blocked), an observability read, kept on `c`.
                let o = terminal_outcome_or_local(state);
                let c = state.counts();
                let unscheduled = c.pending + c.blocked;
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    succeeded = o.succeeded,
                    setup_succeeded = o.setup_succeeded,
                    fail_retry = o.fail_retry,
                    fail_oom = o.fail_oom,
                    fail_final = o.fail_final,
                    in_flight = c.in_flight,
                    unscheduled,
                    "run gracefully aborted: {} succeeded / {} setup / {} failed-final / {} oom / {} retried / {} deliberately unscheduled — shutting down",
                    o.succeeded,
                    o.setup_succeeded,
                    o.fail_final,
                    o.fail_oom,
                    o.fail_retry,
                    unscheduled,
                );
                emitted = true;
            } else if state.run_complete() {
                self.completion_emitted = true;
                // Carried verdict counts (atomic with the RunComplete latch) —
                // see the abort branch above. THIS is the #513 fix: pre-fix
                // this read `outcome_counts()` (the observer's LOCAL, possibly
                // unconverged fold), so a RunComplete observed before the
                // per-task terminals merged narrated a false "0 failed-final"
                // success. The carried counts are the primary's authoritative
                // finalized partition, in hand the moment the latch is.
                let o = terminal_outcome_or_local(state);
                let c = state.counts();
                // This RICH line doubles as the final-stats flush the
                // owner requires before finishing: it is both the
                // operator's "all work done / job finished / shutting
                // down" marker AND the terminal outcome partition.
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    succeeded = o.succeeded,
                    setup_succeeded = o.setup_succeeded,
                    fail_retry = o.fail_retry,
                    fail_oom = o.fail_oom,
                    fail_final = o.fail_final,
                    in_flight = c.in_flight,
                    blocked = c.blocked,
                    "run complete: {} succeeded / {} setup / {} failed-final / {} oom / {} retried — shutting down",
                    o.succeeded,
                    o.setup_succeeded,
                    o.fail_final,
                    o.fail_oom,
                    o.fail_retry,
                );
                emitted = true;
            }
        }
        emitted
    }

    /// Narrate the setup-task phase lifecycle derived from the replicated
    /// setup-task states (#508). Single concern: turn
    /// [`ClusterState::setup_progress`] — the `(complete, total)` projection
    /// over the SETUP-kind tasks the primary's setup-dispatch already drives
    /// through the SAME ledger — into the operator's wake-worthy
    /// setup-progress narrative, idempotently. Narrated HERE, in the
    /// observer's process, so a relocated setup phase reaches the operator's
    /// `--important-stdio-only` stdout regardless of which node now hosts the
    /// primary (the same process-independence rationale as the phase/failover
    /// blocks).
    ///
    /// Three milestones, all on the [`IMPORTANT_TARGET`] channel:
    ///
    ///   - STARTED (one line, once): the first observe whose ledger shows ≥1
    ///     setup task. A run with NO setup tasks (`total == 0`) narrates
    ///     nothing — the whole block is inert.
    ///   - AGGREGATE progress (one line per observe SWEEP, anti-spam #393):
    ///     "setup: N/M tasks complete", emitted only when `complete` ADVANCED
    ///     since the last aggregate emit — never one line per setup task
    ///     (staged uploads can be many).
    ///   - ALL-DONE (one line, once): `complete == total` with `total > 0`.
    ///     Runs before the phase block emits the dependent phases' starts.
    ///
    /// Every line is a PRESENCE / watermark edge over the ledger projection
    /// (the `setup_*_emitted` latches), mirroring the started/done/retry
    /// edge-sets exactly: no narrator-local ledger re-walk, no replicated
    /// milestone fact, failover-consistent by construction.
    ///
    /// Returns whether ≥1 line was emitted (folded into [`Self::observe`]'s
    /// emitted-anything contract).
    fn narrate_setup<I: Identifier>(&mut self, state: &ClusterState<I>) -> bool {
        let progress = state.setup_progress();
        // A run with no setup tasks: the whole block is inert.
        if progress.total == 0 {
            return false;
        }
        let mut emitted = false;

        // STARTED: the first observe that sees any setup task.
        if !self.setup_started_emitted {
            self.setup_started_emitted = true;
            tracing::info!(
                target: IMPORTANT_TARGET,
                setup_total = progress.total,
                "starting setup phase — {} setup tasks to stage",
                progress.total,
            );
            emitted = true;
        }

        // AGGREGATE progress: one line per sweep, only when `complete`
        // advanced past the last emitted watermark (anti-spam). The
        // ALL-DONE line below carries the terminal count, so the aggregate
        // is suppressed once everything is complete (it would otherwise
        // double-narrate the same N/M as the all-done edge).
        if progress.complete < progress.total
            && self.setup_progress_emitted != Some(progress.complete)
        {
            self.setup_progress_emitted = Some(progress.complete);
            tracing::info!(
                target: IMPORTANT_TARGET,
                setup_complete = progress.complete,
                setup_total = progress.total,
                "setup: {}/{} tasks complete",
                progress.complete,
                progress.total,
            );
            emitted = true;
        }

        // ALL-DONE: every planned setup task is terminal. Emitted before the
        // phase block narrates the dependents the setup unblocks.
        if !self.setup_done_emitted && progress.complete == progress.total {
            self.setup_done_emitted = true;
            tracing::info!(
                target: IMPORTANT_TARGET,
                setup_total = progress.total,
                "setup complete — {} setup tasks done",
                progress.total,
            );
            emitted = true;
        }
        emitted
    }

    /// Narrate the failover / degradation transitions derived from the
    /// replicated membership + primary CRDT facts. Single concern: turn the
    /// "who is primary" and "which remote worker-secondaries are live"
    /// projections into the operator's wake-worthy failover narrative,
    /// idempotently per transition, with NO wall-clock (the narrator holds
    /// none by design — see its struct doc).
    ///
    /// # Two-stage noise avoidance (baseline seed + operational gate)
    ///
    /// The FIRST call records the current remote-secondary roster and the
    /// current primary `(id, epoch)` WITHOUT emitting and returns — the
    /// already-converged roster the production observer inherits (it begins
    /// observing only AFTER the bootstrap relocation, so its first observe is
    /// post-establishment) is not a wake event. On TOP of the seed, every
    /// emission is gated on the run being OPERATIONAL — at least one phase has
    /// reached its start edge (`!started_phases.is_empty()`, populated by the
    /// phase block that runs BEFORE this in [`Self::observe`]). The two
    /// together make BOTH the converged-first-observe case AND a slow
    /// multi-observe formation (an early cold-join observer watching the fleet
    /// grow before any work dispatches) silent: a join / departure / primary
    /// change that happens while the run is still forming is absorbed into the
    /// tracked baseline (the live set + `last_primary` are kept current every
    /// call) but never narrated; only once work is dispatching does a genuine
    /// transition of the RUNNING fleet narrate.
    ///
    /// The baseline (live set, `last_primary`) is advanced on EVERY call so it
    /// tracks the truth across the gated window; the once-per-edge sets
    /// (`primary_lost_emitted`) advance ONLY when a line is actually emitted,
    /// so a primary loss that began while gated still narrates once work
    /// starts if it is still unresolved.
    ///
    /// # No wall-clock / implicit "election stuck"
    ///
    /// A wedged failover needs no timer here: a primary leaving the mesh emits
    /// the "failover in progress" line and a resolved failover emits the
    /// "primary failed over" line, so an UNRESOLVED failover is visible as a
    /// primary-left line with no following primary-changed line.
    ///
    /// Returns whether ≥1 line was emitted (folded into
    /// [`Self::observe`]'s emitted-anything contract).
    fn narrate_failover<I: Identifier>(&mut self, state: &ClusterState<I>) -> bool {
        let mut emitted = false;
        // The recognised primary, identity-only (epoch carried separately).
        let current_primary = state.current_primary().map(str::to_owned);

        // The REMOTE worker-secondary live set: every alive worker-secondary
        // EXCEPT the recognised primary's own co-located secondary capability
        // (an `id != current_primary` cut owned here for narration framing).
        // So the primary's own departure is the primary-left event,
        // and a secondary's PROMOTION (it leaves this set because it became
        // the primary) is the primary-changed event — neither is ever a
        // secondary departure.
        let live_remote: HashSet<String> = state
            .alive_secondary_members()
            .filter(|id| Some(*id) != current_primary.as_deref())
            .map(str::to_owned)
            .collect();

        // Baseline seed: capture the inherited roster + primary silently and
        // return. The post-relocation converged fleet the production observer
        // begins from is not a wake event.
        if !self.failover_seeded {
            self.live_remote_secondaries = live_remote;
            self.last_primary = current_primary.map(|id| (id, state.primary_epoch()));
            self.failover_seeded = true;
            return false;
        }

        // Operational gate: a transition narrates only once the run is
        // dispatching work (at least one phase started). The phase block in
        // `observe` runs FIRST, so `started_phases` already reflects this
        // iteration. While not operational, formation churn is absorbed into
        // the tracked baseline below but never emitted.
        let operational = !self.started_phases.is_empty();
        // Run-terminal latch suppresses failover / membership narration
        // (#563 Seam 3). The replicated terminal verdict — `RunAborted` (the
        // deliberate failure twin every `broadcast_terminal_verdict`
        // originator latches) or `RunComplete` (the clean-end latch) — means
        // the run is over from the cluster's authoritative POV: a
        // primary-changed line, a primary-lost line, or a peer-left line
        // observed alongside (or after) the terminal latch is teardown
        // churn, not a wake-worthy mid-run transition. The completion block
        // in `observe` (the one-shot run-aborted / run-complete summary)
        // owns the operator's terminal-state line; suppressing the
        // failover-shaped lines here keeps that authoritative narration
        // from being upstaged by a misleading "primary failed over to X"
        // alarm. The baselines (`live_remote_secondaries`, `last_primary`)
        // continue to advance UNCONDITIONALLY below so a fresh narrator
        // started against a future un-aborted run still emits correctly;
        // suppression is per-call on the emit predicates only, never
        // retroactive. An UNRESOLVED in-flight failover (latch not yet
        // observed) still narrates normally — this gate is reached only
        // once the latch has converged on THIS observer.
        let terminal = state.run_aborted().is_some() || state.run_complete();

        // PEER-LOST: a remote secondary that was live and is now absent from
        // the live set departed. A secondary that became the primary is NOT a
        // departure (it left the remote set by promotion, narrated as
        // primary-changed) — exclude `current_primary`. The membership 2P-set
        // is sticky, so each id departs at most once — once-per-id, no
        // flicker.
        let live_count = live_remote.len();
        for departed in self.live_remote_secondaries.difference(&live_remote) {
            if operational
                && !terminal
                && Some(departed.as_str()) != current_primary.as_deref()
            {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    secondary = %departed,
                    live = live_count,
                    "secondary left the cluster",
                );
                emitted = true;
            }
        }
        // PEER-REJOINED: a remote secondary live now but not in the prior set
        // joined AFTER the run was operational (a never-before-seen id — the
        // sticky-Dead ledger means a departed id cannot reappear).
        for joined in live_remote.difference(&self.live_remote_secondaries) {
            if operational && !terminal {
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    secondary = %joined,
                    "secondary joined the cluster",
                );
                emitted = true;
            }
        }
        self.live_remote_secondaries = live_remote;

        // PRIMARY-LOST / failover-in-progress: the recognised primary is no
        // longer a live member (it left the mesh) but no new primary has been
        // named yet. Read off `is_peer_alive` — the capacity-INDEPENDENT
        // membership signal — NOT `alive_secondary_members`: a primary-only
        // host (no worker capacity) is structurally ABSENT from the secondary
        // roster even while perfectly alive, so the secondary roster would
        // false-positive on it. Idempotent once-per-departed-primary-id; the
        // edge-set advances only on a real emit, so a loss that began while
        // gated still narrates once work starts if still unresolved.
        if operational
            && !terminal
            && let Some(primary) = current_primary.as_deref()
            && !state.is_peer_alive(primary)
            && self.primary_lost_emitted.insert(primary.to_owned())
        {
            tracing::warn!(
                target: IMPORTANT_TARGET,
                primary = %primary,
                "primary left the mesh — failover in progress",
            );
            emitted = true;
        }

        // PRIMARY-CHANGED / failover-resolved: the recognised `(id, epoch)`
        // differs from the seeded/last-observed baseline — a genuine failover
        // result. Emit once per new `(id, epoch)` when operational; the
        // baseline `last_primary` is advanced on the change either way, so a
        // pre-operational change (the bootstrap relocation) is absorbed
        // silently. Epoch is part of the key so a re-election back onto the
        // same id (different epoch) still narrates.
        let current = current_primary.map(|id| (id, state.primary_epoch()));
        if current.is_some() && current != self.last_primary {
            if operational && !terminal && let Some((id, epoch)) = current.as_ref() {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    primary = %id,
                    epoch = *epoch,
                    "primary failed over to {id} (epoch {epoch})",
                );
                emitted = true;
            }
            self.last_primary = current;
        }
        emitted
    }

    /// Narrate every newly-landed `(secondary_id, member_gen)` pair from the
    /// replicated [`dynrunner_protocol_primary_secondary::ClusterMutation::WindDownRequested`]
    /// grow-only set, idempotently per pair. The line is WARN because the
    /// directed secondary will self-depart at its next quiescence and
    /// release its SLURM job slot — the operator must see the resource loss
    /// landing. Mirrors the per-id-edge-set shape of
    /// [`Self::primary_lost_emitted`] exactly.
    ///
    /// Returns whether ≥1 line was emitted (folded into [`Self::observe`]'s
    /// emitted-anything contract).
    fn narrate_wind_down<I: Identifier>(&mut self, state: &ClusterState<I>) -> bool {
        let mut emitted = false;
        for (secondary_id, member_gen) in state.wind_down_requested_pairs() {
            if self
                .wind_down_announced
                .insert((secondary_id.to_string(), member_gen))
            {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    secondary = %secondary_id,
                    member_gen = member_gen,
                    "wind-down requested for replacement secondary {secondary_id} \
                     (gen {member_gen}) — will drain and release slot",
                );
                emitted = true;
            }
        }
        emitted
    }

    /// Narrate the mode-2 discovery-debt startup boundary — the `Owed` /
    /// `Settled` lattice values, each as a one-shot INFO. `Undeclared` (the
    /// default/cold-seeded shape) keeps both bools `false` and the block is
    /// inert — exactly the precedent the setup-task block uses for a
    /// setup-free run (`total == 0` early-returns).
    ///
    /// PRESENCE latch (not a transition diff): an observer whose first
    /// observe sees the CRDT already at `Settled` (the relocation-inherited
    /// shape) narrates ONLY the `Settled` line — `Owed` is silent in that
    /// case, since the operator's question is "is discovery done?", not
    /// "did this observer watch every step?". An observer that watches
    /// `Owed` then `Settled` narrates both lines in order on the respective
    /// observes.
    ///
    /// Returns whether ≥1 line was emitted (folded into [`Self::observe`]'s
    /// emitted-anything contract).
    fn narrate_discovery_debt<I: Identifier>(&mut self, state: &ClusterState<I>) -> bool {
        let mut emitted = false;
        match state.discovery_debt() {
            DiscoveryDebt::Undeclared => {}
            DiscoveryDebt::Owed => {
                if !self.discovery_owed_announced {
                    self.discovery_owed_announced = true;
                    tracing::info!(
                        target: IMPORTANT_TARGET,
                        "discovery owed — awaiting compute-peer primary to seed task ledger",
                    );
                    emitted = true;
                }
            }
            DiscoveryDebt::Settled => {
                if !self.discovery_settled_announced {
                    self.discovery_settled_announced = true;
                    tracing::info!(
                        target: IMPORTANT_TARGET,
                        "discovery settled — task ledger fully seeded by compute-peer primary",
                    );
                    emitted = true;
                }
            }
        }
        emitted
    }

    /// Narrate the F5 custom-message LANDING edge — the `Unhandled` state
    /// from the replicated `custom_messages` inbox, one INFO line per
    /// `(origin, seq)` key. Mirrors the per-id-edge-set shape of
    /// [`Self::primary_lost_emitted`]: the line carries the topic so the
    /// operator's wake-stream names what the handler is about to dispatch
    /// on.
    ///
    /// Terminals (`Handled`, `Failed`) are NOT surfaced here — the
    /// `compact_custom_watermark` apply rule erases the Handled/Failed
    /// label at the SAME apply that lands the terminal, so a state-derived
    /// narrator cannot tell the two apart from the post-apply mirror; the
    /// audit's Gap B (Failed → ERROR) and Gap D-Handled (→ INFO) split is
    /// queued for the event-driven follow-up #570 (sibling to
    /// [`crate::observer::task_narrator::ObserverTaskNarrator`], which
    /// receives per-mutation events that retain the label).
    ///
    /// Returns whether ≥1 line was emitted (folded into [`Self::observe`]'s
    /// emitted-anything contract).
    fn narrate_custom_messages<I: Identifier>(&mut self, state: &ClusterState<I>) -> bool {
        let mut emitted = false;
        for (origin, seq, msg_state) in state.custom_message_entries() {
            if let CustomMsgState::Unhandled {
                topic,
                is_high_volume,
                ..
            } = msg_state
                && self
                    .custom_posted_emitted
                    .insert((origin.to_string(), seq))
            {
                // Operator-narration volume class (#583/#587). A
                // consumer-flagged high-volume custom message emits
                // the landing line on `OBSERVER_TASK_TARGET`
                // (suppressed under `--important-stdio-only`); the
                // rate-limited custom-message-activity aggregator
                // emits a rollup line on `IMPORTANT_TARGET` as the
                // wake signal. Default (low-volume) lines stay on
                // `IMPORTANT_TARGET` unchanged. The `narrate_routed!`
                // macro is the SINGLE OWNER of the `is_high_volume →
                // target` branch for runtime-decided emits — the
                // narrator never spells the if.
                narrate_routed!(
                    info,
                    *is_high_volume,
                    origin = %origin,
                    seq = seq,
                    topic = %topic,
                    "custom message posted: {topic} from {origin} (seq {seq})",
                );
                emitted = true;
            }
        }
        emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primary::retry_bucket::BucketKind;
    use crate::test_capture::{ImportantCapture, important_only};
    use dynrunner_core::{ErrorType, PhaseId, RunnerIdentifier, TaskDep, TaskInfo, TypeId};
    use dynrunner_protocol_primary_secondary::cluster_mutation::{
        ClusterMutation, PrimaryChangeReason,
    };
    use dynrunner_protocol_primary_secondary::removal_cause::RemovalCause;
    use tracing::subscriber::with_default;
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::SubscriberExt;

    /// A `TaskInfo` in `phase`, with id `id` and the given
    /// fully-qualified `(dep_phase, dep_task_id)` prerequisites.
    fn task(phase: &str, id: &str, deps: &[(&str, &str)]) -> TaskInfo<RunnerIdentifier> {
        TaskInfo {
            path: std::path::PathBuf::from(format!("/tmp/{id}")),
            size: 1,
            identifier: RunnerIdentifier::from(id),
            phase_id: PhaseId::from(phase),
            type_id: TypeId::from("default"),
            affinity_id: None,
            payload: serde_json::Value::Null,
            task_id: id.to_string(),
            task_depends_on: deps
                .iter()
                .map(|(dp, dt)| TaskDep {
                    task_id: (*dt).to_string(),
                    phase_id: PhaseId::from(*dp),
                    inherit_outputs: false,
                    def_id: None,
                })
                .collect(),
            preferred_secondaries: Default::default(),
            preferred_version: Default::default(),
            kind: Default::default(),
            setup_affinity: None,
            upload_file: None,
            required_files: None,
            resolved_path: None,
        }
    }

    fn add(state: &mut ClusterState<RunnerIdentifier>, t: &TaskInfo<RunnerIdentifier>) {
        state.apply(ClusterMutation::TaskAdded {
            hash: t.task_id.clone(),
            task: t.clone(),
            def_id: None,
        });
    }

    fn complete(state: &mut ClusterState<RunnerIdentifier>, hash: &str) {
        state.apply(ClusterMutation::TaskCompleted {
            attempt: 0,
            hash: hash.to_string(),
            result_data: None,
        });
    }

    /// Materialise an already-done skip the way the discovery seed seam does:
    /// the task is first `TaskAdded` (Pending), then transitioned to the
    /// terminal `SkippedAlreadyDone` state via `TaskSkippedAlreadyDone`. The
    /// caller adds the task with `add` first; this applies the skip transition.
    fn skip(state: &mut ClusterState<RunnerIdentifier>, hash: &str) {
        state.apply(ClusterMutation::TaskSkippedAlreadyDone {
            hash: hash.to_string(),
        });
    }

    /// A SETUP-kind task in `phase` with id `id` — the `kind` flip the only
    /// difference from `task` (the discriminant is what `setup_progress`
    /// keys on). The caller adds it with `add`.
    fn setup_task(phase: &str, id: &str) -> TaskInfo<RunnerIdentifier> {
        let mut t = task(phase, id, &[]);
        t.kind = dynrunner_core::TaskKind::Setup;
        t
    }

    /// Drive an added setup task to its SUCCESS terminal — the
    /// authoritative `SetupCompleted` the primary's setup-dispatch
    /// originates (`Pending → SetupCompleted`). The caller adds the task
    /// with `add` first.
    fn setup_complete(state: &mut ClusterState<RunnerIdentifier>, hash: &str) {
        state.apply(ClusterMutation::SetupCompleted {
            hash: hash.to_string(),
        });
    }

    /// Bump the replicated retry-pass USED count for `(phase, bucket)` to at
    /// least `used` — the same grow-only-MAX originator the live retry-bucket
    /// caller drives after a reinjecting pass. Modelling the pass-start this
    /// way (rather than a `ClusterMutation`) mirrors the real path: the count
    /// rides the snapshot + anti-entropy digest, there is no wire mutation.
    fn bump_retry_pass(
        state: &mut ClusterState<RunnerIdentifier>,
        phase: &str,
        bucket: BucketKind,
        used: u32,
    ) {
        state.record_retry_pass_used((PhaseId::from(phase), bucket), used);
    }

    /// Bring `id` into the cluster as a LIVE worker-secondary: a `PeerJoined`
    /// (membership `Alive`) plus a `SecondaryCapacity` with `> 0` worker slots
    /// — the same pair the primary originates on `SecondaryWelcome`, the only
    /// shape that lands an id in `alive_secondary_members`.
    fn join_secondary(state: &mut ClusterState<RunnerIdentifier>, id: &str) {
        state.apply(ClusterMutation::PeerJoined {
            peer_id: id.to_string(),
            is_observer: false,
            can_be_primary: true,
            cap_version: Default::default(),
            member_gen: 0,
        });
        state.apply(ClusterMutation::SecondaryCapacity {
            secondary: id.to_string(),
            worker_count: 1,
            resources: Vec::new(),
        });
    }

    /// Authoritatively remove `id` from membership (`peer_state` → sticky
    /// `Dead`), the same `PeerRemoved` a membership-drop / fatal observation
    /// originates. Drops the id out of `alive_secondary_members` AND flips
    /// `is_peer_alive(id)` false.
    fn remove_peer(state: &mut ClusterState<RunnerIdentifier>, id: &str) {
        state.apply(ClusterMutation::PeerRemoved {
            id: id.to_string(),
            cause: RemovalCause::KeepaliveMiss,
            member_gen: 0,
        });
    }

    /// Name `id` the primary at `epoch` — the replicated `PrimaryChanged`
    /// register adopt the failover / bootstrap originates.
    fn set_primary(state: &mut ClusterState<RunnerIdentifier>, id: &str, epoch: u64) {
        state.apply(ClusterMutation::PrimaryChanged {
            new: id.to_string(),
            epoch,
            reason: PrimaryChangeReason::Election,
        });
    }

    /// Make the run OPERATIONAL by dispatching work: add one zero-dep task so
    /// its phase reaches the `has_any && dispatchable` start edge — the same
    /// signal the narrator's operational gate reads (`started_phases`
    /// non-empty after the phase block). Failover / degradation lines narrate
    /// only once the run is operational; this is the formation→running cutover
    /// the gate keys on.
    fn start_work(state: &mut ClusterState<RunnerIdentifier>) {
        add(state, &task("run", "w", &[]));
    }

    /// Run a closure with an `ImportantCapture` installed as the default
    /// subscriber, returning the captured events.
    fn capture(body: impl FnOnce()) -> Vec<crate::test_capture::CapturedEvent> {
        let cap = ImportantCapture::default();
        let subscriber = Registry::default().with(cap.clone().with_filter(important_only()));
        with_default(subscriber, body);
        cap.events()
    }

    /// Phase-started emits the "starting job phase" line exactly once per
    /// dispatchable, work-carrying phase, and re-observing a stable ledger
    /// emits nothing further.
    #[test]
    fn phase_started_emits_once_per_phase() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // One zero-dep phase with a single task.
            add(&mut state, &task("compile", "a", &[]));

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            // Re-observe the unchanged ledger: idempotent.
            narrator.observe(&state);
        });

        let starts: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("starting job phase"))
            .collect();
        assert_eq!(
            starts.len(),
            1,
            "exactly one starting-job-phase line for the one dispatchable phase: {events:?}"
        );
        assert_eq!(
            starts[0].fields.get("phase").map(String::as_str),
            Some("compile")
        );
    }

    /// A phase gated on an upstream phase does NOT emit "starting job
    /// phase" until the upstream phase's FORMAL boundary closes — every
    /// task terminal AND the upstream's `PhaseEnded` fact applied
    /// (`phase_boundary_open(compile)` requires `phase_ended(build)`).
    /// Closes V-A1b: pre-fix the gate was the weaker
    /// `rollup.dispatchable` (transitive no-live), which can flip true
    /// while the upstream's end edge has not formally completed.
    #[test]
    fn phase_started_waits_for_upstream_to_terminate() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            state.apply(ClusterMutation::PhaseDepsSet {
                deps: std::collections::HashMap::from([(
                    PhaseId::from("compile"),
                    vec![PhaseId::from("build")],
                )]),
            });
            add(&mut state, &task("build", "tc", &[]));
            add(&mut state, &task("compile", "a", &[]));

            let mut narrator = RunNarrator::new();
            // Build is live → compile's boundary closed; only build starts.
            narrator.observe(&state);
            // Build's task terminal but `PhaseEnded(build)` not yet
            // landed → compile's boundary STILL closed. Pre-fix this
            // would have narrated compile started here on the weaker
            // dispatchable gate.
            complete(&mut state, "tc");
            narrator.observe(&state);
            // Authoritative end edge for build (the cascade's
            // `phase_can_proceed → PhaseEnded → mark_phase_done` step) —
            // boundary now opens for compile.
            state.apply(ClusterMutation::PhaseEnded {
                phase: PhaseId::from("build"),
            });
            narrator.observe(&state);
        });

        let started: Vec<&str> = events
            .iter()
            .filter(|e| e.message.contains("starting job phase"))
            .filter_map(|e| e.fields.get("phase").map(String::as_str))
            .collect();
        assert_eq!(
            started,
            vec!["build", "compile"],
            "build starts first; compile only after PhaseEnded(build): {events:?}"
        );
    }

    /// Phase-complete emits once the phase owns ≥1 task and every task is
    /// terminal, and only once.
    #[test]
    fn phase_complete_when_all_terminal() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("compile", "a", &[]));
            add(&mut state, &task("compile", "b", &[]));

            let mut narrator = RunNarrator::new();
            // One of two terminal → not complete.
            complete(&mut state, "a");
            narrator.observe(&state);
            // Both terminal → complete.
            complete(&mut state, "b");
            narrator.observe(&state);
            // Stable → idempotent.
            narrator.observe(&state);
        });

        let done: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("phase complete"))
            .collect();
        assert_eq!(
            done.len(),
            1,
            "exactly one phase-complete line, only after both tasks terminal: {events:?}"
        );
        assert_eq!(
            done[0].fields.get("phase").map(String::as_str),
            Some("compile")
        );
    }

    /// #584 ordering symptom: in ONE observe sweep that simultaneously
    /// sees `build` reach all-terminal AND `compile` cross its open-
    /// boundary start edge (`PhaseEnded(build)` lands in the same sweep
    /// that completes build's tasks), the operator's log must read
    /// `phase complete build` STRICTLY BEFORE `starting job phase
    /// compile`. Pre-fix the narrator iterated `phase_rollups()` (an
    /// unordered HashMap) and emitted both edges interleaved per phase,
    /// so the HashMap iter order could put `starting job phase compile`
    /// before `phase complete build` even when the underlying events
    /// fired in causal order. The two-pass split (completes first, then
    /// starts) pins the deterministic order.
    #[test]
    fn complete_emits_before_start_within_one_observe() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            state.apply(ClusterMutation::PhaseDepsSet {
                deps: std::collections::HashMap::from([(
                    PhaseId::from("compile"),
                    vec![PhaseId::from("build")],
                )]),
            });
            add(&mut state, &task("build", "tc", &[]));
            add(&mut state, &task("compile", "a", &[]));

            let mut narrator = RunNarrator::new();
            // Sweep 1: only build is open; compile's boundary is closed.
            // build narrates started.
            narrator.observe(&state);
            // Now within ONE observe sweep, build's task completes AND
            // its end edge fires — both atomically applied to the ledger
            // before the observe. compile's boundary opens; build
            // simultaneously crosses its complete edge.
            complete(&mut state, "tc");
            state.apply(ClusterMutation::PhaseEnded {
                phase: PhaseId::from("build"),
            });
            narrator.observe(&state);
        });

        let build_done_idx = events
            .iter()
            .position(|e| {
                e.message.contains("phase complete")
                    && e.fields.get("phase").map(String::as_str) == Some("build")
            })
            .expect("build phase complete narrated");
        let compile_start_idx = events
            .iter()
            .position(|e| {
                e.message.contains("starting job phase")
                    && e.fields.get("phase").map(String::as_str) == Some("compile")
            })
            .expect("compile starting job phase narrated");
        assert!(
            build_done_idx < compile_start_idx,
            "complete-before-start within one observe: \
             `phase complete build` (idx {build_done_idx}) must precede \
             `starting job phase compile` (idx {compile_start_idx}). events={events:?}"
        );
    }

    /// #508: the setup-task phase narrates its lifecycle in the
    /// important-stdio stream — STARTED once, an AGGREGATE "N/M complete"
    /// line per sweep that advances the count (never one per task), and
    /// ALL-DONE once — and the all-done line precedes the dependent phase's
    /// "starting job phase". Mirrors the milestones+aggregate cadence the
    /// owner confirmed.
    #[test]
    fn setup_phase_narrates_started_progress_and_all_done_before_dependents() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // A dependent "build" phase gated on the "setup" phase, so the
            // dependents' "starting job phase" can only fire after setup
            // fully terminates — the ordering this test pins.
            state.apply(ClusterMutation::PhaseDepsSet {
                deps: std::collections::HashMap::from([(
                    PhaseId::from("build"),
                    vec![PhaseId::from("setup")],
                )]),
            });
            // Three setup tasks (e.g. staged uploads) + one dependent build
            // task.
            for id in ["s1", "s2", "s3"] {
                add(&mut state, &setup_task("setup", id));
            }
            add(&mut state, &task("build", "b1", &[]));

            let mut narrator = RunNarrator::new();
            // Sweep 1: setup phase appears (0/3). STARTED + first aggregate.
            narrator.observe(&state);
            // Sweep 2: two setup tasks complete in one batch → ONE aggregate
            // line (2/3), not two — the anti-spam cadence.
            setup_complete(&mut state, "s1");
            setup_complete(&mut state, "s2");
            narrator.observe(&state);
            // Sweep 3: no advance → no new aggregate line (idempotent sweep).
            narrator.observe(&state);
            // Sweep 4: the last setup task completes → ALL-DONE (the
            // setup-task block fires off its own progress projection,
            // not gated on `phase_boundary_open`). Build's boundary is
            // still closed (no `PhaseEnded(setup)` yet), so its "starting
            // job phase" line does NOT fire here — the strict I1 the
            // narrator now enforces.
            setup_complete(&mut state, "s3");
            narrator.observe(&state);
            // Sweep 5: the authoritative end edge for setup (the
            // cascade's `PhaseEnded → mark_phase_done` step) lands —
            // boundary opens for build, and its "starting job phase"
            // narrates here, AFTER the setup all-done line emitted in
            // sweep 4.
            state.apply(ClusterMutation::PhaseEnded {
                phase: PhaseId::from("setup"),
            });
            narrator.observe(&state);
            // Sweep 6: stable → fully idempotent.
            narrator.observe(&state);
        });

        let started: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("starting setup phase"))
            .collect();
        assert_eq!(
            started.len(),
            1,
            "exactly one setup-started line: {events:?}"
        );
        assert_eq!(
            started[0].fields.get("setup_total").map(String::as_str),
            Some("3")
        );

        // Aggregate progress: 0/3 (sweep 1) + 2/3 (sweep 2) = exactly two
        // lines. The two-in-one-batch sweep emits ONE line (anti-spam), the
        // no-advance sweep emits none, and the all-done sweep uses the
        // distinct all-done line (not a 3/3 aggregate).
        let progress: Vec<_> = events
            .iter()
            .filter(|e| e.message.starts_with("setup: ") && e.message.contains("complete"))
            .collect();
        assert_eq!(
            progress.len(),
            2,
            "one aggregate line per advancing sweep, never per task, no 3/3: {events:?}"
        );
        assert!(
            progress[0].message.contains("0/3"),
            "first aggregate at the start edge: {:?}",
            progress[0].message
        );
        assert!(
            progress[1].message.contains("2/3"),
            "a two-task batch advances the count once to 2/3: {:?}",
            progress[1].message
        );

        let all_done: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("setup complete"))
            .collect();
        assert_eq!(
            all_done.len(),
            1,
            "exactly one setup all-done line: {events:?}"
        );
        assert_eq!(
            all_done[0].fields.get("setup_total").map(String::as_str),
            Some("3")
        );

        // ORDERING: every setup line precedes the dependent "build" phase's
        // "starting job phase". The all-done line in particular must come
        // before the build start it unblocks.
        let setup_done_idx = events
            .iter()
            .position(|e| e.message.contains("setup complete"))
            .expect("setup all-done emitted");
        let build_start_idx = events
            .iter()
            .position(|e| {
                e.message.contains("starting job phase")
                    && e.fields.get("phase").map(String::as_str) == Some("build")
            })
            .expect("dependent build phase narrates started");
        assert!(
            setup_done_idx < build_start_idx,
            "setup all-done must precede the dependent phase start: {events:?}"
        );
    }

    /// #508: a run with NO setup tasks narrates nothing setup-related — the
    /// whole block is inert.
    #[test]
    fn no_setup_tasks_narrates_no_setup_lines() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("compile", "a", &[]));
            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
        });
        assert!(
            events.iter().all(|e| !e.message.contains("setup")),
            "a setup-free run emits no setup narration: {events:?}"
        );
    }

    /// The graceful-abort narration: the REQUEST edge fires exactly one
    /// "graceful abort requested" line the moment the latch lands, and
    /// the terminal summary is the DISTINCT "run gracefully aborted" line
    /// (never the clean "run complete" wording) carrying the
    /// deliberately-unscheduled residue. Both are once-only.
    #[test]
    fn graceful_abort_request_and_verdict_narrate_distinctly_once() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // 1 completed + 1 deliberately-unscheduled pending.
            add(&mut state, &task("p", "done-task", &[]));
            add(&mut state, &task("p", "frozen-task", &[]));
            complete(&mut state, "done-task");

            let mut narrator = RunNarrator::new();
            // The latch lands mid-run: the request edge narrates once.
            state.apply(ClusterMutation::GracefulAbortRequested);
            narrator.observe(&state);
            narrator.observe(&state); // idempotent

            // The drain terminal: the composed verdict narrates once,
            // DISTINCT from the clean-success summary. The verdict carries the
            // primary's authoritative partition (the one completed task); the
            // `unscheduled` residue is read LIVE from the frozen pool, not the
            // carried counts.
            state.apply(ClusterMutation::RunComplete {
                counts: dynrunner_core::TerminalOutcomeCounts {
                    succeeded: 1,
                    ..Default::default()
                },
            });
            narrator.observe(&state);
            narrator.observe(&state); // idempotent
        });

        let requested: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("graceful abort requested"))
            .collect();
        assert_eq!(
            requested.len(),
            1,
            "exactly one request-edge line: {events:?}"
        );

        let verdict: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("run gracefully aborted"))
            .collect();
        assert_eq!(
            verdict.len(),
            1,
            "exactly one graceful terminal summary: {events:?}"
        );
        assert_eq!(
            verdict[0].fields.get("succeeded").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            verdict[0].fields.get("unscheduled").map(String::as_str),
            Some("1"),
            "the deliberately-unscheduled residue is carried on the verdict"
        );
        assert!(
            !events.iter().any(|e| e.message.contains("run complete:")),
            "a graceful run must NEVER narrate the clean-success summary: {events:?}"
        );
    }

    /// The run-complete summary fires exactly once with the correct
    /// outcome partition; a second observe() is silent.
    #[test]
    fn completion_summary_once_with_correct_counts() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // 2 completed + 1 failed-final, then RunComplete.
            add(&mut state, &task("p", "ok-a", &[]));
            add(&mut state, &task("p", "ok-b", &[]));
            add(&mut state, &task("p", "bad", &[]));
            complete(&mut state, "ok-a");
            complete(&mut state, "ok-b");
            state.apply(ClusterMutation::TaskFailed {
                attempt: 0,
                hash: "bad".to_string(),
                kind: ErrorType::NonRecoverable,
                error: "boom".into(),
                version: Default::default(),
            });
            // The verdict carries the primary's authoritative partition
            // (2 succeeded, 1 failed-final) — the narrator reports THESE.
            state.apply(ClusterMutation::RunComplete {
                counts: dynrunner_core::TerminalOutcomeCounts {
                    succeeded: 2,
                    fail_final: 1,
                    ..Default::default()
                },
            });

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            // Second observe must be silent on the summary.
            narrator.observe(&state);
        });

        let summary: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("run complete"))
            .collect();
        assert_eq!(
            summary.len(),
            1,
            "exactly one run-complete summary across both observes: {events:?}"
        );
        let fields = &summary[0].fields;
        assert_eq!(fields.get("succeeded").map(String::as_str), Some("2"));
        assert_eq!(fields.get("fail_final").map(String::as_str), Some("1"));
        assert!(
            summary[0].message.contains("2 succeeded")
                && summary[0].message.contains("1 failed-final"),
            "prose summary carries the partition: {:?}",
            summary[0].message
        );
        assert!(
            events.iter().all(|e| !e.message.contains("aborted")),
            "a completed run must NOT narrate as aborted: {events:?}"
        );
    }

    /// RunAborted narrates the aborted summary, never the completed one,
    /// even though the run is over.
    #[test]
    fn aborted_not_complete_on_run_aborted() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("p", "a", &[]));
            complete(&mut state, "a");
            // The verdict carries the primary's authoritative partition (the
            // one succeeded task) — what the narrator now reports (the carried
            // counts, NOT the local fold). `succeeded: 1` is exactly what the
            // primary's `outcome_summary()` stamps here.
            state.apply(ClusterMutation::RunAborted {
                reason: "fleet collapsed".into(),
                counts: dynrunner_core::TerminalOutcomeCounts {
                    succeeded: 1,
                    ..Default::default()
                },
            });

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            narrator.observe(&state);
        });

        let aborted: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("run aborted"))
            .collect();
        assert_eq!(
            aborted.len(),
            1,
            "exactly one run-aborted summary: {events:?}"
        );
        assert_eq!(
            aborted[0].fields.get("succeeded").map(String::as_str),
            Some("1")
        );
        assert!(
            events.iter().all(|e| !e.message.contains("run complete")),
            "an aborted run must NOT narrate as completed: {events:?}"
        );
    }

    /// #513 — the terminal summary narrates the VERDICT's CARRIED counts, NOT
    /// a re-fold of the observer's LOCAL (possibly unconverged) ledger.
    ///
    /// Replays the production sequence VERBATIM: the observer's mirror still
    /// holds the work task as `Blocked` (its `TaskFailed` has NOT converged),
    /// yet the `RunComplete` verdict — carrying the primary's authoritative
    /// `fail_final` — has landed (the latch+counts arrive ATOMICALLY on the
    /// one mutation, so a converged latch implies converged counts). The
    /// narrator MUST report the carried `fail_final`, never the local fold.
    ///
    /// REVERT-CONFIRM: the pre-fix narrator read `outcome_counts()` (the
    /// LOCAL fold), which on this exact mirror reads `fail_final = 0` (the
    /// build is `Blocked` → folded into nothing) — the false "0 failed-final"
    /// success on the operator's --important-stdio stream. The assertion
    /// below (`Some("3")`) FAILS against that local read, so reverting the
    /// `terminal_outcome_or_local` change re-reds this test.
    #[test]
    fn terminal_summary_uses_carried_counts_not_local_unconverged_fold() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // 2 succeeded work tasks (converged) ...
            add(&mut state, &task("p", "ok0", &[]));
            add(&mut state, &task("p", "ok1", &[]));
            complete(&mut state, "ok0");
            complete(&mut state, "ok1");
            // ... and 3 builds the OBSERVER still sees as Blocked (their
            // TaskFailed terminals have NOT converged to this mirror).
            for i in 0..3 {
                let b = task("build", &format!("b{i}"), &[]);
                add(&mut state, &b);
                state.apply(ClusterMutation::TaskBlocked {
                    hash: format!("b{i}"),
                    on: "absent-gate".to_string(),
                });
            }
            // The PRIMARY's verdict: RunComplete carrying the AUTHORITATIVE
            // partition (2 succeeded, 3 failed-final) — what the primary
            // finalized. Atomic latch+counts: applying RunComplete latches
            // both.
            state.apply(ClusterMutation::RunComplete {
                counts: dynrunner_core::TerminalOutcomeCounts {
                    succeeded: 2,
                    fail_final: 3,
                    ..Default::default()
                },
            });
            // Sanity: the LOCAL fold still under-counts (the pre-fix source).
            assert_eq!(
                state.outcome_counts().fail_final,
                0,
                "precondition: the local fold sees the builds as Blocked → 0 \
                 failed-final (the unconverged mirror the bug narrated from)"
            );

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            narrator.observe(&state);
        });

        let summary: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("run complete:"))
            .collect();
        assert_eq!(summary.len(), 1, "exactly one run-complete summary");
        assert_eq!(
            summary[0].fields.get("fail_final").map(String::as_str),
            Some("3"),
            "the terminal summary must narrate the VERDICT's carried \
             fail_final (3), NOT the observer's local unconverged fold (0): {:?}",
            summary[0].fields
        );
        assert_eq!(
            summary[0].fields.get("succeeded").map(String::as_str),
            Some("2"),
        );
    }

    /// #513 / #469 regression guard — the terminal summary derives ENTIRELY
    /// from the carried verdict counts, so it NEVER waits on per-task
    /// convergence. Models the cluster-gone-before-convergence shape: a
    /// `RunAborted` verdict carrying the authoritative failure partition lands
    /// while the observer's mirror still has the work tasks non-terminal
    /// (`Blocked` — their terminals will NEVER converge, the fleet is gone).
    /// The narrator must emit the carried counts on the FIRST observe — no
    /// hang, no convergence dependence, and NEVER success-zeros.
    #[test]
    fn aborted_summary_does_not_wait_on_convergence() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // Work tasks the observer will NEVER see terminalize (cluster gone).
            for i in 0..4 {
                let b = task("build", &format!("b{i}"), &[]);
                add(&mut state, &b);
                state.apply(ClusterMutation::TaskBlocked {
                    hash: format!("b{i}"),
                    on: "gate".to_string(),
                });
            }
            // The primary's abort verdict, carrying its finalized partition.
            state.apply(ClusterMutation::RunAborted {
                reason: "cluster routing collapsed".into(),
                counts: dynrunner_core::TerminalOutcomeCounts {
                    succeeded: 1,
                    fail_final: 4,
                    ..Default::default()
                },
            });

            let mut narrator = RunNarrator::new();
            // ONE observe — the summary must already be emitted (no second
            // pass / convergence wait needed).
            assert!(
                narrator.observe(&state),
                "the terminal summary must emit on the FIRST observe — no \
                 convergence wait (the #469 hang guard)"
            );
        });

        let aborted: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("run aborted"))
            .collect();
        assert_eq!(aborted.len(), 1, "exactly one aborted summary: {events:?}");
        assert_eq!(
            aborted[0].fields.get("fail_final").map(String::as_str),
            Some("4"),
            "the aborted summary narrates the carried failure count (4), \
             never success-zeros off the unconverged local fold: {:?}",
            aborted[0].fields
        );
    }

    /// The narrator reads `phase_rollups()`, whose terminal rule treats
    /// `Blocked` as live (cascade-paused, auto-resumes). This pins that a
    /// phase whose only task is `Blocked` is NOT narrated complete.
    #[test]
    fn blocked_task_keeps_phase_live() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("p", "a", &[]));
            // Pending → Blocked via the public cascade mutation.
            state.apply(ClusterMutation::TaskBlocked {
                hash: "a".to_string(),
                on: "x".to_string(),
            });
            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
        });
        assert!(
            events.iter().all(|e| !e.message.contains("phase complete")),
            "a phase whose only task is Blocked is not complete: {events:?}"
        );
    }

    /// The phase-task-spawning milestone fires on the SAME `has_any &&
    /// dispatchable` edge as the "starting job phase" line — once per phase,
    /// not before the phase becomes dispatchable, not twice on a re-observe.
    #[test]
    fn phase_task_spawning_on_dispatchable_edge_once() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("compile", "a", &[]));

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            // Re-observe the unchanged ledger: idempotent.
            narrator.observe(&state);
        });

        let spawn: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("phase preparation / task spawning"))
            .collect();
        assert_eq!(
            spawn.len(),
            1,
            "exactly one task-spawning line on the dispatchable edge: {events:?}"
        );
        assert_eq!(
            spawn[0].fields.get("phase").map(String::as_str),
            Some("compile")
        );
    }

    /// #337: a phase with N to-run tasks and M `SkippedAlreadyDone` ledger
    /// entries emits, on the same dispatchable edge, one
    /// "<N> to run, <M> skipped (already done)" per-phase line AND an
    /// "overall: <N> to run, <D> done, <F> failed, <M> skipped" running total
    /// — the overall derived from summing `phase_task_partition` over the
    /// started phases (no mutable accumulator). A re-observe of the stable
    /// ledger emits nothing further.
    #[test]
    fn phase_skip_partition_emits_per_phase_and_overall() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // 2 to-run (Pending) + 3 already-done skips in one phase.
            for id in ["r1", "r2"] {
                add(&mut state, &task("build", id, &[]));
            }
            for id in ["s1", "s2", "s3"] {
                add(&mut state, &task("build", id, &[]));
                skip(&mut state, id);
            }

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            // Re-observe the unchanged ledger: idempotent.
            narrator.observe(&state);
        });

        let per_phase: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("2 to run, 3 skipped (already done)"))
            .filter(|e| e.fields.get("phase").map(String::as_str) == Some("build"))
            .collect();
        assert_eq!(
            per_phase.len(),
            1,
            "exactly one per-phase skip-partition line for the build phase: {events:?}"
        );
        assert_eq!(
            per_phase[0].fields.get("to_run").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            per_phase[0].fields.get("skipped").map(String::as_str),
            Some("3")
        );

        let overall: Vec<_> = events
            .iter()
            .filter(|e| e.message.starts_with("overall:"))
            .collect();
        assert_eq!(
            overall.len(),
            1,
            "exactly one overall line for the single started phase: {events:?}"
        );
        assert!(
            overall[0]
                .message
                .contains("2 to run, 0 done, 0 failed, 3 skipped (already done)"),
            "overall reflects the single phase's partition: {:?}",
            overall[0].message
        );
        assert_eq!(
            overall[0].fields.get("to_run").map(String::as_str),
            Some("2")
        );
        assert_eq!(overall[0].fields.get("done").map(String::as_str), Some("0"));
        assert_eq!(
            overall[0].fields.get("failed").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            overall[0].fields.get("skipped").map(String::as_str),
            Some("3")
        );
    }

    /// #337: a phase with no already-done skips emits "<N> to run, 0 skipped"
    /// — the all-unmarked back-compat shape — and the overall mirrors it.
    #[test]
    fn phase_skip_partition_all_unmarked_emits_zero_skipped() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("compile", "a", &[]));
            add(&mut state, &task("compile", "b", &[]));

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
        });

        let per_phase: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("2 to run, 0 skipped (already done)"))
            .filter(|e| e.fields.get("phase").map(String::as_str) == Some("compile"))
            .collect();
        assert_eq!(
            per_phase.len(),
            1,
            "an all-unmarked phase emits '2 to run, 0 skipped': {events:?}"
        );

        let overall: Vec<_> = events
            .iter()
            .filter(|e| e.message.starts_with("overall:"))
            .collect();
        assert_eq!(overall.len(), 1, "one overall line: {events:?}");
        assert!(
            overall[0]
                .message
                .contains("2 to run, 0 done, 0 failed, 0 skipped (already done)"),
            "overall mirrors the all-unmarked phase: {:?}",
            overall[0].message
        );
    }

    /// The overall running total RECLASSIFIES terminal tasks instead of
    /// re-counting every ever-planned task as still "to run" (the
    /// run_20260610_153749 production shape: after matrix_eval's tasks all
    /// completed and dependency_graph spawned its 1 task, the overall read
    /// "17 to run" — the 16 completed tasks wore the to-run label). Phase
    /// A's terminal tasks must read `done` in the overall emitted on phase
    /// B's start edge, with the skip count unchanged.
    #[test]
    fn overall_reclassifies_terminal_tasks_at_phase_transition() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            state.apply(ClusterMutation::PhaseDepsSet {
                deps: std::collections::HashMap::from([(
                    PhaseId::from("dependency_graph"),
                    vec![PhaseId::from("matrix_eval")],
                )]),
            });
            // matrix_eval: 2 to-run + 1 already-done skip.
            for id in ["m1", "m2"] {
                add(&mut state, &task("matrix_eval", id, &[]));
            }
            add(&mut state, &task("matrix_eval", "ms", &[]));
            skip(&mut state, "ms");
            // dependency_graph: 1 task, gated on matrix_eval.
            add(&mut state, &task("dependency_graph", "d1", &[]));

            let mut narrator = RunNarrator::new();
            // matrix_eval starts → overall #1.
            narrator.observe(&state);
            // matrix_eval's tasks all terminate but its formal end edge
            // has not yet completed (`PhaseEnded(matrix_eval)` is the
            // authoritative wire fact `phase_can_proceed` originates AT
            // the same point it calls `mark_phase_done`). Without it,
            // dependency_graph's `phase_boundary_open` is closed — the
            // I1 the narrator now enforces.
            complete(&mut state, "m1");
            complete(&mut state, "m2");
            state.apply(ClusterMutation::PhaseEnded {
                phase: PhaseId::from("matrix_eval"),
            });
            // dependency_graph's boundary opens → it starts → overall
            // #2 on this observe.
            narrator.observe(&state);
        });

        let overall: Vec<_> = events
            .iter()
            .filter(|e| e.message.starts_with("overall:"))
            .collect();
        assert_eq!(
            overall.len(),
            2,
            "one overall line per started phase: {events:?}"
        );
        assert!(
            overall[0]
                .message
                .contains("2 to run, 0 done, 0 failed, 1 skipped (already done)"),
            "at matrix_eval's start nothing is terminal yet: {:?}",
            overall[0].message
        );
        // The production bug: the completed matrix_eval tasks must NOT
        // still count as to-run once dependency_graph spawns.
        assert!(
            overall[1]
                .message
                .contains("1 to run, 2 done, 0 failed, 1 skipped (already done)"),
            "phase A's terminal tasks read done, the skip count is unchanged: {:?}",
            overall[1].message
        );
        assert_eq!(
            overall[1].fields.get("to_run").map(String::as_str),
            Some("1")
        );
        assert_eq!(overall[1].fields.get("done").map(String::as_str), Some("2"));
        assert_eq!(
            overall[1].fields.get("failed").map(String::as_str),
            Some("0")
        );
        assert_eq!(
            overall[1].fields.get("skipped").map(String::as_str),
            Some("1")
        );
        // The per-phase line for the just-spawned phase is unchanged — a
        // freshly-dispatchable phase owns no terminal work at ITS emit
        // moment, so its idiom needs no done/failed split.
        let dep_phase: Vec<_> = events
            .iter()
            .filter(|e| {
                e.fields.get("phase").map(String::as_str) == Some("dependency_graph")
                    && e.message.contains("to run")
            })
            .collect();
        assert_eq!(
            dep_phase.len(),
            1,
            "one per-phase partition line: {events:?}"
        );
        assert!(
            dep_phase[0]
                .message
                .contains("1 to run, 0 skipped (already done)"),
            "per-phase line keeps its shape: {:?}",
            dep_phase[0].message
        );
    }

    /// An overall emitted MID-phase (an independent phase spawning while an
    /// earlier phase is still partially complete) partitions the earlier
    /// phase's tasks honestly: its completions read `done`, its terminal
    /// failure reads `failed`, and only the genuinely-live remainder (plus
    /// the new phase's fresh tasks) reads "to run".
    #[test]
    fn overall_mid_phase_partial_completions_read_correctly() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // Independent phase alpha with 3 tasks.
            for id in ["a1", "a2", "a3"] {
                add(&mut state, &task("alpha", id, &[]));
            }
            let mut narrator = RunNarrator::new();
            // alpha starts → overall #1.
            narrator.observe(&state);
            // Mid-phase: one completes, one fails terminally, one stays live.
            complete(&mut state, "a1");
            state.apply(ClusterMutation::TaskFailed {
                attempt: 0,
                hash: "a2".to_string(),
                kind: ErrorType::NonRecoverable,
                error: "boom".into(),
                version: Default::default(),
            });
            // An independent phase beta spawns while alpha is mid-flight.
            add(&mut state, &task("beta", "b1", &[]));
            add(&mut state, &task("beta", "b2", &[]));
            // beta starts → overall #2.
            narrator.observe(&state);
        });

        let overall: Vec<_> = events
            .iter()
            .filter(|e| e.message.starts_with("overall:"))
            .collect();
        assert_eq!(
            overall.len(),
            2,
            "one overall line per started phase: {events:?}"
        );
        // alpha's live remainder (1) + beta's fresh tasks (2) = 3 to run;
        // alpha's completion and failure are reclassified, not re-counted.
        assert!(
            overall[1]
                .message
                .contains("3 to run, 1 done, 1 failed, 0 skipped (already done)"),
            "mid-phase partial completions partition honestly: {:?}",
            overall[1].message
        );
        assert_eq!(
            overall[1].fields.get("to_run").map(String::as_str),
            Some("3")
        );
        assert_eq!(overall[1].fields.get("done").map(String::as_str), Some("1"));
        assert_eq!(
            overall[1].fields.get("failed").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            overall[1].fields.get("skipped").map(String::as_str),
            Some("0")
        );
    }

    /// A gated phase emits NO task-spawning milestone until its
    /// upstream's formal end edge completes (`PhaseEnded` lands) — the
    /// milestone shares the start edge, which the narrator now gates on
    /// `phase_boundary_open` (strict I1), not the weaker
    /// `rollup.dispatchable`.
    #[test]
    fn phase_task_spawning_waits_for_dispatchable() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            state.apply(ClusterMutation::PhaseDepsSet {
                deps: std::collections::HashMap::from([(
                    PhaseId::from("compile"),
                    vec![PhaseId::from("build")],
                )]),
            });
            add(&mut state, &task("build", "tc", &[]));
            add(&mut state, &task("compile", "a", &[]));

            let mut narrator = RunNarrator::new();
            // build dispatchable, compile's boundary closed.
            narrator.observe(&state);
            complete(&mut state, "tc");
            // build's task terminal but its formal end edge has not
            // completed — compile's boundary stays closed.
            narrator.observe(&state);
            // Authoritative end edge: compile's boundary now opens.
            state.apply(ClusterMutation::PhaseEnded {
                phase: PhaseId::from("build"),
            });
            narrator.observe(&state);
        });

        let spawned: Vec<&str> = events
            .iter()
            .filter(|e| e.message.contains("phase preparation / task spawning"))
            .filter_map(|e| e.fields.get("phase").map(String::as_str))
            .collect();
        assert_eq!(
            spawned,
            vec!["build", "compile"],
            "task-spawning fires per phase only once boundary is open: {events:?}"
        );
    }

    /// The first positive count for a `(phase, bucket)` emits the
    /// retry-pass-start milestone whose wording matches the bucket:
    /// Recoverable → error-retry, Oom → OOM-retry. Once per `(phase, bucket)`
    /// presence; a re-observe of the unchanged counts is silent.
    #[test]
    fn retry_pass_milestone_per_bucket_wording() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();

            // Error-retry pass opens (count 0 → 1).
            bump_retry_pass(&mut state, "compile", BucketKind::Recoverable, 1);
            narrator.observe(&state);
            // OOM-retry pass opens (count 0 → 1).
            bump_retry_pass(&mut state, "compile", BucketKind::Oom, 1);
            narrator.observe(&state);
            // Re-observe the unchanged counts: idempotent.
            narrator.observe(&state);
        });

        let err: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("error-retry-pass start"))
            .collect();
        assert_eq!(err.len(), 1, "one error-retry-pass line: {events:?}");
        assert_eq!(
            err[0].fields.get("phase").map(String::as_str),
            Some("compile")
        );

        let oom: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("OOM-retry-pass start"))
            .collect();
        assert_eq!(oom.len(), 1, "one OOM-retry-pass line: {events:?}");
        assert_eq!(
            oom[0].fields.get("phase").map(String::as_str),
            Some("compile")
        );
    }

    /// The retry-pass milestone fires ONCE per `(phase, bucket)` regardless of
    /// how high the count climbs: observing the count step 1 → 2 → 3 emits a
    /// single line, because the milestone marks the PRESENCE of a retry pass
    /// for that bucket, not each increment. This is the failover-consistent
    /// behaviour — a count diff would emit three lines on a node that watched
    /// the steps but only one on a node fed the converged 3.
    #[test]
    fn retry_pass_milestone_once_per_phase_bucket() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();

            for n in 1..=3 {
                bump_retry_pass(&mut state, "compile", BucketKind::Recoverable, n);
                narrator.observe(&state);
            }
        });

        let err: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("error-retry-pass start"))
            .collect();
        assert_eq!(
            err.len(),
            1,
            "exactly one error-retry-pass line for the (phase, bucket) across all increments: {events:?}"
        );
    }

    /// Failover consistency: a node that observes the count climb 0→1→2→3
    /// incrementally and a node fed only the final converged count 3 must
    /// derive the SAME narration — exactly ONE retry-pass line for that
    /// `(phase, bucket)` — since both see the same presence in the converged
    /// CRDT. This pins the presence-set (not count-diff) derivation.
    #[test]
    fn retry_pass_milestone_failover_consistent_incremental_vs_converged() {
        // Node A: watched each increment as a live primary.
        let incremental = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            for n in 1..=3 {
                bump_retry_pass(&mut state, "compile", BucketKind::Oom, n);
                narrator.observe(&state);
            }
        });
        // Node B: promoted/observing, fed only the converged count 3.
        let converged = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            bump_retry_pass(&mut state, "compile", BucketKind::Oom, 3);
            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
        });

        let count = |events: &[crate::test_capture::CapturedEvent]| {
            events
                .iter()
                .filter(|e| e.message.contains("OOM-retry-pass start"))
                .count()
        };
        assert_eq!(
            count(&incremental),
            1,
            "incremental observer emits one line: {incremental:?}"
        );
        assert_eq!(
            count(&converged),
            1,
            "converged-count observer emits one line: {converged:?}"
        );
        assert_eq!(
            count(&incremental),
            count(&converged),
            "incremental and converged nodes derive the SAME narration from the converged CRDT",
        );
    }

    /// Snapshot-driven: a freshly-promoted/observing node fed a converged
    /// CRDT whose `retry_passes_used` is ALREADY at N (with no per-task work
    /// for the phase in this replica's mirror) emits the retry-pass milestone
    /// once for the 0→N step — and an idempotent re-observe of that same
    /// state emits nothing further. Proves the derivation is purely
    /// snapshot-driven (the milestone has no source but the converged count)
    /// and dedups on re-observe.
    #[test]
    fn retry_pass_milestone_snapshot_driven_and_dedups() {
        let events = capture(|| {
            // A remote promoted primary ran 4 OOM-retry passes for this
            // phase; only the converged count survives in this replica.
            let mut state = ClusterState::<RunnerIdentifier>::new();
            bump_retry_pass(&mut state, "remote-phase", BucketKind::Oom, 4);

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            // Idempotent re-observe of the converged state.
            narrator.observe(&state);
        });

        let oom: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("OOM-retry-pass start"))
            .collect();
        assert_eq!(
            oom.len(),
            1,
            "one OOM-retry-pass line for the converged 0→N step, dedup'd on re-observe: {events:?}"
        );
        assert_eq!(
            oom[0].fields.get("phase").map(String::as_str),
            Some("remote-phase")
        );
    }

    // ── Failover / degradation narration ──

    /// Initial fleet formation — the primary being named and the secondaries
    /// trickling in across SEVERAL observe() calls before any work dispatches
    /// — narrates NO failover/degradation line. The first observe seeds the
    /// baseline; the operational gate keeps every later formation join silent
    /// until work starts. Models an early cold-join observer watching the
    /// fleet grow during setup.
    #[test]
    fn initial_formation_seeds_silently() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();

            // Fleet forms incrementally; each step is observed. NO work has
            // started, so the run is not operational.
            set_primary(&mut state, "n1", 1);
            narrator.observe(&state);
            join_secondary(&mut state, "n1");
            join_secondary(&mut state, "n2");
            narrator.observe(&state);
            join_secondary(&mut state, "n3");
            narrator.observe(&state);
            // Re-observe the formed, stable (still-setup) fleet.
            narrator.observe(&state);
        });

        assert!(
            events.iter().all(|e| {
                let m = &e.message;
                !m.contains("left the cluster")
                    && !m.contains("joined the cluster")
                    && !m.contains("failed over")
                    && !m.contains("failover in progress")
            }),
            "formation (pre-operational) must narrate NO failover/degradation line: {events:?}"
        );
    }

    /// Production shape: the observer's FIRST observe already sees the
    /// converged, operational fleet (work dispatching, full roster, primary
    /// established post-relocation). That first observe seeds silently, and a
    /// stable re-observe narrates nothing — no spurious "joined" / "primary
    /// failed over" for the inherited roster.
    #[test]
    fn converged_operational_first_observe_seeds_silently() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // Operational, fully-formed fleet at construction (the relocated /
            // cold-join observer inherits this converged state).
            start_work(&mut state);
            set_primary(&mut state, "n1", 1);
            join_secondary(&mut state, "n1");
            join_secondary(&mut state, "n2");
            join_secondary(&mut state, "n3");

            let mut narrator = RunNarrator::new();
            // First observe of the converged, operational state: seed only.
            narrator.observe(&state);
            // Stable re-observe: idempotent.
            narrator.observe(&state);
        });

        assert!(
            events.iter().all(|e| {
                let m = &e.message;
                !m.contains("left the cluster")
                    && !m.contains("joined the cluster")
                    && !m.contains("failed over")
                    && !m.contains("failover in progress")
            }),
            "a converged operational first-observe seeds the roster silently: {events:?}"
        );
    }

    /// A remote secondary that departs the live membership AFTER the fleet was
    /// seeded narrates exactly one "secondary left the cluster" line carrying
    /// the post-departure live count; a re-observe is idempotent.
    #[test]
    fn peer_lost_on_post_seed_departure() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "n1", 1);
            join_secondary(&mut state, "n1");
            join_secondary(&mut state, "n2");
            join_secondary(&mut state, "n3");

            let mut narrator = RunNarrator::new();
            // Seed the formed fleet (n2, n3 are remote; n1 is the primary's
            // own co-located secondary, excluded from the remote roster).
            narrator.observe(&state);
            // n2 dies.
            remove_peer(&mut state, "n2");
            narrator.observe(&state);
            // Stable → idempotent.
            narrator.observe(&state);
        });

        let lost: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("secondary left the cluster"))
            .collect();
        assert_eq!(lost.len(), 1, "exactly one peer-lost line: {events:?}");
        assert_eq!(
            lost[0].fields.get("secondary").map(String::as_str),
            Some("n2")
        );
        // One remote secondary (n3) remains live after n2's departure.
        assert_eq!(lost[0].fields.get("live").map(String::as_str), Some("1"));
    }

    /// A brand-new remote secondary appearing AFTER the seed narrates exactly
    /// one "secondary joined the cluster" line; the seeded ones do not.
    #[test]
    fn peer_rejoined_on_post_seed_join() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "n1", 1);
            join_secondary(&mut state, "n1");
            join_secondary(&mut state, "n2");

            let mut narrator = RunNarrator::new();
            // Seed: n2 is the only remote secondary.
            narrator.observe(&state);
            // A new worker-secondary joins post-establishment.
            join_secondary(&mut state, "n3");
            narrator.observe(&state);
            // Stable → idempotent.
            narrator.observe(&state);
        });

        let joined: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("secondary joined the cluster"))
            .collect();
        assert_eq!(
            joined.len(),
            1,
            "exactly one peer-rejoined line for the post-seed joiner: {events:?}"
        );
        assert_eq!(
            joined[0].fields.get("secondary").map(String::as_str),
            Some("n3")
        );
    }

    /// PRIMARY-LOST: when the recognised primary is no longer a live member
    /// (its `PeerRemoved` landed but no new `PrimaryChanged` has yet) the
    /// "primary left the mesh — failover in progress" line fires once. This
    /// holds for a PRIMARY-ONLY host (no worker capacity): the detection reads
    /// `is_peer_alive`, NOT the worker-secondary roster, so a healthy
    /// primary-only host never false-positives and a dead one is caught.
    #[test]
    fn primary_lost_when_primary_leaves_membership() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            // Primary-only host (PeerJoined, but NO SecondaryCapacity → absent
            // from alive_secondary_members even while alive).
            state.apply(ClusterMutation::PeerJoined {
                peer_id: "p".to_string(),
                is_observer: false,
                can_be_primary: true,
                cap_version: Default::default(),
                member_gen: 0,
            });
            set_primary(&mut state, "p", 1);
            join_secondary(&mut state, "w1");

            let mut narrator = RunNarrator::new();
            // Seed: healthy primary-only host must NOT narrate primary-lost.
            narrator.observe(&state);
            // The primary node dies; no new primary named yet.
            remove_peer(&mut state, "p");
            narrator.observe(&state);
            // Stable → idempotent.
            narrator.observe(&state);
        });

        let lost: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("failover in progress"))
            .collect();
        assert_eq!(lost.len(), 1, "exactly one primary-lost line: {events:?}");
        assert_eq!(lost[0].fields.get("primary").map(String::as_str), Some("p"));
    }

    /// PRIMARY-CHANGED: a differing `(id, epoch)` after the seed narrates the
    /// "primary failed over" line once per new primary; the initial
    /// establishment is silent and a stable re-observe is idempotent.
    #[test]
    fn primary_changed_on_failover_once() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");

            let mut narrator = RunNarrator::new();
            // Seed the initial primary (p1, epoch 1) silently.
            narrator.observe(&state);
            // Failover: p1 dies, p2 promoted at a higher epoch.
            remove_peer(&mut state, "p1");
            set_primary(&mut state, "p2", 2);
            narrator.observe(&state);
            // Stable → idempotent.
            narrator.observe(&state);
        });

        let changed: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("failed over to"))
            .collect();
        assert_eq!(
            changed.len(),
            1,
            "exactly one primary-changed line, not on the initial seed: {events:?}"
        );
        assert_eq!(
            changed[0].fields.get("primary").map(String::as_str),
            Some("p2")
        );
        assert_eq!(
            changed[0].fields.get("epoch").map(String::as_str),
            Some("2")
        );
    }

    /// No-wall-clock implicit "election stuck": a primary that leaves the mesh
    /// with NO following `PrimaryChanged` stays visible as a primary-lost line
    /// with no primary-changed line — a wedged failover is NOT silent.
    #[test]
    fn wedged_failover_visible_without_resolution() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");

            let mut narrator = RunNarrator::new();
            narrator.observe(&state); // seed
            // p1 dies, election never completes (no new PrimaryChanged).
            remove_peer(&mut state, "p1");
            narrator.observe(&state);
            // Time passes (more observes), still no resolution.
            narrator.observe(&state);
            narrator.observe(&state);
        });

        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.contains("failover in progress"))
                .count(),
            1,
            "the wedged failover is visible: one primary-lost line: {events:?}"
        );
        assert!(
            events.iter().all(|e| !e.message.contains("failed over to")),
            "an unresolved failover narrates NO primary-changed line: {events:?}"
        );
    }

    /// A multi-hop failover (p1 → p2 → p3) narrates each transition once. The
    /// first hop is WEDGED — the observer catches the dead-primary-with-no-
    /// replacement window (a primary-lost line), then sees it resolve (a
    /// primary-changed line). The second hop is FAST — the removal and the new
    /// `PrimaryChanged` land in the same observe, so the observer never sees a
    /// dead recognised primary and only the resolution (primary-changed)
    /// narrates. primary-lost is keyed per departed-primary-id and
    /// primary-changed per new `(id, epoch)`, so nothing re-emits.
    #[test]
    fn second_failover_narrates_each_transition_once() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");
            join_secondary(&mut state, "p3");

            let mut narrator = RunNarrator::new();
            narrator.observe(&state); // seed at p1
            // First failover (WEDGED then resolved): p1 dies, election lags.
            remove_peer(&mut state, "p1");
            narrator.observe(&state); // catches p1 dead, no replacement
            set_primary(&mut state, "p2", 2);
            narrator.observe(&state); // resolved → p2
            // Second failover (FAST): removal + promotion in one window.
            remove_peer(&mut state, "p2");
            set_primary(&mut state, "p3", 3);
            narrator.observe(&state); // only the resolution is observable
            narrator.observe(&state); // stable → idempotent
        });

        // p1's death was caught wedged → one primary-lost line. p2 never had a
        // dead-recognised-primary window (fast hop) → no primary-lost for p2.
        let lost: Vec<&str> = events
            .iter()
            .filter(|e| e.message.contains("failover in progress"))
            .filter_map(|e| e.fields.get("primary").map(String::as_str))
            .collect();
        assert_eq!(
            lost,
            vec!["p1"],
            "only the wedged hop's departed primary narrates a lost line: {events:?}"
        );
        // BOTH resolutions narrate, once each, in order.
        let changed: Vec<&str> = events
            .iter()
            .filter(|e| e.message.contains("failed over to"))
            .filter_map(|e| e.fields.get("primary").map(String::as_str))
            .collect();
        assert_eq!(
            changed,
            vec!["p2", "p3"],
            "each new primary narrates a changed line once, in order: {events:?}"
        );
    }

    /// The wake-stream host contract: `observe` returns `true` exactly on
    /// calls that emitted ≥1 narrative event, `false` on quiet calls —
    /// the seam the observer loop keys the reconnection-note flush on.
    #[test]
    fn observe_returns_whether_anything_was_narrated() {
        capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            // Empty ledger: nothing to narrate.
            assert!(!narrator.observe(&state), "empty ledger → quiet");
            // A phase reaches its start edge: narrated.
            add(&mut state, &task("compile", "a", &[]));
            assert!(narrator.observe(&state), "phase start → emitted");
            // Stable ledger: idempotent re-observe is quiet.
            assert!(!narrator.observe(&state), "stable ledger → quiet");
            // Phase completes: narrated again.
            complete(&mut state, "a");
            assert!(narrator.observe(&state), "phase complete → emitted");
            assert!(!narrator.observe(&state), "stable again → quiet");
        });
    }

    /// Rule 2 replay (the down-7-minutes spec case at the narrator host):
    /// nothing emits at reconnect; the NEXT phase event carries the
    /// reconnection note — attached exactly once, via the same
    /// `if observe() { flush_after_host() }` composition the observer
    /// loop runs.
    #[test]
    fn reconnection_note_rides_next_phase_event_exactly_once() {
        use crate::observer::lost_visibility::WakeNoteSlot;

        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            let note = WakeNoteSlot::default();

            // The loss policy parked the note at regain (outage > 5 min,
            // no periodic elapsed). Quiet iterations flush nothing — the
            // note waits.
            note.set("reconnection-note".to_string());
            if narrator.observe(&state) {
                note.flush_after_host();
            }
            assert!(note.is_pending(), "no host yet — the note waits");

            // The next phase event hosts the note.
            add(&mut state, &task("compile", "a", &[]));
            if narrator.observe(&state) {
                note.flush_after_host();
            }
            assert!(!note.is_pending(), "the phase event hosted the note");

            // A later phase event must NOT re-attach it.
            complete(&mut state, "a");
            if narrator.observe(&state) {
                note.flush_after_host();
            }
        });

        let notes: Vec<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, e)| e.message.contains("reconnection-note"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(notes.len(), 1, "the note attaches exactly once: {events:?}");
        // It rides AFTER the phase-start block of the hosting iteration
        // and BEFORE the next iteration's phase-complete line.
        let start_idx = events
            .iter()
            .position(|e| e.message.contains("starting job phase"))
            .expect("phase start narrated");
        let done_idx = events
            .iter()
            .position(|e| e.message.contains("phase complete"))
            .expect("phase complete narrated");
        assert!(
            start_idx < notes[0] && notes[0] < done_idx,
            "the note rides together with the hosting iteration: {events:?}"
        );
    }

    // ── #563 Seam 3 — failover-narration suppression under the run-terminal latch ──
    //
    // Production trace (asm-tokenizer 2026-06-15): the observer saw a
    // "primary failed over to secondary-1 (epoch 2)" WARN line and NO "run
    // aborted — shutting down" ERROR line, because the dying primary's
    // RunAborted broadcast was lost on the observer's leg (the existing
    // `await_terminal_observer_delivery` 60s re-broadcast hold is observer-
    // leg-only and cannot retroactively cover a never-formed leg). When the
    // verdict DID converge — via snapshot pull / anti-entropy — the
    // narrator's emit ordering puts `narrate_failover` BEFORE the
    // run_aborted completion block, so the operator's eye lands on the
    // failover narrative even when the run-aborted line ALSO fires the
    // same iteration. Seam 3 gates the FAILOVER / MEMBERSHIP narration on
    // `run_aborted().is_some() || run_complete()`: under a terminal latch
    // all churn is teardown noise; the completion block is the single
    // owner of the operator's terminal-state line.

    /// SEAM 3 — when a `PrimaryChanged` and a `RunAborted` are both
    /// observed in the same window (the asm-tokenizer 2026-06-15 race), the
    /// "primary failed over" WARN is SUPPRESSED and the "run aborted —
    /// shutting down" ERROR fires with the verbatim reason. Pinned here so
    /// the operator's authoritative terminal-state line is not upstaged by
    /// the now-noise failover label.
    #[test]
    fn primary_failed_over_suppressed_under_run_aborted_latch() {
        const ABORT_REASON: &str = "runtime spawn_tasks rejected 46497 task(s): \
                                    [duplicate task identity dependency_graph, ...]";
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");

            let mut narrator = RunNarrator::new();
            // Seed at p1 silently.
            narrator.observe(&state);
            // The failover + the terminal verdict converge in the same observe
            // window — the production race the user's transcript captured.
            remove_peer(&mut state, "p1");
            set_primary(&mut state, "p2", 2);
            state.apply(ClusterMutation::RunAborted {
                reason: ABORT_REASON.into(),
                counts: Default::default(),
            });
            narrator.observe(&state);
            // Subsequent observes must stay quiet on the failover seam (the
            // baseline advanced; the gate stays terminal-suppressed).
            narrator.observe(&state);
        });

        assert!(
            events.iter().all(|e| !e.message.contains("failed over to")),
            "the failover narration must be SUPPRESSED under a latched RunAborted; \
             got events: {events:?}",
        );
        assert!(
            events
                .iter()
                .all(|e| !e.message.contains("failover in progress")),
            "the primary-lost narration must also be SUPPRESSED under the latch; \
             got events: {events:?}",
        );
        let aborted: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("run aborted"))
            .collect();
        assert_eq!(
            aborted.len(),
            1,
            "exactly one 'run aborted — shutting down' line carries the operator's \
             terminal state: {events:?}",
        );
        assert_eq!(
            aborted[0].fields.get("reason").map(String::as_str),
            Some(ABORT_REASON),
            "the run-aborted line must carry the dying primary's VERBATIM reason \
             (the rejection-id list the user expects to see)",
        );
    }

    /// SEAM 3 — symmetric clean-finish: a `RunComplete` latch ALSO
    /// suppresses the failover narration. The previously-flagged BUG-B
    /// shape (a clean-end leg drop racing the completion broadcast) must
    /// not narrate as a benign-looking failover.
    #[test]
    fn primary_failed_over_suppressed_under_run_complete_latch() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            remove_peer(&mut state, "p1");
            set_primary(&mut state, "p2", 2);
            state.apply(ClusterMutation::RunComplete {
                counts: Default::default(),
            });
            narrator.observe(&state);
        });

        assert!(
            events.iter().all(|e| !e.message.contains("failed over to")),
            "the failover narration must be SUPPRESSED under a latched RunComplete; \
             got events: {events:?}",
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.contains("run complete"))
                .count(),
            1,
            "exactly one 'run complete' line is the operator's terminal-state line: \
             {events:?}",
        );
    }

    /// SEAM 3 — secondary-membership churn (peer-left / peer-rejoined)
    /// is ALSO suppressed under the terminal latch. A teardown phase's
    /// membership flicker (peers dropping out as the cluster winds down)
    /// is not a wake-worthy operator event under an authored terminal.
    #[test]
    fn membership_churn_suppressed_under_run_terminal_latch() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");
            join_secondary(&mut state, "p3");

            let mut narrator = RunNarrator::new();
            narrator.observe(&state); // seed
            // Terminal latch lands first.
            state.apply(ClusterMutation::RunAborted {
                reason: "deliberate".into(),
                counts: Default::default(),
            });
            // Now a peer departs as part of teardown — should NOT narrate.
            remove_peer(&mut state, "p2");
            narrator.observe(&state);
        });

        assert!(
            events
                .iter()
                .all(|e| !e.message.contains("secondary left the cluster")),
            "peer-departure narration is teardown noise under the latch: {events:?}",
        );
    }

    /// SEAM 3 NEGATIVE REGRESSION — a mid-run failover with NO terminal
    /// latch still narrates exactly as before. Pinned here so a future
    /// refactor that misplaced the `terminal` factor (e.g. inverted, or
    /// applied unconditionally) is caught locally to the narrator file.
    /// Mirrors the existing `primary_changed_on_failover_once` test —
    /// kept here so the suppression's negative twin is grouped with its
    /// positive cases.
    #[test]
    fn no_latch_still_narrates_failover_regression() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");

            let mut narrator = RunNarrator::new();
            narrator.observe(&state); // seed at p1 silently
            remove_peer(&mut state, "p1");
            set_primary(&mut state, "p2", 2);
            // NO RunAborted / RunComplete applied — pure mid-run failover.
            narrator.observe(&state);
        });

        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.contains("failed over to"))
                .count(),
            1,
            "without a terminal latch, mid-run failover must still narrate (no \
             regression on the BUG-H shape): {events:?}",
        );
    }

    /// SEAM 3 — UNRESOLVED failover (latch not yet observed) still
    /// narrates the primary-lost line; the terminal-suppression is
    /// strictly per-call. This is the "primary left the mesh — failover
    /// in progress" path: observed BEFORE the verdict converges, narrated
    /// as normal; once the verdict converges, subsequent observes do not
    /// re-emit (the once-per-id edge-set advances on the real emit). No
    /// prior emit is rewound — the gate is per-call only.
    #[test]
    fn unresolved_failover_pre_latch_still_narrates_then_suppresses() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            start_work(&mut state); // run operational
            set_primary(&mut state, "p1", 1);
            join_secondary(&mut state, "p1");
            join_secondary(&mut state, "p2");

            let mut narrator = RunNarrator::new();
            narrator.observe(&state); // seed at p1
            // p1 leaves; NO terminal latch yet — primary-lost must narrate.
            remove_peer(&mut state, "p1");
            narrator.observe(&state);
            // Verdict converges later; the membership-departure has ALREADY
            // narrated. The terminal-suppression gate does NOT rewind it.
            state.apply(ClusterMutation::RunAborted {
                reason: "late convergence".into(),
                counts: Default::default(),
            });
            narrator.observe(&state);
        });

        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.contains("failover in progress"))
                .count(),
            1,
            "the in-flight failover narrated before the verdict converged (per-call \
             suppression, not retroactive): {events:?}",
        );
        // The completion block also fires once.
        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.contains("run aborted"))
                .count(),
            1,
            "the terminal verdict narrates once when it lands: {events:?}",
        );
    }

    // ── #568 — completion of #520's CRDT-change coverage ──
    //
    // The 4 audit gaps below are all PRESENCE / once-per-key edge emits over
    // CRDT projections — the same shape as the started/done/retry/setup
    // blocks above. Each test applies the relevant `ClusterMutation`, drives
    // `observe()`, and asserts exactly ONE line on the IMPORTANT target with
    // the right class. A regression sibling at the bottom verifies a static
    // sibling (`PhaseDepsSet`) stays SILENT — the negative twin pinning that
    // the new arms emit only on the CRDT changes they own.

    /// Gap A — `WindDownRequested` for `(secondary_id, member_gen)` narrates
    /// one WARN line carrying the pair (operator wake: a SLURM slot is about
    /// to be released). Idempotent across re-observes; a SECOND directive at
    /// a NEW generation for the same id narrates again (different pair).
    #[test]
    fn wind_down_requested_narrates_once_per_pair() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            // First directive lands → one WARN.
            state.apply(ClusterMutation::WindDownRequested {
                secondary_id: "replacement-1".to_string(),
                member_gen: 7,
            });
            narrator.observe(&state);
            // Re-observe of the unchanged ledger: idempotent.
            narrator.observe(&state);
            // A directive at a NEW generation for the SAME id is a distinct
            // pair → narrates again (the grow-only set keys on the full pair).
            state.apply(ClusterMutation::WindDownRequested {
                secondary_id: "replacement-1".to_string(),
                member_gen: 9,
            });
            narrator.observe(&state);
            // And a directive for a different id at gen 7.
            state.apply(ClusterMutation::WindDownRequested {
                secondary_id: "replacement-2".to_string(),
                member_gen: 7,
            });
            narrator.observe(&state);
            // Final stable re-observe: idempotent.
            narrator.observe(&state);
        });

        let wd: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("wind-down requested"))
            .collect();
        assert_eq!(
            wd.len(),
            3,
            "one WARN per distinct (id, member_gen) pair: {events:?}",
        );
        // Pin the exact identity each line carries.
        let pairs: Vec<(Option<&str>, Option<&str>)> = wd
            .iter()
            .map(|e| {
                (
                    e.fields.get("secondary").map(String::as_str),
                    e.fields.get("member_gen").map(String::as_str),
                )
            })
            .collect();
        assert!(pairs.contains(&(Some("replacement-1"), Some("7"))));
        assert!(pairs.contains(&(Some("replacement-1"), Some("9"))));
        assert!(pairs.contains(&(Some("replacement-2"), Some("7"))));
    }

    /// Gap D-Posted — the F5 inbox LANDING edge narrates one INFO line per
    /// `(origin, seq)` key the moment the `Unhandled` snapshot lands, carrying
    /// the topic the handler is about to dispatch on. Idempotent across
    /// re-observes; per-key — distinct origins and seqs each narrate once.
    ///
    /// Terminals (`Handled`/`Failed`) are NOT covered by THIS narrator —
    /// the `compact_custom_watermark` apply rule erases the label, so the
    /// state-derived path cannot distinguish them; #570 owns the
    /// event-driven follow-up that surfaces the terminal split.
    #[test]
    fn custom_message_posted_narrates_once_per_key() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();

            // Key #1 lands as Posted (Unhandled) → one INFO carrying topic.
            state.apply(ClusterMutation::CustomMessagePosted {
                origin: "n1".to_string(),
                seq: 1,
                topic: "important-topic".to_string(),
                data: vec![1, 2, 3],
                is_high_volume: false,
            });
            narrator.observe(&state);
            narrator.observe(&state); // idempotent re-observe.

            // A distinct (origin, seq) — second INFO.
            state.apply(ClusterMutation::CustomMessagePosted {
                origin: "n2".to_string(),
                seq: 1,
                topic: "user-topic".to_string(),
                data: vec![4, 5],
                is_high_volume: false,
            });
            narrator.observe(&state);
            narrator.observe(&state); // idempotent.
        });

        let posted: Vec<_> = events
            .iter()
            .filter(|e| e.message.starts_with("custom message posted:"))
            .collect();
        assert_eq!(
            posted.len(),
            2,
            "one posted line per (origin, seq) key: {events:?}"
        );
        let posted_topics: Vec<Option<&str>> = posted
            .iter()
            .map(|e| e.fields.get("topic").map(String::as_str))
            .collect();
        assert!(posted_topics.contains(&Some("important-topic")));
        assert!(posted_topics.contains(&Some("user-topic")));
        let posted_origins: Vec<Option<&str>> = posted
            .iter()
            .map(|e| e.fields.get("origin").map(String::as_str))
            .collect();
        assert!(posted_origins.contains(&Some("n1")));
        assert!(posted_origins.contains(&Some("n2")));
    }

    /// T1 (#583/#587): a CustomMessagePosted with `is_high_volume=true`
    /// narrates the "custom message posted" line on
    /// `OBSERVER_TASK_TARGET` instead of `IMPORTANT_TARGET`. The
    /// `--important-stdio-only` stdio gate (an allow-list on
    /// `IMPORTANT_TARGET`) drops it; the full log still captures it on
    /// either target. The rate-limited custom-message aggregator
    /// rollup on `IMPORTANT_TARGET` is the wake signal at scale.
    #[test]
    fn high_volume_posted_routes_to_observer_task_target() {
        use crate::test_capture::TargetCapture;
        use dynrunner_core::{IMPORTANT_TARGET, OBSERVER_TASK_TARGET};
        let on_observer = TargetCapture::for_target(OBSERVER_TASK_TARGET);
        let on_important = TargetCapture::for_target(IMPORTANT_TARGET);
        let subscriber = Registry::default()
            .with(on_observer.clone())
            .with(on_important.clone());
        with_default(subscriber, || {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            state.apply(ClusterMutation::CustomMessagePosted {
                origin: "n1".to_string(),
                seq: 1,
                topic: "dep_graph_spawn".to_string(),
                data: vec![1, 2, 3],
                is_high_volume: true,
            });
            narrator.observe(&state);
        });
        let on_observer_events = on_observer.events();
        let on_important_events = on_important.events();
        assert!(
            on_observer_events
                .iter()
                .any(|e| e.event.message.starts_with("custom message posted:")),
            "high-volume Posted narrates on OBSERVER_TASK_TARGET: {on_observer_events:?}"
        );
        assert!(
            !on_important_events
                .iter()
                .any(|e| e.event.message.starts_with("custom message posted:")),
            "high-volume Posted MUST NOT narrate on IMPORTANT_TARGET: {on_important_events:?}"
        );
    }

    /// T2 (#583/#587 regression guard): a CustomMessagePosted with the
    /// DEFAULT `is_high_volume=false` keeps narrating the "custom
    /// message posted" line on `IMPORTANT_TARGET` (the pre-#583 shape
    /// — a low-fanout consumer's wake-worthy custom-message landing).
    #[test]
    fn low_volume_posted_stays_on_important_target() {
        use crate::test_capture::TargetCapture;
        use dynrunner_core::{IMPORTANT_TARGET, OBSERVER_TASK_TARGET};
        let on_observer = TargetCapture::for_target(OBSERVER_TASK_TARGET);
        let on_important = TargetCapture::for_target(IMPORTANT_TARGET);
        let subscriber = Registry::default()
            .with(on_observer.clone())
            .with(on_important.clone());
        with_default(subscriber, || {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            state.apply(ClusterMutation::CustomMessagePosted {
                origin: "n1".to_string(),
                seq: 1,
                topic: "low-fanout-control".to_string(),
                data: vec![],
                is_high_volume: false,
            });
            narrator.observe(&state);
        });
        let on_important_events = on_important.events();
        let on_observer_events = on_observer.events();
        assert!(
            on_important_events
                .iter()
                .any(|e| e.event.message.starts_with("custom message posted:")),
            "low-volume Posted stays on IMPORTANT_TARGET: {on_important_events:?}"
        );
        assert!(
            !on_observer_events
                .iter()
                .any(|e| e.event.message.starts_with("custom message posted:")),
            "low-volume Posted MUST NOT route to OBSERVER_TASK_TARGET: {on_observer_events:?}"
        );
    }

    /// T5 (#583/#587 regression guard): the peer-membership /
    /// phase-lifecycle narration arms are NOT per-task and NOT
    /// custom-message — they stay on `IMPORTANT_TARGET`, the
    /// wake-worthy class. A peer-left + a phase-start round-trips
    /// through the narrator AND lands on `IMPORTANT_TARGET` only —
    /// nothing in the new high-volume primitive may flip these normal
    /// arms onto OBSERVER_TASK_TARGET.
    #[test]
    fn peer_leave_and_phase_start_stay_on_important_target() {
        use crate::test_capture::TargetCapture;
        use dynrunner_core::{IMPORTANT_TARGET, OBSERVER_TASK_TARGET};
        let on_observer = TargetCapture::for_target(OBSERVER_TASK_TARGET);
        let on_important = TargetCapture::for_target(IMPORTANT_TARGET);
        let subscriber = Registry::default()
            .with(on_observer.clone())
            .with(on_important.clone());
        with_default(subscriber, || {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // Phase start path: a fresh dispatchable phase fires
            // "starting job phase" on IMPORTANT_TARGET (the canonical
            // wake line for the operator's run-start signal).
            add(&mut state, &task("compile", "a", &[]));
            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
        });
        let on_important_events = on_important.events();
        let on_observer_events = on_observer.events();
        assert!(
            on_important_events
                .iter()
                .any(|e| e.event.message.contains("starting job phase")),
            "phase-start narrates on IMPORTANT_TARGET (regression guard for #583/#587): \
             {on_important_events:?}"
        );
        // Cross-target check: nothing from this narration arm leaks
        // onto OBSERVER_TASK_TARGET — the new primitive is opt-in per
        // narration kind, not a blanket move.
        assert!(
            on_observer_events.is_empty(),
            "phase-start MUST NOT route to OBSERVER_TASK_TARGET (regression guard): \
             {on_observer_events:?}"
        );
    }

    /// #570 follow-up boundary — the state-derived narrator does NOT emit a
    /// terminal line for `CustomMessageHandled` / `CustomMessageFailed`.
    /// `compact_custom_watermark` (apply_custom.rs) erases the
    /// Handled/Failed label at the SAME apply that lands the terminal, so a
    /// state-derived path cannot distinguish the two; the audit's Gap B +
    /// Gap D-Handled split lines belong on the event-driven channel (#570)
    /// that retains the label per-mutation. This test pins the silence so a
    /// future change that re-adds a degraded "terminal" line from state
    /// alone is caught here.
    #[test]
    fn custom_message_terminals_are_silent_in_state_narrator() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();

            // Unhandled → Posted line narrates as usual.
            state.apply(ClusterMutation::CustomMessagePosted {
                origin: "n1".to_string(),
                seq: 1,
                topic: "t".to_string(),
                data: vec![1],
                is_high_volume: false,
            });
            narrator.observe(&state);
            // Clean handler → Handled (compacts immediately, label erased).
            state.apply(ClusterMutation::CustomMessageHandled {
                origin: "n1".to_string(),
                seq: 1,
            });
            narrator.observe(&state);

            // Different key: handler raises → Failed (compacts immediately).
            state.apply(ClusterMutation::CustomMessagePosted {
                origin: "n2".to_string(),
                seq: 1,
                topic: "u".to_string(),
                data: vec![2],
                is_high_volume: false,
            });
            narrator.observe(&state);
            state.apply(ClusterMutation::CustomMessageFailed {
                origin: "n2".to_string(),
                seq: 1,
                // Pre-#570 the wire mutation had no reason; today the
                // state-derived narrator still ignores it (the silence
                // pin is reason-independent — the watermark compactor
                // erases the label either way), so an empty reason is
                // the cleanest re-statement of the same pin.
                reason: String::new(),
            });
            narrator.observe(&state);
        });

        assert!(
            events
                .iter()
                .all(|e| !e.message.starts_with("custom message handled:")),
            "the state-derived narrator never emits a 'handled' terminal line — #570 \
             owns the event-driven follow-up that retains the label: {events:?}",
        );
        assert!(
            events
                .iter()
                .all(|e| !e.message.starts_with("custom message handler FAILED")),
            "the state-derived narrator never emits a 'handler FAILED' terminal line — \
             #570 owns the event-driven follow-up: {events:?}",
        );
        // The Posted lines DO narrate, one per key — pins that the
        // landing-edge arm is unaffected by the terminal-silence policy.
        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.starts_with("custom message posted:"))
                .count(),
            2,
            "the landing-edge narration is unaffected by the terminal-silence policy: \
             {events:?}",
        );
    }

    /// Gap C — `DiscoveryDebtDeclared` narrates the "discovery owed" INFO
    /// once; the later `DiscoverySettled` narrates the "discovery settled"
    /// INFO once. A run that never declares debt stays silent on this seam.
    #[test]
    fn discovery_debt_owed_then_settled_narrates_each_once() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            let mut narrator = RunNarrator::new();
            // Owed lands first → one INFO.
            state.apply(ClusterMutation::DiscoveryDebtDeclared);
            narrator.observe(&state);
            narrator.observe(&state); // idempotent on Owed.
            // Then Settled lands → one INFO closing line.
            state.apply(ClusterMutation::DiscoverySettled);
            narrator.observe(&state);
            narrator.observe(&state); // idempotent on Settled.
        });

        let owed: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("discovery owed"))
            .collect();
        assert_eq!(owed.len(), 1, "one owed line: {events:?}");

        let settled: Vec<_> = events
            .iter()
            .filter(|e| e.message.contains("discovery settled"))
            .collect();
        assert_eq!(settled.len(), 1, "one settled line: {events:?}");

        // Ordering: owed precedes settled.
        let oi = events
            .iter()
            .position(|e| e.message.contains("discovery owed"))
            .unwrap();
        let si = events
            .iter()
            .position(|e| e.message.contains("discovery settled"))
            .unwrap();
        assert!(oi < si, "owed precedes settled: {events:?}");
    }

    /// Gap C — the PRESENCE-latch shape: an observer whose first observe
    /// already sees a converged `Settled` mirror (the relocation-inherited
    /// shape, OR the empty-corpus path that latches `Settled` immediately)
    /// narrates ONLY the "discovery settled" line. The intermediate `Owed`
    /// line is silent because this observer never saw that value.
    #[test]
    fn discovery_settled_first_observe_skips_the_owed_line() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            // A relocated observer inherits the already-converged Settled.
            state.apply(ClusterMutation::DiscoveryDebtDeclared);
            state.apply(ClusterMutation::DiscoverySettled);

            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            narrator.observe(&state); // idempotent.
        });

        assert!(
            events.iter().all(|e| !e.message.contains("discovery owed")),
            "first-observe at Settled skips the Owed line: {events:?}",
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.message.contains("discovery settled"))
                .count(),
            1,
            "the Settled-first observer narrates the settled line once: {events:?}",
        );
    }

    /// Gap C negative — a run that never declares debt (the cold-seeded /
    /// mode-1 shape, `DiscoveryDebt::Undeclared`) narrates NEITHER line.
    /// Mirrors the `no_setup_tasks_narrates_no_setup_lines` precedent: a
    /// state-derived feature must be inert when its CRDT projection is the
    /// default/bottom value.
    #[test]
    fn discovery_debt_undeclared_narrates_nothing() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            add(&mut state, &task("compile", "a", &[]));
            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
        });
        assert!(
            events.iter().all(|e| !e.message.contains("discovery")),
            "an Undeclared run narrates no discovery line: {events:?}",
        );
    }

    /// Negative-regression for the bundle — a sibling state-shape mutation
    /// (`PhaseDepsSet`) that this narrator does NOT own emits ZERO new
    /// lines on its own. Pins that the new emit arms are gated on their
    /// own CRDT projections; an unrelated static-config mutation does not
    /// trigger any of them.
    #[test]
    fn unrelated_static_config_mutation_emits_no_new_lines() {
        let events = capture(|| {
            let mut state = ClusterState::<RunnerIdentifier>::new();
            state.apply(ClusterMutation::PhaseDepsSet {
                deps: std::collections::HashMap::from([(
                    PhaseId::from("late"),
                    vec![PhaseId::from("early")],
                )]),
            });
            let mut narrator = RunNarrator::new();
            narrator.observe(&state);
            narrator.observe(&state);
        });

        assert!(
            events.iter().all(|e| {
                let m = &e.message;
                !m.contains("wind-down")
                    && !m.contains("discovery owed")
                    && !m.contains("discovery settled")
                    && !m.contains("custom message")
            }),
            "PhaseDepsSet emits none of the #568 narration lines: {events:?}",
        );
    }
}
