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

use dynrunner_core::{IMPORTANT_TARGET, Identifier, PhaseId};

use crate::ClusterState;
use crate::primary::retry_bucket::BucketKind;

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
    /// Whether the one-shot run-complete / run-aborted summary has fired.
    /// The two are mutually exclusive and share this single latch so at
    /// most one terminal line is ever emitted.
    completion_emitted: bool,
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
    /// primary (the same `id != current_primary` cut
    /// [`ClusterState::alive_remote_secondary_count`] applies, so the
    /// primary's OWN co-located worker-secondary is never narrated as a peer:
    /// its departure is the primary-left event below, not a secondary
    /// departure). Maintained across observes so a set-difference against the
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
            completion_emitted: false,
            failover_seeded: false,
            live_remote_secondaries: HashSet::new(),
            last_primary: None,
            primary_lost_emitted: HashSet::new(),
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
    pub(crate) fn observe<I: Identifier>(&mut self, state: &ClusterState<I>) {
        // Phase transitions, read off the single owning phase-state
        // accessor. A phase counts as STARTED once it is dispatchable
        // (every dep-phase has fully terminated) and owns ≥1 task; it
        // counts as COMPLETE once it owns ≥1 task and has no live task
        // left (every task reached a terminal state).
        for (phase, rollup) in state.phase_rollups() {
            if rollup.has_any && rollup.dispatchable && self.started_phases.insert(phase.clone()) {
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
                // `has_any && dispatchable` edge the "starting job phase"
                // line fires on — the CRDT-side twin of the milestone the
                // removed projection emitted from `fire_initial_phase_starts`
                // (which originated `PhaseTaskSpawning` on the same
                // `phase_started_emitted.insert` edge as that line). Sharing
                // the `started_phases` edge keeps the two lines on one
                // once-per-phase guard, exactly as the authority did.
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
                let (to_run, skipped) = state.phase_task_partition(phase);
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
                let (overall_to_run, overall_skipped) = self
                    .started_phases
                    .iter()
                    .map(|p| state.phase_task_partition(p))
                    .fold((0usize, 0usize), |(tr, sk), (t, s)| (tr + t, sk + s));
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    to_run = overall_to_run,
                    skipped = overall_skipped,
                    "overall: {overall_to_run} to run, {overall_skipped} skipped (already done)",
                );
            }
            if rollup.has_any && !rollup.has_live && self.done_phases.insert(phase.clone()) {
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    phase = %phase,
                    "phase complete",
                );
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
        self.narrate_failover(state);

        // One-shot run summary, gated on the sticky run-complete /
        // run-aborted latches AND the local `completion_emitted` bool so
        // it fires exactly once across the entire observer tail. The two
        // outcomes are mutually exclusive: `run_complete` is the
        // happy-path terminal, `run_aborted` the failure twin; check
        // aborted first so an aborted run never narrates as completed.
        if !self.completion_emitted {
            if let Some(reason) = state.run_aborted() {
                self.completion_emitted = true;
                let o = state.outcome_counts();
                let c = state.counts();
                tracing::error!(
                    target: IMPORTANT_TARGET,
                    succeeded = o.succeeded,
                    fail_retry = o.fail_retry,
                    fail_oom = o.fail_oom,
                    fail_final = o.fail_final,
                    in_flight = c.in_flight,
                    blocked = c.blocked,
                    reason = %reason,
                    "run aborted — shutting down",
                );
            } else if state.run_complete() {
                self.completion_emitted = true;
                let o = state.outcome_counts();
                let c = state.counts();
                // This RICH line doubles as the final-stats flush the
                // owner requires before finishing: it is both the
                // operator's "all work done / job finished / shutting
                // down" marker AND the terminal outcome partition.
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    succeeded = o.succeeded,
                    fail_retry = o.fail_retry,
                    fail_oom = o.fail_oom,
                    fail_final = o.fail_final,
                    in_flight = c.in_flight,
                    blocked = c.blocked,
                    "run complete: {} succeeded / {} failed-final / {} oom / {} retried — shutting down",
                    o.succeeded,
                    o.fail_final,
                    o.fail_oom,
                    o.fail_retry,
                );
            }
        }
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
    fn narrate_failover<I: Identifier>(&mut self, state: &ClusterState<I>) {
        // The recognised primary, identity-only (epoch carried separately).
        let current_primary = state.current_primary().map(str::to_owned);

        // The REMOTE worker-secondary live set: every alive worker-secondary
        // EXCEPT the recognised primary's own co-located secondary capability
        // (the `id != current_primary` cut `alive_remote_secondary_count`
        // applies). So the primary's own departure is the primary-left event,
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
            return;
        }

        // Operational gate: a transition narrates only once the run is
        // dispatching work (at least one phase started). The phase block in
        // `observe` runs FIRST, so `started_phases` already reflects this
        // iteration. While not operational, formation churn is absorbed into
        // the tracked baseline below but never emitted.
        let operational = !self.started_phases.is_empty();

        // PEER-LOST: a remote secondary that was live and is now absent from
        // the live set departed. A secondary that became the primary is NOT a
        // departure (it left the remote set by promotion, narrated as
        // primary-changed) — exclude `current_primary`. The membership 2P-set
        // is sticky, so each id departs at most once — once-per-id, no
        // flicker.
        let live_count = live_remote.len();
        for departed in self.live_remote_secondaries.difference(&live_remote) {
            if operational && Some(departed.as_str()) != current_primary.as_deref() {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    secondary = %departed,
                    live = live_count,
                    "secondary left the cluster",
                );
            }
        }
        // PEER-REJOINED: a remote secondary live now but not in the prior set
        // joined AFTER the run was operational (a never-before-seen id — the
        // sticky-Dead ledger means a departed id cannot reappear).
        for joined in live_remote.difference(&self.live_remote_secondaries) {
            if operational {
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    secondary = %joined,
                    "secondary joined the cluster",
                );
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
            && let Some(primary) = current_primary.as_deref()
            && !state.is_peer_alive(primary)
            && self.primary_lost_emitted.insert(primary.to_owned())
        {
            tracing::warn!(
                target: IMPORTANT_TARGET,
                primary = %primary,
                "primary left the mesh — failover in progress",
            );
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
            if operational
                && let Some((id, epoch)) = current.as_ref()
            {
                tracing::warn!(
                    target: IMPORTANT_TARGET,
                    primary = %id,
                    epoch = *epoch,
                    "primary failed over to {id} (epoch {epoch})",
                );
            }
            self.last_primary = current;
        }
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
                })
                .collect(),
            preferred_secondaries: Default::default(),
            preferred_version: Default::default(),
            resolved_path: None,
        }
    }

    fn add(state: &mut ClusterState<RunnerIdentifier>, t: &TaskInfo<RunnerIdentifier>) {
        state.apply(ClusterMutation::TaskAdded {
            hash: t.task_id.clone(),
            task: t.clone(),
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
    /// phase" until the upstream phase fully terminates.
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
            // Build is live → compile not dispatchable yet; only build starts.
            narrator.observe(&state);
            // Build completes → compile becomes dispatchable.
            complete(&mut state, "tc");
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
            "build starts first; compile only after build terminates: {events:?}"
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
            state.apply(ClusterMutation::RunComplete);

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
            state.apply(ClusterMutation::RunAborted {
                reason: "fleet collapsed".into(),
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
    /// "overall: <N> to run, <M> skipped" running total — the overall derived
    /// from summing `phase_task_partition` over the started phases (no mutable
    /// accumulator). A re-observe of the stable ledger emits nothing further.
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
        assert_eq!(per_phase[0].fields.get("to_run").map(String::as_str), Some("2"));
        assert_eq!(per_phase[0].fields.get("skipped").map(String::as_str), Some("3"));

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
                .contains("2 to run, 3 skipped (already done)"),
            "overall reflects the single phase's partition: {:?}",
            overall[0].message
        );
        assert_eq!(overall[0].fields.get("to_run").map(String::as_str), Some("2"));
        assert_eq!(overall[0].fields.get("skipped").map(String::as_str), Some("3"));
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
                .contains("2 to run, 0 skipped (already done)"),
            "overall mirrors the all-unmarked phase: {:?}",
            overall[0].message
        );
    }

    /// A gated phase emits NO task-spawning milestone until its upstream
    /// fully terminates and it becomes dispatchable — pinning the milestone
    /// to the dispatchable edge, not mere task presence.
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
            // build dispatchable, compile gated.
            narrator.observe(&state);
            complete(&mut state, "tc");
            // compile now dispatchable.
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
            "task-spawning fires per phase only once it is dispatchable: {events:?}"
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
        assert_eq!(lost[0].fields.get("secondary").map(String::as_str), Some("n2"));
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
        assert_eq!(changed[0].fields.get("epoch").map(String::as_str), Some("2"));
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
}
