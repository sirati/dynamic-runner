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
//! line, and the two retry-pass-start milestones off upward steps of the
//! replicated grow-only [`ClusterState::retry_passes_used`] map (a pass that
//! opened on a remote promoted primary is surfaced here purely via
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

use std::collections::{HashMap, HashSet};

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
    /// Last-emitted retry-pass USED count per `(phase, bucket)`, diffed
    /// against the replicated grow-only-MAX [`ClusterState::retry_passes_used`].
    /// The retry-pass twin of the `started`/`done` edge-sets: instead of a
    /// presence set it holds a per-key count, and a retry-pass-start
    /// milestone is emitted on each UPWARD step (a key absent here is at the
    /// implicit 0), since the retry-bucket bumps the count by one per pass
    /// that actually opened. Re-observing an unchanged (or already-emitted)
    /// count is silent, so a freshly-promoted/observing node fed a CRDT
    /// already at N seeds its baseline to N on first sight and emits nothing
    /// for the already-converged passes.
    retry_passes_emitted: HashMap<(PhaseId, BucketKind), u32>,
    /// Whether the one-shot run-complete / run-aborted summary has fired.
    /// The two are mutually exclusive and share this single latch so at
    /// most one terminal line is ever emitted.
    completion_emitted: bool,
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
            retry_passes_emitted: HashMap::new(),
            completion_emitted: false,
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
        // `retry_passes_used` map. The retry-bucket bumps a `(phase, bucket)`
        // count by one per pass that actually opened (the SAME moment the
        // removed projection originated its retry-pass milestone — the
        // origination sat in the same `if !reinjected.is_empty()` block as
        // the `record_retry_pass_used(used + 1)` bump), so an UPWARD step of
        // the replicated count is the faithful edge for a pass start. Mirrors
        // the started/done edge-sets, keyed by count rather than presence:
        // emit once whenever the observed count exceeds the last-emitted one
        // and store the new count, so a re-observe of an unchanged count is
        // silent and a cold-join/promoted node fed a count already at N
        // emits once for the 0→N step (it has no per-step history to
        // replay). `BucketKind` selects the operator wording.
        for (key, used) in state.retry_passes_used() {
            let last = self.retry_passes_emitted.get(key).copied().unwrap_or(0);
            if used > last {
                self.retry_passes_emitted.insert(key.clone(), used);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primary::retry_bucket::BucketKind;
    use crate::test_capture::{ImportantCapture, important_only};
    use dynrunner_core::{ErrorType, PhaseId, RunnerIdentifier, TaskDep, TaskInfo, TypeId};
    use dynrunner_protocol_primary_secondary::cluster_mutation::ClusterMutation;
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

    /// An upward step of the replicated `retry_passes_used` count emits the
    /// retry-pass-start milestone whose wording matches the bucket:
    /// Recoverable → error-retry, Oom → OOM-retry. Once per increment; a
    /// re-observe of the unchanged count is silent.
    #[test]
    fn retry_pass_milestone_per_bucket_on_increment() {
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

    /// Each successive retry pass (the count stepping 1 → 2 → 3) emits its
    /// own milestone — one line per increment, since each opened pass bumps
    /// the replicated count by exactly one.
    #[test]
    fn retry_pass_milestone_once_per_increment() {
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
            3,
            "one error-retry-pass line per opened pass (1→2→3): {events:?}"
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
}
