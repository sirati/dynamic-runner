//! Process-independent operator run-narration over the replicated CRDT.
//!
//! # Concern
//!
//! Single concern: turn the evolving [`ClusterState`] into the operator's
//! "important" (LLM-wake-worthy) run narrative — phase-started,
//! phase-complete, and the one-shot run-complete / run-aborted summary —
//! by DIFFING the replicated ledger, not by hooking any one process's
//! authority. Every line is emitted at the [`dynrunner_core::IMPORTANT_TARGET`]
//! tracing target, exactly the marker the primary's
//! [`crate::primary::important_events`] siblings use.
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
            }
            if rollup.has_any && !rollup.has_live && self.done_phases.insert(phase.clone()) {
                tracing::info!(
                    target: IMPORTANT_TARGET,
                    phase = %phase,
                    "phase complete",
                );
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
}
