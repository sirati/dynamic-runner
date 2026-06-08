//! Run terminal + outcome accounting for [`super::Node::run`].
//!
//! # Concern
//!
//! ONE concern: the role-agnostic terminal disposition + the post-run
//! accounting a [`super::super::Node::run`] resolves to, plus the pure
//! mappings from each role's own run result onto that uniform vocabulary.
//! A primary, a secondary, and an observer all end in the same
//! [`RunTerminal`] four-way, so the PyO3 boundary maps ONE terminal to the
//! process exit code regardless of which role drove the run.

use crate::observer::ObserverTerminal;
use crate::primary::RunError;
use crate::secondary::SecondaryTerminal;

/// The role-agnostic terminal disposition of one `Node::run`.
///
/// EVERY role's run resolves to one of these four — a primary, a secondary,
/// and an observer all end in the same vocabulary, so the PyO3 boundary maps
/// ONE terminal to the process exit code regardless of which role drove the
/// run (no per-role `Option` fields). The mapping is uniform:
///
/// - [`Self::Done`] ⇒ exit 0 (a clean `RunComplete`).
/// - [`Self::Aborted`] ⇒ exit 1 (the cluster broadcast `RunAborted` — a #3a
///   pre-phase duplicate-task-id).
/// - [`Self::Panik`] ⇒ exit 137 (operator emergency stop; the worker pgids
///   were already killed by the role's own teardown).
/// - [`Self::Failed`] ⇒ a non-zero exit the boundary derives from the
///   carried [`RunError`] (a strand backstop — fleet-dead / primary-silence
///   — a structured primary terminal like `ClusterCollapsed` /
///   `DuplicateTaskIdPrePhase`, or a generic run failure). The boundary
///   destructures the `RunError` for its per-variant exit handling.
#[derive(Debug)]
pub enum RunTerminal {
    /// Clean completion — exit 0.
    Done,
    /// Cluster-wide `RunAborted` (#3a pre-phase duplicate) — exit 1.
    Aborted { reason: String },
    /// Operator panik — exit 137 (pgids already killed by the role teardown).
    Panik { matched_path: std::path::PathBuf },
    /// A strand backstop / structured-error / generic run failure — the
    /// boundary maps the carried error to a non-zero exit.
    Failed { error: RunError },
}

/// The single post-`run` accounting the PyO3 boundary reads.
///
/// `Node::run` produces ONE outcome regardless of how the lifecycle
/// resolved (local primary, promoted primary, relocated→observer, cold-join
/// observer, or a pure secondary). [`Self::terminal`] is the role-agnostic
/// exit disposition (every role ends in the same four-way vocabulary), and
/// the counts come from whichever role held the converged ledger at the end.
#[derive(Debug)]
pub struct NodeRunOutcome {
    /// The role-agnostic terminal — the boundary maps it to the process exit
    /// code uniformly for a primary, secondary, or observer. See
    /// [`RunTerminal`].
    pub terminal: RunTerminal,
    /// Cluster-wide completed terminals (the converged ledger count from
    /// whichever role drove the run).
    pub completed: usize,
    /// Cluster-wide failed-residual terminals.
    pub failed: usize,
    /// Stranded (never-terminal) tasks at shutdown.
    pub stranded: usize,
}

/// What an observer's run task carries back: its run disposition (the
/// three-way [`ObserverTerminal`] or a strand-backstop `Err`) PLUS its
/// converged `completed_count`, both read off the coordinator at run end
/// before the task drops it. Concrete (not generic over `I`) — every member
/// is concrete — so it is a plain type alias usable for the observer arm's
/// `JoinHandle` regardless of the node's identifier type.
pub(super) type ObserverRunResult = (Result<ObserverTerminal, RunError>, usize);

/// The observer arm's join handle. See [`ObserverRunResult`].
pub(super) type ObserverJoinHandle = tokio::task::JoinHandle<ObserverRunResult>;

/// What a secondary's run task carries back: its role-agnostic
/// [`RunTerminal`] PLUS its converged `completed_count`, both read off the
/// coordinator at run end before the task drops it (and after the factory's
/// worker-teardown ladder ran). Concrete — usable for the secondary arm's
/// `JoinHandle` regardless of `I`.
pub(super) type SecondaryRunResult = (RunTerminal, usize);

/// The secondary arm's join handle. See [`SecondaryRunResult`].
pub(super) type SecondaryJoinHandle = tokio::task::JoinHandle<SecondaryRunResult>;

/// Fold an observer's task result into the node outcome.
///
/// The observer task carried back BOTH its run disposition (the three-way
/// [`ObserverTerminal`] or a strand-backstop `Err`) AND its converged
/// completion count. This maps them onto the role-agnostic
/// [`NodeRunOutcome::terminal`] (Done/Aborted/Panik/Failed) + `completed` so
/// the PyO3 boundary maps the terminal uniformly with the primary/secondary.
/// Used by BOTH observer-ending paths: the cold-join late-joiner and the
/// submitter that relocated into the observer tail.
pub(super) fn finalize_observer(
    joined: Result<ObserverRunResult, tokio::task::JoinError>,
) -> NodeRunOutcome {
    let (terminal, completed) = match joined {
        Ok((run_result, completed)) => (observer_terminal(run_result), completed),
        // A panicked/aborted observer task has no terminal and no count; map
        // the join error to a STRUCTURED `Failed` (an unexpected non-clean
        // exit, not the stay-local-primary swallow case) so the boundary
        // raises — the observation never reached a clean terminal.
        Err(join) => (
            RunTerminal::Failed {
                error: RunError::FatalPolicyExit {
                    reason: format!("observer task panicked/aborted: {join}"),
                },
            },
            0,
        ),
    };
    NodeRunOutcome {
        terminal,
        completed,
        failed: 0,
        stranded: 0,
    }
}

/// Map an observer's run disposition onto the role-agnostic [`RunTerminal`].
///
/// # The verdict-authority sever (BUG-B — there are never two authorities)
///
/// The observer carries ZERO authority over the run. Its terminals are
/// either the PRIMARY's verdict it OBSERVED (`Done` from `RunComplete`,
/// `Aborted` from the broadcast `RunAborted`) or a LOCAL operator/policy
/// terminal on its own host (`Panik`, or a `FatalPolicyExit` `Err` from its
/// invalid-task monitor). The observer's loss of its OWN transport
/// visibility — zero peers, a silent named primary, the by-design `-R`
/// setup-tunnel drop after relocation — is NEVER a terminal at all (the
/// observer reports-and-retries instead of exiting; see
/// [`crate::observer::lost_visibility`]), so it can never reach this mapping.
///
/// LOAD-BEARING INVARIANT: a [`RunError::ClusterCollapsed`] must NEVER be
/// the run's verdict via the observer. The compute primary is the SOLE
/// authority that can declare the cluster collapsed; the observer's own
/// view collapsing is not the cluster collapsing. The observer never
/// CONSTRUCTS a `ClusterCollapsed` (the strand backstops were removed), so
/// this arm is unreachable in practice — but the boundary RE-TYPES any
/// stray `ClusterCollapsed` into a non-cluster `FatalPolicyExit` rather
/// than letting an observer reap the run as collapsed, keeping the sever
/// total even against a future regression.
fn observer_terminal(run_result: Result<ObserverTerminal, RunError>) -> RunTerminal {
    match run_result {
        Ok(ObserverTerminal::Done) => RunTerminal::Done,
        Ok(ObserverTerminal::Aborted { reason }) => RunTerminal::Aborted { reason },
        Ok(ObserverTerminal::Panik { matched_path }) => RunTerminal::Panik { matched_path },
        // A genuine LOCAL policy abort (the invalid-task monitor) surfaces
        // non-zero as itself.
        Err(error @ RunError::FatalPolicyExit { .. }) => RunTerminal::Failed { error },
        // The verdict-authority sever: an observer must NEVER produce the
        // run's `ClusterCollapsed` verdict (it has no authority to declare
        // the cluster dead). The source never builds one; if a future change
        // reintroduces it, re-type it so the observer cannot reap the run as
        // collapsed.
        Err(RunError::ClusterCollapsed { stranded, .. }) => RunTerminal::Failed {
            error: RunError::FatalPolicyExit {
                reason: format!(
                    "observer surfaced a ClusterCollapsed ({stranded} stranded) — re-typed: \
                     an observer has zero authority to declare the cluster collapsed; the run \
                     verdict belongs to the primary"
                ),
            },
        },
        Err(error) => RunTerminal::Failed { error },
    }
}

/// Map a secondary's `run` outcome onto the role-agnostic [`RunTerminal`].
///
/// The secondary's `run` returns `Ok(())` on a clean terminal and `Err` only
/// on a `Failed` (a fatal-exit) — so the per-secondary [`SecondaryTerminal`]
/// is the single source of truth for WHICH clean terminal (Done/Aborted/
/// Panik) it reached, read back via `coordinator.terminal()`. An `Err` (or a
/// `Failed`/absent terminal) becomes `Failed`.
pub(super) fn secondary_terminal(
    run_result: Result<(), String>,
    terminal: Option<SecondaryTerminal>,
) -> RunTerminal {
    match run_result {
        Ok(()) => match terminal {
            Some(SecondaryTerminal::Done) | None => RunTerminal::Done,
            Some(SecondaryTerminal::Aborted { reason }) => RunTerminal::Aborted { reason },
            Some(SecondaryTerminal::Panik { matched_path, .. }) => {
                RunTerminal::Panik { matched_path }
            }
            // A `Failed` lifecycle is a deliberate fatal-exit (the secondary's
            // `fatal_exit` latch). The OLD pyo3 secondary RAISED on it (it had
            // no swallow path), so type it STRUCTURED (`FatalPolicyExit`) — the
            // boundary raises, never swallows.
            Some(SecondaryTerminal::Failed { reason }) => RunTerminal::Failed {
                error: RunError::FatalPolicyExit { reason },
            },
        },
        // The secondary's `run` returns `Err` on a fatal-exit (the `fatal_exit`
        // latch propagated). Same disposition as a `Failed` lifecycle: the OLD
        // wrapper RAISED, so structure it so the boundary raises.
        Err(reason) => RunTerminal::Failed {
            error: RunError::FatalPolicyExit { reason },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_state::OutcomeSummary;

    /// BUG-B verdict-authority sever: an observer's run result must NEVER
    /// map to a run-failing `ClusterCollapsed` verdict. The observer never
    /// constructs one (its strand backstops were removed — see
    /// `crate::observer::lost_visibility`), but the boundary itself must be
    /// the second line of defence: even if a `ClusterCollapsed` somehow
    /// reaches the observer mapping, it is RE-TYPED so the observer cannot
    /// reap the run as collapsed. This pins the severed edge at the type
    /// boundary the PyO3 verdict reads.
    #[test]
    fn observer_cluster_collapsed_is_never_the_run_verdict() {
        let terminal = observer_terminal(Err(RunError::ClusterCollapsed {
            stranded: 7,
            outcome: OutcomeSummary::default(),
        }));
        // It MUST NOT carry a `ClusterCollapsed` into the run verdict.
        match terminal {
            RunTerminal::Failed {
                error: RunError::ClusterCollapsed { .. },
            } => panic!(
                "an observer's ClusterCollapsed must NEVER be the run verdict — the \
                 verdict-authority edge is severed (BUG-B)"
            ),
            // Re-typed to a non-cluster policy exit: the run still surfaces
            // non-zero (not swallowed), but NOT as a cluster collapse the
            // observer has no authority to declare.
            RunTerminal::Failed {
                error: RunError::FatalPolicyExit { .. },
            } => {}
            other => panic!("unexpected re-typed terminal: {other:?}"),
        }
    }

    /// A genuine LOCAL policy abort (the invalid-task monitor) still
    /// surfaces as itself — only the cluster-collapse verdict is severed.
    #[test]
    fn observer_fatal_policy_exit_surfaces_as_itself() {
        let terminal = observer_terminal(Err(RunError::FatalPolicyExit {
            reason: "invalid_task monitor".into(),
        }));
        assert!(matches!(
            terminal,
            RunTerminal::Failed {
                error: RunError::FatalPolicyExit { .. }
            }
        ));
    }

    /// The observer's OBSERVED terminals (the primary's verdict it relayed)
    /// map 1:1: RunComplete→Done, RunAborted→Aborted.
    #[test]
    fn observed_primary_verdicts_map_one_to_one() {
        assert!(matches!(
            observer_terminal(Ok(ObserverTerminal::Done)),
            RunTerminal::Done
        ));
        assert!(matches!(
            observer_terminal(Ok(ObserverTerminal::Aborted {
                reason: "dup".into()
            })),
            RunTerminal::Aborted { .. }
        ));
    }
}
