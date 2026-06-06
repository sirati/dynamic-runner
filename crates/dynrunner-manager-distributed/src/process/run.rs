//! [`Node::run`] — compose + drive one peer's role lifecycle.
//!
//! # Concern
//!
//! ONE concern: SEQUENCE the OS-process's role lifecycle. The node owns the
//! composition (mesh + role entries + lifecycle channels); `run` turns that
//! static composition into a running peer by:
//!
//! 1. handing the mesh to the [`super::pump`] (the sole mesh owner) and
//!    keeping a [`super::pump::MeshControlHandle`] to register/retag roles,
//! 2. spawning each live coordinator's run loop on the `LocalSet`,
//! 3. building a snapshot-seeded primary on a [`super::PromotionSignal`]
//!    (SUPREME-LAW #3 & #7 — the secondary SIGNALS, the node BUILDS),
//! 4. swapping a relocated submitter-primary into a standalone observer
//!    (retag the slot in place — H5), or dropping the primary entry on a
//!    compute peer that keeps its secondary,
//! 5. collecting the final run outcome (counts + structured terminal).
//!
//! It is a THIN SEQUENCER (maint-M2): every mesh op goes through the pump's
//! control channel, every role's work is its own coordinator's, and the
//! BUG-6 teardown is the primary's own role-change hook signalling the
//! node's demote channel. The node names no transport and reaches into no
//! coordinator's internals.

use dynrunner_core::Identifier;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_protocol_primary_secondary::address::PeerId;
use dynrunner_protocol_primary_secondary::PeerTransport;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::{mpsc, oneshot};

use super::node::{Node, PromotionSignal};
use super::pump::{self, MeshControlHandle};
use super::role::LocalRole;
use super::run_inputs::{NodeRunInputs, PrimaryRunArgs};
use crate::observer::{ObserverCoordinator, ObserverHandoff, ObserverTerminal};
use crate::primary::{PrimaryCoordinator, PrimaryRunOutcome, RunError};
use crate::secondary::{SecondaryCoordinator, SecondaryTerminal};

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
///   `SetupDeadlineExpired` / `DuplicateTaskIdPrePhase`, or a generic run
///   failure). The boundary destructures the `RunError` for its
///   per-variant exit handling.
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
type ObserverRunResult = (Result<ObserverTerminal, RunError>, usize);

/// The observer arm's join handle. See [`ObserverRunResult`].
type ObserverJoinHandle = tokio::task::JoinHandle<ObserverRunResult>;

/// What a secondary's run task carries back: its role-agnostic
/// [`RunTerminal`] PLUS its converged `completed_count`, both read off the
/// coordinator at run end before the task drops it (and after the factory's
/// worker-teardown ladder ran). Concrete — usable for the secondary arm's
/// `JoinHandle` regardless of `I`.
type SecondaryRunResult = (RunTerminal, usize);

/// The secondary arm's join handle. See [`SecondaryRunResult`].
type SecondaryJoinHandle = tokio::task::JoinHandle<SecondaryRunResult>;

impl<I, Tr, Mgr, Sched, Est>
    Node<I, Tr, PrimaryCoordinator<Sched, Est, I>, SecondaryCoordinator<Mgr, Sched, Est, I>, ObserverCoordinator<I>>
where
    I: Identifier + 'static,
    Tr: PeerTransport<I> + 'static,
    Mgr: ManagerEndpoint + 'static,
    Sched: Scheduler<I> + Clone + 'static,
    Est: ResourceEstimator<I> + Clone + 'static,
{
    /// Compose + drive this node's roles to a single [`NodeRunOutcome`].
    ///
    /// `F` is the secondary's `WorkerFactory<Mgr>` (distinct from `Mgr`, the
    /// `ManagerEndpoint` the factory produces).
    pub async fn run<F>(self, mut inputs: NodeRunInputs<F, Sched, Est, I>) -> NodeRunOutcome
    where
        F: WorkerFactory<Mgr> + 'static,
    {
        let Node {
            mesh,
            primary,
            secondary,
            observer,
            mut promotion_rx,
            demote_rx: _node_demote_rx,
            ..
        } = self;

        // The peer-id this process runs on (every local slot shares it). Read
        // it off whichever role is live before we move the entries into tasks.
        let own_peer_id = first_live_peer_id(&primary, &secondary, &observer);

        // Hand the mesh to the pump (sole owner) + keep the control handle.
        let (control, control_rx) = pump::control_channel::<I>();
        let pump_task = tokio::task::spawn_local(pump::run_pump(mesh, control_rx));

        // ── Spawn the bootstrap roles ───────────────────────────────────

        // PRIMARY (the submitter): register its BUG-6 demote hook (on its own
        // cluster_state, feeding the demote_rx B-PRIMARY's constructor took),
        // then run it CONSUMING so a demote relocates it
        // (Relocated{handoff}). The outcome (and any handoff) rides back on
        // `primary_done`.
        let mut primary_done: Option<oneshot::Receiver<PrimaryRunOutcome<I>>> = None;
        if let Some(entry) = primary {
            let args = inputs.primary_run_args.take().unwrap_or_else(empty_primary_args);
            let mut coordinator = entry.coordinator;
            // BUG-6: the bootstrap primary demotes on any self→other flip
            // (apply OR restore/merge heal). The caller paired the demote_rx
            // (passed to `new`) with this tx.
            if let Some(demote_tx) = inputs.primary_demote_tx.take() {
                coordinator.register_demote_on_displaced(demote_tx);
            }
            let (tx, rx) = oneshot::channel();
            primary_done = Some(rx);
            // Hold the slot Arc for the primary's lifetime (teardown lever).
            let slot = entry.slot;
            tokio::task::spawn_local(async move {
                let _slot = slot;
                std::future::pending::<()>().await;
            });
            spawn_primary_with(coordinator, args, &control, tx);
        }

        // SECONDARY: run it with the supplied factory. Its `run` drains its
        // own inbox; the promotion signal it fires arrives on `promotion_rx`.
        // The task carries the secondary's role-agnostic terminal + converged
        // completion count back out (read off the coordinator at run end,
        // before the task drops it) and runs the factory's worker-teardown
        // ladder — gated on `terminal != Panik`, because a panik already
        // killed every worker pgid inside the coordinator's own teardown.
        let mut secondary_done: Option<SecondaryJoinHandle> = None;
        if let (Some(entry), Some(factory)) = (secondary, inputs.secondary_factory.take()) {
            {
                // Hold the secondary's slot Arc for its run's lifetime so the
                // mesh `Weak` keeps upgrading and the pump can deliver inbound
                // frames to the secondary slot (dropping it would silently
                // sever the secondary's ingress — it would never receive a
                // task assignment).
                let slot = entry.slot;
                secondary_done = Some(tokio::task::spawn_local(async move {
                    let _slot = slot;
                    let mut coordinator = entry.coordinator;
                    let mut factory = factory;
                    let run_result = coordinator.run(&mut factory).await;
                    let completed = coordinator.completed_count();
                    let terminal = secondary_terminal(run_result, coordinator.terminal());
                    // Worker teardown (SIGTERM→grace→SIGKILL) — the factory's
                    // HOW; the node decides WHEN. Skip on panik: the
                    // coordinator already killed every worker pgid, and the
                    // `exit(137)` decision must fire promptly without a second
                    // grace ladder.
                    if !matches!(terminal, RunTerminal::Panik { .. }) {
                        factory.cleanup().await;
                    }
                    (terminal, completed)
                }));
            }
        }

        // OBSERVER (cold-join): run it standalone, holding its slot Arc for
        // the run's lifetime (same ingress-liveness reason as the secondary).
        // The task carries the observer's run disposition (`ObserverTerminal`
        // + a strand-backstop `Err`) AND its converged `completed_count` back
        // out — both are read off the coordinator at run end, before the task
        // drops it, so the node outcome can surface the three distinct
        // observer exit codes + the count instead of flattening them.
        let mut observer_done: Option<ObserverJoinHandle> = None;
        if let Some(entry) = observer {
            observer_done = Some(spawn_observer(entry.coordinator, Some(entry.slot)));
        }

        // ── The lifecycle orchestration loop ────────────────────────────
        //
        // We resolve to a single outcome. The primary outcome is the
        // headline (Local | Relocated→observer); a pure secondary/observer
        // node resolves off its own completion. Promotion builds a primary
        // mid-run and folds its outcome into `primary_done`.

        let mut outcome = NodeRunOutcome {
            terminal: RunTerminal::Done,
            completed: 0,
            failed: 0,
            stranded: 0,
        };

        loop {
            tokio::select! {
                // A self-named promotion: build + seed + spawn the primary.
                Some(signal) = recv_opt(&mut promotion_rx) => {
                    // One primary per node: a promotion while a primary
                    // already runs here is a no-op (the duplicate-build guard).
                    if primary_done.is_none()
                        && let Some(rx) = self_build_promoted_primary(
                            signal,
                            &mut inputs.promote,
                            &control,
                            &own_peer_id,
                        ).await
                    {
                        primary_done = Some(rx);
                    }
                }

                // The primary finished or relocated.
                Some(po) = recv_primary(&mut primary_done) => {
                    match po {
                        PrimaryRunOutcome::Local { result, completed, failed, stranded } => {
                            // A primary's structured exit maps onto the unified
                            // terminal: `Ok` ⇒ Done, any `Err` ⇒ Failed (the
                            // boundary destructures the RunError for its
                            // per-variant exit code — ClusterCollapsed /
                            // SetupDeadlineExpired / DuplicateTaskIdPrePhase /
                            // generic).
                            outcome = NodeRunOutcome {
                                terminal: match result {
                                    Ok(()) => RunTerminal::Done,
                                    Err(error) => RunTerminal::Failed { error },
                                },
                                completed,
                                failed,
                                stranded,
                            };
                            break;
                        }
                        PrimaryRunOutcome::Relocated { handoff } => {
                            // Submitter→observer swap: retag the slot in place
                            // (H5) and run the observer. The SAME Arc<RoleSlot>
                            // / inbound channel survives the retag, so a
                            // primary-facing frame in flight at the retag is
                            // drained by the observer's inbox and applied
                            // idempotently (BUG-8/D3 — never an error).
                            observer_done = Some(swap_primary_to_observer(&control, handoff));
                            primary_done = None;
                        }
                    }
                }

                // A pure-secondary node finished (no primary).
                Some(sr) = join_secondary(&mut secondary_done) => {
                    if primary_done.is_none() && observer_done.is_none() {
                        let (terminal, completed) = sr;
                        outcome = NodeRunOutcome {
                            terminal,
                            completed,
                            failed: 0,
                            stranded: 0,
                        };
                        break;
                    }
                }

                // An observer (cold-join or post-swap) finished.
                Some(or) = join_opt_run(&mut observer_done) => {
                    outcome = finalize_observer(or);
                    break;
                }

                else => break,
            }
        }

        // ── Wind-down ───────────────────────────────────────────────────
        // The headline role has resolved into `outcome`. Drop the control
        // handle so the pump's control arm closes, then ABORT the pump rather
        // than awaiting it: the pump's ingress arm parks on the transport
        // inbound, which stays open as long as a PEER is still connected (the
        // wire does not close just because THIS node's headline role finished)
        // — so awaiting the pump would hang. The pump's egress is already
        // drained for every send issued before this point (the headline role's
        // last frames left the queue before its run future resolved), so
        // aborting loses nothing in flight.
        drop(control);
        pump_task.abort();
        let _ = pump_task.await;
        // A still-running sibling role (e.g. a pure-secondary node whose own
        // run is the headline) is aborted on its own arm; the bootstrap
        // primary's wind-down has no sibling to join here.
        if let Some(h) = secondary_done {
            h.abort();
            let _ = h.await;
        }

        outcome
    }

}

/// Submitter-primary→observer swap (H5): retag the slot in place through the
/// pump (primary→observer, preserving the stable channel), build the observer
/// from the handoff, and spawn it. A free fn (not a method) because the node
/// `self` was destructured into the run loop's locals.
fn swap_primary_to_observer<I>(
    control: &MeshControlHandle<I>,
    handoff: Box<ObserverHandoff<I>>,
) -> ObserverJoinHandle
where
    I: Identifier + 'static,
{
    control.retag(LocalRole::Primary, LocalRole::Observer);
    let observer = ObserverCoordinator::from_handoff(*handoff);
    // The retagged slot (same Arc the primary held) stays alive via the former
    // primary's parked slot-holder task, so no slot is passed here.
    spawn_observer(observer, None)
}

/// Build, seed, register, and spawn a promoted primary on a promotion
/// signal (SUPREME-LAW #3 & #7 — the secondary SIGNALLED; the NODE builds).
///
/// The node MINTS the Primary trio (register through the pump, so the slot +
/// the primary's `secondary_keepalives` seeding land BEFORE its first
/// heartbeat tick — BUG-4 — because the build + spawn run synchronously here
/// before the primary's run loop awaits), wires a FRESH demote channel
/// (BUG-6: the promoted primary demotes itself on any later self→other flip),
/// calls the caller's builder (which snapshot-seeds the primary itself), and
/// spawns it. Returns the outcome receiver + the slot `Arc` (the node holds
/// the latter as the teardown lever). `None` if the promotion cannot proceed
/// (no builder, or the pump is gone).
async fn self_build_promoted_primary<I, Sched, Est>(
    signal: PromotionSignal<I>,
    promote: &mut Option<super::run_inputs::PromotedPrimaryBuilder<Sched, Est, I>>,
    control: &MeshControlHandle<I>,
    own_peer_id: &PeerId,
) -> Option<oneshot::Receiver<PrimaryRunOutcome<I>>>
where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + 'static,
    Est: ResourceEstimator<I> + Clone + 'static,
{
    let builder = promote.as_mut()?;
    // Register the Primary slot + mint its trio through the pump.
    let (slot, client, inbox) = control.register(LocalRole::Primary, own_peer_id.clone()).await?;

    // BUG-6 demote channel: the node owns `demote_tx` (fed by the role-change
    // hook), the promoted primary owns `demote_rx` (its `run_consuming`
    // relocates on it). Minted here so the hook and the receiver pair.
    let (demote_tx, demote_rx) = mpsc::unbounded_channel();

    // The caller's recipe builds + snapshot-seeds the primary from the
    // converged `cluster_state` the secondary captured ON the signal at the
    // promotion-fire instant. The node only threads the snapshot through —
    // the builder owns `seed_from_promotion_snapshot` + coordinator
    // construction (scheduler/estimator are the caller's concern).
    let mut built = builder(client, inbox, demote_rx, signal.snapshot);
    built.coordinator.register_demote_on_displaced(demote_tx);

    let (tx, rx) = oneshot::channel();
    spawn_primary_with(built.coordinator, built.run_args, control, tx);
    // Hold the slot `Arc` for the primary's lifetime — dropping it is the
    // role-teardown lever (the mesh `Weak` then stops upgrading). Park it in a
    // detached task so it lives as long as the run.
    tokio::task::spawn_local(async move {
        let _slot = slot;
        std::future::pending::<()>().await;
    });
    Some(rx)
}

/// Spawn a primary's `run_consuming`, sending the outcome back. The BUG-6
/// demote hook is registered by the caller BEFORE this (bootstrap path) or
/// inside the promotion build, so the consuming run can already race its
/// demote receiver.
fn spawn_primary_with<I, Sched, Est>(
    coordinator: PrimaryCoordinator<Sched, Est, I>,
    args: PrimaryRunArgs<I>,
    control: &MeshControlHandle<I>,
    done: oneshot::Sender<PrimaryRunOutcome<I>>,
) where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + 'static,
    Est: ResourceEstimator<I> + Clone + 'static,
{
    let _ = control;
    tokio::task::spawn_local(async move {
        let PrimaryRunArgs {
            binaries,
            phase_deps,
            on_phase_start,
            on_phase_end,
        } = args;
        match coordinator
            .run_consuming(binaries, phase_deps, on_phase_start, on_phase_end)
            .await
        {
            Ok(outcome) => {
                let _ = done.send(outcome);
            }
            Err(e) => {
                let _ = done.send(PrimaryRunOutcome::Local {
                    result: Err(e),
                    completed: 0,
                    failed: 0,
                    stranded: 0,
                });
            }
        }
    });
}

/// Spawn an observer's standalone `run`, optionally holding a slot `Arc` for
/// the run's lifetime so the mesh `Weak` keeps upgrading (ingress liveness).
/// The cold-join path passes its freshly-registered slot; the relocate-swap
/// path passes `None` (the retagged slot is already held by the former
/// primary's parked slot-holder task).
fn spawn_observer<I>(
    mut observer: ObserverCoordinator<I>,
    slot: Option<std::sync::Arc<crate::process::RoleSlot<I>>>,
) -> ObserverJoinHandle
where
    I: Identifier + 'static,
{
    tokio::task::spawn_local(async move {
        let _slot = slot;
        // Carry BOTH the run disposition AND the converged completion count
        // out of the task. The count is read off the coordinator AFTER `run`
        // returns (the converged ledger) but BEFORE the task drops the
        // coordinator — once the task ends the coordinator is gone, so the
        // count must travel back here, not be re-sourced by the caller.
        let run_result = observer.run().await;
        let completed = observer.completed_count();
        (run_result, completed)
    })
}

/// Empty primary args (no binaries / deps / narration) — the fallback when a
/// primary entry is live but no run args were supplied (a node that composed
/// a primary but drives no pipeline, e.g. a unit fixture).
fn empty_primary_args<I: Identifier>() -> PrimaryRunArgs<I> {
    PrimaryRunArgs {
        binaries: Vec::new(),
        phase_deps: std::collections::HashMap::new(),
        on_phase_start: Box::new(|_| {}),
        on_phase_end: Box::new(|_, _, _| {}),
    }
}

/// The first live role's host peer-id (every local slot shares it).
fn first_live_peer_id<I, P, S, O>(
    primary: &Option<super::node::RoleEntry<P, I>>,
    secondary: &Option<super::node::RoleEntry<S, I>>,
    observer: &Option<super::node::RoleEntry<O, I>>,
) -> PeerId
where
    I: Identifier,
{
    if let Some(e) = primary {
        return e.slot.peer_id().clone();
    }
    if let Some(e) = secondary {
        return e.slot.peer_id().clone();
    }
    if let Some(e) = observer {
        return e.slot.peer_id().clone();
    }
    PeerId::from("")
}

/// Fold an observer's task result into the node outcome.
///
/// The observer task carried back BOTH its run disposition (the three-way
/// [`ObserverTerminal`] or a strand-backstop `Err`) AND its converged
/// completion count. This maps them onto the role-agnostic
/// [`NodeRunOutcome::terminal`] (Done/Aborted/Panik/Failed) + `completed` so
/// the PyO3 boundary maps the terminal uniformly with the primary/secondary.
/// Used by BOTH observer-ending paths: the cold-join late-joiner and the
/// submitter that relocated into the observer tail.
fn finalize_observer(joined: Result<ObserverRunResult, tokio::task::JoinError>) -> NodeRunOutcome {
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
/// The observer's clean terminals map 1:1; a strand-backstop / fatal-exit
/// `Err` becomes `Failed`.
fn observer_terminal(run_result: Result<ObserverTerminal, RunError>) -> RunTerminal {
    match run_result {
        Ok(ObserverTerminal::Done) => RunTerminal::Done,
        Ok(ObserverTerminal::Aborted { reason }) => RunTerminal::Aborted { reason },
        Ok(ObserverTerminal::Panik { matched_path }) => RunTerminal::Panik { matched_path },
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
fn secondary_terminal(
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

// ── small select! helpers (keep the loop arms readable) ────────────────────

/// `recv` on an `Option<Receiver>`, parking forever when `None` so the arm
/// is inert rather than resolving on a missing channel.
async fn recv_opt<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> Option<T> {
    rx.recv().await
}

async fn recv_primary<I: Identifier>(
    rx: &mut Option<oneshot::Receiver<PrimaryRunOutcome<I>>>,
) -> Option<PrimaryRunOutcome<I>> {
    match rx.as_mut() {
        Some(r) => match r.await {
            Ok(v) => {
                *rx = None;
                Some(v)
            }
            Err(_) => {
                *rx = None;
                None
            }
        },
        None => std::future::pending().await,
    }
}

async fn join_secondary(
    h: &mut Option<SecondaryJoinHandle>,
) -> Option<SecondaryRunResult> {
    match h.as_mut() {
        Some(handle) => {
            let r = handle.await;
            *h = None;
            Some(r.unwrap_or_else(|e| {
                (
                    RunTerminal::Failed {
                        // A panicked/aborted task is an UNEXPECTED non-clean
                        // exit, not the stay-local-primary swallow case — type
                        // it structured so the boundary raises.
                        error: RunError::FatalPolicyExit {
                            reason: format!("secondary task panicked/aborted: {e}"),
                        },
                    },
                    0,
                )
            }))
        }
        None => std::future::pending().await,
    }
}

async fn join_opt_run(
    h: &mut Option<ObserverJoinHandle>,
) -> Option<Result<ObserverRunResult, tokio::task::JoinError>> {
    match h.as_mut() {
        Some(handle) => {
            let r = handle.await;
            *h = None;
            Some(r)
        }
        None => std::future::pending().await,
    }
}
