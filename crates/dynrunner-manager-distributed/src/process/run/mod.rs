//! [`Node::run`] — compose + drive one peer's role lifecycle.
//!
//! # Concern
//!
//! ONE concern: SEQUENCE the OS-process's role lifecycle. The node owns the
//! composition (the hosted mesh + role entries + lifecycle channels); `run`
//! turns that static composition into a running peer by:
//!
//! 1. driving the already-running pump (hosted by the composition site's
//!    [`super::MeshHost`] — possibly on the dedicated mesh runtime thread)
//!    through its [`super::pump::MeshControlHandle`] to register/retag roles,
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

mod compose;
mod outcome;
mod promotion;
mod select;
mod swap;

use dynrunner_core::Identifier;
use dynrunner_manager_local::WorkerFactory;
use dynrunner_protocol_manager_worker::ManagerEndpoint;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::oneshot;

use super::node::Node;
use super::run_inputs::NodeRunInputs;
use crate::observer::ObserverCoordinator;
use crate::primary::{PrimaryCoordinator, PrimaryRunOutcome};
use crate::secondary::SecondaryCoordinator;

use compose::{empty_primary_args, first_live_peer_id};
use outcome::{ObserverJoinHandle, SecondaryJoinHandle, finalize_observer, secondary_terminal};
use promotion::{self_build_promoted_primary, spawn_primary_with};
use select::{join_opt_run, join_secondary, recv_opt, recv_primary};
use swap::{spawn_observer, swap_primary_to_observer};

pub use outcome::{NodeRunOutcome, RunTerminal};

impl<I, Mgr, Sched, Est>
    Node<
        I,
        PrimaryCoordinator<Sched, Est, I>,
        SecondaryCoordinator<Mgr, Sched, Est, I>,
        ObserverCoordinator<I>,
    >
where
    I: Identifier + 'static,
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
            host,
            primary,
            secondary,
            observer,
            mut promotion_rx,
            ..
        } = self;

        // The peer-id this process runs on (every local slot shares it). Read
        // it off whichever role is live before we move the entries into tasks.
        let own_peer_id = first_live_peer_id(&primary, &secondary, &observer);

        // The pump is ALREADY RUNNING inside the composition site's
        // `MeshHost` (spawned before any role trio was minted, so its entry
        // `publish_membership()` preceded every coordinator's first egress —
        // the R5 invariant, enforced by the host's mint ordering: a
        // `Register` reply only ships after the pump republishes). The node
        // drives it solely through the control handle.
        let control = host.control().clone();

        // ── Spawn the bootstrap roles ───────────────────────────────────

        // PRIMARY (the submitter): register its BUG-6 demote hook (on its own
        // cluster_state, feeding the demote_rx B-PRIMARY's constructor took),
        // then run it CONSUMING so a demote relocates it
        // (Relocated{handoff}). The outcome (and any handoff) rides back on
        // `primary_done`.
        let mut primary_done: Option<oneshot::Receiver<PrimaryRunOutcome<I>>> = None;
        if let Some(entry) = primary {
            let args = inputs
                .primary_run_args
                .take()
                .unwrap_or_else(empty_primary_args);
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
                            // DuplicateTaskIdPrePhase / generic).
                            outcome = NodeRunOutcome {
                                terminal: match result {
                                    Ok(()) => RunTerminal::Done,
                                    // The graceful-abort verdict is its own
                                    // terminal (distinct from success AND
                                    // from a failure) — the boundary reports
                                    // it loudly and exits clean.
                                    Err(error @ crate::primary::RunError::GracefulAbort {
                                        ..
                                    }) => RunTerminal::GracefulAbort {
                                        reason: error.to_string(),
                                    },
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
        // The headline role has resolved into `outcome`. `MeshHost::stop`
        // first DEFENSIVELY drains any final egress (NEW-C): a frame queued
        // in the same sync step as the headline run-future resolving (e.g. a
        // last keepalive / completion broadcast) has not yet been pulled by
        // the pump, and a bare stop would discard it — `wind_down` has the
        // pump apply every currently-queued egress item through the mesh and
        // ack first (bounded: it drains what is queued NOW, never awaiting a
        // fresh item). Then the host stops its executor: the local flavor
        // ABORTS the pump task (its ingress arm parks on the transport
        // inbound, which stays open as long as a PEER is still connected, so
        // awaiting it would hang); the dedicated-thread flavor signals the
        // mesh runtime and joins it with a bounded grace.
        drop(control);
        host.stop().await;
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
