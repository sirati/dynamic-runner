//! [`spawn_primary_on_thread`] — WHERE a primary coordinator's run loop
//! executes, isolated from its co-located siblings.
//!
//! # Concern
//!
//! ONE concern: own the EXECUTOR of a primary coordinator's operational loop.
//! A promoted primary runs IN-PROCESS, co-resident with the secondary that
//! hosts its node (the SLURM relocate target is a compute secondary that won
//! promotion; the in-process `--multi-computer local` node co-locates every
//! role on one runtime). When both coordinators' loops share ONE
//! `current_thread` runtime + `LocalSet`, a primary CPU burst (the range-digest
//! fold over a 66k ledger, a large snapshot stream) starves the secondary's
//! loop on the same thread — its task dispatch, worker-message handling, and
//! keepalive consumption all stall until the burst yields. (Transport keepalive
//! EGRESS already survives via the dedicated mesh runtime thread — see
//! [`super::super::MeshHost::on_dedicated_thread`] — but the secondary
//! COORDINATOR's progress does not.)
//!
//! This module makes "where the primary loop runs" an explicit decision: the
//! primary's `run_consuming` future is moved onto its OWN dedicated
//! `std::thread` running its OWN `current_thread` tokio runtime + `LocalSet`.
//! A primary CPU burst then pegs only ITS core; CFS schedules the secondary's
//! thread independently, so the secondary keeps dispatching, consuming
//! keepalives, and answering the wire.
//!
//! It is the COORDINATOR analogue of [`super::super::MeshHost`] (which owns
//! WHERE the mesh executes), and follows the SAME two-flavor split: the
//! dedicated-thread executor is used iff the mesh itself runs on a dedicated
//! thread ([`super::super::MeshHost::runs_on_dedicated_thread`] — a real-network
//! node). The in-process `--multi-computer local` node (pure-`mpsc` mesh on the
//! shared `LocalSet`) keeps its primary on that shared `LocalSet`: there is no
//! co-resident wire QoS to protect, and the single-runtime model is load-bearing
//! for the in-process harness (its paused-clock tests need ONE virtual clock —
//! a second runtime thread would carry its own unsynchronised clock). The same
//! channel-shaped boundary + bounded teardown apply to the thread flavor.
//!
//! # Why the move is sound WITHOUT a `Send` refactor of coordinator state
//!
//! A one-time MOVE of the coordinator onto another thread needs `Send`, NOT
//! `Sync`. [`crate::primary::PrimaryCoordinator`]'s `cluster_state` uses
//! `Cell<…>` interior-mutation memos (the digest cache / fold counters), which
//! are `Send` (only `!Sync`): the coordinator is touched ONLY from its own
//! single thread before AND after the move, so the single-thread interior
//! mutability stays valid. `run_consuming` consumes `self` by value and takes
//! `Send` boxed `on_phase_*` closures + a `Send` [`super::super::SeedSource`]
//! (built `Send` by design — see the `PromotedPrimaryBuilder` contract), so the
//! returned future is `Send` for the one-time spawn-move. The coordinator's
//! INTERNAL `spawn_local` tasks (respawn watchers, listener dispatchers) re-home
//! onto THIS thread's `LocalSet` when the future is polled here — they never
//! cross a thread boundary.
//!
//! # The boundary stays channel-shaped (nothing on the node side changes shape)
//!
//! The primary reaches the mesh ONLY through its [`super::super::MeshClient`]
//! (queued egress, an `mpsc::UnboundedSender`) and [`super::super::RoleInbox`]
//! (channel ingress, an `mpsc::UnboundedReceiver`) — tokio channels wake across
//! runtimes natively, so the pump (wherever it is hosted) keeps delivering. The
//! run outcome flows back to [`super::Node::run`]'s `select!` loop over the
//! SAME `oneshot::Sender<PrimaryRunOutcome>` the on-`LocalSet` spawn used; a
//! `oneshot` resolves across runtimes, so `recv_primary` is unchanged. The node
//! holds the returned [`CoordinatorThread`] as the teardown lever.
//!
//! # Isolation invariant
//!
//! Nothing on this crate's coordinator thread touches Python directly (this
//! crate has no `pyo3` dependency); the Python-facing `on_phase_*` closures
//! re-acquire the GIL only briefly via `Python::attach` from the pyo3 bridge,
//! exactly as they did on the shared `LocalSet`. The thread move changes the OS
//! thread the brief GIL excursions run on, not their shape — the GIL is still
//! released (`py.detach`) around the whole run and re-acquired per hook, so no
//! GIL-deadlock surface is introduced (no shared-state mutex, hence no
//! GIL↔lock ordering cycle).

use dynrunner_core::Identifier;
use dynrunner_scheduler_api::{ResourceEstimator, Scheduler};
use tokio::sync::oneshot;

use super::super::run_inputs::PrimaryRunArgs;
use crate::primary::{PrimaryCoordinator, PrimaryRunOutcome};

/// Bounded grace for joining the coordinator thread after its run future
/// resolves. The thread's exit path is non-blocking once `run_consuming`
/// returns (the `LocalSet` + runtime drop synchronously), so the normal join
/// completes in milliseconds; the grace only bounds a pathological wedge (a
/// coordinator-internal `spawn_local` task that refuses to drop), which is
/// reported loudly and then detached rather than wedging process teardown.
///
/// Matched to [`super::super::MeshHost`]'s mesh-thread join grace — the same
/// dedicated-thread teardown discipline.
const COORD_THREAD_JOIN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// The primary coordinator's executor teardown lever, held by
/// [`super::Node::run`] for the primary's lifetime.
///
/// Two flavors, mirroring [`super::super::MeshHost`]'s own split:
///
/// - [`Self::Thread`] — the primary loop runs on its OWN dedicated runtime
///   thread (real-network nodes, where a promoted primary co-resides with a
///   live secondary). The node joins the thread at wind-down (after the run
///   outcome has arrived over the `oneshot`), bounded by
///   [`COORD_THREAD_JOIN_GRACE`].
/// - [`Self::LocalSet`] — the primary loop runs as a `spawn_local` task on the
///   node's shared `LocalSet` (the in-process `--multi-computer local` node,
///   whose pure-`mpsc` mesh + co-located roles deliberately share one runtime).
///   The `JoinHandle` is aborted at wind-down, exactly as the pre-split shape
///   did. No clock-spanning, so the in-process paused-clock harness's
///   single-runtime virtual-time model is unaffected.
///
/// There is NO separate shutdown signal for the thread flavor: the primary's
/// lifecycle ENDS when its `run_consuming` returns (the outcome travels back
/// over the `oneshot`), at which point the thread body has already fallen out
/// of `block_on` and is exiting. The node joins it after observing the outcome;
/// the bounded wait is purely a wedge backstop.
///
/// `teardown` is idempotent and also runs from `Drop` so no path (early error,
/// graceful-abort, panik unwind) leaks the executor.
pub(super) enum CoordinatorThread {
    /// The primary loop on its own dedicated runtime thread.
    Thread {
        /// The thread-exit notification: the body sends `()` after `block_on`
        /// returns and the runtime/`LocalSet` have dropped. A bounded
        /// `recv_timeout` on this (rather than a bare `join`) is what makes the
        /// join BOUNDED — a wedged thread is reported and detached, never hangs
        /// teardown.
        done: std::sync::mpsc::Receiver<()>,
        thread: Option<std::thread::JoinHandle<()>>,
    },
    /// The primary loop as a `spawn_local` task on the node's `LocalSet`.
    LocalSet(Option<tokio::task::JoinHandle<()>>),
}

impl CoordinatorThread {
    /// Tear the executor down. Idempotent.
    ///
    /// Called by the node at wind-down once the run outcome has arrived. The
    /// thread flavor joins within [`COORD_THREAD_JOIN_GRACE`] (a wedged thread
    /// is reported loudly and detached rather than wedging the process-teardown
    /// sequence); the `LocalSet` flavor aborts the task (the outcome already
    /// arrived, so the task is either finished or being abandoned at wind-down,
    /// exactly as the pre-split `spawn_local` shape was).
    pub(super) fn teardown(&mut self) {
        match self {
            CoordinatorThread::Thread { done, thread } => {
                let Some(handle) = thread.take() else {
                    return;
                };
                match done.recv_timeout(COORD_THREAD_JOIN_GRACE) {
                    // `Disconnected` means the body exited without sending (a
                    // panic unwound past the send) — joinable either way.
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        if handle.join().is_err() {
                            tracing::error!(
                                "coordinator runtime: thread panicked; primary loop torn down by \
                                 unwind"
                            );
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        tracing::error!(
                            grace_s = COORD_THREAD_JOIN_GRACE.as_secs(),
                            "coordinator runtime: thread did not exit within the join grace after \
                             the primary run resolved; detaching it so process teardown is not \
                             wedged (the thread leaks until process exit)"
                        );
                    }
                }
            }
            CoordinatorThread::LocalSet(handle) => {
                if let Some(h) = handle.take() {
                    h.abort();
                }
            }
        }
    }
}

impl Drop for CoordinatorThread {
    fn drop(&mut self) {
        self.teardown();
    }
}

/// Drive a primary's `run_consuming` to completion, routing the outcome onto
/// `done_tx`. The SINGLE run body shared by both executor flavors so neither
/// restates the seed-destructure + outcome-routing (an `Err` becomes a
/// structured `Local { Err(.) }`, mirroring the pre-split spawn).
async fn drive_primary<I, Sched, Est>(
    coordinator: PrimaryCoordinator<Sched, Est, I>,
    args: PrimaryRunArgs<I>,
    done_tx: oneshot::Sender<PrimaryRunOutcome<I>>,
) where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + 'static,
    Est: ResourceEstimator<I> + Clone + 'static,
{
    let PrimaryRunArgs {
        seed,
        on_phase_start,
        on_phase_end,
    } = args;
    match coordinator
        .run_consuming(seed, on_phase_start, on_phase_end)
        .await
    {
        Ok(outcome) => {
            let _ = done_tx.send(outcome);
        }
        Err(e) => {
            let _ = done_tx.send(PrimaryRunOutcome::Local {
                result: Err(e),
                completed: 0,
                failed: 0,
                stranded: 0,
            });
        }
    }
}

/// Spawn a primary's `run_consuming`, sending the outcome back over `done_tx`,
/// choosing the executor by `dedicated`:
///
/// - `dedicated == true` → the COORDINATOR analogue of
///   [`super::super::MeshHost::on_dedicated_thread`]: the coordinator (and its
///   `cluster_state`) is MOVED onto its OWN `current_thread` runtime + thread (a
///   one-time `Send` move; see the module doc for why no `Sync` is needed), then
///   driven on a `LocalSet` so its internal `spawn_local` tasks re-home onto
///   that thread. A primary CPU burst then pegs only its core, not the
///   co-located secondary's loop. The caller passes this for real-network nodes
///   (the mesh on its own thread — [`super::super::MeshHost::runs_on_dedicated_thread`]).
/// - `dedicated == false` → the pre-split shape: a `spawn_local` task on the
///   CALLER's `LocalSet` (the in-process `--multi-computer local` node, whose
///   pure-`mpsc` mesh + roles share one runtime — and whose paused-clock test
///   harness needs one virtual clock). Must be called from within a `LocalSet`.
///
/// The boundary is identical for both: the outcome travels back over `done_tx`
/// (a `oneshot`, which resolves across runtimes), so [`super::Node::run`]'s
/// loop is unchanged. The returned [`CoordinatorThread`] is the node's teardown
/// lever. The BUG-6 demote hook is registered by the caller BEFORE this, so the
/// consuming run can already race its demote receiver.
pub(super) fn spawn_primary<I, Sched, Est>(
    coordinator: PrimaryCoordinator<Sched, Est, I>,
    args: PrimaryRunArgs<I>,
    done_tx: oneshot::Sender<PrimaryRunOutcome<I>>,
    dedicated: bool,
) -> CoordinatorThread
where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + Send + 'static,
    Est: ResourceEstimator<I> + Clone + Send + 'static,
{
    if dedicated {
        spawn_primary_on_thread(coordinator, args, done_tx)
    } else {
        // In-process / paused-clock node: keep the primary on the caller's
        // `LocalSet` so the one (possibly virtual-time) runtime governs every
        // co-located role. Identical to the pre-split `spawn_local`.
        let handle = tokio::task::spawn_local(drive_primary(coordinator, args, done_tx));
        CoordinatorThread::LocalSet(Some(handle))
    }
}

/// The dedicated-thread executor (`dedicated == true` in [`spawn_primary`]):
/// move the coordinator onto its own `current_thread` runtime + `LocalSet`
/// thread and drive it there.
///
/// Falls back to sending a structured `Local { Err(.) }` outcome if the
/// runtime cannot be built on the new thread (so the failure surfaces as a run
/// failure, not a silent dropped `oneshot`); a thread-spawn refusal leaves
/// `done_tx` dropped, which the node's `recv_primary` already reaps as "primary
/// gone".
fn spawn_primary_on_thread<I, Sched, Est>(
    coordinator: PrimaryCoordinator<Sched, Est, I>,
    args: PrimaryRunArgs<I>,
    done_tx: oneshot::Sender<PrimaryRunOutcome<I>>,
) -> CoordinatorThread
where
    I: Identifier + 'static,
    Sched: Scheduler<I> + Clone + Send + 'static,
    Est: ResourceEstimator<I> + Clone + Send + 'static,
{
    let (done_notify_tx, done_notify_rx) = std::sync::mpsc::channel::<()>();

    let spawn_result = std::thread::Builder::new()
        .name("dynrunner-primary".to_string())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    // The runtime could not be built ON this thread: surface a
                    // structured run failure (not a dropped oneshot) so the
                    // node's loop reaps it as a failed primary, then notify the
                    // joiner and exit.
                    let _ = done_tx.send(PrimaryRunOutcome::Local {
                        result: Err(crate::primary::RunError::FatalPolicyExit {
                            reason: format!(
                                "coordinator runtime: failed to build tokio runtime on the \
                                 dedicated primary thread: {e}"
                            ),
                        }),
                        completed: 0,
                        failed: 0,
                        stranded: 0,
                    });
                    let _ = done_notify_tx.send(());
                    return;
                }
            };
            let local = tokio::task::LocalSet::new();
            rt.block_on(local.run_until(drive_primary(coordinator, args, done_tx)));
            // `local` (every coordinator-internal spawn_local task) and `rt`
            // drop here, synchronously; then notify the joiner.
            let _ = done_notify_tx.send(());
        });

    match spawn_result {
        Ok(thread) => CoordinatorThread::Thread {
            done: done_notify_rx,
            thread: Some(thread),
        },
        Err(e) => {
            // The OS refused the thread: `done_tx` was moved into the closure
            // that never ran, so it is already dropped here — we cannot send
            // through it. Instead the dropped `done_tx` makes the node's
            // `recv_primary` observe a closed `oneshot` (→ `None`), which its
            // loop already handles as "primary gone". Log the cause loudly.
            tracing::error!(
                error = %e,
                "coordinator runtime: failed to spawn the dedicated primary thread; the primary \
                 will surface as gone (closed outcome channel)"
            );
            // `done_notify_rx` resolves immediately (its sender was dropped with
            // the un-run closure), so teardown is a no-op.
            CoordinatorThread::Thread {
                done: done_notify_rx,
                thread: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The thread-isolation contract this module exists to provide: a
    //! CPU-bound `block_on` on the dedicated coordinator thread MUST NOT starve
    //! the spawning runtime (the node's loop + its co-located secondary). These
    //! tests exercise the thread+runtime+`block_on` mechanism
    //! `spawn_primary_on_thread` uses, decoupled from the heavy
    //! `PrimaryCoordinator` (whose functional correctness on this loop is
    //! covered by the `oploop_arm_hunt` / `phase_ordering` e2e suites). Real
    //! wall-clock (NOT `start_paused`): the whole point is cross-thread
    //! progress under a genuine CPU burst, which a virtual clock cannot model.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    /// Build the dedicated-thread executor's exact scaffolding (own
    /// `current_thread` runtime + `LocalSet` + `block_on`, with the
    /// done-notify channel + `CoordinatorThread::Thread` teardown) around an
    /// arbitrary run body, so the test drives the SAME isolation mechanism
    /// `spawn_primary_on_thread` does without constructing a coordinator.
    fn spawn_thread_running<F>(body: F) -> super::CoordinatorThread
    where
        F: FnOnce() + Send + 'static,
    {
        let (done_notify_tx, done_notify_rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("dynrunner-primary-test".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test coordinator runtime");
                let local = tokio::task::LocalSet::new();
                rt.block_on(local.run_until(async move {
                    body();
                }));
                let _ = done_notify_tx.send(());
            })
            .expect("spawn test coordinator thread");
        super::CoordinatorThread::Thread {
            done: done_notify_rx,
            thread: Some(thread),
        }
    }

    /// THE isolation property: a multi-100ms synchronous CPU burst inside the
    /// dedicated thread's `block_on` does NOT stall the spawning tokio runtime.
    /// A concurrent timer task on the spawning runtime keeps ticking THROUGHOUT
    /// the burst — which is exactly the co-located secondary's loop staying
    /// alive while the primary pegs its own core. Before the thread split (both
    /// loops on one `current_thread` runtime) the burst would block the shared
    /// thread and the ticker would not advance until it finished.
    #[tokio::test(flavor = "current_thread")]
    async fn dedicated_thread_cpu_burst_does_not_starve_the_spawning_runtime() {
        let burst_done = Arc::new(AtomicBool::new(false));
        let burst_done_body = Arc::clone(&burst_done);

        // The "primary CPU burst": a tight, non-yielding spin for ~300ms on the
        // dedicated thread (the range-digest-fold shape — synchronous Rust work
        // that never awaits), then flip the flag.
        let coord = spawn_thread_running(move || {
            let spin_until = Instant::now() + Duration::from_millis(300);
            // Busy-spin WITHOUT yielding to any runtime — the pathological
            // shape #504 hit. `std::hint::black_box` keeps the loop from being
            // optimised away.
            while Instant::now() < spin_until {
                std::hint::black_box(0u64.wrapping_add(1));
            }
            burst_done_body.store(true, Ordering::SeqCst);
        });

        // The "co-located secondary's loop": a concurrent task on the SPAWNING
        // runtime that increments a counter every 10ms. If the burst starved
        // this runtime, the counter would not advance until the burst ended.
        let ticks = Arc::new(AtomicU64::new(0));
        let ticks_task = Arc::clone(&ticks);
        let ticker = tokio::spawn(async move {
            // Run for ~400ms (longer than the 300ms burst) so we observe ticks
            // DURING the burst window, then exit.
            let deadline = Instant::now() + Duration::from_millis(400);
            while Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
                ticks_task.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Sample the tick count at ~150ms — squarely INSIDE the 300ms burst.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let ticks_mid_burst = ticks.load(Ordering::SeqCst);
        assert!(
            !burst_done.load(Ordering::SeqCst),
            "sanity: the 300ms burst must still be running at the 150ms sample"
        );
        // With the burst on its OWN thread, the 10ms ticker has fired many
        // times by 150ms (>= 5 even under generous CI jitter). A starved
        // runtime would show 0 ticks until the burst ended.
        assert!(
            ticks_mid_burst >= 5,
            "the spawning runtime was starved by the dedicated-thread CPU burst: only \
             {ticks_mid_burst} ticks fired in the first 150ms of a 300ms burst (expected the \
             10ms ticker to keep advancing — that is the co-located secondary staying alive)"
        );

        ticker.await.expect("ticker task");
        assert!(
            burst_done.load(Ordering::SeqCst),
            "the burst body must have completed"
        );

        // Teardown joins the now-exited thread promptly (bounded).
        let mut coord = coord;
        let join_start = Instant::now();
        coord.teardown();
        assert!(
            join_start.elapsed() < Duration::from_secs(1),
            "an already-exited coordinator thread must join promptly, not wait out the grace"
        );
    }

    /// `CoordinatorThread::teardown` is idempotent and `Drop` also tears down —
    /// no path leaks the thread. A second `teardown` after the first is a
    /// no-op, and dropping the handle does not panic.
    #[tokio::test(flavor = "current_thread")]
    async fn teardown_is_idempotent_and_drop_safe() {
        let coord = spawn_thread_running(|| {
            // Trivial body: exit immediately.
        });
        let mut coord = coord;
        coord.teardown();
        // Second teardown: no-op (the thread handle was already taken).
        coord.teardown();
        // Drop runs teardown again — must not panic / double-join.
        drop(coord);
    }

    /// The `LocalSet` flavor's teardown aborts its task (the in-process /
    /// paused-clock shape) without touching any OS thread.
    #[tokio::test(flavor = "current_thread")]
    async fn local_set_flavor_teardown_aborts_the_task() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let handle = tokio::task::spawn_local(async {
                    std::future::pending::<()>().await;
                });
                let mut coord = super::CoordinatorThread::LocalSet(Some(handle));
                coord.teardown();
                // Idempotent + drop-safe on the LocalSet flavor too.
                coord.teardown();
                drop(coord);
            })
            .await;
    }
}
