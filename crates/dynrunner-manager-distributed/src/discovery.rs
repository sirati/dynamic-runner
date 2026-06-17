//! The consumer's setup-discovery policy types — the boundary-crossing
//! shapes the relocated/local primary's `discover_on_promotion` driver
//! consumes.
//!
//! Single concern: define the discovery POLICY closure shape
//! ([`SetupDiscoveryFn`]) and the policy + phase-graph bundle
//! ([`SetupDiscovery`]) the primary takes once on its single discovery
//! fire. Discovery is now a PRIMARY concern (the relocated mode-2 primary,
//! and the in-process `--source-already-staged` local primary, both run
//! `discover_items` themselves), so these types live at the crate root
//! rather than under `secondary/` — the secondary no longer owns discovery.

use std::future::Future;
use std::pin::Pin;

use dynrunner_core::{Identifier, TaskInfo};

/// The consumer's setup-discovery policy — the relocated/local primary's
/// mirror of the `register_phase_lifecycle_callbacks` hook family.
///
/// Invoked to produce the discovered task batch in pre-staged
/// (`--source-already-staged`) / relocated mode, where the submitter has
/// no local view of the corpus and discovery runs on the corpus-mounting
/// node. The discovery itself is a consumer POLICY (the pyo3 wrapper
/// supplies a closure that runs Python's `task.discover_items`); the drive
/// is a framework concern (features-in-Rust).
///
/// # Why it returns a FUTURE (the non-blocking contract — correctness)
///
/// The discovery future is awaited inside the primary coordinator's
/// operational loop. If discovery blocked that thread (a slow
/// `--source-already-staged` scan, or a GIL-held Python excursion), the loop
/// would stall — and a stalled loop cannot consume the mesh-delivered
/// keepalives, so a peer declares the node dead and STRANDS the run (the
/// §14/§15 fleet-collapse the one-mesh work fixed). So the closure returns a
/// future the driver `.await`s, yielding the thread; the consumer is
/// responsible for making that future non-thread-blocking (e.g. the pyo3
/// wrapper runs the GIL excursion on a `spawn_blocking` thread and awaits its
/// `Send` handle). `Err` aborts the run.
///
/// # Why `Send` (the thread-move contract)
///
/// The primary coordinator's operational loop runs on its OWN dedicated thread
/// (`process::run::coordinator_host` — isolating a primary CPU burst from a
/// co-located secondary). The coordinator OWNS its `SetupDiscovery`, so it is
/// MOVED onto that thread with the coordinator. The closure and its returned
/// future are therefore `Send`. This is satisfied for free by the real builder:
/// the future captures only `Send` data (the consumer's `Py<PyAny>` handles are
/// `Send + Sync`; the GIL excursion is `spawn_blocking`-ed and its `JoinHandle`
/// is `Send`), so the only Python re-entry happens on a blocking thread under a
/// fresh `Python::attach`, never by holding a GIL token across the boundary.
///
/// `FnMut` because the driver takes it on the one fire.
///
/// Each discovered task is PAIRED with its discovery-time
/// `skipped_already_done` marker (`true` ⇒ the producer found the item's
/// outputs already exist; the driver materialises it terminal
/// `SkippedAlreadyDone` rather than dispatching it). The marker rides the
/// discovery boundary, NOT `TaskInfo<I>` — `discover_on_promotion`
/// partitions on it via the shared `skip_transitions` helper.
/// The discovery future the policy returns and the driver `.await`s: the full
/// discovered batch (each task paired with its `skipped_already_done` marker)
/// or an `Err` that aborts the run. `Send` (the thread-move contract). Named so
/// both [`SetupDiscoveryFn`]'s return and [`InFlightDiscovery::future`] share
/// ONE shape — they are the same future, before vs. after it is started.
pub type DiscoveryFuture<I> =
    Pin<Box<dyn Future<Output = Result<Vec<(TaskInfo<I>, bool)>, String>> + Send>>;

pub type SetupDiscoveryFn<I> = Box<dyn FnMut() -> DiscoveryFuture<I> + Send>;

/// The consumer's setup-discovery policy plus the phase-dependency graph
/// fed alongside the discovered binaries when seeding the replicated
/// ledger. Carried together because both are needed for one seed and
/// neither is meaningful without the other.
///
/// Registered on the [`crate::primary::PrimaryCoordinator`] via
/// `register_setup_discovery` BEFORE `run`; the `discover_on_promotion`
/// driver takes it on its single fire (gated on the replicated
/// `DiscoveryDebt == Owed` marker — inert on every non-relocated primary,
/// whose CRDT is `Undeclared`).
pub struct SetupDiscovery<I: Identifier> {
    /// The discovery policy — produces the task batch. See
    /// [`SetupDiscoveryFn`].
    pub discover: SetupDiscoveryFn<I>,
    /// The phase-dependency graph broadcast with the discovered tasks. The
    /// consumer resolves this from its `TaskDefinition.get_phases()` once at
    /// construction; discovery only resolves the per-task list.
    pub phase_deps:
        std::collections::HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>,
}

/// An IN-FLIGHT mode-2 discovery: the started discovery future plus the
/// phase-dependency graph that seeds the ledger ALONGSIDE its result.
///
/// # Why this exists (the concurrent-arm contract — correctness)
///
/// `discover_items` is collect-all (it returns the FULL `Vec<(TaskInfo, bool)>`
/// in one await — ~6 min for a 46k corpus). Awaiting it SEQUENTIALLY as a
/// pre-loop step parks the primary's whole operational `select!` control flow
/// for that window: the keepalive arm never fires (peers see app-silence) and
/// the setup-servicing arms never run (secondaries sit at the setup deadline →
/// failover). Holding the started future HERE — on a coordinator field — lets
/// the operational loop poll it as ONE concurrent `select!` arm: while it is
/// pending, every sibling arm (inbox, command, heartbeat→keepalive,
/// worker-mgmt→setup-servicing) runs normally, so the primary stays app-alive
/// AND services secondary setup concurrently with discovery.
///
/// `phase_deps` rides alongside the future because the post-resolve seed needs
/// BOTH the discovered batch and the dep graph in ONE atomic ledger mutation
/// (`PhaseDepsSet + TaskAdded* + DiscoverySettled`) — the future yields only
/// the batch, so the graph is carried here from the registered
/// [`SetupDiscovery`] when the future is started.
///
/// `Send` (via the inner `SetupDiscoveryFn` contract) so it polls cleanly as a
/// `select!` arm on the coordinator's own thread. `FnMut`-derived: started by
/// ONE `(discover)()` call when the CRDT declares `DiscoveryDebt::Owed`.
pub struct InFlightDiscovery<I: Identifier> {
    /// The started discovery future — `(SetupDiscovery::discover)()`, polled
    /// by the operational loop's discovery `select!` arm. Resolves to the full
    /// discovered batch (each task paired with its `skipped_already_done`
    /// marker) or an `Err` that aborts the run.
    pub future: DiscoveryFuture<I>,
    /// The phase-dependency graph seeded alongside the discovered batch — the
    /// `SetupDiscovery::phase_deps` carried across the await so the post-resolve
    /// seed (`PhaseDepsSet`) has it without re-consulting the (now-taken) policy.
    pub phase_deps:
        std::collections::HashMap<dynrunner_core::PhaseId, Vec<dynrunner_core::PhaseId>>,
}
