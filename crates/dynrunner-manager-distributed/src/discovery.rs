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
pub type SetupDiscoveryFn<I> = Box<
    dyn FnMut() -> Pin<Box<dyn Future<Output = Result<Vec<(TaskInfo<I>, bool)>, String>> + Send>>
        + Send,
>;

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
