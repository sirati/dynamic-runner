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
/// The run loop shares ONE single-threaded runtime with the `Node`'s
/// mesh-pump. If discovery blocked that thread (a slow
/// `--source-already-staged` scan, or a GIL-held Python excursion), the
/// pump would stall: keepalives stop flowing AND the node stops receiving
/// its peers', so a peer declares it dead and STRANDS the run — the exact
/// §14/§15 fleet-collapse the one-mesh work fixed. So the closure returns a
/// future the driver `.await`s, yielding the thread to the pump; the
/// consumer is responsible for making that future non-thread-blocking (e.g.
/// the pyo3 wrapper runs the GIL excursion on a `spawn_blocking` thread and
/// awaits its handle). `Err` aborts the run.
///
/// `FnMut` because the driver takes it on the one fire; the boxed future
/// need not be `Send` — it is awaited on the node's own `!Send` task.
pub type SetupDiscoveryFn<I> =
    Box<dyn FnMut() -> Pin<Box<dyn Future<Output = Result<Vec<TaskInfo<I>>, String>>>>>;

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
