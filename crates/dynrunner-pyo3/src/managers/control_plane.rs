//! Rust-side bundle of the command-channel + reinject-cap state shared
//! between any in-process primary-hosting manager and the
//! `PyPrimaryHandle`s minted from it.
//!
//! Single concern: own the `(Sender, Option<Receiver>, ReinjectCapCell)`
//! triple and the init / handle-factory / take-for-run sequence that
//! every primary-hosting PyO3 manager (`PyPrimaryCoordinator`,
//! `PyDistributedManager`, and any future symmetric variant) must run
//! through.
//!
//! Why a separate type instead of three fields on each manager:
//!   * The three pieces are always built together at `__init__`, always
//!     cloned together into `to_handle()`, and always consumed together
//!     at `run()` entry. Replicating the sequence on each manager kept
//!     them in step by luck; a single owning type makes the contract
//!     enforced rather than convention.
//!   * Adding a fourth manager (e.g. a slimmer test-harness variant) is
//!     a one-line field declaration — the helper carries the entire
//!     wiring sequence with it.
//!
//! Module boundary:
//!   * Owns: the channel pair, the cap cell, and the receiver-Option
//!     lifecycle (single-shot `run()`).
//!   * Does NOT own: any `PrimaryCoordinator`-touching call.
//!     `replace_command_channel` + `set_unfulfillable_reinject_max_per_task`
//!     run at the caller site where the inner coordinator is already in
//!     scope; the helper hands back the three values to thread through
//!     and stops there. This keeps the helper provider-agnostic.
//!
//! The Option<Receiver> + take-for-run contract: `run()` on every
//! manager is single-shot. The receiver is consumed by
//! `take_for_run()`; a second call returns the "run() already entered"
//! `PyRuntimeError` that the consumer sites used to phrase inline.

use pyo3::PyResult;
use tokio::sync::mpsc as tokio_mpsc;

use dynrunner_manager_distributed::primary::{PrimaryCommand, COMMAND_CHANNEL_CAPACITY};

use crate::identifier::RunnerIdentifier;
use crate::managers::primary_handle::{PyPrimaryHandle, ReinjectCapCell};

/// Three values the manager's `run()` needs at the moment it starts
/// driving the inner coordinator. Produced by
/// [`PrimaryControlPlane::take_for_run`] — the caller threads each
/// piece into the coordinator at the correct step (the sender + rx via
/// `replace_command_channel`, the snapshot via
/// `set_unfulfillable_reinject_max_per_task` or the `PrimaryConfig`
/// field).
pub(crate) struct RunWiring {
    /// Sender clone for `PrimaryCoordinator::replace_command_channel`.
    /// The sender that stays on the helper keeps backing future
    /// `to_handle()` calls — even after `run()` enters — so a Python
    /// caller that fetches a handle post-run still talks to the same
    /// receiver.
    pub(crate) command_tx: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,

    /// Receiver moved out of the helper for
    /// `PrimaryCoordinator::replace_command_channel`. Single-shot:
    /// after `take_for_run` consumes it, a second call returns the
    /// "run() already entered" error.
    pub(crate) command_rx: tokio_mpsc::Receiver<PrimaryCommand<RunnerIdentifier>>,

    /// Snapshot of the per-task reinject cap read at the moment
    /// `run_started` flips. The caller threads this into
    /// `PrimaryConfig.unfulfillable_reinject_max_per_task` (the
    /// in-process distributed manager) or via
    /// `PrimaryCoordinator::set_unfulfillable_reinject_max_per_task`
    /// (the network primary), depending on which is more convenient at
    /// the call site.
    pub(crate) cap_snapshot: Option<u32>,
}

/// Owns the command-channel + reinject-cap state for one primary-
/// hosting manager. Lifecycle:
///   1. `new(cap_kwarg)` at `__init__`: build the channel pair, seed
///      the cap cell from the constructor kwarg.
///   2. `to_handle()` from the PyO3 `handle()` method: mint a
///      `PyPrimaryHandle` that clones the sender + cell. Callable any
///      number of times.
///   3. `take_for_run()?` at `run()` entry: snapshot the cap, flip the
///      cell's `run_started` flag, hand back the rx + tx clone +
///      snapshot in a `RunWiring`. Single-shot — the second call
///      raises the documented "run() called twice" error.
pub(crate) struct PrimaryControlPlane {
    /// Sender side of the command channel. Cloned into each
    /// `PyPrimaryHandle` (see [`Self::to_handle`]) and one more clone
    /// is handed out via [`Self::take_for_run`] so the inner
    /// coordinator's `replace_command_channel` call receives the same
    /// channel the handles dispatch into.
    command_tx: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,

    /// Receiver side. `Some(_)` between `__init__` and `take_for_run`;
    /// `None` after the first `take_for_run` consumes it. The `Option`
    /// is the single-shot guard — a second `take_for_run` surfaces the
    /// "run() called twice" `PyRuntimeError`.
    command_rx: Option<tokio_mpsc::Receiver<PrimaryCommand<RunnerIdentifier>>>,

    /// Shared per-task reinject cap. Mutable via the handle setter
    /// until `take_for_run` snapshots the value and flips
    /// `run_started`; post-flip handle-side mutations raise.
    reinject_cap: ReinjectCapCell,
}

impl PrimaryControlPlane {
    /// Build a fresh control plane: allocate the command channel pair
    /// at the standard manager-distributed capacity and seed the cap
    /// cell from the manager's `unfulfillable_reinject_max_per_task`
    /// `__init__` kwarg. Called from each manager's `#[new]` body in
    /// place of the inline channel-build + cap-seed sequence.
    pub(crate) fn new(unfulfillable_reinject_max_per_task: Option<u32>) -> Self {
        let (command_tx, command_rx) = tokio_mpsc::channel(COMMAND_CHANNEL_CAPACITY);

        let reinject_cap = ReinjectCapCell::default();
        reinject_cap
            .inner
            .lock()
            .expect("ReinjectCapCell poisoned")
            .max_per_task = unfulfillable_reinject_max_per_task;

        Self {
            command_tx,
            command_rx: Some(command_rx),
            reinject_cap,
        }
    }

    /// Mint a fresh `PyPrimaryHandle` from a sender + cap-cell clone.
    /// Symmetric replacement for the body of each manager's PyO3
    /// `handle()` method. Returns the `PyResult` produced by
    /// `PyPrimaryHandle::from_sender` (the only failure today is the
    /// in-handle tokio runtime init).
    pub(crate) fn to_handle(&self) -> PyResult<PyPrimaryHandle> {
        PyPrimaryHandle::from_sender(self.command_tx.clone(), self.reinject_cap.clone())
    }

    /// Consume the receiver, snapshot the cap, flip `run_started`.
    /// Single-shot: returns the documented "run() called twice"
    /// `PyRuntimeError` if the receiver was already taken.
    ///
    /// The caller threads the returned values into the inner
    /// coordinator at the call site where it is already in scope —
    /// `replace_command_channel(wiring.command_tx, wiring.command_rx)`
    /// followed by either the `PrimaryConfig` field assignment or the
    /// `set_unfulfillable_reinject_max_per_task` setter.
    pub(crate) fn take_for_run(&mut self) -> PyResult<RunWiring> {
        let command_rx = self.command_rx.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "run() called twice; primary-hosting manager is single-shot",
            )
        })?;
        let cap_snapshot = self.reinject_cap.snapshot();
        self.reinject_cap.mark_run_started();
        Ok(RunWiring {
            command_tx: self.command_tx.clone(),
            command_rx,
            cap_snapshot,
        })
    }

    /// Snapshot the current cap value without consuming the receiver.
    /// Crate-internal accessor for tests that want to assert the
    /// `__init__` kwarg seeded the cell correctly without reaching
    /// into private fields. Production paths read through
    /// `take_for_run` instead.
    ///
    /// Gated on `test-with-python` because the only call sites live
    /// in the python-linked test module; a plain `#[cfg(test)]` would
    /// build the accessor even under default features (which exclude
    /// the test module) and trip `dead_code`.
    #[cfg(all(test, feature = "test-with-python"))]
    pub(crate) fn cap_snapshot(&self) -> Option<u32> {
        self.reinject_cap.snapshot()
    }

    /// Whether `other` shares this control plane's command channel.
    /// Crate-internal accessor for tests that assert two handles
    /// minted from one manager point at the same receiver — the
    /// minimal `Sender::same_channel` check the
    /// `handle_clones_share_same_command_channel` test phrases.
    /// Production paths never need this comparison.
    ///
    /// Same `test-with-python` gating rationale as `cap_snapshot`.
    #[cfg(all(test, feature = "test-with-python"))]
    pub(crate) fn same_command_channel(
        &self,
        other: &tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,
    ) -> bool {
        self.command_tx.same_channel(other)
    }
}
