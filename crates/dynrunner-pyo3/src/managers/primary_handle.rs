//! Python-visible handle to a running `PrimaryCoordinator`'s
//! cross-thread command channel.
//!
//! Single concern: thin PyO3 adapter that wraps a
//! `tokio::sync::mpsc::Sender<PrimaryCommand>` so Python code (running
//! from an asyncio executor, a worker thread, or any other off-loop
//! caller) can mutate the live primary by sending typed commands and
//! awaiting their reply oneshots.
//!
//! Module boundary:
//!   * Owns: the PyO3 class + the `Sender<...>` clone.
//!   * Does NOT own: the command semantics — every method delegates
//!     to `PrimaryCommand::*` and the Rust-side handler. New mutation
//!     types land as new `PrimaryCommand` variants + a new method here;
//!     no in-Python logic.
//!
//! What callers see (Python):
//!   ```python
//!   coord = _native.RustPrimaryCoordinator(...)
//!   handle = coord.handle()
//!   # ... thread runs coord.run() ...
//!   handle.fail_permanent(hash, "non_recoverable", "operator decision")
//!   handle.reinject_task(hash)
//!   handle.update_preferred_secondaries(hash, ["sec-1", "sec-2"])
//!   ```
//! Each method blocks the calling Python thread until the Rust side
//! either applies the mutation or returns an error. Errors surface as
//! `PyRuntimeError` so the Python control plane can `try/except` them.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::oneshot;

use dynrunner_core::{ErrorType, TaskInfo};
use dynrunner_manager_distributed::primary::{PrimaryCommand, SpawnError};
use pyo3::types::{PyDict, PyList, PyTuple};
use tokio::sync::mpsc as tokio_mpsc;

use crate::identifier::RunnerIdentifier;

/// A `tokio::runtime::Runtime` whose `Drop` is non-blocking, so the
/// runtime can be dropped from *any* context — including from within
/// another tokio runtime's async context.
///
/// # Why this exists (the bug it fixes)
///
/// The default `Runtime::drop` performs a BLOCKING shutdown of the
/// runtime's blocking pool. Tokio forbids that block from inside an
/// asynchronous context and panics with *"Cannot drop a runtime in a
/// context where blocking is not allowed. This happens when a runtime
/// is dropped from within an asynchronous context."*
///
/// A [`PyPrimaryHandle`] owns such a runtime (for the off-loop
/// `block_on` callers — `on_run_start` firing before the coordinator's
/// runtime starts). On the SLURM secondary path the handle is captured
/// into the promoted-primary recipe and threaded into the secondary's
/// `node.run(...).await`, which runs INSIDE the secondary's own
/// `rt.block_on(local.run_until(...))`. A plain (never-promoted)
/// secondary never fires the recipe, so the recipe — and the
/// `PyPrimaryHandle` it holds — is dropped when `node.run` returns at
/// end-of-run, i.e. WITHIN that outer async context. With a default
/// `Runtime::drop` that drop panics; this wrapper makes it a
/// non-blocking `shutdown_background()` instead.
///
/// `shutdown_background()` does not wait for in-flight blocking tasks
/// (it is `shutdown_timeout(0)`). This handle's runtime only ever
/// drives `send().await` + `reply.await` (no `spawn_blocking`), so the
/// "may leak still-running blocking tasks" caveat is vacuous here.
///
/// # Boundary
///
/// Single concern: drop-safety of the handle's runtime. Callers keep
/// invoking `rt.block_on(...)` unchanged via the [`Deref`] to the inner
/// `Runtime`; no call site knows this wrapper exists beyond its
/// construction in [`PyPrimaryHandle::from_sender`].
///
/// [`Deref`]: std::ops::Deref
pub(crate) struct BackgroundDropRuntime {
    /// `Some` for the wrapper's whole life; taken in `Drop` so the
    /// owned `Runtime` can be moved into `shutdown_background(self)`.
    inner: Option<tokio::runtime::Runtime>,
}

impl BackgroundDropRuntime {
    fn new(rt: tokio::runtime::Runtime) -> Self {
        Self { inner: Some(rt) }
    }
}

impl std::ops::Deref for BackgroundDropRuntime {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        // Invariant: `inner` is `Some` for the wrapper's entire life;
        // it is only taken in `Drop`, after which no method runs.
        self.inner
            .as_ref()
            .expect("BackgroundDropRuntime used after drop")
    }
}

impl Drop for BackgroundDropRuntime {
    fn drop(&mut self) {
        // Non-blocking shutdown so this is safe to run inside another
        // runtime's async context (no blocking-pool join → no
        // "Cannot drop a runtime …" panic). `shutdown_background`
        // consumes the `Runtime` by value, hence the `Option::take`.
        if let Some(rt) = self.inner.take() {
            rt.shutdown_background();
        }
    }
}

/// Shared mutable cell carrying the per-task reinject cap. Held by
/// both `PyPrimaryCoordinator` (which threads the cap into
/// `PrimaryConfig` at `run()` start) and every `PyPrimaryHandle`
/// (which exposes the setter to Python). A second flag tracks
/// whether `run()` has been entered — flipped by
/// `PyPrimaryCoordinator::run()` at the moment it captures the
/// initial value, and read by the handle's setter to refuse late
/// mutations with a typed Python error.
#[derive(Default, Clone)]
pub(crate) struct ReinjectCapCell {
    pub(crate) inner: Arc<Mutex<ReinjectCapInner>>,
}

#[derive(Default)]
pub(crate) struct ReinjectCapInner {
    pub(crate) max_per_task: Option<u32>,
    pub(crate) run_started: bool,
}

impl ReinjectCapCell {
    /// Read the current cap. Called from `PyPrimaryCoordinator::run`
    /// once, at the moment it constructs the inner `PrimaryConfig`.
    pub(crate) fn snapshot(&self) -> Option<u32> {
        self.inner
            .lock()
            .expect("ReinjectCapCell poisoned")
            .max_per_task
    }

    /// Mark `run()` as entered so the handle setter starts rejecting
    /// late mutations.
    pub(crate) fn mark_run_started(&self) {
        let mut g = self.inner.lock().expect("ReinjectCapCell poisoned");
        g.run_started = true;
    }
}

/// Python-visible handle to the primary's command channel. Each
/// public method packs a `PrimaryCommand` + a `oneshot::Sender`,
/// dispatches into the channel, and blocks the calling thread on the
/// reply. The runtime that backs `block_on` is a small in-handle
/// `tokio::runtime::Runtime` (multi-thread, current-thread) so the
/// call doesn't need a tokio context on the Python side.
#[pyclass(name = "PrimaryHandle", skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyPrimaryHandle {
    /// Cloned `PrimaryCommand` sender — the canonical ingress for
    /// every mutation method on this class. Stays alive as long as
    /// the underlying `PrimaryCoordinator` is alive; once the
    /// coordinator drops the receiver, every subsequent `send().await`
    /// returns SendError and the method raises `PyRuntimeError`.
    ///
    /// `pub(crate)` for crate-internal contract tests
    /// (`distributed::tests::handle_clones_share_same_command_channel`)
    /// that need `mpsc::Sender::same_channel` to prove two handles
    /// minted by `PyDistributedManager::handle` point at the same
    /// receiver. No Python-facing surface reads it.
    pub(crate) sender: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,

    /// In-handle runtime for `block_on(...)` calls from Python. Shared
    /// across clones of `PrimaryHandle` so multiple calls don't each
    /// pay the runtime-construction cost. `Arc` so `#[derive(Clone)]`
    /// keeps the runtime alive across handle clones.
    ///
    /// Wrapped in [`BackgroundDropRuntime`] so the runtime drops
    /// NON-BLOCKINGLY: this handle is captured into the secondary's
    /// promoted-primary recipe and can therefore be dropped from inside
    /// the secondary's `node.run(...).await` async context (a plain
    /// secondary drops the never-fired recipe at end-of-run). A default
    /// `Runtime::drop` there panics ("Cannot drop a runtime in a context
    /// where blocking is not allowed"); the wrapper's `shutdown_background`
    /// drop is context-safe. The `Deref` keeps `rt.block_on(...)` calls
    /// below unchanged.
    rt: Arc<BackgroundDropRuntime>,

    /// Shared cell for the per-task reinject cap. Lets the handle's
    /// `set_unfulfillable_reinject_max_per_task` setter mutate the
    /// value `PyPrimaryCoordinator::run()` reads when building its
    /// `PrimaryConfig`. The cell's `run_started` flag gates the
    /// setter so post-run() mutations raise a typed Python error.
    reinject_cap: ReinjectCapCell,
}

impl PyPrimaryHandle {
    /// Construct a new handle from the coordinator's command sender
    /// and shared reinject-cap cell. Called only from
    /// `PyPrimaryCoordinator::handle()` (the canonical PyO3 entry
    /// point); kept `pub(crate)` so the Rust-side glue can build it
    /// without going through Python.
    pub(crate) fn from_sender(
        sender: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,
        reinject_cap: ReinjectCapCell,
    ) -> PyResult<Self> {
        // current_thread is enough — the only work this runtime does
        // is `send().await` (one wakeup) + `reply.await` (one wakeup);
        // it doesn't need a thread pool.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "PrimaryHandle: tokio runtime init failed: {e}"
                ))
            })?;
        Ok(Self {
            sender,
            rt: Arc::new(BackgroundDropRuntime::new(rt)),
            reinject_cap,
        })
    }

    /// Drive one (`command`, `reply`) pair end-to-end: send, await
    /// the reply, translate the inner `Result<(), String>` into PyO3
    /// shape. Centralised so the per-method handlers stay one-liners
    /// and the error-translation rules don't drift.
    fn run_command(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<(), String>>) -> PrimaryCommand<RunnerIdentifier>,
    ) -> PyResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = build(reply_tx);
        let sender = self.sender.clone();
        let rt = self.rt.clone();
        let outcome: Result<Result<(), String>, String> = rt.block_on(async move {
            sender.send(cmd).await.map_err(|_| {
                "PrimaryHandle: command channel closed (coordinator dropped?)".to_string()
            })?;
            reply_rx
                .await
                .map_err(|_| "PrimaryHandle: reply oneshot dropped".to_string())
        });
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
            Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        }
    }
}

#[pymethods]
impl PyPrimaryHandle {
    /// Mark `hash` as permanently failed via the
    /// `pending_pool::on_item_failed_permanent` primitive + cascade.
    /// `error_kind` is the wire-token form of `ErrorType` (e.g.
    /// `"non_recoverable"`, `"oom"`, `"recoverable"`, or
    /// `"resource_exhausted:<kind>"` for non-memory exhaustion).
    ///
    /// Raises `PyRuntimeError` if the hash is unknown, the channel is
    /// closed, or any reply oneshot disconnects mid-flight.
    #[pyo3(signature = (hash, error_kind, reason = None))]
    fn fail_permanent(
        &self,
        hash: &Bound<'_, PyBytes>,
        error_kind: &str,
        reason: Option<String>,
    ) -> PyResult<()> {
        let hash_str = bytes_to_hash_string(hash)?;
        let error = ErrorType::from_wire(error_kind).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "fail_permanent: unknown error_kind {error_kind:?} \
                 (accepted: non_recoverable, recoverable, oom, \
                 resource_exhausted:<kind>)"
            ))
        })?;
        let reason = reason.unwrap_or_else(|| "fail_permanent via PrimaryHandle".into());
        self.run_command(move |reply| PrimaryCommand::FailPermanent {
            hash: hash_str,
            error,
            reason,
            reply,
        })
    }

    /// Reinject a task whose CRDT state is the operator-resolvable-
    /// failure class. Returns `Ok(None)` on accept; raises
    /// `PyRuntimeError` on budget exhaustion / wrong-state / unknown
    /// hash.
    fn reinject_task(&self, hash: &Bound<'_, PyBytes>) -> PyResult<()> {
        let hash_str = bytes_to_hash_string(hash)?;
        self.run_command(move |reply| PrimaryCommand::ReinjectTask {
            hash: hash_str,
            reply,
        })
    }

    /// Replace the per-task preferred-secondaries list. Broadcasts
    /// the `TaskPreferredSecondariesUpdated` CRDT mutation so every
    /// node mirrors the new preference. Raises `PyRuntimeError` on
    /// unknown hash or channel failure.
    fn update_preferred_secondaries(
        &self,
        hash: &Bound<'_, PyBytes>,
        secondaries: Vec<String>,
    ) -> PyResult<()> {
        let hash_str = bytes_to_hash_string(hash)?;
        self.run_command(move |reply| PrimaryCommand::UpdatePreferredSecondaries {
            hash: hash_str,
            secondaries,
            reply,
        })
    }

    /// Set whether `peer_id` may ever host the primary role. Broadcasts
    /// the `SetCanBePrimary` CRDT mutation so every node's
    /// `RoleTable.can_be_primary` set converges on the new capability.
    /// A client uses this to permit/forbid specific peers from hosting
    /// the primary at any time during the run (independent of the
    /// peer's join-time advertisement). Raises `PyRuntimeError` on
    /// channel failure.
    fn set_can_be_primary(&self, peer_id: String, can_be_primary: bool) -> PyResult<()> {
        self.run_command(move |reply| PrimaryCommand::SetCanBePrimary {
            peer_id,
            can_be_primary,
            reply,
        })
    }

    /// Inject a batch of brand-new tasks into the running primary's
    /// cluster ledger. Single wire-broadcast event for the batch —
    /// a 100-task graph computed at runtime is ONE mutation, not
    /// 100.
    ///
    /// Returns a list of `(index, error_dict)` tuples for the input
    /// tasks that failed pre-apply validation; an empty list means
    /// every input task was accepted. The rest of the batch
    /// proceeds regardless of per-task failures.
    ///
    /// `error_dict` shape:
    ///   * `{"kind": "duplicate_task_hash", "task_hash": str}` —
    ///     content-hash collision with an existing ledger entry.
    ///   * `{"kind": "unknown_dependency", "task_hash": str,
    ///     "dep_task_id": str}` — `task_depends_on` references a
    ///     task_id not known to the ledger.
    ///
    /// Two call contexts must work:
    ///
    /// 1. **Outside any tokio runtime** (e.g. from `on_run_start`
    ///    which fires synchronously under the GIL BEFORE the
    ///    coordinator's runtime starts driving): uses the handle's
    ///    own `current_thread` runtime to send the command + await
    ///    the reply oneshot synchronously. Per-task validation
    ///    errors are returned in the result list.
    /// 2. **Inside the coordinator's runtime** (e.g. from
    ///    `on_phase_end` which fires from `process_phase_lifecycle`
    ///    while the coordinator's operational loop has yielded to
    ///    Python): `rt.block_on(...)` from this path would panic
    ///    ("Cannot start a runtime from within a runtime"), and the
    ///    coordinator's runtime is `new_current_thread` so neither
    ///    `Handle::block_on` nor `tokio::task::block_in_place`
    ///    work either. Switches to a `try_send` fire-and-forget
    ///    shape: the command lands on the coordinator's `command_rx`
    ///    immediately, the operational loop's `select!` picks it up
    ///    the next time the Python callback returns and the
    ///    coordinator resumes. The reply oneshot is dropped (no one
    ///    awaits it on this path); per-task validation errors are
    ///    therefore NOT surfaced through the sync return value in
    ///    this call context — the coordinator's handler still
    ///    enforces them and the tasks land correctly. Documented as
    ///    a contract trade-off here; the alternative (block_on on a
    ///    current_thread runtime nested inside another runtime) is
    ///    fundamentally impossible.
    ///
    /// Releases the GIL across the `tokio::block_on(...)` wait so
    /// the operational loop in the coordinator's runtime can drive
    /// the command (acquiring the GIL elsewhere is undeadlocked).
    /// Raises `PyRuntimeError` for vec-wide failure modes (command
    /// channel closed, oneshot dropped).
    ///
    /// Item extraction goes through `crate::pytypes::extract_binaries`
    /// — the same duck-typed `getattr` walker every other framework
    /// entry point uses (`run_local`, `run_distributed`,
    /// `run_secondary` via `process_binaries`). This accepts both the
    /// `dynamic_runner._native.TaskInfo` pyclass AND the
    /// `dynamic_runner._shared.task_info.TaskInfo` Python dataclass;
    /// strict `item.extract::<PyTaskInfo>()` would reject the
    /// dataclass shape with `TypeError: 'TaskInfo' object is not an
    /// instance of 'TaskInfo'` (the two-class name collision
    /// documented on `PyTaskInfo`).
    fn spawn_tasks<'py>(
        &self,
        py: Python<'py>,
        tasks: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Convert the Python list into Rust `TaskInfo<RunnerIdentifier>`
        // entries BEFORE releasing the GIL — the conversion touches
        // Python-allocated objects (string interning, dict payloads)
        // and `getattr` calls require the GIL.
        let typed: Vec<TaskInfo<RunnerIdentifier>> = crate::pytypes::extract_binaries(tasks)?;

        let sender = self.sender.clone();
        let rt = self.rt.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = PrimaryCommand::SpawnTasks {
            tasks: typed,
            reply: reply_tx,
        };

        // Discriminate the two call contexts (see the rustdoc above
        // for the full rationale). `Handle::try_current()` succeeds
        // iff we're being called from within a tokio runtime — i.e.
        // from inside a callback the coordinator's loop just fired.
        let in_runtime = tokio::runtime::Handle::try_current().is_ok();

        let outcome: Result<Result<Vec<(usize, SpawnError)>, String>, String> = py.detach(|| {
            if in_runtime {
                // Fire-and-forget into the coordinator's command
                // channel. `try_send` is sync; the command lands
                // immediately. The reply oneshot's receiver
                // (`reply_rx`) is dropped at end-of-scope; the
                // coordinator's handler sees a dropped receiver and
                // silently skips the reply send — no panic, no
                // resource leak. Per-task validation errors flow
                // through the handler's existing tracing path
                // instead of the sync return.
                match sender.try_send(command) {
                    Ok(()) => Ok(Ok(Vec::new())),
                    Err(tokio_mpsc::error::TrySendError::Closed(_)) => Err(
                        "PrimaryHandle: command channel closed (coordinator dropped?)".to_string(),
                    ),
                    Err(tokio_mpsc::error::TrySendError::Full(_)) => {
                        Err("PrimaryHandle: command channel full — coordinator \
                         is not draining commands"
                            .to_string())
                    }
                }
            } else {
                rt.block_on(async move {
                    sender.send(command).await.map_err(|_| {
                        "PrimaryHandle: command channel closed (coordinator dropped?)".to_string()
                    })?;
                    reply_rx
                        .await
                        .map_err(|_| "PrimaryHandle: reply oneshot dropped".to_string())
                })
            }
        });
        let errors = match outcome {
            Ok(Ok(errors)) => errors,
            Ok(Err(e)) => return Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
            Err(e) => return Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
        };
        let out = PyList::empty(py);
        for (idx, err) in errors {
            let dict = PyDict::new(py);
            match err {
                SpawnError::DuplicateTaskHash(hash) => {
                    dict.set_item("kind", "duplicate_task_hash")?;
                    dict.set_item("task_hash", hash)?;
                }
                SpawnError::DuplicateInBatch(hash) => {
                    dict.set_item("kind", "duplicate_in_batch")?;
                    dict.set_item("task_hash", hash)?;
                }
                SpawnError::UnknownDependency {
                    task_hash,
                    dep_task_id,
                } => {
                    dict.set_item("kind", "unknown_dependency")?;
                    dict.set_item("task_hash", task_hash)?;
                    dict.set_item("dep_task_id", dep_task_id)?;
                }
            }
            let tuple = PyTuple::new(py, [idx.into_pyobject(py)?.into_any(), dict.into_any()])?;
            out.append(tuple)?;
        }
        Ok(out)
    }

    /// Set the per-task budget cap for `reinject_task`. Mutates the
    /// shared cell that `PyPrimaryCoordinator::run` reads at run-
    /// start. Raises `PyRuntimeError` if called after the
    /// coordinator has entered `run()` — the inner coordinator owns
    /// its own copy of the cap from that moment on, so a setter call
    /// here would silently no-op without the gate.
    fn set_unfulfillable_reinject_max_per_task(&self, n: Option<u32>) -> PyResult<()> {
        let mut g = self
            .reinject_cap
            .inner
            .lock()
            .expect("ReinjectCapCell poisoned");
        if g.run_started {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "set_unfulfillable_reinject_max_per_task: must be called \
                 before run() starts",
            ));
        }
        g.max_per_task = n;
        Ok(())
    }
}

/// Decode a Python `bytes` value into the wire-canonical hash string
/// the rest of the dispatcher uses. The wire form is
/// `format!("{:016x}", hasher.finish())` (see
/// `dynrunner_manager_distributed::compute_task_hash`); callers pass
/// the raw 16-byte hex-encoded ASCII representation through `bytes`.
/// Invalid UTF-8 raises `PyValueError` so the Python side surfaces a
/// typed exception instead of a panicking unwrap.
fn bytes_to_hash_string(hash: &Bound<'_, PyBytes>) -> PyResult<String> {
    let bytes = hash.as_bytes();
    std::str::from_utf8(bytes)
        .map(|s| s.to_owned())
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "PrimaryHandle: hash bytes are not valid UTF-8: {e}"
            ))
        })
}

#[cfg(test)]
#[cfg(feature = "test-with-python")]
mod tests {
    //! GIL-release smoke for `PrimaryHandle::spawn_tasks`.
    //!
    //! Tests require an embedded CPython interpreter; gated behind
    //! the `test-with-python` feature. Invoke as:
    //!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
    //!        --features test-with-python primary_handle`
    use super::*;
    use dynrunner_manager_distributed::primary::COMMAND_CHANNEL_CAPACITY;

    /// Build a `PyPrimaryHandle` paired with a stub receiver task
    /// that echoes back empty per-index errors for every
    /// `PrimaryCommand::SpawnTasks` it sees. The receiver runs in a
    /// dedicated OS thread with its own tokio runtime so the Python
    /// side's `block_on` (inside `spawn_tasks`) and the stub's
    /// `recv().await` don't deadlock on a single runtime.
    fn handle_with_stub_receiver() -> (PyPrimaryHandle, std::thread::JoinHandle<()>) {
        let (tx, mut rx) =
            tokio_mpsc::channel::<PrimaryCommand<RunnerIdentifier>>(COMMAND_CHANNEL_CAPACITY);
        let cell = crate::managers::primary_handle::ReinjectCapCell::default();
        let handle = PyPrimaryHandle::from_sender(tx, cell).expect("handle init");
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("stub runtime");
            rt.block_on(async {
                while let Some(cmd) = rx.recv().await {
                    if let PrimaryCommand::SpawnTasks { reply, .. } = cmd {
                        let _ = reply.send(Ok(Vec::new()));
                    }
                }
            });
        });
        (handle, thread)
    }

    /// End-to-end smoke: `spawn_tasks` from Python goes through the
    /// PyO3 method, releases the GIL via `py.detach`, the stub
    /// receiver echoes an Ok with empty errors, and the method
    /// returns an empty Python list. Pins:
    ///   1. The PyO3 method compiles + dispatches.
    ///   2. The `py.detach(...)` GIL-release boundary doesn't
    ///      deadlock against the stub receiver's own runtime.
    ///   3. The reply translation produces a `list` (empty here =
    ///      full success).
    #[test]
    fn primary_handle_spawn_tasks_releases_gil() {
        let (handle, stub_thread) = handle_with_stub_receiver();
        Python::attach(|py| {
            let list = PyList::empty(py);
            let result = handle
                .spawn_tasks(py, &list)
                .expect("spawn_tasks must succeed for empty input");
            assert_eq!(result.len(), 0, "empty input → empty errors");
        });
        // Drop the handle so the stub receiver's recv() returns
        // None and the OS thread exits cleanly. The handle's
        // `sender` is the only strong reference here; dropping it
        // closes the channel.
        drop(handle);
        stub_thread.join().expect("stub thread joined");
    }

    /// Regression: a `PyPrimaryHandle` (the LAST `Arc` to its in-handle
    /// runtime) dropped from WITHIN another tokio runtime's async
    /// context must NOT panic.
    ///
    /// This reproduces the secondary-process crash exactly. On the SLURM
    /// secondary path the handle is captured into the promoted-primary
    /// recipe and threaded into the secondary's
    /// `rt.block_on(local.run_until(node.run(inputs)))`. A plain
    /// (never-promoted) secondary never fires the recipe, so the recipe —
    /// and the sole `PyPrimaryHandle` it owns — is dropped when `node.run`
    /// returns at END-OF-RUN, i.e. inside that outer async context. With
    /// the default `Runtime::drop` that drop panics ("Cannot drop a
    /// runtime in a context where blocking is not allowed"), taking the
    /// whole secondary process down. The [`BackgroundDropRuntime`] wrapper
    /// makes the drop a non-blocking `shutdown_background()` instead.
    ///
    /// Revert-check: replace the handle's `Arc<BackgroundDropRuntime>`
    /// with a bare `Arc<tokio::runtime::Runtime>` and this test panics on
    /// the in-context drop (the `catch_unwind` below captures it).
    #[test]
    fn primary_handle_drop_inside_async_context_does_not_panic() {
        let (tx, _rx) =
            tokio_mpsc::channel::<PrimaryCommand<RunnerIdentifier>>(COMMAND_CHANNEL_CAPACITY);
        let cell = ReinjectCapCell::default();
        let handle = PyPrimaryHandle::from_sender(tx, cell).expect("handle init");

        // Mirror the secondary's outer driver: a `current_thread` runtime
        // whose `block_on` runs the (async) scope that owns the handle.
        // Dropping `handle` at the end of the async block drops the LAST
        // strong `Arc` to the in-handle runtime FROM WITHIN this async
        // context — the exact shape that panics without the fix.
        let outer = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("outer runtime");

        // `catch_unwind` so a regression surfaces as a clean test failure
        // (with the captured panic message) rather than aborting the
        // process. The runtime is `RefUnwindSafe`; the handle owns only
        // `Send + 'static` channel/runtime state.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            outer.block_on(async move {
                // The move-in makes this async block the SOLE owner of
                // `handle`; it drops here, inside `block_on`.
                let _h = handle;
            });
        }));

        assert!(
            result.is_ok(),
            "dropping a PyPrimaryHandle inside an async context must not panic \
             (its runtime must shut down in the background); got panic: {:?}",
            result.err(),
        );
    }
}
