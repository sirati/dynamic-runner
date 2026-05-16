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
use crate::pytypes::PyTaskInfo;

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
        self.inner.lock().expect("ReinjectCapCell poisoned").max_per_task
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
    sender: tokio_mpsc::Sender<PrimaryCommand<RunnerIdentifier>>,

    /// In-handle runtime for `block_on(...)` calls from Python. Shared
    /// across clones of `PrimaryHandle` so multiple calls don't each
    /// pay the runtime-construction cost. `Arc` so `#[derive(Clone)]`
    /// keeps the runtime alive across handle clones.
    rt: Arc<tokio::runtime::Runtime>,

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
            rt: Arc::new(rt),
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
            sender
                .send(cmd)
                .await
                .map_err(|_| {
                    "PrimaryHandle: command channel closed (coordinator dropped?)"
                        .to_string()
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
    /// Releases the GIL across the `tokio::block_on(...)` wait so
    /// the operational loop in the coordinator's runtime can drive
    /// the command (acquiring the GIL elsewhere is undeadlocked).
    /// Raises `PyRuntimeError` for vec-wide failure modes (command
    /// channel closed, oneshot dropped).
    fn spawn_tasks<'py>(
        &self,
        py: Python<'py>,
        tasks: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Convert the Python list into Rust `TaskInfo<RunnerIdentifier>`
        // entries BEFORE releasing the GIL. The PyO3 conversion
        // touches Python-allocated objects (string interning, dict
        // payloads); doing it inside `allow_threads` would re-acquire
        // the GIL implicitly. Pre-convert + ship the typed vec into
        // the runtime, then re-acquire only to build the return list.
        let mut typed: Vec<TaskInfo<RunnerIdentifier>> = Vec::with_capacity(tasks.len());
        for item in tasks.iter() {
            let py_task: PyTaskInfo = item.extract()?;
            typed.push(TaskInfo::from(&py_task));
        }

        let sender = self.sender.clone();
        let rt = self.rt.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        let outcome: Result<Result<Vec<(usize, SpawnError)>, String>, String> = py.detach(|| {
            rt.block_on(async move {
                sender
                    .send(PrimaryCommand::SpawnTasks {
                        tasks: typed,
                        reply: reply_tx,
                    })
                    .await
                    .map_err(|_| {
                        "PrimaryHandle: command channel closed (coordinator dropped?)"
                            .to_string()
                    })?;
                reply_rx
                    .await
                    .map_err(|_| "PrimaryHandle: reply oneshot dropped".to_string())
            })
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
                SpawnError::UnknownDependency { task_hash, dep_task_id } => {
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
        let mut g = self.reinject_cap.inner.lock().expect("ReinjectCapCell poisoned");
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
    use pyo3::types::PyAnyMethods;

    /// Build a `PyPrimaryHandle` paired with a stub receiver task
    /// that echoes back empty per-index errors for every
    /// `PrimaryCommand::SpawnTasks` it sees. The receiver runs in a
    /// dedicated OS thread with its own tokio runtime so the Python
    /// side's `block_on` (inside `spawn_tasks`) and the stub's
    /// `recv().await` don't deadlock on a single runtime.
    fn handle_with_stub_receiver() -> (PyPrimaryHandle, std::thread::JoinHandle<()>) {
        let (tx, mut rx) = tokio_mpsc::channel::<PrimaryCommand<RunnerIdentifier>>(
            COMMAND_CHANNEL_CAPACITY,
        );
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
