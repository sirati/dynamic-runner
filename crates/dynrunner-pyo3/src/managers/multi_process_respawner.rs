//! PyO3 adapter that implements [`SecondarySpawner`] by invoking a
//! Python callable.
//!
//! Single concern: bridge the Rust async respawn API surface
//! ([`SecondarySpawner::spawn`] / [`SecondarySpawnSpec`] / [`SpawnError`])
//! to the existing Python ``spawn_secondary`` callback. This is the
//! ``--multi-computer local`` provider: the primary already knows how
//! to construct a local-subprocess argv inside Python (see
//! ``python/dynamic_runner/spawn_secondary.py``); the respawn pipeline
//! reuses that callback verbatim. SLURM and remote-launch providers
//! live in sibling modules and depend on the same trait.
//!
//! Module boundary (the only surface other code crosses):
//!
//! - From Python: [`PyMultiProcessSpawner::new`] takes the callable,
//!   the primary's endpoint URL, and the primary's PEM-encoded public
//!   key. Python instantiates the pyclass once at primary startup and
//!   hands it to the coordinator (the actual ``JoinSet`` wiring lands
//!   in the sibling subtask that owns the operational loop).
//! - From Rust: the coordinator holds it as
//!   ``Arc<dyn SecondarySpawner>`` and calls ``spawn(spec)`` whenever
//!   ``peer_lifecycle::PeerRemoved`` triggers a replacement. Internals
//!   of this adapter are not visible.
//!
//! GIL discipline: ``spawn`` is async, so we acquire the GIL on a
//! ``tokio::task::spawn_blocking`` thread (not on the executor's
//! tokio task) to avoid stalling the executor for the duration of the
//! Python call. The Python callback is pure argv assembly today — sub-
//! millisecond — but a future provider may issue an ``ssh`` or
//! ``podman`` call from inside the same callable, so respecting the
//! "no blocking work on tokio worker threads" invariant up front
//! keeps the contract honest.
//!
//! Return-value contract: the Python callback may return ``None``
//! (SLURM-style "already spawned, no Rust child to own") or a
//! ``SubprocessSpec``. For the respawn surface, both are success —
//! producing a live ``std::process::Child`` and owning its lifetime
//! is the wider primary's job (see ``managers::primary``), not this
//! adapter's. ``SpawnError`` is reserved for callback failures
//! (raised ``PyErr``), so a successful no-op return is ``Ok(())``.

use std::sync::Arc;

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawnSpec, SecondarySpawner, SpawnError,
};

/// Inner state, separated out from the pyclass so the coordinator
/// can hold an `Arc<dyn SecondarySpawner>` cloned from this adapter
/// without requiring `&PyCell` access. The pyclass itself owns one
/// `Arc<MultiProcessSpawnerInner>`; calls to
/// [`PyMultiProcessSpawner::as_arc`] (Rust-only) hand back a clone of
/// that Arc upcast to the trait object the coordinator consumes.
///
/// Single concern: hold the Python callback + the construction-time
/// snapshot. The `SecondarySpawner` impl below lives on this type
/// (not on the pyclass wrapper) so the trait-object impl can move
/// freely across thread / runtime boundaries — pyclass instances are
/// pinned to the GIL-managed `PyCell` and cannot be sent as
/// `Arc<dyn SecondarySpawner>` directly.
pub(crate) struct MultiProcessSpawnerInner {
    spawn_callable: Py<PyAny>,
    primary_endpoint: String,
    primary_pubkey_pem: String,
}

/// Adapter from the Rust [`SecondarySpawner`] trait to a Python
/// ``spawn_secondary`` callable.
///
/// Construction-time fields (``primary_endpoint`` / ``primary_pubkey_pem``)
/// are the source of truth for the respawn callbacks: the Rust primary
/// already knows its own listen endpoint and certificate at startup,
/// and threading those through ``SecondarySpawnSpec`` would force every
/// trait-level caller to repeat that knowledge. The per-call spec
/// supplies ``new_secondary_id`` — the only piece the adapter cannot
/// know ahead of time. This split mirrors how the existing
/// ``primary.rs`` initial-spawn path treats ``primary_url`` (built once
/// from the bound port) and ``secondary_id`` (formatted per call).
#[pyclass(name = "PyMultiProcessSpawner")]
pub(crate) struct PyMultiProcessSpawner {
    inner: Arc<MultiProcessSpawnerInner>,
}

#[pymethods]
impl PyMultiProcessSpawner {
    #[new]
    fn new(
        spawn_callable: Py<PyAny>,
        primary_endpoint: String,
        primary_pubkey_pem: String,
    ) -> Self {
        Self {
            inner: Arc::new(MultiProcessSpawnerInner {
                spawn_callable,
                primary_endpoint,
                primary_pubkey_pem,
            }),
        }
    }
}

impl PyMultiProcessSpawner {
    /// Rust-side hand-off: clone the inner `Arc` and upcast it to the
    /// trait object the coordinator's `enable_respawn` consumes.
    /// Single concern: bridge the pyclass-owned `Arc<Inner>` to the
    /// trait-object surface; the coordinator never sees the pyclass
    /// type directly. Called by `PyPrimaryCoordinator::run` at
    /// coordinator-construction time.
    pub(crate) fn as_arc(&self) -> Arc<dyn SecondarySpawner> {
        Arc::clone(&self.inner) as Arc<dyn SecondarySpawner>
    }
}

/// GIL-side invocation of the Python callback. Free function (not a
/// method on `PyMultiProcessSpawner`) so the async impl can hand it a
/// `'static`-friendly snapshot of the callable + arguments without
/// borrowing `&self` — `async_trait`'s desugaring requires the future
/// to be `'static`+`Send`, which a `&self` borrow held across
/// `spawn_blocking.await` would violate. Single concern: acquire the
/// GIL, build the kwarg dict, invoke. The caller chooses whether to
/// run this on the executor thread or a blocking thread.
fn invoke_python_callback(
    callable: &Py<PyAny>,
    primary_endpoint: &str,
    new_secondary_id: &str,
    primary_pubkey_pem: &str,
) -> Result<(), SpawnError> {
    Python::attach(|py| -> PyResult<()> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("primary_pubkey_pem", primary_pubkey_pem)?;
        let _ = callable.bind(py).call(
            (
                primary_endpoint,
                new_secondary_id,
                // quic_port: the multi-process callback ignores this
                // today (subprocess auto-binds), but the positional
                // contract from the initial-spawn path is
                // `(primary_url, secondary_id, quic_port)` so we keep
                // symmetry with `managers/primary.rs`.
                0u16,
            ),
            Some(&kwargs),
        )?;
        Ok(())
    })
    .map_err(|e| SpawnError::Other(e.to_string()))
}

#[async_trait(?Send)]
impl SecondarySpawner for MultiProcessSpawnerInner {
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError> {
        // Snapshot the bits the blocking task needs. Cloning is cheap
        // (3 strings + one Py<PyAny> refcount bump) and keeps the
        // lifetime story trivial: `spawn_blocking` requires a
        // `'static` closure, so it cannot borrow `&self`. `Py<PyAny>`
        // refcounts live in the interpreter — `clone_ref` is the
        // GIL-acquire path for "another owner".
        let callable = Python::attach(|py| self.spawn_callable.clone_ref(py));
        let endpoint = self.primary_endpoint.clone();
        let pubkey = self.primary_pubkey_pem.clone();
        let new_id = spec.new_secondary_id;

        tokio::task::spawn_blocking(move || {
            invoke_python_callback(&callable, &endpoint, &new_id, &pubkey)
        })
        .await
        .map_err(|join_err| {
            SpawnError::Other(format!("spawn_blocking join failed: {join_err}"))
        })?
    }
}

#[cfg(test)]
mod tests {
    //! Adapter-level contract tests. Drive the Python callback through
    //! the `SecondarySpawner` trait surface — i.e. the same path the
    //! coordinator will take in the sibling subtask that wires up the
    //! `JoinSet`. Each test stands up an `tokio::runtime` so the
    //! `async fn spawn` can be `block_on`-ed in a `#[test]`.

    use super::*;
    use pyo3::types::{PyDict, PyList, PyTuple};

    /// Compile + run a tiny Python module under the current GIL and
    /// hand back the named attribute as a callable. Centralised so
    /// each test phrases its mock callback in pure Python without
    /// fighting PyO3's `PyModule::from_code` lifetime story at the
    /// callsite.
    fn make_python_callable(source: &str, attr: &str) -> Py<PyAny> {
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(source).unwrap().as_c_str(),
                std::ffi::CString::new("mock_spawn.py").unwrap().as_c_str(),
                std::ffi::CString::new("mock_spawn").unwrap().as_c_str(),
            )
            .expect("compile mock python module");
            module.getattr(attr).unwrap().unbind()
        })
    }

    /// Tokio runtime for the async trait method. `current_thread` is
    /// sufficient — `spawn_blocking` only needs the blocking pool,
    /// which `current_thread + enable_all` provides.
    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
    }

    fn spec(new_id: &str) -> SecondarySpawnSpec {
        SecondarySpawnSpec {
            new_secondary_id: new_id.to_owned(),
            // The adapter intentionally ignores these — the
            // construction-time fields on the pyclass are the source
            // of truth. Filled here only so the spec is well-formed.
            primary_endpoint: "tcp://ignored-by-adapter:0".to_owned(),
            primary_pubkey_pem: "-----IGNORED BY ADAPTER-----".to_owned(),
        }
    }

    #[test]
    fn multi_process_spawner_invokes_python_callback_with_kwargs() {
        // Mock callback records every positional + keyword argument it
        // sees onto a module-level `calls` list. The test inspects the
        // list after `spawn` returns to assert both the positional
        // contract and the `primary_pubkey_pem` kwarg.
        let callable = make_python_callable(
            "calls = []\n\
             def cb(*args, **kwargs):\n    \
                 calls.append((args, dict(kwargs)))\n    \
                 return None\n",
            "cb",
        );
        let module_handle = Python::attach(|py| {
            callable
                .bind(py)
                .getattr("__globals__")
                .unwrap()
                .unbind()
        });

        let spawner = PyMultiProcessSpawner::new(
            callable,
            "tcp://127.0.0.1:5555".to_owned(),
            "-----BEGIN PUBLIC KEY-----\nFAKEPEM\n".to_owned(),
        );

        rt().block_on(async {
            spawner
                .as_arc()
                .spawn(spec("sec-replacement-1"))
                .await
                .expect("spawn ok");
        });

        // Inspect the recorded call.
        Python::attach(|py| {
            let globals = module_handle.bind(py);
            let calls = globals.get_item("calls").unwrap();
            let calls_list = calls.cast::<PyList>().unwrap();
            assert_eq!(calls_list.len(), 1, "callback should be invoked exactly once");
            let entry = calls_list.get_item(0).unwrap();
            let entry_tuple = entry.cast::<PyTuple>().unwrap();
            let args = entry_tuple.get_item(0).unwrap();
            let kwargs = entry_tuple.get_item(1).unwrap();

            let args_tuple = args.cast::<PyTuple>().unwrap();
            assert_eq!(
                args_tuple.len(),
                3,
                "positional contract is (primary_url, secondary_id, quic_port)",
            );
            let primary_url: String = args_tuple.get_item(0).unwrap().extract().unwrap();
            let secondary_id: String = args_tuple.get_item(1).unwrap().extract().unwrap();
            let quic_port: u16 = args_tuple.get_item(2).unwrap().extract().unwrap();
            assert_eq!(primary_url, "tcp://127.0.0.1:5555");
            assert_eq!(secondary_id, "sec-replacement-1");
            assert_eq!(quic_port, 0);

            let kwargs_dict = kwargs.cast::<PyDict>().unwrap();
            let pem: String = kwargs_dict
                .get_item("primary_pubkey_pem")
                .unwrap()
                .expect("primary_pubkey_pem kwarg must be set")
                .extract()
                .unwrap();
            assert_eq!(pem, "-----BEGIN PUBLIC KEY-----\nFAKEPEM\n");
        });
    }

    #[test]
    fn multi_process_spawner_translates_pyerr_to_spawn_error() {
        // Callback raises a plain RuntimeError. Adapter must surface
        // it as `SpawnError::Other(stringified)`; budget/cooldown
        // logic in the coordinator's JoinSet drain treats `Other(_)`
        // as a transient failure (per the per-secondary cap).
        let callable = make_python_callable(
            "def cb(*args, **kwargs):\n    \
                 raise RuntimeError('mock spawn failure')\n",
            "cb",
        );
        let spawner = PyMultiProcessSpawner::new(
            callable,
            "tcp://127.0.0.1:5555".to_owned(),
            "-----BEGIN PUBLIC KEY-----\n".to_owned(),
        );

        let outcome =
            rt().block_on(async { spawner.as_arc().spawn(spec("sec-replacement-1")).await });

        let err = outcome.expect_err("callback raised, adapter must report SpawnError");
        match err {
            SpawnError::Other(msg) => {
                assert!(
                    msg.contains("mock spawn failure"),
                    "stringified PyErr should preserve the Python message; got {msg}",
                );
            }
            other => panic!("expected SpawnError::Other, got {other:?}"),
        }
    }

    #[test]
    fn multi_process_spawner_respects_spec_secondary_id() {
        // Two invocations with different `new_secondary_id`s must
        // reach the Python callback with the exact same strings —
        // i.e. the spec is what flows through, not a hard-coded
        // construction-time value. This is the regression pin for
        // "respawn picks a fresh id; the adapter must forward it".
        let callable = make_python_callable(
            "seen_ids = []\n\
             def cb(primary_url, secondary_id, quic_port, **kwargs):\n    \
                 seen_ids.append(secondary_id)\n    \
                 return None\n",
            "cb",
        );
        let module_handle = Python::attach(|py| {
            callable
                .bind(py)
                .getattr("__globals__")
                .unwrap()
                .unbind()
        });

        let spawner = PyMultiProcessSpawner::new(
            callable,
            "tcp://127.0.0.1:5555".to_owned(),
            "-----BEGIN PUBLIC KEY-----\n".to_owned(),
        );

        let rt = rt();
        rt.block_on(async {
            let arc = spawner.as_arc();
            arc.spawn(spec("sec-a-replacement")).await.unwrap();
            arc.spawn(spec("sec-b-replacement")).await.unwrap();
        });

        Python::attach(|py| {
            let globals = module_handle.bind(py);
            let seen = globals.get_item("seen_ids").unwrap();
            let seen_list = seen.cast::<PyList>().unwrap();
            assert_eq!(seen_list.len(), 2);
            let first: String = seen_list.get_item(0).unwrap().extract().unwrap();
            let second: String = seen_list.get_item(1).unwrap().extract().unwrap();
            assert_eq!(first, "sec-a-replacement");
            assert_eq!(second, "sec-b-replacement");
        });
    }
}
