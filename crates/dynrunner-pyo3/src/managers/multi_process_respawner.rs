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
//! - From Python: [`PyMultiProcessSpawner::new`] takes the spawn
//!   callable. The primary's endpoint and PEM-encoded public key are
//!   NOT construction-time inputs — they reach the adapter through
//!   each [`SecondarySpawnSpec`] the coordinator hands to ``spawn``.
//!   Reading per-spawn means a respawned secondary from a later
//!   generation can in principle see a refreshed pubkey without
//!   re-instantiating the adapter (today's primary keeps the same
//!   cert for the whole run; the per-spec read keeps the contract
//!   honest for future rotation paths).
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
//! ``SubprocessSpec``. For the ``SubprocessSpec`` branch, this
//! adapter calls ``SubprocessSpec::spawn`` to launch a
//! ``std::process::Child`` and OWNS its lifetime — the resulting
//! handle is registered on an internal ``tracked_children`` Vec and
//! reaped via [`crate::subprocess_factory::terminate_children`] on
//! ``Drop`` (SIGTERM → grace → SIGKILL). This mirrors the
//! initial-spawn path in ``managers::primary`` (which owns its own
//! ``child_processes`` Vec) and the SLURM provider's job-id Vec —
//! each provider owns the cleanup registry for the resources it
//! produces.
//!
//! Orphan-safety shape: the (callback invoke + ``Command::spawn`` +
//! ``tracked_children.push``) sequence runs inside
//! ``tokio::task::spawn_local`` on the surrounding ``LocalSet`` and
//! rendezvouses with the outer ``spawn()`` future via a
//! ``tokio::sync::oneshot``. The outer future awaiting on the
//! receiver can be aborted (e.g. by a ``JoinSet::shutdown``) WITHOUT
//! cancelling the inner task — so a Child that gets spawned always
//! lands in the registry before the cleanup window opens. Matches
//! the same hardening applied to the SLURM provider in
//! ``crates/dynrunner-slurm/src/respawn.rs``.
//!
//! ``SpawnError`` is reserved for callback failures (raised
//! ``PyErr``) and ``std::process::Command::spawn`` failures (the
//! executable does not exist / not executable / etc.). A successful
//! ``None`` return is ``Ok(())``.

use std::sync::{Arc, Mutex};

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
/// Single concern: hold the Python callback AND the cleanup
/// registry for the `std::process::Child` handles this adapter
/// produces (one per successful respawn). The primary's listen
/// endpoint and pubkey arrive per-spawn through the
/// [`SecondarySpawnSpec`] handed to the trait method — they are NOT
/// stored on this struct. The `SecondarySpawner` impl below lives on
/// this type (not on the pyclass wrapper) so the trait-object impl
/// can move freely across thread / runtime boundaries — pyclass
/// instances are pinned to the GIL-managed `PyCell` and cannot be
/// sent as `Arc<dyn SecondarySpawner>` directly.
///
/// The `tracked_children` Vec is the single source of truth for
/// respawn-produced `Child` handles in the local provider — its
/// counterpart in the SLURM provider is the `job_ids` Vec on
/// `SlurmJobManager`. `Option<Child>` matches the slot shape
/// [`crate::subprocess_factory::terminate_children`] expects so the
/// kill-ladder primitive can be reused verbatim from `Drop`.
pub(crate) struct MultiProcessSpawnerInner {
    spawn_callable: Py<PyAny>,
    /// Registry of Rust-owned subprocess handles for every
    /// successfully-respawned secondary. Pushed under the lock by
    /// the inner `spawn_local` task before `spawn()` returns Ok;
    /// drained by `Drop` to issue the SIGTERM/SIGKILL ladder via
    /// [`crate::subprocess_factory::terminate_children`].
    tracked_children: Arc<Mutex<Vec<Option<std::process::Child>>>>,
}

/// Adapter from the Rust [`SecondarySpawner`] trait to a Python
/// ``spawn_secondary`` callable.
///
/// The Python callback receives `(primary_url, secondary_id, quic_port)`
/// positionally and `primary_pubkey_pem` as a kwarg. Each value comes
/// from the per-spawn [`SecondarySpawnSpec`]: the coordinator
/// populates the spec from its own bound NetworkServer's cert and
/// endpoint inside `enable_respawn`, so the adapter only relays — no
/// construction-time cache, no GIL-side snapshotting.
#[pyclass(name = "PyMultiProcessSpawner")]
pub(crate) struct PyMultiProcessSpawner {
    inner: Arc<MultiProcessSpawnerInner>,
}

#[pymethods]
impl PyMultiProcessSpawner {
    #[new]
    fn new(spawn_callable: Py<PyAny>) -> Self {
        Self {
            inner: Arc::new(MultiProcessSpawnerInner {
                spawn_callable,
                tracked_children: Arc::new(Mutex::new(Vec::new())),
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
/// GIL, build the kwarg dict, invoke the callable, and turn its return
/// value into an owned `Option<SubprocessSpec>` — `None` for the
/// SLURM-style "already spawned" no-op signal, `Some(spec)` for the
/// data-only argv/env bundle the local-subprocess path returns. The
/// caller takes the `SubprocessSpec` and is responsible for the actual
/// `Command::spawn` outside the GIL.
fn invoke_python_callback(
    callable: &Py<PyAny>,
    primary_endpoint: &str,
    new_secondary_id: &str,
    primary_pubkey_pem: &str,
) -> Result<Option<crate::managers::subprocess_spec::SubprocessSpec>, SpawnError> {
    Python::attach(
        |py| -> PyResult<Option<crate::managers::subprocess_spec::SubprocessSpec>> {
            let kwargs = PyDict::new(py);
            kwargs.set_item("primary_pubkey_pem", primary_pubkey_pem)?;
            let ret = callable.bind(py).call(
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
            if ret.is_none() {
                Ok(None)
            } else {
                Ok(Some(
                    crate::managers::subprocess_spec::SubprocessSpec::from_pyany(&ret)?,
                ))
            }
        },
    )
    .map_err(|e| SpawnError::Other(e.to_string()))
}

#[async_trait(?Send)]
impl SecondarySpawner for MultiProcessSpawnerInner {
    /// Orphan-safety shape (parallel to the SLURM provider): the
    /// (callback invoke + `Command::spawn` + `tracked_children.push`)
    /// sequence runs inside `tokio::task::spawn_local`, NOT inline on
    /// the caller's future. The `spawn_local` task is parented to the
    /// surrounding `LocalSet` (the operational loop's `run_until`),
    /// not to the coordinator's `respawn_tasks` JoinSet which gets
    /// `.shutdown().await`-d on teardown. The outer `spawn()` future
    /// awaits a `oneshot::Receiver`; dropping the receiver does NOT
    /// abort the inner task — the `Command::spawn` completes, the
    /// resulting `Child` is registered on `tracked_children`, and the
    /// coordinator's `Drop` path (which drains the same Vec) will
    /// SIGTERM/SIGKILL it.
    ///
    /// This closes the equivalent hazard window the SLURM provider
    /// closed in `crates/dynrunner-slurm/src/respawn.rs`: a JoinSet
    /// abort racing against the Child-spawn point can never produce
    /// an unregistered (i.e. unreaped) subprocess.
    async fn spawn(&self, spec: SecondarySpawnSpec) -> Result<(), SpawnError> {
        // Snapshot the bits the inner task needs. Cloning is cheap
        // (3 strings + one Py<PyAny> refcount bump + one Arc clone)
        // and keeps the lifetime story trivial: `spawn_local`
        // requires a `'static` future, so it cannot borrow `&self`.
        // `Py<PyAny>` refcounts live in the interpreter —
        // `clone_ref` is the GIL-acquire path for "another owner".
        //
        // Endpoint + pubkey come from the per-spawn `spec`, NOT a
        // construction-time field — see the module-level rationale
        // (the coordinator owns the trust anchor; this adapter is
        // pure relay).
        let callable = Python::attach(|py| self.spawn_callable.clone_ref(py));
        let tracked = Arc::clone(&self.tracked_children);
        let endpoint = spec.primary_endpoint;
        let pubkey = spec.primary_pubkey_pem;
        let new_id = spec.new_secondary_id;

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), SpawnError>>();

        tokio::task::spawn_local(async move {
            // The Python callback may issue blocking work in future
            // providers (ssh / podman) even though today it is pure
            // argv assembly. Run it on a blocking thread to honour
            // the "no blocking work on tokio worker threads"
            // invariant up front; `spawn_blocking` is the GIL-safe
            // path because we hand the future an owned `Py<PyAny>`
            // refcount, not a borrow.
            let callback_outcome = tokio::task::spawn_blocking(move || {
                invoke_python_callback(&callable, &endpoint, &new_id, &pubkey)
                    .map(|maybe_spec| (maybe_spec, new_id))
            })
            .await;

            let outcome = match callback_outcome {
                Err(join_err) => Err(SpawnError::Other(format!(
                    "spawn_blocking join failed: {join_err}"
                ))),
                Ok(Err(spawn_err)) => Err(spawn_err),
                Ok(Ok((None, new_id))) => {
                    // SLURM-style no-op: the callback declined to
                    // produce a Child. Nothing to register; the
                    // respawn is a successful pass-through.
                    tracing::info!(
                        new_secondary_id = %new_id,
                        "respawn callback returned None (no Rust-owned Child; \
                         external launcher path)",
                    );
                    Ok(())
                }
                Ok(Ok((Some(subproc_spec), new_id))) => {
                    match subproc_spec.spawn() {
                        Ok(child) => {
                            let pid = child.id();
                            // Register BEFORE `tx.send(Ok)`: by the
                            // time the outer future observes
                            // success, the Child is on the cleanup
                            // registry. `std::sync::Mutex` —
                            // intentional: the only other lock site
                            // is `Drop` (no async), so a poisoned
                            // lock is a programming error worth
                            // panicking on, not silently swallowing.
                            tracked
                                .lock()
                                .expect("tracked_children mutex poisoned")
                                .push(Some(child));
                            tracing::info!(
                                new_secondary_id = %new_id,
                                pid,
                                "respawned secondary subprocess (Rust-owned Child registered \
                                 for cleanup)",
                            );
                            Ok(())
                        }
                        Err(e) => Err(SpawnError::Other(format!(
                            "Command::spawn for respawn {new_id}: {e}"
                        ))),
                    }
                }
            };

            // Best-effort send: if the outer future was aborted, the
            // receiver is gone and `send` returns Err. The inner
            // task has already registered the Child (or determined
            // there was no Child to register), so the orphan-safety
            // invariant holds regardless.
            let _ = tx.send(outcome);
        });

        // Outer future awaits the oneshot. If the JoinSet drops this
        // future, the `rx` is dropped — the inner task's `tx.send`
        // becomes a no-op (we ignore the Err via `let _ =`). The
        // inner task continues to completion, so any spawned Child
        // is on the registry by the time `Drop` runs.
        rx.await.unwrap_or_else(|_recv_err| {
            Err(SpawnError::Other(
                "respawn inner task dropped its sender before completion".to_string(),
            ))
        })
    }
}

impl Drop for MultiProcessSpawnerInner {
    /// Drain the cleanup registry and reap every respawn-produced
    /// `Child` with the SIGTERM → grace → SIGKILL ladder. The kill
    /// primitive lives in `subprocess_factory` so containerised
    /// (podman) workers — which trap SIGTERM to clean up their
    /// conmon-supervised container — share the same teardown shape
    /// as the initial-spawn path and the SLURM provider's cleanup.
    /// Re-implementing it here would be the duplicated-logic
    /// antipattern.
    fn drop(&mut self) {
        // Take ownership of the Vec under the lock — even a poisoned
        // lock yields the guarded Vec via `into_inner`, because
        // teardown must run regardless of upstream panic state.
        let mut children = match self.tracked_children.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        if children.is_empty() {
            return;
        }
        tracing::debug!(
            count = children.len(),
            "draining respawned secondary children on spawner drop",
        );
        crate::subprocess_factory::terminate_children(&mut children);
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
    /// sufficient — the inner work hops between `spawn_blocking` (for
    /// the GIL-acquiring callback) and `spawn_local` (for the
    /// orphan-safety detach), both of which `current_thread +
    /// enable_all` provides.
    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
    }

    /// Drive an async block on a fresh `current_thread` runtime under
    /// a `LocalSet`. The production `SecondarySpawner::spawn`
    /// implementation calls `tokio::task::spawn_local` internally
    /// (orphan-safety: the inner task must outlive a JoinSet abort);
    /// `spawn_local` requires a running `LocalSet`. Mirrors the
    /// test-scaffold shape used by the SLURM provider's tests.
    fn run_local<F, T>(future: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let rt = rt();
        let local = tokio::task::LocalSet::new();
        rt.block_on(local.run_until(future))
    }

    /// Poll `kill(pid, 0)` until the kernel reports ESRCH (process is
    /// gone) or the deadline expires. Used by the cleanup test to
    /// observe that `Drop` on the spawner SIGTERMed the registered
    /// child. Returns `true` when ESRCH is observed, `false` on
    /// timeout. The Drop path's SIGTERM → grace → SIGKILL ladder caps
    /// at ~5s + poll slack so a 10s window leaves comfortable
    /// headroom on slow CI without dragging the test out.
    fn wait_for_pid_gone(pid: u32, deadline: std::time::Duration) -> bool {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        let nix_pid = Pid::from_raw(pid as i32);
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            match kill(nix_pid, None) {
                Err(Errno::ESRCH) => return true,
                _ => std::thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
        false
    }

    fn spec(new_id: &str) -> SecondarySpawnSpec {
        SecondarySpawnSpec {
            new_secondary_id: new_id.to_owned(),
            // The adapter consumes these from the spec — the
            // coordinator populates them from its NetworkServer's
            // cert PEM + bind endpoint inside `enable_respawn`.
            primary_endpoint: "tcp://127.0.0.1:5555".to_owned(),
            primary_pubkey_pem: "-----BEGIN PUBLIC KEY-----\nFAKEPEM\n".to_owned(),
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

        let spawner = PyMultiProcessSpawner::new(callable);

        run_local(async {
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
        let spawner = PyMultiProcessSpawner::new(callable);

        let outcome =
            run_local(async { spawner.as_arc().spawn(spec("sec-replacement-1")).await });

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

        let spawner = PyMultiProcessSpawner::new(callable);

        run_local(async {
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

    /// `primary_pubkey_pem` reaches the Python callback verbatim from
    /// the per-spawn `SecondarySpawnSpec`, NOT from a construction-
    /// time field. Two successive `spawn()` calls each carry a
    /// distinct pem; the callback must observe both in order — a
    /// regression here means the adapter accidentally cached a
    /// construction-time pem (the pre-fix shape) and the SLURM /
    /// cert-rotation paths would silently authenticate against the
    /// wrong anchor.
    #[test]
    fn primary_pubkey_pem_reaches_spawner_spec() {
        let callable = make_python_callable(
            "seen_pems = []\n\
             seen_endpoints = []\n\
             def cb(primary_url, secondary_id, quic_port, **kwargs):\n    \
                 seen_pems.append(kwargs['primary_pubkey_pem'])\n    \
                 seen_endpoints.append(primary_url)\n    \
                 return None\n",
            "cb",
        );
        let module_handle = Python::attach(|py| {
            callable.bind(py).getattr("__globals__").unwrap().unbind()
        });

        let spawner = PyMultiProcessSpawner::new(callable);

        let pem_a = "-----BEGIN PUBLIC KEY-----\nAAA\n-----END PUBLIC KEY-----\n";
        let pem_b = "-----BEGIN PUBLIC KEY-----\nBBB\n-----END PUBLIC KEY-----\n";
        let endpoint_a = "127.0.0.1:5555".to_owned();
        let endpoint_b = "127.0.0.1:6666".to_owned();

        run_local(async {
            let arc = spawner.as_arc();
            arc.spawn(SecondarySpawnSpec {
                new_secondary_id: "sec-a".into(),
                primary_endpoint: endpoint_a.clone(),
                primary_pubkey_pem: pem_a.to_owned(),
            })
            .await
            .unwrap();
            arc.spawn(SecondarySpawnSpec {
                new_secondary_id: "sec-b".into(),
                primary_endpoint: endpoint_b.clone(),
                primary_pubkey_pem: pem_b.to_owned(),
            })
            .await
            .unwrap();
        });

        Python::attach(|py| {
            let globals = module_handle.bind(py);
            let seen_pems_any = globals.get_item("seen_pems").unwrap();
            let seen_pems = seen_pems_any.cast::<PyList>().unwrap();
            let seen_endpoints_any = globals.get_item("seen_endpoints").unwrap();
            let seen_endpoints = seen_endpoints_any.cast::<PyList>().unwrap();
            assert_eq!(seen_pems.len(), 2);
            assert_eq!(seen_endpoints.len(), 2);
            let pem0: String = seen_pems.get_item(0).unwrap().extract().unwrap();
            let pem1: String = seen_pems.get_item(1).unwrap().extract().unwrap();
            let ep0: String = seen_endpoints.get_item(0).unwrap().extract().unwrap();
            let ep1: String = seen_endpoints.get_item(1).unwrap().extract().unwrap();
            assert_eq!(
                pem0, pem_a,
                "first spawn's spec.primary_pubkey_pem must reach the callback",
            );
            assert_eq!(
                pem1, pem_b,
                "second spawn's spec.primary_pubkey_pem must reach the callback (per-spawn read, NOT cached)",
            );
            assert_eq!(
                ep0, endpoint_a,
                "first spawn's spec.primary_endpoint must reach the callback as primary_url",
            );
            assert_eq!(
                ep1, endpoint_b,
                "second spawn's spec.primary_endpoint must reach the callback as primary_url",
            );
        });
    }

    /// Happy path for the SubprocessSpec branch: the callback returns
    /// a duck-typed object with `argv` (`["<resolved-sleep>", "30"]`)
    /// and `env=None`. The adapter must:
    ///
    ///   (a) call `Command::spawn` to launch the subprocess (PID
    ///       becomes observable);
    ///   (b) push the resulting `Child` onto `tracked_children`
    ///       BEFORE returning Ok from `spawn()` (so a JoinSet abort
    ///       racing the Ok-return cannot orphan the subprocess);
    ///   (c) reap that Child on `Drop` of the spawner via the
    ///       `subprocess_factory::terminate_children` ladder (the
    ///       process is gone within a bounded window after we drop
    ///       the last `Arc<MultiProcessSpawnerInner>` reference).
    ///
    /// `sleep` is the classic "stays-around long enough to be
    /// observed, no special signal handling" cleanup target. The
    /// path is resolved via `PATH` rather than hard-coded to
    /// `/bin/sleep` because the nix devshell ships coreutils under
    /// `/nix/store/<hash>-coreutils/bin` and `/bin/sleep` does NOT
    /// exist there. Both `std::process::Command::new("sleep")` and
    /// the test's `kill(pid, 0)` poll talk only to the kernel by
    /// pid, so the resolved absolute path is irrelevant once
    /// `Command::spawn` returns.
    #[test]
    fn spawn_registers_subprocess_for_cleanup() {
        // Resolve `sleep` via PATH up front so the Python callback
        // can return an absolute path the way the production
        // `spawn_secondary` callback does. The test assumes the nix
        // devshell (or any POSIX environment running cargo test) has
        // `coreutils`' `sleep` on PATH.
        let sleep_path = std::env::split_paths(
            &std::env::var_os("PATH").expect("PATH must be set for the test runner"),
        )
        .map(|p| p.join("sleep"))
        .find(|p| p.is_file())
        .map(|p| p.to_string_lossy().into_owned())
        .expect("`sleep` must be on PATH for the cleanup test");

        let source = format!(
            "class _DuckSpec:\n    \
                 def __init__(self, argv):\n        \
                     self.argv = argv\n        \
                     self.env = None\n\
             def cb(primary_url, secondary_id, quic_port, **kwargs):\n    \
                 return _DuckSpec([{sleep_path:?}, '30'])\n",
        );
        let callable = make_python_callable(&source, "cb");
        let spawner = PyMultiProcessSpawner::new(callable);
        let inner = Arc::clone(&spawner.inner);

        run_local(async {
            spawner
                .as_arc()
                .spawn(spec("sec-respawned-1"))
                .await
                .expect("spawn must succeed when callback returns a SubprocessSpec");
        });

        // (a) + (b): a Child landed on `tracked_children` before
        // `spawn()` returned Ok.
        let pid = {
            let guard = inner
                .tracked_children
                .lock()
                .expect("tracked_children mutex unpoisoned");
            assert_eq!(
                guard.len(),
                1,
                "exactly one Child must be registered after one successful respawn",
            );
            guard[0]
                .as_ref()
                .expect("Child slot must be populated, not drained")
                .id()
        };

        // Sanity: the recorded pid is a real process before we drop
        // the spawner. ESRCH here means we registered a corpse and
        // the test is meaningless.
        use nix::sys::signal::kill;
        use nix::unistd::Pid;
        let nix_pid = Pid::from_raw(pid as i32);
        kill(nix_pid, None).expect("registered Child must be alive before Drop");

        // (c): drop the only handles to the spawner; `Drop` on the
        // inner must SIGTERM/SIGKILL the registered child. We wait
        // up to 10s for the kernel to report ESRCH — well above the
        // 5s SIGTERM grace + slack for the SIGKILL escalation.
        drop(spawner);
        drop(inner);
        assert!(
            wait_for_pid_gone(pid, std::time::Duration::from_secs(10)),
            "respawned subprocess (pid={pid}) must be reaped by spawner Drop",
        );
    }

    /// SLURM-style no-op contract: the callback returns `None`. The
    /// adapter must return Ok WITHOUT touching `tracked_children`
    /// (there is no Rust-owned Child for an externally-launched
    /// secondary). Regression pin against the easy-to-reintroduce
    /// "always push something" bug.
    #[test]
    fn spawn_none_return_is_clean_noop() {
        let callable = make_python_callable(
            "def cb(primary_url, secondary_id, quic_port, **kwargs):\n    \
                 return None\n",
            "cb",
        );
        let spawner = PyMultiProcessSpawner::new(callable);
        let inner = Arc::clone(&spawner.inner);

        run_local(async {
            spawner
                .as_arc()
                .spawn(spec("sec-noop-1"))
                .await
                .expect("None return is a successful no-op");
        });

        let guard = inner.tracked_children.lock().unwrap();
        assert!(
            guard.is_empty(),
            "None return must not register a Child; got {} entries",
            guard.len(),
        );
    }

    /// Callback raises a Python exception: `spawn()` must surface it
    /// as `SpawnError::Other` containing the original message, AND
    /// `tracked_children` must stay empty (the failure happens
    /// upstream of `Command::spawn`). Mirrors the existing
    /// `multi_process_spawner_translates_pyerr_to_spawn_error` test
    /// but adds the registry-state invariant introduced by the fix.
    #[test]
    fn spawn_pyerr_surfaces_as_spawn_error_and_leaves_registry_empty() {
        let callable = make_python_callable(
            "def cb(*args, **kwargs):\n    \
                 raise RuntimeError('mock spawn failure for registry pin')\n",
            "cb",
        );
        let spawner = PyMultiProcessSpawner::new(callable);
        let inner = Arc::clone(&spawner.inner);

        let outcome = run_local(async {
            spawner.as_arc().spawn(spec("sec-pyerr-1")).await
        });

        match outcome {
            Err(SpawnError::Other(msg)) => assert!(
                msg.contains("mock spawn failure for registry pin"),
                "stringified PyErr must preserve the Python message; got {msg}",
            ),
            other => panic!("expected SpawnError::Other from raised PyErr, got {other:?}"),
        }
        assert!(
            inner.tracked_children.lock().unwrap().is_empty(),
            "PyErr from callback must not register any Child",
        );
    }

    /// `Command::spawn` failure (executable does not exist) must
    /// surface as `SpawnError::Other` mentioning the spawn failure,
    /// AND leave `tracked_children` empty — registration only
    /// happens after a successful `Command::spawn` returns a live
    /// `Child`. Distinct from the PyErr path: the callback returned
    /// a valid `SubprocessSpec`; it is the OS-level launch that
    /// failed.
    #[test]
    fn spawn_command_failure_surfaces_as_spawn_error() {
        let callable = make_python_callable(
            "class _DuckSpec:\n    \
                 def __init__(self, argv):\n        \
                     self.argv = argv\n        \
                     self.env = None\n\
             def cb(primary_url, secondary_id, quic_port, **kwargs):\n    \
                 return _DuckSpec(['/no/such/binary/at/this/path'])\n",
            "cb",
        );
        let spawner = PyMultiProcessSpawner::new(callable);
        let inner = Arc::clone(&spawner.inner);

        let outcome = run_local(async {
            spawner.as_arc().spawn(spec("sec-badbin-1")).await
        });

        match outcome {
            Err(SpawnError::Other(msg)) => {
                assert!(
                    msg.contains("Command::spawn"),
                    "OS-level spawn failure must be tagged with the 'Command::spawn' prefix; \
                     got: {msg}",
                );
            }
            other => panic!("expected SpawnError::Other from Command::spawn failure, got {other:?}"),
        }
        assert!(
            inner.tracked_children.lock().unwrap().is_empty(),
            "failed Command::spawn must not register any Child",
        );
    }
}
