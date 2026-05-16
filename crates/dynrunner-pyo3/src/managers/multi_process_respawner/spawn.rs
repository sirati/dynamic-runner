//! `SecondarySpawner` impl for `MultiProcessSpawnerInner` — handles
//! the orphan-safe spawn-local task that invokes the Python callback,
//! launches the resulting subprocess, and registers it on the
//! cleanup registry.

use std::sync::Arc;

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawnSpec, SecondarySpawner, SpawnError,
};

use super::MultiProcessSpawnerInner;

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
