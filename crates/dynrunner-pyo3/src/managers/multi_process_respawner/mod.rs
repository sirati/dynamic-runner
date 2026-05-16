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

use pyo3::prelude::*;

use dynrunner_manager_distributed::primary::respawn::SecondarySpawner;

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
    pub(super) spawn_callable: Py<PyAny>,
    /// Registry of Rust-owned subprocess handles for every
    /// successfully-respawned secondary. Pushed under the lock by
    /// the inner `spawn_local` task before `spawn()` returns Ok;
    /// drained by `Drop` to issue the SIGTERM/SIGKILL ladder via
    /// [`crate::subprocess_factory::terminate_children`].
    pub(super) tracked_children: Arc<Mutex<Vec<Option<std::process::Child>>>>,
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

mod cleanup;
mod spawn;

#[cfg(test)]
mod tests;
