//! Python adapter for [`dynrunner_slurm::preparation`].
//!
//! Owned concern: bridge the Rust SSH-reverse-tunnel watcher state
//! machine to Python — start the cohort's INCREMENTAL tunnel
//! establishment on a background driver thread (so the pipeline
//! proceeds to the welcome-accepting primary while tunnels are still
//! materializing), and tear everything down on `cleanup`. Everything
//! above (image build, job submit, run-id bookkeeping) stays in the
//! pipeline orchestrator because it composes other higher-level
//! objects. Single concern at the bridge: own the driver thread's
//! runtime + cancellation; the establishment semantics live in
//! [`dynrunner_slurm::preparation::SlurmPreparation::run_tunnel_cohort`].
//!
//! The InfoFileReader bridge calls back into the Python gateway's
//! `execute_command(f"cat {path}")` — single source of truth for
//! the gateway connection lives on the Python side; the Rust
//! preparation crate stays gateway-impl-agnostic by accepting a
//! reader closure. GIL is re-acquired only for the cat call (small
//! window, infrequent during the 2s poll cadence).

use std::future::Future;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use dynrunner_gateway::shell::shell_quote;
use dynrunner_slurm::preparation::{
    EstablishmentPolicy, InfoFileReader, PrepError, PreparationOptions, SlurmPreparation,
};

/// Bridge that calls back into a Python gateway's
/// `execute_command(f"cat {path}")` to read connection-info files.
///
/// `Clone` (required by `InfoFileReader`) is implemented via
/// `Py::clone_ref` under a re-acquired GIL — pyo3 doesn't expose
/// `Clone` for `Py<T>` without the `py-clone` feature, so this is
/// the explicit-GIL path. Each watcher gets its own clone at
/// spawn time.
pub(crate) struct PyGatewayReader {
    gateway: Py<PyAny>,
}

impl PyGatewayReader {
    pub(crate) fn new(gateway: Py<PyAny>) -> Self {
        Self { gateway }
    }
}

impl Clone for PyGatewayReader {
    fn clone(&self) -> Self {
        // `Python::attach` acquires the GIL momentarily for the
        // refcount bump — pyo3 lacks a non-GIL Clone for Py<T>
        // unless `py-clone` is enabled.
        Python::attach(|py| Self {
            gateway: self.gateway.clone_ref(py),
        })
    }
}

impl InfoFileReader for PyGatewayReader {
    fn read(
        &self,
        path: String,
    ) -> impl Future<Output = Result<Option<String>, PrepError>> + 'static {
        let gateway = Python::attach(|py| self.gateway.clone_ref(py));
        async move {
            // The Python gateway's `execute_command` is sync and may
            // shell out to ssh, which can block briefly on the
            // master connection. Run it on a tokio blocking thread
            // so the watcher's spawn_local task doesn't stall the
            // current_thread runtime — other watchers continue
            // polling in parallel. The closure re-acquires the GIL
            // inside the blocking thread.
            let res = tokio::task::spawn_blocking(move || {
                Python::attach(|py| -> PyResult<(i32, String)> {
                    let bound = gateway.bind(py);
                    let cmd = format!("cat {}", shell_quote(&path));
                    // execute_command(cmd) → (rc, stdout, stderr)
                    let result = bound.call_method1("execute_command", (cmd,))?;
                    let rc: i32 = result.get_item(0)?.extract()?;
                    let stdout: String = result.get_item(1)?.extract()?;
                    Ok((rc, stdout))
                })
            })
            .await
            .map_err(|e| PrepError::WatcherPanic(format!("execute_command join failed: {e}")))?
            .map_err(|e| PrepError::WatcherLost(format!("execute_command raised: {e}")))?;

            // Match Python: `returncode == 0 and stdout.strip()` →
            // we have content; otherwise still polling.
            let (rc, stdout) = res;
            if rc == 0 && !stdout.trim().is_empty() {
                Ok(Some(stdout))
            } else {
                Ok(None)
            }
        }
    }
}

/// Python-facing tunnel-lifecycle manager. The pipeline orchestrator
/// (`run_preparation`) instantiates one per-run, calls
/// [`Self::start_ssh_tunnels`] (background, non-blocking) and hands
/// the instance to the `CleanupGuard`, whose teardown calls
/// [`Self::cleanup`].
///
/// Send-safe: all fields are `Send` (`SlurmPreparation` holds only
/// owned plain data + `Arc<Mutex<...>>`; `Py<PyAny>` is unconditionally
/// `Send` per pyo3; the driver handle is plain thread bookkeeping).
/// Cross-thread use is intentional — the cohort driver runs on its own
/// OS thread.
#[pyclass(name = "RustSlurmPreparation")]
pub(crate) struct PySlurmPreparation {
    /// Held as `Arc<SlurmPreparation>` so respawn callers (the SLURM
    /// `SecondarySpawner` adapter built in `slurm/pipeline.rs`) can
    /// share the SAME preparation instance the initial-cohort
    /// `setup_ssh_tunnels` loop is using. This keeps the per-tunnel
    /// `ssh_tunnels` cleanup set and the `establish_pool` rate-limiter
    /// pool unified across both code paths — without an Arc the
    /// respawn spawner would need a fresh `SlurmPreparation` and its
    /// tunnels would never join the operator's `scancel`-on-shutdown
    /// path. `setup_ssh_tunnels` / `cleanup` use `&self` (interior-
    /// mutable fields under `Arc<Mutex<...>>` / `StdMutex<...>`) so
    /// the Arc shape requires no other change at the lifecycle call
    /// sites.
    inner: std::sync::Arc<SlurmPreparation>,
    gateway: Py<PyAny>,
    /// The background cohort-establishment driver started by
    /// [`Self::start_ssh_tunnels`]: its OS-thread handle plus the
    /// cancellation signal [`Self::cleanup`] fires before joining.
    /// `None` until started (and again after cleanup took it). Plain
    /// `StdMutex` — held only for the synchronous take/replace, never
    /// across an await.
    cohort_driver: std::sync::Mutex<Option<CohortDriver>>,
}

/// Handle to the in-flight background tunnel-cohort driver thread.
///
/// The thread owns a dedicated current-thread tokio runtime + LocalSet
/// driving `SlurmPreparation::run_tunnel_cohort`, and it lives until
/// `cancel` fires — NOT merely until the cohort completes. The park is
/// load-bearing: every `ssh -N -R` child the cohort spawns is
/// PDEATHSIG-linked to THIS thread (`dynrunner_slurm::child_reaping`
/// arms `PR_SET_PDEATHSIG`, and the kernel delivers the death signal
/// when the SPAWNING THREAD exits, not the process), so a thread that
/// exited at cohort completion would have the kernel SIGTERM every
/// just-verified tunnel — run_20260612_084041: all 4 verified
/// listeners died within the driver's exit window and 0/4 secondaries
/// ever welcomed. See [`drive_cohort_until_cancelled`].
///
/// Firing `cancel` drops the cohort future (JoinSet abort;
/// `kill_on_drop` reaps un-committed ssh children; committed ones stay
/// in the shared stores for the subsequent `SlurmPreparation::cleanup`
/// drain, which runs after the join — the thread-exit PDEATHSIG and
/// the drain's SIGTERM ladder are the same orderly teardown signal).
struct CohortDriver {
    thread: std::thread::JoinHandle<()>,
    cancel: std::sync::Arc<tokio::sync::Notify>,
}

/// Body of the cohort-driver thread: drive `cohort` to completion on a
/// dedicated current-thread runtime + LocalSet, then PARK until
/// `cancel` fires.
///
/// The park (rather than returning when the cohort completes) is the
/// PDEATHSIG-lifetime contract: the tunnel children spawned while
/// polling `cohort` are death-linked to the calling thread, so this
/// function returning is the kernel's cue to SIGTERM all of them. It
/// must therefore only return once the caller has decided the tunnels'
/// lifetime is over (`cleanup()` fires `cancel` and then drains the
/// children explicitly).
///
/// Free function (rather than inlined in the `thread::spawn` closure)
/// so the park contract is testable without ssh: the regression test
/// drives an instantly-completing cohort and asserts the thread is
/// still parked afterwards.
fn drive_cohort_until_cancelled<F>(cohort: F, cancel: std::sync::Arc<tokio::sync::Notify>)
where
    F: std::future::Future<Output = ()>,
{
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(
                error = %e,
                "tunnel-cohort driver: tokio runtime construction \
                 failed; NO reverse tunnels will be established"
            );
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async {
        // Cohort, then park: completion of the cohort must NOT end
        // this thread (PDEATHSIG — see the function doc), so the arm
        // pends forever once the cohort is done and only `cancel`
        // resolves the select.
        let parked_cohort = async {
            cohort.await;
            std::future::pending::<()>().await
        };
        tokio::select! {
            _ = parked_cohort => {
                unreachable!("the parked cohort future never completes")
            }
            _ = cancel.notified() => {
                tracing::info!(
                    "tunnel-cohort driver cancelled (cleanup); \
                     in-flight watchers aborted"
                );
            }
        }
    }));
    // Bound the runtime teardown: an in-flight blocking gateway poll
    // (`execute_command` against a wedged gateway) must not park
    // cleanup forever.
    rt.shutdown_timeout(Duration::from_secs(10));
}

#[pymethods]
impl PySlurmPreparation {
    /// Construct from the Python-side options.
    ///
    /// `gateway` must be the gateway object whose
    /// `execute_command(cmd) -> (rc, stdout, stderr)` is called for
    /// info-file polling. `gateway_host`, `gateway_user`,
    /// `gateway_port`, `auth_options` mirror the gateway's `host`,
    /// `user`, `port`, and `auth_options()` — passed in from the
    /// caller so the Rust core doesn't reach into Python attributes.
    #[new]
    #[pyo3(signature = (
        gateway,
        run_log_dir,
        gateway_host,
        gateway_port,
        auth_options,
        extra_port_forwards,
        gateway_user = None,
        setup_timeout_secs = 600.0,
        poll_interval_secs = 2.0,
        establishment_max_concurrent = None,
        establishment_attempts = None,
        establishment_backoff_secs = None,
        establishment_per_tunnel_timeout_secs = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        gateway: Py<PyAny>,
        run_log_dir: String,
        gateway_host: String,
        gateway_port: u16,
        auth_options: Vec<String>,
        extra_port_forwards: Vec<(u16, u16)>,
        gateway_user: Option<String>,
        setup_timeout_secs: f64,
        poll_interval_secs: f64,
        establishment_max_concurrent: Option<usize>,
        establishment_attempts: Option<usize>,
        establishment_backoff_secs: Option<Vec<f64>>,
        establishment_per_tunnel_timeout_secs: Option<f64>,
    ) -> PyResult<Self> {
        let mut opts = PreparationOptions::new(
            run_log_dir,
            gateway_host,
            gateway_user,
            gateway_port,
            auth_options,
            extra_port_forwards,
        );
        opts.setup_timeout = Duration::from_secs_f64(setup_timeout_secs);
        opts.poll_interval = Duration::from_secs_f64(poll_interval_secs);
        // Establishment-policy overrides. `None` for any field keeps
        // the Rust-side default — operator-friendly: callers that
        // don't care pass nothing and get the safe 4-concurrent /
        // 3-attempt / 5+15s / 90s defaults.
        let mut est = EstablishmentPolicy::default();
        if let Some(n) = establishment_max_concurrent {
            est.max_concurrent = n;
        }
        if let Some(n) = establishment_attempts {
            est.attempts = n;
        }
        if let Some(backoff) = establishment_backoff_secs {
            est.backoff = backoff.into_iter().map(Duration::from_secs_f64).collect();
        }
        if let Some(t) = establishment_per_tunnel_timeout_secs {
            est.per_tunnel_timeout = Duration::from_secs_f64(t);
        }
        opts.establishment = est;
        // Diagnostic knob (#415 face (a)): `DYNRUNNER_SSH_TUNNEL_LOGLEVEL`
        // (e.g. `DEBUG1`) raises the per-secondary `-R` tunnel CHILD's
        // OpenSSH LogLevel so a fleet-wide-drop investigation can see the
        // rekey / channel-forwarding / mux lines the default banner-only
        // child stderr hides. Read ONCE here (the single env→config seam)
        // so the argv builder stays pure. Empty/unset keeps the default.
        opts.tunnel_child_log_level = std::env::var("DYNRUNNER_SSH_TUNNEL_LOGLEVEL")
            .ok()
            .filter(|v| !v.trim().is_empty());
        Ok(Self {
            inner: std::sync::Arc::new(SlurmPreparation::new(opts)),
            gateway,
            cohort_driver: std::sync::Mutex::new(None),
        })
    }

    /// Start the cohort's reverse-tunnel establishment in the
    /// BACKGROUND and return immediately — the bring-up shape that
    /// keeps the pipeline moving to the welcome-accepting primary
    /// while tunnels materialize per-member (see
    /// [`SlurmPreparation::run_tunnel_cohort`] for the incremental /
    /// late-join semantics; one PENDING SLURM job must never hold the
    /// primary's bind — and with it all welcome service — hostage).
    ///
    /// The driver is a dedicated OS thread owning a current-thread
    /// tokio runtime + LocalSet (the watchers use `spawn_local`); the
    /// gateway reader re-acquires the GIL only for its short
    /// `execute_command` polls on a blocking thread. The thread PARKS
    /// after the cohort completes and lives until [`Self::cleanup`]
    /// cancels + joins it — the tunnel children are PDEATHSIG-linked
    /// to it, so its exit is the kernel's cue to SIGTERM every tunnel
    /// (see [`CohortDriver`] / [`drive_cohort_until_cancelled`]).
    /// Cleanup then drains the children explicitly.
    ///
    /// Raises `RuntimeError` on a double start — one cohort per
    /// preparation instance.
    pub(crate) fn start_ssh_tunnels(
        &self,
        py: Python<'_>,
        num_secondaries: usize,
        primary_quic_port: u16,
    ) -> PyResult<()> {
        let mut slot = self
            .cohort_driver
            .lock()
            .expect("cohort_driver mutex poisoned");
        if slot.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "ssh tunnel setup already started for this preparation instance",
            ));
        }

        let reader = PyGatewayReader::new(self.gateway.clone_ref(py));
        // `SlurmPreparation::run_tunnel_cohort` is `&self` (every
        // mutating field is interior-mutable under
        // `Arc<Mutex<...>>` / `StdMutex<...>`), so the Arc clone
        // gives the driver thread a `'static` handle to the SAME
        // instance the respawn / reconnect / cleanup paths share.
        let inner = std::sync::Arc::clone(&self.inner);
        let cancel = std::sync::Arc::new(tokio::sync::Notify::new());
        let cancel_bg = std::sync::Arc::clone(&cancel);

        let thread = std::thread::Builder::new()
            .name("dynrunner-tunnel-cohort".into())
            .spawn(move || {
                drive_cohort_until_cancelled(
                    async move {
                        inner
                            .run_tunnel_cohort(reader, num_secondaries, primary_quic_port)
                            .await;
                    },
                    cancel_bg,
                );
            })
            .map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!("spawn tunnel-cohort thread: {e}"))
            })?;

        *slot = Some(CohortDriver { thread, cancel });
        Ok(())
    }

    /// Drain all tracked tunnel subprocesses (SIGTERM → 5s wait →
    /// SIGKILL escalation). Idempotent.
    ///
    /// Stops the background cohort driver FIRST (cancel + join) so no
    /// watcher can commit a fresh child into the stores while — or
    /// after — they are drained.
    pub(crate) fn cleanup(&self, py: Python<'_>) -> PyResult<()> {
        let driver = self
            .cohort_driver
            .lock()
            .expect("cohort_driver mutex poisoned")
            .take();
        if let Some(driver) = driver {
            driver.cancel.notify_one();
            // GIL released across the join: the driver's gateway reader
            // re-acquires the GIL on a blocking thread to finish its
            // in-flight poll — joining while holding it would deadlock.
            py.detach(|| {
                if driver.thread.join().is_err() {
                    tracing::warn!("tunnel-cohort driver thread panicked before cleanup");
                }
            });
        }

        let inner = std::sync::Arc::clone(&self.inner);
        py.detach(|| -> PyResult<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| pyo3::exceptions::PyOSError::new_err(format!("tokio runtime: {e}")))?;
            rt.block_on(async {
                inner.cleanup().await;
            });
            Ok(())
        })
    }

    /// Read-only view of the `secondary_id -> tunnel_port` map. Useful
    /// for the Python caller to pass into downstream phases (e.g.
    /// pipeline orchestration).
    fn secondary_port_map(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in self.inner.secondary_port_map().iter() {
            dict.set_item(k, v)?;
        }
        Ok(dict.into())
    }
}

impl PySlurmPreparation {
    /// Rust-only hand-off: clone the inner `Arc<SlurmPreparation>` so
    /// the respawn wiring (in `slurm/pipeline.rs`) can construct a
    /// `SlurmPreparationTunnelEstablisher` over the SAME preparation
    /// instance the initial-cohort tunnel setup is using. Without
    /// this hand-off, a respawn's `establish_one_tunnel` would not
    /// share the `ssh_tunnels` cleanup set or the `establish_pool`
    /// rate-limiter with the initial cohort.
    pub(crate) fn arc_handle(&self) -> std::sync::Arc<SlurmPreparation> {
        std::sync::Arc::clone(&self.inner)
    }

    /// Rust-only access to the gateway `Py<PyAny>` the preparation
    /// was constructed with. The respawn wiring needs it to build a
    /// fresh `PyGatewayReader` for the tunnel-establisher's info-file
    /// polling — same gateway the initial cohort uses, so per-respawn
    /// info files land at the same path the operator's logs already
    /// show.
    pub(crate) fn gateway_handle(&self, py: Python<'_>) -> Py<PyAny> {
        self.gateway.clone_ref(py)
    }
}

#[cfg(test)]
mod tests {
    //! Driver-thread lifetime pin for the PDEATHSIG-linked tunnel
    //! children (run_20260612_084041): the cohort driver thread must
    //! PARK after the cohort completes and exit only on cancel.
    //!
    //! Replays the production sequence that killed the fleet: the
    //! cohort finished fast (all 4 tunnels verified within seconds),
    //! the driver thread exited, and the kernel's parent-death signal
    //! — armed per SPAWNING THREAD by
    //! `dynrunner_slurm::child_reaping`, not per process — SIGTERMed
    //! every just-verified `ssh -N -R` child, erasing the worker-side
    //! listeners before any secondary dialed. The pdeathsig→child-kill
    //! half is pinned in `child_reaping`'s own test; this test pins
    //! the OTHER half of the contract: the thread that spawned the
    //! children stays alive until cleanup cancels it.

    use std::sync::Arc;
    use std::time::Duration;

    use super::drive_cohort_until_cancelled;

    /// RED on the regression (driver returned at cohort completion):
    /// the thread must still be parked well after an
    /// instantly-completing cohort, and must exit promptly on cancel.
    #[test]
    fn cohort_driver_parks_after_cohort_completion_until_cancelled() {
        let cancel = Arc::new(tokio::sync::Notify::new());
        let cancel_for_thread = Arc::clone(&cancel);
        let (cohort_done_tx, cohort_done_rx) = std::sync::mpsc::channel::<()>();

        let thread = std::thread::Builder::new()
            .name("test-tunnel-cohort".into())
            .spawn(move || {
                drive_cohort_until_cancelled(
                    async move {
                        let _ = cohort_done_tx.send(());
                    },
                    cancel_for_thread,
                );
            })
            .expect("spawn driver thread");

        // The cohort itself completed...
        cohort_done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("cohort future never ran");

        // ...but the driver thread must STAY parked: its exit is the
        // kernel's cue to SIGTERM every PDEATHSIG-linked tunnel child.
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            !thread.is_finished(),
            "cohort driver thread exited at cohort completion — the kernel \
             would deliver PDEATHSIG to every established ssh tunnel child \
             (run_20260612_084041: 0/4 SecondaryWelcome)"
        );

        // Cancel = cleanup's signal that the tunnels' lifetime is over;
        // only now may the thread exit.
        cancel.notify_one();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !thread.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "cohort driver thread did not exit after cancel"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        thread.join().expect("driver thread panicked");
    }
}
