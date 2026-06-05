//! Background container-image build/transfer for the SLURM preparation
//! phase.
//!
//! Single concern: run `job_manager.build_and_transfer_images(...)` on a
//! dedicated OS thread so the gateway-independent setup work the
//! submitter does in parallel (gateway connect already done upstream,
//! directory prep, shutdown-manager + wrapper binary uploads) overlaps
//! the long pole of preparation — the local nix image build and its
//! layered transfer.
//!
//! ## Why an OS thread (not a tokio task)
//!
//! The build is a Python call: `build_and_transfer_images` is bridge
//! code that drives the still-Python `PodmanPackaging` (local `nix
//! build` subprocess) and the gateway transfer. Backgrounding it means
//! a thread that re-acquires the GIL for the glue and releases it for
//! the heavy steps (the `nix build` subprocess and the
//! `transfer_file`/`execute_command` gateway ops `py.detach()` inside
//! the Rust gateway binding). While the build thread sits in those
//! GIL-released windows, the foreground preparation thread makes
//! progress on its own Python calls (the binary uploads, dir prep).
//! That is the overlap.
//!
//! ## Concurrency safety
//!
//! Both threads touch the same Python `gateway` once `connect()` has
//! completed (it runs on the foreground before this build is ever
//! spawned). The gateway's data-plane methods (`create_directory`,
//! `transfer_file`, `execute_command`) take `&self` on the Rust side
//! and run over the already-established SSH ControlMaster, which is
//! purpose-built for concurrent multiplexed channels — so concurrent
//! data-plane I/O from the two threads is safe. The mutating
//! `connect()`/`disconnect()` (`&mut self`) never run concurrently with
//! this build: connect precedes the spawn, disconnect runs only after
//! the join (cleanup guard).
//!
//! ## Error propagation
//!
//! A build failure is captured as the `PyErr` and re-raised at
//! `join` — it is never swallowed. The caller awaits the handle before
//! the first consumer of the image metadata (the sbatch submit-loop),
//! so a failed build aborts preparation exactly where the synchronous
//! version did.

use dynrunner_core::IMPORTANT_TARGET;
use pyo3::prelude::*;

/// Handle to an in-flight background image build.
///
/// Construct with [`ImageBuild::spawn`]; retrieve the resulting
/// `PodmanImageMetadata` (or propagate the build error) with
/// [`ImageBuild::join`]. The join MUST happen before the metadata is
/// consumed downstream — dropping the handle without joining would
/// detach the build thread and silently discard its result/error.
pub(super) struct ImageBuild {
    handle: std::thread::JoinHandle<PyResult<Py<PyAny>>>,
}

impl ImageBuild {
    /// Spawn the build on a dedicated OS thread and return immediately.
    ///
    /// The thread re-acquires the GIL via `Python::attach`, fires the
    /// A2 build-start importance event, calls
    /// `job_manager.build_and_transfer_images(project_root)`, then fires
    /// the A3 image-ready importance event with the same
    /// `remote_path`/`uploaded` discriminator the synchronous path
    /// emitted. The `log.info` parity lines stay on this thread too so
    /// the full-log output order is preserved relative to the importance
    /// emits.
    ///
    /// `job_manager`, `project_root`, and `log` are `Py<PyAny>` so they
    /// cross the thread boundary (`Send`); each is re-`bind`ed under the
    /// thread's GIL token.
    pub(super) fn spawn(
        job_manager: Py<PyAny>,
        project_root: Py<PyAny>,
        log: Py<PyAny>,
    ) -> Self {
        let handle = std::thread::spawn(move || {
            Python::attach(|py| build_and_emit(py, &job_manager, &project_root, &log))
        });
        Self { handle }
    }

    /// Block until the build thread finishes and return its result.
    ///
    /// The GIL is RELEASED across the join: the build thread needs the
    /// GIL to finish its glue (build the metadata object, fire A3), so
    /// holding it here would deadlock — foreground parked in
    /// `JoinHandle::join` while the build thread waits for the GIL.
    ///
    /// Propagates the build's `PyErr` if it failed. A panic in the build
    /// thread surfaces as a `PyRuntimeError` rather than aborting the
    /// process (the synchronous path would have unwound through `?`; a
    /// thread panic must not be more lenient).
    pub(super) fn join(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let joined = py.detach(|| self.handle.join());
        match joined {
            Ok(result) => result,
            Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "image build thread panicked",
            )),
        }
    }
}

/// Run the build under the supplied GIL token and emit the A2/A3
/// importance events around it. Shared by the spawned thread; factored
/// out so the emit sites read as one linear sequence.
fn build_and_emit(
    py: Python<'_>,
    job_manager: &Py<PyAny>,
    project_root: &Py<PyAny>,
    log: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let job_manager = job_manager.bind(py);
    let project_root = project_root.bind(py);
    let log = log.bind(py);

    // A2 build-start milestone (LLM-wake): occurrence point is the start
    // of the container image build+transfer. Direct importance emit so
    // the dual-sink surfaces it on stdio under `--important-stdio-only`.
    tracing::info!(
        target: IMPORTANT_TARGET,
        "Building and transferring container image...",
    );

    let metadata = job_manager.call_method1("build_and_transfer_images", (project_root,))?;
    let uploaded: bool = metadata.getattr("uploaded")?.extract().unwrap_or(false);
    let remote_path = metadata.getattr("remote_path")?;

    // A3 image-ready milestone (LLM-wake): occurrence point is the
    // image-transfer result. `uploaded` discriminates an actual upload
    // (cache miss) from a reused remote artifact (cache hit); both are
    // the "image is now on the gateway" milestone. Same importance target.
    tracing::info!(
        target: IMPORTANT_TARGET,
        remote_path = %remote_path,
        uploaded,
        "container image ready on gateway",
    );
    let image_hash: String = metadata
        .getattr("image_hash")
        .and_then(|v| v.extract())
        .unwrap_or_default();
    log.call_method1(
        "info",
        (format!(
            "Image {} at: {}",
            if uploaded { "uploaded" } else { "reused" },
            remote_path
        ),),
    )?;
    log.call_method1("info", (format!("Image hash: {image_hash}"),))?;
    Ok(metadata.unbind())
}

#[cfg(all(test, feature = "test-with-python"))]
mod tests;
