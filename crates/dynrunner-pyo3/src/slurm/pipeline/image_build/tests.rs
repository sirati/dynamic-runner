//! Python-interpreter-backed tests for the background image build.
//!
//! Single concern: pin the two contractual properties the
//! concurrency restructure must hold:
//!
//! 1. The build no longer serializes BEFORE the independent setup
//!    work — the foreground can make progress while the build is in
//!    flight. Encoded as a rendezvous: the fake build blocks on a
//!    `threading.Event` that the foreground only sets AFTER doing its
//!    "independent work". A serial build-before-independent-work
//!    implementation could never set the event, so the join would
//!    block past the timeout and the build would observe a timed-out
//!    wait. A concurrent implementation unblocks the build and the
//!    recorded event order shows independent work landing between the
//!    build's start and its completion.
//!
//! 2. A build failure still propagates — `join` re-raises the build's
//!    `PyErr` rather than swallowing it.
//!
//! Invoke as:
//!   `cargo test -p dynrunner-pyo3 --lib --no-default-features \
//!        --features test-with-python image_build`
//!
//! Tests require an embedded CPython interpreter (gated behind the
//! `test-with-python` feature).

use super::*;
use pyo3::types::{PyAnyMethods, PyList, PyModule};

/// Compile a tiny Python module exporting a `job_manager`-shaped stub
/// whose `build_and_transfer_images` rendezvouses with the foreground.
///
/// * `events` — a shared `list` the stub appends ordered milestones to
///   (`build_started`, `gate_released`, `build_done`). The foreground
///   appends `independent_work` to the same list, so the final order
///   reflects the actual interleaving.
/// * `gate` — a `threading.Event` the foreground sets after its
///   independent work; the build blocks on `gate.wait(timeout)` (which
///   releases the GIL, letting the foreground run).
///
/// The stub returns a `SimpleNamespace`-shaped metadata object carrying
/// the `remote_path` / `image_hash` / `uploaded` attributes the
/// build-emit reads. `time.sleep` is not used — the `Event` makes the
/// rendezvous deterministic, not timing-dependent.
fn build_concurrent_stub(py: Python<'_>) -> Bound<'_, PyModule> {
    let source = r#"
import threading
from types import SimpleNamespace

events = []
gate = threading.Event()

class _JobManager:
    def build_and_transfer_images(self, project_root):
        events.append("build_started")
        # GIL is released across this wait, so the foreground thread
        # can run its independent work and set the gate.
        released = gate.wait(timeout=10.0)
        events.append("gate_released" if released else "gate_timeout")
        events.append("build_done")
        return SimpleNamespace(
            remote_path="/srv/img/app.tar.gz",
            image_hash="deadbeef",
            uploaded=True,
        )

job_manager = _JobManager()
log = SimpleNamespace(info=lambda *a, **k: None)
"#;
    PyModule::from_code(
        py,
        std::ffi::CString::new(source).unwrap().as_c_str(),
        std::ffi::CString::new("stub_concurrent_build.py").unwrap().as_c_str(),
        std::ffi::CString::new("stub_concurrent_build").unwrap().as_c_str(),
    )
    .expect("compile concurrent-build stub module")
}

/// Compile a stub whose `build_and_transfer_images` raises, to pin the
/// error-propagation contract.
fn build_failing_stub(py: Python<'_>) -> Bound<'_, PyModule> {
    let source = r#"
from types import SimpleNamespace

class _JobManager:
    def build_and_transfer_images(self, project_root):
        raise RuntimeError("image build blew up")

job_manager = _JobManager()
log = SimpleNamespace(info=lambda *a, **k: None)
"#;
    PyModule::from_code(
        py,
        std::ffi::CString::new(source).unwrap().as_c_str(),
        std::ffi::CString::new("stub_failing_build.py").unwrap().as_c_str(),
        std::ffi::CString::new("stub_failing_build").unwrap().as_c_str(),
    )
    .expect("compile failing-build stub module")
}

#[test]
fn build_overlaps_independent_work_instead_of_serializing_before_it() {
    Python::attach(|py| {
        let module = build_concurrent_stub(py);
        let job_manager = module.getattr("job_manager").unwrap();
        let log = module.getattr("log").unwrap();
        let gate = module.getattr("gate").unwrap();
        let events: Bound<'_, PyList> =
            module.getattr("events").unwrap().cast_into().unwrap();
        let project_root = py
            .import("pathlib")
            .unwrap()
            .getattr("Path")
            .unwrap()
            .call0()
            .unwrap();

        // Spawn the background build. If the build serialised before
        // the foreground work, the spawn would do nothing observable
        // yet and the build would be parked on gate.wait(...).
        let build = ImageBuild::spawn(
            job_manager.clone().unbind(),
            project_root.unbind(),
            log.clone().unbind(),
        );

        // Foreground "independent work" runs WHILE the build is parked
        // on the gate. Releasing the GIL gives the spawned thread a
        // chance to enter build_and_transfer_images first; we then
        // record our own milestone and release the gate.
        py.detach(|| std::thread::sleep(std::time::Duration::from_millis(50)));
        events.append("independent_work").unwrap();
        gate.call_method0("set").unwrap();

        // Join yields the metadata; a serial-before-work implementation
        // would have timed the gate out (gate_timeout) — the assertions
        // below reject that.
        let metadata = build.join(py).expect("build must succeed");
        let metadata = metadata.bind(py);
        let remote_path: String = metadata
            .getattr("remote_path")
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(remote_path, "/srv/img/app.tar.gz");

        let recorded: Vec<String> = events.extract().unwrap();
        // The build started, THEN the foreground independent work was
        // recorded while the build was still parked (not after
        // build_done), THEN the gate released and the build finished.
        let started = recorded
            .iter()
            .position(|e| e == "build_started")
            .expect("build_started recorded");
        let independent = recorded
            .iter()
            .position(|e| e == "independent_work")
            .expect("independent_work recorded");
        let done = recorded
            .iter()
            .position(|e| e == "build_done")
            .expect("build_done recorded");
        assert!(
            started < independent,
            "build must already be in flight when independent work runs: {recorded:?}"
        );
        assert!(
            independent < done,
            "independent work must land BEFORE the build completes (overlap), \
             not be serialised after it: {recorded:?}"
        );
        assert!(
            recorded.iter().any(|e| e == "gate_released"),
            "the gate must have been released by the foreground, not timed out \
             (a serial build-before-work would time out): {recorded:?}"
        );
    });
}

#[test]
fn build_failure_propagates_through_join() {
    Python::attach(|py| {
        let module = build_failing_stub(py);
        let job_manager = module.getattr("job_manager").unwrap();
        let log = module.getattr("log").unwrap();
        let project_root = py
            .import("pathlib")
            .unwrap()
            .getattr("Path")
            .unwrap()
            .call0()
            .unwrap();

        let build = ImageBuild::spawn(
            job_manager.unbind(),
            project_root.unbind(),
            log.unbind(),
        );
        let err = build.join(py).expect_err("a failed build must propagate");
        let msg = err.to_string();
        assert!(
            msg.contains("image build blew up"),
            "the build's error must surface verbatim, not be swallowed: {msg}"
        );
    });
}
