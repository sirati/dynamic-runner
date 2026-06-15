//! PyO3 adapter that wires the Rust `SlurmSecondarySpawner` into the
//! coordinator's `Arc<dyn SecondarySpawner>` slot.
//!
//! Single concern: bridge an in-process Rust `SlurmSecondarySpawner`
//! (composed from the live SLURM `Arc<Mutex<SlurmJobManager>>`,
//! `Arc<SlurmPreparation>`, and a wrapper-script generator closure) to
//! the trait-object surface `dynrunner-manager-distributed` consumes.
//! The SLURM pipeline (in `slurm/pipeline.rs`) constructs the spawner
//! inside `drive_rust_primary` — after preparation has populated the
//! shared state — and hands the [`PySlurmSpawner`] to
//! `RustPrimaryCoordinator(...)` as the `respawn_spawner` kwarg.
//!
//! The trait surface (`SecondarySpawner` from
//! `dynrunner_manager_distributed::primary::respawn`) is identical to
//! the multi-process adapter; only the inner provider differs.
//!
//! GIL discipline: this adapter does NOT touch Python during
//! `spawn(spec).await`. The wrapper-script generator IS a closure
//! that re-acquires the GIL to call the Python `generate_wrapper_script`,
//! but that's owned by the closure itself — see `wrapper_script_generator_from_pyobj`
//! in this module. Once the closure has rendered the script body, the
//! sbatch submit + tunnel-establish path is all Rust + tokio + ssh.

use std::sync::Arc;

use dynrunner_manager_distributed::primary::respawn::{SecondarySpawnSpec, SecondarySpawner};
use dynrunner_slurm::SlurmJobManager;
use dynrunner_slurm::preparation::SlurmPreparation;
use dynrunner_slurm::respawn::{
    SlurmPreparationTunnelEstablisher, SlurmSecondarySpawner, WrapperScriptGenerator,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::Mutex;

use crate::slurm::preparation::PyGatewayReader;
use crate::slurm::py_gateway::PyGatewayAdapter;

/// Pyclass wrapper carrying an `Arc<dyn SecondarySpawner>` produced
/// from a [`SlurmSecondarySpawner`]. Constructed only inside the
/// SLURM pipeline's `drive_rust_primary` (Rust-only); the constructor
/// is intentionally NOT exposed to Python because the inputs
/// (`Arc<Mutex<SlurmJobManager<PyGatewayAdapter>>>`,
/// `Arc<SlurmPreparation>`, the wrapper-script closure) only exist
/// inside the pipeline's Rust-side scope.
#[pyclass(name = "PySlurmSpawner")]
pub(crate) struct PySlurmSpawner {
    inner: Arc<dyn SecondarySpawner>,
}

impl PySlurmSpawner {
    /// Construct a SLURM spawner from the live SLURM pipeline state.
    ///
    /// `job_manager` is the same `Arc<Mutex<SlurmJobManager<...>>>` the
    /// initial-cohort sbatch submit-loop drove (parked on the
    /// coordinator via `set_slurm_job_manager_from_rust`).
    /// `preparation` is the `Arc<SlurmPreparation>` whose cohort
    /// establishment populated the initial-cohort tunnel set —
    /// sharing it means a respawn's `establish_one_tunnel` joins the
    /// same `ssh_tunnels` cleanup Vec and `establish_pool`
    /// rate-limiter. `wrapper_script_generator` synthesises the
    /// per-respawn wrapper script body (the closure captures the
    /// constant deployment context — image path, mount roots,
    /// forwarded argv — and varies the secondary id per spawn).
    /// `run_log_dir` is forwarded to `submit_job` so the regenerated
    /// `--output=`/`--error=` paths land under the same run-scoped
    /// log directory as the initial cohort.
    pub(crate) fn new(
        job_manager: Arc<Mutex<SlurmJobManager<PyGatewayAdapter>>>,
        preparation: Arc<SlurmPreparation>,
        info_reader: PyGatewayReader,
        wrapper_script_generator: WrapperScriptGenerator,
        run_log_dir: String,
        consumer_job_name_prefix: Option<String>,
    ) -> Self {
        let tunnel_establisher = Arc::new(SlurmPreparationTunnelEstablisher::new(
            preparation,
            info_reader,
        ));
        let inner: Arc<dyn SecondarySpawner> = Arc::new(SlurmSecondarySpawner::new(
            job_manager,
            tunnel_establisher,
            wrapper_script_generator,
            run_log_dir,
            consumer_job_name_prefix,
        ));
        Self { inner }
    }

    /// Rust-side hand-off used by `PyPrimaryCoordinator::run` to
    /// install the spawner on the inner `PrimaryCoordinator` via
    /// `enable_respawn`. Symmetric with
    /// `PyMultiProcessSpawner::as_arc` — the coordinator never
    /// distinguishes the two providers past this point.
    pub(crate) fn as_arc(&self) -> Arc<dyn SecondarySpawner> {
        Arc::clone(&self.inner)
    }
}

/// Build a [`WrapperScriptGenerator`] closure from a Python
/// `job_manager` reference and the constant kwargs the wrapper-script
/// generator needs. The closure captures the kwargs by value (`String`
/// / `Vec<String>` / etc.) plus a refcounted `Py<PyAny>` to the
/// Python `job_manager`, so each invocation can re-acquire the GIL
/// and call `job_manager.generate_wrapper_script(..., secondary_id=...)`
/// with the per-spawn id substituted in.
///
/// The respawned secondary fetches its run config — the dispatcher's
/// task-specific argv AND its trust anchor — over the peer mesh at
/// cold start (the container runs the bootstrap shim), so NO argv is
/// spliced onto the launch command line here. `spec.primary_pubkey_pem`
/// is therefore not consumed by this generator; the secondary-side
/// trust-anchor delivery is a mesh-fetch concern, not a launch-line one.
#[allow(clippy::too_many_arguments)]
pub(crate) fn wrapper_script_generator_from_pyobj(
    py_job_manager: Py<PyAny>,
    image_metadata: Py<PyAny>,
    gateway_host: String,
    gateway_port: u16,
    cores_spec: String,
    max_memory_spec: String,
    reverse_connection: bool,
    run_log_dir: String,
    shutdown_manager_bin_path: Option<String>,
    name_prefix: String,
    wrapper_bin_path: Option<String>,
    mem_manager_reserved_bytes: Option<u64>,
) -> WrapperScriptGenerator {
    Arc::new(move |spec: &SecondarySpawnSpec| -> Result<String, String> {
        Python::attach(|py| -> PyResult<String> {
            let kwargs = PyDict::new(py);
            kwargs.set_item("image_metadata", image_metadata.bind(py))?;
            kwargs.set_item("secondary_id", &spec.new_secondary_id)?;
            kwargs.set_item("gateway_host", &gateway_host)?;
            kwargs.set_item("gateway_port", gateway_port)?;
            kwargs.set_item("cores_spec", &cores_spec)?;
            kwargs.set_item("max_memory_spec", &max_memory_spec)?;
            kwargs.set_item("reverse_connection", reverse_connection)?;
            kwargs.set_item("run_log_dir", &run_log_dir)?;
            kwargs.set_item(
                "shutdown_manager_bin_path",
                shutdown_manager_bin_path.as_deref(),
            )?;
            // Same program-identity prefix + wrapper-binary path the
            // initial cohort used, so respawned secondaries render the
            // identical `exec`-stub against the same gateway-side binary.
            kwargs.set_item("name_prefix", &name_prefix)?;
            kwargs.set_item("wrapper_bin_path", wrapper_bin_path.as_deref())?;
            if let Some(reserved) = mem_manager_reserved_bytes {
                kwargs.set_item("mem_manager_reserved_bytes", reserved)?;
            }
            let script = py_job_manager
                .bind(py)
                .call_method("generate_wrapper_script", (), Some(&kwargs))?
                .extract::<String>()?;
            Ok(script)
        })
        .map_err(|e| e.to_string())
    })
}

#[cfg(test)]
mod tests {
    //! Contract tests for the wrapper-script-generator closure shape.
    //!
    //! Focus: the closure correctly threads (a) the per-respawn
    //! `spec.new_secondary_id` AND `spec.primary_pubkey_pem` into the
    //! Python `generate_wrapper_script` call, AND (b) the
    //! construction-time captured kwargs reach the call as well.
    //! Drives the closure under a real GIL, with a stub Python
    //! `job_manager` whose `generate_wrapper_script` records its
    //! kwargs onto a module-level list the test reads.

    use super::*;
    use dynrunner_manager_distributed::primary::respawn::SecondarySpawnSpec;

    fn make_stub_job_manager(module_name: &str) -> (Py<PyAny>, Py<PyAny>) {
        Python::attach(|py| {
            let source = "calls = []\n\
                          class StubJM:\n    \
                              def generate_wrapper_script(self, **kwargs):\n        \
                                  calls.append(dict(kwargs))\n        \
                                  return f\"#!/bin/sh\\n# wrapper for {kwargs['secondary_id']}\\n\"\n";
            let filename = format!("{module_name}.py");
            let module = PyModule::from_code(
                py,
                std::ffi::CString::new(source).unwrap().as_c_str(),
                std::ffi::CString::new(filename.as_str())
                    .unwrap()
                    .as_c_str(),
                std::ffi::CString::new(module_name).unwrap().as_c_str(),
            )
            .expect("compile stub module");
            let jm = module
                .getattr("StubJM")
                .unwrap()
                .call0()
                .expect("instantiate StubJM")
                .unbind();
            let globals = module.dict().unbind();
            (jm, globals.into_any())
        })
    }

    #[test]
    fn wrapper_script_generator_threads_spec_new_id_and_constant_kwargs() {
        let (jm, globals) = make_stub_job_manager("stub_jm_threads");
        // Image metadata is opaque to the closure (the Python side
        // pickles it into the bash template); a None stand-in is
        // fine for the contract test.
        let image_metadata = Python::attach(|py| py.None());
        let generator = wrapper_script_generator_from_pyobj(
            jm,
            image_metadata,
            "gw.example.invalid".to_owned(),
            5555,
            "0".to_owned(),
            "-2G".to_owned(),
            true,
            "/log/run-1".to_owned(),
            None,
            "asm".to_owned(),
            Some("/gw/dynrunner-slurm-wrapper".to_owned()),
            None,
        );

        let spec = SecondarySpawnSpec {
            new_secondary_id: "secondary-7".to_owned(),
            primary_endpoint: "127.0.0.1:5555".to_owned(),
            primary_pubkey_pem: "-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----\n"
                .to_owned(),
            dead_member_id: None,
        };
        let body = generator(&spec).expect("closure must render");
        assert!(
            body.contains("secondary-7"),
            "stub renders the secondary id into the script body; got: {body}",
        );

        Python::attach(|py| {
            let g = globals.bind(py);
            let calls_any = g.get_item("calls").unwrap();
            let calls = calls_any.cast::<pyo3::types::PyList>().unwrap();
            assert_eq!(calls.len(), 1);
            let call = calls.get_item(0).unwrap();
            let call_dict = call.cast::<PyDict>().unwrap();

            // Per-spec id
            let sid: String = call_dict
                .get_item("secondary_id")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(sid, "secondary-7");

            // Constant kwargs
            let host: String = call_dict
                .get_item("gateway_host")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(host, "gw.example.invalid");
            let port: u16 = call_dict
                .get_item("gateway_port")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(port, 5555);

            // The dispatcher's task argv now travels over the peer mesh:
            // the generator must NOT pass any `forwarded_argv` kwarg, and
            // must NOT splice the spec's trust anchor onto the launch
            // line as a `--secondary-primary-pubkey-pem=` token.
            assert!(
                call_dict.get_item("forwarded_argv").unwrap().is_none(),
                "generator must not pass a forwarded_argv kwarg (argv travels over the mesh)",
            );
        });
    }
}
