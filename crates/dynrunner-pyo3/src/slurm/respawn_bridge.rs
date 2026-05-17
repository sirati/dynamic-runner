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

use dynrunner_manager_distributed::primary::respawn::{
    SecondarySpawner, SecondarySpawnSpec,
};
use dynrunner_slurm::respawn::{
    SlurmPreparationTunnelEstablisher, SlurmSecondarySpawner, WrapperScriptGenerator,
};
use dynrunner_slurm::SlurmJobManager;
use dynrunner_slurm::preparation::SlurmPreparation;
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
    /// `preparation` is the `Arc<SlurmPreparation>` whose
    /// `setup_ssh_tunnels` populated the initial-cohort tunnel set —
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
    ) -> Self {
        let tunnel_establisher = Arc::new(SlurmPreparationTunnelEstablisher::new(
            preparation,
            info_reader,
        ));
        let inner: Arc<dyn SecondarySpawner> =
            Arc::new(SlurmSecondarySpawner::new(
                job_manager,
                tunnel_establisher,
                wrapper_script_generator,
                run_log_dir,
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
/// `primary_pubkey_pem` from each spec is appended to `forwarded_argv`
/// as `--secondary-primary-pubkey-pem=<pem>` so the respawned
/// secondary's argparse sees the live trust anchor at startup.
/// Today the secondary's `--secondary-primary-pubkey-pem` parsing +
/// QUIC-handshake verification is a TODO (see the spawner brief);
/// the structural plumbing is in place so a follow-up that adds the
/// argparse + verification reads the value without further wire
/// changes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn wrapper_script_generator_from_pyobj(
    py_job_manager: Py<PyAny>,
    image_metadata: Py<PyAny>,
    gateway_host: String,
    gateway_port: u16,
    cores_spec: String,
    max_memory_spec: String,
    forwarded_argv: Vec<String>,
    reverse_connection: bool,
    run_log_dir: String,
    shutdown_manager_bin_path: Option<String>,
) -> WrapperScriptGenerator {
    Arc::new(move |spec: &SecondarySpawnSpec| -> Result<String, String> {
        // Append the primary's cert PEM to forwarded_argv so the
        // respawned secondary's argparse can pin it as the trust
        // anchor at handshake time. Per-spawn read: a future cert
        // rotation propagates without re-instantiating the spawner.
        // TODO: the secondary's argparse for
        // `--secondary-primary-pubkey-pem` and the corresponding
        // QUIC peer-cert validation are follow-on work (see the
        // SLURM-respawn fix brief). The wrapper-script side is wired
        // so the value reaches the secondary; the missing piece is
        // the secondary-side parse + verify.
        let mut argv = forwarded_argv.clone();
        if !spec.primary_pubkey_pem.is_empty() {
            argv.push(format!(
                "--secondary-primary-pubkey-pem={}",
                spec.primary_pubkey_pem,
            ));
        }
        Python::attach(|py| -> PyResult<String> {
            let kwargs = PyDict::new(py);
            kwargs.set_item("image_metadata", image_metadata.bind(py))?;
            kwargs.set_item("secondary_id", &spec.new_secondary_id)?;
            kwargs.set_item("gateway_host", &gateway_host)?;
            kwargs.set_item("gateway_port", gateway_port)?;
            kwargs.set_item("cores_spec", &cores_spec)?;
            kwargs.set_item("max_memory_spec", &max_memory_spec)?;
            kwargs.set_item("forwarded_argv", argv)?;
            kwargs.set_item("reverse_connection", reverse_connection)?;
            kwargs.set_item("run_log_dir", &run_log_dir)?;
            kwargs.set_item(
                "shutdown_manager_bin_path",
                shutdown_manager_bin_path.as_deref(),
            )?;
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
                std::ffi::CString::new(filename.as_str()).unwrap().as_c_str(),
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
    fn wrapper_script_generator_threads_spec_new_id_and_pubkey_pem() {
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
            vec!["--source".to_owned(), "/src".to_owned()],
            true,
            "/log/run-1".to_owned(),
            None,
        );

        let spec = SecondarySpawnSpec {
            new_secondary_id: "secondary-7".to_owned(),
            primary_endpoint: "127.0.0.1:5555".to_owned(),
            primary_pubkey_pem:
                "-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----\n"
                    .to_owned(),
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

            // forwarded_argv contains the original entries PLUS the
            // injected --secondary-primary-pubkey-pem= entry.
            let argv: Vec<String> = call_dict
                .get_item("forwarded_argv")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!(argv.contains(&"--source".to_owned()));
            assert!(argv.contains(&"/src".to_owned()));
            assert!(
                argv.iter().any(|s| s.starts_with("--secondary-primary-pubkey-pem=")),
                "spec.primary_pubkey_pem must be appended to forwarded_argv as \
                 --secondary-primary-pubkey-pem=<pem>; got argv = {argv:?}",
            );
            let pem_arg = argv
                .iter()
                .find(|s| s.starts_with("--secondary-primary-pubkey-pem="))
                .unwrap();
            assert!(
                pem_arg.contains("-----BEGIN PUBLIC KEY-----"),
                "the appended argv entry must carry the full PEM; got: {pem_arg}",
            );
        });
    }

    #[test]
    fn wrapper_script_generator_skips_pubkey_arg_when_pem_empty() {
        let (jm, globals) = make_stub_job_manager("stub_jm_empty_pem");
        let image_metadata = Python::attach(|py| py.None());
        let generator = wrapper_script_generator_from_pyobj(
            jm,
            image_metadata,
            "gw.example.invalid".to_owned(),
            5555,
            "0".to_owned(),
            "-2G".to_owned(),
            vec!["--source".to_owned()],
            true,
            "/log/run-1".to_owned(),
            None,
        );

        let spec = SecondarySpawnSpec {
            new_secondary_id: "secondary-0".to_owned(),
            primary_endpoint: "127.0.0.1:5555".to_owned(),
            primary_pubkey_pem: String::new(),
        };
        let _body = generator(&spec).expect("closure must render");

        Python::attach(|py| {
            let g = globals.bind(py);
            let calls_any = g.get_item("calls").unwrap();
            let calls = calls_any.cast::<pyo3::types::PyList>().unwrap();
            let call = calls.get_item(0).unwrap();
            let call_dict = call.cast::<PyDict>().unwrap();
            let argv: Vec<String> = call_dict
                .get_item("forwarded_argv")
                .unwrap()
                .unwrap()
                .extract()
                .unwrap();
            assert!(
                !argv.iter().any(|s| s.starts_with("--secondary-primary-pubkey-pem")),
                "empty pem must NOT inject an empty --secondary-primary-pubkey-pem= \
                 argv entry (that would mask the missing-value follow-up); got: {argv:?}",
            );
        });
    }
}
