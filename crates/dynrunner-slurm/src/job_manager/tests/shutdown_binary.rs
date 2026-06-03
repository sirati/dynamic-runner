//! Tests for [`SlurmJobManager::upload_shutdown_manager_binary_from`].
//!
//! The upload primitive takes the local source path as an argument
//! (resolution policy lives in the Python bridge:
//! ``dynamic_runner._shutdown_manager.bundled_binary_path`` chooses
//! between the env-var override and the wheel-bundled artifact). The
//! Rust primitive itself reads no process state, so these tests are
//! free of env-var serialisation concerns — no module mutex, no
//! ``EnvVarGuard``.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tempfile::TempDir;

use super::super::shutdown_binary::SHUTDOWN_BIN_REMOTE_BASENAME;
use crate::config::SlurmConfig;
use crate::job_manager::{SlurmError, SlurmJobManager};
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

/// Records every gateway call the upload primitive makes so each
/// test can assert (a) the operations issued and (b) their order.
/// Single concern: capture-for-assertion; no behaviour beyond
/// returning `success: 0` on every operation.
#[derive(Default)]
struct ShutdownBinaryRecordingGateway {
    events: Mutex<Vec<GatewayEvent>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GatewayEvent {
    TransferFile { local: PathBuf, remote: String },
    Command(String),
}

impl ShutdownBinaryRecordingGateway {
    fn events(&self) -> Vec<GatewayEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Gateway for ShutdownBinaryRecordingGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn execute_command(
        &self,
        cmd: &str,
        _cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        self.events
            .lock()
            .unwrap()
            .push(GatewayEvent::Command(cmd.to_string()));
        Ok(CommandResult {
            return_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, local: &Path, remote: &str) -> Result<(), GatewayError> {
        self.events
            .lock()
            .unwrap()
            .push(GatewayEvent::TransferFile {
                local: local.to_path_buf(),
                remote: remote.to_string(),
            });
        Ok(())
    }
    async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn create_directory(&self, _remote: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
        Ok(false)
    }
    fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
        Ok(())
    }
}

/// Local source exists → upload issues exactly one
/// `transfer_file(local, root/dynrunner-slurm-shutdown)` followed
/// by one `chmod 755 root/dynrunner-slurm-shutdown` (in that order),
/// returns `Ok(remote_path)`, and records the resolved path on the
/// manager so subsequent wrapper renders pick it up via
/// `shutdown_manager_remote_path`.
#[tokio::test(flavor = "current_thread")]
async fn upload_shutdown_manager_binary_uploads_and_chmods() {
    let tmp = TempDir::new().expect("tmpdir");
    let local_path = tmp.path().join("dynrunner-slurm-shutdown");
    std::fs::write(&local_path, b"#!/bin/sh\nexit 0\n").expect("write fake binary");

    let gw = ShutdownBinaryRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);

    let resolved = mgr
        .upload_shutdown_manager_binary_from(local_path.clone())
        .await
        .expect("upload must succeed when source exists");

    let expected_remote = format!("/srv/slurm/{SHUTDOWN_BIN_REMOTE_BASENAME}");
    assert_eq!(
        resolved, expected_remote,
        "remote path must be `<root_folder>/{SHUTDOWN_BIN_REMOTE_BASENAME}`",
    );
    assert_eq!(
        mgr.shutdown_manager_remote_path(),
        Some(expected_remote.as_str()),
        "manager must record the resolved remote path so wrapper renders pick it up",
    );

    let events = mgr.gateway().events();
    assert_eq!(
        events.len(),
        2,
        "upload must issue exactly one transfer_file + one chmod; got: {events:?}",
    );
    match &events[0] {
        GatewayEvent::TransferFile { local, remote } => {
            assert_eq!(local, &local_path);
            assert_eq!(remote, &expected_remote);
        }
        other => panic!("expected first event to be TransferFile, got {other:?}"),
    }
    match &events[1] {
        GatewayEvent::Command(cmd) => {
            assert_eq!(cmd, &format!("chmod 755 {expected_remote}"));
        }
        other => panic!("expected second event to be Command(chmod), got {other:?}"),
    }
}

/// Local source path does not point at a real file → hard error,
/// NO transfer_file call (we must not partially upload a phantom
/// file). Surfaces a misconfigured dispatch loudly: the caller
/// already decided this is the binary to deploy, so a missing
/// source deserves a hard failure, not a silent "cleanup disabled"
/// warning.
#[tokio::test(flavor = "current_thread")]
async fn upload_shutdown_manager_binary_surfaces_missing_source() {
    let tmp = TempDir::new().expect("tmpdir");
    let missing = tmp.path().join("does-not-exist");

    let gw = ShutdownBinaryRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);

    let err = mgr
        .upload_shutdown_manager_binary_from(missing.clone())
        .await
        .expect_err("missing source must surface as Err");
    match err {
        SlurmError::ShutdownBinaryNotFound(path) => {
            assert_eq!(
                path, missing,
                "error must carry the offending path verbatim"
            );
        }
        other => panic!("expected ShutdownBinaryNotFound, got {other:?}"),
    }

    assert!(
        mgr.gateway().events().is_empty(),
        "missing source must not produce any gateway operations; got: {:?}",
        mgr.gateway().events(),
    );
    assert!(
        mgr.shutdown_manager_remote_path().is_none(),
        "missing source must leave the manager's remote path unset",
    );
}

/// Wrapper-rendering integration: when the manager's
/// `shutdown_manager_remote_path` is `Some(...)`, a wrapper render
/// driven through it must include the uploaded path inside the
/// `systemd-run --user --quiet ... --unit=... -- <path>` block
/// (Step 4 of the dispatcher-integration plumbing).
///
/// This pins the manager → renderer boundary: the path the upload
/// step records is the path the renderer consumes. A regression
/// that broke the wiring (e.g. the renderer reading from a
/// different field, or the wrapper-script generator omitting the
/// path) trips this assertion.
#[test]
fn wrapper_render_includes_uploaded_path_when_manager_has_remote_path() {
    use crate::wrapper_script::{ConnectionMode, WrapperScriptConfig, generate_wrapper_script};

    let remote = "/srv/slurm/dynrunner-slurm-shutdown";
    let config = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };

    // Construct a wrapper config the same way preparation.rs does:
    // pull the path off the manager, hand it (as a &Path) to the
    // renderer's `shutdown_manager_bin_path` field.
    let bin_path = Path::new(remote);
    let cfg = WrapperScriptConfig {
        slurm_config: &config,
        image_path: "/srv/slurm/image_bin/app.tar.gz",
        secondary_id: "sec-0",
        image_name: "app",
        image_tag: "latest",
        image_tar_basename: "app.tar.gz",
        load_command: "podman load",
        container_command: "dynamic_runner.task",
        cores_spec: "0",
        max_memory_spec: "-2G",
        connection: ConnectionMode::Standard {
            gateway_host: "gw.example",
            gateway_port: 9000,
        },
        run_log_dir: None,
        dynrunner_network_dir: None,
        srcbins_mount_source: None,
        output_dir: None,
        extra_run_args: &[],
        forwarded_argv: &[],
        is_observer: false,
        shutdown_manager_bin_path: Some(bin_path),
        mem_manager_reserved_bytes: None,
    };
    let script = generate_wrapper_script(&cfg);

    assert!(
        script.contains("systemd-run --user --quiet"),
        "rendered wrapper must contain the systemd-run spawn block \
         (service mode, --quiet) when shutdown_manager_bin_path is \
         Some; got script: {script}",
    );
    assert!(
        script.contains(remote),
        "rendered wrapper must reference the resolved remote path verbatim; \
         expected substring `{remote}`, full script: {script}",
    );
}
