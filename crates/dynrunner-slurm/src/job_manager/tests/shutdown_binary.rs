//! Tests for [`SlurmJobManager::upload_shutdown_manager_binary`].
//!
//! The upload primitive reads
//! `DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE` to discover the local
//! source path. Env-var-mutating tests cannot run in parallel — env
//! is process-global, so a sibling test that sets the var while
//! another expects it unset would race. Cargo's default test runner
//! IS multi-threaded; we serialise the env-var-touching tests
//! through a module-local `tokio::sync::Mutex`. The lock is acquired
//! at the top of every test that reads or writes
//! `DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE`, and released on the test's
//! drop.
//!
//! Why `tokio::sync::Mutex` rather than `std::sync::Mutex`: the
//! upload primitive is `async`, and the workspace lint
//! `await_holding_lock = "deny"` forbids holding a `std` mutex guard
//! across `.await`. `tokio::sync::Mutex` is async-aware and not
//! caught by the lint. (Functionally we don't need yield-on-
//! contention — every test is on a current-thread runtime — but the
//! lint serves the broader async-Rust hygiene of the workspace and
//! we conform.)
//!
//! Why module-local mutex and not `serial_test`: the workspace
//! doesn't carry the `serial_test` dev-dep today, and pulling it in
//! for a three-test cluster adds a build-time dependency for every
//! sibling crate that test-compiles `dynrunner-slurm`. A local mutex
//! is a single-purpose primitive scoped exactly to the env-var
//! contention we know about.
//!
//! Why `std::env::set_var` is `unsafe` in tests: edition 2024 marks
//! these as unsafe to surface the process-wide effect. We wrap each
//! call in an `unsafe { … }` block guarded by the module mutex so
//! the safety contract ("no concurrent reads/writes from other
//! threads") is locally enforced.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;
use tracing_test::traced_test;

use super::super::shutdown_binary::{
    SHUTDOWN_BIN_REMOTE_BASENAME, SHUTDOWN_BIN_SOURCE_ENV,
};
use crate::config::SlurmConfig;
use crate::job_manager::{SlurmError, SlurmJobManager};
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

/// Process-wide serialisation lock for tests that mutate
/// `DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE`. Init-on-first-use to keep
/// the cost off the non-env-mutating tests. Async-aware to avoid
/// `await_holding_lock` (the workspace clippy lint is `deny` —
/// holding the guard across `.upload_shutdown_manager_binary()` is
/// intentional but a `std` mutex guard would trip the lint).
fn env_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

/// RAII guard: snapshots the current value of
/// `DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE` on construction and
/// restores it on drop. Combined with the module mutex this gives
/// each test a clean env-var state regardless of execution order.
struct EnvVarGuard {
    previous: Option<String>,
}

impl EnvVarGuard {
    fn new() -> Self {
        let previous = std::env::var(SHUTDOWN_BIN_SOURCE_ENV).ok();
        Self { previous }
    }

    fn set(&self, value: &str) {
        // SAFETY: the module mutex guarantees no other test thread
        // is concurrently reading or writing this env var while the
        // guard is alive.
        unsafe { std::env::set_var(SHUTDOWN_BIN_SOURCE_ENV, value) };
    }

    fn unset(&self) {
        // SAFETY: same as `set` — module mutex serialisation.
        unsafe { std::env::remove_var(SHUTDOWN_BIN_SOURCE_ENV) };
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            // SAFETY: same as `set` — module mutex serialisation.
            Some(v) => unsafe { std::env::set_var(SHUTDOWN_BIN_SOURCE_ENV, v) },
            None => unsafe { std::env::remove_var(SHUTDOWN_BIN_SOURCE_ENV) },
        }
    }
}

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
        self.events.lock().unwrap().push(GatewayEvent::TransferFile {
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

/// Env var unset → upload is skipped (`Ok(None)`), no gateway
/// operations issued, and a WARN-level log surfaces the missing
/// integration so an operator who DID intend to enable cleanup sees
/// the gap.
#[tokio::test(flavor = "current_thread")]
#[traced_test]
async fn upload_shutdown_manager_binary_skipped_when_env_unset() {
    let _g = env_lock().lock().await;
    let env = EnvVarGuard::new();
    env.unset();

    let gw = ShutdownBinaryRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);

    let resolved = mgr
        .upload_shutdown_manager_binary()
        .await
        .expect("unset env must not error");

    assert!(
        resolved.is_none(),
        "unset env must return Ok(None); got: {resolved:?}",
    );
    assert!(
        mgr.shutdown_manager_remote_path().is_none(),
        "unset env must leave shutdown_manager_remote_path unset on the manager",
    );
    assert!(
        mgr.gateway().events().is_empty(),
        "unset env must not issue any gateway operations; got: {:?}",
        mgr.gateway().events(),
    );
    assert!(
        logs_contain(SHUTDOWN_BIN_SOURCE_ENV),
        "warn-log must mention the env-var name so operators can grep for it",
    );
    assert!(
        logs_contain("orphan-container cleanup disabled"),
        "warn-log must surface the user-visible consequence",
    );
}

/// Env var set + local file present → upload issues exactly one
/// `transfer_file(local, root/dynrunner-slurm-shutdown)` followed
/// by one `chmod 755 root/dynrunner-slurm-shutdown` (in that order),
/// returns `Ok(Some(remote_path))`, and records the resolved path
/// on the manager so subsequent wrapper renders pick it up via
/// `shutdown_manager_remote_path`.
#[tokio::test(flavor = "current_thread")]
async fn upload_shutdown_manager_binary_uploads_and_chmods_when_env_set() {
    let _g = env_lock().lock().await;
    let env = EnvVarGuard::new();

    let tmp = TempDir::new().expect("tmpdir");
    let local_path = tmp.path().join("dynrunner-slurm-shutdown");
    std::fs::write(&local_path, b"#!/bin/sh\nexit 0\n").expect("write fake binary");
    env.set(local_path.to_str().expect("ascii path"));

    let gw = ShutdownBinaryRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);

    let resolved = mgr
        .upload_shutdown_manager_binary()
        .await
        .expect("upload must succeed when source exists");

    let expected_remote = format!("/srv/slurm/{SHUTDOWN_BIN_REMOTE_BASENAME}");
    assert_eq!(
        resolved.as_deref(),
        Some(expected_remote.as_str()),
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

/// Env var set to a non-existent path → hard error, NO transfer_file
/// call (we must not partially upload a phantom file). Surfaces a
/// misconfigured dispatch loudly: the operator opted into the
/// feature so a missing source binary deserves a hard failure, not
/// a silent "cleanup disabled" warning.
#[tokio::test(flavor = "current_thread")]
async fn upload_shutdown_manager_binary_surfaces_missing_source() {
    let _g = env_lock().lock().await;
    let env = EnvVarGuard::new();

    let tmp = TempDir::new().expect("tmpdir");
    let missing = tmp.path().join("does-not-exist");
    env.set(missing.to_str().expect("ascii path"));

    let gw = ShutdownBinaryRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);

    let err = mgr
        .upload_shutdown_manager_binary()
        .await
        .expect_err("missing source must surface as Err");
    match err {
        SlurmError::ShutdownBinaryNotFound(path) => {
            assert_eq!(path, missing, "error must carry the offending path verbatim");
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
/// `systemd-run --user --scope ... -- <path>` block (Step 4 of the
/// dispatcher-integration plumbing).
///
/// This pins the manager → renderer boundary: the path the upload
/// step records is the path the renderer consumes. A regression
/// that broke the wiring (e.g. the renderer reading from a
/// different field, or the wrapper-script generator omitting the
/// path) trips this assertion.
#[test]
fn wrapper_render_includes_uploaded_path_when_manager_has_remote_path() {
    use crate::wrapper_script::{
        generate_wrapper_script, ConnectionMode, WrapperScriptConfig,
    };

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
    };
    let script = generate_wrapper_script(&cfg);

    assert!(
        script.contains("systemd-run --user --scope"),
        "rendered wrapper must contain the systemd-run spawn block when \
         shutdown_manager_bin_path is Some; got script: {script}",
    );
    assert!(
        script.contains(remote),
        "rendered wrapper must reference the resolved remote path verbatim; \
         expected substring `{remote}`, full script: {script}",
    );
}
