//! Tests for the shared hash-conditional staging mechanics
//! ([`SlurmJobManager::upload_binary_hash_conditional`], exercised
//! through the public [`SlurmJobManager::upload_wrapper_binary_from`]
//! primitive).
//!
//! Concern under test: the gateway-hash gate. The local source always
//! exists here (the missing-source branch is covered in
//! [`super::shutdown_binary`]); what varies is what `sha256sum
//! <remote>` reports back, which decides skip vs transfer.
//!
//! The recording gateway computes its `sha256sum` reply from a
//! caller-supplied "remote bytes" fixture so the test asserts genuine
//! hash equality rather than a hard-coded digest: the *same* bytes on
//! both sides must skip, *different* bytes (or no remote at all) must
//! transfer.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use dynrunner_manager_distributed::compute_file_hash;
use tempfile::TempDir;

use super::super::wrapper_binary::WRAPPER_BIN_REMOTE_BASENAME;
use crate::config::SlurmConfig;
use crate::job_manager::SlurmJobManager;
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

#[derive(Debug, Clone, PartialEq, Eq)]
enum GatewayEvent {
    TransferFile { local: PathBuf, remote: String },
    Command(String),
}

/// Records every gateway call and answers a `sha256sum <remote>`
/// probe with the SHA-256 of `remote_contents` (or a `success: false`
/// result — mimicking `test -f`/`sha256sum` on a missing file — when
/// `remote_contents` is `None`). Single concern: capture-for-assertion
/// plus a controllable remote-hash reply.
struct HashProbingGateway {
    events: Mutex<Vec<GatewayEvent>>,
    /// Hex SHA-256 the `sha256sum` probe reports, or `None` to report
    /// "no such file" (probe exits non-zero).
    remote_hash: Option<String>,
}

impl HashProbingGateway {
    /// Gateway that reports `remote_contents` as the bytes already on
    /// the gateway (the probe replies with their SHA-256).
    ///
    /// Hashes the bytes through a per-call [`TempDir`] so concurrent
    /// tests in the same process never race on a shared scratch file.
    fn with_remote_bytes(remote_contents: &[u8]) -> Self {
        let dir = TempDir::new().expect("tmpdir");
        let staged = dir.path().join("remote.bin");
        std::fs::write(&staged, remote_contents).unwrap();
        let hash = compute_file_hash(&staged);
        Self {
            events: Mutex::new(Vec::new()),
            remote_hash: hash,
        }
    }

    /// Gateway with no remote copy: the `sha256sum` probe exits
    /// non-zero.
    fn absent() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            remote_hash: None,
        }
    }

    fn events(&self) -> Vec<GatewayEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Gateway for HashProbingGateway {
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
        if cmd.starts_with("sha256sum ") {
            return Ok(match &self.remote_hash {
                Some(hash) => CommandResult {
                    return_code: 0,
                    // `sha256sum` prints `<hex>␣␣<path>`.
                    stdout: format!("{hash}  /remote/path\n"),
                    stderr: String::new(),
                },
                None => CommandResult {
                    return_code: 1,
                    stdout: String::new(),
                    stderr: "sha256sum: No such file or directory".into(),
                },
            });
        }
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

fn manager_with(gw: HashProbingGateway) -> SlurmJobManager<HashProbingGateway> {
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    SlurmJobManager::new(cfg, gw)
}

fn write_local(bytes: &[u8]) -> (TempDir, PathBuf) {
    let tmp = TempDir::new().expect("tmpdir");
    let local = tmp.path().join("dynrunner-slurm-wrapper");
    std::fs::write(&local, bytes).expect("write fake binary");
    (tmp, local)
}

/// Gateway already holds a byte-identical copy (same SHA-256) → the
/// transfer is SKIPPED: the only gateway calls are the `sha256sum`
/// probe and the idempotent `chmod 755`, with NO `transfer_file`. The
/// resolved remote path is still returned and recorded.
#[tokio::test(flavor = "current_thread")]
async fn same_hash_skips_transfer() {
    let bytes = b"musl-static binary bytes v1";
    let (_tmp, local) = write_local(bytes);

    let gw = HashProbingGateway::with_remote_bytes(bytes);
    let mut mgr = manager_with(gw);

    let expected_remote = format!("/srv/slurm/{WRAPPER_BIN_REMOTE_BASENAME}");
    let resolved = mgr
        .upload_wrapper_binary_from(local)
        .await
        .expect("up-to-date remote still resolves the path");
    assert_eq!(resolved, expected_remote);
    assert_eq!(
        mgr.wrapper_bin_remote_path(),
        Some(expected_remote.as_str()),
        "skip branch must still record the resolved remote path",
    );

    let events = mgr.gateway().events();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, GatewayEvent::TransferFile { .. })),
        "matching remote hash must skip the transfer; got: {events:?}",
    );
    assert_eq!(
        events,
        vec![
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::Command(format!("chmod 755 {expected_remote}")),
        ],
        "skip path issues exactly the probe + chmod, in order",
    );
}

/// Gateway holds a DIFFERENT binary (different SHA-256) → the binary
/// is re-transferred (probe → transfer → chmod, in order). Pins the
/// "changed binary MUST be re-uploaded" correctness requirement.
#[tokio::test(flavor = "current_thread")]
async fn different_hash_transfers() {
    let (_tmp, local) = write_local(b"musl-static binary bytes v2");

    let gw = HashProbingGateway::with_remote_bytes(b"stale binary bytes v1");
    let mut mgr = manager_with(gw);

    let expected_remote = format!("/srv/slurm/{WRAPPER_BIN_REMOTE_BASENAME}");
    let resolved = mgr
        .upload_wrapper_binary_from(local.clone())
        .await
        .expect("hash mismatch re-uploads and resolves the path");
    assert_eq!(resolved, expected_remote);

    let events = mgr.gateway().events();
    assert_eq!(
        events,
        vec![
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::TransferFile {
                local,
                remote: expected_remote.clone(),
            },
            GatewayEvent::Command(format!("chmod 755 {expected_remote}")),
        ],
        "hash mismatch must re-transfer; got: {events:?}",
    );
}

/// Gateway has no remote copy at all (`sha256sum` exits non-zero) →
/// the binary is transferred. Pins the "absent remote → upload"
/// branch.
#[tokio::test(flavor = "current_thread")]
async fn absent_remote_transfers() {
    let (_tmp, local) = write_local(b"musl-static binary bytes");

    let gw = HashProbingGateway::absent();
    let mut mgr = manager_with(gw);

    let expected_remote = format!("/srv/slurm/{WRAPPER_BIN_REMOTE_BASENAME}");
    mgr.upload_wrapper_binary_from(local.clone())
        .await
        .expect("absent remote uploads");

    let events = mgr.gateway().events();
    assert_eq!(
        events,
        vec![
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::TransferFile {
                local,
                remote: expected_remote.clone(),
            },
            GatewayEvent::Command(format!("chmod 755 {expected_remote}")),
        ],
        "absent remote must transfer; got: {events:?}",
    );
}
