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
//! transfer — and a transfer must end in a PASSING post-upload hash
//! verification (the gate re-hashes the remote after `transfer_file`).
//! The mock is therefore stateful: a FAITHFUL transfer updates the
//! reply to the transferred bytes' hash, while the corrupted-transfer
//! fixture leaves the stale reply in place so the verification's
//! hard-error branch is observable.

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
/// probe with the SHA-256 of the CURRENT remote bytes (or a
/// `success: false` result — mimicking `test -f`/`sha256sum` on a
/// missing file — when there is no remote copy). Single concern:
/// capture-for-assertion plus a controllable remote-hash reply.
///
/// Stateful like the real gateway: a `transfer_file` on a
/// `faithful_transfer` gateway updates the reply to the hash of the
/// transferred local bytes (so the post-upload verification sees what
/// a successful transfer produces); with `faithful_transfer == false`
/// the reply stays whatever it was (a truncated/corrupted transfer
/// whose remote bytes do NOT match the source).
struct HashProbingGateway {
    events: Mutex<Vec<GatewayEvent>>,
    /// Hex SHA-256 the `sha256sum` probe reports, or `None` to report
    /// "no such file" (probe exits non-zero). Updated by a faithful
    /// `transfer_file`.
    remote_hash: Mutex<Option<String>>,
    /// Whether `transfer_file` lands the source bytes intact (updates
    /// `remote_hash` from the local file) or corrupts them (leaves the
    /// stale `remote_hash` in place).
    faithful_transfer: bool,
}

impl HashProbingGateway {
    /// Gateway that reports `remote_contents` as the bytes already on
    /// the gateway (the probe replies with their SHA-256).
    ///
    /// Hashes the bytes through a per-call [`TempDir`] so concurrent
    /// tests in the same process never race on a shared scratch file.
    fn with_remote_bytes(remote_contents: &[u8]) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            remote_hash: Mutex::new(Some(hash_of_bytes(remote_contents))),
            faithful_transfer: true,
        }
    }

    /// Gateway with no remote copy: the `sha256sum` probe exits
    /// non-zero.
    fn absent() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            remote_hash: Mutex::new(None),
            faithful_transfer: true,
        }
    }

    /// Gateway whose `transfer_file` CORRUPTS the payload: the probe
    /// keeps reporting the pre-transfer remote state (here: stale
    /// `remote_contents`), exactly what a truncated transfer or an
    /// out-of-band clobber racing the upload looks like to the
    /// post-upload verification.
    fn with_corrupting_transfer(remote_contents: &[u8]) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            remote_hash: Mutex::new(Some(hash_of_bytes(remote_contents))),
            faithful_transfer: false,
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
            return Ok(match &*self.remote_hash.lock().unwrap() {
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
        if self.faithful_transfer {
            // The remote now holds the source bytes — the subsequent
            // `sha256sum` probe must see their hash, as on a real
            // gateway.
            *self.remote_hash.lock().unwrap() = compute_file_hash(local);
        }
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

/// SHA-256 (hex) of `bytes` through the SAME helper production uses
/// (`compute_file_hash` is file-based), staged via a per-call
/// [`TempDir`] so concurrent tests never race on a shared scratch file.
fn hash_of_bytes(bytes: &[u8]) -> String {
    let dir = TempDir::new().expect("tmpdir");
    let staged = dir.path().join("remote.bin");
    std::fs::write(&staged, bytes).unwrap();
    compute_file_hash(&staged).expect("hash scratch file")
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

/// Gateway holds a DIFFERENT (stale) binary → the binary is
/// re-transferred and the remote is RE-HASHED to prove the upload
/// landed intact (probe → transfer → verify-probe → chmod, in order).
/// Pins the "changed binary MUST be re-uploaded" correctness
/// requirement plus the post-upload freshness verification.
#[tokio::test(flavor = "current_thread")]
async fn different_hash_transfers_and_verifies() {
    let (_tmp, local) = write_local(b"musl-static binary bytes v2");

    let gw = HashProbingGateway::with_remote_bytes(b"stale binary bytes v1");
    let mut mgr = manager_with(gw);

    let expected_remote = format!("/srv/slurm/{WRAPPER_BIN_REMOTE_BASENAME}");
    let resolved = mgr
        .upload_wrapper_binary_from(local.clone())
        .await
        .expect("hash mismatch re-uploads, verifies, and resolves the path");
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
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::Command(format!("chmod 755 {expected_remote}")),
        ],
        "hash mismatch must re-transfer and re-verify; got: {events:?}",
    );
}

/// Gateway has no remote copy at all (`sha256sum` exits non-zero) →
/// the binary is transferred and the post-upload verification re-hashes
/// the now-present remote. Pins the "absent remote → upload" branch.
#[tokio::test(flavor = "current_thread")]
async fn absent_remote_transfers_and_verifies() {
    let (_tmp, local) = write_local(b"musl-static binary bytes");

    let gw = HashProbingGateway::absent();
    let mut mgr = manager_with(gw);

    let expected_remote = format!("/srv/slurm/{WRAPPER_BIN_REMOTE_BASENAME}");
    mgr.upload_wrapper_binary_from(local.clone())
        .await
        .expect("absent remote uploads and verifies");

    let events = mgr.gateway().events();
    assert_eq!(
        events,
        vec![
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::TransferFile {
                local,
                remote: expected_remote.clone(),
            },
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::Command(format!("chmod 755 {expected_remote}")),
        ],
        "absent remote must transfer then verify; got: {events:?}",
    );
}

/// A transfer whose remote bytes do NOT end up matching the local
/// source (truncated/corrupted transfer, or an out-of-band clobber
/// racing the upload) must surface as a HARD error — the staleness
/// window the verify-after-upload design closes. The chmod must not
/// run and no remote path may be recorded: nothing downstream may
/// treat the corrupt staging as deployable.
#[tokio::test(flavor = "current_thread")]
async fn corrupted_transfer_is_a_hard_error() {
    let (_tmp, local) = write_local(b"musl-static binary bytes v2");

    // Stale remote AND a corrupting transfer: the post-upload probe
    // keeps reporting the stale hash.
    let gw = HashProbingGateway::with_corrupting_transfer(b"stale binary bytes v1");
    let mut mgr = manager_with(gw);

    let expected_remote = format!("/srv/slurm/{WRAPPER_BIN_REMOTE_BASENAME}");
    let err = mgr
        .upload_wrapper_binary_from(local.clone())
        .await
        .expect_err("post-upload hash mismatch must hard-error");
    assert!(
        matches!(
            &err,
            crate::job_manager::SlurmError::StagedBinaryHashMismatch { remote, .. }
                if remote == &expected_remote
        ),
        "expected StagedBinaryHashMismatch for {expected_remote}, got: {err:?}",
    );
    assert_eq!(
        mgr.wrapper_bin_remote_path(),
        None,
        "a failed verification must not record a deployable remote path",
    );

    let events = mgr.gateway().events();
    assert_eq!(
        events,
        vec![
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
            GatewayEvent::TransferFile {
                local,
                remote: expected_remote.clone(),
            },
            GatewayEvent::Command(format!("sha256sum {expected_remote}")),
        ],
        "verification failure must abort BEFORE the chmod; got: {events:?}",
    );
}
