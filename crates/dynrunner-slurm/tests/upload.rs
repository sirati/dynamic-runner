//! Integration tests for `SlurmJobManager::upload_source_binaries`.
//!
//! Covers the three legitimate `binary.path` shapes that the
//! relative-path-fix from d5d0604 (Python) / 09f96f7 (Rust primary)
//! formalised:
//!
//! 1. Relative-under-src — joined against `source_root` for the
//!    on-disk read, uploaded to `<srcbins>/<rel>` verbatim.
//! 2. Absolute-under-src — strip-prefix succeeds, uploaded to the
//!    stripped tail.
//! 3. Absolute-out-of-tree — strip-prefix fails, skipped with
//!    warning, never uploaded (the secondary's `stage_file` handler
//!    treats the absolute path as out-of-band-staged).
//!
//! Uses an in-memory `Gateway` test double that records every
//! `transfer_file` and `create_directory` call so tests can assert
//! on the exact gateway-side layout without hitting the real
//! filesystem (apart from the local source files the manager
//! actually opens).

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use dynrunner_core::{PhaseId, TaskInfo, TypeId};
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};
use dynrunner_slurm::{SlurmConfig, SlurmJobManager};

/// Gateway test double that records every mutating call.
///
/// Read-side methods (`execute_command`, `file_exists`,
/// `download_file`) aren't exercised by `upload_source_binaries`, so
/// they return canned no-op success — keeping the double minimal
/// rather than bolting on unused branches.
#[derive(Default)]
struct RecordingGateway {
    inner: Mutex<RecordingState>,
}

#[derive(Default)]
struct RecordingState {
    /// `(local_path, remote_path)` for every `transfer_file` call,
    /// in invocation order.
    transfers: Vec<(PathBuf, String)>,
    /// Every `create_directory` call, in invocation order.
    created_dirs: Vec<String>,
}

impl RecordingGateway {
    fn transfers(&self) -> Vec<(PathBuf, String)> {
        self.inner.lock().unwrap().transfers.clone()
    }

    fn created_dirs(&self) -> Vec<String> {
        self.inner.lock().unwrap().created_dirs.clone()
    }
}

impl Gateway for RecordingGateway {
    async fn connect(&mut self) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn execute_command(
        &self,
        _cmd: &str,
        _cwd: Option<&str>,
    ) -> Result<CommandResult, GatewayError> {
        Ok(CommandResult {
            return_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, local: &Path, remote: &str) -> Result<(), GatewayError> {
        self.inner
            .lock()
            .unwrap()
            .transfers
            .push((local.to_path_buf(), remote.to_string()));
        Ok(())
    }
    async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        self.inner
            .lock()
            .unwrap()
            .created_dirs
            .push(remote.to_string());
        Ok(())
    }
    async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
        Ok(false)
    }
    fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
        Ok(())
    }
}

fn make_binary(path: impl Into<PathBuf>) -> TaskInfo<String> {
    TaskInfo {
        path: path.into(),
        size: 0,
        identifier: "test".to_string(),
        phase_id: PhaseId::from("default"),
        type_id: TypeId::from("default"),
        affinity_id: None,
        payload: serde_json::Value::Null,
        task_id: None,
        task_depends_on: vec![],
        resolved_path: None,
    }
}

/// Build a manager whose `src_bins_path()` is the literal string
/// `"/srcbins"` — keeping the test assertions on exact remote paths
/// independent of `SlurmConfig::default()` evolution.
fn make_manager() -> SlurmJobManager<RecordingGateway> {
    let cfg = SlurmConfig {
        root_folder: "".into(),
        image_dir: "image_bin".into(),
        src_bins_dir: "srcbins".into(),
        output_dir: "out".into(),
        log_dir: "log".into(),
        partition: None,
        time_limit: None,
        cpus_per_task: None,
        mem: None,
        email: None,
        prestaged_src_bins_path: None,
    };
    // `src_bins_path()` returns `format!("{root}/{src_bins_dir}")`,
    // so an empty root yields the leading-slash `/srcbins` we want.
    SlurmJobManager::new(cfg, RecordingGateway::default())
}

/// Case 1 (relative-under-src): wire-id-shape relative path joins
/// against source_root for the on-disk read, lands at
/// `<srcbins>/<rel>` on the gateway verbatim.
#[tokio::test]
async fn upload_relative_under_src() {
    let tmp = tempfile::tempdir().unwrap();
    let src_root = tmp.path().to_path_buf();
    let local_file = src_root.join("subdir").join("foo.bin");
    std::fs::create_dir_all(local_file.parent().unwrap()).unwrap();
    std::fs::write(&local_file, b"hello").unwrap();

    let mgr = make_manager();
    let binaries = vec![make_binary("subdir/foo.bin")];

    mgr.upload_source_binaries(&binaries, &src_root).await.unwrap();

    let transfers = mgr.gateway().transfers();
    assert_eq!(transfers.len(), 1, "exactly one transfer expected");
    let (local, remote) = &transfers[0];
    assert_eq!(local, &local_file, "manager must read from source_root-joined path");
    assert_eq!(
        remote, "/srcbins/subdir/foo.bin",
        "remote dest must mirror the relative tail under srcbins",
    );
    let dirs = mgr.gateway().created_dirs();
    assert!(
        dirs.contains(&"/srcbins/subdir".to_string()),
        "parent directory must be created before transfer (got {:?})",
        dirs,
    );
}

/// Case 2 (absolute-under-src, legacy shape): strip_prefix succeeds,
/// upload lands at the stripped tail under srcbins.
#[tokio::test]
async fn upload_absolute_under_src() {
    let tmp = tempfile::tempdir().unwrap();
    let src_root = tmp.path().to_path_buf();
    let local_file = src_root.join("foo.bin");
    std::fs::write(&local_file, b"hello").unwrap();

    let mgr = make_manager();
    // Absolute path verbatim, sitting under source_root.
    let binaries = vec![make_binary(&local_file)];

    mgr.upload_source_binaries(&binaries, &src_root).await.unwrap();

    let transfers = mgr.gateway().transfers();
    assert_eq!(transfers.len(), 1);
    let (local, remote) = &transfers[0];
    assert_eq!(local, &local_file);
    assert_eq!(
        remote, "/srcbins/foo.bin",
        "absolute-under-src must strip to the tail and upload to <srcbins>/<tail>",
    );
}

/// Case 3 (absolute-out-of-tree): strip_prefix fails, the binary is
/// skipped with a warning rather than uploaded; the StageFile record
/// secondary-side will treat the absolute path as out-of-band-staged.
#[tokio::test]
async fn skip_absolute_out_of_tree() {
    let tmp = tempfile::tempdir().unwrap();
    let src_root = tmp.path().to_path_buf();
    // The file does NOT need to exist for the skip path — the manager
    // bails on strip_prefix before any I/O. Using a path that's
    // syntactically absolute and lexically not-under src_root suffices.
    let outside = PathBuf::from("/elsewhere/foo.bin");

    let mgr = make_manager();
    let binaries = vec![make_binary(outside)];

    mgr.upload_source_binaries(&binaries, &src_root).await.unwrap();

    let transfers = mgr.gateway().transfers();
    assert!(
        transfers.is_empty(),
        "out-of-tree binary must not be uploaded; got transfers {:?}",
        transfers,
    );
}

/// Mixed-input regression: one binary of each shape in a single call.
/// The two in-tree binaries upload, the out-of-tree one is skipped,
/// and the loop continues past the skip rather than aborting (a
/// regression risk if someone added `?` propagation on the skip path
/// later). This is the shape the d5d0604 fix unblocks: pre-fix the
/// relative case landed in the skip branch and the tally read `0/N`.
#[tokio::test]
async fn mixed_inputs_skip_only_out_of_tree() {
    let tmp = tempfile::tempdir().unwrap();
    let src_root = tmp.path().to_path_buf();
    let rel_file = src_root.join("a").join("rel.bin");
    let abs_file = src_root.join("abs.bin");
    std::fs::create_dir_all(rel_file.parent().unwrap()).unwrap();
    std::fs::write(&rel_file, b"r").unwrap();
    std::fs::write(&abs_file, b"a").unwrap();

    let mgr = make_manager();
    let binaries = vec![
        make_binary("a/rel.bin"),
        make_binary(&abs_file),
        make_binary("/elsewhere/skip.bin"),
    ];

    mgr.upload_source_binaries(&binaries, &src_root).await.unwrap();

    let transfers = mgr.gateway().transfers();
    let remotes: Vec<&str> = transfers.iter().map(|(_, r)| r.as_str()).collect();
    assert_eq!(transfers.len(), 2, "exactly the two in-tree binaries upload");
    assert!(remotes.contains(&"/srcbins/a/rel.bin"));
    assert!(remotes.contains(&"/srcbins/abs.bin"));
}
