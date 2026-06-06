use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::types::{SlurmError, SlurmJobManager};
use crate::config::SlurmConfig;
use crate::packaging::{PackagingError, PodmanImageMetadata, PodmanPackaging};
use dynrunner_gateway::local::LocalGateway;
use dynrunner_gateway::traits::{CommandResult, Gateway, GatewayError};

/// Records the inputs the manager hands to the packager so we
/// can assert the boundary contract (output_dir == image_path)
/// without standing up a real builder.
struct RecordingPackaging {
    calls: AtomicUsize,
    last_output_dir: Mutex<Option<PathBuf>>,
    last_project_root: Mutex<Option<PathBuf>>,
    result: PodmanImageMetadata,
}

impl<G: Gateway> PodmanPackaging<G> for RecordingPackaging {
    async fn build_images(
        &self,
        _gateway: &G,
        local_project_root: &Path,
        output_dir: &Path,
    ) -> Result<PodmanImageMetadata, PackagingError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last_output_dir.lock().unwrap() = Some(output_dir.to_path_buf());
        *self.last_project_root.lock().unwrap() = Some(local_project_root.to_path_buf());
        Ok(self.result.clone())
    }
}

#[tokio::test]
async fn build_and_transfer_images_forwards_to_packager() {
    let gw = LocalGateway::new();
    let config = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let manager = SlurmJobManager::new(config, gw);

    let packager = RecordingPackaging {
        calls: AtomicUsize::new(0),
        last_output_dir: Mutex::new(None),
        last_project_root: Mutex::new(None),
        result: PodmanImageMetadata {
            remote_path: PathBuf::from("/srv/slurm/image_bin/app.tar.gz"),
            image_hash: "abc123".into(),
            uploaded: true,
        },
    };

    let project_root = PathBuf::from("/work/proj");
    let metadata = manager
        .build_and_transfer_images(&packager, &project_root)
        .await
        .expect("delegation succeeds");

    // Boundary contract: SlurmJobManager translates its config's
    // image_path() into the packager's output_dir argument; the
    // local project root is forwarded unchanged.
    assert_eq!(packager.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        packager.last_output_dir.lock().unwrap().as_deref(),
        Some(Path::new("/srv/slurm/image_bin")),
    );
    assert_eq!(
        packager.last_project_root.lock().unwrap().as_deref(),
        Some(project_root.as_path()),
    );

    // Returned metadata is forwarded verbatim — the manager owns
    // no normalisation policy.
    assert_eq!(
        metadata.remote_path,
        PathBuf::from("/srv/slurm/image_bin/app.tar.gz")
    );
    assert_eq!(metadata.image_hash, "abc123");
    assert!(metadata.uploaded);
}

#[tokio::test]
async fn build_and_transfer_images_propagates_packager_failure() {
    struct FailingPackaging;
    impl<G: Gateway> PodmanPackaging<G> for FailingPackaging {
        async fn build_images(
            &self,
            _gateway: &G,
            _local_project_root: &Path,
            _output_dir: &Path,
        ) -> Result<PodmanImageMetadata, PackagingError> {
            Err(PackagingError::BuildFailed("nix build crashed".into()))
        }
    }

    let gw = LocalGateway::new();
    let manager = SlurmJobManager::new(SlurmConfig::default(), gw);
    let err = manager
        .build_and_transfer_images(&FailingPackaging, Path::new("/proj"))
        .await
        .expect_err("packager error must surface");
    match err {
        SlurmError::Packaging(PackagingError::BuildFailed(msg)) => {
            assert_eq!(msg, "nix build crashed");
        }
        other => panic!("expected Packaging(BuildFailed), got {other:?}"),
    }
}

/// Recording gateway for `submit_job` tests: captures every
/// `execute_command` and answers any `sbatch ...` line with a
/// canned job ID, every other command with empty stdout. Routing
/// by command-prefix (rather than call-index) means the test stays
/// correct if `submit_job` ever inserts an additional setup
/// command before the sbatch invocation.
#[derive(Default)]
struct SubmitRecordingGateway {
    commands: Mutex<Vec<String>>,
    created_dirs: Mutex<Vec<String>>,
}

impl SubmitRecordingGateway {
    fn commands(&self) -> Vec<String> {
        self.commands.lock().unwrap().clone()
    }

    fn created_dirs(&self) -> Vec<String> {
        self.created_dirs.lock().unwrap().clone()
    }

    /// The single submit command: `printf '%s' '<body>' | sbatch …`.
    /// The script is piped over STDIN, so there is no separate
    /// script-write command and no trailing script-path argument —
    /// the one recorded command carries the `| sbatch` pipe.
    fn sbatch_command(&self) -> String {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.contains("| sbatch "))
            .expect("sbatch command must have been issued")
            .clone()
    }
}

impl Gateway for SubmitRecordingGateway {
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
        self.commands.lock().unwrap().push(cmd.to_string());
        // The `printf … | sbatch --parsable` pipe is the only command
        // that produces stdout (the sbatch job id on its last pipeline
        // stage); anything else is silent in the real shell.
        let stdout = if cmd.contains("| sbatch ") {
            "12345".to_string()
        } else {
            String::new()
        };
        Ok(CommandResult {
            return_code: 0,
            stdout,
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn download_file(&self, _remote: &str, _local: &Path) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn create_directory(&self, remote: &str) -> Result<(), GatewayError> {
        self.created_dirs.lock().unwrap().push(remote.to_string());
        Ok(())
    }
    async fn file_exists(&self, _remote: &str) -> Result<bool, GatewayError> {
        Ok(false)
    }
    fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
        Ok(())
    }
}

/// Parity vs. Python `SlurmJobManager.submit_job` in
/// `packaging/job_manager.py`:
///
/// (a) `--mail-type=ALL` is the only mail-type emitted when notify
///     is set (Python uses ALL; the negative assertion guards
///     against accidental regression to FAIL).
/// (b) `--mem` is OMITTED when `memory_per_node` is `None` —
///     matches Python, which never emits `--mem` at all.
/// (c) `memory_per_node = Some("...")` → `--mem={val}` IS emitted
///     so opt-in operators still get a cap.
/// (d) The wrapper body is piped to sbatch over STDIN
///     (`printf '%s' '<body>' | sbatch …`): the submit issues a SINGLE
///     command — no preceding script-write, no `chmod`, no
///     `<root_folder>/job_<name>.sh` file, and no trailing script-path
///     argument on the sbatch invocation.
/// (e) `--ntasks=1` IS emitted (legacy Python had it; Rust
///     previously omitted it — a parity gap that this assertion
///     locks down so `sbatch` defaults can't drift the launched
///     proc count on partitions whose default ntasks is > 1).
/// (f) `--signal=B:SIGTERM@<N>` IS emitted at the Rust-side default
///     lead time (60s), giving the wrapper's trap → shutdown-manager
///     chain a deterministic warning window before SLURM's
///     `KillWait`-driven SIGKILL. Python never emitted this flag —
///     it's a Rust-only behavioural improvement, locked down here so
///     default-config callers can't silently regress to the
///     no-warning shape.
#[tokio::test]
async fn submit_job_matches_python_invocation_shape() {
    // Case A+B+D+F: defaults — no mem, mail=ALL on notify, script in
    // root, --signal at the default 60s lead time.
    let gw = SubmitRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        notify_email: Some("ops@example.com".into()),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    let jid = mgr
        .submit_job(
            "#!/bin/sh\necho hi",
            "myjob",
            "secondary-0",
            1,
            "/srv/slurm/log/run-1",
        )
        .await
        .expect("submit succeeds");
    assert_eq!(jid, "12345");

    let cmds = mgr.gateway().commands();

    // (d) STDIN pipe: the submit issues a SINGLE shell command — the
    // `printf '%s' '<body>' | sbatch …` pipe. No preceding script-write
    // (`> …`) and no `chmod` command exist, and no
    // `<root_folder>/job_<name>.sh` file is referenced.
    assert!(
        !cmds.iter().any(|c| c.contains("chmod")),
        "STDIN pipe must not chmod a script file; got: {cmds:?}",
    );
    assert!(
        !cmds.iter().any(|c| c.contains("job_myjob.sh")),
        "STDIN pipe must not write a per-secondary job script; got: {cmds:?}",
    );
    assert!(
        cmds.iter().filter(|c| c.contains("sbatch ")).count() == 1,
        "exactly one sbatch command (the STDIN pipe) is issued; got: {cmds:?}",
    );

    let sbatch = mgr.gateway().sbatch_command();
    // The submit command pipes the body over stdin to sbatch.
    assert!(
        sbatch.starts_with("printf '%s' '") && sbatch.contains("' | sbatch --parsable "),
        "submit must pipe the wrapper body to sbatch over STDIN; got: {sbatch}",
    );
    // (a) mail=ALL only.
    assert!(
        sbatch.contains("--mail-type=ALL"),
        "expected --mail-type=ALL in sbatch; got: {sbatch}",
    );
    assert!(
        !sbatch.contains("--mail-type=FAIL"),
        "--mail-type=FAIL must not appear: {sbatch}",
    );
    // (b) no --mem when memory_per_node is unset.
    assert!(
        !sbatch.contains("--mem="),
        "--mem must be omitted when memory_per_node is None; got: {sbatch}",
    );
    // (e) --ntasks=1 must be present (Python parity, locks down the
    // partition-default-ntasks-drift class of bug).
    assert!(
        sbatch.contains("--ntasks=1"),
        "--ntasks=1 must be emitted for Python-parity; got: {sbatch}",
    );
    // (f) Default-config --signal lead time. `SlurmConfig::default()`
    // sets `signal_lead_seconds = 60`; we assert against the named
    // default rather than the literal `60` so a deliberate default
    // change updates the test through one well-known field.
    let default_lead = SlurmConfig::default().signal_lead_seconds;
    assert!(
        sbatch.contains(&format!("--signal=B:SIGTERM@{default_lead}")),
        "expected --signal=B:SIGTERM@{default_lead} (default lead) in sbatch; got: {sbatch}",
    );
    // No trailing script-path argument: sbatch reads the script from
    // STDIN, so the sbatch invocation (everything after the pipe) must
    // NOT end with a `…/job_*.sh` path.
    let sbatch_invocation = sbatch
        .split_once("| sbatch ")
        .map(|(_, rest)| rest.trim_end())
        .expect("submit command must contain the `| sbatch ` pipe");
    assert!(
        !sbatch_invocation.ends_with(".sh"),
        "sbatch must not carry a trailing script-path argument (STDIN pipe); got: {sbatch_invocation}",
    );

    // Case C: memory_per_node explicitly set → --mem={val} emitted.
    let gw = SubmitRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        memory_per_node: Some("32G".into()),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    mgr.submit_job("#!/bin/sh", "j2", "secondary-0", 1, "/srv/slurm/log/run-2")
        .await
        .expect("submit succeeds");
    let sbatch = mgr.gateway().sbatch_command();
    assert!(
        sbatch.contains("--mem=32G"),
        "expected --mem=32G when memory_per_node is set; got: {sbatch}",
    );
}

/// `signal_lead_seconds` is plumbed verbatim into the sbatch flag.
/// Locks the override path: setting a non-default lead time on the
/// config must produce `--signal=B:SIGTERM@<that value>` so operators
/// tuning the teardown window get the value they configured.
#[tokio::test]
async fn submit_job_emits_signal_lead_time_flag() {
    let gw = SubmitRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        signal_lead_seconds: 90,
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    mgr.submit_job(
        "#!/bin/sh",
        "j-lead",
        "secondary-0",
        1,
        "/srv/slurm/log/run-lead",
    )
    .await
    .expect("submit succeeds");
    let sbatch = mgr.gateway().sbatch_command();
    assert!(
        sbatch.contains("--signal=B:SIGTERM@90"),
        "expected --signal=B:SIGTERM@90 when signal_lead_seconds=90; got: {sbatch}",
    );
}

/// `signal_lead_seconds = 0` skips the flag entirely. Rationale:
/// `sbatch(1)` requires `@N > 0`; passing `--signal=B:SIGTERM@0`
/// would be a hard-error from sbatch. The `0` value is the documented
/// opt-out for clusters whose `slurm.conf` disables `--signal`.
#[tokio::test]
async fn submit_job_skips_signal_flag_when_lead_seconds_is_zero() {
    let gw = SubmitRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        signal_lead_seconds: 0,
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    mgr.submit_job(
        "#!/bin/sh",
        "j-no-signal",
        "secondary-0",
        1,
        "/srv/slurm/log/run-x",
    )
    .await
    .expect("submit succeeds");
    let sbatch = mgr.gateway().sbatch_command();
    assert!(
        !sbatch.contains("--signal="),
        "--signal must be omitted when signal_lead_seconds is 0; got: {sbatch}",
    );
}

/// sbatch's own `--output`/`--error` land in the per-secondary folder
/// `<run_log_dir>/<secondary_id>/`, NOT at the run-dir root. This is the
/// same folder the container's `--full-log-dir=<root>/<sid>` writes
/// `secondary.log` into and the worker logs land in. Pins the BUG2 fix:
/// pre-fix the paths were `<run_log_dir>/slurm_%j.{out,err}` at the
/// root. Also asserts the folder is `mkdir -p`'d on the gateway before
/// sbatch (SLURM does not create the `--output=` parent directory).
#[tokio::test]
async fn submit_job_anchors_slurm_out_err_in_per_secondary_dir() {
    let gw = SubmitRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    mgr.submit_job(
        "#!/bin/sh",
        "prefix-secondary-2",
        "secondary-2",
        1,
        "/srv/slurm/log/run-1",
    )
    .await
    .expect("submit succeeds");

    let sbatch = mgr.gateway().sbatch_command();
    assert!(
        sbatch.contains("--output=/srv/slurm/log/run-1/secondary-2/slurm_%j.out"),
        "sbatch --output must land in the per-secondary folder; got: {sbatch}",
    );
    assert!(
        sbatch.contains("--error=/srv/slurm/log/run-1/secondary-2/slurm_%j.err"),
        "sbatch --error must land in the per-secondary folder; got: {sbatch}",
    );
    // Negative: no root-level slurm_%j.{out,err}.
    assert!(
        !sbatch.contains("--output=/srv/slurm/log/run-1/slurm_%j.out"),
        "sbatch --output must not be at the run-dir root; got: {sbatch}",
    );

    // The per-secondary folder is created before sbatch.
    let dirs = mgr.gateway().created_dirs();
    assert!(
        dirs.iter().any(|d| d == "/srv/slurm/log/run-1/secondary-2"),
        "per-secondary log dir must be created on the gateway; got: {dirs:?}",
    );
}
