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

/// A thread-local default subscriber for tests that drive
/// `build_and_transfer_images` WITHOUT asserting on tracing.
///
/// Load-bearing, not cosmetic: those tests share `images.rs`'s
/// important-target callsites with
/// `important_events::build_and_transfer_emits_building_and_uploading_important_events`,
/// which captures them through a thread-local `set_default` subscriber.
/// `tracing`'s per-callsite Interest cache is GLOBAL: a first hit of a
/// shared callsite from a thread with NO dispatcher can race the capture
/// test's `set_default` interest-cache rebuild and stamp
/// `Interest::never` AFTER the rebuild stamped `always` — permanently
/// muting that callsite for the capture (observed: the capture saw only
/// the first of the two events, ~5% of parallel full-suite runs;
/// reproduced 3/40 on a tight `build_and_transfer` filter loop).
/// Installing a plain `Registry` here means every interest computation
/// this thread can ever contribute is "interested", so the poisoned
/// state is unreachable. Events go to the `Registry` sink (dropped) —
/// these tests assert on the packager boundary, not on tracing.
fn tracing_interest_guard() -> tracing::subscriber::DefaultGuard {
    tracing::subscriber::set_default(tracing_subscriber::registry())
}

#[tokio::test]
async fn build_and_transfer_images_forwards_to_packager() {
    let _tracing_guard = tracing_interest_guard();
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

    let _tracing_guard = tracing_interest_guard();
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
            None,
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
    // (g) --no-requeue on every framework sbatch: SLURM auto-requeue can
    // only resurrect a killed member as a re-admission-refused ghost
    // (the framework owns replacement via fresh-identity respawn).
    assert!(
        sbatch.contains("--no-requeue"),
        "every framework sbatch must carry --no-requeue; got: {sbatch}",
    );
    // (h) The initial-cohort submit passed `exclude_node = None`, so no
    // `--exclude` flag is emitted (a blank `--exclude=` hard-errors
    // sbatch — the None case must omit it cleanly).
    assert!(
        !sbatch.contains("--exclude"),
        "no --exclude when exclude_node is None; got: {sbatch}",
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
    mgr.submit_job("#!/bin/sh", "j2", "secondary-0", 1, "/srv/slurm/log/run-2", None)
        .await
        .expect("submit succeeds");
    let sbatch = mgr.gateway().sbatch_command();
    assert!(
        sbatch.contains("--mem=32G"),
        "expected --mem=32G when memory_per_node is set; got: {sbatch}",
    );
}

/// A submit that carries `exclude_node = Some(node)` emits
/// `--exclude=<node>` (the respawn-onto-a-different-node path), while
/// `None` omits the flag entirely (asserted in
/// `submit_job_matches_python_invocation_shape`). One flag, opt-in by
/// the caller — the respawn provider passes the dead member's node.
#[tokio::test]
async fn submit_job_emits_exclude_when_node_known() {
    let gw = SubmitRecordingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    mgr.submit_job(
        "#!/bin/sh",
        "j-respawn",
        "secondary-5",
        1,
        "/srv/slurm/log/run-3",
        Some("krater07"),
    )
    .await
    .expect("submit succeeds");
    let sbatch = mgr.gateway().sbatch_command();
    assert!(
        sbatch.contains("--exclude=krater07"),
        "expected --exclude=krater07 when exclude_node is Some; got: {sbatch}",
    );
}

/// Gateway that REJECTS any sbatch carrying `--exclude` (rc=1, mirroring
/// SLURM's "Invalid node name specified") and ACCEPTS a bare one. Records
/// every command so the test can assert the retry happened.
#[derive(Default)]
struct ExcludeRejectingGateway {
    commands: Mutex<Vec<String>>,
}

impl ExcludeRejectingGateway {
    fn sbatch_commands(&self) -> Vec<String> {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.contains("| sbatch "))
            .cloned()
            .collect()
    }
}

impl Gateway for ExcludeRejectingGateway {
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
        if cmd.contains("| sbatch ") {
            if cmd.contains("--exclude=") {
                return Ok(CommandResult {
                    return_code: 1,
                    stdout: String::new(),
                    stderr: "sbatch: error: Batch job submission failed: \
                             Invalid node name specified"
                        .into(),
                });
            }
            return Ok(CommandResult {
                return_code: 0,
                stdout: "55555".into(),
                stderr: String::new(),
            });
        }
        Ok(CommandResult {
            return_code: 0,
            stdout: String::new(),
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, _local: &Path, _remote: &str) -> Result<(), GatewayError> {
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

/// REGRESSION (run_20260612_095601): an sbatch carrying `--exclude=<bad
/// node>` is rejected outright ("Invalid node name specified") and the
/// respawn pipeline died with no retry. The submission seam must now
/// retry ONCE without `--exclude` so the spawn proceeds regardless of
/// whether the excluded node string is a valid SLURM NodeName.
#[tokio::test]
async fn submit_job_retries_bare_when_exclude_submission_fails() {
    let gw = ExcludeRejectingGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    let jid = mgr
        .submit_job(
            "#!/bin/sh\necho hi",
            "j-respawn",
            "secondary-5",
            1,
            "/srv/slurm/log/run-r",
            // A node string SLURM rejects (e.g. a container hostname /
            // FQDN that is not the cluster's NodeName).
            Some("worker3.container.invalid"),
        )
        .await
        .expect("the bare retry must let the spawn succeed");
    // The bare retry's job id is returned.
    assert_eq!(jid, "55555");

    let sbatch = mgr.gateway().sbatch_commands();
    // Exactly two sbatch attempts: the rejected exclusion + the bare
    // retry.
    assert_eq!(
        sbatch.len(),
        2,
        "expected one rejected exclusion attempt + one bare retry; got: {sbatch:?}",
    );
    assert!(
        sbatch[0].contains("--exclude=worker3.container.invalid"),
        "first attempt must carry the exclusion; got: {}",
        sbatch[0],
    );
    assert!(
        !sbatch[1].contains("--exclude"),
        "retry must drop --exclude; got: {}",
        sbatch[1],
    );
    // The marker bookkeeping is clean: only the retry's real id is on
    // `job_ids` (the failed attempt removed its own marker).
    assert_eq!(
        mgr.job_ids(),
        &["55555".to_string()],
        "only the successful retry's id must be on job_ids",
    );
}

/// A submission with NO exclusion that fails has nothing to retry: the
/// error surfaces directly, no second attempt, and the marker is
/// cleaned up. Guards against the retry firing on the non-exclusion
/// path.
#[tokio::test]
async fn submit_job_no_retry_when_no_exclusion_present() {
    // ExcludeRejectingGateway only rejects `--exclude` sbatches; to test
    // the no-exclusion failure path, drive a gateway that fails ALL
    // sbatches.
    #[derive(Default)]
    struct AllSbatchFailGateway {
        sbatch_calls: AtomicUsize,
    }
    impl Gateway for AllSbatchFailGateway {
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
            if cmd.contains("| sbatch ") {
                self.sbatch_calls.fetch_add(1, Ordering::SeqCst);
                return Ok(CommandResult {
                    return_code: 1,
                    stdout: String::new(),
                    stderr: "sbatch: error: simulated".into(),
                });
            }
            Ok(CommandResult {
                return_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
        async fn transfer_file(&self, _l: &Path, _r: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn download_file(&self, _r: &str, _l: &Path) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn create_directory(&self, _r: &str) -> Result<(), GatewayError> {
            Ok(())
        }
        async fn file_exists(&self, _r: &str) -> Result<bool, GatewayError> {
            Ok(false)
        }
        fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
            Ok(())
        }
    }

    let gw = AllSbatchFailGateway::default();
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    let err = mgr
        .submit_job(
            "#!/bin/sh\necho hi",
            "j0",
            "secondary-0",
            1,
            "/srv/slurm/log/run-0",
            None,
        )
        .await
        .expect_err("a no-exclusion submission failure must surface");
    assert!(matches!(err, SlurmError::Command(_)));
    // Exactly ONE sbatch attempt — no retry on the no-exclusion path.
    assert_eq!(
        mgr.gateway().sbatch_calls.load(Ordering::SeqCst),
        1,
        "a no-exclusion failure must not retry",
    );
    // The marker was cleaned up on the failure path.
    assert!(
        mgr.job_ids().is_empty(),
        "a failed submission must leave no marker on job_ids",
    );
}

/// Gateway answering node-resolution probes from canned outputs:
/// `squeue -j … -o '%N'` returns `squeue_out`, `sacct … NodeList`
/// returns `sacct_out`. sbatch is answered with a canned id so a job can
/// be registered into `secondary_jobs` via a real `submit_job`. Each
/// probe's rc tracks whether its output is empty (a missing job row is
/// rc!=0 / empty in the real tools — both map to "no node here").
struct ResolveGateway {
    squeue_out: String,
    sacct_out: String,
}

impl Gateway for ResolveGateway {
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
        if cmd.contains("squeue") && cmd.contains("-o '%N'") {
            return Ok(CommandResult {
                return_code: 0,
                stdout: self.squeue_out.clone(),
                stderr: String::new(),
            });
        }
        if cmd.contains("sacct") && cmd.contains("NodeList") {
            return Ok(CommandResult {
                return_code: 0,
                stdout: self.sacct_out.clone(),
                stderr: String::new(),
            });
        }
        let stdout = if cmd.contains("| sbatch ") {
            "42424".to_string()
        } else {
            String::new()
        };
        Ok(CommandResult {
            return_code: 0,
            stdout,
            stderr: String::new(),
        })
    }
    async fn transfer_file(&self, _l: &Path, _r: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn download_file(&self, _r: &str, _l: &Path) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn create_directory(&self, _r: &str) -> Result<(), GatewayError> {
        Ok(())
    }
    async fn file_exists(&self, _r: &str) -> Result<bool, GatewayError> {
        Ok(false)
    }
    fn setup_port_forwarding(&mut self, _l: u16, _r: u16) -> Result<(), GatewayError> {
        Ok(())
    }
}

async fn resolve_fixture(squeue_out: &str, sacct_out: &str) -> SlurmJobManager<ResolveGateway> {
    let gw = ResolveGateway {
        squeue_out: squeue_out.to_string(),
        sacct_out: sacct_out.to_string(),
    };
    let cfg = SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    };
    let mut mgr = SlurmJobManager::new(cfg, gw);
    // Register `secondary-0`'s job so the resolver can map it.
    mgr.submit_job(
        "#!/bin/sh\n",
        "j0",
        "secondary-0",
        1,
        "/srv/slurm/log/run-x",
        None,
    )
    .await
    .expect("submit succeeds");
    mgr
}

/// squeue still reports the node (the job had not yet been reaped) →
/// resolution returns it without consulting sacct.
#[tokio::test]
async fn resolve_excluded_node_prefers_squeue() {
    let mgr = resolve_fixture("krater04", "SHOULD-NOT-BE-USED").await;
    assert_eq!(
        mgr.resolve_excluded_node("secondary-0").await.as_deref(),
        Some("krater04"),
    );
}

/// squeue is empty (the job left the queue — the common respawn-time
/// case) → resolution falls through to sacct's NodeList.
#[tokio::test]
async fn resolve_excluded_node_falls_back_to_sacct() {
    let mgr = resolve_fixture("", "krater09").await;
    assert_eq!(
        mgr.resolve_excluded_node("secondary-0").await.as_deref(),
        Some("krater09"),
    );
}

/// Neither squeue nor sacct yields a node → `None` (the spawn proceeds
/// without `--exclude`). The SLURM placeholder `None assigned` is
/// treated as no node.
#[tokio::test]
async fn resolve_excluded_node_none_when_unresolvable() {
    let mgr = resolve_fixture("", "None assigned").await;
    assert_eq!(mgr.resolve_excluded_node("secondary-0").await, None);
}

/// A member the manager never submitted a job for → no `secondary_jobs`
/// entry → `None` (no job id to probe).
#[tokio::test]
async fn resolve_excluded_node_none_for_unknown_member() {
    let mgr = resolve_fixture("krater04", "krater04").await;
    assert_eq!(mgr.resolve_excluded_node("secondary-99").await, None);
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
        None,
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
        None,
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
        None,
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
