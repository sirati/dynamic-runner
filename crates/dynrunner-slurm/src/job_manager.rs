use std::collections::HashSet;
use std::path::{Path, PathBuf};

use dynrunner_core::TaskInfo;
use dynrunner_gateway::traits::{Gateway, GatewayError};
use tracing;

use crate::config::SlurmConfig;
use crate::packaging::{PackagingError, PodmanImageMetadata, PodmanPackaging};

/// Status of a SLURM job (parsed from the raw squeue state string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Unknown(String),
}

/// Full snapshot returned by `get_job_status`.
///
/// `state`/`state_kind` are `None` when squeue had no record for the
/// job (transient query failure or post-purge). The Python wrapper
/// exposes that as `state="UNKNOWN"` to mirror the historical
/// `SlurmJobManager.get_job_status` shape; Rust callers that need the
/// "no longer in queue → presumed completed" interpretation should
/// layer it themselves rather than have it baked in here, because
/// the squeue purge horizon and "actually completed" are not the
/// same thing on every cluster.
#[derive(Debug, Clone)]
pub struct JobStatusInfo {
    /// Raw squeue state string (e.g. "RUNNING", "PENDING"). `None` if
    /// the job had no row in squeue's output.
    pub state: Option<String>,
    /// Parsed `JobStatus` for Rust callers. `None` mirrors `state`.
    pub state_kind: Option<JobStatus>,
    /// Node assignment from squeue (`%N`); empty when unknown.
    pub node: String,
    /// Reason field from squeue (`%r`); empty when unknown.
    pub reason: String,
}

/// Manages SLURM job submission and lifecycle via a `Gateway`.
pub struct SlurmJobManager<G: Gateway> {
    pub config: SlurmConfig,
    gateway: G,
    job_ids: Vec<String>,
}

impl<G: Gateway> SlurmJobManager<G> {
    pub fn new(config: SlurmConfig, gateway: G) -> Self {
        Self {
            config,
            gateway,
            job_ids: Vec::new(),
        }
    }

    pub fn job_ids(&self) -> &[String] {
        &self.job_ids
    }

    pub fn gateway(&self) -> &G {
        &self.gateway
    }

    pub fn gateway_mut(&mut self) -> &mut G {
        &mut self.gateway
    }

    /// Build the container image and transfer it to the gateway.
    ///
    /// Pure delegation to a `PodmanPackaging` implementation: the
    /// job manager owns SLURM lifecycle, NOT image-build technology.
    /// Mirrors the Python `SlurmJobManager.build_and_transfer_images`
    /// in `packaging/job_manager.py` — the image is placed under
    /// `slurm_config.image_path()` and the resulting metadata
    /// (remote path, content hash, upload-vs-cache outcome) is
    /// returned to the caller for use in wrapper-script generation.
    ///
    /// `local_project_root` is forwarded to the packager unchanged
    /// (the build's source tree, e.g. the directory containing
    /// `flake.nix`).
    pub async fn build_and_transfer_images<P>(
        &self,
        packager: &P,
        local_project_root: &Path,
    ) -> Result<PodmanImageMetadata, SlurmError>
    where
        P: PodmanPackaging<G>,
    {
        tracing::info!("Building and transferring container image...");
        let output_dir = self.config.image_path();
        let metadata = packager
            .build_images(&self.gateway, local_project_root, Path::new(&output_dir))
            .await?;
        tracing::info!(
            remote_path = %metadata.remote_path.display(),
            uploaded = metadata.uploaded,
            "container image ready on gateway",
        );
        Ok(metadata)
    }

    /// Upload each binary's underlying file to `<srcbins>/<rel>` on the
    /// gateway so the wrapper's read-only bind-mount of srcbins into
    /// `/app/src-network` actually has the staged source.
    ///
    /// Without this the StageFile pipeline (which tells the secondary
    /// "the file is now at src_network/<rel_path>") points at an empty
    /// directory and every TaskAssignment surfaces as "not pre-staged"
    /// — the framework had no primitive that turned the consumer's
    /// local `--source` tree into a populated `src_network` view on
    /// the cluster.
    ///
    /// Caller-side gating decides WHEN to call this (file-based task,
    /// not `--source-already-staged`); this method assumes the caller
    /// already wants the upload.
    ///
    /// `binary.path` may be:
    ///
    /// * absolute under `source_root` — uploaded to `<srcbins>/<rel>`
    ///   where `<rel>` is the strip-prefixed tail (legacy shape);
    /// * absolute out-of-tree — skipped; the StageFile record ships
    ///   the absolute path which the secondary's `stage_file` handler
    ///   treats as out-of-band-staged (must already exist on the
    ///   secondary by some other means);
    /// * relative — joined with `source_root` for the on-disk read;
    ///   uploaded to `<srcbins>/<binary.path>` verbatim. This is the
    ///   wire-identifier shape consumers should prefer post-Bug-B
    ///   (mirrors the Rust `queue_initial_staging` fix in
    ///   `crates/dynrunner-pyo3/src/managers/primary.rs` and the
    ///   Python `upload_source_binaries` fix in d5d0604).
    ///
    /// Strip-prefix is purely lexical (no canonicalize), matching
    /// `queue_initial_staging`. Symlinked source trees would diverge
    /// from the Python `Path.resolve()` shape uniformly across both
    /// sites — that's a separate latent issue not in this fix's scope.
    pub async fn upload_source_binaries<I>(
        &self,
        binaries: &[TaskInfo<I>],
        source_root: &Path,
    ) -> Result<(), SlurmError> {
        let srcbins_dir = PathBuf::from(self.config.src_bins_path());
        tracing::info!(
            count = binaries.len(),
            srcbins_dir = %srcbins_dir.display(),
            "uploading source files to gateway",
        );

        // Track parent dirs we've already requested so a flat tree
        // doesn't issue N redundant `mkdir -p` round-trips when every
        // file lives under the same subdirectory.
        let mut created_dirs: HashSet<PathBuf> = HashSet::new();
        created_dirs.insert(srcbins_dir.clone());

        let mut uploaded = 0usize;
        for binary in binaries {
            // Resolve the on-disk read location: relative paths join
            // against source_root (post-Bug-B wire-id shape — mirrors
            // the Rust queue_initial_staging fix); absolute paths use
            // binary.path verbatim.
            let local: PathBuf = if binary.path.is_absolute() {
                binary.path.clone()
            } else {
                source_root.join(&binary.path)
            };
            let rel = match local.strip_prefix(source_root) {
                Ok(p) => p.to_path_buf(),
                Err(_) => {
                    tracing::warn!(
                        raw = %binary.path.display(),
                        resolved = %local.display(),
                        source_root = %source_root.display(),
                        "binary is not under --source root; skipping upload \
                         (absolute path will ship as out-of-band; secondary \
                         must already see it).",
                    );
                    continue;
                }
            };
            let remote = srcbins_dir.join(&rel);
            if let Some(parent) = remote.parent()
                && created_dirs.insert(parent.to_path_buf())
            {
                self.gateway
                    .create_directory(&parent.to_string_lossy())
                    .await?;
            }
            self.gateway
                .transfer_file(&local, &remote.to_string_lossy())
                .await?;
            uploaded += 1;
        }
        tracing::info!(
            uploaded,
            total = binaries.len(),
            "source-binary upload complete",
        );
        Ok(())
    }

    /// Create required directories on the gateway.
    pub async fn prepare_directories(&self) -> Result<(), SlurmError> {
        for dir in [
            &self.config.image_path(),
            &self.config.src_bins_path(),
            &self.config.output_path(),
            &self.config.log_path(),
        ] {
            self.gateway.create_directory(dir).await?;
        }
        tracing::info!("SLURM directories prepared on gateway");
        Ok(())
    }

    /// Submit a SLURM job using the given wrapper script content.
    ///
    /// The script is written to `<root_folder>/job_<job_name>.sh` on
    /// the gateway and then submitted via `sbatch --parsable`. Script
    /// placement, sbatch argument order, `--ntasks=1`, `--mail-type=ALL`,
    /// and `--mail-user=…` all mirror the legacy Python
    /// `SlurmJobManager.submit_job` in `packaging/job_manager.py` so a
    /// Rust-driven submission produces the same sbatch invocation a
    /// Python-driven one would.
    ///
    /// Two intentional divergences from the legacy Python:
    ///
    /// * **Script write/chmod is one shell command** (`printf … > path
    ///   && chmod +x path`) rather than two (`cat << EOFSCRIPT …
    ///   EOFSCRIPT` + `chmod +x`). Functionally equivalent but saves an
    ///   ssh round-trip on `SshGateway` and avoids the heredoc-marker
    ///   collision risk if a wrapper ever contains a literal
    ///   `\nEOFSCRIPT\n`. Single-quote escaping (`'` → `'\''`) keeps
    ///   `$VAR` and other shell metacharacters literal.
    /// * **`--mem={memory_per_node}` is opt-in** rather than always-off.
    ///   Python never emits `--mem` (the field isn't in its sbatch
    ///   argument list); the Rust path keeps the same default
    ///   (`memory_per_node = None` → no `--mem`) but lets an operator
    ///   that sets it explicitly get the `sbatch --mem=` cap. No-op for
    ///   any caller using the Python-default config.
    ///
    /// `run_log_dir` is used verbatim as the prefix of the
    /// `--output=`/`--error=` paths. Tilde expansion (`~/…` →
    /// `/home/u/…`) is the caller's responsibility: the bash shell
    /// expands a leading `~` for the trailing script-path argument and
    /// for redirected paths in the write command, but it does NOT
    /// expand `~` after `=` in `--output=~/…` style arguments, so
    /// callers that hand a `~`-prefixed `run_log_dir` to `submit_job`
    /// will end up with sbatch literally writing to `~/…`. The PyO3
    /// bridge (see `crates/dynrunner-pyo3/src/slurm/job_manager.rs`)
    /// expands tilde against the Python gateway's `remote_home` before
    /// forwarding here, matching the legacy Python `_expand_path` call
    /// site.
    pub async fn submit_job(
        &mut self,
        wrapper_script: &str,
        job_name: &str,
        nodes: u32,
        run_log_dir: &str,
    ) -> Result<String, SlurmError> {
        // Write script to gateway. Python lays the wrapper directly in
        // `root_folder` as `job_<name>.sh` (NOT under `log_subfolder`)
        // — keeping that location so a side-by-side cluster has a
        // single canonical path for the submitted script regardless of
        // which binding launched the job.
        let script_path = format!("{}/job_{job_name}.sh", self.config.root_folder);
        let escaped = wrapper_script.replace('\'', "'\\''");
        let write_cmd = format!("printf '%s' '{escaped}' > {script_path} && chmod +x {script_path}");
        let result = self.gateway.execute_command(&write_cmd, None).await?;
        if !result.success() {
            return Err(SlurmError::Command(format!(
                "failed to write wrapper script: {}",
                result.stderr
            )));
        }

        // Argument order mirrors the legacy Python `submit_job` so
        // operators eyeballing the rendered command see the same flag
        // sequence either binding produces. The order is sbatch-
        // semantics-insensitive (sbatch accepts flags in any order), so
        // this is purely a parity guarantee.
        let mut sbatch_args = vec![
            "sbatch".to_string(),
            "--parsable".to_string(),
            format!("--job-name={job_name}"),
            format!("--nodes={nodes}"),
            // `--ntasks=1` matches Python: every wrapper script SLURM
            // launches is a single secondary process, regardless of how
            // many cpus-per-task the partition allocates. Without it,
            // some sites default `ntasks` to the partition's default
            // (often > 1) and srun-based launchers downstream pick the
            // wrong proc count.
            "--ntasks=1".to_string(),
            format!("--cpus-per-task={}", self.config.cpus_per_task),
            format!("--partition={}", self.config.partition),
            format!("--time={}", self.config.time_limit),
            format!("--output={run_log_dir}/slurm_%j.out"),
            format!("--error={run_log_dir}/slurm_%j.err"),
        ];

        // `--mem` is intentionally opt-in (Python never emits it). See
        // the method doc-comment for the rationale; default-config
        // callers get the same `sbatch` invocation either binding
        // produces.
        if let Some(mem) = &self.config.memory_per_node {
            sbatch_args.push(format!("--mem={mem}"));
        }
        if let Some(email) = &self.config.notify_email {
            // Mirror Python's flag order: `--mail-type` before
            // `--mail-user` (cosmetic — sbatch is order-insensitive).
            sbatch_args.push("--mail-type=ALL".to_string());
            sbatch_args.push(format!("--mail-user={email}"));
        }

        sbatch_args.push(script_path);

        let cmd = sbatch_args.join(" ");
        let result = self.gateway.execute_command(&cmd, None).await?;

        if !result.success() {
            return Err(SlurmError::Command(format!(
                "sbatch failed: {}",
                result.stderr
            )));
        }

        let job_id = result.stdout.trim().to_string();
        if job_id.is_empty() {
            return Err(SlurmError::Command(
                "sbatch returned empty job ID".into(),
            ));
        }

        tracing::info!(job_id = %job_id, job_name, "SLURM job submitted");
        self.job_ids.push(job_id.clone());
        Ok(job_id)
    }

    /// Cancel a specific SLURM job.
    pub async fn cancel_job(&self, job_id: &str) -> Result<(), SlurmError> {
        let cmd = format!("scancel {job_id}");
        let result = self.gateway.execute_command(&cmd, None).await?;
        if !result.success() {
            tracing::warn!(job_id, stderr = %result.stderr, "scancel returned error");
        }
        tracing::info!(job_id, "SLURM job cancelled");
        Ok(())
    }

    /// Cancel all submitted jobs.
    ///
    /// Mirrors the Python `SlurmJobManager.cancel_all_jobs` shape:
    /// after iterating, `self.job_ids` is cleared so a subsequent
    /// `cancel_all_jobs` is a no-op rather than re-cancelling already-
    /// cancelled IDs.
    pub async fn cancel_all_jobs(&mut self) -> Result<(), SlurmError> {
        // Drain into a temporary so the borrow on `self.job_ids` is
        // released before we start awaiting `cancel_job(&self, ...)`.
        let ids: Vec<String> = self.job_ids.drain(..).collect();
        for job_id in &ids {
            if let Err(e) = self.cancel_job(job_id).await {
                tracing::warn!(job_id, error = %e, "failed to cancel job");
            }
        }
        Ok(())
    }

    /// Query the status of a SLURM job.
    ///
    /// Returns the full state/node/reason snapshot from a single
    /// `squeue -o '%T|%N|%r'` line. When the job is missing from
    /// squeue (already purged, transient failure), `state` and
    /// `state_kind` are `None` and `node`/`reason` are empty —
    /// callers that want a "missing → completed" interpretation
    /// apply it explicitly.
    pub async fn get_job_status(&self, job_id: &str) -> Result<JobStatusInfo, SlurmError> {
        let cmd = format!("squeue -j {job_id} -o '%T|%N|%r' --noheader 2>/dev/null");
        let result = self.gateway.execute_command(&cmd, None).await?;

        if !result.success() || result.stdout.trim().is_empty() {
            return Ok(JobStatusInfo {
                state: None,
                state_kind: None,
                node: String::new(),
                reason: String::new(),
            });
        }

        let line = result.stdout.trim();
        let mut parts = line.split('|');
        let state_str = parts.next().unwrap_or("").to_string();
        let node = parts.next().unwrap_or("").to_string();
        let reason = parts.next().unwrap_or("").to_string();

        let state_kind = match state_str.as_str() {
            "PENDING" => JobStatus::Pending,
            "RUNNING" => JobStatus::Running,
            "COMPLETED" | "COMPLETING" => JobStatus::Completed,
            "FAILED" | "NODE_FAIL" | "TIMEOUT" => JobStatus::Failed,
            "CANCELLED" => JobStatus::Cancelled,
            other => JobStatus::Unknown(other.to_string()),
        };

        Ok(JobStatusInfo {
            state: Some(state_str),
            state_kind: Some(state_kind),
            node,
            reason,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SlurmError {
    #[error("gateway error: {0}")]
    Gateway(#[from] GatewayError),
    #[error("command error: {0}")]
    Command(String),
    #[error("packaging error: {0}")]
    Packaging(#[from] PackagingError),
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;
    use dynrunner_gateway::local::LocalGateway;
    use dynrunner_gateway::traits::CommandResult;

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
        assert_eq!(metadata.remote_path, PathBuf::from("/srv/slurm/image_bin/app.tar.gz"));
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
    }

    impl SubmitRecordingGateway {
        fn commands(&self) -> Vec<String> {
            self.commands.lock().unwrap().clone()
        }

        fn sbatch_command(&self) -> String {
            self.commands
                .lock()
                .unwrap()
                .iter()
                .find(|c| c.starts_with("sbatch "))
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
            // Only `sbatch --parsable` is expected to produce stdout;
            // anything else (e.g. the `printf … > path` script-write)
            // is silent in the real shell.
            let stdout = if cmd.starts_with("sbatch ") {
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
    /// (d) Wrapper script lands at `<root_folder>/job_<name>.sh`
    ///     (Python placement; the negative assertion guards against
    ///     regression to the historical `<log_path>/wrapper_<name>.sh`).
    /// (e) `--ntasks=1` IS emitted (legacy Python had it; Rust
    ///     previously omitted it — a parity gap that this assertion
    ///     locks down so `sbatch` defaults can't drift the launched
    ///     proc count on partitions whose default ntasks is > 1).
    #[tokio::test]
    async fn submit_job_matches_python_invocation_shape() {
        // Case A+B+D: defaults — no mem, mail=ALL on notify, script in root.
        let gw = SubmitRecordingGateway::default();
        let cfg = SlurmConfig {
            root_folder: "/srv/slurm".into(),
            notify_email: Some("ops@example.com".into()),
            ..SlurmConfig::default()
        };
        let mut mgr = SlurmJobManager::new(cfg, gw);
        let jid = mgr
            .submit_job("#!/bin/sh\necho hi", "myjob", 1, "/srv/slurm/log/run-1")
            .await
            .expect("submit succeeds");
        assert_eq!(jid, "12345");

        let cmds = mgr.gateway().commands();

        // (d) Script path: `<root_folder>/job_<job_name>.sh`.
        let write_cmd = cmds
            .iter()
            .find(|c| !c.starts_with("sbatch "))
            .expect("script-write command must precede sbatch");
        assert!(
            write_cmd.contains("/srv/slurm/job_myjob.sh"),
            "wrapper script must land under root_folder, got: {write_cmd}",
        );
        assert!(
            !write_cmd.contains("/log/wrapper_"),
            "wrapper must not land at <log_path>/wrapper_<name>.sh: {write_cmd}",
        );

        let sbatch = mgr.gateway().sbatch_command();
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
        // sbatch line ends with the script path argument.
        assert!(
            sbatch.ends_with("/srv/slurm/job_myjob.sh"),
            "sbatch must terminate with the script path; got: {sbatch}",
        );

        // Case C: memory_per_node explicitly set → --mem={val} emitted.
        let gw = SubmitRecordingGateway::default();
        let cfg = SlurmConfig {
            root_folder: "/srv/slurm".into(),
            memory_per_node: Some("32G".into()),
            ..SlurmConfig::default()
        };
        let mut mgr = SlurmJobManager::new(cfg, gw);
        mgr.submit_job("#!/bin/sh", "j2", 1, "/srv/slurm/log/run-2")
            .await
            .expect("submit succeeds");
        let sbatch = mgr.gateway().sbatch_command();
        assert!(
            sbatch.contains("--mem=32G"),
            "expected --mem=32G when memory_per_node is set; got: {sbatch}",
        );
    }
}
