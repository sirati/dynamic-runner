//! SLURM job-lifecycle methods on [`SlurmJobManager`]: prepare
//! directories, `sbatch` submission, cancellation, and `squeue` status
//! snapshots. Pure gateway-side command issuance; image staging lives
//! in [`images`](super::images).

use dynrunner_gateway::traits::Gateway;
use tracing;

use super::types::{JobStatus, JobStatusInfo, SlurmError, SlurmJobManager};

impl<G: Gateway> SlurmJobManager<G> {
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
        let write_cmd =
            format!("printf '%s' '{escaped}' > {script_path} && chmod +x {script_path}");
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
        ];

        // Pre-SIGKILL warning window: `--signal=B:SIGTERM@<N>` tells
        // SLURM to deliver SIGTERM to the batch script (`B:` prefix —
        // not the srun steps) `<N>` seconds before the `--time` limit.
        // Placed directly after `--time` because the lead time is
        // expressed relative to that limit; operators reading the
        // rendered command see the two related flags adjacent.
        //
        // The wrapper's trap → shutdown-manager forwarding chain uses
        // this window for container teardown + secondary signalling +
        // `/tmp` cleanup before SLURM's `KillWait`-driven SIGKILL.
        //
        // `signal_lead_seconds = 0` skips the flag (sbatch(1) requires
        // `@N` > 0); operators on clusters whose `slurm.conf` disables
        // `--signal` set 0 to opt out. Same opt-in shape as `--mem`.
        if self.config.signal_lead_seconds > 0 {
            sbatch_args.push(format!(
                "--signal=B:SIGTERM@{}",
                self.config.signal_lead_seconds
            ));
        }

        sbatch_args.push(format!("--output={run_log_dir}/slurm_%j.out"));
        sbatch_args.push(format!("--error={run_log_dir}/slurm_%j.err"));

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
            return Err(SlurmError::Command("sbatch returned empty job ID".into()));
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
