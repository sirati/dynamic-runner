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
    /// The script is piped to `sbatch` over STDIN
    /// (`printf '%s' '<escaped>' | sbatch --parsable <flags…>`): sbatch
    /// reads the batch script from stdin when no trailing path argument
    /// is given, so NO per-secondary `job_<name>.sh` file is written to
    /// the gateway and NO `chmod +x` is needed. sbatch argument order,
    /// `--ntasks=1`, `--mail-type=ALL`, and `--mail-user=…` all mirror
    /// the legacy Python `SlurmJobManager.submit_job` in
    /// `packaging/job_manager.py` so a Rust-driven submission produces
    /// the same sbatch invocation a Python-driven one would.
    ///
    /// The script body is single-quote escaped (`'` → `'\''`) for the
    /// `printf '%s' '…'` literal, so `$VAR` and other shell
    /// metacharacters reach sbatch verbatim. `--wrap` is deliberately
    /// NOT used: it would re-shell the body and risk a double word-split
    /// of the wrapper's `exec` line.
    ///
    /// One intentional divergence from the legacy Python:
    ///
    /// * **`--mem={memory_per_node}` is opt-in** rather than always-off.
    ///   Python never emits `--mem` (the field isn't in its sbatch
    ///   argument list); the Rust path keeps the same default
    ///   (`memory_per_node = None` → no `--mem`) but lets an operator
    ///   that sets it explicitly get the `sbatch --mem=` cap. No-op for
    ///   any caller using the Python-default config.
    ///
    /// `<run_log_dir>/<secondary_id>` is the prefix of the
    /// `--output=`/`--error=` paths: the SLURM batch-script stdout/stderr
    /// (`slurm_<jobid>.out`/`.err`) land in the SAME per-secondary folder
    /// the worker and role logs use, rather than spilling at the run-dir
    /// root. The folder is `mkdir -p`'d on the gateway before sbatch runs
    /// because SLURM does NOT create the parent directory for an
    /// `--output=` path. Tilde expansion (`~/…` → `/home/u/…`) is the
    /// caller's responsibility: the bash shell does NOT expand `~` after
    /// `=` in `--output=~/…` style arguments, so callers that hand a
    /// `~`-prefixed `run_log_dir` to `submit_job` will end up with sbatch
    /// literally writing to `~/…`. The PyO3 bridge (see
    /// `crates/dynrunner-pyo3/src/slurm/job_manager.rs`) expands tilde
    /// against the Python gateway's `remote_home` before forwarding here,
    /// matching the legacy Python `_expand_path` call site.
    ///
    /// `secondary_id` is known at submit time in every path: the initial
    /// cohort assigns `secondary-{i}` deterministically by index before
    /// rendering the wrapper, and the respawn path carries the
    /// replacement id. SLURM's own job id (`%j`) is still resolved by
    /// SLURM at job start and appended to the filename.
    pub async fn submit_job(
        &mut self,
        wrapper_script: &str,
        job_name: &str,
        secondary_id: &str,
        nodes: u32,
        run_log_dir: &str,
    ) -> Result<String, SlurmError> {
        // The wrapper body is piped to sbatch over STDIN below; escape it
        // once for the `printf '%s' '<body>'` single-quoted literal so
        // every `$VAR` / metacharacter reaches sbatch verbatim. No
        // per-secondary `job_<name>.sh` is written and no `chmod +x` is
        // needed — sbatch reads the batch script from stdin when no
        // trailing path argument is given.
        let escaped = wrapper_script.replace('\'', "'\\''");

        // Per-secondary log folder for sbatch's own stdout/stderr.
        // SLURM does NOT create the parent directory for an `--output=`
        // path, so `mkdir -p` it here (before sbatch) — otherwise the
        // batch job's `slurm_<jobid>.{out,err}` fail to open and the
        // job's own diagnostics vanish. Same folder the container's
        // `--full-log-dir=<root>/<sid>` writes `secondary.log` into and
        // the worker logs land in.
        let sec_log_dir = format!("{run_log_dir}/{secondary_id}");
        self.gateway.create_directory(&sec_log_dir).await?;

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

        sbatch_args.push(format!("--output={sec_log_dir}/slurm_%j.out"));
        sbatch_args.push(format!("--error={sec_log_dir}/slurm_%j.err"));

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

        // No trailing script-path argument: sbatch reads the batch
        // script from STDIN, which the `printf '%s' '<body>' |` prefix
        // supplies. One shell command, no gateway-side script file.
        let cmd = format!("printf '%s' '{escaped}' | {}", sbatch_args.join(" "));
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
