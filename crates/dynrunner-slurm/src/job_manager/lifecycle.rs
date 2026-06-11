//! SLURM job-lifecycle methods on [`SlurmJobManager`]: prepare
//! directories, `sbatch` submission, cancellation, and `squeue` status
//! snapshots. Pure gateway-side command issuance; image staging lives
//! in [`images`](super::images).

use dynrunner_gateway::traits::Gateway;
use tracing;

use super::types::{
    CancelOutcome, CancelVerifyPolicy, JobStatus, JobStatusInfo, SlurmError, SlurmJobManager,
};

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
    ///
    /// Returns what scancel actually did (see [`CancelOutcome`]):
    /// `Cancelled` on a clean exit, `AlreadyGone` when scancel ran but
    /// reported an error (on a reachable gateway that means the job id
    /// is no longer known — finished, cancelled, or purged), and `Err`
    /// only when the gateway transport itself failed (scancel never
    /// ran). Logging here stays severity-neutral (debug for the gone
    /// case) so callers with different stakes — best-effort revocation
    /// vs. teardown sweep — pick their own loudness.
    pub async fn cancel_job(&self, job_id: &str) -> Result<CancelOutcome, SlurmError> {
        let cmd = format!("scancel {job_id}");
        let result = self.gateway.execute_command(&cmd, None).await?;
        if !result.success() {
            tracing::debug!(
                job_id,
                stderr = %result.stderr,
                "scancel reported an error — the job is likely already \
                 finished, cancelled, or purged",
            );
            return Ok(CancelOutcome::AlreadyGone);
        }
        tracing::info!(job_id, "SLURM job cancelled");
        Ok(CancelOutcome::Cancelled)
    }

    /// Cancel all submitted jobs and VERIFY each one actually left the
    /// queue.
    ///
    /// The id set is the manager's own `job_ids` Vec — the single
    /// registry every submission lands on, the initial cohort
    /// (`submit_job`) AND every respawn replacement (the respawn spawner
    /// drives the SAME `Arc<Mutex<SlurmJobManager>>`, see
    /// `crate::respawn::spawner`). So a teardown sweep reaches every
    /// submitted-and-not-yet-terminal job, replacements included; there
    /// is no second list to consult. After iterating, `self.job_ids` is
    /// cleared so a subsequent call is a no-op rather than re-cancelling
    /// already-cancelled IDs.
    ///
    /// Verified, not fire-and-forget: a bare `scancel` exits 0 even when
    /// the job then stays RUNNING (the cancel raced a PENDING→RUNNING
    /// transition, or the gateway round-trip partially failed), which is
    /// exactly how asm-dataset run_20260611_182745 left secondary-2
    /// (job 155629) RUNNING 4+ minutes after a "successful" teardown
    /// scancel. So this re-queries squeue for survivors and re-issues
    /// scancel on them, bounded by [`CancelVerifyPolicy`].
    ///
    /// FAIL-SAFE: the budget is bounded, so verification can never turn
    /// a clean abort into a hang. Any id still present after the budget
    /// is exhausted gets a loud WARN carrying the job id (the operator
    /// needs it to scancel by hand) and the sweep returns.
    pub async fn cancel_all_jobs(&mut self) -> Result<(), SlurmError> {
        self.cancel_all_jobs_verified(CancelVerifyPolicy::default())
            .await
    }

    /// [`cancel_all_jobs`](Self::cancel_all_jobs) with an explicit
    /// verification budget. Production uses the default; tests inject a
    /// near-zero `poll_delay` to keep the bounded loop off the wall
    /// clock.
    pub async fn cancel_all_jobs_verified(
        &mut self,
        policy: CancelVerifyPolicy,
    ) -> Result<(), SlurmError> {
        // Drain into a temporary so the borrow on `self.job_ids` is
        // released before we start awaiting `cancel_job(&self, ...)`.
        // This snapshot IS the cancel set: the live registry at cancel
        // time, replacements included.
        let mut survivors: Vec<String> = self.job_ids.drain(..).collect();

        // Initial scancel pass over the whole set.
        for job_id in &survivors {
            if let Err(e) = self.cancel_job(job_id).await {
                tracing::warn!(job_id, error = %e, "failed to cancel job");
            }
        }

        // Bounded verify-and-re-scancel loop. Each round polls squeue
        // for the still-present ids and re-issues scancel on them; a job
        // that has left the queue drops out of `survivors`.
        for round in 0..policy.attempts {
            survivors = self.retain_still_queued(&survivors).await;
            if survivors.is_empty() {
                // Every cancelled job has left the queue — done.
                return Ok(());
            }
            // Re-issue scancel on the stragglers. A scancel that raced a
            // state transition the first time lands now that the job is
            // fully registered.
            for job_id in &survivors {
                tracing::warn!(
                    job_id,
                    round = round + 1,
                    attempts = policy.attempts,
                    "job still in queue after scancel; re-issuing scancel",
                );
                if let Err(e) = self.cancel_job(job_id).await {
                    tracing::warn!(job_id, error = %e, "re-scancel failed");
                }
            }
            tokio::time::sleep(policy.poll_delay).await;
        }

        // Final check after the last re-scancel + delay.
        survivors = self.retain_still_queued(&survivors).await;
        if !survivors.is_empty() {
            // Loud, id-bearing WARN: the operator must scancel these by
            // hand. We do NOT block past the budget — fail-safe.
            tracing::warn!(
                survivors = ?survivors,
                "scancel verification budget exhausted; these SLURM jobs are STILL in the \
                 queue and must be cancelled manually (e.g. `scancel {}`)",
                survivors.join(" "),
            );
        }
        Ok(())
    }

    /// Filter `ids` down to those squeue still reports as live (PENDING
    /// or RUNNING). A job with no squeue row (empty output or a
    /// non-zero `squeue -j` exit, both of which mean "no such job in the
    /// queue"), or one already in a terminal/cancelling state
    /// (CANCELLED / COMPLETED / COMPLETING / FAILED-class), has left the
    /// queue and is dropped.
    ///
    /// A squeue probe that fails at the GATEWAY TRANSPORT level (the
    /// `Err` arm — `get_job_status` never got an exit code at all) is
    /// conservatively treated as "still present" so the next round
    /// retries rather than declaring a job gone on a flaky probe.
    async fn retain_still_queued(&self, ids: &[String]) -> Vec<String> {
        let mut still_queued = Vec::new();
        for job_id in ids {
            match self.get_job_status(job_id).await {
                Ok(info) => {
                    if is_still_queued(&info) {
                        still_queued.push(job_id.clone());
                    }
                }
                Err(e) => {
                    // Probe failed — do not assume the job is gone.
                    tracing::debug!(
                        job_id,
                        error = %e,
                        "squeue probe failed during cancel verification; keeping the id for the next round",
                    );
                    still_queued.push(job_id.clone());
                }
            }
        }
        still_queued
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

/// Whether a squeue snapshot means the job is STILL holding an
/// allocation we need to re-`scancel`. Single source of truth for the
/// "did the cancel actually land" predicate used by the cancel-verify
/// sweep.
///
/// Live (return `true`): `Pending` / `Running` — the job is queued or
/// executing. `Unknown(_)` is also treated as live: an unrecognised
/// transient state (e.g. `CONFIGURING`, `RESIZING`, `SUSPENDED`) is one
/// SLURM still tracks, so re-scancel and re-poll rather than declare it
/// gone.
///
/// Gone (return `false`): no squeue row at all (`state_kind == None`),
/// or a terminal/cancelling state (`Completed` covers COMPLETED /
/// COMPLETING, `Cancelled`, `Failed` covers FAILED / NODE_FAIL /
/// TIMEOUT) — the allocation is releasing or released; scancel has
/// nothing left to do.
fn is_still_queued(info: &JobStatusInfo) -> bool {
    match &info.state_kind {
        Some(JobStatus::Pending | JobStatus::Running | JobStatus::Unknown(_)) => true,
        Some(JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled) | None => false,
    }
}
