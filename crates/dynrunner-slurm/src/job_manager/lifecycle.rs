//! SLURM job-lifecycle methods on [`SlurmJobManager`]: prepare
//! directories, `sbatch` submission, cancellation, and `squeue` status
//! snapshots. Pure gateway-side command issuance; image staging lives
//! in [`images`](super::images).

use dynrunner_gateway::traits::Gateway;
use tracing;

use super::types::{
    CancelOutcome, CancelVerifyPolicy, JobStatus, JobStatusInfo, RunTerminalDisposition,
    SlurmError, SlurmJobManager, PENDING_SUBMISSION_MARKER,
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
    ///
    /// Every framework sbatch carries `--no-requeue`: the framework owns
    /// member replacement (a dead member is replaced by a fresh-identity
    /// respawn, never resumed under its original id), so SLURM's own
    /// auto-requeue (`Requeue=1` default on some clusters) can only
    /// resurrect a killed member's job as a GHOST — refused re-admission
    /// (not in replicated membership) yet squatting a node until its 600s
    /// give-up, starving the legitimate respawn. There is no framework
    /// path that resumes a SLURM job under its original identity, so
    /// suppressing requeue is always correct.
    ///
    /// `exclude_node` is a SLURM NodeName the respawn path resolved from
    /// SLURM's own vocabulary (see [`Self::resolve_excluded_node`]) so a
    /// replacement never lands back on the node whose member just died
    /// (NODE_FAIL / hardware fault). The initial cohort passes `None` (no
    /// death has occurred). When `Some`, the submission carries
    /// `--exclude=<node>`; a submission that FAILS while excluding a node
    /// is retried ONCE without `--exclude` (the spawn outranks the
    /// best-effort placement hint — see the retry seam below). `None`
    /// omits the flag cleanly.
    ///
    /// Every submission's `secondary_id → job id` is recorded on
    /// `secondary_jobs` so a LATER respawn can resolve THIS member's node
    /// if it dies.
    pub async fn submit_job(
        &mut self,
        wrapper_script: &str,
        job_name: &str,
        secondary_id: &str,
        nodes: u32,
        run_log_dir: &str,
        exclude_node: Option<&str>,
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
            // The framework owns member replacement; SLURM auto-requeue
            // (`Requeue=1`) only ever produces a re-admission-refused ghost
            // that squats a node to its give-up. No framework path resumes
            // a job under its original identity, so suppress requeue on
            // every submission (initial cohort + respawn alike).
            "--no-requeue".to_string(),
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

        // `sbatch_args` above is the EXCLUSION-FREE invocation; the
        // `--exclude=<node>` flag (respawn-only) is appended per-attempt by
        // `issue_sbatch` below so a rejected exclusion can be dropped on
        // retry without re-deriving the rest of the command. The initial
        // cohort passes `exclude_node = None` and emits no `--exclude`.

        // Submit once with the exclusion. A submission that FAILS while
        // `--exclude` was passed is retried ONCE without it: the exclusion
        // is a best-effort placement hint (keep a replacement off a faulty
        // node), but spawning the replacement at all outranks honouring the
        // hint. The mesh-advertised node string the hint may carry need not
        // be a valid SLURM NodeName, in which case sbatch rejects the whole
        // submission ("Invalid node name specified") — the bare retry lets
        // the spawn proceed regardless. The SLURM error text varies by
        // version, so the retry does NOT parse it (it only logs it): ANY
        // submission failure with an exclusion present triggers the bare
        // retry. A submission with no exclusion has nothing to retry and
        // surfaces its error directly.
        let job_id = match self
            .issue_sbatch(&escaped, &sbatch_args, exclude_node, job_name)
            .await
        {
            Ok(job_id) => job_id,
            Err(e) => {
                let Some(node) = exclude_node else {
                    return Err(e);
                };
                tracing::warn!(
                    job_name,
                    excluded_node = %node,
                    error = %e,
                    "sbatch submission failed while excluding a node; retrying \
                     once WITHOUT --exclude (the spawn outranks the placement \
                     hint — the excluded node string may not be a valid SLURM \
                     NodeName)",
                );
                self.issue_sbatch(&escaped, &sbatch_args, None, job_name).await?
            }
        };

        // Record this member's job id so the respawn path can later
        // resolve its SLURM node from SLURM's own vocabulary (see
        // `secondary_jobs` / `resolve_excluded_node`). The latest
        // submission for a re-used id wins.
        self.secondary_jobs
            .insert(secondary_id.to_owned(), job_id.clone());
        Ok(job_id)
    }

    /// Resolve the SLURM node a member's job is (or was) placed on, from
    /// SLURM's OWN vocabulary, for a respawn's `--exclude`. Returns the
    /// node name to keep the replacement off, or `None` when it cannot be
    /// resolved (the hint is best-effort — a `None` simply omits
    /// `--exclude`, never blocks or fails a spawn).
    ///
    /// Why not the mesh-advertised hostname: a member advertises its
    /// transport identity (a container hostname, an FQDN), which need not
    /// equal the SLURM `NodeName`. Feeding that to `--exclude` makes
    /// sbatch reject the whole submission ("Invalid node name specified").
    /// The job's own `%N` is, by construction, a name SLURM accepts.
    ///
    /// Lookup chain: `secondary_id → job id` (this manager submitted it,
    /// cohort or respawn) → `squeue -j <id> -h -o %N` (still queued /
    /// running) → `sacct -j <id> -n -P -o NodeList` (left the queue — the
    /// common case at respawn time, since the member just died). Each
    /// step that yields nothing or fails at the gateway falls through to
    /// the next; an unresolved chain returns `None`. No node string is
    /// ever parsed beyond trimming — the first non-empty `%N`/`NodeList`
    /// token is taken as the node to exclude.
    pub async fn resolve_excluded_node(&self, secondary_id: &str) -> Option<String> {
        let job_id = self.secondary_jobs.get(secondary_id)?;

        // (1) squeue — the job may still be queued/running (e.g. a
        // keepalive-miss death whose SLURM job has not yet been reaped).
        if let Some(node) = self.node_from_squeue(job_id).await {
            return Some(node);
        }

        // (2) sacct — the job left the queue (NODE_FAIL / completed /
        // purged-from-squeue). sacct retains the placement in accounting.
        self.node_from_sacct(job_id).await
    }

    /// First node `squeue -j <id> -h -o %N` reports for the job, or `None`
    /// (no row, gateway failure, or a non-node placeholder like the
    /// PENDING-state `(Resources)`/`(Priority)` reason squeue prints in
    /// the `%N` column when nothing is allocated yet).
    async fn node_from_squeue(&self, job_id: &str) -> Option<String> {
        let cmd = format!("squeue -j {job_id} -h -o '%N' 2>/dev/null");
        let result = self.gateway.execute_command(&cmd, None).await.ok()?;
        if !result.success() {
            return None;
        }
        first_node_token(&result.stdout)
    }

    /// First node `sacct -j <id> -n -P -o NodeList` reports for the job,
    /// or `None`. `-P` parsable (pipe-delimited, no padding), `-n` no
    /// header; the first data line's NodeList holds the allocation.
    async fn node_from_sacct(&self, job_id: &str) -> Option<String> {
        let cmd = format!("sacct -j {job_id} -n -P -o NodeList 2>/dev/null");
        let result = self.gateway.execute_command(&cmd, None).await.ok()?;
        if !result.success() {
            return None;
        }
        result.stdout.lines().find_map(first_node_token)
    }

    /// Issue ONE `sbatch` submission for the given exclusion-free base
    /// args, optionally appending `--exclude=<node>`, and return the
    /// parsed job id. Owns the pending-submission-marker bookkeeping so
    /// every attempt (the exclusion attempt AND its bare retry) is
    /// cancellation-safe and self-cleaning on its own failure path; the
    /// marker is pushed before the sbatch await and is updated to the real
    /// id or removed before this returns on every non-cancellation path.
    async fn issue_sbatch(
        &mut self,
        escaped: &str,
        base_args: &[String],
        exclude_node: Option<&str>,
        job_name: &str,
    ) -> Result<String, SlurmError> {
        // Append the optional exclusion to this attempt's argv. A `None`
        // (initial cohort, or the bare retry) emits no `--exclude` flag —
        // a blank `--exclude=` would itself hard-error sbatch.
        let mut args = base_args.to_vec();
        if let Some(node) = exclude_node {
            args.push(format!("--exclude={node}"));
        }

        // No trailing script-path argument: sbatch reads the batch
        // script from STDIN, which the `printf '%s' '<body>' |` prefix
        // supplies. One shell command, no gateway-side script file.
        let cmd = format!("printf '%s' '{escaped}' | {}", args.join(" "));

        // Push a pending-submission marker BEFORE the sbatch await so that
        // a task-future cancellation mid-sbatch (e.g. the coordinator's
        // LocalSet ending while the SSH round-trip is in flight) still
        // leaves a visible record in `job_ids`.  `cancel_all_jobs_verified`
        // drains and WARNs on any marker it encounters — the job may be on
        // the cluster with an unknown ID and must be checked manually.
        // On every non-cancellation path (success, gateway error, sbatch
        // failure) the marker is updated to the real ID or removed before
        // this method returns.
        let marker_idx = self.job_ids.len();
        self.job_ids.push(PENDING_SUBMISSION_MARKER.to_string());

        let result = match self.gateway.execute_command(&cmd, None).await {
            Ok(r) => r,
            Err(e) => {
                // Gateway transport error: sbatch never ran; remove the
                // marker so teardown doesn't spuriously warn.
                self.job_ids.remove(marker_idx);
                return Err(e.into());
            }
        };

        if !result.success() {
            self.job_ids.remove(marker_idx);
            return Err(SlurmError::Command(format!(
                "sbatch failed: {}",
                result.stderr
            )));
        }

        let job_id = result.stdout.trim().to_string();
        if job_id.is_empty() {
            self.job_ids.remove(marker_idx);
            return Err(SlurmError::Command("sbatch returned empty job ID".into()));
        }

        tracing::info!(job_id = %job_id, job_name, "SLURM job submitted");
        // Replace the pending marker with the real job id.  No further
        // await points follow, so this update is cancellation-safe.
        self.job_ids[marker_idx] = job_id.clone();
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
        let drained: Vec<String> = self.job_ids.drain(..).collect();

        // Separate any pending-submission markers from known job ids.
        // A marker is left behind when a `submit_job` future is
        // cancelled while its sbatch SSH round-trip was in flight
        // (task-future drop while holding the manager mutex mid-await).
        // The sbatch call MAY have been accepted by SLURM before the
        // cancellation, but we never received the job id back.  We
        // cannot scancel an unknown id; the operator must check the
        // queue manually.  Markers on any normal exit path (success,
        // gateway error, sbatch failure) are cleaned up by `submit_job`
        // before it returns, so a marker reaching here is always a sign
        // of an abnormal cancellation.
        let mut pending_marker_count = 0usize;
        let mut survivors: Vec<String> = drained
            .into_iter()
            .filter(|id| {
                if id == PENDING_SUBMISSION_MARKER {
                    pending_marker_count += 1;
                    false
                } else {
                    true
                }
            })
            .collect();

        if pending_marker_count > 0 {
            tracing::warn!(
                count = pending_marker_count,
                "sbatch call(s) were in flight when the run ended; the submitted \
                 SLURM job(s) may be on the cluster with unknown IDs — check the \
                 queue manually (e.g. `squeue -u $USER`) and cancel any stray jobs",
            );
        }

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

    /// Whether ANY of this run's submitted jobs is still in the cluster
    /// queue (PENDING / RUNNING / an unrecognised transient state SLURM
    /// still tracks). The read-only consult the relocated submitter→observer
    /// uses to decide whether the run's cluster is GONE — it reuses the
    /// SAME `get_job_status` probe + `is_still_queued` predicate the
    /// cancel-verify sweep uses, over the manager's own tracked
    /// [`Self::job_ids`].
    ///
    /// Pending-submission markers ([`PENDING_SUBMISSION_MARKER`]) are
    /// skipped: a marker means an sbatch was in flight with an unknown id —
    /// not a queued job this consult can probe. A marker present here is
    /// CONSERVATIVELY treated as "still present" (returns `true`) so a
    /// just-launched job with an as-yet-unknown id never reads as a gone
    /// cluster — the same fail-safe direction as the cancel-verify sweep's
    /// `Err` arm.
    ///
    /// A gateway TRANSPORT failure surfaces as `Err` (the observer's
    /// double-check treats that as a probe failure — no information — and
    /// keeps observing, NOT as evidence the cluster is gone). `Ok(true)`
    /// means at least one job is still queued; `Ok(false)` is positive
    /// evidence every submitted job has left the queue.
    ///
    /// Unlike the cancel-verify sweep's [`Self::retain_still_queued`]
    /// (which conservatively folds a probe `Err` into "still present" so it
    /// keeps re-scancelling), this consult must PROPAGATE the transport
    /// failure so the caller can distinguish "every job is gone" from "the
    /// probe could not run" — declaring a cluster dead on a flaky gateway
    /// would be the very bug this consult guards against.
    pub async fn any_job_still_queued(&self) -> Result<bool, SlurmError> {
        for id in &self.job_ids {
            if id == PENDING_SUBMISSION_MARKER {
                // An sbatch is in flight with an unknown id — the cluster
                // is demonstrably NOT gone; report still-queued.
                return Ok(true);
            }
            // Propagate a transport `Err` (probe failure) rather than
            // swallowing it; the SAME `get_job_status` + `is_still_queued`
            // predicate the cancel-verify sweep applies decides "queued".
            if is_still_queued(&self.get_job_status(id).await?) {
                return Ok(true);
            }
        }
        // `false` only when there WERE real ids and every one has left the
        // queue. An all-marker / empty ledger never reaches here as a
        // "gone" signal (the marker arm returns `true`; a never-submitted
        // ledger carries no leave-the-queue evidence and returns `false`).
        Ok(false)
    }

    /// The AUTHORITATIVE terminal disposition of the whole cohort, read
    /// from `sacct` accounting — the disambiguation [`Self::any_job_still_queued`]
    /// (a `squeue` probe) CANNOT make.
    ///
    /// `squeue` only ever lists PENDING/RUNNING jobs; once every job leaves
    /// the queue it reports nothing REGARDLESS of WHY they left, so
    /// `any_job_still_queued`'s `Ok(false)` ("every job is gone") collapses
    /// a clean COMPLETED framework shutdown and a crash/scancel/OOM into the
    /// same answer. `sacct` retains each job's final State after it leaves
    /// the queue, so it can tell the two apart: this folds the cohort's
    /// per-job States into ONE [`RunTerminalDisposition`].
    ///
    /// Each real job id (markers skipped — a marker means an sbatch was in
    /// flight with an unknown id, which is NOT a clean terminal, so it forces
    /// [`RunTerminalDisposition::AnyFailed`]) is queried with
    /// `sacct -j <id> -n -P -X -o State`: `-X` reports allocations only (no
    /// `.batch`/`.extern` step lines), `-n` no header, `-P` parsable, so a
    /// well-formed reply is ONE State token per job. The token's leading
    /// word is taken (SLURM prints e.g. `CANCELLED by 12345`).
    ///
    /// Folding rule (FAILURE-dominant, fail-safe toward the conservative
    /// verdict): ANY job whose State is a recognised FAILURE terminal
    /// (FAILED / CANCELLED / TIMEOUT / NODE_FAIL / OUT_OF_MEMORY, or any
    /// non-COMPLETED non-running terminal) ⇒ [`RunTerminalDisposition::AnyFailed`]
    /// immediately. Only when EVERY job is positively `COMPLETED` is the
    /// disposition [`RunTerminalDisposition::AllCompleted`]. If any job's
    /// state is UNREADABLE (gateway failure, or accounting returned nothing)
    /// and none was seen failed, the disposition is
    /// [`RunTerminalDisposition::Indeterminate`] — not positive evidence of
    /// a clean completion, so the caller keeps its conservative verdict (the
    /// optimistic AllCompleted is asserted ONLY on a fully-positive read).
    ///
    /// An empty/all-marker ledger has no positive clean-completion evidence:
    /// no real id was COMPLETED, so this returns `Indeterminate` (an empty
    /// ledger carries no "everything completed" proof) — the same fail-safe
    /// direction.
    pub async fn run_terminal_disposition(&self) -> RunTerminalDisposition {
        let mut saw_any_completed = false;
        let mut saw_unreadable = false;
        for id in &self.job_ids {
            if id == PENDING_SUBMISSION_MARKER {
                // An sbatch was in flight with an unknown id when the run
                // ended — that is NOT a clean terminal; treat the cohort as
                // failed (the same direction the cancel-verify sweep WARNs
                // a marker in).
                return RunTerminalDisposition::AnyFailed;
            }
            match self.sacct_terminal_state(id).await {
                Some(JobStatus::Completed) => saw_any_completed = true,
                // Any recognised FAILURE terminal OR an unrecognised
                // terminal token short-circuits to AnyFailed: a cohort with
                // even one non-clean exit is a real failure.
                Some(_) => return RunTerminalDisposition::AnyFailed,
                // Unreadable for this job (gateway failure or no accounting
                // row) — remember it, but a later positively-failed job
                // still wins.
                None => saw_unreadable = true,
            }
        }
        if saw_unreadable {
            // At least one job's authoritative state could not be read and
            // none was seen failed — not positive proof of a clean
            // completion.
            RunTerminalDisposition::Indeterminate
        } else if saw_any_completed {
            // Every real job read back COMPLETED (and at least one existed)
            // — a clean framework shutdown.
            RunTerminalDisposition::AllCompleted
        } else {
            // No real ids at all (empty / all-marker ledger): no
            // clean-completion evidence exists.
            RunTerminalDisposition::Indeterminate
        }
    }

    /// The authoritative terminal `JobStatus` of one job from `sacct`, or
    /// `None` when accounting cannot be read (gateway failure, non-zero
    /// `sacct` exit, or no row for the id — purged/disabled accounting).
    ///
    /// `sacct -j <id> -n -P -X -o State`: `-X` allocations-only (no step
    /// lines), `-n` no header, `-P` parsable. The first data line's State
    /// is parsed; SLURM prints compound tokens like `CANCELLED by 12345`,
    /// so only the leading word is matched. A still-running State
    /// (PENDING/RUNNING) — possible if `sacct` is consulted while a job has
    /// not yet left the queue — maps to `None` (no terminal recorded yet),
    /// the same fail-safe "no clean-completion evidence" direction as an
    /// unreadable consult.
    /// Public wrapper around [`Self::sacct_terminal_state`] for the
    /// off-loop slurm-authoritative probe
    /// (`crate::authority::SlurmJobManagerProbe`). A squeue-empty consult
    /// falls through to sacct here; a terminal disposition (any `Some`)
    /// proves the job has left the queue, a `None` keeps the answer
    /// `Unknown` at the probe layer. Thin pub wrapper — the consult body
    /// stays private so other call sites must go through this seam.
    pub async fn sacct_terminal_state_pub(&self, job_id: &str) -> Option<JobStatus> {
        self.sacct_terminal_state(job_id).await
    }

    async fn sacct_terminal_state(&self, job_id: &str) -> Option<JobStatus> {
        let cmd = format!("sacct -j {job_id} -n -P -X -o State 2>/dev/null");
        let result = self.gateway.execute_command(&cmd, None).await.ok()?;
        if !result.success() {
            return None;
        }
        let token = result.stdout.lines().find_map(|line| {
            let t = line.trim();
            (!t.is_empty()).then(|| t.split_whitespace().next().unwrap_or("").to_string())
        })?;
        match token.as_str() {
            "COMPLETED" => Some(JobStatus::Completed),
            "FAILED" | "NODE_FAIL" | "TIMEOUT" | "OUT_OF_MEMORY" | "BOOT_FAIL" | "DEADLINE" => {
                Some(JobStatus::Failed)
            }
            "CANCELLED" => Some(JobStatus::Cancelled),
            // PENDING/RUNNING (no terminal recorded yet) or an empty/
            // unrecognised token: no authoritative terminal to classify.
            "PENDING" | "RUNNING" | "" => None,
            other => Some(JobStatus::Unknown(other.to_string())),
        }
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
            return Ok(JobStatusInfo::missing());
        }

        Ok(parse_status_fields(result.stdout.trim()))
    }

    /// Batched [`Self::get_job_status`]: ONE `squeue -j <id1>,<id2>,…`
    /// invocation for the whole `job_ids` set instead of one call per id.
    ///
    /// The result is the SAME per-job [`JobStatusInfo`] the per-job path
    /// produces, keyed by job id. A job ABSENT from the batched output
    /// (SLURM omits finished/purged/unknown jobs from a `squeue -j`
    /// comma-list rather than erroring the whole query) maps to the
    /// identical "no row" snapshot ([`JobStatusInfo::missing`],
    /// `state_kind == None`) that a per-job `squeue -j <id>` returns for a
    /// job no longer in the queue — so callers' "missing → consult
    /// sacct/ledger" interpretation is unchanged. The `%i` column is
    /// prepended to the per-job `%T|%N|%r` format purely to map each row
    /// back to its job id; the remaining three fields parse through the
    /// SAME [`parse_status_fields`] the per-job path uses.
    ///
    /// A gateway TRANSPORT failure surfaces as `Err` (mirroring the
    /// per-job `?` on `execute_command`). A non-zero/empty result is
    /// treated as "no rows" — every queried id maps to a missing
    /// snapshot, the same fail-direction the per-job path takes on an
    /// empty/non-zero single query.
    pub async fn get_job_status_batch(
        &self,
        job_ids: &[String],
    ) -> Result<std::collections::HashMap<String, JobStatusInfo>, SlurmError> {
        let mut out = std::collections::HashMap::with_capacity(job_ids.len());
        if job_ids.is_empty() {
            return Ok(out);
        }
        // Every queried id defaults to the "no row" snapshot; rows present
        // in the squeue output overwrite their id below. An id absent from
        // the output therefore retains the missing snapshot — IDENTICAL to
        // the per-job path's empty/non-zero result.
        for id in job_ids {
            out.insert(id.clone(), JobStatusInfo::missing());
        }

        let id_list = job_ids.join(",");
        let cmd = format!("squeue -j {id_list} -o '%i|%T|%N|%r' --noheader 2>/dev/null");
        let result = self.gateway.execute_command(&cmd, None).await?;

        if !result.success() {
            // Whole-query failure (e.g. SLURM rejected the list): leave
            // every id at the missing snapshot, the same fail-direction the
            // per-job path takes on a non-zero single query.
            return Ok(out);
        }

        for line in result.stdout.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // `%i|<rest>`: split the leading job id off, parse the rest with
            // the SAME helper the per-job path uses.
            let (job_id, rest) = match line.split_once('|') {
                Some(pair) => pair,
                None => continue,
            };
            out.insert(job_id.to_string(), parse_status_fields(rest));
        }

        Ok(out)
    }
}

/// Parse one `%T|%N|%r` squeue row (state|node|reason) into a populated
/// [`JobStatusInfo`]. Single source of truth for the per-job status parse,
/// shared by [`SlurmJobManager::get_job_status`] (one row) and
/// [`SlurmJobManager::get_job_status_batch`] (one row per job, after the
/// leading `%i` id is split off).
fn parse_status_fields(fields: &str) -> JobStatusInfo {
    let mut parts = fields.split('|');
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

    JobStatusInfo {
        state: Some(state_str),
        state_kind: Some(state_kind),
        node,
        reason,
    }
}

/// The node token from one line of `squeue %N` / `sacct NodeList`
/// output, or `None` when the line carries no real placement.
///
/// SLURM's `%N` / `NodeList` is a node-LIST expression for the whole
/// allocation (e.g. `krater04`, or `node[01-03]` for a multi-node job).
/// `--exclude` accepts the same expression syntax verbatim, so the
/// token is passed through unaltered rather than expanded. A blank line
/// or the literal `None assigned` SLURM prints for a job with no
/// allocation (a still-PENDING squeue row, a sacct batch/extern step) is
/// dropped — feeding either to `--exclude` would be a no-op at best and
/// a parse error at worst.
fn first_node_token(line: &str) -> Option<String> {
    let token = line.trim();
    if token.is_empty() || token.eq_ignore_ascii_case("None assigned") {
        return None;
    }
    Some(token.to_owned())
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
