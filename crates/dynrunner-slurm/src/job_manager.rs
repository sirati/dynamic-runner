use dynrunner_gateway::traits::{Gateway, GatewayError};
use tracing;

use crate::config::SlurmConfig;

/// Status of a SLURM job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Unknown(String),
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
    /// The script is written to a temporary file on the gateway, then
    /// submitted via `sbatch --parsable`.
    pub async fn submit_job(
        &mut self,
        wrapper_script: &str,
        job_name: &str,
        nodes: u32,
        run_log_dir: &str,
    ) -> Result<String, SlurmError> {
        // Write script to gateway
        let script_path = format!("{}/wrapper_{job_name}.sh", self.config.log_path());
        let escaped = wrapper_script.replace('\'', "'\\''");
        let write_cmd = format!("printf '%s' '{escaped}' > {script_path} && chmod +x {script_path}");
        let result = self.gateway.execute_command(&write_cmd, None).await?;
        if !result.success() {
            return Err(SlurmError::Command(format!(
                "failed to write wrapper script: {}",
                result.stderr
            )));
        }

        // Build sbatch command
        let mut sbatch_args = vec![
            "sbatch".to_string(),
            "--parsable".to_string(),
            format!("--job-name={job_name}"),
            format!("--nodes={nodes}"),
            format!("--output={run_log_dir}/slurm_%j.out"),
            format!("--error={run_log_dir}/slurm_%j.err"),
        ];

        sbatch_args.push(format!("--partition={}", self.config.partition));
        sbatch_args.push(format!("--time={}", self.config.time_limit));
        sbatch_args.push(format!("--cpus-per-task={}", self.config.cpus_per_task));
        sbatch_args.push(format!("--mem={}", self.config.memory_per_node));
        if let Some(email) = &self.config.notify_email {
            sbatch_args.push(format!("--mail-user={email}"));
            sbatch_args.push("--mail-type=FAIL".to_string());
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
    pub async fn cancel_all_jobs(&self) -> Result<(), SlurmError> {
        for job_id in &self.job_ids {
            if let Err(e) = self.cancel_job(job_id).await {
                tracing::warn!(job_id, error = %e, "failed to cancel job");
            }
        }
        Ok(())
    }

    /// Query the status of a SLURM job.
    pub async fn get_job_status(&self, job_id: &str) -> Result<JobStatus, SlurmError> {
        let cmd = format!("squeue -j {job_id} -o '%T|%N|%r' --noheader 2>/dev/null");
        let result = self.gateway.execute_command(&cmd, None).await?;

        if !result.success() || result.stdout.trim().is_empty() {
            // Job not in queue — likely completed or failed
            return Ok(JobStatus::Completed);
        }

        let line = result.stdout.trim();
        let state = line.split('|').next().unwrap_or("UNKNOWN");

        Ok(match state {
            "PENDING" => JobStatus::Pending,
            "RUNNING" => JobStatus::Running,
            "COMPLETED" | "COMPLETING" => JobStatus::Completed,
            "FAILED" | "NODE_FAIL" | "TIMEOUT" => JobStatus::Failed,
            "CANCELLED" => JobStatus::Cancelled,
            other => JobStatus::Unknown(other.to_string()),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SlurmError {
    #[error("gateway error: {0}")]
    Gateway(#[from] GatewayError),
    #[error("command error: {0}")]
    Command(String),
}
