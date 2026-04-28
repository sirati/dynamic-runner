use serde::{Deserialize, Serialize};

/// SLURM job and directory configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlurmConfig {
    /// Root folder on the gateway for all SLURM-related files.
    pub root_folder: String,
    /// Subfolder for Docker/container images.
    pub image_dir: String,
    /// Subfolder for source binary ZIPs.
    pub src_bins_dir: String,
    /// Subfolder for output files.
    pub output_dir: String,
    /// Subfolder for log files.
    pub log_dir: String,
    /// SLURM partition to submit jobs to.
    pub partition: Option<String>,
    /// Time limit for SLURM jobs (e.g. "24:00:00").
    pub time_limit: Option<String>,
    /// Number of CPUs per task.
    pub cpus_per_task: Option<u32>,
    /// Memory per node (e.g. "64G").
    pub mem: Option<String>,
    /// Email for SLURM notifications.
    pub email: Option<String>,
}

impl SlurmConfig {
    /// Get the full image directory path on the gateway.
    pub fn image_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.image_dir)
    }

    /// Get the full source binaries directory path.
    pub fn src_bins_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.src_bins_dir)
    }

    /// Get the full output directory path.
    pub fn output_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.output_dir)
    }

    /// Get the full log directory path.
    pub fn log_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.log_dir)
    }
}

impl Default for SlurmConfig {
    fn default() -> Self {
        Self {
            root_folder: "~/dynamic_batch".into(),
            image_dir: "image_bin".into(),
            src_bins_dir: "src-bins".into(),
            output_dir: "out".into(),
            log_dir: "log".into(),
            partition: None,
            time_limit: None,
            cpus_per_task: None,
            mem: None,
            email: None,
        }
    }
}
