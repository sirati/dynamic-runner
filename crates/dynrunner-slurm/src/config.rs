use std::path::PathBuf;

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
    /// Pre-staged source override. When set, the wrapper script
    /// bind-mounts this host path into the container at
    /// ``/app/src-network`` instead of the primary's staging
    /// directory (and the primary skips its initial-staging pass
    /// entirely). Absolute paths used as-is; relative paths
    /// resolved against ``root_folder``. Mirrors the Python field
    /// of the same name on ``slurm_config.SlurmConfig``.
    pub prestaged_src_bins_path: Option<PathBuf>,
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

    /// Path to bind-mount into the container at ``/app/src-network``.
    ///
    /// Returns ``prestaged_src_bins_path`` (absolute, or resolved
    /// against ``root_folder`` for relative paths) when set;
    /// otherwise the primary-staging directory under the image dir.
    /// Mirrors ``SlurmConfig.get_srcbins_mount_source`` in
    /// ``packaging/slurm_config.py``.
    pub fn srcbins_mount_source(&self) -> String {
        match &self.prestaged_src_bins_path {
            None => self.src_bins_path(),
            Some(path) => {
                if path.is_absolute() {
                    path.to_string_lossy().into_owned()
                } else {
                    format!("{}/{}", self.root_folder, path.to_string_lossy())
                }
            }
        }
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
            prestaged_src_bins_path: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn srcbins_mount_source_defaults_to_staging_dir() {
        let config = SlurmConfig::default();
        assert_eq!(
            config.srcbins_mount_source(),
            "~/dynamic_batch/src-bins",
            "with prestaged_src_bins_path unset, mount source must equal src_bins_path()",
        );
    }

    #[test]
    fn srcbins_mount_source_uses_prestaged_absolute_path_verbatim() {
        let config = SlurmConfig {
            prestaged_src_bins_path: Some(PathBuf::from("/srv/cluster/staged-src")),
            ..SlurmConfig::default()
        };
        assert_eq!(
            config.srcbins_mount_source(),
            "/srv/cluster/staged-src",
            "absolute prestaged path must be used as-is, not joined to root_folder",
        );
    }

    #[test]
    fn srcbins_mount_source_resolves_relative_prestaged_against_root() {
        let config = SlurmConfig {
            prestaged_src_bins_path: Some(PathBuf::from("staged-src")),
            ..SlurmConfig::default()
        };
        assert_eq!(
            config.srcbins_mount_source(),
            "~/dynamic_batch/staged-src",
            "relative prestaged path must be joined to root_folder",
        );
    }

    #[test]
    fn srcbins_mount_source_does_not_mutate_src_bins_path() {
        // Sanity: `src_bins_path()` is the staging-dir contract used
        // by `prepare_directories`; the prestaged toggle is a
        // mount-time decision and must not shift the directory we
        // create on the gateway.
        let config = SlurmConfig {
            prestaged_src_bins_path: Some(PathBuf::from("/elsewhere")),
            ..SlurmConfig::default()
        };
        assert_eq!(config.src_bins_path(), "~/dynamic_batch/src-bins");
        assert_eq!(config.srcbins_mount_source(), "/elsewhere");
    }
}
