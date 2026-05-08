use serde::{Deserialize, Serialize};

/// SLURM job and directory configuration.
///
/// Field names mirror the Python-side dataclass that consumers already
/// configure with: `image_subfolder` / `output_subfolder` / `log_subfolder`
/// for the per-subfolder layout under `root_folder`, `notify_email` for
/// the SLURM mail address, `memory_per_node` for the `--mem` flag.
/// Defaults match the Python defaults so a default-constructed config
/// produces the same SLURM behaviour from either binding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlurmConfig {
    /// Root folder on the gateway for all SLURM-related files.
    pub root_folder: String,
    /// Subfolder under `root_folder` for Docker/container images. The
    /// uploaded source binaries live at `<image_subfolder>/srcbins` —
    /// the production layout used by every Python dispatch today.
    pub image_subfolder: String,
    /// Subfolder under `root_folder` for output files.
    pub output_subfolder: String,
    /// Subfolder under `root_folder` for log files.
    pub log_subfolder: String,
    /// SLURM partition to submit jobs to.
    pub partition: String,
    /// Time limit for SLURM jobs (e.g. "48:00:00").
    pub time_limit: String,
    /// Number of CPUs per task.
    pub cpus_per_task: u32,
    /// Memory per node (e.g. "64G").
    pub memory_per_node: String,
    /// Number of nodes to request per submitted job.
    pub nodes: u32,
    /// Email for SLURM notifications. `None` disables `--mail-user`.
    pub notify_email: Option<String>,
    /// Pre-staged source override. When set, the wrapper script
    /// bind-mounts this host path into the container at
    /// `/app/src-network` instead of the primary's staging directory.
    /// Absolute paths used as-is; relative paths resolved against
    /// `root_folder`.
    pub prestaged_src_bins_path: Option<String>,
}

impl SlurmConfig {
    /// Get the full image directory path on the gateway.
    pub fn image_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.image_subfolder)
    }

    /// Get the full source binaries directory path. Layout is
    /// `<root>/<image_subfolder>/srcbins` so the srcbins live next to
    /// the image tarball under one directory tree (the production
    /// shape used by every Python dispatch today).
    pub fn src_bins_path(&self) -> String {
        format!("{}/srcbins", self.image_path())
    }

    /// Path to bind-mount into the container at `/app/src-network`.
    ///
    /// Returns `prestaged_src_bins_path` (absolute, or resolved against
    /// `root_folder` for relative paths) when set; otherwise the
    /// primary-staging directory under the image dir.
    pub fn srcbins_mount_source(&self) -> String {
        match &self.prestaged_src_bins_path {
            None => self.src_bins_path(),
            Some(path) if path.starts_with('/') => path.clone(),
            Some(path) => format!("{}/{}", self.root_folder, path),
        }
    }

    /// Get the full output directory path.
    pub fn output_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.output_subfolder)
    }

    /// Get the full log directory path.
    pub fn log_path(&self) -> String {
        format!("{}/{}", self.root_folder, self.log_subfolder)
    }

    /// Validate the configuration.
    ///
    /// `root_exists` is a caller-supplied closure: validation here
    /// stays gateway-agnostic (no async, no I/O), and the caller wires
    /// in `gateway.file_exists(...)` from whichever binding it uses.
    /// Pass `None` to skip the existence check.
    ///
    /// `remote_home` (when supplied) is woven into the suggested-path
    /// list returned in the error message; defaults to "~" when absent.
    /// Returns `Err(message)` on validation failure; callers raise the
    /// appropriate language-level error type.
    pub fn validate(
        &self,
        remote_home: Option<&str>,
        root_exists: Option<&dyn Fn(&str) -> bool>,
    ) -> Result<(), String> {
        if self.root_folder.is_empty() {
            return Err("SLURM root folder is required".into());
        }
        if let Some(exists) = root_exists {
            if !exists(&self.root_folder) {
                let home = remote_home.unwrap_or("~");
                let suggestions = format!("{home}/slurm, {home}/BIG/slurm");
                return Err(format!(
                    "SLURM root folder does not exist on gateway: {}\nSuggested locations: {}",
                    self.root_folder, suggestions,
                ));
            }
        }
        Ok(())
    }
}

impl Default for SlurmConfig {
    fn default() -> Self {
        Self {
            root_folder: "~/dynamic_batch".into(),
            image_subfolder: "image_bin".into(),
            output_subfolder: "out".into(),
            log_subfolder: "log".into(),
            partition: "All".into(),
            time_limit: "48:00:00".into(),
            cpus_per_task: 14,
            memory_per_node: "64G".into(),
            nodes: 1,
            notify_email: None,
            prestaged_src_bins_path: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_paths_match_python_layout() {
        let cfg = SlurmConfig::default();
        assert_eq!(cfg.image_path(), "~/dynamic_batch/image_bin");
        assert_eq!(cfg.src_bins_path(), "~/dynamic_batch/image_bin/srcbins");
        assert_eq!(cfg.output_path(), "~/dynamic_batch/out");
        assert_eq!(cfg.log_path(), "~/dynamic_batch/log");
    }

    #[test]
    fn srcbins_mount_source_resolves_prestaged() {
        let mut cfg = SlurmConfig::default();
        // Default: under image dir.
        assert_eq!(cfg.srcbins_mount_source(), cfg.src_bins_path());
        // Absolute prestaged path used verbatim.
        cfg.prestaged_src_bins_path = Some("/abs/staged".into());
        assert_eq!(cfg.srcbins_mount_source(), "/abs/staged");
        // Relative prestaged resolved under root_folder.
        cfg.prestaged_src_bins_path = Some("rel/staged".into());
        assert_eq!(cfg.srcbins_mount_source(), "~/dynamic_batch/rel/staged");
    }

    #[test]
    fn validate_rejects_empty_root() {
        let mut cfg = SlurmConfig::default();
        cfg.root_folder.clear();
        assert!(cfg.validate(None, None).is_err());
    }

    #[test]
    fn validate_uses_root_exists_callback() {
        let cfg = SlurmConfig::default();
        let exists = |_: &str| false;
        let err = cfg
            .validate(Some("/home/u"), Some(&exists))
            .expect_err("should fail when root missing");
        assert!(err.contains("/home/u/slurm"));
        assert!(err.contains("does not exist"));
    }
}
