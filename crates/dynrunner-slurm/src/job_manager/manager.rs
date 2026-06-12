//! Constructors and accessors on [`SlurmJobManager`]: `new`,
//! `job_ids`, `gateway`, `gateway_mut`. Image-staging methods live in
//! [`images`](super::images) and SLURM lifecycle methods in
//! [`lifecycle`](super::lifecycle); all three impl blocks share the
//! same struct + private fields through the `pub(super)` declarations
//! in [`types`](super::types).

use dynrunner_gateway::traits::Gateway;

use crate::config::SlurmConfig;

use super::types::SlurmJobManager;

impl<G: Gateway> SlurmJobManager<G> {
    pub fn new(config: SlurmConfig, gateway: G) -> Self {
        Self {
            config,
            gateway,
            job_ids: Vec::new(),
            secondary_jobs: std::collections::HashMap::new(),
            shutdown_manager_remote_path: None,
            wrapper_bin_remote_path: None,
        }
    }

    pub fn job_ids(&self) -> &[String] {
        &self.job_ids
    }

    /// Seed the tracked job-id list directly, bypassing `submit_job`'s
    /// sbatch round-trip. Test-only: lets a unit test stand up a
    /// manager whose registry already holds a cohort of ids (as if
    /// `submit_job` had run N times) without driving a canned-id
    /// gateway N times. Gated `#[cfg(test)]` so it never widens the
    /// production surface.
    #[cfg(test)]
    pub(crate) fn seed_job_ids_for_test(&mut self, ids: &[&str]) {
        self.job_ids
            .extend(ids.iter().map(|id| (*id).to_string()));
    }

    pub fn gateway(&self) -> &G {
        &self.gateway
    }

    pub fn gateway_mut(&mut self) -> &mut G {
        &mut self.gateway
    }

    /// Gateway-side path of the uploaded `dynrunner-slurm-shutdown`
    /// binary, set by
    /// [`SlurmJobManager::upload_shutdown_manager_binary_from`].
    ///
    /// Returns `None` only when the upload step has not yet been
    /// invoked on this manager (a successful upload always records a
    /// path; the upload step raises on missing source rather than
    /// skipping silently). Wrapper-script renderers consume the value
    /// via
    /// [`WrapperScriptConfig::shutdown_manager_bin_path`](crate::wrapper_script::WrapperScriptConfig::shutdown_manager_bin_path);
    /// the `None` branch in the renderer exists for unit tests and
    /// back-compat callers only.
    pub fn shutdown_manager_remote_path(&self) -> Option<&str> {
        self.shutdown_manager_remote_path.as_deref()
    }

    /// Gateway-side path of the uploaded `dynrunner-slurm-wrapper`
    /// binary, set by
    /// [`SlurmJobManager::upload_wrapper_binary_from`].
    ///
    /// Returns `None` only when the upload step has not yet been
    /// invoked on this manager. Wrapper-script renderers consume the
    /// value via
    /// [`WrapperScriptConfig::wrapper_bin_path`](crate::wrapper_script::WrapperScriptConfig::wrapper_bin_path)
    /// to emit the `exec`-stub body; the `None` branch in the renderer
    /// keeps the legacy inline-bash body for unit tests and
    /// back-compat callers that do not exercise the SLURM dispatch
    /// path. Mirrors [`SlurmJobManager::shutdown_manager_remote_path`].
    pub fn wrapper_bin_remote_path(&self) -> Option<&str> {
        self.wrapper_bin_remote_path.as_deref()
    }
}
