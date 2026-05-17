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
            shutdown_manager_remote_path: None,
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

    /// Gateway-side path of the uploaded `dynrunner-slurm-shutdown`
    /// binary, set by
    /// [`SlurmJobManager::upload_shutdown_manager_binary`].
    ///
    /// Returns `None` when the upload step was skipped (env var
    /// `DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE` unset) or has not been
    /// invoked yet on this manager. Wrapper-script renderers consume
    /// the value via
    /// [`WrapperScriptConfig::shutdown_manager_bin_path`](crate::wrapper_script::WrapperScriptConfig::shutdown_manager_bin_path);
    /// `None` here means the rendered wrapper omits the
    /// `systemd-run`-based shutdown-manager spawn block (legacy
    /// CMD_RELAY-only teardown).
    pub fn shutdown_manager_remote_path(&self) -> Option<&str> {
        self.shutdown_manager_remote_path.as_deref()
    }
}
