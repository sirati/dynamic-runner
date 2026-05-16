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
}
