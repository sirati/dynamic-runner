//! SLURM provider implementation of [`SecondarySpawner`].
//!
//! Single concern: turn a [`SecondarySpawnSpec`] from the
//! `dynrunner-manager-distributed` operational loop into the SLURM
//! provider's triple of operations:
//!
//!   1. **Wrapper-script synthesis** for the new secondary id (delegated
//!      to a caller-supplied closure, because rendering a
//!      [`WrapperScriptConfig`](crate::wrapper_script::WrapperScriptConfig)
//!      requires deployment-specific context — image paths, mount
//!      sources, dispatcher argv — that this module intentionally does
//!      not own).
//!
//!   2. **sbatch submission** via [`SlurmJobManager::submit_job`] on a
//!      1-node allocation, using `spec.new_secondary_id` as the SLURM
//!      job name so operators eyeballing `squeue` see the same id the
//!      framework's respawn-event ring carries.
//!
//!   3. **Reverse-tunnel establishment** via the
//!      [`TunnelEstablisher`](tunnel::TunnelEstablisher) port
//!      (production-bound to
//!      [`SlurmPreparation::establish_one_tunnel`](crate::preparation::SlurmPreparation::establish_one_tunnel)).
//!      The port keeps the spawner from depending on the concrete
//!      `SlurmPreparation` struct, so trait-contract tests can drive
//!      `spawn()` against a no-op tunnel without spinning up a real
//!      `ssh -N -R`. The pool / rate-limiter / retry-budget invariants
//!      are still shared with the initial `setup_ssh_tunnels` loop
//!      because production wires the SAME `Arc<SlurmPreparation>` into
//!      the port.
//!
//! API boundary crossing: this module implements the
//! [`SecondarySpawner`](dynrunner_manager_distributed::primary::respawn::SecondarySpawner)
//! trait. Callers upstream (the primary coordinator) hold a
//! `dyn SecondarySpawner` and never see any of the SLURM-specific
//! types listed above.
//!
//! Why a caller-supplied wrapper-script closure (option (a) from the
//! design sketch) rather than a direct call to
//! [`generate_wrapper_script`](crate::wrapper_script::generate_wrapper_script)
//! (option (b)): a [`WrapperScriptConfig`](crate::wrapper_script::WrapperScriptConfig)
//! has ~20 deployment-specific fields (image path, container command,
//! cores spec, mount sources, forwarded argv, …). Capturing the
//! constant portion in the closure at wire-up time lets `spawn()` stay
//! parameterised purely over the per-respawn id, with no special-
//! casing for "which fields change per respawn vs. stay constant
//! across the run". The closure crosses the boundary cleanly:
//! `Fn(&SecondarySpawnSpec) -> Result<String, _>`.
//!
//! Layout:
//! - [`tunnel`] — `TunnelEstablisher` port + production
//!   `SlurmPreparation` binding.
//! - [`spawner`] — `SlurmSecondarySpawner` and `WrapperScriptGenerator`
//!   alias.
//! - [`tests`] — trait-contract tests. Sits marginally above the
//!   300-line target because the recording-gateway / recording-tunnel
//!   harness (~165 lines) is shared by all four contract tests;
//!   splitting harness from cases would just shuffle the boilerplate
//!   without splitting concerns. Add new contract tests here.

mod spawner;
#[cfg(test)]
mod tests;
mod tunnel;

pub use spawner::{SlurmSecondarySpawner, WrapperScriptGenerator};
pub use tunnel::{SlurmPreparationTunnelEstablisher, TunnelEstablisher};
