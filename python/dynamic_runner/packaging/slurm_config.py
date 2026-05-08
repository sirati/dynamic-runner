"""Thin Python shim around the Rust ``SlurmConfig`` PyO3 binding.

The real configuration class — fields, defaults, derived path methods,
and ``validate`` — lives in ``crates/dynrunner-slurm/src/config.rs``
and is exposed to Python via ``dynamic_runner._native.SlurmConfig``.
This module re-exports it under the historical
``dynamic_runner.packaging.slurm_config.SlurmConfig`` import path so
existing consumers and tests don't have to migrate.

``validate_slurm_config`` is preserved as a free function for backward
compatibility; it forwards to the ``SlurmConfig.validate`` method.
"""

from dynamic_runner._native import SlurmConfig

__all__ = ["SlurmConfig", "validate_slurm_config"]


def validate_slurm_config(config: SlurmConfig, gateway=None) -> None:
    """Validate SLURM configuration.

    Args:
        config: SLURM configuration
        gateway: Optional gateway instance to check remote folder existence
    """
    config.validate(gateway)
