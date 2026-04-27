"""SLURM packaging pipeline: container image build → gateway transfer →
SLURM job submission → handoff to the Rust primary coordinator.

Self-contained: nothing in this package imports from the legacy
`dynamic_batch.{gateway, runtime_env, slurm, multi_computer}` chain, so
those legacy directories can be deleted in the next pass without
breaking `--multi-computer slurm`.
"""

from dataclasses import dataclass

from .pipeline import run_slurm_pipeline
from .podman import PodmanImageMetadata, PodmanPackaging


@dataclass
class PackagingConfig:
    """Configuration for packaging method (only `'podman'` supported)."""

    method: str


def make_packaging(config: PackagingConfig) -> PodmanPackaging:
    """Factory: only podman is supported for SLURM cluster environments.

    Docker requires user-session systemd which isn't available in SLURM
    batch jobs.
    """
    if config.method == "podman":
        return PodmanPackaging()
    raise ValueError(
        f"Unknown or unsupported packaging method: {config.method!r}. "
        "Only 'podman' is supported for SLURM cluster runs."
    )


__all__ = [
    "PodmanImageMetadata",
    "PodmanPackaging",
    "PackagingConfig",
    "make_packaging",
    "run_slurm_pipeline",
]
