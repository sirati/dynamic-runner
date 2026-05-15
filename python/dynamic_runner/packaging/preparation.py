# =====================================================================
# WARNING — PYTHON BRIDGE ONLY. NO LOGIC HERE.
# =====================================================================
# This file is a thin PyO3 / CLI / config bridge. ALL business logic,
# lifecycle, state-tracking, async orchestration, and process management
# lives in Rust under `crates/dynrunner-slurm/` and
# `crates/dynrunner-pyo3/src/slurm/`. If you find yourself adding logic
# here — STOP. Put it in Rust and call it from this file via PyO3.
# =====================================================================
"""SLURM-preparation Python facade.

The preparation orchestration (image build, gateway directory prep,
sbatch submit-loop, reverse-tunnel watcher, primary-entropy generation,
outcome assembly) lives in the Rust PyO3 layer under
``crates/dynrunner-pyo3/src/slurm/pipeline.rs::run_preparation``; the
SSH-tunnel watcher state machine + subprocess teardown lives in
``crates/dynrunner-slurm/src/preparation.rs``.

The framework's live SLURM pipeline (``run_slurm_pipeline``) invokes the
Rust orchestrator directly without touching this module. The
``SlurmPreparation`` class and ``PreparationResult`` dataclass remain
here only as a 1-line bridge for out-of-tree callers that import the
names — ``prepare()`` is a single PyO3 delegation to
``_native.run_preparation`` and stores the returned tunnel-manager
handle on the instance so ``cleanup()`` can tear it down.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..deployment_spec import TaskDeploymentSpec

try:
    from .._native import run_preparation as _native_run_preparation
except ImportError as e:  # pragma: no cover — unbuilt wheel
    raise ImportError(
        "dynamic_runner._native.run_preparation is required; "
        "rebuild the wheel after a Rust change"
    ) from e

logger = logging.getLogger(__name__)


@dataclass
class PreparationResult:
    """Result of preparation phase.

    Field shape is the single source of truth consumed by the Rust
    ``run_preparation`` pyfunction — it constructs an instance of this
    dataclass via ``getattr`` import and populates the same five
    attributes. Mutating the field list here requires a corresponding
    update in ``crates/dynrunner-pyo3/src/slurm/pipeline.rs``.
    """

    num_secondaries: int
    run_id: str
    cert_dir: Path
    primary_entropy: bytes
    mode_specific_data: dict[str, Any] = field(default_factory=dict)


class SlurmPreparation:
    """1-line delegate to ``_native.run_preparation``.

    Kept on the public Python surface for back-compat with any
    out-of-tree caller that imported the class name. ``__init__`` stores
    the constructor arguments verbatim; ``prepare()`` flattens them
    into the PyO3 entry point and stashes the returned tunnel-manager
    so ``cleanup()`` can drain it. No orchestration here.
    """

    def __init__(
        self,
        slurm_config: Any,
        job_manager: Any,
        gateway: Any,
        deployment: TaskDeploymentSpec,
        use_reverse_connection: bool = False,
        run_id: str = "default",
        cores_spec: str = "0",
        max_memory_spec: str = "-2G",
        forwarded_argv: list[str] | None = None,
    ):
        self.slurm_config = slurm_config
        self.job_manager = job_manager
        self.gateway = gateway
        self.deployment = deployment
        self.use_reverse_connection = use_reverse_connection
        self.run_id = run_id
        self.cores_spec = cores_spec
        self.max_memory_spec = max_memory_spec
        self.forwarded_argv = list(forwarded_argv) if forwarded_argv else []
        # Populated by ``prepare`` when reverse-connection mode spawns
        # per-secondary tunnels. ``cleanup`` is a no-op when this stays
        # ``None`` (non-reverse runs, or ``prepare`` not yet called).
        self._tunnel_manager: Any = None

    async def prepare(
        self,
        num_secondaries: int,
        quic_port: int,
        primary_quic_port: int,
        cert_dir: Path,
        skip_image_build: bool = False,
    ) -> PreparationResult:
        """1-line delegation to ``_native.run_preparation``."""
        del quic_port  # legacy alias for primary_quic_port; unused by Rust.
        result, tunnel_manager = _native_run_preparation(
            self.slurm_config,
            self.job_manager,
            self.gateway,
            self.deployment,
            self.run_id,
            num_secondaries,
            primary_quic_port,
            cert_dir,
            self.use_reverse_connection,
            skip_image_build,
            self.cores_spec,
            self.max_memory_spec,
            self.forwarded_argv,
            logger,
        )
        self._tunnel_manager = tunnel_manager
        return result

    def cleanup(self) -> None:
        """Idempotent cleanup; safe in ``finally`` before ``prepare``."""
        if self._tunnel_manager is not None:
            self._tunnel_manager.cleanup()
            self._tunnel_manager = None
