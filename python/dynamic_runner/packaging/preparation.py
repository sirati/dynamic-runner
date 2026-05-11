"""SLURM-specific preparation phase for primary coordinator.

Owns:
- Container image build invocation (delegates to PodmanPackaging via job_manager)
- Gateway transfer of the image artifacts
- SLURM job submission via job_manager
- SSH tunnel setup for reverse connections (when the compute nodes can't
  reach the primary directly)

The SSH-tunnel-watcher state machine + subprocess teardown live in
Rust (`dynamic_runner._native.RustSlurmPreparation`). This module
keeps the orchestration glue (image build, job submit, run-id
bookkeeping) in Python and delegates tunnel lifecycle to the Rust
class — single concern at the FFI boundary.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..deployment_spec import TaskDeploymentSpec
from .podman import PodmanImageMetadata

try:
    from .._native import RustSlurmPreparation
except ImportError as e:  # pragma: no cover — unbuilt wheel
    raise ImportError(
        "dynamic_runner._native.RustSlurmPreparation is required; "
        "rebuild the wheel after a Rust change"
    ) from e

logger = logging.getLogger(__name__)


@dataclass
class PreparationResult:
    """Result of preparation phase. Mirrors the legacy
    `multi_computer.PreparationResult` shape so the SLURM pipeline
    return value stays stable.
    """

    num_secondaries: int
    run_id: str
    cert_dir: Path
    primary_entropy: bytes
    mode_specific_data: dict[str, Any] = field(default_factory=dict)


class SlurmPreparation:
    """Handles SLURM-specific preparation phase."""

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
    ):
        self.slurm_config = slurm_config
        self.job_manager = job_manager
        self.gateway = gateway
        self.deployment = deployment
        self.use_reverse_connection = use_reverse_connection
        self.run_id = run_id
        # Verbatim `--cores` spec string forwarded to each SLURM
        # secondary's container_command. Defaults to "0" (all
        # detected cores) for back-compat with callers that
        # construct SlurmPreparation directly. The framework's
        # `pipeline.rs` always populates this from `args.cores`;
        # the default only applies to programmatic test fixtures.
        self.cores_spec = cores_spec
        # Verbatim `--max-memory` spec string forwarded the same
        # way. Defaults to "-2G" (host minus 2 GiB headroom) matching
        # the CLI default. SLURM-only forward: --multi-computer local
        # doesn't plumb memory through (single-host shared RAM =
        # double-counting); SLURM secondaries are on different hosts
        # with own RAM so per-machine semantic applies.
        self.max_memory_spec = max_memory_spec

        base_log_dir = self.slurm_config.get_log_dir()
        self.run_log_dir = f"{base_log_dir}/{run_id}"

        # The SSH-tunnel watcher + subprocess teardown live in Rust.
        # Constructed lazily inside `_setup_ssh_tunnels` so non-reverse
        # runs (which never call _setup_ssh_tunnels) avoid touching
        # auth_options / extra_port_forwards.
        self._tunnel_manager: RustSlurmPreparation | None = None
        # Map kept for callers that read mode_specific_data; populated
        # from the Rust side once tunnels are established.
        self.secondary_port_map: dict[str, int] = {}
        # Legacy attribute — Rust now owns the subprocess handles.
        # Kept as an empty list so any code reading it (or iterating
        # over mode_specific_data["ssh_tunnels"]) still works.
        self.ssh_tunnels: list[Any] = []

    async def prepare(
        self,
        num_secondaries: int,
        quic_port: int,
        primary_quic_port: int,
        cert_dir: Path,
        skip_image_build: bool = False,
    ) -> PreparationResult:
        """Execute SLURM preparation phase."""
        logger.info("Phase 1: SLURM Preparation")
        self.job_manager.prepare_directories()
        self.gateway.create_directory(self.run_log_dir)

        image_metadata = await self._prepare_docker_images(skip_image_build)
        self._submit_slurm_jobs(num_secondaries, primary_quic_port, image_metadata)

        if self.use_reverse_connection:
            await self._setup_ssh_tunnels(num_secondaries, primary_quic_port)

        mode_specific_data = {
            "image_metadata": image_metadata,
            "run_log_dir": self.run_log_dir,
            "secondary_port_map": self.secondary_port_map,
            "ssh_tunnels": self.ssh_tunnels,
        }

        import secrets

        primary_entropy = secrets.token_bytes(32)

        return PreparationResult(
            num_secondaries=num_secondaries,
            run_id=self.run_id,
            cert_dir=cert_dir,
            primary_entropy=primary_entropy,
            mode_specific_data=mode_specific_data,
        )

    async def _prepare_docker_images(self, skip_image_build: bool) -> PodmanImageMetadata:
        """Build and transfer the docker image, or verify existing path."""
        image_dir = Path(self.job_manager._expand_path(self.slurm_config.get_image_dir()))
        image_path = image_dir / self.deployment.image_tar_basename

        if skip_image_build:
            logger.info("Skipping image build and transfer (--skip-image-build)")
            logger.info("Assuming image exists at: %s", image_path)
            return PodmanImageMetadata(
                remote_path=image_path,
                image_hash="",
                uploaded=False,
            )

        project_root = Path.cwd()
        image_metadata = self.job_manager.build_and_transfer_images(project_root)

        logger.info(
            "Image %s at: %s",
            "uploaded" if image_metadata.uploaded else "reused",
            image_metadata.remote_path,
        )
        logger.info("Image hash: %s", image_metadata.image_hash)

        return image_metadata

    def _submit_slurm_jobs(
        self,
        num_secondaries: int,
        primary_quic_port: int,
        image_metadata: PodmanImageMetadata,
    ) -> None:
        """Submit SLURM jobs for secondaries."""
        logger.info("Submitting SLURM jobs...")
        gateway_host = self._determine_gateway_host()

        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            job_name = f"{self.deployment.effective_job_name_prefix}-{secondary_id}"

            wrapper = self.job_manager.generate_wrapper_script(
                image_metadata=image_metadata,
                secondary_id=secondary_id,
                gateway_host=gateway_host,
                gateway_port=primary_quic_port,
                cores_spec=self.cores_spec,
                max_memory_spec=self.max_memory_spec,
                reverse_connection=self.use_reverse_connection,
                run_log_dir=self.run_log_dir,
            )

            job_id = self.job_manager.submit_job(wrapper, job_name, run_log_dir=self.run_log_dir)
            logger.info("Submitted job %s for %s", job_id, secondary_id)

        logger.info("All %d jobs submitted", num_secondaries)

    def _determine_gateway_host(self) -> str:
        """Determine the hostname that compute nodes should use to reach the gateway.

        The user-given ``self.gateway.host`` is used verbatim. We do NOT
        substitute it with ``hostname -f`` from the gateway: when the
        configured host is a load-balancer alias (e.g. ``remote.cip.ifi.lmu.de``),
        ``hostname -f`` resolves on the gateway side to whichever specific
        node the LB landed us on (e.g. ``benitoit.cip.ifi.lmu.de``), which
        compute nodes may not be able to reach (different routing, different
        cert) — the LB alias is the only stable name. If a caller wants a
        specific node's FQDN they pass it as ``gateway.host`` themselves.
        """
        if hasattr(self.gateway, "host") and self.gateway.host:
            gateway_host = self.gateway.host
            logger.info("Using gateway hostname (as configured by user): %s", gateway_host)
        else:
            gateway_host = "localhost"
            logger.info("Using local gateway host: %s", gateway_host)
        return gateway_host

    async def _setup_ssh_tunnels(self, num_secondaries: int, primary_quic_port: int) -> None:
        """Setup SSH reverse tunnels (compute → primary via gateway).

        Delegates the per-secondary watcher state machine + tunnel
        spawning + subprocess tracking to Rust. Reverse-connection
        mode is used when the gateway has ``GatewayPorts no`` so the
        standard "secondaries dial the gateway" path can't work;
        instead each compute node's wrapper picks a free
        ``TUNNEL_PORT`` locally, the wrapper invokes the secondary
        with ``--secondary tcp://localhost:$TUNNEL_PORT``, and the
        Rust side opens an ``ssh -N -R`` from the primary that asks
        the compute node's sshd to bind ``localhost:tunnel_port`` and
        forward back to the primary's ``localhost:primary_quic_port``.

        Auth-flag chain (``-i`` / ``IdentitiesOnly=yes`` /
        ``IdentityAgent=none`` / ``-F config``) is read off the
        gateway via :meth:`auth_options` so the gateway stays the
        single source of truth — Rust receives an opaque
        ``list[str]`` to splice verbatim into each ssh command.

        Connection-info file is in URI form
        (``<scheme>://<host>:<port>``) per the post-L1.7 wire
        contract.
        """
        logger.info("Setting up SSH reverse tunnels for reverse connections...")

        connection_info_dir = f"{self.run_log_dir}/connection_info"
        self.gateway.create_directory(connection_info_dir)

        gateway_host = self.gateway.host if hasattr(self.gateway, "host") else "localhost"
        gateway_user = self.gateway.user if hasattr(self.gateway, "user") else None
        gateway_port = self.gateway.port if hasattr(self.gateway, "port") else 22
        auth_opts = list(self.gateway.auth_options()) if hasattr(self.gateway, "auth_options") else []
        extra_forwards = list(self.deployment.extra_port_forwards)

        self._tunnel_manager = RustSlurmPreparation(
            self.gateway,
            self.run_log_dir,
            gateway_host,
            int(gateway_port),
            auth_opts,
            extra_forwards,
            gateway_user,
        )

        # The Rust core's `setup_ssh_tunnels` is sync from Python's
        # perspective but releases the GIL inside its tokio runtime.
        # Drive it on a worker thread so this `async def` keeps
        # cooperating with the surrounding asyncio loop — important
        # for any caller awaiting alongside us. `asyncio.to_thread`
        # is the right primitive here; the GIL is held briefly for
        # the bridge call and released for the duration of the
        # 600s state machine.
        port_map = await asyncio.to_thread(
            self._tunnel_manager.setup_ssh_tunnels,
            num_secondaries,
            primary_quic_port,
        )

        # Reflect into the legacy attribute for any caller reading
        # `mode_specific_data["secondary_port_map"]`.
        self.secondary_port_map = {k: int(v) for k, v in port_map.items()}
        logger.info("All %d SSH tunnels established", num_secondaries)

    def cleanup(self) -> None:
        """Cleanup SLURM preparation resources.

        Idempotent — safe to call from a ``finally`` block even when
        ``_setup_ssh_tunnels`` was never invoked (non-reverse mode).
        """
        logger.info("Cleaning up SLURM preparation resources...")
        if self._tunnel_manager is not None:
            self._tunnel_manager.cleanup()
        logger.info("SLURM preparation cleanup complete")
