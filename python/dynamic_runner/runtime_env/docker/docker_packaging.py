import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class DockerPackaging:
    """Docker-based packaging - NOT IMPLEMENTED

    Use PodmanPackaging instead for SLURM cluster environments.
    Docker with default configuration does not work in SLURM job contexts
    because it requires systemd user sessions that are not available.
    """

    def __init__(self):
        self.image_name = "asm-tokenizer"
        self.image_tag = "latest"

    def build_image(self, gateway, local_project_root: Path, output_path: Path) -> Path:
        """Build Docker image - NOT IMPLEMENTED

        Args:
            gateway: Gateway instance to transfer image
            local_project_root: Root directory of project locally
            output_path: Where to store built image on gateway

        Raises:
            NotImplementedError: Docker packaging is not supported
        """
        raise NotImplementedError(
            "Docker packaging is not implemented. "
            "Use --packaging podman instead. "
            "Docker does not work in SLURM job environments because it requires "
            "systemd user sessions (/run/user/{uid}/) which are not available in batch jobs."
        )

    def get_load_command(self, image_path: Path, storage_root: str = None, run_root: str = None) -> str:
        """Get command to load Docker image - NOT IMPLEMENTED

        Raises:
            NotImplementedError: Docker packaging is not supported
        """
        raise NotImplementedError("Docker packaging is not implemented. Use PodmanPackaging instead.")

    def get_run_command(
        self,
        image_name: str,
        image_tag: str,
        mounts: dict[str, str],
        ports: dict[int, int],
        entrypoint_args: list[str],
        storage_root: str = None,
        run_root: str = None,
    ) -> str:
        """Generate Docker run command - NOT IMPLEMENTED

        Raises:
            NotImplementedError: Docker packaging is not supported
        """
        raise NotImplementedError("Docker packaging is not implemented. Use PodmanPackaging instead.")

    def get_image_name(self) -> str:
        """Get the Docker image name"""
        return self.image_name

    def get_image_tag(self) -> str:
        """Get the Docker image tag"""
        return self.image_tag
