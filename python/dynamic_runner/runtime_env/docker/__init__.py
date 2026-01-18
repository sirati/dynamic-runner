from dataclasses import dataclass
from pathlib import Path


@dataclass
class PackagingConfig:
    """Configuration for packaging method"""

    method: str  # "docker"
    build_on_gateway: bool = True


class PackagingMethod:
    """Base interface for packaging methods"""

    def build_image(self, gateway, project_root: Path, output_path: Path) -> Path:
        """Build deployable image

        Args:
            gateway: Gateway instance to execute build commands
            project_root: Root directory of project
            output_path: Where to store built image

        Returns:
            Path to built image file
        """
        raise NotImplementedError

    def get_load_command(self, image_path: Path) -> str:
        """Get command to load image on compute node

        Args:
            image_path: Path to image file on compute node

        Returns:
            Shell command to load image
        """
        raise NotImplementedError

    def get_run_command(
        self,
        image_name: str,
        image_tag: str,
        mounts: dict[str, str],
        ports: dict[int, int],
        entrypoint_args: list[str],
    ) -> str:
        """Generate container invocation command

        Args:
            image_name: Name of container image
            image_tag: Tag of container image
            mounts: Dict of host_path: container_path
            ports: Dict of host_port: container_port
            entrypoint_args: Arguments to pass to entrypoint

        Returns:
            Shell command to run container
        """
        raise NotImplementedError


def create_packaging_method(config: PackagingConfig) -> PackagingMethod:
    """Factory function to create packaging method instance"""
    if config.method == "docker":
        from .docker_packaging import DockerPackaging

        return DockerPackaging()
    else:
        raise ValueError(f"Unknown packaging method: {config.method}")
