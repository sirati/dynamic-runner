from dataclasses import dataclass


@dataclass
class PackagingConfig:
    """Configuration for packaging method"""

    method: str  # "docker" or "podman"


def create_packaging_method(config: PackagingConfig):
    """Factory function to create appropriate packaging instance"""
    if config.method == "docker":
        from .docker.docker_packaging import DockerPackaging

        return DockerPackaging()
    elif config.method == "podman":
        from .podman.podman_packaging import PodmanPackaging

        return PodmanPackaging()
    else:
        raise ValueError(f"Unknown packaging method: {config.method}")


__all__ = ["PackagingConfig", "create_packaging_method"]
