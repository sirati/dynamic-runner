import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class DockerPackaging:
    """Docker-based packaging implementation using Nix"""

    def __init__(self):
        self.image_name = "asm-tokenizer"
        self.image_tag = "latest"

    def build_image(self, gateway, project_root: Path, output_path: Path) -> Path:
        """Build Docker image using Nix on gateway

        Args:
            gateway: Gateway instance to execute build commands
            project_root: Root directory of project on gateway
            output_path: Where to store built image on gateway

        Returns:
            Path to built image file on gateway
        """
        logger.info("Building Docker image using Nix on gateway...")

        # Ensure output directory exists
        if isinstance(output_path, str):
            from pathlib import Path

            parent_dir = str(Path(output_path).parent)
        else:
            parent_dir = output_path.parent
        gateway.create_directory(parent_dir)

        # Build image using nix
        build_cmd = "nix build .#dockerImage --out-link docker-image-result"
        returncode, stdout, stderr = gateway.execute_command(build_cmd, cwd=project_root)

        if returncode != 0:
            logger.error(f"Nix build failed: {stderr}")
            raise RuntimeError(f"Docker image build failed: {stderr}")

        logger.info("Docker image built successfully")

        # Move result to output path
        if isinstance(project_root, str):
            result_path = f"{project_root}/docker-image-result"
        else:
            result_path = project_root / "docker-image-result"
        move_cmd = f"cp {result_path} {output_path}"
        returncode, stdout, stderr = gateway.execute_command(move_cmd)

        if returncode != 0:
            logger.error(f"Failed to move image: {stderr}")
            raise RuntimeError(f"Failed to move Docker image: {stderr}")

        # Clean up result symlink
        gateway.execute_command(f"rm -f {result_path}")

        logger.info(f"Docker image saved to {output_path}")
        return output_path

    def get_load_command(self, image_path: Path) -> str:
        """Get command to load Docker image on compute node

        Args:
            image_path: Path to image tarball on compute node

        Returns:
            Shell command to load image
        """
        return f"docker load < {image_path}"

    def get_run_command(
        self,
        image_name: str,
        image_tag: str,
        mounts: dict[str, str],
        ports: dict[int, int],
        entrypoint_args: list[str],
    ) -> str:
        """Generate Docker run command

        Args:
            image_name: Name of container image
            image_tag: Tag of container image
            mounts: Dict of host_path: container_path
            ports: Dict of host_port: container_port
            entrypoint_args: Arguments to pass to entrypoint

        Returns:
            Shell command to run container
        """
        cmd_parts = ["docker", "run", "--rm"]

        # Add volume mounts
        for host_path, container_path in mounts.items():
            cmd_parts.extend(["-v", f"{host_path}:{container_path}"])

        # Add port mappings
        for host_port, container_port in ports.items():
            cmd_parts.extend(["-p", f"{host_port}:{container_port}"])

        # Add image
        cmd_parts.append(f"{image_name}:{image_tag}")

        # Add entrypoint arguments
        cmd_parts.extend(entrypoint_args)

        return " ".join(cmd_parts)

    def get_image_name(self) -> str:
        """Get the Docker image name"""
        return self.image_name

    def get_image_tag(self) -> str:
        """Get the Docker image tag"""
        return self.image_tag
