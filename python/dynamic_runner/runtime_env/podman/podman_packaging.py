import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class PodmanPackaging:
    """Podman-based packaging implementation for SLURM cluster environments

    This implementation is specifically designed for SLURM batch job environments
    where systemd user sessions are not available. It uses explicit storage paths
    in /tmp to avoid reliance on /run/user/{uid}/ directories.
    """

    def __init__(self):
        self.image_name = "asm-tokenizer"
        self.image_tag = "latest"

    def build_image(self, gateway, local_project_root: Path, output_path: Path) -> Path:
        """Build container image locally using Nix, then transfer to gateway

        Args:
            gateway: Gateway instance to transfer image
            local_project_root: Root directory of project locally
            output_path: Where to store built image on gateway

        Returns:
            Path to built image file on gateway
        """
        logger.info("Building container image locally using Nix...")

        # Build image using nix locally
        build_cmd = ["nix", "build", ".#dockerImage", "--out-link", "docker-image-result"]
        result = subprocess.run(
            build_cmd,
            cwd=str(local_project_root),
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error(f"Nix build failed: {result.stderr}")
            raise RuntimeError(f"Container image build failed: {result.stderr}")

        logger.info("Container image built successfully")

        # The result is a symlink to the nix store
        local_image_path = local_project_root / "docker-image-result"

        if not local_image_path.exists():
            raise RuntimeError("Container image result not found after build")

        # Ensure output directory exists on gateway
        if isinstance(output_path, str):
            parent_dir = str(Path(output_path).parent)
        else:
            parent_dir = str(output_path.parent)
        gateway.create_directory(parent_dir)

        # Remove existing image file if present (may be read-only from previous run)
        logger.debug(f"Removing existing image at {output_path} if present")
        returncode, _, _ = gateway.execute_command(f"rm -f {output_path}")
        if returncode != 0:
            logger.warning(f"Could not remove existing image at {output_path}")

        # Transfer image to gateway
        logger.info(f"Transferring container image to gateway at {output_path}...")
        gateway.transfer_file(local_image_path, output_path)

        # Clean up local result symlink
        try:
            local_image_path.unlink()
            logger.debug("Removed local image symlink")
        except Exception as e:
            logger.warning(f"Failed to remove local result symlink: {e}")

        logger.info(f"Container image transferred to {output_path}")
        return output_path

    def get_load_command(self, image_path: str, storage_root: str, run_root: str) -> str:
        """Get command to load container image on compute node with Podman

        Args:
            image_path: Path to image tarball on compute node
            storage_root: Path for podman storage root (required for SLURM)
            run_root: Path for podman run root (required for SLURM)

        Returns:
            Shell command to load image using Podman with explicit paths
        """
        return f"podman --root {storage_root} --runroot {run_root} --runtime /usr/bin/crun load < {image_path}"

    def get_run_command(
        self,
        image_name: str,
        image_tag: str,
        mounts: dict[str, str],
        ports: dict[int, int],
        entrypoint_args: list[str],
        storage_root: str,
        run_root: str,
    ) -> str:
        """Generate Podman run command with explicit storage paths for SLURM

        Args:
            image_name: Name of container image
            image_tag: Tag of container image
            mounts: Dict of host_path: container_path
            ports: Dict of host_port: container_port
            entrypoint_args: Arguments to pass to entrypoint
            storage_root: Path for podman storage root (required for SLURM)
            run_root: Path for podman run root (required for SLURM)

        Returns:
            Shell command to run container with Podman
        """
        cmd_parts = [
            "podman",
            "--root",
            storage_root,
            "--runroot",
            run_root,
            "--runtime",
            "/usr/bin/crun",
            "run",
            "--rm",
        ]

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

    def get_images_command(self, storage_root: str, run_root: str) -> str:
        """Get command to list images

        Args:
            storage_root: Path for podman storage root
            run_root: Path for podman run root

        Returns:
            Shell command to list images
        """
        return f"podman --root {storage_root} --runroot {run_root} images"

    def get_version_command(self) -> str:
        """Get command to check Podman version

        Returns:
            Shell command to check version
        """
        return "podman --version"

    def get_image_name(self) -> str:
        """Get the container image name"""
        return self.image_name

    def get_image_tag(self) -> str:
        """Get the container image tag"""
        return self.image_tag

    def requires_storage_paths(self) -> bool:
        """Check if this packaging method requires explicit storage paths

        Returns:
            True - Podman in SLURM always requires explicit paths
        """
        return True
