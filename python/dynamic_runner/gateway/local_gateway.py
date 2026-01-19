import logging
import subprocess
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns

logger = logging.getLogger(__name__)


class LocalGateway:
    """Gateway implementation for local SLURM controller"""

    def __init__(self):
        self.connected = False
        self.remote_home = Path.home()

    def connect(self) -> None:
        """Establish connection to local gateway"""
        logger.info(f"Using local gateway (direct SLURM access, home: {self.remote_home})")
        self.connected = True

    def disconnect(self) -> None:
        """Close connection to gateway"""
        self.connected = False
        logger.info("Local gateway disconnected")

    def execute_command(self, command: str, cwd: Path | None = None) -> tuple[int, str, str]:
        """Execute command locally

        Returns:
            (return_code, stdout, stderr)
        """
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        logger.debug(f"Executing locally: {command}")

        try:
            result = subprocess.run(
                command,
                shell=True,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=300,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out: {command}")
            return -1, "", "Command timed out"
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            return -1, "", str(e)

    def transfer_file(self, local_path: Path, remote_path: Path) -> None:
        """Transfer file from local to gateway (just a copy for local mode)"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        # Expand ~ using home directory
        path_str = str(remote_path)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_path = Path(path_str)

        logger.debug(f"Copying {local_path} to {remote_path}")

        try:
            remote_path.parent.mkdir(parents=True, exist_ok=True)
            copy2(local_path, remote_path)
        except Exception as e:
            logger.error(f"File copy failed: {e}")
            raise RuntimeError(f"File copy failed: {e}")

    def upload_file(self, local_path: str | Path, remote_path: str | Path) -> None:
        """Upload file from local to gateway (alias for transfer_file in local mode)"""
        self.transfer_file(Path(local_path), Path(remote_path))

    def download_file(self, remote_path: str | Path, local_path: str | Path) -> None:
        """Download file from gateway to local (just a copy for local mode)"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        remote_path_obj = Path(remote_path)
        local_path_obj = Path(local_path)

        # Expand ~ using home directory
        path_str = str(remote_path_obj)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_path_obj = Path(path_str)

        logger.debug(f"Copying {remote_path_obj} to {local_path_obj}")

        try:
            local_path_obj.parent.mkdir(parents=True, exist_ok=True)
            copy2(remote_path_obj, local_path_obj)
        except Exception as e:
            logger.error(f"File copy failed: {e}")
            raise RuntimeError(f"File copy failed: {e}")

    def create_directory(self, remote_path: Path | str) -> None:
        """Create directory on gateway (local mkdir)"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        # Expand ~ using home directory
        path_str = str(remote_path)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_path = Path(path_str)

        try:
            remote_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {remote_path}")
        except Exception as e:
            logger.error(f"Directory creation failed: {e}")
            raise

    def file_exists(self, remote_path: Path) -> bool:
        """Check if file exists on gateway"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        # Expand ~ using home directory
        path_str = str(remote_path)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_path = Path(path_str)

        return remote_path.exists()

    def sync_project(self, local_project_root: Path, remote_project_root: Path) -> None:
        """Synchronize project files to gateway (local copy with exclusions)"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        # Expand ~ using home directory
        path_str = str(remote_project_root)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_project_root = Path(path_str)

        logger.info(f"Syncing project {local_project_root} to {remote_project_root}")

        # Remove existing directory if present
        if remote_project_root.exists():
            import shutil

            shutil.rmtree(remote_project_root)

        # Copy with exclusions
        copytree(
            local_project_root,
            remote_project_root,
            ignore=ignore_patterns(
                ".git",
                "__pycache__",
                "*.pyc",
                "*.pyo",
                "*.egg-info",
                ".pytest_cache",
                ".mypy_cache",
                ".ruff_cache",
                "result",
                "result-*",
                ".direnv",
                ".venv",
                "venv",
                "node_modules",
                ".DS_Store",
            ),
        )

        logger.info(f"Project synchronized to {remote_project_root}")

    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Setup port forwarding (no-op for local gateway)

        Args:
            local_port: Port on local machine where primary listens
            remote_port: Port that secondaries will connect to
        """
        # For local gateway, no forwarding needed - everything is local
        logger.debug(f"Port forwarding not needed for local gateway (local:{local_port})")
