import logging
import subprocess
from pathlib import Path
from shutil import copy2

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
            logger.error(f"File transfer failed: {e}")
            raise

    def create_directory(self, remote_path: Path) -> None:
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
