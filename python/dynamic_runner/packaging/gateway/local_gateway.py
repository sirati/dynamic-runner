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

    def transfer_file(self, local_path: Path | str, remote_path: Path | str) -> None:
        """Transfer file from local to gateway (just a copy for local mode)"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        local_path_obj = Path(local_path)
        remote_path_obj = Path(remote_path)

        # Expand ~ using home directory
        path_str = str(remote_path_obj)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_path_obj = Path(path_str)

        logger.debug(f"Copying {local_path_obj} to {remote_path_obj}")

        try:
            remote_path_obj.parent.mkdir(parents=True, exist_ok=True)
            # Pre-flight unlink of the destination. shutil.copy2 opens
            # the dest with O_WRONLY|O_TRUNC, which fails with EACCES
            # when the existing file is read-only — observed when
            # sources are produced by a nix derivation (mode 0444) and
            # a prior copy propagated those bits to the dest. unlink
            # no-ops when the dest is absent and only requires write
            # perm on the parent directory, which copy2 itself already
            # requires. Best-effort: if the unlink fails for any reason
            # we still attempt the copy so the failure surfaces with
            # the canonical copy2 error rather than ours.
            try:
                remote_path_obj.unlink(missing_ok=True)
            except OSError:
                pass
            copy2(local_path_obj, remote_path_obj)
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

        remote_path_obj = Path(remote_path)

        # Expand ~ using home directory
        path_str = str(remote_path_obj)
        if path_str.startswith("~"):
            path_str = path_str.replace("~", str(self.remote_home), 1)
            remote_path_obj = Path(path_str)

        try:
            remote_path_obj.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {remote_path_obj}")
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

    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Setup port forwarding (no-op for local gateway)

        Args:
            local_port: Port on local machine where primary listens
            remote_port: Port that secondaries will connect to
        """
        # For local gateway, no forwarding needed - everything is local
        logger.debug(f"Port forwarding not needed for local gateway (local:{local_port})")
