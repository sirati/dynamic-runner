import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class SSHGateway:
    """Gateway implementation for SSH connection to SLURM controller"""

    def __init__(self, host: str, port: int, user: str | None):
        self.host = host
        self.port = port
        self.user = user
        self.connected = False
        self.remote_home = None

    def connect(self) -> None:
        """Establish connection to SSH gateway"""
        if self.user:
            logger.info(f"Connecting to SSH gateway: {self.user}@{self.host}:{self.port}")
        else:
            logger.info(f"Connecting to SSH gateway: {self.host}:{self.port} (using SSH config)")

        # Test connection
        returncode, stdout, stderr = self._execute_ssh_command("echo 'Connection test'")
        if returncode != 0:
            raise RuntimeError(f"SSH connection failed: {stderr}")

        self.connected = True

        # Get remote home directory
        returncode, stdout, stderr = self._execute_ssh_command("echo $HOME")
        if returncode == 0:
            self.remote_home = stdout.strip()
            logger.info(f"SSH gateway connected successfully (remote home: {self.remote_home})")
        else:
            logger.warning("Could not determine remote home directory")
            logger.info("SSH gateway connected successfully")

    def disconnect(self) -> None:
        """Close connection to gateway"""
        self.connected = False
        logger.info("SSH gateway disconnected")

    def _build_ssh_command(self, remote_command: str) -> list[str]:
        """Build SSH command with proper escaping"""
        if self.user:
            target = f"{self.user}@{self.host}"
        else:
            target = self.host

        return [
            "ssh",
            "-p",
            str(self.port),
            target,
            remote_command,
        ]

    def _execute_ssh_command(self, command: str, cwd: Path | None = None) -> tuple[int, str, str]:
        """Execute SSH command"""
        if cwd:
            command = f"cd {cwd} && {command}"

        ssh_cmd = self._build_ssh_command(command)

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=300,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            logger.error(f"SSH command timed out: {command}")
            return -1, "", "Command timed out"
        except Exception as e:
            logger.error(f"SSH command execution failed: {e}")
            return -1, "", str(e)

    def execute_command(self, command: str, cwd: Path | None = None) -> tuple[int, str, str]:
        """Execute command on gateway

        Returns:
            (return_code, stdout, stderr)
        """
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        logger.debug(f"Executing via SSH: {command}")
        return self._execute_ssh_command(command, cwd)

    def transfer_file(self, local_path: Path, remote_path: Path) -> None:
        """Transfer file from local to gateway using scp"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        logger.debug(f"Transferring {local_path} to {self.host}:{remote_path}")

        # Expand ~ using remote home directory
        path_str = str(remote_path)
        if path_str.startswith("~") and self.remote_home:
            path_str = path_str.replace("~", self.remote_home, 1)
            remote_path = path_str

        # Ensure remote directory exists
        self.create_directory(Path(remote_path).parent)

        if self.user:
            target = f"{self.user}@{self.host}:{remote_path}"
        else:
            target = f"{self.host}:{remote_path}"

        scp_cmd = [
            "scp",
            "-P",
            str(self.port),
            str(local_path),
            target,
        ]

        try:
            result = subprocess.run(
                scp_cmd,
                capture_output=True,
                text=True,
                timeout=600,
            )
            if result.returncode != 0:
                raise RuntimeError(f"SCP failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            raise RuntimeError("File transfer timed out")
        except Exception as e:
            logger.error(f"File transfer failed: {e}")
            raise

    def create_directory(self, remote_path: Path) -> None:
        """Create directory on gateway"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        # Expand ~ using remote home directory
        path_str = str(remote_path)
        if path_str.startswith("~") and self.remote_home:
            path_str = path_str.replace("~", self.remote_home, 1)

        command = f"mkdir -p {path_str}"
        returncode, stdout, stderr = self._execute_ssh_command(command)

        if returncode != 0:
            raise RuntimeError(f"Directory creation failed: {stderr}")

        logger.debug(f"Created directory: {remote_path}")

    def file_exists(self, remote_path: Path) -> bool:
        """Check if file exists on gateway"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        # Expand ~ using remote home directory
        path_str = str(remote_path)
        if path_str.startswith("~") and self.remote_home:
            path_str = path_str.replace("~", self.remote_home, 1)

        command = f"test -e {path_str} && echo exists || echo notfound"
        returncode, stdout, stderr = self._execute_ssh_command(command)

        return stdout.strip() == "exists"
