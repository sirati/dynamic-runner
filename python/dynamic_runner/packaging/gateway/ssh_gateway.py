import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


class SSHGateway:
    """Gateway implementation for SSH connection to SLURM controller using persistent connection"""

    def __init__(self, host: str, port: int, user: str | None):
        self.host = host
        self.port = port
        self.user = user
        self.connected = False
        self.remote_home = None
        self.control_path = None
        self.control_dir = None
        self.forwarded_ports: list[tuple[int, int]] = []  # (local_port, remote_port)
        self.gateway_ports_enabled = None  # None = unknown, True = enabled, False = disabled

    def connect(self) -> None:
        """Establish persistent SSH connection using ControlMaster"""
        if self.user:
            logger.info(f"Connecting to SSH gateway: {self.user}@{self.host}:{self.port}")
        else:
            logger.info(f"Connecting to SSH gateway: {self.host}:{self.port} (using SSH config)")

        # Create temporary directory for control socket
        self.control_dir = tempfile.mkdtemp(prefix="ssh-control-")
        self.control_path = f"{self.control_dir}/control-socket"
        logger.debug(f"SSH control socket path: {self.control_path}")

        # Build SSH command for master connection
        ssh_cmd = self._build_ssh_base_command()
        ssh_cmd.extend(
            [
                "-M",  # Master mode
                "-N",  # No remote command (just establish connection)
                "-f",  # Go to background
                "-o",
                f"ControlPath={self.control_path}",
                "-o",
                "ControlMaster=auto",
                "-o",
                "ControlPersist=yes",
                # Keepalive flags on the long-lived master connection.
                # If the master dies (NAT timeout, packet drop, wifi
                # blip on a laptop primary), every -R forward riding
                # on it also dies and every secondary on the cluster
                # side starts missing primary keepalives. Same
                # rationale as the per-secondary reverse tunnel in
                # `preparation.py::_create_ssh_reverse_tunnel`.
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=3",
                "-o",
                "TCPKeepAlive=yes",
                "-o",
                "ExitOnForwardFailure=yes",
            ]
        )

        # Add port forwarding if requested
        # Use 0.0.0.0 to bind to all interfaces on the gateway so compute nodes can connect
        for local_port, remote_port in self.forwarded_ports:
            ssh_cmd.extend(["-R", f"0.0.0.0:{remote_port}:localhost:{local_port}"])
            logger.debug(f"Adding port forwarding: gateway:0.0.0.0:{remote_port} -> localhost:{local_port}")

        if self.user:
            target = f"{self.user}@{self.host}"
        else:
            target = self.host

        ssh_cmd.append(target)

        # Establish master connection
        logger.info(f"Establishing persistent SSH master connection...")
        logger.debug(f"SSH master command: {' '.join(ssh_cmd)}")
        result = subprocess.run(ssh_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"SSH master connection failed with exit code {result.returncode}")
            logger.error(f"stderr: {result.stderr}")
            if result.stdout:
                logger.error(f"stdout: {result.stdout}")
            raise RuntimeError(f"SSH master connection failed: {result.stderr}")

        logger.info("SSH master connection established successfully")
        self.connected = True

        # Get remote home directory
        logger.debug("Detecting remote home directory...")
        returncode, stdout, stderr = self._execute_ssh_command("echo $HOME")
        if returncode == 0:
            self.remote_home = stdout.strip()
            logger.info(f"Remote home directory detected: {self.remote_home}")
        else:
            logger.warning(f"Could not determine remote home directory (exit code {returncode})")
            if stderr:
                logger.warning(f"stderr: {stderr}")

        # Check if forwarded ports are accessible from compute nodes
        self._check_gateway_ports()

        logger.info(f"SSH gateway connected successfully")

    def disconnect(self) -> None:
        """Close persistent SSH connection"""
        if not self.connected:
            logger.debug("SSH gateway already disconnected")
            return

        logger.info("Closing SSH master connection...")
        # Send exit command to master connection
        ssh_cmd = self._build_ssh_base_command()
        ssh_cmd.extend(
            [
                "-O",
                "exit",
                "-o",
                f"ControlPath={self.control_path}",
            ]
        )

        if self.user:
            target = f"{self.user}@{self.host}"
        else:
            target = self.host

        ssh_cmd.append(target)

        logger.debug(f"Sending exit command to SSH master")
        result = subprocess.run(ssh_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"SSH master exit command returned {result.returncode}: {result.stderr}")

        # Clean up control directory
        if self.control_dir:
            try:
                import shutil

                logger.debug(f"Cleaning up SSH control directory: {self.control_dir}")
                shutil.rmtree(self.control_dir)
            except Exception as e:
                logger.warning(f"Failed to clean up control directory {self.control_dir}: {e}")

        self.connected = False
        logger.info("SSH gateway disconnected")

    def _check_gateway_ports(self) -> None:
        """Check if forwarded ports are accessible from remote compute nodes"""
        if not self.forwarded_ports:
            return

        for local_port, remote_port in self.forwarded_ports:
            logger.debug(f"Checking binding for gateway port {remote_port}...")

            # Check what address the port is bound to
            returncode, stdout, stderr = self._execute_ssh_command(f"ss -tulpn 2>/dev/null | grep ':{remote_port}'")

            if returncode == 0 and stdout:
                # Parse ss output - format is: "tcp LISTEN ... LOCAL_ADDR:PORT REMOTE_ADDR:PORT"
                # We need to check the LOCAL_ADDR column (field before the port)
                # Example: "tcp   LISTEN 0      128        127.0.0.1:6000       0.0.0.0:*"
                #                                          ^^^^^^^^^^ this is what we care about
                lines = stdout.strip().split("\n")
                is_public = False
                is_localhost_only = True

                for line in lines:
                    # Look for the local address:port pattern
                    if f":{remote_port}" in line:
                        parts = line.split()
                        # Find the field with our port
                        for part in parts:
                            if part.endswith(f":{remote_port}"):
                                # This is the local address:port
                                if part.startswith("0.0.0.0:") or part.startswith("*:"):
                                    is_public = True
                                    is_localhost_only = False
                                elif part.startswith("[::]"):
                                    is_public = True
                                    is_localhost_only = False
                                elif part.startswith("127.0.0.1:") or part.startswith("[::1]:"):
                                    # Found localhost binding, but keep checking other lines
                                    pass
                                break

                if is_public:
                    logger.info(f"✓ Gateway port {remote_port} is publicly accessible")
                    self.gateway_ports_enabled = True
                elif is_localhost_only:
                    logger.warning(f"✗ Gateway port {remote_port} is bound to localhost only")
                    logger.warning(f"  Port binding details:")
                    for line in stdout.strip().split("\n"):
                        logger.warning(f"    {line}")
                    logger.warning(f"  This means compute nodes cannot connect to the primary coordinator")
                    logger.warning(f"  The gateway SSH server has 'GatewayPorts no' (default)")
                    logger.warning(
                        f"  Will need to use reverse connection strategy (secondary listens, primary connects)"
                    )
                    self.gateway_ports_enabled = False
                else:
                    logger.warning(f"Could not parse port binding: {stdout.strip()}")
                    self.gateway_ports_enabled = None
            else:
                logger.warning(f"Could not check port {remote_port} binding (exit code {returncode})")
                if stderr:
                    logger.debug(f"stderr: {stderr}")
                self.gateway_ports_enabled = None

    def _build_ssh_base_command(self) -> list[str]:
        """Build base SSH command with port and common options"""
        cmd = ["ssh"]

        if self.port != 22:
            cmd.extend(["-p", str(self.port)])

        # Disable host key checking warnings (optional, can be made configurable)
        # cmd.extend(["-o", "StrictHostKeyChecking=no"])
        # cmd.extend(["-o", "UserKnownHostsFile=/dev/null"])

        return cmd

    def _build_ssh_command(self, remote_command: str) -> list[str]:
        """Build SSH command that uses the persistent connection"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        ssh_cmd = self._build_ssh_base_command()
        ssh_cmd.extend(
            [
                "-o",
                f"ControlPath={self.control_path}",
                "-o",
                "ControlMaster=no",
            ]
        )

        if self.user:
            target = f"{self.user}@{self.host}"
        else:
            target = self.host

        ssh_cmd.append(target)
        ssh_cmd.append(remote_command)

        return ssh_cmd

    def _execute_ssh_command(self, command: str, cwd: str | None = None) -> tuple[int, str, str]:
        """Execute command via persistent SSH connection

        Args:
            command: Command to execute
            cwd: Optional working directory

        Returns:
            (return_code, stdout, stderr)
        """
        # Wrap command with cd if cwd is provided
        if cwd:
            command = f"cd {cwd} && {command}"

        ssh_cmd = self._build_ssh_command(command)

        logger.debug(f"Executing SSH command: {command[:100]}{'...' if len(command) > 100 else ''}")
        result = subprocess.run(ssh_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.debug(f"Command failed with exit code {result.returncode}")
            if result.stderr:
                logger.debug(f"stderr: {result.stderr[:200]}{'...' if len(result.stderr) > 200 else ''}")

        return result.returncode, result.stdout, result.stderr

    def execute_command(self, command: str, cwd: str | None = None) -> tuple[int, str, str]:
        """Execute command on gateway

        Args:
            command: Shell command to execute
            cwd: Optional working directory

        Returns:
            (return_code, stdout, stderr)
        """
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        return self._execute_ssh_command(command, cwd)

    def transfer_file(self, local_path: Path, remote_path: Path | str) -> None:
        """Transfer file from local to gateway using scp over persistent connection"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        remote_path_str = str(remote_path)

        # Expand ~ if present
        if remote_path_str.startswith("~") and self.remote_home:
            expanded_path = remote_path_str.replace("~", self.remote_home, 1)
        else:
            expanded_path = remote_path_str

        # Get file size for logging
        try:
            file_size_mb = local_path.stat().st_size / (1024 * 1024)
            logger.info(f"Transferring file: {local_path.name} ({file_size_mb:.1f} MB) -> {expanded_path}")
        except Exception:
            logger.info(f"Transferring file: {local_path} -> {expanded_path}")

        # Build scp command using the same control socket
        scp_cmd = ["scp"]

        if self.port != 22:
            scp_cmd.extend(["-P", str(self.port)])

        # Use the same control socket
        scp_cmd.extend(
            [
                "-o",
                f"ControlPath={self.control_path}",
            ]
        )

        # Source and destination
        local_path_str = str(local_path)

        if self.user:
            remote_target = f"{self.user}@{self.host}:{expanded_path}"
        else:
            remote_target = f"{self.host}:{expanded_path}"

        scp_cmd.extend([local_path_str, remote_target])

        logger.debug(f"SCP command: {' '.join(scp_cmd[:5])}...")
        result = subprocess.run(scp_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"SCP failed with exit code {result.returncode}")
            logger.error(f"stderr: {result.stderr}")
            if result.stdout:
                logger.error(f"stdout: {result.stdout}")
            raise RuntimeError(f"SCP failed: {result.stderr}")

        logger.info(f"File transferred successfully")
        logger.debug(f"Remote path: {expanded_path}")

    def upload_file(self, local_path: str | Path, remote_path: str | Path) -> None:
        """Upload file from local to gateway (alias for transfer_file)"""
        self.transfer_file(Path(local_path), remote_path)

    def download_file(self, remote_path: str | Path, local_path: str | Path) -> None:
        """Download file from gateway to local using scp"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        remote_path_str = str(remote_path)
        local_path_obj = Path(local_path)

        # Expand ~ if present
        if remote_path_str.startswith("~") and self.remote_home:
            expanded_path = remote_path_str.replace("~", self.remote_home, 1)
        else:
            expanded_path = remote_path_str

        logger.info(f"Downloading file: {expanded_path} -> {local_path_obj}")

        # Build scp command using the same control socket
        scp_cmd = ["scp"]

        if self.port != 22:
            scp_cmd.extend(["-P", str(self.port)])

        # Use the same control socket
        scp_cmd.extend(["-o", f"ControlPath={self.control_path}"])

        # Source and destination (reversed from upload)
        if self.user:
            remote_source = f"{self.user}@{self.host}:{expanded_path}"
        else:
            remote_source = f"{self.host}:{expanded_path}"

        scp_cmd.extend([remote_source, str(local_path_obj)])

        logger.debug(f"SCP command: {' '.join(scp_cmd[:5])}...")
        result = subprocess.run(scp_cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"SCP download failed with exit code {result.returncode}")
            logger.error(f"stderr: {result.stderr}")
            if result.stdout:
                logger.error(f"stdout: {result.stdout}")
            raise RuntimeError(f"SCP download failed: {result.stderr}")

        logger.info(f"File downloaded successfully")

    def create_directory(self, remote_path: Path | str) -> None:
        """Create directory on gateway (including parents)"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        remote_path_str = str(remote_path)
        logger.info(f"Creating directory: {remote_path_str}")

        # Expand ~ if present
        if remote_path_str.startswith("~") and self.remote_home:
            expanded_path = remote_path_str.replace("~", self.remote_home, 1)
        else:
            expanded_path = remote_path_str

        returncode, stdout, stderr = self._execute_ssh_command(f"mkdir -p {expanded_path}")

        if returncode != 0:
            logger.error(f"Failed to create directory {remote_path_str}")
            logger.error(f"exit code: {returncode}, stderr: {stderr}")
            raise RuntimeError(f"Failed to create directory {remote_path_str}: {stderr}")

        logger.info(f"Directory created successfully")

    def file_exists(self, remote_path: Path | str) -> bool:
        """Check if file exists on gateway"""
        if not self.connected:
            raise RuntimeError("Gateway not connected")

        remote_path_str = str(remote_path)
        logger.debug(f"Checking if file exists: {remote_path_str}")
        # Expand ~ if present
        if remote_path_str.startswith("~") and self.remote_home:
            expanded_path = remote_path_str.replace("~", self.remote_home, 1)
        else:
            expanded_path = remote_path_str

        returncode, _, _ = self._execute_ssh_command(f"test -e {expanded_path}")
        exists = returncode == 0
        logger.debug(f"File exists check result: {exists}")
        return exists

    def sync_project(self, local_project_root: Path, remote_project_root: Path | str) -> None:
        """Synchronize project files to gateway using rsync over SSH

        TODO REMOVE THIS, WE NEVER WANT TO DO THIS AS WE SENT A DOCKER IMAGE INSTEAD!!!
        """
        raise RuntimeError("TODO REMOVE THIS, WE NEVER WANT TO DO THIS AS WE SENT A DOCKER IMAGE INSTEAD!!!")

    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Setup SSH remote port forwarding: gateway:remote_port -> localhost:local_port

        This must be called BEFORE connect() to take effect.

        Args:
            local_port: Port on local machine where primary listens
            remote_port: Port on gateway that secondaries will connect to
        """
        if self.connected:
            raise RuntimeError("Cannot setup port forwarding after connection established. Call before connect().")

        logger.info(f"Configuring SSH port forwarding: gateway:{remote_port} -> localhost:{local_port}")
        self.forwarded_ports.append((local_port, remote_port))
