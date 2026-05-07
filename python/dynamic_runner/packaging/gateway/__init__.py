from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass
class GatewayConfig:
    """Configuration for gateway connection"""

    mode: str  # "local" or "ssh"
    ssh_host: str | None = None
    ssh_port: int | None = None
    ssh_user: str | None = None
    # Explicit auth primitives. When set, the framework injects them
    # into every ssh/scp subprocess in the gateway path (master,
    # exec, scp upload/download, reverse tunnel). Bypasses
    # ~/.ssh/config-driven IdentityAgent over-offering — common on
    # NixOS+gnome-keyring/1password setups where a many-key agent
    # trips MaxAuthTries before reaching the right key.
    ssh_identity_file: str | None = None
    ssh_config_file: str | None = None


class Gateway(Protocol):
    """Interface for gateway implementations"""

    def connect(self) -> None:
        """Establish connection to gateway"""
        ...

    def disconnect(self) -> None:
        """Close connection to gateway"""
        ...

    def execute_command(self, command: str, cwd: Path | None = None) -> tuple[int, str, str]:
        """Execute command on gateway

        Returns:
            (return_code, stdout, stderr)
        """
        ...

    def transfer_file(self, local_path: Path, remote_path: Path) -> None:
        """Transfer file from local to gateway"""
        ...

    def upload_file(self, local_path: str | Path, remote_path: str | Path) -> None:
        """Upload file from local to gateway"""
        ...

    def download_file(self, remote_path: str | Path, local_path: str | Path) -> None:
        """Download file from gateway to local"""
        ...

    def create_directory(self, remote_path: Path | str) -> None:
        """Create directory on gateway"""
        ...

    def file_exists(self, remote_path: Path) -> bool:
        """Check if file exists on gateway"""
        ...

    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Setup SSH remote port forwarding: gateway:remote_port -> localhost:local_port

        This allows compute nodes to connect to the gateway port, which forwards to
        the primary coordinator running locally.

        Args:
            local_port: Port on local machine where primary listens
            remote_port: Port on gateway that secondaries will connect to
        """
        ...


def parse_gateway_url(url: str) -> GatewayConfig:
    """Parse gateway URL into configuration

    Supported formats:
    - local
    - ssh://user@host
    - ssh://user@host:port
    - ssh://host (uses SSH config for user and other settings)
    """
    if url == "local":
        return GatewayConfig(mode="local")

    if url.startswith("ssh://"):
        url = url[6:]  # Remove ssh://

        # Parse user@host:port or just host (for SSH config aliases)
        if "@" in url:
            user, host_port = url.split("@", 1)
        else:
            # No user specified, rely on SSH config
            user = None
            host_port = url

        if ":" in host_port:
            host, port_str = host_port.rsplit(":", 1)
            port = int(port_str)
        else:
            host = host_port
            port = 22

        return GatewayConfig(mode="ssh", ssh_user=user, ssh_host=host, ssh_port=port)

    raise ValueError(f"Invalid gateway URL: {url}. Use 'local' or 'ssh://[user@]host[:port]'")


def create_gateway(config: GatewayConfig) -> Gateway:
    """Factory function to create appropriate gateway instance"""
    if config.mode == "local":
        from .local_gateway import LocalGateway

        return LocalGateway()
    elif config.mode == "ssh":
        from .ssh_gateway import SSHGateway

        return SSHGateway(
            host=config.ssh_host,
            port=config.ssh_port,
            user=config.ssh_user,
            identity_file=config.ssh_identity_file,
            config_file=config.ssh_config_file,
        )
    else:
        raise ValueError(f"Unknown gateway mode: {config.mode}")
