"""Thin Python shim over the Rust ``RustLocalGateway`` pyclass.

The actual gateway logic (subprocess execution, file copy, directory
creation, existence checks) lives in
``crates/dynrunner-gateway/src/local.rs`` and is exposed to Python by
``crates/dynrunner-pyo3/src/gateway/local.rs`` as
``dynamic_runner._native.RustLocalGateway``.

This module exists only to:

* Preserve the public class name ``LocalGateway`` and its import path
  for backward-compatible callers.
* Pin the Python ``Gateway`` Protocol (see ``packaging/gateway/__init__.py``)
  to the underlying Rust implementation by forwarding each method call
  with the call-site argument shapes Python consumers already use.
"""

from pathlib import Path

from dynamic_runner._native import RustLocalGateway


class LocalGateway:
    """Gateway implementation for local SLURM controller (thin shim)."""

    def __init__(self) -> None:
        self._inner = RustLocalGateway()
        self.connected = False
        # Exposed for callers that template paths like ``f"{gw.remote_home}/slurm"``
        # before handing them back to the gateway (slurm_config / pipeline /
        # job_manager). Tilde-expansion at gateway-call time is the Rust
        # gateway's responsibility; this attribute is purely informational.
        self.remote_home = Path.home()

    def connect(self) -> None:
        """Establish connection to local gateway."""
        self._inner.connect()
        self.connected = True

    def disconnect(self) -> None:
        """Close connection to gateway."""
        self._inner.disconnect()
        self.connected = False

    def execute_command(
        self, command: str, cwd: Path | None = None
    ) -> tuple[int, str, str]:
        """Execute command locally.

        Returns:
            (return_code, stdout, stderr)
        """
        return self._inner.execute_command(command, cwd)

    def transfer_file(
        self, local_path: Path | str, remote_path: Path | str
    ) -> None:
        """Transfer file from local to gateway (a copy in local mode)."""
        self._inner.transfer_file(Path(local_path), Path(remote_path))

    def upload_file(
        self, local_path: str | Path, remote_path: str | Path
    ) -> None:
        """Upload file from local to gateway (alias for transfer_file)."""
        self._inner.upload_file(Path(local_path), Path(remote_path))

    def download_file(
        self, remote_path: str | Path, local_path: str | Path
    ) -> None:
        """Download file from gateway to local (a copy in local mode)."""
        self._inner.download_file(Path(remote_path), Path(local_path))

    def create_directory(self, remote_path: Path | str) -> None:
        """Create directory on gateway (local mkdir)."""
        self._inner.create_directory(Path(remote_path))

    def file_exists(self, remote_path: Path | str) -> bool:
        """Check if file exists on gateway."""
        return self._inner.file_exists(Path(remote_path))

    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Setup port forwarding (no-op for local gateway)."""
        self._inner.setup_port_forwarding(local_port, remote_port)
