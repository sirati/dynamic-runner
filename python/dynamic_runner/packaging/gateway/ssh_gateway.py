"""Thin Python shim around the Rust ``RustSshGateway`` PyO3 wrapper.

The public class name ``SSHGateway`` is preserved verbatim so call
sites (``pipeline.py``, ``preparation.py``, ``slurm_config.py``,
``job_manager.py``, packaging factories) keep working unchanged.
Every method delegates to the Rust gateway; the only piece that
stays Python-side is :meth:`auth_options`, which builds the list of
``ssh`` flags from ``identity_file`` / ``config_file`` for callers
that need the same auth contract on their own ssh subprocesses
(reverse-tunnel spawns in :mod:`preparation`). Once the Rust
gateway grows ``-i`` / ``-F`` plumbing in its ``connect`` / scp
machinery, the wrapper will start carrying those flags on its own
ssh invocations and ``auth_options`` will collapse to a thin
delegate too.
"""

from pathlib import Path

from dynamic_runner._native import RustSshGateway


class SSHGateway:
    """Gateway implementation for SSH connection to a SLURM controller.

    Persistent ControlMaster connection driven by the Rust
    ``RustSshGateway`` underneath. Every method delegates; this class
    only owns Python-side state for :meth:`auth_options`.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str | None,
        identity_file: str | None = None,
        config_file: str | None = None,
    ):
        self._inner = RustSshGateway(host, port, user, identity_file, config_file)

    # --- attribute mirrors -------------------------------------------------
    # Each attribute proxies to the Rust gateway so callers see the
    # same value the Rust side maintains. The original Python class
    # exposed these as plain instance attributes; properties keep the
    # read interface identical without storing duplicate state.

    @property
    def host(self) -> str:
        return self._inner.host

    @property
    def port(self) -> int:
        return self._inner.port

    @property
    def user(self) -> str | None:
        return self._inner.user

    @property
    def identity_file(self) -> str | None:
        value = self._inner.identity_file
        return None if value is None else str(value)

    @property
    def config_file(self) -> str | None:
        value = self._inner.config_file
        return None if value is None else str(value)

    @property
    def connected(self) -> bool:
        return self._inner.connected

    @property
    def remote_home(self) -> str | None:
        return self._inner.remote_home

    @property
    def forwarded_ports(self) -> list[tuple[int, int]]:
        return self._inner.forwarded_ports

    @property
    def gateway_ports_enabled(self) -> bool | None:
        return self._inner.gateway_ports_enabled

    # --- lifecycle ---------------------------------------------------------

    def connect(self) -> None:
        """Establish persistent SSH connection using ControlMaster."""
        self._inner.connect()

    def disconnect(self) -> None:
        """Close persistent SSH connection."""
        self._inner.disconnect()

    # --- commands / files --------------------------------------------------

    def execute_command(self, command: str, cwd: str | None = None) -> tuple[int, str, str]:
        """Execute a command on the gateway.

        Returns:
            ``(return_code, stdout, stderr)``.
        """
        return self._inner.execute_command(command, cwd)

    def transfer_file(self, local_path: Path, remote_path: Path | str) -> None:
        """Upload ``local_path`` to ``remote_path`` over the master connection."""
        self._inner.transfer_file(local_path, str(remote_path))

    def upload_file(self, local_path: str | Path, remote_path: str | Path) -> None:
        """Alias for :meth:`transfer_file` kept for callers that prefer the
        ``upload``/``download`` naming pair."""
        self.transfer_file(Path(local_path), remote_path)

    def download_file(self, remote_path: str | Path, local_path: str | Path) -> None:
        """Download ``remote_path`` from the gateway to ``local_path``."""
        self._inner.download_file(str(remote_path), Path(local_path))

    def create_directory(self, remote_path: Path | str) -> None:
        """Create ``remote_path`` (including parents) on the gateway."""
        self._inner.create_directory(str(remote_path))

    def file_exists(self, remote_path: Path | str) -> bool:
        """Return whether ``remote_path`` exists on the gateway."""
        return self._inner.file_exists(str(remote_path))

    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Setup SSH remote port forwarding: gateway:remote_port -> localhost:local_port.

        This must be called BEFORE :meth:`connect` to take effect.

        Args:
            local_port: Port on local machine where primary listens.
            remote_port: Port on gateway that secondaries will connect to.
        """
        self._inner.setup_port_forwarding(local_port, remote_port)

    # --- auth flags --------------------------------------------------------

    def auth_options(self) -> list[str]:
        """Explicit-auth flags applied to every ssh/scp invocation.

        ``-i``/``IdentitiesOnly=yes``/``IdentityAgent=none`` and ``-F``
        shape *which* credentials ssh considers — orthogonal to ``-p``
        (port). Exposed publicly so other framework-owned ssh
        subprocesses (e.g. ``preparation.py``'s reverse tunnel) can
        mirror the auth contract without bypassing the gateway.

        ``IdentityAgent=none`` is bundled with ``--ssh-identity-file``
        because ``IdentitiesOnly=yes`` alone does NOT prevent over-
        offering on systems where ``~/.ssh/config`` has
        ``Match host * → IdentityAgent <socket>`` (typical
        NixOS+gnome-keyring/1password setups). OpenSSH still
        enumerates agent identities ahead of the configured key,
        and each enumeration counts against the gateway sshd's
        MaxAuthTries — so a many-key agent kills the connection at
        "Too many authentication failures" before ``-i`` is reached.
        ``IdentityAgent=none`` is the only flag that fully shuts the
        agent out (``-o`` settings beat ``Match`` blocks). Single
        concern: "given an explicit identity, no agent may leak in".

        ``--ssh-config`` alone does NOT add ``IdentityAgent=none`` —
        the user's config-file is authoritative about agent behavior
        in that path.
        """
        opts: list[str] = []
        if self.identity_file is not None:
            opts.extend(
                [
                    "-i",
                    self.identity_file,
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "IdentityAgent=none",
                ]
            )
        if self.config_file is not None:
            opts.extend(["-F", self.config_file])
        return opts
