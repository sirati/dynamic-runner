# =====================================================================
# WARNING — PYTHON BRIDGE ONLY. NO LOGIC HERE.
# =====================================================================
# This file is a thin PyO3 / CLI / config bridge. ALL business logic,
# lifecycle, state-tracking, async orchestration, and process management
# lives in Rust under `crates/dynrunner-<crate>/src/<corresponding>.rs`.
# If you find yourself adding logic here — STOP. Put it in Rust and
# call it from this file via PyO3.
# =====================================================================
"""Config-bridge helpers for unusual worker deployments.

The defaults shipped from Rust (`SubprocessWorkerFactory`,
`ProcStatmMonitor`) cover the common case: launch a Python subprocess,
read RSS from `/proc/[pid]/statm`. The helpers here are CLI-bridges for
deployments where the worker launch needs special argv composition or
the resource probe needs a different data source. They are NOT
runtime objects — they synthesise a typed config (`WorkerSpec`,
`PyCallbackResourceMonitor`) that the Rust manager consumes.

- `PodmanExecWorkerFactory`: builds the `podman run ...` argv template
  as a `WorkerSpec`. The Rust `SubprocessWorkerFactory` then spawns,
  tracks, and tears down the resulting subprocess — Python never holds
  a `subprocess.Popen` handle.
- `CgroupResourceMonitor`: reads `memory.current` from cgroup v2 in a
  callback that Rust invokes once per resource-poll tick.

Usage:

    factory = PodmanExecWorkerFactory(image="myimage:latest", ...)
    rs.run_local(config, task=..., worker_spec=factory.to_worker_spec(), ...)
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import dynamic_runner as _rs


class PodmanExecWorkerFactory:
    """Build the `podman run` argv template for a containerised worker.

    The class is pure config: it composes a `WorkerSpec` from the
    image, worker module, mount points, and connection mode. The
    resulting `WorkerSpec` is passed verbatim into the Rust manager
    (e.g. `RustLocalManager(worker_spec=...)`) which spawns the
    subprocess, tracks the PID via the in-Rust subprocess factory, and
    tears down with the shared SIGTERM-then-SIGKILL ladder.

    `connection_mode` MUST match the value passed to the manager. In
    socketpair mode, the template injects `--dynamic_queue {COMM_FD}`;
    in named-socket mode, it injects `--socket-path {SOCKET_PATH}`.
    Rust substitutes the runtime value at spawn time.

    The image must contain the Python worker module on PYTHONPATH and
    a `python3` interpreter.
    """

    def __init__(
        self,
        image: str,
        worker_module: str,
        source_dir: Path | str,
        output_dir: Path | str,
        log_dir: Path | str,
        connection_mode: str = "socketpair",
        extra_podman_args: Sequence[str] | None = None,
        runtime: str | None = None,
    ) -> None:
        if connection_mode not in ("socketpair", "named"):
            raise ValueError(
                f"PodmanExecWorkerFactory: connection_mode must be "
                f"'socketpair' or 'named', got {connection_mode!r}"
            )
        self.image = image
        self.worker_module = worker_module
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        self.connection_mode = connection_mode
        self.extra_podman_args = list(extra_podman_args or [])
        self.runtime = runtime

    def to_worker_spec(self) -> _rs.WorkerSpec:
        """Render the podman argv template as a `WorkerSpec`.

        Rust substitutes `{COMM_FD}` / `{SOCKET_PATH}` / `{WORKER_ID}`
        / `{LOG_FILE}` per spawn; see `crates/dynrunner-pyo3/src/
        config/worker_spec.rs`.
        """
        argv: list[str] = ["podman"]
        if self.runtime is not None:
            argv += ["--runtime", self.runtime]
        argv += [
            "run",
            "--rm",
            "-v",
            f"{self.source_dir}:/app/src:ro",
            "-v",
            f"{self.output_dir}:/app/out",
            "-v",
            f"{self.log_dir}:/app/log",
            *self.extra_podman_args,
            self.image,
            "python3",
            "-m",
            self.worker_module,
        ]
        # Connection-mode dispatch: exactly ONE of the flag pairs goes
        # into the template. WorkerSpec.render() will substitute the
        # corresponding placeholder; the other placeholder would render
        # to an empty string, but it never lands in argv because we
        # only emit the flag pair that matches the configured mode.
        argv += _CONNECTION_MODE_ARGV[self.connection_mode]
        argv += [
            "--source",
            "/app/src",
            "--output",
            "/app/out",
            "--log-file",
            "/app/log/worker_{WORKER_ID}.log",
        ]
        return _rs.WorkerSpec(argv=argv)


# Connection-mode → comm-arg pair. Dispatching through a table keeps the
# argv-builder free of `if mode == "socketpair"` branches; adding a new
# transport later means one new table entry, not a new call-site branch.
_CONNECTION_MODE_ARGV: dict[str, list[str]] = {
    "socketpair": ["--dynamic_queue", "{COMM_FD}"],
    "named": ["--socket-path", "{SOCKET_PATH}"],
}


class CgroupResourceMonitor:
    """Read memory.current (and optionally cpu.stat) from cgroup v2.

    More accurate than `/proc/[pid]/statm` for containerised workers:
    sees the cgroup-effective working set rather than just the page
    table footprint of a single PID.
    """

    def __init__(self, cgroup_root: Path | str = "/sys/fs/cgroup") -> None:
        self.cgroup_root = Path(cgroup_root)

    def measure(self, pid: int | None) -> dict[str, int]:
        if pid is None:
            return {}
        cg_path = self._cgroup_for_pid(pid)
        if cg_path is None:
            return {}
        result: dict[str, int] = {}
        memory = self._read_int(cg_path / "memory.current")
        if memory is not None:
            result["memory"] = memory
        return result

    def into_callback_monitor(self) -> _rs.PyCallbackResourceMonitor:
        return _rs.PyCallbackResourceMonitor(self.measure)

    # ── internals ──

    def _cgroup_for_pid(self, pid: int) -> Path | None:
        cgroup_file = Path(f"/proc/{pid}/cgroup")
        if not cgroup_file.exists():
            return None
        try:
            for line in cgroup_file.read_text().splitlines():
                # cgroup v2 format: "0::<path>"
                parts = line.split(":", 2)
                if len(parts) == 3 and parts[1] == "":
                    rel = parts[2].lstrip("/")
                    return self.cgroup_root / rel
        except OSError:
            return None
        return None

    @staticmethod
    def _read_int(path: Path) -> int | None:
        try:
            return int(path.read_text().strip())
        except (OSError, ValueError):
            return None
