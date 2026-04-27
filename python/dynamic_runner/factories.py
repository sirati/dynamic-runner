"""Reference Python implementations of `WorkerFactory` and
`ResourceMonitor` using the Rust callback wrappers from M4.

The defaults shipped from Rust (`SubprocessWorkerFactory`,
`ProcStatmMonitor`) cover the common case: launch a Python subprocess,
read RSS from `/proc/[pid]/statm`. The classes here are escape hatches
for unusual deployments — they're examples task packages can copy
or subclass when the defaults don't fit.

- `PodmanExecWorkerFactory`: launches the worker inside a podman
  container, useful when the worker has system-level dependencies the
  host environment can't provide.
- `CgroupResourceMonitor`: reads memory (and CPU when available) from
  the v2 cgroup hierarchy, so it sees container-effective limits
  rather than host-process RSS.

Use these by passing them through the typed configs:

    factory = PodmanExecWorkerFactory(image="myimage:latest", ...)
    rs.run_local(config, task=..., factory=factory, ...)

(The current `run_local` doesn't yet accept a factory kwarg; pass via
the legacy `RustLocalManager` constructor or wrap them in
`dynamic_batch_rs.PyCallbackWorkerFactory` directly.)
"""

from __future__ import annotations

import os
import shlex
import subprocess
from collections.abc import Callable
from pathlib import Path

import dynamic_batch_rs as _rs


class PodmanExecWorkerFactory:
    """Spawn each worker inside a fresh podman container.

    The container shares the manager-side socketpair fd via
    `--add-host` and pass-through of the FD. The image must contain
    the Python worker module on PYTHONPATH and a `python3` interpreter.
    """

    def __init__(
        self,
        image: str,
        worker_module: str,
        source_dir: Path | str,
        output_dir: Path | str,
        log_dir: Path | str,
        extra_podman_args: list[str] | None = None,
        runtime: str = "/usr/bin/crun",
    ) -> None:
        self.image = image
        self.worker_module = worker_module
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        self.extra_podman_args = list(extra_podman_args or [])
        self.runtime = runtime
        self._children: list[subprocess.Popen] = []

    def spawn(self, worker_id: int, comm_fd: int | None, socket_path: str | None) -> int | None:
        """Launch the worker. Returns the spawned PID."""
        argv = [
            "podman",
            "--runtime",
            self.runtime,
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
            "--source",
            "/app/src",
            "--output",
            "/app/out",
            "--log-file",
            f"/app/log/worker_{worker_id}.log",
        ]
        if comm_fd is not None:
            argv += ["--dynamic_queue", str(comm_fd)]
            pass_fds = (comm_fd,)
        elif socket_path is not None:
            argv += ["--socket-path", socket_path]
            pass_fds = ()
        else:
            raise ValueError("PodmanExecWorkerFactory: need either comm_fd or socket_path")

        proc = subprocess.Popen(argv, pass_fds=pass_fds)
        self._children.append(proc)
        return proc.pid

    def into_callback_factory(self) -> _rs.PyCallbackWorkerFactory:
        """Wrap as the Rust-side PyCallbackWorkerFactory."""
        return _rs.PyCallbackWorkerFactory(self.spawn)

    def cleanup(self) -> None:
        """SIGTERM and reap any still-alive containers."""
        for proc in self._children:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


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
