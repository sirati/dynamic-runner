"""Stub TaskDefinition + worker entrypoint for the F5 failover harness.

Used as both:
- a TaskDefinition handed to RustSecondaryCoordinator (it's an attribute
  bag, not an instance — `run_secondary` introspects via getattr)
- a worker module the secondary spawns (the worker reads commands from
  the manager via the dynamic_runner.comm interface and replies Done
  after a short sleep)

Kept minimal so the F5 test focuses on the failover protocol, not on
real per-binary work.
"""

from __future__ import annotations

import sys
import time

from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec


# ── Module-level TaskDefinition surface ─────────────────────────────────
# The failover-secondary subprocess imports this module and passes it as
# the `task` argument to `run_secondary`; the duck-typed protocol reads
# attributes off the module. Keep this in sync with `task_protocol.py`.

def get_phases() -> tuple[PhaseSpec, ...]:
    return (
        PhaseSpec(
            phase_id="sleep",
            types=(
                TaskTypeSpec(
                    type_id="default",
                    worker_module="dynamic_runner.tests._failover_stub_worker",
                    reserved_memory_per_worker=50 * 1024 * 1024,
                ),
            ),
        ),
    )


def discover_items(source_dir, args):
    return []


def estimate_memory(item) -> int:
    return 100 * 1024 * 1024


def add_task_arguments(parser) -> None:
    pass


def build_worker_command_args(type_id, args, source_dir, output_dir, skip_existing):
    return []


def get_output_filename_pattern(type_id, item) -> str:
    return f"{item.path}.done"


def on_run_start(source_dir, output_dir, args) -> None:
    pass


def on_run_end(success: bool) -> None:
    pass


def on_phase_start(phase_id) -> None:
    pass


def on_phase_end(phase_id, completed: int, failed: int) -> None:
    pass


# ── Worker subprocess entrypoint ────────────────────────────────────────


def _run_worker_loop() -> None:
    """Worker subprocess entrypoint.

    Connects via dynamic_runner.comm, replies Ready, then for each
    ProcessTask sleeps briefly and replies Done. Stops on Stop or when
    the manager disconnects.
    """
    import argparse
    import socket

    from dynamic_runner.comm import (
        DoneResponse,
        NamedSocketInterface,
        ReadyResponse,
        StopCommand,
        UnixSocketInterface,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--dynamic_queue", type=int, default=None)
    parser.add_argument("--socket-path", type=str, default=None)
    parser.add_argument("--source", type=str)
    parser.add_argument("--output", type=str)
    parser.add_argument("--log-file", type=str)
    parser.add_argument("--skip_existing", action="store_true")
    args, _ = parser.parse_known_args()

    if args.socket_path:
        comm = NamedSocketInterface(args.socket_path, is_server=False)
    else:
        # `UnixSocketInterface` takes a `socket.socket`, not a raw FD —
        # wrap the inherited FD via `socket.socket(fileno=...)` first
        # (the manager's `SubprocessWorkerFactory` passes a socketpair
        # FD via `--dynamic_queue`).
        sock = socket.socket(fileno=args.dynamic_queue)
        comm = UnixSocketInterface(sock)

    comm.send_response(ReadyResponse())

    while True:
        cmd = comm.receive_command(blocking=True)
        if cmd is None or isinstance(cmd, StopCommand):
            break
        # Simulate per-task work; small enough that the failover test
        # can elect mid-run, large enough that backoff/timing helpers
        # have a chance to fire.
        time.sleep(0.05)
        comm.send_response(DoneResponse())

    comm.close()


if __name__ == "__main__":
    _run_worker_loop()
    sys.exit(0)
