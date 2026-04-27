"""Stub TaskDefinition + worker entrypoint for the F5 failover harness.

Used as both:
- a TaskDefinition handed to RustSecondaryCoordinator (it's an attribute
  bag, not an instance — `run_secondary` introspects via getattr)
- a worker module the secondary spawns (the worker reads commands from
  the manager via the dynamic_batch_rs.comm interface and replies Done
  after a short sleep)

Kept minimal so the F5 test focuses on the failover protocol, not on
real per-binary work.
"""

from __future__ import annotations

import sys
import time


def estimate_memory(binary_size: int) -> int:
    return 100 * 1024 * 1024


def get_stages():
    return []


def organize_and_sort_items(items):
    return list(items)


def get_worker_module() -> str:
    return "dynamic_batch.tests._failover_stub_worker"


def add_task_arguments(parser) -> None:
    pass


def build_worker_command_args(args, source_dir, output_dir, skip_existing):
    return []


def get_output_filename_pattern(input_filename: str) -> str:
    return f"{input_filename}.done"


def get_reserved_memory_per_worker() -> int:
    return 50 * 1024 * 1024


def _run_worker_loop() -> None:
    """Worker subprocess entrypoint.

    Connects via dynamic_batch_rs.comm, replies Ready, then for each
    ProcessTask sleeps briefly and replies Done. Stops on Stop or when
    the manager disconnects.
    """
    import argparse

    from dynamic_batch_rs.comm import (
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
        comm = UnixSocketInterface(args.dynamic_queue)

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
