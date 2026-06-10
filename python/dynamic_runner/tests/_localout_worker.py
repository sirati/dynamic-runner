"""Worker for the local-mode output-delivery pin.

Trimmed copy of ``tests/e2e/test_consumer/worker.py``. Single concern:
for each dispatched payload-only task, write one staged file and
publish it — via the real :func:`dynamic_runner.worker.publish`
explicit-``dst`` form — into the worker's framework-threaded
``--output`` directory (``SecondaryConfig.output_dir`` as injected by
``subprocess_factory.rs::legacy_argv``). The companion pin
(``tests/test_local_output_delivery.py``) asserts the file lands in
the OPERATOR's ``--output``.

Staging root: the test sets ``DYNRUNNER_PUBLISH_SRC_ROOT`` to a
tmpdir (the same env contract the e2e harness and the SLURM wrapper
use), so ``publish`` accepts the staged src.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from dynamic_runner.worker import (
    NonRecoverableError,
    Task,
    WorkerOutput,
    publish as _publish,
    run as _run_worker,
    task_function,
)
from dynamic_runner.worker.publish import DEFAULT_SRC_ROOT, ENV_SRC_ROOT

from dynamic_runner.tests._localout_consumer import output_filename


_logger = logging.getLogger(__name__)

# Module-level handle to the framework-injected ``--output`` directory,
# pulled out in :func:`_on_args` (same shape as the e2e consumer).
_OUTPUT_DIR: Path | None = None


@task_function
def handle(task: Task) -> WorkerOutput:
    payload = task.payload or {}
    idx = payload.get("idx")
    if idx is None:
        raise NonRecoverableError(f"task has no 'idx' in payload: {payload!r}")
    if _OUTPUT_DIR is None:
        raise NonRecoverableError(
            "worker output_dir not configured — did the framework "
            "forget to pass --output?"
        )

    staging = Path(os.environ.get(ENV_SRC_ROOT, DEFAULT_SRC_ROOT))
    staging.mkdir(parents=True, exist_ok=True)
    name = output_filename(idx)
    staged = staging / name
    staged.write_bytes(f"localout-{idx}\n".encode())
    _logger.info("item-%d: publishing %s -> %s", idx, staged, _OUTPUT_DIR / name)
    _publish(staged, dst=_OUTPUT_DIR / name)
    return WorkerOutput()


def _build_parser() -> argparse.ArgumentParser:
    """Worker CLI — exactly the framework-injected flags
    (``subprocess_factory.rs::legacy_argv``); this consumer adds none.
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int, metavar="SOCKET_FD")
    group.add_argument("--socket-path", type=str, metavar="SOCKET_PATH")
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--skip_existing", action="store_true")
    return parser


def _on_args(args: argparse.Namespace) -> None:
    global _OUTPUT_DIR
    _OUTPUT_DIR = Path(args.output)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [localout-worker] %(levelname)s %(name)s: %(message)s",
    )
    _logger.info("worker started: output=%s", args.output)


def main() -> None:
    _run_worker(argparser=_build_parser(), on_args=_on_args)


if __name__ == "__main__":
    main()
