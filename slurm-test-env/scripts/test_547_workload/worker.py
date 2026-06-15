"""No-op worker for the #547 chunked-spawn e2e test.

Single concern: be a syntactically-valid ``dynamic_runner`` worker for
the driver's two phases — ``seed`` and ``burst``. The test stresses the
PRIMARY's spawn-burst chunking path, not what workers do, so each task
just touches an output file and returns. Keeping the per-task body
trivial maximises throughput so the test still finishes quickly on a
modest cluster (400 burst tasks against ~2 secondaries).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dynamic_runner.worker import Task, run as _run_worker, task_function

_logger = logging.getLogger(__name__)

_OUTPUT_DIR: Path | None = None


@task_function
def handle(task: Task) -> None:
    assert _OUTPUT_DIR is not None
    out = _OUTPUT_DIR / f"{Path(task.relative_path).name}.out"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f"ran {task.relative_path}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int, metavar="SOCKET_FD")
    group.add_argument("--socket-path", type=str, metavar="SOCKET_PATH")
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--skip_existing", action="store_true")
    parser.add_argument(
        "--burst-tasks",
        type=int,
        default=400,
        help="Inert on the worker side; mirrored from the driver's "
        "argparser so the worker's argv re-parse succeeds when the "
        "framework forwards the run's full CLI.",
    )
    return parser


def _on_args(args: argparse.Namespace) -> None:
    global _OUTPUT_DIR
    _OUTPUT_DIR = Path(args.output)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [test-547-worker] %(levelname)s: %(message)s",
    )
    _logger.info("test-547 worker started: output=%s", args.output)


def main() -> None:
    _run_worker(argparser=_build_parser(), on_args=_on_args)


if __name__ == "__main__":
    main()
