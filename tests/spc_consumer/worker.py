"""Worker for the synthetic composite consumer: writes one output file
per task under --output (the body-ran effect the repro asserts)."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dynamic_runner.worker import Task, run as _run_worker, task_function

_logger = logging.getLogger(__name__)

_OUTPUT_DIR: Path | None = None


@task_function
def handle(task: Task) -> None:
    name = Path(task.relative_path).name
    out = _OUTPUT_DIR / f"{name}.out"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(f"ran {task.relative_path} payload={task.payload!r}\n")
    _logger.info("worker: wrote %s", out)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dynamic_queue", type=int, metavar="SOCKET_FD")
    group.add_argument("--socket-path", type=str, metavar="SOCKET_PATH")
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--source", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--skip_existing", action="store_true")
    parser.add_argument("--num-tasks", type=int, default=40)
    return parser


def _on_args(args: argparse.Namespace) -> None:
    global _OUTPUT_DIR
    _OUTPUT_DIR = Path(args.output)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [worker] %(levelname)s: %(message)s")
    _logger.info("worker started: output=%s", args.output)


def main() -> None:
    _run_worker(argparser=_build_parser(), on_args=_on_args)


if __name__ == "__main__":
    main()
