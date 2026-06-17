"""Worker for the #540 three-phase barrier-false e2e consumer.

Single concern: per-task, write one output file and publish it. The
worker dispatches on ``task.payload['phase']`` to pick the per-phase
behaviour, which differs only in whether the task sleeps before
publishing.

Phase A workers sleep ``DYNRUNNER_TEST540_PHASE_A_SLEEP_S`` seconds
(default 5) so the dispatch-overlap window — phase_b tasks dispatching
while phase_a tasks are still running — is wide enough to be
unambiguous in primary-log timestamps. Phases B and C are quick (no
sleep) so the assertion that phase_c does not start until phase_b
drains is also unambiguous (phase_b's items only contend for the
post-phase_a window).

Like its sibling ``tests.e2e.test_consumer.worker``, this module
re-reads the ``DYNRUNNER_PUBLISH_{SRC,DST}_ROOT`` env vars rather than
hard-coding paths, so a local test, a SLURM-mode run, and an
in-process test all share the same worker logic.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

from dynamic_runner.worker import (
    NonRecoverableError,
    Task,
    WorkerOutput,
    publish as _publish,
    run as _run_worker,
    task_function,
)
from dynamic_runner.worker.publish import (
    DEFAULT_SRC_ROOT,
    ENV_SRC_ROOT,
    dst_root as _dst_root,
)


_logger = logging.getLogger(__name__)

# Env knob — phase_a's per-task sleep, in seconds. Tuned to be visibly
# larger than the per-task dispatch jitter (~100ms) yet short enough to
# keep the whole run under a minute. The brief specifies 5s; the env
# var lets a faster test platform shrink the wallclock cost without
# editing this file.
_PHASE_A_SLEEP_ENV = "DYNRUNNER_TEST540_PHASE_A_SLEEP_S"
_PHASE_A_SLEEP_DEFAULT_S = 5.0

_PHASE_A = "phase_a"
_PHASE_B = "phase_b"
_PHASE_C = "phase_c"


def _staging_root() -> Path:
    return Path(os.environ.get(ENV_SRC_ROOT, DEFAULT_SRC_ROOT))


def _phase_a_sleep_seconds() -> float:
    raw = os.environ.get(_PHASE_A_SLEEP_ENV, "")
    if not raw:
        return _PHASE_A_SLEEP_DEFAULT_S
    try:
        return max(0.0, float(raw))
    except ValueError:
        return _PHASE_A_SLEEP_DEFAULT_S


def _resolve_source_path(task: Task, source_dir: Path) -> Path:
    p = Path(task.open_path)
    if p.is_absolute():
        return p
    return source_dir / p


def _handle_one(task: Task, source_dir: Path) -> WorkerOutput:
    payload = task.payload or {}
    phase = payload.get("phase")
    idx = payload.get("idx")
    if phase not in (_PHASE_A, _PHASE_B, _PHASE_C):
        raise NonRecoverableError(
            f"worker: unknown phase {phase!r} in payload {payload!r}"
        )
    if idx is None:
        raise NonRecoverableError(
            f"worker: missing 'idx' in payload {payload!r}"
        )

    if phase == _PHASE_A:
        sleep_s = _phase_a_sleep_seconds()
        if sleep_s > 0:
            _logger.info(
                "phase_a worker: sleeping %.2fs to widen the dispatch window",
                sleep_s,
            )
            time.sleep(sleep_s)

    src = _resolve_source_path(task, source_dir)
    try:
        content = src.read_bytes()
    except OSError as e:
        raise NonRecoverableError(f"failed to read source {src}: {e}") from e

    staging = _staging_root()
    staging.mkdir(parents=True, exist_ok=True)
    out_name = f"{phase}-{idx}.out"
    out_path = staging / out_name
    out_path.write_bytes(b"barrier540:" + phase.encode() + b":" + content)
    _logger.info(
        "%s-%d: wrote %d bytes to %s; publishing",
        phase,
        idx,
        out_path.stat().st_size,
        out_path,
    )
    # Land the output at the publish DST root under its staging name
    # ({phase}-{idx}.out) — the /app/out-network surface the
    # slurm-test-env test-540 harness asserts published outputs land on.
    _publish(out_path, dst=_dst_root() / out_path.name)
    return WorkerOutput()


@task_function
def handle(task: Task) -> WorkerOutput:
    source_dir = _SOURCE_DIR
    if source_dir is None:
        raise NonRecoverableError(
            "worker source_dir not configured — did the framework "
            "forget to pass --source?"
        )
    return _handle_one(task, source_dir)


_SOURCE_DIR: Path | None = None
_OUTPUT_DIR: Path | None = None


def _build_parser() -> argparse.ArgumentParser:
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
    global _SOURCE_DIR, _OUTPUT_DIR
    _SOURCE_DIR = Path(args.source)
    _OUTPUT_DIR = Path(args.output)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [worker] %(levelname)s %(name)s: %(message)s",
    )
    _logger.info(
        "worker started: source=%s output=%s skip_existing=%s",
        args.source,
        args.output,
        args.skip_existing,
    )


def main() -> None:
    _run_worker(argparser=_build_parser(), on_args=_on_args)


if __name__ == "__main__":
    main()
