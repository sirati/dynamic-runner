"""Payload-only consumer for the local-mode output-delivery pin.

Trimmed copy of ``tests/e2e/test_consumer/task.py`` (one phase, one
type, no file IO). Single concern: give
``tests/test_local_output_delivery.py`` a real ``cli_main`` consumer
whose items are NOT file-based (``uses_file_based_items = False``), so
a ``--multi-computer local`` run needs no StageFile traffic — the wire
``path`` is an opaque identifier and the worker's only side effect is
publishing one file per task into its framework-threaded ``--output``
directory (see ``_localout_worker``).

The module doubles as the run's secondary module: the dispatcher
spawns each secondary as ``python -m
dynamic_runner.tests._localout_consumer --secondary <url> ...`` via
``TaskDeploymentSpec.secondary_module`` — the same one-source-of-truth
shape every real consumer uses.
"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from pathlib import Path

from dynamic_runner import TaskDeploymentSpec, cli_main
from dynamic_runner._shared import BinaryIdentifier, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec, TypeId


_PHASE = "emit"
_TYPE = "emit-default"
_WORKER_MODULE = "dynamic_runner.tests._localout_worker"
_NUM_TASKS = 2

_logger = logging.getLogger(__name__)


def output_filename(idx: int) -> str:
    """Name the worker publishes under its ``--output`` for item ``idx``.

    The pin in ``test_local_output_delivery.py`` asserts these names
    under the OPERATOR's ``--output`` directory; single helper so the
    test and the worker cannot drift.
    """
    return f"item-{idx}.out"


class LocalOutTask:
    """Payload-only TaskDefinition: one phase, N opaque items."""

    # The wire `local_path` is an opaque identifier; the framework does
    # no IO on it and the secondary skips the staged-file resolution
    # entirely (`StagingDispatchContext.uses_file_based_items`).
    uses_file_based_items = False

    def get_phases(self) -> tuple[PhaseSpec, ...]:
        return (
            PhaseSpec(
                phase_id=_PHASE,
                types=(
                    TaskTypeSpec(
                        type_id=_TYPE,
                        worker_module=_WORKER_MODULE,
                    ),
                ),
            ),
        )

    def discover_items(
        self, source_dir: Path, args: Namespace
    ) -> Iterable[TaskInfo]:
        n = getattr(args, "num_tasks", _NUM_TASKS)
        items = [
            TaskInfo(
                path=Path(f"item-{idx}"),
                size=1,
                identifier=BinaryIdentifier(
                    binary_name=f"item-{idx}",
                    platform="synthetic",
                    compiler="none",
                    version="0",
                    opt_level="O0",
                ),
                phase_id=_PHASE,
                type_id=_TYPE,
                payload={"idx": idx},
                task_id=f"item-{idx}",
            )
            for idx in range(n)
        ]
        _logger.info("discover_items: %d payload-only items", len(items))
        return items

    def estimate_memory(self, item: TaskInfo) -> int:
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--num-tasks",
            type=int,
            default=_NUM_TASKS,
            help="Number of payload-only items to emit. Default 2.",
        )

    def build_worker_command_args(
        self,
        type_id: TypeId,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        # The framework already injects --source / --output /
        # --skip_existing into every worker argv.
        return []

    def get_output_filename_pattern(
        self, type_id: TypeId, item: TaskInfo
    ) -> str:
        return output_filename(item.payload["idx"])

    # ── Lifecycle hooks (log-only) ─────────────────────────────────────

    def on_run_start(
        self, source_dir: Path, output_dir: Path, args: Namespace
    ) -> None:
        _logger.info("on_run_start: source=%s output=%s", source_dir, output_dir)

    def on_run_end(self, success: bool) -> None:
        _logger.info("on_run_end: success=%s", success)

    def on_phase_start(self, phase_id: str) -> None:
        _logger.info("on_phase_start: %s", phase_id)

    def on_phase_end(self, phase_id: str, completed: int, failed: int) -> None:
        _logger.info(
            "on_phase_end: %s completed=%d failed=%d", phase_id, completed, failed
        )


def main() -> None:
    cli_main(
        LocalOutTask(),
        deployment=TaskDeploymentSpec(
            secondary_module="dynamic_runner.tests._localout_consumer",
            image_name="dynrunner-localout-test",
        ),
        description=(
            "Payload-only dynamic_runner consumer pinning local-mode "
            "output delivery: each task publishes one file into the "
            "framework-threaded --output directory."
        ),
    )


if __name__ == "__main__":
    main()
