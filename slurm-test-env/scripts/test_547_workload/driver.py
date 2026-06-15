"""Driver for the #547 chunked-spawn e2e test.

Single concern: drive ONE >256-task ``spawn_tasks`` burst at the
primary within a real ``dynamic_runner`` run, so the primary's
operational ``select!`` actually fires the ``PrimaryCommand::SpawnTasks``
path with ``len(tasks) > APPLY_SPAWN_CHUNK_SIZE = 256``. The shell
harness owns the post-mortem assertion against ``--full-log-file``;
this driver only shapes the workload.

Mechanism: two-phase pipeline, phase-1 trivially small so the burst
lands as a SINGLE ``spawn_tasks`` call from ``on_phase_end(seed)`` that
seeds phase-2 with ``N`` tasks. ``N`` defaults to 400 (>256) and is
configurable via ``--burst-tasks``. The phase-2 worker is intentionally
a no-op fixture (``test_547_workload.worker``) so the test stresses
the spawn-burst fan-out path, not the worker.

Boundary: the driver knows ONLY the consumer-facing
``dynamic_runner.cli_main`` + ``TaskDefinition`` shape — every knob the
chunking proof needs (``ARM_HEARTBEAT`` / ``ARM_INBOX`` re-firing
mid-burst) is observed downstream in the Rust ``oploop arm stats`` log,
not constructed here. The driver MUST NOT special-case "chunking" — its
only job is "make a >256 batch arrive at the primary".
"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Optional

from dynamic_runner import TaskDeploymentSpec, cli_main
from dynamic_runner._shared import BinaryIdentifier, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec

_logger = logging.getLogger(__name__)

P_SEED = "seed"
P_BURST = "burst"
T_SEED = "seed-t"
T_BURST = "burst-t"
WORKER_MODULE = "test_547_workload.worker"


def _item(name: str, phase: str, type_id: str) -> TaskInfo:
    return TaskInfo(
        path=Path(name),
        size=64,
        identifier=BinaryIdentifier(
            binary_name=name,
            platform="x",
            compiler="c",
            version="1",
            opt_level="O0",
        ),
        phase_id=phase,
        type_id=type_id,
        task_id=name,
        payload={"phase": phase},
    )


class ChunkingBurstTask:
    """Two-phase pipeline that fires ONE ``spawn_tasks`` call with N>256.

    Phase ``seed`` is a single sentinel task; its ``on_phase_end`` is
    the ONLY ``spawn_tasks`` call site, and it submits the entire burst
    in ONE list. The framework's ``PrimaryHandle.spawn_tasks`` hands
    that list straight to ``PrimaryCommand::SpawnTasks`` so the
    coordinator must chunk via ``PumpSpawnContinuation``.
    """

    uses_file_based_items = False

    def __init__(self) -> None:
        self._primary_handle: Optional[Any] = None
        self._args: Optional[Namespace] = None

    def get_phases(self):
        return (
            PhaseSpec(
                phase_id=P_SEED,
                types=(TaskTypeSpec(type_id=T_SEED, worker_module=WORKER_MODULE),),
            ),
            PhaseSpec(
                phase_id=P_BURST,
                types=(TaskTypeSpec(type_id=T_BURST, worker_module=WORKER_MODULE),),
                depends_on=(P_SEED,),
            ),
        )

    def discover_items(self, source_dir: Path, args: Namespace):
        # Exactly one seed item so phase-1 ends quickly and the burst
        # lands as one contiguous spawn_tasks call.
        return [_item("seed-0", P_SEED, T_SEED)]

    def estimate_memory(self, item) -> int:
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--burst-tasks",
            type=int,
            default=400,
            help="Number of tasks to spawn in the on_phase_end(seed) "
            "burst. Must exceed APPLY_SPAWN_CHUNK_SIZE=256 to exercise "
            "the PumpSpawnContinuation chunking path. Default 400.",
        )

    def build_worker_command_args(
        self, type_id, args, source_dir, output_dir, skip_existing
    ) -> list[str]:
        return []

    def get_output_filename_pattern(self, type_id, item) -> str:
        name = item.task_id if item is not None else "unknown"
        return f"{name}.out"

    # ── Lifecycle ──────────────────────────────────────────────────

    def on_run_start(
        self, source_dir, output_dir, args, primary_handle: Optional[Any] = None
    ) -> None:
        self._args = args
        self._primary_handle = primary_handle
        _logger.info(
            "test-547-driver on_run_start: burst-tasks=%d handle=%r",
            getattr(args, "burst_tasks", 0),
            primary_handle,
        )

    def on_run_end(self, success: bool) -> None:
        _logger.info("test-547-driver on_run_end: success=%s", success)

    def on_phase_start(self, phase_id: str) -> None:
        _logger.info("test-547-driver on_phase_start: %s", phase_id)

    def on_phase_end(self, phase_id: str, completed: int, failed: int) -> None:
        _logger.info(
            "test-547-driver on_phase_end: %s completed=%d failed=%d",
            phase_id,
            completed,
            failed,
        )
        if phase_id != P_SEED:
            return
        if self._primary_handle is None:
            _logger.error(
                "test-547-driver: primary_handle is None on phase=%s; "
                "cannot fire burst",
                phase_id,
            )
            return
        n = int(getattr(self._args, "burst_tasks", 400))
        items = [
            _item(f"burst-{i:04d}", P_BURST, T_BURST) for i in range(n)
        ]
        _logger.info(
            "test-547-driver: firing burst spawn_tasks with %d items "
            "(APPLY_SPAWN_CHUNK_SIZE=256 → expect PumpSpawnContinuation)",
            len(items),
        )
        errors = self._primary_handle.spawn_tasks(items)
        if errors:
            for i, err in errors:
                _logger.warning(
                    "test-547-driver: spawn_tasks rejected burst item %d: %r",
                    i,
                    err,
                )


def main() -> None:
    cli_main(
        ChunkingBurstTask(),
        deployment=TaskDeploymentSpec(
            secondary_module=WORKER_MODULE,
            image_name="dynrunner-test-547-chunking",
        ),
        description=(
            "Driver for the #547 chunked-spawn e2e test: one >256-task "
            "spawn_tasks burst on the on_phase_end(seed) hook."
        ),
    )


if __name__ == "__main__":
    main()
