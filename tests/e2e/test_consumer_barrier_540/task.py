"""``BarrierConsumerTask`` — TaskDefinition for the #540 e2e barrier test.

Single concern: declare the three-phase topology (A → B → C) the
slurm-test-env test-540 harness drives, where the MIDDLE phase declares
``PhaseSpec(barrier=False)`` to opt into the pipelined-edge dispatch
the consumer-facing fix shipped.

Topology
--------

::

    phase_a (barrier=True default, slow tasks)
      └── phase_b (barrier=False, depends_on=phase_a)
            └── phase_c (barrier=True default, depends_on=phase_b)

Phase A's worker sleeps (``DYNRUNNER_TEST540_PHASE_A_SLEEP_S``, default
5s) so the dispatch-overlap signal — phase_b tasks dispatching while
phase_a tasks are still running — is observable in the primary log's
wall-clock timestamps. Phases B and C are quick (no sleep).

The cross-phase ``task_depends_on`` edges deliberately do NOT name
specific upstream tasks: the barrier flag is the only thing under test
here, and per-task deps would re-introduce a phase-A-completes-before-B
ordering via the dep graph (which already works on trunk and isn't the
behaviour we are demonstrating).

Item discovery is straight ``range(N)`` for each phase. Each TaskInfo
carries a payload ``"phase"`` field the worker keys off so one worker
file handles all three phases.
"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from pathlib import Path

from dynamic_runner._shared import BinaryIdentifier, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec, TypeId


_PHASE_A = "phase_a"
_PHASE_B = "phase_b"
_PHASE_C = "phase_c"
_TYPE_A = "phase-a-default"
_TYPE_B = "phase-b-default"
_TYPE_C = "phase-c-default"
_WORKER_MODULE = "tests.e2e.test_consumer_barrier_540.worker"

# Defaults match the slurm-test-env workload sizing the brief specifies:
# 2 slow tasks in phase_a, 5 in phase_b, 5 in phase_c.
_DEFAULT_PHASE_A_TASKS = 2
_DEFAULT_PHASE_BC_TASKS = 5

_logger = logging.getLogger(__name__)


def _task_id(phase: str, idx: int) -> str:
    return f"{phase}-{idx}"


def _input_filename(phase: str, idx: int) -> str:
    """Filename for ``<phase>-<idx>``'s input file under ``source_dir``.

    Each task points at its own input file so the SLURM packaging path
    has something concrete to upload per task — same shape as the
    sibling ``test_consumer`` workload.
    """
    return f"{phase}-input-{idx}.txt"


class BarrierConsumerTask:
    """TaskDefinition for the #540 barrier-false e2e workload."""

    # ── Topology ────────────────────────────────────────────────────────

    def get_phases(self) -> tuple[PhaseSpec, ...]:
        phase_a = PhaseSpec(
            phase_id=_PHASE_A,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_A,
                    worker_module=_WORKER_MODULE,
                ),
            ),
        )
        # The single feature under test: phase_b opts into the
        # pipelined-edge dispatch by declaring barrier=False. This is
        # the no-op-on-pre-#540-trunk flag the test harness asserts now
        # actually lifts the implicit phase-A-drained-before-B-starts
        # gate.
        phase_b = PhaseSpec(
            phase_id=_PHASE_B,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_B,
                    worker_module=_WORKER_MODULE,
                ),
            ),
            depends_on=(_PHASE_A,),
            barrier=False,
        )
        # phase_c keeps the framework default (barrier=True): the test
        # asserts its tasks dispatch ONLY after phase_b drains, proving
        # the runtime-spawn interlock still enforces barriers on the
        # phases that did NOT opt in.
        phase_c = PhaseSpec(
            phase_id=_PHASE_C,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_C,
                    worker_module=_WORKER_MODULE,
                ),
            ),
            depends_on=(_PHASE_B,),
        )
        return (phase_a, phase_b, phase_c)

    # ── Item discovery ─────────────────────────────────────────────────

    def discover_items(
        self, source_dir: Path, args: Namespace
    ) -> Iterable[TaskInfo]:
        n_a = getattr(args, "phase_a_tasks", _DEFAULT_PHASE_A_TASKS)
        n_bc = getattr(args, "phase_bc_tasks", _DEFAULT_PHASE_BC_TASKS)

        items: list[TaskInfo] = []
        for idx in range(n_a):
            items.append(
                _build_task(
                    source_dir=source_dir,
                    phase=_PHASE_A,
                    type_id=_TYPE_A,
                    idx=idx,
                    payload={"phase": _PHASE_A, "idx": idx},
                )
            )
        for idx in range(n_bc):
            items.append(
                _build_task(
                    source_dir=source_dir,
                    phase=_PHASE_B,
                    type_id=_TYPE_B,
                    idx=idx,
                    payload={"phase": _PHASE_B, "idx": idx},
                )
            )
        for idx in range(n_bc):
            items.append(
                _build_task(
                    source_dir=source_dir,
                    phase=_PHASE_C,
                    type_id=_TYPE_C,
                    idx=idx,
                    payload={"phase": _PHASE_C, "idx": idx},
                )
            )

        _logger.info(
            "discover_items: %d phase_a + %d phase_b + %d phase_c = %d items",
            n_a,
            n_bc,
            n_bc,
            len(items),
        )
        return items

    # ── Per-type plumbing ──────────────────────────────────────────────

    def estimate_memory(self, item: TaskInfo) -> int:
        """One MiB per item — the consumer's memory scheduler treats every
        phase identically; the only per-phase difference is the wallclock
        sleep on phase_a.
        """
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--phase-a-tasks",
            type=int,
            default=_DEFAULT_PHASE_A_TASKS,
            help="Number of tasks in phase_a (the slow phase). Default 2.",
        )
        parser.add_argument(
            "--phase-bc-tasks",
            type=int,
            default=_DEFAULT_PHASE_BC_TASKS,
            help="Number of tasks in phase_b and phase_c each. Default 5.",
        )

    def build_worker_command_args(
        self,
        type_id: TypeId,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        # The framework injects --source / --output / --skip_existing /
        # --log-file / one of --dynamic_queue/--socket-path. We add nothing
        # task-specific: the worker keys off ``task.payload['phase']``.
        return []

    def get_output_filename_pattern(
        self, type_id: TypeId, item: TaskInfo
    ) -> str:
        """Final output filename — used by ``--skip-existing`` checks.

        The worker publishes under exactly this name relative to the
        configured publish destination root.
        """
        phase = item.payload["phase"]
        idx = item.payload["idx"]
        return f"{phase}-{idx}.out"

    # ── Lifecycle hooks ────────────────────────────────────────────────

    def on_run_start(
        self, source_dir: Path, output_dir: Path, args: Namespace
    ) -> None:
        _logger.info(
            "on_run_start: source=%s output=%s phase_a=%d phase_bc=%d",
            source_dir,
            output_dir,
            getattr(args, "phase_a_tasks", _DEFAULT_PHASE_A_TASKS),
            getattr(args, "phase_bc_tasks", _DEFAULT_PHASE_BC_TASKS),
        )

    def on_run_end(self, success: bool) -> None:
        _logger.info("on_run_end: success=%s", success)

    def on_phase_start(self, phase_id: str) -> None:
        _logger.info("on_phase_start: %s", phase_id)

    def on_phase_end(
        self, phase_id: str, completed: int, failed: int
    ) -> None:
        _logger.info(
            "on_phase_end: %s completed=%d failed=%d",
            phase_id,
            completed,
            failed,
        )


def _build_task(
    *,
    source_dir: Path,
    phase: str,
    type_id: str,
    idx: int,
    payload: dict,
) -> TaskInfo:
    input_name = _input_filename(phase, idx)
    return TaskInfo(
        path=Path(input_name),
        size=_probe_size(source_dir / input_name),
        identifier=BinaryIdentifier(
            binary_name=input_name,
            platform="synthetic",
            compiler="none",
            version="0",
            opt_level="O0",
        ),
        phase_id=phase,
        type_id=type_id,
        payload=payload,
        task_id=_task_id(phase, idx),
        task_depends_on=(),
    )


def _probe_size(path: Path) -> int:
    """Best-effort ``os.stat`` of an input file. Returns 1 when missing —
    the framework only needs ``size`` for memory-aware scheduling.
    """
    try:
        return max(1, path.stat().st_size)
    except OSError:
        return 1
