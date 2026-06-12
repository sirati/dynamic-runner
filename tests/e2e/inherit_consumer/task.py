"""``InheritSyntheticTask`` — TaskDefinition for the inherit-outputs e2e.

Single concern: declare a three-phase A->B->C topology so the framework
exercises ``TaskDep(..., inherit_outputs=True)`` end-to-end. The
per-item work lives in the worker module so each concern owns one file.

Topology
--------

Three phases with explicit cross-phase dependency edges::

    phase-a ──depends_on──▶ phase-b ──depends_on──▶ phase-c

Each phase contains exactly one task (``a``, ``b``, ``c``). Every
prerequisite here is CROSS-PHASE, so each dep names the
prerequisite's phase explicitly (a bare ``str`` resolves in the
declaring task's OWN phase and would be rejected as a missing
dependency). Task ``b`` carries
``task_depends_on=(TaskDep("a", phase_id="phase-a"),)`` — direct
predecessor, no inherit. Task ``c`` carries
``task_depends_on=(TaskDep("b", phase_id="phase-b"),
TaskDep("a", phase_id="phase-a", inherit_outputs=True))``: a direct
predecessor (``b``) plus an explicit transitive-ancestry edge to ``a``
with the inherit-outputs flag set, requesting that ``a``'s published
outputs (in addition to ``b``'s) land in ``c``'s
``predecessor_outputs`` dict at dispatch time.

Two edges live on ``c``'s ``task_depends_on``:

1. ``TaskDep("b", phase_id="phase-b")`` — the direct predecessor.
   Without ``inherit_outputs=True`` the framework only surfaces
   ``b``'s own outputs to ``c``.
2. ``TaskDep("a", phase_id="phase-a", inherit_outputs=True)`` — the
   ancestor. The flag asks the framework to walk the dep graph
   transitively at dispatch time and surface ``a``'s outputs to ``c``
   as well.

Per the framework's transitive-ancestry contract, ``c.predecessor_outputs``
ends up populated with BOTH ``a`` and ``b`` keys. The worker asserts
that shape and fails loud if either key is missing or its inline value
mismatches the expected nonce.

Items
-----

Each task's ``path`` points at ``input-{a|b|c}.txt`` under
``source_dir`` (which the scenario's :func:`stage_inputs` populates
ahead of dispatch). The path is the framework's wire identifier; the
worker reads the file but the actual data dependency between tasks
flows through ``Task.publish_string`` / ``Task.predecessor_outputs``,
not through the source file.

The items use stable, readable ``task_id``s (``a`` / ``b`` / ``c``) so
the ``task_depends_on`` edges resolve by name.
"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from pathlib import Path

from dynamic_runner._shared import BinaryIdentifier, TaskDep, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec, TypeId


_PHASE_A = "phase-a"
_PHASE_B = "phase-b"
_PHASE_C = "phase-c"
# TypeIds are globally unique across the whole topology (the framework's
# TypeRegistry keys on type_id alone, with no phase qualification), so
# each phase declares its own id rather than sharing a single "default".
_TYPE_A = "phase-a-default"
_TYPE_B = "phase-b-default"
_TYPE_C = "phase-c-default"
_WORKER_MODULE = "tests.e2e.inherit_consumer.worker"

_TASK_A = "a"
_TASK_B = "b"
_TASK_C = "c"

_logger = logging.getLogger(__name__)


def _input_filename(task_id: str) -> str:
    """Filename for ``{task_id}``'s input file under ``source_dir``."""
    return f"input-{task_id}.txt"


def _output_filename(task_id: str) -> str:
    """Final published filename for ``{task_id}``'s worker output."""
    return f"{task_id}.out"


class InheritSyntheticTask:
    """TaskDefinition for the inherit-outputs e2e consumer."""

    # ── Topology ────────────────────────────────────────────────────────

    def get_phases(self) -> tuple[PhaseSpec, ...]:
        # One task type per phase keeps each phase declarative; the
        # framework's phase-state machine enforces the linear
        # phase-a -> phase-b -> phase-c order.
        a = PhaseSpec(
            phase_id=_PHASE_A,
            types=(
                TaskTypeSpec(type_id=_TYPE_A, worker_module=_WORKER_MODULE),
            ),
        )
        b = PhaseSpec(
            phase_id=_PHASE_B,
            types=(
                TaskTypeSpec(type_id=_TYPE_B, worker_module=_WORKER_MODULE),
            ),
            depends_on=(_PHASE_A,),
        )
        c = PhaseSpec(
            phase_id=_PHASE_C,
            types=(
                TaskTypeSpec(type_id=_TYPE_C, worker_module=_WORKER_MODULE),
            ),
            depends_on=(_PHASE_B,),
        )
        return (a, b, c)

    # ── Item discovery ─────────────────────────────────────────────────

    def discover_items(
        self, source_dir: Path, args: Namespace
    ) -> Iterable[TaskInfo]:
        """Emit exactly three TaskInfos shaping the A->B->C chain.

        ``--num-tasks`` is intentionally unused: this consumer's whole
        point is the fixed three-task graph that exercises the inherit-
        outputs edge. The flag is accepted on the parser for argv-
        compatibility with the shared dispatch builder but does not
        change the topology.
        """
        del args  # not used; topology is fixed
        items: list[TaskInfo] = [
            _build_task(
                source_dir=source_dir,
                task_id=_TASK_A,
                phase_id=_PHASE_A,
                type_id=_TYPE_A,
                # No prerequisites — A is the chain's root.
                task_depends_on=(),
            ),
            _build_task(
                source_dir=source_dir,
                task_id=_TASK_B,
                phase_id=_PHASE_B,
                type_id=_TYPE_B,
                # Direct predecessor only; B reads A's outputs by
                # virtue of being A's direct child. A lives in
                # phase-a, so the dep MUST name that phase explicitly
                # — a bare string would resolve same-phase (phase-b)
                # and be rejected as a missing dependency. No inherit
                # flag needed — direct edges always surface their
                # tail's outputs.
                task_depends_on=(TaskDep(_TASK_A, phase_id=_PHASE_A),),
            ),
            _build_task(
                source_dir=source_dir,
                task_id=_TASK_C,
                phase_id=_PHASE_C,
                type_id=_TYPE_C,
                # The load-bearing dependency: ``b`` is the direct
                # predecessor (in phase-b), AND a transitive edge to
                # ``a`` (in phase-a) with ``inherit_outputs=True`` so
                # C's predecessor_outputs ends up carrying BOTH keys.
                # Without the inherit flag, C would only see B's
                # outputs — the e2e gates this difference. Both deps
                # are cross-phase, so both name their prerequisite's
                # phase explicitly.
                task_depends_on=(
                    TaskDep(_TASK_B, phase_id=_PHASE_B),
                    TaskDep(_TASK_A, phase_id=_PHASE_A, inherit_outputs=True),
                ),
            ),
        ]
        _logger.info(
            "discover_items: A->B->C chain, %d items", len(items)
        )
        return items

    # ── Per-type plumbing ──────────────────────────────────────────────

    def estimate_memory(self, item: TaskInfo) -> int:
        # One MiB per item — same scale as test_consumer; small enough
        # the memory scheduler never throttles in the slurm-test-env.
        del item
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        # ``--num-tasks`` is accepted-and-ignored: the shared dispatch
        # builder (build_dispatch_argv) always passes ``--num-tasks``
        # because the canonical consumer relies on it; ignoring rather
        # than rejecting it keeps the builder API uniform across
        # consumers.
        parser.add_argument("--num-tasks", type=int, default=3)

    def build_worker_command_args(
        self,
        type_id: TypeId,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        # Framework injects --source / --output / --skip_existing.
        # Forward --num-tasks so the worker's argparser still accepts
        # it; the worker does not actually read it.
        del type_id, source_dir, output_dir, skip_existing
        return ["--num-tasks", str(args.num_tasks)]

    def get_output_filename_pattern(
        self, type_id: TypeId, item: TaskInfo
    ) -> str:
        """Final output filename — used by ``--skip-existing`` and the
        scenario's "all-files-present" assertion.
        """
        del type_id
        task_id = item.task_id
        if task_id is None:
            raise ValueError(
                "InheritSyntheticTask items always carry a task_id"
            )
        return _output_filename(task_id)

    # ── Lifecycle hooks ────────────────────────────────────────────────

    def on_run_start(
        self, source_dir: Path, output_dir: Path, args: Namespace
    ) -> None:
        del args
        _logger.info(
            "on_run_start: source=%s output=%s",
            source_dir,
            output_dir,
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
    task_id: str,
    phase_id: str,
    type_id: str,
    task_depends_on: tuple,
) -> TaskInfo:
    """One TaskInfo for ``input-{task_id}.txt``.

    Centralises the constant fields (BinaryIdentifier shape, payload
    skeleton) so the discovery loop stays focused on the topology
    differences (task_id / phase_id / task_depends_on).
    """
    input_name = _input_filename(task_id)
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
        phase_id=phase_id,
        type_id=type_id,
        # The worker keys behaviour off task_id (not payload['kind'])
        # because each task is unique in this chain; payload carries
        # only the information the worker can't otherwise derive (the
        # expected nonce values for assertions).
        payload={"task_id": task_id},
        task_id=task_id,
        task_depends_on=task_depends_on,
    )


def _probe_size(path: Path) -> int:
    """Best-effort ``os.stat`` of an input file; defaults to 1 when
    missing. The framework only needs ``size`` for memory-aware
    scheduling, and the worker never reads ``TaskInfo.size``.
    """
    try:
        return max(1, path.stat().st_size)
    except OSError:
        return 1
