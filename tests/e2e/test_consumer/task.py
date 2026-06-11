"""``SyntheticTask`` — the TaskDefinition for the e2e synthetic consumer.

Single concern: declare topology and discover items. The actual per-item
work lives in the worker module so each concern owns one file.

Topology
--------

Two phases with an explicit cross-phase dependency edge::

    produce ──depends_on──▶ consume

``produce`` contains two tasks. The second has an intra-phase
``task_depends_on=("produce-0",)`` so the framework gates its dispatch on
the first finishing — exercising the same code path that variant builds
take in the asm-tokenizer pipeline.

``consume`` contains two tasks. Each declares a cross-phase
``task_depends_on`` naming a specific ``produce`` task; the framework's
phase barrier already enforces "all of produce drained before any of
consume runs", but the per-task edge gives extra coverage of the
PendingPool's blocked-map.

Items
-----

Every TaskInfo's ``path`` points at a real file under ``source_dir``
(which the driver populates with N small input files before running).
The framework needs these files to exist because the SLURM packaging
path uploads ``TaskInfo.path`` to the gateway. In ``--multi-computer
local`` / single-process mode the path is read by the worker but no
upload happens.

The items use stable, readable ``task_id``s so the
``task_depends_on`` edges in the worker can resolve them by name.
"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from pathlib import Path

from dynamic_runner._shared import BinaryIdentifier, TaskDep, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec, TypeId


_PHASE_PRODUCE = "produce"
_PHASE_CONSUME = "consume"
_TYPE_PRODUCE = "produce-default"
_TYPE_CONSUME = "consume-default"
_WORKER_MODULE = "tests.e2e.test_consumer.worker"
_NUM_TASKS_PER_PHASE = 2

_logger = logging.getLogger(__name__)


def _produce_task_id(idx: int) -> str:
    return f"produce-{idx}"


def _consume_task_id(idx: int) -> str:
    return f"consume-{idx}"


def _input_filename(idx: int) -> str:
    """Filename for ``produce-{idx}``'s input file under ``source_dir``.

    The same input is reused by ``consume-{idx}`` — there is no need to
    fabricate a second file just to give the consumer phase a path. The
    consumer's actual data dependency lives in the producer's PUBLISHED
    output (under ``out-network``); the source path just drives the
    framework's per-task wire identifier.
    """
    return f"input-{idx}.txt"


class SyntheticTask:
    """TaskDefinition for the e2e synthetic consumer."""

    # ── Topology ────────────────────────────────────────────────────────

    def get_phases(self) -> tuple[PhaseSpec, ...]:
        produce = PhaseSpec(
            phase_id=_PHASE_PRODUCE,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_PRODUCE,
                    worker_module=_WORKER_MODULE,
                ),
            ),
        )
        consume = PhaseSpec(
            phase_id=_PHASE_CONSUME,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_CONSUME,
                    worker_module=_WORKER_MODULE,
                ),
            ),
            depends_on=(_PHASE_PRODUCE,),
        )
        return (produce, consume)

    # ── Item discovery ─────────────────────────────────────────────────

    def discover_items(
        self, source_dir: Path, args: Namespace
    ) -> Iterable[TaskInfo]:
        """Emit produce + consume items.

        Reads ``--num-tasks`` from ``args`` (added by
        :meth:`add_task_arguments`) so the driver / a manual test run can
        scale the workload without touching this file.
        """
        n = getattr(args, "num_tasks", _NUM_TASKS_PER_PHASE)
        # Optional payload flag opting tasks into the keyed-outputs API
        # exercise (Task.publish_string on produce, Task.predecessor_outputs
        # read on consume). The flag rides on every task's payload so
        # the worker can branch on it per-task without re-parsing CLI;
        # discovery is the single place that knows the run-wide opt-in.
        keyed_outputs = bool(getattr(args, "keyed_outputs", False))
        items: list[TaskInfo] = []

        for idx in range(n):
            # Intra-phase dep: produce-i waits for produce-(i-1).
            # Strictly unnecessary — they could run in parallel — but
            # exercising the intra-phase edge is the whole point.
            prev: tuple[str, ...] = (
                (_produce_task_id(idx - 1),) if idx > 0 else ()
            )
            items.append(
                _build_task(
                    source_dir=source_dir,
                    idx=idx,
                    phase_id=_PHASE_PRODUCE,
                    type_id=_TYPE_PRODUCE,
                    task_id=_produce_task_id(idx),
                    task_depends_on=prev,
                    payload={
                        "kind": _PHASE_PRODUCE,
                        "idx": idx,
                        "keyed_outputs": keyed_outputs,
                    },
                )
            )

        for idx in range(n):
            items.append(
                _build_task(
                    source_dir=source_dir,
                    idx=idx,
                    phase_id=_PHASE_CONSUME,
                    type_id=_TYPE_CONSUME,
                    task_id=_consume_task_id(idx),
                    # Cross-phase dep: consume-i depends on produce-i.
                    # The phase barrier already gates this; the per-task
                    # edge additionally exercises PendingPool's
                    # blocked-map shape. A dependency's full identity is
                    # ``(phase_id, task_id)`` — a bare string resolves to
                    # the ENCLOSING phase (here: consume), where no
                    # ``produce-i`` exists, so the seed would classify the
                    # consume tasks ``InvalidTask { missing dep }``. The
                    # cross-phase edge MUST name the prerequisite's phase
                    # via the ``TaskDep`` dataclass (the documented
                    # consumer contract; see
                    # ``dynamic_runner._shared.task_info.TaskDep``).
                    task_depends_on=(
                        TaskDep(
                            task_id=_produce_task_id(idx),
                            phase_id=_PHASE_PRODUCE,
                        ),
                    ),
                    payload={
                        "kind": _PHASE_CONSUME,
                        "idx": idx,
                        "expects_output": _produce_output_filename(idx),
                        "keyed_outputs": keyed_outputs,
                    },
                )
            )

        _logger.info(
            "discover_items: %d produce + %d consume = %d items",
            n,
            n,
            len(items),
        )
        return items

    # ── Per-type plumbing ──────────────────────────────────────────────

    def estimate_memory(self, item: TaskInfo) -> int:
        """One MiB per item — small enough that the framework's memory
        scheduler never throttles, large enough that a misconfigured
        ``--max-memory`` still spawns >1 worker.
        """
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--num-tasks",
            type=int,
            default=_NUM_TASKS_PER_PHASE,
            help=(
                "Number of tasks per phase (produce + consume). Default 2. "
                "The driver also creates exactly this many input files."
            ),
        )
        # Opt-in flag for the keyed-outputs API exercise. When set,
        # discovered tasks carry ``payload["keyed_outputs"] = True``
        # and the worker calls ``task.publish_string`` on produce
        # and reads ``task.predecessor_outputs`` on consume. Default
        # off so existing scenarios using this consumer are unaffected.
        parser.add_argument(
            "--keyed-outputs",
            action="store_true",
            help=(
                "Exercise the keyed-outputs API: produce tasks call "
                "Task.publish_string('nonce', ...) and consume tasks "
                "assert Task.predecessor_outputs carries the value. "
                "Failure mode: worker raises NonRecoverableError."
            ),
        )

    def build_worker_command_args(
        self,
        type_id: TypeId,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        # The worker reads --source / --output / --skip_existing already
        # (framework-injected). Forward --num-tasks just so workers can
        # emit a startup line that mentions the configured size if they
        # ever need to debug a mismatch. Forward --keyed-outputs so
        # the worker-side argparser accepts the flag even though the
        # discovery side already injected it into payloads (the worker
        # branches on payload, not argv — see worker.handle).
        argv = ["--num-tasks", str(args.num_tasks)]
        if getattr(args, "keyed_outputs", False):
            argv.append("--keyed-outputs")
        return argv

    def get_output_filename_pattern(
        self, type_id: TypeId, item: TaskInfo
    ) -> str:
        """Final output filename — used by ``--skip-existing`` checks
        and by the e2e driver's "already-done detection" assertion.

        The worker publishes under exactly this name relative to
        ``out-network`` / the configured publish destination root.
        """
        idx = item.payload["idx"]
        if type_id == _TYPE_PRODUCE:
            return _produce_output_filename(idx)
        if type_id == _TYPE_CONSUME:
            return _consume_output_filename(idx)
        raise ValueError(f"unknown type_id: {type_id}")

    # ── Lifecycle hooks ────────────────────────────────────────────────

    def on_run_start(
        self, source_dir: Path, output_dir: Path, args: Namespace
    ) -> None:
        _logger.info(
            "on_run_start: source=%s output=%s num_tasks=%d",
            source_dir,
            output_dir,
            getattr(args, "num_tasks", _NUM_TASKS_PER_PHASE),
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


def _produce_output_filename(idx: int) -> str:
    return f"produce-{idx}.out"


def _consume_output_filename(idx: int) -> str:
    return f"consume-{idx}.out"


def _build_task(
    *,
    source_dir: Path,
    idx: int,
    phase_id: str,
    type_id: str,
    task_id: str,
    task_depends_on: tuple[str | TaskDep, ...],
    payload: dict,
) -> TaskInfo:
    """One TaskInfo for ``input-{idx}.txt``.

    Both phases reuse the same source file, so the only fields that
    vary across phases are the topology tags (phase/type/task ids,
    depends_on, payload). Centralising the rest here keeps the
    discovery loop readable and prevents the BinaryIdentifier shape
    drifting between the two phase loops.
    """
    input_name = _input_filename(idx)
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
        payload=payload,
        task_id=task_id,
        task_depends_on=task_depends_on,
    )


def _probe_size(path: Path) -> int:
    """Best-effort ``os.stat`` of an input file. Returns 1 when the file
    is missing — the framework only needs ``size`` for memory-aware
    scheduling, and the worker never reads ``TaskInfo.size`` itself.
    """
    try:
        return max(1, path.stat().st_size)
    except OSError:
        return 1
