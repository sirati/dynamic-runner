"""Synthetic composite TaskDefinition mirroring asm-tokenizer's
FullPipelineTask: three chained phases, only phase 1 discovered
upfront, phases 2/3 lazily injected via ``primary_handle.spawn_tasks``
from ``on_phase_end``. ``uses_file_based_items = False`` (the
composite's class-level setting).

``on_phase_end_hook`` (optional ctor arg) lets a test scenario inject
a deliberate hook failure (the abort-reason-labelling repro) without a
second TaskDefinition class.
"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Callable, Optional

from dynamic_runner._shared import BinaryIdentifier, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec

_logger = logging.getLogger(__name__)

P1, P2, P3 = "tok", "unify", "memmap"
T1, T2, T3 = "tok-t", "unify-t", "memmap-t"
WORKER = "tests.spc_consumer.worker"
_ORDER = (P1, P2, P3)


def _item(name: str, phase: str, type_id: str) -> TaskInfo:
    return TaskInfo(
        path=Path(name),
        size=64,
        identifier=BinaryIdentifier(binary_name=name, platform="x", compiler="c",
                                    version="1", opt_level="O0"),
        phase_id=phase,
        type_id=type_id,
        task_id=name,
        payload={"phase": phase},
    )


class CompositeTask:
    uses_file_based_items = False

    def __init__(
        self,
        on_phase_end_hook: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._primary_handle: Optional[Any] = None
        self._source: Optional[Path] = None
        self._output: Optional[Path] = None
        self._args: Optional[Namespace] = None
        self._on_phase_end_hook = on_phase_end_hook

    def get_phases(self):
        return (
            PhaseSpec(phase_id=P1,
                      types=(TaskTypeSpec(type_id=T1, worker_module=WORKER),)),
            PhaseSpec(phase_id=P2,
                      types=(TaskTypeSpec(type_id=T2, worker_module=WORKER),),
                      depends_on=(P1,)),
            PhaseSpec(phase_id=P3,
                      types=(TaskTypeSpec(type_id=T3, worker_module=WORKER),),
                      depends_on=(P2,)),
        )

    def discover_items(self, source_dir: Path, args: Namespace):
        n = int(getattr(args, "num_tasks", 6))
        items = [_item(f"input-{i}.txt", P1, T1) for i in range(n)]
        _logger.info("discover_items: %d %s items", len(items), P1)
        return items

    def estimate_memory(self, item) -> int:
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("--num-tasks", type=int, default=6)

    def build_worker_command_args(self, type_id, args, source_dir, output_dir,
                                  skip_existing) -> list[str]:
        return []

    def get_output_filename_pattern(self, type_id, item) -> str:
        name = item.task_id if item is not None else "unknown"
        return f"{name}.out"

    # ── Lifecycle ──────────────────────────────────────────────────

    def on_run_start(self, source_dir, output_dir, args,
                     primary_handle: Optional[Any] = None) -> None:
        self._source = Path(source_dir)
        self._output = Path(output_dir)
        self._args = args
        self._primary_handle = primary_handle
        _logger.info("on_run_start: handle=%r", primary_handle)

    def on_run_end(self, success: bool) -> None:
        _logger.info("on_run_end: success=%s", success)

    def on_phase_start(self, phase_id: str) -> None:
        _logger.info("on_phase_start: %s", phase_id)

    def on_phase_end(self, phase_id: str, completed: int, failed: int) -> None:
        _logger.info("on_phase_end: %s completed=%d failed=%d",
                     phase_id, completed, failed)
        if self._on_phase_end_hook is not None:
            self._on_phase_end_hook(phase_id)
        idx = _ORDER.index(phase_id)
        if idx + 1 >= len(_ORDER):
            return
        next_phase = _ORDER[idx + 1]
        if self._primary_handle is None:
            _logger.error("on_phase_end(%s): primary_handle is None; cannot "
                          "spawn %s", phase_id, next_phase)
            return
        type_id = {P2: T2, P3: T3}[next_phase]
        items = [_item(f"{next_phase}-agg", next_phase, type_id)]
        _logger.info("spawning %d item(s) for phase %s", len(items), next_phase)
        errors = self._primary_handle.spawn_tasks(items)
        if errors:
            for i, err in errors:
                _logger.warning("spawn_tasks rejected %s item %d: %r",
                                next_phase, i, err)
