"""Structural protocol for task definitions.

A task definition is any object whose attributes match this protocol;
subclassing is *not* required (Protocol uses structural typing).

Every run is structured as one or more **phases** with optional inter-phase
dependencies. Each phase contains one or more **task types**, and each
type binds a worker module to a memory estimator. Items returned from
``discover_items`` carry a ``(phase_id, type_id, affinity_id)`` tag so the
framework knows where to dispatch them and which items to co-locate on the
same worker for cache reuse (soft pinning).

The framework — not the task — owns phase ordering, drain detection, and
worker dispatch. The task implements four kinds of method:

1. **Topology** (``get_phases``) — declare phases + types once at run start.
2. **Item discovery** (``discover_items``) — yield items for the run, each
   tagged with its phase / type / affinity.
3. **Per-type plumbing** (``estimate_memory``, ``build_worker_command_args``,
   ``get_output_filename_pattern``) — answers questions about a specific
   item or type.
4. **Lifecycle hooks** (``on_run_start``, ``on_run_end``,
   ``on_phase_start``, ``on_phase_end``) — let the task set up / tear down
   resources at the right boundaries.
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from ._shared import TaskInfo


PhaseId = str
TypeId = str
AffinityId = str


@dataclass(frozen=True)
class TaskTypeSpec:
    """One task type within a phase.

    ``worker_module`` names a Python module that runs as a subprocess
    worker. The framework spawns it via the existing subprocess factory
    machinery; the worker reads its argv (built by
    ``TaskDefinition.build_worker_command_args``) and dispatches one
    item at a time.

    ``estimator_attr`` names a method on the ``TaskDefinition`` instance
    that returns per-item memory in bytes. Defaults to
    ``"estimate_memory"`` (one estimator shared by all types of this
    task); set it to a type-specific name to give each type its own
    estimator. The method receives the full :class:`TaskInfo`, not
    just its ``size``.
    """

    type_id: TypeId
    worker_module: str
    estimator_attr: str = "estimate_memory"
    timeout_seconds: float | None = None
    reserved_memory_per_worker: int = 0


@dataclass(frozen=True)
class PhaseSpec:
    """One phase: a set of task types that share an ordering barrier.

    A phase becomes active once every phase listed in ``depends_on`` has
    fully drained (every item terminated, success or failure) and that
    phase's ``on_phase_end`` hook has returned. The framework computes
    the schedule from the dependency graph; the order phases appear in
    ``TaskDefinition.get_phases()`` is informational only.

    ``barrier=True`` (the default) means the framework waits for full
    drain of dependencies before any item of this phase dispatches.
    The ``barrier=False`` path is reserved for future pipelined work
    and is not used today.
    """

    phase_id: PhaseId
    types: tuple[TaskTypeSpec, ...]
    depends_on: tuple[PhaseId, ...] = ()
    barrier: bool = True


@runtime_checkable
class TaskDefinition(Protocol):
    """Duck-typed contract a task package implements.

    Any object with the right attributes satisfies this protocol — there
    is no required base class.
    """

    # ── Topology ────────────────────────────────────────────────────────

    def get_phases(self) -> tuple[PhaseSpec, ...]: ...

    # ── Item discovery ─────────────────────────────────────────────────

    def discover_items(
        self, source_dir: Path, args: Namespace
    ) -> Iterable[TaskInfo]: ...

    # ── Per-type plumbing ──────────────────────────────────────────────

    def estimate_memory(self, item: TaskInfo) -> int: ...

    def add_task_arguments(self, parser: ArgumentParser) -> None: ...

    def build_worker_command_args(
        self,
        type_id: TypeId,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]: ...

    def get_output_filename_pattern(
        self, type_id: TypeId, item: TaskInfo
    ) -> str: ...

    # ── Lifecycle hooks ────────────────────────────────────────────────

    def on_run_start(
        self, source_dir: Path, output_dir: Path, args: Namespace
    ) -> None: ...

    def on_run_end(self, success: bool) -> None: ...

    def on_phase_start(self, phase_id: PhaseId) -> None: ...

    def on_phase_end(
        self, phase_id: PhaseId, completed: int, failed: int
    ) -> None: ...
