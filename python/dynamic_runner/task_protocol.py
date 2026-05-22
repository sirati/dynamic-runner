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
3. **Per-type plumbing** (``estimate_memory``, ``build_worker_command_args``)
   — answers questions about a specific item or type.
4. **Lifecycle hooks** (``on_run_start``, ``on_run_end``,
   ``on_phase_start``, ``on_phase_end``) — let the task set up / tear down
   resources at the right boundaries.

Output durability contract (SLURM dispatch)
-------------------------------------------

When the framework dispatches in SLURM mode, each secondary container
sees TWO output bind-mounts:

* ``/app/out-tmp`` — per-secondary scratch on the compute node's local
  disk. Fast, cleared at job-exit; the framework's worker-side
  bookkeeping (sockets, in-progress logs) lives here.
* ``/app/out-network`` — the shared cluster filesystem mount; this is
  where the framework points the worker's ``--output`` flag (via
  ``SecondaryConfig`` auto-resolution). Outputs survive job-exit and
  are visible to other secondaries on the same gateway.

The framework does NOT mediate writes between the two: workers
write directly to ``/app/out-network`` and the contract is "the
worker owns crash-safety on its own outputs". A worker that may
SIGKILL or OOM mid-write should:

1. Write to ``<output_dir>/<name>.partial``
2. ``os.fsync`` (or platform equivalent) before
3. ``os.replace(<name>.partial, <name>)`` (POSIX-atomic rename)

Otherwise an interrupted run can leave half-written ``<name>.csv``
files that subsequent ``--skip-existing`` passes treat as "done"
and never retry. The framework's ``--skip-existing`` machinery
checks for the existence of the FINAL filename only; it does not
inspect file size or content. This is intentional — the cost of
an integrity check on every output would dwarf the benefit on the
common case — but it means crash-safety is the consumer's job.

For tasks whose outputs are byte-streams that can be partial-read
without observable harm (compressed archives with internal
checksums, append-only logs), the partial-rename pattern is
optional but still recommended for the ``--skip-existing``
correctness reason.

Keyed task outputs (per-edge data, not per-task policy)
-------------------------------------------------------

Each entry on :attr:`TaskInfo.task_depends_on` is a ``TaskDep``
(``crates/dynrunner-core/src/types/task.rs``) with a string
``task_id`` and a ``bool`` ``inherit_outputs`` flag. The legacy
bare-string shape (``["task-a"]``) is still accepted by the
framework's untagged-deserialiser and stays equivalent to
``[TaskDep(task_id="task-a", inherit_outputs=False)]``; only set
the flag explicitly when the dependent task needs to read its
predecessor's predecessors' outputs too.

Python consumers express the structured shape with the
:class:`dynamic_runner.TaskDep` dataclass. ``discover_items``
returns may mix bare strings and ``TaskDep`` instances in the same
``task_depends_on`` tuple::

    from dynamic_runner import TaskDep

    yield TaskInfo(
        ...,
        task_id="C",
        task_depends_on=(
            "B",                                       # legacy, no inherit
            TaskDep("A", inherit_outputs=True),        # transitive read
        ),
    )

The PyO3 bridge (``crates/dynrunner-pyo3/src/pytypes/extract.rs``)
duck-types each entry: bare ``str`` becomes ``TaskDep { task_id,
inherit_outputs: false }``; ``TaskDep``-shaped objects (or any
duck-typed thing exposing ``task_id`` + ``inherit_outputs``) carry
both fields verbatim. The reverse direction (Rust→Python read of
``TaskInfo.task_depends_on``) renders as the legacy ``tuple[str,
...]`` — ``inherit_outputs`` is a declarer-side concern that does
not need to be observable post-extract.

When a worker handler runs, ``task.predecessor_outputs`` carries
the keyed outputs of every direct (and, when the edge sets
``inherit_outputs=True``, transitive) predecessor. The shape is::

    {
        predecessor_task_id: {
            output_key: {"kind": "inline" | "file", "value": str}
        }
    }

``kind == "inline"`` denotes a string the producing task committed
via :meth:`Task.publish_string`; ``kind == "file"`` denotes a
post-publish destination path on the shared mount, committed by
the producing task via ``Task.publish(src, dst, key=...)``. The
``value`` carries the string in both cases; the producing task's
worker module owns the schema of inline strings (the framework
does not inspect them).

A predecessor that emits no outputs still appears as a key with
an empty inner dict, so the dependent's lookup pattern
``task.predecessor_outputs["task-a"].get("nonce")`` is uniform.
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Protocol, runtime_checkable

from ._shared import TaskInfo

if TYPE_CHECKING:
    # `PrimaryHandle` is the in-flight runtime control surface minted
    # off `RustPrimaryCoordinator.handle()`; imported here only for
    # type-checking so the module doesn't carry a runtime dependency on
    # the Rust extension at import time.
    from . import PrimaryHandle


# Type alias for the optional `task_completed_listener` task attribute.
# Called once per terminal task transition with
# `(task_id, success, error_kind)`:
#   - `task_id`: the consumer-supplied identifier from `TaskInfo.task_id`,
#     or `None` if the task carried no id.
#   - `success`: `True` if the apply path transitioned the task to
#     `Completed`; `False` for any failure terminal (`Failed`,
#     `Unfulfillable`).
#   - `error_kind`: `None` on success; on failure the wire-stable
#     `ErrorType.wire_value()` tag (e.g. `"oom"`, `"non_recoverable"`,
#     `"recoverable"`, `"unfulfillable:<reason>"`).
TaskCompletedListener = Callable[[Optional[str], bool, Optional[str]], None]


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
    # Optional global concurrency cap for items of this type. `None`
    # means unconstrained — the historical behaviour. When set, the
    # primary scheduler refuses to dispatch more than `max_concurrent`
    # items of this type concurrently across all workers; useful for
    # capping a compile-heavy type at e.g. `cores // 4` while letting
    # cheap IO-bound types run at the full `--jobs` width.
    max_concurrent: int | None = None


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

    # ── Lifecycle hooks ────────────────────────────────────────────────

    def on_run_start(
        self,
        source_dir: Path,
        output_dir: Path,
        args: Namespace,
        primary_handle: Optional["PrimaryHandle"] = None,
    ) -> None:
        """Fire at run start, after the coordinator is constructed.

        ``primary_handle`` is the in-flight runtime control surface for
        the primary coordinator. It is ``None`` on secondaries (which
        own no coordinator), and the live ``PrimaryHandle`` on the
        primary so the task can drive
        ``primary_handle.spawn_tasks(...)`` from inside ``on_run_start``.

        The framework calls ``on_run_start`` with the kwarg on every
        primary-side dispatcher; legacy task signatures that omit it
        keep working via a positional-only fallback in the PyO3
        bridge.
        """
        ...

    def on_run_end(self, success: bool) -> None: ...

    def on_phase_start(self, phase_id: PhaseId) -> None: ...

    def on_phase_end(
        self, phase_id: PhaseId, completed: int, failed: int
    ) -> None: ...

    # Optional task-completion listener attribute.
    #
    # When the task exposes ``task_completed_listener`` as a callable
    # matching :data:`TaskCompletedListener`, the framework registers
    # it on the primary's cluster-state dispatcher. The listener fires
    # once per terminal task transition (success or failure), off the
    # CRDT apply path, with panic + ``PyErr`` isolation so a buggy
    # listener can never stall the apply path or tear the dispatcher
    # task down. Absent or ``None`` opts out.
    task_completed_listener: Optional[TaskCompletedListener]
