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

# Re-export the framework mount-root selector (#644) on the consumer
# surface so a task can write ``files=[(src, dest, UploadRoot.OUTPUT)]``
# (or pass ``root`` to a custom ``upload_action``) by importing it from
# the same module as the task protocol. The enum is OWNED by the Rust
# core (`dynrunner_core::UploadRoot`); this is purely a re-export.
from ._native import UploadRoot  # noqa: F401  (re-export)
from ._shared import TaskInfo

if TYPE_CHECKING:
    # `PrimaryHandle` is the in-flight runtime control surface minted
    # off `RustPrimaryCoordinator.handle()`; imported here only for
    # type-checking so the module doesn't carry a runtime dependency on
    # the Rust extension at import time.
    from . import PrimaryHandle


# Type alias for the optional `task_completed_listener` task attribute.
# Called once per terminal task transition with
# `(task_id, success, error_kind, last_error)`:
#   - `task_id`: the consumer-supplied identifier from `TaskInfo.task_id`,
#     or `None` if the task carried no id.
#   - `success`: `True` if the apply path transitioned the task to
#     `Completed`; `False` for any failure terminal (`Failed`,
#     `Unfulfillable`).
#   - `error_kind`: `None` on success; on failure the wire-stable
#     `ErrorType.wire_value()` tag (e.g. `"oom"`, `"non_recoverable"`,
#     `"recoverable"`, `"unfulfillable:<reason>"`). The carried error
#     *type* identity.
#   - `last_error`: `None` on success; on failure the operator-facing
#     error *message* recorded on the ledger entry. Carried alongside
#     `error_kind` because a failure is only fully identified by type
#     AND message (e.g. two `non_recoverable` failures with distinct
#     messages are distinct events). Forwarded as the trailing
#     positional argument by the PyO3 bridge.
TaskCompletedListener = Callable[
    [Optional[str], bool, Optional[str], Optional[str]], None
]


# Type alias for the optional ``upload_action`` task attribute (#336 P1 /
# #493 option-A / #644). The framework registers this callable on the
# in-process primary's setup executor; when a TaskInfo declares
# ``files=[...]``, the framework's #336 P2 attach derives one deduped
# upload setup task per unique ``(source, dest, root)`` (executed on the
# source-owning member — the submitter), and the executor invokes this
# callable for each.
# Signature: ``(source, dest, root)`` where ``source`` is the on-disk path
# of the local file to upload (as a ``str``), ``dest`` is the cluster-side
# destination relative to the chosen mount root (``None`` ⇒ derived from
# the source basename), and ``root`` (#644) is an :class:`UploadRoot`
# selecting WHICH framework-owned cluster mount the upload lands under
# (``UploadRoot.SOURCE`` ⇒ the srcbins mount, the default; or
# ``UploadRoot.OUTPUT`` ⇒ the shared output mount). The consumer picks a
# mount, never a host path — the framework owns the host→container mapping.
# Raise ``OSError`` for a transient transport fault (the framework retries
# a bounded number of times before falling back to a permanent failure
# terminal) and anything else for a permanent failure (NO retry — surfaces
# immediately as a non-recoverable setup terminal whose cascade fails
# dependent work tasks). The Python callable OWNS the per-blob transient
# retry (the shipped ``retry_transient`` helper the bulk walk uses); this
# seam classifies the FINAL outcome.
#
# Back-compat: ``root`` is always passed as an explicit 3rd positional arg.
# A pre-#644 consumer override that only accepts ``(source, dest)`` must
# tolerate the extra positional — accept ``*_`` or a defaulted ``root``
# parameter (e.g. ``def upload(source, dest, root=None): ...``); the
# framework-default ``SlurmJobManager.upload_task_file`` already does.
#
# The SLURM packaging pipeline defaults this to
# ``SlurmJobManager.upload_task_file`` (which already does the right
# gateway scp + retry, and maps ``root`` to the cluster mount base); the
# in-process local/single-process/remote-podman paths consult
# ``getattr(task, "upload_action", None)`` only.
# Runtime-spawned TaskInfos must NOT carry ``files=``: the runtime-spawn
# path (``primary_handle.spawn_tasks``) does NOT call
# ``augment_batch_for_staging`` today — declare ``files=`` at submit-time
# (initial cold seed) or pre-upload from the spawner.
UploadAction = Callable[..., None]


# Type alias for the optional ``custom_message_handler`` task attribute
# (F5 -- secondary->primary custom messages). Fired ON THE PRIMARY only,
# once per delivered message, with
# ``(origin, topic, data, important, primary_handle)``:
#   - ``origin``: the originating secondary's id (the message's
#     idempotency-key half; transport replays are deduped by the
#     framework before the handler ever fires).
#   - ``topic``: the consumer routing key the sender supplied -- the
#     framework never interprets it.
#   - ``data``: the opaque payload bytes (<= 100 KiB, enforced at the
#     send API).
#   - ``important``: the sender's delivery class. ``False`` = droppable
#     (at-most-once; lost on failover/no-route by design). ``True`` =
#     important: delivered at-least-once -- retained and replayed by the
#     sending secondary until the primary confirms the landing, recorded
#     in the replicated ledger until this handler RETURNS CLEANLY, and
#     replayed to the promoted primary's handler after a failover that
#     interrupted handling.
#   - ``primary_handle``: the live in-flight ``PrimaryHandle`` of THE
#     primary the handler runs on -- the streamed-spawn site
#     (``primary_handle.spawn_tasks(batch)``).
# Error contract: a raise is a USER ERROR and is TERMINAL -- the
# message transitions to ``Failed`` in the replicated ledger (payload
# dropped), is NEVER retried (not on this primary, not on a promoted
# one), and the framework logs a structured ERROR carrying
# origin/seq/topic and the exception. The handler is all-or-nothing:
# every ``primary_handle`` command it issued before raising (e.g. a
# ``spawn_tasks`` batch) is DISCARDED unexecuted -- a raising handler
# produces NO effect anywhere in the cluster. A clean return is atomic
# the other way: the handler's effects and the handled-fact replicate
# in one batch, so no replica can ever observe one without the other.
# Per-origin send order is preserved: message N+1 from one origin is
# never handled before message N resolves (handled or failed). The
# unhandled backlog is never capped; if the handler cannot keep up the
# primary logs a rate-limited WARN naming the backlog size and the
# oldest entry's age. Absent or ``None`` opts out (important messages
# are then consumed unhandled with a WARN).
# Message-vs-phase-end ordering: every IMPORTANT message handed to
# ``SecondaryHandle.send_to_primary`` BEFORE a task's terminal report
# leaves its secondary is RESOLVED (handled, or failed on a raise)
# before that terminal is processed at the primary -- so
# ``on_phase_end`` for the task's phase always fires AFTER this handler
# saw every message the task streamed before exiting (a
# ``worker_message_listener`` that forwards synchronously gets this for
# free). Droppables make no such claim (lost-by-design never delays a
# phase), and an origin that DIES before its retained messages deliver
# releases the ordering claim too -- the consumer's ``on_phase_end``
# barrier then observes the gap and can fail loudly.
#
# Spawn-anytime note (F4) for handlers that spawn: spawning into a phase
# that already ENDED re-opens it and re-fires its ``on_phase_end`` at
# the re-drain -- ``on_phase_end`` must be idempotent for phases that
# can be late-spawned into. Duplicate task identities (content hash) in
# a re-streamed batch are dropped idempotently by ``spawn_tasks``.
CustomMessageHandler = Callable[[str, str, bytes, bool, "PrimaryHandle"], None]


# Type alias for the optional `worker_message_listener` task attribute.
# Fired ON THE SECONDARY hosting the sending worker, once per
# `Task.send_message(topic, data)` frame, with
# `(worker_id, type_id, topic, data, secondary_handle)`:
#   - `worker_id`: the pool slot of the sending worker (an `int`).
#   - `type_id`: the `TypeId` of the task the worker was running when
#     it sent the message (which worker *kind* is talking).
#   - `topic`: the consumer routing key, verbatim — the framework
#     never interprets it.
#   - `data`: the opaque payload `bytes`
#     (≤ `CUSTOM_MESSAGE_MAX_BYTES`, 100 KiB).
#   - `secondary_handle`: a `SecondaryHandle` — the listener replies
#     via `secondary_handle.send_to_worker(worker_id, topic, data)`
#     (the worker drains replies via `Task.poll_messages()`), and
#     relays cluster-wide via
#     `send_to_primary(topic, data, important=...)` (see
#     :data:`CustomMessageHandler` for the primary-side contract).
# Messages from one worker arrive in send order. The listener runs on
# the secondary's worker-message dispatcher task (off the operational
# loop) with panic + `PyErr` isolation — the `task_completed_listener`
# idiom.
WorkerMessageListener = Callable[[int, str, str, bytes, object], None]


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
    # When `True`, items of this type may ONLY be dispatched to workers
    # on the PRIMARY node (the secondary co-located with the primary
    # coordinator). They are never offered to peer secondaries — not on
    # the steady-state dispatch path, and not after an eviction-driven
    # requeue (e.g. a collective-silence false-eviction of the primary's
    # own secondary). When the primary's own secondary is evicted and
    # later re-admitted, the requeued task waits in the pending pool
    # until a primary-node worker becomes idle again.
    #
    # Use this for task types whose execution is inherently node-local
    # to the primary — e.g. consumer-side planners that read from the
    # primary process's in-memory state, or nix-eval bootstrappers that
    # must run in the primary node's nix-store environment. `False`
    # (the default) preserves the existing any-worker dispatch
    # behaviour; the single concern crossing the framework boundary is
    # one bit per task type.
    primary_pinned: bool = False


@dataclass(frozen=True)
class PhaseSpec:
    """One phase: a set of task types that share an ordering barrier.

    A phase becomes active once every phase listed in ``depends_on`` has
    fully drained (every item terminated, success or failure) and that
    phase's ``on_phase_end`` hook has returned. The framework computes
    the schedule from the dependency graph; the order phases appear in
    ``TaskDefinition.get_phases()`` is informational only.

    ``barrier=True`` (the default) means the framework waits for full
    drain of dependencies before any item of this phase dispatches —
    the strict whole-of-upstream barrier every consumer historically
    saw. ``barrier=False`` is the explicit PIPELINED-EDGE opt-in: the
    framework starts the phase ``Active`` from the run's beginning, and
    items dispatch as soon as their per-task ``task_depends_on`` graph
    resolves (a phase-N+1 task whose specific phase-N predecessor has
    completed dispatches without waiting for the rest of phase N).

    The same flag also gates the ``task_completed_listener`` /
    ``PrimaryHandle.spawn_tasks`` runtime-spawn path: spawning into a
    ``barrier=True`` phase whose upstream is still draining is REJECTED
    with a ``barrier_violation`` per-task entry (mirroring the
    duplicate-hash / unknown-dep classes); spawning into a
    ``barrier=False`` phase is ACCEPTED. The two enforcement sites —
    the scheduler's phase-state gate and the runtime-spawn interlock —
    consult the same per-phase flag, so the explicit declaration
    (``barrier=False``) and the implicit listener-early-spawn form
    carry one source of truth.

    A ``barrier=False`` phase's downstream dependents still wait for
    IT to be ``Done`` (each phase's own drain edge is the gate; only
    the upstream-arrival gate is relaxed). Dependent ordering between
    phases is preserved phase-by-phase — the relaxation is one-edge,
    not transitive.

    ``may_be_empty`` (default ``False``) is the empty-drain honesty
    opt-out. By default the framework FAILS THE RUN LOUD if an activated
    phase drains with zero tasks of any kind — that is the
    silent-partial-success signature of a phase whose planned work was
    never injected (e.g. an ``on_phase_end``-driven lazy-injection or
    discovery step was suppressed), which would otherwise complete the
    run clean ``rc=0`` with that work dropped. Set ``may_be_empty=True``
    on a phase that LEGITIMATELY may have no work of its own — a pure
    sequencing gate/barrier, or a terminal phase that only fans out
    conditionally — to declare that intent and let it drain through.

    (An all-already-done phase needs NO opt-out: a producer that finds an
    item's outputs already exist yields it with
    ``TaskInfo.skipped_already_done=True`` rather than dropping it, so the
    phase still HAS tasks — they land as terminal ``SkippedAlreadyDone``
    ledger entries and the phase proceeds as a phase WITH work, treated as
    success. ``may_be_empty`` is for genuinely-zero-item phases.)
    """

    phase_id: PhaseId
    types: tuple[TaskTypeSpec, ...]
    depends_on: tuple[PhaseId, ...] = ()
    barrier: bool = True
    may_be_empty: bool = False


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

    # Optional upload-action attribute (#336 P1 / #493 option-A).
    #
    # When the task exposes ``upload_action`` as a callable matching
    # :data:`UploadAction`, the framework registers it on the in-process
    # primary's setup executor. Every TaskInfo that declares ``files=[...]``
    # — submitter-PRODUCED local files needed on the cluster before the
    # task runs — gets one deduped upload setup task per unique
    # ``(source, dest)``, and this callable performs each upload.
    # See the alias doc for the full retry/classification contract.
    # Absent or ``None`` opts out (any setup task asking for an upload
    # then fails loud with a wiring-error terminal).
    upload_action: Optional[UploadAction]

    # Optional custom-message handler attribute (F5).
    #
    # When the task exposes ``custom_message_handler`` as a callable
    # matching :data:`CustomMessageHandler`, the framework invokes it ON
    # THE PRIMARY for every secondary->primary custom message
    # (``SecondaryHandle.send_to_primary``). See the alias doc for the
    # full delivery/ordering/error contract. Absent or ``None`` opts
    # out.
    custom_message_handler: Optional[CustomMessageHandler]

    # Optional worker custom-message listener attribute.
    #
    # When the task exposes ``worker_message_listener`` as a callable
    # matching :data:`WorkerMessageListener`, the framework registers
    # it on each SECONDARY's worker-message dispatcher. The listener
    # fires once per ``Task.send_message`` frame from that secondary's
    # own workers (in per-worker send order), off the operational
    # loop, with panic + ``PyErr`` isolation — same contract as
    # ``task_completed_listener``. Absent or non-callable opts out.
    worker_message_listener: Optional[WorkerMessageListener]
