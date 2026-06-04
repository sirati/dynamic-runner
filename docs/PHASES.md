# Phases, Task Types, and Affinity

Migration guide for downstream `TaskDefinition` implementations
(`asm-tokenizer`, `asm-dataset-nix`, and any other consumer that still
holds an old-shape `TaskDefinition`).

## TL;DR

The `TaskDefinition` Protocol now exposes **phases**, **task types**, and
**affinity classes** as first-class concepts. The framework — not the
consumer — owns phase ordering, drain detection, and worker dispatch.
The old `Phase` enum, `StageDefinition`, `get_stages`,
`organize_and_sort_items`, `get_worker_module`,
`get_reserved_memory_per_worker`, `find_binaries`, and `dispatch_binary`
surface is gone. There are no compatibility shims: downstream
consumers must migrate.

## Concepts

### Phase

A **phase** is a unit of work with optional dependencies on other
phases. Items only dispatch when their phase is active. The framework
gates dispatch at phase boundaries — the consumer no longer writes
flag files, no longer calls `barrier()`, no longer maintains an
in-process counter table to know when "phase 2 is finished and phase 3
may start."

A phase is declared by `PhaseSpec`:

```python
@dataclass(frozen=True)
class PhaseSpec:
    phase_id: PhaseId
    types: tuple[TaskTypeSpec, ...]
    depends_on: tuple[PhaseId, ...] = ()
    barrier: bool = True
```

`depends_on` is a tuple of `phase_id` values; the framework computes
the schedule from the dependency DAG. The order phases appear in
`get_phases()` is informational only. `barrier=True` (the default)
means full drain of dependencies before any item of this phase
dispatches; `barrier=False` is reserved for future pipelined work and
is not used today.

A phase becomes active only after every dependency has fully drained
(every item terminated, success or failure) and that phase's
`on_phase_end` hook has returned.

### Task type

Within a phase, a **task type** binds a worker module (a Python module
spawned as a subprocess) to a memory estimator. Multiple types per
phase is supported and is the mechanism by which a single phase can
run two or more different worker modules in parallel.

```python
@dataclass(frozen=True)
class TaskTypeSpec:
    type_id: TypeId
    worker_module: str
    estimator_attr: str = "estimate_memory"
    timeout_seconds: float | None = None
    reserved_memory_per_worker: int = 0
    max_concurrent: int | None = None
```

`max_concurrent` is an optional global cap on how many items of this
type run concurrently across all workers (`None` = unconstrained). Use
it to throttle a compile-heavy type (e.g. `cores // 4`) while cheaper
IO-bound types run at the full `--jobs` width.

`worker_module` names the Python module the framework runs as a
subprocess for items of this type (`python -m <worker_module> ...`).
`estimator_attr` names a method on the `TaskDefinition` instance that
returns per-item memory in bytes; the default `"estimate_memory"` ties
all types to the same estimator, but each type can supply its own by
setting `estimator_attr` to a type-specific method name.

### Affinity class

An **affinity class** is an opaque string per item (`AffinityId`,
which is `str`). Items sharing an affinity prefer the same worker so
kernel page-cache reuse is realized — the kind of locality win the
asm-dataset-nix toolchain build benefits from when consecutive items
read the same `cc1`, `as`, `ld`, headers, etc.

Pinning is **soft**: a worker prefers items from its bucket but never
refuses other work to stay busy. When a worker's affinity bucket is
empty, it joins the free pool. Items with `affinity_id=None` join the
free pool from the start.

## The Protocol

The full Protocol surface lives in
[`python/dynamic_runner/task_protocol.py`](../python/dynamic_runner/task_protocol.py).
Every method below is structurally typed; subclassing
`TaskDefinition` is not required.

### Topology

```python
def get_phases(self) -> tuple[PhaseSpec, ...]: ...
```

Called once at run start. Returns the full topology: every phase, the
types within it, and inter-phase dependencies. The framework computes
the schedule from this declaration.

### Item discovery

```python
def discover_items(
    self, source_dir: Path, args: Namespace
) -> Iterable[TaskInfo]: ...
```

Yields the items for the run. Each item is a `TaskInfo`
(`python/dynamic_runner/_shared/task_info.py`) and must carry its
`phase_id`, `type_id`, `task_id`, and (optionally) `affinity_id` and
`task_depends_on` fields populated.
The discovery method is responsible for both finding the items and
classifying them — the framework does no automatic bucketing.

### Per-type plumbing

```python
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
```

`estimate_memory` receives the full `TaskInfo` (not just its `size`).
Per-type estimators (named via `TaskTypeSpec.estimator_attr`) follow
the same signature.

`build_worker_command_args` receives the `type_id` so a single
`TaskDefinition` instance can build the right argv for whichever
worker module is being launched.

`get_output_filename_pattern` likewise receives the `type_id` so
output paths can be type-specific.

### Lifecycle hooks

```python
def on_run_start(
    self,
    source_dir: Path,
    output_dir: Path,
    args: Namespace,
    primary_handle: Optional["PrimaryHandle"] = None,
) -> None: ...

def on_run_end(self, success: bool) -> None: ...

def on_phase_start(self, phase_id: PhaseId) -> None: ...

def on_phase_end(
    self, phase_id: PhaseId, completed: int, failed: int
) -> None: ...
```

`on_run_start` / `on_run_end` are the run-level boundaries; bring up
peer caches, warm long-lived connections, persist a final manifest,
and so on. `on_phase_start` / `on_phase_end` are the per-phase
boundaries; the consumer can compact intermediate output, snapshot
invariants, or kick off cross-phase compaction work between phases.

`primary_handle` is the in-flight runtime control surface for the
primary coordinator (minted off `RustPrimaryCoordinator.handle()`). It
is the live `PrimaryHandle` on primary-side dispatchers — so the task
can drive `primary_handle.spawn_tasks(...)` from inside `on_run_start`
— and `None` on secondaries, which own no coordinator. The framework
passes it as the `primary_handle=` kwarg on every primary-side
dispatcher; a legacy `on_run_start` that omits the parameter keeps
working via a positional-only fallback in the PyO3 bridge
(`crates/dynrunner-pyo3/src/managers/lifecycle.rs`).

## Migration: from old Protocol to new Protocol

### Method renames

| Old                                  | New                                                  |
|--------------------------------------|------------------------------------------------------|
| `get_stages()`                       | `get_phases()` returning `tuple[PhaseSpec, ...]`     |
| `find_binaries` / `_collect_binaries` (framework-side) | `discover_items(source_dir, args)` (task-side) |
| `get_worker_module()` (per task)     | `TaskTypeSpec.worker_module` (per type)              |
| `get_reserved_memory_per_worker()`   | `TaskTypeSpec.reserved_memory_per_worker`            |
| `organize_and_sort_items(items)`     | fold into `discover_items`                           |
| `dispatch_binary(...)`               | gone; subprocess workers selected per-type by `TaskTypeSpec.worker_module` |

### Removed methods and types

- `Phase` enum — gone. Phase identifiers are opaque strings (`PhaseId`).
- `StageDefinition` — gone. Replaced by `PhaseSpec` + `TaskTypeSpec`.
- `get_stages` — gone. Replaced by `get_phases`.
- `organize_and_sort_items` — gone. Item ordering and classification
  happen inside `discover_items`.
- `find_binaries` (the framework-side binary walker) — gone. Items
  flow from `discover_items`.
- `dispatch_binary` — gone. The framework dispatches items to the
  correct worker subprocess based on the item's `type_id` and the
  matching `TaskTypeSpec.worker_module`.

### Lifecycle hooks (new)

The old code paths used ad-hoc `setup_*` / `teardown` patterns that
varied between consumers. The new Protocol replaces them with the four
named hooks documented under [Lifecycle hooks](#lifecycle-hooks): use
`on_run_start` / `on_run_end` for run-spanning resources (peer-cache
services, output writers, log handles) and `on_phase_start` /
`on_phase_end` for per-phase setup and teardown (intermediate output
directories, cross-phase compaction).

### `BinaryInfo` → `TaskInfo`

The data type was renamed and gained scheduling-metadata fields:

```python
@dataclass
class TaskInfo:
    path: Path
    size: int
    identifier: BinaryIdentifier
    phase_id: str = ""
    type_id: str = ""
    affinity_id: str | None = None
    payload: dict = field(default_factory=dict)
    task_id: str = ""
    task_depends_on: tuple["TaskDep | str", ...] = field(default_factory=tuple)
```

`task_id` is a stable, consumer-supplied identifier. It is required —
the empty-string default exists only to keep positional construction
source-compatible; the Python→Rust extractor rejects empty/`None` ids
with a loud `ValueError`. It is used by per-task dependency wiring, the
memprofile sampler's file naming, the retry tracker, and the failure
reporter.

`task_depends_on` lists prerequisite `task_id`s that must terminate
(success or permanent failure) before this task dispatches — a
per-task ordering constraint independent of the phase barrier (e.g. a
variant build depending on its toolchain build, both in one phase, so
variants dispatch continuously as toolchains drain). Entries are bare
`str` ids (legacy, equals `TaskDep(id, inherit_outputs=False)`) or
`TaskDep` instances; `inherit_outputs=True` opts the dependent into
reading its predecessors' transitive ancestors' published outputs.
Unknown ids and cycles fail loud at run start.

`BinaryIdentifier` (the inner identifier shape) is unchanged:

```python
@dataclass(frozen=True)
class BinaryIdentifier:
    binary_name: str
    platform: str
    compiler: str
    version: str
    opt_level: str
```

`TaskInfo` is exported from `dynamic_runner._shared` (defined in
`dynamic_runner._shared.task_info`). Convenience properties
(`binary_name`, `platform`, `compiler`, `version`, `opt_level`) on
`TaskInfo` proxy through to the inner `identifier` for callers that
don't unpack the identifier directly.

The `payload` field is opaque to the framework — consumers can stash
JSON-serializable per-item metadata there for the worker to pick up.

### Worker subprocess protocol

The framework spawns one worker subprocess per task type. The
`type_id` of the dispatched item picks the matching `TaskTypeSpec`
and that spec's `worker_module` is the Python module run as `python
-m <worker_module> <args>`. The argv is built by
`TaskDefinition.build_worker_command_args(type_id, args, ...)`, so
each worker module can have its own CLI shape.

Workers do not need to know about phase or affinity ordering — the
framework dispatches items in already-correct order.

## Worked example: a single-phase, single-type task

The asm-tokenizer pattern (one worker module, one estimator, no
inter-phase dependencies) collapses to a one-phase, one-type
declaration:

```python
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec

class TokenizerTask:
    def get_phases(self):
        return (
            PhaseSpec(
                phase_id="tokenize",
                types=(
                    TaskTypeSpec(
                        type_id="tokenize",
                        worker_module="dynamic_runner_tokenizer.tokenizer_worker",
                    ),
                ),
            ),
        )

    def discover_items(self, source_dir, args):
        for path in self._walk(source_dir, args):
            yield TaskInfo(
                path=path,
                size=path.stat().st_size,
                identifier=self._parse(path),
                phase_id="tokenize",
                type_id="tokenize",
                task_id=str(path),
            )

    def estimate_memory(self, item):
        return 4 * item.size + 256 * 1024 * 1024

    def build_worker_command_args(
        self, type_id, args, source_dir, output_dir, skip_existing
    ):
        cmd = [str(source_dir), str(output_dir)]
        if skip_existing:
            cmd.append("--skip_existing")  # underscore: matches the framework's own worker-arg injection
        return cmd

    # ...lifecycle hooks default to no-ops; implement only as needed
```

The asm-tokenizer repository's `dynamic_runner_tokenizer/tokenizer_task.py`
is the canonical reference implementation for this pattern.

## Worked example: a multi-phase, multi-type task with affinity

The asm-dataset-nix `compiler_suit_runner` pattern: four phases (toolchain
fetch → toolchain build → variant build → variant tokenize), each with one
or more task types, with affinity used to co-locate the toolchain build
phase's items with their downstream variant builds:

```python
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec

class CompilerSuitRunner:
    def get_phases(self):
        return (
            PhaseSpec(
                phase_id="fetch",
                types=(
                    TaskTypeSpec(
                        type_id="fetch_toolchain",
                        worker_module="compiler_suit_runner.fetch_worker",
                        estimator_attr="estimate_fetch_memory",
                    ),
                ),
            ),
            PhaseSpec(
                phase_id="build_toolchain",
                depends_on=("fetch",),
                types=(
                    TaskTypeSpec(
                        type_id="build_toolchain",
                        worker_module="compiler_suit_runner.toolchain_worker",
                        estimator_attr="estimate_toolchain_memory",
                    ),
                ),
            ),
            PhaseSpec(
                phase_id="build_variant",
                depends_on=("build_toolchain",),
                types=(
                    TaskTypeSpec(
                        type_id="build_variant",
                        worker_module="compiler_suit_runner.variant_worker",
                        estimator_attr="estimate_variant_memory",
                    ),
                ),
            ),
            PhaseSpec(
                phase_id="tokenize",
                depends_on=("build_variant",),
                types=(
                    TaskTypeSpec(
                        type_id="tokenize_variant",
                        worker_module="compiler_suit_runner.tokenize_worker",
                        estimator_attr="estimate_tokenize_memory",
                    ),
                ),
            ),
        )

    def discover_items(self, source_dir, args):
        # For every variant the runner intends to build, yield one item per
        # phase, all sharing the same affinity_id. The framework gates
        # dispatch so the toolchain item runs first; affinity then keeps the
        # downstream variant items on the same worker (and thus the same
        # toolchain page cache).
        for variant in self._enumerate_variants(source_dir, args):
            toolchain_class = variant.toolchain_class  # e.g. "gcc-13.4-aarch64"
            yield TaskInfo(
                path=variant.toolchain_manifest,
                size=variant.toolchain_size,
                identifier=variant.toolchain_id,
                phase_id="build_toolchain",
                type_id="build_toolchain",
                affinity_id=toolchain_class,
                task_id=toolchain_class,
            )
            yield TaskInfo(
                path=variant.source_path,
                size=variant.source_size,
                identifier=variant.identifier,
                phase_id="build_variant",
                type_id="build_variant",
                affinity_id=toolchain_class,
                task_id=variant.variant_id,
                task_depends_on=(toolchain_class,),
            )
```

Items with the same `affinity_id` ("gcc-13.4-aarch64") prefer the same
worker, so the worker that built that toolchain in phase
`build_toolchain` is the worker that picks up its variants in phase
`build_variant`. The kernel page cache for `cc1`, `as`, `ld`, libgcc,
and the toolchain headers stays hot across the phase boundary.

## What the framework now owns

- **Phase ordering.** Phase dependencies are declared via
  `PhaseSpec.depends_on` and the framework gates dispatch at phase
  boundaries based on the resulting DAG. Consumers do not write flag
  files, do not call into a `PhaseBarrier`, and do not maintain
  cross-process counters to detect drain.
- **Drain detection.** Per-phase completion accounting is the
  framework's responsibility. The consumer learns the final tallies
  through `on_phase_end(phase_id, completed, failed)`.
- **Worker affinity.** Soft worker pinning is implemented inside the
  framework. The consumer sets `affinity_id` on each item; the
  framework's pending-item bookkeeping prefers to dispatch
  same-affinity items to the same worker, with graceful fallback to
  the free pool when a worker's bucket is empty.
- **Subprocess worker dispatch.** Each task type gets its own worker
  subprocess. The framework spawns it (one process per type per
  worker slot, in the existing worker-pool / secondary topology) and
  routes items to the right one based on `type_id`.

## What the consumer still owns

- **Item discovery and classification.** `discover_items` returns
  items already tagged with their `phase_id`, `type_id`, and
  `affinity_id`. The framework never inspects file paths, parses
  filenames, or guesses which phase an item belongs to.
- **Per-item memory estimates.** `estimate_memory(item)` (or the
  per-type variant named via `TaskTypeSpec.estimator_attr`) is the
  consumer's responsibility. The estimator receives the full
  `TaskInfo` and can use any field.
- **Per-type worker module CLI args.** `build_worker_command_args`
  receives the `type_id` and constructs the argv for that worker.
  The consumer owns the contract between its own task definition and
  its own worker modules; the framework only dispatches.
- **Per-run setup and teardown.** Out-of-process state — peer-cache
  services, output writers, long-lived connections — is the
  consumer's responsibility. Use `on_run_start` / `on_run_end` to
  bring it up and tear it down. Use `on_phase_end` for cross-phase
  compaction or invariant maintenance.

## Out of scope

The redesign deliberately omits several capabilities that consumers
might want; refer to `new_requirements_plan.md` for the original
requirements that motivated this work.

- **Per-item dependencies inside a phase.** A phase is a flat bag of
  items as far as the framework is concerned; items within a phase
  do not have ordering relationships. If a consumer needs ordering,
  it splits the work across two phases with a `depends_on` between
  them.
- **Per-phase memory ceilings.** `max_resources` is global to the
  run; per-phase budgets are not supported. A long phase that
  monopolises memory for a long stretch is the consumer's
  responsibility to keep within budget.
- **Cross-secondary affinity.** Affinity pinning is per-coordinator
  (per-secondary, in the distributed mode). Items with the same
  `affinity_id` spread across multiple secondaries will not be
  reunited; for inter-node closure reuse, the consumer should
  arrange a peer cache (e.g. asm-dataset-nix's harmonia setup) at a
  layer below the framework.
- **Dynamic re-pinning.** Class-to-worker assignment is computed at
  run start and held; the framework does not reshuffle as items
  complete or as classes drain early.

## Migration checklist

A downstream consumer with an existing `TaskDefinition` should:

1. Replace `get_stages()` returning `list[StageDefinition]` with
   `get_phases()` returning `tuple[PhaseSpec, ...]`.
2. For each old stage, declare a `PhaseSpec` with at least one
   `TaskTypeSpec`. Move `worker_module` from `get_worker_module()`
   into `TaskTypeSpec.worker_module`. Move
   `get_reserved_memory_per_worker()` into
   `TaskTypeSpec.reserved_memory_per_worker`.
3. Replace any `Phase` enum with plain string `phase_id` values used
   throughout `discover_items` and `get_phases`.
4. Replace `find_binaries` / `_collect_binaries` calls with
   `discover_items(source_dir, args)`. Inline any
   `organize_and_sort_items` logic — the items the method yields
   are dispatched in yield order (modulo affinity pinning).
5. Update every `BinaryInfo(...)` constructor call to `TaskInfo(...)`
   (exported from `dynamic_runner._shared`) and populate the required
   `task_id`, the `phase_id` / `type_id` tags, and (optionally)
   `affinity_id` / `task_depends_on`.
6. Update `estimate_memory` to take `item: TaskInfo` instead of
   `binary_size: int`. Use `item.size` if no other field is needed.
7. Update `build_worker_command_args` to accept the leading
   `type_id: str` parameter.
8. Update `get_output_filename_pattern` to accept the leading
   `type_id: str` parameter (and an `item: TaskInfo` instead of
   just a filename string).
9. Replace ad-hoc `setup_*` / `teardown` methods with
   `on_run_start` / `on_run_end`. Move any per-phase setup into
   `on_phase_start` / `on_phase_end`.
10. Delete `dispatch_binary` if implemented; it is no longer called.
11. Remove flag-file barriers, in-process phase counters, and
    affinity-tracking state. The framework owns all of it now.
