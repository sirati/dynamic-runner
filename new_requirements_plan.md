# dynamic_runner worker-pinning requirements

| Field    | Value                                                  |
|----------|--------------------------------------------------------|
| Status   | Proposed                                               |
| Audience | `dynamic-runner` maintainers       |
| Source   | `asm-dataset-nix` dataset team                         |
| Date     | 2026-04-29                                             |

## Motivation

The `asm-dataset-nix` `compiler_suit_runner` drives a build matrix of
roughly 387,000 cross-compiled ELF variants spread across about 300
distinct toolchain classes (a "toolchain class" being a unique
`(target_arch, compiler_id, compiler_version)` triple). Each toolchain
closure is several hundred MiB on disk, and for a typical variant build
its files are the dominant working set: the same `cc1`, `as`, `ld`,
`libgcc`, headers, etc. are read for every package compiled against
that toolchain.

The current `dynamic_runner._native` scheduler re-sorts the pending item list
by `size` descending and dispatches items to workers from a single
shared queue. Locality across consecutive items handed to a single
worker is therefore incidental: two items dispatched in sequence to the
same worker bear no relation to each other beyond being adjacent in the
size-sorted list.

For workloads like the dataset build, this throws away two free wins:

1. **Page-cache reuse.** A worker that just finished compiling
   `coreutils` against `gcc-13.4 / aarch64` already has every file in
   that toolchain hot in the kernel page cache. The next coreutils
   variant against the same toolchain pays only the C-source read cost.
   Dispatching that worker an unrelated `clang-18 / riscv64` item
   instead evicts the working set.

2. **Nix store / GC root locality.** Building against a toolchain takes
   GC roots on its closure. When items of the same class are batched on
   one node, the closure stays referenced and need not be re-fetched
   from substituters when the next sibling item starts.

This document specifies the smallest framework change that lets a
`TaskDefinition` opt into per-class worker pinning, without disturbing
existing tasks (tokenizer, disassembler) that have no use for it.

## Required API surface

Add an optional method to the `TaskDefinition` Protocol. The default
implementation returns `None`, which preserves today's free-pool
dispatch behavior.

```python
from typing import Protocol

class TaskDefinition(Protocol):
    def get_pinning_class(self, item: BinaryInfo) -> str | None:
        """Return a stable identifier grouping this item with siblings
        that benefit from co-location on the same worker(s).

        Returning ``None`` opts the item out of pinning; it joins the
        free pool and is dispatched by the existing size-sorted policy.

        The string is opaque to the scheduler; equality is the only
        operation used. Common shapes: ``"gcc-13.4-aarch64"``,
        ``"clang-18-x86_64"``, ``"native-default"``.
        """
        ...
```

The asm-dataset-nix `compiler_suit_runner.suit_task.SuitTask` would
implement this method by reading the toolchain identifier from the item
manifest's metadata header (set at manifest emission time from the
flake's `_crossToolchainsMeta` attribute).

## Required scheduler change

`assign_normal()` (in `rust/.../assignment.rs`) must consult the
pinning class before falling back to free-pool dispatch.

Algorithm sketch:

1. At `coord.run(items)` start, group items by `get_pinning_class(item)`
   into a map `class_id -> [items]`. Items with class `None` go to a
   distinguished `__free__` bucket.
2. For each class, compute its required worker count
   `k = ceil(len(class_items) / batch_size)`, where `batch_size` is the
   existing per-worker batch parameter the framework already exposes.
3. Allocate the workers: assign workers `W_1..W_k` to class `X` such
   that the union of all class allocations partitions a subset of the
   total worker pool. Workers not allocated to any class serve the
   `__free__` bucket.
4. During dispatch, when worker `W_i` becomes ready, draw the next item
   from its assigned class's queue (FIFO within the class). When that
   queue is empty, the worker rejoins the free pool.

Within a class, ordering is FIFO of the size-DESC sort that the
scheduler already produces; the runner can encode any secondary
ordering (e.g. larger packages first inside a class) in the item's
`size` field as today.

## Backwards compatibility

This change is strictly additive:

- Existing `TaskDefinition` implementations (asm-tokenizer's tokenizer
  task, the disassembler task, anything downstream) do **not** define
  `get_pinning_class`. The Protocol's default returns `None` for every
  item, so every item lands in the `__free__` bucket and the scheduler
  reduces to today's behavior.
- No existing call site of `assign_normal()` changes shape; the
  pinning logic is a pre-bucketing step before the existing free-pool
  loop runs against the `__free__` bucket.
- No on-the-wire protocol changes between primary and secondaries —
  pinning is an internal scheduler concern, opaque to workers.
- The CLI / Python entry points (`dynamic_runner.run`,
  `RustPrimaryCoordinator.run`) need no signature changes.

## Rebalancing

To prevent pathological allocations:

- **Tiny classes**: classes with `len(class_items) < batch_size` join
  the free pool entirely. Reserving a whole worker for two items wastes
  more time on idle than it saves on cache locality.
- **Huge classes**: classes with `len(class_items) > capacity *
  batch_size` (where `capacity` is the total worker count) saturate all
  workers; no spillover bucket is needed because every worker will
  serve that class for some interval before draining to others.
- **Stable assignment**: class-to-worker assignment is computed once at
  `coord.run` start and held for the duration of the run. Items
  produced by the runner already encode a phase rank in the high bits
  of `size`, so the framework never needs to revisit pinning across
  phase boundaries — items in different phases simply don't coexist in
  the dispatch window.

The threshold for "tiny" is `batch_size`. The threshold for "huge"
follows from the worker-count ceiling. Both are derived; no new
configuration is required from the task author.

## Memory implications

Per-worker memory accounting is unaffected:

- The Rust scheduler already tracks each worker's resident memory
  budget against `TaskDefinition.estimate_memory(item)` returns.
- Pinning constrains *which* worker an item lands on; it does not
  change the memory budget for that worker. A pinned worker that is
  full simply waits for an item to drain, identical to the free-pool
  behavior.
- The expected RSS distribution under pinning is *less* even than
  free-pool dispatch (one worker holds many items of one class) but
  the per-worker ceiling is unchanged, so this is not a new failure
  mode.

If a task wants pinning and is also memory-limited, it should size
classes such that any single class fits inside one worker's budget,
multiplied by the number of workers it expects to be allocated. The
scheduler does not attempt to subdivide classes by memory.

## Testing harness

Ship the change with a synthetic task suitable for CI:

- `N` pinning classes, each with `M` items.
- Each item's worker dispatch logs `(class_id, worker_id, dispatch_ts)`.
- A pass criterion verifies, for every class `c`:
  - All items of class `c` were dispatched to at most
    `ceil(M / batch_size)` distinct workers.
  - No worker received items from more than one class as long as
    pending items existed in its assigned class (i.e. the worker only
    drains to free-pool work after its class queue is empty).
- A second test sets `get_pinning_class` to always return `None` and
  verifies that worker assignment matches the pre-change behavior on
  the same input (regression guard for backwards compatibility).
- A stress test with one class of size `1` and one class of size
  `10 * capacity` verifies the rebalancing rules: the singleton class
  joins the free pool and the giant class saturates all workers.

## Out of scope

This requirements doc deliberately excludes:

- **Cross-cluster / cross-secondary pinning.** Pinning is per-coordinator
  / per-secondary. A class with items spread across multiple SLURM
  secondaries will still see those items on different machines; the
  asm-dataset-nix peer cache (harmonia) handles inter-node closure
  reuse.
- **Dynamic re-pinning.** Class-to-worker assignment is computed once
  at `coord.run` start and not reshuffled as items complete. If a
  class drains early, its workers join the free pool; they do not get
  reassigned to another class's overflow.
- **Memory-aware pinning.** The pinning decision ignores per-item
  memory budget; budget is a separate constraint enforced by the
  existing scheduler. A future enhancement could prefer to pin
  high-memory classes to workers with larger budgets, but that is not
  required for the dataset use case.
- **Dependency-aware ordering inside a class.** Items in a class are
  FIFO under the existing `size` sort. Items with build-order
  dependencies (e.g. "build the toolchain first") are handled today
  via the asm-dataset-nix runner's phase-rank encoding in the high
  bits of `size`; pinning does not need to know about phases.

The above are reasonable follow-up work but should not block the
minimum change described in §"Required API surface" and §"Required
scheduler change".
