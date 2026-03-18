# db_python_provider — Generalization Plan

## Role
Root crate (cdylib). Exposes the entire Rust system to Python via PyO3. Defines
the concrete `TokenizerIdentifier`, wraps `LocalManager`, `SecondaryCoordinator`,
`PrimaryCoordinator`, and `DistributedManager` as Python classes. Handles worker
subprocess spawning, memory estimation bridging, and GIL management.

**Critical rule from `todo.md`:** This crate ONLY exposes functionality to
Python. It does NOT implement any logic itself.

## What is Already Generic (in the underlying crates)
- All Rust crates use `I: Identifier` — fully generic over task type.
- `WorkerPool`, `LocalManager`, schedulers, transports — all parameterized.
- This crate is the **only place** where the generic `I` is concretized to
  `TokenizerIdentifier`.

## What Needs to Change

### 1. `TokenizerIdentifier` is the only concrete identifier
```rust
pub struct TokenizerIdentifier {
    pub binary_name: String,
    pub platform: String,
    pub compiler: String,
    pub version: String,
    pub opt_level: String,
}
```
This is correct — the concrete identifier belongs here. But the Python API
should allow users to define **custom identifiers** for different task types.

**Change:** Instead of hardcoding `TokenizerIdentifier`, allow Python users
to provide an identifier schema:

Option A (simpler): Use a `HashMap<String, String>` as a generic identifier:
```rust
pub type DynamicIdentifier = HashMap<String, String>;
```
Python users pass any dict of string key-value pairs. The current
`TokenizerIdentifier` behavior is achieved by passing
`{"binary_name": ..., "platform": ..., ...}`.

Option B (more structured): Keep `TokenizerIdentifier` as the default but
allow the Python module to be parameterized. Less practical with PyO3.

**Recommended: Option A.** A `HashMap<String, String>` satisfies the
`Identifier` trait bounds and is maximally flexible from Python.

### 2. `PyMemoryEstimatorBridge` is memory-only
```rust
struct PyMemoryEstimatorBridge {
    slope: f64,
    intercept: f64,
}
```
Linear memory estimation from two probe points.

**Change:** Generalize to `PyResourceEstimatorBridge`:
```rust
struct PyResourceEstimatorBridge {
    estimators: HashMap<ResourceKind, LinearEstimator>,
}

struct LinearEstimator { slope: f64, intercept: f64 }
```
The Python `task_definition` provides `estimate_memory()` plus optional
`estimate_cpu()`, `estimate_gpu()`, etc. Each is probed and stored.

### 3. `MemoryStealingScheduler` is hardcoded
The scheduler choice is not configurable from Python.

**Change:** Expose scheduler configuration:
```python
RustLocalManager(
    ...,
    scheduler_config={
        "resource_kind": "memory",
        "base_overhead": 150 * 1024 * 1024,
        "pressure_threshold": 500 * 1024 * 1024,
    },
)
```
Or allow passing a Python scheduler object (more complex, deferred).

### 4. `max_memory` parameter is memory-only
```python
RustLocalManager(num_workers=4, max_memory=8_000_000_000, ...)
```

**Change:** Replace with `max_resources` dict:
```python
RustLocalManager(
    num_workers=4,
    max_resources={"memory": 8_000_000_000},
    ...
)
```

### 5. `ProcessingStats` lacks task-specific result data
```rust
pub struct ProcessingStats {
    pub completed: u32,
    pub total: u32,
    pub errored: u32,
    pub skipped: u32,
}
```
No aggregation of `warnings` / `filtered` or any task-specific metrics.

**Change:** Add an optional `result_summary: dict` field that aggregates
task-specific result data. For the tokenizer use case:
```python
stats.result_summary  # {"total_warnings": 42, "total_filtered": 7}
```
This is populated from the generic `TaskResult<R>` payloads.

### 6. `FailedTask` exposes `error_type` as string
Currently `"OutOfMemory"`, `"NonRecoverable"`, `"Recoverable"`.

**Change:** When `ErrorType` gains `ResourceExhausted(ResourceKind)`, the
string representation becomes `"resource_exhausted:memory"`.

### 7. `Response::Done { warnings, filtered }` propagation
Worker completion events carry tokenizer-specific counters through the entire
stack.

**Change:** After `Response::Done` becomes generic (per
`db_manager_runner_comm` generalization), this crate handles the
concrete-to-generic mapping:
- The codec in this crate encodes/decodes `TokenizerDonePayload` in the
  generic `Done` wire format.
- Python workers are updated to use the new format.
- On the receiving end, this crate extracts the task-specific data and
  populates `result_summary`.

### 8. Worker subprocess command construction
```bash
python -m <worker_module> --dynamic_queue <fd> --source ... --output ...
```
The CLI flags are reasonable but `--dynamic_queue` name is coupled to
"dynamic batch."

**Change (low priority):** Consider `--comm-fd` or make the flag name
configurable.

### 9. Distributed modes use `TokenizerIdentifier` throughout
`RustDistributedManager`, `RustPrimaryCoordinator`, `RustSecondaryCoordinator`
all instantiate with `TokenizerIdentifier`.

**Change:** When switching to `DynamicIdentifier` (Option A above), all
distributed types are instantiated with `DynamicIdentifier` instead.

### 10. `ram_bytes` in secondary welcome
The secondary coordinator sends `ram_bytes` in the `SecondaryWelcome` message.

**Change:** Sends `resources` instead (per `db_primary_secondary_comm`
generalization). This crate populates it with
`vec![ResourceAmount { kind: Memory, amount: ram_bytes }]`.

## Summary of Python API Changes (User-Facing)

### Changed Parameters
- `max_memory=N` → `max_resources={"memory": N}` (dict of resource budgets)
- `estimate_memory()` on `task_definition` still works; additional
  `estimate_cpu()` etc. methods can be added for multi-resource estimation
- `BinaryIdentifier` with 5 fields → `dict` of string key-value pairs
  (or keep `BinaryIdentifier` as convenience that converts to dict)
- `error_type` strings: `"OutOfMemory"` → `"resource_exhausted:memory"`

### Unchanged
- `stats.completed`, `.total`, `.errored`, `.skipped`
- `failed_tasks` and `oom_tasks` lists (structure unchanged)
- All three distributed modes

### New Capabilities
- `max_resources={"memory": N, "cpu": M}` for multi-resource scheduling
- `scheduler_config={}` for tuning scheduler parameters
- `stats.result_summary` for aggregated task-specific metrics
- Custom identifier dicts for non-tokenizer task types
