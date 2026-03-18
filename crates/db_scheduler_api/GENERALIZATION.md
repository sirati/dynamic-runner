# db_scheduler_api — Generalization Plan

## Role
Defines the abstract `Scheduler<I>` trait and supporting types for stateless
scheduling decisions. The scheduler sees worker state snapshots and pending
tasks, and returns assignment/OOM-kill decisions. Currently memory-only.

## What is Already Generic
- `Scheduler<I>` — generic over identifier type.
- `AssignmentDecision` — describes assignment with `opportunistic` flag (supports stealing).
- `ProcessingPhase` — lifecycle phases (InitialAssignment, MainPhase, RetryPhase, etc.).
- Stateless design — all state passed as parameters, trivially testable.

## What Needs to Change

### 1. `WorkerBudgetInfo` is memory-only
```rust
pub struct WorkerBudgetInfo<I: Identifier> {
    pub reserved_budget: MemoryBytes,
    pub actual_memory_usage: MemoryBytes,
    pub estimated_memory: MemoryBytes,
    ...
}
```
Every field is a single `MemoryBytes` value.

**Change:** Generalize to multi-resource:
```rust
pub struct WorkerBudgetInfo<I: Identifier> {
    pub worker_id: WorkerId,
    pub reserved_budgets: ResourceMap,      // budget per resource kind
    pub actual_usage: ResourceMap,          // actual usage per resource kind
    pub is_idle: bool,
    pub is_opportunistic: bool,
    pub has_initial_assignment: bool,
    pub current_task: Option<BinaryInfo<I>>,
    pub estimated_usage: ResourceMap,      // estimated per resource kind
}
```
Where `ResourceMap` is a thin wrapper (e.g. `BTreeMap<ResourceKind, u64>` or a
small-vec) from `db_comm_api_base`.

### 2. `MemoryEstimator` trait is memory-only
```rust
pub trait MemoryEstimator {
    fn estimate_memory(&self, binary_size: u64) -> MemoryBytes;
}
```

**Change:** Generalize to multi-resource estimation:
```rust
pub trait ResourceEstimator {
    fn estimate(&self, binary_size: u64) -> ResourceMap;
}
```
A memory-only estimator simply returns a map with one entry.

### 3. `Scheduler` trait methods use `max_memory: MemoryBytes`
Every method takes a single `max_memory` parameter.

**Change:** Replace with `max_resources: &ResourceMap`:
```rust
fn initial_budget(&self, worker_index: u32, max_resources: &ResourceMap) -> ResourceMap;

fn assign_normal(
    &self,
    worker: &WorkerBudgetInfo<I>,
    all_workers: &[WorkerBudgetInfo<I>],
    pending: &[BinaryInfo<I>],
    max_resources: &ResourceMap,
    estimator: &dyn ResourceEstimator,
    retry_attempt: bool,
) -> AssignmentDecision;
```

### 4. `AssignmentDecision::Assign` has `estimated_memory`
```rust
Assign { worker_id, binary_index, estimated_memory: MemoryBytes, opportunistic: bool }
```

**Change:**
```rust
Assign { worker_id, binary_index, estimated_usage: ResourceMap, opportunistic: bool }
```

### 5. `OomDecision` / `check_oom` naming
"OOM" is memory-specific. The concept is really "resource pressure killing."

**Change:** Rename to `ResourcePressureDecision` and `check_resource_pressure`:
```rust
pub enum ResourcePressureDecision {
    Kill { worker_id: WorkerId, reason: String },
    NoAction,
}

fn check_resource_pressure(
    &self,
    workers: &[WorkerBudgetInfo<I>],
    max_resources: &ResourceMap,
    in_pressure_phase: bool,
) -> ResourcePressureDecision;
```

### 6. `ProcessingPhase::OomPhase` naming
**Change:** Rename to `ResourcePressurePhase`.

## Python API Restoration (`db_python_provider`)

1. **Memory-only ResourceMap:** The Python provider constructs `ResourceMap`
   entries with a single `Memory` key, producing identical behavior to current
   `MemoryBytes` usage.

2. **MemoryEstimator → ResourceEstimator:** The Python provider's estimator
   implementation returns `ResourceMap::from([(Memory, estimated_bytes)])`.

3. **Config:** The Python API accepts `max_memory: int` as before; internally
   it becomes `ResourceMap::from([(Memory, max_memory)])`.

4. **Scheduler implementation:** The concrete `MemoryStealingScheduler` in
   `db_scheduler_impl` continues to operate on the `Memory` resource kind,
   ignoring other resources in the map. Future scheduler implementations can
   handle multiple resources.

5. **OOM callbacks:** Python callbacks that handle resource pressure events
   receive the `resource_kind` field (initially always `Memory`).
