# db_scheduler_impl — Generalization Plan

## Role
Concrete implementation of the `Scheduler<I>` trait: `MemoryStealingScheduler`.
Implements memory-constrained scheduling with overallocation, budget stealing
from idle workers, and OOM victim selection. Ported from Python
`DecisionWorkerManMixin` and `ExecutionWorkerManBaseImpl`.

## What is Already Generic
- Implements `Scheduler<I>` generic over identifier type.
- Stateless design — all state passed as parameters.
- Tests use `TestId(String)` — not tied to any concrete identifier.

## What Needs to Change

### 1. All logic operates on single `MemoryBytes` values
Every calculation uses scalar `u64` values: `reserved_budget`, `actual_memory_usage`,
`estimated_memory`, `max_memory`. For multi-resource support, these must operate
on `ResourceMap` (a map of `ResourceKind → u64`).

**Change:** The scheduler continues to focus on **one resource kind at a time**
(initially `Memory`). The generalization is:
```rust
pub struct ResourceStealingScheduler {
    pub resource_kind: ResourceKind,  // which resource to schedule on
}
```
All internal logic remains the same but indexes into `ResourceMap` by
`self.resource_kind`.

### 2. Hardcoded constants are tokenizer-tuned
- **150MB base overhead** in `initial_budget` — assumes tokenizer minimum memory.
- **500MB OOM threshold** in `check_oom` — tuned for tokenizer workloads.
- **Temp factors** (1.5, 2.0, n+1) in `assign_normal` — heuristics for text processing.

**Change:** Make these configurable:
```rust
pub struct ResourceStealingScheduler {
    pub resource_kind: ResourceKind,
    pub base_overhead: u64,        // was: 150MB
    pub pressure_threshold: u64,   // was: 500MB
    pub temp_factors: Vec<f64>,    // was: [1.5, 2.0, 3.0, ...]
}
```
Default values match current behavior.

### 3. `initial_budget` formula is domain-specific
The formula `max_memory / (n+2) + 150MB` assumes a specific workload profile.
Different task types may need different budget distribution strategies.

**Change:** The configurable `base_overhead` handles this. Alternatively,
`initial_budget` could take a closure/strategy parameter, but that's
over-engineering for now — the configurable constants suffice.

### 4. `MemoryEstimator` → `ResourceEstimator`
The trait `MemoryEstimator` used by this scheduler must change to
`ResourceEstimator` (defined in `db_scheduler_api`). The scheduler calls
`estimator.estimate(binary_size)` and extracts `self.resource_kind` from the
returned `ResourceMap`.

### 5. Method names: `check_oom` → `check_resource_pressure`
Follows the rename in `db_scheduler_api`.

### 6. Rename: `MemoryStealingScheduler` → `ResourceStealingScheduler`
Reflects the generalized resource concept.

## Python API Restoration (`db_python_provider`)

1. **Scheduler construction:** The Python provider creates:
   ```python
   scheduler = ResourceStealingScheduler(
       resource_kind=ResourceKind.Memory,
       base_overhead=150 * 1024 * 1024,      # 150MB
       pressure_threshold=500 * 1024 * 1024,  # 500MB
   )
   ```
   This produces identical behavior to the current hardcoded `MemoryStealingScheduler`.

2. **Estimator:** The Python provider's `MemoryEstimator` implementation wraps
   into a `ResourceEstimator` that returns `ResourceMap::from([(Memory, estimate)])`.

3. **Python configuration:** Expose `base_overhead` and `pressure_threshold` as
   optional Python constructor parameters with defaults matching current behavior.
   This allows Python users to tune for different workloads without Rust changes.

4. **Multiple resources (future):** When Python users want to schedule on CPU
   or GPU memory, they create additional `ResourceStealingScheduler` instances
   with different `resource_kind` values. The manager can compose multiple
   schedulers for multi-resource scheduling.
