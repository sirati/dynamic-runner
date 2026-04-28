# db_local_manager — Generalization Plan

## Role
Core orchestration crate. `LocalManager<M, S, E, I>` manages a pool of worker
processes through a 5-phase pipeline (InitialAssignment → Main → Retry → OOM →
Unassigned). Contains `WorkerPool<M, I>` for worker lifecycle, `WorkerHandle`
for per-worker state machines, and event-driven task dispatch via
`tokio::select!`.

## What is Already Generic
- Generic over `M: ManagerEndpoint`, `S: Scheduler<I>`, `E: MemoryEstimator`, `I: Identifier`.
- `WorkerFactory<M>` trait — pluggable worker spawning (subprocess, channel, socket).
- `WorkerPool<M, I>` — generic worker lifecycle management.
- Protocol state machine (`RunnerProtocol`) — compile-time enforced transitions.
- Event-driven loop — no task-specific logic in dispatch.
- No tokenizer, ASM, or text-processing knowledge anywhere.

## What Needs to Change

### 1. All memory tracking uses scalar `MemoryBytes`

`WorkerHandle` fields:
```rust
reserved_budget: MemoryBytes,
estimated_memory: MemoryBytes,
actual_memory_usage: MemoryBytes,
```
`LocalManager` fields:
```rust
total_assigned_memory: MemoryBytes,
```
`LocalManagerConfig`:
```rust
max_memory: MemoryBytes,
```

**Change:** Replace with `ResourceMap` (from the generalized `db_comm_api_base`):
```rust
reserved_budgets: ResourceMap,
estimated_usage: ResourceMap,
actual_usage: ResourceMap,
total_assigned: ResourceMap,
max_resources: ResourceMap,
```

### 2. `/proc/[pid]/statm` reads RSS only (memory)
`WorkerHandle::update_memory_usage()` reads RSS pages from `/proc/[pid]/statm`
and converts to bytes. This is Linux-only and memory-only.

**Change:** Introduce a `ResourceMonitor` trait:
```rust
pub trait ResourceMonitor {
    fn measure(&self, pid: u32) -> ResourceMap;
}
```
Default implementation reads `/proc/[pid]/statm` for memory. Future
implementations can read CPU time from `/proc/[pid]/stat`, GPU usage from
`nvidia-smi`, etc.

### 3. `/proc/meminfo` for system free memory
`get_free_system_memory()` reads `MemAvailable` from `/proc/meminfo`. Used in
the Unassigned phase to skip tasks when system memory is below 300MB.

**Change:** Generalize to `get_free_system_resources() -> ResourceMap`. The
300MB threshold becomes `low_resource_thresholds: ResourceMap` in config.

### 4. OOM phase naming
Phase names `OomPhase`, `check_oom`, `oom_tasks`, `in_oom_phase` are
memory-specific.

**Change:** Rename to `ResourcePressurePhase`, `check_resource_pressure`,
`pressure_tasks`, `in_pressure_phase`.

### 5. `WorkerHandle::budget_info()` returns `WorkerBudgetInfo<I>`
This snapshot struct has memory-only fields (from `db_scheduler_api`). When
`db_scheduler_api` generalizes `WorkerBudgetInfo` to use `ResourceMap`, this
method automatically picks up the change.

### 6. Memory logging CSV
`log_memory_usage()` writes `size,estimated,actual,filename,status`. With
multi-resource support, this should log all tracked resources.

**Change:** Extend CSV columns or use a structured log format that includes
resource kind.

### 7. Hardcoded 300MB threshold in Unassigned phase
```rust
const LOW_MEMORY_THRESHOLD: u64 = 300 * 1024 * 1024;
```

**Change:** Move to config as `low_resource_thresholds: ResourceMap` with
`Memory → 300MB` as default.

### 8. `TaskResult` / `Response::Done` carries `warnings`/`filtered`
The event handler in `process_worker_loop` extracts `warnings` and `filtered`
from completed tasks. This propagates the tokenizer-specific fields from
`db_manager_runner_comm`.

**Change:** When `Response::Done` becomes generic (per the
`db_manager_runner_comm` generalization), the event handler passes through the
generic result payload without interpreting it.

## Python API Restoration (`db_python_provider`)

1. **Config:** Python creates `LocalManagerConfig` with
   `max_resources: ResourceMap::from([(Memory, max_memory_bytes)])`. Single
   memory value — identical behavior.

2. **ResourceMonitor:** Python uses the default `/proc/[pid]/statm`
   implementation. No Python code change needed.

3. **Scheduler:** The scheduler receives `ResourceMap` but the
   `MemoryStealingScheduler` (now `ResourceStealingScheduler`) only looks at
   the `Memory` entry — identical decisions.

4. **Estimator:** Python's estimator returns
   `ResourceMap::from([(Memory, estimate)])`.

5. **Task results:** Python extracts `warnings`/`filtered` from the generic
   result payload in the Python provider layer, exactly as before from the
   user's perspective.

6. **Stats/logging:** Python receives the same completion stats. Memory logging
   CSV includes the `Memory` resource column by default.

7. **Low-memory threshold:** Default `low_resource_thresholds` includes
   `Memory → 300MB`, matching current behavior.
