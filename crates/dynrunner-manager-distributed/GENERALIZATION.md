# db_distributed_manager — Generalization Plan

## Role
Implements the full distributed coordination system:
- **PrimaryCoordinator<T, S, E, I>** — central orchestrator managing work
  across remote nodes via a 9-phase pipeline.
- **SecondaryCoordinator<PT, P, M, S, E, I>** — local node coordinator that
  manages a `WorkerPool` and communicates with primary and peers.
- **SecondaryConnection** state machine — typestate pattern for protocol phases.
- **ExtractionCache** — ZIP extraction with SHA256 verification.
- **MessageRouter** — typed message dispatching.

## What is Already Generic
- All coordinators generic over `I: Identifier`, `S: Scheduler<I>`,
  `E: ResourceEstimator`, transport traits.
- No tokenizer/ASM/text-processing references anywhere.
- Typestate pattern for `SecondaryConnection` — compile-time protocol enforcement.
- ZIP extraction, file hashing, peer discovery — all domain-agnostic.
- SLURM-primary promotion and peer timeout detection — generic coordination.
- Comprehensive tests using `TestId(String)` — not tied to any concrete task.
- Inter-phase ordering between task-definition phases
  (`PhaseSpec.depends_on`) is the primary's responsibility: a
  secondary only ever sees items the primary has already cleared
  for dispatch. Secondaries do not read flag files, do not
  cross-check `phase_id`s against an in-memory drain table, and do
  not negotiate phase order peer-to-peer.

## What Needs to Change

### 1. `RemoteWorkerState<I>` has memory-only budget
```rust
pub struct RemoteWorkerState<I> {
    pub memory_budget: u64,
    pub estimated_memory: u64,
    ...
}
```

**Change:**
```rust
pub struct RemoteWorkerState<I> {
    pub resource_budgets: ResourceMap,
    pub estimated_usage: ResourceMap,
    ...
}
```

### 2. `SecondaryConnection` stores `ram_bytes: u64`
The state machine stores only RAM capacity from the `SecondaryWelcome` message.

**Change:** Store `resources: Vec<ResourceAmount>` (or `ResourceMap`) instead.
When the `DistributedMessage::SecondaryWelcome` field changes from `ram_bytes`
to `resources`, this follows naturally.

### 3. Primary's `handle_task_request` uses `available_memory`
```rust
// From TaskRequest message
available_memory: u64
```
Used to call `scheduler.assign_normal()` with a single memory value.

**Change:** When `TaskRequest` carries `available_resources: Vec<ResourceAmount>`,
the primary constructs a `WorkerBudgetInfo` with a `ResourceMap` instead of a
scalar memory value.

### 4. Primary's initial assignment uses `scheduler.initial_budget()`
Returns a single `MemoryBytes`. The primary stores this in
`worker.memory_budget`.

**Change:** Returns `ResourceMap`. Stored in `worker.resource_budgets`.

### 5. SLURM-primary task selection uses memory fitting
When the SLURM-primary secondary handles `TaskRequest`, it finds a task
fitting the requested `available_memory`.

**Change:** The fitting logic checks all resource dimensions in
`available_resources` against `estimator.estimate()` results.

### 6. `TaskComplete` carries `warnings` / `filtered`
```rust
TaskComplete { ..., warnings: u32, filtered: u32 }
```
The primary and secondary forward these values from worker completion events.

**Change:** When `TaskComplete` changes to carry `result_data: serde_json::Value`
(per `db_primary_secondary_comm` generalization), the coordinators forward
the opaque payload without interpretation.

### 7. `WorkerReadyInfo` has `memory_budget`
Sent in `InitialAssignment` to secondaries.

**Change:** Follows `db_primary_secondary_comm` generalization to
`resource_budgets: Vec<ResourceAmount>`.

### 8. OOM handling in secondary
The secondary calls `pool.check_oom()` every 100ms. Reports failures as
`TaskFailed` with `error_type: "oom"`.

**Change:** When `check_oom` becomes `check_resource_pressure`, the error type
becomes `ResourceExhausted(Memory)`. The `TaskFailed` message's `error_type`
string becomes `"resource_exhausted:memory"`.

### 9. Hardcoded timeouts and constants
- Connect timeout: 600s
- Peer timeout: 300s (primary), 120s (secondary)
- Keepalive interval: 1s
- OOM check interval: 100ms
- Operational timeout: 300s
- Request backoff: 1s → 60s

These are reasonable defaults but should be configurable in `PrimaryConfig` /
`SecondaryConfig`. Most already are — verify all are exposed.

## Python API Restoration (`db_python_provider`)

1. **PrimaryCoordinator construction:** Python creates the coordinator with:
   - `scheduler = ResourceStealingScheduler(resource_kind=Memory, ...)`
   - `estimator` returns `ResourceMap::from([(Memory, estimate)])`
   - Behavior identical to current memory-only scheduling.

2. **SecondaryWelcome:** Secondary reports
   `resources: vec![ResourceAmount { kind: Memory, amount: ram_bytes }]`.
   On the primary side, this is stored as a `ResourceMap`.

3. **TaskRequest:** Workers report
   `available_resources: vec![ResourceAmount { kind: Memory, amount: avail }]`.

4. **TaskComplete:** The Python provider wraps `warnings`/`filtered` into
   `result_data` and extracts them on the receiving end. Python callers see
   the same interface.

5. **WorkerReadyInfo:** Contains
   `resource_budgets: vec![ResourceAmount { kind: Memory, amount: budget }]`.

6. **OOM → Resource pressure:** Error type string changes to
   `"resource_exhausted:memory"`. Python callbacks updated accordingly.

7. **SLURM-primary:** Task selection based on memory fitting continues to work
   because the `ResourceMap` has exactly one entry (`Memory`).
