# db_primary_secondary_comm — Generalization Plan

## Role
Defines the distributed primary-to-secondary protocol: 20+ message types in
`DistributedMessage<I>`, wire codec (length-prefixed JSON), and transport traits
(`SecondaryTransport`, `PrimaryTransport`, `PeerTransport`). Handles the full
lifecycle from welcome → cert exchange → task distribution → completion/failover.

## What is Already Generic
- `DistributedMessage<I>` — generic over identifier type `I`.
- `DistributedBinaryInfo<I>`, `TaskInfo<I>`, `ZipFileAssignment<I>` — all generic.
- `#[serde(flatten)]` on identifier for wire format.
- Peer-to-peer discovery, timeout detection, promotion voting — all domain-agnostic.
- `ExecuteCommand` / `CommandResult` — generic remote command execution.
- Transport traits — fully abstract.

## What Needs to Change

### 1. `SecondaryWelcome` reports only `ram_bytes`
```rust
SecondaryWelcome { ..., ram_bytes: u64, worker_count: u32, hostname: String }
```
Only memory is announced. A general resource system needs the secondary to
report all available resources.

**Change:**
```rust
SecondaryWelcome {
    ...,
    resources: Vec<ResourceAmount>,  // from db_comm_api_base
    worker_count: u32,
    hostname: String,
}
```
Where `ResourceAmount { kind: ResourceKind, amount: u64 }` supports memory,
CPU, GPU, etc.

### 2. `WorkerReadyInfo` has only `memory_budget`
```rust
pub struct WorkerReadyInfo {
    pub worker_id: u32,
    pub memory_budget: u64,
}
```

**Change:**
```rust
pub struct WorkerReadyInfo {
    pub worker_id: u32,
    pub resource_budgets: Vec<ResourceAmount>,
}
```

### 3. `TaskRequest` has only `available_memory`
```rust
TaskRequest { ..., available_memory: u64 }
```

**Change:**
```rust
TaskRequest { ..., available_resources: Vec<ResourceAmount> }
```

### 4. `TaskComplete` has hardcoded `warnings` / `filtered`
```rust
TaskComplete { ..., warnings: u32, filtered: u32 }
```
These are tokenizer-specific result metrics.

**Change:** Replace with an opaque or generic result payload:
```rust
TaskComplete {
    ...,
    task_hash: String,
    result_data: serde_json::Value,  // or HashMap<String, serde_json::Value>
}
```
This lets any task type report its own result metrics.

## Python API Restoration (`db_python_provider`)

1. **Resource announcement:** When constructing `SecondaryWelcome`, the Python
   provider fills `resources: vec![ResourceAmount { kind: Memory, amount: ram }]`
   — same behavior as before but now extensible.

2. **Worker budgets:** `WorkerReadyInfo` gets populated with
   `resource_budgets: vec![ResourceAmount { kind: Memory, amount: budget }]`.

3. **Task requests:** Python workers report `available_resources` with a single
   memory entry.

4. **Task completion:** The Python provider wraps `warnings` and `filtered` into
   `result_data: json!({"warnings": w, "filtered": f})`, and the Python-side
   callback destructures it back.
