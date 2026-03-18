# db_comm_api_base — Generalization Plan

## Role
Foundation crate. Defines core types, error classification, and transport-agnostic
message traits (`MessageSender<M>`, `MessageReceiver<M>`). Every other workspace
crate depends on this.

## What is Already Generic
- `Identifier` trait — any task can plug its own identifier type.
- `BinaryInfo<I>` — generic over identifier.
- `FailedTask<I>` — generic over identifier.
- `MessageSender<M>` / `MessageReceiver<M>` — fully transport-agnostic.
- `ErrorType` — three-way classification (OOM, recoverable, non-recoverable).

## What Needs to Change

### 1. `TaskResult` has domain-specific fields
`warnings: u32` and `filtered: u32` are tokenizer metrics baked into the
base crate. Every consumer must carry these fields even when they are meaningless.

**Change:** Make `TaskResult` generic over a user-defined result payload:
```rust
pub struct TaskResult<R = ()> {
    pub success: bool,
    pub error_type: Option<ErrorType>,
    pub error_message: Option<String>,
    pub payload: R,  // task-specific result data
}
```
The `ok()` / `error()` constructors become:
```rust
impl<R: Default> TaskResult<R> {
    pub fn ok(payload: R) -> Self { ... }
    pub fn error(error_type: ErrorType, message: String) -> Self { ... }
}
```

### 2. `ErrorType::OutOfMemory` is memory-only
The enum models only memory as a scarce resource. For a general batch executor
that can steal/over-allocate CPU, GPU memory, disk, etc., the resource concept
must be extensible.

**Change:** Generalize the OOM variant:
```rust
pub enum ErrorType {
    ResourceExhausted(ResourceKind),  // was: OutOfMemory
    NonRecoverable,
    Recoverable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceKind {
    Memory,
    // future: Cpu, GpuMemory, Disk, ...
}
```
Wire format changes from `"oom"` to `"resource_exhausted:memory"`.

### 3. `MemoryBytes` type alias is too narrow
Only memory is modeled. A general resource system needs a resource quantity type.

**Change:** Keep `MemoryBytes` as a convenience alias but add a general resource
amount type:
```rust
pub type MemoryBytes = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceAmount {
    pub kind: ResourceKind,
    pub amount: u64,
}
```

### 4. `BinaryInfo` naming
"Binary" implies an executable. The struct is really "a unit of work with an
associated file." Consider renaming to `TaskInput<I>` or keeping `BinaryInfo`
but documenting it clearly.

**Suggested:** Keep the name but add a type alias `pub type TaskInput<I> = BinaryInfo<I>;`
so consumers can use the clearer name.

## Python API Restoration (`db_python_provider`)
After these changes, the Python provider must restore current behavior:

1. **Define `TokenizerTaskResult`:**
   ```rust
   #[derive(Default)]
   pub struct TokenizerTaskResult {
       pub warnings: u32,
       pub filtered: u32,
   }
   // Use TaskResult<TokenizerTaskResult> everywhere the old TaskResult was used.
   ```

2. **Expose `MemoryBytes`** as the default resource type in the Python API —
   users of the Python package see memory-based scheduling by default.
