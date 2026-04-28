# db_manager_runner_comm — Generalization Plan

## Role
Defines the manager-to-runner protocol: `Command` / `Response` message types,
wire codec, and a ZST state machine (`RunnerProtocol<State, M>`) that enforces
correct protocol transitions at compile time.

## What is Already Generic
- `Command::ProcessBinary { relative_path }` — just a path string, no task metadata.
- `Response::PhaseUpdate { phase_name }` — arbitrary string, extensible.
- `Response::Error { error_type, message }` — uses generic `ErrorType`.
- `Response::PickledError` — Python interop, domain-agnostic.
- `Response::Keepalive` — heartbeat, generic.
- State machine (`RunnerProtocol`) — enforces protocol transitions, not task semantics.
- `ManagerEndpoint` / `RunnerEndpoint` composite traits.

## What Needs to Change

### 1. `Response::Done` has hardcoded tokenizer metrics
```rust
Done { warnings: u32, filtered: u32 }
```
These fields are tokenizer-specific counters. A generic batch executor should
not mandate specific result counters at the protocol level.

**Change:** Make `Done` carry a generic result payload, or no payload (result
data travels separately):
```rust
Done { result_data: Option<Vec<u8>> }  // opaque serialized bytes
```
Or, matching the `TaskResult<R>` change from `db_comm_api_base`:
```rust
Done  // success is implicit; task-specific data reported via a separate channel
```
The simplest approach: keep `Done` payload-free, and let the runner report
task-specific metrics through the existing `PhaseUpdate` mechanism or a new
`TaskMetrics` response variant.

### 2. Wire format `done:<warnings>:<filtered>` is baked in
The codec (`codec.rs`) parses `done\n` and `done:<w>:<f>\n`. This must be
updated to match whatever replaces the hardcoded fields.

**Change:** If `Done` becomes payload-free, wire format is just `done\n`
(already supported as a fallback). If it carries opaque bytes, use e.g.
`done:<base64_payload>\n`.

### 3. `Command::ProcessBinary` naming
"Binary" suggests a compiled executable. The command sends a relative path to
any kind of input file.

**Suggested:** Rename to `ProcessTask` or `ProcessInput`. Low priority — naming
only, no semantic issue.

## Python API Restoration (`db_python_provider`)

1. **Runner response handling:** The Python-side runner currently expects to
   receive `warnings` and `filtered` from `Done`. After generalization:
   - Define a `TokenizerDonePayload` in `db_python_provider` that carries
     `warnings: u32, filtered: u32`.
   - Either encode it as opaque bytes in `Done { result_data }`, or use a
     `PhaseUpdate`-like mechanism.
   - The Python wrapper decodes the payload and exposes `warnings`/`filtered`
     to Python callers exactly as before.

2. **Wire format:** The codec in `db_python_provider` encodes/decodes the
   `TokenizerDonePayload` in the new generic `Done` wire format. The Python
   worker subprocess is updated to match.
