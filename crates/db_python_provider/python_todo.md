# Python TODO: Changes Required After Rust Generalization

## Wire Protocol (manager ↔ runner)

### `Response::Done` payload change

- Old: `done\n` or `done:<warnings>:<filtered>\n`
- New: `done\n` or `done:<opaque_bytes>\n`

Rust now treats everything after `done:` as opaque `Vec<u8>`. The old `done:3:7` format is still accepted — Rust calls `decode_legacy_result_data` to split on `:` and recover `(warnings, filtered)`.

**Action:** No immediate change needed. If richer result data is desired later, switch to a new encoding (e.g. JSON) in the `done:<payload>` slot.

### `ErrorType::OutOfMemory` wire format

- Rust still sends `oom` on the wire (`wire_value()` returns `"oom"`)
- Rust now accepts both `oom` and `resource_exhausted:memory` when parsing

**Action:** No change needed. Python can continue sending `error:oom:...`.

### `Command::ProcessBinary` → `Command::ProcessTask`

Rust-internal rename only. Wire format unchanged (still just `<path>\n`).

**Action:** None.

## Distributed Protocol (primary ↔ secondary JSON messages)

These only apply if Python constructs/parses distributed messages directly (e.g. a Python secondary talking to a Rust primary over WebSocket/QUIC).

### `SecondaryWelcome`

- Old: `"ram_bytes": 17179869184`
- New: `"resources": [{"kind": "Memory", "amount": 17179869184}]`

### `TaskRequest`

- Old: `"available_memory": 1073741824`
- New: `"available_resources": [{"kind": "Memory", "amount": 1073741824}]`

### `TaskComplete`

- Old: `"warnings": 3, "filtered": 7`
- New: `"result_data": null` (field has `#[serde(default)]`, so omitting it is valid)

### `WorkerReadyInfo`

- Old: `"memory_budget": 536870912`
- New: `"resource_budgets": [{"kind": "Memory", "amount": 536870912}]`

## No Python Changes Needed

- **Memory estimator:** `PyMemoryEstimatorBridge` still takes `slope`/`intercept` from Python and wraps internally into `ResourceMap`. Python estimator API unchanged.
- **Scheduler:** `ResourceStealingScheduler::memory()` is constructed internally. Python never references the scheduler type.
- **Config:** `max_memory` is still a Python-side `u64` field; wrapping into `ResourceMap` happens in Rust (`db_python_provider/src/lib.rs`).
