# db_runner_impl — Generalization Plan

## Role
Defines the runner's main loop (`runner_main_loop`) and the `TaskExecutor<S>`
trait that task-specific code implements. The runner receives commands, delegates
to the executor, and sends back responses. Transport-agnostic.

## What is Already Generic
- `TaskExecutor<S: MessageSender<Response>>` — pluggable execution logic.
- `runner_main_loop<E: RunnerEndpoint>` — works with any transport.
- Error handling via `TaskError { error_type, message }` — uses generic `ErrorType`.
- Phase updates and keepalives during execution — arbitrary strings.

## What Needs to Change

### 1. `TaskOutput` has hardcoded tokenizer metrics
```rust
pub struct TaskOutput {
    pub warnings: u32,
    pub filtered: u32,
}
```
This struct mirrors the `Response::Done` fields — both must change together.

**Change:** Make `TaskOutput` generic or opaque:
```rust
pub struct TaskOutput<R = ()> {
    pub result: R,
}
```
Or, if `Response::Done` becomes payload-free:
```rust
pub struct TaskOutput;  // success is the signal; metrics reported separately
```

### 2. `runner_main_loop` maps `TaskOutput` to `Response::Done`
```rust
Ok(output) => {
    endpoint.send(Response::Done {
        warnings: output.warnings,
        filtered: output.filtered,
    }).await;
}
```
This mapping must change to match the new `Response::Done` shape.

**Change:** If `Done` carries opaque bytes:
```rust
Ok(output) => {
    let data = serialize_result(&output.result);
    endpoint.send(Response::Done { result_data: data }).await;
}
```

### 3. `TaskExecutor` return type should be generic over result
```rust
pub trait TaskExecutor<S: MessageSender<Response>> {
    type Result: Default;  // or a serialization trait bound
    fn execute(
        &self,
        relative_path: &str,
        status_sender: &mut S,
    ) -> impl Future<Output = Result<TaskOutput<Self::Result>, TaskError>>;
}
```
This lets each task type define its own result data.

## Python API Restoration (`db_python_provider`)

1. **`TaskExecutor` impl:** The Python-side executor returns
   `TaskOutput<TokenizerResult>` where `TokenizerResult { warnings, filtered }`.
   This is passed through to `Response::Done` and ultimately to the manager.

2. **Runner main loop:** The generic `runner_main_loop` serializes the
   task-specific result into the `Done` response. The Python provider's
   concrete executor type determines the result shape.

3. **Wire format:** The Python worker subprocess is updated to use the new
   generic `Done` wire format. The codec in `db_python_provider` handles
   encoding/decoding of the `TokenizerResult` payload.
