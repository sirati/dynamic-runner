# db_slurm — Generalization Plan

## Role
Provides SLURM job submission, monitoring, and wrapper script generation.
Generic over `Gateway` trait. Handles Podman container setup, FIFO command
relay, and two connection modes (standard / reverse).

## What is Already Generic
- `SlurmConfig` — supports multiple resource types (CPU, memory, time, partition).
- `SlurmJobManager<G: Gateway>` — generic over gateway, no task-specific logic.
- `JobStatus` — standard SLURM state mapping.
- `WrapperScriptConfig` — accepts arbitrary image name/tag/load command.
- `ConnectionMode` — generic network topology abstraction.
- `submit_job`, `cancel_job`, `get_job_status` — all domain-agnostic.

## What Needs to Change

### 1. Hardcoded "asm" references in wrapper script
- Temp directory: `/tmp/asm-{suffix}` — uses "asm" prefix.
- Docker image copy: `LOCAL_IMAGE="$RNDTMP/asm-tokenizer-docker.tar"` — hardcoded name.
- Script header: `echo "SLURM Secondary Job Starting"` — fine, but the
  image filename is not.

**Change:** Use `cfg.image_name` instead of hardcoded strings:
```rust
let rndtmp = format!("/tmp/db-{rnd_suffix}");  // generic prefix
// ...
let local_image = format!("$RNDTMP/{}-docker.tar", cfg.image_name);
```

### 2. Container entrypoint is hardcoded to `dynamic_batch`
```bash
dynamic_batch --secondary tcp://... --secondary-id ... --secondary-quic-port ...
```
The container command assumes the `dynamic_batch` binary with specific CLI args.

**Change:** Make the container entrypoint and args configurable in
`WrapperScriptConfig`:
```rust
pub struct WrapperScriptConfig<'a> {
    ...
    pub container_command: &'a str,      // e.g. "dynamic_batch"
    pub container_args: Vec<String>,     // additional args beyond connection info
}
```
The connection-related args (`--secondary`, `--secondary-id`,
`--secondary-quic-port`) should still be auto-generated since they are
infrastructure concerns.

### 3. `SlurmConfig` resource fields are stringly-typed
`mem: Option<String>` and `cpus_per_task: Option<u32>` are fine for SLURM
passthrough, but there is no connection to the `ResourceKind` / `ResourceMap`
system being introduced in `db_scheduler_api`.

**Change (low priority):** Add a helper method that converts `SlurmConfig`
resources into a `ResourceMap` for scheduler integration:
```rust
impl SlurmConfig {
    pub fn to_resource_map(&self) -> ResourceMap {
        let mut map = ResourceMap::new();
        if let Some(mem) = &self.mem {
            map.insert(ResourceKind::Memory, parse_slurm_mem(mem));
        }
        // ... CPU, etc.
        map
    }
}
```

## Python API Restoration (`db_python_provider`)

1. **Wrapper script generation:** The Python provider passes `image_name` and
   `container_command` fields when calling `generate_wrapper_script`. For the
   current use case: `image_name = "asm-tokenizer"`,
   `container_command = "dynamic_batch"`. Behavior is identical.

2. **SlurmConfig:** No Python-facing changes needed. The config struct is
   already fully exposed. The `to_resource_map()` helper is an optional
   convenience that Python code can call if needed.

3. **Test updates:** Replace hardcoded "asm-tokenizer" in test assertions with
   the configurable image name.
