# db_gateway — Generalization Plan

## Role
Provides a `Gateway` trait abstracting local vs SSH command execution and file
transfer. Used by `db_slurm` and the distributed manager to run commands and
move files on remote nodes.

## What is Already Generic
- `Gateway` trait — completely domain-agnostic (connect, execute, transfer, etc.)
- `LocalGateway` / `SshGateway` — no task-specific logic at all.
- `GatewayConfig` / `SshConfig` — generic connection configuration.
- `CommandResult` — standard return code + stdout/stderr.

## What Needs to Change

**Nothing.** This crate is already fully generic. It has no knowledge of
resources, tasks, memory, tokenizers, or any domain concept. It is a pure
infrastructure abstraction.

## Notes
- The `Gateway` trait requires `Send + Sync`. This is fine for the gateway layer
  (SSH connections are inherently not single-threaded-local), but note this
  differs from the single-threaded tokio pattern used in the rest of the system.
  Not a problem since gateway usage is at the orchestration boundary.

## Python API Impact
None. The Python provider does not need to change anything related to this crate
for generalization purposes. If the Python API exposes gateway configuration, it
already works generically.
