# db_transport_socket — Generalization Plan

## Role
Unix domain socket transport for manager-to-runner communication. Provides two
variants: `SocketpairManagerEnd`/`SocketpairRunnerEnd` (FD-inherited socketpair)
and `NamedSocketManagerEnd`/`NamedSocketRunnerEnd` (filesystem path sockets).
Implements `MessageSender`/`MessageReceiver` for `Command` and `Response`.

## What is Already Generic
- Transport is a thin pipe — no knowledge of task semantics.
- Uses `MessageSender<M>` / `MessageReceiver<M>` traits from `db_comm_api_base`.
- Wire format handled by `db_manager_runner_comm::codec` (not in this crate).
- No resource tracking, no memory awareness.

## What Needs to Change

**Almost nothing in this crate directly.** The transport just serializes/parses
`Command` and `Response` messages via the codec. Changes to those message types
(e.g. removing `warnings`/`filtered` from `Response::Done`) are handled upstream
in `db_manager_runner_comm`, and this crate automatically picks them up.

### 1. Naming consistency (low priority)
The CLI flag `--dynamic_queue <fd>` used by the socketpair transport to pass
the FD number is slightly coupled to the "dynamic batch" naming. If the system
is rebranded, this flag name should be configurable or generalized.

## Python API Impact
None. The Python provider uses this transport to spawn child worker processes.
The transport layer itself requires no changes for generalization — it is
already fully generic infrastructure.
