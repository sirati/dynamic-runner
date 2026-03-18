# db_transport_channel — Generalization Plan

## Role
In-process channel transport using `tokio::sync::mpsc`. Provides channel pairs
for both manager-runner communication (`ChannelManagerEnd`/`ChannelRunnerEnd`)
and primary-secondary communication (`ChannelSecondaryTransportEnd<I>` /
`ChannelPrimaryTransportEnd<I>`). Used for testing and single-machine deployments.

## What is Already Generic
- `ChannelManagerEnd`/`ChannelRunnerEnd` — implements `MessageSender`/`MessageReceiver`
  for `Command`/`Response` without any serialization.
- `ChannelSecondaryTransportEnd<I>`/`ChannelPrimaryTransportEnd<I>` — generic over
  identifier type `I`, implements `SecondaryTransport<I>` and `PrimaryTransport<I>`.
- No serialization, no resource awareness, no task-specific logic.

## What Needs to Change

**Nothing in this crate directly.** It passes messages by value through channels.
Any changes to message types (`Command`, `Response`, `DistributedMessage<I>`)
propagate automatically.

The only implicit dependency is on the message types themselves:
- When `Response::Done` loses `warnings`/`filtered`, this crate picks it up automatically.
- When `DistributedMessage` fields like `ram_bytes` become `resources: Vec<ResourceAmount>`,
  this crate picks it up automatically.

## Python API Impact
None. This is an internal transport used for testing and local-mode operation.
No Python-facing API changes needed.
