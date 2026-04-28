# db_transport_quic — Generalization Plan

## Role
Network transport layer implementing QUIC (primary) with WebSocket Secure (WSS)
fallback. Provides `NetworkServer<I>` (primary-side multiplexer),
`NetworkClient` (secondary-side smart client), and `PeerNetwork<I>` (peer-to-peer
mesh). Implements `SecondaryTransport<I>`, `PrimaryTransport<I>`, and
`PeerTransport<I>`.

## What is Already Generic
- Fully generic over `I: Identifier` — transport is completely agnostic to message content.
- `MessageSender`/`MessageReceiver` for `DistributedMessage<I>`.
- Self-signed TLS certificates (`CertPair::generate`) — domain-agnostic.
- QUIC/WSS fallback logic — infrastructure only.
- Peer-to-peer broadcast — no task awareness.
- Serialization delegated to `db_primary_secondary_comm::codec`.

## What Needs to Change

**Nothing in this crate directly.** This crate is pure infrastructure — it
serializes/deserializes `DistributedMessage<I>` via the codec and routes messages
by `sender_id`. It has zero knowledge of resources, tasks, or domain concepts.

Any changes to `DistributedMessage<I>` fields (e.g. `ram_bytes` → `resources`)
are handled in `db_primary_secondary_comm` and propagate through the codec
automatically.

### Minor note
Test code uses `ram_bytes: 1024` in `DistributedMessage::SecondaryWelcome` —
these test values will need updating when the message type changes, but this is
trivial.

## Python API Impact
None. The Python provider configures QUIC/WSS connections via address/port/cert
parameters. The transport layer itself requires no changes for generalization.
