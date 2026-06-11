//! Wire-frame size policy + framed-IO pumps for the QUIC and WSS legs.
//!
//! Single concern: how large ONE length-prefixed mesh frame (one
//! serialized [`DistributedMessage`]) may be on the wire, and what
//! happens — LOUDLY — when a frame violates that policy (#366).
//!
//! # Why this exists (#366)
//!
//! The WSS leg previously ran tokio-tungstenite with its DEFAULT limits
//! (16 MiB `max_frame_size`, 64 MiB `max_message_size`). tungstenite's
//! sender does NOT fragment and does NOT size-check on write — one
//! `Message::Binary` goes out as ONE WebSocket frame — so an oversize
//! message (the production 55 MB `TaskComplete`) sailed out of the
//! sender and errored the RECEIVER's frame reader
//! (`CapacityError::MessageTooLong`), whose error branch silently broke
//! the connection. The fire-and-forget outgoing push has no wire-level
//! replay, so the message vanished without a single log line.
//!
//! This module makes the limit explicit ([`MAX_WIRE_FRAME_BYTES`]),
//! coherent with the upstream payload caps (see the constant's doc),
//! and enforced symmetrically:
//!
//! * **Sender-side** ([`encode_outbound`] / [`check_outbound_len`]): an
//!   oversize frame is REJECTED before it touches the wire — ERROR log
//!   naming the peer, message type, size and limit — and the connection
//!   is KEPT (a per-message violation must not tear down a healthy
//!   link; for terminal-bearing reports the secondary's replay buffer
//!   keeps the loss visible until an operator intervenes).
//! * **Receiver-side** (the WSS `WebSocketConfig` from
//!   [`wire_ws_config`]; the announced-length guard in
//!   [`run_quic_reader`]): defense-in-depth against a non-conformant
//!   sender — the violating frame is rejected with an ERROR naming the
//!   peer + size + limit, and the connection tears down through the
//!   NORMAL disconnect path (the reader task exits, the owning
//!   supervisor fires its disconnect signal, prune+redial machinery
//!   runs — nothing about teardown is special-cased here).
//!
//! The reader/writer pump loops themselves also live here
//! ([`run_quic_reader`], [`run_wss_reader`], [`run_quic_writer`],
//! [`run_wss_writer`]) so every connection handler (peer mesh outgoing
//! and accepted, network client and accepted) consumes ONE
//! implementation of the framed-IO concern instead of each carrying
//! its own copy of the loop with its own silent error branches.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, InboundTap, codec};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// The WSS stream type every connection in this crate runs on.
pub(crate) type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Hard cap on one serialized mesh frame (96 MiB), applied to BOTH
/// transport legs in BOTH directions.
///
/// The payload-size chain this sits on top of (each layer deliberately
/// larger than the one below, so a frame that passes an inner gate can
/// NEVER be dropped by an outer one):
///
/// 1. **16 MiB** — [`dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES`]:
///    the API-level per-value cap on inline published outputs
///    (`Task.publish_string` rejects above this).
/// 2. **64 MiB** — `dynrunner_protocol_manager_worker::framing::
///    MAX_RESPONSE_FRAME_BYTES` (= 4 × the per-value cap): the
///    worker→manager IPC frame guard — the largest response frame the
///    manager accepts from a worker, covering multi-key accumulation
///    plus JSON-escaping overhead.
/// 3. **96 MiB** — this constant (= 1.5 × the IPC guard): a
///    `TaskComplete`/`TaskFailed` riding the mesh carries up to one
///    full IPC-guard-sized result plus the `DistributedMessage`
///    envelope and JSON re-encoding overhead. 50% headroom over the
///    IPC guard means a payload the manager accepted from a worker can
///    never be dropped at the mesh hop — the gap #364's arithmetic
///    left open (a near-16 MiB inline value + envelope overhead
///    already exceeded the old 16 MiB WSS frame default).
///
/// The 1↔3 ordering is pinned at compile time below; the full
/// cross-crate 1 < 2 ≤ 3 chain is pinned by the `wire_limit_ordering`
/// test in `dynrunner-manager-distributed` (this crate does not depend
/// on the worker-IPC protocol crate).
pub const MAX_WIRE_FRAME_BYTES: usize = 96 * 1024 * 1024;

// Ordering pin (layer 1 vs layer 3): an inline value the publish API
// accepted — even with the IPC guard's 4× multi-key/escaping allowance
// on top — must fit a mesh frame with room for the envelope.
const _: () = assert!(4 * dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES < MAX_WIRE_FRAME_BYTES);

/// The explicit tungstenite limits for every WSS endpoint (connect AND
/// accept sides — both constructed in `wss.rs`). One WebSocket message
/// carries exactly one mesh frame and tungstenite's sender never
/// fragments, so `max_frame_size` = `max_message_size` =
/// [`MAX_WIRE_FRAME_BYTES`].
pub(crate) fn wire_ws_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_WIRE_FRAME_BYTES))
        .max_frame_size(Some(MAX_WIRE_FRAME_BYTES))
}

/// Sender-side gate: is a serialized frame of `len` bytes allowed onto
/// the wire? `Err` carries the operator-facing reason (size + limit +
/// remediation hint).
pub(crate) fn check_outbound_len(len: usize) -> Result<(), String> {
    if len > MAX_WIRE_FRAME_BYTES {
        return Err(format!(
            "mesh frame of {len} bytes exceeds the wire limit of \
             {MAX_WIRE_FRAME_BYTES} bytes; inline published outputs are \
             capped at {} bytes per value (Task.publish_string) — write \
             bulk artifacts to the staging dir and use \
             Task.publish(src, key=...) instead",
            dynrunner_core::INLINE_VALUE_HARD_CAP_BYTES
        ));
    }
    Ok(())
}

/// Serialize an outbound message and enforce [`MAX_WIRE_FRAME_BYTES`].
///
/// `None` means the frame is UNSENDABLE — already logged at ERROR with
/// the peer, message type, task hash (when terminal-bearing) and the
/// violation — and the caller's writer loop must DROP it and keep the
/// connection: a deterministic per-message failure must not tear down
/// a healthy link (the pre-#366 oversize frame killed the receiver's
/// reader, and a terminal-ACK replay of the SAME frame would re-kill
/// every redialed link forever).
pub(crate) fn encode_outbound<I: Identifier>(
    msg: &DistributedMessage<I>,
    ctx: &'static str,
    peer: &str,
) -> Option<Vec<u8>> {
    let frame = match codec::serialize_message(msg) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::error!(
                ctx,
                peer,
                msg_type = ?msg.msg_type(),
                error,
                "dropping outbound mesh frame: serialization failed \
                 (connection kept; a terminal-bearing report stays in the \
                 sender's replay buffer, which escalates on repeated \
                 replay failure)"
            );
            return None;
        }
    };
    if let Err(error) = check_outbound_len(frame.len()) {
        tracing::error!(
            ctx,
            peer,
            msg_type = ?msg.msg_type(),
            task_hash = ?msg.task_hash(),
            frame_bytes = frame.len(),
            limit_bytes = MAX_WIRE_FRAME_BYTES,
            error,
            "dropping outbound mesh frame: exceeds the wire limit \
             (connection kept; a terminal-bearing report stays in the \
             sender's replay buffer, which escalates on repeated replay \
             failure)"
        );
        return None;
    }
    Some(frame)
}

/// Classify-and-log one WSS read error (#366): the LOUDNESS must track
/// the error class, not the mere fact of an errored read —
///
/// * `Capacity` (an over-limit frame/message, naming its size and the
///   configured limit) → ERROR: this is the class that used to vanish
///   a 55 MB `TaskComplete` silently; it means a peer actually LOST a
///   message.
/// * `Protocol(ResetWithoutClosingHandshake)` → DEBUG: an abrupt TCP
///   reset is ordinary peer-death/teardown churn — the membership
///   layer (prune/redial, failover narration) owns its operator-level
///   visibility, and logging ERROR here would shout on every normal
///   disconnect.
/// * anything else (other protocol violations, I/O errors mid-frame)
///   → WARN with the reason: unusual, worth a line, but not the
///   message-loss class.
pub(crate) fn log_wss_read_error(
    error: &tokio_tungstenite::tungstenite::Error,
    ctx: &'static str,
    peer: &str,
) {
    use tokio_tungstenite::tungstenite::error::{Error as WsError, ProtocolError};
    match error {
        WsError::Capacity(cap) => tracing::error!(
            ctx,
            peer,
            error = %cap,
            limit_bytes = MAX_WIRE_FRAME_BYTES,
            "WSS peer sent an over-limit frame; the message is LOST at \
             the sender and the connection tears down (normal \
             prune/redial machinery takes over)"
        ),
        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => tracing::debug!(
            ctx,
            peer,
            "WSS peer connection reset without closing handshake \
             (ordinary abrupt disconnect)"
        ),
        other => tracing::warn!(
            ctx,
            peer,
            error = %other,
            "WSS read failed; tearing down the connection"
        ),
    }
}

/// The length a buffered (possibly incomplete) length-prefixed frame
/// announces for itself, once the 4-byte prefix is available.
fn announced_frame_len(buf: &[u8]) -> Option<usize> {
    (buf.len() >= 4).then(|| u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize)
}

/// Receiver-side guard for the QUIC leg's accumulate-and-decode loop:
/// `Some(announced)` iff the next frame in `buf` announces a length
/// over [`MAX_WIRE_FRAME_BYTES`] — the caller must reject it BEFORE
/// accumulating the payload (quinn streams have no per-message cap of
/// their own; an unchecked prefix would buffer the whole
/// oversize/corrupt frame in memory).
pub(crate) fn oversize_announced_len(buf: &[u8]) -> Option<usize> {
    announced_frame_len(buf).filter(|&len| len > MAX_WIRE_FRAME_BYTES)
}

/// QUIC-leg frame reader pump: accumulate stream bytes, decode every
/// complete frame, forward to `incoming_tx`. Exits (→ the owning
/// handler's normal disconnect path) on stream end, channel close, an
/// over-limit announced frame length, or a corrupt frame — the latter
/// two at ERROR naming the peer and the violation (#366: no exit that
/// loses data is silent).
///
/// `incoming_tx` is the recording [`InboundTap`]: each push stamps the
/// decoded frame's sender on the transport's arrival-edge clock — this
/// read loop IS the earliest point a peer's frame is attributable on
/// this node, and it keeps running (recording honest arrivals) while
/// the inbound queue's consumer is starved. Bytes that have not yet
/// formed a complete frame are unattributable and stamp nothing.
///
/// `ctx` names the owning handler (e.g. `"peer-outgoing"`) so the done
/// line keeps the provenance the per-handler loops used to carry.
pub(crate) async fn run_quic_reader<I: Identifier>(
    mut recv: quinn::RecvStream,
    mut recv_buf: Vec<u8>,
    incoming_tx: InboundTap<I>,
    ctx: &'static str,
    peer: String,
) {
    loop {
        if let Some(announced) = oversize_announced_len(&recv_buf) {
            tracing::error!(
                ctx,
                peer = %peer,
                announced_bytes = announced,
                limit_bytes = MAX_WIRE_FRAME_BYTES,
                "QUIC peer announced a frame over the wire limit \
                 (oversize message or corrupt length prefix); tearing \
                 down the connection (normal prune/redial machinery \
                 takes over)"
            );
            break;
        }
        match codec::decode_frame::<I>(&recv_buf) {
            Ok(Some((msg, consumed))) => {
                recv_buf.drain(..consumed);
                if incoming_tx.send(msg).is_err() {
                    break;
                }
            }
            Ok(None) => {
                let mut tmp = [0u8; 8192];
                match recv.read(&mut tmp).await {
                    Ok(Some(n)) => recv_buf.extend_from_slice(&tmp[..n]),
                    _ => break,
                }
            }
            Err(error) => {
                tracing::error!(
                    ctx,
                    peer = %peer,
                    error,
                    "QUIC frame failed to decode (corrupt frame); tearing \
                     down the connection"
                );
                break;
            }
        }
    }
    tracing::debug!(ctx, peer = %peer, "QUIC reader done");
}

/// QUIC-leg writer pump: drain `outgoing_rx`, encode each message
/// through the [`encode_outbound`] gate (an unsendable frame is
/// dropped LOUDLY there and the connection kept), write to the stream.
/// Exits on channel close or a wire write error (peer gone — the
/// owning handler's normal disconnect path handles it).
pub(crate) async fn run_quic_writer<I: Identifier>(
    mut send: quinn::SendStream,
    mut outgoing_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ctx: &'static str,
    peer: String,
) {
    while let Some(msg) = outgoing_rx.recv().await {
        let Some(frame) = encode_outbound(&msg, ctx, &peer) else {
            continue;
        };
        if let Err(error) = send.write_all(&frame).await {
            tracing::debug!(ctx, peer = %peer, error = %error, "QUIC write failed");
            break;
        }
    }
    tracing::debug!(ctx, peer = %peer, "QUIC writer done");
}

/// WSS-leg frame reader pump: one WebSocket Binary message carries one
/// length-prefixed mesh frame; decode and forward to `incoming_tx`.
/// Exits (→ the owning handler's normal disconnect path) on Close /
/// stream end / channel close, a transport error — which is where
/// tungstenite surfaces an over-limit frame
/// (`CapacityError::MessageTooLong`, carrying the size and the
/// configured limit) — or a corrupt frame; both error exits at ERROR
/// naming the peer (#366: this branch used to be the silent drop point
/// of the production 55 MB `TaskComplete`).
///
/// `incoming_tx` is the recording [`InboundTap`] — see
/// [`run_quic_reader`]'s arrival-edge note; the same contract applies.
pub(crate) async fn run_wss_reader<I: Identifier>(
    mut ws_read: SplitStream<WsStream>,
    incoming_tx: InboundTap<I>,
    ctx: &'static str,
    peer: String,
) {
    loop {
        match ws_read.next().await {
            Some(Ok(Message::Binary(data))) => match codec::decode_frame::<I>(&data) {
                Ok(Some((msg, _))) => {
                    if incoming_tx.send(msg).is_err() {
                        break;
                    }
                }
                Ok(None) => continue,
                Err(error) => {
                    tracing::error!(
                        ctx,
                        peer = %peer,
                        error,
                        "WSS frame failed to decode (corrupt frame); \
                         tearing down the connection"
                    );
                    break;
                }
            },
            Some(Ok(Message::Close(_))) | None => break,
            Some(Ok(_)) => continue, // skip ping/pong/text
            Some(Err(error)) => {
                // ERROR for the message-loss (over-limit) class, DEBUG
                // for ordinary abrupt-disconnect churn — see the
                // classifier.
                log_wss_read_error(&error, ctx, &peer);
                break;
            }
        }
    }
    tracing::debug!(ctx, peer = %peer, "WSS reader done");
}

/// WSS-leg writer pump: drain `outgoing_rx`, encode each message
/// through the [`encode_outbound`] gate (an unsendable frame is
/// dropped LOUDLY there and the connection kept), send as one Binary
/// WebSocket message. Exits on channel close or a wire write error.
pub(crate) async fn run_wss_writer<I: Identifier>(
    mut ws_write: SplitSink<WsStream, Message>,
    mut outgoing_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ctx: &'static str,
    peer: String,
) {
    while let Some(msg) = outgoing_rx.recv().await {
        let Some(frame) = encode_outbound(&msg, ctx, &peer) else {
            continue;
        };
        if let Err(error) = ws_write.send(Message::Binary(frame.into())).await {
            tracing::debug!(ctx, peer = %peer, error = %error, "WSS write failed");
            break;
        }
    }
    tracing::debug!(ctx, peer = %peer, "WSS writer done");
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_protocol_primary_secondary::KeepaliveRole;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    fn keepalive() -> DistributedMessage<TestId> {
        DistributedMessage::Keepalive {
            target: None,
            sender_id: "framing-test".into(),
            timestamp: 0.0,
            secondary_id: "framing-test".into(),
            active_workers: 0,
            emitter_role: KeepaliveRole::Secondary,
        }
    }

    /// A TaskComplete whose result payload serializes to roughly
    /// `payload_bytes` on the wire (JSON array-of-numbers inflation is
    /// avoided by using a printable byte).
    fn task_complete(payload_bytes: usize) -> DistributedMessage<TestId> {
        DistributedMessage::TaskComplete {
            target: None,
            sender_id: "framing-test".into(),
            timestamp: 0.0,
            secondary_id: "framing-test".into(),
            worker_id: 0,
            task_hash: "deadbeef".into(),
            result_data: Some(vec![b'x'; payload_bytes]),
            delivery_seq: Some(1),
            // Stamped at the send_to_primary chokepoint (ordering gate).
            msgs_posted_through: None,
        }
    }

    /// Sender gate: a frame at the limit passes, one byte over fails
    /// with a reason naming both the size and the limit.
    #[test]
    fn check_outbound_len_boundary() {
        assert!(check_outbound_len(MAX_WIRE_FRAME_BYTES).is_ok());
        let err = check_outbound_len(MAX_WIRE_FRAME_BYTES + 1).unwrap_err();
        assert!(err.contains(&(MAX_WIRE_FRAME_BYTES + 1).to_string()));
        assert!(err.contains(&MAX_WIRE_FRAME_BYTES.to_string()));
        assert!(err.contains("publish"));
    }

    /// The sender-side encode gate: a normal message encodes; an
    /// oversize one is rejected (None — the writer pumps drop it and
    /// keep the connection).
    #[test]
    fn encode_outbound_gates_oversize() {
        assert!(encode_outbound(&keepalive(), "test", "peer-a").is_some());
        // result_data serializes as a JSON number array (~4 bytes per
        // element), so a quarter of the limit in raw bytes is already
        // comfortably over the wire limit — and well under it at a
        // hundredth.
        let oversize = task_complete(MAX_WIRE_FRAME_BYTES / 4 + MAX_WIRE_FRAME_BYTES / 8);
        assert!(encode_outbound(&oversize, "test", "peer-a").is_none());
    }

    /// The QUIC receiver-side announced-length guard: fires only once
    /// the 4-byte prefix is present AND announces over the limit.
    #[test]
    fn oversize_announced_len_guard() {
        // No prefix yet — no verdict.
        assert_eq!(oversize_announced_len(&[0x01, 0x02]), None);
        // In-limit announcement.
        let frame = codec::serialize_message(&keepalive()).unwrap();
        assert_eq!(oversize_announced_len(&frame), None);
        // Over-limit announcement (prefix only; payload need not exist —
        // that is the point: reject BEFORE accumulating).
        let announced = (MAX_WIRE_FRAME_BYTES as u32) + 1;
        let buf = announced.to_be_bytes().to_vec();
        assert_eq!(oversize_announced_len(&buf), Some(announced as usize));
    }
}
