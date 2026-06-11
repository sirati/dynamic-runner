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
//! * **Sender-side** ([`encode_outbound_frames`] / [`check_outbound_len`]):
//!   an oversize frame is either CHUNKED (a chunk-eligible framework
//!   frame — the `ClusterSnapshot` wire-cap fix; split into
//!   `FrameChunk`s under the cap, reassembled at the receiving pump) or
//!   REJECTED before it touches the wire — ERROR log naming the peer,
//!   message type, size and limit — and the connection is KEPT (a
//!   per-message violation must not tear down a healthy link; for
//!   terminal-bearing reports the secondary's replay buffer keeps the
//!   loss visible until an operator intervenes).
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
use dynrunner_protocol_primary_secondary::{DistributedMessage, chunking, codec};
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

/// Raw (pre-base64) slice budget for ONE `FrameChunk` of an oversized
/// chunk-eligible frame: cap/8 = 12 MiB, which lands the encoded chunk
/// frame at ~16 MiB on the wire (base64's fixed 4/3 plus the small JSON
/// envelope) — comfortably under [`MAX_WIRE_FRAME_BYTES`] with 6×
/// headroom, and small enough that a chunk never monopolises a leg the
/// way the unsplit 100 MB production snapshot frame would have.
pub(crate) const CHUNK_RAW_SLICE_BYTES: usize = MAX_WIRE_FRAME_BYTES / 8;

// Budget pin: a full raw slice base64-encodes to ceil(n/3)*4 bytes;
// with a generous fixed allowance for the FrameChunk JSON envelope
// (field names + numeric fields + quotes) the encoded chunk frame must
// stay under the wire cap, so a chunk frame can NEVER itself be
// cap-dropped (the failure mode this whole mechanism exists to kill).
const _: () = assert!(CHUNK_RAW_SLICE_BYTES.div_ceil(3) * 4 + 4096 < MAX_WIRE_FRAME_BYTES);

/// Hard cap on one REASSEMBLED chunked frame (16 × the wire cap =
/// 1.5 GiB): the memory bound a corrupt/malicious `total` cannot
/// exceed at a receiver. The production 67k-task snapshot was ~116 MB;
/// 16× headroom covers an order-of-magnitude ledger growth while still
/// bounding a rogue transfer.
pub(crate) const MAX_REASSEMBLED_FRAME_BYTES: usize = 16 * MAX_WIRE_FRAME_BYTES;

/// One-shot INFO latch for the first time chunking engages in this
/// process (the silent-branch rule: a new mechanism's first activation
/// is operator-visible exactly once; per-transfer detail stays DEBUG).
static CHUNKING_ENGAGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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

/// Serialize an outbound message and enforce [`MAX_WIRE_FRAME_BYTES`],
/// CHUNKING an oversized chunk-eligible frame instead of dropping it.
///
/// Returns the wire frames to write IN ORDER on the same leg:
///
/// * one frame — the common case (within the cap);
/// * N `FrameChunk` frames — the frame exceeded the cap AND
///   [`DistributedMessage::chunk_eligible`] allows splitting it (the
///   `ClusterSnapshot` wire-cap fix; each chunk frame is under the cap
///   by the `CHUNK_RAW_SLICE_BYTES` budget pin). The first engagement
///   per process logs INFO; every chunked send logs the size + chunk
///   count at DEBUG (the measuring line);
/// * empty — the frame is UNSENDABLE: serialization failed, or it
///   exceeded the cap and is NOT chunk-eligible (#364/#366: the cap on
///   consumer payloads is a contract). Already logged at ERROR with the
///   peer, message type, task hash (when terminal-bearing) and the
///   violation; the caller's writer loop must DROP it and keep the
///   connection — a deterministic per-message failure must not tear
///   down a healthy link (the pre-#366 oversize frame killed the
///   receiver's reader, and a terminal-ACK replay of the SAME frame
///   would re-kill every redialed link forever).
pub(crate) fn encode_outbound_frames<I: Identifier>(
    msg: &DistributedMessage<I>,
    ctx: &'static str,
    peer: &str,
) -> Vec<Vec<u8>> {
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
            return Vec::new();
        }
    };
    if frame.len() <= MAX_WIRE_FRAME_BYTES {
        return vec![frame];
    }
    if msg.chunk_eligible() {
        // The receiver's reassembly cap is the upper bound a transfer
        // may reach; refuse upfront (loudly) instead of shipping a
        // transfer every receiver will reject.
        if frame.len() - 4 > MAX_REASSEMBLED_FRAME_BYTES {
            tracing::error!(
                ctx,
                peer,
                msg_type = ?msg.msg_type(),
                frame_bytes = frame.len(),
                limit_bytes = MAX_REASSEMBLED_FRAME_BYTES,
                "dropping outbound mesh frame: exceeds even the chunked-transfer \
                 reassembly cap (connection kept)"
            );
            return Vec::new();
        }
        if !CHUNKING_ENGAGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::info!(
                ctx,
                peer,
                msg_type = ?msg.msg_type(),
                frame_bytes = frame.len(),
                limit_bytes = MAX_WIRE_FRAME_BYTES,
                "chunked transfer engaged for the first time: an oversized \
                 chunk-eligible frame is split into FrameChunks under the \
                 wire limit (further engagements log at DEBUG)"
            );
        }
        // Split the frame's JSON body (the 4-byte length prefix is
        // per-leg framing, re-added on each chunk frame's own encode).
        let chunks = chunking::split_frame(msg, &frame[4..], CHUNK_RAW_SLICE_BYTES);
        drop(frame);
        let chunk_count = chunks.len();
        let mut frames = Vec::with_capacity(chunk_count);
        let mut total_wire_bytes = 0usize;
        for chunk in &chunks {
            match codec::serialize_message(chunk) {
                Ok(chunk_frame) => {
                    // Pinned at compile time via CHUNK_RAW_SLICE_BYTES;
                    // belt-and-braces in debug builds.
                    debug_assert!(chunk_frame.len() <= MAX_WIRE_FRAME_BYTES);
                    total_wire_bytes += chunk_frame.len();
                    frames.push(chunk_frame);
                }
                Err(error) => {
                    // A chunk that cannot serialize makes the WHOLE
                    // transfer unsendable — a partial transfer would be
                    // abandoned at the receiver anyway; drop it all,
                    // loudly, before anything touches the wire.
                    tracing::error!(
                        ctx,
                        peer,
                        msg_type = ?msg.msg_type(),
                        error,
                        "dropping outbound mesh frame: chunk serialization \
                         failed (connection kept)"
                    );
                    return Vec::new();
                }
            }
        }
        tracing::debug!(
            ctx,
            peer,
            msg_type = ?msg.msg_type(),
            payload_bytes = total_wire_bytes,
            chunks = chunk_count,
            chunk_raw_slice_bytes = CHUNK_RAW_SLICE_BYTES,
            "oversized frame sent as a chunked transfer"
        );
        return frames;
    }
    tracing::error!(
        ctx,
        peer,
        msg_type = ?msg.msg_type(),
        task_hash = ?msg.task_hash(),
        frame_bytes = frame.len(),
        limit_bytes = MAX_WIRE_FRAME_BYTES,
        error = check_outbound_len(frame.len()).unwrap_err(),
        "dropping outbound mesh frame: exceeds the wire limit \
         (connection kept; a terminal-bearing report stays in the \
         sender's replay buffer, which escalates on repeated replay \
         failure)"
    );
    Vec::new()
}

/// Construct one connection's chunk reassembler (the policy-configured
/// [`chunking::ChunkReassembler`]). Created ONCE per connection by the
/// connection handler — BEFORE any identification read — and handed to
/// the reader pump, so a transfer whose first chunk arrives as the
/// connection's identification frame reassembles seamlessly across the
/// identify→pump boundary.
pub(crate) fn new_reassembler() -> chunking::ChunkReassembler {
    chunking::ChunkReassembler::new(MAX_REASSEMBLED_FRAME_BYTES)
}

/// One decoded inbound wire message resolved against the connection's
/// chunk reassembler: either a whole logical message to deliver, a
/// consumed bookkeeping step (a chunk buffered / a transfer-level fault
/// already logged), or a connection-fatal violation.
///
/// `large_enum_variant` is suppressed for the same reason as on
/// `DistributedMessage` itself: the value is a TRANSIENT return — built
/// and destructured within one pump iteration, never stored or queued —
/// so boxing `Deliver` would push every delivered frame through an
/// allocation for zero retention benefit.
#[allow(clippy::large_enum_variant)]
pub(crate) enum InboundStep<I> {
    /// Deliver this message to the connection's `incoming_tx`.
    Deliver(DistributedMessage<I>),
    /// Consumed inside the framing layer (chunk buffered, duplicate
    /// ignored, or a transfer abandoned/rejected — already logged).
    Consumed,
    /// Protocol violation that must tear the connection down through
    /// the reader's normal exit (already logged at ERROR): reassembled
    /// bytes that don't decode, or a reassembled frame whose type is
    /// NOT chunk-eligible (a sender smuggling a consumer payload past
    /// the cap — the #366 receiver-side defense, extended to chunks).
    Fatal,
}

/// Route one decoded inbound message through the chunk-reassembly seam
/// (shared by the QUIC and WSS reader pumps — ONE implementation of the
/// receive-side chunking concern).
///
/// Non-chunk messages pass straight through. `FrameChunk`s are fed to
/// the per-connection [`chunking::ChunkReassembler`]; a completed
/// transfer decodes back into the original logical frame and is
/// delivered as if it had arrived unsplit. Transfer-level faults
/// (abandoned partials, rejected chunks) are LOUD-but-connection-kept:
/// WARN here, and the transfer's higher-level trigger (the anti-entropy
/// digest cadence / a re-issued bootstrap pull) is the bounded retry —
/// never a silent partial.
pub(crate) fn resolve_inbound<I: Identifier>(
    msg: DistributedMessage<I>,
    reassembler: &mut chunking::ChunkReassembler,
    ctx: &'static str,
    peer: &str,
) -> InboundStep<I> {
    let DistributedMessage::FrameChunk {
        transfer_id,
        index,
        total,
        checksum,
        payload_b64,
        ..
    } = msg
    else {
        return InboundStep::Deliver(msg);
    };
    let ingest = reassembler.ingest(transfer_id, index, total, checksum, &payload_b64);
    if let Some(abandoned) = &ingest.abandoned {
        tracing::warn!(
            ctx,
            peer,
            transfer_id = abandoned.transfer_id,
            buffered_bytes = abandoned.buffered_bytes,
            chunks_received = abandoned.chunks_received,
            reason = %abandoned.reason,
            "abandoned a partial chunked transfer (the originating pull's \
             cadence re-requests it; nothing partial is delivered)"
        );
    }
    match ingest.outcome {
        chunking::ChunkOutcome::Incomplete => InboundStep::Consumed,
        chunking::ChunkOutcome::Rejected { reason } => {
            tracing::warn!(
                ctx,
                peer,
                transfer_id,
                chunk_index = index,
                chunk_total = total,
                reason = %reason,
                "rejected an inbound FrameChunk (transfer not deliverable; \
                 the originating pull's cadence re-requests it)"
            );
            InboundStep::Consumed
        }
        chunking::ChunkOutcome::Complete(bytes) => {
            let reassembled_bytes = bytes.len();
            let inner: DistributedMessage<I> = match codec::deserialize_message(&bytes) {
                Ok(inner) => inner,
                Err(error) => {
                    tracing::error!(
                        ctx,
                        peer,
                        transfer_id,
                        reassembled_bytes,
                        error,
                        "reassembled chunked frame failed to decode (corrupt \
                         or version-mismatched sender); tearing down the \
                         connection"
                    );
                    return InboundStep::Fatal;
                }
            };
            if !inner.chunk_eligible() {
                tracing::error!(
                    ctx,
                    peer,
                    transfer_id,
                    msg_type = ?inner.msg_type(),
                    reassembled_bytes,
                    limit_bytes = MAX_WIRE_FRAME_BYTES,
                    "peer smuggled a non-chunk-eligible frame through a \
                     chunked transfer (wire-cap bypass); tearing down the \
                     connection"
                );
                return InboundStep::Fatal;
            }
            tracing::debug!(
                ctx,
                peer,
                transfer_id,
                reassembled_bytes,
                chunks = total,
                msg_type = ?inner.msg_type(),
                "chunked transfer reassembled"
            );
            InboundStep::Deliver(inner)
        }
    }
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
/// `ctx` names the owning handler (e.g. `"peer-outgoing"`) so the done
/// line keeps the provenance the per-handler loops used to carry.
pub(crate) async fn run_quic_reader<I: Identifier>(
    mut recv: quinn::RecvStream,
    mut recv_buf: Vec<u8>,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    ctx: &'static str,
    peer: String,
    // Per-connection chunk reassembly (a chunked transfer travels ONE
    // ordered leg, so the state is connection-local; tearing the
    // connection down discards any partial — the pull's cadence
    // re-requests). Passed IN rather than constructed here because an
    // accept-side handler's identification read may already have fed it
    // the transfer's first chunk(s) — see `new_reassembler`'s doc.
    mut reassembler: chunking::ChunkReassembler,
) {
    'pump: loop {
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
                match resolve_inbound(msg, &mut reassembler, ctx, &peer) {
                    InboundStep::Deliver(msg) => {
                        if incoming_tx.send(msg).is_err() {
                            break 'pump;
                        }
                    }
                    InboundStep::Consumed => {}
                    InboundStep::Fatal => break 'pump,
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
/// through the [`encode_outbound_frames`] gate (an unsendable frame is
/// dropped LOUDLY there and the connection kept; an oversized
/// chunk-eligible one becomes N chunk frames written back-to-back),
/// write to the stream. Exits on channel close or a wire write error
/// (peer gone — the owning handler's normal disconnect path handles
/// it).
pub(crate) async fn run_quic_writer<I: Identifier>(
    mut send: quinn::SendStream,
    mut outgoing_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ctx: &'static str,
    peer: String,
) {
    'pump: while let Some(msg) = outgoing_rx.recv().await {
        for frame in encode_outbound_frames(&msg, ctx, &peer) {
            if let Err(error) = send.write_all(&frame).await {
                tracing::debug!(ctx, peer = %peer, error = %error, "QUIC write failed");
                break 'pump;
            }
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
pub(crate) async fn run_wss_reader<I: Identifier>(
    mut ws_read: SplitStream<WsStream>,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
    ctx: &'static str,
    peer: String,
    // Per-connection chunk reassembly — see `run_quic_reader`.
    mut reassembler: chunking::ChunkReassembler,
) {
    loop {
        match ws_read.next().await {
            Some(Ok(Message::Binary(data))) => match codec::decode_frame::<I>(&data) {
                Ok(Some((msg, _))) => match resolve_inbound(msg, &mut reassembler, ctx, &peer) {
                    InboundStep::Deliver(msg) => {
                        if incoming_tx.send(msg).is_err() {
                            break;
                        }
                    }
                    InboundStep::Consumed => continue,
                    InboundStep::Fatal => break,
                },
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
/// through the [`encode_outbound_frames`] gate (an unsendable frame is
/// dropped LOUDLY there and the connection kept; an oversized
/// chunk-eligible one becomes N chunk frames), send each as one Binary
/// WebSocket message. Exits on channel close or a wire write error.
pub(crate) async fn run_wss_writer<I: Identifier>(
    mut ws_write: SplitSink<WsStream, Message>,
    mut outgoing_rx: mpsc::UnboundedReceiver<DistributedMessage<I>>,
    ctx: &'static str,
    peer: String,
) {
    'pump: while let Some(msg) = outgoing_rx.recv().await {
        for frame in encode_outbound_frames(&msg, ctx, &peer) {
            if let Err(error) = ws_write.send(Message::Binary(frame.into())).await {
                tracing::debug!(ctx, peer = %peer, error = %error, "WSS write failed");
                break 'pump;
            }
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

    /// The sender-side encode gate: a normal message encodes to ONE
    /// frame; an oversize NON-eligible one is rejected (empty — the
    /// writer pumps drop it and keep the connection: the #364/#366 cap
    /// on consumer payloads is NOT relaxed by the chunking mechanism).
    #[test]
    fn encode_outbound_gates_oversize() {
        assert_eq!(encode_outbound_frames(&keepalive(), "test", "peer-a").len(), 1);
        // result_data serializes as a JSON number array (~4 bytes per
        // element), so a quarter of the limit in raw bytes is already
        // comfortably over the wire limit — and well under it at a
        // hundredth.
        let oversize = task_complete(MAX_WIRE_FRAME_BYTES / 4 + MAX_WIRE_FRAME_BYTES / 8);
        assert!(encode_outbound_frames(&oversize, "test", "peer-a").is_empty());
    }

    /// An oversized ClusterSnapshot (the chunk-eligible class).
    fn oversize_snapshot() -> DistributedMessage<TestId> {
        DistributedMessage::ClusterSnapshot {
            target: None,
            sender_id: "framing-test".into(),
            timestamp: 0.0,
            snapshot_json: "s".repeat(MAX_WIRE_FRAME_BYTES + 1024),
        }
    }

    /// The chunk path: an oversized ELIGIBLE frame becomes N frames,
    /// each under the cap, that reassemble through `resolve_inbound`
    /// into the byte-identical original message.
    #[test]
    fn encode_outbound_chunks_eligible_oversize_and_reassembles() {
        let msg = oversize_snapshot();
        let frames = encode_outbound_frames(&msg, "test", "peer-a");
        assert!(frames.len() > 1, "an oversize eligible frame must chunk");
        for frame in &frames {
            assert!(frame.len() <= MAX_WIRE_FRAME_BYTES);
        }
        let mut reassembler = chunking::ChunkReassembler::new(MAX_REASSEMBLED_FRAME_BYTES);
        let mut delivered = None;
        for frame in &frames {
            let (decoded, consumed) = codec::decode_frame::<TestId>(frame).unwrap().unwrap();
            assert_eq!(consumed, frame.len());
            assert_eq!(
                decoded.msg_type(),
                dynrunner_protocol_primary_secondary::MessageType::FrameChunk
            );
            match resolve_inbound(decoded, &mut reassembler, "test", "peer-a") {
                InboundStep::Deliver(inner) => {
                    assert!(delivered.is_none(), "exactly one delivery");
                    delivered = Some(inner);
                }
                InboundStep::Consumed => {}
                InboundStep::Fatal => panic!("reassembly must not be fatal"),
            }
        }
        match delivered.expect("transfer must complete") {
            DistributedMessage::ClusterSnapshot {
                sender_id,
                snapshot_json,
                ..
            } => {
                assert_eq!(sender_id, "framing-test");
                assert_eq!(snapshot_json.len(), MAX_WIRE_FRAME_BYTES + 1024);
                assert!(snapshot_json.bytes().all(|b| b == b's'));
            }
            other => panic!("expected ClusterSnapshot, got {:?}", other.msg_type()),
        }
    }

    /// Receiver-side cap-bypass defense: a reassembled frame whose type
    /// is NOT chunk-eligible (a smuggled consumer payload) is FATAL —
    /// the connection tears down loudly instead of delivering it.
    #[test]
    fn reassembled_ineligible_frame_is_fatal() {
        // Hand-build a chunked transfer of a small (in-cap) TaskComplete
        // — only a non-conformant sender would ever do this, since the
        // legit sender's gate never chunks an ineligible type.
        let smuggled = task_complete(64);
        let frame = codec::serialize_message(&smuggled).unwrap();
        let chunks = chunking::split_frame(&smuggled, &frame[4..], 16);
        let mut reassembler = chunking::ChunkReassembler::new(MAX_REASSEMBLED_FRAME_BYTES);
        let mut fatal = false;
        for chunk in &chunks {
            match resolve_inbound(chunk.clone(), &mut reassembler, "test", "peer-a") {
                InboundStep::Consumed => {}
                InboundStep::Fatal => {
                    fatal = true;
                    break;
                }
                InboundStep::Deliver(inner) => {
                    panic!("smuggled {:?} must not be delivered", inner.msg_type())
                }
            }
        }
        assert!(fatal, "the smuggled transfer must be connection-fatal");
    }

    /// Non-chunk frames pass through `resolve_inbound` untouched.
    #[test]
    fn resolve_inbound_passes_normal_frames() {
        let mut reassembler = chunking::ChunkReassembler::new(MAX_REASSEMBLED_FRAME_BYTES);
        match resolve_inbound(keepalive(), &mut reassembler, "test", "peer-a") {
            InboundStep::Deliver(msg) => assert_eq!(
                msg.msg_type(),
                dynrunner_protocol_primary_secondary::MessageType::Keepalive
            ),
            _ => panic!("normal frames must pass through"),
        }
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
