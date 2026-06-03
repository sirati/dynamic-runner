//! Per-connection reader/writer task pair for outgoing peer connections.
//!
//! The accept-side equivalent lives in `accept.rs` (it has slightly
//! different setup because the handshake also has to register the new
//! connection with the owning `PeerNetwork`); this module only handles
//! connections we initiated via `connect_to_peers`.

use dynrunner_core::Identifier;
use dynrunner_protocol_primary_secondary::{DistributedMessage, codec};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use super::util::PeerConnection;

/// Spawn reader/writer tasks for an outgoing connection. Returns the
/// `outgoing_tx` channel the caller pushes outbound messages into.
pub(super) fn spawn_outgoing_handler<I: Identifier>(
    peer_id: String,
    connection: PeerConnection,
    incoming_tx: mpsc::UnboundedSender<DistributedMessage<I>>,
) -> mpsc::UnboundedSender<DistributedMessage<I>> {
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<DistributedMessage<I>>();

    match connection {
        PeerConnection::Quic(conn) => {
            let (send_stream, recv_stream, existing_buf) = conn.into_parts();
            let reader_id = peer_id.clone();

            // Reader
            let mut reader = tokio::task::spawn_local(async move {
                let mut recv_buf = existing_buf;
                let mut recv = recv_stream;
                loop {
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
                        Err(_) => break,
                    }
                }
                tracing::debug!(peer = %reader_id, "peer QUIC reader done");
            });

            // Writer
            let writer_id = peer_id;
            let mut writer = tokio::task::spawn_local(async move {
                let mut send = send_stream;
                while let Some(msg) = outgoing_rx.recv().await {
                    match codec::serialize_message(&msg) {
                        Ok(frame) => {
                            if send.write_all(&frame).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                tracing::debug!(peer = %writer_id, "peer QUIC writer done");
            });

            tokio::task::spawn_local(async move {
                tokio::select! {
                    _ = &mut reader => { writer.abort(); }
                    _ = &mut writer => { reader.abort(); }
                }
            });
        }
        PeerConnection::Wss(conn) => {
            let (mut ws_write, mut ws_read) = conn.into_inner().split();
            let reader_id = peer_id.clone();

            // Reader
            let mut reader = tokio::task::spawn_local(async move {
                loop {
                    match ws_read.next().await {
                        Some(Ok(Message::Binary(data))) => match codec::decode_frame::<I>(&data) {
                            Ok(Some((msg, _))) => {
                                if incoming_tx.send(msg).is_err() {
                                    break;
                                }
                            }
                            Ok(None) => continue,
                            Err(_) => break,
                        },
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => continue,
                        Some(Err(_)) => break,
                    }
                }
                tracing::debug!(peer = %reader_id, "peer WSS reader done");
            });

            // Writer
            let writer_id = peer_id;
            let mut writer = tokio::task::spawn_local(async move {
                while let Some(msg) = outgoing_rx.recv().await {
                    match codec::serialize_message(&msg) {
                        Ok(frame) => {
                            if ws_write.send(Message::Binary(frame.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                tracing::debug!(peer = %writer_id, "peer WSS writer done");
            });

            tokio::task::spawn_local(async move {
                tokio::select! {
                    _ = &mut reader => { writer.abort(); }
                    _ = &mut writer => { reader.abort(); }
                }
            });
        }
    }

    outgoing_tx
}
