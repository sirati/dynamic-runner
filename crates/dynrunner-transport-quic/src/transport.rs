use std::net::SocketAddr;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{DistributedMessage, codec};
use quinn::{Endpoint, RecvStream, SendStream};
use rustls::pki_types::CertificateDer;

use crate::certs::CertPair;

/// A QUIC connection that can send/receive distributed messages.
pub struct QuicConnection {
    send: SendStream,
    recv: RecvStream,
    recv_buf: Vec<u8>,
}

/// Bound on one inbound connection's QUIC handshake + first-stream
/// open. Applied inside [`QuicConnection::from_incoming`] so a dialer
/// that completes the QUIC handshake but never opens its bi stream
/// releases the per-connection handler task instead of holding it for
/// the full idle timeout. Generous: a conformant dialer opens its
/// stream immediately after the handshake.
const INBOUND_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl QuicConnection {
    pub fn from_streams(send: SendStream, recv: RecvStream) -> Self {
        Self {
            send,
            recv,
            recv_buf: Vec::new(),
        }
    }

    /// Drive ONE incoming connection attempt to an established
    /// [`QuicConnection`]: complete the QUIC/TLS handshake, then accept
    /// the dialer's bi-directional stream. PER-CONNECTION fallible —
    /// a dialer that aborts its handshake (or never opens its stream)
    /// errors HERE, in whatever task drove this attempt, and says
    /// nothing about the listener. Accept loops must therefore call
    /// this from the spawned per-connection handler, never inline in
    /// the accept loop itself: pre-fix the loops awaited the handshake
    /// inline and treated its failure as loop-fatal, so one aborted
    /// in-flight handshake (the run_20260611_202345 simultaneous
    /// connection reset) permanently killed the node's ability to
    /// accept re-dialed sessions.
    pub async fn from_incoming(incoming: quinn::Incoming) -> Result<Self, String> {
        let established = tokio::time::timeout(INBOUND_HANDSHAKE_TIMEOUT, async {
            let connection = incoming.await.map_err(|e| e.to_string())?;
            connection.accept_bi().await.map_err(|e| e.to_string())
        })
        .await
        .map_err(|_| {
            format!(
                "inbound QUIC connection did not establish within {}s",
                INBOUND_HANDSHAKE_TIMEOUT.as_secs()
            )
        })?;
        let (send, recv) = established?;
        Ok(Self::from_streams(send, recv))
    }

    /// Consume the connection and return the underlying QUIC streams
    /// along with any buffered data that was read but not yet consumed.
    pub fn into_parts(self) -> (SendStream, RecvStream, Vec<u8>) {
        (self.send, self.recv, self.recv_buf)
    }

    /// Gracefully close the send side.
    pub async fn finish_send(&mut self) -> Result<(), String> {
        self.send.finish().map_err(|e| e.to_string())?;
        // Wait for the peer to receive all data.
        self.send.stopped().await.ok();
        Ok(())
    }
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for QuicConnection {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        let frame = codec::serialize_message(&msg)?;
        // Sender-side wire-limit gate (#366): coherent with the WSS leg
        // — an oversize frame is rejected loudly here instead of being
        // rejected (or worse, buffered unboundedly) by the receiver.
        crate::framing::check_outbound_len(frame.len())?;
        self.send
            .write_all(&frame)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for QuicConnection {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        loop {
            // Receiver-side wire-limit guard (#366): quinn streams have
            // no per-message cap of their own, so reject an over-limit
            // announced frame BEFORE accumulating its payload.
            if let Some(announced) = crate::framing::oversize_announced_len(&self.recv_buf) {
                tracing::error!(
                    announced_bytes = announced,
                    limit_bytes = crate::framing::MAX_WIRE_FRAME_BYTES,
                    "QUIC peer announced a frame over the wire limit \
                     (oversize message or corrupt length prefix); \
                     dropping the connection"
                );
                return None;
            }
            match codec::decode_frame(&self.recv_buf) {
                Ok(Some((msg, consumed))) => {
                    self.recv_buf.drain(..consumed);
                    return Some(msg);
                }
                Ok(None) => {
                    let mut tmp = [0u8; 8192];
                    match self.recv.read(&mut tmp).await {
                        Ok(Some(n)) => {
                            self.recv_buf.extend_from_slice(&tmp[..n]);
                        }
                        Ok(None) => return None,
                        Err(_) => return None,
                    }
                }
                Err(error) => {
                    tracing::error!(
                        error,
                        "QUIC frame failed to decode (corrupt frame); \
                         dropping the connection"
                    );
                    return None;
                }
            }
        }
    }
}

/// A QUIC listener that accepts incoming connections.
pub struct QuicListener {
    endpoint: Endpoint,
    local_addr: SocketAddr,
}

impl QuicListener {
    /// Bind a QUIC server using the given cert pair on an OS-allocated
    /// port. Convenience wrapper around `bind_addr` that uses
    /// `0.0.0.0:0` (any-interface, OS-allocated port).
    pub async fn bind(cert: &CertPair) -> Result<Self, String> {
        Self::bind_addr(cert, "0.0.0.0:0".parse().unwrap()).await
    }

    /// Bind a QUIC server using the given cert pair on the requested
    /// address. Pass port 0 to let the OS choose; pass a fixed port
    /// to coordinate with a secondary that already knows where to
    /// connect (e.g. when the primary published its URL before the
    /// server was up).
    pub async fn bind_addr(cert: &CertPair, addr: SocketAddr) -> Result<Self, String> {
        let server_config = cert.server_config()?;
        Self::try_bind(server_config, addr).map_err(|e| e.to_string())
    }

    /// Bind on a pre-built `ServerConfig`, surfacing the raw
    /// [`std::io::Error`] so a caller can classify the failure (e.g. the
    /// listener-pair retry distinguishing `AddrInUse` from a fatal
    /// bind error). The cert→`ServerConfig` step is the caller's
    /// fail-fast concern; this entry covers only the io-fallible bind.
    pub(crate) fn try_bind(
        server_config: quinn::ServerConfig,
        addr: SocketAddr,
    ) -> std::io::Result<Self> {
        let endpoint = Endpoint::server(server_config, addr)?;
        let local_addr = endpoint.local_addr()?;
        tracing::info!(%local_addr, "QUIC listener bound");
        Ok(Self {
            endpoint,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Listener-level accept: the next incoming connection ATTEMPT.
    /// `None` iff the endpoint itself is closed — the ONLY loop-fatal
    /// condition an accept loop may exit on. The attempt has NOT been
    /// handshaken yet; drive it to an established connection with
    /// [`QuicConnection::from_incoming`] inside the per-connection
    /// handler task, so a dialer that aborts mid-handshake fails its
    /// own attempt and nothing else.
    pub async fn accept_raw(&self) -> Option<quinn::Incoming> {
        self.endpoint.accept().await
    }

    /// Accept the next incoming connection and open a bi-directional
    /// stream — `accept_raw` + [`QuicConnection::from_incoming`] in one
    /// call, for single-connection callers (tests, fixtures). An accept
    /// LOOP must NOT use this: a per-connection handshake failure is
    /// indistinguishable from a listener failure in the flattened
    /// `Err`, and awaiting the handshake inline serializes (and, treated
    /// as fatal, kills) the loop — use the split form via
    /// [`crate::accept_loop`] so one bad connection cannot kill or
    /// wedge the listener.
    pub async fn accept(&self) -> Result<QuicConnection, String> {
        let incoming = self.accept_raw().await.ok_or("endpoint closed")?;
        QuicConnection::from_incoming(incoming).await
    }
}

/// Connect to a QUIC server, trusting the given peer certificate.
///
/// The local UDP endpoint is bound to a wildcard address whose family
/// matches the destination — `0.0.0.0:0` for an IPv4 destination,
/// `[::]:0` for an IPv6 destination. A v4-only socket cannot send to a
/// v6 destination (or vice versa), so without family matching this
/// helper would silently fail every IPv6 dial regardless of network
/// reachability. This affects the peer dialer's happy-eyeballs path,
/// where both families may be tried in parallel.
pub async fn connect(
    addr: SocketAddr,
    server_name: &str,
    peer_cert_der: &CertificateDer<'_>,
) -> Result<QuicConnection, String> {
    let client_config = CertPair::client_config_trusting(peer_cert_der)?;

    let bind_addr: SocketAddr = match addr {
        SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
        SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
    };
    let mut endpoint = Endpoint::client(bind_addr).map_err(|e| e.to_string())?;
    endpoint.set_default_client_config(client_config);

    let connection = endpoint
        .connect(addr, server_name)
        .map_err(|e| e.to_string())?
        .await
        .map_err(|e| e.to_string())?;

    let (send, recv) = connection.open_bi().await.map_err(|e| e.to_string())?;

    Ok(QuicConnection::from_streams(send, recv))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynrunner_protocol_primary_secondary::KeepaliveRole;
    use serde::{Deserialize, Serialize};

    /// Minimal test identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[tokio::test]
    async fn quic_message_roundtrip() {
        let cert = CertPair::generate("localhost").unwrap();
        let listener = QuicListener::bind(&cert).await.unwrap();
        let port = listener.port();
        let cert_der = cert.cert_der.clone();

        let outgoing: DistributedMessage<TestId> = DistributedMessage::Keepalive {
            target: None,
            sender_id: "test".into(),
            timestamp: 42.0,
            secondary_id: "test".into(),
            active_workers: 2,
            emitter_role: KeepaliveRole::Secondary,
        };

        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = async {
            let mut conn = listener.accept().await.expect("accept failed");
            let msg: DistributedMessage<TestId> =
                MessageReceiver::recv(&mut conn).await.expect("no message");
            MessageSender::send(&mut conn, msg.clone())
                .await
                .expect("send failed");
            // Keep connection alive until client is done reading.
            done_rx.await.ok();
            msg
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let mut client = connect(addr, "localhost", &cert_der)
                .await
                .expect("connect failed");
            MessageSender::send(&mut client, outgoing)
                .await
                .expect("client send failed");
            let echoed: DistributedMessage<TestId> =
                MessageReceiver::recv(&mut client).await.expect("no echo");
            done_tx.send(()).ok();
            echoed
        };

        let (server_msg, echoed) = tokio::join!(server_task, client_task);

        match &echoed {
            DistributedMessage::Keepalive { active_workers, .. } => {
                assert_eq!(*active_workers, 2);
            }
            _ => panic!("expected Keepalive"),
        }

        assert_eq!(server_msg.sender_id(), "test");
    }

    /// The QUIC-leg receiver guard (#366): a frame whose 4-byte length
    /// prefix announces more than the wire limit must terminate `recv`
    /// with `None` (loud reject + normal disconnect) BEFORE the
    /// receiver accumulates the payload — quinn itself has no
    /// per-message cap, so without the guard a corrupt/oversize prefix
    /// would buffer without bound.
    #[tokio::test]
    async fn quic_oversize_announcement_rejected() {
        let cert = CertPair::generate("localhost").unwrap();
        let listener = QuicListener::bind(&cert).await.unwrap();
        let port = listener.port();
        let cert_der = cert.cert_der.clone();

        let server_task = async {
            let mut conn = listener.accept().await.expect("accept failed");
            MessageReceiver::<DistributedMessage<TestId>>::recv(&mut conn).await
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let client = connect(addr, "localhost", &cert_der)
                .await
                .expect("connect failed");
            // Bypass the egress gate: write a raw prefix announcing an
            // over-limit frame (no payload needed — the guard must
            // fire on the announcement alone).
            let (mut send, _recv, _buf) = client.into_parts();
            let announced = (crate::framing::MAX_WIRE_FRAME_BYTES as u32) + 1;
            send.write_all(&announced.to_be_bytes())
                .await
                .expect("prefix write failed");
            // Keep the stream open so the server's exit is the guard,
            // not a stream end.
            (send, _recv)
        };

        let (received, _client_streams) = tokio::join!(server_task, client_task);
        assert!(
            received.is_none(),
            "an over-limit announced frame must terminate recv with None"
        );
    }
}
