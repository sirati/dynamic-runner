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

impl QuicConnection {
    pub fn from_streams(send: SendStream, recv: RecvStream) -> Self {
        Self {
            send,
            recv,
            recv_buf: Vec::new(),
        }
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
                Err(_) => return None,
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

        let endpoint = Endpoint::server(server_config, addr).map_err(|e| e.to_string())?;

        let local_addr = endpoint.local_addr().map_err(|e| e.to_string())?;
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

    /// Accept the next incoming connection and open a bi-directional stream.
    pub async fn accept(&self) -> Result<QuicConnection, String> {
        let incoming = self.endpoint.accept().await.ok_or("endpoint closed")?;

        let connection = incoming.await.map_err(|e| e.to_string())?;
        let (send, recv) = connection.accept_bi().await.map_err(|e| e.to_string())?;

        Ok(QuicConnection::from_streams(send, recv))
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
            sender_id: "test".into(),
            timestamp: 42.0,
            secondary_id: "test".into(),
            active_workers: 2,
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
}
