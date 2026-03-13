use std::net::SocketAddr;

use db_comm_api_base::Identifier;
use db_primary_secondary_comm::{DistributedMessage, codec};
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

    /// Send a distributed message (length-prefixed JSON).
    pub async fn send_message<I: Identifier>(&mut self, msg: &DistributedMessage<I>) -> Result<(), String> {
        let frame = codec::serialize_message(msg)?;
        self.send
            .write_all(&frame)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Gracefully close the send side.
    pub async fn finish_send(&mut self) -> Result<(), String> {
        self.send.finish().map_err(|e| e.to_string())?;
        // Wait for the peer to receive all data.
        self.send.stopped().await.ok();
        Ok(())
    }

    /// Receive the next distributed message. Returns None on connection close.
    pub async fn recv_message<I: Identifier>(&mut self) -> Result<Option<DistributedMessage<I>>, String> {
        loop {
            match codec::decode_frame(&self.recv_buf)? {
                Some((msg, consumed)) => {
                    self.recv_buf.drain(..consumed);
                    return Ok(Some(msg));
                }
                None => {
                    let mut tmp = [0u8; 8192];
                    match self.recv.read(&mut tmp).await {
                        Ok(Some(n)) => {
                            self.recv_buf.extend_from_slice(&tmp[..n]);
                        }
                        Ok(None) => return Ok(None),
                        Err(e) => return Err(e.to_string()),
                    }
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
    /// Bind a QUIC server using the given cert pair on an OS-allocated port.
    pub async fn bind(cert: &CertPair) -> Result<Self, String> {
        let server_config = cert.server_config()?;

        let endpoint = Endpoint::server(
            server_config,
            "0.0.0.0:0".parse().unwrap(),
        )
        .map_err(|e| e.to_string())?;

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
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or("endpoint closed")?;

        let connection = incoming.await.map_err(|e| e.to_string())?;
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|e| e.to_string())?;

        Ok(QuicConnection::from_streams(send, recv))
    }
}

/// Connect to a QUIC server, trusting the given peer certificate.
pub async fn connect(
    addr: SocketAddr,
    server_name: &str,
    peer_cert_der: &CertificateDer<'_>,
) -> Result<QuicConnection, String> {
    let client_config = CertPair::client_config_trusting(peer_cert_der)?;

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(|e| e.to_string())?;
    endpoint.set_default_client_config(client_config);

    let connection = endpoint
        .connect(addr, server_name)
        .map_err(|e| e.to_string())?
        .await
        .map_err(|e| e.to_string())?;

    let (send, recv) = connection
        .open_bi()
        .await
        .map_err(|e| e.to_string())?;

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
            let msg: DistributedMessage<TestId> = conn.recv_message().await.expect("recv failed").expect("no message");
            conn.send_message(&msg).await.expect("send failed");
            // Keep connection alive until client is done reading.
            done_rx.await.ok();
            msg
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let mut client = connect(addr, "localhost", &cert_der)
                .await
                .expect("connect failed");
            client.send_message(&outgoing).await.expect("client send failed");
            let echoed: DistributedMessage<TestId> = client.recv_message().await.expect("client recv failed").expect("no echo");
            done_tx.send(()).ok();
            echoed
        };

        let (server_msg, echoed) = tokio::join!(server_task, client_task);

        match &echoed {
            DistributedMessage::Keepalive {
                active_workers, ..
            } => {
                assert_eq!(*active_workers, 2);
            }
            _ => panic!("expected Keepalive"),
        }

        assert_eq!(server_msg.sender_id(), "test");
    }
}
