use std::net::SocketAddr;

use dynrunner_core::{Identifier, MessageReceiver, MessageSender};
use dynrunner_protocol_primary_secondary::{DistributedMessage, codec};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

/// A WSS (WebSocket Secure) connection that can send/receive distributed messages.
///
/// Used as a TCP-based fallback when QUIC (UDP) is blocked by network policy.
/// Implements the same `MessageSender` / `MessageReceiver` traits as `QuicConnection`.
pub struct WssConnection {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WssConnection {
    pub fn new(ws: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { ws }
    }

    /// Consume the connection and return the underlying WebSocket stream.
    pub fn into_inner(self) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
        self.ws
    }
}

impl<I: Identifier> MessageSender<DistributedMessage<I>> for WssConnection {
    async fn send(&mut self, msg: DistributedMessage<I>) -> Result<(), String> {
        let frame = codec::serialize_message(&msg)?;
        self.ws
            .send(Message::Binary(frame.into()))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl<I: Identifier> MessageReceiver<DistributedMessage<I>> for WssConnection {
    async fn recv(&mut self) -> Option<DistributedMessage<I>> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Binary(data))) => match codec::decode_frame(&data) {
                    Ok(Some((msg, _))) => return Some(msg),
                    Ok(None) => continue,
                    Err(_) => return None,
                },
                Some(Ok(Message::Close(_))) | None => return None,
                Some(Ok(_)) => continue, // skip ping/pong/text
                Some(Err(_)) => return None,
            }
        }
    }
}

/// A WSS listener that accepts incoming WebSocket connections.
pub struct WssListener {
    tcp_listener: TcpListener,
    local_addr: SocketAddr,
}

impl WssListener {
    /// Bind a WSS server on the given address.
    pub async fn bind(addr: SocketAddr) -> Result<Self, String> {
        let tcp_listener = TcpListener::bind(addr).await.map_err(|e| e.to_string())?;
        let local_addr = tcp_listener.local_addr().map_err(|e| e.to_string())?;
        tracing::info!(%local_addr, "WSS listener bound");
        Ok(Self {
            tcp_listener,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Accept the next incoming WebSocket connection (plain WS, no TLS).
    ///
    /// For production use behind SSH tunnels / internal networks, TLS is
    /// typically handled at the tunnel level. To add native TLS, wrap the
    /// `TcpStream` with `tokio_native_tls` before the WebSocket handshake.
    pub async fn accept(&self) -> Result<WssConnection, String> {
        let (tcp_stream, peer_addr) = self
            .tcp_listener
            .accept()
            .await
            .map_err(|e| e.to_string())?;
        tracing::debug!(%peer_addr, "WSS TCP connection accepted");

        let ws_stream = tokio_tungstenite::accept_async(MaybeTlsStream::Plain(tcp_stream))
            .await
            .map_err(|e| e.to_string())?;

        Ok(WssConnection::new(ws_stream))
    }
}

/// Connect to a WSS server (plain WS, no TLS — see `WssListener::accept` note).
pub async fn connect_wss(addr: SocketAddr) -> Result<WssConnection, String> {
    let url = format!("ws://{addr}");
    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| e.to_string())?;

    Ok(WssConnection::new(ws_stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(String);

    #[tokio::test]
    async fn wss_message_roundtrip() {
        let listener = WssListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let port = listener.port();

        let outgoing: DistributedMessage<TestId> = DistributedMessage::Keepalive {
            sender_id: "wss-test".into(),
            timestamp: 99.0,
            secondary_id: "wss-test".into(),
            active_workers: 4,
        };

        let server_task = async {
            let mut conn = listener.accept().await.expect("accept failed");
            let msg: DistributedMessage<TestId> =
                MessageReceiver::recv(&mut conn).await.expect("no message");
            MessageSender::send(&mut conn, msg.clone())
                .await
                .expect("send failed");
            msg
        };

        let client_task = async {
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let mut client = connect_wss(addr).await.expect("connect failed");
            MessageSender::send(&mut client, outgoing)
                .await
                .expect("client send failed");
            let echoed: DistributedMessage<TestId> =
                MessageReceiver::recv(&mut client).await.expect("no echo");
            echoed
        };

        let (server_msg, echoed) = tokio::join!(server_task, client_task);

        match &echoed {
            DistributedMessage::Keepalive { active_workers, .. } => {
                assert_eq!(*active_workers, 4);
            }
            _ => panic!("expected Keepalive"),
        }

        assert_eq!(server_msg.sender_id(), "wss-test");
    }
}
