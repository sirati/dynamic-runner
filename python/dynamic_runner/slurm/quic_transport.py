import asyncio
import json
import logging
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import websockets
from aioquic.asyncio import connect, serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived

# Add custom TRACE level (below DEBUG)
TRACE = 5
logging.addLevelName(TRACE, "TRACE")

logger = logging.getLogger(__name__)


@dataclass
class QuicPeerInfo:
    """Information about a QUIC peer"""

    peer_id: str
    ipv4: str | None
    ipv6: str | None
    port: int
    cert_pem: str
    cert_fingerprint: str = ""
    use_wss_fallback: bool = False  # Whether to use WSS instead of QUIC


class QuicProtocol(QuicConnectionProtocol):
    """QUIC protocol handler for peer connections"""

    def __init__(self, *args, peer_id: str, transport: "QuicTransport", **kwargs):
        super().__init__(*args, **kwargs)
        self.peer_id = peer_id
        self.transport = transport
        self.stream_id = None
        self.buffer = bytearray()

    def quic_event_received(self, event: QuicEvent) -> None:
        """Handle QUIC events"""
        if isinstance(event, StreamDataReceived):
            # Accumulate data
            self.buffer.extend(event.data)

            # Try to parse messages
            while len(self.buffer) >= 4:
                # Read length prefix
                message_length = int.from_bytes(self.buffer[:4], "big")

                # Check if we have complete message
                if len(self.buffer) < 4 + message_length:
                    break

                # Extract message
                message_bytes = self.buffer[4 : 4 + message_length]
                self.buffer = self.buffer[4 + message_length :]

                # Parse and handle message
                try:
                    message_str = message_bytes.decode("utf-8")
                    message = json.loads(message_str)
                    asyncio.create_task(self.transport._handle_peer_message(message, self))
                except Exception as e:
                    logger.error(f"Error parsing message from {self.peer_id}: {e}")

            if event.end_stream:
                logger.debug(f"Stream ended from {self.peer_id}")


class QuicTransport:
    """QUIC-based transport for peer-to-peer secondary communication

    This provides reliable, authenticated peer-to-peer messaging between
    secondaries using QUIC protocol with certificate-based mutual authentication.
    """

    def __init__(self, peer_id: str, listen_port: int, bind_address: str = "0.0.0.0"):
        self.peer_id = peer_id
        self.listen_port = listen_port
        self.bind_address = bind_address

        # Certificate and key
        self.cert_path: Path | None = None
        self.key_path: Path | None = None
        self.cert_fingerprint: str | None = None

        # Peer connections
        self.peers: dict[str, QuicPeerInfo] = {}
        self.connections: dict[str, QuicProtocol] = {}
        self.clients: dict[str, Any] = {}  # peer_id -> client connection object
        self.client_cms: dict[str, Any] = {}  # peer_id -> context manager for cleanup
        self.peer_certs: dict[str, Path] = {}  # peer_id -> path to cert file

        # WSS fallback connections
        self.wss_connections: dict[str, websockets.WebSocketServerProtocol | websockets.WebSocketClientProtocol] = {}
        self.wss_server = None

        # Message handlers
        self.message_handlers: dict[str, Callable] = {}

        # Server
        self.server = None
        self.running = False

        # Install custom exception handler to suppress QUIC ConnectionErrors
        self._original_exception_handler = None
        self._install_exception_handler()

    async def generate_certificates(self) -> tuple[str, str]:
        """Generate self-signed certificates for QUIC

        Returns:
            (cert_path, key_path) tuple
        """
        logger.info("Generating QUIC certificates...")

        import tempfile

        tmpdir = Path(tempfile.mkdtemp(prefix="quic-certs-"))
        self.cert_path = tmpdir / "cert.pem"
        self.key_path = tmpdir / "key.pem"

        # Generate self-signed certificate using openssl
        import subprocess

        cmd = [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(self.key_path),
            "-out",
            str(self.cert_path),
            "-days",
            "1",
            "-nodes",
            "-subj",
            f"/CN={self.peer_id}",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Certificate generation failed: {result.stderr}")

        # Compute certificate fingerprint
        self.cert_fingerprint = self._compute_cert_fingerprint(self.cert_path)

        logger.info(f"Certificates generated: {self.cert_path}")
        logger.info(f"Certificate fingerprint: {self.cert_fingerprint}")

        return str(self.cert_path), str(self.key_path)

    def _compute_cert_fingerprint(self, cert_path: Path) -> str:
        """Compute SHA256 fingerprint of certificate"""
        import hashlib

        with open(cert_path, "rb") as f:
            cert_data = f.read()

        return hashlib.sha256(cert_data).hexdigest()[:16]

    def get_public_cert(self) -> bytes:
        """Get public certificate for sharing with peers"""
        if not self.cert_path:
            raise RuntimeError("Certificates not generated yet")

        with open(self.cert_path, "rb") as f:
            return f.read()

    def add_peer(self, peer_info: QuicPeerInfo) -> None:
        """Add a peer to the known peers list"""
        logger.info(f"Adding peer: {peer_info.peer_id} at {peer_info.ipv4}:{peer_info.port}")
        self.peers[peer_info.peer_id] = peer_info

        # Store peer certificate to a temporary file for QUIC verification
        import tempfile

        tmpdir = Path(tempfile.mkdtemp(prefix=f"peer-cert-{peer_info.peer_id}-"))
        cert_path = tmpdir / "cert.pem"
        cert_path.write_text(peer_info.cert_pem)
        self.peer_certs[peer_info.peer_id] = cert_path
        logger.debug(f"Stored certificate for {peer_info.peer_id} at {cert_path}")

    async def connect_to_peers(self) -> None:
        """Establish QUIC connections to all known peers"""
        logger.info(f"Connecting to {len(self.peers)} peers...")

        tasks = []
        for peer_id, peer_info in self.peers.items():
            if peer_id != self.peer_id:  # Don't connect to self
                tasks.append(self._connect_to_peer(peer_id, peer_info))

        await asyncio.gather(*tasks, return_exceptions=True)

        # Count both QUIC and WSS connections
        total_connections = len(self.connections) + len(self.wss_connections)
        logger.info(
            f"Connected to {total_connections} peers ({len(self.connections)} QUIC, {len(self.wss_connections)} WSS)"
        )

    async def _connect_to_peer(self, peer_id: str, peer_info: QuicPeerInfo) -> None:
        """Connect to a specific peer using QUIC, with WSS fallback"""
        logger.debug(f"Connecting to peer {peer_id}...")

        # Prefer IPv4 if available
        host = peer_info.ipv4 if peer_info.ipv4 else peer_info.ipv6
        if not host:
            logger.error(f"No IP address for peer {peer_id}")
            return

        logger.info(f"Attempting to connect to {peer_id} at {host}:{peer_info.port}")

        # Try QUIC first with shorter timeout
        quic_failed = False
        try:
            logger.info(f"Trying QUIC (UDP) to {peer_id}...")
            await self._connect_quic(peer_id, host, peer_info)
            logger.info(f"✅ Successfully connected to {peer_id} via QUIC (UDP)")
            return
        except Exception as e:
            quic_failed = True
            # Don't log yet - will log only if WSS also fails
            quic_error = e

        # Fall back to WSS
        try:
            logger.info(f"Falling back to WSS (WebSocket Secure over TCP) for {peer_id}...")
            await self._connect_wss(peer_id, host, peer_info)
            peer_info.use_wss_fallback = True
            logger.info(f"✅ Successfully connected to {peer_id} via WSS (TCP)")
            # WSS succeeded, so suppress QUIC error logging
        except Exception as e:
            # Both failed - now log both errors
            if quic_failed:
                logger.warning(f"QUIC (UDP) connection to {peer_id} failed: {quic_error}")
            logger.error(f"❌ Failed to connect to peer {peer_id} (both QUIC and WSS failed): {e}", exc_info=True)

    async def _connect_quic(self, peer_id: str, host: str, peer_info: QuicPeerInfo) -> None:
        """Connect to peer using QUIC"""
        # Configure QUIC client
        configuration = QuicConfiguration(is_client=True, alpn_protocols=["asm-tokenizer"])

        # Load our own certificate for mutual TLS
        if self.cert_path and self.key_path:
            configuration.load_cert_chain(self.cert_path, self.key_path)

        # For self-signed certificates, skip verification
        configuration.verify_mode = ssl.CERT_NONE

        # Connect using aioquic with timeout
        client_cm = connect(
            host,
            peer_info.port,
            configuration=configuration,
            create_protocol=lambda *args, **kwargs: QuicProtocol(*args, peer_id=peer_id, transport=self, **kwargs),
        )
        client = await asyncio.wait_for(client_cm.__aenter__(), timeout=10.0)

        protocol = client._protocol
        self.connections[peer_id] = protocol
        self.clients[peer_id] = client
        self.client_cms[peer_id] = client_cm

        # Monitor connection
        asyncio.create_task(self._monitor_quic_connection(peer_id, client))

    async def _connect_wss(self, peer_id: str, host: str, peer_info: QuicPeerInfo) -> None:
        """Connect to peer using WebSocket Secure"""
        # Create SSL context with our certificates
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # Skip verification for self-signed certs

        # Load our client certificate for mutual TLS
        if self.cert_path and self.key_path:
            ssl_context.load_cert_chain(self.cert_path, self.key_path)

        # Connect via WebSocket
        uri = f"wss://{host}:{peer_info.port}"
        websocket = await asyncio.wait_for(
            websockets.connect(uri, ssl=ssl_context, subprotocols=["asm-tokenizer"]), timeout=10.0
        )

        self.wss_connections[peer_id] = websocket

        # Start receiving messages
        asyncio.create_task(self._receive_wss_messages(peer_id, websocket))

    async def _monitor_quic_connection(self, peer_id: str, client: Any) -> None:
        """Monitor QUIC connection and clean up on close"""
        try:
            await client.wait_closed()
            logger.info(f"QUIC connection to peer {peer_id} closed")
        except Exception as e:
            logger.error(f"Error monitoring QUIC connection to {peer_id}: {e}")
        finally:
            # Clean up references
            self.connections.pop(peer_id, None)
            self.clients.pop(peer_id, None)
            cm = self.client_cms.pop(peer_id, None)
            if cm:
                try:
                    await cm.__aexit__(None, None, None)
                except Exception:
                    pass

    async def _receive_wss_messages(self, peer_id: str, websocket) -> None:
        """Receive messages from WSS connection"""
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)

                    # Create a mock protocol object for compatibility
                    class WSSProtocol:
                        def __init__(self, peer_id: str):
                            self.peer_id = peer_id

                    await self._handle_peer_message(data, WSSProtocol(peer_id))
                except Exception as e:
                    logger.error(f"Error processing WSS message from {peer_id}: {e}")
        except Exception as e:
            logger.warning(f"WSS connection to {peer_id} closed: {e}")
        finally:
            self.wss_connections.pop(peer_id, None)

    async def start_server(self) -> None:
        """Start QUIC server (UDP) and WSS server (TCP) on the same port"""
        logger.info(f"Starting QUIC server on port {self.listen_port}...")

        if not self.cert_path or not self.key_path:
            raise RuntimeError("Certificates not generated yet")

        # If port is 0, allocate a free port explicitly
        if self.listen_port == 0:
            import socket

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("", 0))
            self.listen_port = sock.getsockname()[1]
            sock.close()
            logger.info(f"Allocated port: {self.listen_port}")

        # Configure QUIC server (UDP)
        configuration = QuicConfiguration(is_client=False, alpn_protocols=["asm-tokenizer"])
        configuration.load_cert_chain(self.cert_path, self.key_path)

        # Create server factory
        def create_protocol(*args, **kwargs):
            return QuicProtocol(*args, peer_id="incoming", transport=self, **kwargs)

        # Start QUIC server (UDP)
        self.server = await serve(
            self.bind_address,
            self.listen_port,
            configuration=configuration,
            create_protocol=create_protocol,
        )

        logger.info(f"QUIC server listening on {self.bind_address}:{self.listen_port} (UDP)")

        # Start WSS server (TCP) on same port
        await self._start_wss_server()

        self.running = True

        logger.info(f"Transport ready on port {self.listen_port} (QUIC UDP + WSS TCP)")

    def _install_exception_handler(self) -> None:
        """Install custom exception handler to suppress QUIC ConnectionErrors"""
        loop = asyncio.get_event_loop()
        self._original_exception_handler = loop.get_exception_handler()

        def custom_exception_handler(loop, context):
            exception = context.get("exception")
            # Suppress ConnectionError from QUIC when we have WSS fallback
            if isinstance(exception, ConnectionError):
                # Check if this is a QUIC connection error (has 'future' in context)
                if "future" in context:
                    # Silently ignore - this is expected when UDP is blocked
                    return

            # For all other exceptions, use original handler or default
            if self._original_exception_handler:
                self._original_exception_handler(loop, context)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(custom_exception_handler)

    async def _start_wss_server(self) -> None:
        """Start WebSocket Secure server for TCP fallback"""
        # Create SSL context with our certificates
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        if self.cert_path and self.key_path:
            ssl_context.load_cert_chain(str(self.cert_path), str(self.key_path))
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # Accept any client cert for now

        async def handle_wss_connection(websocket):
            peer_addr = websocket.remote_address
            logger.info(f"WSS connection from {peer_addr}")

            # Store connection (we don't know peer_id yet)
            # Messages will identify the peer
            try:
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        peer_id = data.get("from", "unknown")

                        # Store connection by peer_id once we know it
                        if peer_id != "unknown" and peer_id not in self.wss_connections:
                            self.wss_connections[peer_id] = websocket

                        # Create mock protocol for compatibility
                        class WSSProtocol:
                            def __init__(self, peer_id: str):
                                self.peer_id = peer_id

                        await self._handle_peer_message(data, WSSProtocol(peer_id))
                    except Exception as e:
                        logger.error(f"Error processing WSS message: {e}")
            except Exception as e:
                logger.info(f"WSS connection from {peer_addr} closed: {e}")

        self.wss_server = await websockets.serve(
            handle_wss_connection, self.bind_address, self.listen_port, ssl=ssl_context, subprotocols=["asm-tokenizer"]
        )

        logger.info(f"WSS server listening on {self.bind_address}:{self.listen_port} (TCP)")

    async def _handle_peer_message(self, message: dict[str, Any], protocol: Any) -> None:
        """Handle received message from peer"""
        try:
            message_type = message.get("type")
            logger.debug(f"Received message type: {message_type} from {protocol.peer_id}")

            # Call registered handler if available
            if message_type in self.message_handlers:
                # Pass peer_id instead of protocol for compatibility with message_router handlers
                await self.message_handlers[message_type](message, protocol.peer_id)
            else:
                logger.warning(f"No handler for message type: {message_type}")

        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)

    def register_handler(self, message_type: str, handler: Callable) -> None:
        """Register a handler for a specific message type"""
        self.message_handlers[message_type] = handler
        logger.debug(f"Registered handler for message type: {message_type}")

    async def send_to_peer(self, peer_id: str, message: dict[str, Any]) -> None:
        """Send message to a specific peer (via QUIC or WSS)"""
        # Check if we have a WSS connection (fallback)
        if peer_id in self.wss_connections:
            websocket = self.wss_connections[peer_id]
            message_str = json.dumps(message)
            await websocket.send(message_str)
            logger.log(TRACE, f"Sent message to {peer_id} via WSS: {message.get('type')}")
            return

        # Otherwise use QUIC
        if peer_id not in self.connections:
            raise ValueError(f"No connection to peer {peer_id}")

        protocol = self.connections[peer_id]

        # Serialize message
        message_str = json.dumps(message)
        message_bytes = message_str.encode("utf-8")

        # Send length prefix + message
        length_bytes = len(message_bytes).to_bytes(4, "big")
        data = length_bytes + message_bytes

        # Create stream and send data
        stream_id = protocol._quic.get_next_available_stream_id()
        protocol._quic.send_stream_data(stream_id, data, end_stream=False)
        protocol.transmit()

        logger.log(TRACE, f"Sent message to {peer_id} via QUIC: {message.get('type')}")

    async def broadcast_to_peers(self, message: dict[str, Any], exclude: set[str] | None = None) -> None:
        """Broadcast message to all connected peers (except excluded ones)"""
        exclude = exclude or set()

        tasks = []
        for peer_id in self.connections:
            if peer_id not in exclude:
                tasks.append(self.send_to_peer(peer_id, message))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Stop server and close all connections"""
        logger.info("Stopping QUIC transport...")

        self.running = False

        # Close all peer connections
        for peer_id, client in list(self.clients.items()):
            try:
                client.close()
                # Also clean up context manager
                cm = self.client_cms.get(peer_id)
                if cm:
                    try:
                        await cm.__aexit__(None, None, None)
                    except Exception as e:
                        logger.debug(f"Error exiting context manager for {peer_id}: {e}")
            except Exception as e:
                logger.debug(f"Error closing client {peer_id}: {e}")

        self.clients.clear()
        self.client_cms.clear()

        # Close server
        if self.server:
            self.server.close()

        logger.info("QUIC transport stopped")

    def get_local_addresses(self) -> tuple[str | None, str | None]:
        """Get local IPv4 and IPv6 addresses

        Returns:
            (ipv4, ipv6) tuple
        """
        import socket

        ipv4 = None
        ipv6 = None

        try:
            # Get IPv4
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ipv4 = s.getsockname()[0]
            s.close()
        except Exception as e:
            logger.debug(f"Could not determine IPv4: {e}")

        try:
            # Get IPv6
            s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            s.connect(("2001:4860:4860::8888", 80))
            ipv6 = s.getsockname()[0]
            s.close()
        except Exception as e:
            logger.debug(f"Could not determine IPv6: {e}")

        return ipv4, ipv6
