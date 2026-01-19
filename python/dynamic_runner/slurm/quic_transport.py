import asyncio
import json
import logging
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from aioquic.asyncio import connect, serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived

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
        self.peer_certs: dict[str, Path] = {}  # peer_id -> path to cert file

        # Message handlers
        self.message_handlers: dict[str, Callable] = {}

        # Server
        self.server = None
        self.running = False

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

        logger.info(f"Connected to {len(self.connections)} peers")

    async def _connect_to_peer(self, peer_id: str, peer_info: QuicPeerInfo) -> None:
        """Connect to a specific peer using QUIC"""
        logger.debug(f"Connecting to peer {peer_id}...")

        try:
            # Prefer IPv4 if available
            host = peer_info.ipv4 if peer_info.ipv4 else peer_info.ipv6
            if not host:
                raise ValueError(f"No IP address for peer {peer_id}")

            # Configure QUIC client
            configuration = QuicConfiguration(is_client=True, alpn_protocols=["asm-tokenizer"])

            # Load our own certificate for mutual TLS
            if self.cert_path and self.key_path:
                configuration.load_cert_chain(self.cert_path, self.key_path)

            # For self-signed certificates, skip verification (peer auth happens via cert exchange)
            configuration.verify_mode = ssl.CERT_NONE

            # Connect using aioquic
            async with connect(
                host,
                peer_info.port,
                configuration=configuration,
                create_protocol=lambda *args, **kwargs: QuicProtocol(*args, peer_id=peer_id, transport=self, **kwargs),
            ) as client:
                protocol = client._protocol
                self.connections[peer_id] = protocol
                self.clients[peer_id] = client

                logger.info(f"Connected to peer {peer_id} at {host}:{peer_info.port}")

                # Keep connection alive by storing a task that waits
                async def keep_alive():
                    try:
                        await client.wait_closed()
                    except Exception as e:
                        logger.debug(f"Connection to {peer_id} closed: {e}")

                asyncio.create_task(keep_alive())

        except Exception as e:
            logger.warning(f"Failed to connect to peer {peer_id}: {e}")

    async def start_server(self) -> None:
        """Start QUIC server to accept incoming connections"""
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

        # Configure QUIC server
        configuration = QuicConfiguration(is_client=False, alpn_protocols=["asm-tokenizer"])
        configuration.load_cert_chain(self.cert_path, self.key_path)

        # Create server factory
        def create_protocol(*args, **kwargs):
            return QuicProtocol(*args, peer_id="incoming", transport=self, **kwargs)

        # Start server on specified bind address (localhost for primary, 0.0.0.0 for secondaries)
        self.server = await serve(
            self.bind_address,
            self.listen_port,
            configuration=configuration,
            create_protocol=create_protocol,
        )

        self.running = True

        logger.info(f"QUIC server listening on {self.bind_address}:{self.listen_port}")

    async def _handle_peer_message(self, message: dict[str, Any], protocol: QuicProtocol) -> None:
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
        """Send message to a specific peer"""
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

        logger.debug(f"Sent message to {peer_id}: {message.get('type')}")

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
        for peer_id, client in self.clients.items():
            try:
                client.close()
            except Exception as e:
                logger.debug(f"Error closing client {peer_id}: {e}")

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
