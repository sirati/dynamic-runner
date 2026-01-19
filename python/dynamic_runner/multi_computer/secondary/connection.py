import asyncio
import json
import logging
import socket
import ssl
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import websockets

from ..quic_transport import QuicPeerInfo

if TYPE_CHECKING:
    from .coordinator import SecondaryCoordinator

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages connections to primary and peers"""

    def __init__(self, coordinator: "SecondaryCoordinator"):
        self.coordinator = coordinator
        self.peer_list_received = asyncio.Event()

    async def connect_to_primary(self) -> None:
        """Establish connection to primary via WSS with retry logic (up to 60 seconds total)"""
        parsed = urlparse(self.coordinator.primary_url)
        primary_host = parsed.hostname or "localhost"
        primary_port = parsed.port or 6000

        logger.info(f"Connecting to primary: {primary_host}:{primary_port}")
        logger.info("Will retry once per second for up to 60 seconds if primary is not ready yet...")

        timeout = 60.0  # seconds - total time to keep trying
        retry_delay = 1.0  # seconds - delay between retries
        start_time = time.time()
        attempt = 0

        while True:
            attempt += 1
            elapsed = time.time() - start_time

            if elapsed > timeout:
                logger.error(f"Failed to connect to primary after {timeout:.0f} seconds ({attempt} attempts)")
                logger.error("Primary may not have set up SSH tunnel yet, or connection info file was not found")
                raise TimeoutError(
                    f"Could not connect to primary at {primary_host}:{primary_port} within {timeout:.0f}s"
                )

            try:
                logger.info(f"Connection attempt {attempt} (elapsed: {elapsed:.1f}s)...")

                # Connect using WSS (WebSocket Secure)
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE  # Don't verify primary's cert for now

                uri = f"wss://{primary_host}:{primary_port}"
                websocket = await websockets.connect(
                    uri,
                    ssl=ssl_context,
                    subprotocols=["asm-tokenizer"],
                )

                # Store websocket connection
                self.coordinator.primary_websocket = websocket

                logger.info(f"Connected to primary successfully after {elapsed:.1f}s ({attempt} attempts)")

                # Start receive loop in background with connection monitoring
                asyncio.create_task(self._monitor_primary_connection_wss(websocket))
                return  # Success!

            except (ConnectionRefusedError, OSError, websockets.exceptions.WebSocketException) as e:
                error_type = type(e).__name__
                error_msg = str(e)
                remaining = timeout - elapsed
                if remaining > 0:
                    logger.info(
                        f"Connection failed (attempt {attempt}): {error_type}: {error_msg}. "
                        f"Retrying in {retry_delay}s..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to primary after {timeout:.0f} seconds: {error_type}: {error_msg}")
                    raise
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                logger.error(f"Unexpected error connecting to primary: {error_type}: {error_msg}")
                raise

    async def send_welcome(self) -> None:
        """Send welcome message with capabilities to primary"""
        logger.info("Sending welcome message to primary...")

        hostname = socket.gethostname()

        msg = {
            "type": "secondary_welcome",
            "secondary_id": self.coordinator.secondary_id,
            "ram_bytes": self.coordinator.ram_bytes,
            "num_workers": self.coordinator.num_workers,
            "quic_port": self.coordinator.quic_transport.listen_port,
            "quic_cert": self.coordinator.quic_transport.get_public_cert().decode("utf-8"),
            "hostname": hostname,
            "from": self.coordinator.secondary_id,
        }

        # Send via websocket
        await self.coordinator.primary_websocket.send(json.dumps(msg))
        logger.info("Welcome message sent")

    async def send_error_to_primary(self, error: Exception) -> None:
        """Send error message with traceback to primary"""
        import traceback

        if self.coordinator.connection_closing:
            return

        try:
            error_msg = {
                "type": "secondary_error",
                "secondary_id": self.coordinator.secondary_id,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": traceback.format_exc(),
                "from": self.coordinator.secondary_id,
            }
            await self.coordinator.primary_websocket.send(json.dumps(error_msg))
            logger.info("Sent error report to primary")
        except Exception as e:
            logger.error(f"Failed to send error to primary: {e}")

    async def _monitor_primary_connection_wss(self, websocket) -> None:
        """Monitor primary WebSocket connection and handle disconnect"""
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    message_type = data.get("type")

                    logger.debug(f"Received from primary: {message_type}")

                    # Dispatch to message handlers
                    await self.coordinator.message_router.handle_message(data, "primary")

                except Exception as e:
                    logger.error(f"Error processing message from primary: {e}")
        except Exception as e:
            logger.warning(f"Primary connection closed: {e}")
        finally:
            # Mark that connection is closing to prevent sending more messages
            self.coordinator.connection_closing = True

            # Connection closed
            if not self.coordinator.setup_complete:
                logger.error("Primary connection closed before setup was complete!")
                logger.error("Aborting secondary - setup incomplete")
                self.coordinator.running = False
                # Don't try to send error to primary - connection is already closed
                # Exit the process
                import sys

                sys.exit(1)
            else:
                logger.warning("Primary connection closed after setup was complete")
                self.coordinator.running = False

    async def send_cert_exchange(self) -> None:
        """Send certificate exchange message to primary"""
        logger.info("Sending certificate exchange to primary")

        # Get local IP addresses
        ipv4, ipv6 = self.coordinator.quic_transport.get_local_addresses()

        # Get public certificate
        cert_pem = self.coordinator.quic_transport.get_public_cert().decode("utf-8")

        # Send certificate exchange message to primary via WebSocket
        msg = {
            "type": "cert_exchange",
            "secondary_id": self.coordinator.secondary_id,
            "public_cert_pem": cert_pem,
            "ipv4_address": ipv4,
            "ipv6_address": ipv6,
            "quic_port": self.coordinator.quic_transport.listen_port,
            "from": self.coordinator.secondary_id,
        }

        await self.coordinator.primary_websocket.send(json.dumps(msg))
        logger.info(f"Sent certificate exchange: {ipv4}:{self.coordinator.quic_transport.listen_port}")

    async def handle_peer_list(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle peer_list message from primary"""
        peers = message.get("peers", [])
        logger.info(f"Received peer list with {len(peers)} peers from primary")

        # Add all peers to QUIC transport
        for peer_info in peers:
            peer_id = peer_info.get("peer_id")
            if peer_id == self.coordinator.secondary_id:
                # Skip self
                continue

            quic_peer = QuicPeerInfo(
                peer_id=peer_id,
                ipv4=peer_info.get("ipv4"),
                ipv6=peer_info.get("ipv6"),
                port=peer_info.get("port"),
                cert_pem=peer_info.get("cert_pem", ""),
                cert_fingerprint=peer_info.get("cert_fingerprint", ""),
            )
            self.coordinator.quic_transport.add_peer(quic_peer)

        logger.info(f"Added {len(self.coordinator.quic_transport.peers)} peers, connecting...")

        # Connect to all peers (primary controls the peer list, so we know this is complete)
        await self.coordinator.quic_transport.connect_to_peers()

        # Count both QUIC and WSS connections
        total_connections = len(self.coordinator.quic_transport.connections) + len(
            self.coordinator.quic_transport.wss_connections
        )
        logger.info(
            f"Peer connections established: {total_connections} peers connected "
            f"({len(self.coordinator.quic_transport.connections)} QUIC, "
            f"{len(self.coordinator.quic_transport.wss_connections)} WSS)"
        )

        # Signal that peer list has been processed
        self.peer_list_received.set()

    async def connect_to_peers(self) -> None:
        """Wait for peer list from primary and establish connections"""
        logger.info("Waiting for peer list from primary...")

        # Wait for the peer_list message to be received and processed
        # The primary controls when this happens by sending the peer_list message
        await self.peer_list_received.wait()

        logger.info("Peer list received and connections established, continuing...")

        # Notify primary that peer connections are ready
        await self._send_peer_connections_ready()

    async def _send_peer_connections_ready(self) -> None:
        """Notify primary that peer connections are established"""
        msg = {
            "type": "peer_connections_ready",
            "secondary_id": self.coordinator.secondary_id,
            "from": self.coordinator.secondary_id,
        }

        await self.coordinator.send_to_primary_ws(msg)
        logger.info("Notified primary that peer connections are ready")
