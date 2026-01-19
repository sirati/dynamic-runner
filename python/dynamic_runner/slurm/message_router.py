import asyncio
import json
import logging
import time
from typing import Any, Callable

from .protocol import Message

logger = logging.getLogger(__name__)


class MessageRouter:
    """Routes messages between primary/secondary and handles protocol communication"""

    def __init__(self, node_id: str, node_type: str):
        """
        Args:
            node_id: Unique identifier for this node
            node_type: Either "primary" or "secondary"
        """
        self.node_id = node_id
        self.node_type = node_type

        # Message handlers: message_type -> handler function
        self.handlers: dict[str, Callable] = {}

        # Pending responses: request_id -> Future
        self.pending_responses: dict[str, asyncio.Future] = {}

        # Connection management
        self.primary_connection = None
        self.secondary_connections: dict[str, Any] = {}

        self.running = False

    def register_handler(self, message_type: str, handler: Callable) -> None:
        """Register a handler for a specific message type

        Args:
            message_type: Type of message (e.g., "secondary_welcome", "task_complete")
            handler: Async function that takes (message, sender_id) and returns response or None
        """
        self.handlers[message_type] = handler
        logger.debug(f"Registered handler for {message_type}")

    async def handle_message(self, message_dict: dict[str, Any], sender_id: str | None = None) -> dict[str, Any] | None:
        """Handle incoming message and dispatch to appropriate handler

        Args:
            message_dict: Parsed message dictionary
            sender_id: ID of sender (for secondary connections)

        Returns:
            Response message dict or None
        """
        message_type = message_dict.get("type")

        if not message_type:
            logger.warning(f"Message without type field: {message_dict}")
            return None

        logger.debug(f"Handling message type: {message_type} from {sender_id or 'unknown'}")

        # Check if this is a response to a pending request
        request_id = message_dict.get("request_id")
        if request_id and request_id in self.pending_responses:
            future = self.pending_responses.pop(request_id)
            future.set_result(message_dict)
            return None

        # Dispatch to handler
        if message_type in self.handlers:
            try:
                response = await self.handlers[message_type](message_dict, sender_id)
                return response
            except Exception as e:
                logger.error(f"Handler for {message_type} raised exception: {e}", exc_info=True)
                return {
                    "type": "error",
                    "error": str(e),
                    "original_type": message_type,
                }
        else:
            logger.warning(f"No handler registered for message type: {message_type}")
            return None

    async def send_to_primary(self, message: dict[str, Any]) -> None:
        """Send message to primary coordinator

        Args:
            message: Message dictionary to send
        """
        if not self.primary_connection:
            raise RuntimeError("Not connected to primary")

        # Add sender info
        message["sender_id"] = self.node_id
        message["timestamp"] = time.time()

        await self._send_message(self.primary_connection, message)
        logger.debug(f"Sent to primary: {message.get('type')}")

    async def send_to_secondary(self, secondary_id: str, message: dict[str, Any]) -> None:
        """Send message to specific secondary

        Args:
            secondary_id: ID of secondary to send to
            message: Message dictionary to send
        """
        if secondary_id not in self.secondary_connections:
            raise ValueError(f"No connection to secondary {secondary_id}")

        # Add sender info
        message["sender_id"] = self.node_id
        message["timestamp"] = time.time()

        connection = self.secondary_connections[secondary_id]
        await self._send_message(connection, message)
        logger.debug(f"Sent to {secondary_id}: {message.get('type')}")

    async def broadcast_to_secondaries(self, message: dict[str, Any], exclude: set[str] | None = None) -> None:
        """Broadcast message to all connected secondaries

        Args:
            message: Message dictionary to send
            exclude: Set of secondary IDs to exclude from broadcast
        """
        exclude = exclude or set()

        # Add sender info
        message["sender_id"] = self.node_id
        message["timestamp"] = time.time()

        tasks = []
        for secondary_id, connection in self.secondary_connections.items():
            if secondary_id not in exclude:
                tasks.append(self._send_message(connection, message))

        await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(f"Broadcast to {len(tasks)} secondaries: {message.get('type')}")

    async def request_from_primary(self, message: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        """Send request to primary and wait for response

        Args:
            message: Request message
            timeout: Timeout in seconds

        Returns:
            Response message dictionary
        """
        # Generate request ID
        import secrets

        request_id = secrets.token_hex(8)
        message["request_id"] = request_id

        # Create future for response
        future = asyncio.Future()
        self.pending_responses[request_id] = future

        try:
            # Send request
            await self.send_to_primary(message)

            # Wait for response
            response = await asyncio.wait_for(future, timeout=timeout)
            return response

        except asyncio.TimeoutError:
            # Clean up pending response
            self.pending_responses.pop(request_id, None)
            raise TimeoutError(f"Request {message.get('type')} timed out after {timeout}s")

    async def request_from_secondary(
        self, secondary_id: str, message: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        """Send request to secondary and wait for response

        Args:
            secondary_id: ID of secondary to send to
            message: Request message
            timeout: Timeout in seconds

        Returns:
            Response message dictionary
        """
        # Generate request ID
        import secrets

        request_id = secrets.token_hex(8)
        message["request_id"] = request_id

        # Create future for response
        future = asyncio.Future()
        self.pending_responses[request_id] = future

        try:
            # Send request
            await self.send_to_secondary(secondary_id, message)

            # Wait for response
            response = await asyncio.wait_for(future, timeout=timeout)
            return response

        except asyncio.TimeoutError:
            # Clean up pending response
            self.pending_responses.pop(request_id, None)
            raise TimeoutError(f"Request to {secondary_id} timed out after {timeout}s")

    async def _send_message(self, connection: Any, message: dict[str, Any]) -> None:
        """Send message over connection

        Args:
            connection: Connection object (writer or socket)
            message: Message dictionary
        """
        # Serialize message
        message_str = json.dumps(message)
        message_bytes = message_str.encode("utf-8")

        # Send length prefix + message
        length_bytes = len(message_bytes).to_bytes(4, "big")

        # Handle different connection types
        if hasattr(connection, "write"):
            # asyncio StreamWriter
            connection.write(length_bytes)
            connection.write(message_bytes)
            await connection.drain()
        else:
            # Other connection types
            raise NotImplementedError(f"Unsupported connection type: {type(connection)}")

    async def receive_loop(self, reader: Any, sender_id: str | None = None) -> None:
        """Receive and process messages from connection

        Args:
            reader: asyncio StreamReader or similar
            sender_id: ID of sender (for logging/routing)
        """
        self.running = True
        connection_error = None

        try:
            while self.running:
                # Read message length (4 bytes)
                length_bytes = await reader.readexactly(4)
                message_length = int.from_bytes(length_bytes, "big")

                # Read message
                message_bytes = await reader.readexactly(message_length)
                message_str = message_bytes.decode("utf-8")

                # Parse message
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message from {sender_id}: {e}")
                    continue

                # Handle message (async)
                asyncio.create_task(self.handle_message(message_dict, sender_id))

        except asyncio.IncompleteReadError as e:
            connection_error = e
            if self.running:
                logger.warning(f"Connection unexpectedly closed by {sender_id or 'peer'}")
            else:
                logger.info(f"Connection closed by {sender_id or 'peer'}")
        except Exception as e:
            connection_error = e
            logger.error(f"Error in receive loop from {sender_id}: {e}", exc_info=True)
        finally:
            was_running = self.running
            self.running = False

            # If we had an error while still supposed to be running, this was unexpected
            if connection_error and was_running:
                logger.error(f"Connection to {sender_id or 'peer'} terminated abnormally")

    def stop(self) -> None:
        """Stop message router"""
        logger.info("Stopping message router...")
        self.running = False

        # Cancel all pending responses
        for request_id, future in self.pending_responses.items():
            if not future.done():
                future.cancel()

        self.pending_responses.clear()

    def set_primary_connection(self, connection: Any) -> None:
        """Set connection to primary coordinator

        Args:
            connection: Connection object (writer)
        """
        self.primary_connection = connection
        logger.info("Primary connection established")

    def add_secondary_connection(self, secondary_id: str, connection: Any) -> None:
        """Add connection to a secondary

        Args:
            secondary_id: ID of secondary
            connection: Connection object (writer)
        """
        self.secondary_connections[secondary_id] = connection
        logger.info(f"Added connection to secondary {secondary_id}")

    def remove_secondary_connection(self, secondary_id: str) -> None:
        """Remove connection to a secondary

        Args:
            secondary_id: ID of secondary
        """
        if secondary_id in self.secondary_connections:
            del self.secondary_connections[secondary_id]
            logger.info(f"Removed connection to secondary {secondary_id}")
