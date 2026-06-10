import os
import socket
from pathlib import Path

from ..proto import Command, Response, parse_command, parse_response
from .base_interface import CommunicationInterface
from .line_reader import SocketLineReader


class NamedSocketInterface(CommunicationInterface):
    """Named Unix domain socket implementation for manual worker mode."""

    def __init__(self, socket_path: str | Path, is_server: bool = True):
        """Initialize named socket interface.

        Args:
            socket_path: Path to the Unix domain socket file
            is_server: True if this is the server (manager), False if client (worker)
        """
        self.socket_path = Path(socket_path)
        self.is_server = is_server
        self.socket: socket.socket | None = None
        self.connection: socket.socket | None = None
        # ONE buffered line reader for both blocking and non-blocking
        # command reads, lazily bound to `self.connection` on first
        # read (the server side has no connection until accept). See
        # `line_reader.SocketLineReader`.
        self._line_reader: SocketLineReader | None = None

        if is_server:
            self._setup_server()
        else:
            self._setup_client()

    def _setup_server(self) -> None:
        """Setup server socket (manager side)."""
        # Remove existing socket file if it exists
        if self.socket_path.exists():
            self.socket_path.unlink()

        # Create parent directory if needed
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)

        # Create and bind socket
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(str(self.socket_path))
        self.socket.listen(1)
        self.socket.setblocking(False)

    def _setup_client(self) -> None:
        """Setup client socket (worker side)."""
        # Wait for socket file to exist (with timeout)
        import time

        timeout = 30
        start_time = time.time()

        while not self.socket_path.exists():
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Socket file {self.socket_path} did not appear within {timeout}s")
            time.sleep(0.1)

        # Connect to server
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.connect(str(self.socket_path))
        self.connection = self.socket

    def accept_connection(self) -> bool:
        """Accept incoming connection (server only).

        Returns:
            True if connection was accepted, False if no connection pending
        """
        if not self.is_server or self.connection is not None:
            return False

        try:
            self.socket.setblocking(False)
            conn, _ = self.socket.accept()
            self.connection = conn
            return True
        except BlockingIOError:
            return False
        except (BrokenPipeError, ConnectionResetError, OSError):
            return False

    def send_command(self, command: Command) -> tuple[bool, str | None]:
        """Send a command through the socket."""
        if self.connection is None:
            return (False, "No connection established")

        try:
            self.connection.sendall(command.serialize())
            return (True, None)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            return (False, str(e))

    def send_response(self, response: Response) -> tuple[bool, str | None]:
        """Send a response through the socket."""
        if self.connection is None:
            return (False, "No connection established")

        try:
            self.connection.sendall(response.serialize())
            return (True, None)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            return (False, str(e))

    def receive_command(self, blocking: bool = True) -> Command | None:
        """Receive ONE command from the socket.

        Same contract as ``UnixSocketInterface.receive_command``:
        blocking waits for the next complete line; non-blocking
        returns the next already-buffered complete line or ``None``
        (the ``Task.poll_messages()`` drain). Both modes share the one
        line buffer so framing survives interleaving.
        """
        if self.connection is None:
            return None
        if self._line_reader is None:
            self._line_reader = SocketLineReader(self.connection)
        line = self._line_reader.read_line(blocking=blocking)
        if not line:
            return None
        return parse_command(line)

    def receive_responses(self) -> list[Response]:
        """Receive and parse all available responses from the socket."""
        if self.connection is None:
            # Try to accept connection if we're a server
            if self.is_server:
                self.accept_connection()
            if self.connection is None:
                return []

        try:
            self.connection.setblocking(False)
            data = self.connection.recv(1024)

            if not data:
                return []

            responses_str = data.decode("utf-8").strip().split("\n")
            responses = []

            for response_str in responses_str:
                parsed = parse_response(response_str)
                if parsed is not None:
                    responses.append(parsed)

            return responses

        except BlockingIOError:
            return []
        except (BrokenPipeError, ConnectionResetError, OSError):
            return []
        finally:
            try:
                self.connection.setblocking(True)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

    def close(self) -> None:
        """Close the socket and cleanup."""
        try:
            if self.connection and self.connection != self.socket:
                self.connection.close()
            if self.socket:
                self.socket.close()
            # Remove socket file if we're the server
            if self.is_server and self.socket_path.exists():
                self.socket_path.unlink()
        except Exception:
            pass

    def set_blocking(self, blocking: bool) -> None:
        """Set blocking mode for the socket."""
        if self.connection is None:
            return

        try:
            self.connection.setblocking(blocking)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
