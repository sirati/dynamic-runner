import socket

from ..proto import Command, Response, parse_command, parse_response
from .base_interface import CommunicationInterface
from .line_reader import SocketLineReader


class UnixSocketInterface(CommunicationInterface):
    """Unix domain socket implementation of communication interface."""

    def __init__(self, sock: socket.socket):
        self.socket = sock
        # ONE buffered line reader for both blocking and non-blocking
        # command reads — see `line_reader.SocketLineReader` for why
        # the two paths must share a buffer (poll_messages interleaves
        # with the runtime's blocking command loop).
        self._line_reader = SocketLineReader(sock)

    def send_command(self, command: Command) -> tuple[bool, str | None]:
        """Send a command through the socket."""
        try:
            self.socket.sendall(command.serialize())
            return (True, None)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            return (False, str(e))

    def send_response(self, response: Response) -> tuple[bool, str | None]:
        """Send a response through the socket."""
        try:
            self.socket.sendall(response.serialize())
            return (True, None)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            return (False, str(e))

    def receive_command(self, blocking: bool = True) -> Command | None:
        """Receive ONE command from the socket.

        ``blocking=True`` waits for the next complete line (the
        runtime loop's resting read); ``blocking=False`` returns the
        next ALREADY-BUFFERED complete line or ``None`` without
        waiting (the ``Task.poll_messages()`` drain — callers loop
        until ``None``). Both modes share the one line buffer, so a
        frame spanning recv chunks or two frames sharing a chunk are
        framed correctly either way.
        """
        line = self._line_reader.read_line(blocking=blocking)
        if not line:
            return None
        return parse_command(line)

    def receive_responses(self) -> list[Response]:
        """Receive and parse all available responses from the socket."""
        try:
            self.socket.setblocking(False)
            data = self.socket.recv(1024)

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
                self.socket.setblocking(True)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

    def close(self) -> None:
        """Close the socket."""
        try:
            self.socket.close()
        except Exception:
            pass

    def set_blocking(self, blocking: bool) -> None:
        """Set blocking mode for the socket."""
        try:
            self.socket.setblocking(blocking)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
