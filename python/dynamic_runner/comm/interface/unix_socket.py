import socket

from ..proto import Command, Response, parse_command, parse_response
from .base_interface import CommunicationInterface


class UnixSocketInterface(CommunicationInterface):
    """Unix domain socket implementation of communication interface."""

    def __init__(self, sock: socket.socket):
        self.socket = sock
        self.socket_file = None

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
        """Receive a command from the socket (always non-blocking in practice)."""
        try:
            self.socket.setblocking(False)
            data = self.socket.recv(1024)
            if not data:
                return None
            line = data.decode("utf-8").strip()
            return parse_command(line)
        except BlockingIOError:
            return None
        except (BrokenPipeError, ConnectionResetError, OSError):
            return None
        finally:
            try:
                self.socket.setblocking(True)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass

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
            if self.socket_file:
                self.socket_file.close()
            self.socket.close()
        except Exception:
            pass

    def set_blocking(self, blocking: bool) -> None:
        """Set blocking mode for the socket."""
        try:
            self.socket.setblocking(blocking)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
