from ..proto import Command, Response
from .base_interface import CommunicationInterface


class NoopInterface(CommunicationInterface):
    """No-op implementation of communication interface for non-worker modes."""

    def send_command(self, command: Command) -> tuple[bool, str | None]:
        """No-op: always succeeds."""
        return (True, None)

    def send_response(self, response: Response) -> tuple[bool, str | None]:
        """No-op: always succeeds."""
        return (True, None)

    def receive_command(self, blocking: bool = True) -> Command | None:
        """No-op: always returns None."""
        return None

    def receive_responses(self) -> list[Response]:
        """No-op: always returns empty list."""
        return []

    def close(self) -> None:
        """No-op: nothing to close."""
        pass

    def set_blocking(self, blocking: bool) -> None:
        """No-op: nothing to set."""
        pass
