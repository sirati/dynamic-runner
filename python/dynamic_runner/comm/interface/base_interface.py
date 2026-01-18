from abc import ABC, abstractmethod

from ..proto import Command, Response


class CommunicationInterface(ABC):
    """Abstract base class for communication between manager and worker."""

    @abstractmethod
    def send_command(self, command: Command) -> tuple[bool, str | None]:
        """Send a command to the worker.

        Returns:
            (success, error_message)
        """
        pass

    @abstractmethod
    def send_response(self, response: Response) -> tuple[bool, str | None]:
        """Send a response to the manager.

        Returns:
            (success, error_message)
        """
        pass

    @abstractmethod
    def receive_command(self, blocking: bool = True) -> Command | None:
        """Receive a command from the manager.

        Args:
            blocking: Whether to block waiting for data

        Returns:
            Command object or None if no data available (when non-blocking)
        """
        pass

    @abstractmethod
    def receive_responses(self) -> list[Response]:
        """Receive and parse all available responses from the worker.

        Returns:
            List of Response objects
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the communication channel."""
        pass

    @abstractmethod
    def set_blocking(self, blocking: bool) -> None:
        """Set blocking mode for the communication channel."""
        pass
