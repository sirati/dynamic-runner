import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .coordinator import SecondaryCoordinator

logger = logging.getLogger(__name__)


class PrimaryLogHandler(logging.Handler):
    """Custom logging handler that sends warnings and errors to primary"""

    def __init__(self, secondary_coordinator: "SecondaryCoordinator"):
        super().__init__()
        self.secondary_coordinator = secondary_coordinator
        self.setLevel(logging.WARNING)
        self._in_emit = False  # Prevent recursive logging

    def emit(self, record: logging.LogRecord) -> None:
        """Send log record to primary if it's a warning or error"""
        # Prevent recursive calls if sending to primary fails and generates another log
        if self._in_emit:
            return

        try:
            self._in_emit = True
            if record.levelno >= logging.WARNING and self.secondary_coordinator.message_router.primary_connection:
                # Don't send logs about message router failures to avoid infinite recursion
                if record.module == "message_router" or "send_to_primary" in record.message:
                    return

                msg = {
                    "type": "secondary_log",
                    "secondary_id": self.secondary_coordinator.secondary_id,
                    "level": record.levelname,
                    "message": self.format(record),
                    "module": record.module,
                    "funcName": record.funcName,
                    "lineno": record.lineno,
                }
                asyncio.create_task(self.secondary_coordinator.message_router.send_to_primary(msg))
        except Exception:
            pass  # Don't let logging errors crash the application
        finally:
            self._in_emit = False
