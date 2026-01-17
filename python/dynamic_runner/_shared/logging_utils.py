import logging
from pathlib import Path
from typing import Optional


class WarningCounterHandler(logging.Handler):
    def __init__(self, level: int = logging.WARNING):
        super().__init__(level)
        self.warning_count = 0
        self.error_count = 0
        self._loggers: list[logging.Logger] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.ERROR:
            self.error_count += 1
        elif record.levelno >= logging.WARNING:
            self.warning_count += 1

    def reset(self) -> None:
        self.warning_count = 0
        self.error_count = 0

    def get_counts(self) -> tuple[int, int]:
        return self.warning_count, self.error_count

    def attach_to_logger(self, logger: logging.Logger) -> None:
        logger.addHandler(self)
        if logger not in self._loggers:
            self._loggers.append(logger)

    def unregister(self) -> None:
        for logger in self._loggers:
            logger.removeHandler(self)
        self._loggers.clear()


def remove_stream_handlers(logger: logging.Logger) -> None:
    """Remove all stream handlers (stdout/stderr) from logger to avoid blocking."""
    handlers_to_remove = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    for handler in handlers_to_remove:
        logger.removeHandler(handler)


def setup_logger(
    name: str,
    level: int | None = None,
) -> tuple[logging.Logger, WarningCounterHandler]:
    """Create a logger that inherits from root logger configuration."""
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)

    counter_handler = WarningCounterHandler()
    counter_handler.attach_to_logger(logger)

    return logger, counter_handler


def setup_file_logger(
    name: str,
    log_file_path: Path,
    level: int = logging.INFO,
    console: bool = True,
    console_format: str | None = None,
) -> logging.Logger:
    """Create a logger with file handler and optional console handler.

    Args:
        name: Logger name
        log_file_path: Path to log file
        level: Logging level (default: INFO)
        console: Whether to add console handler (default: True)
        console_format: Custom format for console handler. If None, uses standard format.

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(levelname)s | %(asctime)s,%(msecs)03d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        if console_format is None:
            console_format = "%(levelname)s | %(asctime)s,%(msecs)03d | %(message)s"
        console_formatter = logging.Formatter(console_format, datefmt="%Y-%m-%d %H:%M:%S")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger
