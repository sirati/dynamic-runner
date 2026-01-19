"""Common file utilities for primary coordinators.

Shared functionality for file hashing and initial assignment message creation.
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from ...binary_info import BinaryInfo

logger = logging.getLogger(__name__)


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of file.

    Args:
        file_path: Path to file

    Returns:
        Hex hash string
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_task_hash(binary: BinaryInfo) -> str:
    """Compute unique hash for a task.

    Args:
        binary: Binary info

    Returns:
        Task hash string
    """
    hash_input = (
        f"{binary.path}:{binary.identifier.binary_name}:{binary.identifier.platform}:"
        f"{binary.identifier.compiler}:{binary.identifier.version}:{binary.identifier.opt_level}"
    )
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]


async def send_initial_assignment_file_ready(
    secondary_id: str,
    file_ready_list: list[dict[str, Any]],
    worker_assignments: list[dict[str, Any]],
    secondary_info: dict[str, Any],
    message_router: Any,
    quic_transport: Any,
) -> None:
    """Send initial assignment with file_ready mode.

    Args:
        secondary_id: Secondary identifier
        file_ready_list: List of file_ready entries
        worker_assignments: List of worker assignments
        secondary_info: Secondary connection info (unused)
        message_router: Message router (unused)
        quic_transport: QUIC transport for accessing WebSocket connections
    """
    assignment_msg = {
        "type": "initial_assignment",
        "secondary_id": secondary_id,
        "file_ready": file_ready_list,
        "worker_assignments": worker_assignments,
    }

    # Get WebSocket connection from QUIC transport's wss_connections
    if secondary_id not in quic_transport.wss_connections:
        logger.error(f"No WebSocket connection for secondary: {secondary_id}")
        return

    connection = quic_transport.wss_connections[secondary_id]
    await connection.send(json.dumps(assignment_msg))
    logger.debug(f"Sent file_ready assignment to {secondary_id}: {len(file_ready_list)} files")


async def send_initial_assignment_zip(
    secondary_id: str,
    zip_files_info: list[dict[str, Any]],
    worker_assignments: list[dict[str, Any]],
    secondary_info: dict[str, Any],
    message_router: Any,
    quic_transport: Any,
) -> None:
    """Send initial assignment with ZIP transfer mode.

    Args:
        secondary_id: Secondary identifier
        zip_files_info: List of ZIP file info dicts
        worker_assignments: List of worker assignments
        secondary_info: Secondary connection info (unused)
        message_router: Message router (unused)
        quic_transport: QUIC transport for accessing WebSocket connections
    """
    zip_assignments = []
    for zip_info in zip_files_info:
        zip_assignments.append(
            {
                "zip_name": zip_info["zip_name"],
                "zip_path": zip_info["zip_path"],
                "zip_hash": zip_info["zip_hash"],
                "files": zip_info["files"],
            }
        )

    assignment_msg = {
        "type": "initial_assignment",
        "secondary_id": secondary_id,
        "zip_files": zip_assignments,
        "worker_assignments": worker_assignments,
    }

    # Get WebSocket connection from QUIC transport's wss_connections
    if secondary_id not in quic_transport.wss_connections:
        logger.error(f"No WebSocket connection for secondary: {secondary_id}")
        return

    connection = quic_transport.wss_connections[secondary_id]
    await connection.send(json.dumps(assignment_msg))
    logger.info(f"Sent initial assignment to {secondary_id}: {len(zip_assignments)} ZIP files")
