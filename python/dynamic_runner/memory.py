import math
import os
import subprocess


def estimate_memory(binary_size: int) -> int:
    """
    Estimate memory consumption using a power law model.

    Model: RAM (MiB) = 301.65 * file_size^0.915 + 84.02

    Args:
        binary_size: File size in bytes

    Returns:
        Estimated RAM usage in bytes (rounded up)
    """
    mb = binary_size / 1024 / 1024  # Convert to MiB

    # Power law coefficients (MiB units)
    # a = 301.65
    # b = 0.915
    # c = 84.02
    a = 110.516624
    b = 1.368032
    c = 204.066149

    ram_mb = a * (mb**b) + c

    return math.ceil(ram_mb * 1024 * 1024)


def get_actual_memory_usage() -> int:
    """Get current process tree memory usage."""
    try:
        pid = os.getpid()
        result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
        if result.returncode == 0:
            # RSS is in KB
            return int(result.stdout.strip()) * 1024
    except Exception:
        pass
    return 0


def get_free_system_memory() -> int:
    """Get free system memory in bytes."""
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    # MemAvailable is in KB
                    kb = int(line.split()[1])
                    return kb * 1024
    except Exception:
        pass
    return 0
