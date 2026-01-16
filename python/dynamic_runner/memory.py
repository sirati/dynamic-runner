import math
import os
import subprocess


def estimate_memory(binary_size: int) -> int:
    """
    Estimate memory consumption using a power law model.

    Model: RAM (MiB) = 430.870 × size^1.051 + 260.15

    R² = 0.9866, RMSE = 203.66 MiB

    Args:
        binary_size: File size in bytes

    Returns:
        Estimated RAM usage in bytes (rounded up)
    """
    mb = binary_size / 1024 / 1024  # Convert to MiB

    # Power law coefficients (MiB units)
    a = 430.870
    b = 1.051
    c = 260.15
    rmse = 203.66

    # we add the RMSE as we want to rather overestimate
    ram_mb = a * (mb**b) + c + rmse

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
