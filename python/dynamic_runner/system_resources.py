import multiprocessing


def get_cpu_count() -> int:
    """Get the number of CPU cores available."""
    return multiprocessing.cpu_count()


def get_available_memory() -> int:
    """Get available memory in bytes."""
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    # MemAvailable is in kB
                    return int(line.split()[1]) * 1024
    except Exception:
        pass

    # Fallback: use total memory
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass

    # Default to 8GB if we can't determine
    return 8 * 1024 * 1024 * 1024


def parse_cores(cores_str: str) -> int:
    """Parse cores parameter (int, +int, or -int)."""
    total_cores = get_cpu_count()

    if cores_str.startswith("+"):
        delta = int(cores_str[1:])
        return min(total_cores, total_cores + delta)
    elif cores_str.startswith("-"):
        delta = int(cores_str[1:])
        return max(1, total_cores - delta)
    else:
        return max(1, int(cores_str))


def parse_memory(memory_str: str) -> int:
    """Parse memory parameter with M or G suffix, supports +/- relative notation."""
    available_memory = get_available_memory()

    # Check for relative notation
    if memory_str.startswith("+") or memory_str.startswith("-"):
        is_relative = True
        sign = 1 if memory_str.startswith("+") else -1
        memory_str = memory_str[1:]
    else:
        is_relative = False
        sign = 1

    if memory_str.endswith("G"):
        value = int(memory_str[:-1]) * 1024 * 1024 * 1024
    elif memory_str.endswith("M"):
        value = int(memory_str[:-1]) * 1024 * 1024
    else:
        raise ValueError(f"Memory must end with 'M' or 'G': {memory_str}")

    if is_relative:
        return max(1024 * 1024 * 1024, available_memory + sign * value)
    else:
        return value
