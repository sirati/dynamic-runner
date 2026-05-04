from dataclasses import dataclass
from pathlib import Path


@dataclass
class SlurmConfig:
    """Configuration for SLURM execution"""

    root_folder: str | Path
    image_subfolder: str = "image_bin"
    output_subfolder: str = "out"
    log_subfolder: str = "log"
    notify_email: str | None = None
    partition: str = "All"
    nodes: int = 1
    cpus_per_task: int = 14
    memory_per_node: str = "64G"
    time_limit: str = "48:00:00"
    # Pre-staged source override. When set, the wrapper script bind-mounts
    # this host path into the container at /app/src-network instead of
    # the primary's staging directory (and the primary skips the
    # queue_initial_staging pass entirely). Absolute paths used as-is;
    # relative paths resolved against `root_folder`.
    prestaged_src_bins_path: str | None = None

    def get_image_dir(self) -> str:
        """Get full path to image directory"""
        return f"{self.root_folder}/{self.image_subfolder}"

    def get_output_dir(self) -> str:
        """Get full path to output directory"""
        return f"{self.root_folder}/{self.output_subfolder}"

    def get_log_dir(self) -> str:
        """Get full path to log directory"""
        return f"{self.root_folder}/{self.log_subfolder}"

    def get_srcbins_dir(self) -> str:
        """Get full path to source binaries directory"""
        return f"{self.get_image_dir()}/srcbins"

    def get_srcbins_mount_source(self) -> str:
        """Path to bind-mount into the container at /app/src-network.

        Returns `prestaged_src_bins_path` (absolute, or resolved against
        `root_folder` for relative paths) when set; otherwise the
        primary-staging directory under the image dir.
        """
        if self.prestaged_src_bins_path is None:
            return self.get_srcbins_dir()
        path = self.prestaged_src_bins_path
        if path.startswith("/"):
            return path
        return f"{self.root_folder}/{path}"


def validate_slurm_config(config: SlurmConfig, gateway=None) -> None:
    """Validate SLURM configuration

    Args:
        config: SLURM configuration
        gateway: Optional gateway instance to check remote folder existence
    """
    if not config.root_folder:
        raise ValueError("SLURM root folder is required")

    # If gateway is provided, check on remote; otherwise just validate path is set
    if gateway and hasattr(gateway, "file_exists"):
        if not gateway.file_exists(config.root_folder):
            # Use remote home for suggestions
            remote_home = getattr(gateway, "remote_home", "~")
            suggestions = [f"{remote_home}/slurm", f"{remote_home}/BIG/slurm"]
            suggestion_str = ", ".join(suggestions)
            raise ValueError(
                f"SLURM root folder does not exist on gateway: {config.root_folder}\nSuggested locations: {suggestion_str}"
            )
