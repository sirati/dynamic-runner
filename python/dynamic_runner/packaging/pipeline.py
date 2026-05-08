"""Top-level SLURM pipeline driver.

The orchestration body lives in Rust (`crates/dynrunner-slurm/src/pipeline.rs`
+ `crates/dynrunner-pyo3/src/slurm/pipeline.rs`); this Python module is a
thin shim that:

* keeps the public callable `run_slurm_pipeline` importable at this path
  (consumers and `dynamic_runner.run` import via
  ``from .packaging import run_slurm_pipeline``),
* preserves the small argparse-aware helpers (`_make_run_id`,
  `_validate_slurm_args`, `_make_slurm_config`) that the Rust
  orchestrator calls back into via PyO3 imports — these helpers
  manipulate ``argparse.Namespace`` and ``pathlib.Path`` shapes that
  remain Python-side concerns.

The argparse / Path helpers are kept here (rather than ported to Rust)
because:

* Rust has no idiomatic ``argparse.Namespace`` analogue; we'd have to
  marshal a dynamic dict across the FFI boundary on every read.
* Tilde expansion against ``gateway.remote_home`` requires the gateway
  pyclass which is itself an L2.A migration concern; once L2.A lands
  with ``RustSshGateway.remote_home`` exposed, ``_make_slurm_config``
  reduces to a one-line call into the Rust ``SlurmConfig::from_args``
  factory.

That follow-up is part of L2.H (final cleanup), not L2.G.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from .slurm_config import SlurmConfig

# Re-export the Rust orchestration as the canonical
# ``run_slurm_pipeline`` callable. Consumers that previously did
# ``from dynamic_runner.packaging import run_slurm_pipeline`` (or
# ``from dynamic_runner.packaging.pipeline import run_slurm_pipeline``)
# get the Rust-implementation transparently.
from .._native import run_slurm_pipeline as run_slurm_pipeline  # noqa: F401


def _slurm_already_spawned(*_args: object, **_kwargs: object) -> None:
    """No-op spawn_secondary callback for SLURM mode.

    SLURM did the actual spawning, so the Rust runner's
    ``spawn_secondary`` callback isn't responsible for any subprocess.
    Returning ``None`` tells the Rust side it doesn't own a process to
    clean up at the end.

    Defined at module scope so the Rust orchestrator can fetch it via
    ``py.import("dynamic_runner.packaging.pipeline")``-and-getattr,
    avoiding an inline ``py.eval`` that would otherwise be needed for
    a one-line lambda.
    """
    return None

logger = logging.getLogger(__name__)


def _make_run_id() -> str:
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _validate_slurm_args(args: argparse.Namespace, log: logging.Logger) -> bool:
    """Cheap pre-flight check before we touch the gateway."""
    if not args.gateway:
        log.error("--gateway is required when --multi-computer slurm is enabled")
        return False
    if not args.packaging:
        log.error("--packaging is required when --multi-computer slurm is enabled")
        return False
    if args.packaging != "podman":
        log.error(
            f"--packaging={args.packaging!r} is not supported. "
            "Only 'podman' works in SLURM batch jobs (docker requires user-session systemd)."
        )
        return False
    if not args.slurm_root_folder:
        home = Path.home()
        suggestions = [home / "slurm", home / "BIG" / "slurm"]
        log.error("--slurm-root-folder is required when --multi-computer slurm is enabled")
        log.error(f"Suggested locations: {', '.join(str(s) for s in suggestions)}")
        return False
    return True


def _make_slurm_config(args: argparse.Namespace, gateway: object) -> SlurmConfig:
    """Build the SlurmConfig with `~` expanded against the gateway's remote home.

    Expanding once at this entry point means every downstream path
    constructor (`get_image_dir`, `get_log_dir`, …) and every shell
    command emitted from `job_manager` / `layered_transfer` sees an
    absolute path. Without this, `shlex.quote("~/...")` single-quotes
    the path so bash never tilde-expands it, and `mkdir -p` creates a
    literal `~` directory under `$HOME` while `scp` (which expands `~`
    server-side) targets the absolute path — the two paths diverge
    and uploads land in the wrong place.
    """
    root = str(args.slurm_root_folder)
    remote_home = getattr(gateway, "remote_home", None)
    if root.startswith("~") and remote_home:
        root = root.replace("~", str(remote_home), 1)
    overrides: dict[str, object] = {}
    if getattr(args, "slurm_time_limit", None):
        overrides["time_limit"] = args.slurm_time_limit
    if getattr(args, "slurm_partition", None):
        overrides["partition"] = args.slurm_partition
    if getattr(args, "slurm_cpus_per_task", None):
        overrides["cpus_per_task"] = args.slurm_cpus_per_task
    if getattr(args, "source_already_staged", None):
        overrides["prestaged_src_bins_path"] = args.source_already_staged
    return SlurmConfig(
        root_folder=Path(root),
        image_subfolder=args.slurm_image_subfolder,
        output_subfolder=args.slurm_output_subfolder,
        log_subfolder=args.slurm_log_subfolder,
        notify_email=args.slurm_notify_email,
        **overrides,
    )
