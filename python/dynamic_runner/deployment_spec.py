"""Task-package deployment metadata for the SLURM packaging path.

Owned by the consumer (the task package), passed to
:func:`dynamic_runner.run` as the ``deployment`` argument. The framework
treats every field as opaque — the consumer's flake / image / module
layout is not the framework's concern. This is the *only* place
task-package identity crosses the framework boundary; nothing in
``dynamic_runner.packaging`` should reach for a task name another way.

When ``--multi-computer slurm`` is requested, ``deployment`` is required
and supplies four facts the framework cannot derive on its own:

1. **secondary_module** — the Python module the secondary process
   invokes (``python -m {secondary_module} --secondary ...``). Same
   string is used for the local-subprocess spawn path so consumers
   express the module name once.
2. **image_name / image_tag** — the docker tag the consumer's flake
   produces. Image artifact tar / sha256 marker basenames are derived
   from ``image_name``; the SLURM job name prefix defaults to it too.
3. **nix_build_target** — the flake target ``nix build`` runs to
   produce the docker-archive. Resolved against the cwd at
   pipeline-start time (the consumer repo's flake must be the cwd's
   flake).

The single-process and plain-local execution modes ignore
``deployment`` entirely; consumers that never use SLURM may pass
``None`` (the default).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TaskDeploymentSpec:
    """Consumer-supplied deployment identity for SLURM packaging."""

    # Python module name invoked as ``python -m {secondary_module}``
    # for both the local-subprocess spawn (``--multi-computer local``)
    # and the SLURM secondary wrapper.
    secondary_module: str

    # Container image identity. ``image_name`` is the docker repo
    # name (e.g. ``"asm-tokenizer"``); ``image_tag`` is the tag
    # (e.g. ``"latest"``). The framework derives:
    #   - image tar basename:    ``{image_name}.tar``
    #   - sha256 marker basename: ``{image_name}.sha256``
    #   - SLURM job name prefix:  ``{image_name}`` (override below).
    image_name: str
    image_tag: str = "latest"

    # Nix flake target that produces the docker-archive. Built via
    # ``nix build {nix_build_target} --out-link <link>`` from the
    # consumer's cwd.
    nix_build_target: str = ".#dockerImage"

    # Override the SLURM job name prefix when it should differ from
    # ``image_name`` (rare). Final job name is
    # ``{prefix}-{secondary_id}``.
    slurm_job_name_prefix: str | None = None

    @property
    def effective_job_name_prefix(self) -> str:
        """SLURM job name prefix, defaulting to ``image_name``."""
        return self.slurm_job_name_prefix or self.image_name

    @property
    def image_tar_basename(self) -> str:
        """Filename of the docker-archive on disk and on the gateway."""
        return f"{self.image_name}.tar"

    @property
    def image_marker_basename(self) -> str:
        """Filename of the per-image sha256 marker on the gateway."""
        return f"{self.image_name}.sha256"
