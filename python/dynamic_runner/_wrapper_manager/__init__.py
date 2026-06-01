"""Bundled musl-static ``dynrunner-slurm-wrapper`` binary.

In nix-built wheels, the binary lives at
``importlib.resources.files("dynamic_runner") / "_wrapper_manager" /
"dynrunner-slurm-wrapper"`` thanks to the wheel derivation's
``postInstall`` step (see ``nix/wheel.nix``). In editable / non-nix
installs the binary is absent; callers must set
``DYNRUNNER_SLURM_WRAPPER_BIN_SOURCE`` to point at a local build of
``slurm-wrapper/`` (``cargo build --release`` inside that subtree
or ``nix build .#`` against ``slurm-wrapper/flake.nix``).

Resolution policy is centralised in :func:`bundled_binary_path` —
env-var override > bundled artifact > ``None`` (caller surfaces a
fail-loud error). No additional fallback paths (PATH lookup,
in-tree ``target/`` probing) are considered intentional design:
the framework owns exactly two source-of-truth locations for the
binary, and ambiguity beyond those two would mask misconfigured
deployments.
"""
from __future__ import annotations

import os
from importlib import resources
from pathlib import Path


#: Basename of the bundled binary (matches the Cargo `[[bin]]` name
#: in ``slurm-wrapper/wrapper/Cargo.toml`` and
#: ``WRAPPER_BIN_REMOTE_BASENAME`` on the Rust side).
BUNDLED_BINARY_NAME = "dynrunner-slurm-wrapper"

#: Operator-override environment variable. Highest-priority source
#: in :func:`bundled_binary_path` so editable installs and explicit
#: testing can point at a locally-built binary without rebuilding the
#: wheel. Exported here so callers (``packaging.job_manager``) can
#: reference the same name in error messages.
ENV_VAR = "DYNRUNNER_SLURM_WRAPPER_BIN_SOURCE"


def bundled_binary_path() -> Path | None:
    """Resolve the path to the SLURM secondary-wrapper binary.

    Resolution order:

    1. ``DYNRUNNER_SLURM_WRAPPER_BIN_SOURCE`` env var (operator
       override). When set, the named file is used verbatim; a
       set-but-missing path returns ``None`` so the caller surfaces
       the misconfiguration loudly rather than falling through to
       the bundled artifact (the operator's explicit override should
       not be silently shadowed).
    2. ``importlib.resources.files("dynamic_runner") /
       "_wrapper_manager" / "dynrunner-slurm-wrapper"`` (bundled
       in nix-built wheels via ``nix/wheel.nix``'s ``postInstall``).

    Returns ``None`` when neither source resolves to an existing
    file on disk. Callers (today:
    ``SlurmJobManager.upload_wrapper_binary``) should surface a loud
    error in that case — the framework's SLURM dispatch path uploads
    this binary so the per-job wrapper stub can ``exec`` it to run the
    full secondary lifecycle.
    """
    override = os.environ.get(ENV_VAR)
    if override:
        candidate = Path(override)
        return candidate if candidate.exists() else None

    try:
        bundled = resources.files("dynamic_runner") / "_wrapper_manager" / BUNDLED_BINARY_NAME
    except (ImportError, ModuleNotFoundError, FileNotFoundError):
        return None

    # ``files()`` returns a ``Traversable``; for nix-built wheels the
    # underlying resource is a real on-disk file (no zipimport), so a
    # direct ``Path(str(...))`` conversion is correct. The wheel is
    # installed into the nix store as a tree of files, never as a zip
    # archive, so the ``as_file`` contextmanager is not required.
    bundled_path = Path(str(bundled))
    return bundled_path if bundled_path.exists() else None
