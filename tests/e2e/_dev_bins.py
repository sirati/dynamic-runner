"""Dev-convenience SLURM binary sources for editable installs.

Single concern: when the e2e driver runs against an editable /
``maturin develop`` install (no nix-built wheel), the
musl-static ``dynrunner-slurm-shutdown`` / ``dynrunner-slurm-wrapper``
binaries the SLURM dispatch path needs are NOT bundled under
``site-packages`` — only the nix wheel's ``postInstall`` drops them
there (see ``nix/wheel.nix``). The framework's resolution policy
(:func:`dynamic_runner._shutdown_manager.bundled_binary_path` /
:func:`dynamic_runner._wrapper_manager.bundled_binary_path`) is
deliberately two-source — operator env-var override > wheel-bundled
artifact > fail-loud ``None`` — and adding a third discovery path
(``nix build`` probing) into the *framework* is explicitly out of
scope there. So this dev convenience lives here, in the harness: it
``nix build``s the sibling ``.#dynrunner-slurm-{shutdown,wrapper}``
packages (the exact artefacts the wheel embeds) and exports the
``DYNRUNNER_SLURM_{SHUTDOWN,WRAPPER}_BIN_SOURCE`` operator-override
env vars — but ONLY when unset, so an operator who set them keeps
precedence.

This module does NOT know which scenarios will run; it is reached
only in slurm mode. The flake build goes through ``nix build`` (the
same toolchain ``_cluster.py`` shells out to), never raw cargo.

The env-var names and binary basenames are read from the framework's
``_shutdown_manager`` / ``_wrapper_manager`` modules so there is a
single source of truth — the flake attribute name for each package
equals its bundled-binary basename by construction (see ``flake.nix``
``packages.dynrunner-slurm-{shutdown,wrapper}`` and ``nix/wheel.nix``).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from dynamic_runner._shutdown_manager import (
    BUNDLED_BINARY_NAME as _SHUTDOWN_BIN_NAME,
    ENV_VAR as _SHUTDOWN_ENV_VAR,
)
from dynamic_runner._wrapper_manager import (
    BUNDLED_BINARY_NAME as _WRAPPER_BIN_NAME,
    ENV_VAR as _WRAPPER_ENV_VAR,
)


# Each tuple is ``(operator-override env var, nix flake attribute,
# bundled-binary basename)``. The flake attr equals the basename by
# construction (``flake.nix`` names the sibling outputs after the
# binaries), but we keep them as separate fields so the contract is
# explicit at the call site rather than implied by string equality.
_DEV_BIN_SOURCES = (
    (_SHUTDOWN_ENV_VAR, _SHUTDOWN_BIN_NAME, _SHUTDOWN_BIN_NAME),
    (_WRAPPER_ENV_VAR, _WRAPPER_BIN_NAME, _WRAPPER_BIN_NAME),
)


def _nix_build_bin(framework_root: Path, flake_attr: str, basename: str) -> Path:
    """``nix build .#<flake_attr>`` against the framework flake; return
    the ``<out>/bin/<basename>`` path.

    ``--no-link --print-out-paths`` keeps the store path off the
    filesystem-as-symlink while still emitting it on stdout. The
    out path is a derivation whose ``bin/`` holds exactly the
    musl-static artefact the wheel embeds (see ``nix/wheel.nix``'s
    ``postInstall``), so ``<out>/bin/<basename>`` is deterministic.
    """
    out = subprocess.run(
        [
            "nix",
            "build",
            "--no-link",
            "--print-out-paths",
            f"{framework_root}#{flake_attr}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not out:
        raise RuntimeError(
            f"nix build {framework_root}#{flake_attr} printed no out path"
        )
    bin_path = Path(out) / "bin" / basename
    if not bin_path.exists():
        raise RuntimeError(
            f"nix build {framework_root}#{flake_attr} produced {out} but "
            f"{bin_path} does not exist"
        )
    return bin_path


def ensure_slurm_bin_sources(framework_root: Path) -> None:
    """Auto-set the SLURM bin-source env vars for editable installs.

    For each of the shutdown / wrapper binaries: if its
    operator-override env var
    (``DYNRUNNER_SLURM_{SHUTDOWN,WRAPPER}_BIN_SOURCE``) is already
    set, leave it untouched (operator precedence). Otherwise
    ``nix build`` the sibling ``.#dynrunner-slurm-{shutdown,wrapper}``
    package and export the env var to the built ``bin/`` artefact, so
    the framework's ``bundled_binary_path`` override branch resolves
    it without a nix-built wheel under ``site-packages``.

    A no-op when both env vars are already set (e.g. a nix-built
    wheel run, or an operator who pinned their own builds).
    """
    for env_var, flake_attr, basename in _DEV_BIN_SOURCES:
        if os.environ.get(env_var):
            print(
                f"[dev-bins] {env_var} already set "
                f"({os.environ[env_var]}); leaving operator override",
                flush=True,
            )
            continue
        print(
            f"[dev-bins] {env_var} unset; nix build "
            f"{framework_root}#{flake_attr} (editable-install convenience)",
            flush=True,
        )
        bin_path = _nix_build_bin(framework_root, flake_attr, basename)
        os.environ[env_var] = str(bin_path)
        print(f"[dev-bins] {env_var}={bin_path}", flush=True)


__all__ = ["ensure_slurm_bin_sources"]
