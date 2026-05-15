"""Data-only spec a `spawn_secondary` callback returns to the Rust primary.

Single concern: carry the argv (+ optional env) the Rust
``RustPrimaryCoordinator`` needs to spawn one secondary subprocess.
Nothing in this module touches a live process — process lifecycle
(spawn/wait/kill) lives in Rust, see
``crates/dynrunner-pyo3/src/managers/subprocess_spec.rs`` and
``crates/dynrunner-pyo3/src/managers/primary.rs``.

Rationale (per ``feedback_features_in_rust_python_is_bridge``): Python
is the CLI/config bridge. Constructing argv from
``deployment.secondary_module`` and the parsed CLI args is legitimate
Python concern (the spawned process IS Python; the entry-point string
assembly stays where the deployment metadata lives). Owning the
``subprocess.Popen`` handle across calls, however, is runtime
lifecycle — that belongs to Rust. So Python returns this spec and
Rust spawns + tracks the child.

The Rust side reads ``argv`` and ``env`` via attribute access, so any
object with those two attributes works; this dataclass is the
canonical shape. A ``None`` return from ``spawn_secondary`` is also
valid and means "no subprocess to spawn here" — the SLURM mode
(``packaging.pipeline._slurm_already_spawned``) uses this to declare
that the wrapper script + sbatch did the spawning out of band.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SubprocessSpec:
    """Plan for one secondary subprocess. Returned by the Python
    ``spawn_secondary`` callback; consumed by Rust which spawns +
    owns the resulting ``std::process::Child``.

    Args:
        argv: Command line, ``argv[0]`` is the executable path. Must
            be non-empty. Matches the ``subprocess.Popen([...])``
            shape Python callers already build.
        env: Optional environment override. ``None`` (default) means
            inherit the parent process's environment. A dict REPLACES
            the environment entirely — the caller is responsible for
            seeding ``os.environ.copy()`` first when they want
            inheritance plus extras (matches the Python
            ``subprocess.Popen(..., env=env)`` semantic).
    """

    argv: list[str]
    env: dict[str, str] | None = field(default=None)

    def __post_init__(self) -> None:
        if not self.argv:
            raise ValueError(
                "SubprocessSpec.argv must contain at least one element "
                "(the executable path)"
            )
