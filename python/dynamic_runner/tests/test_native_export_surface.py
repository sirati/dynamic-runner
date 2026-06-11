"""Seam test for the `_native` -> package re-export surface.

The bug this guards against (#414, 2026-06): commit `12a7b879`
("feat(cli): expose respawn policy via PyRespawnPolicy and four CLI
flags") added the `RespawnPolicy` / `PyMultiProcessSpawner` pyclasses
to the compiled extension (`m.add_class::<…>()` in
`crates/dynrunner-pyo3/src/lib.rs`) and wired `run.py` to construct
them via `import dynamic_runner as _rs; _rs.RespawnPolicy.…`, but
NEVER added either name to the `from ._native import (…)` block in
`python/dynamic_runner/__init__.py`. The class is reachable as
`dynamic_runner._native.RespawnPolicy` yet absent from the public
`dynamic_runner` namespace, so dispatching `--respawn-policy
on-secondary-death` crashed in production with
``AttributeError: module 'dynamic_runner' has no attribute
'RespawnPolicy'`` (SLURM pipeline `getattr` + in-process
`_rs.RespawnPolicy`).

The single concern guarded here: EVERY native symbol that `run.py`
reaches through the `import dynamic_runner as _rs` alias must be
re-exported from `__init__.py`'s `from ._native import (…)` block.
This is a STATIC check over the two source files' ASTs — it needs no
compiled `_native`, so it runs in the bare `nix develop` shell like
the rest of this suite (`test_cli_api.py` / `test_spawn_secondary.py`)
while still catching the exact "added a pyclass, forgot the
re-export" gap class.

unittest-based + stdlib-only (no pytest, no `_native`).
"""

from __future__ import annotations

import ast
import pathlib
import unittest


_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _native_reexport_names() -> set[str]:
    """Names pulled in by `__init__.py`'s `from ._native import (…)`.

    Parsed from source (not by importing the package) so the assertion
    holds without the compiled extension loaded.
    """
    tree = ast.parse((_PACKAGE_ROOT / "__init__.py").read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "_native":
            names |= {alias.name for alias in node.names}
    return names


def _rs_attribute_refs(relpath: str) -> set[str]:
    """Every `<alias>.<attr>` accessed in `relpath`, where `<alias>` is
    bound by `import dynamic_runner as <alias>`.

    AST-based so it never matches `_rs.run_*`-style text inside
    docstrings/comments (a regex over the source would).
    """
    tree = ast.parse((_PACKAGE_ROOT / relpath).read_text())
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "dynamic_runner" and alias.asname:
                    aliases.add(alias.asname)
    attrs: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in aliases
        ):
            attrs.add(node.attr)
    return attrs


class NativeExportSurfaceTests(unittest.TestCase):
    def test_every_rs_ref_in_run_py_is_reexported(self) -> None:
        # `run.py` is the sole module that reaches native symbols through
        # the `import dynamic_runner as _rs` alias (cli.py / cli_main.py
        # carry no such alias). If a future module grows one, add it here.
        referenced = _rs_attribute_refs("run.py")
        reexported = _native_reexport_names()
        missing = referenced - reexported
        self.assertEqual(
            missing,
            set(),
            "run.py references native symbol(s) via `import dynamic_runner "
            "as _rs` that __init__.py never re-exports from ._native: "
            f"{sorted(missing)}. Add each to the `from ._native import (…)` "
            "block AND `__all__` (this is the #414 RespawnPolicy gap class).",
        )

    def test_respawn_policy_and_spawner_present(self) -> None:
        # Focused pin on the two #414 symbols so a regression names them
        # directly, independent of the run.py scan above.
        reexported = _native_reexport_names()
        # `RespawnPolicy.on_secondary_death(...)` is the constructor
        # behind `--respawn-policy on-secondary-death`; the spawner is the
        # adapter `run.py` pairs with it. Both must cross the package seam.
        for symbol in ("RespawnPolicy", "PyMultiProcessSpawner"):
            self.assertIn(
                symbol,
                reexported,
                f"{symbol} is added to `_native` (lib.rs `m.add_class`) but "
                "not re-exported from `dynamic_runner.__init__`; "
                "`--respawn-policy on-secondary-death` crashes without it.",
            )

    def test_run_py_refs_are_in_dunder_all(self) -> None:
        # The package surface is two lists (`from ._native import (…)` +
        # `__all__`); a name can land in one and not the other. The native
        # symbols `run.py` constructs through `_rs.` are public config
        # classes/constructors, so they must also be advertised in
        # `__all__` — not merely importable. (Internal bridge re-exports
        # like `py_log`, consumed package-internally via `from . import
        # py_log`, are deliberately NOT in `__all__`; this test scopes to
        # the run.py-referenced public surface, not all re-exports.)
        tree = ast.parse((_PACKAGE_ROOT / "__init__.py").read_text())
        dunder_all: set[str] = set()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(t, ast.Name) and t.id == "__all__"
                    for t in node.targets
                )
                and isinstance(node.value, ast.List)
            ):
                dunder_all |= {
                    elt.value
                    for elt in node.value.elts
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                }
        missing = _rs_attribute_refs("run.py") - dunder_all
        self.assertEqual(
            missing,
            set(),
            f"run.py-referenced native symbols absent from __all__: "
            f"{sorted(missing)}",
        )


if __name__ == "__main__":
    unittest.main()
