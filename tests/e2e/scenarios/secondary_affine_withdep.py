"""Scenario module: the with-dep SecondaryAffine gate (#497 / #506 fix).

Single concern: the registry's one-module-per-scenario contract. The scenario
CLASS and all shared affine logic live in :mod:`secondary_affine` (the no-dep
and with-dep scenarios share the invariant checker, marker reads, and prepare
helper — one owner, no duplication). This module exists only so the registry's
``name → module`` mapping (``secondary-affine-withdep`` →
``secondary_affine_withdep``) resolves to a module exporting ``SCENARIO``.

See :class:`secondary_affine.SecondaryAffineWithDepScenario`: the canonical
upload→import→build shape. The with-dep affine gate becomes ``AffineReady``
when its upload-stand-in dep completes (the #506 fix) and its builds dispatch,
so this scenario is EXPECT-PASS — it asserts the same full #497 invariants as
the no-dep variant.
"""

from __future__ import annotations

from .secondary_affine import SCENARIO_WITHDEP as SCENARIO

__all__ = ["SCENARIO"]
