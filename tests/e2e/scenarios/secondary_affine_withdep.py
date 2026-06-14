"""Scenario module: the with-dep SecondaryAffine gate (#497) — XFAIL #506.

Single concern: the registry's one-module-per-scenario contract. The scenario
CLASS and all shared affine logic live in :mod:`secondary_affine` (the no-dep
and with-dep scenarios share the invariant checker, marker reads, and prepare
helper — one owner, no duplication). This module exists only so the registry's
``name → module`` mapping (``secondary-affine-withdep`` →
``secondary_affine_withdep``) resolves to a module exporting ``SCENARIO``.

See :class:`secondary_affine.SecondaryAffineWithDepScenario` for the XFAIL
rationale: the with-dep affine gate currently DEADLOCKS (#506) and this
scenario quarantines the e2e repro under a bounded timeout, passing while the
deadlock exists and failing loudly once #506 lands.
"""

from __future__ import annotations

from .secondary_affine import SCENARIO_WITHDEP as SCENARIO

__all__ = ["SCENARIO"]
