"""Synthetic dynamic_runner consumer for the ``inherit_outputs`` e2e.

The plain ``tests.e2e.test_consumer`` covers the two-phase A->B keyed-
outputs round-trip. This sibling consumer covers the orthogonal
``TaskDep(..., inherit_outputs=True)`` path: a three-task A->B->C chain
where C asks the framework to surface its predecessor's predecessors'
published outputs in addition to its direct predecessor's.

A separate consumer (rather than parameterising ``test_consumer``)
keeps the topology shape declarative — adding a 3-phase mode to the
existing 2-phase consumer would force every dispatch path to branch on
``--keyed-outputs-inherit`` (in ``get_phases``, ``discover_items``,
``build_worker_command_args``, the worker's ``handle`` dispatch),
violating the one-concern paradigm. This module's
:class:`InheritSyntheticTask` owns the 3-phase shape; the
:mod:`test_consumer` keeps its 2-phase shape unchanged.
"""

from .task import InheritSyntheticTask

__all__ = ["InheritSyntheticTask"]
