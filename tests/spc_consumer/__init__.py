"""Synthetic composite consumer for the single-process-mode repro tests.

Mirrors the asm-tokenizer ``FullPipelineTask`` shape: three chained
phases where only phase 1 is discovered upfront and phases 2/3 are
lazily injected from ``on_phase_end`` via ``primary_handle.spawn_tasks``.
See :mod:`tests.test_single_process_composite` for the scenarios.
"""
