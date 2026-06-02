# Planning Conventions

Standing conventions for any multi-subagent build-out in this repository.
Future plans inherit these by reference rather than re-deriving them; a plan
need only state its work and its dependency graph, and may assume everything
below is already in force.

## TL;DR

- Each implementation subagent works in its **own** git worktree off the
  active `handoff/<topic>` branch and touches nothing else. `main` is never
  touched directly and never holds uncommitted code.
- Every long-running operation gets a hang-detect deadline. Nothing blocks
  indefinitely.
- Cargo is run one-at-a-time, under `nix develop`, wrapped in
  `timeout --signal=KILL`. Exit 137 means stop and diagnose, never retry.
- Commit messages are lazy-pro style: a title and at most one sentence, signed
  off, with no co-author trailer and no task-planning references.
- Architectural invariants — one peer network, full task-management state in
  the CRDT, no band-aids, single-concern modularity, deterministic tests —
  hold across every change.

---

## Commit and git rules

These rules are normative for every branch in a build-out.

### Commit message style

Write commit messages in the lazy-pro style most professionals use: a title
line and, at most, one sentence of additional context, plus a sign-off. No
extended body. The message describes the change in plain terms.

A commit message **must not**:

- include a `Co-Authored-By` trailer (or any other co-author attribution),
- refer to task-planning internals — no plan-phase labels, leaf-task ids, or
  similar references (e.g. no `1A`, `F3`, `B2.1`).

### Signing

Commit with:

```
git -c commit.gpgsign=true commit -s
```

The local repo config has signing disabled, so the `-c commit.gpgsign=true`
override is required to produce a signed, signed-off commit. Fall back to a
plain `git commit -s` (unsigned) **only** if signing has just failed in the
same working turn; otherwise always sign.

### Branch and integration model

Work happens on per-subtask branches cut from the active handoff branch. The
**orchestrator serializes all integration merges** — subagents never merge
into the handoff branch or into `main` themselves.

The merge cycle the orchestrator runs after a subtask's work is reviewed:

1. Validate and test the subtask branch.
2. Merge the subtask branch into the handoff branch.
3. Merge the current `main` into the working branch; if that brought changes,
   validate and test again.
4. Merge the working branch into the local `main` (if `main` advanced in the
   meantime, repeat the previous two steps).
5. If the local `main` gained new commits, push them to `origin`.

`main` must never hold uncommitted code. If a working tree has uncommitted
changes, confirm they are complete and error-free and, if so, commit them so
that in-progress state is not lost or corrupted across operations. Never run a
git merge in a worktree while a cargo invocation is running there.

---

## Subagent and parallelism discipline

### Maximal parallelism, leaf-task pre-splitting

Within a phase, work is pre-split into **leaf subtasks** that touch disjoint
files, then dispatched as a parallel batch — one background subagent per leaf.
The orchestrator performs no implementation itself; it dispatches, reviews, and
serializes integration.

Genuine parallelism is the file-disjoint set, not "everything at once." Where
two tracks must edit the same file, they are serialized — either ordered
explicitly, or assigned to a single subagent that owns that file. Subagents
cannot spawn further subagents, so the split must reach leaf granularity before
dispatch.

### Own-worktree rule (critical)

Many worktrees share one `.git`. Each subagent works **only** inside its own
fresh worktree, created from the active handoff branch, and touches nothing
else. A subagent must never edit, check out, or commit in:

- the canonical main worktree,
- the orchestrator worktree,
- any sibling worktree,
- the `main` branch.

This is the single most common source of subagent drift, so it is restated in
the first instruction of every implementation brief.

### Concurrency cap

Keep at most **three concurrent heavy cargo** invocations across the whole
build-out. Heavy compilation contends for CPU and memory; exceeding the cap
risks thrashing and out-of-memory kills.

---

## Liveness-check rule

Whenever a long-running operation is launched — a cargo build or test, a nix
build, a cluster bring-up, an end-to-end run, or a background subagent — arm a
monitor or wakeup that detects a hang or deadlock. **Never block
indefinitely.**

Concretely:

- Do not wait on a long operation with a single blocking call and a long
  timeout; such a call hides a stall until the entire budget is spent.
- Set a short hang-detect deadline — roughly two minutes for a build or test
  expected to finish sooner, proportionately longer for genuinely long
  operations.
- If an operation exceeds its budget, treat it as possibly deadlocked: capture
  state (`pgrep -af cargo`, relevant logs, `podman ps` / `squeue`) and
  diagnose. Do not silently keep waiting.

This rule exists because a prior multi-hour silent hang went undetected; that
is precisely the anti-pattern to avoid.

---

## Cargo discipline (anti-deadlock)

NixOS has no `cc` on the bare `PATH`, so cargo is always run through the dev
shell:

```
nix develop --command cargo ...
```

Additional rules:

- **One cargo at a time per worktree.** Check with `pgrep -af cargo` before and
  after an invocation.
- **Wrap every cargo invocation in `timeout --signal=KILL N`.** Separate the
  build step (`--no-run`) from the test run so a hang is attributable to the
  right phase.
- **Run the `dynrunner-manager-distributed` lib tests with
  `-- --test-threads=1`.** Some tests share global state and wedge under
  parallelism. This is a temporary crutch (see Test discipline below).
- **Exit 137 means the process was KILLed by the timeout.** Stop and diagnose —
  never blindly retry. A killed cargo is evidence of a hang or resource
  exhaustion, not a transient fault.

---

## Architectural invariants

These invariants are established and hold across all work. New code extends
them rather than working around them.

### One peer network

There is exactly **one logical peer network**. SSH, QUIC, WSS, and the
in-process channel are transparent transport *backends* of that single
network, not separate channels. Primary-facing frames are distinguished by
**role addressing** (they are marked via the role-addressing machinery), never
carried on a separate transport handle. A node holding any role participates in
the one mesh; there is no second channel to keep in sync.

### Full task-management state in the CRDT

Every peer holds the **full** task-management state, replicated through the
CRDT. A primary may drop at any moment, so a promoting peer or an observer must
already hold everything needed to take over or to report — in-flight
assignments, the worker roster and its capacity, and task outcomes. State that
lives only in the primary-local ledger is a latent failover and observability
gap: it is not enough for one node to know it.

### No band-aids, no special-casing, single-concern modularity

Find the root cause; do not patch symptoms. Specifically:

- Extend **exhaustive matches** with a new arm rather than branching on a
  special case with an `if`. A proliferation of `if`s is the signature of
  quick-and-dirty, non-modular code.
- A function name carrying two different domain nouns indicates a concern that
  belongs in another module.
- The same import appearing at the same call-site level across two or more
  files signals duplicated logic that should be a shared primitive in its
  owning module.
- A new feature is self-contained and integrated through the modular API. If
  the existing API cannot accommodate it cleanly, improve the API toward the
  general case — not toward the one call site at hand. The framework provides
  primitives; consumers compose policy over them.

Before opening an implementation file, state in one sentence the single
concern of the change and the module boundary it crosses. If that sentence
contains "and", the design is wrong; split it first.

### Test discipline

Tests are **deterministic**. Time-dependent tests use paused-clock advance and
synchronous predicate assertions rather than real loops and wall-clock races.

The `--test-threads=1` default for the `dynrunner-manager-distributed` lib
suite is a **temporary crutch** that masks tests sharing global state. It is to
be removed once the suite is parallel-safe; the offending tests are
root-caused, their shared state isolated, and any wall-clock-raced tests
converted to deterministic form, so that `cargo test --workspace` is green at
default parallelism as CI runs it.
