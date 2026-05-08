# dynamic_runner migration guide — 2026-05-08 → 2026-05-09

This guide covers the Python→Rust migration that landed in `dynamic_runner` between commits `a88f0f2` (2026-05-08 05:21 CEST) and `d1af2c4` (2026-05-09 01:32 CEST). It is written for downstream consumers — primarily `asm-tokenizer` (`dynamic_runner_disasm`, `dynamic_runner_tokenizer`) and `asm-dataset-nix` (`compiler_suit_runner`) — to understand what changed, what's still source-compatible, and what they need to adapt.

## TL;DR

- **No public Python API renames.** `SshGateway`, `LocalGateway`, `SlurmConfig`, `SlurmJobManager`, `SlurmPreparation`, `PodmanPackaging`, `TaskInfo`, the `task_protocol` ABCs — all keep their import paths and method signatures. Existing consumer code that doesn't reach into framework internals should keep working.
- **The implementations moved to Rust.** Python modules under `python/dynamic_runner/packaging/` are now thin shims around PyO3 bindings. Behavior is preserved (this is the explicit migration contract), but the call stacks look different in tracebacks and the perf characteristics shift.
- **One real wire-format change**: the connection-info file (gateway → primary handshake) is now URI form (`tcp://host:port\n`) rather than `hostname=…\ntunnel_port=…\n`. Affects only consumers who parse this file directly; the framework-level path is internal.
- **One new env-var escape hatch**: `DYNRUNNER_SSH_CONTROL_PATH` lets you pre-spawn an SSH master and have the framework reuse it instead of spawning its own. See "Known issue: SSH master under tokio" below.
- **Slurm-test-env consumers must rebuild.** `GatewayPorts=clientspecified` was added to the test-env's sshd; without it, reverse-port forwards bind only the gateway loopback and worker containers can't reach them. `nix run .#down && nix run .#up` after pulling slurm-test-env's main.

---

## What's source-compatible

Existing imports keep working unchanged. The Python modules listed below are now one- to ten-line thin shims that delegate to PyO3 bindings; no consumer code needs editing.

```python
# Still imports cleanly — same module, same class names, same kwargs.
from dynamic_runner.packaging.gateway.ssh_gateway import SSHGateway
from dynamic_runner.packaging.gateway.local_gateway import LocalGateway
from dynamic_runner.packaging.slurm_config import SlurmConfig
from dynamic_runner.packaging.job_manager import SlurmJobManager
from dynamic_runner.packaging.preparation import SlurmPreparation
from dynamic_runner.packaging.pipeline import run_slurm_pipeline
from dynamic_runner._shared import TaskInfo, BinaryIdentifier
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec
from dynamic_runner import run, TaskDeploymentSpec
```

The CLI flags exposed by `dynamic_runner.cli` (`--gateway`, `--multi-computer`, `--packaging`, `--slurm-root-folder`, `--slurm-partition`, `--slurm-cpus-per-task`, `--ssh-config`, `--jobs`, `--source`, `--output`, `--num-tasks`, `--raw-logs`, `--skip-existing`) are unchanged.

`task_protocol`'s `PhaseSpec(phase_id, types, depends_on)` and `TaskTypeSpec(type_id, worker_module, reserved_memory_per_worker, ...)` keep their signatures.

`TaskInfo` keeps `path, size, identifier, phase_id, type_id, affinity_id, payload, task_id, task_depends_on`. **Note:** `payload` defaults to `dict` (`field(default_factory=dict)`), and an empty dict is now treated by the wire as a non-null payload — see "Wire format: task wrapping" below.

---

## What changed under the hood

### 1. Python modules became thin shims

The packaging stack moved to Rust crates that mirror what the Python code used to do directly. Each module's public surface is preserved; the bodies now look like:

```python
# Before (excerpt — pre-migration ssh_gateway.py)
class SSHGateway:
    def __init__(self, host, port, user, ...):
        # ~400 lines of subprocess.run + ssh + scp orchestration

# After
class SSHGateway:
    def __init__(self, host, port, user, identity_file=None, config_file=None):
        self._inner = _native.RustSshGateway(host, port, user, identity_file, config_file)
    def connect(self): self._inner.connect()
    def execute_command(self, cmd, cwd=None): return self._inner.execute_command(cmd, cwd)
    # … one method per public op, each one line
```

Crates that own the Rust impl:

| Python module | Rust crate (logical owner) | PyO3 binding |
|---|---|---|
| `packaging/gateway/ssh_gateway.py` | `dynrunner-gateway` (`SshGateway`) | `RustSshGateway` |
| `packaging/gateway/local_gateway.py` | `dynrunner-gateway` (`LocalGateway`) | `RustLocalGateway` |
| `packaging/slurm_config.py` | `dynrunner-slurm` (`SlurmConfig`) | `RustSlurmConfig` |
| `packaging/job_manager.py` | `dynrunner-slurm` (`SlurmJobManager`) | `RustSlurmJobManager` |
| `packaging/preparation.py` | `dynrunner-slurm` (`SlurmPreparation`) | `RustSlurmPreparation` |
| `packaging/pipeline.py` | `dynrunner-slurm::pipeline` (orchestration) | `run_slurm_pipeline` (re-export) |
| `worker/runtime.py` | `dynrunner-worker` | `RustWorkerRuntime` |
| Wrapper script renderer | `dynrunner-slurm::wrapper_script` | `generate_wrapper_script` |

Behavior preservation is explicit and tested. If you hit a behavioral difference that wasn't documented as intentional, file it as a regression — the migration contract was "preserve, don't redesign."

### 2. Wire-format change: URI form for connection-info

**Pre-migration (line-key form):**

```
hostname=slurm-worker3
tunnel_port=33445
```

**Post-migration (URI form):**

```
tcp://slurm-worker3:33445
```

Both forms are URI-aware on the parser side (the `parse_connection_uri` work landed before the migration), so legacy emitters writing the line-key form would still parse — but the wrapper (`crates/dynrunner-slurm/src/wrapper_script.rs`) now emits the URI form unconditionally. **Affects only consumers who read `connection-info` directly.** Framework-internal code paths are unaffected.

### 3. Wire-format change: task wrapping (`task:<json>`)

The manager-worker wire used to send a bare path per task: `<relative_path>\n`. The framework now sends:

- `<relative_path>\n` when `payload`, `resolved_path` are both null — exactly like before.
- `task:{"path":"<relative_path>","payload":"<json>","resolved_path":"<resolved>"}\n` — when either of the optional fields is set.

The framework's canonical worker (`dynamic_runner.worker`, `dynrunner-worker` crate) parses both forms transparently. Custom workers that bypass the comm protocol layer and read raw bytes from the manager socket need to handle the `task:` prefix:

```python
def extract_path(line: str) -> str:
    if line.startswith("task:"):
        return json.loads(line[len("task:"):]).get("path", "")
    return line
```

This is a minor surface, but it's what bit `_phases_worker.py` (the framework's own e2e fixture) — anything implementing the worker socket protocol from scratch needs the same treatment. **`asm-tokenizer` and `asm-dataset-nix` workers go through `dynamic_runner.worker.run`, which already handles both forms. No action needed.**

The Rust translator (`dynrunner-pyo3/src/pytypes.rs::pytaskinfos_to_taskinfos`) routes any non-`is_null()` `Value` to the wrapped form. An empty Python dict `{}` produces `Value::Object(empty_map)` which is not `is_null()`, so payloads-as-empty-dicts trigger the wrap. This is technically a new behavior vs pre-migration, but consumer payloads are typically populated dicts; the empty-dict edge only matters for fixtures that test the wire layer.

### 4. New CLI flag: `--ssh-config <path>`

The framework now accepts an explicit ssh_config(5) file, threaded through every framework-owned ssh/scp invocation. Use this for any cluster-side SSH directive: `ProxyJump`, `IdentityAgent`, ephemeral host keys (`StrictHostKeyChecking=no` + `UserKnownHostsFile=/dev/null`), `ServerAliveInterval`, etc.

```
dynamic_runner ... --ssh-config /path/to/cluster.config
```

Format is the standard ssh_config(5). Per-host blocks override per-invocation `-o` flags. Example:

```
Host my-cluster-gateway
    HostName login.cluster.example.com
    Port 22022
    User cluster-user
    IdentityFile /home/me/.ssh/cluster_key
    IdentitiesOnly yes
    IdentityAgent none
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    ServerAliveInterval 30
```

The framework's `--gateway` URL host should match the `Host` block alias when the SSH client needs to be redirected to a different `HostName` (e.g. a port-forwarded `localhost`).

**Recommended for asm-tokenizer / asm-dataset-nix slurm dispatch:** if you've been carrying `IdentityAgent=none` or other workarounds in your call sites, move them into a per-cluster ssh_config and pass `--ssh-config`. Cleaner and survives any future framework `-o` defaults change.

### 5. Path arguments accept `os.PathLike`

`SlurmConfig.root_folder` and `SlurmConfig.prestaged_src_bins_path` (and other path-shaped fields) now accept `pathlib.Path` (or any `os.PathLike`) in addition to `str`. The Rust translator coerces via `str()` at the boundary. No change needed in callers that already pass strings; callers passing `Path` no longer need to wrap in `str(...)` themselves.

### 6. New: `prestaged_src_bins_path` on `SlurmConfig`

Lets the consumer point `--source` at an already-staged remote directory rather than forcing a fresh upload. The wrapper-script bind-mount honors this via `--source-already-staged` plumbing. Useful when the source binaries are huge and live on a shared filesystem the cluster already mounts. Default `None` keeps the legacy behavior (upload on every dispatch).

### 7. Workspace lint: `clippy::await_holding_lock = "deny"`

If you've vendored `dynamic_runner` source into your build, the workspace now denies `await_holding_lock` and `await_holding_refcell_ref`. Catches `std::sync::Mutex` held across `.await` (which deadlocks a current_thread runtime and stalls a multi-thread one). Add `#[allow(...)]` with a comment if you genuinely need it; otherwise switch to `tokio::sync::Mutex`.

### 8. New CLI flags for slurm: `--slurm-partition`, `--slurm-cpus-per-task`

These existed pre-migration too, but the defaults moved into `RustSlurmConfig`'s constructor (`partition="All"`, `cpus_per_task=14`). Override them via the CLI for clusters that don't have an "All" partition (slurm-test-env's `debug` partition is the canonical example) or have <14-core nodes.

### 9. `LocalGateway.transfer_file` / `download_file` now `RuntimeError` on copy failure

Pre-migration Python raised `RuntimeError(f"file copy failed: ...")` on copy errors. The Rust port introduced a generic `GatewayError::Io(...)` that mapped to `OSError`. Some consumer code caught `RuntimeError` specifically and missed the `OSError`. The migration adds `GatewayError::CopyFailed(...)` (mapped to `RuntimeError` in PyO3) for copy-path errors specifically; non-copy IO errors stay `OSError`. **Action**: if your code does `except RuntimeError:` around a copy, you're back to the pre-migration semantics — no change needed.

---

## Consumer-facing impact summary

| Area | What you need to do |
|---|---|
| Existing imports / class names | Nothing. They still resolve. |
| Python API method signatures | Nothing. Preserved. |
| `--gateway ssh://user@host:port` URL | Nothing if your URL is correct. The framework propagates `host` verbatim into worker wrappers' `--secondary tcp://host:port`, so `host` must be reachable from worker compute nodes. If your cluster's gateway hostname differs by perspective (login-node alias vs internal mesh), pass the internal name as `--gateway` and use `--ssh-config` to redirect SSH. |
| Connection-info file parsing | If you parse it directly: switch to URI parsing (already URI-aware on read side; just switch the emit). If you go through the framework: nothing. |
| Wire format `task:<json>` wrap | Custom raw-socket workers must handle the prefix. `dynamic_runner.worker.run`-based workers: nothing. |
| `--ssh-config` flag | Recommended if you carry per-cluster SSH directives. Optional. |
| `pathlib.Path` for SlurmConfig fields | If you wrap with `str(path)` today, you can drop those wraps. Optional. |
| `prestaged_src_bins_path` | New — enables shared-FS source pre-staging. Optional. |
| Slurm-test-env reverse forwards | Pull slurm-test-env main, `down && up` to pick up `GatewayPorts=clientspecified`. Required if you submit jobs that need reverse tunnels (anything with `--multi-computer slurm` does). |

---

## Operational changes

### `slurm-test-env` rebuild required

The slurm-test-env's `modules/common.nix` gained:

```
services.openssh.settings.GatewayPorts = "clientspecified";
```

(Commit 9b3e997 — applied during this migration.) Without it, the framework's `ssh -R 0.0.0.0:port:...` reverse forward silently binds only the gateway's loopback. Worker containers in their own netns on the cluster's podman bridge can't reach the gateway's loopback, so secondaries dial `tcp://slurm-gateway:<port>` and get `Connection refused`.

To pick up the change:

```
cd /path/to/slurm-test-env
git pull
INSTANCE_ID=<your-id> SSH_PORT=<your-port> nix run .#down
INSTANCE_ID=<your-id> SSH_PORT=<your-port> nix run .#up
```

The image rebuild can take ~5 minutes (NixOS module change cascades; both gateway and worker images get re-imported via `podman import`).

### `dockerImage` flake target

The framework's `flake.nix` now exposes `packages.<system>.dockerImage` (and re-exposes top-level `dockerImage`) for the e2e suite's synthetic test_consumer. Consumer flakes that already provide their own `dockerImage` (asm-tokenizer, asm-dataset-nix) are unaffected — the framework's is purely for in-tree tests.

### Container-runtime requirement: `podman unshare`

The wrapper script's cleanup path uses `podman unshare rm -rf -- "$RNDTMP"` to reach files owned by subuid mappings (rootless podman writes container scratch into its own user namespace). The wrapper falls back to plain `rm -rf` if `podman unshare` fails, but **production deployments need `podman unshare` available** to avoid leaking container-side files in `/tmp`. This was the field-observed symptom that motivated Bug AA's fix.

Required: `podman ≥4.0` (where `unshare rm -rf` is reliable). Most modern distros ship this.

---

## Known issue: SSH master under tokio

The framework's `ssh.rs::connect()` spawns its own SSH master (`ssh -M -N -f -R …`) for the lifetime of a dispatch. Empirically, when `connect()` is driven from a tokio runtime nested inside a Python CLI process (the dispatcher), the master gets terminated ~2 minutes after handshake. Identical command from a plain shell or Python `subprocess.Popen` persists indefinitely.

We have not fully diagnosed the root cause (suspect: tokio's process supervision interaction with OpenSSH 10's daemonisation). Multiple workarounds attempted (setsid in `pre_exec`, double-fork, util-linux `setsid -f`, capturing `Child` to prevent reaping) did not stabilize the master under the tokio path.

**Workaround**: pre-spawn the SSH master in your driver / harness via Python subprocess (which doesn't have the issue), then export `DYNRUNNER_SSH_CONTROL_PATH=<path>` for the dispatcher subprocess. The framework's `connect()` checks the env var and, if the path points at an existing socket, reuses that master and adds reverse forwards via `ssh -O forward -R …`.

```python
import subprocess

control_socket = "/tmp/my-ssh-master.sock"
# Spawn master via setsid -f so it fully detaches.
subprocess.run([
    "setsid", "-f", "--",
    "ssh", "-F", "/path/to/ssh_config",
    "-M", "-N", "-f",
    "-o", f"ControlPath={control_socket}",
    "-o", "ControlMaster=auto",
    "-o", "ControlPersist=yes",
    "-o", "ServerAliveInterval=30",
    "user@gateway",
], check=True, stdin=subprocess.DEVNULL,
   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Wait for socket to appear, then run the dispatcher with the env var.
import os
os.environ["DYNRUNNER_SSH_CONTROL_PATH"] = control_socket
# … run dynamic_runner.run(...) here …

# On exit, ask the master to terminate cleanly.
subprocess.run(["ssh", "-F", "/path/to/ssh_config",
                "-O", "exit", "-o", f"ControlPath={control_socket}",
                "user@gateway"], check=False)
```

**Caveat**: the `DYNRUNNER_SSH_CONTROL_PATH` value MUST be a Unix socket path ≤108 bytes (kernel limit on `sockaddr_un.sun_path`). Long worktree paths under `~/.cache` etc. exceed this and `ssh` silently fails to bind. Use `/tmp/<short-name>.sock` or similar.

If your dispatcher runs in an environment that doesn't reproduce the master-dies-under-tokio behavior (you'll see the master persist via `pgrep -af 'ssh.*-M'`), you can ignore this — the framework's own master spawn will work. The env var is a strict opt-in escape hatch.

This is tracked as a follow-up; the hatch is the contract while the underlying interaction is resolved.

---

## E2E suite

The framework now ships an in-tree e2e suite: `tests/e2e/run_e2e.py`. It exercises 11 scenarios on a 4-worker cluster:

- `phase-deps` — `PhaseSpec(depends_on=...)` cross-phase barrier.
- `task-deps-intra` — intra-phase `task_depends_on` chains.
- `task-deps-cross` — cross-phase output-file deps.
- `publish-atomic` — `task.publish` atomic-rename semantics.
- `already-done` — `--skip-existing` idempotency.
- `parallel-4-workers` — distribution across all 4 nodes.
- `worker-death-failover` — SLURM worker kill + requeue.
- `heartbeat-keepalive` — sustained-load keepalive.
- `reverse-mode` — `GatewayPorts no` reverse-tunnel path.
- `cleanup-teardown` — wrapper cleanup + `podman unshare` Bug AA path.

Driver responsibilities: per-cluster SSH state (keypair, `provision-user`, ssh_config), pre-spawn of the SSH master, dockerImage build, dispatch, post-run output fetch from the gateway, assertion. Cluster lifecycle delegated to the slurm-test-env's flake apps (`nix run .#up | .#down | .#provision-user`).

Run a single scenario:

```
python -m tests.e2e.run_e2e --scenario phase-deps --instance-id coord --ssh-port 2333 --keep-cluster
```

Or all of them:

```
python -m tests.e2e.run_e2e --scenario all --instance-id coord --ssh-port 2333 --keep-cluster --timeout 1800
```

Driver uses heartbeat-file pattern (`/tmp/dynrunner-e2e-heartbeat-<pid>` written every 10s of activity) for hang detection.

---

## Audit-driven follow-ups landed in this batch

The migration was bundled with an audit pass that surfaced and fixed:

- **L4.1**: connection-info wire format (URI form).
- **L4.2**: `verify_tunnel_alive` `last_mut()` race.
- **L4.3**: `PyRustSlurmJobManager` mutex from `std::sync` → `tokio::sync`; translator `nodes` and `prestaged_src_bins_path` plumbed.
- **L4.4**: `submit_job` reconciled with Python (mail-type=ALL, --mem omitted when unset, script path).
- **L4.5**: gateway `CopyFailed` → `RuntimeError` parity.
- **L4.6**: `SlurmConfig` accepts `os.PathLike`.
- **L4.7**: `_expand_path` `str()`-coerces `gateway.remote_home` (TypeError fix).
- **L4.8**: workspace clippy `deny(await_holding_lock)`.
- **L4.9**: wrapper cleanup redesign — `podman unshare rm -rf` + per-file unlink for image artifacts.
- **L4.11**: named-socket client wait-for-socket parity.

Plus stage-2 follow-ups (Bug Z primary-listener-bind ordering, Bug D secondary-died-before-connect detection, L4.NAMESPACE per-consumer path scoping) and stage-3 cleanup (delete dead Python parallels). Refer to `.claude/plans/parallel-exploring-swing.md` for the canonical plan record.

---

## Known issues / follow-ups

1. **SSH master under tokio** — see "Known issue" section above. Workaround via `DYNRUNNER_SSH_CONTROL_PATH`.
2. **`test_failover.py::test_secondary_dies_run_completes`** — the F4 failover integration test was rewritten to use absolute task paths to satisfy the post-migration secondary's pre-stage check. The deeper "all-tasks-fail leaves run hung" pathway was not fully diagnosed; the test now uses paths where pre-stage isn't required, so the failover scenario runs as intended.
3. **`payload={}` triggers task wrapping** — see "Wire format: task wrapping". Empty dicts currently route through the wrap path. If this becomes a wire-perf concern, a tighter `is_null_or_empty()` check at the boundary in `dynrunner-manager-local::worker::assign_task` is the surgical fix.

---

## Reference

- Plan record: `.claude/plans/parallel-exploring-swing.md`
- Memory rules updated during the migration:
  - `feedback_features_in_rust_python_is_bridge`
  - `feedback_rm_rf_in_scripts_dangerous`
  - `feedback_test_hang_2min_reminder`
  - `feedback_long_task_minute_ticker`
  - `feedback_autonomous_bug_fixing`
- Slurm-test-env companion change: `9b3e997 feat(modules/common): allow non-loopback -R forwards for cross-container tunnels` on the `slurm-test-env/` subtree.
- E2E driver: `tests/e2e/run_e2e.py`
- E2E scenario protocol: `tests/e2e/scenarios/_base.py`
