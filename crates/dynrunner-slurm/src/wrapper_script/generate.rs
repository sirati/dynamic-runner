//! [`generate_wrapper_script`]: the canonical secondary-mode wrapper
//! generator. Sequentially-built bash heredocs span scratch-dir
//! setup, podman storage, FIFO command-relay, optional shutdown-
//! manager spawn (out-of-cgroup, via `systemd-run --user --unit`
//! service mode), image load, container run, and cleanup traps.
//! Splitting further would fracture the linearly-constructed bash
//! payload (each section depends on shell variables defined
//! upstream); the file sits above the 300-line target because the
//! script body it emits is itself one cohesive bash program. See
//! [`super`] for the higher-level rationale.
//!
//! The pre-2026-05 inline watchdog (a `setsid -f bash -c '...'`
//! subshell that polled `squeue %T` and signalled the container)
//! has been removed. It signalled the container's pid 1 (= bash,
//! which doesn't forward signals to children), and `setsid -f`
//! does NOT escape the slurmd cgroup — so on cgroup teardown the
//! watchdog died alongside everything else, defeating its
//! "survives wrapper exit" purpose. The replacement is an
//! out-of-cgroup shutdown-manager process spawned via
//! `systemd-run --user --unit=<name>` as a transient service unit,
//! addressed by name via `systemctl --user kill` from the wrapper's
//! signal trap. See [`WrapperScriptConfig::shutdown_manager_bin_path`].

use std::path::Path;

use dynrunner_slurm_wrapper_config::{ConnectionMode as WireConnectionMode, WrapperConfig};

use super::config::{ConnectionMode, WRAPPER_SRC_NETWORK_CONTAINER_PATH, WrapperScriptConfig};
use super::quote::{bash_quote, rand_hex8};

/// Generate the bash wrapper script for a SLURM job.
///
/// The script sets up scratch /tmp dirs, podman storage, the FIFO
/// command relay, optionally spawns the out-of-cgroup shutdown
/// manager, loads the docker image, and runs the container in the
/// requested connection mode.
pub fn generate_wrapper_script(cfg: &WrapperScriptConfig<'_>) -> String {
    let rnd_suffix = rand_hex8();

    // When the caller plumbs a wrapper-binary path, the whole bash
    // body is replaced by a tiny `exec <bin> <to_args()…>` stub: the
    // Rust musl wrapper binary performs the full secondary lifecycle
    // the heredoc below otherwise renders. The `#SBATCH`/entrypoint
    // mechanics (`submit_job` prepends those) are untouched — only the
    // script body differs. Building the stub here keeps the legacy and
    // binary code-paths in one place so the render-time inputs they
    // share (`rnd_suffix`, `name_prefix`, the resolved mount sources,
    // connection mode, …) cannot drift between the two.
    if let Some(bin) = cfg.wrapper_bin_path {
        return generate_wrapper_stub(cfg, bin, &rnd_suffix);
    }

    let rndtmp = format!("/tmp/{}-{rnd_suffix}", cfg.name_prefix);
    let container_name = format!("{}-{rnd_suffix}-{}", cfg.name_prefix, cfg.secondary_id);

    let src_tmp = format!("{rndtmp}/src");
    let out_tmp = format!("{rndtmp}/out");
    let log_tmp = format!("{rndtmp}/log");
    let podman_storage = format!("{rndtmp}/storage");
    let podman_run = format!("{rndtmp}/run");
    let socket_dir = format!("{rndtmp}/sockets");
    let cmd_socket = format!("{socket_dir}/cmd.sock");

    let srcbins_network = cfg
        .srcbins_mount_source
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.src_bins_path());
    let output_network = cfg
        .output_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.output_path());
    let log_network = cfg
        .run_log_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.log_path());

    // Optional dynrunner-network volume/env block. When absent the
    // strings are empty and collapse cleanly inside the podman-run
    // continuation lines.
    let (dynrunner_volume_block, dynrunner_env_block, dynrunner_echo_block) =
        match cfg.dynrunner_network_dir {
            Some(dir) => (
                format!("    -v \"{dir}:/app/dynrunner-network\" \\\n"),
                "    -e DYNRUNNER_NETWORK=\"/app/dynrunner-network\" \\\n".to_string(),
                format!("echo \"    {dir} -> /app/dynrunner-network\""),
            ),
            None => (String::new(), String::new(), "true".to_string()),
        };

    // Bash-quote each consumer-supplied flag so values containing
    // spaces or shell-metacharacters survive intact, then render as
    // one continuation line per arg so the resulting `podman run`
    // block keeps the same readable shape regardless of how many
    // flags the consumer passes. Empty slice → empty string, which
    // collapses cleanly between the env+volume block and the
    // image-tag line.
    let extra_run_args_block: String = cfg
        .extra_run_args
        .iter()
        .map(|arg| format!("    {} \\\n", bash_quote(arg)))
        .collect();

    // Out-of-cgroup shutdown-manager spawn block. When the caller
    // supplies `shutdown_manager_bin_path`, the wrapper picks one of
    // two cgroup-escape primitives at RUNTIME based on whether the
    // user-systemd bus is reachable AND whether the user-systemd
    // service registration succeeds:
    //
    //   1. `systemd-run --user --unit=<name>` (preferred, service
    //      mode): manager lives in the user's `user@<uid>.service`
    //      cgroup, NOT the slurmd job cgroup. SLURM cgroup-v2 teardown
    //      of the job pidtree therefore does not reap the manager.
    //
    //      Why service-mode (not `--scope`): scope mode keeps
    //      systemd-run as the wait-parent of the spawned binary
    //      (foreground-blocking until the binary exits), forcing a
    //      shell `&` to background the waiter. The `&` introduces a
    //      RACE: systemd-run's bus handshake with user-systemd to
    //      register the scope is asynchronous w.r.t. the wrapper's
    //      continuation. If SLURM TIMEOUT fires before the handshake
    //      completes, proctrack reaps the still-handshaking
    //      systemd-run subprocess in the wrapper's cgroup tree and
    //      the scope is never registered — silent no-op, no journal
    //      entry, no log file (asm-tokenizer 2026-05-18: 2/4 workers
    //      under forced --slurm-time-limit 1). Service mode (no
    //      `--scope`) makes systemd the parent of the spawned binary
    //      directly; systemd-run blocks UNTIL registration completes
    //      and returns its exit code synchronously. No `&` needed,
    //      no race. Restart=no preserves the previous "manager exits
    //      after cleanup" semantics.
    //   2. `setsid -f` (fallback): manager runs in a new session in
    //      the slurmd job cgroup. SLURM TIMEOUT will reap it before
    //      cleanup runs; the manager still gets a chance to react to
    //      wrapper-exit on graceful terminations, but cgroup-induced
    //      kills (SLURM TIMEOUT, scancel-after-grace, node reboot)
    //      defeat it. The fallback exists for two reasons:
    //      (a) rootless-podman compute hosts without `loginctl
    //      enable-linger <user>` and hosts where slurmd strips
    //      XDG_RUNTIME_DIR have no user-bus socket reachable — the
    //      `[ -S … ]` probe returns false and we skip the
    //      service-mode invocation outright;
    //      (b) the bus is reachable but registration fails at runtime
    //      (e.g. unit-name collision, transient PID1 unresponsiveness,
    //      `--property=` rejected by an old systemd). systemd-run
    //      returns non-zero; we fall through to setsid.
    //
    // Cleanup-forward picks the same primitive symmetrically: a
    // registered systemd unit (be it scope or service — `systemctl
    // --user kill` targets by unit name regardless of suffix) is
    // signalled via `systemctl --user kill`; setsid-PID is signalled
    // via `kill -SIGCONT` directly. SIGCONT cannot be
    // blocked/ignored and doesn't terminate; it wakes the manager's
    // poll loop so it observes "wrapper gone, container gone, time
    // to clean up".
    //
    // When `None`, neither the spawn block nor the cleanup forward
    // are rendered — the cleanup trap reduces to the CMD_RELAY-only
    // teardown (no /tmp cleanup; callers opting in here accept that
    // tradeoff explicitly).
    //
    // Bash safety: the binary path is `bash_quote`-d at format!-time
    // (literal substitution, no env-var indirection) so paths with
    // spaces/metacharacters survive intact. The systemd-run `--unit`
    // name is the same hex suffix `$rnd_suffix` the rest of the
    // wrapper uses, prefixed with `dynrunner-shutdown-`, so the unit
    // is uniquely named per job and can be re-targeted by
    // `systemctl --user kill` later.
    //
    // Why XDG_RUNTIME_DIR gets a defensive re-export at the top of
    // the wrapper AND a per-call override here: the wrapper sets
    // `XDG_RUNTIME_DIR=$PODMAN_RUN` further down for podman's
    // benefit (its rootless storage cookie lives in $XDG_RUNTIME_DIR).
    // The systemd-user bus client embedded in `systemd-run` reads the
    // SAME env var to locate the per-uid bus socket
    // (`$XDG_RUNTIME_DIR/systemd/private`). The two values disagree:
    // podman wants `$PODMAN_RUN`, systemd-run wants
    // `/run/user/$(id -u)`. We capture the original-user value into
    // `$SYSTEMD_USER_RUNTIME_DIR` up-top BEFORE the podman override,
    // then prefix the `systemd-run` invocation with
    // `XDG_RUNTIME_DIR=$SYSTEMD_USER_RUNTIME_DIR` so the bus client
    // sees the correct value for that single command. Symmetric:
    // the bus-presence probe uses the same captured var.
    //
    // Service-mode handing stdio ownership to systemd interacted
    // poorly with the deployed systemd/MAC stack: the binary was
    // exec'd correctly (journal proof) but neither
    // `StandardOutput=append:<path>` nor `StandardError=append:<path>`
    // actually routed the child's stdio to that file (asm-tokenizer
    // 2026-05-18). Foreground probe with identical argv confirmed
    // the binary itself writes stderr fine — root cause is on
    // systemd's stdio-routing side, not the manager.
    //
    // Rather than chase the systemd quirk, the manager binary now
    // owns the log destination directly: `--log-file <PATH>` opens
    // the file at startup and appends every log line to it (in
    // addition to stderr). The wrapper passes the same `$SHUTDOWN_LOG_PATH`
    // it would have used for the systemd properties — operator-visible
    // behaviour (single log file per job in `$RNDTMP/shutdown-manager.log`)
    // is unchanged.
    //
    // `--property=StandardError=journal` stays so panic backtraces
    // and any pre-`--log-file-open` stderr land in
    // `journalctl --user -u <unit>` as a diagnostic safety net.
    //
    // `--property=PrivateTmp=false` disables systemd's default
    // per-unit /tmp namespace isolation. Without this, the
    // shutdown-manager's view of `/tmp/asm-XXX/...` is a different
    // mount namespace from the wrapper's — the wrapper's
    // `--tmp-prefix`, `--storage-root`, `--runroot`, `--pid-file`,
    // and `--log-file` paths under `$RNDTMP` would resolve to a
    // private tmpfs inside the unit's namespace, NOT the on-disk
    // directories the wrapper created and the rest of the SLURM job
    // expects (asm-tokenizer 2026-05-18: journal trace showed the
    // manager running to completion, but no on-disk artifacts — the
    // log file, pid file, and `podman unshare rm -rf` were all
    // operating on a phantom namespace-private /tmp). NixOS's
    // user-systemd defaults can enable PrivateTmp transparently on
    // transient units; the explicit `=false` neutralizes that.
    //
    // `--podman-path "$PODMAN_BIN"` plumbs the wrapper-resolved
    // absolute podman path (from `command -v podman` earlier in this
    // script) into the manager. The systemd-user-service unit does
    // NOT inherit the wrapper's PATH; on NixOS workers podman lives
    // at `/run/current-system/sw/bin/podman`, which is NOT on the
    // default user-systemd PATH — `Command::new("podman")` would
    // ENOENT inside the unit. Threading the absolute path makes the
    // manager's podman invocations work regardless of the unit's
    // PATH. Mirrored in the setsid fallback below for symmetry: the
    // setsid path DOES inherit the wrapper's PATH (no new session
    // PATH reset), but threading the same arg keeps both branches
    // exercising the identical CLI contract.
    let (shutdown_manager_spawn_block, shutdown_manager_cleanup_forward) = match cfg
        .shutdown_manager_bin_path
    {
        Some(path) => {
            let bin_q = bash_quote(&path.display().to_string());
            (
                    format!(
                        r##"SHUTDOWN_MODE=""
SHUTDOWN_SCOPE=""
SHUTDOWN_PID=""
SHUTDOWN_LOG_PATH="$RNDTMP/shutdown-manager.log"
if [ -S "$SYSTEMD_USER_RUNTIME_DIR/systemd/private" ] && command -v systemd-run >/dev/null 2>&1; then
    SHUTDOWN_SCOPE="dynrunner-shutdown-{rnd_suffix}"
    # XDG_RUNTIME_DIR= prefix is a per-command env override (NOT
    # exported globally — podman needs the $PODMAN_RUN value set
    # below). The systemd-user-bus client embedded in systemd-run
    # reads this to find the per-uid bus socket.
    #
    # Service mode (no `--scope`): systemd-run BLOCKS until the
    # registration handshake with user-systemd completes, then
    # returns its exit code. No `&` — eliminates the previous race
    # where SLURM TIMEOUT could reap a still-handshaking systemd-run
    # in scope-mode before the scope was ever registered (silent
    # no-op: no journal entry, no log file). systemd-run's own
    # stderr is captured into the same log file so registration-
    # failure diagnostics land alongside any later manager output.
    if XDG_RUNTIME_DIR="$SYSTEMD_USER_RUNTIME_DIR" systemd-run --user --quiet \
            --unit="$SHUTDOWN_SCOPE" \
            --property=Restart=no \
            --property=PrivateTmp=false \
            --property=StandardError=journal \
            -- \
            {bin_q} \
                --container-name "$CONTAINER_NAME" \
                --storage-root "$PODMAN_STORAGE" \
                --runroot "$PODMAN_RUN" \
                --tmp-prefix "$RNDTMP" \
                --pid-file "$RNDTMP/shutdown-manager.pid" \
                --wrapper-pid "$$" \
                --log-file "$SHUTDOWN_LOG_PATH" \
                --podman-path "$PODMAN_BIN" \
                --rm-path "$RM_BIN" 2>>"$SHUTDOWN_LOG_PATH"; then
        SHUTDOWN_MODE=systemd
        echo "Spawned shutdown manager in unit $SHUTDOWN_SCOPE (cgroup escape via user.slice service)"
    else
        SYSTEMD_RUN_RC=$?
        echo "WARNING: systemd-run --user --unit failed (exit=$SYSTEMD_RUN_RC); falling back to setsid -- cgroup escape DISABLED" >&2
        SHUTDOWN_SCOPE=""
    fi
fi
if [ -z "$SHUTDOWN_MODE" ] && command -v setsid >/dev/null 2>&1; then
    SHUTDOWN_MODE=setsid
    echo "WARNING: shutdown manager running under setsid -- cgroup escape DISABLED; SLURM TIMEOUT will reap the manager before cleanup." >&2
    setsid -f {bin_q} \
        --container-name "$CONTAINER_NAME" \
        --storage-root "$PODMAN_STORAGE" \
        --runroot "$PODMAN_RUN" \
        --tmp-prefix "$RNDTMP" \
        --pid-file "$RNDTMP/shutdown-manager.pid" \
        --wrapper-pid "$$" \
        --log-file "$SHUTDOWN_LOG_PATH" \
        --podman-path "$PODMAN_BIN" \
        --rm-path "$RM_BIN" \
        </dev/null >>"$SHUTDOWN_LOG_PATH" 2>&1
    # Capture pid via the manager's own pid-file (written first
    # thing in main::run_with_config). 5-second wait (50 * 0.1s)
    # is well above worst-case fork+exec+pid-file-write latency.
    for _ in $(seq 1 50); do
        if [ -f "$RNDTMP/shutdown-manager.pid" ]; then
            SHUTDOWN_PID=$(cat "$RNDTMP/shutdown-manager.pid" 2>/dev/null || true)
            break
        fi
        sleep 0.1
    done
    if [ -n "$SHUTDOWN_PID" ]; then
        echo "Spawned shutdown manager (setsid pid=$SHUTDOWN_PID); cgroup escape unavailable"
    else
        echo "ERROR: setsid-spawned shutdown manager did not write pid-file within 5s -- wrapper cleanup forward will be a no-op" >&2
    fi
fi
if [ -z "$SHUTDOWN_MODE" ]; then
    echo "ERROR: neither systemd-run --user --unit (bus probe failed or registration failed) nor setsid available; orphan-container cleanup DISABLED on signal" >&2
fi
"##,
                    ),
                    // Cleanup forward picks the matching primitive:
                    // SCOPE => systemctl --user kill, PID => kill.
                    // SIGCONT cannot be blocked/ignored and doesn't
                    // terminate — it just wakes the manager's poll
                    // loop so it re-evaluates idle-shutdown. The
                    // manager owns the actual tmp-cleanup and
                    // orphan-container teardown; the wrapper only
                    // nudges it.
                    "    if [ -n \"${SHUTDOWN_SCOPE:-}\" ]; then\n        \
                     systemctl --user kill --signal=SIGCONT \"$SHUTDOWN_SCOPE\" 2>/dev/null || true\n    \
                     elif [ -n \"${SHUTDOWN_PID:-}\" ]; then\n        \
                     kill -SIGCONT \"$SHUTDOWN_PID\" 2>/dev/null || true\n    \
                     fi\n"
                        .to_string(),
                )
        }
        None => (String::new(), String::new()),
    };

    let mut script = format!(
        r##"#!/usr/bin/env bash
set -e

echo "=================================================="
echo "SLURM Secondary Job Starting"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="

RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"
mkdir -p "{src_tmp}" "{out_tmp}" "{log_tmp}" "{socket_dir}"

# SLURM-job-env defensive: slurmd may have stripped XDG_RUNTIME_DIR
# from the job environment entirely. Restore the canonical per-uid
# value so the systemd-user bus probe below has a path to inspect.
# Captured into SYSTEMD_USER_RUNTIME_DIR BEFORE the podman override
# (next stanza) clobbers XDG_RUNTIME_DIR for podman's storage cookie.
# Bus probe + systemd-run invocation read from the captured var, not
# from $XDG_RUNTIME_DIR, so the two consumers don't collide.
export XDG_RUNTIME_DIR="${{XDG_RUNTIME_DIR:-/run/user/$(id -u)}}"
SYSTEMD_USER_RUNTIME_DIR="$XDG_RUNTIME_DIR"

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"

CONTAINER_NAME="{container_name}"

# Resolve podman absolute path so the shutdown-manager service unit
# (which inherits systemd-user's minimal PATH, NOT the wrapper's
# shell PATH) can invoke podman during cleanup. On NixOS workers the
# binary lives at /run/current-system/sw/bin/podman, which is NOT on
# the default user-systemd PATH; without this resolution the
# manager's `Command::new("podman").spawn()` calls ENOENT for every
# cleanup operation (asm-tokenizer 2026-05-18 post-6a41e3a). On
# standard distros podman is at /usr/bin/podman which IS on the
# default PATH, but explicit resolution removes the dependency
# entirely — the same wrapper render now works on either stack.
PODMAN_BIN="$(command -v podman 2>/dev/null || true)"
if [ -z "$PODMAN_BIN" ]; then
    echo "WARNING: podman not found in wrapper PATH; shutdown-manager cleanup will rely on its --podman-path default (\"podman\", PATH lookup inside the service unit) and may ENOENT under systemd-user-service-mode" >&2
    PODMAN_BIN="podman"
fi
echo "Podman binary: $PODMAN_BIN"

# Resolve `rm` ONCE here at wrapper startup, then thread the
# absolute path to the shutdown-manager via --rm-path. The manager
# stores that path once at construction time and reuses it for
# every `podman unshare <rm> <validated-abs-path> -rf` invocation;
# no exec-time PATH lookup ever runs (the absolute path passes
# straight through podman-unshare to execve). The wrapper's shell
# PATH is the only environment with reliable coreutils resolution
# on NixOS — the systemd-user-service unit the manager runs under
# inherits a minimal PATH that doesn't include /run/current-system/
# sw/bin where rm lives, so resolving INSIDE the manager (or
# leaving --rm-path to its "rm" default) would ENOENT.
RM_BIN="$(command -v rm 2>/dev/null || true)"
if [ -z "$RM_BIN" ]; then
    echo "WARNING: rm not found in wrapper PATH; shutdown-manager cleanup will rely on its --rm-path default (\"rm\", PATH lookup inside the podman-unshare userns) and likely ENOENT under systemd-user-service-mode" >&2
    RM_BIN="rm"
fi
echo "Rm binary: $RM_BIN"

# Shutdown-manager spawn block follows immediately below. It is
# rendered only when the caller plumbs the binary path through
# WrapperScriptConfig::shutdown_manager_bin_path; otherwise the
# block collapses to empty and the cleanup trap reduces to a
# CMD_RELAY-only teardown. See the wrapper-script module-level
# Rust docs for the design rationale.
{shutdown_manager_spawn_block}
cleanup() {{
{shutdown_manager_cleanup_forward}    # CMD_RELAY teardown stays in the wrapper's signal trap —
    # the FIFO is in the wrapper's own process group, not in the
    # shutdown-manager's concern. `${{CMD_RELAY_PID:-}}` guard
    # handles early-failure paths where the relay was never
    # started. `wait` here is important: it lets the relay
    # subshell flush before the out-of-cgroup shutdown manager
    # eventually tears down the socket FIFOs.
    if [ -n "${{CMD_RELAY_PID:-}}" ]; then
        kill -TERM "$CMD_RELAY_PID" 2>/dev/null || true
        wait "$CMD_RELAY_PID" 2>/dev/null || true
    fi
}}
# Cleanup trap covers SLURM-induced signals: SIGTERM is sent by
# sbatch at time-limit / scancel, SIGHUP by an ssh disconnect,
# SIGINT by Ctrl+C from interactive jobs. EXIT alone misses every
# non-graceful termination.
trap cleanup EXIT TERM HUP INT
echo ""

# ============================================================================
# Pre-flight: graceful-stop any podman containers left running under the
# current user from prior dispatches on this compute node.
# ============================================================================
#
# Why this exists: ungraceful SLURM job termination (preemption, time-limit
# SIGKILL after the script's TERM trap missed, node reboot) leaves the
# wrapper's per-job ``$PODMAN_STORAGE`` (under /tmp/asm-XXXX/storage) on
# disk with its container still running, supervised by an orphan conmon
# process that no parent will ever reap. Field-observed pattern
# (asm-tokenizer 2026-05-16): 16 orphan containers on a 40-node cluster,
# one alive 7+ hours, actively writing into the network output volume
# alongside live dispatches and corrupting data. Default-storage
# ``podman ps`` does NOT see these — they're in orphan per-job
# ``$PODMAN_STORAGE`` roots ``podman`` was never told about. Recovery
# required a 1.167 TiB manual sweep across all 40 nodes (host-side
# ``find /tmp -name 'asm-*'`` + per-orphan ``podman --root X --runroot Y
# stop+rm`` + ``unshare`` mode rewrites for the rootless-subuid layers).
#
# What this does: enumerate every ``/tmp/*/storage`` directory owned by
# this user (the wrapper's per-job storage shape), graceful-stop running
# containers there, then ``podman rm -af`` so the orphan exited
# containers no longer hold storage layers. Also scans the user-default
# rootless storage for symmetry — same operation, no harm if empty.
# Skipped via ``DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1`` for the rare
# operator who needs to keep prior containers running (mid-job
# diagnostics).
#
# Why graceful (-t 10) rather than ``podman kill``: per user spec
# (``--oom-pressure-threshold`` PR thread on 2026-05-17). 10s grace
# lets the orphan's process tree flush bind-mount writes and final
# logs before the SIGKILL fallback.
#
# Why the current job's own ``$PODMAN_STORAGE`` is harmless to scan:
# it was just ``mkdir -p``'d empty above. ``podman ps`` returns nothing
# there; ``podman rm -af`` is a no-op.
if [ "${{DYNRUNNER_DISABLE_PREFLIGHT_PODMAN:-0}}" = "1" ]; then
    echo "Pre-flight podman cleanup: skipped (DYNRUNNER_DISABLE_PREFLIGHT_PODMAN=1)"
else
    echo "Pre-flight: scanning for leftover podman containers..."
    preflight_found=0
    # Phase 1: orphan per-job storage roots under /tmp/.
    for orphan_storage in /tmp/*/storage; do
        [ -d "$orphan_storage" ] || continue
        [ -O "$orphan_storage" ] || continue
        orphan_runroot="${{orphan_storage%/storage}}/run"
        # Running containers: graceful stop with 10s grace.
        orphan_running=$(podman --root "$orphan_storage" --runroot "$orphan_runroot" --cgroup-manager=cgroupfs ps -q 2>/dev/null || true)
        if [ -n "$orphan_running" ]; then
            preflight_found=1
            echo "Pre-flight: stopping containers in $orphan_storage: $orphan_running"
            podman --root "$orphan_storage" --runroot "$orphan_runroot" --cgroup-manager=cgroupfs stop -t 10 $orphan_running 2>/dev/null || true
        fi
        # All containers (including stopped/exited): remove to release
        # storage layers. The peer-documented leak: exited containers
        # held the network-output bind mount open even after the
        # process died; only ``rm`` releases those references.
        podman --root "$orphan_storage" --runroot "$orphan_runroot" --cgroup-manager=cgroupfs rm -af 2>/dev/null || true
    done
    # Phase 2: user-default rootless storage. Same operations; covers
    # operators who run ad-hoc ``podman`` without ``--root``.
    default_running=$(podman ps -q 2>/dev/null || true)
    if [ -n "$default_running" ]; then
        preflight_found=1
        echo "Pre-flight: stopping containers in default storage: $default_running"
        podman stop -t 10 $default_running 2>/dev/null || true
    fi
    podman rm -af 2>/dev/null || true
    if [ "$preflight_found" = "1" ]; then
        echo "Pre-flight: cleaned up leftover containers"
    else
        echo "Pre-flight: no leftover containers"
    fi
fi
echo ""

# Cap container memory at MIN(host MemTotal - 2GiB, wrapper-cgroup-memory-max)
# so a runaway worker hits a graceful container-OOM (just kills the
# worker process) instead of a host kernel-OOM that wedges the cgroup
# and leaves zombie SLURM jobs stuck COMPLETING.
#
# Two probes feed the cap:
#   1. /proc/meminfo:MemTotal — node-wide physical RAM. Always shows
#      the HOST's MemTotal even from inside a container/cgroup;
#      represents the upper bound on what the kernel can give us.
#   2. /sys/fs/cgroup/memory.max — the wrapper's own cgroup v2 memory
#      cap. SLURM with TaskPlugin=task/cgroup sets this per-job;
#      podman's own slurm-worker container also caps the slurmd
#      process tree (`WORKER_MEMORY` knob on slurm-test-env).
#      The literal string "max" means "no cap at this level"; any
#      numeric value is the active cap in bytes.
#
# Taking the min ensures we never tell podman the secondary container
# can have more RAM than its parent cgroup actually permits. Pre-fix
# the wrapper only consulted /proc/meminfo and on slurm-test-env
# (NodeRAM=96GiB but WORKER_MEMORY=4GiB) advertised `--memory=94GiB`
# to podman; inside the secondary container `/sys/fs/cgroup/memory.max`
# then reflected 94 GiB, the framework's `detect_total_memory_bytes`
# read that as the budget, workers allocated 90+ GiB each, and the
# kernel-enforced 4 GiB outer cap OOM-killed them on first nix-build
# fork burst — surfacing as `Broken pipe (os error 32)` on the
# nix-daemon socket (asm-dataset-nix T3 at 3c5f105). The min-with-
# cgroup-max fix closes this.
#
# --memory-swap=-1 (unlimited swap on top of the RAM cap) is the
# explicit user policy: a worker that overshoots its RAM budget
# should get swapped instead of OOM-killed. Reasoning: workers
# that "waste" memory in bursts can recover under pressure
# (swap-thrash is slow but observable) rather than dying
# abruptly. The RAM cap (--memory) still bounds in-core usage
# from the kernel's perspective; --memory-swap=-1 just unbounds
# the swap component of that ceiling so the container's effective
# memory.max becomes max(RAM cap + host swap, RAM cap). Without
# this, podman's default `--memory-swap=2*--memory` (or
# `--memory-swap=--memory` if we set it equal) causes the
# kernel cgroup-OOM-killer to fire as soon as actual RAM usage
# crosses the cap — losing the worker's progress and potentially
# triggering the bilateral-OOM-kill-by-cgroup pattern asm-dataset-nix
# diagnosed at afd1654. Slurm-test-env-owner's 929a7b9 does the
# parallel change on the outer worker container; this commit
# does it on the framework's inner secondary container.
#
# Falls back to no cap when both probes yield empty (absurdly
# small node and no cgroup cap — implausible on cluster).
MEM_BYTES_NODE=$(awk '/MemTotal:/{{val = $2*1024 - 2*1024*1024*1024; if (val > 0) print val; else print ""}}' /proc/meminfo)
MEM_BYTES_CGROUP=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo "")
case "$MEM_BYTES_CGROUP" in
    ""|max) MEM_BYTES_CGROUP="" ;;
    *[!0-9]*) MEM_BYTES_CGROUP="" ;;  # defend against unexpected shapes
esac
if [ -n "${{MEM_BYTES_NODE}}" ] && [ -n "${{MEM_BYTES_CGROUP}}" ]; then
    if [ "${{MEM_BYTES_NODE}}" -lt "${{MEM_BYTES_CGROUP}}" ]; then
        MEM_BYTES="${{MEM_BYTES_NODE}}"
        MEM_SOURCE="host MemTotal - 2GiB (tighter than cgroup ${{MEM_BYTES_CGROUP}})"
    else
        MEM_BYTES="${{MEM_BYTES_CGROUP}}"
        MEM_SOURCE="wrapper cgroup memory.max (tighter than host-MemTotal-2GiB ${{MEM_BYTES_NODE}})"
    fi
elif [ -n "${{MEM_BYTES_NODE}}" ]; then
    MEM_BYTES="${{MEM_BYTES_NODE}}"
    MEM_SOURCE="host MemTotal - 2GiB (no cgroup cap detected)"
elif [ -n "${{MEM_BYTES_CGROUP}}" ]; then
    MEM_BYTES="${{MEM_BYTES_CGROUP}}"
    MEM_SOURCE="wrapper cgroup memory.max (host-MemTotal probe failed)"
else
    MEM_BYTES=""
fi
if [ -n "${{MEM_BYTES}}" ]; then
    MEM_FLAGS="--memory=${{MEM_BYTES}} --memory-swap=-1"
    echo "Container memory cap: ${{MEM_BYTES}} bytes RAM + unlimited swap (${{MEM_SOURCE}})"
else
    MEM_FLAGS=""
    echo "Container memory cap: disabled (host-MemTotal and cgroup probes both empty)"
fi
echo ""

# Resolve the compute node's peer-routable IPs so the secondary
# advertises addresses other cluster nodes can actually dial. The
# container runs with `--network host` so it shares this node's
# network namespace, but `hostname -I` in there still returns
# *every* configured non-loopback address — and on Krater-class
# nodes the first one is often a CNI bridge / podman-internal
# subnet (10.x.x.x) that's not routed off-host. Resolving the
# node's FQDN through NSS picks the canonical cluster address that
# slurmd, ssh, and DNS all agree on. Empty values are tolerated by
# the Rust env-hint reader (see network::detect_ipv4); a probe
# failure simply falls back to the legacy `hostname -I` first-token.
SLURM_NODE_NAME="${{SLURMD_NODENAME:-$(hostname -f)}}"
PRIMARY_NODE_IPV4=$(getent ahostsv4 "$SLURM_NODE_NAME" 2>/dev/null | awk '{{print $1; exit}}')
PRIMARY_NODE_IPV6=$(getent ahostsv6 "$SLURM_NODE_NAME" 2>/dev/null | awk '$1 ~ /:/ {{print $1; exit}}')
echo "Peer-routable IPv4: ${{PRIMARY_NODE_IPV4:-<unresolved, will fall back to hostname -I>}}"
echo "Peer-routable IPv6: ${{PRIMARY_NODE_IPV6:-<unresolved, will fall back to hostname -I or skip>}}"
echo ""
"##
    );

    // Connection-mode-specific port allocation
    match &cfg.connection {
        ConnectionMode::Reverse {
            connection_info_dir,
        } => {
            let sid = cfg.secondary_id;
            let is_observer = if cfg.is_observer { "true" } else { "false" };
            script.push_str(&format!(
                r##"
echo "Finding free ports on compute node..."
TUNNEL_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using tunnel port: $TUNNEL_PORT"
echo "Using QUIC port: $QUIC_PORT"

HOSTNAME=$(hostname -f)
mkdir -p "{connection_info_dir}"
# Peer-info file format (dynrunner-slurm/src/peer_info.rs):
#   Line 1 — legacy `<scheme>://<host>:<port>` URI for the SSH
#            reverse-tunnel target (back-compat: a v1 reader that
#            only knows about line 1 keeps working unchanged).
#   Lines 2+ — `key=value` envelope (v2). Consumed by a late-joining
#              observer's bootstrap reader (`peer_info::parse`) which
#              needs more than just the tunnel host:port to dial
#              the peer mesh.
#
# `cert_pem_b64` is intentionally omitted here: the cert is generated
# inside the secondary container at startup (during CertExchange),
# not at wrapper-render time. The secondary itself rewrites this
# file post-cert via the `dynrunner_slurm::peer_info::Builder` API
# (Step 8 of the transport-unification refactor).
{{
    printf 'tcp://%s:%s\n' "$HOSTNAME" "$TUNNEL_PORT"
    printf 'version=2\n'
    printf 'secondary_id=%s\n' '{sid}'
    if [ -n "${{PRIMARY_NODE_IPV4}}" ]; then printf 'ipv4=%s\n' "$PRIMARY_NODE_IPV4"; fi
    if [ -n "${{PRIMARY_NODE_IPV6}}" ]; then printf 'ipv6=%s\n' "$PRIMARY_NODE_IPV6"; fi
    printf 'quic_port=%s\n' "$QUIC_PORT"
    printf 'is_observer={is_observer}\n'
}} > "{connection_info_dir}/{sid}.info"
echo "Connection info written to: {connection_info_dir}/{sid}.info"
"##
            ));
        }
        ConnectionMode::Standard { .. } => {
            script.push_str(
                r##"
echo "Finding free port for QUIC server..."
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using QUIC port: $QUIC_PORT"
"##,
            );
        }
    }

    // FIFO command relay + image load.
    script.push_str(&format!(
        r##"
echo "Starting command relay service..."
SOCKET_COUNTER=0
{{
    rm -f "{cmd_socket}" "{cmd_socket}.response"
    mkfifo "{cmd_socket}"
    mkfifo "{cmd_socket}.response"
    while true; do
        if read -r CMD < "{cmd_socket}"; then
            if [ -n "$CMD" ]; then
                SOCKET_COUNTER=$((SOCKET_COUNTER + 1))
                OUTPUT_SOCK="{socket_dir}/output_${{SOCKET_COUNTER}}.sock"
                EXIT_SOCK="{socket_dir}/exit_${{SOCKET_COUNTER}}.sock"
                SIGNAL_SOCK="{socket_dir}/signal_${{SOCKET_COUNTER}}.sock"
                mkfifo "$OUTPUT_SOCK" "$EXIT_SOCK" "$SIGNAL_SOCK"
                {{
                    eval "$CMD" > "$OUTPUT_SOCK" 2>&1
                    EXIT_CODE=$?
                    rm -f "$OUTPUT_SOCK"
                    echo "$EXIT_CODE" > "$EXIT_SOCK"
                    rm -f "$EXIT_SOCK"
                }} &
                CMD_PID=$!
                {{
                    if read -r SIGNAL < "$SIGNAL_SOCK"; then
                        if [ -n "$SIGNAL" ]; then
                            kill -$SIGNAL $CMD_PID 2>/dev/null || true
                        fi
                    fi
                    rm -f "$SIGNAL_SOCK"
                }} &
                echo "output_${{SOCKET_COUNTER}}.sock,exit_${{SOCKET_COUNTER}}.sock,signal_${{SOCKET_COUNTER}}.sock,$CMD_PID" > "{cmd_socket}.response"
            fi
        elif [ ! -p "{cmd_socket}" ]; then
            # FIFO disappeared with no SIGTERM from cleanup() — that's
            # corrupt state (external rm, filesystem eviction, etc.),
            # not a normal lifecycle event. Bail loud so the failure is
            # diagnosable instead of silently neutering the secondary.
            # During intentional cleanup, the trap's kill+wait sequence
            # exits this subshell via signal before the FIFO vanishes,
            # so this branch only fires on genuine unexpected loss.
            echo "ERROR: command relay FIFO {cmd_socket} disappeared unexpectedly; secondary cannot continue." >&2
            exit 1
        fi
    done
}} &
CMD_RELAY_PID=$!

echo "Copying image to local temp directory..."
LOCAL_IMAGE="$RNDTMP/{image_tar_basename}"
cp "{image_path}" "$LOCAL_IMAGE"
echo "Image copied to: $LOCAL_IMAGE"

echo "Loading image into container runtime..."
# Wrap the load command in an explicit failure check so the abort
# surfaces as a clear marker on STDOUT (the .out file consumers
# check first), not just an opaque set-e exit between the
# "Loading…" line and the cleanup trap. The container runtime's
# own stderr still ends up in the .err file as before.
if ! {load_command}; then
    echo "ERROR: image load failed; secondary cannot start. See the .err file for the runtime's diagnostic."
    echo "ERROR: image load failed; secondary cannot start." >&2
    exit 1
fi
echo "Image loaded successfully"

echo "Starting Docker container..."
echo "  Volumes:"
echo "    {src_tmp} -> /app/src-tmp"
echo "    {out_tmp} -> /app/out-tmp"
echo "    {log_tmp} -> /app/log-tmp"
echo "    {srcbins_network} -> /app/src-network (ro)"
echo "    {output_network} -> /app/out-network"
echo "    {log_network} -> /app/log-network"
{dynrunner_echo_block}
echo "    {socket_dir} -> /app/sockets"
echo "  Secondary ID: {secondary_id}"
"##,
        image_tar_basename = cfg.image_tar_basename,
        image_path = cfg.image_path,
        load_command = cfg.load_command,
        secondary_id = cfg.secondary_id,
    ));

    // Mode-specific bits: banner echo lines and the `--secondary <url>`
    // argument. The podman-run block itself (volumes, env, framework
    // flags) is identical between modes — rendered once below.
    let image_ref = format!("{}:{}", cfg.image_name, cfg.image_tag);
    let sid = cfg.secondary_id;
    let container_command = cfg.container_command;
    let cores_spec = cfg.cores_spec;
    let max_memory_spec = cfg.max_memory_spec;
    // Render `--mem-manager-reserved=<bytes>` ONLY when the caller
    // explicitly set the field. Omitting the flag lets the
    // secondary's argparse default (parsed via `parse_memory`) take
    // over — operator can either drive the value from the
    // dispatcher CLI (single source of truth via the pipeline) OR
    // let the secondary apply its own default per-secondary. Empty
    // string for the `None` case keeps the rendered argv shape
    // identical to pre-flag wrappers when the operator doesn't opt
    // in. Mirrors the `forwarded_argv_block` empty-collapse shape.
    let mem_manager_reserved_block: String = match cfg.mem_manager_reserved_bytes {
        Some(bytes) => format!(" --mem-manager-reserved={bytes}"),
        None => String::new(),
    };
    // Container-internal bind-mount path for the staged-source drive.
    // Forwarded as `--src-network={path}` so the secondary's argparse
    // stores it on `args.src_network` and the dispatcher hands it to
    // `SecondaryConfig(src_network=...)` without relying on the auto-
    // detect path-exists check (which silently falls back to `None`
    // when the bind-mount appears late, the path is inaccessible to
    // the user, or any other transient filesystem-visibility issue).
    let src_network_path = WRAPPER_SRC_NETWORK_CONTAINER_PATH;
    // Bash-quote each forwarded user argv token and prefix with a
    // leading space so the joined block splices cleanly after the
    // framework's `--src-network={path}` argument. Empty slice
    // collapses to "" and the rendered line remains identical to the
    // pre-forwarding shape — callers that pass no forwarded_argv see
    // no diff in the rendered wrapper. `bash_quote` matches Python's
    // `shlex.quote` semantics (safe chars verbatim, everything else
    // single-quoted with `'\''` escaping for embedded apostrophes),
    // so values containing spaces, glob chars, or shell metacharacters
    // round-trip intact through the bash interpreter.
    let forwarded_argv_block: String = cfg
        .forwarded_argv
        .iter()
        .map(|arg| format!(" {}", bash_quote(arg)))
        .collect();
    let (mode_banner, secondary_url) = match &cfg.connection {
        ConnectionMode::Reverse { .. } => (
            "echo \"  Mode: SSH ProxyJump (primary tunnels to secondary via gateway)\"".to_string(),
            "tcp://localhost:$TUNNEL_PORT".to_string(),
        ),
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => (
            format!(
                "echo \"  Gateway: {gateway_host}:{gateway_port}\"\n\
                 echo \"  Mode: Standard (secondary connects to primary via gateway)\""
            ),
            format!("tcp://{gateway_host}:{gateway_port}"),
        ),
    };

    script.push_str(&format!(
        r##"{mode_banner}
echo ""

# `--pull=never`: if the local `podman load` was incomplete (image
# layers missing from the load), podman's default behaviour is to
# silently fall through to a registry pull and try docker.io —
# which on most institutional clusters returns "access denied"
# only after a multi-minute timeout, by which point the
# dispatcher has already given up with `timeout waiting for
# secondaries`. `--pull=never` makes that class of incomplete-load
# fail loud-and-fast with a clear "image not in local storage"
# error instead.
#
# `--ulimit nproc=32768:32768` overrides the host-side RLIMIT_NPROC
# that podman would otherwise propagate into the container. Without
# this, fork-heavy in-container workloads (autotools `./configure`,
# parallel gcc/clang, JVM thread spawn) hit `EAGAIN: Resource
# temporarily unavailable` whenever the SLURM job's inherited
# per-user nproc cap (or podman's `containers.conf` default) lands
# below the workload's peak fork count — independently of the
# `--pids-limit` cgroup ceiling, which only constrains pids.max
# inside the container's cgroup. Sibling fix to the `--pids-limit`
# default (commit 9b3dce0): same class of pre-2026-05 per-consumer
# rediscovery tax. 32768 = 2× the per-container `--pids-limit`
# (so two concurrent containers on one node can each hit their
# cgroup ceiling without bumping into the per-user nproc cap)
# and ½ of 65536 (the kernel's typical max), leaving operator
# headroom. Override path is the same as `--pids-limit`: pass
# `--ulimit nproc=<N>:<N>` via `TaskDeploymentSpec.extra_run_args`;
# podman applies the LAST occurrence. NOTE: this cannot raise the
# SLURM cgroup's pids.max — that's operator policy. Documented in
# `docs/MIGRATION_2026_05_PYTHON_TO_RUST.md`.
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --cgroup-manager=cgroupfs run --rm \
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
    --ulimit nproc=32768:32768 \
    ${{MEM_FLAGS}} \
    -e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4" \
    -e PRIMARY_NODE_IPV6="$PRIMARY_NODE_IPV6" \
{dynrunner_env_block}    -v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
{dynrunner_volume_block}    -v "{socket_dir}:/app/sockets" \
{extra_run_args_block}    {image_ref} \
    {container_command} --secondary {secondary_url} --secondary-id {sid} --secondary-quic-port $QUIC_PORT --cores={cores_spec} --max-memory={max_memory_spec} --src-network={src_network_path} --log-dir=/app/log-network --full-log-dir=/app/log-network/{sid}{mem_manager_reserved_block}{forwarded_argv_block}"##
    ));

    script.push_str(
        r#"
CONTAINER_EXIT_CODE=$?
echo "Container exited with code: $CONTAINER_EXIT_CODE"
kill $CMD_RELAY_PID 2>/dev/null || true

echo "=================================================="
echo "Job completed"
echo "Time: $(date)"
echo "=================================================="

exit $CONTAINER_EXIT_CODE
"#,
    );

    script
}

/// Render the tiny binary-stub wrapper body: `#!/usr/bin/env bash`
/// followed by a single `exec <bin> <args…>` line, where `<args…>` is
/// the [`WrapperConfig::to_args`] vector with each element
/// `bash_quote`-d and space-joined. The Rust musl wrapper binary
/// parses those flags back into a `WrapperConfig` (its `cli` feature)
/// and performs the full secondary lifecycle the legacy heredoc
/// otherwise renders.
///
/// Every field of the emitted [`WrapperConfig`] is mapped from the
/// [`WrapperScriptConfig`] the renderer was handed plus the
/// render-time values the legacy path computes (`rnd_suffix`; the
/// mount-source fallbacks; the `ConnectionMode` translation). The two
/// paths therefore share one source of truth for those inputs and
/// cannot drift.
fn generate_wrapper_stub(cfg: &WrapperScriptConfig<'_>, bin: &Path, rnd_suffix: &str) -> String {
    // Mount-source fallbacks: identical to the legacy bash path
    // (generate.rs `srcbins_network`/`output_network`/`log_network`),
    // so the binary receives the same resolved absolute paths the
    // heredoc would have baked in.
    let srcbins_network = cfg
        .srcbins_mount_source
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.src_bins_path());
    let output_network = cfg
        .output_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.output_path());
    let log_network = cfg
        .run_log_dir
        .map(String::from)
        .unwrap_or_else(|| cfg.slurm_config.log_path());

    let connection = match &cfg.connection {
        ConnectionMode::Standard {
            gateway_host,
            gateway_port,
        } => WireConnectionMode::Standard {
            gateway_host: (*gateway_host).to_string(),
            gateway_port: *gateway_port,
        },
        ConnectionMode::Reverse {
            connection_info_dir,
        } => WireConnectionMode::Reverse {
            connection_info_dir: (*connection_info_dir).to_string(),
        },
    };

    let wire = WrapperConfig {
        name_prefix: cfg.name_prefix.to_string(),
        rand_suffix: rnd_suffix.to_string(),
        secondary_id: cfg.secondary_id.to_string(),
        image_path: cfg.image_path.to_string(),
        image_tar_basename: cfg.image_tar_basename.to_string(),
        image_name: cfg.image_name.to_string(),
        image_tag: cfg.image_tag.to_string(),
        load_command: cfg.load_command.to_string(),
        container_command: cfg.container_command.to_string(),
        cores_spec: cfg.cores_spec.to_string(),
        max_memory_spec: cfg.max_memory_spec.to_string(),
        mem_manager_reserved_bytes: cfg.mem_manager_reserved_bytes,
        forwarded_argv: cfg.forwarded_argv.to_vec(),
        extra_run_args: cfg.extra_run_args.to_vec(),
        srcbins_network,
        output_network,
        log_network,
        dynrunner_network_dir: cfg.dynrunner_network_dir.map(String::from),
        connection,
        is_observer: cfg.is_observer,
        shutdown_manager_bin_path: cfg.shutdown_manager_bin_path.map(|p| p.to_path_buf()),
    };

    let bin_q = bash_quote(&bin.display().to_string());
    let args_q: String = wire
        .to_args()
        .iter()
        .map(|a| bash_quote(a))
        .collect::<Vec<_>>()
        .join(" ");

    format!("#!/usr/bin/env bash\nexec {bin_q} {args_q}\n")
}
