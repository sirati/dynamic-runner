//! Tests for `ConnectionMode::Standard` rendering: --cores /
//! --max-memory forwarding, the src-network mount, gateway URL
//! substitution, dynrunner_network_dir volume/env, and the
//! consumer nproc ulimit ordering.
//!
//! Slightly above the 300-line target because
//! `standard_mode_script_contains_gateway` is itself a ~150-line
//! end-to-end assertion (driver, container_command, ulimit, --cores,
//! --max-memory, --multi-computer flag, all on one rendered script);
//! splitting that single test into per-substring files would inflate
//! the boilerplate without splitting concerns.

use crate::config::SlurmConfig;
use crate::wrapper_script::{WrapperScriptConfig, generate_wrapper_script};

use super::standard_cfg;

#[test]
fn standard_mode_script_forwards_cores_spec() {
    // Task #29: each SLURM secondary's container_command MUST
    // receive `--cores <spec>` as a verbatim argument so the
    // secondary subprocess inside the cgroup-CPU-quota'd
    // container resolves the per-machine core count via
    // `parse_cores` instead of falling back to
    // `available_parallelism` (which inside the SLURM container
    // reads the host's CPU count, not the cgroup quota — the
    // foot-gun this fix closes). Pre-fix this argv suffix was
    // entirely absent and asm-dataset-nix observed
    // `secondary starting workers=32` even with `--cores 2` on
    // the dispatcher.
    let config = SlurmConfig::default();
    let mut cfg = standard_cfg(&config, &[]);
    cfg.cores_spec = "-2";
    let script = generate_wrapper_script(&cfg);
    // `=` syntax mandatory (task #32): `--cores -2` confuses
    // argparse on the secondary because `-2` matches argparse's
    // "looks like a flag" heuristic and the option-with-required-
    // value rejects it. `--cores=-2` always treats RHS as literal
    // value regardless of leading dash.
    assert!(
        script.contains("--cores=-2"),
        "wrapper script must forward `--cores=-2` (not `--cores -2`) \
         to secondary so argparse parses the leading-dash value as \
         a value, not a flag; render did not contain it"
    );
    // The `--cores` flag MUST appear AFTER `--secondary-quic-port`
    // (the argv-build order matches the regular CLI order):
    // assert position to catch regressions that move the flag
    // somewhere broken (e.g. into the podman-run flags block
    // instead of the container_command suffix).
    let port_idx = script
        .find("--secondary-quic-port")
        .expect("--secondary-quic-port must be present");
    let cores_idx = script.find("--cores=").expect("--cores= must be present");
    assert!(
        cores_idx > port_idx,
        "--cores must appear after --secondary-quic-port in the secondary's \
         argv (currently at byte {cores_idx}, port at {port_idx})"
    );
}

#[test]
fn standard_mode_script_forwards_max_memory_spec() {
    // Task #30: each SLURM secondary's container_command MUST
    // receive `--max-memory <spec>` symmetrically with `--cores`.
    // Without forwarding, the secondary inside the cgroup-memory-
    // quota'd container falls through to its argparse default
    // (`-2G` = HOST_MemTotal - 2G as seen via /proc/meminfo).
    // Inside a 4 GiB-capped container, /proc/meminfo still shows
    // the host's full RAM, so the framework computes 90+ GiB
    // worker budgets and workers OOM-die under the outer
    // cgroup's actual cap (asm-dataset-nix observed
    // `worker_id=0 budget_mb=92030` inside WORKER_MEMORY=4g).
    //
    // Defends the explicit-forward contract: dispatcher value
    // reaches the wrapper, wrapper emits the argv suffix.
    let config = SlurmConfig::default();
    let mut cfg = standard_cfg(&config, &[]);
    cfg.max_memory_spec = "3G";
    let script = generate_wrapper_script(&cfg);
    assert!(
        script.contains("--max-memory=3G"),
        "wrapper script must forward `--max-memory=3G` to secondary; \
         render did not contain it"
    );
    // Also test the negative-prefix case explicitly — this is the
    // exact value that caused the original argparse-collision
    // (asm-dataset-nix T3 at 57d7ee8 with default `-2G`).
    cfg.max_memory_spec = "-2G";
    let script_negative = generate_wrapper_script(&cfg);
    assert!(
        script_negative.contains("--max-memory=-2G"),
        "wrapper script must use `=` syntax for negative-offset memory \
         specs (task #32 argparse-collision fix); render did not contain it"
    );
    // `--max-memory` MUST land AFTER `--cores` (argv-build order).
    let cores_idx = script.find("--cores=").expect("--cores= must be present");
    let mem_idx = script
        .find("--max-memory=")
        .expect("--max-memory= must be present");
    assert!(
        mem_idx > cores_idx,
        "--max-memory must appear after --cores in the secondary's argv \
         (currently at byte {mem_idx}, cores at {cores_idx})"
    );
}

#[test]
fn standard_mode_script_forwards_mem_manager_reserved_when_set() {
    // The dispatcher's `--mem-manager-reserved` flow lands at the
    // wrapper-script generator as `Some(bytes)` on
    // `WrapperScriptConfig.mem_manager_reserved_bytes`. The
    // generator MUST render `--mem-manager-reserved=<bytes>` on the
    // secondary's container_command argv so the secondary's
    // argparse stores the value on `args.mem_manager_reserved` and
    // `_dispatch_secondary` passes it through to
    // `SecondaryConfig(mem_manager_reserved_bytes=...)`.
    //
    // Symmetric with `--cores=` / `--max-memory=` forwarding;
    // SLURM-only because the value reaches the secondary via the
    // wrapper rather than CLI propagation (the in-process
    // distributed manager skips the field entirely — its secondaries
    // share the manager's address space and nesting the workers
    // cgroup would tighten the shared cap).
    let config = SlurmConfig::default();
    let mut cfg = standard_cfg(&config, &[]);
    cfg.mem_manager_reserved_bytes = Some(500 * 1024 * 1024);
    let script = generate_wrapper_script(&cfg);
    assert!(
        script.contains("--mem-manager-reserved=524288000"),
        "wrapper script must forward `--mem-manager-reserved=<bytes>` to \
         secondary; render did not contain it. Script body:\n{script}"
    );
}

#[test]
fn standard_mode_script_omits_mem_manager_reserved_when_none() {
    // When the field is `None`, the wrapper MUST NOT emit the flag
    // — the secondary's argparse default takes over. Symmetric with
    // the `forwarded_argv` empty-collapse behaviour.
    let config = SlurmConfig::default();
    let cfg = standard_cfg(&config, &[]);
    assert!(cfg.mem_manager_reserved_bytes.is_none());
    let script = generate_wrapper_script(&cfg);
    assert!(
        !script.contains("--mem-manager-reserved="),
        "wrapper must omit `--mem-manager-reserved=` when the config field \
         is None; render contained it. Script body:\n{script}"
    );
}

#[test]
fn script_forwards_src_network_container_path() {
    // The wrapper bind-mounts the gateway's staged-source drive at
    // `/app/src-network` inside the secondary container (the `-v`
    // line above). The secondary subprocess MUST also receive
    // `--src-network=/app/src-network` so its argparse stores the
    // container-internal path on `args.src_network`; `_dispatch_
    // secondary` then forwards it into `SecondaryConfig(
    // src_network=...)` directly, instead of relying on
    // `PySecondaryConfig.__new__`'s `Path::exists("/app/src-network")`
    // auto-detect.
    //
    // The auto-detect is a leaky pattern: a transient filesystem-
    // visibility issue (delayed bind-mount, permission denied on
    // the path's existence check, race with the wrapper's own
    // `mkdir -p` of the parent dir) makes `Path::exists` return
    // `false`, `src_network` falls back to `None`, and the
    // setup-promoted secondary's discover_items call hits the
    // `RunOutcome::SetupPending observed but src_network is None`
    // programmer-error path (or silently uses the wrong root).
    // Explicit `--src-network=` removes that failure mode by
    // making the wrapper-secondary contract symbolic, not
    // path-existence-dependent.
    //
    // Asserted invariants:
    //   1. The flag is present in `=` form (mandatory under task
    //      #32's argparse-collision rule: leading-dash values
    //      need `--flag=value`).
    //   2. It carries the container path `/app/src-network`,
    //      matching the bind-mount destination on the
    //      `-v "{srcbins_network}:/app/src-network:ro"` line.
    //   3. The flag lands in the container_command suffix AFTER
    //      `--max-memory` (argv-build order); a regression that
    //      moves it into the `podman run` flags block would still
    //      pass invariant 1 but fail this position check.
    let config = SlurmConfig::default();
    let cfg = standard_cfg(&config, &[]);
    let script = generate_wrapper_script(&cfg);
    assert!(
        script.contains("--src-network=/app/src-network"),
        "wrapper script must forward `--src-network=/app/src-network` to \
         secondary so its argparse stores the container-internal bind-\
         mount path on `args.src_network`; render did not contain it"
    );
    let mem_idx = script
        .find("--max-memory=")
        .expect("--max-memory= must be present");
    let srcnet_idx = script
        .find("--src-network=")
        .expect("--src-network= must be present");
    assert!(
        srcnet_idx > mem_idx,
        "--src-network must appear after --max-memory in the secondary's \
         argv (currently at byte {srcnet_idx}, max-memory at {mem_idx}) — \
         a flag in the wrong position likely landed in the podman-run \
         flags block instead of the container_command suffix"
    );
}

#[test]
fn script_forwards_log_dir_container_path() {
    // Log-mount split: the wrapper bind-mounts the gateway's per-run
    // log directory at `/app/log-network` and MUST tell the secondary
    // subprocess to use that as the log-mount root via
    // `--log-dir=/app/log-network`. Without this flag the framework's
    // `resolve_log_dir` falls back to the output-mount root
    // (`/app/out-network`) — pre-split, worker logs landed at
    // `/app/out-network/<timestamp>/<sid>/worker_<N>.log` instead of
    // under `/app/log-network` (asm-tokenizer field report). The fix
    // adds a typed `log_path` on the PyO3 task config plus this
    // explicit framework flag so the wrapper-secondary contract is
    // symbolic (matching `--src-network`), not path-existence-
    // dependent.
    //
    // Asserted invariants:
    //   1. The flag is present in `=` form (task #32 argparse-
    //      collision rule).
    //   2. It carries the container path `/app/log-network`,
    //      matching the bind-mount destination on the
    //      `-v "{log_network}:/app/log-network"` line.
    //   3. The flag lands AFTER `--src-network=` in the
    //      container_command suffix (argv-build order).
    let config = SlurmConfig::default();
    let cfg = standard_cfg(&config, &[]);
    let script = generate_wrapper_script(&cfg);
    assert!(
        script.contains("--log-dir=/app/log-network"),
        "wrapper script must forward `--log-dir=/app/log-network` to \
         secondary so its argparse stores the container-internal log-\
         mount path on `args.log_dir`; render did not contain it"
    );
    // Every container persists its own framework runner log under the
    // gateway-shared log mount, keyed by `secondary_id` so the relocated/
    // co-located primary and each secondary write to distinct files.
    assert!(
        script.contains("-e DYNRUNNER_FULL_LOG_DIR=\"/app/log-network/sec-01\""),
        "wrapper script must inject the per-node runner-log dir so the \
         framework's full log lands host-readably; render did not contain it"
    );
    let srcnet_idx = script
        .find("--src-network=")
        .expect("--src-network= must be present");
    let logdir_idx = script
        .find("--log-dir=")
        .expect("--log-dir= must be present");
    assert!(
        logdir_idx > srcnet_idx,
        "--log-dir must appear after --src-network in the secondary's \
         argv (currently at byte {logdir_idx}, src-network at {srcnet_idx})"
    );
}

#[test]
fn standard_mode_script_contains_gateway() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    assert!(script.contains("gateway.example.com:9000"));
    assert!(script.contains("--secondary-id sec-01"));
    assert!(script.contains("mkfifo"));
    assert!(!script.contains("TUNNEL_PORT"));
    assert!(script.contains("test-app.tar"));
    assert!(script.contains("dynamic_batch_tokenizer --secondary"));
    // Host-IP probe + env plumbing (the bug fix this guards):
    // without these the container's `hostname -I` advertises a
    // non-routable bridge IP and peers can't dial it.
    assert!(script.contains("getent ahostsv4"));
    assert!(script.contains("PRIMARY_NODE_IPV4="));
    assert!(script.contains("-e PRIMARY_NODE_IPV4="));
    assert!(script.contains("-e PRIMARY_NODE_IPV6="));
    // `--pull=never`, `--pids-limit=16384`, and
    // `--ulimit nproc=32768:32768` are framework defaults the
    // wrapper must always emit (commits 48288f7, 9b3dce0, and
    // the nproc framework-default sibling).
    assert!(script.contains("--pull=never"));
    assert!(script.contains("--pids-limit=16384"));
    assert!(script.contains("--ulimit nproc=32768:32768"));
    // Cleanup trap covers SLURM-induced signals (commit 485629c).
    assert!(script.contains("trap cleanup EXIT TERM HUP INT"));
    // The pre-2026-05 inline `setsid -f bash` watchdog has been
    // removed; teardown is now owned by the out-of-cgroup
    // `dynrunner-slurm-shutdown` binary spawned via
    // `systemd-run --user --unit` (service mode) when the caller
    // plumbs `WrapperScriptConfig::shutdown_manager_bin_path`. See
    // `wrapper_script::tests::shutdown_manager` for the
    // positive assertions on the new shape; the negative
    // assertions (watchdog must not reappear) live in
    // `no_watchdog_block_present`. The baseline `standard_cfg`
    // helper renders with `shutdown_manager_bin_path=None` so
    // this end-to-end test deliberately exercises the
    // CMD_RELAY-only cleanup variant.
    // Memory-cap block: both probes (NodeRAM + wrapper cgroup
    // memory.max) must be present so the min() logic engages on
    // any cluster where SLURM imposes a per-job cap tighter than
    // host-MemTotal-2GiB. The renaming from MEM_BYTES to MEM_BYTES_NODE
    // in #31 is intentional — the new shape composes two probes
    // before settling on MEM_BYTES.
    assert!(script.contains("MEM_BYTES_NODE=$(awk"));
    assert!(script.contains("MEM_BYTES_CGROUP="));
    assert!(script.contains("/sys/fs/cgroup/memory.max"));
    assert!(script.contains("${MEM_FLAGS}"));
    // User-policy regression pin: `--memory-swap=-1` (unlimited
    // swap on top of the RAM cap) per explicit instruction.
    // Defends against accidental revert to `--memory-swap=<RAM>`
    // (which would re-introduce immediate cgroup-OOM on RAM
    // overshoot) or to `--memory-swap=<2x RAM>` (podman's
    // unset-flag default — same OOM-on-overshoot behaviour
    // because the swap component is bounded). The string match
    // is exact: `--memory-swap=-1` not `--memory-swap=$<var>`.
    assert!(
        script.contains("--memory-swap=-1"),
        "wrapper must emit `--memory-swap=-1` so workers swap \
         instead of getting cgroup-OOM-killed under RAM pressure; \
         render did not contain it"
    );
    // And the RAM cap must still apply — --memory=<bytes> is
    // load-bearing for the kernel's in-core ceiling.
    assert!(
        script.contains("--memory=${MEM_BYTES}"),
        "wrapper must still emit --memory=<bytes> alongside the \
         unlimited-swap flag — RAM cap is independent of swap cap"
    );
    // FIFO loud-error elif (commit 179afd9).
    assert!(script.contains("disappeared unexpectedly"));
    // Image-load loud-failure marker (commit 733559c).
    assert!(script.contains("ERROR: image load failed"));
    // Container name flow (asm- prefix per L1.7 wire reconciliation).
    assert!(script.contains("--name \"$CONTAINER_NAME\""));
    assert!(script.contains("/tmp/asm-"));
}

#[test]
fn dynrunner_network_dir_emits_volume_and_env() {
    let config = SlurmConfig::default();
    let extra: [String; 0] = [];
    let cfg = WrapperScriptConfig {
        dynrunner_network_dir: Some("/host/dynrunner"),
        extra_run_args: &extra,
        ..standard_cfg(&config, &[])
    };
    let script = generate_wrapper_script(&cfg);
    assert!(script.contains("/host/dynrunner:/app/dynrunner-network"));
    assert!(script.contains("-e DYNRUNNER_NETWORK=\"/app/dynrunner-network\""));
}

#[test]
fn consumer_nproc_ulimit_lands_after_framework_default() {
    let config = SlurmConfig::default();
    let consumer_value = "--ulimit=nproc=65536:65536".to_string();
    let extras = vec![consumer_value.clone()];
    let cfg = standard_cfg(&config, &extras);
    let script = generate_wrapper_script(&cfg);

    let default_idx = script
        .find("--ulimit nproc=32768:32768")
        .expect("framework default `--ulimit nproc=32768:32768` must be present");
    let consumer_idx = script
        .find(consumer_value.as_str())
        .expect("consumer-supplied nproc override must be rendered");
    assert!(
        default_idx < consumer_idx,
        "consumer-supplied nproc override must follow the framework default \
         so podman's last-wins parsing applies the consumer's value; \
         got default at {default_idx} and consumer at {consumer_idx}"
    );
}
