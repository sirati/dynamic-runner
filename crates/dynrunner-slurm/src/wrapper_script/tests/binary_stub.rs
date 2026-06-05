//! Renderer↔binary contract for the wrapper-binary stub path
//! (`WrapperScriptConfig::wrapper_bin_path = Some(...)`):
//!
//!   1. **De-hardcoding parity** — with `wrapper_bin_path = None`, the
//!      legacy bash body still renders, but the `/tmp/<prefix>-…` scratch
//!      dir and `<prefix>-…-<sid>` container name now come from
//!      `name_prefix`, not the old hardcoded `asm` literal.
//!   2. **Stub round-trip** — with `wrapper_bin_path = Some(bin)`, the
//!      rendered body is a `#!/usr/bin/env bash` + single `exec <bin>
//!      <args…>` line, and shell-splitting those args back through the
//!      config crate's `cli` parser reconstructs the exact `WrapperConfig`
//!      the renderer mapped from `WrapperScriptConfig`. This is the
//!      anti-drift guard between the renderer (`generate.rs`) and the
//!      musl wrapper binary that consumes the flags.

use std::path::Path;

use dynrunner_slurm_wrapper_config::{
    ConnectionMode as WireConnectionMode, WrapperConfig, parse_args,
};

use super::standard_cfg;
use crate::config::SlurmConfig;
use crate::wrapper_script::{ConnectionMode, generate_wrapper_script};

fn cfg_config() -> SlurmConfig {
    SlurmConfig {
        root_folder: "/srv/slurm".into(),
        ..SlurmConfig::default()
    }
}

/// Legacy bash path: the scratch dir and container name are derived from
/// `name_prefix`, NOT the old hardcoded `asm`. A non-`asm` prefix must
/// appear in both literals and `asm` must NOT.
#[test]
fn legacy_bash_uses_name_prefix_not_hardcoded_asm() {
    let config = cfg_config();
    let mut cfg = standard_cfg(&config, &[]);
    cfg.name_prefix = "myprog";
    cfg.secondary_id = "sec-9";
    let script = generate_wrapper_script(&cfg);

    assert!(
        script.contains("RNDTMP=\"/tmp/myprog-"),
        "scratch dir must use name_prefix; got: {script}",
    );
    assert!(
        script.contains("CONTAINER_NAME=\"myprog-") && script.contains("-sec-9\""),
        "container name must use name_prefix and secondary_id; got: {script}",
    );
    // The functional scratch/container assignments must NOT carry the
    // old hardcoded `asm` literal. (Preflight-cleanup PROSE comments
    // still mention `/tmp/asm-XXXX` as a historical example and the
    // scan itself uses a `/tmp/*/storage` glob — neither is a
    // name_prefix-derived literal, so they are out of scope here.)
    assert!(
        !script.contains("RNDTMP=\"/tmp/asm-") && !script.contains("CONTAINER_NAME=\"asm-"),
        "no residual hardcoded `asm` literal in the RNDTMP / CONTAINER_NAME assignments",
    );
}

/// Stub path: rendered body is exactly the shebang + a single `exec`
/// line pointing at the supplied binary.
#[test]
fn binary_stub_shape() {
    let config = cfg_config();
    let bin = Path::new("/gw/dynrunner-slurm-wrapper");
    let mut cfg = standard_cfg(&config, &[]);
    cfg.wrapper_bin_path = Some(bin);
    let script = generate_wrapper_script(&cfg);

    let lines: Vec<&str> = script.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "stub is shebang + one exec line; got: {script}"
    );
    assert_eq!(lines[0], "#!/usr/bin/env bash");
    assert!(
        lines[1].starts_with("exec /gw/dynrunner-slurm-wrapper "),
        "exec line must invoke the supplied binary; got: {}",
        lines[1],
    );
}

/// Minimal POSIX-shell word splitter sufficient for the stub line: the
/// renderer `bash_quote`s every arg, so tokens are either bare
/// (safe-chars only) or single-quoted with `'\''`-style apostrophe
/// escaping. Mirrors how bash itself re-splits the `exec` line.
fn shell_split(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    let mut in_word = false;
    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' => {
                if in_word {
                    out.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            '\'' => {
                in_word = true;
                // Consume until the closing single-quote. `bash_quote`
                // renders an embedded apostrophe as `'\''` — i.e. close
                // quote, escaped literal `'`, reopen quote — which this
                // loop handles naturally because the `\'` lands outside
                // the quoted span as a backslash-escaped char.
                for q in chars.by_ref() {
                    if q == '\'' {
                        break;
                    }
                    cur.push(q);
                }
            }
            '\\' => {
                in_word = true;
                if let Some(&n) = chars.peek() {
                    cur.push(n);
                    chars.next();
                }
            }
            other => {
                in_word = true;
                cur.push(other);
            }
        }
    }
    if in_word {
        out.push(cur);
    }
    out
}

/// The render-time `WrapperConfig` the stub MUST encode for a populated
/// standard-mode config. Mirrors `generate_wrapper_stub`'s field mapping
/// so the round-trip asserts against the intended contract, not the
/// renderer's own output.
fn expected_wire(cfg_name_prefix: &str, rand_suffix: &str) -> WrapperConfig {
    WrapperConfig {
        name_prefix: cfg_name_prefix.to_string(),
        rand_suffix: rand_suffix.to_string(),
        secondary_id: "sec-01".to_string(),
        image_path: "/images/test.tar".to_string(),
        image_tar_basename: "test-app.tar".to_string(),
        // Mirrors `standard_cfg`'s baseline digest; the stub encodes it
        // and the cli parser must reconstruct it (anti-drift coverage
        // for the image-digest plumb).
        image_digest: "testdigest0001".to_string(),
        image_name: "test-app".to_string(),
        image_tag: "latest".to_string(),
        load_command:
            "podman --root \"$PODMAN_STORAGE\" --runroot \"$PODMAN_RUN\" --cgroup-manager=cgroupfs load < \"$LOCAL_IMAGE\""
                .to_string(),
        container_command: "dynamic_batch_tokenizer".to_string(),
        cores_spec: "0".to_string(),
        max_memory_spec: "-2G".to_string(),
        mem_manager_reserved_bytes: None,
        forwarded_argv: vec![],
        extra_run_args: vec![],
        // `srcbins_mount_source`/`output_dir`/`run_log_dir` are None in the
        // baseline, so the stub resolves them from the SlurmConfig — the
        // same fallback the legacy bash uses.
        srcbins_network: cfg_config().src_bins_path(),
        output_network: cfg_config().output_path(),
        log_network: cfg_config().log_path(),
        dynrunner_network_dir: None,
        connection: WireConnectionMode::Standard {
            gateway_host: "gateway.example.com".to_string(),
            gateway_port: 9000,
        },
        is_observer: false,
        shutdown_manager_bin_path: None,
    }
}

/// End-to-end: render the stub, shell-split the `exec` args, parse them
/// back through the wrapper binary's own `cli` parser, and assert the
/// reconstructed `WrapperConfig` equals what the renderer was asked to
/// encode. `rand_suffix` is render-time random, so it is read out of the
/// reconstructed config and folded into the expectation.
#[test]
fn binary_stub_round_trips_through_cli_parser() {
    let config = cfg_config();
    let bin = Path::new("/gw/dynrunner-slurm-wrapper");
    let mut cfg = standard_cfg(&config, &[]);
    cfg.name_prefix = "asm";
    cfg.wrapper_bin_path = Some(bin);
    let script = generate_wrapper_script(&cfg);

    let exec_line = script.lines().nth(1).expect("stub has an exec line");
    let mut tokens = shell_split(exec_line);
    assert_eq!(tokens.first().map(String::as_str), Some("exec"));
    assert_eq!(
        tokens.get(1).map(String::as_str),
        Some("/gw/dynrunner-slurm-wrapper")
    );
    // Drop `exec` + the binary path; clap wants argv[0] = program name.
    let arg_tail = tokens.split_off(2);
    let mut argv = vec!["dynrunner-slurm-wrapper".to_string()];
    argv.extend(arg_tail);

    let parsed = parse_args(argv).expect("stub args must parse back via the cli parser");

    let expected = expected_wire("asm", &parsed.rand_suffix);
    assert_eq!(
        parsed, expected,
        "stub args must round-trip to the intended WrapperConfig"
    );
}

/// Reverse-mode + populated optional fields must round-trip too: the
/// connection discriminator flips, `dynrunner_network_dir` /
/// `mem_manager_reserved_bytes` / `shutdown_manager_bin_path` are Some,
/// and the two list flags carry order-sensitive multi-entry values.
#[test]
fn binary_stub_round_trips_reverse_with_optionals() {
    let config = cfg_config();
    let bin = Path::new("/gw/dynrunner-slurm-wrapper");
    let extras = vec!["--ulimit".to_string(), "nofile=8192:8192".to_string()];
    let fwd = vec!["--platform".to_string(), "x86".to_string()];
    let mut cfg = standard_cfg(&config, &extras);
    cfg.name_prefix = "asm";
    cfg.wrapper_bin_path = Some(bin);
    cfg.connection = ConnectionMode::Reverse {
        connection_info_dir: "/logs/connection_info",
    };
    cfg.dynrunner_network_dir = Some("/net/dynrunner");
    cfg.mem_manager_reserved_bytes = Some(524_288_000);
    let sm_bin = Path::new("/gw/dynrunner-slurm-shutdown");
    cfg.shutdown_manager_bin_path = Some(sm_bin);
    cfg.forwarded_argv = &fwd;
    let script = generate_wrapper_script(&cfg);

    let exec_line = script.lines().nth(1).expect("stub has an exec line");
    let mut tokens = shell_split(exec_line);
    let arg_tail = tokens.split_off(2);
    let mut argv = vec!["dynrunner-slurm-wrapper".to_string()];
    argv.extend(arg_tail);
    let parsed = parse_args(argv).expect("reverse-mode stub args must parse back");

    assert_eq!(
        parsed.connection,
        WireConnectionMode::Reverse {
            connection_info_dir: "/logs/connection_info".to_string()
        }
    );
    assert_eq!(
        parsed.dynrunner_network_dir.as_deref(),
        Some("/net/dynrunner")
    );
    assert_eq!(parsed.mem_manager_reserved_bytes, Some(524_288_000));
    assert_eq!(
        parsed.shutdown_manager_bin_path.as_deref(),
        Some(Path::new("/gw/dynrunner-slurm-shutdown"))
    );
    assert_eq!(parsed.forwarded_argv, vec!["--platform", "x86"]);
    assert_eq!(parsed.extra_run_args, vec!["--ulimit", "nofile=8192:8192"]);
}
