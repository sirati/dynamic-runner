//! Tests for the bash-quoting boundary: `extra_run_args` (podman run
//! flags) and `forwarded_argv` (in-container CLI tail) both must
//! pass through `bash_quote` so shell metacharacters in caller input
//! don't break the rendered script.

use crate::config::SlurmConfig;
use crate::wrapper_script::{generate_wrapper_script, WrapperScriptConfig};

use super::standard_cfg;

#[test]
fn extra_run_args_are_bash_quoted_and_appear_before_image_ref() {
    let config = SlurmConfig::default();
    let extras = vec!["--ulimit=nofile=65536".to_string(), "--shm-size=2g".to_string()];
    let cfg = standard_cfg(&config, &extras);
    let script = generate_wrapper_script(&cfg);
    for flag in &extras {
        assert!(
            script.contains(flag),
            "expected extra_run_args entry {flag:?} to appear in rendered script"
        );
    }
    let image_idx = script.find("test-app:latest").expect("image ref present");
    let extra_idx = script.find("--ulimit=nofile=65536").expect("extra arg present");
    assert!(
        extra_idx < image_idx,
        "extra_run_args must precede the image ref; podman parses left-to-right"
    );
}

#[test]
fn extra_run_args_with_metacharacters_are_quoted() {
    let config = SlurmConfig::default();
    let extras = vec!["--annotation=hello world".to_string()];
    let cfg = standard_cfg(&config, &extras);
    let script = generate_wrapper_script(&cfg);
    // The space forces single-quoting.
    assert!(script.contains("'--annotation=hello world'"));
}

/// Tier-2 setup-promote dispatch repro: forwarded task argv MUST
/// reach the secondary's container_command, immediately after the
/// framework-emitted `--src-network={...}` flag, so the setup-
/// promoted secondary's argparse re-parses task-side filter flags
/// (`--platform`, `--compiler`, `--name-regex`, …) and
/// `task.discover_items` sees the same selection the dispatcher saw.
/// Pre-fix only `--cores` and `--max-memory` were plumbed; the
/// secondary ran discovery against the unfiltered corpus and the
/// dispatch path reported `tasks=0`.
#[test]
fn forwarded_argv_lands_after_framework_flags_in_secondary_argv() {
    let config = SlurmConfig::default();
    let forwarded = vec![
        "--platform".to_string(),
        "x64".to_string(),
        "--name-regex".to_string(),
        "x64-gcc-*-*_minigzipsh".to_string(),
    ];
    let cfg = WrapperScriptConfig {
        forwarded_argv: &forwarded,
        ..standard_cfg(&config, &[])
    };
    let script = generate_wrapper_script(&cfg);

    // Each forwarded token must appear, with shell-special chars
    // single-quoted by bash_quote. `x64` is safe and stays bare;
    // `x64-gcc-*-*_minigzipsh` contains `*` and gets quoted.
    assert!(
        script.contains("--platform x64"),
        "forwarded `--platform x64` missing from rendered script"
    );
    assert!(
        script.contains("--name-regex 'x64-gcc-*-*_minigzipsh'"),
        "forwarded `--name-regex` value must be single-quoted (contains glob chars); \
         render did not contain the quoted form"
    );

    // Position: forwarded tokens MUST follow `--src-network={path}`
    // so argparse on the secondary sees the framework flags first
    // (matching the dispatcher's argv order). Guards against a
    // regression that splices the tokens before the framework
    // flags, where a future framework-flag rename could collide
    // with a task flag name.
    let src_network_idx = script
        .find("--src-network=/app/src-network")
        .expect("framework `--src-network={path}` must be present");
    let platform_idx = script
        .find("--platform x64")
        .expect("forwarded `--platform` must be present");
    assert!(
        platform_idx > src_network_idx,
        "forwarded argv must follow `--src-network` in the secondary's argv \
         (currently at byte {platform_idx}, src-network at {src_network_idx})"
    );
}

/// Empty forwarded_argv must collapse to no rendered diff: the
/// secondary's container_command line ends with the framework's
/// final emitted flag (`--log-dir=/app/log-network` since the
/// log-mount split landed) and nothing else. Guards against
/// accidentally introducing a trailing space, empty quote, or
/// stray separator when the consumer passes no extra args.
#[test]
fn empty_forwarded_argv_emits_no_trailing_tokens() {
    let config = SlurmConfig::default();
    let script = generate_wrapper_script(&standard_cfg(&config, &[]));
    // The container_command line ends with `--log-dir={path}`
    // (the last framework-emitted flag) followed immediately by
    // the next line break (no trailing space, no stray quote).
    // Asserting on the joined byte sequence is the strictest way
    // to defend the no-diff invariant.
    assert!(
        script.contains("--log-dir=/app/log-network\n"),
        "with empty forwarded_argv the container_command line must \
         end at `--log-dir={{path}}` with no trailing token"
    );
}

/// Tokens with shell metacharacters (single quotes, spaces, glob
/// chars) must use bash_quote's single-quote-with-escape shape so
/// the bash interpreter reassembles the original byte sequence on
/// the secondary side. Substring assertions on the bash-quoted
/// renderings — the `rendered_scripts_pass_bash_syntax_check` test
/// above runs `bash -n` on the full script which already covers
/// "syntactically valid bash"; this test pins the specific quoting
/// shape so a future bash_quote rewrite can't silently change the
/// escape pattern without flipping a test.
#[test]
fn forwarded_argv_metacharacters_use_single_quote_escape() {
    let config = SlurmConfig::default();
    let forwarded = vec![
        "--label=it's".to_string(),
        "--annotation=hello world".to_string(),
    ];
    let cfg = WrapperScriptConfig {
        forwarded_argv: &forwarded,
        ..standard_cfg(&config, &[])
    };
    let script = generate_wrapper_script(&cfg);
    // Embedded apostrophe: closes the current single-quote run,
    // emits a backslash-escaped apostrophe, opens a new run.
    // Same shape Python's `shlex.quote` uses; tested above for
    // bash_quote in isolation, asserted here at the rendered-script
    // layer to catch a regression where the wrapper bypasses
    // bash_quote.
    assert!(
        script.contains(r"'--label=it'\''s'"),
        "embedded apostrophe in forwarded arg must be escaped as `'\\''`; \
         rendered script lacks the expected pattern"
    );
    // Spaces force single-quoting around the whole token.
    assert!(
        script.contains("'--annotation=hello world'"),
        "space in forwarded arg must force single-quoting; rendered \
         script lacks the quoted form"
    );
}

