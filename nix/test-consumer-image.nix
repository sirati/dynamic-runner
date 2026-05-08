/*
  Layered podman image bundling the synthetic e2e test_consumer.

  Bundled because the e2e suite needs a `.#dockerImage` flake target
  for `--packaging podman` runs against the slurm-test-env. Synthesises
  guidance from `asm-tokenizer/flake.nix` and
  `asm-dataset-nix/nix/docker-image.nix` (the canonical references):

    - `dockerTools.buildLayeredImage` (NOT streamLayered — the
      framework's `layered_transfer` consumes the on-disk tarball
      and dedupes per-blob; the streaming variant defeats that).
    - Single `python3.withPackages` carrying `dynamic-runner`; the
      wheel is resolved through site-packages so no PYTHONPATH /
      entry-point wrappers are needed for the framework wheel
      itself. PYTHONPATH is set so `python -m tests.e2e.test_consumer`
      finds the bundled module tree.
    - Source slice at `/app/tests` (mirroring asm-dataset-nix's
      `/app/python/<pkg>` shape) so `WorkingDir=/app` lets the
      framework run `python -m tests.e2e.test_consumer` against
      the bundled module tree.
    - Explicit NSS DB + nsswitch.conf (passwd / group / shadow /
      nsswitch). `dockerTools.buildLayeredImage` does NOT bake an
      NSS DB by default; without it anything that consults
      `/etc/passwd` (sshd, sudo, getpwuid(0)) fails. The synthetic
      consumer doesn't currently need it but the cost is negligible
      and guards against future workloads that do.
    - `Entrypoint = ["python", "-m"]`, `Cmd = ["tests.e2e.test_consumer", "--help"]`.
      The framework's slurm wrapper REPLACES `Cmd` with the actual
      module + args at run time
      (`crates/dynrunner-slurm/src/wrapper_script.rs`); the default
      Cmd here just makes `podman run <image>` self-explanatory
      for ad-hoc smoke checks.

  Skips semantic layering: the e2e harness isn't optimizing
  first-cold-upload time and the
  `nix-docker-layered-image` overlay would be a fresh input pinned
  for this single output. If the layered_transfer dedup path
  becomes a thing we want to assert under e2e load, swap in the
  pipeline at that point per asm-tokenizer's §4.
*/
{
  pkgs,
  dynamic-runner,
  testsSrc,
  name ? "dynrunner-e2e-test",
  tag ? "latest",
}:

let
  pythonEnv = pkgs.python3.withPackages (_: [ dynamic-runner ]);

  # Source slice mounted at /app/tests so `python -m tests.e2e.test_consumer`
  # resolves the bundled module tree. `__init__.py` files (tests/,
  # tests/e2e/, tests/e2e/test_consumer/) all ship in the source
  # tree already; this just lands them at /app/tests.
  appSrc = pkgs.runCommand "${name}-src" { } ''
    mkdir -p $out/app/tests
    cp -r ${testsSrc}/. $out/app/tests/
    chmod -R +w $out/app
  '';

  # Minimal NSS / nsswitch files. Baked at build time because
  # `buildLayeredImage` does not generate them, and the synthetic
  # workload + any future shell-out (sshd debug, sudo, etc.) needs
  # `/etc/passwd` populated.
  nssFiles = pkgs.runCommand "${name}-nss" { } ''
    mkdir -p $out/etc
    cat > $out/etc/passwd <<EOF
    root:x:0:0:root:/root:${pkgs.bash}/bin/bash
    nobody:x:65534:65534:Nobody:/var/empty:/bin/false
    EOF
    cat > $out/etc/group <<'EOF'
    root:x:0:
    nogroup:x:65534:
    EOF
    cat > $out/etc/shadow <<'EOF'
    root::1::::::
    nobody:!:1::::::
    EOF
    cat > $out/etc/nsswitch.conf <<'EOF'
    passwd: files
    group: files
    shadow: files
    hosts: files dns
    EOF
    sed -i 's/^    //' $out/etc/passwd $out/etc/group $out/etc/shadow $out/etc/nsswitch.conf
    chmod 644 $out/etc/passwd $out/etc/group $out/etc/nsswitch.conf
    chmod 600 $out/etc/shadow
  '';
in

pkgs.dockerTools.buildLayeredImage {
  inherit name tag;
  contents = [
    pythonEnv
    appSrc
    nssFiles
    pkgs.bash
    pkgs.coreutils
    pkgs.cacert
  ];
  extraCommands = ''
    mkdir -p root tmp
    chmod 1777 tmp
  '';
  config = {
    Entrypoint = [
      "${pythonEnv}/bin/python"
      "-m"
    ];
    Cmd = [
      "tests.e2e.test_consumer"
      "--help"
    ];
    Env = [
      "LANG=C.UTF-8"
      "PYTHONPATH=/app"
      "PATH=/usr/local/bin:/usr/bin:/bin"
      "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
      "HOME=/root"
    ];
    WorkingDir = "/app";
  };
}
