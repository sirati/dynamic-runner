{
  lib,
  buildPythonPackage,
  python,
  rustPlatform,
  openssl,
  pkg-config,
  pkgs,
  # The two musl-static helper binaries default to building the in-repo
  # derivations on the TOP-LEVEL pkgs. They must be built there (not via
  # the Python package-set callPackage) because the cross/musl
  # rustPlatform needs `pkgsCross`, which some consumer nixpkgs pins do
  # not expose on the Python package scope (this is why overlay.nix /
  # flake.nix build them with the top-level `callPackage`). Defaulting
  # them means a bare `python3Packages.callPackage ./nix/wheel.nix {}`
  # bundles both binaries, so a consumer needs no special preparation.
  # flake.nix and overlay.nix still pass them explicitly to single-source
  # the derivation shared with the `dynrunner-slurm-{shutdown,wrapper}`
  # outputs.
  shutdownManagerBin ? pkgs.callPackage ./shutdown-manager-bin.nix { },
  wrapperManagerBin ? pkgs.callPackage ./wrapper-bin.nix { },
}:

# Wheel/Python-package derivation for dynamic_runner.
#
# The PyO3 native extension lives in crates/dynrunner-pyo3 and is built
# via maturin. The resulting Python module is `dynamic_runner._native`
# (configured via [tool.maturin] module-name in the root pyproject.toml).
#
# The cargoDeps hash below is the SRI of the vendored Cargo deps. Any
# Cargo.lock change (added/removed/version-bumped crates, including
# workspace.package version edits which propagate into per-crate
# version entries) invalidates it; recalibrate by setting
#   hash = lib.fakeHash;
# running `nix build .#dynamic-runner --max-jobs 6 --cores 4`, copying
# the "got: sha256-..." value from the failure into this field, and
# only then committing + pushing.
buildPythonPackage {
  pname = "dynamic-runner";
  version = "0.4.0";
  pyproject = true;

  src = lib.cleanSource ./..;

  cargoDeps = rustPlatform.fetchCargoVendor {
    src = lib.cleanSource ./..;
    hash = "sha256-8OuVXPKiMJprmlGK+sYxyIHzaKSyUtDcZSam8cwzi8g=";
  };

  nativeBuildInputs = [
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
    pkg-config
  ];

  buildInputs = [ openssl ];

  doCheck = false;

  # Drop the musl-static shutdown-manager binary into the installed
  # package tree so `dynamic_runner._shutdown_manager.bundled_binary_path()`
  # resolves to it. Runs after maturin's install step (which writes
  # `_native.<ext>.so` + the python source tree under `${out}/${python.sitePackages}/dynamic_runner/`),
  # so we just need to land the file at the right path with the
  # right mode. Bypassing the wheel manifest is intentional: this is
  # a nix-derivation install, not a `pip install`, so the file just
  # needs to live on disk under the import-path; no `RECORD` entry
  # required for the framework's import-time resolution path to find
  # it via `importlib.resources.files`.
  postInstall = ''
    install -Dm755 \
      ${shutdownManagerBin}/bin/dynrunner-slurm-shutdown \
      $out/${python.sitePackages}/dynamic_runner/_shutdown_manager/dynrunner-slurm-shutdown
    install -Dm755 \
      ${wrapperManagerBin}/bin/dynrunner-slurm-wrapper \
      $out/${python.sitePackages}/dynamic_runner/_wrapper_manager/dynrunner-slurm-wrapper
  '';

  meta = with lib; {
    description = "Multi-process / multi-host Python task runner backed by a Rust workspace.";
    homepage = "https://github.com/sirati/dynamic-runner";
    license = licenses.asl20;
    platforms = platforms.unix;
  };
}
