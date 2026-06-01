final: prev: {
  # Inject `dynamic-runner` into every Python package set on the consuming
  # nixpkgs instance. Consumers can then reference it as
  # `final.python3Packages.dynamic-runner` (or any other pythonXYPackages
  # set), exactly like an upstream nixpkgs Python package.
  pythonPackagesExtensions = (prev.pythonPackagesExtensions or [ ]) ++ [
    (pyFinal: pyPrev: {
      dynamic-runner = pyFinal.callPackage ./wheel.nix {
        # Build the musl-static shutdown-manager binary on the
        # consumer's nixpkgs and pass it through. The binary's
        # derivation is shared with `flake.nix` — single source of
        # truth in `./shutdown-manager-bin.nix`.
        #
        # `final` (not `pyFinal`) is the right pkgs scope: the
        # shutdown-manager-bin derivation needs `pkgsCross`, which
        # lives on the top-level pkgs, not on the Python package set.
        shutdownManagerBin = final.callPackage ./shutdown-manager-bin.nix { };
        # Musl-static slurm-wrapper binary, same `pkgsCross` plumbing
        # rationale as `shutdownManagerBin` above. Single source of
        # truth in `./wrapper-bin.nix`.
        wrapperManagerBin = final.callPackage ./wrapper-bin.nix { };
      };
    })
  ];
}
