{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-parts,
      rust-overlay,
      crane,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      flake = {
        nixConfig = {
          extra-substituters = [ "https://n1.cachix.org" ];
          extra-trusted-public-keys = [
            "n1.cachix.org-1:vQ3RpPAz7vsJCg0PIWXYuzG+RrgV4fJ1uQkuEvcUfQI="
          ];
        };
      };

      perSystem =
        {
          self',
          inputs',
          pkgs,
          system,
          ...
        }:
        let
          rust-nightly-version = "2025-08-01";
          rust-toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
          rust-toolchain-nightly =
            pkgs.rust-bin.nightly."${rust-nightly-version}".default;
          craneLib = (crane.mkLib pkgs).overrideToolchain (_: rust-toolchain);

          craneCommon = rec {
            src = craneLib.cleanCargoSource ./.;
            strictDeps = true;
            nativeBuildInputs = [ pkgs.protobuf ];
            cargoTestCommand = "cargo test";
            cargoCheckCommand = "cargo clippy --profile release";
            cargoCheckExtraArgs = "--all-targets -- --deny=warnings";
            cargoClippyExtraArgs = "--all-targets -- --deny=warnings";
          };

          # cache keyed by Cargo.lock
          crateArtifacts = craneLib.buildDepsOnly craneCommon;

          crateClippy = craneLib.cargoClippy (
            craneCommon // { cargoArtifacts = crateArtifacts; }
          );
          crate = craneLib.buildPackage (
            craneCommon // { cargoArtifacts = crateArtifacts; }
          );

          treefmt = pkgs.treefmt.withConfig {
            runtimeInputs = [
              pkgs.nixfmt
              rust-toolchain-nightly
            ];
            settings = {
              on-unmatched = "info";
              formatter.nixfmt = {
                command = "nixfmt";
                options = [
                  "--strict"
                  "--width"
                  80
                ];
                includes = [ "*.nix" ];
              };
              formatter.rustfmt = {
                command = "rustfmt";
                options = [
                  "--config"
                  "skip_children=true"
                ];
                includes = [ "*.rs" ];
              };
            };
          };
        in
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            config.allowUnfree = true;
            overlays = [ rust-overlay.overlays.default ];
          };

          formatter = treefmt;

          checks = { inherit crate crateClippy; };

          packages.default = crate;

          devShells.default = pkgs.mkShell {
            buildInputs = [
              pkgs.rustup
              treefmt
              pkgs.coreutils
              pkgs.protobuf
            ];
          };
        };
    };
}
