{
  description = "distributed_prompt — horizontally scalable prompts for RLMs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        python = pkgs.python312;
        pythonEnv = python.withPackages (ps: [ ps.boto3 ps.pytest ps.setuptools ]);
      in {
        devShells.default = pkgs.mkShell {
          packages = [ pythonEnv ];
          shellHook = ''
            export PYTHONPATH="$PWD:$PYTHONPATH"
            # wrapper so `distributed_prompt` CLI works
            distributed_prompt() { ${pythonEnv}/bin/python -m distributed_prompt.cli "$@"; }
            export -f distributed_prompt
            dprompt() { ${pythonEnv}/bin/python -m distributed_prompt.cli "$@"; }
            export -f dprompt
            echo "distributed_prompt dev shell (Python ${python.version})"
          '';
        };
      }
    );
}
