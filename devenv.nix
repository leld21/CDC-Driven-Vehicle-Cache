{ pkgs, lib, config, ... }:

{
  # CDC-Driven Vehicle Cache - developer environment
  # All CLI tooling for the reproducible local stack lives here so that a
  # fresh checkout only needs: nix + devenv (+ direnv) and Docker.

  packages = [
    pkgs.just # task runner (just up / just e2e)
    pkgs.k3d # local Kubernetes in Docker
    pkgs.kubectl # k8s CLI
    pkgs.kubernetes-helm # chart installs (kafka, postgres, etc.)
    pkgs.devspace # dev workflow / deploy to the local cluster
    pkgs.jq # JSON wrangling in scripts/tests
    pkgs.git
  ];

  # .NET 10 for the worker and the mock writers.
  languages.dotnet = {
    enable = true;
    package = pkgs.dotnet-sdk_10;
  };

  env = {
    # Stable k3d cluster name reused by the just recipes.
    K3D_CLUSTER = "nstech";
    KUBECONFIG = "${config.env.DEVENV_STATE}/kubeconfig";
  };

  enterShell = ''
    echo "nstech dev env ready:"
    echo "  dotnet   $(dotnet --version)"
    echo "  just     $(just --version)"
    echo "  k3d      $(k3d version | head -n1)"
    echo "  kubectl  $(kubectl version --client 2>/dev/null | head -n1)"
    echo "  devspace $(devspace --version)"
    echo "Run 'just' to list recipes."
  '';
}
