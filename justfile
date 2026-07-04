set shell := ["bash", "-euo", "pipefail", "-c"]

cluster := env_var_or_default("K3D_CLUSTER", "nstech")

# List available recipes
default:
    @just --list

# Bring up the local Kubernetes cluster (k3d)
up:
    #!/usr/bin/env bash
    set -euo pipefail
    if k3d cluster list 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "{{cluster}}"; then
      echo "k3d cluster '{{cluster}}' already exists"
    else
      echo "creating k3d cluster '{{cluster}}'..."
      k3d cluster create {{cluster}} --wait
    fi
    kubectl cluster-info

# Delete the local cluster
down:
    -k3d cluster delete {{cluster}}

# Show cluster status
status:
    k3d cluster list
    kubectl get nodes -o wide

# Full end-to-end run (to be implemented during the build phase):
# env up -> mocks write to Postgres -> Debezium -> Kafka -> worker -> Valkey
# -> assert the view is correct -> replay from offset 0 -> assert identical cache
e2e:
    @echo "TODO(build phase): implement the end-to-end + replay-rebuild check"
