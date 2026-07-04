set shell := ["bash", "-euo", "pipefail", "-c"]

cluster := env_var_or_default("K3D_CLUSTER", "nstech")

# List available recipes
default:
    @just --list

# Bring up the local cluster (k3d) and deploy the full stack via devspace
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
    echo "==> deploying stack with devspace..."
    devspace deploy --no-warn
    echo "==> waiting for rollouts..."
    kubectl rollout status deploy/postgres --timeout=180s
    kubectl rollout status deploy/kafka --timeout=180s
    kubectl rollout status deploy/valkey --timeout=180s
    kubectl rollout status deploy/connect --timeout=300s
    echo "==> waiting for Debezium connector registration..."
    kubectl wait --for=condition=complete job/register-connector --timeout=300s
    echo "==> stack is up. connector status:"
    just connector-status || true

# Delete the local cluster
down:
    -k3d cluster delete {{cluster}}

# Show cluster + workload status
status:
    k3d cluster list
    kubectl get nodes -o wide
    -kubectl get pods -o wide

# List Kafka topics
topics:
    kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Consume a topic from the beginning (keys + values). Usage: just peek nstech.public.vehicles
peek topic="nstech.public.vehicles" timeout="8000":
    kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 --topic {{topic}} --from-beginning \
      --timeout-ms {{timeout}} --property print.key=true

# Open a psql shell (or run SQL). Usage: just psql -c "select * from vehicles;"
psql *args:
    kubectl exec -i deploy/postgres -- psql -U nstech -d nstech {{args}}

# Debezium connector status
connector-status:
    kubectl exec deploy/connect -- curl -s http://localhost:8083/connectors/nstech-postgres/status

# Full end-to-end run (to be implemented during the build phase):
# env up -> mocks write to Postgres -> Debezium -> Kafka -> worker -> Valkey
# -> assert the view is correct -> replay from offset 0 -> assert identical cache
e2e:
    @echo "TODO(build phase): implement the end-to-end + replay-rebuild check"
