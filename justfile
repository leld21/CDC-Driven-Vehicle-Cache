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
    kubectl delete job register-connector --ignore-not-found
    echo "==> waiting for rollouts..."
    kubectl rollout status deploy/postgres --timeout=180s
    kubectl rollout status deploy/kafka --timeout=180s
    kubectl rollout status deploy/valkey --timeout=180s
    echo "==> restarting Connect (Kafka emptyDir wipes connect_* internal topics)..."
    kubectl apply -f deploy/connect.yaml
    kubectl scale deploy/connect --replicas=0
    sleep 5
    kubectl scale deploy/connect --replicas=1
    kubectl rollout status deploy/connect --timeout=300s
    echo "==> registering Debezium connector (idempotent)..."
    just connector-register
    just topics-ensure
    echo "==> stack is up."

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

# Interactive psql shell
psql:
    kubectl exec -it deploy/postgres -- psql -U nstech -d nstech

# Run a SQL statement. Usage: just sql "INSERT INTO vehicles (id, state) VALUES (1, 'ACTIVE');"
sql statement:
    kubectl exec -i deploy/postgres -- psql -U nstech -d nstech -c {{quote(statement)}}

# Debezium connector status
connector-status:
    kubectl exec deploy/connect -- curl -sf http://localhost:8083/connectors/nstech-postgres/status

# Register or update the Debezium connector (safe to re-run)
connector-register:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "waiting for Kafka Connect REST..."
    until kubectl exec deploy/connect -- curl -sf http://localhost:8083/connectors >/dev/null 2>&1; do
      sleep 3
    done
    echo "registering connector nstech-postgres..."
    kubectl get configmap debezium-connector -o jsonpath='{.data.connector\.json}' \
      | kubectl exec -i deploy/connect -- curl -sf -X PUT \
          http://localhost:8083/connectors/nstech-postgres/config \
          -H 'Content-Type: application/json' \
          -d @-
    echo
    just connector-status
    just topics-ensure

# Delete and re-register the connector (use when CDC stops flowing after a restart)
connector-reset:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "deleting connector nstech-postgres (ignore 404)..."
    kubectl exec deploy/connect -- curl -sf -X DELETE http://localhost:8083/connectors/nstech-postgres || true
    sleep 2
    just connector-register

# Quick health check: connector, topics, sample CDC message, valkey
doctor:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "=== pods ==="
    kubectl get pods
    echo
    echo "=== connector status ==="
    status=$(kubectl exec deploy/connect -- curl -s http://localhost:8083/connectors/nstech-postgres/status 2>/dev/null || echo "")
    if [ -z "$status" ]; then
      echo "(connector missing — run: just connector-register)"
    else
      echo "$status"
      if echo "$status" | grep -q '"tasks":\[\]'; then
        echo
        echo "WARNING: connector RUNNING but tasks=[] (not capturing). Run: just fix-cdc"
      fi
    fi
    echo
    echo "=== topics ==="
    kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep nstech || true
    echo
    echo "=== last vehicles CDC messages (5s) ==="
    kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 --topic nstech.public.vehicles \
      --from-beginning --timeout-ms 5000 --property print.key=true 2>&1 | tail -n 5 || true
    echo
    echo "=== valkey vehicle:1 ==="
    kubectl exec deploy/valkey -- valkey-cli HGETALL vehicle:1 || true
    echo
    echo "=== connect logs (last 15 lines) ==="
    kubectl logs deploy/connect --tail=15

# Show Connect crash logs (current + previous container)
connect-logs:
    #!/usr/bin/env bash
    set -euo pipefail
    pod=$(kubectl get pods -l app=connect -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -z "$pod" ]; then echo "no connect pod"; exit 1; fi
    echo "=== logs $pod (current) ==="
    kubectl logs "$pod" --tail=60 2>/dev/null || true
    echo "=== logs $pod (previous) ==="
    kubectl logs "$pod" --previous --tail=60 2>/dev/null || echo "(no previous)"

# Delete Connect internal Kafka topics (corrupt after Kafka emptyDir restart)
connect-topics-wipe:
    #!/usr/bin/env bash
    set -euo pipefail
    kubectl scale deploy/connect --replicas=0
    for i in $(seq 1 30); do
      kubectl get pods -l app=connect --no-headers 2>/dev/null | grep -q . || break
      sleep 2
    done
    for t in connect_configs connect_offsets connect_statuses; do
      echo "deleting topic $t (if exists)..."
      kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 --delete --topic "$t" 2>/dev/null || true
    done
    echo "connect internal topics wiped"

# Recover CDC after Kafka restart (Connect internal topics were wiped)
fix-cdc:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "==> wiping Connect internal topics + applying manifest..."
    just connect-topics-wipe
    kubectl apply -f deploy/connect.yaml
    echo "==> starting Connect (allow 1–3 min for JVM + plugins)..."
    kubectl scale deploy/connect --replicas=1
    kubectl rollout status deploy/connect --timeout=360s
    just connector-reset
    echo "==> CDC pipeline recovered. Verify with: just doctor"

# Create CDC topics if missing (instant; Debezium also creates them, but this unblocks the worker)
topics-ensure:
    #!/usr/bin/env bash
    set -euo pipefail
    for t in nstech.public.vehicles nstech.public.positions; do
      kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic "$t" --partitions 1 --replication-factor 1
    done
    echo "topics ensured: nstech.public.vehicles nstech.public.positions"

# Wait until Debezium CDC topics exist (fallback when topics-ensure was not run)
wait-topics:
    #!/usr/bin/env bash
    set -euo pipefail
    want=(nstech.public.vehicles nstech.public.positions)
    for i in $(seq 1 60); do
      list=$(kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null || true)
      ok=1
      for t in "${want[@]}"; do
        if ! echo "$list" | grep -qx "$t"; then ok=0; break; fi
      done
      if [ "$ok" -eq 1 ]; then
        echo "CDC topics ready: ${want[*]}"
        exit 0
      fi
      echo "waiting for CDC topics (${i}/60)..."
      sleep 3
    done
    echo "timeout: CDC topics still missing — run: just connector-register && just topics-ensure"
    exit 1

# Build the .NET worker
worker-build:
    dotnet build CDC-Driven-Vehicle-Cache.sln

# Build mock writers + shared scenario library
mocks-build:
    dotnet build src/VehicleWriter/VehicleWriter.csproj
    dotnet build src/PositionWriter/PositionWriter.csproj

# Run only vehicle steps from a scenario (sorted by seq within vehicles[])
mocks-vehicle scenario="scenarios/e2e.json":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
    kubectl port-forward svc/postgres 5432:5432 &
    sleep 2
    export POSTGRES__CONNECTIONSTRING="${POSTGRES__CONNECTIONSTRING:-Host=localhost;Port=5432;Database=nstech;Username=nstech;Password=nstech}"
    dotnet run --project src/VehicleWriter -- --scenario "{{scenario}}"

# Run only position steps from a scenario (sorted by seq within positions[])
mocks-position scenario="scenarios/e2e.json":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
    kubectl port-forward svc/postgres 5432:5432 &
    sleep 2
    export POSTGRES__CONNECTIONSTRING="${POSTGRES__CONNECTIONSTRING:-Host=localhost;Port=5432;Database=nstech;Username=nstech;Password=nstech}"
    export SCENARIO_PATH="{{scenario}}"
    dotnet run --project src/PositionWriter -- --scenario "{{scenario}}"

# Run the full interleaved scenario timeline (global seq order — required for B6 cross-stream race)
mocks-run scenario="scenarios/e2e.json":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
    kubectl port-forward svc/postgres 5432:5432 &
    sleep 2
    export POSTGRES__CONNECTIONSTRING="${POSTGRES__CONNECTIONSTRING:-Host=localhost;Port=5432;Database=nstech;Username=nstech;Password=nstech}"
    export SCENARIO_PATH="{{scenario}}"
    echo "resetting source tables..."
    kubectl exec -i deploy/postgres -- psql -U nstech -d nstech -c "TRUNCATE positions RESTART IDENTITY; TRUNCATE vehicles;"
    just mocks-build
    mapfile -t steps < <(jq -c '
      [.vehicles[] | . + {stream:"vehicles"}] + [.positions[] | . + {stream:"positions"}]
      | sort_by(.seq) | .[]' "{{scenario}}")
    echo "running {{scenario}} (${#steps[@]} steps, interleaved by seq)..."
    for step in "${steps[@]}"; do
      stream=$(echo "$step" | jq -r .stream)
      seq=$(echo "$step" | jq -r .seq)
      echo "--- seq=${seq} stream=${stream} ---"
      if [ "$stream" = "vehicles" ]; then
        dotnet run --project src/VehicleWriter --no-build -- --step "$step"
      else
        dotnet run --project src/PositionWriter --no-build -- --step "$step"
      fi
    done
    echo "mocks-run done"

# Run the worker locally (port-forwards Kafka EXTERNAL + Valkey from the cluster)
worker-run:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
    echo "port-forwarding kafka:9094 (EXTERNAL) and valkey:6379 — Ctrl+C stops all..."
    kubectl port-forward svc/kafka 9094:9094 &
    kubectl port-forward svc/valkey 6379:6379 &
    sleep 2
    export Kafka__BootstrapServers="${Kafka__BootstrapServers:-localhost:9094}"
    export Valkey__ConnectionString="${Valkey__ConnectionString:-localhost:6379}"
    echo "ensuring Debezium connector + CDC topics..."
    just connector-register
    dotnet run --project src/Worker/Worker.csproj

# Build worker container image and import into k3d
worker-image:
    #!/usr/bin/env bash
    set -euo pipefail
    docker build -f src/Worker/Dockerfile -t nstech-worker:local .
    k3d image import nstech-worker:local -c {{cluster}}

# Deploy the worker pod (requires: just worker-image)
worker-deploy:
    kubectl apply -f deploy/worker.yaml
    kubectl rollout status deploy/worker --timeout=120s

# Read a vehicle hash from Valkey. Usage: just valkey-get 1
valkey-get vehicle_id:
    kubectl exec deploy/valkey -- valkey-cli HGETALL vehicle:{{vehicle_id}}

# Full end-to-end: stack up -> mocks -> worker -> assert -> replay-rebuild (spec §9)
e2e scenario="scenarios/e2e.json":
    #!/usr/bin/env bash
    set -euo pipefail
    export SCENARIO="{{scenario}}"
    bash scripts/e2e.sh
