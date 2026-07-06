#!/usr/bin/env bash
# End-to-end: stack up -> mocks -> worker -> assert -> replay-rebuild -> re-assert
# See docs/spec.md §9 and scenarios/e2e.json
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SCENARIO="${SCENARIO:-scenarios/e2e.json}"
WORKER_PID=""

cleanup() {
  if [ -n "${WORKER_PID}" ]; then
    kill "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
    WORKER_PID=""
  fi
  kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT

kill_stale_workers() {
  pkill -f 'Nstech.Worker' 2>/dev/null || true
  pkill -f 'src/Worker/Worker.csproj' 2>/dev/null || true
  sleep 2
}

ensure_port_forward() {
  local port="$1" target="$2"
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null || true
  else
    pkill -f "port-forward ${target} ${port}:" 2>/dev/null || true
    pkill -f "port-forward svc/${target} ${port}:" 2>/dev/null || true
  fi
  sleep 1
  kubectl port-forward "svc/${target}" "${port}:${port}" &
}

valkey_field() {
  local id="$1" field="$2"
  kubectl exec deploy/valkey -- valkey-cli HGET "vehicle:${id}" "${field}" 2>/dev/null | tr -d '\r'
}

wait_lag_zero() {
  local group="$1" timeout="${2:-180}"
  echo "waiting for consumer group ${group} lag=0 (timeout ${timeout}s)..."
  for _ in $(seq 1 "${timeout}"); do
    if ! kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:9092 --group "${group}" --describe &>/dev/null; then
      sleep 1
      continue
    fi
    lag=$(kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --group "${group}" --describe 2>/dev/null \
      | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
    if [ "${lag:-0}" -eq 0 ]; then
      echo "consumer group ${group} lag=0"
      return 0
    fi
    sleep 1
  done
  echo "TIMEOUT: consumer group ${group} still has lag"
  kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --group "${group}" --describe || true
  return 1
}

delete_consumer_group() {
  local group="$1"
  kill_stale_workers
  for _ in $(seq 1 15); do
    out=$(kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --group "${group}" --delete 2>&1) || true
    if echo "${out}" | grep -qE 'GroupIdNotFound|successful|Deletion of'; then
      return 0
    fi
    if echo "${out}" | grep -q 'GroupNotEmpty'; then
      kill_stale_workers
      sleep 2
      continue
    fi
    return 0
  done
  echo "WARNING: could not delete consumer group ${group}"
}

start_worker() {
  local group="$1"
  stop_worker
  export Kafka__BootstrapServers="${Kafka__BootstrapServers:-localhost:9094}"
  export Valkey__ConnectionString="${Valkey__ConnectionString:-localhost:6379}"
  export Kafka__GroupId="${group}"
  echo "starting worker group=${group}..."
  dotnet run --project src/Worker/Worker.csproj --no-build &
  WORKER_PID=$!
  sleep 5
}

stop_worker() {
  if [ -n "${WORKER_PID}" ]; then
    kill "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
    WORKER_PID=""
  fi
}

run_scenario() {
  local scenario="$1"
  export POSTGRES__CONNECTIONSTRING="${POSTGRES__CONNECTIONSTRING:-Host=localhost;Port=5432;Database=nstech;Username=nstech;Password=nstech}"
  export SCENARIO_PATH="${scenario}"
  echo "resetting source tables..."
  kubectl exec -i deploy/postgres -- psql -U nstech -d nstech -c \
    "TRUNCATE positions RESTART IDENTITY; TRUNCATE vehicles;"
  mapfile -t steps < <(jq -c '
    [.vehicles[] | . + {stream:"vehicles"}] + [.positions[] | . + {stream:"positions"}]
    | sort_by(.seq) | .[]' "${scenario}")
  echo "running ${scenario} (${#steps[@]} steps, interleaved by seq)..."
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
}

assert_vehicle() {
  local id="$1"
  local failed=0
  for field in state deleted lat lng recordedAt; do
    exp=$(jq -r ".expected[\"${id}\"].${field} // empty" "${SCENARIO}")
    if [ -z "${exp}" ]; then
      continue
    fi
    got=$(valkey_field "${id}" "${field}")
    if [ "${got}" != "${exp}" ]; then
      echo "ASSERT FAIL vehicle:${id} field=${field} expected='${exp}' got='${got}'"
      failed=1
    else
      echo "ASSERT OK   vehicle:${id} field=${field} = ${got}"
    fi
  done
  return "${failed}"
}

assert_all_expected() {
  local failed=0
  while read -r id; do
    assert_vehicle "${id}" || failed=1
  done < <(jq -r '.expected | keys[]' "${SCENARIO}")
  if [ "${failed}" -ne 0 ]; then
    echo "ASSERT FAILED"
    return 1
  fi
  echo "ASSERT PASSED"
}

echo "==> [1/8] stack up"
just up

echo "==> [2/8] build solution"
dotnet build CDC-Driven-Vehicle-Cache.sln -v q

echo "==> [3/8] port-forwards (kafka EXTERNAL, valkey, postgres)"
kill_stale_workers
ensure_port_forward 9094 kafka
ensure_port_forward 6379 valkey
ensure_port_forward 5432 postgres
sleep 3

echo "==> [3b/8] purge CDC topics (deterministic log — no stale messages)"
for t in nstech.public.vehicles nstech.public.positions; do
  kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --delete --topic "${t}" 2>/dev/null || true
done
just topics-ensure
just connector-reset

echo "==> [4/8] reset valkey + consumer groups"
kubectl exec deploy/valkey -- valkey-cli FLUSHALL
delete_consumer_group nstech-worker
delete_consumer_group nstech-worker-replay

echo "==> [5/8] live run: worker + scenario + assert"
start_worker nstech-worker
run_scenario "${SCENARIO}"
wait_lag_zero nstech-worker 180
assert_all_expected

echo "==> [6/8] replay rebuild (B8): flush valkey, fresh consumer group, re-read log"
stop_worker
kubectl exec deploy/valkey -- valkey-cli FLUSHALL
start_worker nstech-worker-replay
wait_lag_zero nstech-worker-replay 180

echo "==> [7/8] assert replay state"
assert_all_expected

echo "==> [8/8] E2E PASSED"
