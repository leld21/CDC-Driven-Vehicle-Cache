# CDC-Driven Vehicle Cache (.NET 10 Worker)

A read-model / materialized-view built purely from Change Data Capture: Debezium
captures `vehicles` and `positions` from PostgreSQL, streams them through Kafka,
and a .NET 10 worker projects them into a Valkey cache that always reflects, per
vehicle, its latest position and current state. Readers hit Valkey, never the DB.

> Status: design complete; local infra live. The four design docs (C4, Spec,
> test-behaviors, ADR) live in `docs/`, and `just up` deploys the full stack
> (Postgres + logical replication, Kafka in KRaft, Kafka Connect + Debezium,
> Valkey) to a local k3d cluster via devspace, registering the Debezium
> connector. The .NET 10 worker, the mock writers, and the `just e2e` automation
> are built next.

## Prerequisites

The whole stack is Linux-native and reproducible via Nix. On Windows it runs
inside WSL2.

- WSL2 with an Ubuntu distro
- Docker Desktop with WSL integration enabled for the distro
- [Nix](https://determinate.systems/nix) (Determinate installer)
- [devenv](https://devenv.sh) and [direnv](https://direnv.net)

With direnv hooked into your shell, `cd` into this repo auto-loads the dev
environment (declared in `devenv.nix`): .NET 10 SDK, `just`, `k3d`, `kubectl`,
`helm`, `devspace`, `jq`. First entry: run `direnv allow`.

Without direnv you can drop into the same environment with `devenv shell`.

## The two commands that matter

```bash
just up    # bring up the local Kubernetes cluster (k3d) and the stack
just e2e   # full pipeline: mocks -> Debezium -> Kafka -> worker -> Valkey
           # assert against scenarios/e2e.json, then replay-rebuild (B8)
```

Other helpers: `just` (list recipes), `just status`, `just down`.

Worker (skeleton, local run against the cluster):

```bash
just worker-build    # compile
just worker-run      # port-forward kafka EXTERNAL (9094) + valkey, run locally
just worker-image    # build container + import into k3d
just worker-deploy   # deploy worker pod in the cluster (uses kafka:9092 in-cluster)
just valkey-get 1    # inspect vehicle:1 after the worker processed events
just connector-register  # re-register Debezium if connector 404 after a restart
```

Mock writers (C4: Vehicle Writer + Position Writer, Npgsql only):

```bash
just mocks-build
just mocks-run          # interleaved timeline from scenarios/e2e.json (B6 cross-stream)
just mocks-vehicle      # vehicle stream only
just mocks-position     # position stream only
# ad-hoc load: dotnet run --project src/PositionWriter -- --rate 10 1
```

Local `worker-run` uses Kafka's **EXTERNAL** listener (`localhost:9094`) because the
broker advertises `kafka:9092` for in-cluster clients — a plain port-forward on 9092
would still redirect the consumer to the unresolvable `kafka` hostname.

## Layout

```
devenv.nix / devenv.yaml   reproducible toolchain
.envrc                     direnv -> devenv autoload
justfile                   task recipes (up / e2e / worker-* / peek / psql)
devspace.yaml              deploys the deploy/ manifests to the cluster
deploy/                    k8s manifests: postgres, kafka, connect, valkey, connector, worker
src/Worker/                .NET 10 CDC worker (BackgroundService, Confluent.Kafka, StackExchange.Redis)
src/VehicleWriter/         mock writer: vehicle insert/update/delete (Npgsql)
src/PositionWriter/        mock writer: position inserts + --rate mode (Npgsql)
src/Scenario/              shared scenario models + Postgres executor
scenarios/e2e.json         deterministic e2e scenario + expected Valkey state
CDC-Driven-Vehicle-Cache.sln
docs/                      design artifacts:
  ADR.md                     architecture decision records
  spec.md                    spec + implementation plan
  test-behaviors.md          Given/When/Then acceptance behaviors
  c4.md                      C4 model (canonical, C4 notation)
  c4-flowchart.md            C4 model (flowchart rendering, readability)
  case.md                    original challenge brief
```
