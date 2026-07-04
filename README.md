# CDC-Driven Vehicle Cache (.NET 10 Worker)

A read-model / materialized-view built purely from Change Data Capture: Debezium
captures `vehicles` and `positions` from PostgreSQL, streams them through Kafka,
and a .NET 10 worker projects them into a Valkey cache that always reflects, per
vehicle, its latest position and current state. Readers hit Valkey, never the DB.

> Status: environment bootstrap. Worker, mock writers, Debezium/Kafka/Valkey
> manifests, tests and the four design docs (C4, Spec, test-behaviors, ADR) are
> built during the implementation phase.

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
just e2e   # run end-to-end: mocks -> Debezium -> Kafka -> worker -> Valkey,
           # assert the view is correct, then replay from offset 0 and assert
           # the rebuilt cache is identical
```

Other helpers: `just` (list recipes), `just status`, `just down`.

## Layout

```
devenv.nix / devenv.yaml   reproducible toolchain
.envrc                     direnv -> devenv autoload
justfile                   task recipes (up / e2e / down / status)
```
