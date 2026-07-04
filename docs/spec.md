# Spec + Implementation Plan — CDC-Driven Vehicle Cache

This document is the concrete bridge between the **ADR** (the *why*) and the
**code** (the *how*). Where the ADR reasons through a decision, this Spec pins
down the exact schemas, event shapes, configuration, and algorithms the
implementation and `just e2e` rely on. The C4 diagrams (`docs/c4.md`) name the
same containers and components referenced here.

> Anything marked **[verify-on-build]** is a concrete assumption about a third
> party's serialization (mostly Debezium's) that is cheap to confirm against a
> real captured event once the CDC stack is up, and is designed so the worker
> stays correct regardless of the exact wire form.

---

## 1. Scope

- **Read-model / materialized view** over two independently-partitioned CDC
  streams (`vehicles`, `positions`), joined **per vehicle** into a single
  Valkey hash. The cache answers "where is vehicle *X* right now, and what
  state is it in?" in one read.
- The worker is a **pure projection of the CDC log** — no direct DB access
  (case §7). Correctness oracle: replay from offset 0 rebuilds the identical
  cache.
- Out of scope (called out in the ADR): unbounded key growth for soft-deleted
  vehicles, multi-worker horizontal scaling (the design is safe for it via
  server-side atomic writes, but it is not a delivered feature).

---

## 2. Locked decisions (the ADR→code bridge)

These refine, but do not contradict, the ADR. They are the settings a reviewer
would otherwise have to reverse-engineer from the code.

| # | Decision | Value | Anchored in |
|---|----------|-------|-------------|
| 1 | Debezium converter | JSON, `schemas.enable=false` | ADR-006 |
| 2 | Delete representation | `tombstones.on.delete=true`; delete applied from `op=d` (ordered by LSN), the `null` tombstone message handled as an explicit no-op | ADR-004 |
| 3 | `recordedAt` | Postgres `timestamptz`; **normalized to epoch milliseconds (int64) by the worker** before any comparison/storage | ADR-001 |
| 4 | Source schema | see §3; `REPLICA IDENTITY DEFAULT`; **no FK** `positions → vehicles` (to allow the orphan-position race, ADR-005) | ADR-005 |
| 5 | Topics / connector | `topic.prefix=nstech` → `nstech.public.vehicles`, `nstech.public.positions`; `pgoutput`; `snapshot.mode=initial` | ADR-007 |
| 6 | Kafka consumer | group `nstech-worker`; `enable.auto.commit=false`, manual commit **after** the Valkey write, `auto.offset.reset=earliest`; replay uses a fresh group `nstech-worker-replay` | ADR-002 |
| 7 | Valkey encodings | see §6; watermarks stored as integer strings, timestamps as epoch-ms strings | ADR-003 |
| 8 | Mock writers | deterministic, scenario-file driven (§8), injecting out-of-order `recordedAt`, duplicate inserts, out-of-order state updates, and a delete | case §2.1 |

---

## 3. Source database schema

```sql
CREATE TABLE vehicles (
    id         bigint      PRIMARY KEY,
    state      text        NOT NULL,     -- REGISTERED|ACTIVE|IDLE|MAINTENANCE|DECOMMISSIONED
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE positions (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vehicle_id  bigint      NOT NULL,     -- intentionally NOT a FK (see ADR-005 orphan-position case)
    lat         double precision NOT NULL,
    lng         double precision NOT NULL,
    recorded_at timestamptz NOT NULL
);
```

Notes:
- **`state`** is stored as `text`, not a Postgres `enum`, so the mock writer can
  drive transitions freely and the worker never has to know the enum shape — the
  set of valid states is a business concern, not a cache-correctness concern.
- **`REPLICA IDENTITY DEFAULT`** is sufficient: the worker only needs the PK
  (available in the delete `before` image) plus `source.lsn`; it never reads
  `before` *values*. `FULL` would add WAL overhead for data we do not use.
- **Logical replication** must be enabled on the Postgres instance
  (`wal_level=logical`) — part of the container config, not the schema.

---

## 4. Debezium / Kafka Connect configuration

Registered against the Connect REST API as part of `just up` (idempotent —
re-registration is tolerated). Illustrative connector config:

```json
{
  "name": "nstech-postgres",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.dbname": "nstech",
    "database.user": "nstech",
    "database.password": "nstech",
    "topic.prefix": "nstech",
    "table.include.list": "public.vehicles,public.positions",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "decimal.handling.mode": "double",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

- `schemas.enable=false` → lean JSON envelopes (§5), trivial to deserialize in
  .NET without a schema registry.
- `tombstones.on.delete=true` → a delete produces **two** messages: the `op=d`
  change event (carries the LSN, this is the semantic delete) followed by a
  `null`-value tombstone on the same key. The worker handles both (§7.3).
- Temporal serialization of `timestamptz` is **[verify-on-build]**: Debezium
  emits `timestamptz` as an ISO-8601 string (`io.debezium.time.ZonedTimestamp`).
  The worker parses it to epoch-ms, so the exact form does not leak into the
  ordering logic (§7.4).

---

## 5. CDC event shapes the worker relies on

With `schemas.enable=false`, the **value** is the Debezium envelope and the
**key** is the primary key object. Only the fields listed below are consumed;
everything else is ignored (forward-compatible).

### 5.1 `vehicles` — insert / update (`op=c` / `op=u`) and snapshot (`op=r`)

```json
{
  "op": "c",
  "ts_ms": 1700000000123,
  "before": null,
  "after": { "id": 42, "state": "ACTIVE", "updated_at": "2026-07-04T18:00:00.000000Z" },
  "source": { "lsn": 246810, "ts_ms": 1700000000100, "snapshot": "false", "table": "vehicles" }
}
```
Key: `{ "id": 42 }`.

Consumed: `after.id` (vehicle id), `after.state`, `source.lsn` (`?? 0`),
`ts_ms` (→ `stateUpdatedAt`, descriptive only). Snapshot rows are identical with
`op="r"` and `source.snapshot="true"`; `source.lsn` **may be null** → coalesced
to `0` (ADR-007).

### 5.2 `vehicles` — delete (`op=d`) + tombstone

```json
{ "op": "d", "ts_ms": 1700000009000,
  "before": { "id": 42, "state": "ACTIVE", "updated_at": "..." },
  "after": null,
  "source": { "lsn": 999999, "snapshot": "false", "table": "vehicles" } }
```
Key: `{ "id": 42 }`. Consumed: `before.id`, `source.lsn`.
Immediately followed by the **tombstone**: key `{ "id": 42 }`, **value `null`**.

### 5.3 `positions` — insert (`op=c`) and snapshot (`op=r`)

```json
{
  "op": "c",
  "ts_ms": 1700000000500,
  "before": null,
  "after": { "id": 5001, "vehicle_id": 42, "lat": -23.55, "lng": -46.63,
             "recorded_at": "2026-07-04T18:00:05.000000Z" },
  "source": { "lsn": 246999, "snapshot": "false", "table": "positions" }
}
```
Key: `{ "id": 5001 }` (the **position** PK, not the vehicle id).
Consumed: `after.vehicle_id` (**this is the join key**), `after.lat`, `after.lng`,
`after.recorded_at` (→ epoch-ms), `source.lsn` (`?? 0` → `positionLsn`).
Positions are insert-only: no `op=u`, no `op=d`.

---

## 6. Valkey key/value schema

One hash per vehicle, key `vehicle:{id}` (from ADR-003). Field ownership and
encoding:

| Field            | Owner stream | Encoding | Meaning |
|------------------|--------------|----------|---------|
| `vehicleLsn`     | vehicles     | int64 string | watermark for `state`/`deleted` |
| `state`          | vehicles     | string   | current state |
| `stateUpdatedAt` | vehicles     | epoch-ms string | descriptive (Debezium `ts_ms`) |
| `deleted`        | vehicles     | `"0"`/`"1"` | soft-delete flag |
| `deletedLsn`     | vehicles     | int64 string | watermark for the delete (shares `vehicleLsn` domain) |
| `recordedAt`     | positions    | epoch-ms string | watermark for position fields |
| `lat`            | positions    | double string | latest latitude |
| `lng`            | positions    | double string | latest longitude |
| `positionLsn`    | positions    | int64 string | tie-breaker/observability for positions |

- The two streams write **disjoint field sets** and never clear each other's
  fields (ADR-005 partial-write). `HSET` creates the hash on first touch by
  either stream.
- A reader answers the product question with a single `HGETALL vehicle:{id}`,
  checking `deleted` before trusting `state`/`lat`/`lng` (ADR-004).

---

## 7. Worker components and processing

Component names match the C4 Component view (`docs/c4.md`): per-topic
**consumers**, **orderingGuard**, **merger/projector**, **cacheWriter**, offset
committer.

### 7.1 Message loop (per topic, manual commit)

1. Consume a batch from `nstech.public.vehicles` / `nstech.public.positions`.
2. For each message: deserialize → `orderingGuard` normalizes ordering inputs →
   `cacheWriter` runs the atomic conditional Lua script.
3. **Only after** the write returns, commit the offset (ADR-002). A crash
   before commit ⇒ redelivery ⇒ safe (no-op if already applied).

### 7.2 `orderingGuard` — normalization (pure, no I/O)

- `lsn = source.lsn ?? 0` (ADR-007 snapshot floor).
- `recordedAt` string → epoch-ms int64. **[verify-on-build]** parsing path;
  isolated here so the rest of the pipeline is form-agnostic.
- Resolves the **vehicle id**: vehicles → `after.id` (or `before.id` on delete);
  positions → `after.vehicle_id`.

### 7.3 `cacheWriter` — atomic conditional writes (Lua)

Two server-side scripts make read-compare-write atomic (ADR-006), so the design
is correct even with multiple worker instances.

**Vehicle script** — `KEYS[1]=vehicle:{id}`, `ARGV=[lsn, op, state, stateUpdatedAt]`
(`op ∈ {upsert, delete}`):

```lua
local cur = tonumber(redis.call('HGET', KEYS[1], 'vehicleLsn'))
local lsn = tonumber(ARGV[1])
if cur ~= nil and lsn <= cur then return 0 end        -- stale/duplicate: no-op
redis.call('HSET', KEYS[1], 'vehicleLsn', ARGV[1])
if ARGV[2] == 'delete' then
  redis.call('HSET', KEYS[1], 'deleted', '1', 'deletedLsn', ARGV[1])
else
  redis.call('HSET', KEYS[1], 'state', ARGV[3], 'stateUpdatedAt', ARGV[4], 'deleted', '0')
end
return 1
```

**Position script** — `KEYS[1]=vehicle:{id}`, `ARGV=[recordedAtMs, positionLsn, lat, lng]`:

```lua
local curTs  = tonumber(redis.call('HGET', KEYS[1], 'recordedAt'))
local curLsn = tonumber(redis.call('HGET', KEYS[1], 'positionLsn'))
local ts  = tonumber(ARGV[1])
local lsn = tonumber(ARGV[2])
local apply = (curTs == nil or ts > curTs)
           or (ts == curTs and (curLsn == nil or lsn > curLsn))
if not apply then return 0 end                         -- stale/duplicate: no-op
redis.call('HSET', KEYS[1], 'recordedAt', ARGV[1], 'positionLsn', ARGV[2],
                            'lat', ARGV[3], 'lng', ARGV[4])
return 1
```

`cur == nil` (field absent) is the coalesced `-∞`, covering both a brand-new hash
and a partial hash created by the *other* stream (ADR-005). Strict `>` at every
level is what makes exact redelivery a no-op (ADR-002) and a null-LSN snapshot
lose to any streaming event (ADR-007).

**Tombstone (null value):** not passed to either script — logged and its offset
committed as an explicit no-op. The semantic delete already happened via `op=d`.

### 7.4 Mapping table (event → script call)

| Event | id source | Script | Notable |
|-------|-----------|--------|---------|
| vehicle `c`/`u`/`r` | `after.id` | vehicle, `op=upsert` | `deleted` reset to `0` |
| vehicle `d` | `before.id` | vehicle, `op=delete` | keeps existing `state`, sets `deleted=1` |
| vehicle tombstone (null) | key `.id` | none | no-op + commit |
| position `c`/`r` | `after.vehicle_id` | position | insert-only |

---

## 8. Mock writers (deterministic)

Two console apps writing **only** to Postgres via `Npgsql` (ADR-006). Both read
a **checked-in scenario file** (`scenarios/e2e.json`) so a run is byte-for-byte
reproducible — no wall-clock, no RNG unless seeded.

The position writer also accepts a **configurable insert rate** (case §2.1) for
ad-hoc / load runs; the deterministic `just e2e` run drives the fixed scenario
sequence (not a timed rate) so the expected end state is exact and repeatable.
The vehicle writer drives state transitions (`REGISTERED → ACTIVE → IDLE →
MAINTENANCE → DECOMMISSIONED`), out-of-order updates, and a delete from the same
scenario file.

Scenario entries (illustrative):

```jsonc
{
  "vehicles": [
    { "seq": 1, "op": "insert", "id": 1, "state": "REGISTERED" },
    { "seq": 3, "op": "update", "id": 1, "state": "ACTIVE" },
    { "seq": 9, "op": "update", "id": 1, "state": "IDLE" },      // emitted with an EARLIER recordedAt intent -> stale, must lose
    { "seq": 12, "op": "delete", "id": 1 }
  ],
  "positions": [
    { "seq": 2, "vehicleId": 1, "lat": -23.50, "lng": -46.60, "recordedAt": "2026-07-04T18:00:02Z" },
    { "seq": 4, "vehicleId": 1, "lat": -23.51, "lng": -46.61, "recordedAt": "2026-07-04T18:00:10Z" },
    { "seq": 5, "vehicleId": 1, "lat": -23.99, "lng": -46.99, "recordedAt": "2026-07-04T18:00:05Z" }, // out-of-order: older recordedAt, must NOT overwrite seq 4
    { "seq": 6, "vehicleId": 1, "lat": -23.51, "lng": -46.61, "recordedAt": "2026-07-04T18:00:10Z", "duplicateOf": 4 },
    { "seq": 7, "vehicleId": 2, "lat": 0.0, "lng": 0.0, "recordedAt": "2026-07-04T18:00:07Z" }  // orphan: no vehicle row for id 2
  ]
}
```

Capabilities exercised (case §2.1): out-of-order `recordedAt` (pos seq 5),
duplicate insert (pos seq 6), out-of-order / stale state update (veh seq 9),
delete (veh seq 12), cross-stream orphan position (pos seq 7). The **expected
end state** for this scenario is computed by hand and encoded in the e2e
assertion (§9).

---

## 9. `just e2e` and the replay-rebuild oracle

`just e2e` runs from a clean checkout with zero manual steps (case §4):

1. **Up:** `just up` brings up the k3d cluster; `devspace` deploys Postgres
   (logical replication), Kafka + Connect/Debezium, Valkey, and the worker;
   the connector (§4) and schema (§3) are applied.
2. **Drive:** run the mock writers against `scenarios/e2e.json`.
3. **Converge:** wait until both topics' consumer lag for the worker group is 0.
4. **Assert:** compare `HGETALL vehicle:{id}` for every id against the scenario's
   expected end state (state, deleted flag, latest lat/lng/recordedAt).
5. **Replay-rebuild:** `FLUSHALL` Valkey, then re-consume **the same committed
   messages** from offset 0 using a **fresh consumer group** (re-reading the
   log, not re-snapshotting — ADR-007). Wait for lag 0.
6. **Re-assert:** the rebuilt cache must equal the §4 expectation exactly.

The single code path (conditional writes) serves both the live run and the
replay; there is no "replay mode" (ADR-002/007).

---

## 10. Implementation plan (sequencing)

| Phase | Deliverable | Exit check |
|-------|-------------|------------|
| 0 | Infra manifests + `devspace` + `just up` | cluster up, all pods ready |
| 1 | Schema (§3) + connector (§4) | real envelopes visible on both topics (`kafka-console-consumer`) — **validates [verify-on-build] premises** |
| 2 | Worker skeleton: `Confluent.Kafka` consumers + `StackExchange.Redis`, deserialize + log | messages consumed, offsets committed manually |
| 3 | `orderingGuard` + `cacheWriter` (§7) — ordering + idempotency + partial-write merge | out-of-order & duplicate handled in a unit test |
| 4 | Delete/tombstone + snapshot handling | delete flags, tombstone no-op, snapshot seeds then streaming overrides |
| 5 | Mock writers + `scenarios/e2e.json` (§8) | deterministic run reproducible |
| 6 | `just e2e` assertions + replay-rebuild (§9) | e2e green incl. replay |
| 7 | Clean-checkout dry-run, open PR, notification email | fresh clone: `just up` + `just e2e` pass |

Ordering follows the case's weighting: the design artifacts (this Spec, ADR, C4,
test-behaviors) and the hard-case correctness (phases 3–4, 6) are prioritized
over breadth, per case §4's "defensible partial build" guidance.

---

## 11. Open premises to confirm in Phase 1

- Exact `timestamptz` serialization (ISO string vs numeric) — isolated in
  `orderingGuard` (§7.2); worker normalizes either way.
- Snapshot `source.lsn`: null vs `consistent_point` — both handled by the `?? 0`
  coalesce (ADR-007).
- `positions` message **key** is the position PK (join key comes from
  `after.vehicle_id`) — confirm against a real key.
- Tombstone message emitted with a `null` **value** (not an empty object).
