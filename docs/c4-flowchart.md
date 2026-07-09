# C4 Model (flowchart rendering) — CDC-Driven Vehicle Cache

This is a **readability-oriented alternative** to `docs/c4.md`. It describes the
exact same three C4 levels, but authored as Mermaid `flowchart` instead of the
native C4 shapes — flowchart gives GitHub more room to space nodes and place
edge labels without overlap. The canonical C4 (with boundaries and the C4
notation) lives in `docs/c4.md`; if the two ever disagree, `docs/c4.md` wins.

Data-store shapes (cylinders) are Postgres/Valkey; the double-bordered shapes
are Kafka topics. Dotted arrows are control/feedback (offset commits).

- Level 1 — System Context
- Level 2 — Containers
- Level 3 — Components (worker)

---

## Level 1 — System Context

```mermaid
%%{init: {'flowchart': {'nodeSpacing': 70, 'rankSpacing': 90}}}%%
flowchart LR
    reader["Fleet Operator / Reader"]
    mockWriters["Mock Writers (built for the exercise)"]
    postgres[("PostgreSQL: vehicles + positions")]
    cdc["Debezium / Kafka (CDC)"]
    worker[".NET 10 CDC Worker (built for the exercise)"]
    valkey[("Valkey: state + position per vehicle")]

    mockWriters -->|"writes rows (Npgsql)"| postgres
    postgres -->|"logical replication"| cdc
    cdc -->|"CDC events (Kafka)"| worker
    worker -->|"conditional writes (Lua)"| valkey
    reader -->|"reads (HGETALL)"| valkey
```

---

## Level 2 — Containers

```mermaid
%%{init: {'flowchart': {'nodeSpacing': 60, 'rankSpacing': 85}}}%%
flowchart LR
    operator["Fleet Operator / Reader"]

    subgraph mocks["Mock Writers (built for the exercise)"]
        vehicleWriter[".NET Vehicle Writer<br/>insert/update/delete, deterministic, out-of-order"]
        positionWriter[".NET Position Writer<br/>inserts at a set rate, out-of-order + duplicates"]
    end

    postgres[("PostgreSQL<br/>vehicles, positions (logical replication)")]

    subgraph cdc["CDC Pipeline"]
        connect["Kafka Connect + Debezium<br/>snapshot + streaming"]
        kafka[["Kafka topics<br/>vehicles, positions"]]
    end

    worker[".NET 10 Worker<br/>consumes both topics, merges per vehicle, conditional writes"]
    valkey[("Valkey<br/>hash per vehicle: vehicle:{id}")]

    vehicleWriter -->|"INSERT/UPDATE/DELETE"| postgres
    positionWriter -->|"INSERT positions"| postgres
    postgres -->|"logical replication (pgoutput)"| connect
    connect -->|"publishes change events"| kafka
    kafka -->|"consumes; commits after write"| worker
    worker -->|"conditional HSET per field group (Lua)"| valkey
    operator -->|"HGETALL vehicle:{id}"| valkey
```

---

## Level 3 — Components (worker)

```mermaid
%%{init: {'flowchart': {'nodeSpacing': 55, 'rankSpacing': 80}}}%%
flowchart TB
    kafkaVehicles[["Kafka: vehicles topic<br/>op r/c/u/d"]]
    kafkaPositions[["Kafka: positions topic<br/>op r/c"]]
    valkey[("Valkey<br/>hash per vehicle")]

    subgraph worker[".NET 10 Worker"]
        vehicleConsumer["Vehicle Topic Consumer<br/>Confluent.Kafka, manual commit"]
        positionConsumer["Position Topic Consumer<br/>Confluent.Kafka, manual commit"]
        projector["Per-Vehicle Projector<br/>deserialize -> partial field-group update"]
        orderingGuard["Ordering + Idempotency Guard<br/>resolve ordering key; coalesce null LSN to 0"]
        cacheWriter["Cache Writer<br/>atomic compare-and-set HSET via Lua<br/>(enforces ordering + idempotency)"]
    end

    kafkaVehicles -->|"CDC envelope"| vehicleConsumer
    kafkaPositions -->|"CDC envelope"| positionConsumer
    vehicleConsumer -->|"raw envelope"| projector
    positionConsumer -->|"raw envelope"| projector
    projector -->|"field-group update + ordering value"| orderingGuard
    orderingGuard -->|"write request (values + watermark)"| cacheWriter
    cacheWriter -->|"EVAL: conditional HSET"| valkey
    cacheWriter -.->|"write confirmed -> commit offset"| vehicleConsumer
    cacheWriter -.->|"write confirmed -> commit offset"| positionConsumer
```

Notes (same as `docs/c4.md`):

- The per-vehicle **merge is emergent in the Valkey hash** via independent
  per-field-group partial writes (ADR-005); the worker performs no in-memory
  join and holds no cross-stream buffer.
- **Ordering + idempotency are enforced atomically inside the Lua script**
  (Cache Writer), not by a read-then-decide step in the worker.
- Offsets are committed **after** the write is confirmed (ADR-002), so a crash
  mid-stream redelivers and safely re-applies as a no-op.
