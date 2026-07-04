# Architecture Decision Records — CDC-Driven Vehicle Cache

---

## ADR-001: Ordering Key and Conditional-Write Strategy

### Context

The worker consumes two independently-partitioned CDC topics (`vehicles` and
`positions`) produced by Debezium. Debezium is at-least-once and can re-emit
events on restart, snapshot re-run, or full replay from offset 0. The mock
writers additionally inject out-of-order timestamps and duplicate inserts on
purpose. The worker must therefore be able to decide, for any incoming
event, whether it represents state that is genuinely **newer** than what is
already stored in the Valkey view for that vehicle.

The hard requirement driving this decision: **a stale or late-arriving
event — an old position or an old state — must never overwrite a newer value
already sitting in the cache.** A naive `SET` on every consumed message
satisfies the happy path but silently corrupts the cache the moment an event
arrives late, which is exactly the failure mode this exercise is built to
expose.

Three candidate orderings were evaluated:

1. **Debezium `ts_ms`** — processing-time timestamp assigned by Debezium.
2. **Postgres LSN** (`source.lsn`) — the physical, monotonic position of the
   change in the PostgreSQL write-ahead log.
3. **Business timestamp** — `recordedAt` on positions, an update timestamp
   on vehicles — supplied by the source row itself.

### Decision

Use a **mixed ordering strategy per stream**, not a single universal key:

- **`vehicles` stream (state + delete): ordered by LSN.** Every write to the
  state or delete fields is conditional on the incoming event's
  `source.lsn` being strictly greater than the `vehicleLsn` currently stored
  for that vehicle. `ts_ms` is persisted as `stateUpdatedAt`, but purely as
  descriptive metadata — never as the value the write condition branches on.

- **`positions` stream: ordered by `recordedAt`, with `positionLsn` as
  tie-breaker.** A position event is applied when its `recordedAt` is
  **strictly greater** than the `recordedAt` currently stored, **or** when
  the two `recordedAt` values are **equal and** its `positionLsn` is
  **strictly greater** than the stored `positionLsn`. `positionLsn` (the
  event's LSN) is therefore both the tie-breaker on an exact `recordedAt`
  collision and an observability/audit field. Using strict inequalities at
  both levels keeps an exact redelivery (same `recordedAt`, same
  `positionLsn`) a no-op, which is what makes position writes idempotent
  under replay (ADR-002).

`ts_ms` was rejected as a primary ordering key for either stream: it is a
processing-time timestamp, not a log position, and under a high insert rate
(as the position writer produces) multiple source events can be batched and
stamped with the same `ts_ms`, making it unable to reliably distinguish
which of two events is authoritative.

### Reasoning

For the **vehicle** stream, LSN is preferred over any business timestamp
because vehicle state transitions are exactly the kind of field the exercise
asks us to treat as adversarial input — the mock writer injects
out-of-order updates on purpose. LSN is unique and strictly monotonic by
construction (it is a WAL position), so it gives an incorruptible notion of
"what really happened first in the source," independent of what timestamp a
writer claims.

For the **position** stream, a deliberate trade-off was made in the other
direction: `recordedAt` is the GPS-captured, real-world moment the vehicle
was at that location, and it is what actually answers "where is the vehicle
*right now*" — the question this whole cache exists to answer. Ordering
positions by log-arrival order (LSN) instead of capture time would mean a
position recorded earlier but delivered later (e.g. due to network
buffering on the reporting device before the row is inserted) could be
wrongly treated as authoritative over a position recorded later but
inserted first. `recordedAt` is closer to ground truth for this specific
question, so it is used as the primary discriminator, with LSN demoted to a
tie-breaker for the (expected) case of an exact `recordedAt` collision, and
retained anyway for observability/debugging.

LSN comparisons are always scoped **within a single stream and field
group**: `vehicleLsn` is only ever compared against incoming `vehicles`
events, and `positionLsn` only against incoming `positions` events. The two
LSNs are never compared against each other — `vehicles` and `positions`
advance independently in the WAL and a cross-stream LSN comparison has no
meaningful interpretation. How the two streams are merged into one
per-vehicle record is handled in ADR-005.

### Alternatives Considered

- **`ts_ms` as the universal ordering key** — rejected for both streams:
  not guaranteed unique/monotonic when Debezium batches events.
- **LSN as the universal ordering key (including positions)** — considered
  and rejected: correct for correctness-under-replay, but degrades the
  real-world meaning of "current position," which is the actual product
  requirement.
- **`recordedAt` as the universal ordering key (including vehicle state)**
  — rejected: state transitions are the field the exercise explicitly
  attacks with out-of-order writes, so trusting a writer-supplied timestamp
  there would let a malformed/adversarial writer directly corrupt the
  cache's notion of current state.
- **Kafka partition offset** — rejected as a primary key for either stream:
  only monotonic within a single partition, and partitioning strategy is
  determined by Debezium/Connect configuration, not guaranteed to reflect a
  single global order.

### Consequences

- Two comparison strategies must be implemented and tested independently
  (LSN-based for vehicles, timestamp-based-with-LSN-tiebreak for positions),
  which is slightly more code than a single universal rule, but each rule
  is a better fit for what it protects.
- Correctness for the vehicle stream depends on Debezium reliably surfacing
  `source.lsn`, which is standard for the PostgreSQL connector.
- Correctness of "current position" is only as good as the accuracy of the
  writer-supplied `recordedAt`. This is an accepted trade-off: it reflects
  the real product requirement (GPS truth) rather than the technically
  safest option (log order).

---

## ADR-002: Idempotency

### Context

Debezium's at-least-once delivery guarantees mean the same CDC event can be
delivered to the worker more than once (consumer restart before offset
commit, Kafka Connect rebalance, or a deliberate full replay from offset 0
used as the correctness oracle for this exercise). Re-consuming an event
must never change the cache's end state. Separately, the mock writers can
also insert genuine **duplicate rows** at the source (duplicate inserts, not
just duplicate CDC delivery of the same row), which must not be double-
counted or produce inconsistent state either.

### Decision

Idempotency is achieved as a **direct consequence of the conditional-write
rule in ADR-001**, not as a separate mechanism:

- Every cache write compares the incoming event's ordering value (LSN for
  vehicle events, `recordedAt`+`positionLsn` for position events) against
  the value already stored, using **strictly greater than**.
- Redelivery of an event whose ordering value is **equal to** what is
  already stored (the exact-redelivery case) fails the "strictly greater"
  check and is a no-op. This covers Debezium re-emission and full replay:
  replaying the same LSN a second time can never re-apply it.
- Source-level **duplicate inserts** injected by the mock writers surface
  as two distinct CDC events with two distinct LSNs (each insert is its own
  WAL record). When they represent duplicate *position* rows for the same
  `recordedAt`, they resolve through the tie-breaker path from ADR-001: the
  two events share the same `recordedAt`, so the one with the **greater**
  `positionLsn` wins. Because a duplicate insert carries identical
  `lat`/`lng`, the winning write reapplies the **same values** (only
  advancing `positionLsn`), leaving the observable position state unchanged.
  The duplicate is therefore **idempotent in effect** — a redundant write
  of identical data, not a second, meaningful update — rather than being
  dropped as a strict no-op.
- The write itself is executed as a single atomic operation server-side
  (see ADR-006 for the Lua-script mechanism) so that the
  read-compare-write is not split across a race window if multiple worker
  instances are ever run in parallel.

Offset commits to Kafka are only made **after** the corresponding Valkey
write has been confirmed, never before. This closes the gap the "worker
restart mid-stream" test behavior targets: if the worker crashes between
consuming a message and writing to Valkey, the message has not been
committed and will be redelivered on restart — which is safe, because
re-applying it is a no-op if it was somehow already written, and a normal
first-time apply if it wasn't.

### Reasoning

Building idempotency as a byproduct of ADR-001's ordering rule, rather than
as a separate deduplication table or dedup-by-event-id mechanism, keeps the
system's correctness resting on a single, auditable invariant ("a field
group only advances forward") instead of two independent mechanisms that
could drift out of sync. It also means the full-replay correctness oracle
required by the exercise (`replay from offset 0 rebuilds the identical
cache`) is guaranteed by the same code path as ordinary out-of-order
handling — there is no special "replay mode."

### Alternatives Considered

- **Explicit dedup table/set keyed by event id or Kafka offset** — rejected:
  adds a second piece of state to keep consistent with the cache itself,
  and does not, on its own, solve the "duplicate insert at the source"
  case (which produces two different, legitimate LSNs).
- **Commit Kafka offsets immediately on consume, before writing Valkey** —
  rejected: reintroduces exactly the "lost update on crash" failure mode
  the restart test behavior is designed to catch.

### Consequences

- No separate dedup store to maintain; the conditional write is the only
  idempotency mechanism, which simplifies reasoning and testing.
- The worker must commit offsets manually (not on Kafka's default
  auto-commit interval), which requires slightly more explicit consumer
  configuration but is necessary for the ordering guarantee above.

---

## ADR-003: Valkey View Schema

### Context

The cache must answer, for any vehicle, "where is it right now, and what
state is it in?" in a **single read**, and must carry enough metadata per
field group to support the conditional-write rules from ADR-001 and the
soft-delete rule from ADR-004.

### Decision

One Redis/Valkey **hash** per vehicle, keyed `vehicle:{id}`, with the
following fields:

| Field           | Written by         | Purpose                                                             |
|-----------------|---------------------|----------------------------------------------------------------------|
| `vehicleLsn`    | `vehicles` events   | Ordering watermark for `state` and `deleted` (ADR-001, ADR-004)     |
| `state`         | `vehicles` events   | Current vehicle state                                                |
| `stateUpdatedAt`| `vehicles` events   | Debezium `ts_ms`, descriptive only (ADR-001)                        |
| `deleted`       | `vehicles` events   | Soft-delete flag (ADR-004)                                          |
| `deletedLsn`    | `vehicles` events   | Watermark for the delete flag itself, shares `vehicleLsn`'s ordering domain (ADR-004) |
| `recordedAt`    | `positions` events  | Ordering key for position fields (ADR-001)                          |
| `lat`           | `positions` events  | Latest known latitude                                                |
| `lng`           | `positions` events  | Latest known longitude                                               |
| `positionLsn`   | `positions` events  | Tie-breaker/observability for position ordering (ADR-001)           |

A single `HGETALL vehicle:{id}` answers the "where is it, what state" query
in one round trip.

### Reasoning

A hash was chosen over a serialized JSON blob in a plain string key because
the two source streams update **disjoint subsets of fields** independently
and concurrently. A hash lets the worker update only the fields relevant to
the event it just consumed (`HSET` on the position fields, or on the state
fields) without a read-modify-write cycle over the whole record, which would
otherwise be required to avoid a position update clobbering unrelated state
fields (or vice versa) under a naive whole-object write. Per-field
granularity is also what makes ADR-001's per-stream ordering rules directly
implementable: each field group carries its own watermark.

`vehicleLsn` doubles as the watermark for `deleted`/`deletedLsn` rather than
introducing a third independent watermark, because deletion is itself a
row-level change on the `vehicles` table and therefore lives in the same
ordering domain as state changes (see ADR-004).

### Alternatives Considered

- **Single JSON string per vehicle** — rejected: forces a read-modify-write
  for every update regardless of which stream produced it, and loses
  per-field atomicity guarantees needed for the conditional-write approach.
- **Two separate keys** (`vehicle:{id}:state`, `vehicle:{id}:position`) —
  rejected: works, but requires two round trips to answer the "one read"
  requirement, or a pipelined-but-still-two-command read; a single hash
  gets the same separation of concerns with one command.

### Consequences

- The hash carries more fields than the minimal illustrative schema in the
  challenge brief; this is a deliberate trade-off in favor of auditability
  (every field group's provenance is inspectable) over minimalism.
- Consumers of the cache that only care about `state`/`lat`/`lng` can ignore
  the watermark fields, so the extra metadata does not complicate the
  common read path.

---

## ADR-004: Delete / Tombstone Handling

### Context

A `DELETE` on the `vehicles` table is captured by Debezium as a change
event with `payload.after = null` (and, depending on connector
configuration, a Kafka tombstone record). The cache must reflect that the
vehicle is gone, while still being correct under out-of-order delivery: a
position or state event for that vehicle that was in flight before the
delete, but is delivered after it, must not resurrect a vehicle that no
longer exists in the source.

### Decision

Deletes are represented as a **soft delete**, not a physical removal of the
Valkey key:

- On receiving a delete/tombstone event for a vehicle, the worker sets
  `deleted = true` and `deletedLsn = <event LSN>` on the existing
  `vehicle:{id}` hash (creating the hash first, with only the delete fields
  populated, if no prior event for that vehicle had been seen — see
  ADR-005 for the general partial-write pattern).
- The delete is itself subject to the same LSN-based conditional-write rule
  as any other `vehicles`-stream update: it only applies if the event's LSN
  is greater than the hash's current `vehicleLsn`, and once applied, it
  advances `vehicleLsn` like any other vehicle event would.
- A subsequent legitimate re-creation of the same vehicle id (a new INSERT
  with a higher LSN) is allowed to clear `deleted` and continue updating the
  hash normally, since it carries a higher LSN than the delete that
  preceded it — this is treated as a valid business event (vehicle re-added
  to the fleet), not as a stale write.

### Reasoning

Physically deleting the Valkey key was considered but rejected because of a
specific race it does not handle correctly: if a `positions` event for that
vehicle was delayed in its own partition and arrives **after** the delete
has already removed the key, a plain `DEL` followed by a fresh `HSET` from
that late position event would silently recreate the vehicle in the cache
as if it still existed — using stale data, for a vehicle that has actually
been deleted. Keeping the key alive with a `deleted` flag and a watermark
(`deletedLsn`, living in the same ordering domain as `vehicleLsn`) lets the
worker distinguish "this vehicle was never seen" from "this vehicle existed
and was deleted," and lets any late-arriving field update still be recorded
without being visible as a live vehicle, since readers are expected to
check the `deleted` flag before trusting `state`/`lat`/`lng`.

### Alternatives Considered

- **Physical `DEL` of the key on delete** — rejected: reintroduces exactly
  the late-arriving-update resurrection race described above, and produces
  a cache that is not deterministically rebuildable under replay if events
  are replayed in a different relative order than their original delivery
  (the exercise's replay-rebuild oracle would not reliably reproduce the
  same end state).
- **Publish a Valkey key expiration (`EXPIRE`) instead of a flag** —
  rejected as the *primary* mechanism: still exposed to the same
  resurrection race while the key is briefly absent, though it remains a
  reasonable complementary mechanism for eventually reclaiming space (see
  Consequences).

### Consequences

- Deleted vehicles' keys are never physically reclaimed by this mechanism
  alone, so the cache will grow unboundedly over the lifetime of the fleet.
  This is called out explicitly as out of scope for the exercise; a
  production system would pair the soft-delete flag with a long TTL applied
  at the time `deleted` is set, or a periodic sweep job.
- Readers of the cache must check `deleted` before treating a hash's
  presence as "this vehicle exists," which is a small additional contract
  compared to "key exists implies vehicle exists."

---

## ADR-005: Cross-Stream Race — Position Before Vehicle

### Context

`vehicles` and `positions` are independent topics with independent
partitioning and independent consumer lag. It is entirely normal for a
`positions` event for a given vehicle id to arrive before any `vehicles`
event for that same id has been consumed — the worker has "seen" a location
report for a vehicle it does not yet know exists. The design must decide
what the cache shows in that window, and must remain correct once the
vehicle event eventually does arrive, including in combinations with the
delete case from ADR-004.

### Decision

Adopt a **partial-write, per-field-group** approach: each stream is allowed
to independently create or update only the fields it owns on the
`vehicle:{id}` hash, using `HSET` (which creates the hash if it does not yet
exist, and adds/updates only the specified fields if it does). Concretely:

- A `positions` event writes `lat`, `lng`, `recordedAt`, `positionLsn` —
  regardless of whether `state`/`vehicleLsn` are already present.
- A `vehicles` event writes `state`, `vehicleLsn`, `stateUpdatedAt` (and
  `deleted`/`deletedLsn` for a delete) — regardless of whether
  `lat`/`lng`/`recordedAt` are already present.
- Neither stream ever resets or clears the other stream's fields. A vehicle
  event arriving after a position-only record fills in the missing state
  fields without touching the existing position fields, and vice versa.

This single rule is sufficient to cover every ordering of the two streams:

- **Position, then state**: position creates a partial hash; state fills it
  in. Handled directly by the rule above.
- **State, then position**: state creates a partial hash; position fills it
  in. Same rule, other direction.
- **Delete arrives while only a position record exists** (vehicle never
  otherwise seen): the delete event still only touches `deleted` and
  `deletedLsn`/`vehicleLsn`, using the same partial-write/`HSET` behavior —
  the hash ends up with position data plus a `deleted = true` flag, which is
  the correct representation of "we only ever saw where it was, and now the
  source says it's gone."
- **State, then position, then a later state update**: the second state
  event only compares against and advances `vehicleLsn`/`state` fields; it
  does not touch or reset the position fields written in between. Likewise
  a position update in between two state events never resets state fields.
  Each field group's freshness is judged solely against its own watermark,
  never against the other group's.
- **Position that never gets a matching vehicle event** (the vehicle id
  never appears on the `vehicles` topic, e.g. bad test data or a
  never-registered id): the cache deliberately keeps this as a permanent
  partial record (state fields simply absent) rather than treating it as an
  error or expiring it — this is accepted as correct: the cache honestly
  reflects that a position was observed for an id whose state is unknown.

### Reasoning

Buffering position events until a matching vehicle event arrives was
considered but rejected: it requires the worker to hold additional,
unbounded state (an out-of-order buffer keyed by vehicle id, with no
principled answer to "how long do we wait"), and turns a stateless,
replayable projection into a stateful one whose behavior depends on timing
and buffer size — undermining the replay-determinism goal that is the
correctness oracle for this whole exercise. Discarding orphan position
events until the vehicle is known was also rejected, for the same
determinism reason in the opposite direction: whether a given position
survives would depend on the arrival order relative to the vehicle event
across different runs, and a full replay from offset 0 is not guaranteed to
reproduce that same relative arrival order, so it would not reliably
rebuild an identical cache.

The chosen partial-write approach requires no additional buffering state,
is trivially deterministic under any replay ordering (each field group's
end state depends only on which events for that group were seen and their
relative LSNs/`recordedAt`, never on interleaving with the other group), and
gives a semantically honest answer to the cache's actual question
("where is it, what state is it in") even in the partial-information case:
"we know where it is, we don't yet know its state" is a valid, useful
answer, not an error condition.

### Alternatives Considered

- **Buffer position events until the vehicle event arrives** — rejected:
  unbounded/unclear buffering window, adds stateful complexity, breaks
  deterministic replay.
- **Discard position events for unknown vehicles** — rejected: outcome
  becomes dependent on arrival-order timing, which is not guaranteed
  reproducible across replays, violating the replay-rebuild requirement.
- **Reject/dead-letter position events for unknown vehicles for manual
  reconciliation** — rejected as the default behavior: reintroduces a
  database-lookup-shaped dependency in spirit (needing to know "is this
  vehicle real") that conflicts with the CDC-only constraint, and adds
  operational overhead disproportionate to what the exercise is testing.

### Consequences

- The cache can contain permanently partial records (position known, state
  unknown) for vehicle ids that never receive a `vehicles` event. This is
  an accepted, intentional outcome rather than a bug.
- Readers must be prepared for either field group to be absent on a given
  hash, in addition to checking `deleted` (ADR-004).
- No additional in-memory or external buffering component is needed in the
  worker, keeping it a simple, replayable, stateless-between-events
  projector.

---

## ADR-006: Library and Technology Choices

### Context

The Kafka and Valkey client libraries for the .NET 10 worker are an open
choice per the exercise, to be justified here. The worker also needs to
interact indirectly with PostgreSQL/Debezium concerns (schema/config for the
mock writers and the Debezium connector setup), which involves additional
library choices even though the worker itself never queries Postgres
directly.

### Decision

- **Kafka client: `Confluent.Kafka`.** Official client maintained by
  Confluent, a wrapper over `librdkafka`. De facto standard for .NET Kafka
  consumers, with first-class support for manual offset commit (required by
  ADR-002) and consumer group management.
- **Valkey client: `StackExchange.Redis`.** Valkey is wire-protocol
  compatible with Redis, and `StackExchange.Redis` is the most mature .NET
  client for that protocol, with support for `ScriptEvaluateAsync`, used to
  run the Lua scripts that make the conditional `HSET` writes in ADR-001/
  ADR-002/ADR-004/ADR-005 atomic on the server side (avoiding a
  read-then-write race if more than one worker instance is ever run
  concurrently).
- **PostgreSQL client (mock writers only): `Npgsql`.** Standard, actively
  maintained ADO.NET/EF Core provider for PostgreSQL; used exclusively by
  the mock writer projects, which are the only components that talk to
  Postgres directly.
- **Debezium / Kafka Connect: no .NET library involved.** Debezium runs as
  a Kafka Connect plugin (JVM-based), configured via its HTTP Connect REST
  API (a JSON connector configuration posted to Kafka Connect), not
  consumed as a .NET dependency. The worker only ever sees Debezium's
  output as ordinary Kafka messages via `Confluent.Kafka`.

### Reasoning

All three .NET-side choices favor the most widely adopted, actively
maintained client for their respective protocol over more niche
alternatives, minimizing the risk of obscure bugs or missing features while
learning an unfamiliar stack under a time-boxed exercise. `Npgsql` in
particular is the same ecosystem-standard choice regardless of CDC being
involved at all, since the mock writers are ordinary PostgreSQL client
applications.

### Alternatives Considered

- **`kafka-sharp` or other minor Kafka clients** — rejected: far smaller
  community/support surface than `Confluent.Kafka`.
- **A raw `RESP` client hand-rolled over sockets for Valkey** — rejected:
  reinvents functionality `StackExchange.Redis` already provides reliably,
  including Lua scripting support needed for atomic conditional writes.
- **Dapper/raw ADO.NET vs. EF Core for the mock writers** — left as an
  implementation detail rather than an architectural decision; either sits
  on top of `Npgsql` and does not affect any of the CDC/cache correctness
  decisions above.

### Consequences

- The worker has zero direct dependency on Postgres or Debezium client
  libraries, which is a natural consequence of — and reinforces — the
  "CDC only, no direct DB access" constraint at the heart of this exercise.
- Reliance on `StackExchange.Redis`'s Lua scripting ties the atomicity
  guarantees in this design to Valkey/Redis's server-side scripting
  support, which is standard and stable, but worth naming explicitly as a
  dependency of the correctness story, not just a performance detail.

---

## ADR-007: Snapshot-to-Streaming Transition

### Context

When Debezium first connects, it performs an **initial snapshot**: it reads
the current contents of the captured tables and emits one event per existing
row with `op = r` (a READ) before it begins **streaming** ongoing changes as
`op = c/u/d`. The worker must therefore handle both phases and, crucially,
guarantee that a snapshot row for a vehicle never overrides a newer streaming
change for that same vehicle (or vice-versa), so the cache converges to the
same state whether a value arrived via snapshot or via streaming.

This matters specifically because the ordering key from ADR-001 for the
`vehicles` stream is `source.lsn`, and Debezium's behavior for the `lsn` of a
snapshot record is not uniform: depending on connector version and load, a
snapshot event's `source.lsn` may be the replication slot's
`consistent_point` (non-null) **or** it may be `null`.

### Decision

Snapshot events are **not** a special case. They flow through the exact same
conditional-write path as streaming events (ADR-001/ADR-002); there is no
separate "snapshot mode" in the worker.

- **Null/absent LSN is coalesced to `0`.** Before any comparison, an
  incoming event's LSN is read as `lsn ?? 0`. A snapshot row therefore lands
  at the LSN floor (`0` when null), so **any** subsequent streaming event
  (which always carries a real, strictly-positive LSN) is strictly greater
  and overrides the snapshot baseline. The snapshot is the floor; streaming
  wins.
- **`vehicles` snapshot:** the snapshot emits one row per vehicle key, so
  snapshot rows never compete against each other on `vehicleLsn`; each simply
  seeds the hash and is later overridden (if at all) by streaming updates
  with higher LSNs.
- **`positions` snapshot:** the snapshot emits the full position history per
  vehicle (many `op = r` rows). Ordering by `recordedAt` (ADR-001) naturally
  reduces this history to the single latest position; the `positionLsn`
  tie-breaker still applies, with a null snapshot LSN coalesced to `0`.
- **Replay is re-reading the log, not re-snapshotting.** The exercise's
  correctness oracle — "replay from offset 0 rebuilds the identical cache" —
  replays the **same committed messages already in the Kafka topics**. It
  does not re-run the snapshot. Determinism therefore depends only on the
  projection being a pure function of the message sequence, which the
  conditional-write rules already guarantee. Forcing a brand-new snapshot
  (`snapshot.mode = always`) is a separate operational scenario and is not
  what the oracle exercises.

### Reasoning

Coalescing a null/absent LSN to `0` is what lets a single code path serve
both phases: the snapshot becomes the lowest possible watermark, so the
"only advance forward" invariant from ADR-001/ADR-002 does the rest. This
avoids introducing snapshot-specific branches (and the bugs that hide in
them), and keeps the replay-determinism story intact — the worker behaves
identically whether a value's first appearance was a snapshot READ or a
streaming CREATE.

Snapshot `ts_ms` is deliberately **not** used for ordering: it is the time
the snapshot was taken, not the time the row last changed, so it carries no
authoritative ordering meaning here — consistent with ADR-001's rejection of
`ts_ms` as an ordering key.

### Alternatives Considered

- **A dedicated snapshot buffer / distinct snapshot handling** — rejected:
  adds state and a second code path, and undermines the single-path
  determinism that the replay oracle depends on.
- **Trusting snapshot `ts_ms` (or wall-clock) to order snapshot vs
  streaming** — rejected: snapshot `ts_ms` is processing/snapshot time, not
  change time, and would not reliably place the snapshot below later
  streaming changes.
- **Failing/skipping events with a null LSN** — rejected: snapshot rows are
  legitimate initial state and must seed the cache; coalescing to `0` keeps
  them as a safe baseline instead of discarding them.

### Consequences

- Correctness rests on the (standard) guarantee that streaming LSNs are
  strictly greater than the snapshot's consistent-point floor, so streaming
  always wins over the snapshot baseline for the same key.
- The worker must coalesce a null/absent `source.lsn` to `0` before every
  comparison; this is a small but mandatory implementation detail, without
  which a null snapshot LSN would break the comparison or make it
  non-deterministic.
- No snapshot-specific state or configuration is required in the worker,
  keeping it the same simple, replayable projector described in ADR-005.
