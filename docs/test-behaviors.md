# Test Behaviors — CDC-Driven Vehicle Cache

Behaviours are written **Given/When/Then** against observable state: what the
mock writers do to Postgres (Given/When) and what a reader sees in Valkey via
`HGETALL vehicle:{id}` (Then). They are the acceptance criteria the worker must
satisfy and mirror the required list in case §3.3.

**Automation legend**
- **E2E** — asserted by `just e2e` end-to-end (real Postgres→Debezium→Kafka→
  worker→Valkey), driven by `scenarios/e2e.json` (see `docs/spec.md` §8–§9).
- **UNIT** — asserted by a fast worker unit/integration test over the
  ordering/idempotency logic (Lua scripts + `orderingGuard`), no full stack.
- Behaviours B3, B4, B7, B8 are the **critical** ones the case asks to automate;
  all are **E2E** (B3/B4 additionally **UNIT** for fast feedback).

Field references use the Valkey schema in `docs/spec.md` §6.

---

## B1 — Vehicle state update is reflected in the cache
**Automation: E2E**

- **Given** no cache entry for vehicle `1`.
- **When** the vehicle writer inserts `vehicle 1 = REGISTERED` and later updates
  it to `ACTIVE` (higher LSN).
- **Then** `vehicle:1.state = "ACTIVE"`, `deleted = "0"`, and `vehicleLsn`
  equals the LSN of the `ACTIVE` update.

Anchor: ADR-001 (LSN ordering for the vehicles stream).

---

## B2 — New position is reflected in the cache
**Automation: E2E**

- **Given** `vehicle:1` exists (any state).
- **When** the position writer inserts a position for vehicle `1` with
  `recordedAt = T`.
- **Then** `vehicle:1.lat/lng` equal the inserted values, `recordedAt` equals
  `T` (epoch-ms), and the state fields are untouched.

Anchor: ADR-005 (per-field-group partial write; streams don't clobber each other).

---

## B3 — Out-of-order event is ignored when stale (position **and** state)
**Automation: E2E + UNIT**

- **Given** `vehicle:1.recordedAt = 18:00:10` (from scenario pos `seq 4`).
- **When** an older position arrives, `recordedAt = 18:00:05` (scenario
  `seq 5`), delivered *after* the newer one.
- **Then** `vehicle:1` still shows the `18:00:10` lat/lng; the stale position is
  a no-op.

- **And Given** `vehicle:1.vehicleLsn = L`.
- **When** a state update with LSN `< L` is delivered late.
- **Then** `state` and `vehicleLsn` are unchanged.

Anchor: ADR-001 (strict `>` at every level). This is the failure a naive
`SET`-per-message worker silently gets wrong (case §1).

---

## B4 — Duplicate CDC event is a no-op
**Automation: E2E + UNIT**

- **Given** a position `(recordedAt=18:00:10, lat, lng)` already applied to
  `vehicle:1`.
- **When** the exact same row is delivered again (Debezium redelivery) **or** a
  duplicate insert of identical data is captured (scenario `seq 6`,
  `duplicateOf: 4`).
- **Then** the observable position state (`lat`, `lng`, `recordedAt`) is
  unchanged. An exact redelivery (same `recordedAt` + `positionLsn`) is a strict
  no-op; a duplicate *insert* re-applies identical values while only advancing
  `positionLsn` — **idempotent in effect**.

Anchor: ADR-002 (idempotency as a consequence of conditional writes).

---

## B5 — Delete / tombstone flags the vehicle
**Automation: E2E**

- **Given** `vehicle:1` exists as `ACTIVE`.
- **When** the vehicle writer deletes vehicle `1` (Debezium emits `op=d` with an
  LSN, followed by a `null`-value tombstone on the same key).
- **Then** `vehicle:1.deleted = "1"`, `deletedLsn` equals the delete's LSN, and
  the prior `state`/position fields remain present (soft delete). The tombstone
  message changes nothing (explicit no-op) and its offset is committed.

- **And When** a stale state/position event for vehicle `1` with a lower
  watermark arrives *after* the delete.
- **Then** it does not resurrect the vehicle as live (the delete watermark holds
  for state; a late position may still record lat/lng but `deleted` stays `1`).

Anchor: ADR-004 (soft delete + watermark), spec §7.3 (tombstone no-op).

---

## B6 — Position arrives before the vehicle exists (cross-stream race)
**Automation: E2E**

- **Given** no `vehicles` event has been consumed for vehicle `2`.
- **When** a position for vehicle `2` is consumed (scenario `seq 7`, orphan).
- **Then** `vehicle:2` exists with `lat/lng/recordedAt/positionLsn` set and the
  state fields **absent** — a valid partial record ("we know where it is, not
  what state it is in").

- **And When** a `vehicles` event for vehicle `2` later arrives.
- **Then** it fills in `state`/`vehicleLsn` without touching the existing
  position fields.

Anchor: ADR-005 (partial-write, no buffering, deterministic under replay).

---

## B7 — Worker restart mid-stream loses nothing
**Automation: E2E**

- **Given** the worker is consuming and has written some entries to Valkey.
- **When** the worker process is killed **between** consuming a message and
  committing its offset, then restarted.
- **Then** the uncommitted message is redelivered and re-applied; because the
  re-apply is a conditional write, the final cache is identical to the
  no-crash run — no lost update, no double-count.

Anchor: ADR-002 (commit *after* the Valkey write; redelivery is safe).

---

## B8 — Full replay from offset 0 rebuilds the identical cache
**Automation: E2E (the correctness oracle)**

- **Given** a completed run whose Valkey state has been asserted correct (B1–B6).
- **When** Valkey is flushed and the **same committed messages** are re-consumed
  from offset 0 by a fresh consumer group (re-reading the log, not
  re-snapshotting).
- **Then** the rebuilt cache equals the pre-flush asserted state, field for
  field, for every vehicle.

Anchor: ADR-002 + ADR-007 (single conditional-write path, no replay mode). This
is the minimum-bar oracle in case §4.

---

## B9 — Snapshot seeds the cache, streaming overrides it
**Automation: E2E**

- **Given** rows already exist in Postgres when Debezium first connects, so the
  initial snapshot emits them as `op=r` (possibly with a null `source.lsn`).
- **When** the snapshot is consumed and then a streaming `op=u` for the same
  vehicle arrives.
- **Then** the snapshot value seeds the hash (null LSN coalesced to `0`), and the
  streaming update — carrying a strictly-positive LSN — overrides it. A snapshot
  row never overrides a value already set by a streaming event.

Anchor: ADR-007 (snapshot→streaming, `lsn ?? 0` floor).

---

## B10 — Position ordering tie-break by `positionLsn`
**Automation: UNIT**

- **Given** `vehicle:1.recordedAt = T` and `positionLsn = P`.
- **When** another position with the **same** `recordedAt = T` arrives with
  `positionLsn = P' > P`.
- **Then** it is applied (advancing `positionLsn` to `P'`); with `P' <= P` it is
  a no-op.

Anchor: ADR-001 (recordedAt primary, positionLsn strict tie-breaker).

---

## Coverage vs case §3.3

| Case-required behaviour | Covered by |
|-------------------------|------------|
| Vehicle state update reflected | B1 |
| New position reflected | B2 |
| Out-of-order stale ignored | **B3** |
| Duplicate is a no-op | **B4** |
| Delete / tombstone | B5 |
| Position before vehicle | B6 |
| Worker restart loses nothing | **B7** |
| Full replay rebuilds identical cache | **B8** |
| (extra) snapshot→streaming | B9 |
| (extra) position tie-break | B10 |
