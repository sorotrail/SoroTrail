# Pagination Cursor Stability Contract

SoroTrail paginates event listings (`GET /events`, `GET /events/export`, `GET /addresses/{address}/events`), contract summaries (`GET /contracts`), contract spec override history (`GET /contracts/{id}/spec/history`), and dead-letter queues (`GET /dead-letters`) using opaque keyset cursors.

Because downstream indexers, catch-up workers, and audit pipelines persist `cursor` values in their own databases and replay them hours or days later — potentially after a SoroTrail binary upgrade, configuration change, or storage migration — cursors are governed by an explicit **Cursor Stability Contract**.

---

## 1. Wire-Format Envelope

Default event-ID cursors (`order_by=id` or omitted `order_by` on `GET /events` and `GET /addresses/{address}/events`) emit the event's canonical TOID (`0001099511627776-0000000001`) verbatim so bare event IDs remain valid across all versions.

All composite and non-TOID cursors (`order_by=ledger`, `order_by=created_at`, `GET /contracts`, `GET /contracts/{id}/spec/history`, and `GET /dead-letters`), as well as explicitly versioned event-ID cursors (`store.EncodeVersionedEventCursor`), use a versioned, backend-tagged envelope with a keyed HMAC-SHA256 integrity checksum:

```text
<version>.<backend>.<payload_b64url>.<mac_b64url>
```

| Segment | Values | Purpose |
| --- | --- | --- |
| `<version>` | `v2` (current), `v1` (legacy) | Identifies the cursor wire-format version so decoders can distinguish supported legacy formats from retired or unknown versions (`store.ErrStaleCursor`). |
| `<backend>` | `pg` (PostgreSQL), `sq` (SQLite), `ch` (ClickHouse), `an` (backend-agnostic) | Binds the cursor to the storage backend family that minted it (`store.ErrCrossBackendCursor`). |
| `<payload_b64url>` | Raw URL-safe base64 (`[A-Za-z0-9-_]`) | Encodes the keyset position (`<sort_value>\|<id>` for composite orderings or `<id>` for single-column keysets). |
| `<mac_b64url>` | 4-character raw URL-safe base64 (24-bit keyed HMAC-SHA256 checksum) | Detects accidental truncation, corruption, or client-side mutation (`store.ErrTamperedCursor`) while keeping worst-case cursors within `config.ValidCursor`'s 128-character ceiling. (This checksum is an integrity guard, not a cryptographic authorization token; legacy `v1` cursors remain accepted during the `N-1` upgrade window, and multi-tenant isolation is enforced by `store.Scope`.) |

Clients **must** treat cursors as opaque tokens and never parse or synthesize `<payload_b64url>` directly.

---

## 2. What Cursors Are Guaranteed to Survive

A cursor returned by SoroTrail is guaranteed to remain valid and resume at the exact next row across:

1. **Process restarts and rolling deployments** of the same SoroTrail major version against the same storage backend.
2. **Patch and minor version upgrades** (`v0.x.y -> v0.x.z` and `v1.x -> v1.y`): decoders in `v2` continue to accept `v1` envelopes (`v1.<backend>.<payload>.<mac>`, `v1.<backend>.<payload>`, `v1.<payload>`) as well as legacy unversioned base64url cursors minted prior to envelope versioning.
3. **Concurrent ingestion, backfill, and audit repair**: because cursors encode keyset values `(sort_column, id)` rather than SQL `OFFSET` numbers, newly ingested or repaired rows never shift page boundaries or cause duplicate/skipped events.
4. **Retention pruning (`RETENTION_DAYS` / `RETENTION_MIN_LEDGER`)**: even if the exact anchor row referenced by a cursor has been pruned between pages, keyset comparison (`(sort_column, id) > (cursor_sort, cursor_id)`) still resumes cleanly from the first surviving row after that position.

---

## 3. What Invalidates a Cursor (`400 Bad Request` / `INVALID_CURSOR`)

SoroTrail intentionally rejects a cursor with HTTP `400 Bad Request` (`{"error": "...", "code": "INVALID_CURSOR", "field": "cursor"}`) — and **never** `500 Internal Server Error` or silent wrong results — when:

1. **Storage backend change (`store.ErrCrossBackendCursor`)**: a cursor minted by PostgreSQL (`pg`) is replayed against SQLite (`sq`) or ClickHouse (`ch`), or vice versa. Different backends have distinct timestamp precisions and primary-key sequences, so cross-backend cursor replay is rejected immediately.
2. **Sort ordering or sort key change (`store.ErrStaleCursor`)**: a cursor minted for `order_by=ledger` or `order_by=created_at` is replayed with `order_by=id` (or across incompatible `sort_key` types on `GET /contracts`).
3. **Retired or unknown major cursor version (`store.ErrStaleCursor`)**: a cursor carries a version prefix (`v0.`, `v3.`, etc.) outside the server's supported compatibility window.
4. **Truncation or tampering (`store.ErrTamperedCursor`)**: any character in the version, backend tag, base64url payload, or HMAC tag was modified or truncated.

---

## 4. Multi-Tenant Isolation Guarantee

Cursors carry only a keyset coordinate `(sort_value, id)` and **never** encode or bypass tenant authorization:

- Every query path (`Postgres.QueryEvents`, `SQLite.QueryEvents`, `ClickHouse.QueryEvents`, `ListContracts`) evaluates the authenticated caller's `store.Scope` (`contract_id = ANY($scope)`) **before** and **conjunctively (`AND`) with** the cursor predicate.
- Replaying a cursor obtained by Tenant A under Tenant B's API key can never return or disclose Tenant A's rows.

---

## 5. Client Recovery Pattern (`client.IsStaleCursor`)

Long-running consumers that persist cursors across restarts should store **both** the opaque `cursor` and the last durably processed `ledger` sequence (`last_processed_ledger`).

When a cursor is rejected as stale (`HTTP 400` with `code: "INVALID_CURSOR"`):

1. Detect the condition with `client.IsStaleCursor(err)` or `errors.Is(err, client.ErrStaleCursor)` in the Go SDK (`pkg/client`), or by checking `err.status === 400 && err.code === "INVALID_CURSOR"` in TypeScript / Python.
2. Discard the persisted `cursor`.
3. Resume pagination from `from_ledger = last_processed_ledger` with an empty `cursor`, deduplicating already-seen event IDs on the boundary ledger.

```go
page, err := cli.ListEvents(ctx, client.ListEventsParams{
    ContractID: contractID,
    Cursor:     savedCursor,
    Limit:      100,
})
if client.IsStaleCursor(err) {
    // Cursor invalidated by a backend migration or major upgrade;
    // fall back to the last durably processed ledger anchor.
    savedCursor = ""
    page, err = cli.ListEvents(ctx, client.ListEventsParams{
        ContractID: contractID,
        FromLedger: lastProcessedLedger,
        Limit:      100,
    })
}
```
