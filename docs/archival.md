# Cold-tier archival (planned)

> **Status: planned — not implemented.** This document sketches the
> proposed design for the cold-tier archival feature tracked in
> [#41](https://github.com/sorotrail/SoroTrail/issues/41). Nothing in the
> codebase implements it yet; no Go code, no migrations, no S3 / Parquet
> dependencies. The goal of this PR is to put the design in writing so a
> future implementation PR has a reference and reviewers can sign off on
> the shape before code lands.
>
> Treat every paragraph below as **proposed** and revisit during
> implementation. Open decisions are called out at the bottom.

## Why

Stellar RPC only retains contract events for roughly 24h–7d, and SoroTrail's
purpose is to keep them around longer than that. But "longer than that"
is not "forever" — Postgres storage is expensive at multi-year horizons,
and ad-hoc pruning decisions are dangerous in an archival indexer.

Cold-tier archival resolves the dilemma: closed ledger ranges are exported
as immutable Parquet files to S3-compatible object storage, where
years of events cost cents and remain analyzable by the entire data
ecosystem (DuckDB, Spark, pandas) without SoroTrail even running. With
archival in place, retention-pruning becomes safe by construction: rows
may only be deleted after their range has been durably archived and
verified.

This is distinct from the user-facing `/contracts/{id}/export` endpoint
(CSV / NDJSON): that is on-demand query output to a single human-driven
`curl`; this is automated, background, continuous storage tiering.

## Goal (pending sign-off on Open Decisions below)

When implemented, this is roughly the surface archival should offer.
Items 1–3 are subject to Open Decisions 1–5 below; items 4–6 are
uncontroversial mechanics and can land once those decisions are
signed off. Treat the rest of this doc as proposals that the Open
Decisions call out:

1. A background archiver exports events in fixed ledger-range chunks to
   `s3://$BUCKET/$PREFIX/events/ledger_start=$N/` as Parquet, with a
   manifest per chunk (row count, first/last event ID, checksum, schema
   version).
2. Archival state is tracked in the database; export is idempotent and
   resumable; a chunk is marked **archived** only after a read-back
   verification (manifest row count matches a re-count of the uploaded
   file).
3. Retention gains an opt-in **archive-before-prune** mode: refuse to
   delete any range not in `status='archived'` and verified.
4. `GET /archive/chunks` lists archived ranges with their object URIs so
   consumers can query cold data directly.
5. `/stats` adds an `archived_through_ledger` field — the inclusive
   highest ledger that has been durably archived, the analogue of
   `verified_through_ledger` but for cold storage.
6. Everything is **optional**: without the `ARCHIVE_*` configuration
   vars, the binary behaves exactly like today — no S3 client
   instantiated, no Parquet writer linked into the build.

## Storage layout (proposed)

```
s3://$ARCHIVE_BUCKET/$ARCHIVE_PREFIX/
└── events/
    └── schema=v1/
        └── ledger_start=0240000/
            ├── data-00000-of-00001.parquet
            └── manifest.json
```

Each Parquet file holds at most one chunk's events. The chunk size is a
fixed `ARCHIVE_CHUNK_LEDGERS` (default `17280`, ~24h at 5s/ledger).
The `ledger_start=…` layout mirrors the eventual events-table partitioning
key, so a future switch to native partitions and a switch to S3 archive
have the same key.

The `schema=v1/` directory level and the same field in each chunk's
`manifest.json` are expected to agree. If they ever diverge — for
example, an older chunk was re-uploaded inconsistently during a
v1→v2 migration — the archive is corrupt and must be re-exported;
treat any schema mismatch as a hard error on read.

`manifest.json` proposed shape:

```json
{
  "schema_version": 1,
  "chunk": {
    "ledger_start": 240000,
    "ledger_end": 257279,
    "event_count": 18234,
    "first_event_id": "0000096000038400-0000000000",
    "last_event_id": "0000102909952768-0000000009",
    "closed_at": "2026-07-21T00:00:00Z"
  },
  "files": [
    {"name": "data-00000-of-00001.parquet", "sha256": "…", "size": 4321}
  ],
  "producer": "sorotrail",
  "producer_commit": "<git-sha>"
}
```

The `schema_version` field is the long-lived decision: every reader of
the archive must check it before parsing. Bump it whenever the Parquet
schema below changes; keep the old schema readable side-by-side for at
least one major release so consumers can re-migrate.

## Parquet schema (proposed, awaiting sign-off)

```text
message Event {
  required binary id (STRING);                  // event TOID
  required binary contract_id (STRING);         // C... strkey
  required int64  ledger;
  required binary type (STRING);                // contract | system | diagnostic
  required binary tx_hash (STRING);             // hex
  required int32  tx_index;
  required int32  op_index;
  required boolean in_successful_call;
  required group topics (LIST) {
    repeated group list {
      optional binary element (STRING);         // JSON-encoded ScVal
    }
  }
  optional binary value (STRING);               // JSON-encoded ScVal
  optional binary topics_xdr (LIST) {
    repeated group list { optional binary element (STRING); }   // base64
  }
  optional binary value_xdr (STRING);           // base64
  optional int64  created_at (TIMESTAMP(MILLIS));
}
```

Open decisions on the schema:

- **`topics` / `value` as JSON strings** (above) vs. an exploded
  `struct topics_sym`, `topics_addr`, … column per ScVal variant.
  The exploded form is faster to filter in DuckDB but explodes the
  schema any time an ScVal variant changes. **Proposal: start with JSON
  strings** — it parallels the in-database representation exactly, so
  archived rows and live rows stay trivially comparable — and add
  materialized exploded columns in a future v2 if there's a real query
  workload that needs them.
- **`schema_version`** lives in the manifest, not in the Parquet
  metadata. Parquet metadata is fine for tooling; we want version
  visible at the bucket level for human operators too.

## Database state (proposed)

A new table in the next migration pair:

```sql
CREATE TABLE archive_chunks (
  ledger_start    BIGINT PRIMARY KEY,         -- aligned to ARCHIVE_CHUNK_LEDGERS
  ledger_end      BIGINT NOT NULL,
  status          TEXT NOT NULL,              -- pending | archived | failed
  object_uri      TEXT,                       -- s3://…/events/schema=v1/ledger_start=$ledger_start/
  row_count       BIGINT,
  manifest_sha256 TEXT,
  attempts        INT NOT NULL DEFAULT 0,
  last_error      TEXT,
  started_at      TIMESTAMPTZ NOT NULL,
  verified_at     TIMESTAMPTZ,
  closed_at       TIMESTAMPTZ
);
```

The `status` enum is intentionally lean — three stable states. In-flight
detail (uploading the temp object, read-back verification, promoting
to the real key) lives in logs and Prometheus counters, not in a
column. This matches the philosophy in `audit_findings` where
in-flight repairs are tracked by `attempts` + `last_error`, not a
`repairing` status. Status transitions:

```
   pending ─────────────────▶ archived
      │                          ▲
      │   (read-back verifies    │
      │    row count + checksum) │
      ▼                          │
   failed ◀──── (next pass retries, attempts++)
```

Verifying is **part of the transition**: a chunk is uploaded to a
temp S3 key, read back via `HeadObject` + a streaming row-count scan,
promoted to the real key, and only then marked `archived`. A partial
upload never flips state — a crash mid-chunk increments `attempts`,
sets `status='failed'` on the next pass, and retries from scratch.

## Archiver goroutine (proposed)

A long-running background goroutine, wired in `cmd/sorotrail/main.go`
alongside the ingester / auditor / webhook dispatcher:

1. **Find candidates.** Query archives for ranges whose `ledger_end <
   last_ingested_ledger − ARCHIVE_SAFETY_MARGIN` and whose row count
   in `archive_chunks` does not yet exist, plus any row in
   `status='failed'` whose attempts < `ARCHIVE_MAX_ATTEMPTS`.
2. **Insert / upsert row.** Insert `archive_chunks` if no row exists for
   the chunk's `ledger_start` (status starts at `pending`). The
   primary-key on `ledger_start` makes a second archiver instance a
   no-op on the same chunk; an existing `failed` row whose
   `attempts < ARCHIVE_MAX_ATTEMPTS` is reused in-place.
3. **Stream rows.** Reuse the bounded cursor-pagination pattern from
   the store (`MaxQueryLimit = 200`); stream into the Parquet writer,
   never materialise the chunk in memory.
4. **Upload to temp key.** Write the chunk to
   `…/ledger_start=$N/.tmp/<uuid>/data-…parquet` and `manifest.json`.
5. **Read back & verify.** `GetObject` the temp Parquet, re-count rows,
   compare against `archive_chunks.row_count`. Mismatch → delete temp
   objects, mark `failed`, leave the previous archived row untouched.
6. **Promote.** `CopyObject` temp → real key, then `DeleteObject` the
   temp prefix. Mark `archived` and stamp `verified_at`.

Wiring follows the same shape as `internal/audit`: a struct with a
`Run(ctx)` loop, a budget cap, and a constructor that the `main`
goroutine table runs alongside the ingester.

## Open decisions (sign-off needed before code lands)

The Open Decisions section is the load-bearing part of this doc —
everything above is contingent on these being resolved. A future
implementation PR is expected to either apply these defaults or call
out the deviation.

1. **Parquet library.** `github.com/parquet-go/parquet-go` (segmentio)
   is the most-overlap with the project's "stays small" convention
   and writes the in-schema above directly. `parquet-go` (xitongsys)
   is smaller but less actively maintained. *Proposal:* segmentio's
   `parquet-go`.

2. **S3 client library.** `aws-sdk-go-v2` (heavier, AWS-native) vs.
   `minio-go` (works against any S3-compatible store including
   production AWS as well as the MinIO used in tests). The doc
   assumes `minio-go` because it covers both worlds with one
   interface. *Proposal:* `minio-go`.

3. **Parquet schema layout.** Topics/value as JSON strings (proposed)
   vs. exploded `struct topics_sym`, `topics_addr`, … columns per
   ScVal variant. The exploded form is faster to filter in DuckDB but
   explodes the schema any time an ScVal variant changes. *Proposal:*
   JSON strings — it parallels the in-database representation exactly,
   so archived rows and live rows stay trivially comparable.

4. **Late writes into an archived range.** When backfill or the
   auditor repairs a row inside a range already marked `archived`:

   - **Reopen + re-archive (proposed default).** Bump
     `schema_version`, re-export the range to a versioned suffix
     (`ledger_start=$N/v=$v/…`), leave the old object under its old
     version. Operators get the latest data; one extra S3 round-trip
     and a few extra cents of storage. Matches the archival-indexer
     promise of "never lose history".
   - **Reject the write.** Backfill refuses to insert into an
     archived range; operator must explicitly invalidate the archive
     first. Cheapest, but manual.

   Whichever is picked must be **tested explicitly** in the
   integration suite (MinIO + a backfill that writes into a known-
   archived ledger range) and called out in the implementation PR
   description.

5. **Retention interlock default.** `ARCHIVE_REQUIRE_VERIFIED_FOR_PRUNE=false`
   (proposed) so deployments can turn archival on without immediately
   cutting over the prune. Once enough chunks are archived, a
   follow-up flips the default to `true`.

## Configuration (proposed)

New env vars, **all optional** — the binary builds and runs identically
if none are set:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ARCHIVE_ENABLED` | `false` | Master switch. `false` → archiver is never wired. |
| `ARCHIVE_BUCKET` | unset | S3-compatible bucket (works against MinIO in tests). Required to enable. |
| `ARCHIVE_PREFIX` | `""` | Object key prefix. Empty string writes at the bucket root. |
| `ARCHIVE_ENDPOINT` | unset | S3 endpoint URL. Unset = AWS default; set for MinIO / R2 / etc. |
| `ARCHIVE_REGION` | unset | S3 region. Required by AWS, optional for MinIO. |
| `ARCHIVE_ACCESS_KEY_ID` | unset | Credential pair. Both must be set; falling back to the ambient chain is **not** used here. |
| `ARCHIVE_SECRET_ACCESS_KEY` | unset | See above. |
| `ARCHIVE_CHUNK_LEDGERS` | `17280` | Ledger span per chunk; aligned to the eventual partition key. |
| `ARCHIVE_FRONTIER_LAG` | `200` | Don't archive any range ending in the last `N` ledgers behind frontier. Same default + intent as `AUDIT_LAG_THRESHOLD`; named to keep operator configs aligned across auditor + archiver. |
| `ARCHIVE_POLL_INTERVAL` | `30s` | Archiver idle-sleep between passes. |
| `ARCHIVE_MAX_ATTEMPTS` | `5` | Per-chunk retry budget before the chunk is kept `failed` and surfaced via `/stats`. |
| `ARCHIVE_BATCH_ROWS` | `1000` | Rows per stream page into the Parquet writer; bounded memory. |
| `ARCHIVE_REQUIRE_VERIFIED_FOR_PRUNE` | `false` | On-by-default-once-stable hook for #8: refuse a prune that would delete rows from a not-archived chunk. |

## Retention interlock (#8)

With `ARCHIVE_REQUIRE_VERIFIED_FOR_PRUNE=true`, the prune job queries
the inverse: rows younger than the lowest archived range are still
live-prunable; rows older must be in `archive_chunks.status='archived'`
before they can be deleted. A prune that targets an unarchived range
returns `400` with the unarchived ranges listed.

This turns archival from "nice to have" into the **prerequisite** for
safe deletion, which is the whole point of #41.

## API surface (proposed)

### `GET /archive/chunks`

```sh
curl -s localhost:8080/archive/chunks?limit=10
```

```json
{
  "chunks": [
    {
      "ledger_start": 240000,
      "ledger_end": 257279,
      "status": "archived",
      "object_uri": "s3://sorotrail-archive/events/schema=v1/ledger_start=0240000/",
      "row_count": 18234,
      "manifest_sha256": "…",
      "verified_at": "2026-07-22T00:00:30Z"
    }
  ],
  "cursor": "240000"
}
```

Standard cursor pagination. `?status=archived|failed|pending` filter is
helpful for operators.

### Extensions to `GET /stats`

```json
{
  "archived_through_ledger": 257279,
  "archive": {
    "chunks_archived": 12,
    "chunks_pending": 0,
    "chunks_failed": 1,
    "last_pass_at": "2026-07-22T00:00:30Z",
    "last_error": null
  }
}
```

`archived_through_ledger == 0` (or field absent) when
`ARCHIVE_ENABLED=false`.

## Querying the archive (proposed, for the README)

```sh
duckdb -c "
  SELECT contract_id, COUNT(*)
  FROM read_parquet('s3://sorotrail-archive/events/schema=v1/ledger_start=0240000/data-*.parquet')
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10;
"
```

This is the reason for the `#41` choice of columns: an analyst should
never need SoroTrail running to ask "what contracts emitted the most
events in this 24h window?" DuckDB gives the answer directly off S3.

## Edge cases to handle (proposed test list)

- Upload interrupted mid-chunk (partial object must never be marked
  archived — temp-key + verify + promote).
- Backfill inserting into an already-archived range (decision above;
  reopened by default, with a test).
- Credential expiry / bucket unavailable for days: archiver retries
  with backoff and surfaces `last_pass_at` + `last_error` through
  `/stats`; ingestion is **completely** unaffected (the live ingester
  does not import the S3 client).
- Malformed Parquet written by a buggy archiver version:
  `schema_version` mismatch detected on read → `failed`, manual review.
- `ARCHIVE_BUCKET` writable but `LIST` restricted (least-privilege
  deploys): promotion to real key still works; consumer-side
  `read_parquet` may not — out of scope, document it.



## Tests (proposed)

Integration tests against MinIO in the existing `docker-compose.yml`:

- Chunk export end-to-end (ingest a known range → mark `archived` →
  verify the Parquet file in the bucket has the same row count).
- Read-back verification with a deliberately corrupted temp Parquet
  (`failed` transition, no `archived` row written).
- Resume-after-interrupt (`stop` mid-chunk → `start` → same chunk is
  re-exported, no duplicate `archived` rows).
- Archive-before-prune refusal path (`ARCHIVE_REQUIRE_VERIFIED_FOR_PRUNE=true`
  + prune targeting non-archived range → `400` with the unarchived
  ranges listed).
- DuckDB load: the test assertably runs `duckdb -c "SELECT COUNT(*)
  FROM read_parquet(...)"` against the bucket and asserts the row
  count matches the manifest.

The DuckDB check is gated behind a build tag / `make test-archive`
target that skips cleanly when `duckdb` is absent from `$PATH`. CI
installs DuckDB in the archive-test job; downstream operators do
not need to. An equivalent Go test using `parquet-go`'s row reader is
acceptable as an alternative that drops the binary dependency
entirely.

Unit tests for the state machine in `internal/archive` and the
Parquet-row-count verifier; no real network in the unit suite.

## What this PR does **not** do

- Add any Go code, dependencies, or migrations. There is no
  `internal/archive/` package today.
- Wire any new background goroutine into `cmd/sorotrail/main.go`.
- Touch Postgres. No `archive_chunks` table is created.
- Touch S3 or MinIO. The integration test service is **not** added in
  this PR — that's part of the implementation PR.
- Modify `README.md`'s configuration table. The implementation PR
  adds the rows.

If you're reviewing this and expect to see code, the wrong PR landed:
this one is the design doc that comes first.

## References

- Issue [#41](https://github.com/sorotrail/SoroTrail/issues/41) — design source.
- [docs/replay.md](replay.md) — sibling batched + resumable + idempotent
  background tool; same `Run(ctx)` + advisory-locking shape.
- [docs/backfill.md](backfill.md) — sibling batched + resumable CLI tool;
  same `single-row state table` design.
- Retention issue [#8](https://github.com/sorotrail/SoroTrail/issues/8) —
  the consumer of `archive_chunks.status` once archival is wired.
