# Operator Runbook

Practical troubleshooting guide for running SoroTrail in production. Written
for someone paged at 3 a.m. who has not read the source.

Looking up one specific error message rather than working an incident? See
[Troubleshooting](troubleshooting.md), which indexes the common RPC,
database, API and replay errors by their text.

---

## Table of Contents

- [Pre-deployment preflight self-check](#pre-deployment-preflight-self-check)
- [Quick health check](#quick-health-check)
- [Ingestion stalled](#ingestion-stalled)
- [Dead-letter / error log filling up](#dead-letter--error-log-filling-up)
- [Audit findings piling up](#audit-findings-piling-up)
- [Dirty migration recovery](#dirty-migration-recovery)
- [Poisoned event](#poisoned-event)
- [API returning stale or missing data](#api-returning-stale-or-missing-data)
- [Database connection exhaustion](#database-connection-exhaustion)
- [RPC rate limiting / 429 errors](#rpc-rate-limiting--429-errors)
- [Gap in ingested data (retention window missed)](#gap-in-ingested-data-retention-window-missed)
- [Bulk event deletion (`DELETE /events`)](#bulk-event-deletion-delete-events)
- [Backup and restore](#backup-and-restore)
- [Useful queries](#useful-queries)
- [Configuration reference](#configuration-reference)

---

## Pre-deployment preflight self-check

Run `sorotrail preflight` before rolling out a new version or as a Kubernetes
`initContainer` to verify that configuration, credentials, network reachability,
and remote state are coherent before starting the indexer:

```sh
# Run all startup self-checks (human-readable table).
sorotrail preflight

# Emit machine-readable JSON for CI/CD gates or initContainers.
sorotrail preflight --json

# Run only a subset of checks when staging dependencies incrementally.
sorotrail preflight --check config,database,rpc --timeout 5s

# Allow pending migrations when the runtime role will apply them on startup.
sorotrail preflight --allow-pending-migrations
```

`sorotrail preflight` validates:
- **`config`**: runs the full startup configuration validator and checks relative `START_LEDGER` syntax with all secrets redacted.
- **`database`**: connects and pings the configured PostgreSQL or SQLite backend.
- **`migrations`**: inspects `schema_migrations` without mutating the database, catching dirty schemas, unapplied migrations, or missing `CREATE` privileges on the target schema.
- **`rpc`**: probes each configured Soroban RPC endpoint (`getNetwork` and `getHealth`), verifying that the node's network passphrase matches `NETWORK` (`testnet`, `mainnet`, `futurenet`) and that `START_LEDGER` falls within the node's retention window.
- **`archive`**: when `ARCHIVE_BUCKET` is set, verifies S3 bucket reachability and write/delete permissions using a temporary probe object (or read-only with `--read-only`).
- **`retention`**: verifies coherence between `RETENTION_*` and `ARCHIVE_*` settings so pruning cannot delete unarchived events.

Exit codes: `0` when all selected checks pass, `1` when any dependency check fails, and `2` on configuration validation or CLI flag errors.

---

## Quick health check

Run these first, before diving into any specific issue.

```sh
# 1. Is the API reachable?
curl -s localhost:8080/health | jq .

# 2. Current ingestion posture and audit counters.
curl -s localhost:8080/stats | jq .

# 3. Recent logs (if running via Docker).
docker compose logs --tail 50 sorotrail

# 4. Is Postgres accepting connections?
psql "$DATABASE_URL" -c "SELECT 1"
```

The `/health` endpoint reports each dependency individually: every entry in
`checks` carries a `status` (`ok`, `degraded` or `unready`) plus a `detail`
explaining any non-ok status. A `503` names the failing dependency — and
whether it is unreachable (`unready`) or reachable-but-unhealthy
(`degraded`) — so you don't need the logs to see which one broke.
`/readyz` answers the same envelope with an added `ingestion_lag` check
(degraded beyond 100 ledgers behind the chain head); `/livez` never touches
a dependency. The `/stats` endpoint shows `last_ingested_ledger`,
`verified_through_ledger`, and audit counters — the three numbers that
tell you whether the system is making progress and whether the auditor is
happy.

---

## Ingestion stalled

**Symptoms**

- `last_ingested_ledger` in `/stats` stops advancing.
- Logs repeat `"ingestion pass failed"` with an error message.
- The process is running but no new events appear.

**Diagnosis**

1. Check the logs for the error pattern. Common causes:

   | Error pattern | Likely cause |
   | --- | --- |
   | `getEvents from ledger N: ...` | RPC is returning an error (rate limit, outage, malformed response). |
   | `connecting to postgres` / `pinging postgres` | Database is down or unreachable. |
   | `Resume ledger fell outside RPC retention window` | The indexer was offline long enough that its resume point aged out. The ingester automatically skips ahead, but events in the gap are lost. |
   | `getHealth for cold start` | RPC is unreachable during position resolution. |

2. If the error is RPC-related, check the Stellar RPC status page or hit
   the health endpoint directly:

   ```sh
   curl -s https://soroban-testnet.stellar.org | head
   ```

3. If the error is Postgres-related, verify the connection:

   ```sh
   psql "$DATABASE_URL" -c "SELECT now()"
   ```

**Remedy**

- **RPC outage**: Wait for it to recover. SoroTrail retries with jittered
  exponential backoff automatically. The backoff resets to 1 second after
  every successful pass, and caps at `MaxBackoff` (default 1 minute).

- **Postgres down**: Restart Postgres, then restart SoroTrail. Migrations
  run automatically on startup so there is nothing extra to do.

- **Retention gap**: If the log shows `"resume ledger fell outside RPC
  retention window; skipping ahead"`, data in the gap is irrecoverable from
  RPC. This is the exact problem SoroTrail exists to prevent. To minimize
  recurrence, ensure the indexer stays running or set up process supervision
  (systemd, Docker restart policy, Kubernetes liveness probe).

- **Config error**: If the process fails on startup with a validation error,
  fix the environment variable and restart. Run `sorotrail` with
  `LOG_LEVEL=debug` to see the exact validation failure.

---

## Dead-letter / error log filling up

SoroTrail does not have a traditional dead-letter queue. Failed RPC requests
are retried with exponential backoff and logged. If you see a burst of
error-level log lines:

**Diagnosis**

1. Check the error type in the logs. The two main categories:

   - **Transient RPC errors** (timeouts, 429s, temporary network blips):
     SoroTrail handles these with retries. If they persist, the upstream
     RPC endpoint may be degraded.

   - **Application errors** (malformed response, unexpected JSON shape):
     These usually indicate an RPC version change or a new event type that
     the decoder does not handle yet. Unknown ScVal types fall back to a
     lossless `{"unknown": {...}}` wrapper — ingestion never stalls on
     decoding alone.

2. Check if `last_ingested_ledger` is still advancing. If it is, the
   errors are non-fatal and the system is self-healing.

**Remedy**

- If errors are transient, let the backoff run its course. The ingester
  will recover automatically.
- If the RPC endpoint is persistently failing, switch `RPC_URL` to an
  alternative provider and restart.
- If a new event type is causing decode failures, check the logs for
  `decoding event` errors. The decoder's fallback prevents stalls, but
  you should update the decoder and run a replay afterward.

---

## Audit findings piling up

**Symptoms**

- `/stats` shows `findings_opened` climbing without corresponding
  `findings_repaired`.
- `findings_unrecoverable` is non-zero.

**Diagnosis**

1. Check the findings table directly:

   ```sh
   psql "$DATABASE_URL" -c "
     SELECT id, from_ledger, to_ledger, expected_count, actual_count,
            status, attempts, last_error
     FROM audit_findings
     WHERE status IN ('open', 'unrecoverable')
     ORDER BY id DESC
     LIMIT 20"
   ```

2. Understanding finding statuses:

   | Status | Meaning |
   | --- | --- |
   | `open` | Auditor found a mismatch and is attempting repair. |
   | `repaired` | Repair succeeded; stored events now match RPC. |
   | `unverifiable` | The ledger range aged out of the RPC retention window during repair. The auditor cannot re-fetch to verify. |
   | `unrecoverable` | After `AUDIT_MAX_REPAIR_ATTEMPTS` (default 3) the RPC keeps returning different results. Needs manual investigation. |

3. If findings are `unverifiable`, the data in that range is permanently
   unverifiable against the RPC. The stored data may still be correct —
   the auditor just cannot prove it.

4. If findings are `unrecoverable`, the RPC is returning inconsistent data
   for that range across multiple fetches. This can happen with certain
   RPC edge cases during network upgrades or state resets.

**Remedy**

- **Open findings**: The auditor auto-repairs. Give it time. Check
  `AUDIT_LAG_THRESHOLD` — the auditor pauses when ingest has not moved
  far enough past `verified_through_ledger`. If ingest is stuck, fix
  ingestion first.

- **Unrecoverable findings**: These are informational. The system is
  designed to surface them rather than crash. If the data matters, you can
  manually re-ingest the affected range:

  ```sh
  # Check what the RPC has for this range.
  sorotrail replay --from-ledger FROM --to-ledger TO --dry-run
  ```

  Or restart the process — the auditor will retry open findings on the
  next pass.

- **To disable the auditor entirely**: Set `AUDIT_ENABLED=false` (the
  default). The binary behaves identically to a pre-audit build.

---

## Dirty migration recovery

Migrations run automatically on startup via `store.Migrate`. If a migration
fails halfway (e.g., Postgres crashes mid-`ALTER TABLE`):

**Symptoms**

- Process fails on startup with an error mentioning migrations.
- The `schema_migrations` table may show a version that does not match the
  applied schema.

**Diagnosis**

1. Check which migration failed:

   ```sh
   psql "$DATABASE_URL" -c "SELECT * FROM schema_migrations"
   ```

2. Inspect whether the migration's `up.sql` was partially applied. Look at
   the tables it creates/modifies:

   ```sh
   psql "$DATABASE_URL" -c "\dt"
   ```

**Remedy**

The migrations use `golang-migrate` which tracks applied versions in
`schema_migrations`. If a migration partially applied:

1. **If the `up.sql` is idempotent** (CREATE TABLE IF NOT EXISTS, additive
   columns): simply restart the process — the migration will complete on
   the next attempt.

2. **If the migration is not idempotent**: manually fix the schema to match
   the target state, then force the migration version:

   ```sh
   # Force the schema_migrations version to the last fully-applied migration.
   # Example: migration 2 failed, force to version 1.
   psql "$DATABASE_URL" -c "UPDATE schema_migrations SET dirty = false WHERE version = 2"
   psql "$DATABASE_URL" -c "DELETE FROM schema_migrations WHERE version = 2"
   ```

   Then restart. The migration will run from scratch.

3. **Nuclear option — restore from backup**: If the schema is corrupted
   beyond quick fix, restore from a backup and let migrations replay.

**Prevention**: Always back up the database before upgrading the binary in
production. Migrations are designed to be forward-only (no down migrations
in production).

---

## Poisoned event

A "poisoned" event is one whose XDR cannot be decoded, causing the decoder
to error.

**Symptoms**

- Logs contain `decoding event <ID>: ...` at error level.
- `last_ingested_ledger` still advances (the error is per-event, not
  per-pass — SoroTrail logs and skips bad events rather than stalling).

**Diagnosis**

1. The decoder's fallback mechanism means unknown ScVal types produce a
   lossless `{"unknown": {...}}` wrapper. A decode error means something
   more fundamental (malformed XDR, unexpected structure).

2. Check the event ID in the logs, then look it up:

   ```sh
   curl -s "localhost:8080/events/EVENT_ID" | jq .
   ```

3. If the event was persisted despite the decode error, its `topics` and
   `value` fields will contain the best-effort decoding. If it was not
   persisted, the upsert failed and the event was dropped for that pass.

**Remedy**

- The system is designed to be resilient: one bad event does not block
  ingestion. Monitor the error count; if it stays at 1, it is a one-off.
  If it grows, there may be a new event type that needs decoder support.

- If you need to recover events that were dropped due to decode errors,
  update the decoder to handle the new type, then run a replay:

  ```sh
  sorotrail replay --from-ledger LEDGER_WITH_BAD_EVENT --dry-run
  sorotrail replay --from-ledger LEDGER_WITH_BAD_EVENT
  ```

---

## API returning stale or missing data

**Symptoms**

- `/events` returns no results or returns data that looks old.
- `total_events` in `/stats` is zero or very low.

**Diagnosis**

1. Check `last_ingested_ledger`. If it is advancing, the data is being
   ingested — the query might be too restrictive.

2. Check if `WATCHED_CONTRACTS` is set. If you are watching specific
   contracts, only events for those contracts are stored. An empty
   `WATCHED_CONTRACTS` means all contract events are ingested.

3. Verify the query parameters. The API defaults to ascending order and
   limit 50. If you expect recent events, try:

   ```sh
   curl -s 'localhost:8080/events?order=desc&limit=5' | jq .
   ```

4. If `total_events` is zero and `last_ingested_ledger` is zero, the
   ingester has not successfully completed a single pass. Check logs for
   startup errors.

**Remedy**

- Ensure `RPC_URL` points to a valid Stellar RPC endpoint.
- Ensure `DATABASE_URL` is correct and Postgres is reachable.
- If `WATCHED_CONTRACTS` is set, verify the contract IDs are valid
  Soroban strkey format (`C...`, 56 characters).
- If the ingester was recently restarted with `START_LEDGER` set to a
  very high value, it may be waiting for the RPC to reach that ledger.

---

## Database connection exhaustion

**Symptoms**

- Logs show `connecting to postgres` errors or `too many connections`.
- The API and ingester become unresponsive.

**Diagnosis**

1. Check active connections:

   ```sh
   psql "$DATABASE_URL" -c "
     SELECT count(*), state
     FROM pg_stat_activity
     WHERE datname = current_database()
     GROUP BY state"
   ```

2. Check for long-running transactions:

   ```sh
   psql "$DATABASE_URL" -c "
     SELECT pid, state, query_start, now() - query_start AS duration, query
     FROM pg_stat_activity
     WHERE datname = current_database()
       AND state != 'idle'
     ORDER BY query_start"
   ```

**Remedy**

- SoroTrail uses `pgxpool` which manages a connection pool. The default
  pool size is fine for single-instance deployments.
- If you are sharing the database with other services, increase
  `max_connections` in Postgres or set up a separate database.
- A stuck replay holding the advisory lock does not block connections —
  the lock is on a single pooled connection. If you see connection pressure,
  check for external consumers.
- Restart the SoroTrail process to reset the connection pool.

---

## RPC rate limiting / 429 errors

**Symptoms**

- Logs show HTTP 429 or rate-limit errors from the RPC.
- Ingestion slows down or stalls temporarily.

**Diagnosis**

1. SoroTrail limits requests to approximately 10/s (matching public
   endpoint limits). If `AUDIT_ENABLED=true`, the budget is shared between
   ingestion and audit via `AUDIT_BUDGET_SHARE` (default 10% to audit).

2. Check if `AUDIT_MAX_RPS` is set appropriately for your RPC provider's
   limits.

**Remedy**

- **Public testnet endpoints** are rate-limited. For production, use a
  dedicated RPC provider with higher limits.
- If audit is enabled and competing for budget, reduce `AUDIT_BUDGET_SHARE`
  or increase `AUDIT_MAX_RPS`.
- The jittered exponential backoff handles 429s gracefully — the ingester
  will slow down and recover automatically.

---

## Gap in ingested data (retention window missed)

**Symptoms**

- Logs contain `"resume ledger fell outside RPC retention window; skipping
  ahead"`.
- `/stats` shows `last_ingested_ledger` jumped forward by thousands.

**Diagnosis**

1. This means the indexer was offline (or unable to reach the RPC) long
   enough that its resume point fell outside the RPC's retention window
   (typically 7 days on mainnet).

2. The gap is irrecoverable from the RPC. The `verified_through_ledger`
   in `/stats` will remain at 0 (or wherever it was) until the auditor
   catches up to the new position.

**Remedy**

- This is the problem SoroTrail was built to prevent. If uptime matters,
  set up process supervision:
  - Docker: `restart: unless-stopped` in `docker-compose.yml`.
  - systemd: `Restart=always` in the unit file.
  - Kubernetes: liveness/readiness probes.
- If you have a backup from before the gap, you could theoretically
  restore it, but the events in the gap are only available from the RPC
  while the RPC still retains them. Once the retention window passes, they
  are gone.

---

## Bulk event deletion (`DELETE /events`)

`DELETE /events` is the manual, admin-only bulk delete. It is the only API
surface that removes stored events outside the retention pruner, and it
deliberately does not inherit the pruner's safeguards. Use it for a one-off
correction, not as a retention policy: the pruner is batched, scheduled,
bounded by `last_ingested_ledger`, optionally archived, and reports metrics.

### What it deletes

`DELETE /events?before_ledger=N` runs one SQL statement:

```sql
DELETE FROM events WHERE ledger < N;
```

Consequences:

- The bound is **exclusive**: ledger `N-1` is deleted, ledger `N` is kept.
- The delete is **instance-wide**. The statement has no `network` predicate,
  so it removes the range on every network stored in the database, not only
  the server's configured `NETWORK`.
- It is **one unbatched statement**, with no `LIMIT`, no dry run, and no
  `last_ingested_ledger` guard. A bound above the chain head deletes every
  stored event. A large range can hold locks and grow WAL for a long time.
- It removes only rows from `events`. On Postgres, `event_addresses` has no
  foreign key to `events`, so those index rows survive; address endpoints
  join `events`, so they will not return the deleted events, but the index
  entries are not reclaimed here. `token_balances`, `contract_meta`,
  `archive_chunks`, subscriptions, and delivery history are also untouched.
- It does not reset `ingestion_state`, `contract_cursors`, `backfill_state`,
  or `audit_state`.

### Authentication, parameters and response

The route is gated by the static `API_KEY`, not by the tenant API-key
system. `X-API-Key` must match `API_KEY` byte-for-byte; an
`Authorization: Bearer ...` key minted by `/apikeys` is not accepted here.
When `API_KEY` is unset the endpoint is closed and every request gets `503`
rather than running unauthenticated.

| Input / outcome | Behaviour |
| --- | --- |
| `before_ledger` | Required. Positive integer. Exclusive: `ledger < before_ledger`. There is no maximum, so validate it before sending. |
| success | `200 {"deleted": N}`. `N` is the number of rows the statement removed; an empty range returns `0`. `Cache-Control: no-store`. |
| missing or invalid `before_ledger` | `400` with the parse error. |
| missing or wrong key | `401 invalid or missing X-API-Key header`. |
| no `API_KEY` configured | `503 API_KEY env var must be set ...`. |
| store failure | `500 deleting events failed`. |

On ClickHouse the delete is an asynchronous `ALTER TABLE ... DELETE`
mutation; the response count is a snapshot taken before the mutation, so
rows can remain visible briefly. Postgres and SQLite delete synchronously.

```sh
curl -sS -X DELETE \
  -H "X-API-Key: $API_KEY" \
  "$SOROTRAIL_URL/events?before_ledger=250000"
```

### What it cannot undo

**There is no undo.** The endpoint returns only a count, not the deleted
IDs, and it does not write a backup. Recovery is limited to what still
exists in the RPC, the archive, or your own export.

- **RPC retention.** Stellar RPC serves only the most recent
  `ledgerRetentionWindow` ledgers (roughly 24h-7d). Check the live window
  with:

  ```sh
  curl -sS -X POST "$RPC_URL" \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
    | jq '{oldestLedger: .result.oldestLedger, latestLedger: .result.latestLedger, ledgerRetentionWindow: .result.ledgerRetentionWindow}'
  ```

  If `before_ledger - 1 < oldestLedger`, part or all of the range is
  already outside the RPC's retention window. Those rows cannot be
  re-ingested; export them before deleting.

- **Archive.** The endpoint never archives. The pruner's archive path is
  separate: with `ARCHIVE_BUCKET` and `ARCHIVE_BEFORE_PRUNE=true`, the
  pruner writes the range to `<prefix>/events/schema=1/ledger_start=.../`
  as `data.ndjson.gz` plus `manifest.json` before deleting it. The bulk
  endpoint bypasses that path, so a range can be deleted without ever being
  archived.

  Check the bucket, not `archive_chunks`: the current archiver writes
  objects but does not populate the `archive_chunks` table (see the status
  note in [archival.md](archival.md)). A manifest whose `chunk.ledger_end`
  is greater than `before_ledger` covers the range. Archived objects are
  never removed by this endpoint.

- **Audit.** The auditor is forward-only. It verifies from
  `verified_through_ledger + 1` toward the ingest frontier and never
  revisits a lower ledger. Therefore:

  - A delete at or below `verified_through_ledger` is invisible to the
    auditor and stays deleted.
  - A delete above the high-water mark, while the RPC still retains the
    range, looks like missing data: the auditor opens a finding and
    re-ingests the range, restoring exactly the events just deleted.
  - Once the range has aged out of the RPC, the auditor cannot restore it;
    findings in that range can end up `unverifiable` or `unrecoverable`.

  If you need a deletion above `verified_through_ledger` to stick, keep the
  auditor disabled for the operation; re-enabling it with the old
  high-water mark will trigger a repair. The simplest safe rule is to
  delete only at or below the high-water mark. When `AUDIT_ENABLED=false`,
  `verified_through_ledger` stays `0`, so there is no auditor to undo the
  delete and this check does not apply.

- **Backfill.** `backfill_state` and `contract_cursors` are untouched. A
  backfill that is still running resumes from
  `backfill_state.last_ledger + 1` and will re-insert any deleted ledgers
  above that point as it advances. A completed backfill will not replay
  them. Deleting rows does not reset either cursor.

- **Live subscribers.** Neither the WebSocket stream nor webhook
  subscriptions receive a delete or tombstone event. They only see newly
  ingested events at delivery time, and neither has server-side replay;
  clients implement the catch-up pattern by querying `/events`, which also
  stops returning the deleted rows. A delivery already queued in memory may
  still arrive after the delete, and historical `delivery_attempts` rows
  remain.

- **HTTP caches.** `GET /events/{id}` is served with
  `Cache-Control: public, max-age=31536000, immutable`. A CDN or browser
  that cached a deleted event can keep serving it for up to a year even
  though the API now returns `404`. Purge the CDN/proxy cache after the
  delete if the event must disappear immediately.

### Safe procedure

1. **Pick the bound and record the current state.**

   ```sh
   BOUND=250000

   psql "$DATABASE_URL" <<SQL
   SELECT count(*) AS events_below_bound FROM events WHERE ledger < $BOUND;
   SELECT coalesce(max(last_ingested_ledger), 0) AS last_ingested
     FROM ingestion_state;
   SELECT network, verified_through_ledger FROM audit_state ORDER BY network;
   SELECT contract_id, from_ledger, to_ledger, last_ledger, completed_at
     FROM backfill_state;
   SQL
   ```

   If `events_below_bound` is `0`, there is nothing to delete. If a
   backfill is not complete and `last_ledger < BOUND - 1`, it can restore
   part of the range; stop it first or account for that.

2. **Confirm the RPC and archive cannot cover the range.** Run the
   `getHealth` check above. If `BOUND - 1 < oldestLedger` and no archive
   manifest covers the range, this delete is permanent. Export first.

3. **Export the exact range and verify the export.**

   ```sh
   curl -sG "$SOROTRAIL_URL/events" \
     --data-urlencode "stream=true" \
     --data-urlencode "include_xdr=true" \
     --data-urlencode "from_ledger=1" \
     --data-urlencode "to_ledger=$((BOUND - 1))" \
     -o "events-before-$BOUND.ndjson"

   EXPECTED=$(curl -sG "$SOROTRAIL_URL/events/count" \
     --data-urlencode "to_ledger=$((BOUND - 1))" | jq -r .count)
   test "$(wc -l < "events-before-$BOUND.ndjson")" -eq "$EXPECTED"
   sha256sum "events-before-$BOUND.ndjson"
   ```

   `include_xdr=true` keeps the raw topic/value XDR needed by
   `sorotrail replay`; without it the export is only the decoded JSON.
   Store the file somewhere durable before continuing. `/events` is
   streamed, so it has no page limit, but the whole range must fit on disk
   and the request lasts as long as the scan.

4. **Make the delete auditable.** Only proceed when
   `BOUND - 1 <= verified_through_ledger` for every network you are
   deleting from. If that is not true, either wait for the auditor to
   verify through the range or keep the auditor disabled for the whole
   operation (see above).

5. **Delete.**

   ```sh
   curl -sS -X DELETE \
     -H "X-API-Key: $API_KEY" \
     "$SOROTRAIL_URL/events?before_ledger=$BOUND" \
     | tee "delete-$BOUND.json"
   ```

6. **Verify the range is gone and nothing was re-ingested.**

   ```sh
   curl -sG "$SOROTRAIL_URL/events/count" \
     --data-urlencode "to_ledger=$((BOUND - 1))"
   # {"count":0}

   curl -s "$SOROTRAIL_URL/stats" \
     | jq '{total_events, oldest_stored_ledger, verified_through_ledger, last_ingested_ledger}'
   ```

   The count must be `0`. `oldest_stored_ledger` should move forward (or
   become `0` if the table is empty). Check `audit_findings` for a new
   finding over the range; if one appears, the auditor is re-fetching it.
   Then purge any CDN or browser cache for the deleted event URLs.

7. **Keep the export.** The NDJSON file and its checksum are the only
   recovery path once the RPC has aged out the range; keep them for at
   least as long as the deleted data matters.

---

## Backup and restore

### What to back up

SoroTrail's only persistent state is the Postgres database. There are no
files on disk that matter (the binary is stateless, cursors and progress
are in the database).

### Backup procedure

```sh
# Full logical backup (recommended for small-to-medium databases).
pg_dump "$DATABASE_URL" > sorotrail-backup-$(date +%Y%m%d-%H%M%S).sql

# Or compressed:
pg_dump "$DATABASE_URL" | gzip > sorotrail-backup-$(date +%Y%m%d-%H%M%S).sql.gz
```

For large databases, use `pg_basebackup` or your cloud provider's snapshot
feature.

### When to back up

- Before upgrading the binary (migrations run automatically on startup).
- Before changing `WATCHED_CONTRACTS` or `START_LEDGER`.
- Periodically if you want point-in-time recovery.

### Restore procedure

```sh
# Restore from a logical backup.
# WARNING: this drops and recreates tables. Stop SoroTrail first.
psql "$DATABASE_URL" < sorotrail-backup-YYYYMMDD-HHMMSS.sql

# Then restart SoroTrail — it will re-run migrations if needed and resume
# ingestion from the persisted cursor.
```

### What is NOT in the backup

- The running process state (cursor, backoff counter). On restart, the
  ingester resumes from the persisted `ingestion_state` row.
- Audit state and findings. These are in the database and will be restored.
- Replay state. Also in the database.

### Recovery point objective

SoroTrail's RPO is effectively zero under normal operation — every
successful RPC page is immediately persisted. If the process crashes between
pages, at most one page of events (up to 1000 events) needs to be
re-fetched. Idempotent upserts mean re-fetching never creates duplicates.

---

## Useful queries

### Check ingestion progress

```sh
psql "$DATABASE_URL" -c "
  SELECT last_ingested_ledger, last_cursor, updated_at
  FROM ingestion_state WHERE id = 1"
```

### Check audit verification progress

```sh
psql "$DATABASE_URL" -c "
  SELECT verified_through_ledger, updated_at
  FROM audit_state WHERE id = 1"
```

### Count events by contract

```sh
psql "$DATABASE_URL" -c "
  SELECT contract_id, count(*) AS events
  FROM events
  GROUP BY contract_id
  ORDER BY events DESC
  LIMIT 10"
```

### Find events in a ledger range

```sh
psql "$DATABASE_URL" -c "
  SELECT id, contract_id, ledger, type
  FROM events
  WHERE ledger BETWEEN 250000 AND 260000
  ORDER BY id
  LIMIT 20"
```

### Check for events without raw XDR (unreplayable)

```sh
psql "$DATABASE_URL" -c "
  SELECT count(*)
  FROM events
  WHERE raw_topic_xdr IS NULL AND raw_value_xdr IS NULL"
```

### List open audit findings

```sh
psql "$DATABASE_URL" -c "
  SELECT id, from_ledger, to_ledger, expected_count, actual_count,
         status, attempts, last_error
  FROM audit_findings
  WHERE status IN ('open', 'unrecoverable')
  ORDER BY id DESC"
```

### Replay a range (dry run)

```sh
sorotrail replay --from-ledger 250000 --to-ledger 260000 --dry-run
```

### Replay a range (actual rewrite)

```sh
sorotrail replay --from-ledger 250000 --to-ledger 260000
```

---

## Configuration reference

Quick-reference for environment variables that affect troubleshooting.
See the README for full descriptions.

| Variable | Default | Troubleshooting note |
| --- | --- | --- |
| `LOG_LEVEL` | `info` | Set to `debug` for verbose output during investigation. |
| `POLL_INTERVAL` | `5s` | Increase if the RPC is rate-limited; decrease if you need lower latency. |
| `RPC_URL` | testnet | Point at a provider URL for mainnet. |
| `START_LEDGER` | unset | Forces cold-start from this ledger. Use to re-ingest a specific range. |
| `RETENTION_LEDGERS` | `17280` | How far back to reach on cold start (~24h). |
| `WATCHED_CONTRACTS` | empty | Empty = ingest all contract events. Comma-separated `C...` IDs to filter. |
| `AUDIT_ENABLED` | `false` | Enable the background auditor. Set to `true` for data integrity checks. |
| `AUDIT_MAX_REPAIR_ATTEMPTS` | `3` | Repair iterations before a finding becomes `unrecoverable`. |
| `AUDIT_MAX_RPS` | `10` | Total RPC request budget shared between ingest and audit. |

---

## escalation checklist

When nothing above resolves the issue:

1. Gather the output of `/health`, `/stats`, and the last 200 lines of
   logs.
2. Check `pg_stat_activity` for stuck queries or connection exhaustion.
3. Check the Stellar network status for known RPC outages.
4. If the database is corrupted, restore from backup and let SoroTrail
   re-migrate and re-ingest.
5. Open an issue on the repository with the gathered diagnostics.

## Dashboards and alerts

Dashboards are provisioned from `deploy/grafana/*.json` (versioned, not exported from a live Grafana). Start at the Overview (`/d/sorotrail-overview`), which shows the SLO indicators recorded in `deploy/prometheus/sorotrail-rules.yml`, then follow its links to a focused view:

| Symptom / alert | Dashboard | Section above |
|---|---|---|
| `IngestionLagHigh`, `IngestionErrorsBurst`, `RPCP99LatencyHigh` | Ingestion (`/d/sorotrail-ingestion`) | [Ingestion stalled](#ingestion-stalled) |
| High API latency or 5xx | Read path (`/d/sorotrail-read-path`) | [API returning stale or missing data](#api-returning-stale-or-missing-data) |
| `StoreWriteLatencyHigh`, `BackpressureActive`, `EventBatchSizeCritical` | Storage (`/d/sorotrail-storage`) | [Database connection exhaustion](#database-connection-exhaustion) |

Every alert carries a `dashboard_url` annotation and every dashboard links back to this runbook. `make check-monitoring` fails if a dashboard references a metric that is not in `deploy/prometheus/exported-metrics.txt`, reads a recording rule that does not exist, reuses a `uid`, has a panel without a "what good looks like" description, or an alert lacks `dashboard_url`.

Not covered yet: there is no per-tenant, webhook-delivery, or retention/archiver dashboard, because SoroTrail exports no per-tenant or delivery metrics to build them from.
