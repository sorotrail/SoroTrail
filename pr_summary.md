# SoroTrail: ingester cursor discipline, per-cycle summary, decode cache, migrate subcommand

Closes #263
Closes #265
Closes #285
Closes #288

Four medium tasks, scoped to the `ingester`, `decoder`, and `cli` areas. No
breaking changes to existing endpoints, configuration, or database schema.

---

## 1. Handle empty `getEvents` pages correctly (ingester)

**Problem.** `nextState` advanced the persisted pagination cursor whenever the
RPC response carried a cursor, *including on a zero-event page*. Resuming from a
cursor the RPC has already exhausted can wedge the ingester on the same empty
page. A half-finished version of this change also introduced two defects:

- `sweepBatch` dereferenced the per-cycle budget unconditionally, so the
  default (uncapped) deployment panicked with a nil pointer on every window
  sweep.
- `singlePage` carried a no-op `if` block and a misleading comment while a
  regression test asserted behavior the code did not have.

**Change.**

- `nextState` now advances the cursor **only when the page carried events**.
  With events it keeps `resp.Cursor` and falls back to the last event's
  `CursorValue()`; on an empty page it returns no cursor and reports caught up,
  so `resolvePosition` resumes from the ledger position (`LastIngestedLedger+1`)
  on the next cycle. The frontier still advances to `LatestLedger-1` so a
  caught-up loop does not re-scan the same window.
- Restored the nil guard on the per-cycle budget in `sweepBatch` (fixes the
  panic) and removed the dead cold-start block in `singlePage`, replacing it
  with a documented guard for information-free responses.
- Removed a now-unused parameter from `persistEvents`.

**Tests.**

- `TestPagination_EmptyPageDoesNotAdvanceCursor` (rewritten regression test): an
  empty page that *does* carry a cursor must not persist it, and the next cycle
  must resume by ledger, not by reusing that cursor.
- `TestNextState_CursorOnlyAdvancesOnProgress`: table-driven coverage of the
  response-cursor path, the `CursorValue()` fallback, and both empty-page shapes.

## 2. Log a structured summary per poll cycle (ingester)

**Problem.** Progress was only visible as a per-batch `ingested events` line, so
a multi-batch window sweep produced several lines and an empty cycle produced
none.

**Change.**

- Added a per-cycle `cycleStats` accumulator (atomic, since window-sweep batches
  run concurrently) tracking **fetched / written / skipped**.
- `runOnce` now emits exactly one structured `poll cycle complete` line per
  cycle via a deferred call, so it fires on every exit path — success, empty
  page, decode skip, or error (tagged with `error`). Extra fields: `caught_up`
  and `duration`.
- Removed the superseded per-batch `ingested events` line.

**Tests.**

- `TestRunOnce_LogsCycleSummary` (table-driven): a normal page, an empty page,
  and an event routed to the dead-letter sink (fetched 2 / written 1 /
  skipped 1).
- `TestRunOnce_LogsOneCycleSummaryForWindowSweep`: a two-batch sweep still emits
  exactly one aggregated line.
- `TestRunOnce_CycleSummaryOnError`: a failing cycle still emits one line,
  tagged with the error.

## 3. Cache decoded results by raw-XDR hash (decoder)

**Change.** Added `decode.CachingDecoder`, an LRU wrapper around any `Decoder`:

- Memoizes `DecodeScVal` output keyed by an FNV-1a hash of the base64 XDR.
- Stores the original key alongside the value and re-checks it on every hit, so
  a hash collision degrades to a miss instead of returning wrong JSON.
- Bounded (`decode.DefaultCacheSize` = 4096), evicts least-recently-used, is
  safe for concurrent use, exposes `Stats()`/`Len()`, and returns copies of the
  cached bytes so callers cannot poison the cache.
- Errors are never cached, so a transient or fixed decode failure is retried.

Wired into the ingester in `cmd/sorotrail/main.go`; replay/backfill keep using
`XDRDecoder` directly (opt-in).

**Tests.** `internal/decode/cache_test.go`: hit/miss accounting, distinct
payloads, errors not cached, nil inner decoder, LRU eviction, default capacity,
returned-bytes isolation, a 64-goroutine concurrency test, and a round-trip
through the production `XDRDecoder`.

## 4. Add `sorotrail migrate (up/down/status)` (cli)

**Change.**

- `internal/store/migrate.go`: `MigrateUp(url, steps)`, `MigrateDown(url, steps)`
  and `MigrateStatus(url)`, all wrapping golang-migrate. `Migrate(url)` is kept
  as the startup alias for `MigrateUp(url, 0)`, so `main`, `replay`, `backfill`
  and `index-addresses` are unchanged. The SQLite series now supports stepping
  up, rolling back, and status (not just apply-all).
- `MigrationStatus` (existing type) is now produced for both Postgres and
  SQLite; the previously unusable `GetMigrationStatus` delegates to it.
- `cmd/sorotrail/migrate.go`: `sorotrail migrate <up|down|status> [--steps N]`.
  `up` defaults to all pending, `down` defaults to a conservative single step
  (`--steps 0` rolls back everything), and `status` prints version, dirty flag,
  and pending versions. Added to dispatch and `sorotrail help`.

**Tests.**

- `internal/store/migrate_sqlite_test.go`: status before/after up, idempotent
  up, single-step and full rollback, legacy `Migrate`/`GetMigrationStatus`
  agreement, a real store usable after up, unsupported scheme, and ClickHouse
  no-op.
- `cmd/sorotrail/migrate_test.go`: table-driven argument handling (usage errors,
  unknown action, `--help` short-circuits, negative `--steps`, stray positional)
  and dispatch routing.

---

## Verification

```
go build ./...          # OK
go vet ./...            # OK
golangci-lint run       # 0 issues
go test ./...           # all packages pass
go test -race ./internal/ingester/... ./internal/decode/... ./internal/store/   # clean
```

## Notes

- No schema, config, or endpoint changes.
- The branch is kept in sync with upstream `main`. Upstream added read-only
  `sorotrail migrate-status`; `sorotrail migrate status` complements it by
  sharing the same `store.MigrateStatus` result while adding `up`/`down`.
- ClickHouse URLs have an apply-only migration series, so `migrate up` runs it
  while `migrate down`/`status` report that it is unsupported.
- The `Makefile` `migrate-up`/`migrate-down` targets still shell out to the
  `migrate` binary; they are left untouched to avoid changing existing
  workflows. They could be repointed at `sorotrail migrate` in a follow-up.
- `deploy/helm/sorotrail` golden tests can differ locally with a mismatched
  local Helm version; the CI `helm` job is unaffected.

  ...
