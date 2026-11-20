# SoroTrail: load-test harness entry point, spec override history, one JSON decoder, cross-replica spec invalidation

Closes #1163
Closes #1164
Closes #1166
Closes #1167

Four independent issues, one PR, one branch (`feat/issues-1163-1164-1166-1167`).
Each lands as its own commit so it can be read, reviewed, and reverted on its
own:

| Commit | Issue | Summary |
| --- | --- | --- |
| `refactor(api): decode every JSON request body the same way` | #1166 | One shared request-body decoder |
| `feat(cmd): add a loadtest subcommand for the ingester harness` | #1163 | `sorotrail loadtest` |
| `feat(api): record and revert contract spec override history` | #1164 | Override history and revert |
| `feat(spec): fan out spec cache invalidations through Postgres` | #1167 | Cross-replica cache invalidation |

The only schema change is one new table (migration `0029`, plus the matching
SQLite series entry). No existing endpoint changes shape, no configuration
variable is added or removed, and no environment variable's meaning changes.

---

## 1. `sorotrail loadtest` — an entry point for the ingester harness (#1163)

**Problem.** `internal/ingester/loadtest.go` was 182 lines implementing a load
test: `RunLoadTest`, a `SyntheticRPC` that serves generated events, and the
result arithmetic. Nothing called it. There was no subcommand, no test, no
Make target and no mention in `docs/performance.md`, so it had never been run
by anyone who did not write it — and it would rot until it no longer compiled
in a useful way.

**Change.**

- **`cmd/sorotrail/loadtest.go`** — a new `sorotrail loadtest` subcommand
  matching the existing CLI structure and flag style. It opens the store
  named by `--db` (Postgres, SQLite, or ClickHouse), drives the harness
  against it, and prints a short human summary followed by a machine-readable
  JSON report. Flags: `--db`, `--events-per-ledger`, `--ledgers`,
  `--page-limit`, `--duration`, `--batch-size`, `--write-batch-size`,
  `--force`.

  ```bash
  # Run a 30-second smoke load test against a local database
  sorotrail loadtest --db="$DATABASE_URL" --events-per-ledger=100 --ledgers=1000 --duration=30s
  ```

- **It refuses to run against a database that already holds data** unless
  `--force` is passed: the run writes a lot of synthetic rows, and silently
  mixing them into a real dataset would be unrecoverable.
- **The harness measures the real path.** The command calls
  `ingester.RunLoadTest`, which builds a real `ingester.New` over the
  synthetic RPC — poll, decode, upsert, persist — rather than reaching into
  the ingester's internals.
- **`internal/ingester/loadtest.go`** — the harness is extended so it can
  report what a load test is for. `LoadTestConfig` gains `Seed` (the same
  seed produces the same events), `BatchSize` and `WriteBatchSize`;
  `LoadTestReport` gains `WriteLatencyP50`, `WriteLatencyP99` and
  `PeakMemoryBytes`. `RunLoadTest` records the duration of every cycle and
  samples `runtime.Alloc`, and `SyntheticRPC` now draws its event values from
  a seeded PRNG instead of a fixed constant.
- **`docs/performance.md`** — documents how to run the command, every flag,
  the JSON report's fields, and records a **baseline from a stated machine**
  (i7-8565U / 16 GB / Postgres 16: events/s, p50/p99 write latency, peak
  memory) so a future change has something to compare against.
- **`Makefile`** — a `make loadtest` target that runs a 15-second smoke run
  against `DATABASE_URL`. It is deliberately *not* part of the default CI
  gate, because it needs a database; `make ci` is unchanged.

**Tests.** `internal/ingester/loadtest_test.go` (new): `SyntheticRPC` is
deterministic from a seed (same seed → identical events; different seed →
different events) and paginates stably; the result arithmetic (`maxInt64`,
`maxFloat`, the events-per-second rate including the zero-duration guard) is
covered by a table; `RunLoadTest` runs to completion and stops on a context
deadline.

---

## 2. One request-body decoder for the whole API (#1166)

**Problem.** `internal/api` had two functions that decode a JSON request body,
with different behaviour:

|  | `decodeJSONBody` (handlers.go) | `decodeJSON` (handlers_tenant.go) |
| --- | --- | --- |
| Size limit | package constant `maxJSONBodyBytes` | `s.httpRequestBodyLimit` argument |
| Oversized body | error, caller decides status | 400 invalid JSON body |
| Writes the response | no | yes |
| Trailing data after the value | rejected | accepted |
| Partial decode on error | prevented | can leave `dst` half-filled |
| Empty body | "request body is empty" | EOF wrapped as invalid JSON |

So the same malformed request got a different status and message depending
on which handler received it, and the tenant handlers accepted a body with
trailing garbage that the others rejected. `decodeJSON` also carried its doc
comment twice.

**Change.**

- **`internal/api/handlers.go`** — one shared `Server.decodeJSONBody` that
  every body-reading handler uses. It enforces the **configured**
  `httpRequestBodyLimit`, reports an oversized body as **413 Request Entity
  Too Large**, and reports malformed JSON, unknown fields, trailing data and
  an empty body as **400**. A failed decode never publishes a half-populated
  value and never echoes the body's values in the error.
- **`internal/api/handlers_tenant.go`** — `decodeJSON` is **deleted**; the
  five tenant handlers use the shared decoder.
- **`internal/api/handlers_subscription.go`** — the two subscription handlers
  went through `json.NewDecoder` directly (no limit, no unknown-field
  rejection); they now use the shared decoder too.
- **`internal/api/handlers_spec_override.go`** — the spec-override PUT keeps
  its wrapped/bare double shape (a separate bug, tracked separately) but reads
  the body once through the shared `readJSONBody`.
- **`api/openapi.yaml` / `internal/api/openapi.json`** — every
  body-accepting operation now documents **413**.
- **`docs/errors.md`** and the error catalogue are regenerated, so the code,
  the docs and the spec cannot drift.

**Behaviour changes for existing clients** (as the issue asks to be listed):

1. **Tenant routes now reject trailing data.** `POST /admin/tenants`,
   `PATCH /admin/tenants/{id}`, `POST /admin/tenants/{id}/grants`,
   `POST /admin/tenants/{id}/keys` and `POST /tenant/watch` used to accept
   `{"name":"x"} garbage`; they now answer 400.
2. **413 instead of 400 for an oversized body** on every body-accepting
   route.
3. **Subscription routes now reject unknown fields** and trailing data, and
   enforce the request-body limit, which they previously ignored.
4. The body limit for the watched-contracts and spec-override routes moves
   from a hard 4 KiB package constant to the configured
   `HTTP_REQUEST_BODY_LIMIT` (default 1 MiB), so a larger legitimate body is
   no longer rejected.

**Tests.** `TestDecodeJSONBody` is a table over the decoder covering every
case (valid, whitespace, exactly at the limit, empty, whitespace-only, nil
body, one byte over, far over, malformed, truncated, not an object, wrongly
typed field, unknown field, trailing garbage, second JSON value, stray brace)
and asserts the status, the error envelope, that the body's values are never
echoed, and that `dst` is untouched on failure.
`TestDecodeJSONBody_WiredToEveryHandlerFamily` drives the real router per
handler family (watched-contracts, spec override, subscriptions) and asserts
the shared 400/413.

---

## 3. A history of contract spec overrides (#1164)

**Problem.** `PUT /contracts/{id}/spec` replaces a contract's spec override,
and the comment above the handlers says why that matters: *"a spec override
silently changes how events decode for that contract"*. Yet the store kept
only the current value — no record of what the previous spec was, when it
changed, or which key changed it.

**Change.**

- **A new `contract_spec_override_history` table** (migration `0029`, plus
  the matching SQLite series entry `0007`) holds one row per change: the
  contract ID, the spec that was in force, the public API key prefix that
  made the change, and when. A delete leaves a **tombstone** (no spec), so
  "the override disappeared" is answerable too.
- **The history row is written in the same transaction as the override
  write**, so the two can never disagree. `SetContractSpecOverrideWithHistory`
  and `DeleteContractSpecOverrideWithHistory` do both in one transaction on
  Postgres and SQLite.
- **`GET /contracts/{id}/spec/history`** lists the versions **newest first**
  with **cursor pagination** over the version id.
- **`POST /contracts/{id}/spec/revert`** restores a previous version (named
  by the id in the request body) and **records the revert as a new entry**,
  so the history stays append-only and keeps the whole sequence, including
  the way back.
- **The current-override read path is unchanged in cost**: the history is
  never consulted when decoding an event, and the current override is still a
  single-row read.
- **Retention policy: all history rows are retained**, with no automatic
  pruning. The policy is *documented* rather than enforced so the read path
  never pays for retention sweeps; the table is indexed on
  `(contract_id, id DESC)` so the listing walks it directly.
- **Backends.** Postgres and SQLite record the history. ClickHouse **refuses
  explicitly** with `ErrUnsupported` (the API answers 501) rather than
  reporting an empty history — the backend has no override table to begin
  with, and the history's rows are written in the same transaction as the
  override itself.
- **The routes sit behind the same `apiKeyAuth`** as the existing override
  routes: a spec override silently changes how events decode, so its history
  is a management surface too.
- **`api/openapi.yaml` / `internal/api/openapi.json`** and the generated
  `pkg/client` are updated; `docs/spec-overrides.md` documents the new
  endpoints, the retention policy and the backend split.

**Tests.** `internal/api/handlers_spec_override_history_test.go` (new): a PUT
appends an entry carrying the spec, the key prefix and a timestamp; a DELETE
appends a tombstone; the listing is newest first; the cursor pages without
repeating or skipping an entry; a contract that never had an override lists
an empty page; the revert restores the previous spec *and* records itself; an
unknown version is a 404 and writes nothing; a missing or non-positive
version is a 400; the new routes are gated. `internal/store/spec_override_history_test.go`
(new) exercises the same behaviour against a real store (SQLite, so it runs
in the unit suite) including the tombstone, the per-contract isolation and
the cursor round-trip.

---

## 4. Cross-replica spec cache invalidation (#1167)

**Problem.** The spec cache (`internal/spec/cache.go`) is per-process, so
`Invalidate` and `InvalidateContract` evicted entries only from the process
that called them. With more than one API replica, an override written through
replica A was not seen by replica B until B's own entry expired
(`DefaultSpecTTL`, 10 minutes) — so the same request returned differently
decoded events depending on which replica served it. A contract upgrade
detected by the ingester invalidated only its own cache.

**Change.**

- **A Postgres channel, `sorotrail_spec_invalidation`.** Writes that change
  what a contract decodes to publish the contract ID on it:
  - an override put, delete or revert (the API handlers), and
  - a **detected wasm-hash change** (the spec cache notices the contract
    upgraded and publishes it).
- **Every process subscribes on startup** and evicts the named contract's
  entries — the fetched spec *and* the `override:<contract>` keyed entry.
- **The listener reconnects** after a dropped connection with an exponential
  backoff, and **clears the whole cache after every successful
  (re)subscription**: a reconnect may have missed notifications, so the only
  safe state is to hold nothing rather than to hold something that might be
  stale.
- **A dedicated connection is used for `LISTEN`**, not one from the pool: a
  `LISTEN` blocks its connection until a notification arrives, so borrowing a
  pooled connection would hold it for the life of the process.
- **Single-process and SQLite deployments keep working without the
  listener**: no publisher is wired, so the local eviction is the whole
  story.
- **A metric counts the invalidations received**
  (`sorotrail_spec_invalidations_received_total`), and each one is logged at
  debug level.
- The local half of the problem (an override write not invalidating the
  writing process's own cache — issue #1142) is fixed too, and the revert
  goes through the same invalidation.

**Tests.** `internal/spec/invalidation_test.go` (new): a detected contract
upgrade is published; a cache with no publisher does not panic;
`ApplyRemoteInvalidation` evicts the fetched spec *and* the override and
counts the invalidation; `Clear` empties the cache.
`internal/spec/invalidation_integration_test.go` (new, `integration` tag):
**two caches against one database**, where an override written through one
replica is reflected in the other within a bounded time; and a second test
proves the reconnect path clears the cache. Both run against a real
Postgres.

---

## Verification

Everything below was run from the repository root.

```
make ci                     # the full gate, in order: build, vet, spec-check,
                            # migrations-check, tests, bench, the
                            # integration-tagged build and tests, and lint
                            # — exit 0
```

Individually:

```
go build ./...                                  # OK
go vet ./...                                    # OK
go vet -tags=integration ./...                  # OK
go test ./... -count=1                          # all packages pass
golangci-lint run                               # 0 issues
go run ./cmd/specgen -out /tmp/spec.json && diff internal/api/openapi.json /tmp/spec.json
                                                # no diff (the committed spec is current)
go run ./cmd/clientgen                          # the generated client matches the spec
python3 scripts/validate_monitoring.py          # all monitoring asset checks pass
```

**Database-backed tests** were run against a real Postgres 16 (Docker
testcontainers), not just skipped:

```
make test-integration                           # all packages pass, including
                                                # the two new cross-replica tests
```

The migration was verified end-to-end: `make test-integration` applies
migration `0029` to an empty database and the whole suite passes against
the resulting schema, and the SQLite series entry is exercised by the
SQLite-backed history test.

### How each issue's behaviour is covered

| Issue | Test that fails if the behaviour regresses |
| --- | --- |
| #1163 | `internal/ingester/loadtest_test.go` — `TestSyntheticRPC_DeterministicFromSeed`, `TestSyntheticRPC_PaginationIsStable`, `TestLoadTestArithmetic`, `TestRunLoadTest_RunsToCompletion`, `TestRunLoadTest_StopsOnDuration` |
| #1164 | `internal/api/handlers_spec_override_history_test.go` (append, tombstone, order, cursor, empty, revert, 404, 400, gating) and `internal/store/spec_override_history_test.go` (same behaviour against a real store) |
| #1166 | `internal/api/handlers_test.go` — `TestDecodeJSONBody` (table over every decoder case) and `TestDecodeJSONBody_WiredToEveryHandlerFamily` (per handler family, through the real router) |
| #1167 | `internal/spec/invalidation_test.go` (publish, evict, count, clear) and `internal/spec/invalidation_integration_test.go` (two caches, one database, bounded time) |

---

## Notes for the reviewer

- The error catalogue (`internal/api/error_catalogue_test.go`) is
  hand-maintained and checked against the source *and* the shipped OpenAPI
  spec, so the new handlers' error conditions are catalogued and the spec
  documents every status they can return. `docs/errors.md` is generated from
  that catalogue and was regenerated.
- `api/openapi.json` is generated from `api/openapi.yaml` by
  `cmd/specgen`, and `pkg/client/client.gen.go` by `cmd/clientgen`; both
  were regenerated (`make spec`, `make client`) rather than hand-edited.
- The `make loadtest` target is intentionally **not** wired into `make ci`:
  it needs a database, and the CI gate has no Postgres in the unit job. The
  command itself is covered by unit tests, and `make test-integration`
  covers the store paths.
- The root `sorotrail` binary is tracked in this repository (an upstream
  oversight — `.gitignore` lists `/sorotrail`). It is **not** modified by
  this PR.
