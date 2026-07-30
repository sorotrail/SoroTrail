# Contributing to SoroTrail

Thanks for helping! SoroTrail aims to stay small, idiomatic Go with clear
seams — most features should slot in behind an existing interface.

## Dev setup

1. Go 1.25+ (any Go ≥ 1.21 works too — the go toolchain auto-downloads the
   version pinned in go.mod) and Docker.
2. `docker compose up -d postgres` for a local database (the integration
   suite can also spin up its own ephemeral container — see "How the
   integration test layer works" below).
3. `make test` for the unit suite; it stays fast (under ~10s) because
   the integration tests are gated behind the `integration` build tag.
4. `make test-integration` runs the integration suite against a real
   Postgres — `go test -tags=integration -p 1 ./... -count=1`.
5. `make test-db` runs everything, including integration tests, against
   whatever `TEST_DATABASE_URL` points at — kept for backwards
   compatibility with the previous workflow.
6. `make cover` / `make cover-html` for coverage.
7. `make lint` (install [golangci-lint](https://golangci-lint.run/) locally).

### How the integration test layer works (issue #9)

Per the issue, four coverage areas are required. They each have an
explicit assertion against a real Postgres:

| Area                                    | Test file                                       |
|-----------------------------------------|-------------------------------------------------|
| Migrations up from empty                | `internal/store/store_integration_test.go`       |
| Event upsert idempotency                | `internal/store/events_integration_test.go`      |
| Ingestion_state save/resume (cursor)    | `internal/ingester/integration_test.go`          |
| GET /events filter combinations         | `internal/api/api_integration_test.go`           |

The shared helper is `internal/testdb/testdb.go` (`//go:build integration`).
Its `Setup(t, migrate)` returns a migrated `*pgxpool.Pool`. The migrate
argument is supplied by each caller (`store.Migrate` for store-domain
tests, `nil` only when the test package already provides its own
migration step) — function injection so the helper itself does not
import `internal/store`, which would create an import cycle across
test packages.

Database resolution, in order:

- `TEST_DATABASE_URL` set → `setupShared`: migrate then TRUNCATE the
  shared DB. Tests across packages share the DB; `go test -p 1`
  serializes them (the default in `make test-integration`). **Never**
  point `TEST_DATABASE_URL` at a database you care about — the
  helper truncates `events`, `ingestion_state`, `watched_contracts`,
  `replay_state` and the audit tables between tests.
- `TEST_DATABASE_URL` unset → `setupContainer`: spin up an ephemeral
  Postgres 16-alpine via [testcontainers-go](https://golang.testcontainers.org/).
  Each test gets its own isolated container; truncation is unnecessary
  because nothing is shared.
- Neither path is available → the helper `t.Skip`s with a message
  that points back here, so an integration run never fails loud for
  missing infra.

`make test-integration` runs `go test -tags=integration -p 1 ./... -count=1`.
Without the tag, `go test ./...` is unit-suite-only and stays fast
(under ~10s).

## Fuzz testing

The decoder fuzz targets run automatically on pull requests with a 30-second
budget per target. To run the short local versions:

```sh
go test ./internal/decode -run '^$' -fuzz FuzzDecodeScVal -fuzztime 30s
go test ./internal/decode -run '^$' -fuzz FuzzDecodeTopicArray -fuzztime 30s
```

For a longer local session, increase `-fuzztime`, for example:

```sh
go test ./internal/decode -run '^$' -fuzz FuzzDecodeScVal -fuzztime 30m
go test ./internal/decode -run '^$' -fuzz FuzzDecodeTopicArray -fuzztime 30m
```

Fuzzing may save reproducing inputs under the package's `testdata/fuzz`
directory. Commit any panic reproducer together with a regression test.

## Architecture

```
cmd/sorotrail        main: wiring + graceful shutdown
internal/config      env parsing + validation
internal/rpc         Stellar RPC JSON-RPC client (interface: rpc.Client)
internal/decode      ScVal → JSON            (interface: decode.Decoder)
internal/store       Postgres persistence    (interface: store.Store)
internal/ingester    polling loop, pagination, backoff
internal/replay      re-decode stored raw XDR (sorotrail replay)
internal/api         chi HTTP handlers
internal/testdb      //go:build integration helper shared by tests
```

The ingester, replay and API depend only on the listed interfaces —
never on concrete implementations — so each layer is independently
testable and replaceable.

### Extension points

- **Richer ScVal decoding** — implement `decode.Decoder` or extend the switch
  in `internal/decode/xdr.go` (marked with a `contributors:` comment).
  Unknown ScVal types intentionally fall back to a lossless
  `{"unknown": {"type": ..., "base64": ...}}` wrapper instead of erroring, so
  ingestion never stalls; keep that property.
- **Per-standard decoders** (SEP-41 token events, etc.) — build on top of the
  stored JSON or as a decorator around `decode.Decoder`; don't widen the core
  interface. When your decoder writes a derived table, wire it into replay so
  it can be backfilled: add a field to `store.ReplayBatch` and write it in
  `store.CommitReplayBatch` after `events`. See [docs/replay.md](docs/replay.md).
- **Changing decoder output** — any change to what a decoder emits should
  come with a note in the PR that operators need to run
  `sorotrail replay --from-ledger N`, otherwise the change only applies to
  events ingested from then on.
- **New API endpoints** — add routes in `internal/api/server.go`. Keep
  endpoints read-only unless you also add authentication.
- **Alternative storage** — implement `store.Store`. The contract is spelled
  out on the interface; note that `QueryEvents` must return events in
  ascending ID order for cursor pagination to work.
- **RPC methods** — add to `rpc.Client` only what the ingester/API actually
  needs; the client deliberately isn't a full RPC SDK.

## Conventions

- Plain SQL via pgx; no ORM. Schema changes are new numbered migration pairs
  in `internal/store/migrations/` — never edit an applied migration.
- `log/slog` for logging; pass loggers explicitly, no globals.
- Tests use testify. RPC/store behavior is tested through the interfaces with
  hand-written mocks (see `internal/ingester/mocks_test.go`).
- Integration tests carry `//go:build integration` and use the shared
  `internal/testdb` helper so `go test ./...` stays a fast unit-only run.
- Keep functions small and packages focused. When in doubt, match the
  surrounding code.

## Dependency management

Dependency updates are handled by Dependabot, which opens grouped weekly
PRs for Go modules, GitHub Actions, and the Docker base image. PRs with minor
or patch bumps are grouped together to keep the review stream manageable;
major version bumps come individually. The `vulncheck` CI job runs
`govulncheck ./...` and fails if any reachable vulnerability is found, so
known-vulnerable code paths are surfaced before they ship. Review dependency
PRs promptly — a green check on `vulncheck` is a good signal that the bump can
be merged without deep audit.

## Verification & Automated Checks

Before submitting a pull request, please run the following commands locally:

```bash
go build ./...
make test-db   # Requires local Postgres instance
make lint

## Pull requests

- `go build ./...`, `make test`, `make test-integration` and `make lint`
  must pass.
- Touching the events table? Update the column list in the migration test
  (`TestMigrations_ApplyFromEmptyLand`) — it's what catches drift
  between SQL and Go.
- Include tests for behavior changes; add a new integration test for any
  public API or schema change.
- Update the README's API reference and config table when you touch either.
- Include `Closes #[issue_id]` and summarize fuzz findings, including when no
  panics were found.
