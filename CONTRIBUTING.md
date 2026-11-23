# Contributing to SoroTrail

Thanks for helping! SoroTrail aims to stay small, idiomatic Go with clear
seams — most features should slot in behind an existing interface.

Before contributing, please read our [Code of Conduct](CODE_OF_CONDUCT.md).
For security issues, see [SECURITY.md](SECURITY.md).

## Start here

- **New contributor?** Read [`docs/local-setup.md`](docs/local-setup.md) to
  clone, build, run, and test locally, and [`docs/architecture-overview.md`](docs/architecture-overview.md)
  for the ingester / store / api / spec seams.
- **Triaging or reviewing?** See [`docs/triage.md`](docs/triage.md) for the
  label taxonomy and workflow.

## Dev setup

1. Go 1.25+ (any Go ≥ 1.21 works too — the go toolchain auto-downloads the
   version pinned in go.mod) and Docker.
2. `docker compose up -d postgres` for a local database.
3. `make test` for the unit suite; `make test-db` runs everything including
   the Postgres integration tests (they use `TEST_DATABASE_URL` and skip
   themselves when it's unset).
4. `make cover` runs the test suite with coverage and prints a per-package
   summary; `make cover-html` opens the HTML report in your browser.
5. `make lint` (install [golangci-lint](https://golangci-lint.run/) locally).
6. `make check-monitoring` validates the Prometheus rules and the Grafana
   dashboard the way CI does (needs [promtool](https://prometheus.io/download/)
   and python3; no running Prometheus required) — run it when you touch
   anything under `deploy/`.
7. [Helm](https://helm.sh) for the chart's golden tests. Without it those
   tests skip, and the suite says so — see "Tests that skip, and how to
   notice" below.

The integration tests truncate the tables they use — point
`TEST_DATABASE_URL` at a throwaway database, not one with data you care
about. `internal/store` and `internal/replay` share those tables, so the
database suite runs with `go test -p 1` (already the case in `make test-db`
and CI); don't drop that flag.
2. Run `make help` to see every available target.
3. `make docker-up` is the quickest route to a local Postgres (starts
   Postgres and the indexer via `docker compose up -d --build`). Alternatively,
   run `docker compose up -d postgres` for a database-only service. The
   integration suite can also spin up its own ephemeral container via Docker —
   see "How the integration test layer works" below.
4. `make test` for the unit suite, race-detector enabled
   (`go test -race ./...`) — the same race checking CI runs, so a data
   race can't pass locally and fail in CI. Note that a plain `go test ./...`
   (and `make test-fast`) skips the database-backed tests rather than failing.
   The integration tests are gated behind the `integration` build tag, so it
   stays a unit-only run. `-race` requires cgo and a C toolchain; on Windows,
   install gcc (e.g. MinGW-w64) or use `make test-fast` for the plain,
   non-race run.
5. `make test-integration` runs the integration-tagged suite against a real
   Postgres — `go test -tags=integration -p 1 ./... -count=1`. This additionally
   needs Docker because the tagged suite starts an ephemeral container through
   testcontainers when `TEST_DATABASE_URL` is unset.
6. `make test-db` runs the full test suite against Postgres (requires
   `DATABASE_URL`) — `TEST_DATABASE_URL=$(DATABASE_URL) go test -p 1 ./...`.
   This target requires a live Postgres instance; without one, the tests will
   fail with a connection error rather than skipping. Kept for backwards
   compatibility with the previous workflow.
7. `make cover` / `make cover-html` for coverage.
8. `make lint` (install [golangci-lint](https://golangci-lint.run/) locally).
9. `make lint-fix` runs golangci-lint run --fix and applies the fixes in place.

The full, copy-pasteable setup and testing walkthrough lives in
[`docs/local-setup.md`](docs/local-setup.md).

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

`make test-integration` runs `go test -tags=integration -p 1 ./... -count=1`
(requires Docker running for testcontainers, or an explicit `TEST_DATABASE_URL`).
Without the tag, the run is unit-suite-only: a plain `go test ./...` skips the
database-backed tests rather than failing; `make test` adds `-race` (CI's race
checking) and `make test-fast` keeps the plain, fastest run.

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

The API's input parsers (JSON body decoder, cursors, contract IDs, ledger and
time parameters, topic filters and the event filter builder) have fuzz targets
too. They assert invariants, not just "does not panic": an accepted value
round-trips, a rejected one is an error, and the filter builder never drops or
widens a constraint. `make fuzz` runs all of them, `FUZZTIME` per target
(default `10s`; CI uses `3s` so the targets keep compiling):

```sh
make fuzz
make fuzz FUZZTIME=2m
```

Fuzzing may save reproducing inputs under the package's `testdata/fuzz`
directory. Commit any panic reproducer together with a regression test.

## Architecture

A concise tour of the four seams (ingester, store, api, spec) and how they
connect is in [`docs/architecture-overview.md`](docs/architecture-overview.md);
the full component write-up with data-flow diagrams is in
[`docs/architecture.md`](docs/architecture.md).

```
cmd/sorotrail        main: wiring + graceful shutdown
internal/config      env parsing + validation
internal/rpc         Stellar RPC JSON-RPC client (interface: rpc.Client)
internal/decode      ScVal → JSON            (interface: decode.Decoder)
internal/store       Postgres persistence    (interface: store.Store)
internal/ingester    polling loop, pagination, backoff
internal/replay      re-decode stored raw XDR (sorotrail replay)
internal/api         chi HTTP handlers
internal/spec        contract-spec enrichment (interface: spec.Enricher)
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
- **New API endpoints** — add routes in `internal/api/server.go` and document
  them in `api/openapi.yaml` (see "API specification" below). Keep endpoints
  read-only unless you also add authentication.
- **Alternative storage** — implement `store.Store`. The contract is spelled
  out on the interface; note that `QueryEvents` must return events in
  ascending ID order for cursor pagination to work.
- **RPC methods** — add to `rpc.Client` only what the ingester/API actually
  needs; the client deliberately isn't a full RPC SDK.

## API specification

`api/openapi.yaml` is the **source of truth** for the HTTP API — edit that
file, never the generated files. Two committed artifacts are derived from it:

| Generated file                 | Regenerate with | Drift test                                        |
|--------------------------------|-----------------|---------------------------------------------------|
| `internal/api/openapi.json`    | `make spec`     | `pkg/docs.TestSpecCopiesAreIdentical`             |
| `pkg/client/client.gen.go`     | `make client`   | `pkg/client.TestGeneratedClientIsUpToDate`        |

`internal/api/openapi.json` is the copy `internal/api` embeds and serves at
`/openapi.json`; a spec edit that skips `make spec` ships documentation nobody
sees. `make spec-check` regenerates the JSON into a temporary file and diffs it
against the committed copy, and CI runs it, so a stale copy fails the build
rather than drifting silently. After any change to `api/openapi.yaml`, run
`make spec` (and `make client` if you touched operations or schemas).

## Conventions

- When a pull request introduces user-visible or behavioral changes, contributors must append a concise line-item summary to the root [CHANGELOG.md](CHANGELOG.md) under the `[Unreleased]` section.
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

The golangci-lint pin in `.golangci-lint-version` is the exception. Dependabot
keeps the `golangci/golangci-lint-action` **action** current but does not touch
its `version:` input — which now comes from that file — so a human must bump the
pin, and the Go version recorded beside it, whenever the Go version moves. See
[Lint toolchain drift](#lint-toolchain-drift).

## Verification & Automated Checks

Before submitting a pull request, run `make ci` — it reproduces the CI gate
locally (build, `go vet` with and without the `integration` tag, the OpenAPI
drift check, the migration numbering gate, the tagged and untagged test suites,
the benchmark smoke run, and `golangci-lint`) and
stops at the first failing step, just like CI. The database-backed tests skip
gracefully when `TEST_DATABASE_URL` (or Docker) is unavailable, so `make ci`
passes without Postgres and runs the full gate once one is available. Every
target is documented; run `make help` to list them all.

`make test-ci` is the exact command the CI test job runs
(`go test -p 1 ./... -count=1 -race -timeout=120s`); `make test` runs the
same race detector with less ceremony, and `make test-fast` is the plain
non-race run.

For individual checks, run each step on its own:

```bash
make build-all        # go build ./...
make vet              # go vet ./...
make test-ci          # CI test job's exact command; DB-backed tests skip without TEST_DATABASE_URL
make test-integration # integration-tagged suite against a real Postgres (requires Docker or TEST_DATABASE_URL)
make test-db          # full test suite against Postgres (requires live DATABASE_URL)
make bench-ci         # benchmark smoke run
make lint             # golangci-lint run, guarded against toolchain drift
make migrations-check # migration numbering is loadable; needs no database
```

### Tests that skip, and how to notice

Some tests need an external dependency the contributor may not have: the Helm
chart's golden tests need the `helm` binary, and the store's conformance suite
needs a Postgres. Those tests **skip** rather than fail — failing would break
every contributor without the dependency — but a skip that prints nothing is
worse than no test at all, because a green run then says nothing about the
coverage it did not exercise.

So the skip is loud. Every test target (`make test`, `make test-fast`,
`make test-ci`, `make test-db`, `make test-integration`, `make cover*`, and
therefore `make ci`) first runs `make test-prereqs`, which prints a banner
naming what was skipped, why, what the skip cost, and how to get the
dependency. The same convention applies to the store's database-backed tests,
so there is one wording for the whole suite.

A raw `go test ./...` still only shows the banner under `-v`: Go discards the
output of a package that passes, so nothing a test prints can reach a
non-verbose run. Use the make targets, or `go test -v`, when you want to see
it.

**A green local run does not cover the Helm chart unless `helm` is
installed.** The chart's golden tests compare the rendered manifests byte for
byte against the committed goldens, and `TestRender_ConfigValuesReachThePod`
checks that every chart config value reaches the pod. Neither runs without
`helm`.

**Adding a configuration variable requires the chart to be updated in the same
pull request.** The guard that catches a chart value which never reaches the
pod is `TestRender_ConfigValuesReachThePod` in
[`deploy/helm/sorotrail/render_test.go`](deploy/helm/sorotrail/render_test.go):
it fails when a value under the chart's `config:` block is not wired into the
ConfigMap, and when the ConfigMap renders a variable the application does not
read. A new variable therefore needs, in the same pull request:

- an entry in `internal/config/config.go` (the application reads it),
- an entry in `deploy/helm/sorotrail/values.yaml` and
  `deploy/helm/sorotrail/values.schema.json` (the operator can set it),
- a line in `deploy/helm/sorotrail/templates/configmap.yaml` (the pod gets it),
- a row in the chart's `README.md` values table, and
- the regenerated goldens:
  `go test ./deploy/helm/sorotrail -run TestRenderGolden -update`.

CI runs the chart's golden tests in the `helm` job with
`SOROTRAIL_REQUIRE_HELM=1`, which turns the skip into a failure: a runner
that lost `helm` fails the job instead of skipping the coverage and
reporting success. Set the same variable locally to make a missing
dependency fail rather than skip.

### Lint toolchain drift

`golangci-lint` is compiled with a single Go version and refuses to run when the
repository targets a newer one:

```text
the Go language version (go1.26) used to build golangci-lint is lower than the targeted Go version (1.27.1)
```

The "targeted" version is go.mod's `toolchain` directive when it has one and its
`go` directive otherwise. A `toolchain` line is easy to pick up by accident: any
`go get` or `go mod tidy` run under a newer local toolchain can add one. **Do not
commit an unintended `toolchain` line** — if `git diff go.mod` shows one you did
not mean to add, remove it. Otherwise the lint job fails while `make lint` stays
green locally, because only CI runs the pinned binary.

The pinned release lives in [`.golangci-lint-version`](../.golangci-lint-version)
as `<version> <go-built-with>`; both the CI action and the guard read it, so they
cannot disagree about which release runs. `make lint` runs
`scripts/check_lint_toolchain.sh` first and fails with the fix spelled out when
go.mod targets a newer Go than that release supports, so a `toolchain` bump is
caught before you push rather than in CI.

To move the project to a newer Go on purpose, bump all three together: the `go`
(and `toolchain`) directives in go.mod, and the version plus Go field in
`.golangci-lint-version` to a golangci-lint release built with that Go. A
release's build Go is the wording after `built with` in `golangci-lint version`.
Then run `make ci`.

## Migration safety

Migrations are the riskiest change applied to a running database. The CI gate
runs several static checks without a Postgres connection so a broken migration
is caught at build time:

- **Version uniqueness** — no two .up.sql files share a version number
  (`TestMigrate_VersionNumbersAreUnique`). CI runs this one on its own, in the
  `migrations` job, before the suites that a collision would break; the failure
  names both colliding files. Reproduce it locally with
  `make migrations-check`, which needs no database.
- **Paired up/down** — every .up.sql has a matching .down.sql and vice versa
  (`TestMigrate_FilesAreWellFormed`).
- **No drop-after-create** — a migration cannot drop a table that a later
  migration depends on (`TestMigrate_NoUpDropsTableDependency`).
- **No rebuild-after-alter** — a migration cannot rebuild a table (rename +
  create) that a later migration alters, because the rebuild silently discards
  columns added before it (`TestMigrate_NoUpRebuildsTableDependency`).

The integration suite (`go test -tags=integration`) exercises the full
up-from-empty → down-to-zero → up-again round-trip against a real Postgres
(`TestMigrate_UpDownUpRoundTrip`), and verifies that each down migration
reverses its up exactly (`TestMigrate_EachDownReversesItsUp`).

### Picking the next migration version number

A schema change is a pair of files in `internal/store/migrations/` sharing one
four-digit version: `0028_some_change.up.sql` and `0028_some_change.down.sql`.
Take the next free number from the directory listing rather than from the
highest number you remember:

```bash
ls internal/store/migrations/*.up.sql | sort | tail -3
```

The series must be strictly loadable: golang-migrate refuses to open an
embedded directory in which two `.up.sql` files carry the same version, and
that failure is not confined to your migration — every database-backed test
reports the same load error, so the collision looks like a broken suite rather
than a broken numbering.

The realistic way to hit it is a merge, not an authoring mistake: two branches
each take "the next number" from the same base, both look correct on their own,
and both pass CI on their own branch. Neither is wrong until they are combined.
So re-run `make migrations-check` after you merge or rebase `main` into your
branch — not only when you add the file — and if your number is already taken
on `main`, renumber **your own** files to the next free number. Never renumber
or edit a migration that has been applied anywhere, including on a shared
development database; add a forward migration instead.

`migrations-check` covers the Postgres series. SQLite keeps its own numbered
series in `internal/store/migrations/sqlite/`, so adding a Postgres migration
does not consume a SQLite version, and a SQLite migration does not need a
number that is free in the Postgres directory.

Run `sorotrail migrate-status` to report pending migrations without applying
them — useful for pre-deploy checks.

### Dirty version recovery

If a migration partially applies and then fails, the database enters a
dirty state (the `dirty` flag in `schema_migrations` is set to `1`).
Recovery steps:

1. Inspect the current state:
   ```sql
   SELECT version, dirty FROM schema_migrations;
   ```
2. If the version number is correct but dirty, mark it clean:
   ```sql
   UPDATE schema_migrations SET dirty = false;
   ```
3. If the version number is wrong, set it to the correct value:
   ```sql
   UPDATE schema_migrations SET version = <N>, dirty = false;
   ```
4. Re-run `sorotrail` to verify it starts cleanly.

Never manually modify migration SQL files after they have been applied to
any environment. If a migration is broken, write a new forward migration
to fix it.

## Pull requests

- `make ci` must pass before pushing.
- Touching the events table? Update the column list in the migration test
  (`TestMigrations_ApplyFromEmptyLand`) — it's what catches drift
  between SQL and Go.
- Include tests for behavior changes; add a new integration test for any
  public API or schema change.
- Update the README's API reference and config table when you touch either.
- Touching `api/openapi.yaml`? Run `make spec` and `make client` so the
  generated copies stay in step (see "API specification" above).
- Include `Closes #[issue_id]` and summarize fuzz findings, including when no
  panics were found.
