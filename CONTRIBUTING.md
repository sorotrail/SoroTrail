# Contributing to SoroTrail

Thanks for helping! SoroTrail aims to stay small, idiomatic Go with clear
seams — most features should slot in behind an existing interface.

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

The integration tests truncate the tables they use — point
`TEST_DATABASE_URL` at a throwaway database, not one with data you care
about. `internal/store` and `internal/replay` share those tables, so the
database suite runs with `go test -p 1` (already the case in `make test-db`
and CI); don't drop that flag.

## Architecture

```
cmd/sorotrail        main: wiring + graceful shutdown
internal/config      env parsing + validation
internal/rpc         Stellar RPC JSON-RPC client (interface: rpc.Client)
internal/decode      ScVal → JSON            (interface: decode.Decoder)
internal/store       Postgres + migrations   (interface: store.Store)
internal/ingester    polling loop, pagination, backoff
internal/replay      re-decode stored raw XDR (sorotrail replay)
internal/api         chi HTTP handlers
```

The ingester and API depend only on the three interfaces, never on concrete
implementations, so each layer is independently testable and replaceable.

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
- Keep functions small and packages focused. When in doubt, match the
  surrounding code.

## Dependency management

Dependency updates are handled by Dependabot, which opens grouped weekly PRs
for Go modules, GitHub Actions, and the Docker base image. PRs with minor or
patch bumps are grouped together to keep the review stream manageable; major
version bumps come individually. The `vulncheck` CI job runs
`govulncheck ./...` and fails if any reachable vulnerability is found, so
known-vulnerable code paths are surfaced before they ship. Review dependency
PRs promptly — a green check on `vulncheck` is a good signal that the bump
can be merged without deep audit.

## Pull requests

- `go build ./...`, `make test` and `make lint` must pass.
- Include tests for behavior changes.
- Update the README's API reference and config table when you touch either.
