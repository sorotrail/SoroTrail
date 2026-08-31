# Local Setup & Testing

Everything a new contributor needs to clone, build, run, and test SoroTrail
locally. Architecture context lives in [`architecture-overview.md`](architecture-overview.md).

## Prerequisites

| Tool | Version | Needed for |
| --- | --- | --- |
| Go | 1.25+ | Building and testing. The toolchain auto-downloads the version pinned in `go.mod`, so any Go ≥ 1.21 works too. |
| Docker | any recent | Local Postgres / full-stack `docker compose`, and the ephemeral Postgres used by integration tests. |
| golangci-lint | latest | `make lint` (CI runs it; install locally from [golangci-lint.run](https://golangci-lint.run/)). |
| migrate (golang-migrate) | latest | Manual `make migrate-up`/`migrate-down` only — migrations run automatically on startup, so this is optional. |

## 1. Clone

```sh
git clone https://github.com/sorotrail/SoroTrail.git
cd SoroTrail
```

## 2. Build

The quick path (writes `bin/sorotrail`, embedding version/commit/build-date):

```sh
make build
```

Or build the binary directly:

```sh
go build ./cmd/sorotrail
```

A pre-built `sorotrail` binary is already in the repo root; rebuild only after
changing code.

## 3. Run locally

### Option A — zero dependencies (SQLite)

Fastest way to see it work; no Docker required:

```sh
DATABASE_URL=sqlite:./sorotrail.db make run
```

Migrations apply automatically on startup. The API is on `http://localhost:8080`.

### Option B — Postgres via Docker Compose (recommended for dev)

Brings up Postgres and the indexer together against the public Stellar
testnet RPC:

```sh
docker compose up --build        # foreground
# or
docker compose up -d             # background
```

The API is on `http://localhost:8080`. To ingest only specific contracts,
set `WATCHED_CONTRACTS` (see `docker-compose.yml`).

### Option C — bring-your-own Postgres

```sh
docker compose up -d postgres
cp .env.example .env             # edit DATABASE_URL / RPC_URL as needed
set -a; source .env; set +a
make run
```

The full list of environment variables is in `.env.example` and the
[README configuration table](../README.md#configuration). The minimum to run
is `DATABASE_URL` (and `RPC_URL` to ingest real events).

### Verify it's up

```sh
curl -s http://localhost:8080/health
curl -s http://localhost:8080/stats
```

## 4. Testing

`make test` is the **unit suite only**, race-detector enabled (`go test -race ./...`),
and stays fast because the integration tests are gated behind the
`integration` build tag; `make test-fast` is the plain non-race run.

| Command | What it does |
| --- | --- |
| `make test` | Unit tests with the race detector (`go test -race ./...`), no external services; `make test-fast` for the plain run. |
| `make test-integration` | Integration suite (`-tags=integration -p 1`) against a **real Postgres**. Uses `TEST_DATABASE_URL` if set; otherwise spins up an ephemeral Postgres 16-alpine via testcontainers-go per test. |
| `make test-db` | Runs everything, including integration tests, against the Postgres in `TEST_DATABASE_URL`. |
| `make simtest` | Deterministic simulation suite (mock store, fast). |
| `make simtest-long` | Randomized/longer simulation budget with reproducible seeds. |
| `make lint` | `golangci-lint run`. |
| `make cover` / `make cover-html` | Coverage summary / HTML report. |
| `make bench-ci` | Compile + short benchmark run. |

### Common testing pitfalls

- **`-p 1` is required for the integration suite.** `internal/store` and
  `internal/replay` share one database and truncate the same tables, so
  parallel packages race. `make test-integration` already passes it.
- **Never point `TEST_DATABASE_URL` at a database you care about.** The
  integration helper truncates `events`, `ingestion_state`, `watched_contracts`,
  `replay_state`, and the audit tables between tests.
- **No Postgres?** Leave `TEST_DATABASE_URL` unset and the suite provisions its
  own container (needs Docker). Missing infra causes the test to `Skip`, not
  fail. See [`CONTRIBUTING.md`](../CONTRIBUTING.md#how-the-integration-test-layer-works-issue-9).

### Fuzz targets

Run locally with a budget:

```sh
go test ./internal/decode -run '^$' -fuzz FuzzDecodeScVal -fuzztime 30s
go test ./internal/decode -run '^$' -fuzz FuzzDecodeTopicArray -fuzztime 30s
```

Commit any panic reproducer under `testdata/fuzz` together with a regression test.

## 5. Common dev tasks

| Task | Command |
| --- | --- |
| Regenerate embedded OpenAPI JSON | `make spec` (after editing `api/openapi.yaml`; `pkg/docs` test fails the build otherwise) |
| Run manual migrations | `make migrate-up` / `make migrate-down` (needs `migrate` CLI) |
| Seed synthetic events | `make seed` (writes into `DATABASE_URL`) |
| Build & push image | `docker compose build` (see `Dockerfile`, `RELEASING.md`) |
| Grep config drift | `./scripts/check_env_sync.sh` |

## 6. Troubleshooting

- **`bind: address already in use`** — the API port (`HTTP_ADDR`, default
  `:8080`) is taken; set `HTTP_ADDR=:9090` and retry.
- **Postgres connection refused** — for `docker compose`, ensure the
  container is healthy (`docker ps`); for byo Postgres, check `DATABASE_URL`
  and that the DB/user/password match `.env.example`.
- **Integration tests skip** — Docker isn't available or `TEST_DATABASE_URL`
  is unset *and* testcontainers can't start a container; run
  `docker compose up -d postgres` and export `TEST_DATABASE_URL`.
- **`make lint` missing** — install golangci-lint; its config is
  `.golangci.yml`.
- **Schema/Go drift after a migration change** — update the column list in
  `TestMigrations_ApplyFromEmptyLand` (the migration test guards this).

## Before opening a PR

```sh
go build ./...
make test
make lint
# and, if you have Postgres available:
make test-integration
```

See [`CONTRIBUTING.md`](../CONTRIBUTING.md) and [`triage.md`](triage.md)
for review expectations and labeling.
