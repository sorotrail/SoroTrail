BINARY := bin/sorotrail
MIGRATIONS := internal/store/migrations
# Keep in sync with the postgres service in docker-compose.yml (port, user, password, db).
# CI overrides TEST_DATABASE_URL to localhost:5433 because it maps the container
# to a non-default host port.
DATABASE_URL ?= postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable

.PHONY: build run test test-db lint cover cover-html migrate-up migrate-down docker-up docker-down clean check-monitoring
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo "unknown")

LDFLAGS := -ldflags="-X github.com/sorotrail/sorotrail/internal/buildinfo.Version=$(VERSION) -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=$(COMMIT) -X github.com/sorotrail/sorotrail/internal/buildinfo.BuildDate=$(BUILD_DATE)"

.PHONY: help build build-all build-all-integration run test test-fast test-db test-ci test-integration simtest simtest-long vet vet-integration lint bench bench-ci ci cover cover-html cover-ci cover-integration-ci migrate-up migrate-down migrations-check seed docker-up docker-down spec clean
.PHONY: build build-all build-all-integration run test test-fast test-db test-integration vet vet-integration test-ci lint cover cover-html cover-ci cover-integration-ci migrate-up migrate-down migrations-check docker-up docker-down simtest simtest-long clean bench bench-ci seed spec spec-check client ci
.PHONY: help build build-all build-all-integration run test test-fast test-db test-ci test-integration simtest simtest-long vet vet-integration lint lint-guard bench bench-ci ci client client-check cover cover-html cover-ci cover-integration-ci migrate-up migrate-down migrations-check seed docker-up docker-down spec clean loadtest test-prereqs

# ── Self-documenting help ────────────────────────────────────────────────────
# Every target that starts with a double-hash comment (##) is listed by
# `make help`.  Bare `make` prints this list so contributors never have
# to read the Makefile end-to-end to discover what's available.
.DEFAULT_GOAL := help

help: ## Show this help message
	@printf "\nUsage:  make <target>\n\n"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf "\n"

# ── Build ────────────────────────────────────────────────────────────────────

build: ## Build the sorotrail binary
	go build $(LDFLAGS) -o $(BINARY) ./cmd/sorotrail

build-all: ## Compile every package (mirrors CI build step)
	go build ./...

build-all-integration: ## Compile with the integration build tag
	go build -tags=integration ./...

run: build ## Build and run the binary
	./$(BINARY)

# ── Test ─────────────────────────────────────────────────────────────────────

# Report the external dependencies the suite needs but cannot find.
#
# A skipped dependency is invisible inside `go test`: Go discards the output
# of a package that passes, so a notice printed by the test binary itself
# only shows up under -v. This target prints the notice before the suite
# runs, so `make test` and every other full-gate target say out loud that
# the Helm chart golden tests or the database-backed tests did not run.
#
# Set SOROTRAIL_REQUIRE_HELM (or SOROTRAIL_REQUIRE_POSTGRES) to turn a
# missing dependency into a failure instead of a notice — that is how CI
# asserts the tests really ran.
test-prereqs: ## Report external test dependencies that are missing
	@go run ./cmd/testprereqs

test: test-prereqs ## Run the unit suite with race detector
	go test -race ./...

test-fast: test-prereqs ## Run unit tests without the race detector
	go test ./...

test-db: test-prereqs ## Run full test suite against Postgres (requires DATABASE_URL)
	TEST_DATABASE_URL=$(DATABASE_URL) go test -p 1 ./...

# Shared with cover-ci so the plain and coverage-instrumented runs of the CI
# suite cannot drift: same packages, same serialization, same race detector.
TEST_CI_FLAGS := -p 1 ./... -count=1 -race -timeout=120s

test-ci: test-prereqs ## Mirror the CI test job exactly (race-enabled, serial, 120s timeout)
	go test $(TEST_CI_FLAGS)

test-integration: test-prereqs ## Run the integration-tagged suite against a real Postgres
	go test -tags=integration -p 1 ./... -count=1

loadtest: ## Run a short synthetic load test against a database (requires DATABASE_URL)
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL is required (set it or pass --db)"; exit 1; fi
	go run ./cmd/sorotrail loadtest --ledgers=200 --events-per-ledger=25 --duration=15s

simtest: ## Run the deterministic simulation suite (mock store, fast)
	go test ./internal/simtest/... -count=1 -timeout 120s

simtest-long: ## Run simulations with randomized-mode extended budget
	go test ./internal/simtest/... -count=1 -timeout 600s -v -run "TestAllCuratedScenarios|TestRandomizedMode"

# ── Vet / Lint ───────────────────────────────────────────────────────────────

vet: ## Run go vet on all packages
	go vet ./...

vet-integration: ## Vet integration-tagged code too
	go vet -tags=integration ./...

# Auto-fix golangci-lint findings that can be resolved without human
#
# judgement (mostly goimports reorderings): writes the fixed files in
# place, then re-runs the linter so a contributor can see the result.
# `make lint-fix` does not exit 0 when files were changed; run it, then
# `make lint` on the updated tree.

lint-fix: ## Run golangci-lint run --fix and apply the fixes
	golangci-lint run --fix

# The guard runs first so a go.mod that outran the pinned linter fails with the
# fix spelled out, instead of golangci-lint's "lower than the targeted Go
# version" error. See CONTRIBUTING.md#lint-toolchain-drift.
lint: lint-guard ## Run golangci-lint (guards go.mod/toolchain drift first)
	golangci-lint run

# Validate the operator-facing monitoring assets. promtool parses the rule
# file and runs the unit tests in deploy/prometheus/sorotrail-rules-test.yml;
# scripts/validate_monitoring.py then asserts the dashboard parses as a real
# Grafana dashboard and that every metric both assets reference is one
# SoroTrail actually exports (deploy/prometheus/exported-metrics.txt).
# Needs promtool on PATH (ships with Prometheus); no Prometheus server or
# secrets required, so CI runs the same thing.
check-monitoring:
	promtool check rules deploy/prometheus/sorotrail-rules.yml
	promtool test rules deploy/prometheus/sorotrail-rules-test.yml
	python3 scripts/validate_monitoring.py

cover:
lint-guard: ## Fail fast when go.mod targets a newer Go than the pinned linter
	@scripts/check_lint_toolchain.sh

# ── Benchmarks ───────────────────────────────────────────────────────────────

bench: ## Run benchmarks with environment capture
	@echo "=================================================================="
	@echo " SoroTrail Benchmark Environment Capture"
	@echo "=================================================================="
	@echo "Date: $$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo "unknown")"
	@echo "Go Version: $$(go version 2>/dev/null || echo "go version unknown")"
	@echo "OS/Arch: $$(go env GOOS 2>/dev/null || echo "unknown")/$$(go env GOARCH 2>/dev/null || echo "unknown")"
	@echo "Postgres URL: $(DATABASE_URL)"
	@echo "=================================================================="
	@echo " Running Benchmarks..."
	@echo "=================================================================="
	TEST_DATABASE_URL=$(DATABASE_URL) go test -bench=. -benchmem ./...

bench-ci: ## Benchmark smoke run (CI-length, no DB required)
	go test -bench=. -benchtime=10ms ./...

# ── CI gate ──────────────────────────────────────────────────────────────────
# Composed from the existing targets so the two cannot drift.
# Runs as sub-makes so the first failure stops the run.

ci: build-all vet spec-check client-check migrations-check test-ci bench-ci build-all-integration vet-integration test-integration lint ## Reproduce the full CI gate locally (first failure stops)

# ── Client ───────────────────────────────────────────────────────────────────

# Regenerate the versioned API client in pkg/client from api/openapi.yaml.
# Run this after changing the spec, or pkg/client's drift test fails the
# build (see pkg/client/README.md).
client: ## Regenerate the versioned API client in pkg/client
	go run ./cmd/clientgen

client-check: spec-check ## Verify every client target in pkg/client/targets.yaml is current, compiles, and passes contract tests
	go run ./cmd/clientgen -check -manifest pkg/client/targets.yaml
	go test ./pkg/client/... ./pkg/docs/... -count=1

# ── Coverage ─────────────────────────────────────────────────────────────────

cover: test-prereqs ## Run tests with coverage profile
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out

cover-html: cover ## Open coverage report in browser
	go tool cover -html=coverage.out

# CI runs this target in both test-matrix legs (see .github/workflows/ci.yml).
# The command lives here rather than in the workflow so a contributor can
# reproduce the exact CI coverage profile locally with one make target.
# -covermode=atomic is what -race requires and must match the integration
# legs' mode, or scripts/merge_coverage.sh rejects the mixed profiles.
cover-ci: test-prereqs ## Mirror the CI test job, recording an atomic coverage profile
	go test $(TEST_CI_FLAGS) -coverprofile=coverage.out -covermode=atomic

# Tagged counterpart of cover-ci: the same database-backed suite the
# integration job runs, plus a profile so code that only exists behind the
# integration tag counts as covered once the coverage job merges the two
# flavours. Without it, migrations and the store/ingester paths that need a
# real Postgres look untested in the reported figure.
cover-integration-ci: test-prereqs ## Mirror the CI integration job, recording an atomic coverage profile
	go test -tags=integration -p 1 -timeout 5m ./... -count=1 -coverprofile=coverage.out -covermode=atomic

# ── Database / Migrations ────────────────────────────────────────────────────
# Migrations run automatically on startup; these targets are for manual control.
# Requires the migrate CLI: https://github.com/golang-migrate/migrate

migrate-up: ## Apply all pending migrations
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" up

migrate-down: ## Roll back the last migration
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" down 1

# Two branches can each take "the next number" and merge cleanly, and the
# embedded series then refuses to load — golang-migrate rejects a duplicate
# version, so every database-backed test fails at once instead of the one that
# broke. The check reads the embedded directory only, so it needs no Postgres
# and runs in seconds; CI gates on it in the `migrations` job.
migrations-check: ## Fail when two migrations share a version number (no database needed)
	go test ./internal/store/ -run TestMigrate_VersionNumbersAreUnique -count=1

seed: ## Seed the database with sample events
	go run ./cmd/seed -db="$(DATABASE_URL)" -count=1000000

# ── Docker ───────────────────────────────────────────────────────────────────

docker-up: ## Start Postgres and the indexer via docker compose
	docker compose --profile dev up -d --build

docker-down: ## Tear down docker compose services
	docker compose down

# ── OpenAPI ──────────────────────────────────────────────────────────────────
# api/openapi.yaml is the source of truth; internal/api/openapi.json is
# generated from it. `spec` regenerates the copy, `spec-check` verifies the
# committed copy is current without writing to the working tree.

spec: ## Regenerate the OpenAPI spec JSON that internal/api embeds
	go run ./cmd/specgen

spec-check: ## Fail if internal/api/openapi.json is stale relative to api/openapi.yaml
	@tmp=$$(mktemp); \
	trap 'rm -f "$$tmp"' EXIT; \
	go run ./cmd/specgen -out "$$tmp"; \
	if ! diff -u internal/api/openapi.json "$$tmp"; then \
		echo "internal/api/openapi.json is stale; run 'make spec'"; \
		exit 1; \
	fi

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts
	rm -rf bin coverage.out
