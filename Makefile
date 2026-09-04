BINARY := bin/sorotrail
MIGRATIONS := internal/store/migrations
# Keep in sync with the postgres service in docker-compose.yml (port, user, password, db).
# CI overrides TEST_DATABASE_URL to localhost:5433 because it maps the container
# to a non-default host port.
DATABASE_URL ?= postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo "unknown")

LDFLAGS := -ldflags="-X github.com/sorotrail/sorotrail/internal/buildinfo.Version=$(VERSION) -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=$(COMMIT) -X github/sorotrail/sorotrail/internal/buildinfo.BuildDate=$(BUILD_DATE)"

.PHONY: help build build-all build-all-integration run test test-fast test-db test-ci test-integration simtest simtest-long vet vet-integration lint bench bench-ci ci cover cover-html migrate-up migrate-down seed docker-up docker-down spec clean
.PHONY: build build-all build-all-integration run test test-fast test-db test-integration vet vet-integration test-ci lint cover cover-html migrate-up migrate-down docker-up docker-down simtest simtest-long clean bench bench-ci seed spec client ci

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

test: ## Run the unit suite with race detector
	go test -race ./...

test-fast: ## Run unit tests without the race detector
	go test ./...

test-db: ## Run full test suite against Postgres (requires DATABASE_URL)
	TEST_DATABASE_URL=$(DATABASE_URL) go test -p 1 ./...

test-ci: ## Mirror the CI test job exactly (race-enabled, serial, 120s timeout)
	go test -p 1 ./... -count=1 -race -timeout=120s

test-integration: ## Run the integration-tagged suite against a real Postgres
	go test -tags=integration -p 1 ./... -count=1

simtest: ## Run the deterministic simulation suite (mock store, fast)
	go test ./internal/simtest/... -count=1 -timeout 120s

simtest-long: ## Run simulations with randomized-mode extended budget
	go test ./internal/simtest/... -count=1 -timeout 600s -v -run "TestAllCuratedScenarios|TestRandomizedMode"

# ── Vet / Lint ───────────────────────────────────────────────────────────────

vet: ## Run go vet on all packages
	go vet ./...

vet-integration: ## Vet integration-tagged code too
	go vet -tags=integration ./...

lint: ## Run golangci-lint
	golangci-lint run

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

ci: build-all vet test-ci bench-ci build-all-integration vet-integration test-integration lint ## Reproduce the full CI gate locally (first failure stops)

# ── Coverage ─────────────────────────────────────────────────────────────────

cover: ## Run tests with coverage profile
# Regenerate the versioned API client in pkg/client from api/openapi.yaml.
# Run this after changing the spec, or pkg/client's drift test fails the
# build (see pkg/client/README.md).
client:
	go run ./cmd/clientgen

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out

cover-html: cover ## Open coverage report in browser
	go tool cover -html=coverage.out

# ── Database / Migrations ────────────────────────────────────────────────────
# Migrations run automatically on startup; these targets are for manual control.
# Requires the migrate CLI: https://github.com/golang-migrate/migrate

migrate-up: ## Apply all pending migrations
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" up

migrate-down: ## Roll back the last migration
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" down 1

seed: ## Seed the database with sample events
	go run ./cmd/seed -db="$(DATABASE_URL)" -count=1000000

# ── Docker ───────────────────────────────────────────────────────────────────

docker-up: ## Start Postgres and the indexer via docker compose
	docker compose up -d --build

docker-down: ## Tear down docker compose services
	docker compose down

# ── OpenAPI ──────────────────────────────────────────────────────────────────

spec: ## Regenerate the OpenAPI spec JSON that internal/api embeds
	go run ./cmd/specgen

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts
	rm -rf bin coverage.out
