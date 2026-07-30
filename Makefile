BINARY := bin/sorotrail
MIGRATIONS := internal/store/migrations
DATABASE_URL ?= postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable

.PHONY: build run test test-db test-integration lint cover cover-html migrate-up migrate-down docker-up docker-down clean
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo "unknown")

LDFLAGS := -ldflags="-X github.com/sorotrail/sorotrail/internal/buildinfo.Version=$(VERSION) -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=$(COMMIT) -X github.com/sorotrail/sorotrail/internal/buildinfo.BuildDate=$(BUILD_DATE)"

.PHONY: build run test test-db lint cover cover-html migrate-up migrate-down docker-up docker-down simtest simtest-long clean bench bench-ci seed

build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/sorotrail
	go build $(LDFLAGS) -o $(BINARY) ./cmd/sorotrail

run: build
	./$(BINARY)

test:
	go test ./...

# Run the full test suite including Postgres integration tests.
# Requires a running Postgres, e.g. `make docker-up` first.
# -p 1 serializes packages: the integration tests in internal/store and
# internal/replay share one database and truncate the same tables.
test-db:
	TEST_DATABASE_URL=$(DATABASE_URL) go test -p 1 ./...

# Integration test suite only, gated behind the `integration` build tag so
# `go test ./...` stays fast. The suite honors TEST_DATABASE_URL when set
# (CI's services-postgres path), and otherwise spins up an ephemeral
# Postgres 16-alpine via testcontainers-go per `internal/testdb.Setup`
# call. Either way, the four required coverage areas — migration-up from
# empty, event upsert idempotency, ingestion_state save/resume across
# ingester restarts, GET /events filter combinations against seeded
# data — are asserted against a real PostgreSQL.
#
# -p 1 because the integration tests in internal/store and internal/replay
# share one database and truncate the same tables.
test-integration:
	go test -tags=integration -p 1 ./... -count=1
# Run the deterministic simulation test suite (mock store, fast).
simtest:
	go test ./internal/simtest/... -count=1 -timeout 120s

# Run the simulation test suite with randomized mode extended budget.
# Uses a higher iteration count and prints seeds for reproducibility.
simtest-long:
	go test ./internal/simtest/... -count=1 -timeout 600s -v -run "TestAllCuratedScenarios|TestRandomizedMode"

bench:
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

bench-ci:
	go test -bench=. -benchtime=10ms ./...

seed:
	go run ./cmd/seed -db="$(DATABASE_URL)" -count=1000000

lint:
	golangci-lint run

cover:
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out

cover-html: cover
	go tool cover -html=coverage.out

# Migrations run automatically on startup; these targets are for manual control.
# Requires the migrate CLI: https://github.com/golang-migrate/migrate
migrate-up:
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" down 1

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

clean:
	rm -rf bin coverage.out

