BINARY := bin/sorotrail
MIGRATIONS := internal/store/migrations
DATABASE_URL ?= postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

LDFLAGS := -X github.com/khaylebfortune/sorotrail/internal/version.Version=$(VERSION) \
           -X github.com/khaylebfortune/sorotrail/internal/version.Commit=$(COMMIT) \
           -X github.com/khaylebfortune/sorotrail/internal/version.Date=$(DATE)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "unknown")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo "unknown")

LDFLAGS := -ldflags="-X github.com/sorotrail/sorotrail/internal/buildinfo.Version=$(VERSION) -X github.com/sorotrail/sorotrail/internal/buildinfo.Commit=$(COMMIT) -X github.com/sorotrail/sorotrail/internal/buildinfo.BuildDate=$(BUILD_DATE)"

.PHONY: build run test test-db lint cover cover-html migrate-up migrate-down docker-up docker-down clean

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
