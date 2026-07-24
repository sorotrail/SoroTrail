BINARY := bin/sorotrail
MIGRATIONS := internal/store/migrations
DATABASE_URL ?= postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable

.PHONY: build run test test-db lint cover cover-html migrate-up migrate-down docker-up docker-down clean

build:
	go build -o $(BINARY) ./cmd/sorotrail

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
