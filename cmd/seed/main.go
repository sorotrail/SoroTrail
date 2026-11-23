// Command seed fills a database with realistic event shapes for local
// development, demos and smoke tests.
//
// The target is chosen by the database URL: postgres:// or postgresql:// seed
// Postgres, sqlite: or file: seed a SQLite file. The same -seed value always
// produces the same events, so two runs against an empty database are
// identical. With -drop-first (the default) the events table is emptied first;
// with -drop-first=false the run appends, and because event IDs are
// deterministic, re-running with the same flags inserts nothing new.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stellar/go-stellar-sdk/xdr"
	_ "modernc.org/sqlite"

	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	defaultPostgresURL = "postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable"
	defaultSeedValue   = int64(42)
	firstLedger        = int64(100000)
	eventsPerLedger    = 20
)

// config is the validated command line.
type config struct {
	DatabaseURL string
	Count       int
	BatchSize   int
	Contracts   int
	DropFirst   bool
	Seed        int64
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

// parseConfig reads the flags. getenv supplies the default database URL so a
// test does not depend on the process environment.
func parseConfig(args []string, getenv func(string) string) (config, error) {
	defaultDB := getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = defaultPostgresURL
	}

	var cfg config
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.DatabaseURL, "db", defaultDB, "database URL: postgres://... or sqlite:<path>")
	fs.IntVar(&cfg.Count, "count", 1000000, "Total number of events to seed")
	fs.IntVar(&cfg.BatchSize, "batch-size", 1000, "Batch size for insertion")
	fs.IntVar(&cfg.Contracts, "contracts", 50, "Number of distinct contracts to generate")
	fs.BoolVar(&cfg.DropFirst, "drop-first", true, "Delete existing events before seeding")
	fs.Int64Var(&cfg.Seed, "seed", defaultSeedValue, "Random seed; the same seed produces the same events")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	switch {
	case cfg.Count < 1:
		return config{}, fmt.Errorf("-count must be at least 1, got %d", cfg.Count)
	case cfg.BatchSize < 1:
		return config{}, fmt.Errorf("-batch-size must be at least 1, got %d", cfg.BatchSize)
	case cfg.Contracts < 1:
		return config{}, fmt.Errorf("-contracts must be at least 1, got %d", cfg.Contracts)
	}
	if _, err := targetKind(cfg.DatabaseURL); err != nil {
		return config{}, err
	}
	return cfg, nil
}

type target int

const (
	targetPostgres target = iota
	targetSQLite
)

// targetKind picks the backend from the URL scheme.
func targetKind(databaseURL string) (target, error) {
	switch {
	case strings.HasPrefix(databaseURL, "postgres://"), strings.HasPrefix(databaseURL, "postgresql://"):
		return targetPostgres, nil
	case strings.HasPrefix(databaseURL, "sqlite:"), strings.HasPrefix(databaseURL, "sqlite3:"), strings.HasPrefix(databaseURL, "file:"):
		return targetSQLite, nil
	default:
		return 0, fmt.Errorf("unsupported database URL %q: use postgres:// or sqlite:<path>", redactURL(databaseURL))
	}
}

// redactURL hides credentials so a URL can be logged.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = url.User(u.User.Username())
	return u.String()
}

// seedStore is the storage surface the seeder needs.
type seedStore interface {
	UpsertEvents(ctx context.Context, events []store.Event) (int64, error)
}

// openTarget migrates the database and returns the store, a function that
// deletes existing events, and a cleanup function.
func openTarget(ctx context.Context, databaseURL string) (seedStore, func(context.Context) error, func(), error) {
	kind, err := targetKind(databaseURL)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := store.Migrate(databaseURL); err != nil {
		return nil, nil, nil, fmt.Errorf("running database migrations: %w", err)
	}

	if kind == targetPostgres {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("opening pgx pool: %w", err)
		}
		truncate := func(ctx context.Context) error {
			_, err := pool.Exec(ctx, "TRUNCATE TABLE events CASCADE;")
			return err
		}
		return store.NewPostgres(pool), truncate, pool.Close, nil
	}

	path := databaseURL
	for _, p := range []string{"sqlite3://", "sqlite://", "sqlite3:", "sqlite:", "file:"} {
		if strings.HasPrefix(path, p) {
			path = strings.TrimPrefix(path, p)
			break
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("opening sqlite: %w", err)
	}
	truncate := func(ctx context.Context) error {
		_, err := db.ExecContext(ctx, "DELETE FROM events")
		return err
	}
	return store.NewSQLite(db), truncate, func() { _ = db.Close() }, nil
}

// run parses args, opens the target and seeds it. Progress goes to stdout.
func run(ctx context.Context, args []string, getenv func(string) string, stdout io.Writer) error {
	cfg, err := parseConfig(args, getenv)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(stdout, "Connecting to %s...\n", redactURL(cfg.DatabaseURL))
	st, truncate, cleanup, err := openTarget(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer cleanup()

	if cfg.DropFirst {
		_, _ = fmt.Fprintln(stdout, "Deleting existing events...")
		if err := truncate(ctx); err != nil {
			return fmt.Errorf("deleting existing events: %w", err)
		}
	}

	_, err = seedEvents(ctx, cfg, st, stdout)
	return err
}

// generator produces the deterministic event stream.
type generator struct {
	rng       *rand.Rand
	contracts []string
	topics    []topicShape
	b64Value  string
	baseTime  time.Time
	ledger    int64
}

type topicShape struct {
	json   string
	rawXDR []string
}

var eventTypes = []string{"contract", "contract", "contract", "contract", "system", "diagnostic"}

func contractIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("C%055d", i)
	}
	return ids
}

func symbolXDR(name string) (string, error) {
	sym := xdr.ScSymbol(name)
	return xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym})
}

func newGenerator(cfg config) (*generator, error) {
	const holder = "CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"

	g := &generator{
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		contracts: contractIDs(cfg.Contracts),
		baseTime:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ledger:    firstLedger,
	}
	for _, name := range []string{"transfer", "mint", "burn"} {
		raw, err := symbolXDR(name)
		if err != nil {
			return nil, fmt.Errorf("encoding %s topic: %w", name, err)
		}
		g.topics = append(g.topics, topicShape{
			json:   fmt.Sprintf(`[{"symbol":%q},{"address":%q}]`, name, holder),
			rawXDR: []string{raw},
		})
	}
	v := xdr.Uint64(100000)
	value, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &v})
	if err != nil {
		return nil, fmt.Errorf("encoding value: %w", err)
	}
	g.b64Value = value
	return g, nil
}

// next returns event number i (1-based).
func (g *generator) next(i int) store.Event {
	if i%eventsPerLedger == 0 {
		g.ledger++
	}
	contract := g.contracts[g.rng.Intn(len(g.contracts))]
	eventType := eventTypes[g.rng.Intn(len(eventTypes))]
	topic := g.topics[g.rng.Intn(len(g.topics))]
	txHash := fmt.Sprintf("%064x", g.rng.Int63())

	return store.Event{
		ID:               fmt.Sprintf("%019d-%010d", g.ledger, i%eventsPerLedger),
		ContractID:       contract,
		Ledger:           g.ledger,
		Type:             eventType,
		TxHash:           txHash,
		TxIndex:          int32((i % eventsPerLedger) / 2),
		OpIndex:          int32(i % 2),
		InSuccessfulCall: true,
		Topics:           []byte(topic.json),
		Value:            []byte(`{"u64":100000}`),
		CreatedAt:        g.baseTime.Add(time.Duration(g.ledger*5) * time.Second),
		RawTopicXDR:      topic.rawXDR,
		RawValueXDR:      g.b64Value,
	}
}

// seedEvents inserts cfg.Count events in batches and returns how many rows
// were newly inserted (events already present are not counted).
func seedEvents(ctx context.Context, cfg config, st seedStore, stdout io.Writer) (int64, error) {
	g, err := newGenerator(cfg)
	if err != nil {
		return 0, err
	}
	_, _ = fmt.Fprintf(stdout, "Seeding %d events (batch size: %d, contracts: %d, seed: %d)...\n",
		cfg.Count, cfg.BatchSize, cfg.Contracts, cfg.Seed)

	start := time.Now()
	var inserted int64
	batch := make([]store.Event, 0, cfg.BatchSize)
	for i := 1; i <= cfg.Count; i++ {
		batch = append(batch, g.next(i))
		if len(batch) < cfg.BatchSize && i != cfg.Count {
			continue
		}
		if err := ctx.Err(); err != nil {
			return inserted, err
		}
		n, err := st.UpsertEvents(ctx, batch)
		if err != nil {
			return inserted, fmt.Errorf("UpsertEvents failed at row %d: %w", i, err)
		}
		inserted += n
		batch = batch[:0]

		if i%50000 == 0 || i == cfg.Count {
			elapsed := time.Since(start).Seconds()
			if elapsed <= 0 {
				elapsed = 1e-9
			}
			_, _ = fmt.Fprintf(stdout, "Progress: %d/%d (%.1f%%) | %d rows inserted | Rate: %.0f events/sec\n",
				i, cfg.Count, float64(i)/float64(cfg.Count)*100.0, inserted, float64(i)/elapsed)
		}
	}

	_, _ = fmt.Fprintf(stdout, "Seeding complete! Inserted %d new events in %v.\n", inserted, time.Since(start))
	return inserted, nil
}
