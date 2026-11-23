package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/store"
)

func noEnv(string) string { return "" }

func sqliteURL(t *testing.T) string {
	t.Helper()
	return "sqlite:" + filepath.ToSlash(filepath.Join(t.TempDir(), "seed.db"))
}

// readAll pages through every stored event.
func readAll(t *testing.T, st store.Store) []store.Event {
	t.Helper()
	var all []store.Event
	cursor := ""
	for {
		page, next, err := st.QueryEvents(context.Background(), store.EventFilter{Limit: 500, Cursor: cursor})
		require.NoError(t, err)
		all = append(all, page...)
		if next == "" {
			return all
		}
		cursor = next
	}
}

func openSQLiteStore(t *testing.T, databaseURL string) store.Store {
	t.Helper()
	st, _, cleanup, err := openTarget(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	full, ok := st.(store.Store)
	require.True(t, ok, "the SQLite target must be a full store")
	return full
}

func TestParseConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig(nil, noEnv)
	require.NoError(t, err)
	assert.Equal(t, config{
		DatabaseURL: defaultPostgresURL,
		Count:       1000000,
		BatchSize:   1000,
		Contracts:   50,
		DropFirst:   true,
		Seed:        defaultSeedValue,
	}, cfg)
}

func TestParseConfigFlagsAndEnvironment(t *testing.T) {
	t.Parallel()

	env := func(k string) string {
		if k == "DATABASE_URL" {
			return "sqlite:/from/env.db"
		}
		return ""
	}

	cfg, err := parseConfig(nil, env)
	require.NoError(t, err)
	assert.Equal(t, "sqlite:/from/env.db", cfg.DatabaseURL, "DATABASE_URL supplies the default")

	cfg, err = parseConfig([]string{
		"-db", "postgres://u:p@h/db", "-count", "7", "-batch-size", "3",
		"-contracts", "2", "-drop-first=false", "-seed", "99",
	}, env)
	require.NoError(t, err)
	assert.Equal(t, config{
		DatabaseURL: "postgres://u:p@h/db", Count: 7, BatchSize: 3, Contracts: 2, DropFirst: false, Seed: 99,
	}, cfg, "flags override both the environment and the built-in defaults")
}

func TestParseConfigRejectsBadInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"zero count", []string{"-count", "0"}, "-count must be at least 1"},
		{"negative batch", []string{"-batch-size", "-1"}, "-batch-size must be at least 1"},
		{"zero contracts", []string{"-contracts", "0"}, "-contracts must be at least 1"},
		{"unknown flag", []string{"-nope"}, "nope"},
		{"stray argument", []string{"extra"}, `unexpected argument "extra"`},
		{"unsupported scheme", []string{"-db", "mysql://u:secret@h/db"}, "unsupported database URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseConfig(tt.args, noEnv)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.NotContains(t, err.Error(), "secret", "credentials must never reach an error message")
		})
	}
}

func TestTargetSelectionAndRedaction(t *testing.T) {
	t.Parallel()

	for url, want := range map[string]target{
		"postgres://h/db":    targetPostgres,
		"postgresql://h/db":  targetPostgres,
		"sqlite:/tmp/x.db":   targetSQLite,
		"sqlite3:/tmp/x.db":  targetSQLite,
		"file:/tmp/x.db?a=b": targetSQLite,
		"sqlite::memory:":    targetSQLite,
	} {
		got, err := targetKind(url)
		require.NoError(t, err, url)
		assert.Equal(t, want, got, url)
	}

	assert.Equal(t, "postgres://user@host/db", redactURL("postgres://user:hunter2@host/db"))
	assert.Equal(t, "sqlite:/tmp/x.db", redactURL("sqlite:/tmp/x.db"))
}

func TestGeneratorIsDeterministicPerSeed(t *testing.T) {
	t.Parallel()

	gen := func(seed int64, n int) []store.Event {
		g, err := newGenerator(config{Contracts: 5, Seed: seed})
		require.NoError(t, err)
		out := make([]store.Event, n)
		for i := range out {
			out[i] = g.next(i + 1)
		}
		return out
	}

	a, b, other := gen(42, 60), gen(42, 60), gen(43, 60)
	assert.Equal(t, a, b, "same seed, same data")
	assert.NotEqual(t, a, other, "a different seed changes the data")

	seen := map[string]bool{}
	for _, e := range a {
		assert.False(t, seen[e.ID], "event IDs are unique: %s", e.ID)
		seen[e.ID] = true
	}
}

func TestGeneratedEventsAreWellFormedAndDecode(t *testing.T) {
	t.Parallel()

	g, err := newGenerator(config{Contracts: 4, Seed: defaultSeedValue})
	require.NoError(t, err)
	dec := decode.XDRDecoder{}
	before := decode.DecodeErrorCount()

	for i := 1; i <= 100; i++ {
		e := g.next(i)
		assert.Len(t, e.ContractID, 56, "contract IDs are strkey length")
		assert.True(t, strings.HasPrefix(e.ContractID, "C"))
		assert.Contains(t, eventTypes, e.Type)
		assert.Len(t, e.TxHash, 64)
		assert.True(t, e.InSuccessfulCall)
		assert.GreaterOrEqual(t, e.Ledger, firstLedger)

		assert.True(t, json.Valid(e.Topics), "topics are valid JSON: %s", e.Topics)
		assert.True(t, json.Valid(e.Value), "value is valid JSON: %s", e.Value)

		require.Len(t, e.RawTopicXDR, 1)
		topic, err := dec.DecodeScVal(e.RawTopicXDR[0])
		require.NoError(t, err)
		var first []map[string]any
		require.NoError(t, json.Unmarshal(e.Topics, &first))
		assert.JSONEq(t, string(mustMarshal(t, first[0])), string(topic), "raw topic XDR decodes to the stored JSON topic")

		value, err := dec.DecodeScVal(e.RawValueXDR)
		require.NoError(t, err)
		assert.JSONEq(t, string(e.Value), string(value), "raw value XDR decodes to the stored JSON value")
	}
	assert.Equal(t, before, decode.DecodeErrorCount(), "no generated payload may hit the decoder's fallback")
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestRunSeedsSQLite(t *testing.T) {
	url := sqliteURL(t)
	var out bytes.Buffer

	err := run(context.Background(), []string{
		"-db", url, "-count", "250", "-batch-size", "60", "-contracts", "7",
	}, noEnv, &out)
	require.NoError(t, err)
	assert.Contains(t, out.String(), "Seeding 250 events (batch size: 60, contracts: 7, seed: 42)")
	assert.Contains(t, out.String(), "Inserted 250 new events")

	st := openSQLiteStore(t, url)
	events := readAll(t, st)
	require.Len(t, events, 250, "the expected number of events is stored")

	contracts, types := map[string]bool{}, map[string]bool{}
	ids := map[string]bool{}
	for _, e := range events {
		contracts[e.ContractID] = true
		types[e.Type] = true
		assert.False(t, ids[e.ID], "no duplicate event IDs")
		ids[e.ID] = true
		assert.True(t, json.Valid(e.Topics) && json.Valid(e.Value), "stored payloads stay valid JSON")
	}
	assert.LessOrEqual(t, len(contracts), 7)
	assert.Greater(t, len(contracts), 1, "events are spread across contracts")
	assert.Subset(t, eventTypes, keys(types))
	assert.Greater(t, len(types), 1, "more than one event type is produced")

	total, err := st.CountEvents(context.Background(), store.EventFilter{})
	require.NoError(t, err)
	assert.Equal(t, int64(250), total)

	dec := decode.XDRDecoder{}
	for _, e := range events {
		if len(e.RawTopicXDR) == 0 {
			continue
		}
		_, err := dec.DecodeScVal(e.RawTopicXDR[0])
		assert.NoError(t, err, "stored raw topic XDR still decodes")
		_, err = dec.DecodeScVal(e.RawValueXDR)
		assert.NoError(t, err, "stored raw value XDR still decodes")
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSameSeedSameDataAcrossRuns(t *testing.T) {
	args := func(url, seed string) []string {
		return []string{"-db", url, "-count", "120", "-contracts", "6", "-seed", seed}
	}
	a, b, c := sqliteURL(t), sqliteURL(t), sqliteURL(t)
	require.NoError(t, run(context.Background(), args(a, "5"), noEnv, &bytes.Buffer{}))
	require.NoError(t, run(context.Background(), args(b, "5"), noEnv, &bytes.Buffer{}))
	require.NoError(t, run(context.Background(), args(c, "6"), noEnv, &bytes.Buffer{}))

	ea, eb, ec := readAll(t, openSQLiteStore(t, a)), readAll(t, openSQLiteStore(t, b)), readAll(t, openSQLiteStore(t, c))
	assert.Equal(t, ea, eb, "the same seed reproduces the same rows")
	assert.NotEqual(t, ea, ec, "another seed changes them")
}

func TestRunAgainstNonEmptyDatabase(t *testing.T) {
	url := sqliteURL(t)
	ctx := context.Background()
	require.NoError(t, run(ctx, []string{"-db", url, "-count", "100", "-contracts", "4"}, noEnv, &bytes.Buffer{}))
	st := openSQLiteStore(t, url)

	count := func() int64 {
		n, err := st.CountEvents(ctx, store.EventFilter{})
		require.NoError(t, err)
		return n
	}
	require.Equal(t, int64(100), count())

	// Appending the same data again adds nothing: the IDs are deterministic.
	var out bytes.Buffer
	require.NoError(t, run(ctx, []string{"-db", url, "-count", "100", "-contracts", "4", "-drop-first=false"}, noEnv, &out))
	assert.Equal(t, int64(100), count(), "-drop-first=false is idempotent for the same flags")
	assert.Contains(t, out.String(), "Inserted 0 new events")

	// A larger append only adds the new tail.
	require.NoError(t, run(ctx, []string{"-db", url, "-count", "130", "-contracts", "4", "-drop-first=false"}, noEnv, &bytes.Buffer{}))
	assert.Equal(t, int64(130), count())

	// The default replaces what was there.
	require.NoError(t, run(ctx, []string{"-db", url, "-count", "10", "-contracts", "4"}, noEnv, &bytes.Buffer{}))
	assert.Equal(t, int64(10), count(), "-drop-first (the default) empties the table first")
}

func TestRunReportsErrorsInsteadOfExiting(t *testing.T) {
	t.Parallel()

	err := run(context.Background(), []string{"-count", "0"}, noEnv, &bytes.Buffer{})
	require.Error(t, err)

	err = run(context.Background(), []string{"-db", "mysql://x"}, noEnv, &bytes.Buffer{})
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = run(ctx, []string{"-db", sqliteURL(t), "-count", "5"}, noEnv, &bytes.Buffer{})
	require.Error(t, err, "a cancelled context stops the run with an error")
}
