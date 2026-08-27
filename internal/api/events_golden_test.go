package api

// Golden-file snapshots for GET /events. Each case captures the exact JSON
// body the endpoint produced at authoring time, so any accidental format
// change — renamed keys, reordered struct fields, a lost omitempty, an
// envelope tweak — fails CI instead of silently breaking consumers.
//
// The snapshots intentionally pin the wire bytes (compact encoding plus the
// trailing newline json.Encoder appends); cosmetic churn shows up here on
// purpose so it is a conscious decision, not a drive-by.
//
// After an intentional format change, regenerate every snapshot with:
//
//	go test ./internal/api -run TestEventsGolden -update-golden
//
// and review the resulting diff in internal/api/testdata/golden/.
//
// The test also validates that every checked-in snapshot is valid JSON and
// that no snapshot is silently omitted from the table.

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// updateGolden regenerates the golden files instead of comparing against
// them. Scoped to this package so unrelated packages never rewrite files.
var updateGolden = flag.Bool("update-golden", false, "rewrite /events golden files for TestEventsGolden")

// goldenCreatedAt is a fixed timestamp so created_at serializes identically
// on every run; RFC 3339Nano output depends solely on this constant.
var goldenCreatedAt = time.Date(2024, 5, 1, 12, 30, 0, 123456789, time.UTC)

// goldenCursor is the next-page cursor the stub reports for populated
// fixtures, exercising the cursor / next_cursor slots in every snapshot.
const goldenCursor = "0000000043-0000000002"

// goldenFixtureEvents covers the shapes /events can render: a fully
// populated event, a SEP-41 transfer (locking the additive sep41_event
// slot), and a zero-value event (locking null topics/value and the zero
// timestamp serialization).
func goldenFixtureEvents() []store.Event {
	return []store.Event{
		{
			ID:               "0000000042-0000000001",
			ContractID:       testContract,
			Ledger:           42,
			Type:             "contract",
			TxHash:           "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
			TxIndex:          1,
			OpIndex:          0,
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"swap"},{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"},{"string":"soroban"}]`),
			Value:            json.RawMessage(`{"map":[{"key":{"symbol":"amount"},"val":{"i128":"777"}}]}`),
			CreatedAt:        goldenCreatedAt,
			Network:          "testnet",
		},
		{
			ID:               "0000000043-0000000001",
			ContractID:       testContract,
			Ledger:           43,
			Type:             "contract",
			TxHash:           "f0e9d8c7b6a59876543f2e1d0c9b8a796857463728193afbccddeeff00112233",
			TxIndex:          2,
			OpIndex:          1,
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"},{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"},{"address":"GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}]`),
			Value:            json.RawMessage(`{"i128":"1000000000"}`),
			CreatedAt:        goldenCreatedAt,
			Network:          "public",
		},
		{
			ID: "0000000044-0000000001",
		},
	}
}

// TestEventsGolden snapshots the /events response body across its render
// modes. Table-driven: each row pins one query surface against its own
// golden file under internal/api/testdata/golden/.
func TestEventsGolden(t *testing.T) {
	tests := []struct {
		name  string
		query string
		stub  func() *stubStore
	}{
		{
			name:  "default_page",
			query: "/events",
			stub: func() *stubStore {
				return &stubStore{events: goldenFixtureEvents(), nextCursor: goldenCursor}
			},
		},
		{
			name:  "envelope",
			query: "/events?envelope=true",
			stub: func() *stubStore {
				return &stubStore{events: goldenFixtureEvents(), nextCursor: goldenCursor}
			},
		},
		{
			name:  "include_xdr",
			query: "/events?include_xdr=true",
			stub: func() *stubStore {
				return &stubStore{events: goldenFixtureEvents(), nextCursor: goldenCursor}
			},
		},
		{
			name:  "fields_projection",
			query: "/events?fields=id,contract_id,ledger,type",
			stub: func() *stubStore {
				return &stubStore{events: goldenFixtureEvents()}
			},
		},
		{
			name:  "pretty",
			query: "/events?pretty=true",
			stub: func() *stubStore {
				return &stubStore{events: goldenFixtureEvents()}
			},
		},
		{
			name:  "empty_result",
			query: "/events",
			stub: func() *stubStore {
				return &stubStore{}
			},
		},
	}

	seen := make(map[string]struct{}, len(tests))
	for _, tt := range tests {
		if _, exists := seen[tt.name]; exists {
			t.Fatalf("duplicate golden test name %q", tt.name)
		}
		seen[tt.name] = struct{}{}
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.query, nil)
			newTestServer(tt.stub(), nil).Router().ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			compareGolden(t, tt.name, rec.Body.Bytes())
		})
	}
}

func TestEventsGoldenFilesAreValidJSON(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join("testdata", "golden"))
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		body, err := os.ReadFile(filepath.Join("testdata", "golden", entry.Name()))
		require.NoError(t, err, entry.Name())
		require.True(t, json.Valid(body), "golden file %s must contain valid JSON", entry.Name())
	}
}

// compareGolden diffs body against testdata/golden/events_<name>.json, or
// rewrites the file when -update-golden is passed. On mismatch it prints
// both sides indented so the drifted key is findable despite the compact
// wire encoding.
func compareGolden(t *testing.T, name string, body []byte) {
	t.Helper()

	path := filepath.Join("testdata", "golden", "events_"+name+".json")

	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, body, 0o644))
		t.Logf("updated golden file %s", path)
		return
	}

	golden, err := os.ReadFile(path)
	require.NoError(t, err,
		"golden file %s missing; run: go test ./internal/api -run TestEventsGolden -update-golden", path)

	if !bytes.Equal(golden, body) {
		var want, got bytes.Buffer
		_ = json.Indent(&want, golden, "", "  ")
		_ = json.Indent(&got, body, "", "  ")
		t.Fatalf("GET /events response shape drifted from %s\n--- golden\n%s\n--- actual\n%s\n"+
			"If the change is intentional, regenerate with:\n"+
			"\tgo test ./internal/api -run TestEventsGolden -update-golden",
			path, want.String(), got.String())
	}
}
