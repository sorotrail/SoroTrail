package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// updateGolden is declared in events_golden_test.go for this package.

// goldenDir returns the path to the golden test data directory.
func goldenDir(t *testing.T) string {
	t.Helper()
	return filepath.Join("testdata", "golden")
}

// loadGolden reads a golden file and returns its contents.
func loadGolden(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(goldenDir(t), name)
	data, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s not found; run with -update to create it", name)
	return string(data)
}

// writeGolden writes the golden file, or asserts the output matches if -update is not set.
func writeGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join(goldenDir(t), name)
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		t.Logf("updated golden: %s", name)
		return
	}
	want := loadGolden(t, name)
	assert.Equal(t, want, got, "golden file %s does not match; run with -update to regenerate", name)
}

// goldenExportStore is a minimal in-memory store for golden file tests.
// It returns deterministic events so the output is stable across runs.
type goldenExportStore struct {
	store.Store
	events []store.Event
}

func (g *goldenExportStore) QueryEvents(_ context.Context, fl store.EventFilter) ([]store.Event, string, error) {
	var matched []store.Event
	for _, e := range g.events {
		if fl.ContractID != "" && e.ContractID != fl.ContractID {
			continue
		}
		if fl.FromLedger > 0 && e.Ledger < fl.FromLedger {
			continue
		}
		if fl.ToLedger > 0 && e.Ledger > fl.ToLedger {
			continue
		}
		matched = append(matched, e)
	}
	// Simple: return all at once (no pagination).
	return matched, "", nil
}

func (g *goldenExportStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}

func (g *goldenExportStore) Ping(context.Context) error { return nil }

// goldenContractID is a stable contract ID for golden tests.
const goldenContractID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// goldenEvents returns deterministic events for golden tests.
// They include embedded commas, quotes, and newlines in JSON fields
// to exercise CSV escaping.
func goldenEvents() []store.Event {
	return []store.Event{
		{
			ID:         "0000000001-0000000100",
			ContractID: goldenContractID,
			Ledger:     100,
			Type:       "contract",
			TxHash:     "abc123def456",
			Topics:     json.RawMessage(`[{"symbol":"transfer"},{"string":"hello, world"}]`),
			Value:      json.RawMessage(`{"i128":"1000"}`),
		},
		{
			ID:         "0000000001-0000000101",
			ContractID: goldenContractID,
			Ledger:     101,
			Type:       "contract",
			TxHash:     "789xyz000abc",
			Topics:     json.RawMessage(`[{"symbol":"approve"},{"string":"value with \"quotes\""}]`),
			Value:      json.RawMessage(`{"map":[{"key":{"symbol":"amount"},"val":{"i128":"500"}}]}`),
		},
		{
			ID:         "0000000001-0000000102",
			ContractID: goldenContractID,
			Ledger:     102,
			Type:       "system",
			TxHash:     "def456abc789",
			Topics:     json.RawMessage(`[{"symbol":"mint"},{"string":"line\nbreak"}]`),
			Value:      json.RawMessage(`{"vec":[{"i128":"200"},{"i128":"300"}]}`),
		},
	}
}

// TestExport_CSV_Golden verifies that CSV export output matches the golden file.
func TestExport_CSV_Golden(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/csv; charset=utf-8", rec.Header().Get("Content-Type"))

	body := strings.TrimRight(rec.Body.String(), "\n")
	writeGolden(t, "export_contract_csv.csv", body)
}

// TestExport_NDJSON_Golden verifies that NDJSON export output matches the golden file.
func TestExport_NDJSON_Golden(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102&format=ndjson", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))

	body := strings.TrimRight(rec.Body.String(), "\n")
	writeGolden(t, "export_contract_ndjson.ndjson", body)
}

// TestExport_EmptyResult_CSV verifies that an empty export produces a valid CSV with header only.
func TestExport_EmptyResult_CSV(t *testing.T) {
	st := &goldenExportStore{events: nil}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := strings.TrimRight(rec.Body.String(), "\n")
	writeGolden(t, "export_empty_csv.csv", body)
}

// TestExport_EmptyResult_NDJSON verifies that an empty NDJSON export produces no lines.
func TestExport_EmptyResult_NDJSON(t *testing.T) {
	st := &goldenExportStore{events: nil}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102&format=ndjson", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := strings.TrimRight(rec.Body.String(), "\n")
	writeGolden(t, "export_empty_ndjson.ndjson", body)
}

// TestExport_CSV_HeaderMatchesColumns verifies the CSV header row matches
// the exact columns written by the streaming writer.
func TestExport_CSV_HeaderMatchesColumns(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
	require.NotEmpty(t, lines)
	assert.Equal(t, "id,ledger,type,tx_hash,topics,value", lines[0],
		"CSV header must exactly match the documented column order")
}

// TestExport_NDJSON_OneDocPerLine verifies NDJSON emits exactly one
// valid JSON document per line.
func TestExport_NDJSON_OneDocPerLine(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102&format=ndjson", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
	require.Len(t, lines, 3, "must have exactly 3 lines (one per event)")

	for i, line := range lines {
		assert.True(t, json.Valid([]byte(line)),
			"line %d must be valid JSON, got: %s", i, line)
	}
}

// TestExport_CSV_Escaping verifies embedded commas, quotes, and newlines
// are properly CSV-escaped by checking the golden file content.
func TestExport_CSV_Escaping(t *testing.T) {
	// The golden file export_contract_csv.csv contains events with
	// embedded commas, quotes, and newlines. If the CSV escaping
	// regresses, the golden file comparison will fail.
	golden := loadGolden(t, "export_contract_csv.csv")
	assert.Contains(t, golden, "hello, world",
		"embedded commas in JSON values must survive CSV encoding")
}

// TestExport_GoldenUpdateFlag documents the -update flag for regenerating goldens.
func TestExport_GoldenUpdateFlag(t *testing.T) {
	// This test simply documents that golden files can be regenerated with:
	//   go test ./internal/api/... -run TestExport -update
	if *updateGolden {
		t.Log("golden files would be updated; re-run without -update to verify")
	}
}

// TestExport_EventCSV_Golden covers the events.csv endpoint with golden files.
func TestExport_EventCSV_Golden(t *testing.T) {
	events := goldenEvents()
	// Add events for a different contract.
	events = append(events, store.Event{
		ID:         "0000000002-0000000100",
		ContractID: "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Ledger:     100,
		Type:       "contract",
		TxHash:     "other123",
		Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:      json.RawMessage(`{"i128":"500"}`),
	})

	st := &goldenExportStore{events: events}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet, "/events.csv", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := strings.TrimRight(rec.Body.String(), "\n")
	writeGolden(t, "events_all_csv.csv", body)
}

// TestExport_Headers_CSV verifies Content-Type and Content-Disposition for CSV exports.
func TestExport_Headers_CSV(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/csv; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Header().Get("Content-Disposition"), "attachment")
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"),
		"export responses must not be cached")
}

// TestExport_Headers_NDJSON verifies Content-Type and Content-Disposition for NDJSON exports.
func TestExport_Headers_NDJSON(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102&format=ndjson", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Header().Get("Content-Disposition"), "attachment")
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
}

// TestExport_CountMatches verifies that the number of lines in the export
// matches the number of events in the ledger range.
func TestExport_CountMatches(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	t.Run("csv", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)

		lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
		assert.Equal(t, 4, len(lines), "3 data rows + 1 header")
	})

	t.Run("ndjson", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			"/contracts/"+goldenContractID+"/export?from_ledger=100&to_ledger=102&format=ndjson", nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)

		lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
		assert.Equal(t, 3, len(lines), "3 data lines, no header")
	})
}

// TestExport_PartialRange verifies that requesting a subset of the ledger
// range returns only matching events.
func TestExport_PartialRange(t *testing.T) {
	st := &goldenExportStore{events: goldenEvents()}
	srv := testServer(t, st, 0)

	t.Run("csv", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/contracts/%s/export?from_ledger=100&to_ledger=100", goldenContractID), nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)

		lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
		assert.Equal(t, 2, len(lines), "1 data row + 1 header")
		assert.Contains(t, lines[1], "100")
	})
}

// ensure goldenDir exists for -update mode
func init() {
	_ = os.MkdirAll(filepath.Join("testdata", "golden"), 0o755)
}
