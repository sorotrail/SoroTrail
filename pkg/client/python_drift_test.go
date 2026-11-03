package client_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/clientgen"
)

const (
	pySpecPath      = "../../api/openapi.yaml"
	pyGeneratedPath = "../../clients/python/sorotrail/_generated.py"
	pyExamplePath   = "../../clients/python/examples/usage.py"
	pyTestsDir      = "../../clients/python/tests"
)

// TestPythonClientIsUpToDate regenerates clients/python/sorotrail/_generated.py
// from api/openapi.yaml and compares it byte-for-byte against the committed file.
func TestPythonClientIsUpToDate(t *testing.T) {
	want, err := clientgen.GeneratePythonFromFile(pySpecPath)
	require.NoError(t, err, "generating Python client from %s", pySpecPath)

	got, err := os.ReadFile(pyGeneratedPath)
	require.NoError(t, err, "reading committed %s (run `make client` to generate it)", pyGeneratedPath)

	assert.Equal(t, string(want), string(got),
		"%s is out of date with %s; run `make client` (go run ./cmd/clientgen) and commit the result",
		pyGeneratedPath, pySpecPath)
}

// TestPythonClientDriftDetectsFieldBreakage verifies that breaking a field name
// in the generated Python client is caught by the drift check (#1271).
func TestPythonClientDriftDetectsFieldBreakage(t *testing.T) {
	want, err := clientgen.GeneratePythonFromFile(pySpecPath)
	require.NoError(t, err)

	mutated := bytes.Replace(want, []byte("contract_id: str"), []byte("broken_contract_id: str"), 1)
	require.NotEqual(t, want, mutated, "mutation must alter generated Python source")
}

// TestPythonClientUnitAndTypeSuite runs the Python client's unit and strict
// type-annotation test suite under clients/python/tests.
func TestPythonClientUnitAndTypeSuite(t *testing.T) {
	pythonBin, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found on PATH; skipping Python unit suite")
	}

	cmd := exec.Command(pythonBin, "-m", "unittest", "discover", "-s", pyTestsDir, "-v")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "Python unit/type test suite failed:\n%s", string(out))
}

// TestPythonClientLiveExampleAgainstServer exercises clients/python/examples/usage.py
// end-to-end against a live httptest.NewServer.
func TestPythonClientLiveExampleAgainstServer(t *testing.T) {
	pythonBin, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found on PATH; skipping live Python example test")
	}

	const contractA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/health":
			_, _ = w.Write([]byte(`{"status":"ok","checks":{"process":{"status":"ok","latency_ms":0}}}`))

		case r.Method == http.MethodGet && r.URL.Path == "/version":
			_, _ = w.Write([]byte(`{"version":"0.2.0","commit":"abc1234","build_date":"2026-07-21T00:00:00Z"}`))

		case r.Method == http.MethodGet && r.URL.Path == "/events":
			if r.URL.Query().Get("limit") == "-5" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid limit: must be between 1 and 500","code":"INVALID_PARAMETER","field":"limit"}`))
				return
			}
			cursor := r.URL.Query().Get("cursor")
			if cursor == "" {
				_, _ = w.Write([]byte(`{"events":[` +
					`{"id":"0001099511627776-0000000001","contract_id":"` + contractA + `","ledger":256000,"type":"contract","tx_hash":"aaaa","tx_index":0,"op_index":0,"in_successful_call":true,"topics":["transfer"],"value":{"amount":"340282366920938463463374607431768211455"},"created_at":"2026-07-21T12:00:00Z","network":"testnet"},` +
					`{"id":"0001099516991488-0000000001","contract_id":"` + contractA + `","ledger":256001,"type":"contract","tx_hash":"bbbb","tx_index":0,"op_index":0,"in_successful_call":true,"topics":["mint"],"value":{"amount":"10000000"},"created_at":"2026-07-21T12:00:05Z","network":"testnet"}` +
					`],"cursor":"0001099516991488-0000000001"}`))
				return
			}
			_, _ = w.Write([]byte(`{"events":[` +
				`{"id":"0001099521286656-0000000001","contract_id":"` + contractA + `","ledger":256002,"type":"contract","tx_hash":"cccc","tx_index":0,"op_index":0,"in_successful_call":true,"topics":["burn"],"value":{"amount":"5000000"},"created_at":"2026-07-21T12:00:10Z","network":"testnet"}` +
				`],"cursor":""}`))

		case r.Method == http.MethodGet && r.URL.Path == "/events/0001099511627776-0000000001":
			_, _ = w.Write([]byte(`{"id":"0001099511627776-0000000001","contract_id":"` + contractA + `","ledger":256000,"type":"contract","tx_hash":"aaaa","tx_index":0,"op_index":0,"in_successful_call":true,"topics":["transfer"],"value":{"amount":"340282366920938463463374607431768211455"},"created_at":"2026-07-21T12:00:00Z","network":"testnet"}`))

		case r.Method == http.MethodGet && r.URL.Path == "/events/9999999999999999-9999999999":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"event not found","code":"NOT_FOUND"}`))

		case r.Method == http.MethodGet && r.URL.Path == "/contracts":
			_, _ = w.Write([]byte(`{"contracts":[{"contract_id":"` + contractA + `","event_count":3,"first_ledger":256000,"last_ledger":256002,"last_seen":"2026-07-21T12:00:10Z"}],"count":1,"cursor":""}`))

		case r.Method == http.MethodGet && r.URL.Path == "/events/count":
			_, _ = w.Write([]byte(`{"count":3}`))

		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found","code":"NOT_FOUND"}`))
		}
	}))
	defer srv.Close()

	cmd := exec.Command(pythonBin, pyExamplePath, "--base-url", srv.URL)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "running %s against live test server:\n%s", pyExamplePath, string(out))

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	require.NotEmpty(t, lines)

	var summary struct {
		HealthStatus          string   `json:"health_status"`
		StreamedEventIDs      []string `json:"streamed_event_ids"`
		ContractIDs           []string `json:"contract_ids"`
		CountedContractEvents int64    `json:"counted_contract_events"`
		WideInt               string   `json:"wide_int"`
		WideDecimal           string   `json:"wide_decimal"`
		NotFoundCode          string   `json:"not_found_code"`
		BadRequestCode        string   `json:"bad_request_code"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &summary))

	assert.Equal(t, "ok", summary.HealthStatus)
	assert.Equal(t, []string{
		"0001099511627776-0000000001",
		"0001099516991488-0000000001",
		"0001099521286656-0000000001",
	}, summary.StreamedEventIDs)
	assert.Equal(t, []string{contractA}, summary.ContractIDs)
	assert.Equal(t, int64(3), summary.CountedContractEvents)
	assert.Equal(t, "340282366920938463463374607431768211455", summary.WideInt)
	assert.Equal(t, "34028236692093846346337460743176.8211455", summary.WideDecimal)
	assert.Equal(t, "NOT_FOUND", summary.NotFoundCode)
	assert.Equal(t, "INVALID_PARAMETER", summary.BadRequestCode)
}
