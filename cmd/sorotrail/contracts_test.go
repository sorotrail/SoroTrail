package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testContractID = "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF"

type capturedContractsRequest struct {
	method string
	path   string
	query  url.Values
	auth   string
	body   map[string]any
}

func contractsTestServer(t *testing.T, requests *[]capturedContractsRequest, mu *sync.Mutex) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		mu.Lock()
		*requests = append(*requests, capturedContractsRequest{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.Query(),
			auth:   r.Header.Get("X-API-Key"),
			body:   body,
		})
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/watched-contracts":
			_, _ = fmt.Fprintf(w, `{"contracts":[{"contract_id":%q,"added_at":"2026-07-21T12:00:00Z"}],"count":1}`, testContractID)
		case r.Method == http.MethodPost && r.URL.Path == "/watched-contracts":
			_, _ = fmt.Fprintf(w, `{"contract_id":%q,"added_at":"2026-07-21T12:00:00Z","history_from_ledger":42,"mode_transition":"all_to_specific"}`, testContractID)
		case r.Method == http.MethodDelete && r.URL.Path == "/watched-contracts/"+testContractID:
			_, _ = fmt.Fprintf(w, `{"contract_id":%q,"removed_at":"2026-07-21T12:00:00Z","history_preserved":true,"mode_transition":"specific_to_all"}`, testContractID)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestContractsCommandsUseTheManagementAPI(t *testing.T) {
	var (
		requests []capturedContractsRequest
		mu       sync.Mutex
	)
	server := contractsTestServer(t, &requests, &mu)
	defer server.Close()

	t.Setenv("API_KEY", "environment-key")
	tests := []struct {
		name       string
		args       []string
		wantMethod string
		wantPath   string
		wantQuery  string
		wantOutput []string
	}{
		{
			name:       "add",
			args:       []string{"add", "--url", server.URL, "--confirm", testContractID},
			wantMethod: http.MethodPost,
			wantPath:   "/watched-contracts",
			wantQuery:  "confirm=true",
			wantOutput: []string{"added watched contract " + testContractID, "all_to_specific", "history starts at ledger 42"},
		},
		{
			name:       "list",
			args:       []string{"list", "--url", server.URL},
			wantMethod: http.MethodGet,
			wantPath:   "/watched-contracts",
			wantOutput: []string{"CONTRACT_ID", testContractID, "2026-07-21T12:00:00Z"},
		},
		{
			name:       "remove",
			args:       []string{"remove", "--url", server.URL, "--confirm", testContractID},
			wantMethod: http.MethodDelete,
			wantPath:   "/watched-contracts/" + testContractID,
			wantQuery:  "confirm=true",
			wantOutput: []string{"removed watched contract " + testContractID, "specific_to_all", "stored events were preserved"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := runContractsTo(&stdout, &stderr, tc.args)
			require.NoError(t, err, "stderr: %s", stderr.String())
			for _, want := range tc.wantOutput {
				assert.Contains(t, stdout.String(), want)
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, len(tests))
	for i, tc := range tests {
		got := requests[i]
		assert.Equal(t, tc.wantMethod, got.method)
		assert.Equal(t, tc.wantPath, got.path)
		assert.Equal(t, "environment-key", got.auth, "the management key must use X-API-Key")
		assert.Equal(t, tc.wantQuery, got.query.Encode())
		if tc.wantMethod == http.MethodPost {
			assert.Equal(t, testContractID, got.body["contract_id"])
		}
	}
}

func TestContractsCommandValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "missing subcommand", wantErr: "requires a subcommand"},
		{name: "unknown subcommand", args: []string{"wat"}, wantErr: "unknown contracts subcommand"},
		{name: "add missing ID", args: []string{"add", "--url", "http://example.test"}, wantErr: "contract ID is required"},
		{name: "add invalid ID", args: []string{"add", "not-a-contract"}, wantErr: "valid C-prefixed"},
		{name: "add too many IDs", args: []string{"add", testContractID, testContractID}, wantErr: "exactly one"},
		{name: "add both ID forms", args: []string{"add", "--contract", testContractID, testContractID}, wantErr: "both positionally"},
		{name: "list positional", args: []string{"list", "extra"}, wantErr: "does not accept positional"},
		{name: "bad URL", args: []string{"list", "--url", "ftp://example.test"}, wantErr: "http://"},
		{name: "URL query", args: []string{"list", "--url", "http://example.test?x=1"}, wantErr: "query string"},
		{name: "nonpositive timeout", args: []string{"list", "--timeout", "0"}, wantErr: "--timeout"},
		{name: "unknown flag", args: []string{"list", "--nope"}, wantErr: "flag provided but not defined"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := runContractsTo(&stdout, &stderr, tc.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestContractsHelpDoesNotContactAPI(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"add", "--help"}, {"list", "--help"}, {"remove", "--help"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			require.NoError(t, runContractsTo(&stdout, &stderr, args))
		})
	}
}

func TestContractsBaseURL(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":19090")
	got, err := contractsBaseURL("")
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:19090", got)

	got, err = contractsBaseURL("https://api.example.test/")
	require.NoError(t, err)
	assert.Equal(t, "https://api.example.test", got)
}
