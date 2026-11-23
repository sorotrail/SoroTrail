package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/workerhealth"
)

func createMigratedPreflightTestDB(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "preflight.db")
	db, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer db.Close()

	total, err := store.CountEmbeddedMigrations()
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE schema_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO schema_migrations (version, dirty) VALUES (?, 0)`, total)
	require.NoError(t, err)
	return dbPath
}

func newPreflightTestRPCServer(t *testing.T, passphrase string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "getNetwork":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"result": map[string]any{
					"passphrase":      passphrase,
					"protocolVersion": 21,
				},
			})
		case "getHealth":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"result": map[string]any{
					"status":                "healthy",
					"latestLedger":          2000,
					"oldestLedger":          100,
					"ledgerRetentionWindow": 1901,
				},
			})
		case "getLatestLedger":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"result": map[string]any{
					"id":              "h",
					"protocolVersion": 21,
					"sequence":        2000,
				},
			})
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testnetPassphrase(t *testing.T) string {
	t.Helper()
	nc, ok := config.NetworkConfigFor("testnet")
	require.True(t, ok)
	return nc.Passphrase
}

func TestPreflightCLI_HumanAndJSONOutput(t *testing.T) {
	dbPath := createMigratedPreflightTestDB(t)

	testnetPass := testnetPassphrase(t)
	rpcSrv := newPreflightTestRPCServer(t, testnetPass)

	t.Setenv("DATABASE_URL", dbPath)
	t.Setenv("RPC_URL", rpcSrv.URL)
	t.Setenv("NETWORK", "testnet")

	t.Run("human-readable output passes with exit code 0", func(t *testing.T) {
		var out bytes.Buffer
		err := runPreflightTo(context.Background(), nil, &out, workerhealth.PreflightOptions{
			WriteProbe: true,
		})
		require.NoError(t, err)
		assert.Equal(t, exitOK, exitCode(err))

		text := out.String()
		assert.Contains(t, text, "CHECK")
		assert.Contains(t, text, "STATUS")
		assert.Contains(t, text, "config")
		assert.Contains(t, text, "database")
		assert.Contains(t, text, "migrations")
		assert.Contains(t, text, "rpc")
		assert.Contains(t, text, "archive")
		assert.Contains(t, text, "retention")
		assert.Contains(t, text, "preflight: PASSED")
	})

	t.Run("--json output produces machine-readable PreflightReport", func(t *testing.T) {
		var out bytes.Buffer
		err := runPreflightTo(context.Background(), []string{"--json", "--check", "database,rpc"}, &out, workerhealth.PreflightOptions{})
		require.NoError(t, err)
		assert.Equal(t, exitOK, exitCode(err))

		var report workerhealth.PreflightReport
		require.NoError(t, json.Unmarshal(out.Bytes(), &report))
		assert.Equal(t, workerhealth.StatusOK, report.Status)
		assert.Equal(t, workerhealth.ExitOK, report.ExitCode)
		require.Len(t, report.Checks, 2)
		assert.Equal(t, workerhealth.CheckDatabase, report.Checks[0].Name)
		assert.Equal(t, workerhealth.CheckRPC, report.Checks[1].Name)
	})
}

func TestPreflightCLI_ExitCodes(t *testing.T) {
	testnetPass := testnetPassphrase(t)
	rpcSrv := newPreflightTestRPCServer(t, testnetPass)

	t.Run("invalid config exits with code 2 (exitConfigError)", func(t *testing.T) {
		t.Setenv("DATABASE_URL", ":memory:")
		t.Setenv("RPC_URL", rpcSrv.URL)
		t.Setenv("LOG_LEVEL", "invalid_level")

		var out bytes.Buffer
		err := runPreflightTo(context.Background(), []string{"--json"}, &out, workerhealth.PreflightOptions{})
		require.Error(t, err)
		assert.Equal(t, exitConfigError, exitCode(err))

		var report workerhealth.PreflightReport
		require.NoError(t, json.Unmarshal(out.Bytes(), &report))
		assert.Equal(t, workerhealth.ExitConfigError, report.ExitCode)
		require.Len(t, report.Checks, 1)
		assert.Equal(t, workerhealth.CheckConfig, report.Checks[0].Name)
		assert.NotEmpty(t, report.Checks[0].Remediation)
	})

	t.Run("invalid CLI flag exits with code 2 (exitConfigError)", func(t *testing.T) {
		var out bytes.Buffer
		err := runPreflightTo(context.Background(), []string{"--check", "nonexistent"}, &out, workerhealth.PreflightOptions{})
		require.Error(t, err)
		assert.Equal(t, exitConfigError, exitCode(err))
	})

	t.Run("dependency failure exits with code 1 (exitRuntimeError)", func(t *testing.T) {
		t.Setenv("DATABASE_URL", filepath.Join(t.TempDir(), "unmigrated.db"))
		t.Setenv("RPC_URL", rpcSrv.URL)
		t.Setenv("NETWORK", "mainnet") // mismatch against testnet RPC

		var out bytes.Buffer
		err := runPreflightTo(context.Background(), nil, &out, workerhealth.PreflightOptions{})
		require.Error(t, err)
		assert.Equal(t, exitRuntimeError, exitCode(err))
		assert.Contains(t, out.String(), "remediation:")
		assert.Contains(t, out.String(), "preflight: FAILED")
	})
}

func TestStartupCatchesRPCNetworkMismatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "startup.db")
	testnetPass := testnetPassphrase(t)
	rpcSrv := newPreflightTestRPCServer(t, testnetPass)

	t.Setenv("DATABASE_URL", dbPath)
	t.Setenv("RPC_URL", rpcSrv.URL)
	t.Setenv("NETWORK", "mainnet")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")

	err := runService(false)
	require.Error(t, err)
	var mismatch *workerhealth.NetworkPassphraseMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.Equal(t, "mainnet", mismatch.ConfiguredNetwork)
	assert.Equal(t, "testnet", mismatch.ActualNetwork)
}
