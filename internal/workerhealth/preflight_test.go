package workerhealth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

func createMigratedSQLiteDB(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "migrated.db")
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

func newMockRPCServer(t *testing.T, passphrase, healthStatus string, latestLedger, oldestLedger int64, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(delay):
			}
		}
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
					"status":                healthStatus,
					"latestLedger":          latestLedger,
					"oldestLedger":          oldestLedger,
					"ledgerRetentionWindow": latestLedger - oldestLedger + 1,
				},
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

type fakeArchiveProber struct {
	mu          sync.Mutex
	exists      bool
	existsErr   error
	putErr      error
	removeErr   error
	objects     map[string][]byte
	putCount    int
	removeCount int
}

func newFakeArchiveProber(exists bool) *fakeArchiveProber {
	return &fakeArchiveProber{
		exists:  exists,
		objects: make(map[string][]byte),
	}
}

func (f *fakeArchiveProber) BucketExists(_ context.Context, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.exists, f.existsErr
}

func (f *fakeArchiveProber) PutObject(_ context.Context, _, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCount++
	if f.putErr != nil {
		return f.putErr
	}
	f.objects[key] = append([]byte(nil), data...)
	return nil
}

func (f *fakeArchiveProber) RemoveObject(_ context.Context, _, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeCount++
	if f.removeErr != nil {
		return f.removeErr
	}
	delete(f.objects, key)
	return nil
}

func validTestConfig(dbURL, rpcURL string) config.Config {
	return config.Config{
		DatabaseURL: dbURL,
		RPCURL:      rpcURL,
		Network:     "testnet",
		LogLevel:    "info",
		LogFormat:   "text",
	}
}

func mustPassphrase(t *testing.T, network string) string {
	t.Helper()
	nc, ok := config.NetworkConfigFor(network)
	require.True(t, ok)
	return nc.Passphrase
}

func TestRunPreflight_AllChecksPass(t *testing.T) {
	dbPath := createMigratedSQLiteDB(t)

	testnetPass := mustPassphrase(t, "testnet")
	rpcSrv := newMockRPCServer(t, testnetPass, "healthy", 5000, 1000, 0)
	prober := newFakeArchiveProber(true)

	cfg := validTestConfig(dbPath, rpcSrv.URL)
	cfg.ArchiveBucket = "sorotrail-archive"
	cfg.ArchivePrefix = "testnet"
	cfg.ArchiveBeforePrune = true
	cfg.RetentionMaxAge = 24 * time.Hour

	report := RunPreflight(context.Background(), PreflightOptions{
		Timeout:    5 * time.Second,
		WriteProbe: true,
		LoadConfig: func() (config.Config, error) { return cfg, nil },
		ArchiveProberFactory: func(config.Config) (ArchiveProber, error) {
			return prober, nil
		},
	})

	assert.Equal(t, StatusOK, report.Status)
	assert.Equal(t, ExitOK, report.ExitCode)
	require.Len(t, report.Checks, 6)
	for _, chk := range report.Checks {
		assert.Equal(t, StatusOK, chk.Status, "check %s should pass: %+v", chk.Name, chk)
		assert.Empty(t, chk.Error)
		assert.Empty(t, chk.Remediation)
	}
	assert.Equal(t, 1, prober.putCount)
	assert.Equal(t, 1, prober.removeCount)
	assert.Empty(t, prober.objects, "temporary write-probe object must be cleaned up")
}

func TestRunPreflight_FailureCasesForEachCheck(t *testing.T) {
	testnetPass := mustPassphrase(t, "testnet")
	mainnetPass := mustPassphrase(t, "mainnet")

	t.Run("invalid config returns ExitConfigError (2)", func(t *testing.T) {
		report := RunPreflight(context.Background(), PreflightOptions{
			LoadConfig: func() (config.Config, error) {
				return config.Config{}, errors.New(`LOG_LEVEL: "loud" must be one of debug|info|warn|error`)
			},
		})
		assert.Equal(t, StatusFailed, report.Status)
		assert.Equal(t, ExitConfigError, report.ExitCode)
		require.Len(t, report.Checks, 1)
		assert.Equal(t, CheckConfig, report.Checks[0].Name)
		assert.Equal(t, StatusFailed, report.Checks[0].Status)
		assert.Contains(t, report.Checks[0].Error, "LOG_LEVEL")
		assert.NotEmpty(t, report.Checks[0].Remediation)
	})

	t.Run("unreachable database fails with remediation and ExitDependencyError (1)", func(t *testing.T) {
		rpcSrv := newMockRPCServer(t, testnetPass, "healthy", 500, 100, 0)
		cfg := validTestConfig("postgres://sorotrail:topsecret@127.0.0.1:1/sorotrail?connect_timeout=1", rpcSrv.URL)

		report := RunPreflight(context.Background(), PreflightOptions{
			Checks:     []string{CheckConfig, CheckDatabase},
			Timeout:    2 * time.Second,
			LoadConfig: func() (config.Config, error) { return cfg, nil },
		})
		assert.Equal(t, StatusFailed, report.Status)
		assert.Equal(t, ExitDependencyError, report.ExitCode)

		var dbRes PreflightCheckResult
		for _, c := range report.Checks {
			if c.Name == CheckDatabase {
				dbRes = c
			}
		}
		assert.Equal(t, StatusFailed, dbRes.Status)
		assert.NotContains(t, dbRes.Error, "topsecret")
		assert.NotEmpty(t, dbRes.Remediation)
	})

	t.Run("pending migrations and missing permission fail with remediation", func(t *testing.T) {
		// Unmigrated SQLite file has 0 applied migrations and remains untouched (read-only).
		unmigratedPath := filepath.Join(t.TempDir(), "unmigrated.db")
		rpcSrv := newMockRPCServer(t, testnetPass, "healthy", 500, 100, 0)
		cfg := validTestConfig(unmigratedPath, rpcSrv.URL)

		report := RunPreflight(context.Background(), PreflightOptions{
			Checks:     []string{CheckMigrations},
			LoadConfig: func() (config.Config, error) { return cfg, nil },
		})
		assert.Equal(t, StatusFailed, report.Status)
		assert.Equal(t, ExitDependencyError, report.ExitCode)
		require.Len(t, report.Checks, 1)
		assert.Equal(t, StatusFailed, report.Checks[0].Status)
		assert.Contains(t, report.Checks[0].Error, "pending migration")
		assert.NotEmpty(t, report.Checks[0].Remediation)

		// Verify read-only: unmigrated file was not created on disk.
		_, statErr := os.Stat(unmigratedPath)
		assert.True(t, os.IsNotExist(statErr), "migrations check must not create SQLite file on disk")

		// Also verify permission failure when migrations are behind and user lacks CREATE privilege.
		permReport := RunPreflight(context.Background(), PreflightOptions{
			Checks:                 []string{CheckMigrations},
			AllowPendingMigrations: true,
			LoadConfig:             func() (config.Config, error) { return cfg, nil },
			InspectMigrations: func(context.Context, string) (MigrationInspection, error) {
				return MigrationInspection{
					AppliedVersion:  3,
					Dirty:           false,
					TotalMigrations: 14,
					CanApply:        false,
					PermissionNote:  "current PostgreSQL role lacks CREATE privilege on schema",
				}, nil
			},
		})
		assert.Equal(t, StatusFailed, permReport.Status)
		assert.Contains(t, permReport.Checks[0].Error, "lacks CREATE privilege")
		assert.Contains(t, permReport.Checks[0].Remediation, "CREATE")
	})

	t.Run("wrong RPC network passphrase fails with remediation", func(t *testing.T) {
		// RPC node is testnet, but config says mainnet.
		rpcSrv := newMockRPCServer(t, testnetPass, "healthy", 500, 100, 0)
		cfg := validTestConfig(":memory:", rpcSrv.URL)
		cfg.Network = "mainnet"

		report := RunPreflight(context.Background(), PreflightOptions{
			Checks:     []string{CheckRPC},
			LoadConfig: func() (config.Config, error) { return cfg, nil },
		})
		assert.Equal(t, StatusFailed, report.Status)
		assert.Equal(t, ExitDependencyError, report.ExitCode)
		require.Len(t, report.Checks, 1)
		assert.Equal(t, StatusFailed, report.Checks[0].Status)
		assert.Contains(t, report.Checks[0].Error, "testnet")
		assert.Contains(t, report.Checks[0].Error, "mainnet")
		assert.Contains(t, report.Checks[0].Error, mainnetPass)
		assert.Contains(t, report.Checks[0].Remediation, "NETWORK")
	})

	t.Run("archive read-only credentials fail write probe with remediation", func(t *testing.T) {
		prober := newFakeArchiveProber(true)
		prober.putErr = errors.New("AccessDenied: read-only credentials")

		cfg := validTestConfig(":memory:", "http://127.0.0.1:8545")
		cfg.ArchiveBucket = "sorotrail-archive"

		report := RunPreflight(context.Background(), PreflightOptions{
			Checks:     []string{CheckArchive},
			WriteProbe: true,
			LoadConfig: func() (config.Config, error) { return cfg, nil },
			ArchiveProberFactory: func(config.Config) (ArchiveProber, error) {
				return prober, nil
			},
		})
		assert.Equal(t, StatusFailed, report.Status)
		assert.Equal(t, ExitDependencyError, report.ExitCode)
		require.Len(t, report.Checks, 1)
		assert.Contains(t, report.Checks[0].Error, "AccessDenied")
		assert.Contains(t, report.Checks[0].Remediation, "s3:PutObject")
	})

	t.Run("incoherent retention and archival settings fail with remediation", func(t *testing.T) {
		cfg := validTestConfig(":memory:", "http://127.0.0.1:8545")
		cfg.ArchiveBucket = "sorotrail-archive"
		cfg.RetentionMaxAge = 48 * time.Hour
		cfg.ArchiveBeforePrune = false // would prune before archiving

		report := RunPreflight(context.Background(), PreflightOptions{
			Checks:     []string{CheckRetention},
			LoadConfig: func() (config.Config, error) { return cfg, nil },
		})
		assert.Equal(t, StatusFailed, report.Status)
		assert.Equal(t, ExitDependencyError, report.ExitCode)
		require.Len(t, report.Checks, 1)
		assert.Contains(t, report.Checks[0].Error, "ARCHIVE_BEFORE_PRUNE is false")
		assert.Contains(t, report.Checks[0].Remediation, "ARCHIVE_BEFORE_PRUNE=true")
	})
}

func TestRunPreflight_ConcurrentChecksBoundedTimeout(t *testing.T) {
	testnetPass := mustPassphrase(t, "testnet")
	// runRPCCheck makes 2 requests (getNetwork + getHealth); 120ms per request = ~240ms total.
	// InspectMigrations also sleeps 240ms. Sequential would be >= 480ms; concurrent finishes in ~240ms.
	rpcSrv := newMockRPCServer(t, testnetPass, "healthy", 1000, 100, 120*time.Millisecond)

	cfg := validTestConfig(":memory:", rpcSrv.URL)

	start := time.Now()
	report := RunPreflight(context.Background(), PreflightOptions{
		Checks:     []string{CheckMigrations, CheckRPC},
		Timeout:    2 * time.Second,
		LoadConfig: func() (config.Config, error) { return cfg, nil },
		InspectMigrations: func(ctx context.Context, _ string) (MigrationInspection, error) {
			select {
			case <-ctx.Done():
				return MigrationInspection{}, ctx.Err()
			case <-time.After(240 * time.Millisecond):
			}
			return MigrationInspection{AppliedVersion: 14, TotalMigrations: 14, CanApply: true}, nil
		},
	})
	elapsed := time.Since(start)

	assert.Equal(t, StatusOK, report.Status)
	assert.Less(t, elapsed, 420*time.Millisecond, "independent checks must run concurrently")
}

func TestRunPreflight_NeverPrintsSecretsOnFailure(t *testing.T) {
	dbSecret := "db_s3cr3t_pw_99"
	rpcSecret := "rpc_tok3n_abc123"
	s3Secret := "aws_s3cr3t_key_xyz"

	cfg := validTestConfig(
		"postgres://admin:"+dbSecret+"@127.0.0.1:1/sorotrail",
		"http://user:"+rpcSecret+"@127.0.0.1:1",
	)
	cfg.ArchiveBucket = "my-bucket"
	cfg.ArchiveSecretAccessKey = s3Secret

	report := RunPreflight(context.Background(), PreflightOptions{
		Timeout:    500 * time.Millisecond,
		LoadConfig: func() (config.Config, error) { return cfg, nil },
		ArchiveProberFactory: func(config.Config) (ArchiveProber, error) {
			return nil, errors.New("auth failed with key " + s3Secret)
		},
	})

	rawJSON, err := json.Marshal(report)
	require.NoError(t, err)
	serialized := string(rawJSON)

	assert.False(t, strings.Contains(serialized, dbSecret), "database password must never appear in report")
	assert.False(t, strings.Contains(serialized, rpcSecret), "RPC password must never appear in report")
	assert.False(t, strings.Contains(serialized, s3Secret), "archive secret key must never appear in report")
}

func TestVerifyRPCNetworkOnStartup(t *testing.T) {
	testnetPass := mustPassphrase(t, "testnet")
	rpcSrv := newMockRPCServer(t, testnetPass, "healthy", 100, 10, 0)

	// Matching network succeeds.
	err := VerifyRPCNetworkOnStartup(context.Background(), rpcSrv.Client(), []string{rpcSrv.URL}, "testnet")
	require.NoError(t, err)

	// Mismatched network fails with NetworkPassphraseMismatchError.
	err = VerifyRPCNetworkOnStartup(context.Background(), rpcSrv.Client(), []string{rpcSrv.URL}, "mainnet")
	require.Error(t, err)
	var mismatch *NetworkPassphraseMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.Equal(t, "mainnet", mismatch.ConfiguredNetwork)
	assert.Equal(t, "testnet", mismatch.ActualNetwork)

	// Unreachable endpoint is tolerated on startup so cold-start resilience works.
	err = VerifyRPCNetworkOnStartup(context.Background(), rpcSrv.Client(), []string{"http://127.0.0.1:1"}, "testnet")
	require.NoError(t, err)
}
