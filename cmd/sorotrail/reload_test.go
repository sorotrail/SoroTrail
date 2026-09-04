package main

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
)

// newTestReloadIngester builds a real *ingester.Ingester for reload tests.
// applyReload only ever calls SetPollInterval on it — never Run, never the
// RPC client or store — so nil client/store are safe stand-ins here.
func newTestReloadIngester(pollInterval time.Duration) *ingester.Ingester {
	return ingester.New(nil, nil, decode.XDRDecoder{}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), ingester.Options{
		PollInterval: pollInterval,
	})
}

// setBaseReloadEnv sets the environment variables config.Load needs to
// succeed, isolated from whatever the host environment happens to carry,
// so reload tests are deterministic regardless of ambient env vars.
func setBaseReloadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("RPC_URL", "https://rpc.example.com")
	t.Setenv("POLL_INTERVAL", "5s")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("LOG_FORMAT", "text")
}

func TestApplyReload_ValidChangeAppliesPollIntervalAndLogLevel(t *testing.T) {
	setBaseReloadEnv(t)
	t.Setenv("POLL_INTERVAL", "9s")
	t.Setenv("LOG_LEVEL", "debug")

	old := config.Config{DatabaseURL: "postgres://localhost/test", RPCURL: "https://rpc.example.com", PollInterval: 5 * time.Second, LogLevel: "info", LogFormat: "text"}
	ing := newTestReloadIngester(5 * time.Second)
	var level slog.LevelVar
	level.Set(slog.LevelInfo)
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	next, err := applyReload(old, log, ing, &level)
	require.NoError(t, err)
	assert.Equal(t, 9*time.Second, next.PollInterval)
	assert.Equal(t, 9*time.Second, ing.PollInterval(), "the running ingester must observe the new interval immediately")
	assert.Equal(t, slog.LevelDebug, level.Level(), "the shared LevelVar must reflect the new log level immediately")
	assert.Contains(t, buf.String(), "config reloaded via SIGHUP")
}

func TestApplyReload_InvalidEnvRejectedKeepsOldConfigActive(t *testing.T) {
	setBaseReloadEnv(t)
	t.Setenv("LOG_LEVEL", "verbose") // not one of debug|info|warn|error

	old := config.Config{DatabaseURL: "postgres://localhost/test", RPCURL: "https://rpc.example.com", PollInterval: 5 * time.Second, LogLevel: "info", LogFormat: "text"}
	ing := newTestReloadIngester(5 * time.Second)
	var level slog.LevelVar
	level.Set(slog.LevelWarn)
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	next, err := applyReload(old, log, ing, &level)
	require.Error(t, err)
	assert.Equal(t, old, next, "a rejected reload must return the old config unchanged")
	assert.Equal(t, 5*time.Second, ing.PollInterval(), "a rejected reload must not touch the running ingester")
	assert.Equal(t, slog.LevelWarn, level.Level(), "a rejected reload must not touch the running log level")
}

func TestApplyReload_NonPositivePollIntervalRejected(t *testing.T) {
	setBaseReloadEnv(t)
	t.Setenv("POLL_INTERVAL", "0s")

	old := config.Config{DatabaseURL: "postgres://localhost/test", RPCURL: "https://rpc.example.com", PollInterval: 5 * time.Second, LogLevel: "info", LogFormat: "text"}
	ing := newTestReloadIngester(5 * time.Second)
	var level slog.LevelVar
	level.Set(slog.LevelInfo)
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	next, err := applyReload(old, log, ing, &level)
	require.Error(t, err, "POLL_INTERVAL=0 must fail config.Load's own validation")
	assert.Equal(t, old, next)
	assert.Equal(t, 5*time.Second, ing.PollInterval())
}

func TestWarnIgnoredTopologyChanges(t *testing.T) {
	tests := []struct {
		name    string
		old     config.Config
		next    config.Config
		wantLog string
	}{
		{
			name:    "database url change is flagged",
			old:     config.Config{DatabaseURL: "postgres://a/db"},
			next:    config.Config{DatabaseURL: "postgres://b/db"},
			wantLog: "DATABASE_URL changed",
		},
		{
			name:    "rpc url change is flagged",
			old:     config.Config{RPCURL: "https://a.example.com"},
			next:    config.Config{RPCURL: "https://b.example.com"},
			wantLog: "RPC_URL changed",
		},
		{
			name:    "rpc urls list change is flagged",
			old:     config.Config{RPCURLS: []string{"https://a.example.com"}},
			next:    config.Config{RPCURLS: []string{"https://a.example.com", "https://b.example.com"}},
			wantLog: "RPC_URLS changed",
		},
		{
			name:    "log format change is flagged",
			old:     config.Config{LogFormat: "text"},
			next:    config.Config{LogFormat: "json"},
			wantLog: "LOG_FORMAT changed",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			warnIgnoredTopologyChanges(tc.old, tc.next, log)
			assert.Contains(t, buf.String(), tc.wantLog)
		})
	}
}

func TestWarnIgnoredTopologyChanges_NoChangeNoWarning(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	cfg := config.Config{
		DatabaseURL: "postgres://a/db",
		RPCURL:      "https://a.example.com",
		RPCURLS:     []string{"https://a.example.com"},
		LogFormat:   "text",
	}
	warnIgnoredTopologyChanges(cfg, cfg, log)
	assert.Empty(t, buf.String(), "identical configs must not produce any topology warning")
}
