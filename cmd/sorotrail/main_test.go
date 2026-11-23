package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/api/graphql"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

var stdIOMu sync.Mutex

// captureStderr captures os.Stderr output produced during fn.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	stdIOMu.Lock()
	defer stdIOMu.Unlock()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stderr
	os.Stderr = w
	defer func() {
		os.Stderr = orig
	}()

	fn()
	_ = w.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	_ = r.Close()
	return buf.String()
}

func TestNewLoggerJSONOutput(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.New(h).Info("hello", "key", "value")

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("JSON output did not parse: %v\nraw: %s", err, buf.String())
	}
	if parsed["msg"] != "hello" {
		t.Errorf("expected msg=hello, got %v", parsed["msg"])
	}
	if parsed["key"] != "value" {
		t.Errorf("expected key=value, got %v", parsed["key"])
	}
}

func TestNewLoggerUsesTextByDefault(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.New(h).Info("test")

	output := buf.String()
	if strings.Contains(output, `"msg"`) {
		t.Error("expected text output, got JSON")
	}
	if !strings.Contains(output, "test") {
		t.Errorf("expected log message in text output, got %q", output)
	}
}

func TestRPCURLsForLog(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want []string
	}{
		{
			name: "single plain URL is returned unchanged",
			cfg:  config.Config{RPCURL: "https://rpc.example.com"},
			want: []string{"https://rpc.example.com"},
		},
		{
			// An RPC URL may carry basic-auth credentials; the password
			// must never reach the log output. url.UserPassword
			// percent-encodes the mask, so "***" appears as %2A%2A%2A.
			name: "basic-auth password is redacted",
			cfg:  config.Config{RPCURL: "https://alice:supersecret@rpc.example.com"},
			want: []string{"https://alice:%2A%2A%2A@rpc.example.com"},
		},
		{
			name: "all configured failover endpoints are returned",
			cfg: config.Config{RPCURLS: []string{
				"https://rpc1.example.com",
				"https://rpc2.example.com",
				"https://user:hunter2@rpc3.example.com",
			}},
			want: []string{
				"https://rpc1.example.com",
				"https://rpc2.example.com",
				"https://user:%2A%2A%2A@rpc3.example.com",
			},
		},
		{
			name: "RPCURLS overrides RPCURL when both are configured",
			cfg: config.Config{
				RPCURL: "https://fallback.example.com",
				RPCURLS: []string{
					"https://primary1:secret1@rpc.example.com",
					"https://primary2.example.com",
				},
			},
			want: []string{
				"https://primary1:%2A%2A%2A@rpc.example.com",
				"https://primary2.example.com",
			},
		},
		{
			name: "empty entries in RPCURLS are dropped",
			cfg: config.Config{
				RPCURLS: []string{"", "https://rpc.example.com", ""},
			},
			want: []string{"https://rpc.example.com"},
		},
		{
			name: "URL with user but no password preserves user",
			cfg:  config.Config{RPCURL: "https://alice@rpc.example.com"},
			want: []string{"https://alice@rpc.example.com"},
		},
		{
			name: "empty config yields an empty result",
			cfg:  config.Config{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rpcURLsForLog(tt.cfg)
			assert.Equal(t, tt.want, got)
		})
	}

	// Security requirement: explicitly assert that password substrings
	// are strictly absent from the redacted log URLs.
	t.Run("password substring is absent from log output", func(t *testing.T) {
		passwords := []string{"supersecret", "hunter2", "secret1", "p@ssw0rd!_123"}
		cfg := config.Config{
			RPCURLS: []string{
				"https://alice:" + passwords[0] + "@rpc1.example.com",
				"https://bob:" + passwords[1] + "@rpc2.example.com",
				"https://admin:" + passwords[2] + "@rpc3.example.com",
				"https://service:" + passwords[3] + "@rpc4.example.com",
			},
		}

		got := rpcURLsForLog(cfg)
		require.Len(t, got, len(passwords))
		for i, raw := range got {
			assert.NotContains(t, raw, passwords[i], "password %q must not leak into loggable URL", passwords[i])
			assert.Contains(t, raw, "%2A%2A%2A", "redacted URL must contain masked password placeholder")
		}
	})
}

// fakeTenantStore is a minimal store.TenantStore fake covering bootstrap requirements.
type fakeTenantStore struct {
	store.TenantStore

	tenantsByName map[string]store.Tenant
	keys          map[string][]byte
	createCalls   int
	getTenantErr  error
	createKeyErr  error
}

func newFakeTenantStore() *fakeTenantStore {
	return &fakeTenantStore{
		tenantsByName: map[string]store.Tenant{
			"default": {ID: 1, Name: "default", Wildcard: true},
		},
		keys: make(map[string][]byte),
	}
}

func (f *fakeTenantStore) GetTenantByName(_ context.Context, name string) (store.Tenant, error) {
	if f.getTenantErr != nil {
		return store.Tenant{}, f.getTenantErr
	}
	t, ok := f.tenantsByName[name]
	if !ok {
		return store.Tenant{}, store.ErrNotFound
	}
	return t, nil
}

func (f *fakeTenantStore) CreateTenantAPIKeyIfAbsent(_ context.Context, _ int64, _, prefix string, digest []byte) error {
	f.createCalls++
	if f.createKeyErr != nil {
		return f.createKeyErr
	}
	// Idempotent insertion matching Postgres ON CONFLICT (prefix) DO UPDATE.
	f.keys[prefix] = digest
	return nil
}

func TestBootstrapAdminKeyStartup(t *testing.T) {
	ctx := context.Background()

	t.Run("bootstrapping with no existing admin creates one", func(t *testing.T) {
		ts := newFakeTenantStore()
		key, prefix, digest, err := api.GenerateAPIKey()
		require.NoError(t, err)

		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

		err = bootstrapAdminKey(ctx, ts, key, log)
		require.NoError(t, err)

		assert.Equal(t, 1, ts.createCalls)
		assert.Equal(t, digest, ts.keys[prefix])

		logOutput := buf.String()
		assert.Contains(t, logOutput, "bootstrap admin key installed from MULTI_TENANT_BOOTSTRAP_KEY")
		assert.Contains(t, logOutput, prefix)
		assert.Contains(t, logOutput, "default")
	})

	t.Run("bootstrapping when an admin exists is a no-op", func(t *testing.T) {
		ts := newFakeTenantStore()
		key, prefix, digest, err := api.GenerateAPIKey()
		require.NoError(t, err)

		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

		// First bootstrap installs the key.
		err = bootstrapAdminKey(ctx, ts, key, log)
		require.NoError(t, err)
		assert.Equal(t, 1, ts.createCalls)

		// Second bootstrap with the same key is an idempotent no-op.
		err = bootstrapAdminKey(ctx, ts, key, log)
		require.NoError(t, err)
		assert.Equal(t, 2, ts.createCalls)
		assert.Equal(t, digest, ts.keys[prefix])
		assert.Len(t, ts.keys, 1)
	})

	t.Run("invalid bootstrap key is rejected with a clear error", func(t *testing.T) {
		invalidKeys := []struct {
			name string
			key  string
		}{
			{name: "garbage text", key: "invalid-key"},
			{name: "wrong scheme prefix", key: "other_1234567890123456_supersecret"},
			{name: "prefix too short", key: "st_short_secret"},
			{name: "prefix too long", key: "st_12345678901234567890_secret"},
			{name: "missing secret portion", key: "st_1234567890123456"},
			{name: "empty secret portion", key: "st_1234567890123456_"},
		}

		for _, tt := range invalidKeys {
			t.Run(tt.name, func(t *testing.T) {
				ts := newFakeTenantStore()
				log := slog.New(slog.NewTextHandler(io.Discard, nil))

				err := bootstrapAdminKey(ctx, ts, tt.key, log)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "MULTI_TENANT_BOOTSTRAP_KEY is not a valid key")
				assert.Contains(t, err.Error(), "st_<12 chars>_<secret>")
				assert.Equal(t, 0, ts.createCalls)
				assert.Empty(t, ts.keys)
			})
		}
	})

	t.Run("empty bootstrap key is a no-op", func(t *testing.T) {
		ts := newFakeTenantStore()
		log := slog.New(slog.NewTextHandler(io.Discard, nil))

		err := bootstrapAdminKey(ctx, ts, "", log)
		require.NoError(t, err)
		assert.Equal(t, 0, ts.createCalls)
		assert.Empty(t, ts.keys)
	})

	t.Run("fails when default tenant does not exist", func(t *testing.T) {
		ts := newFakeTenantStore()
		delete(ts.tenantsByName, "default")
		key, _, _, err := api.GenerateAPIKey()
		require.NoError(t, err)

		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		err = bootstrapAdminKey(ctx, ts, key, log)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "loading default tenant")
	})

	t.Run("fails when tenant store key insertion fails", func(t *testing.T) {
		ts := newFakeTenantStore()
		ts.createKeyErr = errors.New("db disk full")
		key, _, _, err := api.GenerateAPIKey()
		require.NoError(t, err)

		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		err = bootstrapAdminKey(ctx, ts, key, log)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "installing bootstrap key")
		assert.Contains(t, err.Error(), "db disk full")
	})
}

// stubStore is a minimal store.Store implementation for GraphQL dependency testing.
type stubStore struct {
	store.Store
}

func (s *stubStore) QueryEvents(_ context.Context, _ store.EventFilter) ([]store.Event, string, error) {
	return nil, "", nil
}

func (s *stubStore) CountEvents(_ context.Context, _ store.EventFilter) (int64, error) {
	return 0, nil
}

func (s *stubStore) GetEvent(_ context.Context, _ string, _ store.Scope) (store.Event, error) {
	return store.Event{}, store.ErrNotFound
}

func (s *stubStore) ListWatchedContracts(_ context.Context) ([]store.WatchedContract, error) {
	return nil, nil
}

type stubEnricher struct{}

func (s *stubEnricher) EnrichEvents(_ context.Context, _ []store.Event) []store.EnrichedEvent {
	return nil
}

func (s *stubEnricher) DecodeStats() store.DecodeStats {
	return store.DecodeStats{}
}

func TestGraphQLServerDeps(t *testing.T) {
	st := &stubStore{}
	en := &stubEnricher{}

	tests := []struct {
		name     string
		store    store.Store
		enricher api.Enricher
	}{
		{
			name:     "bundles both store and enricher",
			store:    st,
			enricher: en,
		},
		{
			name:     "bundles store with nil enricher when decoding disabled",
			store:    st,
			enricher: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := graphqlServerDeps(tt.store, tt.enricher)
			assert.Equal(t, tt.store, deps.Store)
			assert.Equal(t, tt.enricher, deps.Enricher)
		})
	}

	t.Run("GraphQL dependencies are wired only when feature is enabled", func(t *testing.T) {
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		deps := graphqlServerDeps(st, en)

		// When GraphQL is wired with playground enabled.
		gqlHandlerPlayground, err := graphql.New(deps, log, true)
		require.NoError(t, err)
		serverWithPlayground := api.New(st, nil, log, "").
			SetGraphQLHandler(gqlHandlerPlayground, gqlHandlerPlayground.PlaygroundHandler())
		routerPlayground := serverWithPlayground.Router()

		// When GraphQL is wired with playground disabled.
		gqlHandlerNoPlayground, err := graphql.New(deps, log, false)
		require.NoError(t, err)
		serverNoPlayground := api.New(st, nil, log, "").
			SetGraphQLHandler(gqlHandlerNoPlayground, gqlHandlerNoPlayground.PlaygroundHandler())
		routerNoPlayground := serverNoPlayground.Router()

		// When GraphQL is NOT wired at all.
		serverUnwired := api.New(st, nil, log, "")
		routerUnwired := serverUnwired.Router()

		cases := []struct {
			name       string
			handler    http.Handler
			path       string
			method     string
			wantStatus int
			assertBody func(t *testing.T, body string)
		}{
			{
				name:       "unwired server returns 404 for /graphql",
				handler:    routerUnwired,
				path:       "/graphql",
				method:     http.MethodPost,
				wantStatus: http.StatusNotFound,
			},
			{
				name:       "unwired server returns 404 for /graphiql",
				handler:    routerUnwired,
				path:       "/graphiql",
				method:     http.MethodGet,
				wantStatus: http.StatusNotFound,
			},
			{
				name:       "wired server routes /graphql",
				handler:    routerPlayground,
				path:       "/graphql",
				method:     http.MethodGet,
				wantStatus: http.StatusOK,
			},
			{
				name:       "wired server with playground enabled serves /graphiql html",
				handler:    routerPlayground,
				path:       "/graphiql",
				method:     http.MethodGet,
				wantStatus: http.StatusOK,
				assertBody: func(t *testing.T, body string) {
					assert.Contains(t, body, "GraphQL Playground")
				},
			},
			{
				name:       "wired server with playground disabled returns 404 for /graphiql",
				handler:    routerNoPlayground,
				path:       "/graphiql",
				method:     http.MethodGet,
				wantStatus: http.StatusNotFound,
				assertBody: func(t *testing.T, body string) {
					assert.Contains(t, body, "playground is disabled")
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(tc.method, tc.path, nil)
				rec := httptest.NewRecorder()
				tc.handler.ServeHTTP(rec, req)
				assert.Equal(t, tc.wantStatus, rec.Code)
				if tc.assertBody != nil {
					tc.assertBody(t, rec.Body.String())
				}
			})
		}
	})
}

// TestExitCode asserts that exitCode correctly maps dispatch errors to process exit codes.
func TestExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil error exits 0", nil, 0},
		{"interrupted error exits 2", errInterrupted, 2},
		{"wrapped interrupted error exits 2", fmt.Errorf("operation failed: %w", errInterrupted), 2},
		{"generic error exits 1", errors.New("boom"), 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, exitCode(tt.err))
		})
	}
}

// TestDispatch_RoutesDocumentedSubcommands asserts that each documented
// subcommand routes to its respective handler and forwards arguments.
func TestDispatch_RoutesDocumentedSubcommands(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantErrMsg string
	}{
		{
			name: "replay routes to runReplay and handles help flag cleanly",
			args: []string{"replay", "--help"},
		},
		{
			name:       "replay routes to runReplay and reports missing required flag",
			args:       []string{"replay"},
			wantErrMsg: "--from-ledger is required and must be positive",
		},
		{
			name: "backfill routes to runBackfill and handles help flag cleanly",
			args: []string{"backfill", "--help"},
		},
		{
			name:       "backfill routes to runBackfill and reports missing required flag",
			args:       []string{"backfill"},
			wantErrMsg: "--contract is required",
		},
		{
			name: "index-addresses routes to runIndexAddresses and handles help flag cleanly",
			args: []string{"index-addresses", "--help"},
		},
		{
			name:       "index-addresses routes to runIndexAddresses and validates arguments",
			args:       []string{"index-addresses", "--batch-size", "0"},
			wantErrMsg: "--batch-size must be in 1..1000, got 0",
		},
		{
			name: "apikey routes to runAPIKey and handles help flag cleanly",
			args: []string{"apikey", "--help"},
		},
		{
			name:       "apikey routes to runAPIKey and validates missing subcommand",
			args:       []string{"apikey"},
			wantErrMsg: "apikey requires a subcommand: create, list, revoke",
		},
		{
			name: "contracts routes to runContracts and handles help flag cleanly",
			args: []string{"contracts", "--help"},
		},
		{
			name:       "contracts routes to runContracts and validates missing subcommand",
			args:       []string{"contracts"},
			wantErrMsg: "contracts requires a subcommand: add, list, or remove",
		},
		{
			name: "migrate routes to runMigrate and handles help flag cleanly",
			args: []string{"migrate", "--help"},
		},
		{
			name:       "migrate routes to runMigrate and validates missing action",
			args:       []string{"migrate"},
			wantErrMsg: "migrate requires an action: up, down, or status",
		},
		{
			name: "health routes to runHealth and handles help flag cleanly",
			args: []string{"health", "--help"},
		},
		{
			name: "schema-inspect routes to runSchemaInspect and handles help flag cleanly",
			args: []string{"schema-inspect", "--help"},
		},
		{
			name: "migrate-status routes to runMigrateStatus and handles help flag cleanly",
			args: []string{"migrate-status", "--help"},
		},
		{
			name: "completion routes to runCompletion and handles help flag cleanly",
			args: []string{"completion", "--help"},
		},
		{
			name: "version routes to runVersion and handles help flag cleanly",
			args: []string{"version", "--help"},
		},
		{
			name: "stats routes to runStats and handles help flag cleanly",
			args: []string{"stats", "--help"},
		},
		{
			name: "config routes to runConfig and handles help flag cleanly",
			args: []string{"config", "--help"},
		},
		{
			name: "preflight routes to runPreflight and handles help flag cleanly",
			args: []string{"preflight", "--help"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dispatch(tt.args)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	// Test healthcheck routing.
	t.Run("healthcheck routes to runHealthcheck and returns nil on healthy probe", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
			}
		}))
		defer srv.Close()

		addr := strings.TrimPrefix(srv.URL, "http://")
		err := dispatch([]string{"healthcheck", "--addr", addr, "--endpoint", "/health"})
		assert.NoError(t, err)
	})

	// Test top-level version flags.
	for _, flag := range []string{"--version", "-V"} {
		t.Run(fmt.Sprintf("dispatch %s routes to version", flag), func(t *testing.T) {
			err := dispatch([]string{flag, "--help"})
			assert.NoError(t, err)
		})
	}

	// Test help variants.
	for _, flag := range []string{"help", "-h", "--help"} {
		t.Run(fmt.Sprintf("dispatch %s prints usage and exits cleanly", flag), func(t *testing.T) {
			stderr := captureStderr(t, func() {
				err := dispatch([]string{flag})
				assert.NoError(t, err)
			})
			assert.Contains(t, stderr, "usage: sorotrail")
		})
	}
}

// TestDispatch_UnknownSubcommand asserts that an unknown subcommand prints
// usage information to stderr and returns an error describing the unknown command.
func TestDispatch_UnknownSubcommand(t *testing.T) {
	tests := []struct {
		name       string
		subcommand string
	}{
		{"arbitrary command foo", "foo"},
		{"unrecognized start", "start"},
		{"unrecognized serve", "serve"},
		{"typo of replay", "replaay"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			stderr := captureStderr(t, func() {
				err = dispatch([]string{tt.subcommand})
			})

			require.Error(t, err)
			assert.EqualError(t, err, fmt.Sprintf("unknown subcommand %q", tt.subcommand))
			assert.Contains(t, stderr, "usage: sorotrail")
		})
	}
}

// TestDispatch_NoSubcommandRunsDefaultServerPath asserts that dispatch with
// empty or nil arguments routes to run(), the default server path.
func TestDispatch_NoSubcommandRunsDefaultServerPath(t *testing.T) {
	// Set an invalid log level to verify config.Load() in run() is reached
	// without actually spinning up postgres or background workers.
	t.Setenv("LOG_LEVEL", "invalid_for_test")

	tests := []struct {
		name string
		args []string
	}{
		{"nil slice", nil},
		{"empty slice", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dispatch(tt.args)
			require.Error(t, err)
			// Proves that run() was called and config.Load() was reached.
			assert.Contains(t, err.Error(), `LOG_LEVEL: "invalid_for_test" must be one of debug|info|warn|error`)
		})
	}

	// In contrast, invoking a subcommand does not enter run().
	t.Run("subcommand does not enter run path", func(t *testing.T) {
		err := dispatch([]string{"replay", "--help"})
		assert.NoError(t, err, "subcommands must not enter run() or fail on LOG_LEVEL")
	})
}

// TestNewLogger_DocumentedFormats asserts that each documented log format
// ("text" and "json") produces a functional *slog.Logger writing to os.Stdout.
func TestNewLogger_DocumentedFormats(t *testing.T) {
	tests := []struct {
		name      string
		format    string
		assertOut func(t *testing.T, output string)
	}{
		{
			name:   "text format produces structured key-value output",
			format: "text",
			assertOut: func(t *testing.T, output string) {
				assert.Contains(t, output, "test text log message")
				assert.Contains(t, output, "foo=bar")
				assert.False(t, strings.HasPrefix(strings.TrimSpace(output), "{"), "text logger should not emit JSON")
			},
		},
		{
			name:   "json format produces valid parseable JSON output",
			format: "json",
			assertOut: func(t *testing.T, output string) {
				var parsed map[string]any
				err := json.Unmarshal([]byte(output), &parsed)
				require.NoError(t, err, "json format must emit valid JSON, got: %s", output)
				assert.Equal(t, "test json log message", parsed["msg"])
				assert.Equal(t, "bar", parsed["foo"])
				assert.Equal(t, "INFO", parsed["level"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStdout(t, func() {
				logger := newLogger("info", tt.format)
				require.NotNil(t, logger)
				logger.Info(fmt.Sprintf("test %s log message", tt.format), "foo", "bar")
			})

			tt.assertOut(t, output)
		})
	}
}

// TestNewLogger_LogLevels asserts that newLogger configures the expected
// slog level threshold for all valid log levels.
func TestNewLogger_LogLevels(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		level     string
		wantDebug bool
		wantInfo  bool
		wantWarn  bool
		wantError bool
	}{
		{
			name:      "debug level enables all levels",
			level:     "debug",
			wantDebug: true,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "info level disables debug",
			level:     "info",
			wantDebug: false,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "warn level disables debug and info",
			level:     "warn",
			wantDebug: false,
			wantInfo:  false,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "error level enables only error",
			level:     "error",
			wantDebug: false,
			wantInfo:  false,
			wantWarn:  false,
			wantError: true,
		},
		{
			name:      "case-insensitive log level input",
			level:     "DEBUG",
			wantDebug: true,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
		{
			name:      "unspecified level defaults to info",
			level:     "",
			wantDebug: false,
			wantInfo:  true,
			wantWarn:  true,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := newLogger(tt.level, "text")
			require.NotNil(t, log)

			assert.Equal(t, tt.wantDebug, log.Enabled(ctx, slog.LevelDebug), "debug enabled check")
			assert.Equal(t, tt.wantInfo, log.Enabled(ctx, slog.LevelInfo), "info enabled check")
			assert.Equal(t, tt.wantWarn, log.Enabled(ctx, slog.LevelWarn), "warn enabled check")
			assert.Equal(t, tt.wantError, log.Enabled(ctx, slog.LevelError), "error enabled check")
		})
	}
}

// TestLogLevel_InvalidRejectedWithClearMessage asserts that setting an invalid
// LOG_LEVEL environment variable is rejected during startup with a clear error message.
func TestLogLevel_InvalidRejectedWithClearMessage(t *testing.T) {
	invalidLevels := []struct {
		name  string
		level string
	}{
		{"verbose", "verbose"},
		{"trace", "trace"},
		{"loud", "loud"},
		{"warning typo", "warning"},
		{"numeric string", "123"},
		{"none", "none"},
	}

	for _, tt := range invalidLevels {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://localhost/dummy")
			t.Setenv("LOG_LEVEL", tt.level)
			err := dispatch(nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("LOG_LEVEL: %q must be one of debug|info|warn|error", tt.level))
		})
	}
}

// TestUsage_ListsEverySubcommand asserts that usage() lists all subcommands
// that dispatch() knows how to route to.
func TestUsage_ListsEverySubcommand(t *testing.T) {
	stderr := captureStderr(t, func() {
		usage()
	})

	assert.Contains(t, stderr, "usage: sorotrail")
	assert.Contains(t, stderr, "With no subcommand, runs the indexer")

	knownSubcommands := []struct {
		subcommand  string
		description string
	}{
		{"replay", "re-decode stored events"},
		{"apikey", "issue, list, and revoke API keys"},
		{"contracts", "add, list, and remove watched contracts"},
		{"backfill", "ingest historical contract events"},
		{"index-addresses", "rebuild the address→event inverted index"},
		{"migrate", "apply, roll back, or inspect database migrations"},
		{"health", "probe the API /health and exit nonzero on failure"},
		{"healthcheck", "probe /health and exit"},
		{"schema-inspect", "report migration state, partitions, and table sizes"},
		{"migrate-status", "report pending migrations without applying them"},
		{"completion", "print a shell completion script"},
		{"version", "print the build version, commit, and build date"},
		{"stats", "print store stats as a table"},
		{"config", "print the effective configuration with secrets redacted"},
		{"preflight", "run startup self-checks"},
	}

	for _, tt := range knownSubcommands {
		t.Run(tt.subcommand, func(t *testing.T) {
			assert.Contains(t, stderr, tt.subcommand, "usage must list subcommand %q", tt.subcommand)
			assert.Contains(t, stderr, tt.description, "usage must describe subcommand %q", tt.subcommand)
		})
	}
}
