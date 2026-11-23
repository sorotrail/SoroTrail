package workerhealth

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	_ "modernc.org/sqlite"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Check names supported by the startup preflight self-check.
const (
	CheckConfig     = "config"
	CheckDatabase   = "database"
	CheckMigrations = "migrations"
	CheckRPC        = "rpc"
	CheckArchive    = "archive"
	CheckRetention  = "retention"
)

// CheckStatus represents the outcome of a single preflight check or the overall report.
type CheckStatus string

const (
	StatusOK      CheckStatus = "ok"
	StatusFailed  CheckStatus = "failed"
	StatusSkipped CheckStatus = "skipped"
)

// Exit codes returned by the preflight self-check.
const (
	ExitOK              = 0
	ExitDependencyError = 1
	ExitConfigError     = 2
)

// AllCheckNames returns the canonical ordered list of preflight checks.
func AllCheckNames() []string {
	return []string{
		CheckConfig,
		CheckDatabase,
		CheckMigrations,
		CheckRPC,
		CheckArchive,
		CheckRetention,
	}
}

// ParseCheckSelection validates and normalizes a list of check names (which may
// contain comma-separated entries). An empty input selects all checks.
func ParseCheckSelection(specs []string) ([]string, error) {
	if len(specs) == 0 {
		return AllCheckNames(), nil
	}
	selected := make(map[string]bool)
	for _, spec := range specs {
		for _, part := range strings.Split(spec, ",") {
			name := strings.ToLower(strings.TrimSpace(part))
			if name == "" {
				continue
			}
			switch name {
			case "db":
				name = CheckDatabase
			case "s3", "object-storage":
				name = CheckArchive
			}
			switch name {
			case CheckConfig, CheckDatabase, CheckMigrations, CheckRPC, CheckArchive, CheckRetention:
				selected[name] = true
			default:
				return nil, fmt.Errorf("unknown check %q (valid checks: %s)", part, strings.Join(AllCheckNames(), ", "))
			}
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("at least one check name must be specified (valid checks: %s)", strings.Join(AllCheckNames(), ", "))
	}
	var ordered []string
	for _, name := range AllCheckNames() {
		if selected[name] {
			ordered = append(ordered, name)
		}
	}
	return ordered, nil
}

// PreflightCheckResult holds the outcome of one named preflight check.
type PreflightCheckResult struct {
	Name        string      `json:"name"`
	Status      CheckStatus `json:"status"`
	DurationMS  int64       `json:"duration_ms"`
	Detail      string      `json:"detail,omitempty"`
	Error       string      `json:"error,omitempty"`
	Remediation string      `json:"remediation,omitempty"`
}

// PreflightReport summarizes the full preflight run in a form suitable for both
// human-readable tables and --json output.
type PreflightReport struct {
	Status     CheckStatus            `json:"status"`
	ExitCode   int                    `json:"exit_code"`
	DurationMS int64                  `json:"duration_ms"`
	Network    string                 `json:"network,omitempty"`
	Checks     []PreflightCheckResult `json:"checks"`
}

// ArchiveProber abstracts object-storage bucket and write-permission checks so
// tests can inject faults or custom transports while production uses MinIO/S3.
type ArchiveProber interface {
	BucketExists(ctx context.Context, bucket string) (bool, error)
	PutObject(ctx context.Context, bucket, key string, data []byte) error
	RemoveObject(ctx context.Context, bucket, key string) error
}

// MigrationInspection captures the read-only migration state and schema permissions
// of the target database.
type MigrationInspection struct {
	AppliedVersion  uint
	Dirty           bool
	TotalMigrations int
	CanApply        bool
	PermissionNote  string
}

// PreflightOptions configures a RunPreflight execution.
type PreflightOptions struct {
	// Checks lists the check names to run. Empty runs AllCheckNames().
	Checks []string
	// Timeout bounds the execution of dependency probes. Default 10s.
	Timeout time.Duration
	// WriteProbe controls whether the archive check writes and immediately deletes
	// a temporary probe object to verify write permission.
	WriteProbe bool
	// AllowPendingMigrations allows the migrations check to pass when migrations are
	// pending as long as the database user has permission to apply them on startup.
	AllowPendingMigrations bool
	// HTTPClient overrides the HTTP client used for RPC probes.
	HTTPClient *http.Client
	// LoadConfig overrides config.Load for testing.
	LoadConfig func() (config.Config, error)
	// InspectMigrations overrides the database migration inspector for testing.
	InspectMigrations func(ctx context.Context, databaseURL string) (MigrationInspection, error)
	// ArchiveProberFactory overrides S3 client construction for testing.
	ArchiveProberFactory func(cfg config.Config) (ArchiveProber, error)
}

// NetworkPassphraseMismatchError is returned when a Soroban RPC endpoint's
// getNetwork passphrase does not match the configured NETWORK.
type NetworkPassphraseMismatchError struct {
	Endpoint           string
	ConfiguredNetwork  string
	ExpectedPassphrase string
	ActualPassphrase   string
	ActualNetwork      string
}

func (e *NetworkPassphraseMismatchError) Error() string {
	actualLabel := fmt.Sprintf("%q", e.ActualPassphrase)
	if e.ActualNetwork != "" {
		actualLabel = fmt.Sprintf("%q (%s)", e.ActualPassphrase, e.ActualNetwork)
	}
	return fmt.Sprintf(
		"RPC endpoint %s reports network passphrase %s, which does not match configured NETWORK=%q (expected %q)",
		RedactEndpointURL(e.Endpoint),
		actualLabel,
		e.ConfiguredNetwork,
		e.ExpectedPassphrase,
	)
}

// RPCNetworkInfo holds the response fields from Soroban RPC's getNetwork and getHealth methods.
type RPCNetworkInfo struct {
	Passphrase      string
	ProtocolVersion int
	LatestLedger    int64
	OldestLedger    int64
	HealthStatus    string
}

// VerifyRPCNetwork queries the Soroban RPC endpoint's getNetwork method and
// verifies that its passphrase matches the configured network.
func VerifyRPCNetwork(ctx context.Context, httpClient *http.Client, rpcURL, configuredNetwork string) (*RPCNetworkInfo, error) {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	netCfg, ok := config.NetworkConfigFor(configuredNetwork)
	if !ok || netCfg.Passphrase == "" {
		return nil, fmt.Errorf("unknown configured network %q", configuredNetwork)
	}
	expected := netCfg.Passphrase

	var netResult struct {
		Passphrase      string `json:"passphrase"`
		ProtocolVersion int    `json:"protocolVersion"`
	}
	if err := callJSONRPC(ctx, httpClient, rpcURL, "getNetwork", &netResult); err != nil {
		return nil, fmt.Errorf("calling getNetwork on %s: %w", RedactEndpointURL(rpcURL), err)
	}
	if strings.TrimSpace(netResult.Passphrase) == "" {
		return nil, fmt.Errorf("RPC endpoint %s returned an empty network passphrase from getNetwork", RedactEndpointURL(rpcURL))
	}
	if netResult.Passphrase != expected {
		return nil, &NetworkPassphraseMismatchError{
			Endpoint:           rpcURL,
			ConfiguredNetwork:  configuredNetwork,
			ExpectedPassphrase: expected,
			ActualPassphrase:   netResult.Passphrase,
			ActualNetwork:      identifyNetworkByPassphrase(netResult.Passphrase),
		}
	}

	return &RPCNetworkInfo{
		Passphrase:      netResult.Passphrase,
		ProtocolVersion: netResult.ProtocolVersion,
	}, nil
}

// VerifyRPCNetworkOnStartup checks the configured RPC endpoints during normal
// service startup. If any endpoint explicitly reports a network passphrase that
// mismatches configuredNetwork, startup fails immediately. Transient network or
// method-not-found errors do not abort startup so cold-start resilience is preserved.
func VerifyRPCNetworkOnStartup(ctx context.Context, httpClient *http.Client, rpcURLs []string, configuredNetwork string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	for _, rawURL := range rpcURLs {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		_, err := VerifyRPCNetwork(probeCtx, httpClient, rawURL, configuredNetwork)
		var mismatch *NetworkPassphraseMismatchError
		if errors.As(err, &mismatch) {
			return mismatch
		}
	}
	return nil
}

func identifyNetworkByPassphrase(passphrase string) string {
	for _, name := range []string{"testnet", "mainnet", "futurenet"} {
		if netCfg, ok := config.NetworkConfigFor(name); ok && netCfg.Passphrase == passphrase {
			return name
		}
	}
	return ""
}

// EffectiveRPCURLs returns the configured RPC URLs in priority order:
// RPCURLS when non-empty, otherwise RPCURL.
func EffectiveRPCURLs(cfg config.Config) []string {
	var out []string
	for _, u := range cfg.RPCURLS {
		if trimmed := strings.TrimSpace(u); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) > 0 {
		return out
	}
	if trimmed := strings.TrimSpace(cfg.RPCURL); trimmed != "" {
		return []string{trimmed}
	}
	return nil
}

func callJSONRPC(ctx context.Context, httpClient *http.Client, endpoint, method string, out any) error {
	reqPayload := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
	}{
		JSONRPC: "2.0",
		ID:      1,
		Method:  method,
	}
	body, err := json.Marshal(reqPayload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var rpcEnv struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBytes, &rpcEnv); err != nil {
		return fmt.Errorf("invalid JSON-RPC response: %w", err)
	}
	if rpcEnv.Error != nil {
		return fmt.Errorf("rpc error %d: %s", rpcEnv.Error.Code, rpcEnv.Error.Message)
	}
	if len(rpcEnv.Result) == 0 || string(rpcEnv.Result) == "null" {
		return fmt.Errorf("empty JSON-RPC result for %s", method)
	}
	if out != nil {
		if err := json.Unmarshal(rpcEnv.Result, out); err != nil {
			return fmt.Errorf("decoding %s result: %w", method, err)
		}
	}
	return nil
}

// RunPreflight executes the selected startup preflight checks and returns a
// structured PreflightReport with an appropriate exit code.
func RunPreflight(ctx context.Context, opts PreflightOptions) PreflightReport {
	startAll := time.Now()
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	checksToRun := opts.Checks
	if len(checksToRun) == 0 {
		checksToRun = AllCheckNames()
	}
	selected := make(map[string]bool, len(checksToRun))
	for _, name := range checksToRun {
		selected[name] = true
	}

	loadFn := config.Load
	if opts.LoadConfig != nil {
		loadFn = opts.LoadConfig
	}

	cfgStart := time.Now()
	cfg, cfgErr := loadFn()
	cfgDuration := time.Since(cfgStart).Milliseconds()

	secrets := collectSecrets(cfg)

	if cfgErr != nil {
		res := PreflightCheckResult{
			Name:        CheckConfig,
			Status:      StatusFailed,
			DurationMS:  cfgDuration,
			Error:       RedactSecrets(cfgErr.Error(), secrets),
			Remediation: "Fix the invalid environment variables reported above (run 'sorotrail config validate' to test configuration locally).",
		}
		return PreflightReport{
			Status:     StatusFailed,
			ExitCode:   ExitConfigError,
			DurationMS: time.Since(startAll).Milliseconds(),
			Checks:     []PreflightCheckResult{res},
		}
	}

	// Also validate relative START_LEDGER syntax if provided.
	if err := validateStartLedgerSyntax(cfg.StartLedgerRaw); err != nil {
		res := PreflightCheckResult{
			Name:        CheckConfig,
			Status:      StatusFailed,
			DurationMS:  cfgDuration,
			Error:       RedactSecrets(err.Error(), secrets),
			Remediation: "Set START_LEDGER to a non-negative integer, 'latest', or 'latest-N' (for example 'latest-100').",
		}
		return PreflightReport{
			Status:     StatusFailed,
			ExitCode:   ExitConfigError,
			DurationMS: time.Since(startAll).Milliseconds(),
			Network:    cfg.Network,
			Checks:     []PreflightCheckResult{res},
		}
	}

	resultsByName := make(map[string]PreflightCheckResult, len(checksToRun))
	if selected[CheckConfig] {
		resultsByName[CheckConfig] = PreflightCheckResult{
			Name:       CheckConfig,
			Status:     StatusOK,
			DurationMS: cfgDuration,
			Detail: fmt.Sprintf(
				"network=%s, db=%s, rpc_endpoints=%d",
				cfg.Network,
				describeDatabaseBackend(cfg.DatabaseURL),
				len(EffectiveRPCURLs(cfg)),
			),
		}
	}

	// Run all remaining selected checks concurrently under a bounded timeout so
	// slow dependencies do not add their timeouts together.
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type checkRunner struct {
		name string
		fn   func(context.Context) PreflightCheckResult
	}
	var runners []checkRunner
	if selected[CheckDatabase] {
		runners = append(runners, checkRunner{
			name: CheckDatabase,
			fn: func(c context.Context) PreflightCheckResult {
				return runDatabaseCheck(c, cfg, secrets)
			},
		})
	}
	if selected[CheckMigrations] {
		runners = append(runners, checkRunner{
			name: CheckMigrations,
			fn: func(c context.Context) PreflightCheckResult {
				return runMigrationsCheck(c, cfg, opts, secrets)
			},
		})
	}
	if selected[CheckRPC] {
		runners = append(runners, checkRunner{
			name: CheckRPC,
			fn: func(c context.Context) PreflightCheckResult {
				return runRPCCheck(c, cfg, opts, secrets)
			},
		})
	}
	if selected[CheckArchive] {
		runners = append(runners, checkRunner{
			name: CheckArchive,
			fn: func(c context.Context) PreflightCheckResult {
				return runArchiveCheck(c, cfg, opts, secrets)
			},
		})
	}
	if selected[CheckRetention] {
		runners = append(runners, checkRunner{
			name: CheckRetention,
			fn: func(c context.Context) PreflightCheckResult {
				return runRetentionCheck(c, cfg, secrets)
			},
		})
	}

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, r := range runners {
		wg.Add(1)
		go func(runner checkRunner) {
			defer wg.Done()
			res := runner.fn(probeCtx)
			res.Error = RedactSecrets(res.Error, secrets)
			res.Detail = RedactSecrets(res.Detail, secrets)
			mu.Lock()
			resultsByName[runner.name] = res
			mu.Unlock()
		}(r)
	}
	wg.Wait()

	var ordered []PreflightCheckResult
	hasConfigFailure := false
	hasDependencyFailure := false
	for _, name := range checksToRun {
		res, ok := resultsByName[name]
		if !ok {
			continue
		}
		ordered = append(ordered, res)
		if res.Status == StatusFailed {
			if res.Name == CheckConfig {
				hasConfigFailure = true
			} else {
				hasDependencyFailure = true
			}
		}
	}

	overallStatus := StatusOK
	exitCode := ExitOK
	if hasDependencyFailure {
		overallStatus = StatusFailed
		exitCode = ExitDependencyError
	} else if hasConfigFailure {
		overallStatus = StatusFailed
		exitCode = ExitConfigError
	}

	return PreflightReport{
		Status:     overallStatus,
		ExitCode:   exitCode,
		DurationMS: time.Since(startAll).Milliseconds(),
		Network:    cfg.Network,
		Checks:     ordered,
	}
}

func validateStartLedgerSyntax(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "latest" {
		return nil
	}
	if strings.HasPrefix(raw, "latest-") {
		_, err := resolveStartLedger(raw, 1_000_000_000)
		return err
	}
	return nil
}

func resolveStartLedger(raw string, latestLedger int64) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if raw == "latest" {
		return latestLedger, nil
	}
	if strings.HasPrefix(raw, "latest-") {
		offsetStr := strings.TrimPrefix(raw, "latest-")
		var offset int64
		if _, err := fmt.Sscanf(offsetStr, "%d", &offset); err != nil || offset <= 0 {
			return 0, fmt.Errorf("invalid START_LEDGER offset in %q", raw)
		}
		if latestLedger-offset <= 0 {
			return 0, fmt.Errorf("START_LEDGER=%q resolves to non-positive ledger %d (latest=%d)", raw, latestLedger-offset, latestLedger)
		}
		return latestLedger - offset, nil
	}
	var ledger int64
	if _, err := fmt.Sscanf(raw, "%d", &ledger); err != nil || ledger < 0 {
		return 0, fmt.Errorf("invalid START_LEDGER %q", raw)
	}
	return ledger, nil
}

func runDatabaseCheck(ctx context.Context, cfg config.Config, secrets []string) PreflightCheckResult {
	start := time.Now()
	dbURL := cfg.DatabaseURL
	redactedTarget := RedactDatabaseTarget(dbURL)

	if strings.HasPrefix(dbURL, "clickhouse://") {
		return PreflightCheckResult{
			Name:        CheckDatabase,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("ClickHouse backend (%s) is not yet wired for runtime use", redactedTarget),
			Remediation: "Configure DATABASE_URL with a PostgreSQL DSN (postgres://...) or SQLite file path.",
		}
	}

	if isSQLiteURL(dbURL) {
		dsn := strings.TrimPrefix(dbURL, "sqlite://")
		filePath := sqliteFilePath(dsn)
		if filePath != "" && filePath != ":memory:" {
			dir := filepath.Dir(filePath)
			if info, err := os.Stat(dir); err != nil || !info.IsDir() {
				return PreflightCheckResult{
					Name:        CheckDatabase,
					Status:      StatusFailed,
					DurationMS:  time.Since(start).Milliseconds(),
					Error:       fmt.Sprintf("SQLite parent directory %q does not exist or is not accessible", dir),
					Remediation: "Create the parent directory for DATABASE_URL or update DATABASE_URL to a valid path.",
				}
			}
		}

		existedBefore := true
		if filePath != "" && filePath != ":memory:" {
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				existedBefore = false
			}
		}

		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			return PreflightCheckResult{
				Name:        CheckDatabase,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       RedactSecrets(fmt.Sprintf("opening SQLite database (%s): %v", redactedTarget, err), secrets),
				Remediation: "Verify DATABASE_URL points to a readable and writable SQLite file path.",
			}
		}
		defer func() {
			_ = db.Close()
			if !existedBefore && filePath != "" && filePath != ":memory:" {
				_ = os.Remove(filePath)
			}
		}()

		var one int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
			return PreflightCheckResult{
				Name:        CheckDatabase,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       RedactSecrets(fmt.Sprintf("pinging SQLite database (%s): %v", redactedTarget, err), secrets),
				Remediation: "Ensure the SQLite database file is not corrupted, locked, or missing permissions.",
			}
		}

		return PreflightCheckResult{
			Name:       CheckDatabase,
			Status:     StatusOK,
			DurationMS: time.Since(start).Milliseconds(),
			Detail:     fmt.Sprintf("connected to SQLite (%s)", redactedTarget),
		}
	}

	poolCfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return PreflightCheckResult{
			Name:        CheckDatabase,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       RedactSecrets(fmt.Sprintf("parsing DATABASE_URL (%s): %v", redactedTarget, err), secrets),
			Remediation: "Verify DATABASE_URL is a valid PostgreSQL connection string (postgres://user:pass@host:5432/dbname).",
		}
	}
	poolCfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return PreflightCheckResult{
			Name:        CheckDatabase,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       RedactSecrets(fmt.Sprintf("connecting to PostgreSQL (%s): %v", redactedTarget, err), secrets),
			Remediation: "Check PostgreSQL host, port, credentials, TLS settings (sslmode), and network firewall rules.",
		}
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return PreflightCheckResult{
			Name:        CheckDatabase,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       RedactSecrets(fmt.Sprintf("pinging PostgreSQL (%s): %v", redactedTarget, err), secrets),
			Remediation: "Ensure the PostgreSQL server is running, reachable from this host, and accepting connections for the configured database and user.",
		}
	}

	return PreflightCheckResult{
		Name:       CheckDatabase,
		Status:     StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Detail:     fmt.Sprintf("connected to PostgreSQL (%s)", redactedTarget),
	}
}

func runMigrationsCheck(ctx context.Context, cfg config.Config, opts PreflightOptions, secrets []string) PreflightCheckResult {
	start := time.Now()
	inspectFn := inspectDatabaseMigrations
	if opts.InspectMigrations != nil {
		inspectFn = opts.InspectMigrations
	}

	info, err := inspectFn(ctx, cfg.DatabaseURL)
	if err != nil {
		return PreflightCheckResult{
			Name:        CheckMigrations,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       RedactSecrets(fmt.Sprintf("inspecting migration state: %v", err), secrets),
			Remediation: "Verify DATABASE_URL is reachable and the database user has SELECT permission on schema_migrations.",
		}
	}

	if info.Dirty {
		return PreflightCheckResult{
			Name:        CheckMigrations,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("database schema is marked dirty at version %d (target=%d)", info.AppliedVersion, info.TotalMigrations),
			Remediation: "Inspect schema_migrations with 'sorotrail migrate status', resolve the partially applied migration, and clear the dirty flag before deploying (see docs/runbook.md#dirty-migration-recovery).",
		}
	}

	if int(info.AppliedVersion) > info.TotalMigrations {
		return PreflightCheckResult{
			Name:        CheckMigrations,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("database schema version %d is newer than binary migrations (%d)", info.AppliedVersion, info.TotalMigrations),
			Remediation: "Deploy a sorotrail binary version that includes migration version " + fmt.Sprintf("%d", info.AppliedVersion) + " or roll back the schema before starting an older binary.",
		}
	}

	pending := info.TotalMigrations - int(info.AppliedVersion)
	if pending > 0 {
		if !info.CanApply {
			permDetail := info.PermissionNote
			if permDetail == "" {
				permDetail = "current database user lacks permission to apply migrations"
			}
			return PreflightCheckResult{
				Name:        CheckMigrations,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       fmt.Sprintf("%d pending migration(s) (applied=%d, target=%d) and %s", pending, info.AppliedVersion, info.TotalMigrations, permDetail),
				Remediation: "Apply pending migrations with a privileged database role ('sorotrail migrate up') before rollout, or grant schema CREATE/write privileges to the runtime database user.",
			}
		}
		if !opts.AllowPendingMigrations {
			return PreflightCheckResult{
				Name:        CheckMigrations,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       fmt.Sprintf("%d pending migration(s) (applied=%d, target=%d)", pending, info.AppliedVersion, info.TotalMigrations),
				Remediation: "Run 'sorotrail migrate up' before deploying, or pass --allow-pending-migrations if this instance is expected to apply migrations on startup.",
			}
		}
		return PreflightCheckResult{
			Name:       CheckMigrations,
			Status:     StatusOK,
			DurationMS: time.Since(start).Milliseconds(),
			Detail:     fmt.Sprintf("%d pending migration(s) ready to apply on startup (applied=%d, target=%d)", pending, info.AppliedVersion, info.TotalMigrations),
		}
	}

	return PreflightCheckResult{
		Name:       CheckMigrations,
		Status:     StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Detail:     fmt.Sprintf("schema up to date at version %d/%d (0 pending)", info.AppliedVersion, info.TotalMigrations),
	}
}

func inspectDatabaseMigrations(ctx context.Context, databaseURL string) (MigrationInspection, error) {
	total, err := store.CountEmbeddedMigrations()
	if err != nil {
		return MigrationInspection{}, fmt.Errorf("counting embedded migrations: %w", err)
	}

	if strings.HasPrefix(databaseURL, "clickhouse://") {
		return MigrationInspection{}, fmt.Errorf("ClickHouse backend does not support embedded SQL migrations")
	}

	if isSQLiteURL(databaseURL) {
		return inspectSQLiteMigrations(ctx, databaseURL, total)
	}
	return inspectPostgresMigrations(ctx, databaseURL, total)
}

func inspectSQLiteMigrations(ctx context.Context, databaseURL string, total int) (MigrationInspection, error) {
	dsn := strings.TrimPrefix(databaseURL, "sqlite://")
	filePath := sqliteFilePath(dsn)
	readOnlyDSN := strings.Contains(strings.ToLower(dsn), "mode=ro")

	// Check if the database is in-memory or does not exist on disk yet (read-only check: never create it).
	if filePath == ":memory:" || strings.Contains(strings.ToLower(dsn), "mode=memory") {
		return MigrationInspection{
			AppliedVersion:  0,
			Dirty:           false,
			TotalMigrations: total,
			CanApply:        !readOnlyDSN,
			PermissionNote:  boolNote(readOnlyDSN, "SQLite DSN has mode=ro"),
		}, nil
	}

	if filePath != "" {
		st, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			dir := filepath.Dir(filePath)
			dirInfo, dirErr := os.Stat(dir)
			if dirErr != nil || !dirInfo.IsDir() {
				return MigrationInspection{}, fmt.Errorf("SQLite directory %q is not accessible: %w", dir, dirErr)
			}
			canWrite := !readOnlyDSN && isDirWritable(dir)
			return MigrationInspection{
				AppliedVersion:  0,
				Dirty:           false,
				TotalMigrations: total,
				CanApply:        canWrite,
				PermissionNote:  boolNote(!canWrite, "SQLite directory or DSN is read-only"),
			}, nil
		}
		if err != nil {
			return MigrationInspection{}, fmt.Errorf("stat SQLite file: %w", err)
		}
		canWrite := !readOnlyDSN && (st.Mode().Perm()&0o200 != 0)
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			return MigrationInspection{}, fmt.Errorf("opening SQLite: %w", err)
		}
		defer db.Close()

		var version int64
		var dirty bool
		err = db.QueryRowContext(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no such table: schema_migrations") {
				return MigrationInspection{
					AppliedVersion:  0,
					Dirty:           false,
					TotalMigrations: total,
					CanApply:        canWrite,
					PermissionNote:  boolNote(!canWrite, "SQLite database file is read-only"),
				}, nil
			}
			return MigrationInspection{}, fmt.Errorf("querying schema_migrations: %w", err)
		}
		var uver uint
		if version > 0 {
			uver = uint(version)
		}
		return MigrationInspection{
			AppliedVersion:  uver,
			Dirty:           dirty,
			TotalMigrations: total,
			CanApply:        canWrite,
			PermissionNote:  boolNote(!canWrite, "SQLite database file is read-only"),
		}, nil
	}

	return MigrationInspection{}, fmt.Errorf("empty SQLite database path")
}

func inspectPostgresMigrations(ctx context.Context, databaseURL string, total int) (MigrationInspection, error) {
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return MigrationInspection{}, fmt.Errorf("parsing DATABASE_URL: %w", err)
	}
	poolCfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return MigrationInspection{}, fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	defer pool.Close()

	var tableExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = 'schema_migrations'
		)`).Scan(&tableExists)
	if err != nil {
		return MigrationInspection{}, fmt.Errorf("checking schema_migrations existence: %w", err)
	}

	var hasCreate bool
	var readOnlyTx string
	err = pool.QueryRow(ctx, `
		SELECT has_schema_privilege(current_user, current_schema(), 'CREATE'),
		       current_setting('transaction_read_only')`).Scan(&hasCreate, &readOnlyTx)
	if err != nil {
		return MigrationInspection{}, fmt.Errorf("checking schema privileges: %w", err)
	}
	canApply := hasCreate && !strings.EqualFold(readOnlyTx, "on")
	var permNote string
	if !hasCreate {
		permNote = "current PostgreSQL role lacks CREATE privilege on schema"
	} else if strings.EqualFold(readOnlyTx, "on") {
		permNote = "PostgreSQL connection has transaction_read_only=on"
	}

	if !tableExists {
		return MigrationInspection{
			AppliedVersion:  0,
			Dirty:           false,
			TotalMigrations: total,
			CanApply:        canApply,
			PermissionNote:  permNote,
		}, nil
	}

	var version int64
	var dirty bool
	err = pool.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return MigrationInspection{
				AppliedVersion:  0,
				Dirty:           false,
				TotalMigrations: total,
				CanApply:        canApply,
				PermissionNote:  permNote,
			}, nil
		}
		return MigrationInspection{}, fmt.Errorf("reading schema_migrations: %w", err)
	}
	var uver uint
	if version > 0 {
		uver = uint(version)
	}
	return MigrationInspection{
		AppliedVersion:  uver,
		Dirty:           dirty,
		TotalMigrations: total,
		CanApply:        canApply,
		PermissionNote:  permNote,
	}, nil
}

func runRPCCheck(ctx context.Context, cfg config.Config, opts PreflightOptions, secrets []string) PreflightCheckResult {
	start := time.Now()
	urls := EffectiveRPCURLs(cfg)
	if len(urls) == 0 {
		return PreflightCheckResult{
			Name:        CheckRPC,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       "no RPC endpoint configured",
			Remediation: "Set RPC_URL or RPC_URLS to a valid Soroban RPC endpoint matching NETWORK=" + cfg.Network + ".",
		}
	}

	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: opts.Timeout}
	}

	var details []string
	for _, rawURL := range urls {
		redacted := RedactEndpointURL(rawURL)
		netInfo, err := VerifyRPCNetwork(ctx, hc, rawURL, cfg.Network)
		if err != nil {
			var mismatch *NetworkPassphraseMismatchError
			if errors.As(err, &mismatch) {
				remed := fmt.Sprintf(
					"Update RPC_URL/RPC_URLS to point to a %s Soroban RPC node, or set NETWORK=%s to match the endpoint.",
					cfg.Network,
					nonEmptyOr(mismatch.ActualNetwork, "<matching-network>"),
				)
				return PreflightCheckResult{
					Name:        CheckRPC,
					Status:      StatusFailed,
					DurationMS:  time.Since(start).Milliseconds(),
					Error:       RedactSecrets(mismatch.Error(), secrets),
					Remediation: remed,
				}
			}
			return PreflightCheckResult{
				Name:        CheckRPC,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       RedactSecrets(fmt.Sprintf("probing RPC endpoint %s: %v", redacted, err), secrets),
				Remediation: fmt.Sprintf("Verify RPC_URL (%s) is reachable, supports Soroban JSON-RPC (getNetwork/getHealth), and any API token is valid.", redacted),
			}
		}

		var health struct {
			Status                string `json:"status"`
			LatestLedger          int64  `json:"latestLedger"`
			OldestLedger          int64  `json:"oldestLedger"`
			LedgerRetentionWindow int64  `json:"ledgerRetentionWindow"`
		}
		if err := callJSONRPC(ctx, hc, rawURL, "getHealth", &health); err != nil {
			return PreflightCheckResult{
				Name:        CheckRPC,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       RedactSecrets(fmt.Sprintf("calling getHealth on %s: %v", redacted, err), secrets),
				Remediation: fmt.Sprintf("Check that the Soroban RPC node at %s is synced and responding to getHealth.", redacted),
			}
		}
		if !strings.EqualFold(health.Status, "healthy") {
			return PreflightCheckResult{
				Name:        CheckRPC,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       fmt.Sprintf("RPC endpoint %s reported status %q (expected \"healthy\")", redacted, health.Status),
				Remediation: "Wait for the Soroban RPC node to finish syncing or switch RPC_URL to a healthy provider.",
			}
		}
		if health.LatestLedger <= 0 {
			return PreflightCheckResult{
				Name:        CheckRPC,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       fmt.Sprintf("RPC endpoint %s reported non-positive latestLedger=%d", redacted, health.LatestLedger),
				Remediation: "Ensure the Soroban RPC node has ingested ledgers before starting sorotrail.",
			}
		}

		// Verify START_LEDGER resolution against live chain head when configured.
		if cfg.StartLedgerRaw != "" {
			resolved, err := resolveStartLedger(cfg.StartLedgerRaw, health.LatestLedger)
			if err != nil {
				return PreflightCheckResult{
					Name:        CheckRPC,
					Status:      StatusFailed,
					DurationMS:  time.Since(start).Milliseconds(),
					Error:       fmt.Sprintf("START_LEDGER=%q cannot be resolved against latestLedger=%d: %v", cfg.StartLedgerRaw, health.LatestLedger, err),
					Remediation: "Lower the offset in START_LEDGER=latest-N or set START_LEDGER to a ledger within the chain's history.",
				}
			}
			if health.OldestLedger > 0 && resolved > 0 && resolved < health.OldestLedger {
				return PreflightCheckResult{
					Name:        CheckRPC,
					Status:      StatusFailed,
					DurationMS:  time.Since(start).Milliseconds(),
					Error:       fmt.Sprintf("START_LEDGER=%d is older than RPC oldest retained ledger %d on %s", resolved, health.OldestLedger, redacted),
					Remediation: fmt.Sprintf("Set START_LEDGER >= %d (or 'latest') or use an archive RPC node that retains historical ledgers.", health.OldestLedger),
				}
			}
		}

		details = append(details, fmt.Sprintf("%s (network=%s, latest_ledger=%d, protocol=%d)", redacted, cfg.Network, health.LatestLedger, netInfo.ProtocolVersion))
	}

	return PreflightCheckResult{
		Name:       CheckRPC,
		Status:     StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Detail:     strings.Join(details, "; "),
	}
}

func runArchiveCheck(ctx context.Context, cfg config.Config, opts PreflightOptions, secrets []string) PreflightCheckResult {
	start := time.Now()
	if !cfg.ArchiveEnabled() {
		return PreflightCheckResult{
			Name:       CheckArchive,
			Status:     StatusSkipped,
			DurationMS: time.Since(start).Milliseconds(),
			Detail:     "archival not enabled (ARCHIVE_BUCKET is empty)",
		}
	}

	factory := defaultArchiveProberFactory
	if opts.ArchiveProberFactory != nil {
		factory = opts.ArchiveProberFactory
	}

	prober, err := factory(cfg)
	if err != nil {
		return PreflightCheckResult{
			Name:        CheckArchive,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       RedactSecrets(fmt.Sprintf("initializing S3 client for bucket %q: %v", cfg.ArchiveBucket, err), secrets),
			Remediation: "Check ARCHIVE_ENDPOINT, ARCHIVE_REGION, ARCHIVE_USE_SSL, and S3 credentials (ARCHIVE_ACCESS_KEY_ID / ARCHIVE_SECRET_ACCESS_KEY).",
		}
	}

	exists, err := prober.BucketExists(ctx, cfg.ArchiveBucket)
	if err != nil {
		return PreflightCheckResult{
			Name:        CheckArchive,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       RedactSecrets(fmt.Sprintf("probing archive bucket %q: %v", cfg.ArchiveBucket, err), secrets),
			Remediation: "Verify network reachability to ARCHIVE_ENDPOINT and ensure the credentials have s3:ListBucket / HeadBucket permission on " + cfg.ArchiveBucket + ".",
		}
	}
	if !exists {
		return PreflightCheckResult{
			Name:        CheckArchive,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("archive bucket %q does not exist", cfg.ArchiveBucket),
			Remediation: fmt.Sprintf("Create bucket %q in object storage before deployment or grant s3:CreateBucket permission.", cfg.ArchiveBucket),
		}
	}

	if opts.WriteProbe {
		probeKey := buildArchiveProbeKey(cfg.ArchivePrefix)
		payload := []byte("sorotrail-preflight-write-probe\n")
		if err := prober.PutObject(ctx, cfg.ArchiveBucket, probeKey, payload); err != nil {
			return PreflightCheckResult{
				Name:        CheckArchive,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       RedactSecrets(fmt.Sprintf("write permission probe failed on bucket %q: %v", cfg.ArchiveBucket, err), secrets),
				Remediation: fmt.Sprintf("Grant s3:PutObject and s3:DeleteObject permissions on bucket %q (prefix %q) to the configured archive credentials.", cfg.ArchiveBucket, cfg.ArchivePrefix),
			}
		}
		if err := prober.RemoveObject(ctx, cfg.ArchiveBucket, probeKey); err != nil {
			return PreflightCheckResult{
				Name:        CheckArchive,
				Status:      StatusFailed,
				DurationMS:  time.Since(start).Milliseconds(),
				Error:       RedactSecrets(fmt.Sprintf("cleaning up write probe object %q in bucket %q: %v", probeKey, cfg.ArchiveBucket, err), secrets),
				Remediation: fmt.Sprintf("Grant s3:DeleteObject permission on bucket %q and remove temporary object %q if it remains.", cfg.ArchiveBucket, probeKey),
			}
		}
		return PreflightCheckResult{
			Name:       CheckArchive,
			Status:     StatusOK,
			DurationMS: time.Since(start).Milliseconds(),
			Detail:     fmt.Sprintf("bucket %q reachable and write/delete probe succeeded", cfg.ArchiveBucket),
		}
	}

	return PreflightCheckResult{
		Name:       CheckArchive,
		Status:     StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Detail:     fmt.Sprintf("bucket %q reachable (read-only check; pass --write-probe to verify write permission)", cfg.ArchiveBucket),
	}
}

func runRetentionCheck(_ context.Context, cfg config.Config, _ []string) PreflightCheckResult {
	start := time.Now()

	// 1. ARCHIVE_BEFORE_PRUNE=true requires ARCHIVE_BUCKET to be set.
	if cfg.ArchiveBeforePrune && !cfg.ArchiveEnabled() {
		return PreflightCheckResult{
			Name:        CheckRetention,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       "ARCHIVE_BEFORE_PRUNE=true is set, but ARCHIVE_BUCKET is empty",
			Remediation: "Set ARCHIVE_BUCKET to the target S3 bucket so pruned events can be archived, or set ARCHIVE_BEFORE_PRUNE=false.",
		}
	}

	// 2. When archival is configured alongside retention pruning, pruning without
	// ARCHIVE_BEFORE_PRUNE=true (or using partition-dropping RETENTION_AGE) would
	// delete events before the archiver exports them.
	if cfg.ArchiveEnabled() && cfg.RetentionAge > 0 {
		return PreflightCheckResult{
			Name:        CheckRetention,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("RETENTION_AGE=%s drops whole partitions directly and bypasses ARCHIVE_BUCKET=%q", cfg.RetentionAge, cfg.ArchiveBucket),
			Remediation: "Unset RETENTION_AGE and use RETENTION_MAX_AGE with ARCHIVE_BEFORE_PRUNE=true so events are exported to S3 before deletion.",
		}
	}

	if cfg.ArchiveEnabled() && cfg.RetentionEnabled() && !cfg.ArchiveBeforePrune {
		return PreflightCheckResult{
			Name:        CheckRetention,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("retention pruning is enabled with ARCHIVE_BUCKET=%q, but ARCHIVE_BEFORE_PRUNE is false", cfg.ArchiveBucket),
			Remediation: "Set ARCHIVE_BEFORE_PRUNE=true so the pruner refuses to delete ledger ranges that have not yet been exported to the archive bucket.",
		}
	}

	// 3. RETENTION_AGE partition pruner requires PostgreSQL partitioned tables.
	if cfg.RetentionAge > 0 && isSQLiteURL(cfg.DatabaseURL) {
		return PreflightCheckResult{
			Name:        CheckRetention,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       "RETENTION_AGE partition pruning is only supported on PostgreSQL, but DATABASE_URL is SQLite",
			Remediation: "Use RETENTION_MAX_AGE or RETENTION_MIN_LEDGER on SQLite, or switch DATABASE_URL to PostgreSQL.",
		}
	}

	// 4. RETENTION_MIN_LEDGER higher than an explicit START_LEDGER would immediately
	// prune historical events as they are ingested.
	if cfg.RetentionMinLedger > 0 && cfg.StartLedger > 0 && cfg.RetentionMinLedger > uint64(cfg.StartLedger) {
		return PreflightCheckResult{
			Name:        CheckRetention,
			Status:      StatusFailed,
			DurationMS:  time.Since(start).Milliseconds(),
			Error:       fmt.Sprintf("RETENTION_MIN_LEDGER (%d) is greater than START_LEDGER (%d), which would immediately prune newly ingested events", cfg.RetentionMinLedger, cfg.StartLedger),
			Remediation: "Set RETENTION_MIN_LEDGER <= START_LEDGER or advance START_LEDGER above the retention floor.",
		}
	}

	if !cfg.RetentionEnabled() && cfg.RetentionAge == 0 && !cfg.ArchiveEnabled() {
		return PreflightCheckResult{
			Name:       CheckRetention,
			Status:     StatusOK,
			DurationMS: time.Since(start).Milliseconds(),
			Detail:     "retention pruning and archival disabled",
		}
	}

	return PreflightCheckResult{
		Name:       CheckRetention,
		Status:     StatusOK,
		DurationMS: time.Since(start).Milliseconds(),
		Detail: fmt.Sprintf(
			"coherent (retention_enabled=%t, archive_enabled=%t, archive_before_prune=%t)",
			cfg.RetentionEnabled() || cfg.RetentionAge > 0,
			cfg.ArchiveEnabled(),
			cfg.ArchiveBeforePrune,
		),
	}
}

type minioArchiveProber struct {
	client *minio.Client
}

func defaultArchiveProberFactory(cfg config.Config) (ArchiveProber, error) {
	endpoint := strings.TrimSpace(cfg.ArchiveEndpoint)
	useSSL := cfg.ArchiveUseSSL
	if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		useSSL = false
	} else if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
		useSSL = true
	}
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}

	minioOpts := &minio.Options{
		Secure: useSSL,
		Region: cfg.ArchiveRegion,
	}
	if cfg.ArchiveAccessKeyID != "" || cfg.ArchiveSecretAccessKey != "" {
		minioOpts.Creds = credentials.NewStaticV4(cfg.ArchiveAccessKeyID, cfg.ArchiveSecretAccessKey, "")
	}

	mc, err := minio.New(endpoint, minioOpts)
	if err != nil {
		return nil, err
	}
	return &minioArchiveProber{client: mc}, nil
}

func (p *minioArchiveProber) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return p.client.BucketExists(ctx, bucket)
}

func (p *minioArchiveProber) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := p.client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "text/plain",
	})
	return err
}

func (p *minioArchiveProber) RemoveObject(ctx context.Context, bucket, key string) error {
	return p.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}

func buildArchiveProbeKey(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	name := ".sorotrail-preflight-probe-" + hex.EncodeToString(b[:])
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// RedactEndpointURL returns raw with any embedded userinfo password replaced by "***".
func RedactEndpointURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err == nil && u.User != nil {
		user := u.User.Username()
		if _, hasPwd := u.User.Password(); hasPwd {
			u.User = url.UserPassword(user, "***")
		}
		return u.String()
	}
	// Fallback if url.Parse failed on a malformed connection string containing user:pass@host.
	if schemeIdx := strings.Index(raw, "://"); schemeIdx >= 0 {
		rest := raw[schemeIdx+3:]
		if atIdx := strings.Index(rest, "@"); atIdx >= 0 {
			return raw[:schemeIdx+3] + "***@" + rest[atIdx+1:]
		}
	}
	return raw
}

// RedactDatabaseTarget returns a safe-to-print description of a database URL
// without credentials or sensitive query parameters.
func RedactDatabaseTarget(dbURL string) string {
	if isSQLiteURL(dbURL) {
		dsn := strings.TrimPrefix(dbURL, "sqlite://")
		return sqliteFilePath(dsn)
	}
	u, err := url.Parse(dbURL)
	if err != nil {
		return RedactEndpointURL(dbURL)
	}
	host := u.Host
	if host == "" {
		host = "localhost"
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName != "" {
		return host + "/" + dbName
	}
	return host
}

// RedactSecrets replaces any occurrence of known secret values or URL passwords in s with "***".
func RedactSecrets(s string, secrets []string) string {
	if s == "" {
		return ""
	}
	out := s
	for _, sec := range secrets {
		if sec != "" && len(sec) >= 3 {
			out = strings.ReplaceAll(out, sec, "***")
			out = strings.ReplaceAll(out, url.QueryEscape(sec), "***")
		}
	}
	return out
}

func collectSecrets(cfg config.Config) []string {
	var secrets []string
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v != "" {
			secrets = append(secrets, v)
		}
	}
	add(cfg.APIKey)
	add(cfg.ArchiveAccessKeyID)
	add(cfg.ArchiveSecretAccessKey)
	add(cfg.MultiTenantBootstrapKey)
	for _, envKey := range []string{
		"API_KEY",
		"ARCHIVE_ACCESS_KEY_ID",
		"ARCHIVE_SECRET_ACCESS_KEY",
		"MULTI_TENANT_BOOTSTRAP_KEY",
	} {
		add(os.Getenv(envKey))
	}
	addURLPassword := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if u, err := url.Parse(raw); err == nil && u.User != nil {
			if pwd, ok := u.User.Password(); ok && pwd != "" {
				add(pwd)
			}
		}
	}
	addURLPassword(cfg.DatabaseURL)
	addURLPassword(cfg.RPCURL)
	for _, u := range cfg.RPCURLS {
		addURLPassword(u)
	}
	addURLPassword(os.Getenv("DATABASE_URL"))
	addURLPassword(os.Getenv("RPC_URL"))
	for _, u := range strings.Split(os.Getenv("RPC_URLS"), ",") {
		addURLPassword(u)
	}
	return secrets
}

func isSQLiteURL(databaseURL string) bool {
	return strings.HasPrefix(databaseURL, "sqlite://") ||
		strings.HasSuffix(sqliteFilePath(databaseURL), ".db") ||
		databaseURL == ":memory:"
}

func sqliteFilePath(dsn string) string {
	if idx := strings.Index(dsn, "?"); idx >= 0 {
		dsn = dsn[:idx]
	}
	return strings.TrimPrefix(dsn, "file:")
}

func describeDatabaseBackend(databaseURL string) string {
	switch {
	case strings.HasPrefix(databaseURL, "clickhouse://"):
		return "clickhouse"
	case isSQLiteURL(databaseURL):
		return "sqlite"
	default:
		return "postgres"
	}
}

func isDirWritable(dir string) bool {
	f, err := os.CreateTemp(dir, ".sorotrail-perm-check-*")
	if err != nil {
		return false
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return true
}

func boolNote(cond bool, note string) string {
	if cond {
		return note
	}
	return ""
}

func nonEmptyOr(val, fallback string) string {
	if val != "" {
		return val
	}
	return fallback
}
