// Command sorotrail runs the SoroTrail indexer: a Stellar RPC event ingester
// and a query API in one process.
//
// With no arguments it runs the indexer. Subcommands cover maintenance:
//
//	sorotrail replay --from-ledger N [--to-ledger M]
//	sorotrail backfill --contract C... --from-ledger N [--to-ledger M]
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/audit"
	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/spec"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/webhook"
)

func main() {
	err := dispatch(os.Args[1:])
	switch {
	case err == nil:
	case errors.Is(err, errInterrupted):
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, "sorotrail:", err)
		os.Exit(1)
	}
}

// dispatch routes to a subcommand, defaulting to the indexer so existing
// deployments (and the Dockerfile entrypoint) keep working unchanged.
func dispatch(args []string) error {
	if len(args) == 0 {
		return run()
	}
	switch args[0] {
	case "replay":
		return runReplay(args[1:])
	case "backfill":
		return runBackfill(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: sorotrail [subcommand]

With no subcommand, runs the indexer (ingester + HTTP API).

subcommands:
  replay    re-decode stored events with the current decoder
            (sorotrail replay --help)
  backfill  ingest historical contract events from Horizon
            (sorotrail backfill --help)
`)
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel, cfg.LogFormat)

	log.Info("startup configuration", cfg.LoggableFields()...)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return err
	}

	var (
		st   store.Store
		pool *pgxpool.Pool
	)
	if strings.HasPrefix(cfg.DatabaseURL, "clickhouse://") {
		st, err = store.NewStoreFromURL(cfg.DatabaseURL)
		if err != nil {
			return err
		}
	} else {
		pool, err = pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("connecting to postgres: %w", err)
		}
		defer pool.Close()
		if err := pool.Ping(ctx); err != nil {
			return fmt.Errorf("pinging postgres: %w", err)
		}
		st = store.NewPostgres(pool, int64(cfg.PartitionLedgerSpan))
	}
	for _, id := range cfg.WatchedContracts {
		if err := st.AddWatchedContract(ctx, id); err != nil {
			return err
		}
	}

	rpcClient := rpc.NewHTTPClient(cfg.RPCURL)
	bcast := broadcast.New(broadcast.DefaultBufferSize)
	// Webhook delivery runs alongside ingestion — the notifier is attached
	// to the ingester so events flow to subscriber callbacks asynchronously.
	wh := webhook.NewNotifier(st, log)

	// Wire the spec cache and enricher for spec-decoded event views.
	specCache := spec.NewCache(st)
	specFetcher := spec.NewFetcher(rpcClient)
	specEnricher := spec.NewEnricher(specFetcher, specCache, log)

	// Wrap the raw RPC client so per-method error totals are tracked and
	// surfaced via /stats. specFetcher already holds a reference to the
	// unwrapped client (spec lookups are not counted as ingestion errors).
	countingClient := rpc.NewCountingClient(rpcClient)
	api.SetRPCCounter(countingClient)

	ing := ingester.New(countingClient, st, decode.XDRDecoder{}, log, ingester.Options{
		PollInterval:            cfg.PollInterval,
		StartLedger:             cfg.StartLedger,
		RetentionLedgers:        cfg.RetentionLedgers,
		SweepConcurrency:        cfg.SweepConcurrency,
		ReorgConfirmationWindow: cfg.ReorgConfirmationWindow,
		ReorgRescanInterval:     cfg.ReorgRescanInterval,
	}).WithBroadcaster(bcast)
	ing.SetNotifier(wh)

	// The auditor and its request-rate budget are constructed lazily:
	// AUDIT_ENABLED=false (the default) means a binary identical to a
	// pre-audit build, so we skip every allocation.
	var aud *audit.Auditor
	if cfg.AuditEnabled {
		budget, err := rpc.NewBudget(cfg.AuditMaxRPS, cfg.AuditBudgetShare)
		if err != nil {
			return err
		}
		auditClient := audit.NewBudgetedClient(countingClient, budget)
		aud = audit.New(auditClient, st, ing, log, audit.Options{
			PollInterval:      cfg.AuditPollInterval,
			BatchLedgers:      cfg.AuditBatchLedgers,
			LagThreshold:      cfg.AuditLagThreshold,
			MaxRepairAttempts: cfg.AuditMaxRepair,
			FindingMaxLedgers: cfg.AuditFindingMaxLgrs,
		})
		// Expose the auditor's counters via /stats so operators don't need
		// to parse logs to see pass/finding rates.
		api.SetAuditor(aud)
	}

	// Per-client HTTP rate limiter. Disabled when RATE_LIMIT_RPS or
	// RATE_LIMIT_BURST is unset; the limiter is then a pass-through and
	// its cleanup goroutine is never started.
	limiterOpts := []api.LimiterOption{}
	if cfg.MultiTenant {
		// Key buckets on the authenticated tenant rather than the source
		// IP, so a tenant's quota follows its identity across however many
		// addresses it calls from.
		limiterOpts = append(limiterOpts,
			api.WithLimitResolver(api.TenantLimitResolver(cfg.RateLimitRPS, cfg.RateLimitBurst)))
	}
	limiter := api.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitTrustedProxy, limiterOpts...)
	limiter.Start(ctx)
	defer limiter.Stop()

	apiStore := store.NewGuardedStore(st, store.GuardedStoreOptions{
		Timeout:            cfg.APIQueryTimeout,
		SlowQueryThreshold: cfg.APISlowQueryThreshold,
		Logger:             log,
	})
	apiServer := api.New(apiStore, countingClient, log, cfg.APIKey, specEnricher).WithBroadcaster(bcast)
	apiServer.SetRateLimiter(limiter)
	apiServer.SetCompressMinSize(cfg.CompressMinSize)
	apiServer.SetExportMaxRange(cfg.ExportMaxRange)

	if cfg.MultiTenant {
		// Tenancy lives in tables (tenants, grants, api_keys, usage) that
		// only the Postgres backend has. Refusing at startup is the whole
		// point: silently running a ClickHouse deployment with MULTI_TENANT
		// set would mean an operator believing a boundary is enforced when
		// there is none, which is the one failure this feature must not have.
		tenants, ok := st.(store.TenantStore)
		if !ok {
			return fmt.Errorf(
				"MULTI_TENANT=true requires a backend with tenant storage, but %T has none; use a postgres:// DATABASE_URL", st)
		}
		apiServer = apiServer.WithMultiTenancy(tenants, api.MultiTenantOptions{
			MaxWatchedContracts: cfg.MultiTenantMaxWatched,
			UsageFlushInterval:  cfg.MultiTenantUsageFlush,
			StreamScopeSync:     cfg.MultiTenantStreamScopeSync,
		})
		if err := bootstrapAdminKey(ctx, tenants, cfg.MultiTenantBootstrapKey, log); err != nil {
			return err
		}
		usage := apiServer.Usage()
		usage.Start(ctx)
		defer usage.Stop()
		log.Info("multi-tenant mode enabled",
			"max_watched_contracts", cfg.MultiTenantMaxWatched,
			"stream_scope_sync", cfg.MultiTenantStreamScopeSync)
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           apiServer.Router(),
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
	}
	if cfg.APIKey == "" {
		log.Warn("API_KEY env is unset; watched-contracts endpoints will reject every request with 503")
	} else {
		log.Info("watched-contracts endpoints are auth-gated")
	}

	errCh := make(chan error, 4)
	go func() {
		go wh.Run(ctx)
	}()
	go func() {
		log.Info("ingester starting", "rpc_url", cfg.RPCURL, "poll_interval", cfg.PollInterval)
		if err := ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("ingester: %w", err)
		} else {
			errCh <- nil
		}
	}()
	go func() {
		log.Info("http api listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		} else {
			errCh <- nil
		}
	}()
	if aud != nil {
		go func() {
			log.Info("auditor starting",
				"budget_share", cfg.AuditBudgetShare,
				"batch_ledgers", cfg.AuditBatchLedgers,
				"lag_threshold", cfg.AuditLagThreshold,
				"max_repair_attempts", cfg.AuditMaxRepair)
			if err := aud.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("auditor: %w", err)
			} else {
				errCh <- nil
			}
		}()
	}

	var firstErr error
	remaining := 3 // ingester + http server + webhook
	if aud != nil {
		remaining = 4
	}
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case firstErr = <-errCh:
		remaining--
		stop() // one component failed; wind down the others
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	for ; remaining > 0; remaining-- {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	log.Info("shutdown complete")
	return firstErr
}

// bootstrapAdminKey installs MULTI_TENANT_BOOTSTRAP_KEY as a credential for
// the seeded "default" admin tenant, so a fresh multi-tenant install has a
// way to mint its first real keys.
//
// Without this an operator enabling MULTI_TENANT=true locks themselves out
// completely: every endpoint demands a key, and the only endpoint that
// issues keys demands an admin key. The bootstrap value is a plaintext
// credential in the environment, which is why it is opt-in and why the log
// line nudges toward replacing it — it exists to be used once and revoked.
//
// Re-running with the same value is a no-op, so restarts are safe.
func bootstrapAdminKey(ctx context.Context, ts store.TenantStore, key string, log *slog.Logger) error {
	if key == "" {
		return nil
	}
	prefix, digest, ok := api.ParseAPIKeyForBootstrap(key)
	if !ok {
		return fmt.Errorf("MULTI_TENANT_BOOTSTRAP_KEY is not a valid key; " +
			"generate one with `sorotrail help` format st_<12 chars>_<secret>")
	}
	tenant, err := ts.GetTenantByName(ctx, "default")
	if err != nil {
		return fmt.Errorf("loading default tenant: %w", err)
	}
	err = ts.CreateAPIKeyIfAbsent(ctx, tenant.ID, "bootstrap", prefix, digest)
	if err != nil {
		return fmt.Errorf("installing bootstrap key: %w", err)
	}
	log.Warn("bootstrap admin key installed from MULTI_TENANT_BOOTSTRAP_KEY; "+
		"mint per-tenant keys via POST /admin/tenants/{id}/keys and revoke this one",
		"tenant", tenant.Name, "prefix", prefix)
	return nil
}

func newLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(format) {
	case "json":
		h = slog.NewJSONHandler(os.Stdout, opts)
	default:
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h)
}
