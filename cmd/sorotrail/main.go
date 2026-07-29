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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/api/graphql"
	"github.com/sorotrail/sorotrail/internal/audit"
	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/pruner"
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
	case "index-addresses":
		return runIndexAddresses(args[1:])
	case "healthcheck":
		// The healthcheck subcommand manages its own exit codes
		// (0 healthy, 1 unhealthy, 2 usage error) — the docker
		// HEALTHCHECK directive inspects them directly, so we
		// hand control to os.Exit here rather than letting the
		// main switch collapse everything into 1-with-a-prefix.
		code := runHealthcheck(args[1:])
		if code != 0 {
			os.Exit(code)
		}
		return nil
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
  replay       re-decode stored events with the current decoder
               (sorotrail replay --help)
  backfill     ingest historical contract events from Horizon
               (sorotrail backfill --help)
  index-addresses  rebuild the address→event inverted index from stored events
               (sorotrail index-addresses --help)
  healthcheck  probe /health and exit (used by docker HEALTHCHECK)
               (sorotrail healthcheck --help)
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

		// Retry the initial ping with exponential backoff so transient
		// startup races (database container still initialising, network
		// not yet ready) don't crash the process before it can serve.
		const (
			maxRetries  = 5
			baseBackoff = 500 * time.Millisecond
			maxBackoff  = 5 * time.Second
		)
		var pingErr error
		for attempt := 1; attempt <= maxRetries; attempt++ {
			pingErr = pool.Ping(ctx)
			if pingErr == nil {
				break
			}
			log.Warn("postgres ping failed, retrying",
				"attempt", attempt,
				"max_retries", maxRetries,
				"error", pingErr,
			)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * baseBackoff
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}
			}
		}
		if pingErr != nil {
			return fmt.Errorf("pinging postgres after %d retries: %w", maxRetries, pingErr)
		}
		log.Info("postgres connection established")
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

	// Advisory lock: when enabled (opt-in), acquire a Postgres advisory
	// lock keyed by the RPC URL so a second instance targeting the same
	// network yields ingestion to the lock holder. The API server still
	// runs so the passive instance can serve reads.
	ingesterEnabled := true
	if cfg.IngestionLockEnabled {
		lockKey := store.AdvisoryLockKey(cfg.RPCURL)
		// Only Postgres-backed stores support advisory locks; ClickHouse
		// and other backends skip silently.
		if pg, ok := st.(*store.Postgres); ok {
			lockConn, acquired, err := pg.TryAdvisoryLock(ctx, lockKey)
			if err != nil {
				return fmt.Errorf("advisory lock: %w", err)
			}
			if acquired {
				defer lockConn.Release() // releases lock + connection on shutdown
				log.Info("acquired ingestion advisory lock",
					"key", lockKey, "rpc_url", cfg.RPCURL)
			} else {
				log.Warn("ingestion advisory lock held by another instance; skipping ingestion",
					"key", lockKey, "rpc_url", cfg.RPCURL)
				ingesterEnabled = false
			}
		} else {
			log.Warn("INGESTION_LOCK_ENABLED is set but the store is not Postgres; skipping advisory lock")
		}
	}

	ing := ingester.New(countingClient, st, decode.XDRDecoder{}, log, ingester.Options{
		PollInterval:            cfg.PollInterval,
		StartLedger:             cfg.StartLedger,
		RetentionLedgers:        cfg.RetentionLedgers,
		LagWarnLedgers:          cfg.LagWarnLedgers,
		SweepConcurrency:        cfg.SweepConcurrency,
		ReorgConfirmationWindow: cfg.ReorgConfirmationWindow,
		ReorgRescanInterval:     cfg.ReorgRescanInterval,
	}).WithBroadcaster(bcast)
	ing.SetNotifier(wh)
	// Wire the same store as the dead-letter sink: events that fail to
	// decode/persist land in the dead_letters table instead of
	// stalling the cycle (issue #131).
	ing.SetDeadLetterSink(st)

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

	// The pruner is constructed lazily: when neither RETENTION_MAX_AGE nor
	// RETENTION_MIN_LEDGER is set, the pruner is a no-op goroutine that
	// returns immediately. Only when at least one retention policy is
	// configured does it allocate a goroutine and a metrics struct.
	prn := pruner.New(st, log, pruner.Options{
		MaxAge:    cfg.RetentionMaxAge,
		MinLedger: cfg.RetentionMinLedger,
		BatchSize: cfg.RetentionBatchSize,
		Pause:     cfg.RetentionPause,
		Interval:  cfg.RetentionInterval,
	})
	if cfg.RetentionEnabled() {
		api.SetPruner(prn)
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
	api.SetMaxLimit(cfg.APIMaxLimit)

	apiServer := api.New(apiStore, countingClient, log, cfg.APIKey, specEnricher).WithBroadcaster(bcast)
	apiServer.SetRateLimiter(limiter)
	apiServer.SetCompressMinSize(cfg.CompressMinSize)
	apiServer.SetExportMaxRange(cfg.ExportMaxRange)
	apiServer.SetCORSConfig(api.CORSConfig{
		AllowedOrigins: cfg.CORSAllowedOrigins,
		AllowedMethods: cfg.CORSAllowedMethods,
		AllowedHeaders: cfg.CORSAllowedHeaders,
	})

	// GraphQL transport: reads against the same store + spec enricher
	// the REST handlers use. Dev-mode playground is gated on
	// GRAPHQL_PLAYGROUND. The schema is the same shape as
	// internal/api/graphql/schema.graphqls.
	gqlHandler, gqlErr := graphql.New(graphqlServerDeps(apiStore, specEnricher), log, cfg.GraphQLPlayground)
	if gqlErr != nil {
		return fmt.Errorf("constructing graphql handler: %w", gqlErr)
	}
	apiServer.SetGraphQLHandler(gqlHandler, gqlHandler.PlaygroundHandler())

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

	// Start the ingester only when the advisory lock was acquired (or
	// when lock enforcement is disabled). The goroutine is skipped
	// when another instance holds the lock.
	remaining := 2 // http server + webhook
	if ingesterEnabled {
		remaining++ // + ingester
		go func() {
			log.Info("ingester starting", "rpc_url", cfg.RPCURL, "poll_interval", cfg.PollInterval)
			if err := ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("ingester: %w", err)
			} else {
				errCh <- nil
			}
		}()
	}
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
	go func() {
		if cfg.RetentionEnabled() {
			log.Info("pruner starting",
				"max_age", cfg.RetentionMaxAge,
				"min_ledger", cfg.RetentionMinLedger,
				"batch_size", cfg.RetentionBatchSize,
				"pause", cfg.RetentionPause,
				"interval", cfg.RetentionInterval)
		}
		if err := prn.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("pruner: %w", err)
		} else {
			errCh <- nil
		}
	}()

	var firstErr error
	if aud != nil {
		remaining++
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

// graphqlServerDeps wraps the live store + enricher into the typed
// bundle the GraphQL Handler consumes. Centralising the cast here
// keeps the route wiring in main.go one line wide.
func graphqlServerDeps(st store.Store, enricher api.Enricher) api.ServerDeps {
	return api.ServerDeps{Store: st, Enricher: enricher}
}
