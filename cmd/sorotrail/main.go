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

	"github.com/khaylebfortune/sorotrail/internal/api"
	"github.com/khaylebfortune/sorotrail/internal/audit"
	"github.com/khaylebfortune/sorotrail/internal/broadcast"
	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/decode"
	"github.com/khaylebfortune/sorotrail/internal/ingester"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/spec"
	"github.com/khaylebfortune/sorotrail/internal/store"
	"github.com/khaylebfortune/sorotrail/internal/webhook"
)

// compositeNotifier fans out to multiple EventNotifiers.
type compositeNotifier []ingester.EventNotifier

func (n compositeNotifier) NotifyEvents(ctx context.Context, events []store.Event) {
	for _, notifier := range n {
		notifier.NotifyEvents(ctx, events)
	}
}

var errInterrupted = errors.New("interrupted")

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
	log := newLogger(cfg.LogLevel)

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

	// Tag any events that have empty network with the default network.
	// This handles the upgrade path for single-network deployments.
	if pool != nil {
		defaultNetwork := cfg.DefaultNetworkName()
		if defaultNetwork == "" {
			networks := cfg.NetworksOrDefault()
			if len(networks) > 0 {
				defaultNetwork = networks[0].Name
			}
		}
		if defaultNetwork != "" {
			if _, err := pool.Exec(ctx, `UPDATE events SET network = $1 WHERE network = '' OR network IS NULL`, defaultNetwork); err != nil {
				log.Warn("tagging legacy events with default network", "error", err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO ingestion_state (network, last_ingested_ledger, last_cursor, updated_at)
				SELECT $1, last_ingested_ledger, last_cursor, updated_at FROM ingestion_state WHERE network = '' OR network IS NULL
				ON CONFLICT (network) DO NOTHING`, defaultNetwork); err != nil {
				log.Warn("migrating ingestion state", "error", err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO audit_state (network, verified_through_ledger, updated_at)
				SELECT $1, verified_through_ledger, updated_at FROM audit_state WHERE network = '' OR network IS NULL
				ON CONFLICT (network) DO NOTHING`, defaultNetwork); err != nil {
				log.Warn("migrating audit state", "error", err)
			}
		}
	}

	for _, id := range cfg.WatchedContracts {
		// In multi-network mode, watched contracts apply to all networks.
		for _, n := range cfg.NetworksOrDefault() {
			if err := st.AddWatchedContract(ctx, id); err != nil {
				return err
			}
			_ = n // preserve for per-network contract lists
		}
	}

	// Shared broadcaster for live event streaming across all networks.
	bcast := broadcast.New(broadcast.DefaultBufferSize)

	// Build per-network components.
	networks := cfg.NetworksOrDefault()
	type networkIngester struct {
		ing     *ingester.Ingester
		auditor *audit.Auditor
		rpc     rpc.Client
	}
	ingesters := make([]networkIngester, 0, len(networks))
	specCache := spec.NewCache(st)

	for _, net := range networks {
		rpcClient := rpc.NewHTTPClient(net.RPCURL)

		ing := ingester.New(rpcClient, st, decode.XDRDecoder{}, log, ingester.Options{
			PollInterval:     cfg.PollInterval,
			StartLedger:      cfg.StartLedger,
			RetentionLedgers: cfg.RetentionLedgers,
			Network:          net.Name,
		}).WithBroadcaster(bcast)

		sni := networkIngester{
			ing: ing,
			rpc: rpcClient,
		}

		// Build auditor per network (one per ingester).
		if cfg.AuditEnabled {
			budget, err := rpc.NewBudget(cfg.AuditMaxRPS, cfg.AuditBudgetShare)
			if err != nil {
				return fmt.Errorf("creating budget for network %q: %w", net.Name, err)
			}
			auditClient := audit.NewBudgetedClient(rpcClient, budget)
			aud := audit.New(auditClient, st, ing, log, audit.Options{
				PollInterval:      cfg.AuditPollInterval,
				BatchLedgers:      cfg.AuditBatchLedgers,
				LagThreshold:      cfg.AuditLagThreshold,
				MaxRepairAttempts: cfg.AuditMaxRepair,
				FindingMaxLedgers: cfg.AuditFindingMaxLgrs,
			})
			sni.auditor = aud
		}

		ingesters = append(ingesters, sni)
	}

	// Webhook delivery runs alongside ingestion.
	wh := webhook.NewNotifier(st, log)

	// Token balance processor derives SEP-41 holder balances from ingested events.
	tokenProc := ingester.NewTokenBalanceProcessor(st, log)

	// Wire spec enricher for the API using the first network's fetcher.
	firstSpecFetcher := spec.NewFetcher(ingesters[0].rpc)
	firstSpecEnricher := spec.NewEnricher(firstSpecFetcher, specCache, log)

	// Guarded store for API-originated reads with timeout and slow-query logging.
	apiStore := store.NewGuardedStore(st, store.GuardedStoreOptions{
		Timeout:            cfg.APIQueryTimeout,
		SlowQueryThreshold: cfg.APISlowQueryThreshold,
		Logger:             log,
	})

	// Set up the API server.
	apiServer := api.New(apiStore, ingesters[0].rpc, log, cfg.APIKey, firstSpecEnricher).WithBroadcaster(bcast)
	limiter := api.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitTrustedProxy)
	if cfg.RateLimitRPS > 0 {
		limiter.Start(ctx)
		defer limiter.Stop()
	}
	apiServer.SetRateLimiter(limiter)
	apiServer.SetNetworks(cfg.NetworkNames())
	api.SetCachePrivate(cfg.CachePrivate)

	// Connect notifier to all ingesters.
	for _, ni := range ingesters {
		ni.ing.SetNotifier(compositeNotifier{wh, tokenProc})
	}

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           apiServer.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	if cfg.APIKey == "" {
		log.Warn("API_KEY env is unset; watched-contracts endpoints will reject every request with 503")
	} else {
		log.Info("watched-contracts endpoints are auth-gated")
	}

	// Error channel: one per ingester + HTTP server + webhook + auditor per network.
	errCh := make(chan error, 3+len(ingesters)*2)

	// Start webhook notifier.
	go wh.Run(ctx)

	// Start one ingester + auditor per network.
	for _, ni := range ingesters {
		ni := ni // capture
		go func() {
			log.Info("ingester starting", "network", ni.ing.Network(), "rpc_url", networks[0].RPCURL)
			if err := ni.ing.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("ingester[%s]: %w", ni.ing.Network(), err)
			} else {
				errCh <- nil
			}
		}()
		if ni.auditor != nil {
			go func() {
				log.Info("auditor starting", "network", ni.ing.Network())
				if err := ni.auditor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					errCh <- fmt.Errorf("auditor[%s]: %w", ni.ing.Network(), err)
				} else {
					errCh <- nil
				}
			}()
		}
	}

	// Start HTTP server.
	go func() {
		log.Info("http api listening", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		} else {
			errCh <- nil
		}
	}()

	// Count expected goroutines that send to errCh.
	remaining := 2 + len(ingesters) // http server + webhook + ingesters
	for _, ni := range ingesters {
		if ni.auditor != nil {
			remaining++
		}
	}

	var firstErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case firstErr = <-errCh:
		remaining--
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func newLogger(level string) *slog.Logger {
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
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
