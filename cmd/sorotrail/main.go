// Command sorotrail runs the SoroTrail indexer: a Stellar RPC event ingester
// and a query API in one process.
//
// With no arguments it runs the indexer. Subcommands cover maintenance:
//
//	sorotrail replay --from-ledger N [--to-ledger M]
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
	"github.com/khaylebfortune/sorotrail/internal/version"
	"github.com/khaylebfortune/sorotrail/internal/webhook"
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
	case "version", "--version":
		fmt.Printf("sorotrail %s (commit: %s, date: %s)\n",
			version.GetVersion(), version.GetCommit(), version.GetDate())
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
  replay    re-decode stored events with the current decoder
            (sorotrail replay --help)
  version   print version information
`)
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	log.Info("sorotrail starting",
		"version", version.GetVersion(),
		"commit", version.GetCommit(),
		"date", version.GetDate())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}

	st := store.NewPostgres(pool, int64(cfg.PartitionLedgerSpan))
	for _, id := range cfg.WatchedContracts {
		if err := st.AddWatchedContract(ctx, id); err != nil {
			return err
		}
	}

	rpcClient := rpc.NewHTTPClient(cfg.RPCURL)
	// Webhook delivery runs alongside ingestion — the notifier is attached
	// to the ingester so events flow to subscriber callbacks asynchronously.
	wh := webhook.NewNotifier(st, log)

	// Wire the spec cache and enricher for spec-decoded event views.
	specCache := spec.NewCache(st)
	specFetcher := spec.NewFetcher(rpcClient)
	specEnricher := spec.NewEnricher(specFetcher, specCache, log)

	bcast := broadcast.New(broadcast.DefaultBufferSize)
	ing := ingester.New(rpcClient, st, decode.XDRDecoder{}, log, ingester.Options{
		PollInterval:     cfg.PollInterval,
		StartLedger:      cfg.StartLedger,
		RetentionLedgers: cfg.RetentionLedgers,
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
		auditClient := audit.NewBudgetedClient(rpcClient, budget)
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
	limiter := api.NewRateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.RateLimitTrustedProxy)
	limiter.Start(ctx)
	defer limiter.Stop()

	apiServer := api.New(st, rpcClient, log, specEnricher).WithBroadcaster(bcast)
	apiServer.SetRateLimiter(limiter)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           apiServer.Router(),
		ReadHeaderTimeout: 10 * time.Second,
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
