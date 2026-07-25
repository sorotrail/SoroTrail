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
	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/decode"
	"github.com/khaylebfortune/sorotrail/internal/ingester"
	"github.com/khaylebfortune/sorotrail/internal/rpc"
	"github.com/khaylebfortune/sorotrail/internal/store"
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
`)
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

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

	st := store.NewPostgres(pool)
	for _, id := range cfg.WatchedContracts {
		if err := st.AddWatchedContract(ctx, id); err != nil {
			return err
		}
	}

	// Build the RPC client: single URL (RPC_URL) or multi-provider
	// failover (RPC_URLS). The single-URL path keeps existing deployments
	// working unchanged.
	var rpcClient rpc.Client
	if len(cfg.RPCURLS) > 0 {
		fc := rpc.NewFailoverClient(
			cfg.RPCURLS,
			cfg.RPCRateLimitRPS,
			rpc.NewHTTPClientForFailover,
			rpc.WithFailoverLogger(log),
		)
		// Background getHealth probes for demoted providers so they can
		// be promoted back when they recover.
		fc.RunProbes(ctx)
		rpcClient = fc
		log.Info("failover rpc client created", "providers", len(cfg.RPCURLS),
			"rate_limit_rps", cfg.RPCRateLimitRPS)
	} else {
		rpcClient = rpc.NewHTTPClient(cfg.RPCURL)
	}

	ing := ingester.New(rpcClient, st, decode.XDRDecoder{}, log, ingester.Options{
		PollInterval:     cfg.PollInterval,
		StartLedger:      cfg.StartLedger,
		RetentionLedgers: cfg.RetentionLedgers,
	})

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

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, rpcClient, log).Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 3)
	go func() {
		log.Info("ingester starting", "rpc_urls", rpcURLsForLog(cfg), "poll_interval", cfg.PollInterval)
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
	remaining := 2 // ingester + http server
	if aud != nil {
		remaining = 3
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

func rpcURLsForLog(cfg config.Config) []string {
	if len(cfg.RPCURLS) > 0 {
		return cfg.RPCURLS
	}
	return []string{cfg.RPCURL}
}
