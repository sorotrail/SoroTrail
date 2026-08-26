package main

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/ingester"
)

// applyReload re-reads and validates the full environment via config.Load,
// then applies only the safe reloadable subset — poll interval and log
// level — to the already-running ingester and logger. See run()'s SIGHUP
// wiring for how this is invoked in production (issue #148).
//
// It returns the config now in effect: on success that's the freshly
// loaded one; on any failure (env parse/validation, or applying the poll
// interval) it returns old unchanged along with an error describing why,
// so the caller can log the rejection and keep the previous configuration
// active — an invalid reload must never touch running state.
//
// Topology fields (DATABASE_URL, RPC_URL/RPC_URLS, LOG_FORMAT) are never
// applied here: taking them live would mean reconstructing the store, the
// RPC client, or the log handler, which the indexer only does at startup.
// A reload where one of those differs from old is not treated as a
// validation failure — it's a valid config that simply can't be hot-
// applied — but warnIgnoredTopologyChanges logs it so an operator who
// edited several variables at once isn't left assuming the untouched ones
// took effect too.
func applyReload(old config.Config, log *slog.Logger, ing *ingester.Ingester, level *slog.LevelVar) (config.Config, error) {
	next, err := config.Load()
	if err != nil {
		return old, fmt.Errorf("loading environment: %w", err)
	}

	if err := ing.SetPollInterval(next.PollInterval); err != nil {
		// config.Load already rejects a non-positive POLL_INTERVAL via
		// Validate, so this is unreachable in practice; kept as a
		// defensive guard so a future relaxation of that rule can't
		// silently corrupt the live ingester loop.
		return old, fmt.Errorf("applying poll interval: %w", err)
	}
	level.Set(config.ParseLogLevel(next.LogLevel))

	warnIgnoredTopologyChanges(old, next, log)

	log.Info("config reloaded via SIGHUP",
		"old_poll_interval", old.PollInterval,
		"new_poll_interval", next.PollInterval,
		"old_log_level", old.LogLevel,
		"new_log_level", next.LogLevel,
	)
	return next, nil
}

// warnIgnoredTopologyChanges logs, at warn level, any difference between
// old and next in a field this reload path cannot apply live. It never
// rejects the reload: these fields require a restart to take effect, and
// an operator's unrelated valid changes (poll interval, log level) still
// deserve to land rather than being blocked by, say, an incidental edit
// to DATABASE_URL in the same .env file.
func warnIgnoredTopologyChanges(old, next config.Config, log *slog.Logger) {
	if old.DatabaseURL != next.DatabaseURL {
		log.Warn("DATABASE_URL changed but is not hot-reloadable; restart to apply the new value")
	}
	if old.RPCURL != next.RPCURL {
		log.Warn("RPC_URL changed but is not hot-reloadable; restart to apply",
			"old", redactURLUserinfo(old.RPCURL), "new", redactURLUserinfo(next.RPCURL))
	}
	if !slices.Equal(old.RPCURLS, next.RPCURLS) {
		log.Warn("RPC_URLS changed but is not hot-reloadable; restart to apply")
	}
	if old.LogFormat != next.LogFormat {
		log.Warn("LOG_FORMAT changed but is not hot-reloadable (only LOG_LEVEL is); restart to apply",
			"old", old.LogFormat, "new", next.LogFormat)
	}
}
