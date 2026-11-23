package config

// This file is the catalogue of which configuration values are
// reloadable and which are explicitly static (#721): a value is either
// re-read and applied live when the operator sends SIGHUP, or read
// exactly once at startup and held for the life of the process. The
// catalogue is two small tables plus the Config struct itself, and the
// tests tie all three to the README's configuration table and to the
// real reload path in cmd/sorotrail, so the three cannot drift:
//
//   - TestEveryValueIsClassifiedStaticOrReloadable requires every env
//     var Config reads to carry a Reload cell in the README — "SIGHUP"
//     exactly for the reloadable ones — and the reverse.
//   - TestReloadableValuesMatchTheReloadPath parses
//     cmd/sorotrail/reload.go and requires the set of fields applyReload
//     actually applies to equal the reloadable set, and the set
//     warnIgnoredTopologyChanges announces to equal the warned-static
//     set. A value moved onto the reload path without documenting it,
//     or documented as reloadable without code that applies it, fails.
//   - TestStaticValuesAreSnapshotsReadOnce pins the mechanism that makes
//     "static" mean what it says: Load returns a value copy, nothing
//     re-reads it, so a startup-only value cannot change under the
//     running process.
//   - TestReloadableGuardsHold pins the two invariants applyReload
//     relies on (a positive POLL_INTERVAL survives validation; every
//     documented LOG_LEVEL maps to a slog level), so the reload path's
//     defensive guards stay honest.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reloadableValues maps each reloadable env var to the live mechanism
// that applies it on SIGHUP. Keeping the mechanism beside the name is
// the point: "reloadable" is a claim about code, and the matching test
// checks the code still exists.
//
// Everything Config reads that is not in this map is static: read once
// at startup, change requires a restart.
var reloadableValues = map[string]string{
	"POLL_INTERVAL": "SetPollInterval on the running ingester (issue #148)",
	"LOG_LEVEL":     "slog.LevelVar on the running root logger",
}

// warnedStaticValues is the subset of static values whose change the
// SIGHUP path announces at warn level. They are topology: applying them
// live would mean reconstructing the store, the RPC client, or the log
// handler, which the indexer only does at startup. The rest of the
// static values are silently held — restarting is the documented
// contract for them — but these four are called out because an
// operator editing several variables at once must not be left assuming
// a database or endpoint change took effect.
var warnedStaticValues = map[string]bool{
	"DATABASE_URL": true,
	"RPC_URL":      true,
	"RPC_URLS":     true,
	"LOG_FORMAT":   true,
}

// envFieldNamesFromConfig maps every env var Config reads to its Go
// field name, taking the struct tags as the single source of truth.
func envFieldNamesFromConfig() map[string]string {
	t := reflect.TypeOf(Config{})
	out := make(map[string]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("env")
		if tag == "" || tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			continue
		}
		out[name] = f.Name
	}
	return out
}

// reloadPathFields parses cmd/sorotrail/reload.go and returns the Config
// fields the named function touches through the given receiver
// identifiers ("next" for the freshly loaded config, "old" for the
// running one). Reading the real function keeps the catalogue honest:
// the set of values a SIGHUP can change is whatever the code says it
// is, not whatever this file claims.
func reloadPathFields(t *testing.T, funcName string, receivers ...string) map[string]bool {
	t.Helper()

	fset := token.NewFileSet()
	path := filepath.Join("..", "..", "cmd", "sorotrail", "reload.go")
	f, err := parser.ParseFile(fset, path, nil, 0)
	require.NoErrorf(t, err, "parse %s", path)

	recv := make(map[string]bool, len(receivers))
	for _, r := range receivers {
		recv[r] = true
	}

	out := map[string]bool{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != funcName {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && recv[id.Name] {
				out[sel.Sel.Name] = true
			}
			return true
		})
	}
	require.NotEmptyf(t, out, "%s no longer touches %v fields — the reload path moved; update this catalogue", funcName, receivers)
	return out
}

// fieldSetToEnv inverts the env→field map over a set of field names, so
// code-derived field sets and catalogue-derived env sets can be
// compared directly.
func fieldSetToEnv(fields map[string]bool, envToField map[string]string) map[string]bool {
	out := map[string]bool{}
	for env, field := range envToField {
		if fields[field] {
			out[env] = true
		}
	}
	return out
}

// TestEveryValueIsClassifiedStaticOrReloadable makes the distinction
// visible in the configuration table and keeps it true: every env var
// Config reads carries a Reload cell, and the cell says "SIGHUP"
// exactly when the value is on the reload path.
func TestEveryValueIsClassifiedStaticOrReloadable(t *testing.T) {
	fields := envFieldNamesFromConfig()
	require.NotEmpty(t, fields, "no env vars found in Config")

	rows := configTableRows(t)
	require.NotEmpty(t, rows, "no variable rows found in the README Configuration table")

	for env := range fields {
		t.Run(env, func(t *testing.T) {
			row, ok := rows[env]
			require.Truef(t, ok, "env var %s is read by Config but missing from the Configuration table", env)

			want := "restart"
			if reloadableValues[env] != "" {
				want = "SIGHUP"
			}
			assert.Equalf(t, want, row[3],
				"the README says %s is reloadable via %q, but the reload catalogue says %q — "+
					"the Reload cell must match the code", env, row[3], want)
		})
	}

	for env := range reloadableValues {
		_, ok := fields[env]
		assert.Truef(t, ok, "%s is catalogued as reloadable but Config no longer reads it", env)
	}
	for env := range warnedStaticValues {
		_, ok := fields[env]
		assert.Truef(t, ok, "%s is catalogued as a warned static but Config no longer reads it", env)
		assert.NotContainsf(t, reloadableValues, env,
			"%s cannot be both reloadable and a restart-required topology value", env)
	}
}

// TestReloadableValuesMatchTheReloadPath ties the catalogue to the code
// that performs the reload, both directions: the fields applyReload
// applies are exactly the reloadable values, and the fields
// warnIgnoredTopologyChanges announces are exactly the warned statics.
func TestReloadableValuesMatchTheReloadPath(t *testing.T) {
	envToField := envFieldNamesFromConfig()

	applied := fieldSetToEnv(reloadPathFields(t, "applyReload", "next"), envToField)
	wantApplied := map[string]bool{}
	for env := range reloadableValues {
		wantApplied[env] = true
	}
	assert.Equalf(t, wantApplied, applied,
		"applyReload's applied set and the reloadable catalogue must agree — "+
			"a value that lands live must be documented as SIGHUP, and vice versa")

	warned := fieldSetToEnv(reloadPathFields(t, "warnIgnoredTopologyChanges", "old", "next"), envToField)
	wantWarned := map[string]bool{}
	for env := range warnedStaticValues {
		wantWarned[env] = true
	}
	assert.Equalf(t, wantWarned, warned,
		"warnIgnoredTopologyChanges' announced set and the warned-static catalogue must agree")
}

// TestStaticValuesAreSnapshotsReadOnce pins what "read exactly once, at
// startup" means mechanically: Load returns a Config value, and nothing
// in the process re-reads the environment behind the caller's back, so
// a running configuration can only change where code explicitly applies
// a new one — which, for static values, is a restart.
func TestStaticValuesAreSnapshotsReadOnce(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/one")
	t.Setenv("POLL_INTERVAL", "5s")
	t.Setenv("LOG_LEVEL", "info")

	first, err := Load()
	require.NoError(t, err)
	require.Equal(t, "postgres://localhost/one", first.DatabaseURL)

	t.Setenv("DATABASE_URL", "postgres://localhost/two")

	assert.Equalf(t, "postgres://localhost/one", first.DatabaseURL,
		"a loaded Config is a snapshot: changing the environment must not mutate it, "+
			"which is what makes a static value's lifetime exactly one process run")
	assert.Equal(t, 5*time.Second, first.PollInterval,
		"the snapshot holds every field, not just the warned topology ones")

	second, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "postgres://localhost/two", second.DatabaseURL,
		"the next Load — startup, or a SIGHUP reload — reads the environment afresh")
	assert.Equal(t, "postgres://localhost/one", first.DatabaseURL,
		"loading again must leave the previous snapshot untouched")
}

// TestReloadableGuardsHold pins the two invariants the reload path
// relies on when it applies the reloadable subset: config.Validate
// rejects a non-positive POLL_INTERVAL (applyReload's SetPollInterval
// guard is documented as unreachable because of it), and ParseLogLevel
// maps every documented level to a slog level rather than silently
// demoting a typo to info mid-reload.
func TestReloadableGuardsHold(t *testing.T) {
	t.Run("POLL_INTERVAL must be positive", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://localhost/test")
		cfg, err := Load()
		require.NoError(t, err)

		cfg.PollInterval = 0
		err = cfg.Validate()
		require.Error(t, err, "a zero POLL_INTERVAL must not survive validation — "+
			"applyReload's SetPollInterval guard depends on it")
		assert.Contains(t, err.Error(), "POLL_INTERVAL",
			"the rejection must name the variable an operator set")
	})

	t.Run("every documented LOG_LEVEL maps to a slog level", func(t *testing.T) {
		levels := map[string]slog.Level{
			"debug": slog.LevelDebug,
			"info":  slog.LevelInfo,
			"warn":  slog.LevelWarn,
			"error": slog.LevelError,
		}
		for raw, want := range levels {
			assert.Equalf(t, want, ParseLogLevel(raw), "LOG_LEVEL=%s must map exactly", raw)
			assert.Equalf(t, want, ParseLogLevel(strings.ToUpper(raw)),
				"LOG_LEVEL=%s must be case-insensitive, as the reload path passes operator input straight through", raw)
		}
	})
}
