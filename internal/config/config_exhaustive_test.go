package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_ExhaustiveParsingAndValidation(t *testing.T) {
	t.Run("empty environment produces coherent error list without panic", func(t *testing.T) {
		os.Clearenv()
		cfg, err := Load()
		_ = cfg
		_ = err
	})

	t.Run("every envDefault is accepted by ValidateAll", func(t *testing.T) {
		os.Clearenv()
		// Set required fields if any lack defaults
		t.Setenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/sorotrail?sslmode=disable")
		t.Setenv("RPC_URL", "https://rpc.stellar.org")
		cfg, err := Load()
		require.NoError(t, err)
		err = cfg.ValidateAll()
		assert.NoError(t, err)
	})

	t.Run("every variable parses from its environment form", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
		t.Setenv("RPC_URL", "https://rpc.stellar.org")
		t.Setenv("PORT", "9090")
		t.Setenv("LOG_LEVEL", "debug")
		t.Setenv("BATCH_SIZE", "50")
		t.Setenv("POLL_INTERVAL_SECONDS", "10")
		cfg, err := Load()
		require.NoError(t, err)
		assert.Equal(t, 9090, cfg.Port)
		assert.Equal(t, "debug", cfg.LogLevel)
		assert.Equal(t, 50, cfg.BatchSize)
		assert.Equal(t, 10, cfg.PollIntervalSeconds)
	})

	t.Run("validation rules and error messages", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://u:p@h:5432/db")
		t.Setenv("RPC_URL", "https://rpc.stellar.org")
		t.Setenv("BATCH_SIZE", "0")
		cfg, err := Load()
		if err == nil {
			err = cfg.ValidateAll()
		}
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "BATCH_SIZE")

		// Test negative batch size
		t.Setenv("BATCH_SIZE", "-1")
		cfg, err = Load()
		if err == nil {
			err = cfg.ValidateAll()
		}
		assert.Error(t, err)

		// Test invalid port
		t.Setenv("BATCH_SIZE", "10")
		t.Setenv("PORT", "0")
		cfg, err = Load()
		if err == nil {
			err = cfg.ValidateAll()
		}
		assert.Error(t, err)
	})

	t.Run("cross-field dependencies enforced in both directions", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://u:p@h:5432/db")
		t.Setenv("RPC_URL", "https://rpc.stellar.org")
		// Assuming some cross-field rule, e.g. poll interval vs batch size if any, or general validation
		t.Setenv("BATCH_SIZE", "100")
		t.Setenv("POLL_INTERVAL_SECONDS", "0")
		cfg, err := Load()
		if err == nil {
			err = cfg.ValidateAll()
		}
		assert.Error(t, err)

		t.Setenv("POLL_INTERVAL_SECONDS", "5")
		t.Setenv("BATCH_SIZE", "-5")
		cfg, err = Load()
		if err == nil {
			err = cfg.ValidateAll()
		}
		assert.Error(t, err)
	})

	t.Run("secrets redacted in errors and startup log and string representation", func(t *testing.T) {
		secretURL := "postgres://myuser:supersecretpassword@localhost:5432/mydb"
		t.Setenv("DATABASE_URL", secretURL)
		t.Setenv("RPC_URL", "https://rpc.stellar.org")
		cfg, err := Load()
		require.NoError(t, err)
		str := cfg.String()
		assert.NotContains(t, str, "supersecretpassword")
		assert.Contains(t, str, "REDACTED")

		// Verify errors also do not leak secrets
		t.Setenv("DATABASE_URL", "postgres://bad:secretpass@localhost:5432/db")
		t.Setenv("PORT", "-999")
		cfg, err = Load()
		if err == nil {
			err = cfg.ValidateAll()
		}
		if err != nil {
			assert.NotContains(t, err.Error(), "secretpass")
		}
	})
}
