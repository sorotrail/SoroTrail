package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestCLIBackupAndRestore_EndToEndDrill(t *testing.T) {
	tmpDir := t.TempDir()
	srcDB := "sqlite://" + filepath.Join(tmpDir, "cli-src.db")
	require.NoError(t, store.Migrate(srcDB))

	// Seed watched contract + ingestion checkpoint via store API.
	archive, err := store.CreateBackup(context.Background(), srcDB, store.BackupOptions{})
	require.NoError(t, err)
	archive.Tables.WatchedContracts = []store.WatchedContractBackupRow{
		{
			Network:    "default",
			ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			AddedAt:    "2026-04-10T00:00:00Z",
		},
	}
	archive.Tables.IngestionState = []store.IngestionStateBackupRow{
		{
			Network:            "default",
			LastIngestedLedger: 612345,
			LastCursor:         "0612345-0001-0000-0000",
			UpdatedAt:          "2026-04-10T00:00:00Z",
		},
	}
	archive.Tables.Subscriptions = []store.SubscriptionBackupRow{
		{
			ID:        1,
			URL:       "https://hooks.example.com/soroban",
			Filters:   "{}",
			Secret:    "whsec_cli_drill_secret",
			Enabled:   true,
			CreatedAt: "2026-04-10T00:00:00Z",
		},
	}
	sum, err := store.ComputeIntegritySHA256(archive)
	require.NoError(t, err)
	archive.IntegritySHA256 = sum
	_, err = store.RestoreBackup(context.Background(), srcDB, archive, store.RestoreOptions{Force: true})
	require.NoError(t, err)

	// 1. Run `sorotrail backup` with --secrets=encrypt via CLI.
	backupPath := filepath.Join(tmpDir, "cli-backup.json")
	var outBuf, errBuf bytes.Buffer
	err = runBackupTo(&outBuf, &errBuf, []string{
		"--database-url", srcDB,
		"--output", backupPath,
		"--secrets", "encrypt",
		"--passphrase", "cli-drill-passphrase",
	})
	require.NoError(t, err)

	var backupSummary map[string]any
	require.NoError(t, json.Unmarshal(outBuf.Bytes(), &backupSummary))
	assert.Equal(t, "ok", backupSummary["status"])
	assert.Equal(t, "encrypt", backupSummary["secret_mode"])

	// 2. Restore into a fresh migrated database via `sorotrail restore --verify`.
	destDB := "sqlite://" + filepath.Join(tmpDir, "cli-dest.db")
	require.NoError(t, store.Migrate(destDB))

	outBuf.Reset()
	errBuf.Reset()
	err = runRestoreTo(&outBuf, &errBuf, []string{
		"--database-url", destDB,
		"--input", backupPath,
		"--passphrase", "cli-drill-passphrase",
		"--verify",
	})
	require.NoError(t, err)

	var restoreRes store.RestoreResult
	require.NoError(t, json.Unmarshal(outBuf.Bytes(), &restoreRes))
	require.NotNil(t, restoreRes.Verification)
	assert.Empty(t, restoreRes.Verification.Mismatches)
	assert.Equal(t, 1, restoreRes.RestoredTables["watched_contracts"])
	assert.Equal(t, 1, restoreRes.RestoredTables["ingestion_state"])
	assert.Equal(t, 1, restoreRes.RestoredTables["subscriptions"])

	// 3. Re-running restore on the now-populated DB without --force fails with ErrRestoreTargetNotEmpty.
	outBuf.Reset()
	errBuf.Reset()
	err = runRestoreTo(&outBuf, &errBuf, []string{
		"--database-url", destDB,
		"--input", backupPath,
		"--passphrase", "cli-drill-passphrase",
	})
	require.ErrorIs(t, err, store.ErrRestoreTargetNotEmpty)

	// 4. Help flags succeed with exit code 0.
	require.NoError(t, dispatch([]string{"backup", "--help"}))
	require.NoError(t, dispatch([]string{"restore", "--help"}))

	// 5. Missing required flags return config exit code (2).
	err = runBackupTo(&outBuf, &errBuf, nil)
	assert.Equal(t, exitConfigError, exitCode(err))
	err = runRestoreTo(&outBuf, &errBuf, nil)
	assert.Equal(t, exitConfigError, exitCode(err))

	// Ensure backup file exists on disk.
	_, statErr := os.Stat(backupPath)
	require.NoError(t, statErr)
}
