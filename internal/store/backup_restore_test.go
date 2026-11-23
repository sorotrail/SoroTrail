package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestSQLiteByURL(t *testing.T, dbURL string) (*SQLite, func()) {
	t.Helper()
	db, _, err := openSQLForBackup(dbURL)
	require.NoError(t, err)
	return NewSQLite(db), func() { _ = db.Close() }
}

func populateRealisticDRDatabase(t *testing.T, dbURL string, eventCount int) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, Migrate(dbURL))
	st, closeFn := openTestSQLiteByURL(t, dbURL)
	defer closeFn()

	contracts := []string{
		"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		"CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75",
		"CB23WRDQWGSP6YPMY4UV5C4OW5CBTXKYN3XEATG7KJEZCXMJBYEHOUOV",
	}

	for i, cid := range contracts {
		require.NoError(t, st.AddWatchedContract(ctx, WatchedContract{ContractID: cid}))
		require.NoError(t, st.SaveContractCursor(ctx, ContractCursor{
			ContractID:         cid,
			LastIngestedLedger: int64(500000 + eventCount - 1),
			LastCursor:         fmt.Sprintf("0500%03d-0001-0000-%04d", eventCount-1, i),
		}))
		require.NoError(t, st.SetContractSpec(ctx, fmt.Sprintf("wasm_hash_%d", i), cid, []byte(`[{"name":"transfer","topic_specs":[{"name":"from","type":"address"},{"name":"to","type":"address"}],"value_spec":{"name":"amount","type":"i128"}}]`)))
	}

	// Upload and update spec overrides to populate both contract_spec_overrides and contract_spec_override_history.
	require.NoError(t, st.SetContractSpecOverrideWithHistory(ctx, contracts[0], []byte(`{"events":[{"name":"transfer","value_spec":{"name":"amount","type":"i128"}}]}`), "sorotrail_ops01"))
	require.NoError(t, st.SetContractSpecOverrideWithHistory(ctx, contracts[0], []byte(`{"events":[{"name":"transfer","doc":"v2 override","value_spec":{"name":"amount","type":"i128"}}]}`), "sorotrail_ops02"))
	require.NoError(t, st.SetContractSpecOverrideWithHistory(ctx, contracts[1], []byte(`{"events":[{"name":"mint"}]}`), "sorotrail_ops01"))

	pollTime := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	require.NoError(t, st.SaveIngestionState(ctx, IngestionState{
		LastIngestedLedger: int64(500000 + eventCount - 1),
		LastCursor:         fmt.Sprintf("0500%03d-0001-0000-0002", eventCount-1),
		LastSuccessfulPoll: &pollTime,
	}))

	require.NoError(t, st.SaveAuditState(ctx, AuditState{
		VerifiedThroughLedger: int64(500000 + eventCount - 1),
	}))
	_, err := st.RecordAuditFinding(ctx, AuditFinding{
		FromLedger:    500010,
		ToLedger:      500020,
		ExpectedCount: 15,
		ActualCount:   14,
		MissingIDs:    []string{"0500015-0001-0000-0000"},
	})
	require.NoError(t, err)

	_, err = st.db.ExecContext(ctx, `INSERT INTO replay_state (id, from_ledger, to_ledger, last_event_id, processed, changed, skipped, started_at, updated_at) VALUES (1, 500000, ?, '0500120-0001-0000-0000', 120, 18, 102, ?, ?)`,
		int64(500000+eventCount-1), pollTime.Add(-5*time.Minute).Format(time.RFC3339Nano), pollTime.Format(time.RFC3339Nano))
	require.NoError(t, err)

	_, err = st.db.ExecContext(ctx, `INSERT INTO subscriptions (url, filters, secret, enabled, failure_count, created_at) VALUES (?, ?, ?, 1, 0, ?)`,
		"https://hooks.example.com/soroban/transfers",
		fmt.Sprintf(`{"contract_id":%q}`, contracts[0]),
		"whsec_live_super_secret_hmac_key_01",
		pollTime.Format(time.RFC3339Nano),
	)
	require.NoError(t, err)

	_, err = st.db.ExecContext(ctx, `INSERT INTO subscriptions (url, filters, secret, enabled, failure_count, created_at) VALUES (?, ?, ?, 1, 0, ?)`,
		"https://hooks.example.com/soroban/all",
		`{}`,
		"whsec_live_super_secret_hmac_key_02",
		pollTime.Format(time.RFC3339Nano),
	)
	require.NoError(t, err)

	events := make([]Event, 0, eventCount)
	baseTime := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	for i := 0; i < eventCount; i++ {
		ledger := int64(500000 + i)
		cid := contracts[i%len(contracts)]
		events = append(events, Event{
			ID:               fmt.Sprintf("0500%03d-0001-0000-%04d", i, i%4),
			ContractID:       cid,
			Ledger:           ledger,
			Type:             "contract",
			TxHash:           fmt.Sprintf("%064x", i+1),
			TxIndex:          1,
			OpIndex:          0,
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:            json.RawMessage(fmt.Sprintf(`{"amount":%d}`, (i+1)*100)),
			CreatedAt:        baseTime.Add(time.Duration(i) * time.Second),
		})
	}
	_, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)
}

func TestDisasterRecoveryDrill_FullAndConfigOnlyRestoreWithMeasuredRPORTO(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	primaryDBPath := filepath.Join(tmpDir, "primary.db")
	primaryURL := "sqlite://" + primaryDBPath
	const seededEvents = 300

	populateRealisticDRDatabase(t, primaryURL, seededEvents)

	// 1. Take both a config+checkpoint backup and a full backup (including events).
	configBackupPath := filepath.Join(tmpDir, "backup-config.json")
	fullBackupPath := filepath.Join(tmpDir, "backup-full.json")

	t0 := time.Now()
	cfgArchive, err := CreateBackup(ctx, primaryURL, BackupOptions{
		IncludeEvents: false,
		SecretMode:    SecretModeInclude,
	})
	require.NoError(t, err)
	require.NoError(t, WriteBackupFile(configBackupPath, cfgArchive))
	cfgBackupDuration := time.Since(t0)

	t1 := time.Now()
	fullArchive, err := CreateBackup(ctx, primaryURL, BackupOptions{
		IncludeEvents: true,
		SecretMode:    SecretModeInclude,
	})
	require.NoError(t, err)
	require.NoError(t, WriteBackupFile(fullBackupPath, fullArchive))
	fullBackupDuration := time.Since(t1)

	// Verify 0600 file permissions on plaintext secret backups.
	fi, err := os.Stat(fullBackupPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), fi.Mode().Perm(), "SecretModeInclude backup file must be 0600")

	// Verify EventSummary is populated even when IncludeEvents=false.
	assert.Equal(t, int64(seededEvents), cfgArchive.EventSummary.TotalCount)
	assert.Equal(t, uint32(500000), cfgArchive.EventSummary.MinLedger)
	assert.Equal(t, uint32(500000+seededEvents-1), cfgArchive.EventSummary.MaxLedger)
	assert.Empty(t, cfgArchive.Tables.Events)
	assert.Len(t, fullArchive.Tables.Events, seededEvents)

	// 2. Simulate catastrophic loss of primary database.
	require.NoError(t, os.Remove(primaryDBPath))
	_, err = os.Stat(primaryDBPath)
	require.True(t, os.IsNotExist(err))

	// 3. Drill Path A: Full Restore (config + checkpoints + events) into fresh migrated DB.
	restoredFullDBPath := filepath.Join(tmpDir, "restored-full.db")
	restoredFullURL := "sqlite://" + restoredFullDBPath

	rtoStart := time.Now()
	require.NoError(t, Migrate(restoredFullURL))
	loadedFull, err := ReadBackupFile(fullBackupPath)
	require.NoError(t, err)

	fullRes, err := RestoreBackup(ctx, restoredFullURL, loadedFull, RestoreOptions{
		Verify: true,
	})
	require.NoError(t, err)
	fullRTO := time.Since(rtoStart)

	require.NotNil(t, fullRes.Verification)
	assert.Empty(t, fullRes.Verification.Mismatches)
	assert.False(t, fullRes.Verification.RetentionReport.BackfillRequired)
	assert.Equal(t, 3, fullRes.RestoredTables["watched_contracts"])
	assert.Equal(t, 3, fullRes.RestoredTables["contract_cursors"])
	assert.Equal(t, 1, fullRes.RestoredTables["ingestion_state"])
	assert.Equal(t, 1, fullRes.RestoredTables["audit_state"])
	assert.Equal(t, 1, fullRes.RestoredTables["audit_findings"])
	assert.Equal(t, 1, fullRes.RestoredTables["replay_state"])
	assert.Equal(t, 2, fullRes.RestoredTables["subscriptions"])
	assert.Equal(t, 3, fullRes.RestoredTables["contract_specs"])
	assert.Equal(t, 2, fullRes.RestoredTables["contract_spec_overrides"])
	assert.Equal(t, 3, fullRes.RestoredTables["contract_spec_override_history"])
	assert.Equal(t, seededEvents, fullRes.RestoredTables["events"])

	// Open the restored store through the production SQLite adapter and assert
	// live read paths return the exact pre-disaster state (RPO = 0).
	stFull, closeFull := openTestSQLiteByURL(t, restoredFullURL)
	defer closeFull()

	watched, err := stFull.ListWatchedContracts(ctx)
	require.NoError(t, err)
	assert.Len(t, watched, 3)

	ingState, err := stFull.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(500000+seededEvents-1), ingState.LastIngestedLedger)
	require.NotNil(t, ingState.LastSuccessfulPoll)

	audState, err := stFull.GetAuditState(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, int64(500000+seededEvents-1), audState.VerifiedThroughLedger)

	subs, err := stFull.ListSubscriptions(ctx, SubscriptionOwner{})
	require.NoError(t, err)
	require.Len(t, subs, 2)
	assert.Equal(t, "whsec_live_super_secret_hmac_key_01", subs[0].Secret)
	assert.True(t, subs[0].Enabled)

	overrideJSON, err := stFull.GetContractSpecOverride(ctx, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC")
	require.NoError(t, err)
	assert.Contains(t, string(overrideJSON), "v2 override")

	historyRows, _, err := stFull.ListContractSpecOverrideHistory(ctx, "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC", 10, "")
	require.NoError(t, err)
	assert.Len(t, historyRows, 2)

	queriedEvents, _, err := stFull.QueryEvents(ctx, EventFilter{Limit: 500})
	require.NoError(t, err)
	assert.Len(t, queriedEvents, seededEvents)

	// 4. Drill Path B: Config+Checkpoint-only Restore with Retention Floor check.
	restoredCfgDBPath := filepath.Join(tmpDir, "restored-cfg.db")
	restoredCfgURL := "sqlite://" + restoredCfgDBPath
	require.NoError(t, Migrate(restoredCfgURL))

	loadedCfg, err := ReadBackupFile(configBackupPath)
	require.NoError(t, err)
	cfgRes, err := RestoreBackup(ctx, restoredCfgURL, loadedCfg, RestoreOptions{
		Verify:             true,
		RetentionMinLedger: 500100,
	})
	require.NoError(t, err)
	require.NotNil(t, cfgRes.Verification)
	assert.True(t, cfgRes.Verification.RetentionReport.BackfillRequired)
	assert.NotEmpty(t, cfgRes.Verification.RetentionReport.Warnings)
	joinedWarnings := strings.Join(cfgRes.Verification.RetentionReport.Warnings, " | ")
	assert.Contains(t, joinedWarnings, "sorotrail backfill --from 500000 --to 500299")
	assert.Contains(t, joinedWarnings, "retention floor ledger 500100")

	assert.Equal(t, 16, len(fullRes.Verification.TableCounts)-1, "all 16 irreplaceable tables must be verified")
	t.Logf("DR Drill Measured Timings (SQLite, %d events, 16 irreplaceable tables): config_backup=%s full_backup=%s full_restore_tx=%s total_RTO=%s RPO=0_rows",
		seededEvents, cfgBackupDuration, fullBackupDuration, fullRes.Duration, fullRTO)
}

func TestBackupRestore_RefusesPopulatedDatabaseWithoutForceAndSucceedsWithForce(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	srcURL := "sqlite://" + filepath.Join(tmpDir, "src.db")
	populateRealisticDRDatabase(t, srcURL, 15)

	archive, err := CreateBackup(ctx, srcURL, BackupOptions{IncludeEvents: true})
	require.NoError(t, err)

	// Target DB already has a watched contract row.
	targetURL := "sqlite://" + filepath.Join(tmpDir, "target.db")
	require.NoError(t, Migrate(targetURL))
	stTarget, closeTarget := openTestSQLiteByURL(t, targetURL)
	existingContract := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"
	require.NoError(t, stTarget.AddWatchedContract(ctx, WatchedContract{ContractID: existingContract}))
	closeTarget()

	// Without Force: must refuse with ErrRestoreTargetNotEmpty and leave existingContract intact.
	_, err = RestoreBackup(ctx, targetURL, archive, RestoreOptions{Force: false})
	require.ErrorIs(t, err, ErrRestoreTargetNotEmpty)

	stCheck, closeCheck := openTestSQLiteByURL(t, targetURL)
	watchedBeforeForce, err := stCheck.ListWatchedContracts(ctx)
	require.NoError(t, err)
	require.Len(t, watchedBeforeForce, 1)
	assert.Equal(t, existingContract, watchedBeforeForce[0].ContractID)
	closeCheck()

	// With Force: overwrites cleanly and passes verification.
	res, err := RestoreBackup(ctx, targetURL, archive, RestoreOptions{
		Force:  true,
		Verify: true,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, res.RestoredTables["watched_contracts"])
	assert.Equal(t, 15, res.RestoredTables["events"])
}

func TestBackupRestore_OlderSchemaVersionCompatibilityAndNewerSchemaRejection(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a database migrated only to SQLite schema v4 (before 0005_network_alignment
	// and 0007_contract_spec_override_history).
	legacyDBURL := "sqlite://" + filepath.Join(tmpDir, "legacy-v4.db")
	require.NoError(t, MigrateUp(legacyDBURL, 4))
	migSt, err := MigrateStatus(legacyDBURL)
	require.NoError(t, err)
	require.False(t, migSt.Dirty)
	require.Equal(t, uint(4), migSt.Version)

	db, _, err := openSQLForBackup(legacyDBURL)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO watched_contracts (contract_id, added_at) VALUES ('CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC', '2026-04-10T00:00:00Z')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO ingestion_state (network, last_ingested_ledger, last_cursor, updated_at) VALUES ('default', 410000, '0410000-0001', '2026-04-10T00:00:00Z')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO audit_state (id, verified_through_ledger, updated_at) VALUES (1, 410000, '2026-04-10T00:00:00Z')`)
	require.NoError(t, err)
	db.Close()

	legacyArchive, err := CreateBackup(ctx, legacyDBURL, BackupOptions{IncludeEvents: false})
	require.NoError(t, err)
	assert.Equal(t, uint(4), legacyArchive.SchemaVersion)

	// Restore the v4 backup into a modern latest-schema database.
	modernDBURL := "sqlite://" + filepath.Join(tmpDir, "modern-latest.db")
	require.NoError(t, Migrate(modernDBURL))
	modernMigSt, err := MigrateStatus(modernDBURL)
	require.NoError(t, err)
	require.False(t, modernMigSt.Dirty)
	require.Greater(t, modernMigSt.Version, uint(4))

	res, err := RestoreBackup(ctx, modernDBURL, legacyArchive, RestoreOptions{Verify: true})
	require.NoError(t, err)
	assert.Equal(t, uint(4), res.ArchiveSchemaVer)
	assert.Equal(t, modernMigSt.Version, res.TargetSchemaVer)
	assert.Equal(t, 1, res.RestoredTables["watched_contracts"])

	// Conversely, attempting to restore a modern backup into a v4 database must be rejected with ErrBackupNewerSchema.
	modernArchive, err := CreateBackup(ctx, modernDBURL, BackupOptions{IncludeEvents: false})
	require.NoError(t, err)
	assert.Equal(t, modernMigSt.Version, modernArchive.SchemaVersion)

	emptyV4URL := "sqlite://" + filepath.Join(tmpDir, "empty-v4.db")
	require.NoError(t, MigrateUp(emptyV4URL, 4))
	_, err = RestoreBackup(ctx, emptyV4URL, modernArchive, RestoreOptions{})
	require.ErrorIs(t, err, ErrBackupNewerSchema)
}

func TestBackupRestore_SecretModes_IncludeRedactEncrypt(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	srcURL := "sqlite://" + filepath.Join(tmpDir, "secrets-src.db")
	populateRealisticDRDatabase(t, srcURL, 5)

	// 1. Redact mode: secrets replaced on disk, subscriptions disabled on restore.
	redactArchive, err := CreateBackup(ctx, srcURL, BackupOptions{
		SecretMode: SecretModeRedact,
	})
	require.NoError(t, err)
	for _, sub := range redactArchive.Tables.Subscriptions {
		assert.Equal(t, redactedSecretPlaceholder, sub.Secret)
	}

	redactRestoreURL := "sqlite://" + filepath.Join(tmpDir, "secrets-redact-dest.db")
	require.NoError(t, Migrate(redactRestoreURL))
	redactRes, err := RestoreBackup(ctx, redactRestoreURL, redactArchive, RestoreOptions{Verify: true})
	require.NoError(t, err)
	assert.Equal(t, 2, redactRes.RedactedSubsLocked)

	stRedact, closeRedact := openTestSQLiteByURL(t, redactRestoreURL)
	subsRedact, err := stRedact.ListSubscriptions(ctx, SubscriptionOwner{})
	require.NoError(t, err)
	for _, s := range subsRedact {
		assert.False(t, s.Enabled, "redacted subscription must be disabled on restore until secret rotation")
		assert.Equal(t, redactedSecretPlaceholder, s.Secret)
	}
	closeRedact()

	// 2. Encrypt mode: requires passphrase, encrypts secrets on disk, rejects wrong passphrase, decrypts with right passphrase.
	_, err = CreateBackup(ctx, srcURL, BackupOptions{SecretMode: SecretModeEncrypt})
	require.ErrorIs(t, err, ErrSecretPassphraseRequired)

	encArchive, err := CreateBackup(ctx, srcURL, BackupOptions{
		SecretMode: SecretModeEncrypt,
		Passphrase: "correct-horse-battery-staple",
	})
	require.NoError(t, err)
	for _, sub := range encArchive.Tables.Subscriptions {
		assert.True(t, strings.HasPrefix(sub.Secret, encryptedSecretPrefix))
		assert.NotContains(t, sub.Secret, "whsec_live_super_secret")
	}

	encRestoreURL := "sqlite://" + filepath.Join(tmpDir, "secrets-enc-dest.db")
	require.NoError(t, Migrate(encRestoreURL))

	// Missing passphrase fails.
	_, err = RestoreBackup(ctx, encRestoreURL, encArchive, RestoreOptions{})
	require.ErrorIs(t, err, ErrSecretPassphraseRequired)

	// Wrong passphrase fails.
	_, err = RestoreBackup(ctx, encRestoreURL, encArchive, RestoreOptions{
		Passphrase: "wrong-passphrase",
	})
	require.ErrorIs(t, err, ErrSecretDecryptFailed)

	// Correct passphrase succeeds and restores plaintext secrets with enabled=true.
	encRes, err := RestoreBackup(ctx, encRestoreURL, encArchive, RestoreOptions{
		Passphrase: "correct-horse-battery-staple",
		Verify:     true,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, encRes.RedactedSubsLocked)

	stEnc, closeEnc := openTestSQLiteByURL(t, encRestoreURL)
	subsEnc, err := stEnc.ListSubscriptions(ctx, SubscriptionOwner{})
	require.NoError(t, err)
	require.Len(t, subsEnc, 2)
	assert.Equal(t, "whsec_live_super_secret_hmac_key_01", subsEnc[0].Secret)
	assert.True(t, subsEnc[0].Enabled)
	closeEnc()
}

func TestBackupRestore_CorruptArchiveRejected(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	srcURL := "sqlite://" + filepath.Join(tmpDir, "corrupt-src.db")
	populateRealisticDRDatabase(t, srcURL, 5)

	archivePath := filepath.Join(tmpDir, "corrupt.json")
	archive, err := CreateBackup(ctx, srcURL, BackupOptions{})
	require.NoError(t, err)
	require.NoError(t, WriteBackupFile(archivePath, archive))

	// Tamper with a contract ID inside the written JSON file without updating integrity_sha256.
	raw, err := os.ReadFile(archivePath)
	require.NoError(t, err)
	tampered := strings.Replace(string(raw), "500004", "999999", 1)
	require.NoError(t, os.WriteFile(archivePath, []byte(tampered), 0600))

	_, err = ReadBackupFile(archivePath)
	require.ErrorIs(t, err, ErrCorruptBackup)
}
