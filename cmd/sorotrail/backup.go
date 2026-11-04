package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sorotrail/sorotrail/internal/store"
)

func resolveBackupDatabaseURL(flagVal string) (string, error) {
	u := strings.TrimSpace(flagVal)
	if u == "" {
		u = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if u == "" {
		return "", &exitError{
			code: exitConfigError,
			err:  errors.New("DATABASE_URL is required (set DATABASE_URL or pass --database-url)"),
		}
	}
	return u, nil
}

func runBackup(args []string) error {
	return runBackupTo(os.Stdout, os.Stderr, args)
}

func runBackupTo(stdout, stderr io.Writer, args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, `usage: sorotrail backup --output <path> [flags]

Snapshot irreplaceable operator configuration and ingestion checkpoints
(watched_contracts, contract_cursors, ingestion_state, audit_state,
audit_findings, replay_state, backfill_state, subscriptions, api_keys,
tenants/grants, contract_specs, contract_spec_overrides, and
contract_spec_override_history) into an integrity-verified JSON archive.

Chain events are reconstructible via 'sorotrail backfill' and omitted by
default; pass --include-events for a self-contained snapshot.

flags:
  --output <path>                 destination archive path (required)
  --include-events                include reconstructible chain events in archive
  --secrets <include|redact|encrypt>
                                  how webhook secrets and key hashes are stored (default: include)
  --passphrase <str>              passphrase for --secrets=encrypt (or SOROTRAIL_BACKUP_PASSPHRASE)
  --database-url <url>            override DATABASE_URL environment variable
`)
	}

	outputPath := fs.String("output", "", "destination archive path")
	includeEvents := fs.Bool("include-events", false, "include reconstructible chain events in archive")
	secretsMode := fs.String("secrets", string(store.SecretModeInclude), "secret handling mode: include, redact, or encrypt")
	passphrase := fs.String("passphrase", "", "passphrase when --secrets=encrypt")
	dbURLFlag := fs.String("database-url", "", "override DATABASE_URL")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{code: exitConfigError, err: err}
	}
	if fs.NArg() > 0 {
		return &exitError{code: exitConfigError, err: fmt.Errorf("unexpected argument %q", fs.Arg(0))}
	}
	if strings.TrimSpace(*outputPath) == "" {
		fs.Usage()
		return &exitError{code: exitConfigError, err: errors.New("--output is required")}
	}

	dbURL, err := resolveBackupDatabaseURL(*dbURLFlag)
	if err != nil {
		return err
	}

	pass := strings.TrimSpace(*passphrase)
	if pass == "" {
		pass = strings.TrimSpace(os.Getenv("SOROTRAIL_BACKUP_PASSPHRASE"))
	}

	mode := store.SecretMode(strings.ToLower(strings.TrimSpace(*secretsMode)))
	if mode == store.SecretModeInclude {
		fmt.Fprintln(stderr, "warning: --secrets=include writes webhook signing secrets and API key hashes in plaintext (file permissions set to 0600); use --secrets=encrypt or --secrets=redact for untrusted storage")
	}

	archive, err := store.CreateBackup(context.Background(), dbURL, store.BackupOptions{
		IncludeEvents: *includeEvents,
		SecretMode:    mode,
		Passphrase:    pass,
	})
	if err != nil {
		return err
	}

	if err := store.WriteBackupFile(*outputPath, archive); err != nil {
		return err
	}

	summary := map[string]any{
		"status":           "ok",
		"output":           *outputPath,
		"backend":          archive.Backend,
		"schema_version":   archive.SchemaVersion,
		"include_events":   archive.IncludeEvents,
		"secret_mode":      archive.SecretMode,
		"integrity_sha256": archive.IntegritySHA256,
		"event_summary":    archive.EventSummary,
	}
	enc := json.NewEncoder(stdout)
	return enc.Encode(summary)
}

func runRestore(args []string) error {
	return runRestoreTo(os.Stdout, os.Stderr, args)
}

func runRestoreTo(stdout, stderr io.Writer, args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, `usage: sorotrail restore --input <path> [flags]

Restore a SoroTrail backup archive into a migrated database. Refuses to
overwrite a non-empty database unless --force is supplied.

flags:
  --input <path>                  source backup archive path (required)
  --force                         overwrite existing rows in target database
  --verify                        run post-restore completeness and retention checks (default: true)
  --passphrase <str>              passphrase for archives created with --secrets=encrypt
  --retention-min-ledger <uint>   active retention floor ledger for consistency checks
  --database-url <url>            override DATABASE_URL environment variable
`)
	}

	inputPath := fs.String("input", "", "source backup archive path")
	force := fs.Bool("force", false, "overwrite existing rows in target database")
	verify := fs.Bool("verify", true, "run post-restore completeness and retention verification")
	passphrase := fs.String("passphrase", "", "passphrase for encrypted secrets")
	retentionMin := fs.Uint("retention-min-ledger", 0, "active retention floor ledger")
	dbURLFlag := fs.String("database-url", "", "override DATABASE_URL")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return &exitError{code: exitConfigError, err: err}
	}
	if fs.NArg() > 0 {
		return &exitError{code: exitConfigError, err: fmt.Errorf("unexpected argument %q", fs.Arg(0))}
	}
	if strings.TrimSpace(*inputPath) == "" {
		fs.Usage()
		return &exitError{code: exitConfigError, err: errors.New("--input is required")}
	}

	dbURL, err := resolveBackupDatabaseURL(*dbURLFlag)
	if err != nil {
		return err
	}

	pass := strings.TrimSpace(*passphrase)
	if pass == "" {
		pass = strings.TrimSpace(os.Getenv("SOROTRAIL_BACKUP_PASSPHRASE"))
	}

	archive, err := store.ReadBackupFile(*inputPath)
	if err != nil {
		return err
	}

	res, err := store.RestoreBackup(context.Background(), dbURL, archive, store.RestoreOptions{
		Force:              *force,
		Verify:             *verify,
		Passphrase:         pass,
		RetentionMinLedger: uint32(*retentionMin),
	})
	if err != nil {
		return err
	}

	if res.RedactedSubsLocked > 0 {
		fmt.Fprintf(stderr, "warning: %d webhook subscription(s) had redacted secrets and were restored with enabled=false; rotate their secrets before re-enabling\n", res.RedactedSubsLocked)
	}
	if res.Verification != nil {
		for _, w := range res.Verification.RetentionReport.Warnings {
			fmt.Fprintf(stderr, "warning: %s\n", w)
		}
	}

	enc := json.NewEncoder(stdout)
	return enc.Encode(res)
}
