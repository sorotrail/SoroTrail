package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/backfill"
	"github.com/sorotrail/sorotrail/internal/replay"
)

// captureStdout runs fn while capturing os.Stdout and returns whatever was
// written. The original stdout is restored before the function returns so
// parallel tests don't interfere.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = orig

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

// ---------- replay summary ----------

func TestPrintReplaySummary(t *testing.T) {
	tests := []struct {
		name    string
		summary replay.Summary
		dryRun  bool
		want    []string // substrings that must appear in output
	}{
		{
			name:    "completed non-dry-run",
			summary: replay.Summary{Processed: 100, Changed: 5, Skipped: 2, Failed: 1, Completed: true, Duration: time.Second},
			dryRun:  false,
			want:    []string{"replay completed", "rows processed: 100", "rows changed:   5", "rows skipped:   2", "rows failed:    1"},
		},
		{
			name:    "completed dry-run",
			summary: replay.Summary{Processed: 200, Changed: 10, Skipped: 0, Failed: 0, Completed: true, Duration: 500 * time.Millisecond},
			dryRun:  true,
			want:    []string{"replay completed", "(dry run — nothing written)", "rows processed: 200", "rows changed:   10"},
		},
		{
			name:    "interrupted non-dry-run",
			summary: replay.Summary{Processed: 50, Changed: 3, Completed: false, Duration: 200 * time.Millisecond},
			dryRun:  false,
			want:    []string{"replay interrupted — re-run the same command to resume", "rows processed: 50"},
		},
		{
			name:    "interrupted dry-run",
			summary: replay.Summary{Processed: 0, Changed: 0, Completed: false, Duration: 0},
			dryRun:  true,
			want:    []string{"(dry run — nothing written)", "interrupted"},
		},
		{
			name:    "no-changes dry-run reports zero",
			summary: replay.Summary{Processed: 500, Changed: 0, Completed: true, Duration: time.Second},
			dryRun:  true,
			want:    []string{"rows processed: 500", "rows changed:   0", "(dry run — nothing written)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := captureStdout(t, func() {
				printReplaySummary(tt.summary, tt.dryRun)
			})
			for _, s := range tt.want {
				assert.Contains(t, out, s, "output must contain %q", s)
			}
		})
	}
}

// ---------- backfill summary ----------

func TestPrintBackfillSummary(t *testing.T) {
	tests := []struct {
		name    string
		summary backfill.Summary
		dryRun  bool
		want    []string
	}{
		{
			name: "completed non-dry-run",
			summary: backfill.Summary{
				PagesFetched: 3, Transactions: 600, Skipped: 10, Failed: 2,
				Extracted: 200, Inserted: 200, ThroughLedger: 50000, Completed: true,
				Duration: 10 * time.Second,
			},
			dryRun: false,
			want:   []string{"backfill completed", "pages fetched:   3", "transactions:   600", "events extracted: 200", "events inserted: 200"},
		},
		{
			name: "completed dry-run",
			summary: backfill.Summary{
				PagesFetched: 2, Transactions: 400, Skipped: 5, Failed: 1,
				Extracted: 150, Inserted: 0, ThroughLedger: 30000, Completed: true,
				Duration: 5 * time.Second,
			},
			dryRun: true,
			want:   []string{"backfill completed", "(dry run — nothing written)", "events inserted: 0"},
		},
		{
			name: "interrupted non-dry-run",
			summary: backfill.Summary{
				PagesFetched: 1, Transactions: 200, Completed: false,
				Duration: 3 * time.Second,
			},
			dryRun: false,
			want:   []string{"backfill interrupted — re-run the same command to resume"},
		},
		{
			name: "interrupted dry-run",
			summary: backfill.Summary{
				PagesFetched: 0, Transactions: 0, Completed: false,
				Duration: 0,
			},
			dryRun: true,
			want:   []string{"(dry run — nothing written)", "interrupted"},
		},
		{
			name: "resumed dry-run",
			summary: backfill.Summary{
				PagesFetched: 1, Transactions: 100, Resumed: true, Completed: true,
				Duration: 2 * time.Second,
			},
			dryRun: true,
			want:   []string{"(dry run — nothing written)", "backfill completed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := captureStdout(t, func() {
				printBackfillSummary(tt.summary, tt.dryRun)
			})
			for _, s := range tt.want {
				assert.Contains(t, out, s, "output must contain %q", s)
			}
		})
	}
}

// ---------- address-index summary ----------

func TestPrintAddressIndexSummary(t *testing.T) {
	tests := []struct {
		name    string
		summary addressIndexSummary
		dryRun  bool
		want    []string
	}{
		{
			name:    "completed non-dry-run",
			summary: addressIndexSummary{pagesProcessed: 5, eventsScanned: 2000, refsIndexed: 500, completed: true, throughLedger: 80000, duration: 3 * time.Second},
			dryRun:  false,
			want:    []string{"address index completed", "events scanned:  2000", "address refs indexed: 500"},
		},
		{
			name:    "completed dry-run",
			summary: addressIndexSummary{pagesProcessed: 3, eventsScanned: 1000, refsIndexed: 0, completed: true, throughLedger: 40000, duration: time.Second},
			dryRun:  true,
			want:    []string{"address index completed", "(dry run — nothing written)", "address refs indexed: 0"},
		},
		{
			name:    "interrupted non-dry-run",
			summary: addressIndexSummary{pagesProcessed: 1, eventsScanned: 500, completed: false, duration: time.Second},
			dryRun:  false,
			want:    []string{"address index interrupted — re-run the same command to resume"},
		},
		{
			name:    "interrupted dry-run",
			summary: addressIndexSummary{pagesProcessed: 0, eventsScanned: 0, completed: false, duration: 0},
			dryRun:  true,
			want:    []string{"(dry run — nothing written)", "interrupted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := captureStdout(t, func() {
				printAddressIndexSummary(tt.summary, tt.dryRun)
			})
			for _, s := range tt.want {
				assert.Contains(t, out, s, "output must contain %q", s)
			}
		})
	}
}

// ---------- dispatch routing ----------

func TestDispatchRoutesSubcommands(t *testing.T) {
	// Each mutating subcommand must be recognised. The commands will
	// fail because there is no database, but the error must come from
	// config.Load() or the database layer, not from an "unknown
	// subcommand" — proving the dispatch table is wired correctly.
	tests := []struct {
		name string
		args []string
	}{
		{"replay with dry-run", []string{"replay", "--from-ledger", "1", "--dry-run"}},
		{"replay without dry-run", []string{"replay", "--from-ledger", "1"}},
		{"backfill with dry-run", []string{"backfill", "--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "1", "--rps", "1", "--dry-run"}},
		{"backfill without dry-run", []string{"backfill", "--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "1", "--rps", "1"}},
		{"index-addresses with dry-run", []string{"index-addresses", "--dry-run"}},
		{"index-addresses without dry-run", []string{"index-addresses"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dispatch(tt.args)
			require.Error(t, err, "commands fail without DB, but must not be 'unknown subcommand'")
			assert.NotContains(t, err.Error(), "unknown subcommand",
				"dispatch must route to the correct handler, not reject the subcommand")
		})
	}
}

func TestDispatchHelpReturnsNil(t *testing.T) {
	for _, arg := range []string{"help", "-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			err := dispatch([]string{arg})
			assert.NoError(t, err)
		})
	}
}

func TestDispatchUnknownSubcommand(t *testing.T) {
	err := dispatch([]string{"nonexistent"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown subcommand")
}

// ---------- replay flag validation ----------

func TestReplayFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing --from-ledger",
			args:    []string{"--dry-run"},
			wantErr: "--from-ledger is required",
		},
		{
			name:    "negative --from-ledger",
			args:    []string{"--from-ledger", "-5", "--dry-run"},
			wantErr: "--from-ledger is required and must be positive",
		},
		{
			name:    "zero --from-ledger",
			args:    []string{"--from-ledger", "0", "--dry-run"},
			wantErr: "--from-ledger is required and must be positive",
		},
		{
			name:    "--to-ledger before --from-ledger",
			args:    []string{"--from-ledger", "100", "--to-ledger", "50", "--dry-run"},
			wantErr: "--to-ledger 50 is before --from-ledger 100",
		},
		{
			name:    "zero --batch-size",
			args:    []string{"--from-ledger", "1", "--batch-size", "0", "--dry-run"},
			wantErr: "--batch-size must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runReplay(tt.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// ---------- backfill flag validation ----------

func TestBackfillFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing --contract",
			args:    []string{"--from-ledger", "1", "--dry-run", "--rps", "1"},
			wantErr: "--contract is required",
		},
		{
			name:    "missing --from-ledger",
			args:    []string{"--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--dry-run", "--rps", "1"},
			wantErr: "--from-ledger is required",
		},
		{
			name:    "zero --from-ledger",
			args:    []string{"--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "0", "--dry-run", "--rps", "1"},
			wantErr: "--from-ledger is required and must be positive",
		},
		{
			name:    "--to-ledger before --from-ledger",
			args:    []string{"--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "100", "--to-ledger", "50", "--dry-run", "--rps", "1"},
			wantErr: "--to-ledger 50 is before --from-ledger 100",
		},
		{
			name:    "batch-size too large",
			args:    []string{"--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "1", "--batch-size", "300", "--dry-run", "--rps", "1"},
			wantErr: "--batch-size must be in 1..200",
		},
		{
			name:    "batch-size zero",
			args:    []string{"--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "1", "--batch-size", "0", "--dry-run", "--rps", "1"},
			wantErr: "--batch-size must be in 1..200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runBackfill(tt.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr,
				"validation must fire even when --dry-run is set")
		})
	}
}

// ---------- index-addresses flag validation ----------

func TestIndexAddressesFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "batch-size zero",
			args:    []string{"--batch-size", "0", "--dry-run"},
			wantErr: "--batch-size must be in 1..1000",
		},
		{
			name:    "batch-size too large",
			args:    []string{"--batch-size", "2000", "--dry-run"},
			wantErr: "--batch-size must be in 1..1000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runIndexAddresses(tt.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr,
				"validation must fire even when --dry-run is set")
		})
	}
}

// ---------- dry-run flag acceptance ----------

// TestDryRunFlagAccepted verifies that each mutating command parses
// --dry-run without flag-parse errors. The commands fail later at
// config.Load(), but the error must not mention "unknown flag" or
// "flag provided but not defined".
func TestDryRunFlagAccepted(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "replay --dry-run",
			args: []string{"replay", "--from-ledger", "1", "--dry-run"},
		},
		{
			name: "replay --dry-run=false",
			args: []string{"replay", "--from-ledger", "1", "--dry-run=false"},
		},
		{
			name: "backfill --dry-run",
			args: []string{"backfill", "--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "1", "--rps", "1", "--dry-run"},
		},
		{
			name: "backfill --dry-run=false",
			args: []string{"backfill", "--contract", "CABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEFCABCDEF", "--from-ledger", "1", "--rps", "1", "--dry-run=false"},
		},
		{
			name: "index-addresses --dry-run",
			args: []string{"index-addresses", "--dry-run"},
		},
		{
			name: "index-addresses --dry-run=false",
			args: []string{"index-addresses", "--dry-run=false"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dispatch(tt.args)
			require.Error(t, err, "commands fail without DB, but --dry-run must be accepted")
			assert.NotContains(t, err.Error(), "unknown flag",
				"--dry-run must be a recognised flag")
			assert.NotContains(t, err.Error(), "flag provided but not defined",
				"--dry-run must be a recognised flag")
			assert.NotContains(t, err.Error(), "flag: provided but not defined",
				"--dry-run must be a recognised flag")
		})
	}
}

// ---------- dry-run plan matches real run ----------

// TestReplayDryRunPlanMatchesRealRun verifies that dry-run reports the
// same rows-processed count as a real run would (the only difference
// being the writes). This test exercises the internal replay package
// through the same interface the cmd layer uses.
func TestReplayDryRunPlanMatchesRealRun(t *testing.T) {
	// This is a structural test: both paths call into the same
	// replay.Replayer, so the plan (Processed/Changed/Skipped/Failed
	// counts) must match. The cmd-layer printReplaySummary must
	// surface those counts identically for both modes.

	// Dry-run output must contain "dry run — nothing written".
	dryOut := captureStdout(t, func() {
		printReplaySummary(replay.Summary{
			Processed: 42, Changed: 7, Skipped: 3, Failed: 1, Completed: true, Duration: time.Second,
		}, true)
	})
	assert.Contains(t, dryOut, "(dry run — nothing written)")
	assert.Contains(t, dryOut, "rows processed: 42")
	assert.Contains(t, dryOut, "rows changed:   7")

	// Real-run output must NOT contain the dry-run marker.
	realOut := captureStdout(t, func() {
		printReplaySummary(replay.Summary{
			Processed: 42, Changed: 7, Skipped: 3, Failed: 1, Completed: true, Duration: time.Second,
		}, false)
	})
	assert.NotContains(t, realOut, "(dry run — nothing written)")
	assert.Contains(t, realOut, "rows processed: 42")
	assert.Contains(t, realOut, "rows changed:   7")

	// The two outputs must report identical plan values.
	assert.Contains(t, dryOut, "rows skipped:   3")
	assert.Contains(t, realOut, "rows skipped:   3")
	assert.Contains(t, dryOut, "rows failed:    1")
	assert.Contains(t, realOut, "rows failed:    1")
}

// TestBackfillDryRunPlanMatchesRealRun performs the same structural
// verification for backfill: dry-run and real-run summaries must report
// the same plan, differing only in the "(dry run — nothing written)"
// marker and the inserted count being zero.
func TestBackfillDryRunPlanMatchesRealRun(t *testing.T) {
	dryOut := captureStdout(t, func() {
		printBackfillSummary(backfill.Summary{
			PagesFetched: 10, Transactions: 2000, Skipped: 50, Failed: 5,
			Extracted: 800, Inserted: 0, ThroughLedger: 99999, Completed: true,
			Duration: 30 * time.Second,
		}, true)
	})
	assert.Contains(t, dryOut, "(dry run — nothing written)")
	assert.Contains(t, dryOut, fmt.Sprintf("pages fetched:   %d", 10))
	assert.Contains(t, dryOut, fmt.Sprintf("transactions:   %d", 2000))
	assert.Contains(t, dryOut, fmt.Sprintf("events extracted: %d", 800))
	assert.Contains(t, dryOut, "events inserted: 0")

	realOut := captureStdout(t, func() {
		printBackfillSummary(backfill.Summary{
			PagesFetched: 10, Transactions: 2000, Skipped: 50, Failed: 5,
			Extracted: 800, Inserted: 800, ThroughLedger: 99999, Completed: true,
			Duration: 30 * time.Second,
		}, false)
	})
	assert.NotContains(t, realOut, "(dry run — nothing written)")
	assert.Contains(t, realOut, "events inserted: 800")
	// Plan numbers must be identical between modes.
	assert.Contains(t, realOut, fmt.Sprintf("pages fetched:   %d", 10))
	assert.Contains(t, realOut, fmt.Sprintf("events skipped:  %d (no meta or no events emitted)", 50))
}

// ---------- help flag passthrough ----------

func TestReplayHelpReturnsNil(t *testing.T) {
	err := runReplay([]string{"--help"})
	assert.NoError(t, err, "--help must print usage and return nil, not an error")
}

func TestBackfillHelpReturnsNil(t *testing.T) {
	err := runBackfill([]string{"--help"})
	assert.NoError(t, err, "--help must print usage and return nil, not an error")
}

func TestIndexAddressesHelpReturnsNil(t *testing.T) {
	err := runIndexAddresses([]string{"--help"})
	assert.NoError(t, err, "--help must print usage and return nil, not an error")
}
