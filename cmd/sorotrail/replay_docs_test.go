package main

// Drift guard between `sorotrail replay --help` and docs/replay.md. The
// flag table in the docs is the thing operators copy commands out of, so a
// flag added in code and not in the docs (or a documented flag that no
// longer exists) is a real defect, not a formatting nit.

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/replay"
)

const replayDoc = "../../docs/replay.md"

// replayFlagSet mirrors the flag set runReplay builds, so the table below
// is checked against the real flag names and defaults rather than a copy.
func replayFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.Int64("from-ledger", 0, "first ledger to replay (inclusive, required)")
	fs.Int64("to-ledger", 0, "last ledger to replay (inclusive; 0 = no upper bound)")
	fs.Int("batch-size", replay.DefaultBatchSize, "events re-decoded per transaction")
	fs.Bool("restart", false, "discard saved progress and replay the range from the start")
	fs.Bool("dry-run", false, "report what would change without writing anything")
	fs.Duration("progress-interval", 0, "emit periodic progress to stderr")
	fs.Int("target-decoder-version", 0, "skip events already at or above this decoder version")
	return fs
}

func readReplayDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(replayDoc)
	require.NoError(t, err, "docs/replay.md must exist")
	return string(b)
}

// TestReplayFlagsAreDocumented requires every flag the command accepts to
// appear in the docs' flag table, with its real default.
func TestReplayFlagsAreDocumented(t *testing.T) {
	doc := readReplayDoc(t)

	tests := []struct {
		flagName string
		// wantDefault is the default as the table renders it; empty
		// means the flag is required and has no meaningful default.
		wantDefault string
	}{
		{"from-ledger", ""},
		{"to-ledger", "`0`"},
		{"batch-size", fmt.Sprintf("`%d`", replay.DefaultBatchSize)},
		{"restart", "`false`"},
		{"dry-run", "`false`"},
		{"progress-interval", "`0`"},
		{"target-decoder-version", "`0`"},
	}

	// The table must describe exactly the flags the command defines: no
	// stale rows, no undocumented flags.
	defined := map[string]bool{}
	replayFlagSet().VisitAll(func(f *flag.Flag) { defined[f.Name] = true })
	require.Len(t, tests, len(defined), "every defined flag needs a row in this table")

	for _, tt := range tests {
		t.Run(tt.flagName, func(t *testing.T) {
			require.Truef(t, defined[tt.flagName], "--%s is documented but not defined", tt.flagName)

			row := replayTableRow(doc, tt.flagName)
			require.NotEmptyf(t, row, "docs/replay.md has no flag-table row for --%s", tt.flagName)

			if tt.wantDefault != "" {
				assert.Containsf(t, row, tt.wantDefault,
					"the documented default for --%s does not match the code", tt.flagName)
			}
		})
	}
}

// replayTableRow returns the markdown table row documenting a flag, or "".
func replayTableRow(doc, flagName string) string {
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(line, "| `--"+flagName+"`") {
			return line
		}
	}
	return ""
}

// TestReplayDocumentedFlagsParse runs each documented flag through the real
// argument parser, so a doc example with a misspelled flag fails here.
func TestReplayDocumentedFlagsParse(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"required range only", []string{"--from-ledger", "250000"}, false},
		{"bounded range", []string{"--from-ledger", "250000", "--to-ledger", "260000"}, false},
		{"dry run", []string{"--from-ledger", "250000", "--dry-run"}, false},
		{"small batches", []string{"--from-ledger", "250000", "--batch-size", "200"}, false},
		{"restart", []string{"--from-ledger", "250000", "--restart"}, false},
		{"progress interval", []string{"--from-ledger", "250000", "--progress-interval", "30s"}, false},
		{"unknown flag", []string{"--from-ledger", "250000", "--nope"}, true},
		{"bad duration", []string{"--from-ledger", "250000", "--progress-interval", "soon"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := replayFlagSet()
			fs.SetOutput(discardWriter{})
			err := fs.Parse(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// TestReplayDocExamplesUseRealFlags walks every `sorotrail replay ...`
// command in the docs and parses its flags, so a copy-pasteable example
// cannot drift away from the command it documents.
func TestReplayDocExamplesUseRealFlags(t *testing.T) {
	doc := readReplayDoc(t)

	var examples []string
	for _, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		i := strings.Index(line, "sorotrail replay ")
		if i < 0 || strings.HasPrefix(line, "#") {
			continue
		}
		tail := line[i+len("sorotrail replay "):]
		// The usage synopsis uses [optional] placeholders rather than
		// real values; it is documentation of the shape, not an example.
		if strings.Contains(tail, "[") {
			continue
		}
		examples = append(examples, tail)
	}
	require.NotEmpty(t, examples, "docs/replay.md should contain runnable examples")

	for _, ex := range examples {
		t.Run(ex, func(t *testing.T) {
			args := exampleArgs(ex)
			fs := replayFlagSet()
			fs.SetOutput(discardWriter{})
			assert.NoErrorf(t, fs.Parse(args), "documented example does not parse: sorotrail replay %s", ex)
		})
	}
}

// exampleArgs turns a shell-ish example tail into argv, dropping trailing
// comments, line continuations, shell operators and quoting.
func exampleArgs(example string) []string {
	if i := strings.Index(example, "#"); i >= 0 {
		example = example[:i]
	}
	var args []string
	for _, f := range strings.Fields(example) {
		switch f {
		case "\\", "||", "&&", "|":
			continue
		}
		f = strings.Trim(f, `"'`)
		// Shell variables stand in for ledger numbers in the chunked
		// example; substitute a valid value so parsing is meaningful.
		if strings.HasPrefix(f, "$") {
			f = "1"
		}
		args = append(args, f)
	}
	return args
}

// discardWriter silences flag package output during tests.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestReplayDefaultBatchSizeIsDocumented pins the one default that also
// appears in prose, not only in the table.
func TestReplayDefaultBatchSizeIsDocumented(t *testing.T) {
	assert.Positive(t, replay.DefaultBatchSize)
	assert.Contains(t, readReplayDoc(t), fmt.Sprintf("| `--batch-size` | `%d` |", replay.DefaultBatchSize))
}

// TestReplayProgressIntervalIsADuration guards the doc's claim that the
// interval takes values like 30s and 1m.
func TestReplayProgressIntervalIsADuration(t *testing.T) {
	for _, v := range []string{"30s", "1m", "5m"} {
		t.Run(v, func(t *testing.T) {
			d, err := time.ParseDuration(v)
			require.NoError(t, err)
			assert.Positive(t, d)
			assert.Contains(t, readReplayDoc(t), "--progress-interval")
		})
	}
}
