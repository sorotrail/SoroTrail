package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validContract is a syntactically valid strkey (56 chars, 'C' + charset
// letters) accepted by config.ValidContractID, used across these tests.
var validContract = "C" + strings.Repeat("A", 55)

func TestParseBackfillFlags_FromToAliases(t *testing.T) {
	f, err := parseBackfillFlags([]string{
		"--contract", validContract,
		"--from", "100",
		"--to", "200",
	})
	require.NoError(t, err)
	assert.EqualValues(t, 100, f.fromLedger)
	assert.EqualValues(t, 200, f.toLedger)
}

func TestParseBackfillFlags_LongFormFlagsStillWork(t *testing.T) {
	f, err := parseBackfillFlags([]string{
		"--contract", validContract,
		"--from-ledger", "100",
		"--to-ledger", "200",
	})
	require.NoError(t, err)
	assert.EqualValues(t, 100, f.fromLedger)
	assert.EqualValues(t, 200, f.toLedger)
}

func TestParseBackfillFlags_RequiresContract(t *testing.T) {
	_, err := parseBackfillFlags([]string{"--from", "100"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--contract")
}

func TestParseBackfillFlags_RequiresPositiveFrom(t *testing.T) {
	_, err := parseBackfillFlags([]string{"--contract", validContract, "--from", "0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--from is required")
}

func TestParseBackfillFlags_RejectsToBeforeFrom(t *testing.T) {
	_, err := parseBackfillFlags([]string{
		"--contract", validContract, "--from", "200", "--to", "100",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "before")
}

func TestParseBackfillFlags_RejectsBatchSizeOutOfRange(t *testing.T) {
	_, err := parseBackfillFlags([]string{
		"--contract", validContract, "--from", "1", "--batch-size", "500",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--batch-size")
}

func TestParseBackfillFlags_DefaultsAndOptionalFlags(t *testing.T) {
	f, err := parseBackfillFlags([]string{
		"--contract", validContract,
		"--from", "1",
		"--dry-run",
		"--restart",
		"--rps", "5",
		"--horizon-url", "https://horizon.example.com",
		"--include-failed=false",
	})
	require.NoError(t, err)
	assert.True(t, f.dryRun)
	assert.True(t, f.restart)
	assert.False(t, f.includeFail)
	assert.Equal(t, 5.0, f.rps)
	assert.Equal(t, "https://horizon.example.com", f.horizonURL)
	assert.EqualValues(t, 0, f.toLedger, "unset --to means no upper bound")
}
