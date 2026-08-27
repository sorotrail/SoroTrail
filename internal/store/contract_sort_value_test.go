package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// contractSortValue projects the field a ContractSummary is sorted by into
// the string half of a keyset cursor (see EncodeContractsCursor). The
// contract it must uphold: every documented SortBy* key maps to exactly one
// column's value, and any other key — including empty — falls back to the
// default ordering (contract_id), never to the raw key itself, because the
// output is embedded in a cursor and later compared against a SQL column.
func TestContractSortValue(t *testing.T) {
	fixture := ContractSummary{
		ContractID:  contractA,
		EventCount:  7,
		FirstLedger: 100,
		LastLedger:  200,
		LastSeen:    time.Date(2026, 8, 1, 12, 30, 45, 123456789, time.UTC),
	}

	tests := []struct {
		name    string
		sortKey string
		want    string
	}{
		{
			// The zero value is the documented default listing order: an
			// ascending walk by contract ID.
			name:    "empty key sorts by contract id",
			sortKey: "",
			want:    fixture.ContractID,
		},
		{
			name:    "contract_id key sorts by contract id",
			sortKey: SortByContractID,
			want:    fixture.ContractID,
		},
		{
			// FirstLedger is a bigint; the cursor carries it verbatim so a
			// later page can resume with a typed comparison.
			name:    "first_ledger key projects the first ledger",
			sortKey: SortByFirstLedger,
			want:    "100",
		},
		{
			name:    "last_ledger key projects the last ledger",
			sortKey: SortByLastLedger,
			want:    "200",
		},
		{
			// LastSeen is a timestamptz; RFC3339Nano keeps the round trip
			// lossless so no two events collapse onto the same cursor value.
			name:    "last_seen key projects an RFC3339 timestamp",
			sortKey: SortByLastSeen,
			want:    "2026-08-01T12:30:45.123456789Z",
		},
		{
			// The activity key is count(*) in the query; the cursor half is
			// the count itself, matching what the ORDER BY ranks by.
			name:    "count key projects the event count",
			sortKey: SortByActivity,
			want:    "7",
		},
		{
			// Unknown keys are rejected upstream by ValidContractsSortKey,
			// but the helper must still be total over its input and degrade
			// to the default ordering rather than interpolate the key.
			name:    "unknown key falls back to the default ordering",
			sortKey: "bogus",
			want:    "7",
		},
		{
			// Matching is exact, not case-insensitive: validation happens
			// upstream and is case-sensitive, so an uppercased key is
			// unknown here and must fall back rather than half-match.
			name:    "a differently-cased key does not half-match",
			sortKey: "CONTRACT_ID",
			want:    "7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contractSortValue(fixture, tt.sortKey)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The SQL-injection boundary: the cursor value is later compared against a
// column in a keyset predicate, so the sort key must never leak into the
// output. An arbitrary string has to resolve to the documented count
// fallback, and none of its bytes may appear in the returned value.
func TestContractSortValue_RejectsArbitrarySortKey(t *testing.T) {
	fixture := ContractSummary{ContractID: contractA, EventCount: 7}
	evil := "contract_id; DROP TABLE events"

	got := contractSortValue(fixture, evil)
	assert.Equal(t, "7", got,
		"an arbitrary sort key must fall back to the default (count) value")
	assert.NotContains(t, got, evil, "the raw sort key must never reach the output")
	assert.NotContains(t, got, "DROP TABLE")
}
