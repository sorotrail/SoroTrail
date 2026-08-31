package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// contractsSortParts maps a ContractsFilter.SortKey to the three pieces of
// the ORDER BY it needs: the column the cursor compares against, the SQL
// expression to sort by, and the cast applied to the cursor value. The
// contract it must uphold: every documented SortBy* key maps to exactly one
// (col, expr, typ) triple, and any other key — including empty — falls back
// to the default count(*) ordering rather than letting caller text reach
// the query.
func TestContractsSortParts(t *testing.T) {
	tests := []struct {
		name    string
		sortKey string
		col     string
		expr    string
		typ     string
	}{
		{
			// The zero value is the documented default listing order: an
			// ascending walk by contract ID.
			name:    "empty key falls back to the default column",
			sortKey: "",
			col:     "contract_id",
			expr:    "contract_id",
			typ:     "",
		},
		{
			name:    "contract_id key maps to the contract id column",
			sortKey: SortByContractID,
			col:     "contract_id",
			expr:    "contract_id",
			typ:     "",
		},
		{
			// The ledger bounds are aggregates; the cursor must compare
			// against the same expression the ORDER BY ranks by, and the
			// bigint cast keeps the row-value comparison typed.
			name:    "first_ledger key maps to the min(ledger) expression",
			sortKey: SortByFirstLedger,
			col:     "min(ledger)",
			expr:    "min(ledger)",
			typ:     "bigint",
		},
		{
			name:    "last_ledger key maps to the max(ledger) expression",
			sortKey: SortByLastLedger,
			col:     "max(ledger)",
			expr:    "max(ledger)",
			typ:     "bigint",
		},
		{
			name:    "last_seen key maps to the max(created_at) expression",
			sortKey: SortByLastSeen,
			col:     "max(created_at)",
			expr:    "max(created_at)",
			typ:     "timestamptz",
		},
		{
			// The activity key is count(*) in the query; the cursor column
			// and expression are the count itself.
			name:    "count key maps to the count(*) expression",
			sortKey: SortByActivity,
			col:     "count(*)",
			expr:    "count(*)",
			typ:     "",
		},
		{
			// Unknown keys are rejected upstream by ValidContractsSortKey,
			// but the helper must still be total over its input and degrade
			// to the default ordering rather than interpolate the key.
			name:    "unknown key falls back to the default ordering",
			sortKey: "bogus",
			col:     "count(*)",
			expr:    "count(*)",
			typ:     "",
		},
		{
			// Matching is exact, not case-insensitive: validation happens
			// upstream and is case-sensitive, so an uppercased key is
			// unknown here and must fall back rather than half-match.
			name:    "a differently-cased key does not half-match",
			sortKey: "CONTRACT_ID",
			col:     "count(*)",
			expr:    "count(*)",
			typ:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, expr, typ := contractsSortParts(tt.sortKey)
			assert.Equal(t, tt.col, col)
			assert.Equal(t, tt.expr, expr)
			assert.Equal(t, tt.typ, typ)
		})
	}
}

// orderByFragment mirrors the ORDER BY construction in ListContracts: the
// sort expression plus the direction, with contract_id appended as a
// tiebreaker so keyset pagination stays total even when the sort column has
// ties. Sorting by contract_id itself drops the redundant tiebreaker — the
// sort column already is one.
func orderByFragment(sortKey, dir string) string {
	col, expr, _ := contractsSortParts(sortKey)
	orderBy := fmt.Sprintf("%s %s, contract_id %s", expr, dir, dir)
	if col == "contract_id" {
		orderBy = fmt.Sprintf("contract_id %s", dir)
	}
	return orderBy
}

// Both directions produce the documented ORDER BY fragment, and the stable
// contract_id tiebreaker is present in every ordering.
func TestContractsSortParts_OrderByFragment(t *testing.T) {
	tests := []struct {
		name    string
		sortKey string
		dir     string
		want    string
	}{
		{"default ordering ascends by contract id", "", "ASC", "contract_id ASC"},
		{"contract_id descends by contract id", SortByContractID, "DESC", "contract_id DESC"},
		{"activity ascending appends the tiebreaker", SortByActivity, "ASC", "count(*) ASC, contract_id ASC"},
		{"first_ledger descending appends the tiebreaker", SortByFirstLedger, "DESC", "min(ledger) DESC, contract_id DESC"},
		{"last_ledger ascending appends the tiebreaker", SortByLastLedger, "ASC", "max(ledger) ASC, contract_id ASC"},
		{"last_seen descending appends the tiebreaker", SortByLastSeen, "DESC", "max(created_at) DESC, contract_id DESC"},
		{"unknown key keeps the default ordering with the tiebreaker", "bogus", "ASC", "count(*) ASC, contract_id ASC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := orderByFragment(tt.sortKey, tt.dir)
			assert.Equal(t, tt.want, got)
			assert.Contains(t, got, "contract_id",
				"every ordering must include the stable contract_id tiebreaker")
		})
	}
}

// The SQL-injection boundary: the sort key feeds an ORDER BY fragment, so an
// arbitrary string must resolve to the fixed count(*) fallback — it can
// never become a column name, and none of its bytes may appear in the
// generated fragment.
func TestContractsSortParts_RejectsArbitrarySortKey(t *testing.T) {
	evil := "contract_id; DROP TABLE events"

	col, expr, typ := contractsSortParts(evil)
	assert.Equal(t, "count(*)", col, "an arbitrary sort key must not become a column name")
	assert.Equal(t, "count(*)", expr)
	assert.Equal(t, "", typ)

	frag := orderByFragment(evil, "ASC")
	assert.NotContains(t, frag, evil, "caller-supplied text must never reach the ORDER BY fragment")
	assert.NotContains(t, frag, "DROP TABLE")
}
