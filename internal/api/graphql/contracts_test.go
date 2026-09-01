package graphql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sorotrail/sorotrail/internal/store"
)

// TestIndexAfter covers the contract-list cursor lookup used to
// resume pagination: it must locate the row matching the cursor's
// id, report a sentinel when the id isn't in the list, and never
// panic on an empty slice.
func TestIndexAfter(t *testing.T) {
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	rows := []store.WatchedContract{
		{ContractID: "CABC", AddedAt: now},
		{ContractID: "CDEF", AddedAt: now},
		{ContractID: "CGHI", AddedAt: now},
	}

	tests := []struct {
		name string
		rows []store.WatchedContract
		id   string
		want int
	}{
		{name: "known id returns the index of the matching row", rows: rows, id: "CDEF", want: 1},
		{name: "last element returns the final index", rows: rows, id: "CGHI", want: 2},
		{name: "unknown id returns the documented sentinel", rows: rows, id: "missing", want: -1},
		{name: "empty id with no matching row returns the sentinel", rows: rows, id: "", want: -1},
		{name: "empty slice is handled without panicking", rows: nil, id: "CABC", want: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, indexAfter(tt.rows, tt.id))
		})
	}
}
