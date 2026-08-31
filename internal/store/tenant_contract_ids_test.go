package store

// contractIDs is the projection helper behind ListGrants and
// ListTenantWatchedContracts: it copies the id column out of a row set,
// preserving row order and duplicates. Ordering and de-duplication are
// deliberately the query's job — the callers pass ORDER BY and rely on
// their tables' constraints — so the contract worth pinning down here is
// faithful projection: rows in, ids out, in the same order, with an empty
// row set yielding an empty (non-nil) slice rather than nil, which a
// caller like NewScope would read as "not queried".

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeContractRows is a canned pgx.Rows for the projection tests: a fixed
// list of ids that Next walks one at a time, and Scan writes the current
// id into its first destination — the only thing contractIDs asks for.
// The rest of the pgx.Rows surface (Values, RawValues, CommandTag, ...)
// is never touched by the projection, so those methods are stubbed inert
// to keep the fake a plain slice of strings in disguise.
type fakeContractRows struct {
	ids []string
	pos int

	// scanErr, when set, makes every Scan fail — standing in for a row
	// that cannot be read mid-projection.
	scanErr error
}

func (f *fakeContractRows) Next() bool {
	if f.pos >= len(f.ids) {
		return false
	}
	f.pos++
	return true
}

func (f *fakeContractRows) Scan(dest ...any) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	*(dest[0].(*string)) = f.ids[f.pos-1]
	return nil
}

func (f *fakeContractRows) Err() error                                   { return nil }
func (f *fakeContractRows) Close()                                       {}
func (f *fakeContractRows) Values() ([]any, error)                       { return nil, nil }
func (f *fakeContractRows) RawValues() [][]byte                          { return nil }
func (f *fakeContractRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (f *fakeContractRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (f *fakeContractRows) Conn() *pgx.Conn                              { return nil }

func TestContractIDs(t *testing.T) {
	tests := []struct {
		name    string
		ids     []string
		scanErr error
		want    []string
		wantErr bool
	}{
		{
			// Deliberately unsorted: the caller's ORDER BY owns ordering,
			// so the helper must pass rows through untouched rather than
			// impose its own sort.
			name: "populated rows yield the ids in row order",
			ids:  []string{"CGRANTED", "CAFIRST", "CMIDDLE"},
			want: []string{"CGRANTED", "CAFIRST", "CMIDDLE"},
		},
		{
			// A tenant with no grants yields no rows; the projection must
			// come back as an empty slice, not nil, or NewScope would not
			// be able to tell "granted nothing" from "never queried".
			name: "an empty row set yields an empty slice, not nil",
			ids:  nil,
			want: []string{},
		},
		{
			// De-duplication is SQL's job (UNIQUE constraints, DISTINCT),
			// so duplicates that reach the projection must survive it —
			// silently collapsing them here would hide a query bug.
			name: "duplicate ids are preserved",
			ids:  []string{"CAFIRST", "CAFIRST", "CMIDDLE", "CAFIRST"},
			want: []string{"CAFIRST", "CAFIRST", "CMIDDLE", "CAFIRST"},
		},
		{
			name: "a single row projects to a one-element slice",
			ids:  []string{"CONLY"},
			want: []string{"CONLY"},
		},
		{
			// A row that cannot be scanned must abort the projection, not
			// be silently dropped — losing an id here would silently
			// narrow a tenant's read scope.
			name:    "a scan error aborts the projection",
			ids:     []string{"CAFIRST"},
			scanErr: errors.New("corrupt row"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := contractIDs(&fakeContractRows{ids: tt.ids, scanErr: tt.scanErr})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
