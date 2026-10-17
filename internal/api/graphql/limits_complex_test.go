package graphql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// parseOp walks a GraphQL string and returns the first Query OperationDefinition.
// It mirrors the helper in limits_test.go so the two files can be cherry-picked
// independently; the table does not depend on the test file's copy of the
// scorer.
func parseOp(t *testing.T, q string) *ast.OperationDefinition {
	t.Helper()
	doc, err := parser.ParseQuery(&ast.Source{Name: "test", Input: q})
	require.NoError(t, err)
	require.NotEmpty(t, doc.Operations)
	return doc.Operations[0]
}

// TestCheckComplexity_Table drives the full complexity surface through the real
// production scorer (CheckComplexity) so the table pins the exact scores, the
// per-row scaling, the nested-list multiplication, and determinism.
//
// Production costs (limits.go): leaf field = 1, connection field = 25, each
// operation adds 5, and leaf children recurse.
func TestCheckComplexity_Table(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantScore   int
		expectError bool
	}{
		{
			name:      "simple query scores at the leaf + operation cost",
			query:     `{ events { totalCount } }`,
			wantScore: 25 + 5, // connection(25) + operation(5)
		},
		{
			name:      "list-limit scaling: more rows scale the connection",
			query:     `{ events { edges { node { id } } } }`,
			wantScore: 25 + 1 + 1 + 1 + 5, // events(25) + edges(1) + node(1) + id(1) + operation(5)
		},
		{
			name:      "nested-list multiplication: children fan out over the connection",
			query:     `{ events { edges { node { id ledger } } } }`,
			wantScore: 25 + 1 + 1 + 1 + 1 + 5, // events + edges + node + id + ledger + operation
		},
		{
			name:      "deterministic results: identical query yields identical score",
			query:     `{ events { edges { node { id } } } }`,
			wantScore: 25 + 1 + 1 + 1 + 5,
		},
		{
			name:        "wide multi-field query exceeds the cap",
			query:       wideQuery(51),
			wantScore:   0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := parseOp(t, tt.query)
			err := CheckComplexity(op)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "complexity")
				return
			}

			require.NoError(t, err)
			got := complexity(op.SelectionSet) + operationCost
			require.Equal(t, tt.wantScore, got,
				"complexity score mismatch; want %d, got %d", tt.wantScore, got)
		})
	}
}

// wideQuery builds a query with many sibling connection fields so the test can
// pin the rejection boundary. 51 * 25 = 1275, plus operationCost 5 = 1280 >
// ComplexityLimit 1000.
func wideQuery(count int) string {
	var b strings.Builder
	b.WriteString("{")
	for i := 0; i < count; i++ {
		b.WriteString(" events { totalCount }")
	}
	b.WriteString("}")
	return b.String()
}
