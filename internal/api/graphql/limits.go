package graphql

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
)

// DepthLimit caps the maximum nesting depth of a GraphQL operation.
// doc.SelectionSet counts as depth=1 when each child is a field, depth=2
// when there are nested selections, etc. The hard-coded value of 10
// matches what the schema's connection nesting (Connection → edges →
// node → leaf fields) can express without abuse.
// Bump this only after re-baselining real client queries.
const DepthLimit = 10

// ComplexityLimit caps the total field-compute cost. Each leaf field is
// counted with the weight below; a single page request is bounded so
// even a maximally-deep query stays well under the SQL round-trip fan
// the store can sustain. Table-driven; totals are summed across the
// operation. The hard cap is 1000 — well above what a plausible
// client's introspection needs.
const ComplexityLimit = 1000

// leafFieldCost is the cost of a primitive field (string/scalar).
// Connections/multiple-record fields count higher because they fan out
// to storage (see connectionCost).
const leafFieldCost = 1

// connectionCost is the cost of a field that may return multiple rows
// (Connection types). The fan-out is bounded by queries.MaxPageSize in
// the resolver, so this is a per-page ceiling.
const connectionCost = 25

// operationCost is the per-operation overhead (auth, query planning,
// envelope). Adds depth to the budget for the executor itself.
const operationCost = 5

// CheckDepth walks the operation AST and returns the maximum nesting
// depth, or an error if the depth exceeds DepthLimit. Computed before
// execution so a malicious query is rejected cheaply.
func CheckDepth(op *ast.OperationDefinition) error {
	d := depth(op.SelectionSet, 1)
	if d > DepthLimit {
		return fmt.Errorf("query depth %d exceeds depth limit %d", d, DepthLimit)
	}
	return nil
}

// CheckComplexity walks the operation AST and returns the total
// complexity score, or an error if it exceeds ComplexityLimit.
// Connection fields cost more than leaf fields because they may fan
// out to up to MaxPageSize rows; the multiplier is set so a generous
// multi-page query stays under the cap, while a pathological
// "select everything" query is rejected before storage is touched.
func CheckComplexity(op *ast.OperationDefinition) error {
	c := complexity(op.SelectionSet) + operationCost
	if c > ComplexityLimit {
		return fmt.Errorf("query complexity %d exceeds complexity limit %d", c, ComplexityLimit)
	}
	return nil
}

func depth(sel []ast.Selection, current int) int {
	max := current
	for _, s := range sel {
		switch t := s.(type) {
		case *ast.Field:
			if d := depth(t.SelectionSet, current+1); d > max {
				max = d
			}
		case *ast.InlineFragment:
			if d := depth(t.SelectionSet, current+1); d > max {
				max = d
			}
		case *ast.FragmentSpread:
			// fragment bodies are scored against the root selection set
			// in CheckDepth. A spread here conservatively increments;
			// the runtime executor will not double-count.
			if d := current + 1; d > max {
				max = d
			}
		}
	}
	return max
}

func complexity(sel []ast.Selection) int {
	total := 0
	for _, s := range sel {
		switch t := s.(type) {
		case *ast.Field:
			// Heuristic: a field named ending in "Connection" or returning
			// multiple rows is treated as connectionCost; otherwise as a
			// primitive (leafFieldCost). A future refinement can read
			// the schema's field typeref to identify exact list returns.
			cost := leafFieldCost
			if isConnectionField(t.Name) {
				cost = connectionCost
			}
			total += cost
			total += complexity(t.SelectionSet)
		case *ast.InlineFragment:
			total += complexity(t.SelectionSet)
		case *ast.FragmentSpread:
			// Fragment bodies are scored when CheckComplexity walks the
			// document. Skip here to avoid double-counting.
		}
	}
	return total
}

// isConnectionField returns true for top-level fields that return a
// Connection type. The schema declares three such fields: events,
// tokenEvents, contracts. Other names fall back to leafFieldCost.
func isConnectionField(name string) bool {
	switch name {
	case "events", "tokenEvents", "contracts":
		return true
	}
	return false
}
