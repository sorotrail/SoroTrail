package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildEventWhereClauseAllFilters pins the complete fragment and argument
// order. The shared builder feeds both event queries and count queries, so a
// change in one filter must not silently renumber the placeholders used by the
// filters that follow it.
func TestBuildEventWhereClauseAllFilters(t *testing.T) {
	fromTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	toTime := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	successful := true
	hasValue := true
	txIndex := int32(3)
	opIndex := int32(4)
	filter := EventFilter{
		Network:          "testnet",
		ContractID:       "contract-b",
		Types:            []string{"contract", "system"},
		TxHash:           "deadbeef",
		InSuccessfulCall: &successful,
		Topic:            json.RawMessage(`{"symbol":"transfer"}`),
		TopicContains:    json.RawMessage(`[{"u64":7}]`),
		Topic0:           json.RawMessage(`{"a":0}`),
		Topic1:           json.RawMessage(`{"a":1}`),
		Topic2:           json.RawMessage(`{"a":2}`),
		Topic3:           json.RawMessage(`{"a":3}`),
		HasValue:         &hasValue,
		TxIndex:          &txIndex,
		OpIndex:          &opIndex,
		FromLedger:       100,
		ToLedger:         200,
		FromTime:         fromTime,
		ToTime:           toTime,
		Scope:            WildcardScope(),
	}

	fragments, args := buildEventWhereClause(filter)

	assert.Equal(t, []string{
		"network = $1",
		"contract_id = $2",
		"type = ANY($3)",
		"tx_hash = $4",
		"in_successful_call = $5",
		"topics @> $6::jsonb",
		"topics @> $7::jsonb",
		"topics->0 = $8::jsonb",
		"topics->1 = $9::jsonb",
		"topics->2 = $10::jsonb",
		"topics->3 = $11::jsonb",
		"value IS NOT NULL",
		"tx_index = $12",
		"op_index = $13",
		"ledger >= $14",
		"ledger <= $15",
		"created_at >= $16",
		"created_at <= $17",
	}, fragments)
	assert.Equal(t, []any{
		"testnet",
		"contract-b",
		[]string{"contract", "system"},
		"deadbeef",
		true,
		`[{"symbol":"transfer"}]`,
		`[{"u64":7}]`,
		json.RawMessage(`{"a":0}`),
		json.RawMessage(`{"a":1}`),
		json.RawMessage(`{"a":2}`),
		json.RawMessage(`{"a":3}`),
		int32(3),
		int32(4),
		int64(100),
		int64(200),
		fromTime,
		toTime,
	}, args)

	sql := strings.Join(fragments, " AND ")
	assert.Equal(t, 17, strings.Count(sql, "$"), "all and only argument placeholders must remain in order")
	assert.NotContains(t, sql, "contract-b", "caller values must be bound, not interpolated")
	assert.NotContains(t, sql, "deadbeef", "caller values must be bound, not interpolated")
	assert.NotContains(t, sql, " OR ", "combined filters must be conjunctive")
}

// TestContractsWhereFragments covers the empty predicate and the exact
// conjunction form used when contract-list queries add scope and ownership
// restrictions.
func TestContractsWhereFragments(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{name: "nil", want: ""},
		{name: "empty", parts: []string{}, want: ""},
		{name: "one", parts: []string{"a = 1"}, want: " WHERE a = 1"},
		{name: "many", parts: []string{"a = 1", "b = 2", "c = 3"}, want: " WHERE a = 1 AND b = 2 AND c = 3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, contractsWhere(tt.parts))
		})
	}
}

func TestEncodeCursorOrderings(t *testing.T) {
	createdAt := time.Date(2026, 7, 1, 12, 30, 45, 123456789, time.FixedZone("offset", 2*60*60))
	event := Event{ID: "0000000000000007-0000000001", Ledger: 4242, CreatedAt: createdAt}

	tests := []struct {
		name     string
		orderBy  string
		wantSort string
	}{
		{name: "id", orderBy: OrderByID},
		{name: "default", orderBy: ""},
		{name: "ledger", orderBy: OrderByLedger, wantSort: "4242"},
		{name: "created at", orderBy: OrderByCreatedAt, wantSort: "2026-07-01T10:30:45.123456789Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := EncodeCursor(tt.orderBy, event)
			if tt.wantSort == "" {
				assert.Equal(t, event.ID, cursor)
				return
			}

			sortValue, id, err := decodeCompositeCursor(cursor)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSort, sortValue)
			assert.Equal(t, event.ID, id)
		})
	}
}

func TestCompositeCursorRejectsMalformedComponents(t *testing.T) {
	tests := []struct {
		name   string
		cursor string
	}{
		{name: "not base64", cursor: "not base64!"},
		{name: "empty", cursor: ""},
		{name: "missing separator", cursor: "cGxhaW4"},
		{name: "empty sort value", cursor: "fA"},
		{name: "empty id", cursor: "MQ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := decodeCompositeCursor(tt.cursor)
			assert.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}

func TestContractsCursorIsOpaque(t *testing.T) {
	contractID := "contract-with-a-distinctive-name"
	cursor := EncodeContractsCursor("last_ledger", "987654321", contractID)
	decodedSort, decodedID, err := DecodeContractsCursor(cursor)
	require.NoError(t, err)
	assert.Equal(t, "987654321", decodedSort)
	assert.Equal(t, contractID, decodedID)
	assert.NotContains(t, cursor, contractID, "the contract ID must not be exposed in cleartext")
	assert.NotContains(t, cursor, "987654321", "the sort value must not be exposed in cleartext")
}

func TestScopeConstructors(t *testing.T) {
	tests := []struct {
		name       string
		scope      Scope
		wildcard   bool
		deniesAll  bool
		contracts  []string
	}{
		{name: "zero", scope: Scope{}, deniesAll: true},
		{name: "wildcard", scope: WildcardScope(), wildcard: true},
		{name: "system", scope: SystemScope(), wildcard: true},
		{name: "contracts", scope: NewScope([]string{"contract-b", "contract-a", "contract-b"}), contracts: []string{"contract-a", "contract-b"}},
		{name: "empty strings", scope: NewScope([]string{"", ""}), deniesAll: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wildcard, tt.scope.IsWildcard())
			assert.Equal(t, tt.deniesAll, tt.scope.DeniesAll())
			assert.Equal(t, tt.contracts, tt.scope.Contracts())
		})
	}
}

func TestScopeFingerprintTracksGrantSet(t *testing.T) {
	first := NewScope([]string{"contract-a", "contract-b"})
	reordered := NewScope([]string{"contract-b", "contract-a"})
	different := NewScope([]string{"contract-a", "contract-c"})

	assert.Equal(t, first.Fingerprint(), reordered.Fingerprint())
	assert.NotEqual(t, first.Fingerprint(), different.Fingerprint())
	assert.Equal(t, "none", (Scope{}).Fingerprint())
	assert.Equal(t, "wildcard", WildcardScope().Fingerprint())
	assert.Equal(t, SystemScope().Fingerprint(), WildcardScope().Fingerprint())
}

func TestScopeGrantHelpers(t *testing.T) {
	scope := NewScope([]string{"contract-a", "contract-b"})
	assert.True(t, scope.Allows("contract-a"))
	assert.True(t, scope.Allows("contract-b"))
	assert.False(t, scope.Allows("contract-c"))

	contracts := scope.Contracts()
	contracts[0] = "contract-c"
	assert.True(t, scope.Allows("contract-a"), "the returned grant slice must not widen the scope")
	assert.False(t, scope.Allows("contract-c"))
}
