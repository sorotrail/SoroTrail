package store_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// TestEvent_WithSEP41 exercises the seam between store.Event and the
// SEP-41 normalizer: a SEP-41-shape event populates the slot, a
// non-matching event leaves it nil, the JSON encoding omits the slot
// via omitempty, and a nil receiver is a no-op.
func TestEvent_WithSEP41(t *testing.T) {
	t.Run("transfer populates sep41_event slot", func(t *testing.T) {
		e := store.Event{
			ID:     "0000000000000001-0000000001",
			Type:   "contract",
			Topics: json.RawMessage(`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`),
			Value:  json.RawMessage(`{"i128":"100"}`),
		}
		e.WithSEP41()
		require.NotNil(t, e.SEP41Event, "transfer must populate sep41_event")
		assert.JSONEq(t,
			`{"standard":"sep41","event":"transfer","from":"GA1","to":"GA2","amount":"100"}`,
			string(*e.SEP41Event))
	})

	t.Run("approve populates with spender + expiration_ledger", func(t *testing.T) {
		e := store.Event{
			ID:     "0000000000000002-0000000001",
			Type:   "contract",
			Topics: json.RawMessage(`[{"symbol":"approve"},{"address":"GA1"},{"address":"GA2"}]`),
			Value:  json.RawMessage(`{"vec":[{"i128":"500"},{"u32":1234567}]}`),
		}
		e.WithSEP41()
		require.NotNil(t, e.SEP41Event)
		assert.JSONEq(t,
			`{"standard":"sep41","event":"approve","from":"GA1","spender":"GA2","amount":"500","expiration_ledger":1234567}`,
			string(*e.SEP41Event))
	})

	t.Run("non-SEP-41 event leaves slot nil", func(t *testing.T) {
		e := store.Event{
			ID:     "0000000000000003-0000000001",
			Type:   "contract",
			Topics: json.RawMessage(`[{"symbol":"swap"},{"address":"GA1"}]`),
			Value:  json.RawMessage(`{"i128":"1"}`),
		}
		e.WithSEP41()
		assert.Nil(t, e.SEP41Event, "non-SEP-41 events must keep sep41_event nil")
	})

	t.Run("nil receiver is a no-op", func(t *testing.T) {
		var e *store.Event
		assert.NotPanics(t, func() { e.WithSEP41() })
	})

	t.Run("JSON encoding omits sep41_event on non-match", func(t *testing.T) {
		e := store.Event{
			ID:     "0000000000000004-0000000001",
			Type:   "contract",
			Topics: json.RawMessage(`[{"symbol":"swap"}]`),
		}
		e.WithSEP41()
		body, err := json.Marshal(e)
		require.NoError(t, err)
		assert.False(t, strings.Contains(string(body), "sep41_event"),
			"non-match must omit sep41_event via omitempty, got %s", string(body))
	})

	t.Run("JSON encoding includes populated sep41_event", func(t *testing.T) {
		e := store.Event{
			ID:     "0000000000000005-0000000001",
			Type:   "contract",
			Topics: json.RawMessage(`[{"symbol":"mint"},{"address":"GA1"}]`),
			Value:  json.RawMessage(`{"i128":"42"}`),
		}
		e.WithSEP41()
		body, err := json.Marshal(e)
		require.NoError(t, err)
		assert.True(t, strings.Contains(string(body), "\"sep41_event\":"),
			"populated sep41_event must appear in JSON, got %s", string(body))
	})
}

func TestValidOrderBy(t *testing.T) {
	tests := []struct {
		name    string
		orderBy string
		want    bool
	}{
		{name: "empty string defaults to id", orderBy: "", want: true},
		{name: "order by id", orderBy: store.OrderByID, want: true},
		{name: "order by ledger", orderBy: store.OrderByLedger, want: true},
		{name: "order by created_at", orderBy: store.OrderByCreatedAt, want: true},
		{name: "unknown column error path", orderBy: "invalid_column", want: false},
		{name: "sql injection error path", orderBy: "id; DROP TABLE events;", want: false},
		{name: "uppercase mismatch error path", orderBy: "ID", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := store.ValidOrderBy(tc.orderBy)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsAll(t *testing.T) {
	tests := []struct {
		name   string
		filter store.EventFilter
		want   bool
	}{
		{
			name:   "empty filter is all",
			filter: store.EventFilter{},
			want:   true,
		},
		{
			name:   "filter with contract id is not all",
			filter: store.EventFilter{ContractID: "C1234"},
			want:   false,
		},
		{
			name:   "filter with limit is not all",
			filter: store.EventFilter{Limit: 10},
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.filter.IsAll()
			assert.Equal(t, tc.want, got)
		})
	}
}
