package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestEventMatches(t *testing.T) {
	event := store.Event{
		ID:         "evt-1",
		ContractID: "CAAAAA...",
		Ledger:     100,
		Type:       "payment",
		Topics:     json.RawMessage(`["payment"]`),
	}

	tests := []struct {
		name   string
		filter store.EventFilter
		event  store.Event
		want   bool
	}{
		{
			name:   "empty filter matches every event",
			filter: store.EventFilter{},
			event:  event,
			want:   true,
		},
		{
			name: "contract-id filter matches",
			filter: store.EventFilter{
				ContractID: "CAAAAA...",
			},
			event: event,
			want:  true,
		},
		{
			name: "contract-id filter excludes other contracts",
			filter: store.EventFilter{
				ContractID: "CBBBBB...",
			},
			event: event,
			want:  false,
		},
		{
			name: "from-ledger excludes events before range",
			filter: store.EventFilter{
				FromLedger: 200,
			},
			event: event,
			want:  false,
		},
		{
			name: "to-ledger excludes events after range",
			filter: store.EventFilter{
				ToLedger: 50,
			},
			event: event,
			want:  false,
		},
		{
			name: "ledger range includes event",
			filter: store.EventFilter{
				FromLedger: 50,
				ToLedger:   200,
			},
			event: event,
			want:  true,
		},
		{
			name: "type filter matches",
			filter: store.EventFilter{
				Types: []string{"payment"},
			},
			event: event,
			want:  true,
		},
		{
			name: "type filter excludes",
			filter: store.EventFilter{
				Types: []string{"mint"},
			},
			event: event,
			want:  false,
		},
		{
			name: "topic filter matches",
			filter: store.EventFilter{
				Topic: json.RawMessage(`"payment"`),
			},
			event: event,
			want:  true,
		},
		{
			name: "topic filter excludes",
			filter: store.EventFilter{
				Topic: json.RawMessage(`"mint"`),
			},
			event: event,
			want:  false,
		},
		{
			name: "combined filters must all match",
			filter: store.EventFilter{
				ContractID: "CAAAAA...",
				Types:      []string{"payment"},
				Topic:      json.RawMessage(`"payment"`),
			},
			event: event,
			want:  true,
		},
		{
			name: "combined filters fail if any does not match",
			filter: store.EventFilter{
				ContractID: "CAAAAA...",
				Types:      []string{"mint"},
				Topic:      json.RawMessage(`"payment"`),
			},
			event: event,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := eventMatches(tt.filter, tt.event)
			assert.Equal(t, tt.want, got)
		})
	}
}
