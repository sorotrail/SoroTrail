package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryEvents_TopicContains(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	events := []Event{
		testEvent(eventID(1), 101, contractA),
		testEvent(eventID(2), 102, contractA),
		testEvent(eventID(3), 103, contractA),
		testEvent(eventID(4), 104, contractA),
	}
	events[0].Topics = json.RawMessage(`[{"symbol":"transfer"},{"u64":7}]`)
	events[1].Topics = json.RawMessage(`[{"symbol":"transfer","amount":1}]`)
	events[2].Topics = json.RawMessage(`[{"symbol":"mint"}]`)
	events[3].Topics = json.RawMessage(`[{"symbol":"O'Reilly"}]`)

	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	tests := []struct {
		name          string
		topicContains string
		wantIDs       []string
	}{
		{
			name:          "matches object subset at any array position",
			topicContains: `[{"symbol":"transfer"}]`,
			wantIDs:       []string{eventID(1), eventID(2)},
		},
		{
			name:          "matches object in a later position",
			topicContains: `[{"u64":7}]`,
			wantIDs:       []string{eventID(1)},
		},
		{
			name:          "binds JSON containing a quote",
			topicContains: `[{"symbol":"O'Reilly"}]`,
			wantIDs:       []string{eventID(4)},
		},
		{
			name:          "returns no matches",
			topicContains: `[{"symbol":"missing"}]`,
			wantIDs:       []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := st.QueryEvents(ctx, EventFilter{
				TopicContains: json.RawMessage(tt.topicContains),
			})
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(got))
			for _, event := range got {
				gotIDs = append(gotIDs, event.ID)
			}
			assert.Equal(t, tt.wantIDs, gotIDs)
		})
	}
}
