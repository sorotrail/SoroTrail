package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreatedAtCursorOrdering(t *testing.T) {
	tests := []struct {
		name      string
		createdAt time.Time
		wantValue string
	}{
		{
			name:      "whole second",
			createdAt: time.Date(2026, 7, 1, 12, 30, 45, 0, time.UTC),
			wantValue: "2026-07-01T12:30:45Z",
		},
		{
			name:      "nanosecond precision",
			createdAt: time.Date(2026, 7, 1, 12, 30, 45, 123456789, time.UTC),
			wantValue: "2026-07-01T12:30:45.123456789Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := testEvent(eventID(1), 100, contractA)
			e.CreatedAt = tt.createdAt

			cursor := EncodeCursor(OrderByCreatedAt, e)
			value, id, err := decodeCompositeCursor(cursor)
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, value)
			assert.Equal(t, e.ID, id)
			assert.NotEqual(t, e.ID, cursor, "created_at ordering must use a composite cursor")
		})
	}
}
