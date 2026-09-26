package rpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRPCMatrix_Table(t *testing.T) {
	tests := []struct {
		name          string
		errors        []error
		maxErrors     int
		expectedState ProviderState
	}{
		{
			name:          "success on first attempt keeps healthy",
			errors:        []error{nil},
			maxErrors:     3,
			expectedState: StateActive,
		},
		{
			name: "HTTP 500 demotes after max errors",
			errors: []error{
				fmt.Errorf("getEvents returned HTTP 500"),
				fmt.Errorf("getEvents returned HTTP 502"),
				fmt.Errorf("getEvents returned HTTP 503"),
			},
			maxErrors:     3,
			expectedState: StateDegraded,
		},
		{
			name: "semantic errors do not demote",
			errors: []error{
				fmt.Errorf("requested ledger is outside retention window"),
				fmt.Errorf("requested ledger is outside retention window"),
				fmt.Errorf("requested ledger is outside retention window"),
			},
			maxErrors:     3,
			expectedState: StateActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc, mocks := newFailoverTestClient(
				[]string{"rpc0"},
				WithFailoverLogger(testLogger()),
				WithFailoverMaxErrors(tt.maxErrors),
			)

			mocks[0].getEventsErr = tt.errors

			// pad response to avoid index out of bounds
			mocks[0].getEventsResp = make([]GetEventsResponse, len(tt.errors))

			for i := 0; i < len(tt.errors); i++ {
				_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
				if tt.errors[i] != nil {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			assert.Equal(t, tt.expectedState, ProviderState(fc.providers[0].state.Load()))
		})
	}
}
