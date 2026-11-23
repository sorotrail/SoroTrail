package store

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

// stubNetError implements net.Error (Timeout only; Temporary was removed in
// Go 1.18) for testing the transient-network branch of IsPermanentEventError.
type stubNetError struct{}

func (stubNetError) Error() string { return "stub network error" }
func (stubNetError) Timeout() bool { return false }

func TestIsPermanentEventError(t *testing.T) {
	t.Parallel()

	pgData := &pgconn.PgError{Code: "22001"}
	pgIntegrity := &pgconn.PgError{Code: "23505"}
	pgOther := &pgconn.PgError{Code: "42601"}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"net error", stubNetError{}, false},
		{"ErrPermanentEventFailure direct", ErrPermanentEventFailure, true},
		{"ErrPermanentEventFailure wrapped", fmt.Errorf("ingest: %w", ErrPermanentEventFailure), true},
		{"pg class-22 data exception", pgData, true},
		{"pg class-23 integrity violation", pgIntegrity, true},
		{"pg class-22 wrapped", fmt.Errorf("store: %w", pgData), true},
		{"pg other class is not permanent", pgOther, false},
		{"arbitrary error is not permanent", errors.New("something went wrong"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsPermanentEventError(tc.err))
		})
	}
}
