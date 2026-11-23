package store

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrPermanentEventFailure marks an error caused by an individual event's
// malformed or constraint-violating data (e.g. invalid JSON/UTF-8, check or
// not-null violation), as opposed to a transient database or transport failure.
// Wrapping a store error with ErrPermanentEventFailure tells the ingester to
// isolate and dead-letter the offending event rather than retrying the batch
// forever.
var ErrPermanentEventFailure = errors.New("permanent per-event storage failure")

// IsPermanentEventError reports whether err is a permanent per-event data
// error (either wrapped with ErrPermanentEventFailure or a PostgreSQL data /
// integrity SQLSTATE class 22 or 23) rather than a transient connection,
// timeout, or serialization failure.
func IsPermanentEventError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return false
	}
	if errors.Is(err, ErrPermanentEventFailure) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// SQLSTATE class 22 = Data Exception (invalid JSON, invalid byte
		// sequence, numeric value out of range); class 23 = Integrity
		// Constraint Violation (not_null_violation, check_violation).
		if strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23") {
			return true
		}
	}
	return false
}
