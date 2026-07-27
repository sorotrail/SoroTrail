package store

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

// Pagination cursors.
//
// Keyset pagination needs the sort key to be unique, or a page boundary that
// lands in the middle of a run of equal values will skip or repeat rows.
// Event IDs are unique, so ordering by id can use the id alone as the
// cursor — which is what SoroTrail has always returned, and those cursors
// keep working unchanged.
//
// ledger and created_at are not unique: one ledger holds many events, and
// created_at is a batch-insert timestamp shared by every event written
// together. Ordering by either appends id as a tiebreaker, making the pair
// (sort value, id) total, and the cursor has to carry both halves. Those
// cursors are base64url of "<sort value>|<id>" — opaque by contract, so the
// encoding can change without breaking the API.

// EncodeCursor builds the cursor that resumes after e under the given
// ordering. orderBy uses the OrderBy* constants; "" means OrderByID.
func EncodeCursor(orderBy string, e Event) string {
	switch orderBy {
	case OrderByLedger:
		return encodeCompositeCursor(fmt.Sprint(e.Ledger), e.ID)
	case OrderByCreatedAt:
		// RFC3339 with nanoseconds so the round trip is lossless.
		return encodeCompositeCursor(e.CreatedAt.UTC().Format(time.RFC3339Nano), e.ID)
	default:
		return e.ID
	}
}

func encodeCompositeCursor(sortValue, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(sortValue + "|" + id))
}

// decodeCompositeCursor splits a composite cursor back into its sort value
// and id. It reports an error the caller should surface as a bad request:
// a malformed cursor is client input, not a server fault.
func decodeCompositeCursor(cursor string) (sortValue, id string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", fmt.Errorf("%w: not valid base64url", ErrInvalidCursor)
	}
	// The id is a fixed-shape TOID and never contains "|", while a
	// timestamp does not either — but split on the last separator anyway so
	// a future sort value containing "|" stays decodable.
	sep := strings.LastIndex(string(raw), "|")
	if sep < 0 {
		return "", "", fmt.Errorf("%w: missing separator", ErrInvalidCursor)
	}
	sortValue, id = string(raw[:sep]), string(raw[sep+1:])
	if sortValue == "" || id == "" {
		return "", "", fmt.Errorf("%w: empty component", ErrInvalidCursor)
	}
	return sortValue, id, nil
}
