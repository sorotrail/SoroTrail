package store

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultMaxSparseFilterLedgerSpan is the recommended maximum ledger span
// (~4 partitions of DefaultEventPartitionSpan = 120,960 ledgers) when
// bounding sparse JSONB topic filter queries over wide ledger ranges.
const DefaultMaxSparseFilterLedgerSpan int64 = 500_000

// ErrSparseFilterRangeTooWide is returned when a query with a sparse JSONB
// topic filter (topic, topic_contains, topic0..topic3) spans an unbounded or
// excessively wide ledger window. Callers should narrow from_ledger/to_ledger
// or switch to the streaming export endpoints (/events.csv or /contracts/{id}/export).
var ErrSparseFilterRangeTooWide = errors.New("sparse topic filter ledger range is too wide")

var globalMaxSparseFilterLedgerSpan atomic.Int64

// SetMaxSparseFilterLedgerSpan configures the package-wide default maximum
// ledger span allowed for sparse JSONB topic filter queries when a store
// instance does not set its own override. A value <= 0 disables the guard.
func SetMaxSparseFilterLedgerSpan(maxSpan int64) {
	globalMaxSparseFilterLedgerSpan.Store(maxSpan)
}

// MaxSparseFilterLedgerSpan returns the package-wide maximum ledger span for
// sparse JSONB topic filter queries.
func MaxSparseFilterLedgerSpan() int64 {
	return globalMaxSparseFilterLedgerSpan.Load()
}

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

// EncodeContractsCursor keys keyset pagination for ListContracts over
// the (sortValue, contractID) pair. sortAs is the literal column being
// sorted — the API keeps the same cursor format for any sort so a
// client can mix sortings between pages, but typically a caller picks
// one and sticks with it.
func EncodeContractsCursor(sortAs, sortValue, contractID string) string {
	return encodeCompositeCursor(sortValue, contractID)
}

// DecodeContractsCursor is the inverse of EncodeContractsCursor. It
// surfaces a typed error the handler maps to 400.
func DecodeContractsCursor(cursor string) (sortValue, contractID string, err error) {
	sortValue, contractID, err = decodeCompositeCursor(cursor)
	if err != nil {
		err = fmt.Errorf("%w: contracts cursor", ErrInvalidContractsCursor)
	}
	return
}

// EncodeSpecOverrideHistoryCursor keys keyset pagination for the spec-override
// history over its monotonic version id. The history is walked newest first,
// so the cursor carries the last id seen and the next page is the rows with
// a smaller id.
func EncodeSpecOverrideHistoryCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}

// DecodeSpecOverrideHistoryCursor is the inverse of
// EncodeSpecOverrideHistoryCursor. A malformed cursor is client input, so it
// surfaces as ErrInvalidCursor for the handler to map to 400.
func DecodeSpecOverrideHistoryCursor(cursor string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: spec override history cursor is not valid base64url", ErrInvalidCursor)
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: spec override history cursor is not a version id", ErrInvalidCursor)
	}
	return id, nil
}

// FormatTOIDCursor encodes a canonical Stellar SEP-0035 64-bit Total Order ID
// (TOID) event identifier ("<20-digit TOID>-<10-digit eventIndex>") where the
// upper 32 bits of the TOID hold the ledger sequence:
//
//	toid = (ledger << 32) | ((txIndex & 0xFFFFF) << 12) | (opIndex & 0xFFF)
//
// Because the ledger sits in the most-significant bits of the zero-padded
// decimal prefix, lexicographical order on ID matches chronological
// (ledger, tx_index, op_index, event_index) order, and CursorLedger can
// recover the exact ledger sequence from any OrderByID cursor to enable
// PostgreSQL range partition pruning on deep cursor walks.
func FormatTOIDCursor(ledger int64, txIndex, opIndex, eventIndex int32) string {
	if ledger < 0 {
		ledger = 0
	}
	toid := (ledger << 32) | (int64(txIndex&0xFFFFF) << 12) | int64(opIndex&0xFFF)
	if eventIndex < 0 {
		eventIndex = 0
	}
	return fmt.Sprintf("%020d-%010d", toid, eventIndex)
}

// CursorLedger extracts the Stellar ledger sequence embedded in a canonical
// 64-bit TOID event cursor ("<toid>-<eventSeq>").
//
// In Stellar SEP-0035, the upper 32 bits of the 64-bit TOID before '-' encode
// the ledger sequence (toid >> 32 > 0, i.e. toid >= 1<<32). Synthetic unit-test
// IDs that place a small sequential counter before '-' (e.g. "00000000000000000001-0000000000"
// where toid < 1<<32) have toid >> 32 == 0 and return (0, false) so synthetic
// test fixtures whose counter is decoupled from Event.Ledger are never
// mis-pruned.
func CursorLedger(cursor string) (int64, bool) {
	sep := strings.IndexByte(cursor, '-')
	if sep <= 0 || sep == len(cursor)-1 {
		return 0, false
	}
	prefix := cursor[:sep]
	suffix := cursor[sep+1:]
	for i := 0; i < len(prefix); i++ {
		if prefix[i] < '0' || prefix[i] > '9' {
			return 0, false
		}
	}
	for i := 0; i < len(suffix); i++ {
		if suffix[i] < '0' || suffix[i] > '9' {
			return 0, false
		}
	}
	toid, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil || toid < (int64(1)<<32) {
		return 0, false
	}
	ledger := toid >> 32
	if ledger <= 0 {
		return 0, false
	}
	return ledger, true
}

// HasSparseTopicFilter reports whether f specifies any JSONB topic predicate
// (Topic, TopicContains, or positional Topic0..Topic3).
func HasSparseTopicFilter(f EventFilter) bool {
	return len(f.Topic) > 0 ||
		len(f.TopicContains) > 0 ||
		len(f.Topic0) > 0 ||
		len(f.Topic1) > 0 ||
		len(f.Topic2) > 0 ||
		len(f.Topic3) > 0
}

// ValidateSparseFilterRange enforces that queries with a sparse JSONB topic
// filter bound their ledger window to at most maxSpan ledgers when a positive
// span limit is configured (via f.MaxSparseFilterLedgerSpan or maxSpan).
//
// Without a bounded ledger range, a sparse topic filter that matches zero or
// very few rows in a wide table forces the database to scan across every
// partition (or intersect a large GIN posting list across millions of rows)
// before filling a single page. Returning ErrSparseFilterRangeTooWide lets the
// API reject unbounded/over-wide sparse scans with HTTP 400 (RANGE_TOO_WIDE)
// and direct callers to narrow [from_ledger, to_ledger] or use streaming bulk
// export (/events.csv or /contracts/{id}/export).
func ValidateSparseFilterRange(f EventFilter, maxSpan int64) error {
	effectiveMax := maxSpan
	if effectiveMax <= 0 {
		effectiveMax = globalMaxSparseFilterLedgerSpan.Load()
	}
	if effectiveMax <= 0 || !HasSparseTopicFilter(f) {
		return nil
	}
	if f.FromLedger <= 0 || f.ToLedger <= 0 {
		return fmt.Errorf(
			"ledger range [%d,%d] for sparse topic filter requires both from_ledger and to_ledger within %d ledgers (or use /events.csv or /contracts/{id}/export): %w",
			f.FromLedger, f.ToLedger, effectiveMax, ErrSparseFilterRangeTooWide,
		)
	}
	span := f.ToLedger - f.FromLedger
	if span > effectiveMax {
		return fmt.Errorf(
			"ledger range [%d,%d] spans %d ledgers, exceeding max sparse filter span (%d); narrow from_ledger/to_ledger or use /events.csv or /contracts/{id}/export: %w",
			f.FromLedger, f.ToLedger, span, effectiveMax, ErrSparseFilterRangeTooWide,
		)
	}
	return nil
}
