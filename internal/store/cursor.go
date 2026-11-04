package store

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
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

// Pagination cursors and the Cursor Stability Contract.
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
// (sort value, id) total, and the cursor has to carry both halves.
//
// # Envelope Format & Stability Contract
//
// Opaque cursors minted by SoroTrail use a self-identifying, versioned,
// backend-tagged envelope with a keyed HMAC-SHA256 integrity checksum:
//
//	<version>.<backend>.<payload_b64url>.<mac_b64url>
//
// Where:
//   - <version> is the cursor wire-format version ("v2" is current; "v1" and
//     legacy unversioned base64url payloads remain accepted for backwards
//     compatibility across rolling upgrades).
//   - <backend> identifies the storage backend family that minted the cursor
//     ("pg" for PostgreSQL, "sq" for SQLite, "ch" for ClickHouse, or "an" for
//     backend-agnostic helpers). A cursor minted by one backend is rejected
//     when replayed against a different backend (ErrCrossBackendCursor).
//   - <payload_b64url> is the raw URL-safe base64 encoding of the keyset tuple
//     (e.g. "<sort_value>|<id>" or "<id>").
//   - <mac_b64url> is a 24-bit (4-character base64url) keyed HMAC-SHA256
//     integrity checksum over "<version>.<backend>.<payload_b64url>" that
//     detects accidental truncation, corruption, or client-side mutation
//     (ErrTamperedCursor) while keeping the worst-case cursor under
//     config.ValidCursor's 128-character ceiling. Note that this tag is an
//     integrity checksum rather than a cryptographic security boundary:
//     legacy v1 cursors without a checksum remain accepted during the N-1
//     upgrade window, and multi-tenant authorization is always enforced by
//     store.Scope predicates independently of the cursor.
//
// All cursor decode failures wrap ErrInvalidCursor (or
// ErrInvalidContractsCursor for contracts/dead-letter cursors) in addition to
// the specific sentinel (ErrStaleCursor, ErrCrossBackendCursor,
// ErrTamperedCursor), so HTTP handlers uniformly surface 400 Bad Request with
// code INVALID_CURSOR and never 500 Internal Server Error.

const (
	// CursorVersionV1 is the legacy cursor format version. Decoders continue
	// to accept both "v1.<payload>" / "v1.<backend>.<payload>" envelopes and
	// bare unversioned base64url cursors minted before envelope versioning.
	CursorVersionV1 = "v1"

	// CursorVersionCurrent is the active cursor wire-format version minted by
	// all store cursor encoders.
	CursorVersionCurrent = "v2"

	// CursorBackendPostgres tags cursors minted by the PostgreSQL store.
	CursorBackendPostgres = "pg"
	// CursorBackendSQLite tags cursors minted by the SQLite store.
	CursorBackendSQLite = "sq"
	// CursorBackendClickHouse tags cursors minted by the ClickHouse store.
	CursorBackendClickHouse = "ch"
	// CursorBackendAny tags cursors minted by backend-agnostic helpers.
	// It is accepted by every backend decoder.
	CursorBackendAny = "an"

	cursorHMACKey = "sorotrail-cursor-stability-contract-v2"
)

var (
	// ErrStaleCursor indicates that a pagination cursor was minted under an
	// incompatible cursor version, a different storage backend, or a different
	// sort ordering, and the caller should restart pagination from an explicit
	// anchor (such as from_ledger) or an empty cursor.
	ErrStaleCursor = errors.New("stale cursor")

	// ErrCrossBackendCursor indicates that a pagination cursor minted by one
	// storage backend (e.g. PostgreSQL) was replayed against a different
	// storage backend (e.g. SQLite or ClickHouse).
	ErrCrossBackendCursor = errors.New("cursor from incompatible storage backend")

	// ErrTamperedCursor indicates that a pagination cursor's envelope,
	// base64url payload, or integrity tag was corrupted or modified.
	ErrTamperedCursor = errors.New("tampered cursor")
)

func normalizeCursorBackend(backend string) string {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "pg", "postgres", "postgresql":
		return CursorBackendPostgres
	case "sq", "sqlite", "sqlite3":
		return CursorBackendSQLite
	case "ch", "clickhouse":
		return CursorBackendClickHouse
	case "", "an", "any":
		return CursorBackendAny
	default:
		return ""
	}
}

func cursorMAC(version, backend, encodedPayload string) string {
	mac := hmac.New(sha256.New, []byte(cursorHMACKey))
	mac.Write([]byte(version))
	mac.Write([]byte{'.'})
	mac.Write([]byte(backend))
	mac.Write([]byte{'.'})
	mac.Write([]byte(encodedPayload))
	sum := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(sum[:3])
}

func validCursorMAC(version, rawBackend, normBackend, encodedPayload, gotMAC string) bool {
	if gotMAC == "" {
		return false
	}
	if hmac.Equal([]byte(gotMAC), []byte(cursorMAC(version, normBackend, encodedPayload))) {
		return true
	}
	if rawBackend != normBackend && hmac.Equal([]byte(gotMAC), []byte(cursorMAC(version, rawBackend, encodedPayload))) {
		return true
	}
	return false
}

func wrapCursorEnvelope(version, backend string, payload []byte) string {
	normBackend := normalizeCursorBackend(backend)
	if normBackend == "" {
		normBackend = CursorBackendAny
	}
	if version == "" {
		version = CursorVersionCurrent
	}
	enc := base64.RawURLEncoding.EncodeToString(payload)
	sig := cursorMAC(version, normBackend, enc)
	return version + "." + normBackend + "." + enc + "." + sig
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func hasCursorEnvelopePrefix(cursor string) bool {
	dot := strings.IndexByte(cursor, '.')
	if dot < 2 || cursor[0] != 'v' {
		return false
	}
	return isDigits(cursor[1:dot])
}

func checkCursorBackend(envBackend, expectedBackend string) error {
	normExpected := normalizeCursorBackend(expectedBackend)
	if normExpected == "" || normExpected == CursorBackendAny || envBackend == CursorBackendAny {
		return nil
	}
	if envBackend != normExpected {
		return fmt.Errorf("%w: %w: %w: cursor backend %q cannot be replayed on %q",
			ErrInvalidCursor, ErrStaleCursor, ErrCrossBackendCursor, envBackend, normExpected)
	}
	return nil
}

func unwrapCursorEnvelope(cursor, expectedBackend string) ([]byte, string, string, error) {
	if cursor == "" {
		return nil, "", "", fmt.Errorf("%w: %w: empty cursor", ErrInvalidCursor, ErrTamperedCursor)
	}
	if strings.Contains(cursor, ".") {
		parts := strings.Split(cursor, ".")
		ver := parts[0]
		if len(ver) < 2 || ver[0] != 'v' || !isDigits(ver[1:]) {
			return nil, "", "", fmt.Errorf("%w: %w: malformed cursor envelope prefix", ErrInvalidCursor, ErrTamperedCursor)
		}
		if ver != CursorVersionCurrent && ver != CursorVersionV1 {
			return nil, ver, "", fmt.Errorf("%w: %w: unsupported cursor version %q", ErrInvalidCursor, ErrStaleCursor, ver)
		}

		switch {
		case len(parts) == 4:
			normBackend := normalizeCursorBackend(parts[1])
			if normBackend == "" {
				return nil, ver, "", fmt.Errorf("%w: %w: unknown cursor backend %q", ErrInvalidCursor, ErrTamperedCursor, parts[1])
			}
			if !validCursorMAC(ver, parts[1], normBackend, parts[2], parts[3]) {
				return nil, ver, normBackend, fmt.Errorf("%w: %w: cursor integrity check failed", ErrInvalidCursor, ErrTamperedCursor)
			}
			if err := checkCursorBackend(normBackend, expectedBackend); err != nil {
				return nil, ver, normBackend, err
			}
			raw, err := base64.RawURLEncoding.DecodeString(parts[2])
			if err != nil || len(raw) == 0 {
				return nil, ver, normBackend, fmt.Errorf("%w: %w: not valid base64url", ErrInvalidCursor, ErrTamperedCursor)
			}
			return raw, ver, normBackend, nil

		case ver == CursorVersionV1 && len(parts) == 3:
			normBackend := normalizeCursorBackend(parts[1])
			if normBackend == "" {
				return nil, ver, "", fmt.Errorf("%w: %w: unknown cursor backend %q", ErrInvalidCursor, ErrTamperedCursor, parts[1])
			}
			if err := checkCursorBackend(normBackend, expectedBackend); err != nil {
				return nil, ver, normBackend, err
			}
			raw, err := base64.RawURLEncoding.DecodeString(parts[2])
			if err != nil || len(raw) == 0 {
				return nil, ver, normBackend, fmt.Errorf("%w: %w: not valid base64url", ErrInvalidCursor, ErrTamperedCursor)
			}
			return raw, ver, normBackend, nil

		case ver == CursorVersionV1 && len(parts) == 2:
			raw, err := base64.RawURLEncoding.DecodeString(parts[1])
			if err != nil || len(raw) == 0 {
				return nil, ver, CursorBackendAny, fmt.Errorf("%w: %w: not valid base64url", ErrInvalidCursor, ErrTamperedCursor)
			}
			return raw, ver, CursorBackendAny, nil

		default:
			return nil, ver, "", fmt.Errorf("%w: %w: malformed %s cursor envelope", ErrInvalidCursor, ErrTamperedCursor, ver)
		}
	}

	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, "", "", fmt.Errorf("%w: %w: not valid base64url", ErrInvalidCursor, ErrTamperedCursor)
	}
	return raw, CursorVersionV1, CursorBackendAny, nil
}

// EncodeCursor builds the cursor that resumes after e under the given
// ordering. orderBy uses the OrderBy* constants; "" means OrderByID.
func EncodeCursor(orderBy string, e Event) string {
	return EncodeCursorForBackend(CursorBackendAny, orderBy, e)
}

// EncodeCursorForBackend builds the cursor that resumes after e under the
// given ordering and stamps the storage backend into composite cursors.
// OrderByID (and "") continues to return e.ID verbatim so bare event-ID
// cursors remain drop-in compatible across versions.
func EncodeCursorForBackend(backend, orderBy string, e Event) string {
	switch orderBy {
	case OrderByLedger:
		return encodeCompositeCursorForBackend(backend, fmt.Sprint(e.Ledger), e.ID)
	case OrderByCreatedAt:
		// RFC3339 with nanoseconds so the round trip is lossless.
		return encodeCompositeCursorForBackend(backend, e.CreatedAt.UTC().Format(time.RFC3339Nano), e.ID)
	default:
		return e.ID
	}
}

// EncodeVersionedEventCursor builds an explicitly versioned and backend-tagged
// cursor envelope for any event ordering, including OrderByID. Decoders accept
// both bare event IDs and versioned OrderByID envelopes.
func EncodeVersionedEventCursor(backend, orderBy string, e Event) string {
	switch orderBy {
	case OrderByLedger, OrderByCreatedAt:
		return EncodeCursorForBackend(backend, orderBy, e)
	default:
		return wrapCursorEnvelope(CursorVersionCurrent, backend, []byte(e.ID))
	}
}

// EncodeCursorV1 builds a v1 cursor envelope for backwards-compatibility and
// rolling-upgrade tests.
func EncodeCursorV1(backend, orderBy string, e Event) string {
	switch orderBy {
	case OrderByLedger:
		return wrapCursorEnvelope(CursorVersionV1, backend, []byte(fmt.Sprint(e.Ledger)+"|"+e.ID))
	case OrderByCreatedAt:
		return wrapCursorEnvelope(CursorVersionV1, backend, []byte(e.CreatedAt.UTC().Format(time.RFC3339Nano)+"|"+e.ID))
	default:
		return wrapCursorEnvelope(CursorVersionV1, backend, []byte(e.ID))
	}
}

// DecodeEventIDCursor validates an OrderByID cursor (either a bare event ID or
// a versioned OrderByID envelope).
func DecodeEventIDCursor(cursor string) (string, error) {
	return DecodeEventIDCursorForBackend(CursorBackendAny, cursor)
}

// DecodeEventIDCursorForBackend validates an OrderByID cursor against
// expectedBackend. Bare event IDs are returned unchanged; versioned envelopes
// have their version, backend tag, and HMAC verified; and composite cursors
// (minted for ledger or created_at ordering) are rejected as stale/invalid.
func DecodeEventIDCursorForBackend(expectedBackend, cursor string) (string, error) {
	if cursor == "" {
		return "", fmt.Errorf("%w: %w: empty cursor", ErrInvalidCursor, ErrTamperedCursor)
	}
	if hasCursorEnvelopePrefix(cursor) || strings.Contains(cursor, ".") {
		raw, _, _, err := unwrapCursorEnvelope(cursor, expectedBackend)
		if err != nil {
			return "", err
		}
		id := string(raw)
		if strings.Contains(id, "|") {
			return "", fmt.Errorf("%w: %w: composite cursor cannot be replayed with order_by=id", ErrInvalidCursor, ErrStaleCursor)
		}
		if id == "" || !utf8.ValidString(id) {
			return "", fmt.Errorf("%w: %w: invalid event id in cursor", ErrInvalidCursor, ErrTamperedCursor)
		}
		return id, nil
	}
	if raw, err := base64.RawURLEncoding.DecodeString(cursor); err == nil {
		if sep := strings.LastIndex(string(raw), "|"); sep > 0 && sep < len(raw)-1 {
			sv := string(raw[:sep])
			if _, err := strconv.ParseInt(sv, 10, 64); err == nil {
				return "", fmt.Errorf("%w: %w: ledger cursor cannot be replayed with order_by=id", ErrInvalidCursor, ErrStaleCursor)
			}
			if _, err := time.Parse(time.RFC3339Nano, sv); err == nil {
				return "", fmt.Errorf("%w: %w: created_at cursor cannot be replayed with order_by=id", ErrInvalidCursor, ErrStaleCursor)
			}
		}
	}
	return cursor, nil
}

func encodeCompositeCursor(sortValue, id string) string {
	return encodeCompositeCursorForBackend(CursorBackendAny, sortValue, id)
}

func encodeCompositeCursorForBackend(backend, sortValue, id string) string {
	return wrapCursorEnvelope(CursorVersionCurrent, backend, []byte(sortValue+"|"+id))
}

// decodeCompositeCursor splits a composite cursor back into its sort value
// and id. It reports an error the caller should surface as a bad request:
// a malformed cursor is client input, not a server fault.
func decodeCompositeCursor(cursor string) (sortValue, id string, err error) {
	return decodeCompositeCursorForBackend(CursorBackendAny, cursor)
}

func decodeCompositeCursorForBackend(expectedBackend, cursor string) (sortValue, id string, err error) {
	raw, _, _, err := unwrapCursorEnvelope(cursor, expectedBackend)
	if err != nil {
		return "", "", err
	}
	// The id is a fixed-shape TOID and never contains "|", while a
	// timestamp does not either — but split on the last separator anyway so
	// a future sort value containing "|" stays decodable.
	sep := strings.LastIndex(string(raw), "|")
	if sep < 0 {
		return "", "", fmt.Errorf("%w: %w: missing separator", ErrInvalidCursor, ErrTamperedCursor)
	}
	sortValue, id = string(raw[:sep]), string(raw[sep+1:])
	if sortValue == "" || id == "" {
		return "", "", fmt.Errorf("%w: %w: empty component", ErrInvalidCursor, ErrTamperedCursor)
	}
	return sortValue, id, nil
}

// EncodeContractsCursor keys keyset pagination for ListContracts over
// the (sortValue, contractID) pair. sortAs is the literal column being
// sorted — the API keeps the same cursor format for any sort so a
// client can mix sortings between pages, but typically a caller picks
// one and sticks with it.
func EncodeContractsCursor(sortAs, sortValue, contractID string) string {
	return EncodeContractsCursorForBackend(CursorBackendAny, sortAs, sortValue, contractID)
}

// EncodeContractsCursorForBackend keys keyset pagination for ListContracts and
// binds the cursor envelope to the issuing storage backend.
func EncodeContractsCursorForBackend(backend, sortAs, sortValue, contractID string) string {
	_ = sortAs
	return encodeCompositeCursorForBackend(backend, sortValue, contractID)
}

// DecodeContractsCursor is the inverse of EncodeContractsCursor. It
// surfaces a typed error the handler maps to 400.
func DecodeContractsCursor(cursor string) (sortValue, contractID string, err error) {
	return DecodeContractsCursorForBackend(CursorBackendAny, cursor)
}

// DecodeContractsCursorForBackend decodes a ListContracts cursor and verifies
// that its backend tag is compatible with expectedBackend.
func DecodeContractsCursorForBackend(expectedBackend, cursor string) (sortValue, contractID string, err error) {
	sortValue, contractID, err = decodeCompositeCursorForBackend(expectedBackend, cursor)
	if err != nil {
		err = fmt.Errorf("%w: %w: contracts cursor", ErrInvalidContractsCursor, err)
	}
	return
}

// EncodeSpecOverrideHistoryCursor keys keyset pagination for the spec-override
// history over its monotonic version id. The history is walked newest first,
// so the cursor carries the last id seen and the next page is the rows with
// a smaller id.
func EncodeSpecOverrideHistoryCursor(id int64) string {
	return EncodeSpecOverrideHistoryCursorForBackend(CursorBackendAny, id)
}

// EncodeSpecOverrideHistoryCursorForBackend encodes a spec-override history
// cursor bound to the given storage backend.
func EncodeSpecOverrideHistoryCursorForBackend(backend string, id int64) string {
	return wrapCursorEnvelope(CursorVersionCurrent, backend, []byte(strconv.FormatInt(id, 10)))
}

// DecodeSpecOverrideHistoryCursor is the inverse of
// EncodeSpecOverrideHistoryCursor. A malformed cursor is client input, so it
// surfaces as ErrInvalidCursor for the handler to map to 400.
func DecodeSpecOverrideHistoryCursor(cursor string) (int64, error) {
	return DecodeSpecOverrideHistoryCursorForBackend(CursorBackendAny, cursor)
}

// DecodeSpecOverrideHistoryCursorForBackend decodes a spec-override history
// cursor and checks backend compatibility.
func DecodeSpecOverrideHistoryCursorForBackend(expectedBackend, cursor string) (int64, error) {
	raw, _, _, err := unwrapCursorEnvelope(cursor, expectedBackend)
	if err != nil {
		return 0, fmt.Errorf("%w: spec override history cursor: %w", ErrInvalidCursor, err)
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: %w: spec override history cursor is not a version id", ErrInvalidCursor, ErrTamperedCursor)
	}
	return id, nil
}

// EncodeDeadLetterCursor encodes a dead-letter primary-key cursor.
func EncodeDeadLetterCursor(id int64) string {
	return EncodeDeadLetterCursorForBackend(CursorBackendAny, id)
}

// EncodeDeadLetterCursorForBackend encodes a dead-letter primary-key cursor
// bound to the given storage backend.
func EncodeDeadLetterCursorForBackend(backend string, id int64) string {
	return wrapCursorEnvelope(CursorVersionCurrent, backend, []byte(strconv.FormatInt(id, 10)))
}

// DecodeDeadLetterCursor decodes a dead-letter cursor into its positive row ID.
func DecodeDeadLetterCursor(cursor string) (int64, error) {
	return DecodeDeadLetterCursorForBackend(CursorBackendAny, cursor)
}

// DecodeDeadLetterCursorForBackend decodes a dead-letter cursor and verifies
// backend compatibility. It wraps ErrInvalidContractsCursor so existing
// handlers and tests continue to map failures to 400 INVALID_CURSOR.
func DecodeDeadLetterCursorForBackend(expectedBackend, cursor string) (int64, error) {
	raw, _, _, err := unwrapCursorEnvelope(cursor, expectedBackend)
	if err != nil {
		return 0, fmt.Errorf("%w: %w: dead letter cursor", ErrInvalidContractsCursor, err)
	}
	id, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: %w: %w: dead letter cursor is not a positive id", ErrInvalidContractsCursor, ErrInvalidCursor, ErrTamperedCursor)
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
