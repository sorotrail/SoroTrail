package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/config"
)

func TestEncodeDecodeCompositeCursorRoundTrip(t *testing.T) {
	cases := []struct{ sortValue, id string }{
		{"123", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{"2024-01-02T03:04:05.000000000Z", "00aabbccdd002233aabbccdd002233aabbccdd002233aabbccdd002233"},
		{"token#1", "c"},
	}
	for _, c := range cases {
		enc := encodeCompositeCursor(c.sortValue, c.id)
		sv, id, err := decodeCompositeCursor(enc)
		if err != nil {
			t.Fatalf("decode(%q): %v", enc, err)
		}
		if sv != c.sortValue || id != c.id {
			t.Fatalf("round trip = (%q,%q), want (%q,%q)", sv, id, c.sortValue, c.id)
		}
	}
}

func TestDecodeCompositeCursorRejectsInvalid(t *testing.T) {
	for _, c := range []string{"!!!not-base64!!!", "abcdef", "|", "lead|", "|tail"} {
		if _, _, err := decodeCompositeCursor(c); err == nil {
			t.Fatalf("decode(%q) expected error", c)
		} else if !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("decode(%q) error = %v, want ErrInvalidCursor", c, err)
		}
	}
}

func TestEncodeDecodeContractsCursor(t *testing.T) {
	enc := EncodeContractsCursor("value", "99", "CA000000000000000000000000000000000000000000000000000000")
	sv, id, err := DecodeContractsCursor(enc)
	if err != nil {
		t.Fatalf("DecodeContractsCursor: %v", err)
	}
	if sv != "99" || id != "CA000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("DecodeContractsCursor = (%q,%q)", sv, id)
	}
	if _, _, err := DecodeContractsCursor("!!!not-base64!!!"); err == nil || !errors.Is(err, ErrInvalidContractsCursor) {
		t.Fatalf("expected ErrInvalidContractsCursor, got %v", err)
	}
}

func TestEncodeCursorOrderByID(t *testing.T) {
	if got := EncodeCursor(OrderByID, Event{ID: "abc123"}); got != "abc123" {
		t.Fatalf("EncodeCursor(OrderByID) = %q, want abc123", got)
	}
}

func TestEncodeCursorCompositeOrdersRoundTrip(t *testing.T) {
	e := Event{ID: "abc123", Ledger: 99, CreatedAt: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)}
	for _, ob := range []string{OrderByLedger, OrderByCreatedAt} {
		cursor := EncodeCursor(ob, e)
		sv, id, err := decodeCompositeCursor(cursor)
		if err != nil {
			t.Fatalf("decode %s cursor: %v", ob, err)
		}
		if id != "abc123" {
			t.Fatalf("id = %q, want abc123", id)
		}
		if ob == OrderByLedger && sv != "99" {
			t.Fatalf("sort = %q, want 99", sv)
		}
	}
}

func TestFormatTOIDCursorAndCursorLedger(t *testing.T) {
	c1 := FormatTOIDCursor(100_000, 1, 0, 0)
	c2 := FormatTOIDCursor(100_000, 1, 0, 1)
	c3 := FormatTOIDCursor(100_000, 2, 0, 0)
	c4 := FormatTOIDCursor(250_000, 0, 0, 0)

	if c1 >= c2 || c2 >= c3 || c3 >= c4 {
		t.Fatalf("TOID cursors not ordered chronologically: %q, %q, %q, %q", c1, c2, c3, c4)
	}

	for _, tc := range []struct {
		cursor     string
		wantLedger int64
		wantOK     bool
	}{
		{c1, 100_000, true},
		{c4, 250_000, true},
		{FormatTOIDCursor(1, 0, 0, 0), 1, true},
		{"0001099511627776-0000000001", 256, true},
		// Small synthetic test IDs (toid < 1<<32) must return (0, false) so
		// unit tests with synthetic counters decoupled from Event.Ledger are
		// never mis-pruned.
		{"00000000000000000001-0000000000", 0, false},
		{"00000000000000000999-0000000010", 0, false},
		{"abc123", 0, false},
		{"12345-", 0, false},
		{"-0000000001", 0, false},
		{EncodeCursor(OrderByLedger, Event{Ledger: 500, ID: c1}), 0, false},
	} {
		gotLedger, gotOK := CursorLedger(tc.cursor)
		if gotOK != tc.wantOK || gotLedger != tc.wantLedger {
			t.Fatalf("CursorLedger(%q) = (%d, %v), want (%d, %v)",
				tc.cursor, gotLedger, gotOK, tc.wantLedger, tc.wantOK)
		}
	}
}

func TestValidateSparseFilterRange(t *testing.T) {
	topic := json.RawMessage(`{"symbol":"transfer"}`)

	// Non-sparse filter is always permitted even over an unbounded ledger range.
	if err := ValidateSparseFilterRange(EventFilter{ContractID: "C123"}, DefaultMaxSparseFilterLedgerSpan); err != nil {
		t.Fatalf("expected non-sparse filter to pass, got %v", err)
	}

	// Sparse filter with maxSpan <= 0 (disabled) is permitted.
	if err := ValidateSparseFilterRange(EventFilter{Topic0: topic}, 0); err != nil {
		t.Fatalf("expected disabled maxSpan to pass, got %v", err)
	}

	// Sparse filter without both FromLedger and ToLedger fails with ErrSparseFilterRangeTooWide.
	for _, f := range []EventFilter{
		{Topic: topic},
		{TopicContains: json.RawMessage(`[{"symbol":"transfer"}]`), FromLedger: 100},
		{Topic0: topic, ToLedger: 500},
		{Topic3: topic},
	} {
		err := ValidateSparseFilterRange(f, DefaultMaxSparseFilterLedgerSpan)
		if !errors.Is(err, ErrSparseFilterRangeTooWide) {
			t.Fatalf("expected ErrSparseFilterRangeTooWide for %+v, got %v", f, err)
		}
		if !strings.HasPrefix(err.Error(), "ledger range [") {
			t.Fatalf("expected error message to start with 'ledger range [' for API classification, got %q", err.Error())
		}
	}

	// Sparse filter exceeding maxSpan fails.
	wide := EventFilter{
		Topic0:     topic,
		FromLedger: 100_000,
		ToLedger:   100_000 + DefaultMaxSparseFilterLedgerSpan + 1,
	}
	if err := ValidateSparseFilterRange(wide, DefaultMaxSparseFilterLedgerSpan); !errors.Is(err, ErrSparseFilterRangeTooWide) {
		t.Fatalf("expected ErrSparseFilterRangeTooWide for wide span, got %v", err)
	}

	// Sparse filter within maxSpan passes.
	bounded := EventFilter{
		Topic0:     topic,
		FromLedger: 100_000,
		ToLedger:   100_000 + DefaultMaxSparseFilterLedgerSpan,
	}
	if err := ValidateSparseFilterRange(bounded, DefaultMaxSparseFilterLedgerSpan); err != nil {
		t.Fatalf("expected bounded sparse filter to pass, got %v", err)
	}
}

func TestBuildQueryEventsSQL_PartitionPruningPredicates(t *testing.T) {
	toidCursor := FormatTOIDCursor(240_000, 3, 1, 2)

	t.Run("OrderByID with canonical TOID cursor injects scalar ledger >= bound", func(t *testing.T) {
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:  WildcardScope(),
			Cursor: toidCursor,
			Order:  "asc",
			Limit:  25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if !strings.Contains(q, "ledger >= $1") || !strings.Contains(q, "id > $2") {
			t.Fatalf("expected scalar ledger bound and id keyset predicate in query, got: %s (args=%v)", q, args)
		}
		if len(args) < 2 || args[0] != int64(240_000) || args[1] != toidCursor {
			t.Fatalf("unexpected args: %v", args)
		}
	})

	t.Run("OrderByID DESC with canonical TOID cursor injects scalar ledger <= bound", func(t *testing.T) {
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:  WildcardScope(),
			Cursor: toidCursor,
			Order:  "desc",
			Limit:  25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if !strings.Contains(q, "ledger <= $1") || !strings.Contains(q, "id < $2") {
			t.Fatalf("expected scalar ledger <= bound and id < predicate in query, got: %s (args=%v)", q, args)
		}
	})

	t.Run("OrderByID with synthetic small-integer cursor omits ledger bound", func(t *testing.T) {
		q, _, _, err := buildQueryEventsSQL(EventFilter{
			Scope:  WildcardScope(),
			Cursor: "00000000000000000005-0000000000",
			Order:  "asc",
			Limit:  25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if strings.Contains(q, "ledger >=") {
			t.Fatalf("synthetic test ID must not inject ledger bound: %s", q)
		}
	})

	t.Run("OrderByLedger injects scalar ledger bound alongside row-value comparison", func(t *testing.T) {
		ledgerCursor := EncodeCursor(OrderByLedger, Event{Ledger: 360_000, ID: toidCursor})
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:   WildcardScope(),
			OrderBy: OrderByLedger,
			Order:   "asc",
			Cursor:  ledgerCursor,
			Limit:   25,
		}, 0)
		if err != nil {
			t.Fatalf("buildQueryEventsSQL: %v", err)
		}
		if !strings.Contains(q, "ledger >= $1::bigint") || !strings.Contains(q, "(ledger, id) > ($1::bigint, $2)") {
			t.Fatalf("expected both scalar ledger bound and row-value predicate, got: %s (args=%v)", q, args)
		}
	})

	t.Run("Cross-order composite cursor replay returns ErrInvalidCursor", func(t *testing.T) {
		createdAtCursor := EncodeCursor(OrderByCreatedAt, Event{
			ID:        toidCursor,
			CreatedAt: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
		})
		if _, _, _, err := buildQueryEventsSQL(EventFilter{
			Scope:   WildcardScope(),
			OrderBy: OrderByLedger,
			Cursor:  createdAtCursor,
		}, 0); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("expected ErrInvalidCursor when replaying created_at cursor on ledger order, got %v", err)
		}

		ledgerCursor := EncodeCursor(OrderByLedger, Event{Ledger: 360_000, ID: toidCursor})
		if _, _, _, err := buildQueryEventsSQL(EventFilter{
			Scope:   WildcardScope(),
			OrderBy: OrderByCreatedAt,
			Cursor:  ledgerCursor,
		}, 0); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("expected ErrInvalidCursor when replaying ledger cursor on created_at order, got %v", err)
		}
	})
}

func TestCursorStabilityContract_EnvelopeFormatAndValidCursorBounds(t *testing.T) {
	contractID := "CA000000000000000000000000000000000000000000000000000000"
	ts := time.Date(2026, 10, 20, 23, 59, 59, 987654321, time.UTC)
	ev := Event{
		ID:         "0001099511627776-0000000001",
		ContractID: contractID,
		Ledger:     999999999,
		CreatedAt:  ts,
	}

	for _, backend := range []string{CursorBackendPostgres, CursorBackendSQLite, CursorBackendClickHouse, CursorBackendAny} {
		ledgerCur := EncodeCursorForBackend(backend, OrderByLedger, ev)
		assert.True(t, strings.HasPrefix(ledgerCur, CursorVersionCurrent+"."+backend+"."), "unexpected prefix: %s", ledgerCur)
		assert.True(t, config.ValidCursor(ledgerCur), "ledger cursor must pass config.ValidCursor (len=%d): %s", len(ledgerCur), ledgerCur)

		createdCur := EncodeCursorForBackend(backend, OrderByCreatedAt, ev)
		assert.True(t, strings.HasPrefix(createdCur, CursorVersionCurrent+"."+backend+"."), "unexpected prefix: %s", createdCur)
		assert.True(t, config.ValidCursor(createdCur), "created_at cursor must pass config.ValidCursor (len=%d): %s", len(createdCur), createdCur)

		versionedIDCur := EncodeVersionedEventCursor(backend, OrderByID, ev)
		assert.True(t, config.ValidCursor(versionedIDCur), "versioned id cursor must pass config.ValidCursor")
		gotID, err := DecodeEventIDCursorForBackend(backend, versionedIDCur)
		require.NoError(t, err)
		assert.Equal(t, ev.ID, gotID)

		contractsCur := EncodeContractsCursorForBackend(backend, SortByLastSeen, ts.Format(time.RFC3339Nano), contractID)
		assert.True(t, config.ValidCursor(contractsCur), "worst-case contracts cursor must pass config.ValidCursor (len=%d): %s", len(contractsCur), contractsCur)
		sv, cid, err := DecodeContractsCursorForBackend(backend, contractsCur)
		require.NoError(t, err)
		assert.Equal(t, ts.Format(time.RFC3339Nano), sv)
		assert.Equal(t, contractID, cid)

		specCur := EncodeSpecOverrideHistoryCursorForBackend(backend, 42)
		assert.True(t, config.ValidCursor(specCur))
		specID, err := DecodeSpecOverrideHistoryCursorForBackend(backend, specCur)
		require.NoError(t, err)
		assert.Equal(t, int64(42), specID)

		dlCur := EncodeDeadLetterCursorForBackend(backend, 77)
		assert.True(t, config.ValidCursor(dlCur))
		dlID, err := DecodeDeadLetterCursorForBackend(backend, dlCur)
		require.NoError(t, err)
		assert.Equal(t, int64(77), dlID)
	}
}

func TestCursorStabilityContract_VersionBumpAndStaleRejection(t *testing.T) {
	ev := Event{
		ID:        "0001099511627776-0000000001",
		Ledger:    12345,
		CreatedAt: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC),
	}

	t.Run("accepts v1 signed, v1 unsigned backend-tagged, v1 bare, and legacy unversioned cursors", func(t *testing.T) {
		rawPayload := base64.RawURLEncoding.EncodeToString([]byte("12345|" + ev.ID))

		v1Signed := EncodeCursorV1(CursorBackendPostgres, OrderByLedger, ev)
		v1BackendUnsigned := "v1.pg." + rawPayload
		v1Bare := "v1." + rawPayload
		legacyUnversioned := rawPayload

		for _, cur := range []string{v1Signed, v1BackendUnsigned, v1Bare, legacyUnversioned} {
			sv, id, err := decodeCompositeCursorForBackend(CursorBackendPostgres, cur)
			require.NoError(t, err, "cursor %q must remain accepted across minor/rolling upgrades", cur)
			assert.Equal(t, "12345", sv)
			assert.Equal(t, ev.ID, id)
		}
	})

	t.Run("rejects unknown or retired cursor versions with ErrStaleCursor and ErrInvalidCursor", func(t *testing.T) {
		rawPayload := base64.RawURLEncoding.EncodeToString([]byte("12345|" + ev.ID))
		for _, ver := range []string{"v0", "v3", "v99"} {
			sig := cursorMAC(ver, CursorBackendPostgres, rawPayload)
			stale := ver + ".pg." + rawPayload + "." + sig

			_, _, err := decodeCompositeCursorForBackend(CursorBackendPostgres, stale)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)

			_, err = DecodeEventIDCursorForBackend(CursorBackendPostgres, stale)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)

			_, _, err = DecodeContractsCursorForBackend(CursorBackendPostgres, stale)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidContractsCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)
		}
	})
}

func TestCursorStabilityContract_CrossBackendRejection(t *testing.T) {
	ev := Event{
		ID:        "0001099511627776-0000000001",
		Ledger:    500,
		CreatedAt: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC),
	}
	backends := []string{CursorBackendPostgres, CursorBackendSQLite, CursorBackendClickHouse}

	for _, src := range backends {
		ledgerCur := EncodeCursorForBackend(src, OrderByLedger, ev)
		idCur := EncodeVersionedEventCursor(src, OrderByID, ev)
		contractsCur := EncodeContractsCursorForBackend(src, SortByFirstLedger, "500", "CA000000000000000000000000000000000000000000000000000000")
		specCur := EncodeSpecOverrideHistoryCursorForBackend(src, 11)
		dlCur := EncodeDeadLetterCursorForBackend(src, 22)

		for _, dst := range backends {
			if src == dst {
				_, _, err := decodeCompositeCursorForBackend(dst, ledgerCur)
				require.NoError(t, err)
				_, err = DecodeEventIDCursorForBackend(dst, idCur)
				require.NoError(t, err)
				_, _, err = DecodeContractsCursorForBackend(dst, contractsCur)
				require.NoError(t, err)
				_, err = DecodeSpecOverrideHistoryCursorForBackend(dst, specCur)
				require.NoError(t, err)
				_, err = DecodeDeadLetterCursorForBackend(dst, dlCur)
				require.NoError(t, err)
				continue
			}

			_, _, err := decodeCompositeCursorForBackend(dst, ledgerCur)
			require.Error(t, err, "composite cursor from %s must fail on %s", src, dst)
			assert.ErrorIs(t, err, ErrInvalidCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)
			assert.ErrorIs(t, err, ErrCrossBackendCursor)

			_, err = DecodeEventIDCursorForBackend(dst, idCur)
			require.Error(t, err, "versioned ID cursor from %s must fail on %s", src, dst)
			assert.ErrorIs(t, err, ErrInvalidCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)
			assert.ErrorIs(t, err, ErrCrossBackendCursor)

			_, _, err = DecodeContractsCursorForBackend(dst, contractsCur)
			require.Error(t, err, "contracts cursor from %s must fail on %s", src, dst)
			assert.ErrorIs(t, err, ErrInvalidContractsCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)
			assert.ErrorIs(t, err, ErrCrossBackendCursor)

			_, err = DecodeSpecOverrideHistoryCursorForBackend(dst, specCur)
			require.Error(t, err, "spec override cursor from %s must fail on %s", src, dst)
			assert.ErrorIs(t, err, ErrInvalidCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)
			assert.ErrorIs(t, err, ErrCrossBackendCursor)

			_, err = DecodeDeadLetterCursorForBackend(dst, dlCur)
			require.Error(t, err, "dead letter cursor from %s must fail on %s", src, dst)
			assert.ErrorIs(t, err, ErrInvalidContractsCursor)
			assert.ErrorIs(t, err, ErrStaleCursor)
			assert.ErrorIs(t, err, ErrCrossBackendCursor)
		}
	}

	// ClickHouse SQL cursor builder must reject Postgres and SQLite cursors.
	pgCursor := EncodeCursorForBackend(CursorBackendPostgres, OrderByLedger, ev)
	_, err := clickHouseEventCursorClause(EventFilter{Cursor: pgCursor, OrderBy: OrderByLedger})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCrossBackendCursor)
	assert.ErrorIs(t, err, ErrInvalidCursor)
}

func TestCursorStabilityContract_TamperedCursorRejection(t *testing.T) {
	ev := Event{
		ID:        "0001099511627776-0000000001",
		Ledger:    500,
		CreatedAt: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC),
	}
	valid := EncodeCursorForBackend(CursorBackendPostgres, OrderByLedger, ev)
	parts := strings.Split(valid, ".")
	require.Len(t, parts, 4)

	// Flip one character in payload without updating MAC.
	flippedPayload := parts[2][:len(parts[2])-1] + "A"
	if flippedPayload == parts[2] {
		flippedPayload = parts[2][:len(parts[2])-1] + "B"
	}
	tamperedCases := []struct {
		name   string
		cursor string
	}{
		{
			name:   "mutated payload with original MAC",
			cursor: parts[0] + "." + parts[1] + "." + flippedPayload + "." + parts[3],
		},
		{
			name:   "mutated MAC",
			cursor: parts[0] + "." + parts[1] + "." + parts[2] + ".ZZZZ",
		},
		{
			name:   "backend tag swapped without re-signing",
			cursor: parts[0] + ".sq." + parts[2] + "." + parts[3],
		},
		{
			name:   "stripped MAC on v2 envelope",
			cursor: parts[0] + "." + parts[1] + "." + parts[2],
		},
		{
			name:   "unknown backend tag",
			cursor: "v2.mysql." + parts[2] + "." + parts[3],
		},
		{
			name:   "non-version dot prefix",
			cursor: "bad.pg." + parts[2] + "." + parts[3],
		},
	}

	for _, tc := range tamperedCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := decodeCompositeCursorForBackend(CursorBackendPostgres, tc.cursor)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidCursor)
			assert.ErrorIs(t, err, ErrTamperedCursor)
		})
	}
}

func TestCursorStabilityContract_CrossOrderingReplayRejected(t *testing.T) {
	ev := Event{
		ID:        "0001099511627776-0000000001",
		Ledger:    500,
		CreatedAt: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC),
	}
	ledgerCur := EncodeCursorForBackend(CursorBackendPostgres, OrderByLedger, ev)
	createdCur := EncodeCursorForBackend(CursorBackendPostgres, OrderByCreatedAt, ev)
	legacyLedgerCur := base64.RawURLEncoding.EncodeToString([]byte("500|" + ev.ID))

	// Composite cursors replayed against order_by=id must be rejected as stale/invalid.
	for _, cur := range []string{ledgerCur, createdCur, legacyLedgerCur} {
		_, err := DecodeEventIDCursorForBackend(CursorBackendPostgres, cur)
		require.Error(t, err, "composite cursor %q must be rejected on order_by=id", cur)
		assert.ErrorIs(t, err, ErrInvalidCursor)
		assert.ErrorIs(t, err, ErrStaleCursor)
	}
}

func TestCursorStabilityContract_SQLiteCrossBackendAndMultiTenantIsolation(t *testing.T) {
	st := newSQLiteStore(t)
	ctx := context.Background()

	const (
		tenantAContract = "CA00000000000000000000000000000000000000000000000000000A"
		tenantBContract = "CB00000000000000000000000000000000000000000000000000000B"
	)
	baseTime := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	events := []Event{
		{
			ID: "000100-0001", Network: "default", ContractID: tenantAContract,
			Ledger: 100, Type: "contract", TxHash: "tx1", Topics: json.RawMessage(`[]`),
			Value: json.RawMessage(`{"amount":1}`), CreatedAt: baseTime,
		},
		{
			ID: "000101-0001", Network: "default", ContractID: tenantAContract,
			Ledger: 101, Type: "contract", TxHash: "tx2", Topics: json.RawMessage(`[]`),
			Value: json.RawMessage(`{"amount":2}`), CreatedAt: baseTime.Add(time.Second),
		},
		{
			ID: "000102-0001", Network: "default", ContractID: tenantBContract,
			Ledger: 102, Type: "contract", TxHash: "tx3", Topics: json.RawMessage(`[]`),
			Value: json.RawMessage(`{"amount":3}`), CreatedAt: baseTime.Add(2 * time.Second),
		},
	}
	_, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)

	t.Run("SQLite store rejects Postgres and ClickHouse cursors across endpoints", func(t *testing.T) {
		pgLedgerCursor := EncodeCursorForBackend(CursorBackendPostgres, OrderByLedger, events[0])
		_, _, err := st.QueryEvents(ctx, EventFilter{
			OrderBy: OrderByLedger,
			Cursor:  pgLedgerCursor,
			Scope:   WildcardScope(),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidCursor)
		assert.ErrorIs(t, err, ErrCrossBackendCursor)

		chCreatedCursor := EncodeCursorForBackend(CursorBackendClickHouse, OrderByCreatedAt, events[0])
		_, _, err = st.QueryEvents(ctx, EventFilter{
			OrderBy: OrderByCreatedAt,
			Cursor:  chCreatedCursor,
			Scope:   WildcardScope(),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidCursor)
		assert.ErrorIs(t, err, ErrCrossBackendCursor)

		pgContractsCursor := EncodeContractsCursorForBackend(CursorBackendPostgres, SortByFirstLedger, "100", tenantAContract)
		_, _, err = st.ListContracts(ctx, ContractsFilter{
			SortKey: SortByFirstLedger,
			Cursor:  pgContractsCursor,
			Scope:   WildcardScope(),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidContractsCursor)
		assert.ErrorIs(t, err, ErrCrossBackendCursor)

		pgSpecCursor := EncodeSpecOverrideHistoryCursorForBackend(CursorBackendPostgres, 1)
		_, _, err = st.ListContractSpecOverrideHistory(ctx, tenantAContract, 10, pgSpecCursor)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidCursor)
		assert.ErrorIs(t, err, ErrCrossBackendCursor)

		pgDLCursor := EncodeDeadLetterCursorForBackend(CursorBackendPostgres, 1)
		_, _, err = st.ListDeadLetters(ctx, "", 10, pgDLCursor)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidContractsCursor)
		assert.ErrorIs(t, err, ErrCrossBackendCursor)
	})

	t.Run("cursor from tenant A replayed under tenant B scope never leaks tenant A events", func(t *testing.T) {
		scopeA := NewScope([]string{tenantAContract})
		scopeB := NewScope([]string{tenantBContract})

		for _, orderBy := range []string{OrderByID, OrderByLedger, OrderByCreatedAt} {
			pageA, cursorA, err := st.QueryEvents(ctx, EventFilter{
				Scope:   scopeA,
				OrderBy: orderBy,
				Limit:   1,
			})
			require.NoError(t, err)
			require.Len(t, pageA, 1)
			assert.Equal(t, tenantAContract, pageA[0].ContractID)
			require.NotEmpty(t, cursorA)

			// Replay Tenant A's cursor under Tenant B's scope: must only see Tenant B rows, never Tenant A's second row.
			pageB, _, err := st.QueryEvents(ctx, EventFilter{
				Scope:   scopeB,
				OrderBy: orderBy,
				Cursor:  cursorA,
				Limit:   10,
			})
			require.NoError(t, err)
			for _, ev := range pageB {
				assert.Equal(t, tenantBContract, ev.ContractID,
					"cursor from tenant A replayed by tenant B leaked contract %s under order_by=%s", ev.ContractID, orderBy)
			}
		}
	})
}
