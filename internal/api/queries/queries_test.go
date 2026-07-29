package queries

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildEventFilter_HappyPath checks that a fully-populated
// EventFilterArgs round-trips into a store.EventFilter with the right
// shape. This is the contract REST /events and GraphQL events both
// rely on.
func TestBuildEventFilter_HappyPath(t *testing.T) {
	topic, err := json.Marshal(map[string]any{"symbol": "transfer"})
	require.NoError(t, err)
	cursor := "valid_cursor"
	args := EventFilterArgs{
		ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Types:      []string{"contract"},
		Topic:      topic,
		TxHash:     "abc123",
		FromLedger: 10,
		ToLedger:   20,
		FromTime:   time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		ToTime:     time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		Order:      "asc",
		OrderBy:    "id",
		Cursor:     cursor,
		Limit:      5,
	}

	got, err := BuildEventFilter(args)
	require.NoError(t, err)
	assert.Equal(t, args.ContractID, got.ContractID)
	assert.Equal(t, []string{"contract"}, got.Types)
	assert.JSONEq(t, string(topic), string(got.Topic))
	assert.Equal(t, cursor, got.Cursor)
	assert.Equal(t, int64(10), got.FromLedger)
	assert.Equal(t, int64(20), got.ToLedger)
	assert.Equal(t, 5, got.Limit, "explicit Limit should not be replaced by default")
}

// TestBuildEventFilter_DefaultsLimitOnZero asserts the storefront rule
// for absent limits: a zero value is replaced with DefaultPageSize.
func TestBuildEventFilter_DefaultsLimitOnZero(t *testing.T) {
	got, err := BuildEventFilter(EventFilterArgs{})
	require.NoError(t, err)
	assert.Equal(t, DefaultPageSize, got.Limit)
}

// TestBuildEventFilter_DefaultsLimitExactlyTwo checks the limit cap is
// enforced at MaxQueryLimit (200) with a clean error message.
func TestBuildEventFilter_LimitOutOfRange(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{Limit: MaxPageSize + 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be")

	_, err = BuildEventFilter(EventFilterArgs{Limit: -1})
	require.Error(t, err)
}

// TestBuildEventFilter_TopicConflict enforces the rule that topic and
// any positional topic filter (t0..t3) cannot be combined. This is
// the same rule REST /events?topic=...&topic0=... tests rely on.
func TestBuildEventFilter_TopicConflict(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{
		Topic: json.RawMessage(`"transfer"`),
		T0:    json.RawMessage(`"transfer"`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be combined")
}

// TestBuildEventFilter_BadContractID rejects malformed contract strkeys.
func TestBuildEventFilter_BadContractID(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{ContractID: "not-a-cstrkey"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid contract_id")
}

// TestBuildEventFilter_BadOrder rejects unsupported sort directions.
func TestBuildEventFilter_BadOrder(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{Order: "reverse"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid order")
}

// TestBuildEventFilter_BadOrderBy rejects unsupported sort columns.
func TestBuildEventFilter_BadOrderBy(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{OrderBy: "tx_hash"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid order_by")
}

// TestBuildEventFilter_LedgerRangeInverted rejects fromLedger > toLedger.
func TestBuildEventFilter_LedgerRangeInverted(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{FromLedger: 20, ToLedger: 10})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after")
}

// TestBuildEventFilter_BadCursor rejects cursors with bad chars.
func TestBuildEventFilter_BadCursor(t *testing.T) {
	_, err := BuildEventFilter(EventFilterArgs{Cursor: "has space"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cursor")
}

// TestResolvePage_DefaultSize returns DefaultPageSize for zero-valued
// first, matching REST "no ?limit=" behavior.
func TestResolvePage_DefaultSize(t *testing.T) {
	limit, _, order, orderBy, err := ResolvePage(PageArgs{})
	require.NoError(t, err)
	assert.Equal(t, DefaultPageSize, limit)
	assert.Equal(t, "", order)
	assert.Equal(t, "", orderBy)
}

// TestResolvePage_RejectsBackward ensures the spec'd "no backward
// pagination" surface is shared between GraphQL and any future REST
// caller of ResolvePage.
func TestResolvePage_RejectsBackward(t *testing.T) {
	_, _, _, _, err := ResolvePage(PageArgs{Last: 5})
	assert.Error(t, err)
	_, _, _, _, err = ResolvePage(PageArgs{Before: "abc"})
	assert.Error(t, err)
}

// TestResolvePage_OutOfRangeLimit caps first at MaxPageSize.
func TestResolvePage_OutOfRangeLimit(t *testing.T) {
	_, _, _, _, err := ResolvePage(PageArgs{First: MaxPageSize + 1})
	assert.Error(t, err)
}

// TestParseTopic_AutoQuote verifies bare words become JSON strings
// (matches REST ?topic=transfer behaviour).
func TestParseTopic_AutoQuote(t *testing.T) {
	got, err := ParseTopic("transfer")
	require.NoError(t, err)
	assert.JSONEq(t, `"transfer"`, string(got))

	got, err = ParseTopic(`{"symbol":"transfer"}`)
	require.NoError(t, err)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(got))
}

// TestParseTopicContains_RejectsNonJSON mirrors REST's strict JSON
// requirement on this filter.
func TestParseTopicContains_RejectsNonJSON(t *testing.T) {
	_, err := ParseTopicContains("transfer")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valid JSON")
}

// TestParseLedgerParam_ZeroOnEmpty ensures empty input is a no-op.
func TestParseLedgerParam_ZeroOnEmpty(t *testing.T) {
	got, err := ParseLedgerParam("")
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)
}

// TestParseTimeParam_RejectsSubSecond ensures RFC3339 precision
// matches what REST enforces.
func TestParseTimeParam_RejectsSubSecond(t *testing.T) {
	_, err := ParseTimeParam("2026-07-21T00:00:00.123Z")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sub-second")
}
