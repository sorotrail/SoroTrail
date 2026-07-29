package sep41

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonStr is a tiny helper that turns a Go literal into the raw JSON the
// decoder emits — it keeps the test table readable while letting us assert
// on byte-exact match for the marshal path.
func jsonStr(s string) json.RawMessage {
	return json.RawMessage(s)
}

// decode calls Decode under test and also verifies the JSON marshal
// path round-trips through Normalize unchanged for positive cases.
func decode(t *testing.T, topics, value string) *Normalized {
	t.Helper()
	n := Decode(jsonStr(topics), jsonStr(value))
	if n == nil {
		return nil
	}
	// For positive cases, Normalize and the manual Decode result should
	// produce the exact same JSON. This blocks accidental field drift
	// between the typed struct and the wire shape.
	b, err := json.Marshal(n)
	require.NoError(t, err)
	assert.JSONEq(t, string(Normalize(jsonStr(topics), jsonStr(value))), string(b),
		"Normalize and Decode+Marshal must agree on the JSON shape")
	return n
}

func TestDecode_Transfer(t *testing.T) {
	tests := []struct {
		name          string
		topics        string
		value         string
		wantAmount    string
		wantToMuxedID *uint64
		wantAsset     string
	}{
		{
			"basic G->G i128",
			`[{"symbol":"transfer"},{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"},{"address":"GBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}]`,
			`{"i128":"10000000"}`,
			"10000000", nil, "",
		},
		{
			"negative i128 preserved verbatim",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"i128":"-42"}`,
			"-42", nil, "",
		},
		{
			"contract addresses (C...) preserved as-is",
			`[{"symbol":"transfer"},{"address":"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3"},{"address":"CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"}]`,
			`{"i128":"1"}`,
			"1", nil, "",
		},
		{
			"CAP-0067 trailing asset string captured",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"},{"string":"native"}]`,
			`{"i128":"10"}`,
			"10", nil, "native",
		},
		{
			"CAP-0067 trailing issued asset captured",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"},{"string":"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"}]`,
			`{"i128":"10"}`,
			"10", nil, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
		},
		{
			"CAP-0067 muxed data with to_muxed_id",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"map":[
				{"key":{"symbol":"amount"},"val":{"i128":"7"}},
				{"key":{"symbol":"to_muxed_id"},"val":{"u64":12345}}
			]}`,
			"7", ptrU64(12345), "",
		},
		{
			"CAP-0067 muxed data with reversed map order",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"map":[
				{"key":{"symbol":"to_muxed_id"},"val":{"u64":99}},
				{"key":{"symbol":"amount"},"val":{"i128":"500"}}
			]}`,
			"500", ptrU64(99), "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := decode(t, tt.topics, tt.value)
			require.NotNil(t, n, "transfer event must match")
			assert.Equal(t, "sep41", n.Standard)
			assert.Equal(t, "transfer", n.Event)
			assert.Equal(t, tt.wantAmount, n.Amount)
			assert.Equal(t, tt.wantAsset, n.Asset)
			assert.Equal(t, tt.wantToMuxedID, n.ToMuxedID)
			// from/to round-trip from the topic literal — wire shape sanity.
			require.NotEmpty(t, n.From)
			require.NotEmpty(t, n.To)
			assert.Empty(t, n.ExpirationLedger, "transfer must not carry approve-only fields")
		})
	}
}

func TestDecode_MintBurnClawbackApprove(t *testing.T) {
	mint := decode(t,
		`[{"symbol":"mint"},{"address":"GA1"},{"string":"native"}]`,
		`{"i128":"100"}`)
	require.NotNil(t, mint)
	assert.Equal(t, "mint", mint.Event)
	assert.Equal(t, "GA1", mint.To)
	assert.Equal(t, "100", mint.Amount)
	assert.Empty(t, mint.From, "mint must not carry a from field")
	assert.Equal(t, "native", mint.Asset)

	burn := decode(t,
		`[{"symbol":"burn"},{"address":"GA1"}]`,
		`{"i128":"100"}`)
	require.NotNil(t, burn)
	assert.Equal(t, "burn", burn.Event)
	assert.Equal(t, "GA1", burn.From)
	assert.Equal(t, "100", burn.Amount)
	assert.Empty(t, burn.To, "burn must not carry a to field")

	clawback := decode(t,
		`[{"symbol":"clawback"},{"address":"GA1"},{"string":"USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"}]`,
		`{"i128":"9"}`)
	require.NotNil(t, clawback)
	assert.Equal(t, "clawback", clawback.Event)
	assert.Equal(t, "GA1", clawback.From)
	assert.Equal(t, "9", clawback.Amount)
	assert.Equal(t, "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", clawback.Asset)

	approve := decode(t,
		`[{"symbol":"approve"},{"address":"GA1"},{"address":"GA2"}]`,
		`{"vec":[{"i128":"500"},{"u32":1234567}]}`)
	require.NotNil(t, approve)
	assert.Equal(t, "approve", approve.Event)
	assert.Equal(t, "GA1", approve.From)
	assert.Equal(t, "GA2", approve.Spender)
	assert.Equal(t, "500", approve.Amount)
	assert.Equal(t, uint32(1234567), approve.ExpirationLedger)
	assert.Empty(t, approve.Asset, "approve must reject trailing asset")
}

func TestDecode_Negatives(t *testing.T) {
	// Three is the bare minimum the issue asks for; we exercise a few more
	// shape mismatches that look "transfer-ish" but aren't SEP-41.
	tests := []struct {
		name   string
		topics string
		value  string
	}{
		{
			"transfer with too few topics",
			`[{"symbol":"transfer"},{"address":"GA1"}]`,
			`{"i128":"1"}`,
		},
		{
			"transfer with non-address topic[1]",
			`[{"symbol":"transfer"},{"string":"who"},"{"address":"GA2"}]`,
			`{"i128":"1"}`,
		},
		{
			"transfer with i64 amount instead of i128",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"i64":1}`,
		},
		{
			"transfer with map missing amount key",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"map":[{"key":{"symbol":"to_muxed_id"},"val":{"u64":1}}]}`,
		},
		{
			"transfer with map whose muxed id is u32 not u64",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"map":[
				{"key":{"symbol":"amount"},"val":{"i128":"3"}},
				{"key":{"symbol":"to_muxed_id"},"val":{"u32":1}}
			]}`,
		},
		{
			"transfer with null value",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`null`,
		},
		{
			"transfer with stray non-asset trailing string kept",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"},{"string":"memo"}]`,
			`{"i128":"1"}`,
		},
		{
			"mint with non-address topic[1]",
			`[{"symbol":"mint"},{"string":"not-an-address"}]`,
			`{"i128":"1"}`,
		},
		{
			"mint with wrong amount type",
			`[{"symbol":"mint"},{"address":"GA1"}]`,
			`{"u64":1}`,
		},
		{
			"burn with too many topics",
			`[{"symbol":"burn"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"i128":"1"}`,
		},
		{
			"approve with i128 value instead of vec",
			`[{"symbol":"approve"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"i128":"1"}`,
		},
		{
			"approve with trailing asset topic (forbidden in CAP-0067)",
			`[{"symbol":"approve"},{"address":"GA1"},{"address":"GA2"},{"string":"native"}]`,
			`{"vec":[{"i128":"1"},{"u32":7}]}`,
		},
		{
			"unknown symbol",
			`[{"symbol":"swap"},{"address":"GA1"},{"address":"GA2"}]`,
			`{"i128":"1"}`,
		},
		{
			"topic[0] is not a symbol",
			`[{"address":"GA1"},{"address":"GA2"}]`,
			`{"i128":"1"}`,
		},
		{
			"empty topics array",
			`[]`,
			`{"i128":"1"}`,
		},
		{
			"invalid topics JSON",
			`{not-json}`,
			`{"i128":"1"}`,
		},
		{
			"invalid value JSON",
			`[{"symbol":"transfer"},{"address":"GA1"},{"address":"GA2"}]`,
			`{not-json}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := Decode(jsonStr(tt.topics), jsonStr(tt.value))
			assert.Nil(t, n, "non-SEP-41 events must pass through unrecognized (got %+v)", n)
			assert.Nil(t, Normalize(jsonStr(tt.topics), jsonStr(tt.value)),
				"Normalize must return nil for non-matches")
		})
	}
}

func TestDecode_AssetStringRecognition(t *testing.T) {
	// Sanity-check the asset-string helper directly so a future tweak to
	// the matcher doesn't silently widen what we accept. Issuers here are
	// 56 chars: G + 55 alphanumeric, matching Stellar account IDs.
	const g56 = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	var g56b = "G" + strings.Repeat("B", 55) // explicit 56-char literal helper
	cases := []struct {
		s    string
		want bool
	}{
		{"native", true},
		{"USDC:" + g56, true},
		{"XLM:" + g56b, true},
		{"memo", false},
		{":" + g56, false},              // empty code
		{"USDC:" + g56[:55], false},     // short issuer (55)
		{"USDC:" + g56 + "X", false},    // long issuer (57)
		{"verylongcodex:" + g56, false}, // 13-char code (above SEP-0011 limit)
		{"verylongcode:" + g56, true},   // 12-char code is the maximum per SEP-0011
		{"ABCDEFGH9:" + g56, true},      // 9-char alphanumeric code
		{"12345:" + g56, true},          // numeric-only code is alphanumeric
		{"USDC:CA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", false}, // wrong issuer prefix (C...)
		{"USDC:" + g56[:3] + "@" + g56[3:], false},                               // non-alphanumeric in issuer
		{"a:b", false},
		{"", false},
		{"native ", false}, // strict — no whitespace tolerance
	}
	for _, tc := range cases {
		t.Run(strings.ReplaceAll(strings.ReplaceAll(tc.s, ":", "_"), " ", "_"), func(t *testing.T) {
			assert.Equal(t, tc.want, looksLikeAsset(tc.s))
		})
	}
}

func TestDecode_Normalize_ReturnsNilForInvalidJSON(t *testing.T) {
	// Defensive: malformed inputs must not panic and must report nil so
	// the API layer can rely on the same null check that distinguishes
	// non-matches.
	assert.Nil(t, Normalize(json.RawMessage(`{`), json.RawMessage(`null`)))
	assert.Nil(t, Normalize(json.RawMessage(`null`), json.RawMessage(`null`)))
}

func ptrU64(v uint64) *uint64 { return &v }
