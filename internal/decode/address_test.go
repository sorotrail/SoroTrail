package decode

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// test addresses — one G... (account) and one C... (contract).
const (
	addrG = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"
	addrC = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
)

func TestIsAddress(t *testing.T) {
	assert.True(t, isAddress(addrG), "G... address")
	assert.True(t, isAddress(addrC), "C... address")
	assert.False(t, isAddress(""), "empty")
	assert.False(t, isAddress("short"), "too short")
	assert.False(t, isAddress("XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), "wrong prefix")
	assert.False(t, isAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), "too short (54 A's)")
	assert.False(t, isAddress(addrG[:55]+"a"), "lowercase suffix is not base32")
	assert.False(t, isAddress(addrG[:55]+"1"), "digit 1 is not base32 (base32 uses 2-7)")
	assert.True(t, isAddress("GBADG5ABUAKSLCJ4T6TYCCJ73ZVFSIKF6J2QHCM6R5UKDNSYU4MF4YD2"), "valid G address")
	// Base32 digit
	// GC... address with only A-Z and 2-7, exactly 56 chars
	assert.True(t, isAddress("GCK3WQK3Y22GT2R6FQK7VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCY"), "valid C with base32 digits")
}

func canonicalJSON(t *testing.T, raw string) json.RawMessage {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal([]byte(raw), &v))
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return json.RawMessage(b)
}

func TestExtractAddresses(t *testing.T) {
	tests := []struct {
		name     string
		topics   string // JSON array
		value    string // JSON value
		want     []AddressRef
		wantOnly bool // if true, check that we get exactly want (no extras)
	}{
		{
			name:   "empty",
			topics: "[]",
			value:  "null",
			want:   nil,
		},
		{
			name:   "address in topic[0]",
			topics: `["` + addrG + `"]`,
			value:  "null",
			want: []AddressRef{
				{Address: addrG, Role: "topic[0]"},
			},
		},
		{
			name:   "address in topic[1] and value",
			topics: `["symbol", "` + addrC + `"]`,
			value:  `"` + addrG + `"`,
			want: []AddressRef{
				{Address: addrC, Role: "topic[1]"},
				{Address: addrG, Role: "value"},
			},
		},
		{
			name:   "address in nested object value",
			topics: `["symbol"]`,
			value:  `{"from":"` + addrG + `","to":"` + addrC + `","amount":"1000"}`,
			want: []AddressRef{
				{Address: addrG, Role: "value"},
				{Address: addrC, Role: "value"},
			},
		},
		{
			name:   "address in nested array value",
			topics: `["symbol"]`,
			value:  `["` + addrG + `","` + addrC + `"]`,
			want: []AddressRef{
				{Address: addrG, Role: "value"},
				{Address: addrC, Role: "value"},
			},
		},
		{
			name:   "same address in topic and value produces two roles",
			topics: `["` + addrG + `"]`,
			value:  `"` + addrG + `"`,
			want: []AddressRef{
				{Address: addrG, Role: "topic[0]"},
				{Address: addrG, Role: "value"},
			},
		},
		{
			name:   "deduplicate same address same role",
			topics: `["` + addrG + `","` + addrG + `"]`,
			value:  "null",
			want: []AddressRef{
				{Address: addrG, Role: "topic[0]"},
			},
		},
		{
			name:   "ignore unknown fallback",
			topics: `["symbol"]`,
			value:  `{"unknown":"` + addrG + `"}`,
			want:   nil,
		},
		{
			name:   "ignore non-address strings",
			topics: `["symbol","transfer","1000"]`,
			value:  `{"name":"token","decimals":7}`,
			want:   nil,
		},
		{
			name:   "address in nested map field",
			topics: `["symbol"]`,
			value:  `{"map":[{"key":"address","val":"` + addrG + `"},{"key":"amount","val":"500"}]}`,
			want: []AddressRef{
				{Address: addrG, Role: "value"},
			},
		},
		{
			name:   "multiple distinct addresses",
			topics: `["` + addrG + `","` + addrC + `"]`,
			value:  `["` + addrG + `","GBADG5ABUAKSLCJ4T6TYCCJ73ZVFSIKF6J2QHCM6R5UKDNSYU4MF4YD2"]`,
			want: []AddressRef{
				{Address: addrG, Role: "topic[0]"},
				{Address: addrC, Role: "topic[1]"},
				{Address: "GBADG5ABUAKSLCJ4T6TYCCJ73ZVFSIKF6J2QHCM6R5UKDNSYU4MF4YD2", Role: "value"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topics := canonicalJSON(t, tt.topics)
			value := canonicalJSON(t, tt.value)
			got := ExtractAddresses(topics, value)
			if len(tt.want) == 0 {
				assert.Empty(t, got, "expected no addresses")
				return
			}
			if tt.wantOnly {
				assert.ElementsMatch(t, tt.want, got)
				return
			}
			// By default verify that each wanted address appears at least once.
			for _, w := range tt.want {
				found := false
				for _, g := range got {
					if g.Address == w.Address && g.Role == w.Role {
						found = true
						break
					}
				}
				assert.True(t, found, "expected address %s with role %s", w.Address, w.Role)
			}
			// Dedupe: no two entries with same (address, role).
			seen := map[[2]string]bool{}
			for _, g := range got {
				key := [2]string{g.Address, g.Role}
				assert.False(t, seen[key], "duplicate (address, role) pair: %s %s", g.Address, g.Role)
				seen[key] = true
			}
		})
	}
}

func TestFormatTopicRole(t *testing.T) {
	assert.Equal(t, "topic[0]", formatTopicRole(0))
	assert.Equal(t, "topic[1]", formatTopicRole(1))
	assert.Equal(t, "topic[42]", formatTopicRole(42))
}
