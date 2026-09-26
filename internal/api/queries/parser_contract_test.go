package queries

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shared layer's promise is one parser per parameter type, so a handler
// must be able to trust a parser it delegates to instead of re-testing it.
// queries_test.go covers BuildEventFilter and ResolvePage thoroughly; each
// parser is exercised there only for the behaviour that happened to be added
// when it was needed. ParseTypes has no direct test at all.
//
// This file is the completeness pass: one table per parser, covering every
// class of input the parser distinguishes. Some rows repeat a case an existing
// test already makes — that is deliberate, so this reads as the whole contract
// for each parser rather than a diff against three other files.

func TestParseTypes_Table(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr string
	}{
		{name: "absent means no constraint", raw: "", want: nil},
		{name: "one accepted value", raw: "contract", want: []string{"contract"}},
		{name: "every accepted value", raw: "contract,system,diagnostic", want: []string{"contract", "system", "diagnostic"}},
		{name: "spaces around elements are trimmed", raw: " contract , diagnostic ", want: []string{"contract", "diagnostic"}},
		{name: "empty elements are dropped", raw: "contract,,system,", want: []string{"contract", "system"}},
		{name: "a separator alone is still no constraint", raw: ",", want: nil},
		{name: "a whitespace-only value is no constraint", raw: " ", want: nil},
		// Duplicates are carried through rather than de-duplicated: the filter
		// is an OR over the list, so a repeat changes nothing semantically and
		// normalising it here would be a surprise at the other end.
		{name: "duplicates are kept in order", raw: "contract,system,contract", want: []string{"contract", "system", "contract"}},
		{name: "an unknown value names itself", raw: "bogus",
			wantErr: `invalid type "bogus" (want contract|system|diagnostic)`},
		{name: "case is significant", raw: "Contract", wantErr: `invalid type "Contract" (want contract|system|diagnostic)`},
		{name: "one bad element rejects the whole list", raw: "contract,bogus", wantErr: `invalid type "bogus" (want contract|system|diagnostic)`},
		{name: "a numeric type is not a type", raw: "1", wantErr: `invalid type "1" (want contract|system|diagnostic)`},
		{name: "the plural spelling is not a type", raw: "types", wantErr: `invalid type "types" (want contract|system|diagnostic)`},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTypes(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				assert.Nil(t, got, "a rejected list must not hand back a partial one")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseLedgerParam_Table(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr string
	}{
		{name: "absent is zero, which the builder reads as no constraint", raw: "", want: 0},
		{name: "one is the smallest accepted ledger", raw: "1", want: 1},
		{name: "leading zeros are just zeros", raw: "007", want: 7},
		// ParseInt accepts an explicit sign, so "+7" is seven here even though
		// the same text on a URL is a space by the time a handler sees it. The
		// REST side pins that second behaviour.
		{name: "a leading plus is accepted", raw: "+7", want: 7},
		{name: "the largest int64 is accepted", raw: "9223372036854775807", want: 9223372036854775807},
		{name: "zero is not a ledger", raw: "0", wantErr: "must be a positive integer"},
		{name: "negative zero is not a ledger either", raw: "-0", wantErr: "must be a positive integer"},
		{name: "a negative ledger", raw: "-5", wantErr: "must be a positive integer"},
		{name: "not a number", raw: "abc", wantErr: "must be a positive integer"},
		{name: "a decimal ledger", raw: "1.5", wantErr: "must be a positive integer"},
		{name: "out of int64 range is a bad input, not an overflow", raw: "9223372036854775808",
			wantErr: "must be a positive integer"},
		{name: "surrounding spaces are not trimmed", raw: " 7 ", wantErr: "must be a positive integer"},
		{name: "hexadecimal is not a ledger", raw: "0x10", wantErr: "must be a positive integer"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseLedgerParam(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				assert.Zero(t, got, "a rejected value must not be forwarded as a constraint")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseTimeParam_Table(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    time.Time
		wantErr string
	}{
		{name: "absent is the zero time, which the builder reads as no constraint", raw: "", want: time.Time{}},
		{name: "UTC at second precision", raw: "2026-07-21T00:00:00Z",
			want: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)},
		// The parser takes any RFC 3339 offset, so a caller comparing two bounds
		// has to remember they may be written in different zones.
		{name: "a positive offset is accepted", raw: "2026-07-21T02:00:00+02:00",
			want: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)},
		{name: "a fractional second of exactly zero passes the precision check",
			raw: "2026-07-21T00:00:00.000Z", want: time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)},
		{name: "a date with no time is not a timestamp", raw: "2026-07-21",
			wantErr: "must be an RFC 3339 timestamp"},
		{name: "a missing zone is not a timestamp", raw: "2026-07-21T00:00:00",
			wantErr: "must be an RFC 3339 timestamp"},
		{name: "free text", raw: "yesterday", wantErr: "must be an RFC 3339 timestamp"},
		{name: "a trailing space is not trimmed", raw: "2026-07-21T00:00:00Z ",
			wantErr: "must be an RFC 3339 timestamp"},
		{name: "sub-second precision is refused", raw: "2026-07-21T00:00:00.123Z",
			wantErr: "sub-second precision is not supported"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimeParam(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.True(t, got.IsZero(), "a rejected timestamp must not be forwarded as a constraint")
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got), "want %s, got %s", tt.want.String(), got.String())
		})
	}
}

func TestParseTopicFamily_Table(t *testing.T) {
	// ParseTopic and ParseTopicContains differ in exactly one way — one
	// auto-quotes a bare word, the other requires JSON — so their tables sit
	// side by side to keep that difference visible.
	topic, err := ParseTopic("transfer")
	require.NoError(t, err)
	assert.JSONEq(t, `"transfer"`, string(topic))

	topicTests := []struct {
		name     string
		raw      string
		wantJSON string // empty means the result must be nil
		wantErr  string
	}{
		{name: "absent is no constraint", raw: "", wantJSON: ""},
		{name: "whitespace only is no constraint", raw: "   ", wantJSON: ""},
		{name: "a bare word becomes a JSON string", raw: "transfer", wantJSON: `"transfer"`},
		{name: "valid JSON passes through", raw: `{"symbol":"USDC"}`, wantJSON: `{"symbol":"USDC"}`},
		{name: "a JSON number stays a number", raw: "42", wantJSON: `42`},
		{name: "true is a JSON value, not a word", raw: "true", wantJSON: `true`},
		{name: "a quoted string is already JSON", raw: `"transfer"`, wantJSON: `"transfer"`},
		{name: "malformed JSON is refused rather than quoted", raw: `{"symbol":`, wantErr: "topic must be valid JSON"},
		{name: "a truncated array", raw: "[1,2", wantErr: "topic must be valid JSON"},
	}
	for _, tt := range topicTests {
		tt := tt
		t.Run("topic/"+tt.name, func(t *testing.T) {
			got, err := ParseTopic(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if tt.wantJSON == "" {
				assert.Nil(t, got, "an absent topic must stay absent rather than become an empty value")
				return
			}
			assert.JSONEq(t, tt.wantJSON, string(got))
		})
	}

	containsTests := []struct {
		name     string
		raw      string
		wantJSON string
		wantErr  string
	}{
		{name: "absent is no constraint", raw: "", wantJSON: ""},
		// The asymmetry with ParseTopic: no auto-quoting, so the bare word that
		// means "substring match" to a caller is a rejected input.
		{name: "a bare word is refused", raw: "transfer", wantErr: "topic_contains must be valid JSON"},
		{name: "a JSON string is accepted", raw: `"transfer"`, wantJSON: `"transfer"`},
		{name: "a JSON array is accepted", raw: `["a","b"]`, wantJSON: `["a","b"]`},
		{name: "malformed JSON is refused", raw: `[`, wantErr: "topic_contains must be valid JSON"},
	}
	for _, tt := range containsTests {
		tt := tt
		t.Run("topic_contains/"+tt.name, func(t *testing.T) {
			got, err := ParseTopicContains(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if tt.wantJSON == "" {
				assert.Nil(t, got)
				return
			}
			assert.JSONEq(t, tt.wantJSON, string(got))
		})
	}
}

// TestParsersEitherNameTheParameterOrLeaveItToTheCaller pins the half of the
// contract the REST handlers depend on when they compose a response body.
// queries.ParseLedgerParam and ParseTimeParam return an unprefixed sentence
// because one function serves from_/to_ for both, so the caller has to say
// which parameter failed. The value-parsing helpers name their own parameter,
// so prefixing them again would print "topic_contains: topic_contains must be
// valid JSON".
//
// A parser that silently started naming (or un-naming) itself would make every
// endpoint that calls it emit a doubled or nameless parameter, which is the
// "identical error messages for the same class of invalid input" property the
// API contract promises. The other half of that property — that handlers
// actually relay these messages rather than restating them — is pinned in
// internal/api/param_validation_test.go.
func TestParsersEitherNameTheParameterOrLeaveItToTheCaller(t *testing.T) {
	t.Run("unprefixed parsers must not name a parameter", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			calls func() error
		}{
			{"ParseLedgerParam", func() error { _, err := ParseLedgerParam("0"); return err }},
			{"ParseTimeParam", func() error { _, err := ParseTimeParam("nope"); return err }},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				err := tc.calls()
				require.Error(t, err)
				for _, param := range []string{"from_ledger", "to_ledger", "from_time", "to_time", "ledger "} {
					assert.NotContainsf(t, err.Error(), param,
						"%s now names the parameter itself, so every caller's prefix doubles it", tc.name)
				}
			})
		}
	})

	t.Run("self-naming parsers must name the right parameter", func(t *testing.T) {
		for _, tc := range []struct {
			name, want string
			calls      func() error
		}{
			{"ParseTypes", `invalid type "bogus"`, func() error { _, err := ParseTypes("bogus"); return err }},
			{"ParseTopic", "topic must be valid JSON", func() error { _, err := ParseTopic("{"); return err }},
			{"ParseTopicContains", "topic_contains must be valid JSON", func() error {
				_, err := ParseTopicContains("transfer")
				return err
			}},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				err := tc.calls()
				require.Error(t, err)
				assert.Containsf(t, err.Error(), tc.want,
					"%s's message no longer names its own parameter, so the caller has to", tc.name)
			})
		}
	})
}
