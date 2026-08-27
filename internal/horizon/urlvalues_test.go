package horizon

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestURLValues pins the query string Horizon requests are built from. A
// dropped or reordered parameter changes which records come back, and the
// caller pastes the result straight after "?" — so what is asserted here is
// the exact string, not a parsed approximation of it.
func TestURLValues(t *testing.T) {
	tests := []struct {
		name          string
		cursor        string
		limit         int
		includeFailed bool
		want          string
	}{
		{"no cursor, failures excluded", "", 200, false, "limit=200&order=asc&include_failed=false"},
		{"no cursor, failures included", "", 200, true, "limit=200&order=asc&include_failed=true"},
		{"with cursor", "12884905984-1", 200, false, "limit=200&order=asc&include_failed=false&cursor=12884905984-1"},
		{"cursor and failures", "12884905984-1", 50, true, "limit=50&order=asc&include_failed=true&cursor=12884905984-1"},
		{"limit of one", "", 1, false, "limit=1&order=asc&include_failed=false"},
		{"limit of zero", "", 0, false, "limit=0&order=asc&include_failed=false"},
		{"multi-digit limit", "", 137, false, "limit=137&order=asc&include_failed=false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, urlValues(tt.cursor, tt.limit, tt.includeFailed))
		})
	}
}

// TestURLValuesAlwaysRequestsAscendingOrder pins the one parameter that is not
// a function of any argument.
//
// Ordering is not cosmetic here: the ingester walks pages forward from a
// cursor, so a descending page would hand it the wrong end of the range and
// the cursor would advance past records it never saw.
func TestURLValuesAlwaysRequestsAscendingOrder(t *testing.T) {
	for _, q := range []string{
		urlValues("", 200, false),
		urlValues("cur", 1, true),
		urlValues("cur", 0, false),
	} {
		assert.Contains(t, q, "order=asc")
		assert.NotContains(t, q, "order=desc")
	}
}

// TestURLValuesOmitsAnEmptyCursor guards a real difference: Horizon reads
// "cursor=" as a request from the start, which is the same as omitting it, but
// only by coincidence of this endpoint. Omission is what the code intends.
func TestURLValuesOmitsAnEmptyCursor(t *testing.T) {
	q := urlValues("", 200, false)

	assert.NotContains(t, q, "cursor")
}

// TestURLValuesParsesAsAQueryString checks the result is well-formed as well as
// exact — the caller concatenates it after "?" with no further escaping.
func TestURLValuesParsesAsAQueryString(t *testing.T) {
	values, err := url.ParseQuery(urlValues("12884905984-1", 137, true))
	require.NoError(t, err)

	assert.Equal(t, "137", values.Get("limit"))
	assert.Equal(t, "asc", values.Get("order"))
	assert.Equal(t, "true", values.Get("include_failed"))
	assert.Equal(t, "12884905984-1", values.Get("cursor"))
	assert.Len(t, values, 4)
}

// TestURLValuesDoesNotEscapeTheCursor documents current behaviour rather than
// endorsing it.
//
// The cursor is written into the query verbatim. Horizon paging tokens are
// digits and hyphens, so this is safe for every cursor the client actually
// receives — it is `resp.Links.Next` fed back in, never user input. But the
// parameter is documented as opaque, and an opaque token containing "&" or "="
// would inject parameters rather than be carried as a value.
//
// Pinned here so that if the encoding is ever added, this test fails and the
// change is deliberate rather than incidental.
func TestURLValuesDoesNotEscapeTheCursor(t *testing.T) {
	q := urlValues("a&limit=1", 200, false)

	assert.Equal(t, "limit=200&order=asc&include_failed=false&cursor=a&limit=1", q)

	// The consequence, stated as a fact rather than left implicit: parsed back,
	// "limit" now has two values and the injected one is not last.
	values, err := url.ParseQuery(q)
	require.NoError(t, err)
	assert.Equal(t, []string{"200", "1"}, values["limit"])
}

// TestURLValuesParameterOrderIsStable keeps the assertions above meaningful:
// they are exact-string comparisons, so the order they encode is part of what
// is pinned.
func TestURLValuesParameterOrderIsStable(t *testing.T) {
	q := urlValues("cur", 200, true)

	assert.Less(t, strings.Index(q, "limit="), strings.Index(q, "order="))
	assert.Less(t, strings.Index(q, "order="), strings.Index(q, "include_failed="))
	assert.Less(t, strings.Index(q, "include_failed="), strings.Index(q, "cursor="))
}
