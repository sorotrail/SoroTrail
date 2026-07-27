package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// stubEnricher implements Enricher for tests. It marks events as decoded
// without actually looking up contract specs.
type stubEnricher struct{}

func (e *stubEnricher) EnrichEvents(_ context.Context, events []store.Event) []store.EnrichedEvent {
	if len(events) == 0 {
		return nil
	}
	return []store.EnrichedEvent{{
		Event:   events[0],
		Decoded: true,
	}}
}

// doGetWithHeader is doGet plus a header for conditional requests. The
// 304 tests use this so the If-None-Match setup reads naturally without
// the caller constructing http.Request by hand.
func doGetWithHeader(t *testing.T, s *Server, path, header, value string) (*http.Response, []byte) {
	t.Helper()
	srv := httptest.NewServer(s.Router())
	defer srv.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	require.NoError(t, err)
	if header != "" {
		req.Header.Set(header, value)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return resp, body
}

// assertImmutable asserts that a response carries the immutable-cache
// header set: strong ETag (when expected), Vary: Accept-Encoding,
// Cache-Control: public + max-age + immutable.
func assertImmutable(t *testing.T, resp *http.Response, wantETag string) {
	t.Helper()
	assert.Equal(t, resp.Header.Get("Vary"), "Accept-Encoding", "Vary must include Accept-Encoding for future compression interplay")
	cc := resp.Header.Get("Cache-Control")
	assert.True(t, strings.HasPrefix(cc, "public, max-age="), "Cache-Control must start with 'public, max-age=', got %q", cc)
	assert.True(t, strings.HasSuffix(cc, ", immutable"), "Cache-Control must declare immutable, got %q", cc)
	if wantETag != "" {
		assert.Equal(t, wantETag, resp.Header.Get("ETag"))
	}
}

// TestGetEvent_StrongETag: GET /events/{id} returns the strong-ETag
// header set, with Cache-Control: public, max-age=..., immutable.
func TestGetEvent_StrongETag(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{event: store.Event{ID: id, Ledger: 100}}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events/"+id)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assertImmutable(t, resp, `"`+id+`"`)
}

// TestGetEvent_IfNoneMatch_Hit: a conditional GET with the matching
// validator returns 304, an empty body, and the same cache headers.
// The handler must NOT load the full row — it short-circuits to the
// cheap EventExists probe.
func TestGetEvent_IfNoneMatch_Hit(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{exists: true}
	s := newTestServer(st, nil)

	resp, body := doGetWithHeader(t, s, "/events/"+id, "If-None-Match", `"`+id+`"`)

	require.Equal(t, http.StatusNotModified, resp.StatusCode)
	assert.Empty(t, string(body), "304 must not carry a body")
	assertImmutable(t, resp, `"`+id+`"`)
	assert.Equal(t, 1, st.existsCalls, "the 304 path must use EventExists, not GetEvent")
	assert.Equal(t, id, st.lastExistsID)
}

// TestGetEvent_IfNoneMatch_Wildcard: If-None-Match: * matches any
// existing resource (RFC 7232 §3.2). Used by clients that don't know
// the specific validator.
func TestGetEvent_IfNoneMatch_Wildcard(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{exists: true}
	s := newTestServer(st, nil)

	resp, _ := doGetWithHeader(t, s, "/events/"+id, "If-None-Match", "*")
	assert.Equal(t, http.StatusNotModified, resp.StatusCode)
	assert.Equal(t, 1, st.existsCalls)
}

// TestGetEvent_IfNoneMatch_Evicted: the cached event was deleted out
// from under the cache by retention/pruning (#8). The validator still
// matches the ID, so 304 must NOT be returned — we have to surface the
// 404 to the client. EventExists is the safety net that catches it.
func TestGetEvent_IfNoneMatch_Evicted(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{exists: false} // retention/pruning deleted it
	s := newTestServer(st, nil)

	resp, body := doGetWithHeader(t, s, "/events/"+id, "If-None-Match", `"`+id+`"`)

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(body), id)
	assert.Equal(t, 1, st.existsCalls, "must probe existence, not load the row")
}

// TestGetEvent_IfNoneMatch_Mismatch: the client's validator is stale or
// from a different resource. We must serve the current body (200) and
// emit the latest ETag so the client can re-validate next time.
func TestGetEvent_IfNoneMatch_Mismatch(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{event: store.Event{ID: id, Ledger: 100}}
	s := newTestServer(st, nil)

	resp, _ := doGetWithHeader(t, s, "/events/"+id, "If-None-Match", `"a-different-id"`)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	// ETag matches the resource we're actually serving, not the
	// client-supplied (mismatched) one.
	assert.Equal(t, `"`+id+`"`, resp.Header.Get("ETag"))
	assert.Equal(t, 0, st.existsCalls, "mismatch must go through GetEvent, not the cheap existence probe")
}

// TestListEvents_OpenEnded: a list page with no upper-ledger bound
// cannot be cached. The policy must say no-cache and OMIT the ETag so
// caches don't try to byte-compare.
func TestListEvents_OpenEnded(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}, {ID: "e2"}},
		ingestion: store.IngestionState{LastIngestedLedger: 1000},
	}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Accept-Encoding", resp.Header.Get("Vary"))
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	assert.Empty(t, resp.Header.Get("ETag"), "no-cache responses must not carry an ETag")
}

// TestListEvents_AboveFrontier: to_ledger is past the frontier. The
// page can still grow, so no-cache (same rationale as open-ended).
func TestListEvents_AboveFrontier(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}},
		ingestion: store.IngestionState{LastIngestedLedger: 500},
	}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?to_ledger=1000")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	assert.Empty(t, resp.Header.Get("ETag"))
}

// TestListEvents_AtFrontier: to_ledger == last_ingested_ledger.
// Equal-to-frontier is deliberately NOT cacheable: the boundary
// ledger may be partially ingested in a window from the perspective
// of a stale replica, and any error here lets staleness into the
// cache. The spec says "when in doubt, don't cache".
func TestListEvents_AtFrontier(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}},
		ingestion: store.IngestionState{LastIngestedLedger: 100},
	}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?to_ledger=100")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"),
		"to_ledger equal to frontier must be treated as still-growing")
	assert.Empty(t, resp.Header.Get("ETag"))
}

// TestListEvents_BelowFrontier: to_ledger is strictly below the last
// ingested ledger. The query cannot gain rows, so the page is
// immutable and gets the strong-validator + long max-age set.
func TestListEvents_BelowFrontier(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}},
		ingestion: store.IngestionState{LastIngestedLedger: 1000},
	}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?to_ledger=999&from_ledger=500")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	etag := resp.Header.Get("ETag")
	assert.NotEmpty(t, etag, "immutable page must carry an ETag")
	assertImmutable(t, resp, etag)
	// Two identical filters must produce identical ETags (strong
	// validator: same filter → same body, same hash).
	resp2, _ := doGet(t, s, "/events?to_ledger=999&from_ledger=500")
	assert.Equal(t, etag, resp2.Header.Get("ETag"), "ETag must be stable for an immutable filter")

	// A different filter gets a distinct ETag.
	resp3, _ := doGet(t, s, "/events?to_ledger=998")
	assert.NotEqual(t, etag, resp3.Header.Get("ETag"))
}

// TestListEvents_Immutable304: conditional GET with the right ETag on
// an immutable page returns 304 without touching the events table.
// LastFilter stays empty because QueryEvents is not called.
func TestListEvents_Immutable304(t *testing.T) {
	st := &stubStore{
		ingestion: store.IngestionState{LastIngestedLedger: 1000},
	}
	s := newTestServer(st, nil)

	// Warm the cache: first call computes the ETag.
	resp1, _ := doGet(t, s, "/events?to_ledger=999")
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	// Reset the tracking field so we can prove the 304 path skipped
	// the actual query.
	st.lastFilter = store.EventFilter{Limit: -1} // sentinel

	resp2, body := doGetWithHeader(t, s, "/events?to_ledger=999", "If-None-Match", resp1.Header.Get("ETag"))
	require.Equal(t, http.StatusNotModified, resp2.StatusCode)
	assert.Empty(t, string(body))
	assertImmutable(t, resp2, resp1.Header.Get("ETag"))
	assert.Equal(t, -1, st.lastFilter.Limit, "304 must skip QueryEvents entirely")
}

// TestListEvents_ColdStart: a freshly-created database has no
// ingestion_state row. frontier=0 → no filter can be strictly below
// → every page is no-cache. Conservative on the safe side.
func TestListEvents_ColdStart(t *testing.T) {
	st := &stubStore{
		events:       []store.Event{},
		ingestionErr: store.ErrNotFound,
	}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?to_ledger=1")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"),
		"cold-start must never declare an immutable page")
	assert.Empty(t, resp.Header.Get("ETag"))
}

// otherContract is a second shape-valid (56 chars, C-prefix, base32
// alphabet) Soroban contract strkey so cross-contract ETag
// distinctness can be asserted end-to-end without triggering
// ValidContractID's 400 path. The actual on-chain checksum is not
// relevant to this test.
const otherContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSD"

// TestContractEvents_ImmutableBelowFrontier: /contracts/{id}/events
// shares the list policy. The contract_id is part of the filter hash
// so different contracts produce different ETags.
func TestContractEvents_ImmutableBelowFrontier(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}},
		ingestion: store.IngestionState{LastIngestedLedger: 1000},
	}
	s := newTestServer(st, nil)

	respA, _ := doGet(t, s, "/contracts/"+testContract+"/events?to_ledger=999")
	require.Equal(t, http.StatusOK, respA.StatusCode)
	assertImmutable(t, respA, respA.Header.Get("ETag"))

	respB, _ := doGet(t, s, "/contracts/"+otherContract+"/events?to_ledger=999")
	require.Equal(t, http.StatusOK, respB.StatusCode)
	assert.NotEqual(t, respA.Header.Get("ETag"), respB.Header.Get("ETag"),
		"different contracts must produce different ETags; a collision would let one contract serve another's cached response")
}

// TestHealth_NoStore: /health exposes operational state and must
// never be cached (CDN/proxy could otherwise mask outages for hours).
func TestHealth_NoStore(t *testing.T) {
	resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/health")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.Empty(t, resp.Header.Get("ETag"), "/health must not carry an ETag")
}

// TestStats_NoStore: /stats mutates on every ingest cycle and would
// be otherwise be cached by anything that saw a healthy response.
func TestStats_NoStore(t *testing.T) {
	st := &stubStore{stats: store.Stats{TotalEvents: 1, LastIngestedLedger: 5}}
	resp, _ := doGet(t, newTestServer(st, nil), "/stats")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.Empty(t, resp.Header.Get("ETag"))
}

// TestCDN_VaryHeader: the Vary header is what makes a shared cache
// keep distinct variants (Accept-Encoding for future compression
// interplay). The spec calls this out explicitly: get the header set
// right together. Test it on every cacheable shape.
func TestCDN_VaryHeader(t *testing.T) {
	const id = "0001099511627776-0000000001"

	t.Run("immutable event", func(t *testing.T) {
		st := &stubStore{event: store.Event{ID: id}}
		resp, _ := doGet(t, newTestServer(st, nil), "/events/"+id)
		assert.Equal(t, "Accept-Encoding", resp.Header.Get("Vary"))
	})
	t.Run("immutable list", func(t *testing.T) {
		st := &stubStore{
			events:    []store.Event{{ID: "e1"}},
			ingestion: store.IngestionState{LastIngestedLedger: 1000},
		}
		resp, _ := doGet(t, newTestServer(st, nil), "/events?to_ledger=999")
		assert.Equal(t, "Accept-Encoding", resp.Header.Get("Vary"))
	})
	t.Run("304 event", func(t *testing.T) {
		st := &stubStore{exists: true}
		resp, _ := doGetWithHeader(t, newTestServer(st, nil), "/events/"+id, "If-None-Match", `"`+id+`"`)
		assert.Equal(t, http.StatusNotModified, resp.StatusCode)
		assert.Equal(t, "Accept-Encoding", resp.Header.Get("Vary"))
	})
}

// TestCachePrivate: when CACHE_PRIVATE is on, the Cache-Control
// directive flips from `public` to `private` so shared caches can't
// pool responses across authenticated users. Auth (#17) not yet
// merged — this flag is the documented escape hatch.
func TestCachePrivate(t *testing.T) {
	SetCachePrivate(true)
	t.Cleanup(func() { SetCachePrivate(false) })

	const id = "0001099511627776-0000000001"
	t.Run("event flips to private", func(t *testing.T) {
		st := &stubStore{event: store.Event{ID: id}}
		resp, _ := doGet(t, newTestServer(st, nil), "/events/"+id)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		cc := resp.Header.Get("Cache-Control")
		assert.True(t, strings.HasPrefix(cc, "private, max-age="),
			"CACHE_PRIVATE must promote public→private, got %q", cc)
		assert.True(t, strings.HasSuffix(cc, ", immutable"))
	})
	t.Run("list page flips to private", func(t *testing.T) {
		st := &stubStore{
			events:    []store.Event{{ID: "e1"}},
			ingestion: store.IngestionState{LastIngestedLedger: 1000},
		}
		resp, _ := doGet(t, newTestServer(st, nil), "/events?to_ledger=999")
		cc := resp.Header.Get("Cache-Control")
		assert.True(t, strings.HasPrefix(cc, "private, max-age="))
	})
	t.Run("304 responds private", func(t *testing.T) {
		st := &stubStore{exists: true}
		resp, _ := doGetWithHeader(t, newTestServer(st, nil), "/events/"+id, "If-None-Match", `"`+id+`"`)
		require.Equal(t, http.StatusNotModified, resp.StatusCode)
		cc := resp.Header.Get("Cache-Control")
		assert.True(t, strings.HasPrefix(cc, "private, max-age="),
			"304 responses must mirror Cache-Control for the same key")
	})
	t.Run("/health stays no-store", func(t *testing.T) {
		resp, _ := doGet(t, newTestServer(&stubStore{}, nil), "/health")
		assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"),
			"CACHE_PRIVATE must NOT promote no-store to something weaker")
	})
}

// TestListEvents_FrontierErr: if GetIngestionState fails for any
// reason, fall back to no-cache. The spec says: "When in doubt, don't
// cache" — a frontier error is exactly that case.
func TestListEvents_FrontierErr(t *testing.T) {
	st := &stubStore{
		events:       []store.Event{{ID: "e1"}},
		ingestionErr: context.DeadlineExceeded,
	}
	s := newTestServer(st, nil)

	resp, _ := doGet(t, s, "/events?to_ledger=999")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"),
		"a frontier lookup error must fall through to no-cache")
	assert.Empty(t, resp.Header.Get("ETag"))
}

// TestGetEvent_IfNoneMatch_WeakPrefix: per RFC 7232 §3.2, an
// If-None-Match header with the W/ weakness prefix matches a strong
// server ETag for the purpose of conditional GETs (the weakness is
// ignored on the comparison; only the opaque-tag value matters).
// Lock in this behavior so a stray tweak of ifNoneMatch can't drift.
func TestGetEvent_IfNoneMatch_WeakPrefix(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{exists: true}
	s := newTestServer(st, nil)

	resp, body := doGetWithHeader(t, s, "/events/"+id, "If-None-Match", `W/"`+id+`"`)
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
	assert.Empty(t, string(body))
	assert.Equal(t, `"`+id+`"`, resp.Header.Get("ETag"),
		"304 responses carry the server's strong ETag (W/ belongs to the client's request)")
}

// TestListEvents_ETagNormalizesDefaults: filterFromQuery leaves
// Order and Limit at their zero values when unset, but the SQL layer
// treats Order="" as "asc" and Limit<=0 as DefaultQueryLimit. Without
// normalization, two requests that produce identical bodies would get
// distinct ETags and the cache would thrash on equivalent pages.
func TestListEvents_ETagNormalizesDefaults(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}},
		ingestion: store.IngestionState{LastIngestedLedger: 1000},
	}
	s := newTestServer(st, nil)

	respDefault, _ := doGet(t, s, "/events?to_ledger=999")
	respAsc, _ := doGet(t, s, "/events?to_ledger=999&order=asc")
	respLimit, _ := doGet(t, s, "/events?to_ledger=999&limit=50")
	require.Equal(t, http.StatusOK, respDefault.StatusCode)
	assert.Equal(t, respDefault.Header.Get("ETag"), respAsc.Header.Get("ETag"),
		"order=asc and no order param must share an ETag")
	assert.Equal(t, respDefault.Header.Get("ETag"), respLimit.Header.Get("ETag"),
		"limit=50 and no limit param must share an ETag")

	// And the resolver breaks ties correctly: the resolved defaults
	// must collide, while any non-default value must produce a
	// distinct ETag.
	respDesc, _ := doGet(t, s, "/events?to_ledger=999&order=desc")
	respLimit2, _ := doGet(t, s, "/events?to_ledger=999&limit=10")
	assert.NotEqual(t, respDefault.Header.Get("ETag"), respDesc.Header.Get("ETag"),
		"order=desc changes the body and must produce a distinct ETag")
	assert.NotEqual(t, respDefault.Header.Get("ETag"), respLimit2.Header.Get("ETag"),
		"limit=10 changes the body and must produce a distinct ETag")
}

// TestErrors_NoStore: any 4xx/5xx is treated as no-store so a CDN
// that warmed on an ETag-bearing 200 cannot pool the not-found /
// bad-request / internal-error response behind the same key.
func TestErrors_NoStore(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	// Validation error path: filterFromQuery rejects and writeError
	// emits a 400 with no-store.
	resp, _ := doGet(t, s, "/events?from_ledger=abc")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"),
		"validation errors must be no-store")

	// Eviction 404 path: the cheap EventExists probe says the row is
	// gone, so we surface a 404 with no-store — a CDN that warmed on
	// a still-present copy must not be allowed to cache the not-found
	// for the immutable max-age.
	st := &stubStore{exists: false}
	resp, _ = doGetWithHeader(t, newTestServer(st, nil), "/events/0001099511627776-0000000001", "If-None-Match", `"0001099511627776-0000000001"`)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"),
		"the eviction path must not let a CDN cache a stale 404 for the immutable max-age")
}

// TestListETag_CoversEveryFilterField pins the invariant that makes the
// list ETag a *strong* validator: any filter field that changes the response
// body must change the validator.
//
// The table is written as "every filter that narrows a query", not as "the
// filters listETag currently hashes", because the failure mode here is a
// contributor adding a filter and not knowing the validator exists. That is
// exactly how topic0..topic3 (#64) and topic_contains (#180) each shipped
// missing from it: two different features, the same omission, neither
// visible without a test that enumerates the fields independently.
//
// The consequence is not theoretical. Two requests that differ only in a
// missing field hash identically, so on a page below the ingest frontier —
// where responses are `immutable` with a one-year max-age — a conditional
// request for one filter is answered 304 for the other, and a shared cache
// pools one filter's body under the other's key. The filters most affected
// are the topic ones, whose entire purpose is to return a narrow subset.
func TestListETag_CoversEveryFilterField(t *testing.T) {
	base := store.EventFilter{FromLedger: 500, ToLedger: 999}
	baseETag := listETag(base)

	// Each case mutates exactly one field away from base.
	cases := []struct {
		field  string
		mutate func(f *store.EventFilter)
	}{
		{"ContractID", func(f *store.EventFilter) { f.ContractID = testContract }},
		{"Type", func(f *store.EventFilter) { f.Types = []string{"diagnostic"} }},
		{"Topic", func(f *store.EventFilter) { f.Topic = json.RawMessage(`{"symbol":"transfer"}`) }},
		{"Topic0", func(f *store.EventFilter) { f.Topic0 = json.RawMessage(`{"symbol":"transfer"}`) }},
		{"Topic1", func(f *store.EventFilter) { f.Topic1 = json.RawMessage(`{"symbol":"transfer"}`) }},
		{"Topic2", func(f *store.EventFilter) { f.Topic2 = json.RawMessage(`{"symbol":"transfer"}`) }},
		{"Topic3", func(f *store.EventFilter) { f.Topic3 = json.RawMessage(`{"symbol":"transfer"}`) }},
		{"TopicContains", func(f *store.EventFilter) { f.TopicContains = json.RawMessage(`[{"u64":7}]`) }},
		{"TxHash", func(f *store.EventFilter) { f.TxHash = "abc123def" }},
		{"FromLedger", func(f *store.EventFilter) { f.FromLedger = 501 }},
		{"ToLedger", func(f *store.EventFilter) { f.ToLedger = 998 }},
		{"FromTime", func(f *store.EventFilter) { f.FromTime = time.Unix(1_000_000, 0).UTC() }},
		{"ToTime", func(f *store.EventFilter) { f.ToTime = time.Unix(2_000_000, 0).UTC() }},
		{"Cursor", func(f *store.EventFilter) { f.Cursor = "e1" }},
		{"Limit", func(f *store.EventFilter) { f.Limit = 7 }},
		{"Order", func(f *store.EventFilter) { f.Order = "desc" }},
	}

	seen := map[string]string{baseETag: "base"}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			f := base
			tc.mutate(&f)

			got := listETag(f)
			require.NotEqual(t, baseETag, got,
				"changing %s changes which events are returned, so it must change the ETag; "+
					"a filter missing from listETag lets one filter's cached page be served for another",
				tc.field)

			// And distinct from every other single-field change, so two
			// different filters cannot collide with each other either.
			if other, dup := seen[got]; dup {
				t.Fatalf("%s produces the same ETag as %s", tc.field, other)
			}
			seen[got] = tc.field
		})
	}
}

// The positional topic filters address different array positions, so filters
// that differ only in which position they constrain must not share a
// validator.
func TestListETag_PositionalTopicsAreDistinguished(t *testing.T) {
	value := json.RawMessage(`{"symbol":"transfer"}`)
	base := store.EventFilter{ToLedger: 999}

	at0, at1 := base, base
	at0.Topic0 = value
	at1.Topic1 = value

	assert.NotEqual(t, listETag(at0), listETag(at1),
		"topic0={x} and topic1={x} select different events and must hash differently")

	// And the any-position filter is distinct from a positional one.
	any := base
	any.Topic = value
	assert.NotEqual(t, listETag(at0), listETag(any))
}

// End to end: two cacheable requests differing only in a topic filter must
// not be able to answer each other's conditional request.
func TestListEvents_TopicFilterCannotReuseAnothersValidator(t *testing.T) {
	st := &stubStore{
		events:    []store.Event{{ID: "e1"}},
		ingestion: store.IngestionState{LastIngestedLedger: 1000},
	}
	s := newTestServer(st, nil)

	const (
		mint     = `/events?to_ledger=999&topic_contains=[{"symbol":"mint"}]`
		transfer = `/events?to_ledger=999&topic_contains=[{"symbol":"transfer"}]`
	)

	respMint, _ := doGet(t, s, mint)
	require.Equal(t, http.StatusOK, respMint.StatusCode)
	mintETag := respMint.Header.Get("ETag")
	require.NotEmpty(t, mintETag, "a page below the frontier must be cacheable for this to matter")

	respTransfer, _ := doGet(t, s, transfer)
	assert.NotEqual(t, mintETag, respTransfer.Header.Get("ETag"),
		"two different topic filters must not share a validator")

	// Replaying the mint validator against the transfer query must not be
	// answered 304 — that would serve the mint page for a transfer request,
	// with a one-year immutable max-age behind it.
	resp, _ := doGetWithHeader(t, s, transfer, "If-None-Match", mintETag)
	assert.NotEqual(t, http.StatusNotModified, resp.StatusCode,
		"a validator minted for a different filter must not produce a 304")
}

// TestGetEvent_Decoded_Immutable: GET /events/{id}?decoded=true returns
// the same immutable cache headers as the non-decoded path.
func TestGetEvent_Decoded_Immutable(t *testing.T) {
	const id = "0001099511627776-0000000001"
	st := &stubStore{event: store.Event{ID: id, Ledger: 100}}
	s := New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key", &stubEnricher{})

	resp, _ := doGet(t, s, "/events/"+id+"?decoded=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assertImmutable(t, resp, `"`+id+`"`)
}

// TestGetEvent_DecodedWithXDR_Immutable: GET /events/{id}?decoded=true&include_xdr=true
// returns the same immutable cache headers.
func TestGetEvent_DecodedWithXDR_Immutable(t *testing.T) {
	const id = "0001099511627776-0000000002"
	st := &stubStore{event: store.Event{ID: id, Ledger: 100}}
	s := New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key", &stubEnricher{})

	resp, _ := doGet(t, s, "/events/"+id+"?decoded=true&include_xdr=true")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assertImmutable(t, resp, `"`+id+`"`)
}
