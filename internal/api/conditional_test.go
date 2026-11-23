package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Conditional requests across the read surface (#1244): a poller that has
// nothing new should pay for a header round trip, not the whole body.

func conditionalStore() *stubStore {
	return &stubStore{events: []store.Event{{ID: "e1", Ledger: 1}, {ID: "e2", Ledger: 2}}}
}

func TestListEventsReturnsAValidatorAndAnswers304(t *testing.T) {
	st := conditionalStore()
	s := newTestServer(st, nil)

	first, body := doGet(t, s, "/events")
	require.Equal(t, http.StatusOK, first.StatusCode)
	etag := first.Header.Get("ETag")
	require.Regexp(t, `^"[0-9a-f]{32}"$`, etag, "a strong validator, not a weak one")
	assert.Equal(t, "no-cache", first.Header.Get("Cache-Control"),
		"the response stays revalidate-every-time; the validator makes revalidation cheap")
	require.NotEmpty(t, body)

	second, secondBody := doGetWithHeader(t, s, "/events", "If-None-Match", etag)
	assert.Equal(t, http.StatusNotModified, second.StatusCode)
	assert.Empty(t, secondBody, "a 304 has no body")
	assert.Equal(t, etag, second.Header.Get("ETag"), "the 304 repeats the validator")
	assert.Equal(t, "no-cache", second.Header.Get("Cache-Control"), "and the cache policy")
	assert.NotEmpty(t, second.Header.Get("Vary"), "and Vary, so a cache keys it the same way")
	assert.Empty(t, second.Header.Get("Content-Type"), "representation headers are dropped from a 304")
}

func TestValidatorChangesWhenTheBodyChanges(t *testing.T) {
	st := conditionalStore()
	s := newTestServer(st, nil)

	first, _ := doGet(t, s, "/events")
	oldTag := first.Header.Get("ETag")

	st.events = append(st.events, store.Event{ID: "e3", Ledger: 3})

	second, body := doGetWithHeader(t, s, "/events", "If-None-Match", oldTag)
	assert.Equal(t, http.StatusOK, second.StatusCode, "new data must not be answered 304")
	assert.Contains(t, string(body), "e3")
	assert.NotEqual(t, oldTag, second.Header.Get("ETag"))
}

func TestSameRequestSameValidator(t *testing.T) {
	s := newTestServer(conditionalStore(), nil)

	a, _ := doGet(t, s, "/events")
	b, _ := doGet(t, s, "/events")
	assert.Equal(t, a.Header.Get("ETag"), b.Header.Get("ETag"),
		"the validator is derived from the bytes, so identical responses share it")
}

func TestDifferentRepresentationsGetDifferentValidators(t *testing.T) {
	s := newTestServer(conditionalStore(), nil)

	plain, _ := doGet(t, s, "/events")
	limited, _ := doGet(t, s, "/events?limit=1")
	assert.NotEqual(t, plain.Header.Get("ETag"), limited.Header.Get("ETag"), "a different page is a different resource")
}

func TestIfNoneMatchForms(t *testing.T) {
	s := newTestServer(conditionalStore(), nil)
	first, _ := doGet(t, s, "/events")
	etag := first.Header.Get("ETag")

	tests := []struct {
		name   string
		header string
		want   int
	}{
		{"exact", etag, http.StatusNotModified},
		{"weak form of the same tag matches (weak comparison)", "W/" + etag, http.StatusNotModified},
		{"a list containing the tag", `"other", ` + etag + `, "another"`, http.StatusNotModified},
		{"the wildcard", "*", http.StatusNotModified},
		{"a different tag", `"0000000000000000000000000000000"`, http.StatusOK},
		{"an empty header", "", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, _ := doGetWithHeader(t, s, "/events", "If-None-Match", tt.header)
			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}

func TestHeadCarriesTheValidatorAndNoBody(t *testing.T) {
	s := newTestServer(conditionalStore(), nil)
	req := httptest.NewRequest(http.MethodHead, "/events", nil)
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, req)

	// A HEAD on a GET-only route may be 405; when the router answers it, it
	// must still never leak a body.
	assert.Empty(t, rec.Body.Bytes())
	if rec.Code == http.StatusOK {
		assert.NotEmpty(t, rec.Header().Get("ETag"))
	}
}

func TestErrorsAreNotTagged(t *testing.T) {
	s := newTestServer(conditionalStore(), nil)

	resp, _ := doGet(t, s, "/events?limit=not-a-number")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("ETag"), "only a 200 representation has a validator")
}

func TestOtherReadEndpointsAreConditionalToo(t *testing.T) {
	s := newTestServer(conditionalStore(), nil)

	for _, path := range []string{"/events/count", "/contracts", "/events/aggregate?bucket=ledger"} {
		t.Run(path, func(t *testing.T) {
			first, _ := doGet(t, s, path)
			if first.StatusCode != http.StatusOK {
				t.Skipf("%s answers %d with this stub store", path, first.StatusCode)
			}
			etag := first.Header.Get("ETag")
			require.NotEmpty(t, etag)
			second, body := doGetWithHeader(t, s, path, "If-None-Match", etag)
			assert.Equal(t, http.StatusNotModified, second.StatusCode)
			assert.Empty(t, body)
		})
	}
}

func TestStatsAndHealthStayUntagged(t *testing.T) {
	s := newTestServer(&stubStore{stats: store.Stats{TotalEvents: 1}}, nil)

	for _, path := range []string{"/stats", "/health"} {
		resp, _ := doGet(t, s, path)
		assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"), path)
		assert.Empty(t, resp.Header.Get("ETag"), "%s changes constantly and must not be revalidated", path)
	}
}

func TestAHandlerThatTagsItselfKeepsItsTag(t *testing.T) {
	h := conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"mine"`)
		_, _ = io.WriteString(w, "body")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, `"mine"`, rec.Header().Get("ETag"))
	assert.Equal(t, "body", rec.Body.String())
}

func TestWritesAndNonSuccessPassThrough(t *testing.T) {
	h := conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, "made")
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "nope")
	}))

	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/", nil))
	assert.Equal(t, http.StatusCreated, post.Code)
	assert.Equal(t, "made", post.Body.String())
	assert.Empty(t, post.Header().Get("ETag"))

	missing := httptest.NewRecorder()
	h.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusNotFound, missing.Code)
	assert.Equal(t, "nope", missing.Body.String())
	assert.Empty(t, missing.Header().Get("ETag"))
}

func TestAnEmptyBodyStillGetsAValidator(t *testing.T) {
	h := conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("ETag"))
}

func TestAResponseOverTheBufferLimitStreamsThroughUntagged(t *testing.T) {
	big := strings.Repeat("x", conditionalMaxBody+1)
	h := conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, big[:1024])
		_, _ = io.WriteString(w, big[1024:])
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, len(big), rec.Body.Len(), "every byte still arrives")
	assert.Empty(t, rec.Header().Get("ETag"), "an unbounded buffer is not worth a validator")
}

func TestAFlushingHandlerIsStreamedNotBuffered(t *testing.T) {
	h := conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "first,")
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "second")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, "first,second", rec.Body.String())
	assert.True(t, rec.Flushed)
	assert.Empty(t, rec.Header().Get("ETag"), "a stream cannot be validated")
}

func TestPrettyFlagSurvivesTheWrapper(t *testing.T) {
	h := prettyMiddleware(conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]int{"a": 1})
	})))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/?pretty=true", nil))

	assert.Contains(t, rec.Body.String(), "\n  \"a\"", "indentation means writeJSON still saw the pretty flag")
}

// Compression sits outside conditionalGET: the validator is computed on the
// identity body and Compress then weakens it for the gzip representation. A
// client that revalidates with that weak tag must still get its 304.
func TestValidatorSurvivesCompression(t *testing.T) {
	payload := strings.Repeat("abcdefgh", 4096)
	h := Compress(1)(conditionalGET(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, payload)
	})))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
	etag := rec.Header().Get("ETag")
	require.True(t, strings.HasPrefix(etag, "W/"), "a compressed representation carries a weak tag, got %q", etag)
	zr, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	require.NoError(t, err)
	decoded, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, payload, string(decoded))

	again := httptest.NewRequest(http.MethodGet, "/", nil)
	again.Header.Set("Accept-Encoding", "gzip")
	again.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, again)
	assert.Equal(t, http.StatusNotModified, rec2.Code)
	assert.Empty(t, rec2.Body.Bytes())
}
