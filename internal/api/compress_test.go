package api

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// bodyOf runs h behind the compression middleware and returns the response
// plus the body as the client would see it after decoding.
func bodyOf(t *testing.T, h http.Handler, acceptEncoding string, minSize int) (*http.Response, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	rec := httptest.NewRecorder()
	Compress(minSize)(h).ServeHTTP(rec, req)
	resp := rec.Result()
	t.Cleanup(func() { _ = resp.Body.Close() })

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		require.NoError(t, err, "response claimed gzip but isn't decodable")
		defer zr.Close()
		out, err := io.ReadAll(zr)
		require.NoError(t, err)
		return resp, string(out)
	case "deflate":
		out, err := io.ReadAll(flate.NewReader(bytes.NewReader(raw)))
		require.NoError(t, err)
		return resp, string(out)
	default:
		return resp, string(raw)
	}
}

func jsonHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
}

// A large JSON body is gzipped when the client advertises support, and the
// bytes on the wire really are smaller than the original.
func TestCompress_LargeBodyIsGzipped(t *testing.T) {
	body := `{"events":[` + strings.Repeat(`{"id":"0000000000000000001-0000000000","contract_id":"CAAA"},`, 200) + `]}`

	resp, got := bodyOf(t, jsonHandler(body), "gzip", 0)

	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	assert.Equal(t, body, got, "round trips to the identical body")
	assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding")
	assert.Empty(t, resp.Header.Get("Content-Length"), "identity length must not describe a compressed body")

	// Confirm it actually saved bytes rather than merely claiming to.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	Compress(0)(jsonHandler(body)).ServeHTTP(rec, req)
	assert.Less(t, rec.Body.Len(), len(body), "compressed body is smaller than the original")
}

// A client that advertises nothing gets the original bytes untouched.
func TestCompress_UncompressedClientStillWorks(t *testing.T) {
	body := strings.Repeat("a", 10_000)
	resp, got := bodyOf(t, jsonHandler(body), "", 0)

	assert.Empty(t, resp.Header.Get("Content-Encoding"))
	assert.Equal(t, body, got)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// Bodies under the threshold are sent as-is: compressing them costs more
// than it saves.
func TestCompress_SmallBodyNotCompressed(t *testing.T) {
	body := `{"error":"not found"}`
	resp, got := bodyOf(t, jsonHandler(body), "gzip", 0)

	assert.Empty(t, resp.Header.Get("Content-Encoding"), "below threshold stays identity")
	assert.Equal(t, body, got)
}

// The threshold is the boundary: one byte under stays plain, one byte over
// compresses.
func TestCompress_ThresholdBoundary(t *testing.T) {
	const min = 512
	t.Run("just under", func(t *testing.T) {
		resp, got := bodyOf(t, jsonHandler(strings.Repeat("x", min-1)), "gzip", min)
		assert.Empty(t, resp.Header.Get("Content-Encoding"))
		assert.Len(t, got, min-1)
	})
	t.Run("at threshold", func(t *testing.T) {
		resp, got := bodyOf(t, jsonHandler(strings.Repeat("x", min)), "gzip", min)
		assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
		assert.Len(t, got, min)
	})
}

// Bodies written in many small chunks still cross the threshold: the
// decision is about the total, not any single Write.
func TestCompress_AccumulatesAcrossWrites(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for range 100 {
			_, _ = io.WriteString(w, strings.Repeat("y", 100))
		}
	})
	resp, got := bodyOf(t, h, "gzip", 0)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	assert.Len(t, got, 10_000)
}

func TestCompress_DeflateWhenGzipNotOffered(t *testing.T) {
	body := strings.Repeat("z", 5000)
	resp, got := bodyOf(t, jsonHandler(body), "deflate", 0)
	assert.Equal(t, "deflate", resp.Header.Get("Content-Encoding"))
	assert.Equal(t, body, got)
}

func TestCompress_PrefersGzipOverDeflate(t *testing.T) {
	resp, _ := bodyOf(t, jsonHandler(strings.Repeat("q", 5000)), "deflate, gzip", 0)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
}

// q=0 means "I refuse this encoding", not "rank it lowest".
func TestCompress_HonorsQZero(t *testing.T) {
	resp, got := bodyOf(t, jsonHandler(strings.Repeat("w", 5000)), "gzip;q=0", 0)
	assert.Empty(t, resp.Header.Get("Content-Encoding"))
	assert.Len(t, got, 5000)

	resp, _ = bodyOf(t, jsonHandler(strings.Repeat("w", 5000)), "gzip;q=0, deflate", 0)
	assert.Equal(t, "deflate", resp.Header.Get("Content-Encoding"), "falls back to what is acceptable")
}

// Already-compressed and unknown media types are left alone — they don't
// shrink, so encoding them is pure cost.
func TestCompress_SkipsNonCompressibleTypes(t *testing.T) {
	for _, ct := range []string{"image/png", "application/gzip", "application/octet-stream"} {
		t.Run(ct, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", ct)
				_, _ = w.Write(bytes.Repeat([]byte{0xff}, 5000))
			})
			resp, _ := bodyOf(t, h, "gzip", 0)
			assert.Empty(t, resp.Header.Get("Content-Encoding"))
		})
	}
}

// A handler that encoded its own body keeps ownership of the encoding.
func TestCompress_DoesNotDoubleEncode(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "br")
		_, _ = io.WriteString(w, strings.Repeat("b", 5000))
	})
	resp, _ := bodyOf(t, h, "gzip", 0)
	assert.Equal(t, "br", resp.Header.Get("Content-Encoding"))
}

// A 304 has no body; adding Content-Encoding to one misleads caches.
func TestCompress_LeavesNotModifiedAlone(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusNotModified)
	})
	resp, body := bodyOf(t, h, "gzip", 0)

	assert.Equal(t, http.StatusNotModified, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"))
	assert.Empty(t, body)
	assert.Equal(t, `"abc"`, resp.Header.Get("ETag"), "validator must stay byte-identical on a 304")
}

// The status a handler sets survives the deferred header write.
func TestCompress_PreservesStatusCode(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, strings.Repeat("t", 5000))
	})
	resp, got := bodyOf(t, h, "gzip", 0)
	assert.Equal(t, http.StatusTeapot, resp.StatusCode)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	assert.Len(t, got, 5000)
}

// Compressing produces a different representation, so a strong ETag is
// weakened rather than reused for bytes it no longer identifies.
func TestCompress_WeakensStrongETagWhenCompressing(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(w, strings.Repeat("e", 5000))
	})
	resp, _ := bodyOf(t, h, "gzip", 0)
	assert.Equal(t, `W/"v1"`, resp.Header.Get("ETag"))

	// The weakened validator still matches on the way back in, so
	// conditional requests keep working.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("If-None-Match", `W/"v1"`)
	assert.True(t, ifNoneMatch(req, `"v1"`))
}

// An uncompressed response must keep its strong validator untouched.
func TestCompress_KeepsStrongETagWhenNotCompressing(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(w, "small")
	})
	resp, _ := bodyOf(t, h, "gzip", 0)
	assert.Equal(t, `"v1"`, resp.Header.Get("ETag"))
}

// Streaming: a handler that flushes must not have its bytes held back
// waiting for the threshold, or a live stream stalls.
func TestCompress_FlushDeliversWithoutWaitingForThreshold(t *testing.T) {
	released := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "event: first\n")
		w.(http.Flusher).Flush()
		close(released)
		_, _ = io.WriteString(w, "event: second\n")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	Compress(0)(h).ServeHTTP(rec, req)

	<-released
	assert.Empty(t, rec.Header().Get("Content-Encoding"),
		"a stream below the threshold gives up compression rather than buffering")
	assert.Equal(t, "event: first\nevent: second\n", rec.Body.String())
}

// A WebSocket upgrade must reach the real ResponseWriter: the middleware
// wrapper would otherwise sit between the upgrade and the connection.
func TestCompress_SkipsWebSocketUpgrade(t *testing.T) {
	var got http.ResponseWriter
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = w })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events/ws", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	Compress(0)(h).ServeHTTP(rec, req)

	_, wrapped := got.(*compressWriter)
	assert.False(t, wrapped, "upgrade handlers must see the unwrapped ResponseWriter")
}

// A handler that writes no body at all still produces a valid response.
func TestCompress_EmptyBody(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	})
	resp, body := bodyOf(t, h, "gzip", 0)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, body)
	assert.Empty(t, resp.Header.Get("Content-Encoding"))
}

func TestNegotiateEncoding(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{"", ""},
		{"gzip", "gzip"},
		{"deflate", "deflate"},
		{"gzip, deflate", "gzip"},
		{"GZIP", "gzip"},
		{" gzip ;q=0.5 ", "gzip"},
		{"gzip;q=0", ""},
		{"gzip;q=0, deflate;q=0", ""},
		{"br", ""},
		{"identity", ""},
		{"*", ""},
	}
	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			assert.Equal(t, tt.want, negotiateEncoding(tt.header))
		})
	}
}

// ---------------------------------------------------------------------------
// Router-level integration tests: compression is scoped to list endpoints
// via chi.Group so health/metrics/writes stay identity always.
// ---------------------------------------------------------------------------

// doGetWithAE sends a GET through s.Router() with an optional
// Accept-Encoding header and returns the response plus body (raw, no decode).
func doGetWithAE(t *testing.T, s *Server, path, acceptEncoding string) (*http.Response, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	rec := httptest.NewRecorder()
	s.Router().ServeHTTP(rec, req)
	resp := rec.Result()
	t.Cleanup(func() { _ = resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, body
}

// decodeGzip decodes a gzip body and returns the uncompressed bytes.
func decodeGzip(t *testing.T, raw []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err, "gzip decode failed")
	defer zr.Close()
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	return out
}

// bigListStore returns enough events that the serialized JSON response
// will exceed CompressMinSize (1400). Each event ~120 bytes × 30 = ~3600.
func bigListStore() *stubStore {
	events := make([]store.Event, 30)
	for i := range events {
		events[i] = store.Event{
			ID:         "0000000000000000001-000000000" + string(rune('0'+i%10)),
			ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			Ledger:     int64(100 + i),
			Type:       "contract",
			Topics:     json.RawMessage(`["transfer"]`),
			Value:      json.RawMessage(`{"amount":"100","from":"GABC","to":"GDEF"}`),
		}
	}
	return &stubStore{events: events, totalCount: int64(len(events))}
}

func TestCompress_ListEndpointGzippedWithAcceptEncoding(t *testing.T) {
	st := bigListStore()
	s := newTestServer(st, nil)

	resp, body := doGetWithAE(t, s, "/events", "gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"),
		"list endpoint must compress when client advertises gzip")
	assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding")
	assert.Empty(t, resp.Header.Get("Content-Length"),
		"identity length must not describe a compressed body")

	// Round-trip: decode and verify the events are intact.
	decoded := decodeGzip(t, body)
	var out eventsResponse
	require.NoError(t, json.Unmarshal(decoded, &out))
	assert.Len(t, out.Events, 30)
	assert.Equal(t, int64(100), out.Events[0].Ledger)
}

func TestCompress_ListEndpointNotCompressedWithoutAcceptEncoding(t *testing.T) {
	st := bigListStore()
	s := newTestServer(st, nil)

	resp, body := doGetWithAE(t, s, "/events", "")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"without Accept-Encoding the response stays identity")

	var out eventsResponse
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Len(t, out.Events, 30)
}

func TestCompress_NonListEndpointNeverCompressed(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	// /health is NOT in the compression Group — Accept-Encoding must not
	// matter even if the client sends it. Health responses are always tiny
	// and value identity encoding for probe simplicity.
	resp, _ := doGetWithAE(t, s, "/health", "gzip")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"non-list endpoint /health must never be compressed")

	resp, _ = doGetWithAE(t, s, "/livez", "gzip")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"non-list endpoint /livez must never be compressed")

	resp, _ = doGetWithAE(t, s, "/version", "gzip")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"non-list endpoint /version must never be compressed")

	resp, _ = doGetWithAE(t, s, "/stats", "gzip")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"non-list endpoint /stats must never be compressed")
}

func TestCompress_ContractEventsEndpointCompressed(t *testing.T) {
	st := bigListStore()
	s := newTestServer(st, nil)

	resp, body := doGetWithAE(t, s,
		"/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC/events",
		"gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"),
		"contract events endpoint must compress with Accept-Encoding: gzip")

	decoded := decodeGzip(t, body)
	var out eventsResponse
	require.NoError(t, json.Unmarshal(decoded, &out))
	assert.Len(t, out.Events, 30)
}

func TestCompress_DisabledViaNegativeMinSize(t *testing.T) {
	st := bigListStore()
	s := newTestServer(st, nil)
	s.SetCompressMinSize(-1)

	resp, body := doGetWithAE(t, s, "/events", "gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"negative compressMinSize must leave responses uncompressed")

	var out eventsResponse
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Len(t, out.Events, 30)
}

func TestCompress_ListEndpointSmallErrorNotCompressed(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	// A 400 error body is well below CompressMinSize — the middleware
	// must not compress it even though it's a list endpoint.
	resp, body := doGetWithAE(t, s, "/events?type=bogus", "gzip")

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Content-Encoding"),
		"small error bodies stay identity regardless of Accept-Encoding")

	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "invalid type")
}

func TestCompress_CountEndpointCompressed(t *testing.T) {
	// count response is small — won't compress in practice, but the
	// middleware is present and sets Vary: Accept-Encoding.
	st := &stubStore{totalCount: 42}
	s := newTestServer(st, nil)

	resp, body := doGetWithAE(t, s, "/events/count", "gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// Body under threshold, so no compression — but Vary is still set.
	assert.Empty(t, resp.Header.Get("Content-Encoding"))
	assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding")

	var out countResponse
	require.NoError(t, json.Unmarshal(body, &out))
	assert.Equal(t, int64(42), out.Count)
}

func TestCompress_ListSubscriptionsEndpoint(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGetWithAE(t, s, "/subscriptions", "gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding",
		"list subscriptions endpoint must set Vary: Accept-Encoding")
}

func TestCompress_GetSubscriptionEndpoint(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)

	resp, _ := doGetWithAE(t, s, "/subscriptions/1", "gzip")

	assert.Equal(t, http.StatusNotFound, resp.StatusCode,
		"not found — but the route is in the compression group")
	assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding",
		"error from a list-route must still carry Vary: Accept-Encoding")
}

func TestCompress_GetEventEndpointCompressed(t *testing.T) {
	eventID := "0000000001-0000000001"
	st := &stubStore{
		event: store.Event{
			ID:         eventID,
			ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			Ledger:     1,
			Type:       "contract",
			Topics:     json.RawMessage(`["transfer"]`),
			Value:      json.RawMessage(`{"amount":"100"}`),
		},
	}
	s := newTestServer(st, nil)

	resp, _ := doGetWithAE(t, s, "/events/"+eventID, "gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding",
		"GET /events/{id} is in the compression group")
}

func TestCompress_WatchedContractsListEndpoint(t *testing.T) {
	st := &stubStore{
		watchedList: []store.WatchedContract{
			{ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"},
		},
	}

	t.Run("compressed when authed with Accept-Encoding: gzip", func(t *testing.T) {
		s := newTestServerWithKey(st, nil, "test-key")
		req := httptest.NewRequest(http.MethodGet, "/watched-contracts", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		req.Header.Set("X-API-Key", "test-key")
		rec := httptest.NewRecorder()
		s.Router().ServeHTTP(rec, req)
		resp := rec.Result()
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, resp.Header.Get("Vary"), "Accept-Encoding",
			"authed watched-contracts list must set Vary: Accept-Encoding")
	})
}

// The metrics endpoint is NOT in the compression Group — our compress
// middleware must not touch it. The Prometheus client library may apply
// its own encoding, but that's independent of our middleware.
func TestCompress_MetricsEndpointOutsideGroup(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	resp, _ := doGetWithAE(t, s, "/metrics", "gzip")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// The route is outside the Group so our compress middleware never
	// runs — Vary: Accept-Encoding is not added by us.
	assert.NotContains(t, resp.Header.Get("Vary"), "Accept-Encoding",
		"/metrics is not in the compression group so our compress middleware doesn't touch it")
}

func TestCompress_ListEndpointDeflate(t *testing.T) {
	st := bigListStore()
	s := newTestServer(st, nil)

	resp, body := doGetWithAE(t, s, "/events", "deflate")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "deflate", resp.Header.Get("Content-Encoding"),
		"must compress with deflate when client only advertises deflate")

	// Round-trip decode
	out, err := io.ReadAll(flate.NewReader(bytes.NewReader(body)))
	require.NoError(t, err)
	var evs eventsResponse
	require.NoError(t, json.Unmarshal(out, &evs))
	assert.Len(t, evs.Events, 30)
}
