package api

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
