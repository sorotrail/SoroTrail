package api

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/store"
)

func gzipPayload(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	return buf.Bytes()
}

func deflatePayload(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	fw, err := flate.NewWriter(&buf, flate.BestCompression)
	require.NoError(t, err)
	_, err = fw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, fw.Close())
	return buf.Bytes()
}

func TestRequestAndResponseSizeLimits(t *testing.T) {
	const testKey = "secret-admin-key"
	const validContract = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4"

	t.Run("oversized request body returns 413 naming limit and increments metric", func(t *testing.T) {
		srv := newTestServer(&stubStore{}, nil)
		srv.apiKey = testKey
		srv.SetHTTPRequestBodyLimit(64)

		before := testutil.ToFloat64(metrics.HTTPPayloadSizeRejectionsTotal.WithLabelValues("/watched-contracts", "request_body"))

		oversized := fmt.Sprintf(`{"contract_id":%q,"network":%q}`, validContract, strings.Repeat("a", 128))
		req := httptest.NewRequest(http.MethodPost, "/watched-contracts", strings.NewReader(oversized))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", testKey)
		rec := httptest.NewRecorder()

		srv.Router().ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		var env errorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
		assert.Equal(t, CodePayloadTooLarge, env.Code)
		assert.Contains(t, env.Error, "64 bytes")

		after := testutil.ToFloat64(metrics.HTTPPayloadSizeRejectionsTotal.WithLabelValues("/watched-contracts", "request_body"))
		assert.Equal(t, before+1, after)
	})

	t.Run("gzip and deflate compression bombs are rejected at decompressed byte cap", func(t *testing.T) {
		srv := newTestServer(&stubStore{}, nil)
		srv.apiKey = testKey
		// Raw compressed payload is tiny (< 512 bytes), but expands to 64 KiB.
		srv.SetHTTPRequestBodyLimit(4096)
		srv.SetMaxDecompressedBodySize(1024)

		bombRaw := []byte(fmt.Sprintf(`{"contract_id":%q,"network":%q}`, validContract, strings.Repeat("0", 64*1024)))
		compressedGzip := gzipPayload(t, bombRaw)
		require.Less(t, len(compressedGzip), 1024, "compressed bomb must fit comfortably under raw HTTPRequestBodyLimit")

		before := testutil.ToFloat64(metrics.HTTPPayloadSizeRejectionsTotal.WithLabelValues("/watched-contracts", "decompressed_body"))

		req := httptest.NewRequest(http.MethodPost, "/watched-contracts", bytes.NewReader(compressedGzip))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("X-API-Key", testKey)
		rec := httptest.NewRecorder()

		srv.Router().ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		var env errorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
		assert.Equal(t, CodePayloadTooLarge, env.Code)
		assert.Contains(t, env.Error, "decompressed request body exceeds 1024 bytes")

		after := testutil.ToFloat64(metrics.HTTPPayloadSizeRejectionsTotal.WithLabelValues("/watched-contracts", "decompressed_body"))
		assert.Equal(t, before+1, after)

		// Verify deflate bomb is rejected identically.
		compressedDeflate := deflatePayload(t, bombRaw)
		reqDeflate := httptest.NewRequest(http.MethodPost, "/watched-contracts", bytes.NewReader(compressedDeflate))
		reqDeflate.Header.Set("Content-Type", "application/json")
		reqDeflate.Header.Set("Content-Encoding", "deflate")
		reqDeflate.Header.Set("X-API-Key", testKey)
		recDeflate := httptest.NewRecorder()

		srv.Router().ServeHTTP(recDeflate, reqDeflate)
		require.Equal(t, http.StatusRequestEntityTooLarge, recDeflate.Code)
	})

	t.Run("valid gzipped request body within decompressed limit succeeds", func(t *testing.T) {
		srv := newTestServer(&stubStore{}, nil)
		srv.apiKey = testKey
		srv.SetHTTPRequestBodyLimit(1024)
		srv.SetMaxDecompressedBodySize(2048)

		validBody := []byte(fmt.Sprintf(`{"contract_id":%q}`, validContract))
		compressed := gzipPayload(t, validBody)

		req := httptest.NewRequest(http.MethodPost, "/watched-contracts?confirm=true", bytes.NewReader(compressed))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("X-API-Key", testKey)
		rec := httptest.NewRecorder()

		srv.Router().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("per-endpoint override allows larger body on overridden route while enforcing global limit elsewhere", func(t *testing.T) {
		srv := newTestServer(&stubStore{}, nil)
		srv.apiKey = testKey
		// Tight global default (48 bytes), higher override on POST /watched-contracts (512 bytes).
		srv.SetHTTPRequestBodyLimit(48)
		srv.SetEndpointRequestBodyLimit("POST /watched-contracts", 512)

		body := fmt.Sprintf(`{"contract_id":%q}`, validContract)
		require.Greater(t, len(body), 48)

		reqAllowed := httptest.NewRequest(http.MethodPost, "/watched-contracts?confirm=true", strings.NewReader(body))
		reqAllowed.Header.Set("Content-Type", "application/json")
		reqAllowed.Header.Set("X-API-Key", testKey)
		recAllowed := httptest.NewRecorder()
		srv.Router().ServeHTTP(recAllowed, reqAllowed)
		assert.Equal(t, http.StatusOK, recAllowed.Code, "endpoint with higher override must accept body")

		// Another endpoint without an override still enforces the 48-byte global cap.
		subBody := `{"url":"https://example.com/webhook","secret":"my-hmac-secret-123456"}`
		reqRejected := httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(subBody))
		reqRejected.Header.Set("Content-Type", "application/json")
		recRejected := httptest.NewRecorder()
		srv.Router().ServeHTTP(recRejected, reqRejected)
		assert.Equal(t, http.StatusRequestEntityTooLarge, recRejected.Code, "non-overridden endpoint must enforce global 48-byte limit")
	})

	t.Run("non-streaming response under and over HTTPResponseBodyLimit", func(t *testing.T) {
		events := []store.Event{
			{
				ID:         "0000000429496729600-0000000000",
				ContractID: validContract,
				Ledger:     100,
				Type:       "contract",
				TxHash:     strings.Repeat("a", 64),
				Topics:     json.RawMessage(`["transfer"]`),
				Value:      json.RawMessage(`{"amount":1000}`),
			},
		}
		st := &stubStore{events: events}
		srv := newTestServer(st, nil)

		// 1. Generous response limit: request succeeds with 200 OK.
		srv.SetHTTPResponseBodyLimit(64 * 1024)
		reqOK := httptest.NewRequest(http.MethodGet, "/events?limit=1", nil)
		recOK := httptest.NewRecorder()
		srv.Router().ServeHTTP(recOK, reqOK)
		require.Equal(t, http.StatusOK, recOK.Code)

		// 2. Tight response limit (32 bytes): response is rejected with 413 + truncation headers + metric.
		srv.SetHTTPResponseBodyLimit(32)
		before := testutil.ToFloat64(metrics.HTTPPayloadSizeRejectionsTotal.WithLabelValues("/events", "response_body"))

		reqTooBig := httptest.NewRequest(http.MethodGet, "/events?limit=1", nil)
		recTooBig := httptest.NewRecorder()
		srv.Router().ServeHTTP(recTooBig, reqTooBig)

		require.Equal(t, http.StatusRequestEntityTooLarge, recTooBig.Code)
		assert.Equal(t, "true", recTooBig.Header().Get("X-Response-Truncated"))
		assert.Equal(t, "32", recTooBig.Header().Get("X-Response-Limit-Bytes"))
		var env errorResponse
		require.NoError(t, json.Unmarshal(recTooBig.Body.Bytes(), &env))
		assert.Equal(t, CodePayloadTooLarge, env.Code)
		assert.Contains(t, env.Error, "response body exceeds the configured limit of 32 bytes")

		after := testutil.ToFloat64(metrics.HTTPPayloadSizeRejectionsTotal.WithLabelValues("/events", "response_body"))
		assert.Equal(t, before+1, after)
	})

	t.Run("streaming CSV export and SSE responses bypass response size cap", func(t *testing.T) {
		events := []store.Event{
			{
				ID:         "0000000429496729600-0000000000",
				ContractID: validContract,
				Ledger:     100,
				Type:       "contract",
				TxHash:     strings.Repeat("b", 64),
				Topics:     json.RawMessage(`["transfer"]`),
				Value:      json.RawMessage(`{"amount":1000}`),
			},
		}
		st := &stubStore{events: events}
		srv := newTestServer(st, nil)
		// Set a tiny 16-byte response cap; /events.csv must still stream 200 OK.
		srv.SetHTTPResponseBodyLimit(16)

		reqCSV := httptest.NewRequest(http.MethodGet, "/events.csv", nil)
		recCSV := httptest.NewRecorder()
		srv.Router().ServeHTTP(recCSV, reqCSV)
		require.Equal(t, http.StatusOK, recCSV.Code)
		assert.Greater(t, recCSV.Body.Len(), 16, "streaming CSV export must not be blocked by response size cap")
	})
}
