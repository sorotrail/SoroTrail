package api

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
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

func gzipBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(raw)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

func deflateBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := flate.NewWriter(&buf, flate.BestCompression)
	require.NoError(t, err)
	_, err = zw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestRequestLimits_OversizedRawBodyRejectedAndCounted(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)
	s.SetHTTPRequestBodyLimit(256)
	rtr := s.router()

	before := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonRequestBody))

	oversized := `{"contract_id":"` + testContract + `","label":"` + strings.Repeat("x", 512) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/watched-contracts", strings.NewReader(oversized))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", "test-key")
	rec := httptest.NewRecorder()
	rtr.ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Contains(t, rec.Body.String(), "request body exceeds 256 bytes")
	after := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonRequestBody))
	assert.Equal(t, before+1, after)
}

func TestRequestLimits_DecompressionBombRejectedWithoutFullExpand(t *testing.T) {
	const limit = 1024
	st := &stubStore{}
	s := newTestServer(st, nil)
	s.SetHTTPRequestBodyLimit(limit)
	rtr := s.router()

	// Construct a highly compressible JSON payload (256 KiB decompressed) whose
	// compressed representation is well below the 1 KiB wire limit.
	uncompressed := []byte(`{"contract_id":"` + testContract + `","label":"` + strings.Repeat("A", 256*1024) + `"}`)

	for _, tc := range []struct {
		encoding string
		payload  []byte
	}{
		{encoding: "gzip", payload: gzipBytes(t, uncompressed)},
		{encoding: "deflate", payload: deflateBytes(t, uncompressed)},
	} {
		t.Run(tc.encoding+"_bomb", func(t *testing.T) {
			require.Less(t, len(tc.payload), limit, "compressed wire size must be smaller than the limit")

			before := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonRequestDecompressed))

			req := httptest.NewRequest(http.MethodPost, "/watched-contracts", bytes.NewReader(tc.payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", tc.encoding)
			req.Header.Set("X-Api-Key", "test-key")
			rec := httptest.NewRecorder()
			rtr.ServeHTTP(rec, req)

			require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
			assert.Contains(t, rec.Body.String(), fmt.Sprintf("decompressed request body exceeds %d bytes", limit))

			after := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonRequestDecompressed))
			assert.Equal(t, before+1, after)
		})
	}

	t.Run("valid_compressed_json_accepted", func(t *testing.T) {
		validJSON := []byte(`{"contract_id":"` + testContract + `"}`)
		compressed := gzipBytes(t, validJSON)

		req := httptest.NewRequest(http.MethodPost, "/watched-contracts?confirm=true", bytes.NewReader(compressed))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("X-Api-Key", "test-key")
		rec := httptest.NewRecorder()
		rtr.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	})
}

func TestRequestLimits_PerEndpointOverride(t *testing.T) {
	st := &stubStore{}
	s := newTestServer(st, nil)
	s.SetHTTPRequestBodyLimit(128)
	s.SetEndpointRequestBodyLimit("PUT /contracts/{id}/spec", 4096)
	rtr := s.router()

	// 1. Standard endpoint uses the 128-byte global cap and rejects a 300-byte payload.
	postBody := `{"contract_id":"` + testContract + `"}` + strings.Repeat(" ", 200)
	postReq := httptest.NewRequest(http.MethodPost, "/watched-contracts", strings.NewReader(postBody))
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("X-Api-Key", "test-key")
	postRec := httptest.NewRecorder()
	rtr.ServeHTTP(postRec, postReq)
	require.Equal(t, http.StatusRequestEntityTooLarge, postRec.Code)
	assert.Contains(t, postRec.Body.String(), "request body exceeds 128 bytes")

	// 2. PUT /contracts/{id}/spec uses its 4096-byte override and accepts a payload > 128 bytes.
	specDoc := `{"spec":{"events":[{"name":"transfer_alpha_event"},{"name":"approve_beta_event"},{"name":"mint_gamma_event"},{"name":"burn_delta_event"},{"name":"clawback_epsilon_event"}]}}`
	require.Greater(t, len(specDoc), 128)
	putReq := httptest.NewRequest(http.MethodPut, "/contracts/"+testContract+"/spec", strings.NewReader(specDoc))
	putReq.Header.Set("Content-Type", "application/json")
	putReq.Header.Set("X-Api-Key", "test-key")
	putRec := httptest.NewRecorder()
	rtr.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code, "body: %s", putRec.Body.String())

	// 3. Exceeding the per-endpoint override still returns 413 with the endpoint's limit.
	hugeSpec := `{"spec":{"events":[{"name":"` + strings.Repeat("x", 5000) + `","topics":[]}]}}`
	hugeReq := httptest.NewRequest(http.MethodPut, "/contracts/"+testContract+"/spec", strings.NewReader(hugeSpec))
	hugeReq.Header.Set("Content-Type", "application/json")
	hugeReq.Header.Set("X-Api-Key", "test-key")
	hugeRec := httptest.NewRecorder()
	rtr.ServeHTTP(hugeRec, hugeReq)
	require.Equal(t, http.StatusRequestEntityTooLarge, hugeRec.Code)
	assert.Contains(t, hugeRec.Body.String(), "request body exceeds 4096 bytes")
}

func TestResponseLimits_NonStreamingBoundedAndStreamingExempt(t *testing.T) {
	events := make([]store.Event, 20)
	for i := range events {
		events[i] = store.Event{
			ID:               fmt.Sprintf("0000100000-%05d", i),
			ContractID:       testContract,
			Ledger:           100000 + int64(i),
			Type:             "contract",
			TxHash:           strings.Repeat("a", 64),
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`["transfer","` + strings.Repeat("T", 80) + `"]`),
			Value:            json.RawMessage(`{"amount":"100000000"}`),
		}
	}
	st := &stubStore{events: events}
	s := newTestServer(st, nil)
	s.SetCompressMinSize(-1) // disable response gzip to inspect raw JSON/CSV bodies directly

	t.Run("server_response_limit_rejects_oversized_json", func(t *testing.T) {
		s.SetHTTPResponseBodyLimit(512)
		t.Cleanup(func() { s.SetHTTPResponseBodyLimit(0) })
		rtr := s.router()

		before := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonResponseBody))

		req := httptest.NewRequest(http.MethodGet, "/events?limit=20", nil)
		rec := httptest.NewRecorder()
		rtr.ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		assert.Contains(t, rec.Body.String(), "response body exceeds 512 bytes")

		after := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonResponseBody))
		assert.Equal(t, before+1, after)
	})

	t.Run("client_header_tightens_response_limit", func(t *testing.T) {
		s.SetHTTPResponseBodyLimit(1 << 20)
		t.Cleanup(func() { s.SetHTTPResponseBodyLimit(0) })
		rtr := s.router()

		before := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonResponseBody))

		req := httptest.NewRequest(http.MethodGet, "/events?limit=20", nil)
		req.Header.Set(MaxResponseBytesHeader, "400")
		rec := httptest.NewRecorder()
		rtr.ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
		assert.Contains(t, rec.Body.String(), "response body exceeds 400 bytes")

		after := testutil.ToFloat64(metrics.HTTPLimitRejectionsTotal.WithLabelValues(metrics.LimitReasonResponseBody))
		assert.Equal(t, before+1, after)
	})

	t.Run("streaming_csv_exempt_from_non_streaming_response_cap", func(t *testing.T) {
		s.SetHTTPResponseBodyLimit(256)
		t.Cleanup(func() { s.SetHTTPResponseBodyLimit(0) })
		rtr := s.router()

		req := httptest.NewRequest(http.MethodGet, "/events.csv?limit=20", nil)
		rec := httptest.NewRecorder()
		rtr.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		assert.Greater(t, len(body), 256, "streaming CSV export must stream past the non-streaming cap without truncation")
		assert.Contains(t, string(body), "0000100000-00019")
	})
}
