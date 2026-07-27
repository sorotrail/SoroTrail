package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_ServesRequestDurationHistogram(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	// Generate a sample on a known route before scraping.
	resp, _ := doGet(t, s, "/health")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := doGet(t, s, "/metrics")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	text := string(body)
	assert.Contains(t, text, "http_request_duration_seconds")
	assert.Contains(t, text, `route="/health"`)
	assert.Contains(t, text, `method="GET"`)
	assert.Contains(t, text, `status="200"`)
}

func TestMetrics_ExemptFromRateLimit(t *testing.T) {
	assert.True(t, exemptPaths["/metrics"], "/metrics must stay exempt from the rate limiter")
}
