package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// gatherLag extracts the current float64 value of the ingestion-lag
// metric from a Collector's registry.
func gatherLag(t *testing.T, c *Collector) float64 {
	t.Helper()
	mfs, err := c.registry.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "sorotrail_ingestion_lag_ledgers" {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("sorotrail_ingestion_lag_ledgers metric not found")
	return 0
}

func TestNew_FreshGaugeIsZero(t *testing.T) {
	c := New()
	got := gatherLag(t, c)
	if got != 0 {
		t.Errorf("fresh gauge = %v, want 0", got)
	}
}

func TestSetIngestionLag(t *testing.T) {
	tests := []struct {
		name        string
		latestRPC   int64
		lastIngested int64
		want        float64
	}{
		{"both zero", 0, 0, 0},
		{"behind", 100, 42, 58},
		{"large gap", 1_000_000, 900_000, 100_000},
		{"caught up", 500, 500, 0},
		{"negative (anomaly)", 100, 103, -3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New()
			c.SetIngestionLag(tt.latestRPC, tt.lastIngested)
			got := gatherLag(t, c)
			if got != tt.want {
				t.Errorf("SetIngestionLag(%d, %d): got %v, want %v",
					tt.latestRPC, tt.lastIngested, got, tt.want)
			}
		})
	}
}

func TestSetIngestionLag_Sequence(t *testing.T) {
	c := New()
	sequence := []struct {
		latestRPC, lastIngested int64
		want                   float64
	}{
		{500, 0, 500},
		{600, 200, 400},
		{1000, 1000, 0},
		{1001, 1000, 1},
		{1001, 1001, 0},
	}
	for i, s := range sequence {
		c.SetIngestionLag(s.latestRPC, s.lastIngested)
		got := gatherLag(t, c)
		if got != s.want {
			t.Errorf("step %d: SetIngestionLag(%d, %d): got %v, want %v",
				i, s.latestRPC, s.lastIngested, got, s.want)
		}
	}
}

func TestHandler_ServesPrometheusFormat(t *testing.T) {
	c := New()
	c.SetIngestionLag(107, 100)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handler status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "sorotrail_ingestion_lag_ledgers") {
		t.Error("response body does not contain the ingestion-lag metric name")
	}
	if !strings.Contains(body, "7") {
		t.Error("response body does not reflect the set lag value")
	}
}

func TestHandler_MultipleInstancesAreIsolated(t *testing.T) {
	c1 := New()
	c1.SetIngestionLag(110, 100)
	c2 := New()
	c2.SetIngestionLag(120, 100)

	if got := gatherLag(t, c1); got != 10 {
		t.Errorf("c1 lag = %v, want 10", got)
	}
	if got := gatherLag(t, c2); got != 20 {
		t.Errorf("c2 lag = %v, want 20", got)
	}
}

func TestHandler_ContentType(t *testing.T) {
	c := New()
	c.SetIngestionLag(101, 100)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, req)

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") && !strings.Contains(ct, "openmetrics") {
		t.Errorf("Content-Type = %q, want Prometheus text format", ct)
	}
}
