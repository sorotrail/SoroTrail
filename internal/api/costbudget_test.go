package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/store"
)

type costClock struct{ t time.Time }

func (c *costClock) now() time.Time          { return c.t }
func (c *costClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func tenantReq(path string, tenantID int64) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	return r.WithContext(WithPrincipal(r.Context(), Principal{Tenant: store.Tenant{ID: tenantID}}))
}

func serveCost(b *CostBudget, h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	b.Middleware(h).ServeHTTP(w, r)
	return w
}

var costOK = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

func TestCostEstimationPerDriver(t *testing.T) {
	b := NewCostBudget(1000, 1)
	est := func(path string) float64 {
		c, _ := b.estimate(httptest.NewRequest(http.MethodGet, path, nil))
		return c
	}
	const cid = "&contract_id=CABC"
	base := est("/events?limit=50&from_ledger=1&to_ledger=10000" + cid)

	assert.Greater(t, est("/events?limit=200&from_ledger=1&to_ledger=10000"+cid), base, "page size")
	assert.Greater(t, est("/events?limit=50&from_ledger=1&to_ledger=90000"+cid), base, "range width")
	assert.Greater(t, est("/events?limit=50&from_ledger=1&to_ledger=10000"), base, "no index-backed filter")
	assert.Greater(t, est("/events?limit=50&from_ledger=1"+cid), base, "unbounded range")
	assert.Greater(t, est("/events/aggregate?bucket=ledger&from_ledger=1&to_ledger=10000"+cid),
		est("/events/aggregate?bucket=ledger&from_ledger=1&to_ledger=100"+cid), "aggregate breadth")
	assert.Equal(t, float64(queries.BaseRequestCost), est("/stats"))
}

func TestCostBudgetExhaustionAndRefill(t *testing.T) {
	clk := &costClock{t: time.Unix(1_000_000, 0)}
	// Capacity 100, refill 10/s. A /events page with a contract filter and a
	// 10k-ledger range costs 1+25+1 = 27.
	b := NewCostBudget(100, 10, WithCostClock(clk.now))
	path := "/events?limit=50&from_ledger=1&to_ledger=10000&contract_id=CABC"

	for i := 0; i < 3; i++ {
		require.Equal(t, http.StatusOK, serveCost(b, costOK, tenantReq(path, 7)).Code, "request %d", i)
	}
	w := serveCost(b, costOK, tenantReq(path, 7))
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After")) // needs 8 more units at 10/s
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, costBudgetName, body["budget"])
	assert.Equal(t, "cost_budget_exhausted", body["code"])

	// Another tenant has its own bucket.
	assert.Equal(t, http.StatusOK, serveCost(b, costOK, tenantReq(path, 8)).Code)

	// The bucket refills over time.
	clk.advance(2 * time.Second)
	assert.Equal(t, http.StatusOK, serveCost(b, costOK, tenantReq(path, 7)).Code)
}

func TestCostBudgetReconcilesActualAgainstEstimate(t *testing.T) {
	clk := &costClock{t: time.Unix(1_000_000, 0)}
	b := NewCostBudget(1000, 1, WithCostClock(clk.now))
	// limit=200 estimates 1+100+20(unbounded)+25(unindexed)=146; serving 3
	// rows reprices to 1+25+20+25=71, so 75 units are refunded.
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reportRowsServed(r.Context(), 3)
		w.WriteHeader(http.StatusOK)
	})
	require.Equal(t, http.StatusOK, serveCost(b, h, tenantReq("/events?limit=200", 1)).Code)
	b.mu.Lock()
	got := b.buckets["tenant:1"].tokens
	b.mu.Unlock()
	assert.InDelta(t, 1000-71, got, 0.001)

	// A handler that reports nothing settles at the estimate.
	require.Equal(t, http.StatusOK, serveCost(b, costOK, tenantReq("/events?limit=200", 2)).Code)
	b.mu.Lock()
	got = b.buckets["tenant:2"].tokens
	b.mu.Unlock()
	assert.InDelta(t, 1000-146, got, 0.001)
}

func TestCostBudgetExemptAndUntenantedNotCharged(t *testing.T) {
	b := NewCostBudget(1, 0.001) // a single unit: any charged request would drain it
	for _, p := range []string{"/health", "/livez", "/readyz", "/metrics", "/events.csv", "/contracts/CABC/export"} {
		for i := 0; i < 5; i++ {
			require.Equal(t, http.StatusOK, serveCost(b, costOK, tenantReq(p, 1)).Code, p)
		}
	}
	// Untenanted principals (single-tenant mode) and anonymous requests are not charged.
	for i := 0; i < 5; i++ {
		r := httptest.NewRequest(http.MethodGet, "/events?limit=200", nil)
		assert.Equal(t, http.StatusOK, serveCost(b, costOK, r).Code)
		r = httptest.NewRequest(http.MethodGet, "/events?limit=200", nil)
		r = r.WithContext(WithPrincipal(r.Context(), Principal{Untenanted: true}))
		assert.Equal(t, http.StatusOK, serveCost(b, costOK, r).Code)
	}
	b.mu.Lock()
	assert.Empty(t, b.buckets, "exempt and untenanted traffic must not create buckets")
	b.mu.Unlock()
}

func TestCostBudgetDisabledIsPassThrough(t *testing.T) {
	b := NewCostBudget(0, 0)
	assert.False(t, b.Enabled())
	for i := 0; i < 3; i++ {
		assert.Equal(t, http.StatusOK, serveCost(b, costOK, tenantReq("/events?limit=200", 1)).Code)
	}
}

func TestCostBudgetGraphQLEstimateAndBodyRestored(t *testing.T) {
	b := NewCostBudget(1000, 1, WithGraphQLEstimator(func(q string, _ map[string]any) int { return 400 }))
	var seen string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		seen = string(raw)
	})
	body := `{"query":"{ events { totalCount } }"}`
	r := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
	r = r.WithContext(WithPrincipal(r.Context(), Principal{Tenant: store.Tenant{ID: 3}}))
	serveCost(b, h, r)
	assert.Equal(t, body, seen, "handler must still see the full body")
	b.mu.Lock()
	assert.InDelta(t, 600, b.buckets["tenant:3"].tokens, 0.001)
	b.mu.Unlock()
}
