// Package api — per-tenant query cost budget.
//
// CostBudget charges each tenanted request against a token bucket measured in
// cost units (see queries.CostDrivers), refilled at a fixed rate. It is the
// "how expensive" companion to RateLimiter's "how many": a tenant that issues
// few but very wide queries drains its budget just as a chatty one does.
//
// Lifecycle of a request:
//
//  1. estimate — the cost is computed from the request before execution;
//  2. admit    — the estimate is taken from the tenant's bucket, or the
//     request is rejected with 429 + Retry-After and a JSON body naming
//     the exhausted budget;
//  3. charge   — after the handler returns, the cost is recomputed from the
//     rows it actually served and the difference is refunded or charged;
//  4. record   — actual/estimate is observed in a histogram so estimator
//     drift is visible.
//
// Scope: per instance. Buckets live in process memory, so N replicas grant a
// tenant up to N times its budget. docs/performance.md describes the gap.
// The budget is off unless COST_BUDGET_CAPACITY is set.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/store"
)

// costBudgetName is the budget named in the 429 body.
const costBudgetName = "tenant_query_cost"

// graphQLBodyLimit bounds how much of a POST /graphql body is read to
// estimate its cost.
const graphQLBodyLimit = 1 << 20

// costBucket is one tenant's token bucket. tokens may go negative after an
// under-estimate is charged; it is floored at -capacity.
type costBucket struct {
	tokens float64
	last   time.Time
}

// CostBudget is the per-tenant cost-budget middleware.
type CostBudget struct {
	capacity float64
	refill   float64 // cost units per second

	now func() time.Time

	// gqlEstimate prices a POST /graphql document. It is injected because
	// the graphql package imports this one.
	gqlEstimate func(query string, vars map[string]any) int

	mu        sync.Mutex
	buckets   map[string]*costBucket
	lastSweep time.Time
}

// CostBudgetOption configures a CostBudget.
type CostBudgetOption func(*CostBudget)

// WithCostClock overrides the time source (tests).
func WithCostClock(now func() time.Time) CostBudgetOption {
	return func(b *CostBudget) {
		if now != nil {
			b.now = now
		}
	}
}

// WithGraphQLEstimator sets the function that prices GraphQL documents.
func WithGraphQLEstimator(f func(query string, vars map[string]any) int) CostBudgetOption {
	return func(b *CostBudget) { b.gqlEstimate = f }
}

// NewCostBudget returns a budget holding up to capacity cost units per
// tenant, refilled at refillPerSec units per second. A non-positive capacity
// or refill disables it.
func NewCostBudget(capacity, refillPerSec float64, opts ...CostBudgetOption) *CostBudget {
	b := &CostBudget{
		capacity: capacity,
		refill:   refillPerSec,
		now:      time.Now,
		buckets:  make(map[string]*costBucket),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Enabled reports whether the budget charges anything.
func (b *CostBudget) Enabled() bool {
	return b != nil && b.capacity > 0 && b.refill > 0
}

// costExempt reports whether a path is never charged. It reuses the rate
// limiter's exemptPaths (health, readiness, metrics must stay observable).
// Streaming exports are also exempt: they are bounded by EXPORT_MAX_RANGE and
// a single estimate cannot price a stream of unknown length; they are still
// subject to the request-rate limiter.
func costExempt(path string) bool {
	if exemptPaths[path] {
		return true
	}
	return path == "/events.csv" || (strings.HasPrefix(path, "/contracts/") && strings.HasSuffix(path, "/export"))
}

// costTracker carries the rows a handler served back to the middleware.
type costTracker struct {
	mu   sync.Mutex
	rows int
	seen bool
}

type costTrackerKey struct{}

// reportRowsServed lets a handler tell the cost budget how many rows it
// returned. It is a no-op when no budget is charging this request.
func reportRowsServed(ctx context.Context, n int) {
	if t, ok := ctx.Value(costTrackerKey{}).(*costTracker); ok && n >= 0 {
		t.mu.Lock()
		t.rows += n
		t.seen = true
		t.mu.Unlock()
	}
}

// estimateRequest prices r before it runs. The returned drivers are kept so
// the actual cost can be recomputed with the same weights afterwards.
func (b *CostBudget) estimateRequest(r *http.Request) queries.CostDrivers {
	q := r.URL.Query()
	switch {
	case r.URL.Path == "/graphql" && r.Method == http.MethodPost:
		return queries.CostDrivers{} // priced separately in Middleware
	case r.URL.Path == "/events/aggregate":
		width := ledgerWidth(q.Get("from_ledger"), q.Get("to_ledger"))
		buckets := 1000
		if q.Get("bucket") == "ledger" && width > 0 {
			buckets = int(min(width, 100_000))
		}
		return queries.CostDrivers{Ranged: true, LedgerWidth: width, Indexed: indexedFilter(q), AggregateBuckets: buckets}
	case r.URL.Path == "/events":
		size := queries.DefaultPageSize
		if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
			size = min(n, queries.MaxPageSize)
		}
		return queries.CostDrivers{
			PageSize: size, Ranged: true, Indexed: indexedFilter(q),
			LedgerWidth: ledgerWidth(q.Get("from_ledger"), q.Get("to_ledger")),
		}
	case r.URL.Path == "/events/count":
		return queries.CostDrivers{Ranged: true, Indexed: indexedFilter(q), LedgerWidth: ledgerWidth(q.Get("from_ledger"), q.Get("to_ledger"))}
	}
	return queries.CostDrivers{}
}

// ledgerWidth is the inclusive width of a closed ledger range, or 0 (unbounded)
// when either bound is missing or malformed.
func ledgerWidth(from, to string) int64 {
	f, err1 := queries.ParseLedgerParam(from)
	t, err2 := queries.ParseLedgerParam(to)
	if err1 != nil || err2 != nil || f == 0 || t == 0 || t < f {
		return 0
	}
	return t - f + 1
}

// indexedFilter reports whether the query carries a filter backed by an index
// on contract_id or tx_hash.
func indexedFilter(q map[string][]string) bool {
	for _, k := range []string{"contract_id", "contract_ids", "tx_hash"} {
		if len(q[k]) > 0 && q[k][0] != "" {
			return true
		}
	}
	return false
}

// estimate returns the estimated cost and the drivers to reprice with.
func (b *CostBudget) estimate(r *http.Request) (float64, queries.CostDrivers) {
	if r.URL.Path == "/graphql" && r.Method == http.MethodPost {
		cost := queries.BaseRequestCost
		if b.gqlEstimate != nil && r.Body != nil {
			raw, err := io.ReadAll(io.LimitReader(r.Body, graphQLBodyLimit))
			r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(raw), r.Body))
			var req struct {
				Query     string         `json:"query"`
				Variables map[string]any `json:"variables"`
			}
			if err == nil && json.Unmarshal(raw, &req) == nil && req.Query != "" {
				cost = b.gqlEstimate(req.Query, req.Variables)
			}
		}
		return float64(cost), queries.CostDrivers{}
	}
	d := b.estimateRequest(r)
	return float64(d.Cost()), d
}

// take refills key's bucket and deducts cost, or reports how long until the
// bucket could cover it. A cost above capacity is clamped to capacity so a
// full bucket can always run one maximal query.
func (b *CostBudget) take(key string, cost float64) (ok bool, available float64, wait time.Duration) {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sweepLocked(now)
	bk := b.refillLocked(key, now)
	if bk.tokens >= cost {
		bk.tokens -= cost
		return true, bk.tokens, 0
	}
	secs := (cost - bk.tokens) / b.refill
	return false, bk.tokens, time.Duration(math.Ceil(secs)) * time.Second
}

func (b *CostBudget) refillLocked(key string, now time.Time) *costBucket {
	bk, ok := b.buckets[key]
	if !ok {
		bk = &costBucket{tokens: b.capacity, last: now}
		b.buckets[key] = bk
		return bk
	}
	if dt := now.Sub(bk.last).Seconds(); dt > 0 {
		bk.tokens = math.Min(b.capacity, bk.tokens+dt*b.refill)
	}
	bk.last = now
	return bk
}

// settle applies the difference between what was admitted and the actual cost.
func (b *CostBudget) settle(key string, estimate, actual float64) {
	now := b.now()
	b.mu.Lock()
	bk := b.refillLocked(key, now)
	bk.tokens = math.Max(-b.capacity, math.Min(b.capacity, bk.tokens+estimate-actual))
	b.mu.Unlock()
}

// sweepLocked drops buckets that have refilled to capacity; a full bucket is
// indistinguishable from a fresh one, so this bounds memory without changing
// any decision.
func (b *CostBudget) sweepLocked(now time.Time) {
	if now.Sub(b.lastSweep) < time.Minute {
		return
	}
	b.lastSweep = now
	for k, bk := range b.buckets {
		if bk.tokens+now.Sub(bk.last).Seconds()*b.refill >= b.capacity {
			delete(b.buckets, k)
		}
	}
}

// Middleware wraps next with the budget; it returns next unchanged when
// disabled. It must run after authenticate: untenanted principals
// (single-tenant mode) and unauthenticated requests are not charged.
func (b *CostBudget) Middleware(next http.Handler) http.Handler {
	if !b.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if costExempt(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		p, ok := PrincipalFrom(r.Context())
		if !ok || p.Untenanted || p.Tenant.ID == 0 {
			next.ServeHTTP(w, r)
			return
		}
		key := "tenant:" + strconv.FormatInt(p.Tenant.ID, 10)
		estimate, drivers := b.estimate(r)
		estimate = math.Min(estimate, b.capacity)

		admitted, available, wait := b.take(key, estimate)
		if !admitted {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(wait.Seconds()))))
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error":               "query cost budget exhausted; retry later",
				"code":                "cost_budget_exhausted",
				"budget":              costBudgetName,
				"estimated_cost":      estimate,
				"available":           math.Max(0, available),
				"capacity":            b.capacity,
				"retry_after_seconds": max(1, int(wait.Seconds())),
			})
			return
		}

		tr := &costTracker{}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), costTrackerKey{}, tr)))

		actual := estimate
		tr.mu.Lock()
		if tr.seen && drivers.PageSize > 0 {
			d := drivers
			d.PageSize = tr.rows
			actual = math.Min(float64(d.Cost()), b.capacity)
			if tr.rows == 0 {
				d.PageSize = 0
				actual = math.Min(float64(d.Cost()), b.capacity)
			}
		}
		tr.mu.Unlock()
		b.settle(key, estimate, actual)
		metrics.CostBudgetConsumedTotal.WithLabelValues(store.OpaqueTenantLabel(strconv.FormatInt(p.Tenant.ID, 10))).Add(actual)
		if estimate > 0 {
			metrics.CostActualToEstimateRatio.Observe(actual / estimate)
		}
	})
}
