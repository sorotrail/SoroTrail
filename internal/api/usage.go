package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

// UsageRecorder accumulates per-tenant consumption in memory and flushes it
// to the store on a ticker.
//
// Writing a row per request was the obvious design and is the wrong one: it
// puts a database write on the hot path of every read, and it makes the
// usage table grow with traffic rather than with tenants. Instead counters
// are folded together in memory and flushed as one batched UPSERT per
// interval, so a tenant serving a thousand requests a second costs one
// statement every flush. The store applies increments with += so several
// API servers behind a load balancer can flush independently.
//
// The tradeoff is bounded loss: an ungraceful termination drops at most one
// interval's counters. That is acceptable for usage accounting — these
// numbers inform quotas and billing conversations, not authorization — and
// Stop() flushes explicitly so an orderly shutdown loses nothing.
type UsageRecorder struct {
	tenants store.TenantStore
	log     *slog.Logger
	every   time.Duration

	mu      sync.Mutex
	pending map[int64]store.UsageDelta

	once sync.Once
	stop chan struct{}
	wg   sync.WaitGroup
}

// DefaultUsageFlushInterval is how often accumulated counters are written.
const DefaultUsageFlushInterval = 10 * time.Second

// NewUsageRecorder returns a recorder writing to ts. A nil ts yields a
// recorder whose Record is a no-op, so single-tenant deployments pay
// nothing and callers need no nil checks.
func NewUsageRecorder(ts store.TenantStore, log *slog.Logger, every time.Duration) *UsageRecorder {
	if every <= 0 {
		every = DefaultUsageFlushInterval
	}
	return &UsageRecorder{
		tenants: ts,
		log:     log,
		every:   every,
		pending: make(map[int64]store.UsageDelta),
		stop:    make(chan struct{}),
	}
}

func (u *UsageRecorder) enabled() bool { return u != nil && u.tenants != nil }

// Record folds a delta into the pending counters for one tenant.
func (u *UsageRecorder) Record(tenantID int64, d store.UsageDelta) {
	if !u.enabled() || tenantID == 0 || d.Empty() {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	cur := u.pending[tenantID]
	cur.Requests += d.Requests
	cur.EventsServed += d.EventsServed
	cur.StreamSeconds += d.StreamSeconds
	u.pending[tenantID] = cur
}

// Start runs the periodic flush until ctx is cancelled or Stop is called.
func (u *UsageRecorder) Start(ctx context.Context) {
	if !u.enabled() {
		return
	}
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		ticker := time.NewTicker(u.every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-u.stop:
				return
			case <-ticker.C:
				u.Flush(ctx)
			}
		}
	}()
}

// Stop halts the flush loop after draining whatever is pending, so an
// orderly shutdown does not discard the final interval.
func (u *UsageRecorder) Stop() {
	if !u.enabled() {
		return
	}
	u.once.Do(func() { close(u.stop) })
	u.wg.Wait()

	// A fresh context: the caller's is typically already cancelled by the
	// signal that triggered shutdown, and this last write is the one most
	// worth completing.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u.Flush(ctx)
}

// Flush writes and clears the pending counters. Pending is swapped out under
// the lock so request handlers are never blocked on the database.
func (u *UsageRecorder) Flush(ctx context.Context) {
	if !u.enabled() {
		return
	}
	u.mu.Lock()
	if len(u.pending) == 0 {
		u.mu.Unlock()
		return
	}
	batch := u.pending
	u.pending = make(map[int64]store.UsageDelta, len(batch))
	u.mu.Unlock()

	if err := u.tenants.AddUsage(ctx, time.Now().UTC(), batch); err != nil {
		u.log.Error("flushing tenant usage", "tenants", len(batch), "error", err)
		// Counters are deliberately not restored on failure. Re-merging
		// them would let a persistently failing database grow the pending
		// map without bound, trading a memory leak in the API server for
		// an accounting gap — and the accounting gap is the cheaper of the
		// two failures.
	}
}

// usageMiddleware counts one request per authenticated tenant.
func (s *Server) usageMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := PrincipalFrom(r.Context()); ok && !p.Untenanted {
			s.usage.Record(p.Tenant.ID, store.UsageDelta{Requests: 1})
		}
		next.ServeHTTP(w, r)
	})
}

// recordEventsServed attributes a page of events to the requesting tenant.
func (s *Server) recordEventsServed(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	if p, ok := PrincipalFrom(ctx); ok && !p.Untenanted {
		s.usage.Record(p.Tenant.ID, store.UsageDelta{EventsServed: int64(n)})
	}
}

// recordStreamTime attributes a stream's duration to the requesting tenant.
func (s *Server) recordStreamTime(ctx context.Context, d time.Duration) {
	secs := int64(d.Seconds())
	if secs <= 0 {
		return
	}
	if p, ok := PrincipalFrom(ctx); ok && !p.Untenanted {
		s.usage.Record(p.Tenant.ID, store.UsageDelta{StreamSeconds: secs})
	}
}
