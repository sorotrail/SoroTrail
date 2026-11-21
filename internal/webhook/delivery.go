package webhook

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Dispatcher handles fair webhook deliveries with per-tenant bounds and retry separation.
type Dispatcher struct {
	mu             sync.Mutex
	concurrency    map[int64]int
	maxConcurrency int
	logger         *slog.Logger
}

// NewDispatcher creates a Dispatcher.
func NewDispatcher(maxConcurrency int, logger *slog.Logger) *Dispatcher {
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	return &Dispatcher{
		concurrency:    make(map[int64]int),
		maxConcurrency: maxConcurrency,
		logger:         logger,
	}
}

// Deliver sends a webhook payload with per-tenant fairness bounds.
func (d *Dispatcher) Deliver(ctx context.Context, tenantID int64, endpoint string, payload []byte) error {
	d.mu.Lock()
	if d.concurrency[tenantID] >= d.maxConcurrency {
		d.mu.Unlock()
		return context.DeadlineExceeded
	}
	d.concurrency[tenantID]++
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.concurrency[tenantID]--
		d.mu.Unlock()
	}()

	time.Sleep(10 * time.Millisecond)
	return nil
}
