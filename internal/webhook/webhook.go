// Package webhook delivers contract events to registered subscriber
// callback URLs asynchronously, with HMAC-SHA256 signing, retry with
// exponential backoff, and automatic subscription disabling after
// consecutive failures.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	// MaxDeliveryAttempts is the number of times a delivery is retried
	// before giving up. After this many consecutive failures the
	// subscription is auto-disabled.
	MaxDeliveryAttempts = 5

	// InitialBackoff is the base retry delay.
	InitialBackoff = 1 * time.Second

	// MaxBackoff caps the exponential backoff.
	MaxBackoff = 30 * time.Second

	// DeliveryTimeout is the HTTP client timeout per attempt.
	DeliveryTimeout = 10 * time.Second

	// WorkerQueueSize is the capacity of the in-process delivery queue.
	WorkerQueueSize = 4096

	// NumWorkers is the number of concurrent delivery goroutines.
	NumWorkers = 4

	// SignatureHeader is the HTTP header carrying the HMAC-SHA256 hex
	// digest of the request body, signed with the subscription's secret.
	SignatureHeader = "X-SoroTrail-Signature"
)

// Notifier receives events from the ingester and queues matching
// subscriptions for delivery. It implements ingester.EventNotifier.
type Notifier struct {
	store       store.Store
	queue       chan deliveryTask
	sendOnly    chan<- deliveryTask
	log         *slog.Logger
	client      *http.Client
	maxAttempts int
	backoffFunc func(attempt int) time.Duration
}

// deliveryTask is one event destined for one subscription.
type deliveryTask struct {
	Subscription store.Subscription
	Event        store.Event
}

// Payload is the JSON body POSTed to subscriber endpoints.
type Payload struct {
	Event store.Event `json:"event"`
}

// NewNotifier creates a Notifier backed by a buffered channel. Call Run
// to start the worker pool; the returned Notifier is safe to pass to the
// ingester immediately (writes to the channel are non-blocking until the
// channel is full).
func NewNotifier(st store.Store, log *slog.Logger) *Notifier {
	queue := make(chan deliveryTask, WorkerQueueSize)
	client := &http.Client{Timeout: DeliveryTimeout}
	n := &Notifier{
		store:       st,
		queue:       queue,
		sendOnly:    queue,
		log:         log,
		client:      client,
		maxAttempts: MaxDeliveryAttempts,
		backoffFunc: backoffDuration,
	}
	return n
}

// NotifyEvents is called by the ingester after persisting a batch of
// events. It finds all enabled subscriptions whose filters match each
// event and enqueues delivery tasks. Enqueuing is non-blocking for the
// caller (drops into buffered channel); the channel is large enough that
// back-pressure is unlikely in normal operation.
func (n *Notifier) NotifyEvents(ctx context.Context, events []store.Event) {
	if len(events) == 0 {
		return
	}

	subs, err := n.store.ListEnabledSubscriptions(ctx)
	if err != nil {
		n.log.Error("listing enabled subscriptions for webhook delivery", "error", err)
		return
	}
	if len(subs) == 0 {
		return
	}

	for i := range events {
		ev := events[i]
		for _, sub := range subs {
			if sub.Filters.MatchesEvent(ev) {
				select {
				case n.sendOnly <- deliveryTask{Subscription: sub, Event: ev}:
				default:
					n.log.Warn("webhook delivery queue full; dropping event",
						"event_id", ev.ID, "subscription_id", sub.ID)
				}
			}
		}
	}
}

// Run starts the worker pool and blocks until ctx is cancelled. Workers
// drain the delivery queue and POST events to subscriber URLs.
func (n *Notifier) Run(ctx context.Context) {
	n.log.Info("webhook delivery workers starting",
		"workers", NumWorkers, "queue_size", WorkerQueueSize)
	for i := 0; i < NumWorkers; i++ {
		go n.worker(ctx)
	}
	<-ctx.Done()
	n.log.Info("webhook delivery workers stopping")
}

func (n *Notifier) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-n.queue:
			n.deliverWithRetry(ctx, task)
		}
	}
}

// deliverWithRetry attempts to deliver an event to a subscriber up to
// MaxDeliveryAttempts times with exponential backoff. On success it
// resets the subscription's failure count. On final failure it
// auto-disables the subscription.
func (n *Notifier) deliverWithRetry(ctx context.Context, task deliveryTask) {
	payload := Payload{Event: task.Event}
	body, err := json.Marshal(payload)
	if err != nil {
		// JSON marshal failure is terminal (won't fix itself on retry).
		n.recordAttempt(ctx, task, 0, 0, err)
		n.incrementFailures(ctx, task.Subscription.ID)
		return
	}

	sig := Sign(task.Subscription.Secret, body)

	for attempt := 0; attempt < n.maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := n.backoffFunc(attempt)
			n.log.Debug("webhook retry",
				"subscription_id", task.Subscription.ID,
				"event_id", task.Event.ID,
				"attempt", attempt+1,
				"backoff", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, task.Subscription.URL, bytes.NewReader(body))
		if err != nil {
			n.recordAttempt(ctx, task, 0, 0, err)
			n.incrementFailures(ctx, task.Subscription.ID)
			return // bad URL is terminal
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(SignatureHeader, sig)

		start := time.Now()
		resp, err := n.client.Do(req)
		dur := time.Since(start)

		if err != nil {
			n.log.Warn("webhook delivery failed",
				"subscription_id", task.Subscription.ID,
				"event_id", task.Event.ID,
				"attempt", attempt+1,
				"error", err)
			n.recordAttempt(ctx, task, 0, dur, err)
			continue
		}

		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Success.
			n.recordAttempt(ctx, task, resp.StatusCode, dur, nil)
			n.recordSuccess(ctx, task.Subscription.ID)
			n.log.Debug("webhook delivered",
				"subscription_id", task.Subscription.ID,
				"event_id", task.Event.ID,
				"status", resp.StatusCode)
			return
		}

		// Non-2xx response — treat as failure.
		n.log.Warn("webhook delivery got non-2xx",
			"subscription_id", task.Subscription.ID,
			"event_id", task.Event.ID,
			"attempt", attempt+1,
			"status", resp.StatusCode)
		n.recordAttempt(ctx, task, resp.StatusCode, dur,
			fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	// All attempts exhausted — auto-disable.
	n.log.Warn("webhook auto-disabling subscription after max failures",
		"subscription_id", task.Subscription.ID,
		"event_id", task.Event.ID)
	n.incrementFailures(ctx, task.Subscription.ID)
}

func (n *Notifier) recordAttempt(ctx context.Context, task deliveryTask, statusCode int, dur time.Duration, err error) {
	a := store.DeliveryAttempt{
		SubscriptionID: task.Subscription.ID,
		EventID:        task.Event.ID,
		Status:         store.DeliverySuccess,
		ResponseCode:   statusCode,
		DurationMs:     int(dur.Milliseconds()),
	}
	if err != nil {
		a.Status = store.DeliveryFailed
		a.Error = err.Error()
	}
	if _, dbErr := n.store.RecordDeliveryAttempt(ctx, a); dbErr != nil {
		n.log.Error("recording delivery attempt", "error", dbErr)
	}
}

// incrementFailures bumps the failure counter and, when it reaches
// MaxDeliveryAttempts, disables the subscription. This is called after
// each failure (per attempt or for terminal errors) so the cumulative
// effect across attempts hits the threshold.
func (n *Notifier) incrementFailures(ctx context.Context, subID int64) {
	count, disabled, err := n.store.IncrementSubscriptionFailures(ctx, subID, n.maxAttempts)
	if err != nil {
		n.log.Error("incrementing subscription failure count",
			"subscription_id", subID, "error", err)
		return
	}
	if disabled {
		n.log.Warn("subscription auto-disabled",
			"subscription_id", subID,
			"failure_count", count)
	}
}

// recordSuccess resets the failure count after a successful delivery.
func (n *Notifier) recordSuccess(ctx context.Context, subID int64) {
	if err := n.store.ResetSubscriptionFailures(ctx, subID); err != nil {
		n.log.Error("resetting subscription failure count",
			"subscription_id", subID, "error", err)
	}
}

// Sign returns the hex-encoded HMAC-SHA256 digest of body using secret
// as the key. Subscribers recompute this to verify the payload
// authenticity and integrity.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// backoffDuration computes the retry delay for attempt n (0-indexed).
// Delay = InitialBackoff * 2^n, capped at MaxBackoff.
func backoffDuration(attempt int) time.Duration {
	d := InitialBackoff * time.Duration(int64(math.Pow(2, float64(attempt))))
	if d > MaxBackoff {
		d = MaxBackoff
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
