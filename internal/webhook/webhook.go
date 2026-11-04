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
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/sorotrail/sorotrail/internal/requestid"
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
	// digest of the delivery metadata headers and request body, signed
	// with the subscription's secret.
	SignatureHeader = "X-SoroTrail-Signature"

	// IdempotencyKeyHeader is the standard HTTP header carrying the stable,
	// subscription-and-event-scoped deduplication key ("sub-<sub_id>:<event_id>").
	// It is identical across retries and dead-letter replays of the same delivery.
	IdempotencyKeyHeader = "Idempotency-Key"

	// SoroTrailIdempotencyKeyHeader mirrors IdempotencyKeyHeader under the
	// X-SoroTrail-* namespace for consumers that inspect vendor-prefixed headers.
	SoroTrailIdempotencyKeyHeader = "X-SoroTrail-Idempotency-Key"

	// DeliveryAttemptHeader carries the 1-based attempt number ("1", "2", ...)
	// so consumers can distinguish an initial delivery from a retry.
	DeliveryAttemptHeader = "X-SoroTrail-Delivery-Attempt"

	// DeliveryTimestampHeader carries the RFC3339 UTC timestamp of the original
	// delivery attempt. It remains fixed across retries and dead-letter replays
	// of the same delivery.
	DeliveryTimestampHeader = "X-SoroTrail-Delivery-Timestamp"
)

// Clock abstracts the retry sleep. Production uses RealClock; tests inject a
// recording clock so the backoff schedule is asserted without real timers —
// mirroring ingester.Clock's single sleep seam.
type Clock interface {
	// SleepCtx sleeps for d or until ctx is done; it reports whether the
	// full sleep completed.
	SleepCtx(ctx context.Context, d time.Duration) bool
}

// RealClock is the wall-clock implementation of Clock.
type RealClock struct{}

func (RealClock) SleepCtx(ctx context.Context, d time.Duration) bool { return sleepCtx(ctx, d) }

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
	clock       Clock
	nowFunc     func() time.Time
}

// deliveryTask is one event destined for one subscription.
type deliveryTask struct {
	Subscription      store.Subscription
	Event             store.Event
	OriginalTimestamp time.Time
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
		store:    st,
		queue:    queue,
		sendOnly: queue,
		// Deliveries are background work; tagging their log lines with a
		// stable job id keeps the correlation field meaningful for them too.
		log:         log.With(requestid.Field, requestid.JobWebhook),
		client:      client,
		maxAttempts: MaxDeliveryAttempts,
		backoffFunc: backoffDuration,
		clock:       RealClock{},
		nowFunc:     time.Now,
	}
	return n
}

func (n *Notifier) now() time.Time {
	if n.nowFunc != nil {
		return n.nowFunc()
	}
	return time.Now()
}

// IdempotencyKey returns the deterministic deduplication key for delivering
// eventID to subscriptionID.
//
// Format: "sub-<subscriptionID>:<eventID>" (for example,
// "sub-42:00001073741824-0000000001").
//
// Using the zero-padded event identifier as the basis makes the key a pure
// function of the event's canonical ledger coordinate, so retries and
// dead-letter replays of the same delivery always produce the identical key,
// and re-deliveries after a replay or ledger re-application are deduplicated
// by default rather than double-triggering downstream side effects. Scoping
// the prefix by subscriptionID ensures that when a single event fans out to
// multiple subscriptions, one subscription's deduplication state cannot
// collide with another's even when both target the same receiver.
func IdempotencyKey(subscriptionID int64, eventID string) string {
	return fmt.Sprintf("sub-%d:%s", subscriptionID, eventID)
}

// ReplayDelivery re-delivers a previously failed (dead-lettered) event to sub
// using the exact same deterministic Idempotency-Key and original delivery
// timestamp (when non-zero) as the initial delivery, so replaying never looks
// like new traffic to a consumer that already processed the event before a
// timeout.
func (n *Notifier) ReplayDelivery(ctx context.Context, sub store.Subscription, event store.Event, originalTimestamp time.Time) {
	n.deliverWithRetry(ctx, deliveryTask{
		Subscription:      sub,
		Event:             event,
		OriginalTimestamp: originalTimestamp,
	})
}

// EnqueueReplay schedules a dead-letter replay on the asynchronous worker
// queue, preserving the original delivery timestamp and deterministic
// Idempotency-Key. It returns false if the worker queue is full.
func (n *Notifier) EnqueueReplay(sub store.Subscription, event store.Event, originalTimestamp time.Time) bool {
	select {
	case n.sendOnly <- deliveryTask{
		Subscription:      sub,
		Event:             event,
		OriginalTimestamp: originalTimestamp,
	}:
		return true
	default:
		n.log.Warn("webhook delivery queue full; dropping replay",
			"event_id", event.ID, "subscription_id", sub.ID)
		return false
	}
}

// webhookTracer returns the OTel tracer for the webhook package.
// Using a function ensures we always pick up the current global provider.
func webhookTracer() trace.Tracer { return otel.Tracer("sorotrail/webhook") }

// NotifyEvents is called by the ingester after persisting a batch of
// events. It finds all enabled subscriptions whose filters match each
// event and enqueues delivery tasks. Enqueuing is non-blocking for the
// caller (drops into buffered channel); the channel is large enough that
// back-pressure is unlikely in normal operation.
func (n *Notifier) NotifyEvents(ctx context.Context, events []store.Event) {
	if len(events) == 0 {
		return
	}

	_, span := webhookTracer().Start(ctx, "webhook.notify_events")
	defer span.End()

	subs, err := n.store.ListEnabledSubscriptions(ctx)
	if err != nil {
		n.log.Error("listing enabled subscriptions for webhook delivery", "error", err)
		return
	}
	if len(subs) == 0 {
		return
	}

	span.SetAttributes(attribute.Int("webhook.matching_events", len(events)),
		attribute.Int("webhook.subscriptions", len(subs)))

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
	// Carry the job id on the context so delivery-time store writes are
	// tagged with "webhook" in their slow-query logs.
	ctx = requestid.WithJob(ctx, requestid.JobWebhook)
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
	// Stamp the SEP-41 normalized envelope (when applicable) before signing
	// the body — subscribers verify the signature against the exact bytes
	// POSTed, so the augmentation has to land here, not in a post-decode
	// step on the subscriber side.
	event := task.Event
	event.WithSEP41()
	payload := Payload{Event: event}
	body, err := json.Marshal(payload)
	if err != nil {
		// JSON marshal failure is terminal (won't fix itself on retry).
		n.recordAttempt(ctx, task, 0, 0, err)
		n.incrementFailures(ctx, task.Subscription.ID)
		return
	}

	idempotencyKey := IdempotencyKey(task.Subscription.ID, task.Event.ID)
	origTime := task.OriginalTimestamp
	if origTime.IsZero() {
		origTime = n.now().UTC()
	}
	originalTimestamp := origTime.UTC().Format(time.RFC3339)

	for attempt := 0; attempt < n.maxAttempts; attempt++ {
		attemptNum := attempt + 1
		attemptStr := strconv.Itoa(attemptNum)
		attemptCtx, span := webhookTracer().Start(ctx, "webhook.deliver",
			trace.WithAttributes(
				attribute.Int("webhook.attempt", attemptNum),
				attribute.Int64("webhook.subscription_id", task.Subscription.ID),
				attribute.String("webhook.event_id", task.Event.ID),
				attribute.String("webhook.idempotency_key", idempotencyKey),
			))

		if attempt > 0 {
			backoff := n.backoffFunc(attempt)
			n.log.Debug("webhook retry",
				"subscription_id", task.Subscription.ID,
				"event_id", task.Event.ID,
				"attempt", attemptNum,
				"backoff", backoff)
			if !n.clock.SleepCtx(ctx, backoff) {
				span.End()
				return
			}
		}

		req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, task.Subscription.URL, bytes.NewReader(body))
		if err != nil {
			n.recordAttempt(attemptCtx, task, 0, 0, err)
			n.incrementFailures(ctx, task.Subscription.ID)
			span.End()
			return // bad URL is terminal
		}
		sig := SignDelivery(task.Subscription.Secret, idempotencyKey, attemptStr, originalTimestamp, body)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(IdempotencyKeyHeader, idempotencyKey)
		req.Header.Set(SoroTrailIdempotencyKeyHeader, idempotencyKey)
		req.Header.Set(DeliveryAttemptHeader, attemptStr)
		req.Header.Set(DeliveryTimestampHeader, originalTimestamp)
		req.Header.Set(SignatureHeader, sig)

		// Propagate trace context to the subscriber endpoint.
		otel.GetTextMapPropagator().Inject(attemptCtx, propagation.HeaderCarrier(req.Header))

		start := time.Now()
		resp, err := n.client.Do(req)
		dur := time.Since(start)

		if err != nil {
			n.log.Warn("webhook delivery failed",
				"subscription_id", task.Subscription.ID,
				"event_id", task.Event.ID,
				"attempt", attempt+1,
				"error", err)
			n.recordAttempt(attemptCtx, task, 0, dur, err)
			span.SetAttributes(
				attribute.Int64("webhook.duration_ms", dur.Milliseconds()),
				attribute.Bool("webhook.success", false),
			)
			span.End()
			continue
		}

		span.SetAttributes(
			attribute.Int("webhook.status_code", resp.StatusCode),
			attribute.Int64("webhook.duration_ms", dur.Milliseconds()),
		)

		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Success.
			n.recordAttempt(attemptCtx, task, resp.StatusCode, dur, nil)
			n.recordSuccess(ctx, task.Subscription.ID)
			n.log.Debug("webhook delivered",
				"subscription_id", task.Subscription.ID,
				"event_id", task.Event.ID,
				"status", resp.StatusCode)
			span.SetAttributes(attribute.Bool("webhook.success", true))
			span.End()
			return
		}

		// Non-2xx response — treat as failure.
		n.log.Warn("webhook delivery got non-2xx",
			"subscription_id", task.Subscription.ID,
			"event_id", task.Event.ID,
			"attempt", attempt+1,
			"status", resp.StatusCode)
		n.recordAttempt(attemptCtx, task, resp.StatusCode, dur,
			fmt.Errorf("HTTP %d", resp.StatusCode))
		span.SetAttributes(attribute.Bool("webhook.success", false))
		span.End()
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
// as the key. For full webhook request signing covering the deduplication
// headers and body, use SignDelivery / VerifyDelivery.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// SignDelivery returns the hex-encoded HMAC-SHA256 digest over the canonical
// delivery envelope:
//
//	<idempotency_key>\n<attempt>\n<original_timestamp>\n<raw_body>
//
// Covering the deduplication key, attempt counter, and original delivery
// timestamp inside the HMAC prevents an attacker or misbehaving proxy from
// mutating the idempotency key or replay metadata in transit without
// invalidating X-SoroTrail-Signature. When all three metadata strings are
// empty, SignDelivery falls back to Sign(secret, body).
func SignDelivery(secret, idempotencyKey, attempt, timestamp string, body []byte) string {
	if idempotencyKey == "" && attempt == "" && timestamp == "" {
		return Sign(secret, body)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(idempotencyKey))
	mac.Write([]byte("\n"))
	mac.Write([]byte(attempt))
	mac.Write([]byte("\n"))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("\n"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyDelivery verifies that h carries a valid X-SoroTrail-Signature covering
// Idempotency-Key (and X-SoroTrail-Idempotency-Key when present),
// X-SoroTrail-Delivery-Attempt, X-SoroTrail-Delivery-Timestamp, and body.
func VerifyDelivery(secret string, h http.Header, body []byte) bool {
	sig := h.Get(SignatureHeader)
	key := h.Get(IdempotencyKeyHeader)
	soroKey := h.Get(SoroTrailIdempotencyKeyHeader)
	attempt := h.Get(DeliveryAttemptHeader)
	timestamp := h.Get(DeliveryTimestampHeader)
	if sig == "" || key == "" || attempt == "" || timestamp == "" {
		return false
	}
	if soroKey != "" && soroKey != key {
		return false
	}
	expected := SignDelivery(secret, key, attempt, timestamp, body)
	return hmac.Equal([]byte(sig), []byte(expected))
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
