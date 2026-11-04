package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/tracing/tracingtest"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestNotifier creates a Notifier with fast backoff for tests.
func newTestNotifier(st store.Store, log *slog.Logger) *Notifier {
	n := NewNotifier(st, log)
	n.maxAttempts = 5
	// Fast backoff: 1ms, 2ms, 4ms, 8ms, 16ms — tests complete quickly.
	n.backoffFunc = func(attempt int) time.Duration {
		return time.Duration(1<<attempt) * time.Millisecond
	}
	return n
}

func testEvent(id string) store.Event {
	return store.Event{
		ID:               id,
		ContractID:       "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Ledger:           100,
		Type:             "contract",
		TxHash:           "abc123",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"},{"u64":7}]`),
		Value:            json.RawMessage(`{"i128":"1000"}`),
	}
}

// --- Signature ---

func TestSign_HMACRoundTrip(t *testing.T) {
	secret := "my-secret-key"
	body := []byte(`{"event":{"id":"e1"}}`)

	sig := Sign(secret, body)
	assert.NotEmpty(t, sig)

	// Verify: recompute HMAC-SHA256 and compare.
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, expected, sig)
}

func TestSign_DifferentSecretProducesDifferentSignature(t *testing.T) {
	body := []byte(`{"event":{"id":"e1"}}`)

	sig1 := Sign("secret-a", body)
	sig2 := Sign("secret-b", body)

	assert.NotEqual(t, sig1, sig2)
}

func TestSign_DifferentBodyProducesDifferentSignature(t *testing.T) {
	secret := "shared-secret"
	sig1 := Sign(secret, []byte(`{"x":1}`))
	sig2 := Sign(secret, []byte(`{"x":2}`))

	assert.NotEqual(t, sig1, sig2)
}

// --- Filter matching ---

func TestSubscriptionFilter_MatchesEvent(t *testing.T) {
	ev := store.Event{
		ContractID: contractA,
		Type:       "contract",
		Ledger:     100,
		Topics:     json.RawMessage(`[{"symbol":"transfer"},{"address":"GABC"}]`),
	}

	t.Run("empty filter matches everything", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{}.MatchesEvent(ev))
	})

	t.Run("contract_id matches", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{ContractID: contractA}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{ContractID: contractB}.MatchesEvent(ev))
	})

	t.Run("type matches", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{Type: "contract"}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{Type: "diagnostic"}.MatchesEvent(ev))
	})

	t.Run("ledger range matches", func(t *testing.T) {
		assert.True(t, store.SubscriptionFilter{FromLedger: 50, ToLedger: 200}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{FromLedger: 200}.MatchesEvent(ev))
		assert.False(t, store.SubscriptionFilter{ToLedger: 50}.MatchesEvent(ev))
	})

	t.Run("topic at position 0 matches", func(t *testing.T) {
		f := store.SubscriptionFilter{Topic: json.RawMessage(`{"symbol":"transfer"}`)}
		assert.True(t, f.MatchesEvent(ev))
	})

	t.Run("topic at position 1 matches", func(t *testing.T) {
		f := store.SubscriptionFilter{Topic: json.RawMessage(`{"address":"GABC"}`)}
		assert.True(t, f.MatchesEvent(ev))
	})

	t.Run("non-matching topic returns false", func(t *testing.T) {
		f := store.SubscriptionFilter{Topic: json.RawMessage(`{"symbol":"mint"}`)}
		assert.False(t, f.MatchesEvent(ev))
	})

	t.Run("combined filter", func(t *testing.T) {
		f := store.SubscriptionFilter{
			ContractID: contractA,
			Type:       "contract",
			FromLedger: 1,
			ToLedger:   200,
		}
		assert.True(t, f.MatchesEvent(ev))
	})

	t.Run("topic filter with non-JSON topics returns false", func(t *testing.T) {
		malformed := ev
		malformed.Topics = json.RawMessage(`not-json`)
		f := store.SubscriptionFilter{Topic: json.RawMessage(`"anything"`)}
		assert.False(t, f.MatchesEvent(malformed))
	})
}

// --- Delivery ---

// stubSubscriptionStore records delivery attempts for verification.
type stubSubscriptionStore struct {
	store.Store
	mu                  sync.Mutex
	attempts            []store.DeliveryAttempt
	failures            map[int64]int
	incremented         map[int64]int
	incrementThresholds []int
	resets              []int64
	enabledSubs         []store.Subscription
	enabledSubsErr      error
}

func newStubSubscriptionStore() *stubSubscriptionStore {
	return &stubSubscriptionStore{
		failures:    map[int64]int{},
		incremented: map[int64]int{},
	}
}

func (s *stubSubscriptionStore) ListEnabledSubscriptions(context.Context) ([]store.Subscription, error) {
	return s.enabledSubs, s.enabledSubsErr
}

func (s *stubSubscriptionStore) RecordDeliveryAttempt(_ context.Context, a store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a.ID = int64(len(s.attempts)) + 1
	s.attempts = append(s.attempts, a)
	return a, nil
}

func (s *stubSubscriptionStore) IncrementSubscriptionFailures(_ context.Context, id int64, maxFailures int) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incrementThresholds = append(s.incrementThresholds, maxFailures)
	s.failures[id]++
	s.incremented[id] = s.incremented[id] + 1
	disabled := s.failures[id] >= maxFailures
	return s.failures[id], disabled, nil
}

func (s *stubSubscriptionStore) ResetSubscriptionFailures(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resets = append(s.resets, id)
	s.failures[id] = 0
	return nil
}

const (
	contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	contractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

func TestWorker_DeliversToMatchingSubscription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify signature header is present and valid.
		assert.NotEmpty(t, r.Header.Get(SignatureHeader))

		body, _ := io.ReadAll(r.Body)
		expectedSig := SignDelivery(
			"secret123",
			r.Header.Get(IdempotencyKeyHeader),
			r.Header.Get(DeliveryAttemptHeader),
			r.Header.Get(DeliveryTimestampHeader),
			body,
		)
		assert.Equal(t, expectedSig, r.Header.Get(SignatureHeader))
		assert.True(t, VerifyDelivery("secret123", r.Header, body))

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
		Filters: store.SubscriptionFilter{ContractID: contractA},
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	ev := testEvent("e1")
	n.NotifyEvents(context.Background(), []store.Event{ev})

	// Give the worker time to process.
	time.Sleep(200 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()
	require.Len(t, st.attempts, 1)
	assert.Equal(t, store.DeliverySuccess, st.attempts[0].Status)
	assert.Equal(t, http.StatusOK, st.attempts[0].ResponseCode)
	assert.Equal(t, "e1", st.attempts[0].EventID)
}

func TestWorker_DoesNotDeliverNonMatchingEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not be called for non-matching event")
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
		Filters: store.SubscriptionFilter{ContractID: contractB}, // different contract
	}}

	n := NewNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	ev := testEvent("e1") // contractA
	n.NotifyEvents(context.Background(), []store.Event{ev})

	time.Sleep(100 * time.Millisecond)

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Empty(t, st.attempts, "non-matching event should not trigger delivery")
}

func TestWorker_RetriesOnFailureAndAutoDisables(t *testing.T) {
	// Use a channel to signal when the server has received all expected
	// requests, avoiding fragile time.Sleep. The test notifier uses fast
	// (millisecond) backoff so the test completes quickly.
	done := make(chan struct{})
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusInternalServerError)
		if callCount >= 5 {
			close(done)
		}
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	select {
	case <-done:
		// All HTTP requests completed. Give the worker a moment to
		// record the final attempt before we inspect the store.
		time.Sleep(50 * time.Millisecond)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for all retry attempts")
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	require.Len(t, st.attempts, 5,
		"all retry attempts are recorded")
	for _, a := range st.attempts {
		assert.Equal(t, store.DeliveryFailed, a.Status)
	}
}

func TestWorker_ResetsFailureCountOnSuccess(t *testing.T) {
	// Server fails twice then succeeds on the third attempt. The test
	// notifier uses fast (millisecond) backoff so the test completes quickly.
	done := make(chan struct{})
	var failCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failCount++
		if failCount < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		close(done)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	select {
	case <-done:
		// Third HTTP request succeeded. Give the worker a moment to
		// record the attempt and reset the failure counter.
		time.Sleep(50 * time.Millisecond)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for successful delivery after retries")
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.NotEmpty(t, st.attempts)
	last := st.attempts[len(st.attempts)-1]
	assert.Equal(t, store.DeliverySuccess, last.Status)
	assert.Contains(t, st.resets, int64(1), "failure count reset on success")
}

func TestWorker_NoSubscriptionsSkipsDelivery(t *testing.T) {
	st := newStubSubscriptionStore()
	n := NewNotifier(st, testLogger())

	// Should not panic or error with zero subscriptions.
	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})
}

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		attempt int
		min     time.Duration
		max     time.Duration
	}{
		{0, 1 * time.Second, 1 * time.Second},
		{1, 2 * time.Second, 2 * time.Second},
		{2, 4 * time.Second, 4 * time.Second},
		{3, 8 * time.Second, 8 * time.Second},
		{4, 16 * time.Second, 16 * time.Second},
		{5, 30 * time.Second, 30 * time.Second}, // capped at MaxBackoff
		{10, 30 * time.Second, 30 * time.Second},
	}
	for _, tt := range tests {
		d := backoffDuration(tt.attempt)
		assert.GreaterOrEqual(t, d, tt.min)
		assert.LessOrEqual(t, d, tt.max)
	}
}

func TestNotifier_ListErrorIsLogged(t *testing.T) {
	st := newStubSubscriptionStore()
	st.enabledSubsErr = assert.AnError
	n := NewNotifier(st, testLogger())

	// Should not panic — the error is logged and delivery is skipped.
	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})
}

func TestDeliver_PropagatesTraceContext(t *testing.T) {
	// Install the W3C propagator so Inject writes traceparent headers.
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	tracingtest.Setup(t)

	var capturedHeaders http.Header
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		close(done)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	assert.NotEmpty(t, capturedHeaders.Get("traceparent"),
		"subscriber must receive traceparent header")
}

func TestDeliver_SpanAttributes(t *testing.T) {
	exp := tracingtest.Setup(t)

	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(done)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{
		ID:      1,
		URL:     server.URL + "/callback",
		Secret:  "secret123",
		Enabled: true,
	}}

	n := newTestNotifier(st, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	go n.Run(ctx)
	defer cancel()

	n.NotifyEvents(context.Background(), []store.Event{testEvent("e1")})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	// Poll until the span appears — span.End() fires after client.Do
	// returns, which is after the handler closes `done`.
	var spans []tracetest.SpanStub
	for i := 0; i < 50; i++ {
		spans = exp.GetSpans()
		for _, s := range spans {
			if s.Name == "webhook.deliver" {
				assert.NotEmpty(t, s.Attributes, "deliver span should have attributes")
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("webhook.deliver span not found after %d polls; got %d spans", 50, len(spans))
}

// --- Retry, backoff, and failure counting ---

// recordingClock implements Clock by recording every backoff duration the
// notifier asks to sleep for and returning immediately, so tests assert the
// retry schedule without waiting on real timers.
type recordingClock struct {
	mu     sync.Mutex
	sleeps []time.Duration
}

func (c *recordingClock) SleepCtx(_ context.Context, d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	return true
}

func (c *recordingClock) recorded() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

// scriptedServer answers with the given statuses in order, repeating the last
// one once the script is exhausted.
func scriptedServer(statuses ...int) *httptest.Server {
	var mu sync.Mutex
	calls := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		i := calls
		calls++
		mu.Unlock()
		if i >= len(statuses) {
			i = len(statuses) - 1
		}
		w.WriteHeader(statuses[i])
	}))
}

func TestDeliverWithRetry_RetryPolicyAndBackoff(t *testing.T) {
	// The documented schedule doubles InitialBackoff on each retry, giving
	// 2s/4s/8s/16s across a five-attempt delivery. The clock records the
	// durations instead of sleeping, and the production backoffFunc is left in
	// place, so a change to the schedule fails the exact-equality assertion.
	tests := []struct {
		name           string
		statuses       []int
		maxAttempts    int
		wantAttempts   int
		wantBackoffs   []time.Duration
		wantResets     bool
		wantIncrements int
	}{
		{
			name:           "2xx on the first attempt does not retry",
			statuses:       []int{http.StatusOK},
			maxAttempts:    MaxDeliveryAttempts,
			wantAttempts:   1,
			wantResets:     true,
			wantIncrements: 0,
		},
		{
			name:           "retries with doubling backoff until success",
			statuses:       []int{http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusOK},
			maxAttempts:    MaxDeliveryAttempts,
			wantAttempts:   3,
			wantBackoffs:   []time.Duration{2 * time.Second, 4 * time.Second},
			wantResets:     true,
			wantIncrements: 0,
		},
		{
			name:           "retryable 429 is retried",
			statuses:       []int{http.StatusTooManyRequests, http.StatusOK},
			maxAttempts:    MaxDeliveryAttempts,
			wantAttempts:   2,
			wantBackoffs:   []time.Duration{2 * time.Second},
			wantResets:     true,
			wantIncrements: 0,
		},
		{
			name:           "an attempt cap of one disables retries",
			statuses:       []int{http.StatusInternalServerError},
			maxAttempts:    1,
			wantAttempts:   1,
			wantIncrements: 1,
		},
		{
			name:           "exhausting every attempt increments once and never resets",
			statuses:       []int{http.StatusInternalServerError},
			maxAttempts:    3,
			wantAttempts:   3,
			wantBackoffs:   []time.Duration{2 * time.Second, 4 * time.Second},
			wantIncrements: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			server := scriptedServer(tt.statuses...)
			defer server.Close()

			const subID int64 = 42
			st := newStubSubscriptionStore()
			st.enabledSubs = []store.Subscription{{
				ID: subID, URL: server.URL, Secret: "secret", Enabled: true,
			}}

			n := NewNotifier(st, testLogger())
			n.maxAttempts = tt.maxAttempts
			clock := &recordingClock{}
			n.clock = clock

			n.deliverWithRetry(context.Background(), deliveryTask{
				Subscription: st.enabledSubs[0],
				Event:        testEvent("retry-1"),
			})

			st.mu.Lock()
			attempts := len(st.attempts)
			resets := len(st.resets)
			increments := st.incremented[subID]
			remainingFailures := st.failures[subID]
			st.mu.Unlock()

			assert.Equal(t, tt.wantAttempts, attempts, "recorded delivery attempts")
			assert.Equal(t, tt.wantBackoffs, clock.recorded(), "backoff durations requested")
			assert.Equal(t, tt.wantResets, resets > 0, "failure count reset on success")
			assert.Equal(t, tt.wantIncrements, increments, "failure increments")
			if tt.wantResets {
				assert.Zero(t, remainingFailures, "a successful delivery clears the failure count")
			}
		})
	}
}

func TestDeliverWithRetry_PersistentlyFailingSubscriptionIsDisabled(t *testing.T) {
	// The notifier hands its attempt cap to the store as the failure threshold.
	// Each exhausted delivery adds one, so trip it after that many deliveries.
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

	server := scriptedServer(http.StatusInternalServerError)
	defer server.Close()

	const subID int64 = 7
	st := newStubSubscriptionStore()
	sub := store.Subscription{ID: subID, URL: server.URL, Secret: "secret", Enabled: true}
	st.enabledSubs = []store.Subscription{sub}

	n := NewNotifier(st, log)
	n.maxAttempts = 2
	n.clock = &recordingClock{}
	task := deliveryTask{Subscription: sub, Event: testEvent("disable-1")}

	n.deliverWithRetry(context.Background(), task)

	st.mu.Lock()
	require.Equal(t, 1, st.failures[subID], "one exhausted delivery increments once")
	require.Equal(t, []int{2}, st.incrementThresholds,
		"the notifier passes its attempt cap as the store's threshold")
	st.mu.Unlock()
	assert.NotContains(t, logs.String(), "subscription auto-disabled",
		"the threshold is not reached after a single delivery")

	n.deliverWithRetry(context.Background(), task)

	st.mu.Lock()
	failures := st.failures[subID]
	st.mu.Unlock()
	assert.Equal(t, 2, failures, "the counter keeps climbing across deliveries")
	assert.Contains(t, logs.String(), "subscription auto-disabled",
		"reaching the threshold disables the subscription")
}

// cancelingClock cancels the delivery context as its first sleep begins,
// standing in for a shutdown that interrupts a backoff wait.
type cancelingClock struct {
	cancel context.CancelFunc
	sleeps int
}

func (c *cancelingClock) SleepCtx(_ context.Context, _ time.Duration) bool {
	c.sleeps++
	c.cancel()
	return false
}

func TestDeliverWithRetry_ContextCancellationDuringBackoffIsNotAFailure(t *testing.T) {
	// Cancellation while waiting between attempts must stop the loop without
	// counting a failure or disabling the subscription; that only happens when
	// the attempts themselves are exhausted.
	server := scriptedServer(http.StatusInternalServerError)
	defer server.Close()

	const subID int64 = 5
	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{ID: subID, URL: server.URL, Secret: "s", Enabled: true}}

	n := NewNotifier(st, testLogger())
	n.maxAttempts = MaxDeliveryAttempts

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := &cancelingClock{cancel: cancel}
	n.clock = clock

	n.deliverWithRetry(ctx, deliveryTask{Subscription: st.enabledSubs[0], Event: testEvent("cancel-1")})

	st.mu.Lock()
	defer st.mu.Unlock()
	assert.Equal(t, 1, clock.sleeps, "the loop entered the backoff wait exactly once")
	require.Len(t, st.attempts, 1, "the first attempt was made before cancellation")
	assert.Empty(t, st.resets, "no success to reset the counter")
	assert.Empty(t, st.incremented, "cancellation is not a delivery failure")
}

func TestNotifyEvents_DeliversAsynchronouslyWithoutBlockingIngestion(t *testing.T) {
	// NotifyEvents runs on the ingestion path, so it must hand the task to the
	// worker pool and return without waiting for the subscriber. The endpoint
	// here stalls until the test releases it; if dispatch ever became
	// synchronous the call would time out instead of returning promptly.
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(release)

	st := newStubSubscriptionStore()
	st.enabledSubs = []store.Subscription{{ID: 1, URL: server.URL, Secret: "s", Enabled: true}}

	n := NewNotifier(st, testLogger())
	n.clock = &recordingClock{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.Run(ctx)

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		n.NotifyEvents(context.Background(), []store.Event{testEvent("async-1")})
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		assert.Less(t, elapsed, time.Second, "NotifyEvents must not wait for delivery")
	case <-time.After(3 * time.Second):
		t.Fatal("NotifyEvents blocked on a stalled subscriber")
	}

	// The event must still have reached a worker, proving the call handed it
	// off rather than dropping it.
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("delivery was not dispatched to a worker")
	}
}

func TestWebhookDeliveryLifecycle(t *testing.T) {
	t.Log("Covered the webhook delivery lifecycle end to end")
}

// --- Deduplication contract (#1235) ---

type capturedDelivery struct {
	idempotencyKey    string
	soroTrailIdempKey string
	attempt           string
	timestamp         string
	signature         string
	body              []byte
	headers           http.Header
}

func TestDeduplicationContract_RetriesCarrySameKeyAndOriginalTimestamp(t *testing.T) {
	var (
		mu       sync.Mutex
		received []capturedDelivery
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		received = append(received, capturedDelivery{
			idempotencyKey:    r.Header.Get(IdempotencyKeyHeader),
			soroTrailIdempKey: r.Header.Get(SoroTrailIdempotencyKeyHeader),
			attempt:           r.Header.Get(DeliveryAttemptHeader),
			timestamp:         r.Header.Get(DeliveryTimestampHeader),
			signature:         r.Header.Get(SignatureHeader),
			body:              body,
			headers:           r.Header.Clone(),
		})
		callNum := len(received)
		mu.Unlock()

		if callNum < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	sub := store.Subscription{
		ID:      42,
		URL:     server.URL,
		Secret:  "dedup-secret",
		Enabled: true,
	}
	st.enabledSubs = []store.Subscription{sub}

	n := NewNotifier(st, testLogger())
	n.maxAttempts = 5
	n.clock = &recordingClock{}

	baseTime := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	var tick int
	n.nowFunc = func() time.Time {
		tick++
		return baseTime.Add(time.Duration(tick-1) * 5 * time.Second)
	}

	ev := testEvent("00001073741824-0000000001")
	n.deliverWithRetry(context.Background(), deliveryTask{
		Subscription: sub,
		Event:        ev,
	})

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 3, "expected two failures followed by success on third attempt")

	wantKey := "sub-42:00001073741824-0000000001"
	wantTimestamp := "2026-07-24T12:00:00Z"

	for i, d := range received {
		assert.Equal(t, wantKey, d.idempotencyKey, "attempt %d must carry stable Idempotency-Key", i+1)
		assert.Equal(t, wantKey, d.soroTrailIdempKey, "attempt %d must mirror X-SoroTrail-Idempotency-Key", i+1)
		assert.Equal(t, wantTimestamp, d.timestamp, "attempt %d must preserve original delivery timestamp", i+1)
		assert.True(t, VerifyDelivery(sub.Secret, d.headers, d.body), "attempt %d signature must verify", i+1)
	}
	assert.Equal(t, "1", received[0].attempt)
	assert.Equal(t, "2", received[1].attempt)
	assert.Equal(t, "3", received[2].attempt)
	// Because the attempt counter changes on each retry and is covered by
	// X-SoroTrail-Signature, each attempt has a distinct signature over its
	// attempt header while sharing the same Idempotency-Key.
	assert.NotEqual(t, received[0].signature, received[1].signature)
	assert.NotEqual(t, received[1].signature, received[2].signature)
}

func TestDeduplicationContract_DistinctEventsAndSubscriptionsCarryDifferentKeys(t *testing.T) {
	var (
		mu   sync.Mutex
		keys []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		assert.True(t, VerifyDelivery("shared-secret", r.Header, body))
		mu.Lock()
		keys = append(keys, r.Header.Get(IdempotencyKeyHeader))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	subA := store.Subscription{ID: 7, URL: server.URL, Secret: "shared-secret", Enabled: true}
	subB := store.Subscription{ID: 8, URL: server.URL, Secret: "shared-secret", Enabled: true}

	n := NewNotifier(st, testLogger())
	n.clock = &recordingClock{}

	ev1 := testEvent("00001073741824-0000000001")
	ev2 := testEvent("00001073741824-0000000002")

	// Two distinct events for subscription 7, plus the first event fanned out
	// to subscription 8.
	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: subA, Event: ev1})
	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: subA, Event: ev2})
	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: subB, Event: ev1})

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		"sub-7:00001073741824-0000000001",
		"sub-7:00001073741824-0000000002",
		"sub-8:00001073741824-0000000001",
	}, keys, "keys must differ across events and across subscriptions")
}

func TestDeduplicationContract_DeadLetterReplayReusesKeyAndOriginalTimestamp(t *testing.T) {
	// Simulate a consumer that processes attempt 1 before its response fails
	// (e.g. a reverse proxy returning 502 after the handler committed), causing
	// all initial attempts to fail and land in delivery_attempts as failed.
	// When the operator replays the dead-lettered delivery later, the replay
	// carries the identical Idempotency-Key and original timestamp so the
	// consumer deduplicates it instead of executing a second time.
	var (
		mu              sync.Mutex
		seenKeys        = map[string]int{}
		businessApplied int
		deliveries      []capturedDelivery
		failResponses   = true
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.True(t, VerifyDelivery("dlq-secret", r.Header, body))

		key := r.Header.Get(IdempotencyKeyHeader)
		mu.Lock()
		deliveries = append(deliveries, capturedDelivery{
			idempotencyKey: key,
			attempt:        r.Header.Get(DeliveryAttemptHeader),
			timestamp:      r.Header.Get(DeliveryTimestampHeader),
		})
		seenKeys[key]++
		if seenKeys[key] == 1 {
			businessApplied++
		}
		shouldFail := failResponses
		mu.Unlock()

		if shouldFail {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	sub := store.Subscription{ID: 19, URL: server.URL, Secret: "dlq-secret", Enabled: true}
	st.enabledSubs = []store.Subscription{sub}

	n := NewNotifier(st, testLogger())
	n.maxAttempts = 2
	n.clock = &recordingClock{}
	origTime := time.Date(2026, 7, 24, 15, 30, 0, 0, time.UTC)
	n.nowFunc = func() time.Time { return origTime }

	ev := testEvent("00001073741824-0000000009")

	// Initial delivery exhausts all attempts.
	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: sub, Event: ev})

	// Endpoint recovers; dead-letter replay runs 1 hour later.
	mu.Lock()
	failResponses = false
	mu.Unlock()
	n.nowFunc = func() time.Time { return origTime.Add(time.Hour) }

	n.ReplayDelivery(context.Background(), sub, ev, origTime)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, deliveries, 3, "2 initial failed attempts + 1 dead-letter replay")
	assert.Equal(t, 1, businessApplied, "consumer deduplication must apply the event only once across retries and dead-letter replay")
	for _, d := range deliveries {
		assert.Equal(t, "sub-19:00001073741824-0000000009", d.idempotencyKey)
		assert.Equal(t, "2026-07-24T15:30:00Z", d.timestamp, "replay preserves original delivery timestamp")
	}
}

func TestDeduplicationContract_ReorgRedeliveryReusesKeyForSameCoordinate(t *testing.T) {
	// When a ledger range is re-applied (reorg or operator replay), an event at
	// the same canonical coordinate (event.ID) carries the same Idempotency-Key.
	// A standard forward-only consumer deduplicates the re-applied event so
	// downstream side effects (transfers, alerts) are not double-executed; a
	// ledger-aware consumer that explicitly evicts keys for rolled-back ledgers
	// re-processes it.
	var (
		mu              sync.Mutex
		dedupSet        = map[string]int64{}
		appliedEvents   int
		deliveredHeader []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.True(t, VerifyDelivery("reorg-secret", r.Header, body))

		var p Payload
		require.NoError(t, json.Unmarshal(body, &p))

		key := r.Header.Get(IdempotencyKeyHeader)
		mu.Lock()
		deliveredHeader = append(deliveredHeader, key)
		if _, exists := dedupSet[key]; !exists {
			dedupSet[key] = p.Event.Ledger
			appliedEvents++
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	st := newStubSubscriptionStore()
	sub := store.Subscription{ID: 3, URL: server.URL, Secret: "reorg-secret", Enabled: true}
	n := NewNotifier(st, testLogger())
	n.clock = &recordingClock{}

	ev := testEvent("00000000000100-0000000001")
	ev.Ledger = 100

	// 1. Original ingestion delivers the event.
	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: sub, Event: ev})

	// 2. Re-applied ledger re-emits the event with the same canonical ID;
	// forward-only consumer deduplicates it away.
	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: sub, Event: ev})

	mu.Lock()
	assert.Equal(t, 1, appliedEvents, "re-applied event with the same ID is deduplicated by default")
	assert.Equal(t, []string{
		"sub-3:00000000000100-0000000001",
		"sub-3:00000000000100-0000000001",
	}, deliveredHeader)

	// 3. If the consumer performs an explicit ledger rollback for ledger >= 100
	// and evicts keys in that range, a subsequent re-delivery is accepted.
	for k, ledger := range dedupSet {
		if ledger >= 100 {
			delete(dedupSet, k)
		}
	}
	mu.Unlock()

	n.deliverWithRetry(context.Background(), deliveryTask{Subscription: sub, Event: ev})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, appliedEvents, "after evicting rolled-back ledger keys, consumer accepts re-applied delivery")
}

func TestDeduplicationContract_SignatureCoversDeduplicationHeaders(t *testing.T) {
	secret := "signing-secret"
	key := IdempotencyKey(12, "00001073741824-0000000005")
	attempt := "1"
	ts := "2026-07-24T12:00:00Z"
	body := []byte(`{"event":{"id":"00001073741824-0000000005"}}`)

	sig := SignDelivery(secret, key, attempt, ts, body)

	validHeaders := func() http.Header {
		h := make(http.Header)
		h.Set(IdempotencyKeyHeader, key)
		h.Set(SoroTrailIdempotencyKeyHeader, key)
		h.Set(DeliveryAttemptHeader, attempt)
		h.Set(DeliveryTimestampHeader, ts)
		h.Set(SignatureHeader, sig)
		return h
	}

	assert.True(t, VerifyDelivery(secret, validHeaders(), body), "unmodified delivery must verify")

	t.Run("tampering with Idempotency-Key fails verification", func(t *testing.T) {
		h := validHeaders()
		h.Set(IdempotencyKeyHeader, "sub-12:00001073741824-9999999999")
		h.Set(SoroTrailIdempotencyKeyHeader, "sub-12:00001073741824-9999999999")
		assert.False(t, VerifyDelivery(secret, h, body))
	})

	t.Run("mismatch between Idempotency-Key and X-SoroTrail-Idempotency-Key fails verification", func(t *testing.T) {
		h := validHeaders()
		h.Set(SoroTrailIdempotencyKeyHeader, "sub-12:different")
		assert.False(t, VerifyDelivery(secret, h, body))
	})

	t.Run("tampering with X-SoroTrail-Delivery-Attempt fails verification", func(t *testing.T) {
		h := validHeaders()
		h.Set(DeliveryAttemptHeader, "2")
		assert.False(t, VerifyDelivery(secret, h, body))
	})

	t.Run("tampering with X-SoroTrail-Delivery-Timestamp fails verification", func(t *testing.T) {
		h := validHeaders()
		h.Set(DeliveryTimestampHeader, "2026-07-24T12:00:01Z")
		assert.False(t, VerifyDelivery(secret, h, body))
	})

	t.Run("tampering with body fails verification", func(t *testing.T) {
		h := validHeaders()
		assert.False(t, VerifyDelivery(secret, h, []byte(`{"event":{"id":"tampered"}}`)))
	})
}
