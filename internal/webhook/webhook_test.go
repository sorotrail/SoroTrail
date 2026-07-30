package webhook

import (
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

	"github.com/sorotrail/sorotrail/internal/store"
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
	mu             sync.Mutex
	attempts       []store.DeliveryAttempt
	failures       map[int64]int
	incremented    map[int64]int
	resets         []int64
	enabledSubs    []store.Subscription
	enabledSubsErr error
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
		expectedSig := Sign("secret123", body)
		assert.Equal(t, expectedSig, r.Header.Get(SignatureHeader))

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
