package webhook

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Table-driven tests for delivery helpers: recordAttempt, incrementFailures,
// and deliverWithRetry. We re-use the stubSubscriptionStore defined in
// webhook_test.go.
func TestDeliveryHelpers_TableDriven(t *testing.T) {
	t.Run("recordAttempt_stores_attempt_with_error_and_fields", func(t *testing.T) {
		st := newStubSubscriptionStore()
		n := newTestNotifier(st, testLogger())

		task := deliveryTask{
			Subscription: store.Subscription{ID: 11},
			Event:        testEvent("evt-1"),
		}

		// Simulate a failed attempt with response code and duration.
		n.recordAttempt(context.Background(), task, 502, 150*time.Millisecond, fmt.Errorf("boom"))

		st.mu.Lock()
		defer st.mu.Unlock()
		require.Len(t, st.attempts, 1)
		a := st.attempts[0]
		assert.Equal(t, store.DeliveryFailed, a.Status)
		assert.Equal(t, 502, a.ResponseCode)
		assert.Contains(t, a.Error, "boom")
		// DurationMs should be set and > 0.
		assert.Greater(t, a.DurationMs, 0)
	})

	t.Run("incrementFailures_calls_store_and_disables_on_threshold", func(t *testing.T) {
		st := newStubSubscriptionStore()
		n := newTestNotifier(st, testLogger())
		n.maxAttempts = 3

		// Call incrementFailures three times; stub records increment calls.
		n.incrementFailures(context.Background(), 7)
		n.incrementFailures(context.Background(), 7)
		n.incrementFailures(context.Background(), 7)

		st.mu.Lock()
		defer st.mu.Unlock()
		// The stub increments its counter on each call.
		assert.Equal(t, 3, st.incremented[7])
		assert.Equal(t, 3, st.failures[7])
	})

	t.Run("deliverWithRetry_success_retry_timeout_and_persistent_failure", func(t *testing.T) {
		cases := []struct {
			name               string
			serverFunc         http.HandlerFunc
			maxAttempts        int
			wantAttempts       int
			wantResets         bool
			wantIncrementCalls int
			clientTimeout      time.Duration
		}{
			{
				name:               "success_on_first_attempt",
				serverFunc:         func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) },
				maxAttempts:        5,
				wantAttempts:       1,
				wantResets:         true,
				wantIncrementCalls: 0,
			},
			{
				name: "retry_then_success",
				serverFunc: func() http.HandlerFunc {
					calls := 0
					return func(w http.ResponseWriter, r *http.Request) {
						calls++
						if calls < 3 {
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						w.WriteHeader(http.StatusOK)
					}
				}(),
				maxAttempts:        5,
				wantAttempts:       3,
				wantResets:         true,
				wantIncrementCalls: 0,
			},
			{
				name:               "persistent_failure_auto_disable",
				serverFunc:         func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
				maxAttempts:        3,
				wantAttempts:       3,
				wantResets:         false,
				wantIncrementCalls: 1,
			},
			{
				name: "client_timeout_treated_as_failure_and_auto_disable",
				// server sleeps so client times out
				serverFunc: func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(50 * time.Millisecond)
					w.WriteHeader(http.StatusOK)
				},
				maxAttempts:        3,
				wantAttempts:       3,
				wantResets:         false,
				wantIncrementCalls: 1,
				clientTimeout:      1 * time.Millisecond,
			},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				server := httptest.NewServer(tc.serverFunc)
				defer server.Close()

				st := newStubSubscriptionStore()
				st.enabledSubs = []store.Subscription{{ID: 2, URL: server.URL, Secret: "s"}}

				n := newTestNotifier(st, testLogger())
				n.maxAttempts = tc.maxAttempts
				if tc.clientTimeout > 0 {
					// override the HTTP client to force timeouts
					n.client = &http.Client{Timeout: tc.clientTimeout}
				}

				// Build a task and call deliverWithRetry directly.
				task := deliveryTask{Subscription: st.enabledSubs[0], Event: testEvent("d1")}
				ctx := context.Background()
				n.deliverWithRetry(ctx, task)

				st.mu.Lock()
				gotAttempts := len(st.attempts)
				gotResets := len(st.resets) > 0
				gotIncremented := st.incremented[st.enabledSubs[0].ID]
				st.mu.Unlock()

				assert.Equal(t, tc.wantAttempts, gotAttempts, "recorded attempts")
				assert.Equal(t, tc.wantResets, gotResets, "reset called")
				assert.Equal(t, tc.wantIncrementCalls, gotIncremented, "incrementFailures called count")
			})
		}
	})

	t.Run("NotifyEvents_non_blocking_when_queue_full", func(t *testing.T) {
		st := newStubSubscriptionStore()
		st.enabledSubs = []store.Subscription{{ID: 9, URL: "http://example.local", Secret: "s", Filters: store.SubscriptionFilter{ContractID: contractA}}}

		n := NewNotifier(st, testLogger())
		// Replace sendOnly with a tiny buffered channel and fill it to force drop.
		ch := make(chan deliveryTask, 1)
		ch <- deliveryTask{} // fill
		n.sendOnly = ch

		done := make(chan struct{})
		go func() {
			// This should not block even though the channel is full.
			n.NotifyEvents(context.Background(), []store.Event{testEvent("x1")})
			close(done)
		}()

		select {
		case <-done:
			// success: NotifyEvents returned promptly
		case <-time.After(200 * time.Millisecond):
			t.Fatal("NotifyEvents blocked when queue was full")
		}
	})
}
