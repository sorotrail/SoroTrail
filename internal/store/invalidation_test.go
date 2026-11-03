package store

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestFormatAndParseInvalidationPayload(t *testing.T) {
	cases := []struct {
		name        string
		kind        string
		target      string
		wantPayload string
		wantKind    string
		wantTarget  string
	}{
		{
			name:        "legacy bare contract ID defaults to spec",
			kind:        "",
			target:      "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			wantPayload: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			wantKind:    InvalidationKindSpec,
			wantTarget:  "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		},
		{
			name:        "explicit spec kind uses legacy bare contract ID for wire compatibility",
			kind:        InvalidationKindSpec,
			target:      "CCONTRACT123",
			wantPayload: "CCONTRACT123",
			wantKind:    InvalidationKindSpec,
			wantTarget:  "CCONTRACT123",
		},
		{
			name:        "tenant invalidation",
			kind:        InvalidationKindTenant,
			target:      "42",
			wantPayload: "tenant:42",
			wantKind:    InvalidationKindTenant,
			wantTarget:  "42",
		},
		{
			name:        "apikey invalidation",
			kind:        InvalidationKindAPIKey,
			target:      "99",
			wantPayload: "apikey:99",
			wantKind:    InvalidationKindAPIKey,
			wantTarget:  "99",
		},
		{
			name:        "persisted graphql invalidation",
			kind:        InvalidationKindPersisted,
			target:      "*",
			wantPayload: "persisted:*",
			wantKind:    InvalidationKindPersisted,
			wantTarget:  "*",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotPayload := FormatInvalidationPayload(tc.kind, tc.target)
			if gotPayload != tc.wantPayload {
				t.Fatalf("FormatInvalidationPayload(%q, %q) = %q, want %q",
					tc.kind, tc.target, gotPayload, tc.wantPayload)
			}
			ev := ParseInvalidationPayload(gotPayload)
			if ev.Kind != tc.wantKind || ev.Target != tc.wantTarget {
				t.Fatalf("ParseInvalidationPayload(%q) = %+v, want {Kind:%q Target:%q}",
					gotPayload, ev, tc.wantKind, tc.wantTarget)
			}
		})
	}

	// Also verify explicit "spec:<id>" prefix parses cleanly as InvalidationKindSpec.
	ev := ParseInvalidationPayload("spec:CCONTRACT999")
	if ev.Kind != InvalidationKindSpec || ev.Target != "CCONTRACT999" {
		t.Fatalf("ParseInvalidationPayload(spec:CCONTRACT999) = %+v", ev)
	}
}

func TestInMemoryInvalidationBusAndListenerLifecycle(t *testing.T) {
	bus := NewInMemoryInvalidationBus()

	var (
		mu          sync.Mutex
		received    []InvalidationEvent
		specTargets []string
		flushes     int
		disconnects int
	)

	listener := &SpecInvalidationListener{
		OnInvalidation: func(contractID string) {
			mu.Lock()
			defer mu.Unlock()
			specTargets = append(specTargets, contractID)
		},
		OnEvent: func(ev InvalidationEvent) {
			mu.Lock()
			defer mu.Unlock()
			received = append(received, ev)
		},
		OnReconnect: func() {
			mu.Lock()
			defer mu.Unlock()
			flushes++
		},
		OnDisconnect: func(_ error) {
			mu.Lock()
			defer mu.Unlock()
			disconnects++
		},
	}

	subID := bus.Attach(listener)

	if err := bus.PublishInvalidation(context.Background(), InvalidationKindTenant, "7"); err != nil {
		t.Fatalf("PublishInvalidation tenant: %v", err)
	}
	if err := bus.PublishInvalidation(context.Background(), InvalidationKindAPIKey, "15"); err != nil {
		t.Fatalf("PublishInvalidation apikey: %v", err)
	}
	if err := bus.PublishSpecInvalidation(context.Background(), "CCONTRACT1"); err != nil {
		t.Fatalf("PublishSpecInvalidation: %v", err)
	}

	// Simulate disconnect: messages published while disconnected are missed by
	// the listener, then OnReconnect flushes caches when reconnected.
	bus.SetConnected(subID, false, errors.New("simulated connection drop"))
	if err := bus.PublishInvalidation(context.Background(), InvalidationKindTenant, "88"); err != nil {
		t.Fatalf("PublishInvalidation while disconnected: %v", err)
	}
	bus.SetConnected(subID, true, nil)

	stats := listener.Stats()
	if stats.NotificationsReceived != 3 {
		t.Fatalf("NotificationsReceived = %d, want 3", stats.NotificationsReceived)
	}
	if stats.DisconnectErrors != 1 {
		t.Fatalf("DisconnectErrors = %d, want 1", stats.DisconnectErrors)
	}
	// Initial Attach + 1 reconnect = 2
	if stats.Reconnects != 2 {
		t.Fatalf("Reconnects = %d, want 2", stats.Reconnects)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 || received[0].Kind != InvalidationKindTenant || received[0].Target != "7" ||
		received[1].Kind != InvalidationKindAPIKey || received[1].Target != "15" ||
		received[2].Kind != InvalidationKindSpec || received[2].Target != "CCONTRACT1" {
		t.Fatalf("unexpected received events: %+v", received)
	}
	if len(specTargets) != 1 || specTargets[0] != "CCONTRACT1" {
		t.Fatalf("unexpected specTargets: %+v", specTargets)
	}
	if flushes != 2 {
		t.Fatalf("Reconnect flushes = %d, want 2", flushes)
	}
	if disconnects != 1 {
		t.Fatalf("Disconnects = %d, want 1", disconnects)
	}
}
