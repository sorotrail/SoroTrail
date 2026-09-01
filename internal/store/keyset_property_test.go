//go:build integration

package store

import (
	"context"
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestQueryEvents_KeysetPaginationProperty generates tied sort keys and walks
// every supported ordering/filter combination. The expected set is computed
// independently from the store query, so a page boundary cannot hide a
// skipped or repeated event.
func TestQueryEvents_KeysetPaginationProperty(t *testing.T) {
	p := testStore(t)
	rng := rand.New(rand.NewSource(168))
	anchor := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	events := make([]Event, 0, 240)
	for i := 1; i <= 240; i++ {
		ledger := int64(1000 + rng.Intn(24))
		contract := contractA
		if rng.Intn(2) == 0 {
			contract = contractB
		}
		e := testEvent(eventID(i), ledger, contract)
		e.Type = "contract"
		if rng.Intn(4) == 0 {
			e.Type = "diagnostic"
		}
		e.CreatedAt = anchor.Add(time.Duration(rng.Intn(8)) * time.Hour)
		if rng.Intn(3) == 0 {
			e.Topics = json.RawMessage(`[{"symbol":"mint"}]`)
		}
		events = append(events, e)
	}
	require.NoError(t, func() error {
		_, err := p.UpsertEvents(context.Background(), events)
		return err
	}())

	filters := []struct {
		name string
		make func() EventFilter
	}{
		{"all", func() EventFilter { return EventFilter{} }},
		{"contract-a", func() EventFilter { return EventFilter{ContractID: contractA} }},
		{"diagnostic", func() EventFilter { return EventFilter{Types: []string{"diagnostic"}} }},
		{"ledger-window", func() EventFilter { return EventFilter{FromLedger: 1006, ToLedger: 1017} }},
		{"topic-mint", func() EventFilter { return EventFilter{Topic: json.RawMessage(`{"symbol":"mint"}`)} }},
		{"combined", func() EventFilter {
			return EventFilter{ContractID: contractB, Types: []string{"contract"}, FromLedger: 1004, ToLedger: 1019}
		}},
	}
	for _, tc := range filters {
		for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
			for _, order := range []string{"asc", "desc"} {
				t.Run(tc.name+"/"+orderBy+"/"+order, func(t *testing.T) {
					f := tc.make()
					f.OrderBy, f.Order, f.Limit, f.Scope = orderBy, order, 7, WildcardScope()
					want := propertyFilter(events, f)
					got := walkPages(t, p, f)
					requireSortedByRequest(t, got, orderBy, order)
					require.Equal(t, len(want), len(got))

					wantIDs := propertyEventIDs(want)
					gotIDs := propertyEventIDs(got)
					sort.Strings(wantIDs)
					sort.Strings(gotIDs)
					require.Equal(t, wantIDs, gotIDs)
					require.Equal(t, len(gotIDs), uniqueStrings(gotIDs))
				})
			}
		}
	}
}

func propertyFilter(events []Event, f EventFilter) []Event {
	out := make([]Event, 0, len(events))
	for _, e := range events {
		if f.ContractID != "" && e.ContractID != f.ContractID {
			continue
		}
		if len(f.Types) > 0 && e.Type != f.Types[0] {
			continue
		}
		if f.FromLedger > 0 && e.Ledger < f.FromLedger {
			continue
		}
		if f.ToLedger > 0 && e.Ledger > f.ToLedger {
			continue
		}
		if len(f.Topic) > 0 && string(f.Topic) != `{"symbol":"mint"}` {
			continue
		}
		if len(f.Topic) > 0 && string(e.Topics) != `[{"symbol":"mint"}]` {
			continue
		}
		out = append(out, e)
	}
	return out
}

func propertyEventIDs(events []Event) []string {
	ids := make([]string, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.ID)
	}
	return ids
}

func uniqueStrings(values []string) int {
	count := 0
	for i, value := range values {
		if i == 0 || value != values[i-1] {
			count++
		}
	}
	return count
}

func requireSortedByRequest(t *testing.T, events []Event, orderBy, order string) {
	t.Helper()
	copyEvents := append([]Event(nil), events...)
	sort.Slice(copyEvents, func(i, j int) bool {
		less := func() bool {
			switch orderBy {
			case OrderByLedger:
				if copyEvents[i].Ledger != copyEvents[j].Ledger {
					return copyEvents[i].Ledger < copyEvents[j].Ledger
				}
			case OrderByCreatedAt:
				if !copyEvents[i].CreatedAt.Equal(copyEvents[j].CreatedAt) {
					return copyEvents[i].CreatedAt.Before(copyEvents[j].CreatedAt)
				}
			}
			return copyEvents[i].ID < copyEvents[j].ID
		}
		if order == "desc" {
			return !less() && copyEvents[i].ID != copyEvents[j].ID
		}
		return less()
	})
	for i := range events {
		require.Equal(t, copyEvents[i].ID, events[i].ID)
	}
}
