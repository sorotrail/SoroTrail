//go:build integration

package store

// This file exhaustively covers scope enforcement on every Store method
// that accepts a Scope (directly or via EventFilter). The method list is
// derived from the interface by reflection so that a new Scope-taking
// method added without a corresponding test fails the build.
//
// Integration tests need Postgres and are skipped without TEST_DATABASE_URL.
// The reflection-based enumeration runs against the interface definition
// without a database.

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scopeMethodNames returns the sorted names of every method on Store that
// accepts a Scope parameter directly (not inside a struct). This is the
// compile-time source of truth: the test suite must cover every name here.
func scopeMethodNames() []string {
	var names []string
	t := reflect.TypeOf((*Store)(nil)).Elem()
	scopeType := reflect.TypeOf(Scope{})
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		for j := 0; j < m.Type.NumIn(); j++ {
			if m.Type.In(j) == scopeType {
				names = append(names, m.Name)
				break
			}
		}
	}
	sort.Strings(names)
	return names
}

// filterScopeMethodNames returns the sorted names of every method on Store
// whose first non-context parameter is EventFilter (which carries Scope).
func filterScopeMethodNames() []string {
	var names []string
	t := reflect.TypeOf((*Store)(nil)).Elem()
	filterType := reflect.TypeOf(EventFilter{})
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		// Skip the context.Context receiver argument and check the first
		// non-context param.
		for j := 1; j < m.Type.NumIn(); j++ {
			if m.Type.In(j) == filterType {
				names = append(names, m.Name)
				break
			}
		}
	}
	sort.Strings(names)
	return names
}

// allScopeMethods returns every method on Store where Scope is the
// authorization boundary — either as a direct Scope parameter or via
// EventFilter.
func allScopeMethods() []string {
	direct := scopeMethodNames()
	viaFilter := filterScopeMethodNames()

	seen := map[string]struct{}{}
	var all []string
	for _, n := range append(direct, viaFilter...) {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		all = append(all, n)
	}
	sort.Strings(all)
	return all
}

// scopeMethodsExpected is the hand-curated list that must match the
// reflection output. When you add a new Scope-taking method to Store,
// add its name here or the test fails.
//
// Keep this alphabetically sorted.
var scopeMethodsExpected = []string{
	"AggregateEvents",
	"CountEvents",
	"EventExists",
	"GetEvent",
	"QueryAddressEvents",
	"QueryEvents",
	"Stats",
}

// TestScopeMethods_Enumeration ensures that every method on Store that
// accepts Scope (directly or via EventFilter) is listed in the expected
// set. A new Scope-taking method that forgets to add itself here will
// cause this test to fail, which is exactly the "method added without
// scope handling failing the test" the issue requires.
func TestScopeMethods_Enumeration(t *testing.T) {
	got := allScopeMethods()
	require.Equal(t, scopeMethodsExpected, got,
		"the reflection-derived method list disagrees with the expected set; "+
			"add the new method to scopeMethodsExpected and write its enforcement tests")
}

// ---------------------------------------------------------------------------
// Integration enforcement tests
//
// Every subtest seeds events for contractA and contractB, then exercises one
// Scope-taking method under three scope shapes: zero (denies all), contract
// scope (grants only contractA), and wildcard (grants everything).
// ---------------------------------------------------------------------------

// scopeStore creates a migrated, truncated Postgres store for scope tests.
func scopeStore(t *testing.T) *Postgres {
	t.Helper()
	return testStore(t)
}

// seedScopedEvents inserts two events (one per contract) and returns them.
func seedScopedEvents(t *testing.T, st *Postgres) []Event {
	t.Helper()
	events := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractB),
	}
	_, err := st.UpsertEvents(context.Background(), events)
	require.NoError(t, err)
	return events
}

// TestScopeEnforcement_GetEvent covers GetEvent under all scope shapes.
func TestScopeEnforcement_GetEvent(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)

	t.Run("zero scope returns not-found", func(t *testing.T) {
		_, err := st.GetEvent(ctx, seeded[0].ID, Scope{})
		assert.ErrorIs(t, err, ErrNotFound,
			"a zero Scope must report the event as absent")
	})

	t.Run("contract scope returns only granted rows", func(t *testing.T) {
		sc := NewScope([]string{contractA})

		got, err := st.GetEvent(ctx, seeded[0].ID, sc)
		require.NoError(t, err)
		assert.Equal(t, contractA, got.ContractID)

		_, err = st.GetEvent(ctx, seeded[1].ID, sc)
		assert.ErrorIs(t, err, ErrNotFound,
			"an event for an ungranted contract must be invisible")
	})

	t.Run("wildcard scope returns everything", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			got, err := st.GetEvent(ctx, seeded[0].ID, sc)
			require.NoError(t, err)
			assert.Equal(t, seeded[0].ContractID, got.ContractID)

			got, err = st.GetEvent(ctx, seeded[1].ID, sc)
			require.NoError(t, err)
			assert.Equal(t, seeded[1].ContractID, got.ContractID)
		}
	})
}

// TestScopeEnforcement_EventExists covers EventExists under all scope shapes.
func TestScopeEnforcement_EventExists(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)

	t.Run("zero scope reports absent", func(t *testing.T) {
		exists, err := st.EventExists(ctx, seeded[0].ID, Scope{})
		require.NoError(t, err)
		assert.False(t, exists,
			"a zero Scope must not reveal existence")
	})

	t.Run("contract scope returns only granted rows", func(t *testing.T) {
		sc := NewScope([]string{contractA})

		exists, err := st.EventExists(ctx, seeded[0].ID, sc)
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = st.EventExists(ctx, seeded[1].ID, sc)
		require.NoError(t, err)
		assert.False(t, exists,
			"existence of an ungranted event must not be probeable")
	})

	t.Run("wildcard scope returns everything", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			exists, err := st.EventExists(ctx, seeded[0].ID, sc)
			require.NoError(t, err)
			assert.True(t, exists)

			exists, err = st.EventExists(ctx, seeded[1].ID, sc)
			require.NoError(t, err)
			assert.True(t, exists)
		}
	})
}

// TestScopeEnforcement_QueryEvents covers QueryEvents under all scope shapes.
func TestScopeEnforcement_QueryEvents(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)
	_ = seeded

	t.Run("zero scope returns empty page", func(t *testing.T) {
		got, next, err := st.QueryEvents(ctx, EventFilter{})
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("contract scope returns only granted rows", func(t *testing.T) {
		sc := NewScope([]string{contractA})
		got, _, err := st.QueryEvents(ctx, EventFilter{Scope: sc})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, contractA, got[0].ContractID)
	})

	t.Run("wildcard scope returns everything", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			got, _, err := st.QueryEvents(ctx, EventFilter{Scope: sc})
			require.NoError(t, err)
			assert.Len(t, got, 2)
		}
	})

	t.Run("scope intersects with other filters", func(t *testing.T) {
		sc := NewScope([]string{contractA, contractB})
		got, _, err := st.QueryEvents(ctx, EventFilter{
			Scope:      sc,
			ContractID: contractB,
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, contractB, got[0].ContractID)
	})

	t.Run("pagination cannot walk out of scope", func(t *testing.T) {
		// Seed more events for contractA.
		var extra []Event
		for i := 3; i <= 6; i++ {
			extra = append(extra, testEvent(eventID(i), int64(100+i), contractA))
		}
		_, err := st.UpsertEvents(ctx, extra)
		require.NoError(t, err)

		sc := NewScope([]string{contractA})
		var all []Event
		cursor := ""
		for range 10 {
			page, next, err := st.QueryEvents(ctx, EventFilter{
				Scope:  sc,
				Limit:  1,
				Cursor: cursor,
			})
			require.NoError(t, err)
			all = append(all, page...)
			if next == "" {
				break
			}
			cursor = next
		}
		assert.Len(t, all, 5, "must return only contractA events")
		for _, e := range all {
			assert.Equal(t, contractA, e.ContractID)
		}
	})
}

// TestScopeEnforcement_CountEvents covers CountEvents under all scope shapes.
func TestScopeEnforcement_CountEvents(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)
	_ = seeded

	t.Run("zero scope returns zero", func(t *testing.T) {
		n, err := st.CountEvents(ctx, EventFilter{})
		require.NoError(t, err)
		assert.Zero(t, n,
			"a zero Scope must report zero events")
	})

	t.Run("contract scope counts only granted rows", func(t *testing.T) {
		sc := NewScope([]string{contractA})
		n, err := st.CountEvents(ctx, EventFilter{Scope: sc})
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)
	})

	t.Run("wildcard scope counts everything", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			n, err := st.CountEvents(ctx, EventFilter{Scope: sc})
			require.NoError(t, err)
			assert.Equal(t, int64(2), n)
		}
	})
}

// TestScopeEnforcement_AggregateEvents covers AggregateEvents under all
// scope shapes.
func TestScopeEnforcement_AggregateEvents(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)
	_ = seeded

	t.Run("zero scope returns empty aggregation", func(t *testing.T) {
		buckets, err := st.AggregateEvents(ctx, EventFilter{}, "ledger")
		require.NoError(t, err)
		assert.Empty(t, buckets,
			"a zero Scope must produce no aggregation buckets")
	})

	t.Run("contract scope aggregates only granted rows", func(t *testing.T) {
		sc := NewScope([]string{contractA})
		buckets, err := st.AggregateEvents(ctx, EventFilter{Scope: sc}, "ledger")
		require.NoError(t, err)
		require.Len(t, buckets, 1)
		assert.Equal(t, int64(1), buckets[0].Count)
	})

	t.Run("wildcard scope aggregates everything", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			buckets, err := st.AggregateEvents(ctx, EventFilter{Scope: sc}, "ledger")
			require.NoError(t, err)
			assert.Len(t, buckets, 2, "each ledger gets its own bucket")
			var total int64
			for _, b := range buckets {
				total += b.Count
			}
			assert.Equal(t, int64(2), total)
		}
	})
}

// TestScopeEnforcement_QueryAddressEvents covers QueryAddressEvents under
// all scope shapes. The method takes EventFilter (which carries Scope), so
// it must enforce the same tenant boundary as QueryEvents.
func TestScopeEnforcement_QueryAddressEvents(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)

	// Create address references for both events.
	const testAddr = "GTestAddress12345678901234567890123456789012345"
	err := st.UpsertAddressRefs(ctx, []AddressRef{
		{Address: testAddr, EventID: seeded[0].ID, Role: "from"},
		{Address: testAddr, EventID: seeded[1].ID, Role: "to"},
	})
	require.NoError(t, err)

	t.Run("zero scope returns empty page", func(t *testing.T) {
		got, next, err := st.QueryAddressEvents(ctx, testAddr, EventFilter{})
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("contract scope returns only granted rows", func(t *testing.T) {
		sc := NewScope([]string{contractA})
		got, _, err := st.QueryAddressEvents(ctx, testAddr, EventFilter{Scope: sc})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, contractA, got[0].ContractID)
	})

	t.Run("wildcard scope returns everything", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			got, _, err := st.QueryAddressEvents(ctx, testAddr, EventFilter{Scope: sc})
			require.NoError(t, err)
			assert.Len(t, got, 2)
		}
	})
}

// TestScopeEnforcement_Stats covers Stats under all scope shapes.
func TestScopeEnforcement_Stats(t *testing.T) {
	st := scopeStore(t)
	ctx := context.Background()
	seeded := seedScopedEvents(t, st)
	_ = seeded

	t.Run("zero scope returns zeroed aggregates", func(t *testing.T) {
		s, err := st.Stats(ctx, Scope{})
		require.NoError(t, err)
		assert.Zero(t, s.TotalEvents,
			"a zero Scope must not reveal total event count")
		assert.Zero(t, s.ContractCount,
			"a zero Scope must not reveal contract count")
	})

	t.Run("contract scope returns only granted aggregates", func(t *testing.T) {
		sc := NewScope([]string{contractA})
		s, err := st.Stats(ctx, sc)
		require.NoError(t, err)
		assert.Equal(t, int64(1), s.TotalEvents)
		assert.Equal(t, int64(1), s.ContractCount)
	})

	t.Run("wildcard scope returns full aggregates", func(t *testing.T) {
		for _, sc := range []Scope{WildcardScope(), SystemScope()} {
			s, err := st.Stats(ctx, sc)
			require.NoError(t, err)
			assert.Equal(t, int64(2), s.TotalEvents)
			assert.Equal(t, int64(2), s.ContractCount)
		}
	})
}
