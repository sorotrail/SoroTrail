package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openWideRangeSQLite creates an in-memory SQLite store migrated with the full
// SQLite migration series plus the composite (contract_id, ledger, id),
// (contract_id, id), and (ledger, id) indexes that mirror Postgres migration
// 0022_event_quick_filter_indexes.
func openWideRangeSQLite(tb testing.TB) *SQLite {
	tb.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		tb.Fatalf("opening sqlite: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })

	entries, err := sqliteMigrationsFS.ReadDir("migrations/sqlite")
	if err != nil {
		tb.Fatalf("reading sqlite migrations: %v", err)
	}
	var upFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			upFiles = append(upFiles, e.Name())
		}
	}
	sort.Strings(upFiles)
	ctx := context.Background()
	for _, name := range upFiles {
		schema, err := sqliteMigrationsFS.ReadFile("migrations/sqlite/" + name)
		if err != nil {
			tb.Fatalf("reading sqlite migration %s: %v", name, err)
		}
		if _, err := db.ExecContext(ctx, string(schema)); err != nil {
			tb.Fatalf("applying sqlite migration %s: %v", name, err)
		}
	}

	// Ensure the composite indexes mirroring 0022_event_quick_filter_indexes
	// exist for keyset pagination and query-plan verification.
	for _, ddl := range []string{
		`CREATE INDEX IF NOT EXISTS idx_events_contract_ledger_id ON events (contract_id, ledger, id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_contract_id_id ON events (contract_id, id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_ledger_id ON events (ledger, id)`,
	} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			tb.Fatalf("creating index: %v", err)
		}
	}

	return NewSQLite(db)
}

// seedWideRangeEvents seeds a realistic wide-ledger dataset spanning multiple
// partition widths (fromLedger..toLedger) with multiple events per ledger
// (creating ties on both ledger and created_at), multiple contracts, and both
// common ("transfer") and sparse ("rare_clawback") topics, followed by ANALYZE.
func seedWideRangeEvents(tb testing.TB, st Store, totalEvents int) (targetContract string, allEvents []Event) {
	tb.Helper()
	ctx := context.Background()

	const (
		startLedger = int64(100_000)
		ledgerStep  = int64(250) // spans 500,000+ ledgers across 2,000 ledgers (~4+ partitions)
		eventsPerLg = 3          // 3 events per ledger so page boundaries hit ledger & timestamp ties
		numContract = 8
	)
	targetContract = fmt.Sprintf("C%055d", 1)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	allEvents = make([]Event, 0, totalEvents)
	for i := 0; i < totalEvents; i++ {
		ledgerGroup := int64(i / eventsPerLg)
		seqInLedger := int32(i % eventsPerLg)
		ledger := startLedger + ledgerGroup*ledgerStep

		contract := fmt.Sprintf("C%055d", i%numContract)
		if i%5 == 0 {
			contract = targetContract
		}

		topics := json.RawMessage(`[{"symbol":"transfer"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`)
		if i%40 == 0 {
			topics = json.RawMessage(`[{"symbol":"rare_clawback"},{"address":"CCW67TSBWVENNVMTQPEXNGXYL6G5CZWKW563CYCPBQR27XMTC2AFAXXT"}]`)
		}

		ev := Event{
			ID:               FormatTOIDCursor(ledger, seqInLedger, 0, seqInLedger),
			ContractID:       contract,
			Ledger:           ledger,
			Type:             "contract",
			TxHash:           fmt.Sprintf("%064x", i+1),
			TxIndex:          seqInLedger,
			OpIndex:          0,
			InSuccessfulCall: i%11 != 0,
			Topics:           topics,
			Value:            json.RawMessage(fmt.Sprintf(`{"u64":%d}`, i+1)),
			// Events in the same ledger share CreatedAt so OrderByCreatedAt
			// exercises tie-breaking by ID across page boundaries.
			CreatedAt: baseTime.Add(time.Duration(ledgerGroup) * time.Minute),
			Network:   "default",
		}
		allEvents = append(allEvents, ev)
	}

	if _, err := st.UpsertEvents(ctx, allEvents); err != nil {
		tb.Fatalf("seeding wide-range events: %v", err)
	}

	switch b := st.(type) {
	case *SQLite:
		_, _ = b.db.ExecContext(ctx, `ANALYZE`)
	case *Postgres:
		_, _ = b.pool.Exec(ctx, `ANALYZE events`)
	}
	return targetContract, allEvents
}

// TestDeepPagination_WideRange_CursorCorrectnessAndPruning verifies that:
//  1. Walking deep cursor pages across a wide ledger range (spanning multiple
//     partition widths and multi-row ties on ledger and created_at) never
//     repeats or skips a single row under OrderByID, OrderByLedger, or
//     OrderByCreatedAt (both asc and desc).
//  2. Contract-filtered and bounded sparse-topic cursor walks return the exact
//     matching subset with zero duplicates or gaps.
//  3. Unbounded or over-wide sparse-topic queries are rejected with
//     ErrSparseFilterRangeTooWide when MaxSparseFilterLedgerSpan is enabled.
//  4. SQLite EXPLAIN QUERY PLAN uses the composite index and applies the
//     cursor's scalar ledger bound on deep pages.
func TestDeepPagination_WideRange_CursorCorrectnessAndPruning(t *testing.T) {
	st := openWideRangeSQLite(t)
	ctx := context.Background()

	const totalRows = 600 // 200 distinct ledgers * 3 events/ledger spanning 100,000..149,750
	targetContract, seeded := seedWideRangeEvents(t, st, totalRows)

	wantAllIDs := make([]string, len(seeded))
	for i, e := range seeded {
		wantAllIDs[i] = e.ID
	}
	sort.Strings(wantAllIDs)

	// 1. Exhaustive cursor-walk correctness across all orderings and directions.
	// Page size 17 does not divide 3 (events per ledger) or 600, forcing page
	// boundaries to land in the middle of equal-ledger and equal-timestamp runs.
	for _, orderBy := range []string{"", OrderByID, OrderByLedger, OrderByCreatedAt} {
		for _, order := range []string{"asc", "desc"} {
			label := orderBy
			if label == "" {
				label = "default_id"
			}
			t.Run(label+"_"+order, func(t *testing.T) {
				seen := make(map[string]struct{}, totalRows)
				got := make([]Event, 0, totalRows)
				cursor := ""
				pages := 0

				for {
					page, next, err := st.QueryEvents(ctx, EventFilter{
						Scope:   WildcardScope(),
						OrderBy: orderBy,
						Order:   order,
						Limit:   17,
						Cursor:  cursor,
					})
					require.NoError(t, err)
					for _, ev := range page {
						if _, dup := seen[ev.ID]; dup {
							t.Fatalf("duplicate event ID %s on page %d", ev.ID, pages+1)
						}
						seen[ev.ID] = struct{}{}
						got = append(got, ev)
					}
					pages++
					if next == "" {
						break
					}
					cursor = next
					require.LessOrEqual(t, pages, totalRows, "cursor walk did not terminate")
				}

				require.Len(t, got, totalRows, "every seeded event must be visited")
				gotIDs := make([]string, len(got))
				for i, ev := range got {
					gotIDs[i] = ev.ID
				}
				sort.Strings(gotIDs)
				assert.Equal(t, wantAllIDs, gotIDs, "no row may be skipped or duplicated across deep pages")
				assertMonotonicPageOrder(t, got, orderBy, order)
			})
		}
	}

	// 2. Contract-filtered deep cursor walk across wide ledger range.
	t.Run("contract_filtered_deep_walk", func(t *testing.T) {
		var wantContractIDs []string
		for _, ev := range seeded {
			if ev.ContractID == targetContract {
				wantContractIDs = append(wantContractIDs, ev.ID)
			}
		}
		sort.Strings(wantContractIDs)

		for _, orderBy := range []string{OrderByID, OrderByLedger} {
			var gotIDs []string
			cursor := ""
			for {
				page, next, err := st.QueryEvents(ctx, EventFilter{
					Scope:      WildcardScope(),
					ContractID: targetContract,
					OrderBy:    orderBy,
					Order:      "asc",
					Limit:      11,
					Cursor:     cursor,
				})
				require.NoError(t, err)
				for _, ev := range page {
					gotIDs = append(gotIDs, ev.ID)
				}
				if next == "" {
					break
				}
				cursor = next
			}
			assert.Equal(t, wantContractIDs, gotIDs, "contract-filtered walk (%s) must return exact subset", orderBy)
		}
	})

	// 3. Sparse-topic range guard and bounded deep cursor walk.
	t.Run("sparse_topic_range_guard_and_bounded_walk", func(t *testing.T) {
		st.SetMaxSparseFilterLedgerSpan(50_000)
		t.Cleanup(func() { st.SetMaxSparseFilterLedgerSpan(0) })

		rareTopic := json.RawMessage(`{"symbol":"rare_clawback"}`)

		// Unbounded sparse topic query is rejected.
		_, _, err := st.QueryEvents(ctx, EventFilter{
			Scope:  WildcardScope(),
			Topic0: rareTopic,
			Limit:  10,
		})
		require.ErrorIs(t, err, ErrSparseFilterRangeTooWide)

		// Over-wide sparse topic query is rejected.
		_, _, err = st.QueryEvents(ctx, EventFilter{
			Scope:      WildcardScope(),
			Topic0:     rareTopic,
			FromLedger: 100_000,
			ToLedger:   160_000, // span 60,000 > 50,000
			Limit:      10,
		})
		require.ErrorIs(t, err, ErrSparseFilterRangeTooWide)

		// Bounded sparse topic query within 50,000 ledgers succeeds and pages cleanly.
		var wantRareIDs []string
		for _, ev := range seeded {
			if ev.Ledger >= 100_000 && ev.Ledger <= 150_000 && strings.Contains(string(ev.Topics), "rare_clawback") {
				wantRareIDs = append(wantRareIDs, ev.ID)
			}
		}

		var gotRareIDs []string
		cursor := ""
		for {
			page, next, qerr := st.QueryEvents(ctx, EventFilter{
				Scope:      WildcardScope(),
				Topic0:     rareTopic,
				FromLedger: 100_000,
				ToLedger:   150_000,
				Limit:      4,
				Cursor:     cursor,
			})
			require.NoError(t, qerr)
			for _, ev := range page {
				gotRareIDs = append(gotRareIDs, ev.ID)
			}
			if next == "" {
				break
			}
			cursor = next
		}
		assert.Equal(t, wantRareIDs, gotRareIDs)
	})

	// 4. Verify SQLite EXPLAIN QUERY PLAN uses the composite index on deep cursor pages.
	t.Run("query_plan_uses_composite_index_on_deep_cursor", func(t *testing.T) {
		deepCursor := FormatTOIDCursor(140_000, 1, 0, 1)
		q, args, _, err := buildQueryEventsSQL(EventFilter{
			Scope:      WildcardScope(),
			ContractID: targetContract,
			Cursor:     deepCursor,
			Order:      "asc",
			Limit:      20,
		}, 0)
		require.NoError(t, err)
		assert.Contains(t, q, "ledger >= $2")
		assert.Contains(t, q, "id > $3")

		rows, err := st.db.QueryContext(ctx,
			`EXPLAIN QUERY PLAN SELECT id FROM events WHERE contract_id = ? AND ledger >= ? AND id > ? ORDER BY id ASC LIMIT 21`,
			args[0], args[1], args[2])
		require.NoError(t, err)
		defer rows.Close()

		var details []string
		for rows.Next() {
			var id, parent, notused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Err())
		plan := strings.Join(details, "\n")
		assert.Contains(t, plan, "USING INDEX", "deep cursor query must use an index scan, got plan: %s", plan)
	})
}

func assertMonotonicPageOrder(t *testing.T, events []Event, orderBy, order string) {
	t.Helper()
	desc := order == "desc"
	for i := 1; i < len(events); i++ {
		prev, cur := events[i-1], events[i]
		switch orderBy {
		case OrderByLedger:
			if prev.Ledger == cur.Ledger {
				if desc {
					require.Greater(t, prev.ID, cur.ID)
				} else {
					require.Less(t, prev.ID, cur.ID)
				}
				continue
			}
			if desc {
				require.Greater(t, prev.Ledger, cur.Ledger)
			} else {
				require.Less(t, prev.Ledger, cur.Ledger)
			}
		case OrderByCreatedAt:
			if prev.CreatedAt.Equal(cur.CreatedAt) {
				if desc {
					require.Greater(t, prev.ID, cur.ID)
				} else {
					require.Less(t, prev.ID, cur.ID)
				}
				continue
			}
			if desc {
				require.True(t, prev.CreatedAt.After(cur.CreatedAt))
			} else {
				require.True(t, prev.CreatedAt.Before(cur.CreatedAt))
			}
		default:
			if desc {
				require.Greater(t, prev.ID, cur.ID)
			} else {
				require.Less(t, prev.ID, cur.ID)
			}
		}
	}
}

// BenchmarkDeepPagination_WideRange benchmarks per-page query latency at
// increasing cursor depths (Page 1, Page 20, Page 50, Page 100) across a
// wide ledger range (6,000 events across ~500,000 ledgers).
//
// When TEST_DATABASE_URL is set it also runs against Postgres after ANALYZE;
// otherwise it runs deterministically against the indexed SQLite backend so
// `go test -bench=BenchmarkDeepPagination_WideRange ./internal/store` works
// in any environment.
func BenchmarkDeepPagination_WideRange(b *testing.B) {
	st := openWideRangeSQLite(b)
	ctx := context.Background()

	const (
		totalEvents = 6_000
		pageSize    = 50
	)
	targetContract, _ := seedWideRangeEvents(b, st, totalEvents)

	collectDepthCursors := func(baseFilter EventFilter, depths []int) map[int]string {
		cursors := map[int]string{1: ""}
		maxDepth := depths[len(depths)-1]
		f := baseFilter
		f.Scope = WildcardScope()
		f.Limit = pageSize
		cursor := ""
		for page := 1; page < maxDepth; page++ {
			f.Cursor = cursor
			evs, next, err := st.QueryEvents(ctx, f)
			if err != nil {
				b.Fatalf("collecting cursor at page %d: %v", page, err)
			}
			if len(evs) == 0 || next == "" {
				break
			}
			cursor = next
			for _, d := range depths {
				if page+1 == d {
					cursors[d] = cursor
				}
			}
		}
		return cursors
	}

	depths := []int{1, 20, 50, 100}

	scenarios := []struct {
		name   string
		filter EventFilter
		depths []int
	}{
		{
			name:   "OrderByID_Asc",
			filter: EventFilter{OrderBy: OrderByID, Order: "asc"},
			depths: depths,
		},
		{
			name:   "OrderByID_Desc",
			filter: EventFilter{OrderBy: OrderByID, Order: "desc"},
			depths: depths,
		},
		{
			name:   "OrderByLedger_Asc",
			filter: EventFilter{OrderBy: OrderByLedger, Order: "asc"},
			depths: depths,
		},
		{
			name:   "OrderByLedger_Desc",
			filter: EventFilter{OrderBy: OrderByLedger, Order: "desc"},
			depths: depths,
		},
		{
			name:   "ContractFiltered_OrderByID",
			filter: EventFilter{ContractID: targetContract, OrderBy: OrderByID, Order: "asc"},
			depths: []int{1, 10, 20},
		},
		{
			name: "SparseTopic_BoundedLedgerRange",
			filter: EventFilter{
				Topic0:     json.RawMessage(`{"symbol":"rare_clawback"}`),
				FromLedger: 100_000,
				ToLedger:   550_000,
				OrderBy:    OrderByID,
				Order:      "asc",
			},
			depths: []int{1, 3},
		},
	}

	for _, sc := range scenarios {
		cursors := collectDepthCursors(sc.filter, sc.depths)
		for _, depth := range sc.depths {
			cursor, ok := cursors[depth]
			if !ok {
				continue
			}
			b.Run(fmt.Sprintf("%s/page_%d", sc.name, depth), func(b *testing.B) {
				f := sc.filter
				f.Scope = WildcardScope()
				f.Limit = pageSize
				f.Cursor = cursor
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					evs, _, err := st.QueryEvents(ctx, f)
					if err != nil {
						b.Fatal(err)
					}
					if len(evs) == 0 {
						b.Fatal("expected non-empty page")
					}
				}
			})
		}
	}

	if os.Getenv("TEST_DATABASE_URL") != "" {
		b.Run("Postgres", func(b *testing.B) {
			pg := benchStore(b)
			_, _ = seedWideRangeEvents(b, pg, totalEvents)
			pgCursors := map[int]string{1: ""}
			cur := ""
			for p := 1; p < 100; p++ {
				_, next, err := pg.QueryEvents(ctx, EventFilter{
					Scope:  WildcardScope(),
					Limit:  pageSize,
					Cursor: cur,
				})
				if err != nil || next == "" {
					break
				}
				cur = next
				if p+1 == 20 || p+1 == 50 || p+1 == 100 {
					pgCursors[p+1] = cur
				}
			}
			for _, d := range depths {
				c, ok := pgCursors[d]
				if !ok {
					continue
				}
				b.Run(fmt.Sprintf("OrderByID_Asc/page_%d", d), func(b *testing.B) {
					f := EventFilter{Scope: WildcardScope(), Limit: pageSize, Cursor: c}
					b.ResetTimer()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, _, err := pg.QueryEvents(ctx, f); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
