package queries

import "testing"

func TestCostDrivers(t *testing.T) {
	base := CostDrivers{PageSize: 50, Ranged: true, Indexed: true, LedgerWidth: 10_000}
	baseCost := base.Cost()
	tests := []struct {
		name string
		mod  func(d *CostDrivers)
		want int // delta over baseCost
	}{
		{"page size doubles", func(d *CostDrivers) { d.PageSize = 100 }, PageUnitCost},
		{"page size within one unit", func(d *CostDrivers) { d.PageSize = 1 }, 0},
		{"wider ledger range", func(d *CostDrivers) { d.LedgerWidth = 50_000 }, 4},
		{"range is capped", func(d *CostDrivers) { d.LedgerWidth = 1 << 40 }, RangeMaxCost - 1},
		{"unbounded range", func(d *CostDrivers) { d.LedgerWidth = 0 }, RangeMaxCost - 1},
		{"no index-backed filter", func(d *CostDrivers) { d.Indexed = false }, UnindexedCost},
		{"aggregate breadth", func(d *CostDrivers) { d.AggregateBuckets = 250 }, 3},
		{"aggregate breadth is capped", func(d *CostDrivers) { d.AggregateBuckets = 1 << 30 }, AggregateMaxCost},
		{"depth within free depth", func(d *CostDrivers) { d.Depth = FreeDepth }, 0},
		{"depth beyond free depth", func(d *CostDrivers) { d.Depth = FreeDepth + 2 }, 2 * DepthLevelCost},
		{"fan-out", func(d *CostDrivers) { d.FanOut = 3 }, 2 * FanOutCost},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := base
			tc.mod(&d)
			if got := d.Cost() - baseCost; got != tc.want {
				t.Fatalf("cost delta = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCostNonListRequest(t *testing.T) {
	if got := (CostDrivers{}).Cost(); got != BaseRequestCost {
		t.Fatalf("non-list request cost = %d, want %d", got, BaseRequestCost)
	}
}

func TestPageCostMatchesGraphQLWeights(t *testing.T) {
	for rows, want := range map[int]int{0: 25, 50: 25, 51: 50, 100: 50, 101: 75, 200: 100} {
		if got := PageCost(rows); got != want {
			t.Errorf("PageCost(%d) = %d, want %d", rows, got, want)
		}
	}
}
