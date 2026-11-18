package queries

// This file is the single place that decides what a query costs. The GraphQL
// complexity check (internal/api/graphql/limits.go) and the per-tenant cost
// budget (internal/api/costbudget.go) both call into it, so a cost driver is
// weighted once rather than once per transport.
//
// Costs are unitless "cost units". One request to a cheap endpoint is
// BaseRequestCost; a full default page of events is PageUnitCost.

const (
	// BaseRequestCost is charged for any request that is not a list query.
	BaseRequestCost = 1
	// PageUnitCost is the cost of one page of up to PageUnitRows rows. It is
	// the same weight GraphQL gives a Connection field.
	PageUnitCost = 25
	// PageUnitRows is the page size covered by one PageUnitCost.
	PageUnitRows = 50
	// RangeUnitLedgers is the ledger-range width that adds one cost unit.
	RangeUnitLedgers = 10_000
	// RangeMaxCost caps the range driver; an unbounded range costs the cap.
	RangeMaxCost = 20
	// UnindexedCost is added when no index-backed filter narrows the scan.
	UnindexedCost = 25
	// AggregateBucketsPerUnit is how many result buckets add one cost unit.
	AggregateBucketsPerUnit = 100
	// AggregateMaxCost caps the aggregate-breadth driver.
	AggregateMaxCost = 50
	// FreeDepth is the nesting depth that carries no extra cost.
	FreeDepth = 3
	// DepthLevelCost is charged per nesting level beyond FreeDepth.
	DepthLevelCost = 2
	// FanOutCost is charged per additional list field beyond the first.
	FanOutCost = 10
)

// CostDrivers are the inputs that determine what a query costs.
type CostDrivers struct {
	// PageSize is the number of rows requested (0 = none, i.e. not a list).
	PageSize int
	// LedgerWidth is the width of the ledger range scanned; 0 means the range
	// is unbounded.
	LedgerWidth int64
	// Ranged marks a list query whose cost depends on the ledger range.
	Ranged bool
	// Indexed reports whether an index-backed filter narrows the scan.
	Indexed bool
	// AggregateBuckets is the number of buckets an aggregate returns.
	AggregateBuckets int
	// Depth is the GraphQL nesting depth (0 for REST).
	Depth int
	// FanOut is the number of list fields in a GraphQL operation.
	FanOut int
}

// PageCost is the cost of fetching rows rows: PageUnitCost per started
// PageUnitRows, never less than one page.
func PageCost(rows int) int {
	if rows <= PageUnitRows {
		return PageUnitCost
	}
	return PageUnitCost * ((rows + PageUnitRows - 1) / PageUnitRows)
}

// RangeCost is the cost of scanning a ledger range of the given width. A
// width of zero is an unbounded scan and costs RangeMaxCost.
func RangeCost(width int64) int {
	if width <= 0 {
		return RangeMaxCost
	}
	c := int((width + RangeUnitLedgers - 1) / RangeUnitLedgers)
	if c > RangeMaxCost {
		return RangeMaxCost
	}
	return c
}

// AggregateCost is the cost of an aggregate that returns buckets buckets.
func AggregateCost(buckets int) int {
	if buckets <= 0 {
		return 0
	}
	c := (buckets + AggregateBucketsPerUnit - 1) / AggregateBucketsPerUnit
	if c > AggregateMaxCost {
		return AggregateMaxCost
	}
	return c
}

// DepthCost is the cost of GraphQL nesting depth and list fan-out.
func DepthCost(depth, fanOut int) int {
	c := 0
	if depth > FreeDepth {
		c += (depth - FreeDepth) * DepthLevelCost
	}
	if fanOut > 1 {
		c += (fanOut - 1) * FanOutCost
	}
	return c
}

// Cost sums every driver. A query with no list component costs
// BaseRequestCost plus any depth/fan-out charge.
func (d CostDrivers) Cost() int {
	c := BaseRequestCost + DepthCost(d.Depth, d.FanOut) + AggregateCost(d.AggregateBuckets)
	if d.PageSize > 0 {
		c += PageCost(d.PageSize)
	}
	if d.Ranged {
		c += RangeCost(d.LedgerWidth)
		if !d.Indexed {
			c += UnindexedCost
		}
	}
	return c
}
