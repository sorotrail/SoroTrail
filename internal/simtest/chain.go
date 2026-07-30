package simtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

// LedgerDuration is the virtual time between ledger closes in the simulation.
// It defaults to 5s, matching the Stellar network's nominal close time.
const LedgerDuration = 5 * time.Second

// GenesisLedger is the first ledger in every simulation chain.
const GenesisLedger uint32 = 2 // ledger 1 predates any events

// ChainEvent is one event the virtual chain generated at a specific ledger.
type ChainEvent struct {
	Event rpc.Event
	// Ledger is the ledger this event was produced at.
	Ledger uint32
}

// VirtualChain implements rpc.Client with a deterministic, time-driven
// model of a Stellar-like blockchain. As virtual time advances, the chain
// head moves forward, old ledgers age out of the retention window, and
// GetEvents returns only events within the retained range.
//
// Events are generated upfront (AddEvents) and become \"visible\" only when
// their ledger is ≤ the chain's latest ledger AND ≥ the oldest retained
// ledger at the time of the request.
type VirtualChain struct {
	mu sync.Mutex

	// clock is the simulation's shared virtual clock.
	clock *VirtualClock

	// genesisTime is the virtual wall time when genesis started.
	genesisTime time.Time

	// retentionLedgers is how many ledgers behind the head are retained.
	retentionLedgers uint32

	// events is a sorted-by-ledger list of all events ever generated.
	events []ChainEvent

	// byLedger indexes events by their ledger for fast lookup.
	byLedger map[uint32][]rpc.Event

	// headLedger is the highest ledger the chain has advanced to.
	headLedger uint32

	// callCount tracks how many times GetEvents has been called; used by
	// the fault scheduler to inject at specific call indices.
	callCount int

	// healthError is injected by the fault scheduler, or nil.
	healthError error

	// outOfRangeGaps records [requested, oldestRetained) ranges for the
	// oracle. When a GetEvents call fails because startLedger is below the
	// retention window, the attempted ledger and the oldest available are
	// recorded so the oracle can classify those events as legitimately lost.
	outOfRangeGaps []outOfRangeGap
}

// outOfRangeGap records a single retention-gap event.
type outOfRangeGap struct {
	requested uint32
	oldest    uint32
}

// NewVirtualChain creates a chain starting at genesisTime with the given
// retention window (in ledgers).
func NewVirtualChain(clock *VirtualClock, genesisTime time.Time, retentionLedgers uint32) *VirtualChain {
	return &VirtualChain{
		clock:            clock,
		genesisTime:      genesisTime,
		retentionLedgers: retentionLedgers,
		byLedger:         make(map[uint32][]rpc.Event),
	}
}

// AddEvents appends events to the chain at the given ledger. Events become
// visible once the chain head reaches ledger and the ledger hasn't aged out.
func (c *VirtualChain) AddEvents(ledger uint32, events ...rpc.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range events {
		events[i].Ledger = ledger
		c.events = append(c.events, ChainEvent{Event: events[i], Ledger: ledger})
	}
	if ledger > c.headLedger {
		c.headLedger = ledger
	}
	// Rebuild the index. This is efficient enough for simulation-scale event
	// counts (thousands, not millions).
	c.byLedger = make(map[uint32][]rpc.Event)
	for _, ce := range c.events {
		c.byLedger[ce.Ledger] = append(c.byLedger[ce.Ledger], ce.Event)
	}
}

// AdvanceTo moves the virtual clock so the chain head reaches targetLedger.
// Events at ledgers ≤ target are discoverable (subject to retention).
func (c *VirtualChain) AdvanceTo(targetLedger uint32) {
	if targetLedger <= GenesisLedger {
		targetLedger = GenesisLedger
	}
	elapsed := time.Duration(targetLedger-GenesisLedger) * LedgerDuration
	newTime := c.genesisTime.Add(elapsed)
	c.clock.SetNow(newTime)

	c.mu.Lock()
	if targetLedger > c.headLedger {
		c.headLedger = targetLedger
	}
	c.mu.Unlock()
}

// AdvanceBy moves the chain head forward by count ledgers.
func (c *VirtualChain) AdvanceBy(count uint32) {
	c.clock.Advance(time.Duration(count) * LedgerDuration)
	c.mu.Lock()
	c.headLedger += count
	c.mu.Unlock()
}

// LatestLedger returns the current chain head.
func (c *VirtualChain) LatestLedger() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headLedger
}

// OldestRetained returns the oldest ledger still in the retention window.
func (c *VirtualChain) OldestRetained() uint32 {
	latest := c.LatestLedger()
	if latest <= c.retentionLedgers {
		return GenesisLedger
	}
	return latest - c.retentionLedgers
}

// CallCount returns how many times GetEvents has been invoked.
func (c *VirtualChain) CallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callCount
}

// SetHealthError makes GetHealth return this error.
func (c *VirtualChain) SetHealthError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.healthError = err
}

// AllEvents returns every event ever added to the chain.
func (c *VirtualChain) AllEvents() []ChainEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ChainEvent, len(c.events))
	copy(out, c.events)
	return out
}

// GetHealth implements rpc.Client.
func (c *VirtualChain) GetHealth(_ context.Context) (rpc.Health, error) {
	c.mu.Lock()
	err := c.healthError
	latest := c.headLedger
	oldest := c.oldestRetainedLocked()
	c.mu.Unlock()

	if err != nil {
		return rpc.Health{}, err
	}
	return rpc.Health{
		Status:                "healthy",
		LatestLedger:          latest,
		OldestLedger:          oldest,
		LedgerRetentionWindow: c.retentionLedgers,
	}, nil
}

// GetLatestLedger implements rpc.Client.
func (c *VirtualChain) GetLatestLedger(_ context.Context) (rpc.LatestLedger, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return rpc.LatestLedger{
		ID:       fmt.Sprintf("00%d", c.headLedger),
		Sequence: c.headLedger,
	}, nil
}

// GetEvents implements rpc.Client, returning events from the chain that
// match the request. It respects retention, pagination, and fault injection.
func (c *VirtualChain) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	c.mu.Lock()
	c.callCount++
	call := c.callCount

	latest := c.headLedger
	oldest := c.oldestRetainedLocked()
	c.mu.Unlock()

	// If the fault scheduler injected an error for this call index, return it.
	if injected, ok := faultForCall(call); ok {
		if injected.err != nil {
			return rpc.GetEventsResponse{}, injected.err
		}
	}

	// Determine the range.
	startLedger := req.StartLedger
	var endLedger uint32
	if req.EndLedger > 0 {
		endLedger = req.EndLedger - 1 // endLedger is exclusive in the RPC protocol
	}

	// Cursor-based pagination: the cursor is an event ID. Return events
	// after (strictly greater than) the cursor, regardless of ledger range.
	cursorAfter := ""
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		cursorAfter = req.Pagination.Cursor
		// When cursor is set, the real RPC ignores startLedger/endLedger.
		startLedger = 0
		endLedger = 0
	}

	// Out-of-range check (only when no cursor is set, since cursor ignores
	// ledger range).
	if cursorAfter == "" && startLedger > 0 && startLedger < oldest {
		c.mu.Lock()
		c.outOfRangeGaps = append(c.outOfRangeGaps, outOfRangeGap{requested: startLedger, oldest: oldest})
		c.mu.Unlock()
		return rpc.GetEventsResponse{}, outOfRangeError(oldest, latest)
	}

	// Collect matching events within range, respecting cursor pagination.
	c.mu.Lock()
	var matched []rpc.Event
	pastCursor := cursorAfter == ""
	for _, ce := range c.events {
		// Cursor pagination: skip events up to and including the cursor.
		if !pastCursor {
			if ce.Event.ID == cursorAfter {
				pastCursor = true
			}
			continue
		}
		if startLedger > 0 && ce.Ledger < startLedger {
			continue
		}
		if endLedger > 0 && ce.Ledger > endLedger {
			break
		}
		if ce.Ledger > latest {
			break
		}
		if !matchesFilters(ce.Event, req.Filters) {
			continue
		}
		matched = append(matched, ce.Event)
	}
	c.mu.Unlock()

	// Apply pagination.
	limit := uint(1000) // default
	if req.Pagination != nil && req.Pagination.Limit > 0 {
		limit = req.Pagination.Limit
	}
	if limit > rpc.MaxEventsPerRequest {
		limit = rpc.MaxEventsPerRequest
	}

	var page []rpc.Event
	if uint(len(matched)) > limit {
		page = matched[:limit]
	} else {
		page = matched
	}

	// Build cursor from the last event ID in the page.
	cursor := ""
	if len(page) > 0 {
		cursor = page[len(page)-1].ID
	}

	return rpc.GetEventsResponse{
		Events:       page,
		LatestLedger: latest,
		OldestLedger: oldest,
		Cursor:       cursor,
	}, nil
}

// oldestRetainedLocked assumes mu is held.
func (c *VirtualChain) oldestRetainedLocked() uint32 {
	if c.headLedger <= c.retentionLedgers {
		return GenesisLedger
	}
	return c.headLedger - c.retentionLedgers
}

// OutOfRangeGaps returns the recorded out-of-range gaps for oracle use.
func (c *VirtualChain) OutOfRangeGaps() []outOfRangeGap {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]outOfRangeGap, len(c.outOfRangeGaps))
	copy(out, c.outOfRangeGaps)
	return out
}

// outOfRangeError creates the standard RPC out-of-range error.
func outOfRangeError(oldest, latest uint32) error {
	return &rpc.Error{
		Code:    -32600,
		Message: fmt.Sprintf("startLedger must be within the ledger range: %d - %d", oldest, latest),
		Data:    "ledger range",
	}
}

// matchesFilters checks whether the event matches the given filters.
// An empty filter list matches everything.
func matchesFilters(ev rpc.Event, filters []rpc.EventFilter) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		if matchesFilter(ev, f) {
			return true
		}
	}
	return false
}

func matchesFilter(ev rpc.Event, f rpc.EventFilter) bool {
	if f.Type != "" && ev.Type != f.Type {
		return false
	}
	if len(f.ContractIDs) > 0 {
		found := false
		for _, cid := range f.ContractIDs {
			if ev.ContractID == cid {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	// Topic matching: if topics are specified, all must be present in the event.
	if len(f.Topics) > 0 {
		eventTopics := makeTopicStrings(ev)
		for _, filterTopic := range f.Topics {
			topicMatched := false
			for _, ft := range filterTopic {
				for _, et := range eventTopics {
					if ft == et {
						topicMatched = true
						break
					}
				}
				if topicMatched {
					break
				}
			}
			if !topicMatched {
				return false
			}
		}
	}
	return true
}

// makeTopicStrings extracts string representations of event topics.
func makeTopicStrings(ev rpc.Event) []string {
	var topics []string
	for _, t := range ev.TopicJSON {
		topics = append(topics, string(t))
	}
	return topics
}

// faultForCall checks the global fault schedule for a GetEvents call at
// the given index. The fault schedule is set by the harness before each run.
func faultForCall(call int) (*faultInjection, bool) {
	faultMu.RLock()
	defer faultMu.RUnlock()
	for _, f := range faultSchedule {
		if f.target == "GetEvents" && f.callIndex == call {
			return &faultInjection{err: f.err}, true
		}
	}
	return nil, false
}

// faultInjection is the result of a fault lookup.
type faultInjection struct {
	err error
}

// EngineEvents returns all events created by the chain, sorted by ledger
// then ID. This is the authoritative set of ChainEvents.
func (c *VirtualChain) EngineEvents() []ChainEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ChainEvent, len(c.events))
	copy(out, c.events)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ledger != out[j].Ledger {
			return out[i].Ledger < out[j].Ledger
		}
		return out[i].Event.ID < out[j].Event.ID
	})
	return out
}

// BuildEvent creates an rpc.Event with the given ID, ledger, and contract.
// Fields are populated enough that the ingester can process it.
func BuildEvent(id string, ledger uint32, contractID string) rpc.Event {
	// Ensure ledger is set — the caller typically relies on AddEvents to set
	// it, but for direct use the field is zero here.
	return rpc.Event{
		ID:         id,
		Type:       "contract",
		Ledger:     ledger,
		ContractID: contractID,
		TxHash:     "abc123",
		ValueJSON:  json.RawMessage(`{"u64":1}`),
		TopicJSON:  []json.RawMessage{json.RawMessage(`{"symbol":"test"}`)},
	}
}
