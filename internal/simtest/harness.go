package simtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Scenario describes one simulation test case: the chain setup, fault
// schedule, and expected outcome.
type Scenario struct {
	// Name identifies the scenario in test output and CI logs.
	Name string
	// Description explains what property or historical bug this covers.
	Description string
	// RetentionLedgers is the RPC retention window size.
	RetentionLedgers uint32
	// StartLedger is the ingester's explicit start position (overrides
	// cold-start logic). Set to 0 for cold-start behavior.
	StartLedger uint32
	// PageLimit is the ingester's pagination limit.
	PageLimit uint
	// SweepWindow is the ledger sweep window size.
	SweepWindow uint32
	// ChainLedgers is the number of ledgers the chain advances to (events
	// are placed at specific ledgers by the scenario builder).
	ChainLedgers uint32
	// Events is the list of events to place on the chain at specific ledgers.
	Events []EventPlacement
	// Faults is the fault schedule for this scenario.
	Faults []FaultDescriptor
	// Steps is how many RunOnce steps to execute (0 = run until caught up).
	Steps int
	// WatchedContracts, if non-empty, seeds the store with these contracts.
	WatchedContracts []string
	// ExpectNoLoss means every event should be in the store (no legitimate gaps).
	ExpectNoLoss bool
}

// EventPlacement places one event at a specific ledger in the chain.
type EventPlacement struct {
	Ledger     uint32
	ContractID string
}

// Harness runs a simulation scenario with a fake RPC and a real (or mock)
// store, then invokes the oracle to verify correctness.
type Harness struct {
	Scenario Scenario
	Chain    *VirtualChain
	Store    store.Store
	Oracle   *Oracle

	clock *VirtualClock
	ing   *ingester.Ingester
	rng   *rand.Rand
}

// NewHarness creates a simulation harness for the given scenario and store.
func NewHarness(scenario Scenario, st store.Store) *Harness {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := NewVirtualClock(now)
	rng := rand.New(rand.NewPCG(42, 42))

	// Override seed if the scenario name provides one.
	scenario.applyDefaults()

	rpcFake := NewVirtualChain(clock, now, scenario.RetentionLedgers)

	oracle := NewOracle(rpcFake)

	return &Harness{
		Scenario: scenario,
		Chain:    rpcFake,
		Store:    st,
		Oracle:   oracle,
		clock:    clock,
		rng:      rng,
	}
}

// applyDefaults fills in zero-valued scenario fields.
func (s *Scenario) applyDefaults() {
	if s.RetentionLedgers == 0 {
		s.RetentionLedgers = 100
	}
	if s.PageLimit == 0 {
		s.PageLimit = 100
	}
	if s.SweepWindow == 0 {
		s.SweepWindow = 1000
	}
	if s.ChainLedgers == 0 && len(s.Events) > 0 {
		// Determine max ledger from events.
		for _, ep := range s.Events {
			if ep.Ledger > s.ChainLedgers {
				s.ChainLedgers = ep.Ledger
			}
		}
	}
}

// Run executes the scenario and returns an error if the oracle finds a
// mismatch.
func (h *Harness) Run(ctx context.Context) error {
	// Build the chain: generate events at specified ledgers.
	for i, ep := range h.Scenario.Events {
		id := fmt.Sprintf("%010d-%09d", ep.Ledger, i)
		ev := BuildEvent(id, ep.Ledger, ep.ContractID)
		h.Chain.AddEvents(ep.Ledger, ev)
	}

	// Advance the chain to its target head.
	if h.Scenario.ChainLedgers > 0 {
		h.Chain.AdvanceTo(h.Scenario.ChainLedgers)
	}

	// Seed watched contracts.
	for _, cid := range h.Scenario.WatchedContracts {
		if err := h.Store.AddWatchedContract(ctx, cid); err != nil {
			return fmt.Errorf("adding watched contract %s: %w", cid, err)
		}
	}

	// Setup fault schedule.
	setFaultSchedule(h.Scenario.Faults)
	defer clearFaultSchedule()

	// Determine crash step.
	crashAt := crashAfterStep(h.Scenario.Faults)

	// Create the ingester.
	h.newIngester()

	// Run simulation steps.
	for step := 1; ; step++ {
		// Check for crash injection.
		if crashAt > 0 && step > crashAt {
			// The ingester "crashes" — we create a new one with the
			// same store, so only persisted state survives.
			h.newIngester()
			crashAt = 0 // only crash once
		}

		// Check if GetHealth fault is scheduled.
		for _, fd := range h.Scenario.Faults {
			if fd.Kind == FaultGetHealthError && fd.AfterStep == step-1 {
				h.Chain.SetHealthError(fmt.Errorf("injected health error at step %d", step))
			}
		}

		// Check if RPC-moving-back fault is scheduled.
		for _, fd := range h.Scenario.Faults {
			if fd.Kind == FaultRPCMovingBack && fd.AfterStep == step-1 {
				// Artificially lower the chain head to simulate provider flap.
				current := h.Chain.LatestLedger()
				if current > fd.Ledger && fd.Ledger > 0 {
					h.Chain.AdvanceTo(fd.Ledger) // move head back
				}
			}
		}

		caughtUp, err := h.ing.RunOnceForTest(ctx)
		_ = err // transient errors (timeouts, etc.) are expected and retried
		_ = caughtUp

		// After each step, check retention-gap tracking.
		h.trackRetentionGaps()

		// Determine whether to continue.
		if h.Scenario.Steps > 0 && step >= h.Scenario.Steps {
			break
		}
		if h.Scenario.Steps == 0 && caughtUp {
			// Run one extra step after caught-up to ensure stability.
			if step > 1 {
				break
			}
		}
	}

	// Drain out-of-range gaps from the chain into the oracle before
	// verification.
	h.Oracle.DrainGaps()

	// Run the oracle.
	return h.Oracle.Verify(ctx, h.Store)
}

// newIngester creates a fresh ingester instance connected to the chain and
// store. This is called on initial setup and after simulated crashes.
func (h *Harness) newIngester() {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	h.ing = ingester.New(
		h.Chain, // rpc.Client
		h.Store,
		decode.XDRDecoder{},
		log,
		ingester.Options{
			PollInterval:     time.Second, // irrelevant — clock is virtual
			StartLedger:      h.Scenario.StartLedger,
			RetentionLedgers: h.Scenario.RetentionLedgers,
			PageLimit:        h.Scenario.PageLimit,
			SweepWindow:      h.Scenario.SweepWindow,
			Clock:            h.clock,
			Jitter:           DeterministicJitter(h.rng),
		},
	)
}

// trackRetentionGaps records legitimate data loss: if the ingester's resume
// ledger is below the chain's oldest retained ledger, a gap exists.
func (h *Harness) trackRetentionGaps() {
	state, err := h.Store.GetIngestionState(context.Background())
	if err != nil {
		return
	}

	oldest := h.Chain.OldestRetained()
	nextLedger := uint32(state.LastIngestedLedger) + 1
	if nextLedger > 1 && nextLedger < oldest {
		// The ingester is about to (or has) hit a retention gap.
		// This ledger range is legitimately lost.
		h.Oracle.RecordReclamp(nextLedger, oldest)
	}
}
