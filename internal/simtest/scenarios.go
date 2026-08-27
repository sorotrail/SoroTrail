package simtest

// CuratedScenarios is the suite of named scenarios that run in normal CI.
// Each covers a known bug class or edge case in the ingestion pipeline.
var CuratedScenarios = []Scenario{
	//
	// Scenario 1: Crash between UpsertEvents and SaveIngestionState
	//
	// The ingester persists events then saves state. If a crash occurs
	// between these two operations, the events are stored but the cursor
	// is not advanced. On restart, the ingester re-fetches the same page.
	// Idempotent upserts must prevent duplicates.
	//
	// Documented order: UpsertEvents happens before SaveIngestionState
	// in singlePage (ingester.go). This is safe because UpsertEvents is
	// idempotent — a crash after persist means the next run re-fetches
	// the same page and upserts the same rows harmlessly.
	{
		Name:             "crash_between_persist_and_save_state",
		Description:      "Crash after UpsertEvents but before SaveIngestionState: events must not duplicate.",
		RetentionLedgers: 200,
		ChainLedgers:     50,
		PageLimit:        10,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 7, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 8, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 9, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 10, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 11, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 12, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 13, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 14, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 15, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultCrash, AfterStep: 1},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 2: Cold start within retention, all events ingested
	//
	{
		Name:             "cold_start_all_in_retention",
		Description:      "Cold start within retention; all generated events must be ingested.",
		RetentionLedgers: 200,
		ChainLedgers:     50,
		PageLimit:        100,
		Events: []EventPlacement{
			{Ledger: 3, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 4, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 10, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 3: RPC head moving backwards briefly (provider flap), timeout retry
	//
	{
		Name:             "rpc_flap_and_timeout_duplicate",
		Description:      "RPC reports a lower head briefly (provider flap), then a timeout causes a retry.",
		RetentionLedgers: 200,
		ChainLedgers:     50,
		PageLimit:        5,
		Events: []EventPlacement{
			{Ledger: 3, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 4, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 7, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 8, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultTimeout, CallIndex: 1},
			{Kind: FaultRPCMovingBack, AfterStep: 0, Ledger: 5},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 4: Empty page with valid cursor (multiple events at same ledger)
	//
	{
		Name:             "multiple_events_same_ledger",
		Description:      "Multiple events at the same ledger, requiring cursor pagination across pages.",
		RetentionLedgers: 200,
		ChainLedgers:     20,
		PageLimit:        5,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 5: Retention clamp — legitimate loss
	//
	// Start ledger is far behind the chain head; the retention window has
	// already passed. The ingester re-clamps and skips ahead. Events in the
	// gap are legitimately lost.
	{
		Name:             "retention_clamp_legitimate_loss",
		Description:      "Resume point aged out of retention: ingester warns and skips ahead. Lost events are tracked, stored events verified.",
		RetentionLedgers: 10,
		ChainLedgers:     100,
		StartLedger:      5,
		PageLimit:        100,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 95, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		ExpectNoLoss: false,
		Steps:        2,
	},

	//
	// Scenario 6: Crash between persist and save state with large page
	//
	{
		Name:             "crash_recovery_pagination",
		Description:      "Crash mid-pagination: cursor resumes from persisted state, no duplicates.",
		RetentionLedgers: 200,
		ChainLedgers:     50,
		PageLimit:        3,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultCrash, AfterStep: 1},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 7: Rate limit on first RPC page is retried
	//
	// The RPC returns HTTP 429 on the first GetEvents call. The ingester
	// must treat this as a transient error, back off, and retry the same
	// page without losing or duplicating any events.
	{
		Name:             "rpc_fault_rate_limit",
		Description:      "A rate-limited page is retried without loss or duplication.",
		RetentionLedgers: 200,
		ChainLedgers:     30,
		PageLimit:        4,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 7, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 8, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultRateLimit, CallIndex: 1},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 8: Malformed page on first RPC call is retried
	//
	// The RPC returns an undecodable JSON response. The ingester must
	// retry and eventually ingest all events once the RPC returns a
	// valid page.
	{
		Name:             "rpc_fault_malformed_page",
		Description:      "A malformed page is retried without loss or duplication.",
		RetentionLedgers: 200,
		ChainLedgers:     30,
		PageLimit:        4,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 7, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 8, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultMalformedPage, CallIndex: 1},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 9: Truncated page on first RPC call is retried
	//
	// The RPC returns a truncated response. The ingester must handle the
	// error gracefully and retry until a complete page is received.
	{
		Name:             "rpc_fault_truncated_page",
		Description:      "A truncated page is retried without loss or duplication.",
		RetentionLedgers: 200,
		ChainLedgers:     30,
		PageLimit:        4,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 7, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 8, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultTruncatedPage, CallIndex: 1},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 10: Health check error does not block ingestion
	//
	// getHealth returns a transient error during one step. The ingester
	// must continue and eventually ingest all events once health clears.
	{
		Name:             "rpc_fault_health_error",
		Description:      "A health failure does not prevent a later ingest step from recovering.",
		RetentionLedgers: 200,
		ChainLedgers:     30,
		PageLimit:        4,
		Steps:            3,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultGetHealthError, AfterStep: 1},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 11: Chain reorganisation — head moves back then forward
	//
	// The chain head briefly drops (e.g. a fork is reverted). The ingester
	// must not skip events it already fetched, and must still ingest events
	// that appear after the chain recovers.
	{
		Name:             "chain_reorg_head_moves_back",
		Description:      "Chain head drops then recovers; events at the reorg boundary are not lost.",
		RetentionLedgers: 200,
		ChainLedgers:     40,
		PageLimit:        5,
		Events: []EventPlacement{
			{Ledger: 10, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 15, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 20, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 30, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultRPCMovingBack, AfterStep: 0, Ledger: 15},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 12: Multiple chain reorgs in sequence
	//
	// The chain head moves back twice in quick succession, simulating a
	// turbulent provider. The ingester must handle repeated head drops
	// without duplicating or losing events.
	{
		Name:             "chain_reorg_repeated",
		Description:      "Repeated head drops cause the ingester to re-fetch pages; no duplicates or loss.",
		RetentionLedgers: 200,
		ChainLedgers:     50,
		PageLimit:        5,
		Events: []EventPlacement{
			{Ledger: 10, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 15, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 20, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 25, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 30, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 40, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 45, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultRPCMovingBack, AfterStep: 0, Ledger: 15},
			{Kind: FaultRPCMovingBack, AfterStep: 1, Ledger: 10},
		},
		ExpectNoLoss: true,
	},

	//
	// Scenario 13: Crash during fault recovery — restart-in-the-middle
	//
	// A timeout fault fires on the first RPC call, and the ingester
	// crashes at the same step. On restart the ingester must re-fetch
	// the page (the fault is already consumed) and ingest every event.
	// This exercises the restart-in-the-middle path for the fault
	// recovery code.
	{
		Name:             "crash_during_fault_retry",
		Description:      "Crash on the same step as a timeout fault; restart must recover all events.",
		RetentionLedgers: 200,
		ChainLedgers:     30,
		PageLimit:        4,
		Events: []EventPlacement{
			{Ledger: 5, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 6, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 7, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 8, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
			{Ledger: 9, ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Faults: []FaultDescriptor{
			{Kind: FaultTimeout, CallIndex: 1},
			{Kind: FaultCrash, AfterStep: 1},
		},
		ExpectNoLoss: true,
	},
}
