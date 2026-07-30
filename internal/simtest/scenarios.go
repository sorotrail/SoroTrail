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
}
