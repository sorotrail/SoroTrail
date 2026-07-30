package simtest

import (
	"fmt"
	"sync"

	"github.com/khaylebfortune/sorotrail/internal/rpc"
)

// FaultKind categorizes the types of faults the simulation can inject.
type FaultKind string

const (
	FaultTimeout         FaultKind = "timeout"
	FaultRateLimit       FaultKind = "rate_limit"        // HTTP 429
	FaultMalformedPage   FaultKind = "malformed_page"    // Corrupt events in response
	FaultDuplicateEvents FaultKind = "duplicate_events"  // Same event appears multiple times
	FaultEmptyPageCursor FaultKind = "empty_page_cursor" // Empty page but cursor says more
	FaultOutOfRange      FaultKind = "out_of_range"      // Retention-clamp at bad time
	FaultCrash           FaultKind = "crash"             // Simulate process restart
	FaultRPCMovingBack   FaultKind = "rpc_moving_back"   // RPC reports lower latestLedger
	FaultTruncatedPage   FaultKind = "truncated_page"    // Fewer events than expected
	FaultGetHealthError  FaultKind = "get_health_error"  // getHealth returns an error
)

// FaultDescriptor is one fault the simulation can inject. It targets a
// specific RPC call index or simulation step.
type FaultDescriptor struct {
	// Kind identifies the fault type.
	Kind FaultKind
	// CallIndex is the GetEvents call number (1-based) to fire on.
	CallIndex int
	// AfterStep is the simulation step index (1-based) on which to fire
	// a crash or health error.
	AfterStep int
	// Ledger, when set, is the ledger at which to fire (for faults that
	// target a specific chain position).
	Ledger uint32
	// DuplicateCount is how many duplicates to insert (for FaultDuplicateEvents).
	DuplicateCount int
}

// faultRecord is the internal representation of a scheduled fault.
type faultRecord struct {
	target    string // "GetEvents", "GetHealth", "Crash"
	callIndex int
	err       error
}

var (
	faultMu       sync.RWMutex
	faultSchedule []faultRecord
)

// setFaultSchedule installs the fault schedule for a simulation run.
func setFaultSchedule(faults []FaultDescriptor) {
	faultMu.Lock()
	defer faultMu.Unlock()
	faultSchedule = nil
	for _, fd := range faults {
		switch fd.Kind {
		case FaultTimeout:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetEvents", callIndex: fd.CallIndex,
				err: fmt.Errorf("context deadline exceeded"),
			})
		case FaultRateLimit:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetEvents", callIndex: fd.CallIndex,
				err: &rpc.Error{Code: -32000, Message: "rate limited", Data: "429"},
			})
		case FaultMalformedPage:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetEvents", callIndex: fd.CallIndex,
				err: fmt.Errorf("unexpected end of JSON input"),
			})
		case FaultOutOfRange:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetEvents", callIndex: fd.CallIndex,
				err: &rpc.Error{Code: -32600, Message: "startLedger outside of retention window"},
			})
		case FaultRPCMovingBack:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetEvents", callIndex: fd.CallIndex,
				err: nil, // handled specially in chain
			})
		case FaultTruncatedPage:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetEvents", callIndex: fd.CallIndex,
				err: fmt.Errorf("truncated response"),
			})
		case FaultGetHealthError:
			faultSchedule = append(faultSchedule, faultRecord{
				target: "GetHealth", callIndex: fd.CallIndex,
				err: fmt.Errorf("getHealth: connection refused"),
			})
		}
	}
}

// clearFaultSchedule removes all scheduled faults.
func clearFaultSchedule() {
	faultMu.Lock()
	defer faultMu.Unlock()
	faultSchedule = nil
}

// crashAfterStep returns the step index at which to crash, or 0 if no crash.
func crashAfterStep(faults []FaultDescriptor) int {
	for _, fd := range faults {
		if fd.Kind == FaultCrash {
			return fd.AfterStep
		}
	}
	return 0
}
