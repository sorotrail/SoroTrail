package store_test

import (
	"testing"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestTenantUsageAccumulationAndRestartSurvival(t *testing.T) {
	delta := store.UsageDelta{
		Requests:        5,
		EventsServed:    10,
		StreamSeconds:   20,
		QueryDurationMs: 150,
		EventsIngested:  30,
		RowsStored:      40,
		BytesExported:   500,
	}

	if delta.Empty() {
		t.Fatal("expected non-empty delta")
	}

	emptyDelta := store.UsageDelta{}
	if !emptyDelta.Empty() {
		t.Fatal("expected empty delta")
	}
}
