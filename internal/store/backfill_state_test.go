package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackfillRange_HelpersAndContiguousWatermark(t *testing.T) {
	now := time.Now()
	r1 := BackfillRange{FromLedger: 100, ToLedger: 199, LastLedger: 199, Status: BackfillRangeCompleted, UpdatedAt: now}
	r2 := BackfillRange{FromLedger: 200, ToLedger: 299, LastLedger: 240, Status: BackfillRangeFailed, Error: "rate limit", UpdatedAt: now}
	r3 := BackfillRange{FromLedger: 300, ToLedger: 399, LastLedger: 399, Status: BackfillRangeCompleted, UpdatedAt: now}

	assert.True(t, r1.Completed())
	assert.False(t, r2.Completed())
	assert.Equal(t, int64(241), r2.ResumeStartLedger())

	unstarted := BackfillRange{FromLedger: 200, ToLedger: 299, LastLedger: 0, Status: BackfillRangePending}
	assert.Equal(t, int64(200), unstarted.ResumeStartLedger())

	// ContiguousWatermark stops at r2's LastLedger (240) even though r3 (399) is completed.
	assert.Equal(t, int64(240), ContiguousWatermark(100, []BackfillRange{r1, r2, r3}))

	// Once r2 completes, ContiguousWatermark advances through r3 (399).
	r2.Status = BackfillRangeCompleted
	r2.LastLedger = 299
	assert.Equal(t, int64(399), ContiguousWatermark(100, []BackfillRange{r1, r2, r3}))
}
