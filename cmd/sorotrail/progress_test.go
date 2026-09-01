package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ---------- Progress reporter tests ----------

func TestNewProgress_DefaultsToIntervalZero(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("test", 0, WithProgressWriter(&buf))
	// With interval 0, Tick must not write anything.
	p.Tick(10)
	p.Tick(20)
	assert.Empty(t, buf.String(), "interval 0 must suppress periodic output")
}

func TestProgress_TickWritesToWriter(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("test", time.Millisecond, WithProgressWriter(&buf))

	// First tick initializes the reporter; second tick after interval writes.
	p.Tick(10)
	time.Sleep(2 * time.Millisecond)
	p.Tick(20)

	out := buf.String()
	assert.Contains(t, out, "test:", "output must contain the title")
	assert.Contains(t, out, "30", "output must show cumulative position")
}

func TestProgress_Position(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("test", time.Millisecond, WithProgressWriter(&buf))

	p.Tick(5)
	assert.Equal(t, int64(5), p.Position())

	p.Tick(10)
	assert.Equal(t, int64(15), p.Position())
}

func TestProgress_ReportOnComplete(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("backfill", time.Millisecond, WithProgressWriter(&buf))
	p.Tick(100)
	p.Report(true)

	out := buf.String()
	assert.Contains(t, out, "backfill completed")
	assert.Contains(t, out, "100 processed")
}

func TestProgress_ReportOnInterrupt(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("replay", time.Millisecond, WithProgressWriter(&buf))
	p.Tick(50)
	p.Report(false)

	out := buf.String()
	assert.Contains(t, out, "replay interrupted")
	assert.Contains(t, out, "50 processed")
}

func TestProgress_WithSetTotal(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("backfill", time.Millisecond, WithProgressWriter(&buf))
	p.SetTotal(1000)
	p.Tick(100)
	time.Sleep(2 * time.Millisecond)
	p.Tick(100)

	out := buf.String()
	// Should contain percentage
	assert.Contains(t, out, "(20%)", "should show percentage when total is set")
}

func TestProgress_NoWriter(t *testing.T) {
	// Without WithProgressWriter, output goes to io.Discard (default).
	p := NewProgress("test", time.Millisecond)
	p.Tick(10)
	// Should not panic.
}

func TestProgress_InterruptionReporting(t *testing.T) {
	// An interrupted run should still report where it stopped.
	var buf bytes.Buffer
	p := NewProgress("index-addresses", time.Millisecond, WithProgressWriter(&buf))
	p.SetTotal(5000)
	p.Tick(2500)
	p.Report(false)

	out := buf.String()
	assert.Contains(t, out, "index-addresses interrupted")
	assert.Contains(t, out, "2500 processed")
	// Verify the position matches what was reported.
	assert.True(t, strings.Contains(out, "2500"), "must show exact position at interruption")
}

func TestProgress_FinalSummaryReconciles(t *testing.T) {
	// The final summary must reconcile with the last progress report.
	var buf bytes.Buffer
	p := NewProgress("backfill", time.Millisecond, WithProgressWriter(&buf))
	p.SetTotal(100)

	// Simulate several ticks.
	p.Tick(25)
	time.Sleep(2 * time.Millisecond)
	p.Tick(25)
	time.Sleep(2 * time.Millisecond)
	p.Tick(25)
	p.Tick(25)

	p.Report(true)

	out := buf.String()
	assert.Contains(t, out, "100 processed", "final summary must show total")
	assert.Contains(t, out, "completed")
}

func TestProgress_RateCalculated(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("test", time.Millisecond, WithProgressWriter(&buf))
	p.Tick(100)
	time.Sleep(10 * time.Millisecond)
	p.Tick(100)

	out := buf.String()
	assert.Contains(t, out, "rate ", "output must contain rate")
	assert.Contains(t, out, "/s", "rate must be per-second")
}

func TestProgress_EstimatedRemaining(t *testing.T) {
	var buf bytes.Buffer
	p := NewProgress("backfill", time.Millisecond, WithProgressWriter(&buf))
	p.SetTotal(1000)
	p.Tick(100)
	time.Sleep(10 * time.Millisecond)
	p.Tick(100)

	out := buf.String()
	// When total is set and rate > 0, ETA should be shown.
	assert.Contains(t, out, "eta ", "should show estimated time remaining")
}
