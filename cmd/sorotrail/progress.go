package main

import (
	"fmt"
	"io"
	"time"
)

// Progress reports periodic status for long-running operations.
// All output goes to stderr so stdout stays parseable for scripts.
type Progress struct {
	w           io.Writer
	interval    time.Duration
	title       string
	position    int64
	totalHint   int64 // 0 means unknown
	lastBatch   int64
	batchCount  int64
	lastTime    time.Time
	initialized bool
}

// ProgressOption configures a Progress instance.
type ProgressOption func(*Progress)

// WithProgressWriter redirects progress output to the given writer
// (default os.Stderr).
func WithProgressWriter(w io.Writer) ProgressOption {
	return func(p *Progress) { p.w = w }
}

// NewProgress creates a progress reporter that writes to stderr at the
// given interval. Pass 0 to disable periodic output (the final summary
// is still printed).
func NewProgress(title string, interval time.Duration, opts ...ProgressOption) *Progress {
	p := &Progress{
		w:        io.Discard,
		interval: interval,
		title:    title,
		lastTime: time.Now(),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// SetTotal sets the expected total for percentage estimates. Pass 0
// to indicate the total is unknown.
func (p *Progress) SetTotal(n int64) { p.totalHint = n }

// Tick records that n items were processed. It prints a progress line
// when at least the configured interval has elapsed.
func (p *Progress) Tick(n int64) {
	if p.interval <= 0 {
		return
	}
	p.position += n
	p.batchCount++
	p.lastBatch = n

	now := time.Now()
	if !p.initialized {
		p.initialized = true
		p.lastTime = now
		return
	}

	elapsed := now.Sub(p.lastTime)
	if elapsed >= p.interval {
		p.report(now)
		p.lastTime = now
	}
}

func (p *Progress) report(now time.Time) {
	elapsed := now.Sub(p.lastTime)
	if elapsed <= 0 {
		return
	}
	rate := float64(p.position) / time.Since(p.lastTime.Add(-elapsed)).Seconds()
	if rate <= 0 {
		rate = float64(p.position) / time.Since(p.lastTime).Seconds()
	}

	line := p.formatLine(rate, now)
	fmt.Fprintln(p.w, line)
}

func (p *Progress) formatLine(rate float64, now time.Time) string {
	elapsed := time.Since(p.lastTime)
	totalElapsed := elapsed

	var rateStr string
	if rate > 0 {
		rateStr = fmt.Sprintf("%.1f/s", rate)
	} else {
		rateStr = "..."
	}

	var pct string
	if p.totalHint > 0 && p.position > 0 {
		pctVal := float64(p.position) / float64(p.totalHint) * 100
		pct = fmt.Sprintf(" (%.0f%%)", pctVal)
	}

	var etaStr string
	if p.totalHint > 0 && rate > 0 && p.position < p.totalHint {
		remaining := float64(p.totalHint-p.position) / rate
		etaStr = fmt.Sprintf("  eta %s", formatDuration(time.Duration(remaining)))
	}

	return fmt.Sprintf("  %s: %d%s  rate %s  elapsed %s%s",
		p.title, p.position, pct, rateStr, formatDuration(totalElapsed), etaStr)
}

// Report writes a final summary line to the writer.
func (p *Progress) Report(completed bool) {
	elapsed := time.Since(p.lastTime)
	if !p.initialized {
		elapsed = 0
	}

	status := "completed"
	if !completed {
		status = "interrupted"
	}

	rate := float64(0)
	if elapsed > 0 {
		rate = float64(p.position) / elapsed.Seconds()
	}

	line := fmt.Sprintf("  %s %s: %d processed  rate %.1f/s  elapsed %s",
		p.title, status, p.position, rate, formatDuration(elapsed))
	fmt.Fprintln(p.w, line)
}

// Position returns the current count of processed items.
func (p *Progress) Position() int64 { return p.position }

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	if d < time.Minute {
		return d.Round(time.Second).String()
	}
	return d.Round(time.Second).String()
}
