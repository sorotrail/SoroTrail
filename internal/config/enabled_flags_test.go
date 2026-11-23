package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRetentionEnabled tables the two policy dimensions against the
// accessor cmd/sorotrail/main.go gates the pruner on: with both unset the
// pruner never starts, either policy alone is enough to run it, and the
// largest ledger a uint64 can hold must not wrap into "off".
//
// A negative age can never come from the environment — Validate rejects
// RETENTION_MAX_AGE < 0 — but the accessor still defines what a hand-built
// Config means, so the row pins that any non-positive age reads as unset.
func TestRetentionEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"both policies unset", Config{}, false},
		{"age policy alone", Config{RetentionMaxAge: 24 * time.Hour}, true},
		{"smallest positive age", Config{RetentionMaxAge: time.Nanosecond}, true},
		{"ledger policy alone", Config{RetentionMinLedger: 1}, true},
		{"largest ledger value", Config{RetentionMinLedger: ^uint64(0)}, true},
		{"both policies", Config{RetentionMaxAge: 72 * time.Hour, RetentionMinLedger: 1000}, true},
		{"negative age reads as unset", Config{RetentionMaxAge: -time.Hour}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.RetentionEnabled())
		})
	}
}

// TestDefaultEventRetention pins the horizon a tenant without its own
// retention policy inherits (#1249): whichever retention job deletes first.
func TestDefaultEventRetention(t *testing.T) {
	day := 24 * time.Hour
	assert.Equal(t, time.Duration(0), Config{}.DefaultEventRetention(), "no age job: no age bound")
	assert.Equal(t, 30*day, Config{RetentionMaxAge: 30 * day}.DefaultEventRetention())
	assert.Equal(t, 7*day, Config{RetentionAge: 7 * day}.DefaultEventRetention())
	assert.Equal(t, 7*day, Config{RetentionMaxAge: 30 * day, RetentionAge: 7 * day}.DefaultEventRetention())
	assert.Equal(t, time.Duration(0), Config{RetentionAge: -day}.DefaultEventRetention())
}

// TestArchiveEnabled tables the flag that decides whether an archiver is
// constructed at all. The bucket is the switch: without it every other
// archive_* value is inert and the binary behaves exactly like the
// pre-archive build, while any non-empty value — Validate trims nothing
// and only demands an endpoint alongside — wires the export in.
func TestArchiveEnabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{"bucket unset", Config{}, false},
		{"bucket set", Config{ArchiveBucket: "sorotrail-events"}, true},
		{"whitespace counts as set", Config{ArchiveBucket: " "}, true},
		{"other archive fields alone", Config{
			ArchivePrefix:   "events/",
			ArchiveEndpoint: "http://127.0.0.1:9000",
		}, false},
		{"bucket with endpoint", Config{
			ArchiveBucket:   "sorotrail-events",
			ArchiveEndpoint: "http://127.0.0.1:9000",
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.ArchiveEnabled())
		})
	}
}

// TestRetentionOnWithoutArchive pins the combination the issue calls out:
// a retention policy configured and no bucket. main.go registers the
// pruner and constructs no archiver (NewWithArchiver receives a nil
// arch), so events are pruned without ever being exported — the
// documented behaviour of a deployment that never opted into archiving,
// not a misconfiguration. Both policy dimensions are checked because each
// one independently starts the pruner.
func TestRetentionOnWithoutArchive(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"age policy without a bucket", Config{RetentionMaxAge: 24 * time.Hour}},
		{"ledger policy without a bucket", Config{RetentionMinLedger: 500}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, tt.cfg.RetentionEnabled(),
				"the pruner must run: retention is configured")
			assert.False(t, tt.cfg.ArchiveEnabled(),
				"no bucket means nothing is exported before deletion")
		})
	}
}
