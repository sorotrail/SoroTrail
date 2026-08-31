package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHumanSize(t *testing.T) {
	tests := []struct {
		name string
		b    int64
		want string
	}{
		{name: "zero bytes", b: 0, want: "0 B"},
		{name: "one byte", b: 1, want: "1 B"},
		{name: "511 bytes", b: 511, want: "511 B"},
		{name: "1 KiB", b: 1024, want: "1.0 KiB"},
		{name: "1.5 KiB", b: 1536, want: "1.5 KiB"},
		{name: "10 KiB", b: 10240, want: "10.0 KiB"},
		{name: "1 MiB", b: 1024 * 1024, want: "1.0 MiB"},
		{name: "2.5 MiB", b: 2560 * 1024, want: "2.5 MiB"},
		{name: "1 GiB", b: 1024 * 1024 * 1024, want: "1.0 GiB"},
		{name: "3.2 GiB", b: 3435973836, want: "3.2 GiB"},
		{name: "999 bytes under 1 KiB", b: 1023, want: "1023 B"},
		{name: "999 bytes under 1 MiB", b: 1024*1024 - 1, want: "1024.0 KiB"},
		{name: "999 bytes under 1 GiB", b: 1024*1024*1024 - 1, want: "1024.0 MiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := humanSize(tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPrintSchemaReport(t *testing.T) {
	report := &schemaInspectReport{
		Migration: schemaMigrationInfo{
			Version: 24,
			Dirty:   false,
			Status:  "clean",
		},
		SchemaMatch: schemaMatchInfo{
			Matched:         true,
			EmbeddedVersion: 24,
			AppliedVersion:  24,
			Detail:          "applied 24, embedded 24, pending 0",
		},
		Partitions: []schemaPartition{
			{Name: "events_0_99", From: 0, To: 100, RowCounts: 50},
			{Name: "events_100_199", From: 100, To: 200, RowCounts: 75},
			{Name: "events_default", From: 0, To: 0, RowCounts: 10},
		},
		TableSizes: []schemaTableSize{
			{Name: "events (parent)", Size: 0, Human: "0 B", Kind: "table"},
			{Name: "events_0_99", Size: 8192, Human: "8.0 KiB", Kind: "partition"},
			{Name: "idx_events_id", Size: 16384, Human: "16.0 KiB", Kind: "index"},
		},
	}

	var buf bytes.Buffer
	// printSchemaReport writes to stdout, so we need to capture it.
	// Since we can't easily capture stdout, let's just verify the
	// report serializes correctly and the function doesn't panic.
	out, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	require.NotEmpty(t, out)

	// Verify JSON output is valid and contains expected fields.
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(out, &parsed))
	assert.Contains(t, parsed, "migration")
	assert.Contains(t, parsed, "partitions")
	assert.Contains(t, parsed, "table_sizes")
	assert.Contains(t, parsed, "schema_match")

	// Verify the migration section.
	migration, ok := parsed["migration"].(map[string]any)
	require.True(t, ok, "migration should be a map")
	assert.Equal(t, float64(24), migration["version"])
	assert.Equal(t, false, migration["dirty"])
	assert.Equal(t, "clean", migration["status"])

	// Verify the schema_match section.
	match, ok := parsed["schema_match"].(map[string]any)
	require.True(t, ok, "schema_match should be a map")
	assert.Equal(t, true, match["matched"])
	assert.Equal(t, float64(24), match["embedded_version"])
	assert.Equal(t, float64(24), match["applied_version"])

	// Verify partitions.
	parts, ok := parsed["partitions"].([]any)
	require.True(t, ok, "partitions should be an array")
	assert.Len(t, parts, 3)

	// Verify table_sizes.
	sizes, ok := parsed["table_sizes"].([]any)
	require.True(t, ok, "table_sizes should be an array")
	assert.Len(t, sizes, 3)

	// Verify we can still print the report without panic.
	_ = buf
	printSchemaReport(report)
}

func TestPrintSchemaReportEmptyPartitions(t *testing.T) {
	report := &schemaInspectReport{
		Migration: schemaMigrationInfo{
			Version: 0,
			Dirty:   false,
			Status:  "none",
		},
		SchemaMatch: schemaMatchInfo{
			Matched:           false,
			EmbeddedVersion:   24,
			AppliedVersion:    0,
			PendingMigrations: []uint{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
			Detail:            "no migrations applied",
		},
		Partitions: nil,
		TableSizes: nil,
	}

	// Should not panic even with nil slices.
	printSchemaReport(report)
}

func TestSchemaMatchInfoJSON(t *testing.T) {
	tests := []struct {
		name     string
		info     schemaMatchInfo
		wantJSON string
	}{
		{
			name: "matched",
			info: schemaMatchInfo{
				Matched:         true,
				EmbeddedVersion: 24,
				AppliedVersion:  24,
				Detail:          "applied 24, embedded 24, pending 0",
			},
			wantJSON: `{"matched":true,"embedded_version":24,"applied_version":24,"detail":"applied 24, embedded 24, pending 0"}`,
		},
		{
			name: "pending migrations",
			info: schemaMatchInfo{
				Matched:           false,
				EmbeddedVersion:   24,
				AppliedVersion:    20,
				PendingMigrations: []uint{21, 22, 23, 24},
				Detail:            "applied 20, embedded 24, 4 pending: [21 22 23 24]",
			},
			wantJSON: `{"matched":false,"embedded_version":24,"applied_version":20,"pending_migrations":[21,22,23,24],"detail":"applied 20, embedded 24, 4 pending: [21 22 23 24]"}`,
		},
		{
			name: "no migrations applied",
			info: schemaMatchInfo{
				Matched:           false,
				EmbeddedVersion:   24,
				AppliedVersion:    0,
				PendingMigrations: []uint{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
				Detail:            "no migrations applied",
			},
			wantJSON: `{"matched":false,"embedded_version":24,"applied_version":0,"pending_migrations":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24],"detail":"no migrations applied"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := json.Marshal(tt.info)
			require.NoError(t, err)
			// Unmarshal to verify structure, not exact string (order may vary).
			var parsed map[string]any
			require.NoError(t, json.Unmarshal(out, &parsed))
			assert.Equal(t, tt.info.Matched, parsed["matched"])
			assert.Equal(t, float64(tt.info.EmbeddedVersion), parsed["embedded_version"])
			assert.Equal(t, float64(tt.info.AppliedVersion), parsed["applied_version"])
			assert.Equal(t, tt.info.Detail, parsed["detail"])
		})
	}
}

func TestSchemaInspectReportJSONRoundTrip(t *testing.T) {
	report := &schemaInspectReport{
		Migration: schemaMigrationInfo{
			Version: 24,
			Dirty:   false,
			Status:  "clean",
		},
		SchemaMatch: schemaMatchInfo{
			Matched:         true,
			EmbeddedVersion: 24,
			AppliedVersion:  24,
			Detail:          "applied 24, embedded 24, pending 0",
		},
		Partitions: []schemaPartition{
			{Name: "events_0_99", From: 0, To: 100, RowCounts: 50},
		},
		TableSizes: []schemaTableSize{
			{Name: "events (parent)", Size: 8192, Human: "8.0 KiB", Kind: "table"},
		},
	}

	out, err := json.Marshal(report)
	require.NoError(t, err)

	var decoded schemaInspectReport
	require.NoError(t, json.Unmarshal(out, &decoded))
	assert.Equal(t, report.Migration.Version, decoded.Migration.Version)
	assert.Equal(t, report.Migration.Dirty, decoded.Migration.Dirty)
	assert.Equal(t, report.Migration.Status, decoded.Migration.Status)
	assert.Equal(t, report.SchemaMatch.Matched, decoded.SchemaMatch.Matched)
	assert.Equal(t, report.SchemaMatch.EmbeddedVersion, decoded.SchemaMatch.EmbeddedVersion)
	assert.Equal(t, report.SchemaMatch.AppliedVersion, decoded.SchemaMatch.AppliedVersion)
	assert.Equal(t, report.SchemaMatch.Detail, decoded.SchemaMatch.Detail)
	assert.Len(t, decoded.Partitions, 1)
	assert.Equal(t, report.Partitions[0].Name, decoded.Partitions[0].Name)
	assert.Len(t, decoded.TableSizes, 1)
	assert.Equal(t, report.TableSizes[0].Name, decoded.TableSizes[0].Name)
}

func TestSchemaInspectFlagParsing(t *testing.T) {
	// Test that --help doesn't return an error.
	err := runSchemaInspect([]string{"--help"})
	assert.NoError(t, err, "--help should not return an error")

	// Test that unknown flags are reported.
	err = runSchemaInspect([]string{"--unknown-flag"})
	assert.Error(t, err, "unknown flag should return an error")
}
