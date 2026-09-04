package spec

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseScSpecEntriesRawAndParseSpecEntries(t *testing.T) {
	// We construct valid/invalid raw xdr/json byte streams or use package structures
	// to exercise parseScSpecEntriesRaw and parseSpecEntries.
	// Since ScSpecEntry is defined via stellar/go or internal packages, let's test via JSON/XDR or mock helpers where applicable.

	t.Run("empty section yields empty spec", func(t *testing.T) {
		// An empty or zero-length raw slice should return an empty spec/entries without error.
		spec, err := parseSpecEntries([]byte{})
		require.NoError(t, err)
		assert.NotNil(t, spec)
		assert.Empty(t, spec.Events)
	})

	t.Run("truncated stream is rejected without panicking", func(t *testing.T) {
		// Deliberately truncated byte sequence
		corruptBytes := []byte{0x01, 0x00, 0x00, 0x00, 0xff}
		_, err := parseScSpecEntriesRaw(corruptBytes)
		assert.Error(t, err)
	})

	t.Run("deliberately corrupt fixture is handled robustly", func(t *testing.T) {
		corruptFixture := bytes.Repeat([]byte{0xFF}, 64)
		_, err := parseScSpecEntriesRaw(corruptFixture)
		assert.Error(t, err)
	})
}

func TestScalarValueAndParseTypeFromTag(t *testing.T) {
	t.Run("scalar value extraction", func(t *testing.T) {
		val, err := ScalarValue([]byte(`{"symbol":"transfer"}`))
		require.NoError(t, err)
		assert.Equal(t, "transfer", val)
	})

	t.Run("parse type from tag", func(t *testing.T) {
		typ := ParseTypeFromTag([]byte(`{"i128":"1000"}`))
		assert.Equal(t, "i128", typ)
	})
}

func TestScalarValue(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    any
		wantErr bool
	}{
		{"symbol", `{"symbol":"transfer"}`, "transfer", false},
		{"address", `{"address":"G..."}`, "G...", false},
		{"i128", `{"i128":"1000"}`, "1000", false},
		{"u64", `{"u64":42}`, 42.0, false},
		{"bool", `{"bool":true}`, true, false},
		{"unknown type", `{"unknown":{"type":"foo","base64":"bar"}}`, map[string]any{"type": "foo", "base64": "bar"}, false},
		{"empty", `{}`, nil, true},
		{"invalid json", `invalid`, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScalarValue(json.RawMessage(tt.raw))
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseTypeFromTag(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"symbol", `{"symbol":"transfer"}`, "symbol"},
		{"i128", `{"i128":"1000"}`, "i128"},
		{"empty", `{}`, ""},
		{"invalid", `invalid`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseTypeFromTag(json.RawMessage(tt.raw))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEnricher_EnrichEvents_WithSpec(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cache := NewCache(nil)

	t.Run("contract with matching spec", func(t *testing.T) {
		// Pre-populate cache with a spec.
		cache.Set(context.Background(), &ContractSpec{
			WasmHash:   "testhash",
			ContractID: "CDLZ...",
			Events: []EventSpec{
				{
					Name: "transfer",
					TopicSpecs: []FieldSpec{
						{Name: "from", Type: "address"},
						{Name: "to", Type: "address"},
					},
					ValueSpec: &FieldSpec{Name: "amount", Type: "i128"},
				},
			},
		})

		// Enricher without a fetcher (use cached specs only).
		enricher := NewEnricher(nil, cache, log)

		events := []store.Event{
			{
				ID:         "evt1",
				ContractID: "CDLZ...",
				Topics:     json.RawMessage(`[{"symbol":"transfer"},{"address":"GA...FROM"},{"address":"GB...TO"}]`),
				Value:      json.RawMessage(`{"i128":"5000"}`),
			},
		}

		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.True(t, enriched[0].Decoded, "event should be decoded")
		require.NotNil(t, enriched[0].DecodedEvent)
		assert.Equal(t, "transfer", enriched[0].DecodedEvent.Event)
		require.NotNil(t, enriched[0].DecodedEvent.Fields)
		assert.Equal(t, "GA...FROM", enriched[0].DecodedEvent.Fields["from"])
		assert.Equal(t, "GB...TO", enriched[0].DecodedEvent.Fields["to"])
		assert.Equal(t, "5000", enriched[0].DecodedEvent.Fields["amount"])
	})

	t.Run("contract without spec", func(t *testing.T) {
		enricher := NewEnricher(nil, cache, log)
		events := []store.Event{
			{
				ID:         "evt2",
				ContractID: "UNKNOWN",
				Topics:     json.RawMessage(`[{"symbol":"something"}]`),
				Value:      json.RawMessage(`{"u64":1}`),
			},
		}

		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded, "unknown contract should not decode")
		assert.Nil(t, enriched[0].DecodedEvent)
	})

	t.Run("nil enricher returns events as-is", func(t *testing.T) {
		events := []store.Event{
			{ID: "evt3", Topics: json.RawMessage(`[{"symbol":"test"}]`)},
		}
		enriched := (*Enricher)(nil).EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
	})
}

func TestEnricher_GracefulDegradation(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cache := NewCache(nil)

	t.Run("malformed topics", func(t *testing.T) {
		enricher := NewEnricher(nil, cache, log)
		events := []store.Event{
			{
				ID:         "evt1",
				ContractID: "CDLZ...",
				Topics:     json.RawMessage(`"not an array"`),
				Value:      json.RawMessage(`{"i128":"100"}`),
			},
		}
		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
	})

	t.Run("nil event topics", func(t *testing.T) {
		enricher := NewEnricher(nil, cache, log)
		events := []store.Event{
			{
				ID:         "evt2",
				ContractID: "CDLZ...",
				Topics:     nil,
				Value:      json.RawMessage(`{"i128":"100"}`),
			},
		}
		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
	})

	t.Run("event name not matching spec", func(t *testing.T) {
		// Pre-populate cache.
		cache.Set(context.Background(), &ContractSpec{
			WasmHash:   "hash",
			ContractID: "CDLZ...",
			Events:     []EventSpec{{Name: "burn"}},
		})
		enricher := NewEnricher(nil, cache, log)
		events := []store.Event{
			{
				ID:         "evt3",
				ContractID: "CDLZ...",
				Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
			},
		}
		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
	})
}

func TestEnricher_SurfacesDecodeErrors(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cache := NewCache(nil)
	cache.Set(context.Background(), &ContractSpec{
		WasmHash:   "hash",
		ContractID: "CDLZ...",
		Events: []EventSpec{
			{
				Name:       "transfer",
				TopicSpecs: []FieldSpec{{Name: "from", Type: "address"}},
				ValueSpec:  &FieldSpec{Name: "amount", Type: "i128"},
			},
		},
	})
	enricher := NewEnricher(nil, cache, log)

	t.Run("field decode failure sets decode_error and keeps raw event", func(t *testing.T) {
		events := []store.Event{
			{
				ID:         "evt1",
				ContractID: "CDLZ...",
				// topic[1] is declared as an address field by the spec but the
				// payload is a plain number, not a shape-tagged object — the
				// scalar decode fails.
				Topics: json.RawMessage(`[{"symbol":"transfer"},123]`),
				Value:  json.RawMessage(`{"i128":"5000"}`),
			},
		}
		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
		require.NotEmpty(t, enriched[0].DecodeError)
		assert.Contains(t, enriched[0].DecodeError, "from")
		// The raw event must be preserved alongside the error. ID and Topics
		// are promoted from the embedded raw Event.
		assert.Equal(t, "evt1", enriched[0].ID)
		assert.JSONEq(t, `[{"symbol":"transfer"},123]`, string(enriched[0].Topics))
		assert.Empty(t, enriched[0].DecodedEvent) // no partial enriched view on failure
	})

	t.Run("malformed topics sets decode_error and counts a failure", func(t *testing.T) {
		events := []store.Event{
			{ID: "evt2", ContractID: "CDLZ...", Topics: json.RawMessage(`"not an array"`)},
		}
		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
		require.NotEmpty(t, enriched[0].DecodeError)
		assert.Equal(t, "evt2", enriched[0].ID) // raw event preserved (promoted from embedded Event)
	})

	t.Run("no spec is not a decode error", func(t *testing.T) {
		events := []store.Event{
			{
				ID:         "evt3",
				ContractID: "UNKNOWN",
				Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
			},
		}
		enriched := enricher.EnrichEvents(context.Background(), events)
		require.Len(t, enriched, 1)
		assert.False(t, enriched[0].Decoded)
		assert.Empty(t, enriched[0].DecodeError)
	})
}

func TestEnricher_DecodeStats(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cache := NewCache(nil)
	cache.Set(context.Background(), &ContractSpec{
		WasmHash:   "hash",
		ContractID: "CDLZ...",
		Events: []EventSpec{
			{
				Name:       "transfer",
				TopicSpecs: []FieldSpec{{Name: "from", Type: "address"}},
				ValueSpec:  &FieldSpec{Name: "amount", Type: "i128"},
			},
		},
	})
	enricher := NewEnricher(nil, cache, log) // One good decode, two failures (bad event shape, bad field payload).
	enricher.EnrichEvents(context.Background(), []store.Event{
		{
			ContractID: "CDLZ...",
			Topics:     json.RawMessage(`[{"symbol":"transfer"},{"address":"G..."}]`),
			Value:      json.RawMessage(`{"i128":"1"}`),
		},
		{ContractID: "CDLZ...", Topics: json.RawMessage(`"junk"`)},                      // no event name
		{ContractID: "CDLZ...", Topics: json.RawMessage(`[{"symbol":"transfer"},123]`)}, // field not shape-tagged
	})

	d := enricher.DecodeStats()
	assert.Equal(t, uint64(3), d.Decodes)
	assert.Equal(t, uint64(2), d.DecodeFailures)

	// A nil enricher reports zero values, so callers can always call it.
	var nilEnricher *Enricher
	assert.Equal(t, uint64(0), nilEnricher.DecodeStats().Decodes)
}

func TestCacheStats(t *testing.T) {
	cache := NewCache(nil)

	stats := cache.Stats()
	assert.Equal(t, 0, stats.CachedSpecs)

	cache.Set(context.Background(), &ContractSpec{WasmHash: "h1", ContractID: "c1"})
	cache.Set(context.Background(), &ContractSpec{WasmHash: "h2", ContractID: "c2"})

	stats = cache.Stats()
	assert.Equal(t, 2, stats.CachedSpecs)
}

func TestLoadFromDB(t *testing.T) {
	db := &stubSpecStore{}
	cache := NewCache(db)

	// No spec in DB.
	spec, err := cache.LoadFromDB(context.Background(), "missing")
	require.NoError(t, err)
	assert.Nil(t, spec)

	// Set spec in DB directly.
	orig := &ContractSpec{WasmHash: "dbhash", ContractID: "CDLZ...", Events: []EventSpec{{Name: "test"}}}
	data, _ := json.Marshal(orig)
	db.SetContractSpec(context.Background(), "dbhash", "CDLZ...", data)

	// Load from DB into cache.
	spec, err = cache.LoadFromDB(context.Background(), "dbhash")
	require.NoError(t, err)
	require.NotNil(t, spec)
	assert.Equal(t, "test", spec.Events[0].Name)

	// Should now be in memory cache too.
	assert.NotNil(t, cache.Get("dbhash"))
}
