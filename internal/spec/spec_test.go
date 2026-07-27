package spec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// stubSpecStore implements spec.Store for testing.
type stubSpecStore struct {
	specs map[string][]byte
}

func (s *stubSpecStore) GetContractSpec(_ context.Context, wasmHash string) ([]byte, error) {
	if s.specs == nil {
		return nil, fmt.Errorf("not found")
	}
	data, ok := s.specs[wasmHash]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return data, nil
}

func (s *stubSpecStore) SetContractSpec(_ context.Context, wasmHash, contractID string, specJSON []byte) error {
	if s.specs == nil {
		s.specs = make(map[string][]byte)
	}
	s.specs[wasmHash] = specJSON
	return nil
}

func TestScalarValue(t *testing.T) {
	tests := []struct {
		name    string
		input   json.RawMessage
		want    any
		wantErr bool
	}{
		{"symbol", json.RawMessage(`{"symbol":"transfer"}`), "transfer", false},
		{"address", json.RawMessage(`{"address":"GABCD..."}`), "GABCD...", false},
		{"i128", json.RawMessage(`{"i128":"1000"}`), "1000", false},
		{"u64", json.RawMessage(`{"u64":42}`), float64(42), false},
		{"bool true", json.RawMessage(`{"bool":true}`), true, false},
		{"empty", json.RawMessage(`{}`), nil, true},
		{"nested", json.RawMessage(`{"vec":[{"symbol":"a"}]}`), []any{map[string]any{"symbol": "a"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScalarValue(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseTypeFromTag(t *testing.T) {
	assert.Equal(t, "symbol", ParseTypeFromTag(json.RawMessage(`{"symbol":"transfer"}`)))
	assert.Equal(t, "i128", ParseTypeFromTag(json.RawMessage(`{"i128":"1000"}`)))
	assert.Equal(t, "", ParseTypeFromTag(json.RawMessage(`{}`)))
	assert.Equal(t, "", ParseTypeFromTag(json.RawMessage(`invalid`)))
}

func TestEventSpec_ExtractEventName(t *testing.T) {
	tests := []struct {
		name     string
		topics   json.RawMessage
		wantName string
		wantOK   bool
	}{
		{
			"symbol event name",
			json.RawMessage(`[{"symbol":"transfer"},{"address":"G..."}]`),
			"transfer", true,
		},
		{
			"no topics",
			json.RawMessage(`[]`),
			"", false,
		},
		{
			"non-symbol first topic",
			json.RawMessage(`[{"address":"G..."}]`),
			"", false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, ok := extractEventName(tt.topics)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantName, name)
		})
	}
}

func TestFindEventSpec(t *testing.T) {
	specs := []EventSpec{
		{Name: "transfer", Doc: "A token transfer"},
		{Name: "burn", Doc: "Token burn event"},
	}

	found := findEventSpec(specs, "transfer")
	require.NotNil(t, found)
	assert.Equal(t, "transfer", found.Name)

	notFound := findEventSpec(specs, "nonexistent")
	assert.Nil(t, notFound)
}

func TestCache_InMemory(t *testing.T) {
	cache := NewCache(nil)

	// Get should return nil for missing key.
	assert.Nil(t, cache.Get("missing"))

	// Set and get.
	spec := &ContractSpec{
		WasmHash:   "hash123",
		ContractID: "CDLZ...",
		Events: []EventSpec{
			{Name: "transfer", TopicSpecs: []FieldSpec{{Name: "from", Type: "address"}}},
		},
	}
	err := cache.Set(context.Background(), spec)
	require.NoError(t, err)

	cached := cache.Get("hash123")
	require.NotNil(t, cached)
	assert.Equal(t, "hash123", cached.WasmHash)
	assert.Len(t, cached.Events, 1)
	assert.Equal(t, "transfer", cached.Events[0].Name)
}

func TestCache_WithDBBacking(t *testing.T) {
	db := &stubSpecStore{}
	cache := NewCache(db)

	spec := &ContractSpec{
		WasmHash:   "wasm456",
		ContractID: "CDLZ...",
		Events:     []EventSpec{{Name: "burn"}},
	}
	err := cache.Set(context.Background(), spec)
	require.NoError(t, err)

	// Should be in memory.
	assert.NotNil(t, cache.Get("wasm456"))

	// Should be in DB too.
	dbData, err := db.GetContractSpec(context.Background(), "wasm456")
	require.NoError(t, err)
	require.NotNil(t, dbData)

	var loaded ContractSpec
	err = json.Unmarshal(dbData, &loaded)
	require.NoError(t, err)
	assert.Equal(t, "burn", loaded.Events[0].Name)
}

func TestExtractEventName(t *testing.T) {
	tests := []struct {
		name   string
		topics json.RawMessage
		want   string
		wantOK bool
	}{
		{"symbol event", json.RawMessage(`[{"symbol":"transfer"}]`), "transfer", true},
		{"two symbols", json.RawMessage(`[{"symbol":"transfer"},{"symbol":"extra"}]`), "transfer", true},
		{"empty array", json.RawMessage(`[]`), "", false},
		{"non-symbol", json.RawMessage(`[{"u64":42}]`), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractEventName(tt.topics)
			assert.Equal(t, tt.wantOK, ok)
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
