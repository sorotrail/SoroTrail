package spec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Enricher maps raw shape-tagged events onto named-field representations
// using cached contract specs.
type Enricher struct {
	fetcher *Fetcher
	cache   *Cache
	log     *slog.Logger
}

// NewEnricher creates an enricher. The fetcher is used to lazily fetch
// specs on first encounter with a contract; nil means no fetching is done
// and enrichment only uses cached specs.
func NewEnricher(fetcher *Fetcher, cache *Cache, log *slog.Logger) *Enricher {
	return &Enricher{
		fetcher: fetcher,
		cache:   cache,
		log:     log,
	}
}

// EnrichEvents enriches a slice of events with decoded fields from the
// contract spec. Events without a matching spec entry are flagged with
// decoded=false. The original event data is always preserved alongside
// the decoded representation.
//
// The enrichment works by:
//  1. Looking up the contract's spec from cache (lazy-fetching if needed)
//  2. Matching topic[0] (the event name symbol) to an event spec entry
//  3. Mapping the positional topics[1..n] and value to the spec's named fields
//
// Each returned EnrichedEvent wraps the base Event with decoded information.
func (e *Enricher) EnrichEvents(ctx context.Context, events []store.Event) []store.EnrichedEvent {
	if e == nil || e.cache == nil {
		// No enricher configured — return raw events with decoded=false.
		out := make([]store.EnrichedEvent, len(events))
		for i, ev := range events {
			out[i] = store.EnrichedEvent{Event: ev, Decoded: false}
		}
		return out
	}

	out := make([]store.EnrichedEvent, len(events))
	for i, ev := range events {
		out[i] = e.enrichOne(ctx, ev)
	}
	return out
}

// enrichOne enriches a single event.
func (e *Enricher) enrichOne(ctx context.Context, ev store.Event) store.EnrichedEvent {
	base := store.EnrichedEvent{Event: ev}

	// Get the event name from topic[0]; it must be a symbol.
	eventName, ok := extractEventName(ev.Topics)
	if !ok {
		base.Decoded = false
		return base
	}

	// Get the spec for this contract.
	spec := e.getSpec(ctx, ev.ContractID)
	if spec == nil {
		base.Decoded = false
		return base
	}

	// Find the matching event definition in the spec.
	eventSpec := findEventSpec(spec.Events, eventName)
	if eventSpec == nil {
		base.Decoded = false
		return base
	}

	// Decode topics and value into named fields.
	fields := e.decodeFields(ev.Topics, ev.Value, eventSpec)

	return store.EnrichedEvent{
		Event:        ev,
		Decoded:      true,
		DecodedEvent: &store.DecodedEventResponse{Event: eventName, Fields: fields},
	}
}

// getSpec returns the spec for a contract, trying cache first and
// falling back to fetching if a fetcher is configured.
func (e *Enricher) getSpec(ctx context.Context, contractID string) *ContractSpec {
	// Try the cache first, by contract ID (specs are keyed by wasm hash,
	// so this is a reverse lookup).
	if s := e.cache.GetByContractID(contractID); s != nil {
		return s
	}

	// Not cached — fetch it if a fetcher is configured.
	if e.fetcher == nil {
		return nil
	}

	// Attempt to fetch the spec (which will cache it).
	spec, err := e.fetcher.FetchSpec(ctx, contractID)
	if err != nil {
		e.log.Warn("failed to fetch spec for contract",
			"contract_id", contractID,
			"error", err,
		)
		return nil
	}

	if spec == nil || len(spec.Events) == 0 {
		return nil
	}

	// Cache the spec for future lookups.
	if err := e.cache.Set(ctx, spec); err != nil {
		e.log.Warn("failed to cache spec", "contract_id", contractID, "error", err)
	}

	return spec
}

// extractEventName extracts the event name from topic[0].
// Topic[0] must be a tagged JSON value with a "symbol" key,
// e.g. {"symbol":"transfer"} → "transfer".
func extractEventName(topics json.RawMessage) (string, bool) {
	var topicArr []json.RawMessage
	if err := json.Unmarshal(topics, &topicArr); err != nil || len(topicArr) == 0 {
		return "", false
	}

	// topic[0] must be a symbol-tagged value: {"symbol":"..."}. Any other
	// scalar (e.g. an address) is not a valid event name.
	var tagged struct {
		Symbol *string `json:"symbol"`
	}
	if err := json.Unmarshal(topicArr[0], &tagged); err != nil || tagged.Symbol == nil {
		return "", false
	}
	return *tagged.Symbol, true
}

// findEventSpec finds the EventSpec matching the given event name.
func findEventSpec(specs []EventSpec, name string) *EventSpec {
	for i := range specs {
		if specs[i].Name == name {
			return &specs[i]
		}
	}
	return nil
}

// decodeFields maps raw topics and value to named fields based on the
// event spec.
func (e *Enricher) decodeFields(topics, value json.RawMessage, eventSpec *EventSpec) map[string]any {
	fields := make(map[string]any)

	var topicArr []json.RawMessage
	if err := json.Unmarshal(topics, &topicArr); err != nil {
		return fields
	}

	// topic[0] is the event name — already known.
	// Map topic[1..n] to the spec's TopicSpecs.
	for i := 1; i < len(topicArr) && i-1 < len(eventSpec.TopicSpecs); i++ {
		fieldSpec := eventSpec.TopicSpecs[i-1]
		if val, err := ScalarValue(topicArr[i]); err == nil {
			fields[fieldSpec.Name] = formatScalarValue(val)
		} else {
			fields[fieldSpec.Name] = string(topicArr[i])
		}
	}

	// Map the value field.
	if eventSpec.ValueSpec != nil && value != nil && string(value) != "null" {
		if val, err := ScalarValue(value); err == nil {
			fields[eventSpec.ValueSpec.Name] = formatScalarValue(val)
		} else {
			fields[eventSpec.ValueSpec.Name] = string(value)
		}
	}

	return fields
}

// formatScalarValue converts a decoded scalar to its string representation.
func formatScalarValue(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return fmt.Sprintf("%.0f", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}
