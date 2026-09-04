package spec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/sorotrail/sorotrail/internal/store"
)

// Enricher maps raw shape-tagged events onto named-field representations
// using cached contract specs.
//
// Decode failures are surfaced rather than dropped: failed decodes set
// DecodeError on the returned EnrichedEvent (with the raw Event preserved
// alongside) and are counted in DecodeStats so the failure rate is
// observable via /stats.
type Enricher struct {
	fetcher *Fetcher
	cache   *Cache
	log     *slog.Logger

	decodes        atomic.Uint64
	decodeFailures atomic.Uint64
	fetcher       *Fetcher
	cache         *Cache
	log           *slog.Logger
	overrideStore SpecOverrideStore
}

// SpecOverrideStore is the subset of store.Store needed to read
// user-supplied spec overrides. Defined here (rather than reusing the
// cache's Store interface) so the enricher never depends on the full store.
type SpecOverrideStore interface {
	GetContractSpecOverride(ctx context.Context, contractID string) ([]byte, error)
}

// NewEnricher creates an enricher. The fetcher is used to lazily fetch
// specs on first encounter with a contract; nil means no fetching is done
// and enrichment only uses cached specs. overrideStore is optional (may be
// nil) and supplies user-uploaded spec overrides that take precedence over
// fetched specs.
func NewEnricher(fetcher *Fetcher, cache *Cache, log *slog.Logger, overrideStore ...SpecOverrideStore) *Enricher {
	e := &Enricher{
		fetcher: fetcher,
		cache:   cache,
		log:     log,
	}
	if len(overrideStore) > 0 {
		e.overrideStore = overrideStore[0]
	}
	return e
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
// DecodeStats returns the enricher's accumulated decode counters, so the
// decode failure rate (Failures/Decodes) is observable to consumers.
func (e *Enricher) DecodeStats() store.DecodeStats {
	if e == nil {
		return store.DecodeStats{}
	}
	return store.DecodeStats{
		Decodes:        e.decodes.Load(),
		DecodeFailures: e.decodeFailures.Load(),
	}
}

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
	e.decodes.Add(1)

	// Get the event name from topic[0]; it must be a symbol. A payload that
	// cannot yield an event name is a decode failure, not merely "without a
	// spec", so it is surfaced via decode_error and counted.
	eventName, ok := extractEventName(ev.Topics)
	if !ok {
		base.Decoded = false
		base.DecodeError = "cannot extract event name from topics (expected a symbol at topic[0])"
		e.decodeFailures.Add(1)
		return base
	}

	// Get the spec for this contract. A missing spec means the event is
	// simply not enrichable; it is not a decode error.
	spec := e.getSpec(ctx, ev.ContractID)
	if spec == nil {
		base.Decoded = false
		return base
	}

	// Find the matching event definition in the spec. An unknown event name
	// is not a decode error either.
	eventSpec := findEventSpec(spec.Events, eventName)
	if eventSpec == nil {
		base.Decoded = false
		return base
	}

	// Decode topics and value into named fields; a failure here is a real
	// decode error and is surfaced (with the raw event) plus counted.
	fields, err := e.decodeFields(ev.Topics, ev.Value, eventSpec)
	if err != nil {
		base.Decoded = false
		base.DecodeError = err.Error()
		e.decodeFailures.Add(1)
		return base
	}

	return store.EnrichedEvent{
		Event:        ev,
		Decoded:      true,
		DecodedEvent: &store.DecodedEventResponse{Event: eventName, Fields: fields},
	}
}

// getSpec returns the spec for a contract, trying cache first and
// falling back to fetching if a fetcher is configured. Cache lookups go
// through GetForContract, which re-verifies the contract's wasm hash so
// a contract upgrade invalidates the previously cached spec.
//
// User-supplied override: when the store holds a spec override for this
// contract (uploaded via the API), it is parsed, cached under the
// "override:<contractID>" key and returned INSTEAD of the fetched spec.
// This serves contracts that expose no fetchable spec at all, and lets
// operators correct a mis-decoded spec without redeploying the contract.
func (e *Enricher) getSpec(ctx context.Context, contractID string) *ContractSpec {
	// 1. User-supplied override always wins (checked first, cheap store read
	// with an in-memory parsed cache in front).
	if s := e.getOverride(ctx, contractID); s != nil {
		return s
	}

	// 2. Try the cache; this also re-resolves the wasm hash when the
	// mapping is due for re-verification and invalidates stale specs.
	if s := e.cache.GetForContract(ctx, contractID); s != nil {
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

// overrideKey is the cache namespace for parsed user-supplied overrides.
// They live in the same in-memory map but under a key that can never
// collide with a real wasm hash (hex/base64 alphabet has no ':').
const overrideKeyPrefix = "override:"

// getOverride loads the user-supplied spec override for a contract.
// Returns nil when no override exists (or the stored JSON is invalid —
// invalid specs are rejected at upload time, so this only guards against
// hand-edited rows).
func (e *Enricher) getOverride(ctx context.Context, contractID string) *ContractSpec {
	key := overrideKeyPrefix + contractID

	// Fast path: parsed override already in memory (never expires — an
	// override is authoritative until explicitly replaced or deleted).
	if s := e.cache.Get(key); s != nil {
		return s
	}

	// Slow path: read from the store, validate, cache.
	if e.overrideStore == nil {
		return nil
	}
	data, err := e.overrideStore.GetContractSpecOverride(ctx, contractID)
	if err != nil || len(data) == 0 {
		return nil
	}
	spec, err := ParseOverrideSpec(data)
	if err != nil {
		e.log.Warn("ignoring invalid spec override",
			"contract_id", contractID,
			"error", err,
		)
		return nil
	}
	spec.ContractID = contractID
	e.cache.Put(key, spec)
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

// decodeFields maps raw topics and value to named fields based on the event
// spec. It returns the mapped fields (best-effort, populated for every
// position it could decode) plus, when any spec-declared field could not be
// decoded, an error naming the first failing field so the caller can surface
// and count it.
func (e *Enricher) decodeFields(topics, value json.RawMessage, eventSpec *EventSpec) (map[string]any, error) {
	fields := make(map[string]any)

	var topicArr []json.RawMessage
	if err := json.Unmarshal(topics, &topicArr); err != nil {
		return nil, fmt.Errorf("unmarshaling topics: %w", err)
	}

	// topic[0] is the event name — already known.
	// Map topic[1..n] to the spec's TopicSpecs.
	for i := 1; i < len(topicArr) && i-1 < len(eventSpec.TopicSpecs); i++ {
		fieldSpec := eventSpec.TopicSpecs[i-1]
		if val, err := ScalarValue(topicArr[i]); err == nil {
			fields[fieldSpec.Name] = formatScalarValue(val)
		} else {
			return fields, fmt.Errorf("decoding topic field %q (position %d): %w", fieldSpec.Name, i, err)
		}
	}

	// Map the value field.
	if eventSpec.ValueSpec != nil && value != nil && string(value) != "null" {
		if val, err := ScalarValue(value); err == nil {
			fields[eventSpec.ValueSpec.Name] = formatScalarValue(val)
		} else {
			return fields, fmt.Errorf("decoding value field %q: %w", eventSpec.ValueSpec.Name, err)
		}
	}

	return fields, nil
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
