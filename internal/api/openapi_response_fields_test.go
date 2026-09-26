package api

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// openapi.json is the contract a generated client is built from, and
// pkg/client is generated from it, so a schema that disagrees with the Go
// handler is a bug in the client rather than a bug in the server. response_
// field_set_test.go pins what the handlers put on the wire; this file holds the
// spec to the same field sets, in three directions:
//
//   - A property the spec advertises must be one the Go type really emits. If
//     it is not, the generated client dereferences a field that never arrives.
//   - A property the spec marks required must be one encoding/json can never
//     drop. required is exactly that promise, and the spec cannot make it on a
//     Go field that carries a working omitempty.
//   - A property the Go type always emits should be marked required. The
//     reverse gap — the spec promising less than the server delivers — is not a
//     break for a live client, so it is allowed, but only through the
//     underReportedRequired registry below. A new entry there is a reviewer
//     noticing the spec lag; a new always-present key with no entry is a
//     failure.
//
// The fourth check (TestSchemasAccountForEveryEmittedKey) is the same idea one
// level up: every key a pinned type emits is either documented by its schema or
// named in an exception list. Undocumented keys are how an API quietly grows
// past its own contract, so the lists below double as an inventory of what
// openapi.json still does not describe.

// schemaNode is one components/schemas entry. Only the shape-level keys are
// decoded; property bodies are kept raw because property *names* are the whole
// subject of this file.
type schemaNode struct {
	Ref        string                     `json:"$ref"`
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
	AllOf      []schemaNode               `json:"allOf"`
}

type schemaDocument struct {
	Components struct {
		Schemas map[string]schemaNode `json:"schemas"`
	} `json:"components"`
}

func loadSchemaDocument(t *testing.T) map[string]schemaNode {
	t.Helper()
	var doc schemaDocument
	require.NoError(t, json.Unmarshal(openapiSpec, &doc), "the embedded openapi.json must parse")
	require.NotEmpty(t, doc.Components.Schemas, "openapi.json declares no components/schemas")
	return doc.Components.Schemas
}

// documentedShape merges a schema with everything it allOf's in, so a composed
// schema like EnrichedEvent is compared against the Go type as one field set.
func documentedShape(t *testing.T, schemas map[string]schemaNode, name string) wireShape {
	t.Helper()
	shape := wireShape{canEmit: map[string]bool{}, alwaysPresent: map[string]bool{}}
	const prefix = "#/components/schemas/"

	var visit func(node schemaNode, seen map[string]bool, from string)
	visit = func(node schemaNode, seen map[string]bool, from string) {
		for property := range node.Properties {
			shape.canEmit[property] = true
		}
		for _, required := range node.Required {
			shape.alwaysPresent[required] = true
		}
		for _, member := range node.AllOf {
			if member.Ref == "" {
				// An allOf member written out inline, which is how
				// EnrichedEvent adds decoded_event and decoded.
				visit(member, seen, from)
				continue
			}
			require.Truef(t, strings.HasPrefix(member.Ref, prefix),
				"schema %q has an unresolvable allOf $ref %q", from, member.Ref)
			target := member.Ref[len(prefix):]
			if seen[target] {
				continue // a self- or mutually-referential allOf is a spec bug, not a loop to follow
			}
			node, ok := schemas[target]
			require.Truef(t, ok, "schema %q references %q, which openapi.json does not define", from, target)
			seen[target] = true
			visit(node, seen, target)
		}
	}

	node, ok := schemas[name]
	require.Truef(t, ok, "openapi.json defines no schema %q", name)
	visit(node, map[string]bool{name: true}, name)
	return shape
}

// schemaSurfaces maps every components/schemas name to the Go type that
// produces it. The mapping is hand-maintained because the two vocabularies
// deliberately differ — the spec names things for clients (ContractsPage) and
// the code names them for handlers (contractListResponse) — and an automated
// match on either side would hide a rename rather than catch one.
var schemaSurfaces = map[string]reflect.Type{
	"APIKey":                    reflect.TypeOf(store.APIKey{}),
	"ContractSummary":           reflect.TypeOf(store.ContractSummary{}),
	"ContractsPage":             reflect.TypeOf(contractListResponse{}),
	"CreateSubscriptionRequest": reflect.TypeOf(createSubscriptionRequest{}),
	"DeliveryAttempt":           reflect.TypeOf(store.DeliveryAttempt{}),
	"EnrichedEvent":             reflect.TypeOf(store.EnrichedEvent{}),
	"ErrorResponse":             reflect.TypeOf(errorResponse{}),
	"Event":                     reflect.TypeOf(store.Event{}),
	"EventsResponse":            reflect.TypeOf(eventsResponse{}),
	"HealthResponse":            reflect.TypeOf(healthResponse{}),
	"Stats":                     reflect.TypeOf(store.Stats{}),
	"Subscription":              reflect.TypeOf(store.Subscription{}),
	"SubscriptionFilter":        reflect.TypeOf(store.SubscriptionFilter{}),
	"UpdateSubscriptionRequest": reflect.TypeOf(updateSubscriptionRequest{}),
}

func TestEverySchemaMapsToAGoType(t *testing.T) {
	// A schema with no Go counterpart is documentation for a response nobody
	// writes; a Go type the spec describes under a different name is a rename
	// that just broke someone's client.
	schemas := loadSchemaDocument(t)
	for name := range schemas {
		assert.Containsf(t, schemaSurfaces, name,
			"openapi.json defines schema %q, which this test cannot check against a Go type — add it to schemaSurfaces", name)
	}
	for name := range schemaSurfaces {
		assert.Containsf(t, schemas, name,
			"schemaSurfaces maps %q to a Go type, but openapi.json no longer defines that schema", name)
	}
}

func TestOpenAPISchemasAdvertiseOnlyEmittedKeys(t *testing.T) {
	schemas := loadSchemaDocument(t)
	for _, name := range sortedSchemaNames(schemas) {
		typ, ok := schemaSurfaces[name]
		require.Truef(t, ok, "schema %q has no entry in schemaSurfaces", name)
		goShape := shapeOf(typ)
		specShape := documentedShape(t, schemas, name)
		for _, property := range sortedKeys(specShape.canEmit) {
			assert.Truef(t, goShape.canEmit[property],
				"schema %q documents %q, but %s never emits it — a generated client will read a field that is always absent",
				name, property, typ)
		}
	}
}

func TestOpenAPIRequiredKeysAreAlwaysOnTheWire(t *testing.T) {
	schemas := loadSchemaDocument(t)
	for _, name := range sortedSchemaNames(schemas) {
		typ := schemaSurfaces[name]
		goShape := shapeOf(typ)
		specShape := documentedShape(t, schemas, name)
		for _, property := range sortedKeys(specShape.alwaysPresent) {
			assert.Truef(t, goShape.alwaysPresent[property],
				"schema %q marks %q required, but %s can omit it — clients generated from the spec dereference it unconditionally",
				name, property, typ)
		}
	}
}

// underReportedRequired lists, per schema, the keys the Go type always emits
// while the spec leaves them optional. Each one understates the server rather
// than lying about it, so no live client breaks — but the spec is telling a
// generated client to handle a missing field that never goes missing. Fix by
// adding the property to the schema's required array in api/openapi.yaml and
// running `make spec`, then dropping it from this list.
var underReportedRequired = map[string][]string{
	// "auditor" is the omitempty-on-a-struct case: the tag reads optional and
	// the key is on every response.
	"Stats":                     {"auditor", "contract_count", "last_ingested_ledger", "total_events", "verified_through_ledger", "watched_contracts"},
	"EnrichedEvent":             {"decoded"},
	"CreateSubscriptionRequest": {"filters"},
}

func TestOpenAPIDoesNotUnderstateAlwaysPresentKeys(t *testing.T) {
	schemas := loadSchemaDocument(t)
	for _, name := range sortedSchemaNames(schemas) {
		typ := schemaSurfaces[name]
		goShape := shapeOf(typ)
		specShape := documentedShape(t, schemas, name)
		want := underReportedRequired[name]
		sort.Strings(want)

		var found []string
		for _, property := range sortedKeys(goShape.canEmit) {
			if !specShape.canEmit[property] {
				continue // undocumented; accounted for elsewhere
			}
			if goShape.alwaysPresent[property] && !specShape.alwaysPresent[property] {
				found = append(found, property)
			}
		}
		assert.Equalf(t, want, found,
			"%s: the spec's required list and %s's always-present keys drifted apart", name, typ)
	}
}

// undocumentedKeys lists the keys a pinned Go type emits that its schema says
// nothing about. This is the inventory of what openapi.json still owes, and the
// reason the check below is an equality rather than a subset: a new key on the
// wire has to either be documented or land here, where it is at least named.
var undocumentedKeys = map[string][]string{
	"Event": {"network", "sep41_event"},
	// EnrichedEvent allOf's Event, so it inherits Event's two gaps on top of
	// its own decode_error.
	"EnrichedEvent": {"decode_error", "network", "sep41_event"},
	"Stats": {
		"chain_head_ledger", "decode", "events_ingested_total", "ingest_lag_ledgers",
		"ingester", "last_successful_poll", "oldest_stored_ledger", "panics_recovered",
		"pruner", "query_errors", "rpc_errors", "spec_cache", "table_size_bytes",
	},
	"Subscription":       {"tenant_id"},
	"SubscriptionFilter": {"network", "topic_contains"},
}

func TestSchemasAccountForEveryEmittedKey(t *testing.T) {
	schemas := loadSchemaDocument(t)
	for _, name := range sortedSchemaNames(schemas) {
		typ := schemaSurfaces[name]
		goShape := shapeOf(typ)
		specShape := documentedShape(t, schemas, name)

		want := undocumentedKeys[name]
		sort.Strings(want)

		var found []string
		for _, property := range sortedKeys(goShape.canEmit) {
			if !specShape.canEmit[property] {
				found = append(found, property)
			}
		}
		assert.Equalf(t, want, found,
			"schema %q and %s drifted apart: %s emits keys the schema does not document. "+
				"Document them in api/openapi.yaml and run `make spec`, or add an entry to "+
				"undocumentedKeys if the gap is deliberate.", name, typ, typ)
	}
}

func TestSchemaRequiredArraysAreWellFormed(t *testing.T) {
	// required naming a property the schema does not list is valid JSON Schema
	// and meaningless to a client, so it is worth its own explicit check.
	schemas := loadSchemaDocument(t)
	for _, name := range sortedSchemaNames(schemas) {
		node, ok := schemas[name]
		require.True(t, ok)
		documented := map[string]bool{}
		var collect func(schemaNode)
		collect = func(current schemaNode) {
			for property := range current.Properties {
				documented[property] = true
			}
			for _, member := range current.AllOf {
				if member.Ref != "" {
					const prefix = "#/components/schemas/"
					require.Truef(t, strings.HasPrefix(member.Ref, prefix), "%s: unresolvable $ref %q", name, member.Ref)
					target, ok := schemas[member.Ref[len(prefix):]]
					require.Truef(t, ok, "%s: allOf references %q, which is not defined", name, member.Ref)
					collect(target)
					continue
				}
				collect(member)
			}
		}
		collect(node)
		for _, required := range node.Required {
			assert.Truef(t, documented[required], "schema %q marks %q required without documenting it", name, required)
		}
	}
}

func TestPinnedSurfacesCoverTheResponseTypes(t *testing.T) {
	// The registry in response_field_set_test.go is the file's whole point, so
	// its coverage is worth pinning: every named response type in the package
	// has to appear there. Types are enumerated by hand below because Go offers
	// no reflection over package-level declarations; adding a response type
	// without a pin fails here first, which is the moment to write the pin.
	pinned := map[string]bool{}
	for _, surface := range responseSurfaces {
		pinned[surface.name] = true
	}
	// Every entry here is a type a handler writes with writeJSON. The pairing
	// is checked against the real shape, so a typo is caught by the assertion
	// below rather than passing vacuously.
	handlerResponses := []any{
		errorResponse{}, eventsResponse{}, enrichedEventsResponse{}, eventWithXDR{}, enrichedEventWithXDR{},
		eventsWithXDRResponse{}, enrichedEventsWithXDRResponse{}, addressEventsResponse{}, envelopeResponse{},
		healthResponse{}, versionResponse{}, countResponse{}, bucketResponse{}, rawEventResponse{},
		contractListResponse{}, deadLetterListResponse{}, addWatchedResponse{}, removeWatchedResponse{},
		watchedListResponse{}, contractStatsResponse{}, specOverrideResponse{}, specOverrideDeleteResponse{},
		whoAmIResponse{}, createAPIKeyResponse{},
		store.Event{}, store.EnrichedEvent{}, store.DecodedEventResponse{}, store.AggregateBucket{},
		store.WatchedContract{}, store.ContractSummary{}, store.ContractEventTypeCount{}, store.AddressSummary{},
		store.DeadLetter{}, store.Subscription{}, store.SubscriptionFilter{}, store.DeliveryAttempt{},
		store.APIKey{}, store.Tenant{}, store.TenantAPIKey{}, store.TenantUsage{}, store.Stats{},
		store.DecodeStats{}, store.SpecCacheStats{}, store.PrunerStats{}, store.IngesterStats{},
		store.AuditStats{}, store.RPCErrorStats{},
	}
	var missing []string
	for _, sample := range handlerResponses {
		typ := reflect.TypeOf(sample)
		if !pinned[shortName(typ)] {
			missing = append(missing, shortName(typ))
		}
	}
	assert.Emptyf(t, missing, "response types with no pinned surface: %s", strings.Join(missing, ", "))
}

// shortName is how responseSurfaces labels an api-package type ("errorResponse")
// versus one it borrows from internal/store ("store.Event").
func shortName(typ reflect.Type) string {
	if typ.PkgPath() == "github.com/sorotrail/sorotrail/internal/store" {
		return "store." + typ.Name()
	}
	return typ.Name()
}

func sortedSchemaNames(schemas map[string]schemaNode) []string {
	out := make([]string, 0, len(schemas))
	for name := range schemas {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
