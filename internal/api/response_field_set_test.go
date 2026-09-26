package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// This file is the API's backward-compatibility tripwire. Every JSON key the
// HTTP layer emits is pinned here, so the change a generated client cannot
// absorb — a key dropped, renamed, or newly made absent — fails CI instead of
// shipping.
//
// The pins are deliberately one-directional:
//
//   - A pinned key that disappears from the Go type fails the test. So does a
//     pinned always-present key that gains omitempty, because a client written
//     against the documented shape dereferences it unconditionally.
//   - A key that is new fails nothing. Additive changes are how the API grows,
//     and the issue this coverage answers asks explicitly that adding an
//     optional field not break.
//   - A pinned optional key that becomes always-present fails nothing either:
//     clients may already handle both, and tightening is not a break.
//
// # Documented process for an intentional breaking change
//
// Sometimes a key has to go anyway. Land it as its own PR — never bundled with
// a feature — and make that PR do all five of these, in this order:
//
//  1. Update the pinned entry in responseSurfaces below, and say in the PR
//     description which client call-sites were checked.
//  2. Update api/openapi.yaml (the source of truth) and run `make spec` so the
//     embedded internal/api/openapi.json copy moves with it. openapi_response
//     _fields_test.go then keeps the two in agreement.
//  3. Regenerate the versioned client: `make client`. pkg/client is generated
//     from the YAML, and its drift test fails if it is forgotten.
//  4. Add a line under "### Removed" or "### Changed" in CHANGELOG.md's
//     [Unreleased] section — those sections are how a breaking release is
//     assembled.
//  5. Bump info.version in api/openapi.yaml per RELEASING.md. The API has no
//     /v2 path, so a removed key is a major-version event, not a patch.
//
// Nothing here is enforceable by a test except the fact that the diff touched
// the pins — which is the point: the pin update is the reviewer's signal that
// a contract broke, and it cannot be missed the way a silent struct edit can.

// wireShape is the JSON key model of one response type, derived from its struct
// tags. The model is checked against real encoding/json output in
// TestResponseShapesMatchWhatEncodingJSONWrites, so a mistaken reading of the
// tags — of the kind the omitempty-on-a-struct-field trap is — cannot make a
// pin looser than the wire.
type wireShape struct {
	// canEmit holds every key the type may put on the wire.
	canEmit map[string]bool
	// alwaysPresent holds the subset encoding/json writes whatever the field
	// values are: keys with no omitempty, plus omitempty keys whose type can
	// never read as empty.
	alwaysPresent map[string]bool
}

// wireName resolves one struct field's JSON key. It mirrors encoding/json:
// an anonymous struct field with no name in its tag contributes its own
// promoted fields instead of a key of its own.
func wireName(field reflect.StructField) (name string, omitEmpty, flattened, dropped bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, false, true
	}
	name, opts, _ := strings.Cut(tag, ",")
	omitEmpty = hasTagOption(opts, "omitempty")
	if !isValidJSONTag(name) {
		name = ""
	}
	if name == "" && field.Anonymous {
		if t := field.Type; t.Kind() == reflect.Struct || (t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct) {
			return "", omitEmpty, true, false
		}
	}
	if name == "" {
		// Untagged, non-flattened fields are keyed by their Go name. None of
		// the pinned types rely on this, but the shape model would silently
		// lose the key if one appeared.
		name = field.Name
	}
	return name, omitEmpty, false, false
}

// hasTagOption reports whether the comma-separated tail of a json tag names
// opt. Options are matched one by one, the way encoding/json parses them, so a
// tag like `json:"a,omitempty,string"` reads the same here as on the wire.
func hasTagOption(opts, opt string) bool {
	for opts != "" {
		var head string
		head, opts, _ = strings.Cut(opts, ",")
		if head == opt {
			return true
		}
	}
	return false
}

func isValidJSONTag(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}

// canReadAsEmpty reports whether omitempty may drop a field of this type.
//
// The struct arm is the one worth reading twice: encoding/json's isEmpty only
// ever returns true for a zero-field struct, so `json:"auditor,omitempty"` on a
// value-typed struct is a no-op and the key ships on every response. store.Stats
// has four such fields, which is why the pins below are derived rather than
// hand-assumed.
func canReadAsEmpty(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Slice, reflect.Map, reflect.Pointer, reflect.Interface:
		return true
	case reflect.Struct:
		return typ.NumField() == 0
	default:
		return false
	}
}

// shapeOf walks typ, flattening embedded structs the way encoding/json does.
func shapeOf(typ reflect.Type) wireShape {
	shape := wireShape{canEmit: map[string]bool{}, alwaysPresent: map[string]bool{}}
	shape.walk(typ)
	return shape
}

func (s wireShape) walk(typ reflect.Type) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue // unexported
		}
		name, omitEmpty, flattened, dropped := wireName(field)
		if dropped {
			continue
		}
		if flattened {
			target := field.Type
			if target.Kind() == reflect.Pointer {
				target = target.Elem()
			}
			// An embedded pointer would also make every promoted key
			// conditional, which the shape model does not track. None of the
			// pinned types embed one; a new one would have to opt in
			// deliberately, and TestResponseShapesMatchWhatEncodingJSONWrites
			// is what catches it if that ever changes.
			s.walk(target)
			continue
		}
		s.canEmit[name] = true
		if !omitEmpty || !canReadAsEmpty(field.Type) {
			s.alwaysPresent[name] = true
		}
	}
}

func (s wireShape) keys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		if s.canEmit[k] {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func sortedKeys[V any](set map[string]V) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// surface pins one response type's contract. required keys are on every
// response; optional keys may be absent. Both lists are allowed to be a strict
// subset of what the type emits — see the header for why.
type surface struct {
	name     string
	typ      reflect.Type
	required []string
	optional []string
}

func (p surface) check(got wireShape) []string {
	var breaks []string
	for _, key := range p.required {
		switch {
		case !got.canEmit[key]:
			breaks = append(breaks, fmt.Sprintf("%s: required key %q is no longer emitted", p.name, key))
		case !got.alwaysPresent[key]:
			breaks = append(breaks, fmt.Sprintf("%s: required key %q can now be absent", p.name, key))
		}
	}
	for _, key := range p.optional {
		if !got.canEmit[key] {
			breaks = append(breaks, fmt.Sprintf("%s: documented key %q is no longer emitted", p.name, key))
		}
	}
	return breaks
}

// responseSurfaces is the whole pinned contract. The list is of payload types,
// not of endpoints: a response type reached from two handlers is one contract,
// and nested types (the store.* rows and the /stats sections) are pinned in
// their own right because clients bind to them, not to the wrapper.
var responseSurfaces = []surface{
	{"errorResponse", reflect.TypeOf(errorResponse{}),
		[]string{"error"}, nil},
	{"eventsResponse", reflect.TypeOf(eventsResponse{}),
		[]string{"events"}, []string{"cursor"}},
	{"enrichedEventsResponse", reflect.TypeOf(enrichedEventsResponse{}),
		[]string{"events"}, []string{"cursor"}},
	{"eventsWithXDRResponse", reflect.TypeOf(eventsWithXDRResponse{}),
		[]string{"events"}, []string{"cursor"}},
	{"enrichedEventsWithXDRResponse", reflect.TypeOf(enrichedEventsWithXDRResponse{}),
		[]string{"events"}, []string{"cursor"}},
	{"addressEventsResponse", reflect.TypeOf(addressEventsResponse{}),
		[]string{"events"}, []string{"cursor"}},
	{"eventWithXDR", reflect.TypeOf(eventWithXDR{}),
		[]string{"topics_xdr"}, []string{"value_xdr"}},
	{"enrichedEventWithXDR", reflect.TypeOf(enrichedEventWithXDR{}),
		[]string{"topics_xdr", "decoded"}, []string{"value_xdr", "decoded_event"}},
	{"envelopeResponse", reflect.TypeOf(envelopeResponse{}),
		// data is documented as "array, never null" and wrapEnvelope
		// normalises a nil slice to satisfy it, so the key is on the wire
		// either way.
		[]string{"data"}, []string{"next_cursor"}},
	{"healthResponse", reflect.TypeOf(healthResponse{}),
		[]string{"status", "checks"}, nil},
	{"versionResponse", reflect.TypeOf(versionResponse{}),
		[]string{"version", "commit", "build_date"}, nil},
	{"countResponse", reflect.TypeOf(countResponse{}),
		[]string{"count"}, nil},
	{"bucketResponse", reflect.TypeOf(bucketResponse{}),
		[]string{"buckets"}, nil},
	{"rawEventResponse", reflect.TypeOf(rawEventResponse{}),
		[]string{"topics_xdr"}, []string{"value_xdr"}},
	{"contractListResponse", reflect.TypeOf(contractListResponse{}),
		[]string{"contracts", "count"}, []string{"cursor"}},
	{"deadLetterListResponse", reflect.TypeOf(deadLetterListResponse{}),
		[]string{"dead_letters", "count"}, []string{"cursor"}},
	{"addWatchedResponse", reflect.TypeOf(addWatchedResponse{}),
		[]string{"contract_id", "added_at", "history_from_ledger"}, []string{"mode_transition"}},
	{"removeWatchedResponse", reflect.TypeOf(removeWatchedResponse{}),
		[]string{"contract_id", "removed_at", "history_preserved"}, []string{"mode_transition"}},
	{"watchedListResponse", reflect.TypeOf(watchedListResponse{}),
		[]string{"contracts", "count"}, nil},
	{"contractStatsResponse", reflect.TypeOf(contractStatsResponse{}),
		[]string{"contract_id", "event_count"},
		[]string{"name", "symbol", "decimals", "type_breakdown"}},
	{"specOverrideResponse", reflect.TypeOf(specOverrideResponse{}),
		[]string{"contract_id", "spec"}, []string{"updated_at"}},
	{"specOverrideDeleteResponse", reflect.TypeOf(specOverrideDeleteResponse{}),
		[]string{"contract_id", "deleted"}, nil},
	{"whoAmIResponse", reflect.TypeOf(whoAmIResponse{}),
		[]string{"tenant", "wildcard"}, []string{"granted_contract_ids"}},
	{"createAPIKeyResponse", reflect.TypeOf(createAPIKeyResponse{}),
		[]string{"id", "name", "prefix", "created_at", "key"}, []string{"revoked_at"}},

	// --- payloads the handlers pass through from internal/store ---
	{"store.Event", reflect.TypeOf(store.Event{}),
		[]string{"id", "contract_id", "ledger", "type", "tx_hash", "tx_index", "op_index",
			"in_successful_call", "topics", "value", "created_at", "network"},
		[]string{"sep41_event"}},
	{"store.EnrichedEvent", reflect.TypeOf(store.EnrichedEvent{}),
		[]string{"id", "contract_id", "ledger", "type", "tx_hash", "tx_index", "op_index",
			"in_successful_call", "topics", "value", "created_at", "network", "decoded"},
		[]string{"sep41_event", "decoded_event", "decode_error"}},
	{"store.DecodedEventResponse", reflect.TypeOf(store.DecodedEventResponse{}),
		[]string{"event"}, []string{"fields"}},
	{"store.AggregateBucket", reflect.TypeOf(store.AggregateBucket{}),
		[]string{"bucket", "count"}, nil},
	{"store.WatchedContract", reflect.TypeOf(store.WatchedContract{}),
		[]string{"contract_id", "added_at"}, nil},
	{"store.ContractSummary", reflect.TypeOf(store.ContractSummary{}),
		[]string{"contract_id", "event_count", "first_ledger", "last_ledger", "last_seen"}, nil},
	{"store.ContractEventTypeCount", reflect.TypeOf(store.ContractEventTypeCount{}),
		[]string{"type", "count"}, nil},
	{"store.AddressSummary", reflect.TypeOf(store.AddressSummary{}),
		[]string{"address", "first_seen_ledger", "last_seen_ledger", "event_count", "distinct_contracts"}, nil},
	{"store.DeadLetter", reflect.TypeOf(store.DeadLetter{}),
		[]string{"id", "event_id", "contract_id", "ledger", "type", "tx_hash", "error",
			"attempts", "last_attempt", "created_at"},
		[]string{"topic_xdr", "value_xdr"}},
	{"store.Subscription", reflect.TypeOf(store.Subscription{}),
		[]string{"id", "url", "filters", "secret", "enabled", "failure_count", "created_at"},
		[]string{"tenant_id"}},
	{"store.SubscriptionFilter", reflect.TypeOf(store.SubscriptionFilter{}),
		nil,
		[]string{"contract_id", "type", "topic", "topic_contains", "from_ledger", "to_ledger", "network"}},
	{"store.DeliveryAttempt", reflect.TypeOf(store.DeliveryAttempt{}),
		[]string{"id", "subscription_id", "event_id", "status", "response_code", "duration_ms", "created_at"},
		[]string{"error"}},
	{"store.APIKey", reflect.TypeOf(store.APIKey{}),
		[]string{"id", "name", "prefix", "created_at"}, []string{"revoked_at"}},
	{"store.Tenant", reflect.TypeOf(store.Tenant{}),
		[]string{"id", "name", "wildcard", "admin", "enabled", "created_at"},
		[]string{"rate_limit_rps", "rate_limit_burst", "max_watched_contracts"}},
	{"store.TenantAPIKey", reflect.TypeOf(store.TenantAPIKey{}),
		[]string{"id", "tenant_id", "name", "prefix", "created_at"},
		[]string{"last_used_at", "revoked_at", "secret"}},
	{"store.TenantUsage", reflect.TypeOf(store.TenantUsage{}),
		[]string{"tenant_id", "day", "requests", "events_served", "stream_seconds"}, nil},
	{"store.Stats", reflect.TypeOf(store.Stats{}),
		// rpc_errors, auditor, spec_cache, pruner and ingester are tagged
		// omitempty on value-typed structs, which encoding/json ignores: all
		// five are on every /stats response, as an empty object when the
		// subsystem is not wired. Pinning them required is what makes that
		// visible rather than a surprise at integration time.
		[]string{"total_events", "last_ingested_ledger", "verified_through_ledger", "oldest_stored_ledger",
			"chain_head_ledger", "ingest_lag_ledgers", "contract_count", "watched_contracts",
			"table_size_bytes", "query_errors", "events_ingested_total", "panics_recovered",
			"rpc_errors", "auditor", "spec_cache", "pruner", "ingester"},
		[]string{"last_successful_poll", "decode"}},
	{"store.DecodeStats", reflect.TypeOf(store.DecodeStats{}),
		[]string{"decodes", "decode_failures"}, nil},
	{"store.SpecCacheStats", reflect.TypeOf(store.SpecCacheStats{}),
		[]string{"cached_specs", "hits", "misses", "fetches", "expiries", "invalidations"}, nil},
	{"store.PrunerStats", reflect.TypeOf(store.PrunerStats{}),
		[]string{"runs_completed", "total_rows_purged"}, nil},
	{"store.IngesterStats", reflect.TypeOf(store.IngesterStats{}),
		[]string{"effective_poll_interval_ms"}, nil},
	{"store.AuditStats", reflect.TypeOf(store.AuditStats{}),
		[]string{"passes_run", "ledgers_checked", "findings_opened", "findings_repaired",
			"findings_unverifiable", "findings_unrecoverable", "rpc_requests"}, nil},
	{"store.RPCErrorStats", reflect.TypeOf(store.RPCErrorStats{}),
		nil,
		[]string{"getEvents", "getLatestLedger", "getHealth", "getLedgerEntries"}},
}

func TestResponseSurfacesStayPinned(t *testing.T) {
	seen := map[string]bool{}
	for _, pinned := range responseSurfaces {
		require.Falsef(t, seen[pinned.name], "duplicate pin for %s", pinned.name)
		seen[pinned.name] = true

		pinned := pinned
		t.Run(pinned.name, func(t *testing.T) {
			require.Equal(t, reflect.Struct, pinned.typ.Kind(), "a surface must pin a struct type")
			// A duplicate key would hide a rename: the pin is satisfied by
			// the first holder of the name, so the second spelling never
			// gets checked.
			names := append(append([]string{}, pinned.required...), pinned.optional...)
			sort.Strings(names)
			for i := 1; i < len(names); i++ {
				assert.NotEqualf(t, names[i-1], names[i], "%s pins %q twice", pinned.name, names[i])
			}
			breaks := pinned.check(shapeOf(pinned.typ))
			assert.Emptyf(t, breaks,
				"the API contract for %s broke:\n  %s\n"+
					"If this is intentional, follow the process documented at the top of this file.",
				pinned.name, strings.Join(breaks, "\n  "))
		})
	}
}

func TestResponseShapesMatchWhatEncodingJSONWrites(t *testing.T) {
	for _, pinned := range responseSurfaces {
		pinned := pinned
		t.Run(pinned.name, func(t *testing.T) {
			shape := shapeOf(pinned.typ)

			// The two models have to agree on the always-present keys: a
			// hand-read tag set that is wrong in the *loose* direction would
			// make every pin below toothless.
			assert.Equal(t, shape.keys(shape.alwaysPresent), wireKeys(t, reflect.New(pinned.typ).Interface()),
				"%s: the derived always-present key set disagrees with encoding/json on a zero value", pinned.name)

			populated := reflect.New(pinned.typ).Elem()
			fill(populated)
			assert.Equal(t, sortedKeys(shape.canEmit), wireKeys(t, populated.Interface()),
				"%s: the derived key set disagrees with encoding/json on a fully populated value", pinned.name)
		})
	}
}

// wireKeys returns the top-level JSON keys of v as encoding/json actually
// writes them.
func wireKeys(t *testing.T, v any) []string {
	t.Helper()
	encoded, err := json.Marshal(v)
	require.NoError(t, err)
	var asObject map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &asObject), "response payloads are JSON objects")
	return sortedKeys(asObject)
}

// fill makes every field of v non-zero, so a marshalled value exercises every
// key the type can emit — including the omitempty ones that a zero value
// hides. Interfaces are left nil: the only pinned payload with one is
// envelopeResponse.data, whose key is present regardless of value.
func fill(v reflect.Value) {
	if v.Type() == reflect.TypeOf(time.Time{}) {
		v.Set(reflect.ValueOf(time.Unix(1_700_000_000, 0).UTC()))
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			// A lowercase embedded type is itself unexported but still
			// flattens into the parent, so the rule here has to match
			// shapeOf's: skip unexported named fields only.
			if field.PkgPath != "" && !field.Anonymous {
				continue
			}
			fill(v.Field(i))
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			// json.RawMessage and friends: write valid JSON, since the
			// encoder passes the bytes straight through.
			v.SetBytes([]byte("{}"))
			return
		}
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fill(v.Index(0))
	case reflect.Map:
		m := reflect.MakeMapWithSize(v.Type(), 1)
		key, val := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		fill(key)
		fill(val)
		if k, ok := key.Interface().(string); ok && k == "" {
			key.SetString("key")
		}
		m.SetMapIndex(key, val)
		v.Set(m)
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	}
}

func TestStructOmitemptyDoesNotMakeAFieldOptional(t *testing.T) {
	// The trap this pins: `json:"…,omitempty"` reads like "omit when there is
	// nothing to report", and on a value-typed struct it never does. A client
	// that trusts the tag instead of the wire sees an empty object where it
	// expected the key to be gone.
	stats := store.Stats{}
	encoded, err := json.Marshal(stats)
	require.NoError(t, err)
	var asObject map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &asObject))

	// The five value-typed sections are objects on every response, and their
	// own tags decide what an unwired one looks like: RPCErrorStats tags every
	// counter omitempty so it renders as {}, while the others carry untagged
	// counters and render as a full set of zeroes. A client has to code
	// against whichever shape it gets, so both are pinned here.
	cases := []struct {
		key   string
		value string
	}{
		{"rpc_errors", `{}`},
		{"auditor", `{"passes_run":0,"ledgers_checked":0,"findings_opened":0,"findings_repaired":0,` +
			`"findings_unverifiable":0,"findings_unrecoverable":0,"rpc_requests":0}`},
		{"spec_cache", `{"cached_specs":0,"hits":0,"misses":0,"fetches":0,"expiries":0,"invalidations":0}`},
		{"pruner", `{"runs_completed":0,"total_rows_purged":0}`},
		{"ingester", `{"effective_poll_interval_ms":0}`},
	}
	for _, tc := range cases {
		value, ok := asObject[tc.key]
		require.Truef(t, ok, "/stats must send %q even with every counter at zero — the tag says omitempty, encoding/json disagrees", tc.key)
		assert.JSONEq(t, tc.value, string(value), "%s is a JSON object when its subsystem is unwired", tc.key)
	}
	// The pointer-typed siblings genuinely do disappear, which is the
	// distinction the pins encode.
	for _, key := range []string{"last_successful_poll", "decode"} {
		assert.NotContains(t, asObject, key, "%s is a pointer, so omitempty really drops it", key)
	}
}

func TestEventProjectionOnlyReachesPinnedKeys(t *testing.T) {
	// ?fields= is an allowlist, so its real failure mode is a name that maps
	// to something other than the documented key — raw_topic_xdr in
	// particular, which store.Event deliberately keeps off the wire.
	event := shapeOf(reflect.TypeOf(store.Event{}))
	for key := range eventFieldNames {
		assert.Truef(t, event.canEmit[key], "?fields=%s is accepted but store.Event never emits %q", key, key)
	}

	projected := projectEvents([]store.Event{{ID: "e1"}}, map[string]bool{
		"id": true, "topics": true, "value": true,
	})
	rows, ok := projected.([]map[string]any)
	require.True(t, ok)
	assert.Equal(t, []string{"id", "topics", "value"}, sortedKeys(rows[0]))

	// Every key the projection cannot name is either raw XDR (never on the
	// wire at all) or a deliberate exception; this assertion is the tripwire
	// for a new store.Event field nobody thought about.
	unprojectable := []string{}
	for _, key := range sortedKeys(event.alwaysPresent) {
		if !eventFieldNames[key] {
			unprojectable = append(unprojectable, key)
		}
	}
	assert.Equal(t, []string{"network"}, unprojectable,
		"store.Event grew or lost an always-present key that ?fields= does not cover")
}

// --- the checks themselves, exercised on synthetic types ---
//
// The pins above only matter if surface.check rejects the changes it claims
// to. These types are the specification for that claim: they replay each
// break class against one pinned shape, so a loosening of check() fails here
// rather than quietly turning the whole file into documentation.

type (
	pinShape struct {
		KeyA string `json:"a"`
		KeyB string `json:"b,omitempty"`
	}
	pinDroppedKey struct {
		KeyA string `json:"a"`
	}
	// pinDroppedRequired keeps only the optional key.
	pinDroppedRequired struct {
		KeyB string `json:"b,omitempty"`
	}
	pinRenamedKey struct {
		KeyA string `json:"a"`
		KeyB string `json:"bee,omitempty"`
		Nope string `json:"-"`
	}
	pinAddedOptional struct {
		KeyA string `json:"a"`
		KeyB string `json:"b,omitempty"`
		KeyC int    `json:"c,omitempty"`
	}
	pinRequiredLoosened struct {
		KeyA string `json:"a,omitempty"`
		KeyB string `json:"b,omitempty"`
	}
	pinOptionalTightened struct {
		KeyA string `json:"a"`
		KeyB string `json:"b"`
	}
)

func TestSurfaceCheckRejectsBreaksAndAcceptsAdditions(t *testing.T) {
	pinned := surface{
		name:     "pinShape",
		typ:      reflect.TypeOf(pinShape{}),
		required: []string{"a"},
		optional: []string{"b"},
	}

	t.Run("the reference shape passes", func(t *testing.T) {
		assert.Empty(t, pinned.check(shapeOf(reflect.TypeOf(pinShape{}))))
	})

	tests := []struct {
		name      string
		typ       reflect.Type
		wantBreak string
	}{
		{
			name:      "a dropped field fails",
			typ:       reflect.TypeOf(pinDroppedKey{}),
			wantBreak: `pinShape: documented key "b" is no longer emitted`,
		},
		{
			name:      "a renamed field fails",
			typ:       reflect.TypeOf(pinRenamedKey{}),
			wantBreak: `pinShape: documented key "b" is no longer emitted`,
		},
		{
			name:      "an always-present field made conditional fails",
			typ:       reflect.TypeOf(pinRequiredLoosened{}),
			wantBreak: `pinShape: required key "a" can now be absent`,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			breaks := pinned.check(shapeOf(tc.typ))
			require.Len(t, breaks, 1, "exactly one break should be reported, got %v", breaks)
			assert.Equal(t, tc.wantBreak, breaks[0])
		})
	}

	t.Run("a dropped required field fails", func(t *testing.T) {
		breaks := pinned.check(shapeOf(reflect.TypeOf(pinDroppedRequired{})))
		require.Len(t, breaks, 1)
		assert.Equal(t, `pinShape: required key "a" is no longer emitted`, breaks[0])
	})

	t.Run("adding an optional field passes", func(t *testing.T) {
		// The requirement is explicit in the issue: additive changes are how
		// the API grows and must not redden CI.
		assert.Empty(t, pinned.check(shapeOf(reflect.TypeOf(pinAddedOptional{}))))
	})

	t.Run("an optional field becoming always-present passes", func(t *testing.T) {
		assert.Empty(t, pinned.check(shapeOf(reflect.TypeOf(pinOptionalTightened{}))))
	})
}

func TestSurfaceCheckSpotsEveryBreakTogether(t *testing.T) {
	// One type with two independent problems: check() has to report both, or a
	// reviewer fixing the first would still ship the second.
	type broken struct {
		KeyC string `json:"c"`
	}
	breaks := surface{
		name:     "broken",
		typ:      reflect.TypeOf(broken{}),
		required: []string{"a"},
		optional: []string{"b"},
	}.check(shapeOf(reflect.TypeOf(broken{})))
	require.Len(t, breaks, 2)
	assert.Equal(t, []string{
		`broken: required key "a" is no longer emitted`,
		`broken: documented key "b" is no longer emitted`,
	}, breaks)
}
