package docs_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const coverageSpecPath = "../../api/openapi.yaml"

// coverageOperation is the small part of an OpenAPI operation needed by the
// documentation checks below. Keeping this test independent of the generated
// JSON means it tests the authored source that contributors edit.
type coverageOperation struct {
	OperationID string                      `yaml:"operationId"`
	Summary     string                      `yaml:"summary"`
	Description string                      `yaml:"description"`
	Parameters  []coverageParameter         `yaml:"parameters"`
	RequestBody *coverageRequestBody        `yaml:"requestBody"`
	Responses   map[string]coverageResponse `yaml:"responses"`
}

type coverageParameter struct {
	Ref         string         `yaml:"$ref"`
	Name        string         `yaml:"name"`
	In          string         `yaml:"in"`
	Required    bool           `yaml:"required"`
	Description string         `yaml:"description"`
	Schema      map[string]any `yaml:"schema"`
}

type coverageRequestBody struct {
	Required bool                     `yaml:"required"`
	Content  map[string]coverageMedia `yaml:"content"`
}

type coverageMedia struct {
	Schema map[string]any `yaml:"schema"`
}

type coverageResponse struct {
	Ref         string                   `yaml:"$ref"`
	Description string                   `yaml:"description"`
	Headers     map[string]any           `yaml:"headers"`
	Content     map[string]coverageMedia `yaml:"content"`
}

type coverageDocument struct {
	OpenAPI    string                                  `yaml:"openapi"`
	Paths      map[string]map[string]coverageOperation `yaml:"paths"`
	Components struct {
		Parameters map[string]coverageParameter `yaml:"parameters"`
		Schemas    map[string]map[string]any    `yaml:"schemas"`
		Responses  map[string]coverageResponse  `yaml:"responses"`
	} `yaml:"components"`
}

func readCoverageSpec(t *testing.T) coverageDocument {
	t.Helper()
	data, err := os.ReadFile(coverageSpecPath)
	require.NoError(t, err)

	var doc coverageDocument
	require.NoError(t, yaml.Unmarshal(data, &doc))
	require.True(t, strings.HasPrefix(doc.OpenAPI, "3.1"), "spec must declare OpenAPI 3.1.x")
	require.NotEmpty(t, doc.Paths, "spec must contain paths")
	return doc
}

func (d coverageDocument) operation(t *testing.T, method, path string) coverageOperation {
	t.Helper()
	pathItem, ok := d.Paths[path]
	require.Truef(t, ok, "spec has no path %q", path)
	op, ok := pathItem[strings.ToLower(method)]
	require.Truef(t, ok, "spec has no %s operation for %s", strings.ToUpper(method), path)
	return op
}

func (d coverageDocument) parameter(t *testing.T, method, path, name string) coverageParameter {
	t.Helper()
	op := d.operation(t, method, path)
	for _, p := range op.Parameters {
		resolved := p
		if p.Ref != "" {
			const prefix = "#/components/parameters/"
			require.True(t, strings.HasPrefix(p.Ref, prefix), "unexpected parameter ref %q", p.Ref)
			var ok bool
			resolved, ok = d.Components.Parameters[strings.TrimPrefix(p.Ref, prefix)]
			require.Truef(t, ok, "parameter ref %q does not resolve", p.Ref)
		}
		if resolved.Name == name {
			return resolved
		}
	}
	t.Fatalf("%s %s does not document parameter %q", strings.ToUpper(method), path, name)
	return coverageParameter{}
}

func resolveResponse(t *testing.T, doc coverageDocument, response coverageResponse) coverageResponse {
	t.Helper()
	if response.Ref == "" {
		return response
	}
	const prefix = "#/components/responses/"
	require.True(t, strings.HasPrefix(response.Ref, prefix), "unexpected response ref %q", response.Ref)
	resolved, ok := doc.Components.Responses[strings.TrimPrefix(response.Ref, prefix)]
	require.Truef(t, ok, "response ref %q does not resolve", response.Ref)
	return resolved
}

func resolveSchema(t *testing.T, doc coverageDocument, schema map[string]any) {
	t.Helper()
	ref, ok := schema["$ref"].(string)
	if !ok {
		return
	}
	const prefix = "#/components/schemas/"
	require.True(t, strings.HasPrefix(ref, prefix), "unexpected schema ref %q", ref)
	_, ok = doc.Components.Schemas[strings.TrimPrefix(ref, prefix)]
	require.Truef(t, ok, "schema ref %q does not resolve", ref)
}

// TestEveryOperationHasDocumentation is the broad guard for issue #295: an
// operation cannot be useful to a generated client if it has no stable ID,
// human-facing description, or response map. Response components are resolved
// before checking their descriptions so reusable errors count as documented
// responses rather than being treated as opaque placeholders.
func TestEveryOperationHasDocumentation(t *testing.T) {
	doc := readCoverageSpec(t)
	for path, pathItem := range doc.Paths {
		for method, op := range pathItem {
			t.Run(strings.ToUpper(method)+" "+path, func(t *testing.T) {
				require.NotEmpty(t, op.OperationID, "operation must have an operationId")
				require.NotEmpty(t, op.Summary, "operation must have a summary")
				require.NotEmpty(t, op.Description, "operation must have a description")
				require.NotEmpty(t, op.Responses, "operation must document at least one response")

				for status, response := range op.Responses {
					resolved := resolveResponse(t, doc, response)
					require.NotEmptyf(t, resolved.Description, "response %s must have a description", status)
					if status == "200" || status == "201" || status == "202" {
						require.NotEmptyf(t, resolved.Content, "%s %s response %s must describe its body", strings.ToUpper(method), path, status)
					}
					for mediaType, media := range resolved.Content {
						require.NotEmptyf(t, media.Schema, "%s %s response %s (%s) must have a schema",
							strings.ToUpper(method), path, status, mediaType)
						resolveSchema(t, doc, media.Schema)
					}
				}
			})
		}
	}
}

// TestEveryMutatingOperationHasARequestBody protects the request side of the
// contract. It is intentionally method-based: a newly added POST/PUT/PATCH
// route cannot silently ship without a body description.
func TestEveryMutatingOperationHasARequestBody(t *testing.T) {
	doc := readCoverageSpec(t)
	for path, pathItem := range doc.Paths {
		for method, op := range pathItem {
			switch method {
			case "post", "put", "patch":
			default:
				continue
			}
			t.Run(strings.ToUpper(method)+" "+path, func(t *testing.T) {
				require.NotNil(t, op.RequestBody, "mutating operation must document requestBody")
				require.Contains(t, op.RequestBody.Content, "application/json", "request body must document JSON")
				media := op.RequestBody.Content["application/json"]
				require.NotEmpty(t, media.Schema, "request body must have a schema")
				resolveSchema(t, doc, media.Schema)
			})
		}
	}
}

// TestKnownRequestSchemasMatchTheWireFormat table-drives the endpoints whose
// bodies are not self-evident from the URL. These schemas are the contract
// used by generated clients, so their names and requiredness are part of the
// documentation test rather than an implementation detail.
func TestKnownRequestSchemasMatchTheWireFormat(t *testing.T) {
	doc := readCoverageSpec(t)
	for _, tc := range []struct {
		name       string
		method     string
		path       string
		wantSchema string
		required   bool
	}{
		{name: "create tenant", method: "post", path: "/admin/tenants", wantSchema: "TenantCreateRequest", required: true},
		{name: "update tenant", method: "patch", path: "/admin/tenants/{id}", wantSchema: "TenantUpdateRequest", required: true},
		{name: "grant contract", method: "post", path: "/admin/tenants/{id}/grants", wantSchema: "GrantContractRequest", required: true},
		{name: "issue tenant key", method: "post", path: "/admin/tenants/{id}/keys", wantSchema: "TenantAPIKeyRequest", required: true},
		{name: "tenant watch", method: "post", path: "/tenant/watch", wantSchema: "WatchedContractRequest", required: true},
		{name: "global watch", method: "post", path: "/watched-contracts", wantSchema: "WatchedContractRequest", required: true},
		{name: "create API key", method: "post", path: "/apikeys", wantSchema: "APIKeyRequest", required: false},
		{name: "contract spec override", method: "put", path: "/contracts/{id}/spec", wantSchema: "ContractSpecOverrideRequest", required: true},
		{name: "create subscription", method: "post", path: "/subscriptions", wantSchema: "CreateSubscriptionRequest", required: true},
		{name: "update subscription", method: "put", path: "/subscriptions/{id}", wantSchema: "UpdateSubscriptionRequest", required: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := doc.operation(t, tc.method, tc.path)
			require.NotNil(t, op.RequestBody)
			require.Equal(t, tc.required, op.RequestBody.Required)
			media := op.RequestBody.Content["application/json"]
			ref, ok := media.Schema["$ref"].(string)
			require.True(t, ok, "request schema should be a component reference")
			require.Equal(t, "#/components/schemas/"+tc.wantSchema, ref)
			_, ok = doc.Components.Schemas[tc.wantSchema]
			require.True(t, ok, "request schema %s is missing", tc.wantSchema)
		})
	}
}

// TestKnownResponseSchemasMatchTheWireFormat table-drives the response shapes
// for management endpoints whose handlers return named records rather than a
// generic object. It keeps examples and generated client types tied to the
// JSON actually written by the API.
func TestKnownResponseSchemasMatchTheWireFormat(t *testing.T) {
	doc := readCoverageSpec(t)
	for _, tc := range []struct {
		name       string
		method     string
		path       string
		status     string
		wantSchema string
	}{
		{name: "list tenants", method: "get", path: "/admin/tenants", status: "200", wantSchema: "TenantList"},
		{name: "create tenant", method: "post", path: "/admin/tenants", status: "201", wantSchema: "Tenant"},
		{name: "get tenant", method: "get", path: "/admin/tenants/{id}", status: "200", wantSchema: "Tenant"},
		{name: "update tenant", method: "patch", path: "/admin/tenants/{id}", status: "200", wantSchema: "Tenant"},
		{name: "list grants", method: "get", path: "/admin/tenants/{id}/grants", status: "200", wantSchema: "GrantList"},
		{name: "current tenant", method: "get", path: "/tenant", status: "200", wantSchema: "CurrentTenant"},
		{name: "list tenant watch", method: "get", path: "/tenant/watch", status: "200", wantSchema: "TenantWatchList"},
		{name: "list global watch", method: "get", path: "/watched-contracts", status: "200", wantSchema: "WatchedContractsPage"},
		{name: "add global watch", method: "post", path: "/watched-contracts", status: "200", wantSchema: "WatchedContractAdded"},
		{name: "remove global watch", method: "delete", path: "/watched-contracts/{id}", status: "200", wantSchema: "WatchedContractRemoved"},
		{name: "tenant usage", method: "get", path: "/admin/tenants/{id}/usage", status: "200", wantSchema: "UsagePage"},
		{name: "tenant keys", method: "get", path: "/admin/tenants/{id}/keys", status: "200", wantSchema: "TenantAPIKeysPage"},
		{name: "address summary", method: "get", path: "/addresses/{address}/summary", status: "200", wantSchema: "AddressSummary"},
		{name: "aggregate events", method: "get", path: "/events/aggregate", status: "200", wantSchema: "AggregateResponse"},
		{name: "transaction events", method: "get", path: "/events/{id}/transaction", status: "200", wantSchema: "EventsResponse"},
		{name: "contract list", method: "get", path: "/contracts", status: "200", wantSchema: "ContractListResponse"},
		{name: "stats", method: "get", path: "/stats", status: "200", wantSchema: "Stats"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := doc.operation(t, tc.method, tc.path)
			response, ok := op.Responses[tc.status]
			require.True(t, ok, "operation must document a 200 response")
			resolved := resolveResponse(t, doc, response)
			media, ok := resolved.Content["application/json"]
			require.True(t, ok, "response must document an application/json body")
			ref, ok := media.Schema["$ref"].(string)
			require.True(t, ok, "response schema should be a component reference")
			require.Equal(t, "#/components/schemas/"+tc.wantSchema, ref)
			_, ok = doc.Components.Schemas[tc.wantSchema]
			require.True(t, ok, "response schema %s is missing", tc.wantSchema)
		})
	}
}

// TestFilterAndProjectionParametersAreDocumented covers the parameters added
// to the shared event parser and response projections. The table is grouped
// by operation so a future endpoint change has to make an explicit decision
// about which query controls it supports.
func TestFilterAndProjectionParametersAreDocumented(t *testing.T) {
	doc := readCoverageSpec(t)
	for _, tc := range []struct {
		name   string
		method string
		path   string
		params []string
	}{
		{
			name: "event list", method: "get", path: "/events",
			params: []string{"contract_id_prefix", "topic_contains", "tx_hash", "tx_index", "op_index", "in_successful_call", "has_value", "recent", "fields", "include_xdr", "stream", "envelope", "pretty", "If-None-Match"},
		},
		{
			name: "event count", method: "get", path: "/events/count",
			params: []string{"contract_id_prefix", "topic_contains", "tx_hash", "tx_index", "op_index", "in_successful_call", "has_value"},
		},
		{
			name: "event aggregate", method: "get", path: "/events/aggregate",
			params: []string{"contract_id_prefix", "topic_contains", "tx_hash", "tx_index", "op_index", "in_successful_call", "has_value"},
		},
		{
			name: "event CSV", method: "get", path: "/events.csv",
			params: []string{"contract_id_prefix", "topic_contains", "tx_hash", "tx_index", "op_index", "in_successful_call", "has_value", "recent"},
		},
		{
			name: "contract events", method: "get", path: "/contracts/{id}/events",
			params: []string{"topic_contains", "tx_hash", "tx_index", "op_index", "in_successful_call", "has_value", "recent", "fields", "include_xdr", "envelope"},
		},
		{
			name: "address events", method: "get", path: "/addresses/{address}/events",
			params: []string{"type", "contract_id", "envelope", "order_by", "recent"},
		},
		{
			name: "single event", method: "get", path: "/events/{id}",
			params: []string{"fields", "include_xdr"},
		},
		{
			name: "transaction events", method: "get", path: "/events/{id}/transaction",
			params: []string{"fields", "include_xdr"},
		},
		{
			name: "contract list", method: "get", path: "/contracts",
			params: []string{"envelope"},
		},
		{
			name: "dead letters", method: "get", path: "/dead-letters",
			params: []string{"envelope"},
		},
		{
			name: "subscription deliveries", method: "get", path: "/subscriptions/{id}/deliveries",
			params: []string{"envelope"},
		},
		{
			name: "live event stream", method: "get", path: "/events/ws",
			params: []string{"topic", "from_ledger", "to_ledger", "from_time", "to_time", "has_value"},
		},
		{
			name: "global watch", method: "post", path: "/watched-contracts",
			params: []string{"confirm"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range tc.params {
				p := doc.parameter(t, tc.method, tc.path, name)
				require.NotEmptyf(t, p.Description, "parameter %s must be explained", name)
				require.NotEmptyf(t, p.Schema, "parameter %s must have a schema", name)
			}
		})
	}
}

func TestStreamingAndExportResponsesDescribeMedia(t *testing.T) {
	doc := readCoverageSpec(t)
	for _, tc := range []struct {
		name   string
		method string
		path   string
		media  string
		header string
	}{
		{name: "event NDJSON stream", method: "get", path: "/events", media: "application/x-ndjson"},
		{name: "CSV export", method: "get", path: "/events.csv", media: "text/csv", header: "Content-Disposition"},
		{name: "contract export", method: "get", path: "/contracts/{id}/export", media: "text/csv", header: "Content-Disposition"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := doc.operation(t, tc.method, tc.path)
			response := resolveResponse(t, doc, op.Responses["200"])
			require.Contains(t, response.Content, tc.media)
			if tc.header != "" {
				require.Contains(t, response.Headers, tc.header)
			}
		})
	}
}

func TestEventSchemasDescribeWireFields(t *testing.T) {
	doc := readCoverageSpec(t)
	event := doc.Components.Schemas["Event"]
	require.Contains(t, event["required"], "network")
	properties, ok := event["properties"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, properties, "sep41_event")

	enriched := doc.Components.Schemas["EnrichedEvent"]
	allOf, ok := enriched["allOf"].([]any)
	require.True(t, ok)
	require.Len(t, allOf, 2)
	extra, ok := allOf[1].(map[string]any)
	require.True(t, ok)
	extraProperties, ok := extra["properties"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, extraProperties, "decode_error")
	require.Contains(t, extra["required"], "decoded")

	for _, name := range []string{"EventWithXDR", "ProjectedEvent", "EventEnvelopeResponse"} {
		require.Contains(t, doc.Components.Schemas, name)
	}
}

func TestEveryComponentParameterIsDescribed(t *testing.T) {
	doc := readCoverageSpec(t)
	for name, parameter := range doc.Components.Parameters {
		require.NotEmptyf(t, parameter.Description, "components parameter %s needs a description", name)
		require.NotEmptyf(t, parameter.Schema, "components parameter %s needs a schema", name)
	}
}
