package clientgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSnakeCase(t *testing.T) {
	cases := map[string]string{
		"listEvents":                      "list_events",
		"exportEventsCSV":                 "export_events_csv",
		"createAPIKey":                    "create_api_key",
		"listAPIKeys":                     "list_api_keys",
		"eventStreamWS":                   "event_stream_ws",
		"ContractID":                      "contract_id",
		"getEvents":                       "get_events",
		"getLatestLedger":                 "get_latest_ledger",
		"listContractSpecOverrideHistory": "list_contract_spec_override_history",
	}
	for in, want := range cases {
		assert.Equal(t, want, toSnakeCase(in), "input: %s", in)
	}
}

func TestGeneratePython_ErrorResponseNormalization(t *testing.T) {
	specLegacy := []byte(`openapi: "3.0.3"
info:
  title: "SoroTrail"
  version: "0.2.0"
paths: {}
components:
  schemas:
    ErrorResponse:
      type: object
      description: Standard error envelope
      properties:
        error:
          type: string
          description: Human-readable error message
      required: [error]
`)

	specWithCodes := []byte(`openapi: "3.0.3"
info:
  title: "SoroTrail"
  version: "0.2.0"
paths: {}
components:
  schemas:
    ErrorResponse:
      type: object
      description: Standard error envelope
      properties:
        error:
          type: string
          description: Human-readable error message
        code:
          type: string
          description: Stable machine-readable error code
        field:
          type: string
          description: Request parameter or JSON field that caused a validation error
      required: [error, code]
`)

	outLegacy, err := GeneratePython(specLegacy)
	require.NoError(t, err)

	outWithCodes, err := GeneratePython(specWithCodes)
	require.NoError(t, err)

	assert.Equal(t, string(outLegacy), string(outWithCodes),
		"ErrorResponse normalization must yield identical Python output before and after #1270")
	assert.Contains(t, string(outWithCodes), "class ErrorResponse:")
	assert.Contains(t, string(outWithCodes), `code: str = ""`)
	assert.Contains(t, string(outWithCodes), `error: str = ""`)
	assert.Contains(t, string(outWithCodes), `field: str = ""`)
}

func TestGeneratePython_FullSpecDeterminism(t *testing.T) {
	out1, err := GeneratePythonFromFile("../../api/openapi.yaml")
	require.NoError(t, err)

	out2, err := GeneratePythonFromFile("../../api/openapi.yaml")
	require.NoError(t, err)

	assert.Equal(t, out1, out2, "GeneratePythonFromFile must be byte-for-byte deterministic")
	assert.Contains(t, string(out1), "class _GeneratedClientBase:")
	assert.Contains(t, string(out1), "def list_events(")
	assert.Contains(t, string(out1), "def list_contracts(")
	assert.Contains(t, string(out1), "def create_tenant(")
}
