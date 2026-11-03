package clientgen_test

import (
	"strings"
	"testing"

	"github.com/sorotrail/sorotrail/internal/clientgen"
)

const deprecatedSpecYAML = `openapi: 3.1.0
info:
  title: SoroTrail API
  version: 0.2.0
paths:
  /events:
    get:
      operationId: listEvents
      summary: List events
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Event"
  /legacy-events:
    get:
      operationId: listLegacyEvents
      summary: List legacy events
      deprecated: true
      x-deprecation-notice: "Legacy event listing is superseded."
      x-deprecation-replacement: "GET /events"
      x-sunset-date: "2026-09-01T00:00:00Z"
      responses:
        "200":
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Event"
  /bare-deprecated:
    get:
      operationId: getBareDeprecated
      summary: Bare deprecated endpoint
      deprecated: true
      responses:
        "204":
          description: No content
components:
  schemas:
    Event:
      type: object
      description: Indexed event
      required:
        - id
      properties:
        id:
          type: string
          description: Event identifier
        legacy_topic:
          type: string
          description: Legacy single topic string
          deprecated: true
          x-deprecation-replacement: "topics"
          x-sunset-date: "2026-09-01"
`

func TestGenerate_DeprecationComments(t *testing.T) {
	goSrc, err := clientgen.Generate([]byte(deprecatedSpecYAML))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	goCode := string(goSrc)

	wantOpComment := "// Deprecated: Legacy event listing is superseded. Use GET /events instead. Sunset: 2026-09-01T00:00:00Z."
	if !strings.Contains(goCode, wantOpComment) {
		t.Errorf("generated Go client missing operation deprecation comment %q in:\n%s", wantOpComment, goCode)
	}

	wantBareComment := "// Deprecated: GetBareDeprecated (GET /bare-deprecated) is deprecated."
	if !strings.Contains(goCode, wantBareComment) {
		t.Errorf("generated Go client missing bare operation deprecation comment %q in:\n%s", wantBareComment, goCode)
	}

	wantFieldComment := "// Deprecated: LegacyTopic is deprecated. Use topics instead. Sunset: 2026-09-01."
	if !strings.Contains(goCode, wantFieldComment) {
		t.Errorf("generated Go client missing field deprecation comment %q in:\n%s", wantFieldComment, goCode)
	}

	if strings.Contains(goCode, "ListEvents (GET /events) is deprecated") {
		t.Errorf("non-deprecated operation ListEvents unexpectedly marked deprecated")
	}

	pySrc, err := clientgen.GeneratePython([]byte(deprecatedSpecYAML))
	if err != nil {
		t.Fatalf("GeneratePython: %v", err)
	}
	pyCode := string(pySrc)

	wantPyOp := `"""List legacy events (GET /legacy-events). Deprecated: Legacy event listing is superseded. Use GET /events instead. Sunset: 2026-09-01T00:00:00Z."""`
	if !strings.Contains(pyCode, wantPyOp) {
		t.Errorf("generated Python client missing operation deprecation notice %q in:\n%s", wantPyOp, pyCode)
	}

	wantPyField := "# Deprecated: legacy_topic is deprecated. Use topics instead. Sunset: 2026-09-01."
	if !strings.Contains(pyCode, wantPyField) {
		t.Errorf("generated Python client missing field deprecation comment %q in:\n%s", wantPyField, pyCode)
	}
}
