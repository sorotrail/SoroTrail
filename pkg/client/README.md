# pkg/client — the SoroTrail API client

`pkg/client` is the versioned Go client for the SoroTrail HTTP API. It is
**generated** from [`api/openapi.yaml`](../../api/openapi.yaml), the source
of truth for the API, by [`cmd/clientgen`](../../cmd/clientgen):

```sh
make client   # regenerates pkg/client/client.gen.go from api/openapi.yaml
```

The generated surface (`client.gen.go`) holds:

- one Go type per `components/schemas` entry;
- one typed method per documented operation, e.g.
  `Client.ListEvents(ctx, ListEventsParams)` or `Client.GetEvent(ctx, id, GetEventParams)`;
- `SpecVersion` (the spec's `info.version` the client was generated from)
  and `SpecRoutes` (every documented route).

`pkg/client/drift_test.go` regenerates the client from the committed spec
and compares byte-for-byte against the committed file, so a spec change
that is not followed by `make client` fails CI instead of drifting.
`client_test.go` exercises the public client against `httptest` in the
default test suite, including request construction, pagination, API
errors, authentication, and context cancellation. The database-backed
real-server test in `client_integration_test.go` remains available with
the `integration` build tag.

## Usage

```go
import "github.com/sorotrail/sorotrail/pkg/client"

c := client.New("https://sorotrail.example.com", client.WithAPIKey("st_..."))

page, err := c.ListEvents(ctx, client.ListEventsParams{
    ContractID: contractID,
    Limit:      50,
})
```

Non-2xx responses surface as `*client.APIError` carrying the server's
error envelope.

## Versioning

The client is versioned in lockstep with the API spec: `SpecVersion`
carries the `info.version` of the `api/openapi.yaml` it was generated
from, and release automation bumps that version with the API. See
[`RELEASING.md`](../../RELEASING.md) for how releases publish the client.

## Regenerating after a spec change

```sh
# 1. edit api/openapi.yaml
make spec          # refresh the served openapi.json copy
make client        # regenerate pkg/client/client.gen.go
make client-check  # run the unified manifest-driven client & route drift gate
```

Both files are committed; the unified drift gate (`make client-check`, backed by `pkg/client/targets.yaml`, `pkg/client/drift_test.go`, and `pkg/docs/drift_test.go`) fails the build until `make spec` / `make client` are run, naming the affected language and OpenAPI `operationId` (`METHOD /path`).

## Adding a new client language target

All generated clients are registered in [`pkg/client/targets.yaml`](targets.yaml) so a single CI job (`client-drift`) regenerates, diffs, type-checks, and contract-tests every language together:

1. **Add an entry under `targets:` in `pkg/client/targets.yaml`**:
   - `language`: unique identifier (e.g., `typescript` or `python`).
   - `generator`: generator package or binary name.
   - `version`: **exact semantic version pin** (`vX.Y.Z`); floating tags such as `latest` or `main` are rejected by `clientgen.ValidateManifest`.
   - `output`: repository-relative path to the committed generated file.
   - `generate_cmd`: command array using `{spec}` and `{output}` placeholders.
   - `typecheck_cmd`: command array that compiles or type-checks the generated client so a syntactically broken regeneration cannot pass.
   - `contract_cmd`: command array that runs the cross-language contract suite.
2. **Wire the contract adapter in `TestCrossLanguageContractHarness` (`pkg/client/drift_test.go`)**:
   - Execute the same `GET /health`, `GET /events`, and `GET /events/{id}` requests against the shared `httptest.Server` through the new client and compare its normalized `contractSnapshot` JSON against `baseline`.
3. **Run `make client-check`** to verify manifest validation, regeneration, compilation, route coverage, and cross-language contract parity.
