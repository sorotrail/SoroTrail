// Command clientgen generates the versioned API client in pkg/client
// from api/openapi.yaml — the source of truth for the HTTP API (the
// same file internal/specgen renders into the served openapi.json).
//
// Run it through `make client` after editing the spec; pkg/client's
// drift test fails the build when the committed client no longer matches
// the spec, so the two cannot silently diverge. The generation logic
// lives in internal/clientgen so the drift test can re-run it and
// compare byte-for-byte against the committed output.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sorotrail/sorotrail/internal/clientgen"
)

func main() {
	in := flag.String("in", "api/openapi.yaml", "path to the YAML source spec")
	out := flag.String("out", "pkg/client/client.gen.go", "path to the generated client")
	flag.Parse()

	code, err := clientgen.GenerateFromFile(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clientgen:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, code, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "clientgen:", err)
		os.Exit(1)
	}
}
