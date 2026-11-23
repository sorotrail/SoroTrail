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
	"path/filepath"

	"github.com/sorotrail/sorotrail/internal/clientgen"
)

const (
	defaultOutput   = "pkg/client/client.gen.go"
	defaultPyOutput = "clients/python/sorotrail/_generated.py"
)

func main() {
	in := flag.String("in", "api/openapi.yaml", "path to the YAML source spec")
	out := flag.String("out", defaultOutput, "path to the generated client")
	pyOut := flag.String("py-out", "", "path to the generated Python client")
	flag.Parse()

	if err := run(*in, *out); err != nil {
		fmt.Fprintln(os.Stderr, "clientgen:", err)
		os.Exit(1)
	}
	resolvedPyOut := *pyOut
	if resolvedPyOut == "" && *out == defaultOutput {
		resolvedPyOut = defaultPyOutput
	}
	if resolvedPyOut != "" {
		if err := runPython(*in, resolvedPyOut); err != nil {
			fmt.Fprintln(os.Stderr, "clientgen:", err)
			os.Exit(1)
		}
	}
}

func run(in, out string) error {
	code, err := clientgen.GenerateFromFile(in)
	if err != nil {
		return err
	}
	return os.WriteFile(out, code, 0o644)
}

func runPython(in, out string) error {
	code, err := clientgen.GeneratePythonFromFile(in)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(out); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	return os.WriteFile(out, code, 0o600)
}
