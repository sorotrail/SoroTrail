// Command clientgen generates the versioned API client in pkg/client
// from api/openapi.yaml — the source of truth for the HTTP API (the
// same file internal/specgen renders into the served openapi.json) — or
// verifies all targets declared in pkg/client/targets.yaml when invoked
// with -check.
//
// Run it through `make client` after editing the spec; `make client-check`
// and pkg/client's drift test fail the build when any committed client no
// longer matches the spec or fails to compile/type-check.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sorotrail/sorotrail/internal/clientgen"
)

func main() {
	in := flag.String("in", "api/openapi.yaml", "path to the YAML source spec")
	out := flag.String("out", "pkg/client/client.gen.go", "path to the generated client")
	manifestPath := flag.String("manifest", "pkg/client/targets.yaml", "path to the client target manifest")
	check := flag.Bool("check", false, "verify every target in the manifest is up to date and compiles")
	flag.Parse()

	if *check {
		if err := runCheck(*manifestPath); err != nil {
			fmt.Fprintln(os.Stderr, "clientgen -check:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(*in, *out); err != nil {
		fmt.Fprintln(os.Stderr, "clientgen:", err)
		os.Exit(1)
	}
}

func run(in, out string) error {
	code, err := clientgen.GenerateFromFile(in)
	if err != nil {
		return err
	}
	return os.WriteFile(out, code, 0o644)
}

func runCheck(manifestPath string) error {
	m, err := clientgen.LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	for _, target := range m.Targets {
		if err := clientgen.VerifyTarget(ctx, ".", m.Spec, target); err != nil {
			return err
		}
		fmt.Printf("[OK] client target %s (generator=%s@%s, output=%s) is up to date and compiles\n",
			target.Language, target.Generator, target.Version, target.Output)
	}
	return nil
}
