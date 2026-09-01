// Command specgen renders api/openapi.yaml into the JSON copy that
// internal/api embeds and serves at /openapi.json.
//
// Run it through `make spec` after editing api/openapi.yaml. The conversion
// itself lives in internal/specgen so the drift test can call it directly.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sorotrail/sorotrail/internal/specgen"
)

func main() {
	in := flag.String("in", "api/openapi.yaml", "path to the YAML source spec")
	out := flag.String("out", "internal/api/openapi.json", "path to the generated JSON copy")
	flag.Parse()

	if err := run(*in, *out); err != nil {
		fmt.Fprintln(os.Stderr, "specgen:", err)
		os.Exit(1)
	}
}

func run(in, out string) error {
	src, err := os.ReadFile(in)
	if err != nil {
		return fmt.Errorf("reading %s: %w", in, err)
	}
	rendered, err := specgen.Render(src)
	if err != nil {
		return fmt.Errorf("rendering %s: %w", in, err)
	}
	if err := os.WriteFile(out, rendered, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", out, err)
	}
	return nil
}
