package clientgen

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// pinnedVersionPattern matches an exact semantic version (with optional "v"
// prefix) and rejects floating references such as "latest", "main", or "v1".
var pinnedVersionPattern = regexp.MustCompile(`^v?\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)

// Manifest describes every generated client target checked by the unified
// client drift gate (pkg/client/targets.yaml).
type Manifest struct {
	SchemaVersion int            `yaml:"schema_version"`
	Spec          string         `yaml:"spec"`
	EmbeddedSpec  string         `yaml:"embedded_spec"`
	Targets       []ClientTarget `yaml:"targets"`
}

// ClientTarget defines a single language client generated from the OpenAPI spec.
type ClientTarget struct {
	Language     string   `yaml:"language"`
	Generator    string   `yaml:"generator"`
	Version      string   `yaml:"version"`
	Output       string   `yaml:"output"`
	GenerateCmd  []string `yaml:"generate_cmd"`
	TypecheckCmd []string `yaml:"typecheck_cmd"`
	ContractCmd  []string `yaml:"contract_cmd"`
}

// LoadManifest reads and validates a client target manifest from disk.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading client manifest %s: %w", path, err)
	}
	return ParseManifest(data)
}

// ParseManifest parses and validates a YAML client target manifest.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing client manifest: %w", err)
	}
	if err := ValidateManifest(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ValidateManifest enforces that every target names a language, generator,
// exact pinned version, output path, generation command, and typecheck command.
func ValidateManifest(m *Manifest) error {
	if m == nil {
		return fmt.Errorf("client manifest is nil")
	}
	if m.Spec == "" {
		return fmt.Errorf("client manifest must declare spec path")
	}
	if len(m.Targets) == 0 {
		return fmt.Errorf("client manifest must declare at least one target")
	}

	seenLang := make(map[string]bool, len(m.Targets))
	for i, t := range m.Targets {
		lang := strings.TrimSpace(t.Language)
		if lang == "" {
			return fmt.Errorf("target[%d]: language must not be empty", i)
		}
		if seenLang[lang] {
			return fmt.Errorf("target[%d]: duplicate language target %q", i, lang)
		}
		seenLang[lang] = true

		if strings.TrimSpace(t.Generator) == "" {
			return fmt.Errorf("target %q: generator must not be empty", lang)
		}
		ver := strings.TrimSpace(t.Version)
		if !pinnedVersionPattern.MatchString(ver) {
			return fmt.Errorf("target %q: generator version %q is not pinned to an exact semver release (expected e.g. v0.1.0)", lang, t.Version)
		}
		if strings.TrimSpace(t.Output) == "" {
			return fmt.Errorf("target %q: output path must not be empty", lang)
		}
		if len(t.GenerateCmd) == 0 {
			return fmt.Errorf("target %q: generate_cmd must not be empty", lang)
		}
		if len(t.TypecheckCmd) == 0 {
			return fmt.Errorf("target %q: typecheck_cmd must not be empty", lang)
		}
	}
	return nil
}

// ExpandCommand substitutes {spec} and {output} placeholders in a target's
// command template.
func ExpandCommand(template []string, specPath, outputPath string) []string {
	out := make([]string, len(template))
	for i, arg := range template {
		replaced := strings.ReplaceAll(arg, "{spec}", specPath)
		replaced = strings.ReplaceAll(replaced, "{output}", outputPath)
		out[i] = replaced
	}
	return out
}

// OperationRef identifies a single OpenAPI operation for drift diagnostics.
type OperationRef struct {
	OperationID string
	Method      string
	Path        string
	GoMethod    string
}

// ExtractSpecOperations parses the OpenAPI YAML and returns all documented
// operations sorted by path and HTTP method.
func ExtractSpecOperations(specBytes []byte) ([]OperationRef, error) {
	var spec rawSpec
	if err := yaml.Unmarshal(specBytes, &spec); err != nil {
		return nil, fmt.Errorf("parsing spec operations: %w", err)
	}
	g := &generator{
		spec:       &spec,
		schemas:    spec.Components.Schemas,
		params:     spec.Components.Parameters,
		synthNames: map[string]*rawSchema{},
	}
	genOps := g.buildOperations()
	out := make([]OperationRef, 0, len(genOps))
	for _, op := range genOps {
		opID := ""
		if methods, ok := spec.Paths[op.Path]; ok {
			if rawOp, ok := methods[strings.ToLower(op.Method)]; ok && rawOp != nil {
				opID = rawOp.OperationID
			}
		}
		if opID == "" {
			opID = op.GoName
		}
		out = append(out, OperationRef{
			OperationID: opID,
			Method:      op.Method,
			Path:        op.Path,
			GoMethod:    op.GoName,
		})
	}
	return out, nil
}

// DiagnoseClientDrift compares the committed client file against the freshly
// generated client output for a given language and returns an actionable error
// message naming the target language and every affected OpenAPI operation
// (operationId and METHOD /path) rather than a bare diff.
func DiagnoseClientDrift(language, outputPath string, specBytes, committed, generated []byte) string {
	normCommitted := normalizeNewlines(committed)
	normGenerated := normalizeNewlines(generated)
	if normCommitted == normGenerated {
		return ""
	}

	ops, _ := ExtractSpecOperations(specBytes)
	var affected []string
	for _, op := range ops {
		committedSnippet := extractOperationBlock(normCommitted, op)
		generatedSnippet := extractOperationBlock(normGenerated, op)
		if committedSnippet != generatedSnippet {
			affected = append(affected, fmt.Sprintf("%s (%s %s)", op.OperationID, op.Method, op.Path))
		}
	}

	// Also detect schema-level changes when operation signatures stayed identical.
	var spec rawSpec
	_ = yaml.Unmarshal(specBytes, &spec)
	var affectedSchemas []string
	for _, schemaName := range sortedKeys(spec.Components.Schemas) {
		committedType := extractTypeBlock(normCommitted, schemaName)
		generatedType := extractTypeBlock(normGenerated, schemaName)
		if committedType != generatedType {
			affectedSchemas = append(affectedSchemas, schemaName)
		}
	}
	sort.Strings(affectedSchemas)

	var b strings.Builder
	fmt.Fprintf(&b, "client drift detected for language=%q (output=%s):\n", language, outputPath)
	if len(affected) > 0 {
		fmt.Fprintf(&b, "  affected operations: %s\n", strings.Join(affected, ", "))
	} else {
		fmt.Fprintf(&b, "  affected operations: spec metadata / global client surface\n")
	}
	if len(affectedSchemas) > 0 {
		fmt.Fprintf(&b, "  affected schemas: %s\n", strings.Join(affectedSchemas, ", "))
	}
	fmt.Fprintf(&b, "  remediation: run `make client` to regenerate all client targets from api/openapi.yaml\n")
	fmt.Fprintf(&b, "  line diff summary:\n%s", summarizeLineDiff(normCommitted, normGenerated, 25))
	return b.String()
}

func normalizeNewlines(in []byte) string {
	return strings.ReplaceAll(string(in), "\r\n", "\n")
}

// extractOperationBlock locates the generated method or route declaration
// corresponding to op in source text so DiagnoseClientDrift can pinpoint which
// operation changed.
func extractOperationBlock(src string, op OperationRef) string {
	var parts []string
	routeNeedle := fmt.Sprintf("{Method: %q, Path: %q}", op.Method, op.Path)
	if strings.Contains(src, routeNeedle) {
		parts = append(parts, routeNeedle)
	}
	paramsHeader := "type " + op.GoMethod + "Params struct"
	if idx := strings.Index(src, paramsHeader); idx >= 0 {
		parts = append(parts, sliceUntilBlankLine(src[idx:]))
	}
	funcHeader := "func (c *Client) " + op.GoMethod + "("
	if idx := strings.Index(src, funcHeader); idx >= 0 {
		parts = append(parts, sliceUntilClosingBrace(src[idx:]))
	}
	return strings.Join(parts, "\n")
}

func extractTypeBlock(src, typeName string) string {
	prefix := "type " + typeName + " "
	idx := strings.Index(src, prefix)
	if idx < 0 {
		return ""
	}
	return sliceUntilBlankLine(src[idx:])
}

func sliceUntilBlankLine(s string) string {
	if end := strings.Index(s, "\n\n"); end >= 0 {
		return s[:end]
	}
	return s
}

func sliceUntilClosingBrace(s string) string {
	if end := strings.Index(s, "\n}\n"); end >= 0 {
		return s[:end+2]
	}
	return s
}

func summarizeLineDiff(want, got string, maxLines int) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	var out bytes.Buffer
	shown := 0
	maxLen := len(wantLines)
	if len(gotLines) > maxLen {
		maxLen = len(gotLines)
	}
	for i := 0; i < maxLen && shown < maxLines; i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			fmt.Fprintf(&out, "    line %d:\n      committed: %s\n      generated: %s\n", i+1, w, g)
			shown++
		}
	}
	return out.String()
}

// VerifyTarget runs a single target's generator into a temporary file, compares
// the output against the committed file on disk, and executes the target's
// compile/typecheck command.
func VerifyTarget(ctx context.Context, repoRoot string, specRelPath string, target ClientTarget) error {
	specAbs := filepath.Join(repoRoot, filepath.FromSlash(specRelPath))
	specBytes, err := os.ReadFile(specAbs)
	if err != nil {
		return fmt.Errorf("target %q: reading spec %s: %w", target.Language, specRelPath, err)
	}

	tmpDir, err := os.MkdirTemp("", "sorotrail-client-drift-"+target.Language+"-*")
	if err != nil {
		return fmt.Errorf("target %q: creating temp dir: %w", target.Language, err)
	}
	defer os.RemoveAll(tmpDir)

	tmpOut := filepath.Join(tmpDir, filepath.Base(target.Output))
	var generated []byte
	if target.Language == "go" && target.Generator == "github.com/sorotrail/sorotrail/cmd/clientgen" {
		generated, err = Generate(specBytes)
		if err != nil {
			return fmt.Errorf("target %q: generating from %s: %w", target.Language, specRelPath, err)
		}
		if writeErr := os.WriteFile(tmpOut, generated, 0o644); writeErr != nil {
			return fmt.Errorf("target %q: writing temp output: %w", target.Language, writeErr)
		}
	} else {
		cmdArgs := ExpandCommand(target.GenerateCmd, specAbs, tmpOut)
		cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
		cmd.Dir = repoRoot
		if out, runErr := cmd.CombinedOutput(); runErr != nil {
			return fmt.Errorf("target %q: generator command %v failed: %w\n%s", target.Language, cmdArgs, runErr, string(out))
		}
		generated, err = os.ReadFile(tmpOut)
		if err != nil {
			return fmt.Errorf("target %q: reading generated output %s: %w", target.Language, tmpOut, err)
		}
	}

	committedAbs := filepath.Join(repoRoot, filepath.FromSlash(target.Output))
	committed, err := os.ReadFile(committedAbs)
	if err != nil {
		return fmt.Errorf("target %q: reading committed client %s: %w", target.Language, target.Output, err)
	}

	if msg := DiagnoseClientDrift(target.Language, target.Output, specBytes, committed, generated); msg != "" {
		return fmt.Errorf("%s", msg)
	}

	// Compile / typecheck the generated client to ensure a clean regeneration
	// also builds cleanly.
	tcArgs := ExpandCommand(target.TypecheckCmd, specAbs, committedAbs)
	tcCmd := exec.CommandContext(ctx, tcArgs[0], tcArgs[1:]...)
	tcCmd.Dir = repoRoot
	if out, tcErr := tcCmd.CombinedOutput(); tcErr != nil {
		return fmt.Errorf("target %q: compile/typecheck command %v failed: %w\n%s", target.Language, tcArgs, tcErr, string(out))
	}

	return nil
}
