package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const smallValidSpec = `openapi: "3.0.3"
info:
  title: "Test Service"
  version: "1.2.3"
paths:
  /status:
    get:
      operationId: getStatus
      summary: Service status endpoint
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/StatusResponse"
components:
  schemas:
    StatusResponse:
      type: object
      required:
        - ok
      properties:
        ok:
          type: boolean
          description: whether service is operational
`

var (
	clientgenBinOnce sync.Once
	clientgenBinPath string
	clientgenBinErr  error
)

func buildClientgenBinary(t *testing.T) string {
	t.Helper()
	clientgenBinOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "clientgen-bin-*")
		if err != nil {
			clientgenBinErr = err
			return
		}
		bin := filepath.Join(tmpDir, "clientgen")
		if runtime.GOOS == "windows" {
			bin += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", bin, ".")
		out, err := cmd.CombinedOutput()
		if err != nil {
			clientgenBinErr = err
			return
		}
		_ = out
		clientgenBinPath = bin
	})
	require.NoError(t, clientgenBinErr, "building clientgen binary")
	return clientgenBinPath
}

// TestClientgen_SmallKnownInput verifies that a small valid OpenAPI spec
// produces the expected Go client types, route table, and methods (#1097).
func TestClientgen_SmallKnownInput(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "spec.yaml")
	outPath := filepath.Join(tmpDir, "client.gen.go")

	require.NoError(t, os.WriteFile(inPath, []byte(smallValidSpec), 0o644))

	err := run(inPath, outPath)
	require.NoError(t, err)

	content, err := os.ReadFile(outPath)
	require.NoError(t, err)

	code := string(content)
	assert.Contains(t, code, "package client")
	assert.Contains(t, code, `const SpecVersion = "1.2.3"`)
	assert.Contains(t, code, `{Method: "GET", Path: "/status"}`)
	assert.Contains(t, code, "type StatusResponse struct")
	assert.Contains(t, code, "Ok bool `json:\"ok\"`")
	assert.Contains(t, code, "func (c *Client) GetStatus(ctx context.Context) (*StatusResponse, error)")
}

// TestClientgen_MalformedYAML asserts that malformed YAML produces a loud error
// and writes nothing to the output path (#1097).
func TestClientgen_MalformedYAML(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "malformed.yaml")
	outPath := filepath.Join(tmpDir, "client.gen.go")

	require.NoError(t, os.WriteFile(inPath, []byte("openapi: [unclosed list"), 0o644))

	err := run(inPath, outPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parsing spec")

	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "output file must not be created on parse failure")
}

// TestClientgen_MissingVersion asserts that a spec without info.version is refused (#1097).
func TestClientgen_MissingVersion(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "no_version.yaml")
	outPath := filepath.Join(tmpDir, "client.gen.go")

	noVersionSpec := `openapi: "3.0.3"
info:
  title: "No Version API"
paths: {}
`
	require.NoError(t, os.WriteFile(inPath, []byte(noVersionSpec), 0o644))

	err := run(inPath, outPath)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "info.version")

	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "output file must not be created when version is missing")
}

// TestClientgen_EmptyInput asserts that an empty file is refused loudly (#1097).
func TestClientgen_EmptyInput(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "empty.yaml")
	outPath := filepath.Join(tmpDir, "client.gen.go")

	require.NoError(t, os.WriteFile(inPath, []byte(""), 0o644))

	err := run(inPath, outPath)
	assert.Error(t, err)

	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "output file must not be created on empty input")
}

// TestClientgen_NonExistentInput asserts that a missing input file returns an error (#1097).
func TestClientgen_NonExistentInput(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "does-not-exist.yaml")
	outPath := filepath.Join(tmpDir, "client.gen.go")

	err := run(inPath, outPath)
	assert.Error(t, err)

	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "output file must not be created on missing input")
}

// TestClientgen_Determinism asserts that running the generator twice on the same spec
// produces byte-identical output (#1097).
func TestClientgen_Determinism(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "spec.yaml")
	outPath1 := filepath.Join(tmpDir, "out1.go")
	outPath2 := filepath.Join(tmpDir, "out2.go")

	require.NoError(t, os.WriteFile(inPath, []byte(smallValidSpec), 0o644))

	require.NoError(t, run(inPath, outPath1))
	require.NoError(t, run(inPath, outPath2))

	bytes1, err := os.ReadFile(outPath1)
	require.NoError(t, err)
	bytes2, err := os.ReadFile(outPath2)
	require.NoError(t, err)

	assert.Equal(t, bytes1, bytes2, "two generator runs on the same input must be byte-identical")
}

// TestClientgen_GeneratedClientCompiles verifies that code emitted by clientgen
// compiles cleanly alongside pkg/client/client.go (#1097).
func TestClientgen_GeneratedClientCompiles(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "spec.yaml")
	outGenPath := filepath.Join(tmpDir, "client.gen.go")

	require.NoError(t, os.WriteFile(inPath, []byte(smallValidSpec), 0o644))
	require.NoError(t, run(inPath, outGenPath))

	// Copy pkg/client/client.go into the same directory so it forms a complete package
	clientGoBytes, err := os.ReadFile("../../pkg/client/client.go")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "client.go"), clientGoBytes, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "go.mod"), []byte("module testpkg\n\ngo 1.22\n"), 0o644))

	// Run `go vet` on the standalone package
	cmd := exec.Command("go", "vet", ".")
	cmd.Dir = tmpDir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "generated client must pass go vet: %s", string(out))
}

// TestClientgen_CLIExecution verifies the command-line flags and non-zero exit code (#1097).
func TestClientgen_CLIExecution(t *testing.T) {
	bin := buildClientgenBinary(t)
	tmpDir := t.TempDir()

	t.Run("success with flags", func(t *testing.T) {
		inPath := filepath.Join(tmpDir, "cli_valid.yaml")
		outPath := filepath.Join(tmpDir, "cli_client.gen.go")
		require.NoError(t, os.WriteFile(inPath, []byte(smallValidSpec), 0o644))

		cmd := exec.Command(bin, "-in", inPath, "-out", outPath)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err := cmd.Run()
		require.NoError(t, err, "binary should exit 0 on valid spec: %s", stderr.String())

		content, err := os.ReadFile(outPath)
		require.NoError(t, err)
		assert.Contains(t, string(content), "const SpecVersion = \"1.2.3\"")
	})

	t.Run("success with python output flag", func(t *testing.T) {
		inPath := filepath.Join(tmpDir, "cli_py_valid.yaml")
		outPath := filepath.Join(tmpDir, "cli_py_client.gen.go")
		pyOutPath := filepath.Join(tmpDir, "py", "_generated.py")
		require.NoError(t, os.WriteFile(inPath, []byte(smallValidSpec), 0o644))

		cmd := exec.Command(bin, "-in", inPath, "-out", outPath, "-py-out", pyOutPath)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err := cmd.Run()
		require.NoError(t, err, "binary should exit 0 on valid spec: %s", stderr.String())

		pyContent, err := os.ReadFile(pyOutPath)
		require.NoError(t, err)
		assert.Contains(t, string(pyContent), `SPEC_VERSION: Final[str] = "1.2.3"`)
		assert.Contains(t, string(pyContent), "class StatusResponse:")
		assert.Contains(t, string(pyContent), "def get_status(self) -> StatusResponse:")
	})

	t.Run("failure with non-zero exit on malformed spec", func(t *testing.T) {
		inPath := filepath.Join(tmpDir, "cli_bad.yaml")
		outPath := filepath.Join(tmpDir, "cli_bad_out.go")
		require.NoError(t, os.WriteFile(inPath, []byte("bad: [yaml"), 0o644))

		cmd := exec.Command(bin, "-in", inPath, "-out", outPath)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err := cmd.Run()
		assert.Error(t, err, "binary should exit non-zero on malformed spec")

		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		assert.Equal(t, 1, exitErr.ExitCode())
		assert.Contains(t, stderr.String(), "clientgen:")

		_, statErr := os.Stat(outPath)
		assert.True(t, os.IsNotExist(statErr), "output file should not exist on failure")
	})
}
