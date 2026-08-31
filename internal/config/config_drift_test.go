package config

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// TestConfigEnvVarsDocumented verifies that every env var the Config struct
// reads appears in the README's Configuration table. This prevents
// documentation drift: adding a new env tag without updating the README
// will fail this test.
func TestConfigEnvVarsDocumented(t *testing.T) {
	// 1. Extract all env var names from Config struct tags.
	var cfg Config
	val := reflect.TypeOf(cfg)
	envVars := make(map[string]bool)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		tag := field.Tag.Get("env")
		if tag == "" || tag == "-" {
			continue
		}
		// Handle comma-separated env vars (env/v11 syntax).
		for _, part := range strings.Split(tag, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				envVars[part] = true
			}
		}
	}

	if len(envVars) == 0 {
		t.Fatal("no env vars found in Config struct")
	}

	// 2. Read the README and extract backtick-quoted variable names from
	// the Configuration table rows.
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}

	// Match `VAR_NAME` patterns in the README (backtick-wrapped identifiers).
	re := regexp.MustCompile("`([A-Z][A-Z0-9_]+)`")
	matches := re.FindAllSubmatch(readme, -1)
	documented := make(map[string]bool)
	for _, m := range matches {
		name := string(m[1])
		// Only consider known config-like names (uppercase, underscores).
		documented[name] = true
	}

	// 3. Check that every env var from Config appears in the README.
	var missing []string
	for env := range envVars {
		if !documented[env] {
			missing = append(missing, env)
		}
	}

	if len(missing) > 0 {
		t.Errorf(
			"the following env vars are read by Config but not documented in README.md:\n"+
				"  %s\n"+
				"Add them to the Configuration table in README.md.", strings.Join(missing, "\n  "),
		)
	}
}

// TestConfigEnvVarsInEnvExample verifies that every env var the Config struct
// reads appears in .env.example (either as an uncommented assignment or a
// commented-out example).
func TestConfigEnvVarsInEnvExample(t *testing.T) {
	// 1. Extract all env var names from Config struct tags.
	var cfg Config
	val := reflect.TypeOf(cfg)
	envVars := make(map[string]bool)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		tag := field.Tag.Get("env")
		if tag == "" || tag == "-" {
			continue
		}
		for _, part := range strings.Split(tag, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				envVars[part] = true
			}
		}
	}

	// 2. Read .env.example and extract variable names (commented or not).
	example, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatalf("reading .env.example: %v", err)
	}

	lines := strings.Split(string(example), "\n")
	inEnvExample := make(map[string]bool)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") && !strings.Contains(line, "=") {
			continue
		}
		// Lines like "# VAR=value" or "VAR=value".
		varName := line
		varName = strings.TrimPrefix(varName, "#")
		varName = strings.TrimSpace(varName)
		if idx := strings.Index(varName, "="); idx > 0 {
			varName = varName[:idx]
		}
		varName = strings.TrimSpace(varName)
		if varName != "" && varName[0] >= 'A' && varName[0] <= 'Z' {
			inEnvExample[varName] = true
		}
	}

	// 3. Check that every env var from Config appears in .env.example.
	var missing []string
	for env := range envVars {
		if !inEnvExample[env] {
			missing = append(missing, env)
		}
	}

	if len(missing) > 0 {
		t.Errorf(
			"the following env vars are read by Config but not present in .env.example:\n"+
				"  %s\n"+
				"Add them to .env.example (commented out is fine).", strings.Join(missing, "\n  "),
		)
	}
}
