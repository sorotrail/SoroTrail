package config

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// envVarsFromConfig returns the env var every Config struct field reads,
// keyed by name with its envDefault tag value ("" when none is set).
// Env args support the env/v11 ",..." form; only the primary name is used.
func envVarsFromConfig() map[string]string {
	var cfg Config
	val := reflect.TypeOf(cfg)
	envVars := make(map[string]string)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		tag := field.Tag.Get("env")
		if tag == "" || tag == "-" {
			continue
		}
		name := strings.TrimSpace(strings.Split(tag, ",")[0])
		if name == "" {
			continue
		}
		if _, dup := envVars[name]; !dup {
			envVars[name] = field.Tag.Get("envDefault")
		}
	}
	return envVars
}

// configTableRows parses the README's "## Configuration" section tables and
// returns each documented variable's cells: [Type, Default, Description].
func configTableRows(t *testing.T) map[string][3]string {
	t.Helper()

	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}

	section := string(readme)
	const marker = "## Configuration"
	start := strings.Index(section, marker)
	if start < 0 {
		t.Fatal("README.md must contain a ## Configuration section")
	}
	section = section[start+len(marker):]
	if end := regexp.MustCompile(`(?m)^## `).FindStringIndex(section); end != nil {
		section = section[:end[0]]
	}

	rowRe := regexp.MustCompile("`([A-Z][A-Z0-9_]+)`")
	rows := make(map[string][3]string)
	lines := strings.Split(section, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) < 4 {
			continue
		}
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		if cells[0] == "Variable" {
			continue
		}
		matches := rowRe.FindStringSubmatch(cells[0])
		if matches == nil {
			continue
		}
		rows[matches[1]] = [3]string{cells[1], cells[2], strings.Join(cells[3:], "|")}
	}
	return rows
}

// TestConfigEnvVarsDocumented verifies that every env var the Config struct
// reads appears as a fully populated row (Variable / Type / Default /
// Description) in the README's Configuration table. This prevents
// documentation drift: adding a new env tag (or removing a row) without
// updating the README fails this test.
func TestConfigEnvVarsDocumented(t *testing.T) {
	envVars := envVarsFromConfig()
	if len(envVars) == 0 {
		t.Fatal("no env vars found in Config struct")
	}

	rows := configTableRows(t)
	if len(rows) == 0 {
		t.Fatal("no variable rows found in the README Configuration table")
	}

	var missing []string
	var malformed []string
	for env := range envVars {
		t.Run(env, func(t *testing.T) {
			row, ok := rows[env]
			if !ok {
				missing = append(missing, env)
				t.Errorf("env var %s is read by Config but missing from the Configuration table", env)
				return
			}
			for _, cell := range row {
				if strings.TrimSpace(cell) == "" {
					malformed = append(malformed, env)
					t.Errorf("env var %s has an empty Type/Default/Description cell in the Configuration table", env)
					return
				}
			}
		})
	}

	// Report all missing/redundant vars once, not only the first.
	documented := make(map[string]bool, len(rows))
	for env := range rows {
		documented[env] = true
	}
	var redundant []string
	for env := range documented {
		if _, ok := envVars[env]; !ok {
			redundant = append(redundant, env)
		}
	}
	if len(missing) > 0 {
		t.Errorf(
			"env vars read by Config but missing from the Configuration table in README.md:\n  %s\n"+
				"Add them as | `VAR` | type | default | description | rows.", strings.Join(missing, "\n  "),
		)
	}
	if len(redundant) > 0 {
		t.Errorf(
			"env vars documented in the README Configuration table but not read by Config:\n  %s\n"+
				"Remove these rows or add the env tag to internal/config/config.go.", strings.Join(redundant, "\n  "),
		)
	}
}

// TestConfigDefaultsMatchREADME verifies the Default cell of each
// Configuration table row matches the struct's envDefault tag. This is a
// table-driven drift guard so a stale default in the README (e.g. the
// CORS_EXPOSED_HEADERS list) fails the build instead of confusing operators.
func TestConfigDefaultsMatchREADME(t *testing.T) {
	envVars := envVarsFromConfig()
	rows := configTableRows(t)

	var stale []string
	for env, want := range envVars {
		row, ok := rows[env]
		if !ok || strings.TrimSpace(want) == "" {
			continue
		}
		t.Run(env, func(t *testing.T) {
			got := strings.Trim(strings.TrimSpace(row[1]), "`")
			if !strings.Contains(got, want) {
				stale = append(stale, fmt.Sprintf("%s (want default %q, table says %q)", env, want, row[1]))
				t.Errorf("README default for %s is %q; struct envDefault is %q", env, row[1], want)
			}
		})
	}
	if len(stale) > 0 {
		t.Errorf("README defaults that disagree with internal/config/config.go:\n  %s", strings.Join(stale, "\n  "))
	}
}

// TestConfigEnvVarsInEnvExample verifies that every env var the Config struct
// reads appears in .env.example (either as an uncommented assignment or a
// commented-out example).
func TestConfigEnvVarsInEnvExample(t *testing.T) {
	envVars := envVarsFromConfig()

	example, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatalf("reading .env.example: %v", err)
	}

	inEnvExample := make(map[string]bool)
	for _, line := range strings.Split(string(example), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") && !strings.Contains(line, "=") {
			continue
		}
		varName := strings.TrimPrefix(line, "#")
		varName = strings.TrimSpace(varName)
		if idx := strings.Index(varName, "="); idx > 0 {
			varName = varName[:idx]
		}
		varName = strings.TrimSpace(varName)
		if varName != "" && varName[0] >= 'A' && varName[0] <= 'Z' {
			inEnvExample[varName] = true
		}
	}

	var missing []string
	for env := range envVars {
		if !inEnvExample[env] {
			missing = append(missing, env)
		}
	}
	if len(missing) > 0 {
		t.Errorf(
			"the following env vars are read by Config but not present in .env.example:\n  %s\n"+
				"Add them to .env.example (commented out is fine).", strings.Join(missing, "\n  "),
		)
	}
}
