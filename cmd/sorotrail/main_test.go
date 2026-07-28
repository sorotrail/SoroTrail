package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLoggerJSONOutput(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.New(h).Info("hello", "key", "value")

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("JSON output did not parse: %v\nraw: %s", err, buf.String())
	}
	if parsed["msg"] != "hello" {
		t.Errorf("expected msg=hello, got %v", parsed["msg"])
	}
	if parsed["key"] != "value" {
		t.Errorf("expected key=value, got %v", parsed["key"])
	}
}

func TestNewLoggerUsesTextByDefault(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.New(h).Info("test")

	output := buf.String()
	if strings.Contains(output, `"msg"`) {
		t.Error("expected text output, got JSON")
	}
	if !strings.Contains(output, "test") {
		t.Errorf("expected log message in text output, got %q", output)
	}
}
