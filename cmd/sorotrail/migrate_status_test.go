package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunMigrateStatusHelp(t *testing.T) {
	// --help should not return an error.
	err := runMigrateStatus([]string{"--help"})
	assert.NoError(t, err, "--help should not return an error")
}

func TestRunMigrateStatusUnknownFlag(t *testing.T) {
	err := runMigrateStatus([]string{"--unknown-flag"})
	assert.Error(t, err, "unknown flag should return an error")
}
