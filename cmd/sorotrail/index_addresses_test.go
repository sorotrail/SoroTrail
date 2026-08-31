package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunIndexAddresses_Flags(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
	}{
		{"invalid batch size", []string{"--batch-size", "9999"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runIndexAddresses(tt.args)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}