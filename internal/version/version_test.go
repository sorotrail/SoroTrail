package version

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultsWhenUnset(t *testing.T) {
	// Backup original values
	origVer, origCommit, origDate := Version, Commit, Date
	defer func() {
		Version, Commit, Date = origVer, origCommit, origDate
	}()

	// Ensure the defaults are as expected
	assert.Equal(t, "dev", GetVersion())
	assert.Equal(t, "unknown", GetCommit())
	assert.Equal(t, "unknown", GetDate())

	// Test fallback when set to empty string
	Version, Commit, Date = "", "", ""
	assert.Equal(t, "dev", GetVersion())
	assert.Equal(t, "unknown", GetCommit())
	assert.Equal(t, "unknown", GetDate())
}
