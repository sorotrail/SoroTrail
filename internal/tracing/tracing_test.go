package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoopExporter(t *testing.T) {
	exp := &noopExporter{}
	err := exp.ExportSpans(context.Background(), nil)
	assert.NoError(t, err)
	err = exp.Shutdown(context.Background())
	assert.NoError(t, err)
}
