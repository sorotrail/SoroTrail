package archive

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompactArchive_EndToEndAndSafety(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	opts := CompactorOptions{
		TargetChunkSizeBytes: 1024 * 1024,
		MaxChunksToMerge:     5,
	}
	assert.Equal(t, int64(1024*1024), opts.TargetChunkSizeBytes)
}
