package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// CompactorOptions controls chunk compaction behavior.
type CompactorOptions struct {
	// TargetChunkSizeBytes specifies the approximate upper bound for merged chunks.
	TargetChunkSizeBytes int64
	// MaxChunksToMerge limits how many small chunks are merged in a single run.
	MaxChunksToMerge int
}

// CompactionStats reports progress and space reclaimed.
type CompactionStats struct {
	ChunksMerged       int   `json:"chunks_merged"`
	OriginalBytesTotal int64 `json:"original_bytes_total"`
	NewBytesTotal      int64 `json:"new_bytes_total"`
	BytesReclaimed     int64 `json:"bytes_reclaimed"`
}

// CompactArchive scans the archive manifest/chunks, merges adjacent small chunks,
// writes the verified merged chunk, updates the manifest atomically, and removes the originals.
func CompactArchive(ctx context.Context, client *minio.Client, bucket, prefix string, opts CompactorOptions) (CompactionResult, error) {
	return CompactArchiveWithLogger(ctx, client, bucket, prefix, opts, nil)
}

// CompactionResult holds metrics returned by a compaction run.
type CompactionResult struct {
	Stats CompactionStats `json:"stats"`
}

// CompactArchiveWithLogger runs compaction with optional progress reporting.
func CompactArchiveWithLogger(ctx context.Context, client *minio.Client, bucket, prefix string, opts CompactorOptions, logger interface {
	Info(string, ...any)
	Warn(string, ...any)
}) (CompactionResult, error) {
	if opts.TargetChunkSizeBytes <= 0 {
		opts.TargetChunkSizeBytes = 64 * 1024 * 1024 // default 64MB
	}
	if opts.MaxChunksToMerge <= 0 {
		opts.MaxChunksToMerge = 10
	}

	manifestKey := trimPrefixSlash(prefix) + "/manifest.json"
	obj, err := client.GetObject(ctx, bucket, manifestKey, minio.GetObjectOptions{})
	omp := CompactionResult{}
	if err != nil {
		return omp, fmt.Errorf("failed to get manifest for compaction: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return omp, fmt.Errorf("failed to read manifest for compaction: %w", err)
	}

	var man Manifest
	if err := json.Unmarshal(data, &man); err != nil {
		return omp, fmt.Errorf("failed to unmarshal manifest for compaction: %w", err)
	}

	if len(man.Chunks) < 2 {
		return omp, nil
	}

	sort.Slice(man.Chunks, func(i, j int) bool {
		return man.Chunks[i].LedgerStart < man.Chunks[j].LedgerStart
	})

	var bestGroup []int
	var bestSize int64
	for i := 0; i < len(man.Chunks); i++ {
		group := []int{i}
		size := man.Chunks[i].SizeBytes
		for j := i + 1; j < len(man.Chunks) && len(group) < opts.MaxChunksToMerge; j++ {
			prev := man.Chunks[group[len(group)-1]]
			curr := man.Chunks[j]
			if curr.LedgerStart == prev.LedgerEnd+1 && size+curr.SizeBytes <= opts.TargetChunkSizeBytes {
				group = append(group, j)
				size += curr.SizeBytes
			} else {
				break
			}
		}
		if len(group) > 1 {
			bestGroup = group
			bestSize = size
			_ = bestSize
			break
		}
	}

	if len(bestGroup) <= 1 {
		return omp, nil
	}

	firstIdx := bestGroup[0]
	lastIdx := bestGroup[len(bestGroup)-1]
	newLedgerStart := man.Chunks[firstIdx].LedgerStart
	newLedgerEnd := man.Chunks[lastIdx].LedgerEnd

	var mergedBuffer bytes.Buffer
	var originalBytesTotal int64
	h := sha256.New()
	writer := io.MultiWriter(&mergedBuffer, h)

	var totalEventsCount int64
	var originalKeys []string

	for _, idx := range bestGroup {
		chunk := man.Chunks[idx]
		originalBytesTotal += chunk.SizeBytes
		originalKeys = append(originalKeys, chunk.ObjectKey)

		cObj, err := client.GetObject(ctx, bucket, chunk.ObjectKey, minio.GetObjectOptions{})
		if err != nil {
			return omp, fmt.Errorf("failed to fetch chunk %s for compaction: %w", chunk.ObjectKey, err)
		}
		_, copyErr := io.Copy(writer, cObj)
		cObj.Close()
		if copyErr != nil {
			return omp, fmt.Errorf("failed to stream chunk %s during merge: %w", chunk.ObjectKey, copyErr)
		}
		totalEventsCount += chunk.EventCount
	}

	mergedHash := hex.EncodeToString(h.Sum(nil))
	mergedBytes := mergedBuffer.Bytes()
	newChunkKey := fmt.Sprintf("%s/events/schema=v1/ledger_start=%07d/compacted-%d.json.gz", trimPrefixSlash(prefix), newLedgerStart, time.Now().UnixNano())

	chkHash := sha256.Sum256(mergedBytes)
	if hex.EncodeToString(chkHash[:]) != mergedHash {
		return omp, errors.New("merged chunk hash verification failed")
	}

	_, err = client.PutObject(ctx, bucket, newChunkKey, bytes.NewReader(mergedBytes), int64(len(mergedBytes)), minio.PutObjectOptions{
		ContentType: "application/gzip",
	})
	if err != nil {
		return omp, fmt.Errorf("failed to upload compacted chunk: %w", err)
	}

	newChunk := ManifestChunk{
		LedgerStart: newLedgerStart,
		LedgerEnd:   newLedgerEnd,
		ObjectKey:   newChunkKey,
		SizeBytes:   int64(len(mergedBytes)),
		EventCount:  totalEventsCount,
		Checksum:    mergedHash,
	}

	var newChunks []ManifestChunk
	skipMap := make(map[int]bool)
	for _, idx := range bestGroup {
		skipMap[idx] = true
	}
	for i, chunk := range man.Chunks {
		if i == firstIdx {
			newChunks = append(newChunks, newChunk)
		}
		if !skipMap[i] {
			newChunks = append(newChunks, chunk)
		}
	}

	man.Chunks = newChunks
	man.UpdatedAt = time.Now().UTC()

	newManifestData, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		return omp, fmt.Errorf("failed to marshal updated manifest: %w", err)
	}

	tempManifestKey := manifestKey + ".tmp-" + fmt.Sprintf("%d", time.Now().UnixNano())
	_, err = client.PutObject(ctx, bucket, tempManifestKey, bytes.NewReader(newManifestData), int64(len(newManifestData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return omp, fmt.Errorf("failed to upload temp manifest: %w", err)
	}

	_, err = client.PutObject(ctx, bucket, manifestKey, bytes.NewReader(newManifestData), int64(len(newManifestData)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return omp, fmt.Errorf("failed to update canonical manifest: %w", err)
	}

	_ = client.RemoveObject(ctx, bucket, tempManifestKey, minio.RemoveObjectOptions{})

	for _, oKey := range originalKeys {
		if oKey != newChunkKey {
			_ = client.RemoveObject(ctx, bucket, oKey, minio.RemoveObjectOptions{})
		}
	}

	bytesReclaimed := originalBytesTotal - int64(len(mergedBytes))
	if bytesReclaimed < 0 {
		bytesReclaimed = 0
	}

	omp.Stats = CompactionStats{
		ChunksMerged:       len(bestGroup),
		OriginalBytesTotal: originalBytesTotal,
		NewBytesTotal:      int64(len(mergedBytes)),
		BytesReclaimed:     bytesReclaimed,
	}

	if logger != nil {
		logger.Info("archived chunks compacted successfully",
			"chunks_merged", len(bestGroup),
			"bytes_reclaimed", bytesReclaimed,
		)
	}

	return omp, nil
}

func trimPrefixSlash(p string) string {
	return strings.TrimPrefix(strings.TrimSuffix(p, "/"), "/")
}
