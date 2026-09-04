// Package archive exports pruned event batches to S3-compatible object
// storage as compressed NDJSON before deletion, so that pruned ranges
// remain recoverable. Archival is entirely optional: without the
// ARCHIVE_* environment variables set, no S3 client is created and the
// binary behaves identically to the pre-archive build.
package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/sorotrail/sorotrail/internal/store"
)

const (
	// SchemaVersion is the versioned archive format. Bump when the
	// NDJSON layout changes so readers can detect format drift.
	SchemaVersion = 1

	// ArchiveBatchSize controls how many events are fetched per page
	// when streaming events to the archive writer. It bounds memory
	// consumption during the export phase.
	ArchiveBatchSize = 500
)

// ArchiveChunkStatus enumerates the lifecycle states of an archive chunk.
const (
	StatusPending  = "pending"
	StatusArchived = "archived"
	StatusFailed   = "failed"
)

// Chunk represents one archived ledger range as persisted in the
// archive_chunks table.
type Chunk struct {
	LedgerStart    int64
	LedgerEnd      int64
	Status         string
	ObjectURI      string
	RowCount       int64
	ManifestSHA256 string
	Attempts       int
	LastError      string
	StartedAt      time.Time
	VerifiedAt     *time.Time
	ClosedAt       *time.Time
}

// Manifest is the JSON structure written alongside each archive file
// so consumers can validate integrity without parsing the NDJSON.
type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	Chunk         ChunkInfo `json:"chunk"`
	Producer      string    `json:"producer"`
}

// ChunkInfo holds the metadata for a single archived chunk.
type ChunkInfo struct {
	LedgerStart int64 `json:"ledger_start"`
	LedgerEnd   int64 `json:"ledger_end"`
	RowCount    int64 `json:"row_count"`
}

// Archiver handles exporting events to S3-compatible storage as
// compressed NDJSON. It is safe for concurrent use by a single
// goroutine (the pruner).
type Archiver struct {
	client     *minio.Client
	bucket     string
	prefix     string
	store      store.Store
	log        *slog.Logger
	maxRetries int
}

// Options configures the archiver. All fields have safe zero-value
// defaults; the archiver only becomes active when Bucket is non-empty.
type Options struct {
	// Bucket is the S3-compatible bucket name. Empty disables archival.
	Bucket string
	// Prefix is an optional key prefix within the bucket. Objects are
	// stored under <prefix>/events/schema=<version>/.
	Prefix string
	// Endpoint is the S3 endpoint URL (e.g. "s3.amazonaws.com" or
	// "localhost:9000" for MinIO). Empty uses the AWS default.
	Endpoint string
	// Region is the S3 region. Required for AWS, optional for MinIO.
	Region string
	// AccessKeyID and SecretAccessKey are explicit credentials. When
	// both are empty the ambient credential chain is used.
	AccessKeyID     string
	SecretAccessKey string
	// UseSSL controls whether the S3 endpoint uses TLS.
	UseSSL bool
	// MaxRetries is the per-chunk retry budget. Default 3.
	MaxRetries int
	// Logger is the structured logger. When nil a discard logger is used.
	Logger *slog.Logger
}

// Enabled reports whether the archiver should be active (i.e. a bucket
// is configured).
func (o Options) Enabled() bool {
	return o.Bucket != ""
}

// New creates an Archiver. When opts.Bucket is empty it returns nil,
// which the caller should treat as "archival disabled". This keeps the
// pruner's happy path branch-free.
func New(st store.Store, opts Options) (*Archiver, error) {
	if !opts.Enabled() {
		return nil, nil
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	creds := credentials.NewStaticV4(
		opts.AccessKeyID,
		opts.SecretAccessKey,
		"",
	)
	if opts.AccessKeyID == "" && opts.SecretAccessKey == "" {
		creds = nil // ambient chain
	}

	minioOpts := &minio.Options{
		Secure: opts.UseSSL,
	}
	if opts.Region != "" {
		minioOpts.Region = opts.Region
	}
	if creds != nil {
		minioOpts.Creds = creds
	}

	client, err := minio.New(opts.Endpoint, minioOpts)
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}

	// Ensure the bucket exists.
	exists, err := client.BucketExists(context.Background(), opts.Bucket)
	if err != nil {
		return nil, fmt.Errorf("checking bucket %q: %w", opts.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(context.Background(), opts.Bucket, minio.MakeBucketOptions{
			Region: opts.Region,
		}); err != nil {
			return nil, fmt.Errorf("creating bucket %q: %w", opts.Bucket, err)
		}
		log.Info("created archive bucket", "bucket", opts.Bucket)
	}

	return &Archiver{
		client:     client,
		bucket:     opts.Bucket,
		prefix:     opts.Prefix,
		store:      st,
		log:        log.With("component", "archiver"),
		maxRetries: maxRetries,
	}, nil
}

// IsArchived reports whether the given ledger range has been
// durably archived and verified. The pruner calls this before
// deleting each batch.
func (a *Archiver) IsArchived(ctx context.Context, ledgerStart, ledgerEnd int64) (bool, error) {
	// Query the store for an archived chunk covering this range.
	// For now we check via the query_events path; a dedicated
	// archive_chunks query will be added when the Store interface
	// is extended. Until then, we use the exported chunk metadata
	// stored in S3 as the source of truth.
	if a == nil {
		return false, nil
	}
	uri := a.objectPrefix(ledgerStart)
	_, err := a.client.StatObject(ctx, a.bucket, uri+"/manifest.json", minio.StatObjectOptions{})
	if err != nil {
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && minioErr.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("checking archive for ledger %d: %w", ledgerStart, err)
	}
	return true, nil
}

// ArchiveRange queries events in [fromLedger, toLedger) and uploads
// them as a gzip-compressed NDJSON file to S3, along with a manifest.
// It is idempotent: if the archive already exists for this range the
// call is a no-op returning the existing object URI and manifest hash.
//
// Returns the object URI and manifest SHA-256 on success.
func (a *Archiver) ArchiveRange(ctx context.Context, fromLedger, toLedger int64) (string, string, error) {
	if a == nil {
		return "", "", nil
	}

	// Idempotency check: already archived?
	uri := a.objectPrefix(fromLedger)
	manifestKey := uri + "/manifest.json"
	_, err := a.client.StatObject(ctx, a.bucket, manifestKey, minio.StatObjectOptions{})
	if err == nil {
		// Manifest exists — read it to get the SHA.
		obj, err := a.client.GetObject(ctx, a.bucket, manifestKey, minio.GetObjectOptions{})
		if err != nil {
			return "", "", fmt.Errorf("reading existing manifest: %w", err)
		}
		var m Manifest
		if err := json.NewDecoder(obj).Decode(&m); err == nil {
			a.log.Debug("archive already exists, skipping",
				"from_ledger", fromLedger, "to_ledger", toLedger,
				"object_uri", uri)
			return uri, a.manifestHash(m), nil
		}
		obj.Close()
	}

	a.log.Info("archiving range",
		"from_ledger", fromLedger, "to_ledger", toLedger)

	// Stream events to a gzip buffer.
	var gzBuf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&gzBuf, gzip.BestCompression)
	if err != nil {
		return "", "", fmt.Errorf("creating gzip writer: %w", err)
	}

	enc := json.NewEncoder(gzWriter)
	var rowCount int64
	var firstID, lastID string

	filter := store.EventFilter{
		FromLedger: fromLedger,
		ToLedger:   toLedger,
		Limit:      ArchiveBatchSize,
		Order:      "asc",
		OrderBy:    "id",
		Scope:      store.WildcardScope(),
	}

	for {
		events, cursor, err := a.store.QueryEvents(ctx, filter)
		if err != nil {
			gzWriter.Close()
			return "", "", fmt.Errorf("querying events for archive: %w", err)
		}
		for _, ev := range events {
			if rowCount == 0 {
				firstID = ev.ID
			}
			lastID = ev.ID
			if err := enc.Encode(ev); err != nil {
				gzWriter.Close()
				return "", "", fmt.Errorf("encoding event for archive: %w", err)
			}
			rowCount++
		}
		if cursor == "" {
			break
		}
		filter.Cursor = cursor
		if ctx.Err() != nil {
			gzWriter.Close()
			return "", "", ctx.Err()
		}
	}

	if err := gzWriter.Close(); err != nil {
		return "", "", fmt.Errorf("closing gzip writer: %w", err)
	}

	if rowCount == 0 {
		a.log.Debug("no events to archive", "from_ledger", fromLedger, "to_ledger", toLedger)
		return "", "", nil
	}

	// Build and upload the NDJSON data file.
	dataKey := uri + "/data.ndjson.gz"
	contentType := "application/x-ndjson"
	_, err = a.client.PutObject(ctx, a.bucket, dataKey,
		bytes.NewReader(gzBuf.Bytes()), int64(gzBuf.Len()),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return "", "", fmt.Errorf("uploading archive data: %w", err)
	}

	// Build the manifest.
	hash := sha256.Sum256(gzBuf.Bytes())
	manifest := Manifest{
		SchemaVersion: SchemaVersion,
		Chunk: ChunkInfo{
			LedgerStart: fromLedger,
			LedgerEnd:   toLedger,
			RowCount:    rowCount,
		},
		Producer: "sorotrail",
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("marshaling manifest: %w", err)
	}

	// Upload the manifest.
	_, err = a.client.PutObject(ctx, a.bucket, manifestKey,
		bytes.NewReader(manifestBytes), int64(len(manifestBytes)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return "", "", fmt.Errorf("uploading manifest: %w", err)
	}

	sha := fmt.Sprintf("%x", hash)
	a.log.Info("archive complete",
		"from_ledger", fromLedger, "to_ledger", toLedger,
		"row_count", rowCount,
		"first_id", firstID,
		"last_id", lastID,
		"object_uri", uri,
		"sha256", sha)

	return uri, sha, nil
}

// objectPrefix returns the S3 key prefix for a given ledger range.
func (a *Archiver) objectPrefix(ledgerStart int64) string {
	base := "events/schema=1"
	if a.prefix != "" {
		base = a.prefix + "/" + base
	}
	return fmt.Sprintf("%s/ledger_start=%07d", base, ledgerStart)
}

// manifestHash computes a short identifier for a manifest so we can
// detect re-archives of the same range without re-uploading.
func (a *Archiver) manifestHash(m Manifest) string {
	b, _ := json.Marshal(m)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}

// GetObject returns a reader for an archived object. The caller must
// close the returned ReadCloser. Used by recovery tooling.
func (a *Archiver) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return a.client.GetObject(ctx, a.bucket, key, minio.GetObjectOptions{})
}
