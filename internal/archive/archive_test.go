package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

type archiveStore struct {
	store.Store
	pages      [][]store.Event
	queryErr   error
	queryCalls int
	filters    []store.EventFilter
}

func (s *archiveStore) QueryEvents(_ context.Context, filter store.EventFilter) ([]store.Event, string, error) {
	s.queryCalls++
	s.filters = append(s.filters, filter)
	if s.queryErr != nil {
		return nil, "", s.queryErr
	}
	page := s.queryCalls - 1
	if page >= len(s.pages) {
		return nil, "", nil
	}
	cursor := ""
	if page+1 < len(s.pages) {
		cursor = fmt.Sprintf("page-%d", page+1)
	}
	return s.pages[page], cursor, nil
}

type archiveObjectClient struct {
	objects      map[string][]byte
	putKeys      []string
	failPutAt    int
	statErr      error
	statErrByKey map[string]error
	getErr       error
}

func newArchiveObjectClient() *archiveObjectClient {
	return &archiveObjectClient{objects: make(map[string][]byte)}
}

func (c *archiveObjectClient) StatObject(_ context.Context, _, key string, _ minio.StatObjectOptions) (minio.ObjectInfo, error) {
	if err := c.statErrByKey[key]; err != nil {
		return minio.ObjectInfo{}, err
	}
	if c.statErr != nil {
		return minio.ObjectInfo{}, c.statErr
	}
	if _, ok := c.objects[key]; !ok {
		return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey", Key: key, StatusCode: 404}
	}
	return minio.ObjectInfo{Key: key}, nil
}

func (c *archiveObjectClient) GetObject(_ context.Context, _, key string, _ minio.GetObjectOptions) (io.ReadCloser, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	body, ok := c.objects[key]
	if !ok {
		return nil, minio.ErrorResponse{Code: "NoSuchKey", Key: key, StatusCode: 404}
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (c *archiveObjectClient) PutObject(_ context.Context, _, key string, body io.Reader, _ int64, _ minio.PutObjectOptions) (minio.UploadInfo, error) {
	c.putKeys = append(c.putKeys, key)
	if c.failPutAt > 0 && len(c.putKeys) == c.failPutAt {
		return minio.UploadInfo{}, errors.New("simulated object write failure")
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	c.objects[key] = data
	return minio.UploadInfo{Key: key, Size: int64(len(data))}, nil
}

func newTestArchiver(st store.Store, client objectClient) *Archiver {
	return &Archiver{
		client: client,
		bucket: "archive-test",
		prefix: "test-prefix",
		store:  st,
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestOptionsDisabledByDefault(t *testing.T) {
	opts := Options{}
	assert.False(t, opts.Enabled(), "archive should be disabled when bucket is empty")
}

func TestOptionsEnabled(t *testing.T) {
	opts := Options{Bucket: "my-bucket"}
	assert.True(t, opts.Enabled(), "archive should be enabled when bucket is set")
}

func TestNewReturnsNilWhenDisabled(t *testing.T) {
	var st store.Store // nil interface
	a, err := New(st, Options{})
	assert.NoError(t, err)
	assert.Nil(t, a, "archiver should be nil when disabled")
}

func TestArchiverIsNilSafe(t *testing.T) {
	var a *Archiver
	// All methods should be safe to call on a nil archiver.
	archived, err := a.IsArchived(context.Background(), 100, 200)
	assert.NoError(t, err)
	assert.False(t, archived)

	uri, sha, err := a.ArchiveRange(context.Background(), 100, 200)
	assert.NoError(t, err)
	assert.Empty(t, uri)
	assert.Empty(t, sha)
	reader, err := a.GetObject(context.Background(), "key")
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestObjectPrefix(t *testing.T) {
	a := &Archiver{prefix: ""}
	assert.Equal(t, "events/schema=1/ledger_start=0240000", a.objectPrefix(240000))
	assert.Equal(t, "events/schema=1/ledger_start=9999999", a.objectPrefix(9999999))
	assert.Equal(t, "events/schema=1/ledger_start=10000000", a.objectPrefix(10000000))
	assert.Equal(t, "events/schema=1/ledger_start=-000001", a.objectPrefix(-1))

	a2 := &Archiver{prefix: "my-prefix"}
	assert.Equal(t, "my-prefix/events/schema=1/ledger_start=0240000", a2.objectPrefix(240000))
}

func TestManifestHash(t *testing.T) {
	a := &Archiver{log: slog.Default()}
	m1 := Manifest{
		SchemaVersion: 1,
		Chunk: ChunkInfo{
			LedgerStart: 100,
			LedgerEnd:   200,
			RowCount:    50,
		},
		Producer: "sorotrail",
	}
	m2 := Manifest{
		SchemaVersion: 1,
		Chunk: ChunkInfo{
			LedgerStart: 100,
			LedgerEnd:   200,
			RowCount:    50,
		},
		Producer: "sorotrail",
	}
	// Same manifest should produce same hash.
	assert.Equal(t, a.manifestHash(m1), a.manifestHash(m2))

	tests := []struct {
		name   string
		change func(*Manifest)
	}{
		{name: "schema version", change: func(m *Manifest) { m.SchemaVersion++ }},
		{name: "ledger start", change: func(m *Manifest) { m.Chunk.LedgerStart++ }},
		{name: "ledger end", change: func(m *Manifest) { m.Chunk.LedgerEnd++ }},
		{name: "row count", change: func(m *Manifest) { m.Chunk.RowCount++ }},
		{name: "producer", change: func(m *Manifest) { m.Producer += "-other" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := m1
			tt.change(&changed)
			assert.NotEqual(t, a.manifestHash(m1), a.manifestHash(changed))
		})
	}
}

func TestSchemaVersionConstant(t *testing.T) {
	assert.Equal(t, 1, SchemaVersion)
}

func TestArchiveRangeUploadsReadableNDJSONAndManifest(t *testing.T) {
	events := make([]store.Event, ArchiveBatchSize+1)
	for i := range events {
		events[i] = store.Event{ID: fmt.Sprintf("event-%03d", i), Ledger: int64(100 + i)}
	}
	st := &archiveStore{pages: [][]store.Event{events[:ArchiveBatchSize], events[ArchiveBatchSize:]}}
	client := newArchiveObjectClient()
	a := newTestArchiver(st, client)

	uri, hash, err := a.ArchiveRange(context.Background(), 100, 700)
	require.NoError(t, err)
	require.Equal(t, "test-prefix/events/schema=1/ledger_start=0000100", uri)
	require.NotEmpty(t, hash)
	assert.Equal(t, 2, st.queryCalls, "events should be fetched in bounded pages")
	assert.Equal(t, ArchiveBatchSize, st.filters[0].Limit)
	assert.Equal(t, "asc", st.filters[0].Order)
	assert.Equal(t, "id", st.filters[0].OrderBy)
	assert.Empty(t, st.filters[0].Cursor)
	assert.Equal(t, "page-1", st.filters[1].Cursor)

	dataKey := uri + "/data.ndjson.gz"
	manifestKey := uri + "/manifest.json"
	require.Equal(t, []string{dataKey, manifestKey}, client.putKeys)

	gz, err := gzip.NewReader(bytes.NewReader(client.objects[dataKey]))
	require.NoError(t, err)
	scanner := bufio.NewScanner(gz)
	var decoded []store.Event
	for scanner.Scan() {
		var event store.Event
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &event))
		decoded = append(decoded, event)
	}
	require.NoError(t, scanner.Err())
	require.NoError(t, gz.Close())
	require.Len(t, decoded, len(events))
	assert.Equal(t, events[0].ID, decoded[0].ID)
	assert.Equal(t, events[len(events)-1].ID, decoded[len(decoded)-1].ID)

	var manifest Manifest
	require.NoError(t, json.Unmarshal(client.objects[manifestKey], &manifest))
	assert.Equal(t, SchemaVersion, manifest.SchemaVersion)
	assert.Equal(t, ChunkInfo{LedgerStart: 100, LedgerEnd: 700, RowCount: int64(len(events))}, manifest.Chunk)
	assert.Equal(t, "sorotrail", manifest.Producer)
	expectedDataHash := sha256.Sum256(client.objects[dataKey])
	assert.Equal(t, fmt.Sprintf("%x", expectedDataHash), hash)

	repeatedURI, repeatedHash, err := a.ArchiveRange(context.Background(), 100, 700)
	require.NoError(t, err)
	assert.Equal(t, uri, repeatedURI)
	assert.Equal(t, hash, repeatedHash, "idempotent exports must return the same hash")
	assert.Equal(t, 2, len(client.putKeys), "the idempotent call must not upload again")
}

func TestArchiveRangeEmptyAndQueryFailure(t *testing.T) {
	t.Run("empty range writes no objects", func(t *testing.T) {
		client := newArchiveObjectClient()
		a := newTestArchiver(&archiveStore{pages: [][]store.Event{{}}}, client)
		uri, hash, err := a.ArchiveRange(context.Background(), 10, 20)
		require.NoError(t, err)
		assert.Empty(t, uri)
		assert.Empty(t, hash)
		assert.Empty(t, client.putKeys)
	})

	t.Run("query failure prevents writes", func(t *testing.T) {
		client := newArchiveObjectClient()
		a := newTestArchiver(&archiveStore{queryErr: errors.New("store unavailable")}, client)
		_, _, err := a.ArchiveRange(context.Background(), 10, 20)
		require.ErrorContains(t, err, "querying events for archive")
		assert.Empty(t, client.putKeys)
	})
}

func TestArchiveRangeUploadFailuresAreErrors(t *testing.T) {
	tests := []struct {
		name        string
		failPutAt   int
		wantObjects []string
		wantErr     string
	}{
		{name: "data upload rejected", failPutAt: 1, wantObjects: []string{}, wantErr: "uploading archive data"},
		{
			name:        "manifest upload rejected after data",
			failPutAt:   2,
			wantObjects: []string{"test-prefix/events/schema=1/ledger_start=0000010/data.ndjson.gz"},
			wantErr:     "uploading manifest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newArchiveObjectClient()
			client.failPutAt = tt.failPutAt
			a := newTestArchiver(&archiveStore{pages: [][]store.Event{{{ID: "one", Ledger: 10}}}}, client)
			uri, hash, err := a.ArchiveRange(context.Background(), 10, 20)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Empty(t, uri)
			assert.Empty(t, hash)
			assert.Equal(t, tt.wantObjects, sortedObjectKeys(client.objects))
			if tt.failPutAt == 2 {
				assert.NotContains(t, client.objects, "test-prefix/events/schema=1/ledger_start=0000010/manifest.json",
					"failed manifest upload must not make the partial data appear archived")
			}
		})
	}
}

func TestArchiveRangeRejectsStatTransportErrorAndMismatchedManifest(t *testing.T) {
	t.Run("stat transport failure", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.statErr = errors.New("S3 unavailable")
		a := newTestArchiver(&archiveStore{pages: [][]store.Event{{{ID: "one"}}}}, client)
		_, _, err := a.ArchiveRange(context.Background(), 10, 20)
		require.ErrorContains(t, err, "checking existing archive manifest")
		assert.Empty(t, client.putKeys)
	})

	t.Run("existing manifest for different range", func(t *testing.T) {
		client := newArchiveObjectClient()
		manifest, err := json.Marshal(Manifest{SchemaVersion: SchemaVersion, Chunk: ChunkInfo{LedgerStart: 10, LedgerEnd: 30}})
		require.NoError(t, err)
		client.objects["test-prefix/events/schema=1/ledger_start=0000010/manifest.json"] = manifest
		a := newTestArchiver(&archiveStore{pages: [][]store.Event{{{ID: "one"}}}}, client)
		_, _, err = a.ArchiveRange(context.Background(), 10, 20)
		require.ErrorContains(t, err, "does not match requested range")
		assert.Empty(t, client.putKeys)
	})
}

func TestIsArchivedRequiresManifestCoverageAndData(t *testing.T) {
	const uri = "test-prefix/events/schema=1/ledger_start=0000100"
	manifestBytes, err := json.Marshal(Manifest{
		SchemaVersion: SchemaVersion,
		Chunk:         ChunkInfo{LedgerStart: 100, LedgerEnd: 200, RowCount: 1},
	})
	require.NoError(t, err)

	t.Run("covering archive", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.objects[uri+"/manifest.json"] = manifestBytes
		client.objects[uri+"/data.ndjson.gz"] = []byte("archive data")
		a := newTestArchiver(nil, client)
		archived, err := a.IsArchived(context.Background(), 100, 200)
		require.NoError(t, err)
		assert.True(t, archived)
	})

	t.Run("manifest not found", func(t *testing.T) {
		a := newTestArchiver(nil, newArchiveObjectClient())
		archived, err := a.IsArchived(context.Background(), 100, 200)
		require.NoError(t, err)
		assert.False(t, archived)
	})

	t.Run("partial overlap is not covered", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.objects[uri+"/manifest.json"] = manifestBytes
		client.objects[uri+"/data.ndjson.gz"] = []byte("archive data")
		a := newTestArchiver(nil, client)
		archived, err := a.IsArchived(context.Background(), 100, 250)
		require.NoError(t, err)
		assert.False(t, archived)
	})

	t.Run("manifest without data is not archived", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.objects[uri+"/manifest.json"] = manifestBytes
		a := newTestArchiver(nil, client)
		archived, err := a.IsArchived(context.Background(), 100, 200)
		require.NoError(t, err)
		assert.False(t, archived)
	})

	t.Run("manifest read failure is an error", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.objects[uri+"/manifest.json"] = manifestBytes
		client.getErr = errors.New("S3 read failed")
		a := newTestArchiver(nil, client)
		archived, err := a.IsArchived(context.Background(), 100, 200)
		require.ErrorContains(t, err, "reading archive manifest")
		assert.False(t, archived)
	})

	t.Run("incompatible manifest is an error", func(t *testing.T) {
		client := newArchiveObjectClient()
		unsupported, err := json.Marshal(Manifest{
			SchemaVersion: SchemaVersion + 1,
			Chunk:         ChunkInfo{LedgerStart: 100, LedgerEnd: 200},
		})
		require.NoError(t, err)
		client.objects[uri+"/manifest.json"] = unsupported
		a := newTestArchiver(nil, client)
		archived, err := a.IsArchived(context.Background(), 100, 200)
		require.ErrorContains(t, err, "unsupported archive schema version")
		assert.False(t, archived)
	})

	t.Run("data stat transport failure is an error", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.objects[uri+"/manifest.json"] = manifestBytes
		client.statErrByKey = map[string]error{uri + "/data.ndjson.gz": errors.New("S3 unavailable")}
		a := newTestArchiver(nil, client)
		archived, err := a.IsArchived(context.Background(), 100, 200)
		require.ErrorContains(t, err, "checking archive data")
		assert.False(t, archived)
	})
}

func TestGetObject(t *testing.T) {
	const key = "events/schema=1/ledger_start=0000010/data.ndjson.gz"
	t.Run("object found", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.objects[key] = []byte("stored archive")
		a := newTestArchiver(nil, client)
		reader, err := a.GetObject(context.Background(), key)
		require.NoError(t, err)
		defer reader.Close()
		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "stored archive", string(body))
	})

	t.Run("object missing", func(t *testing.T) {
		a := newTestArchiver(nil, newArchiveObjectClient())
		_, err := a.GetObject(context.Background(), key)
		require.Error(t, err)
		var minioErr minio.ErrorResponse
		assert.ErrorAs(t, err, &minioErr)
		assert.Equal(t, 404, minioErr.StatusCode)
	})

	t.Run("transport failure", func(t *testing.T) {
		client := newArchiveObjectClient()
		client.getErr = errors.New("S3 unavailable")
		a := newTestArchiver(nil, client)
		_, err := a.GetObject(context.Background(), key)
		require.ErrorIs(t, err, client.getErr)
	})
}

func sortedObjectKeys(objects map[string][]byte) []string {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type stubArchiveLagController struct {
	yield  bool
	reason string
}

func (s *stubArchiveLagController) ShouldYield(string) (bool, string) {
	return s.yield, s.reason
}

func TestArchiveRangeLagBackpressureAndMaxDeferral(t *testing.T) {
	st := &archiveStore{
		pages: [][]store.Event{
			{{ID: "ev-1", Ledger: 10, ContractID: "C1"}},
		},
	}
	client := newArchiveObjectClient()
	lagCtrl := &stubArchiveLagController{yield: true, reason: "ingest_lagging"}
	a := newTestArchiver(st, client).WithLagController(lagCtrl)

	uri, sha, err := a.ArchiveRange(context.Background(), 0, 50)
	require.ErrorIs(t, err, ErrDeferredToLag)
	assert.Empty(t, uri)
	assert.Empty(t, sha)
	assert.Equal(t, uint64(1), a.Metrics().LagYields)
	assert.Empty(t, client.objects)

	// Once max deferral elapses, ArchiveRange proceeds and records the override.
	lagCtrl.yield = false
	lagCtrl.reason = "max_deferral_reached"
	uri, sha, err = a.ArchiveRange(context.Background(), 0, 50)
	require.NoError(t, err)
	assert.NotEmpty(t, uri)
	assert.NotEmpty(t, sha)
	assert.Equal(t, uint64(1), a.Metrics().MaxDeferralOverrides)
}
