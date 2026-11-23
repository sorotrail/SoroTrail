package decode

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingDecoder records how often each payload is decoded and can fail
// specific payloads on demand.
type countingDecoder struct {
	mu      sync.Mutex
	calls   map[string]int
	failFor map[string]error
}

func newCountingDecoder() *countingDecoder {
	return &countingDecoder{calls: map[string]int{}, failFor: map[string]error{}}
}

func (d *countingDecoder) DecodeScVal(xdr string) (json.RawMessage, error) {
	d.mu.Lock()
	d.calls[xdr]++
	failErr := d.failFor[xdr]
	d.mu.Unlock()
	if failErr != nil {
		return nil, failErr
	}
	return json.RawMessage(fmt.Sprintf(`{"v":%q}`, xdr)), nil
}

func (d *countingDecoder) count(xdr string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls[xdr]
}

func TestCachingDecoder_RepeatedDecodeHitsCache(t *testing.T) {
	inner := newCountingDecoder()
	c := NewCachingDecoder(inner, 0)

	first, err := c.DecodeScVal("payload-a")
	require.NoError(t, err)
	second, err := c.DecodeScVal("payload-a")
	require.NoError(t, err)

	assert.JSONEq(t, string(first), string(second))
	assert.Equal(t, 1, inner.count("payload-a"), "the inner decoder must run only once")
	hits, misses := c.Stats()
	assert.Equal(t, uint64(1), hits)
	assert.Equal(t, uint64(1), misses)
}

func TestCachingDecoder_DistinctPayloadsMissIndependently(t *testing.T) {
	inner := newCountingDecoder()
	c := NewCachingDecoder(inner, 0)

	for _, p := range []string{"a", "b", "c", "a", "b"} {
		_, err := c.DecodeScVal(p)
		require.NoError(t, err)
	}

	assert.Equal(t, 1, inner.count("a"))
	assert.Equal(t, 1, inner.count("b"))
	assert.Equal(t, 1, inner.count("c"))
	hits, misses := c.Stats()
	assert.Equal(t, uint64(2), hits, "the two repeats are hits")
	assert.Equal(t, uint64(3), misses, "the three distinct payloads are misses")
}

func TestCachingDecoder_ErrorsAreNotCached(t *testing.T) {
	inner := newCountingDecoder()
	inner.failFor["bad"] = errors.New("nope")
	c := NewCachingDecoder(inner, 0)

	for i := 0; i < 2; i++ {
		_, err := c.DecodeScVal("bad")
		require.Error(t, err)
	}

	assert.Equal(t, 2, inner.count("bad"), "a failed decode must be retried, not memoized")
	hits, misses := c.Stats()
	assert.Equal(t, uint64(0), hits)
	assert.Equal(t, uint64(2), misses)
}

func TestCachingDecoder_NilInnerIsAnError(t *testing.T) {
	c := NewCachingDecoder(nil, 0)
	_, err := c.DecodeScVal("anything")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no inner decoder")
}

func TestCachingDecoder_EvictsLeastRecentlyUsed(t *testing.T) {
	inner := newCountingDecoder()
	c := NewCachingDecoder(inner, 3)

	for _, p := range []string{"a", "b", "c"} {
		_, err := c.DecodeScVal(p)
		require.NoError(t, err)
	}
	// Touch "a" so "b" becomes the least recently used entry.
	_, err := c.DecodeScVal("a")
	require.NoError(t, err)
	// Inserting "d" evicts "b" and nothing else.
	_, err = c.DecodeScVal("d")
	require.NoError(t, err)
	require.Equal(t, 3, c.Len())

	// "b" was evicted while "a", "c" and "d" survive.
	for _, p := range []string{"a", "c", "d"} {
		_, err := c.DecodeScVal(p)
		require.NoError(t, err)
		assert.Equal(t, 1, inner.count(p), "entry %q should still be cached", p)
	}
	_, err = c.DecodeScVal("b")
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("b"), "the evicted entry must be decoded again")
}

func TestCachingDecoder_DefaultCapacity(t *testing.T) {
	tests := []struct {
		name       string
		maxEntries int
		want       int
	}{
		{name: "zero selects the default", maxEntries: 0, want: DefaultCacheSize},
		{name: "negative selects the default", maxEntries: -5, want: DefaultCacheSize},
		{name: "positive is honored", maxEntries: 7, want: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCachingDecoder(newCountingDecoder(), tt.maxEntries)
			assert.Equal(t, tt.want, c.maxEntries)
		})
	}
}

func TestCachingDecoder_ReturnedBytesAreCopies(t *testing.T) {
	inner := newCountingDecoder()
	c := NewCachingDecoder(inner, 0)

	first, err := c.DecodeScVal("payload")
	require.NoError(t, err)
	original := string(first)

	// Corrupting the returned slice must not affect later reads.
	first[0] = 'X'

	second, err := c.DecodeScVal("payload")
	require.NoError(t, err)
	assert.JSONEq(t, original, string(second))
	assert.Equal(t, 1, inner.count("payload"))
}

func TestCachingDecoder_ConcurrentDecodesAreSafe(t *testing.T) {
	inner := newCountingDecoder()
	c := NewCachingDecoder(inner, 8)

	const (
		distinct = 4
		calls    = 64
	)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := c.DecodeScVal(fmt.Sprintf("payload-%d", i%distinct)); err != nil {
				t.Errorf("decode: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Two goroutines can race a cache miss on the same key and both decode it
	// before either inserts, so the guarantee is "decoded at least once", not
	// "exactly once". What must hold is that the cache absorbs almost all of
	// the concurrent load and that every call is accounted for.
	total := 0
	for i := 0; i < distinct; i++ {
		n := inner.count(fmt.Sprintf("payload-%d", i))
		assert.GreaterOrEqual(t, n, 1, "payload %d must be decoded at least once", i)
		total += n
	}
	assert.Less(t, total, calls, "the cache must serve most concurrent reads")

	hits, misses := c.Stats()
	assert.Equal(t, uint64(calls), hits+misses, "every call is a hit or a miss")
	assert.Greater(t, hits, uint64(0))
}

// TestCachingDecoder_WrapsXDRDecoder proves the cache is wire-compatible with
// the production decoder: identical XDR round-trips to identical JSON, with
// the second read served from the cache.
func TestCachingDecoder_WrapsXDRDecoder(t *testing.T) {
	c := NewCachingDecoder(XDRDecoder{}, 0)
	encoded := mustBase64(t, scSymbol("transfer"))

	first, err := c.DecodeScVal(encoded)
	require.NoError(t, err)
	second, err := c.DecodeScVal(encoded)
	require.NoError(t, err)

	assert.JSONEq(t, `{"symbol":"transfer"}`, string(first))
	assert.JSONEq(t, string(first), string(second))
	hits, misses := c.Stats()
	assert.Equal(t, uint64(1), hits)
	assert.Equal(t, uint64(1), misses)
}

func TestCachingDecoder_EdgeCases(t *testing.T) {
	t.Run("empty payload is cached like any other key", func(t *testing.T) {
		inner := newCountingDecoder()
		c := NewCachingDecoder(inner, 0)

		first, err := c.DecodeScVal("")
		require.NoError(t, err)
		second, err := c.DecodeScVal("")
		require.NoError(t, err)

		assert.JSONEq(t, string(first), string(second))
		assert.Equal(t, 1, inner.count(""))
		hits, misses := c.Stats()
		assert.Equal(t, uint64(1), hits)
		assert.Equal(t, uint64(1), misses)
	})

	t.Run("maxEntries of one evicts on every new key", func(t *testing.T) {
		inner := newCountingDecoder()
		c := NewCachingDecoder(inner, 1)

		for _, p := range []string{"a", "b", "a"} {
			_, err := c.DecodeScVal(p)
			require.NoError(t, err)
		}

		assert.Equal(t, 1, c.Len())
		assert.Equal(t, 2, inner.count("a"), "a must be re-decoded after b evicted it")
		assert.Equal(t, 1, inner.count("b"))
	})

	t.Run("maxEntries of one serves repeated hits for the same key", func(t *testing.T) {
		inner := newCountingDecoder()
		c := NewCachingDecoder(inner, 1)

		for i := 0; i < 3; i++ {
			_, err := c.DecodeScVal("same")
			require.NoError(t, err)
		}

		assert.Equal(t, 1, inner.count("same"))
		hits, misses := c.Stats()
		assert.Equal(t, uint64(2), hits)
		assert.Equal(t, uint64(1), misses)
	})
}
