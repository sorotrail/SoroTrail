package decode

import (
	"os"
	"strings"
	"testing"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

func addCorpusFile(f *testing.F, path string) {
	f.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		f.Fatalf("read fuzz corpus %q: %v", path, err)
	}
	for _, seed := range strings.Split(string(data), "\n") {
		seed = strings.TrimSpace(seed)
		if seed != "" {
			f.Add(seed)
		}
	}
}

// FuzzDecodeScVal verifies that malformed or hostile base64/XDR input is
// handled by the decoder without panicking. A decode failure is an expected
// result for arbitrary fuzz input and is intentionally not treated as a test
// failure.
func FuzzDecodeScVal(f *testing.F) {
	addCorpusFile(f, "testdata/scvals.txt")

	f.Fuzz(func(t *testing.T, input string) {
		_, _ = (XDRDecoder{}).DecodeScVal(input)
	})
}

// FuzzDecodeTopicArray verifies that arbitrary event topic input cannot crash
// the local XDR decoding path. Native Go fuzzing supports strings but not
// []string, so NUL-separated strings are converted into topic arrays here.
func FuzzDecodeTopicArray(f *testing.F) {
	addCorpusFile(f, "testdata/topic_arrays.txt")

	f.Fuzz(func(t *testing.T, input string) {
		topics := strings.Split(input, "\x00")
		_, _, _ = EventTopicsValue(XDRDecoder{}, rpc.Event{Topic: topics})
	})
}
