package plugins

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/tetratelabs/wazero/api"
)

// buffer is a host-side scratch region carved out of module linear
// memory; byte offset of the JSON body.
type buffer struct {
	ptr  uint32
	size int
}

func millisToDuration(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// allocOutput ensures the module has at least (size+4) bytes of linear
// memory and returns (outputOffset, lengthOffset). The length u32 lives
// right after the JSON buffer so a plugin writes
//
//	[outPtr..outPtr+len          : JSON object]
//	[outPtr+outCap..outPtr+outCap+4: u32 LE byte count]
//
// On warm calls where memory is already large enough, no Grow happens.
func allocOutput(mod api.Module, size int) (buffer, uint32, error) {
	if size <= 0 {
		return buffer{}, 0, errors.New("output cap must be positive")
	}
	mem := mod.Memory()
	const pageSize = 65536
	totalBytes := uint64(size) + 4
	cur := uint64(mem.Size())
	if cur < totalBytes {
		extra := totalBytes - cur
		// mem.Grow takes a uint32 page delta. With outCap ≤ 64KiB and
		// max input 256KiB, we never exceed ~5 pages; safe in v1.
		pages := uint32((extra + pageSize - 1) / pageSize)
		if _, ok := mem.Grow(pages); !ok {
			return buffer{}, 0, fmt.Errorf("grow memory %d pages failed", pages)
		}
	}
	ptr := uint32(cur)
	lenPtr := ptr + uint32(size)
	return buffer{ptr: ptr, size: size}, lenPtr, nil
}

// callWithDeadline runs fn with a per-call timeout. The WithCloseOnContextDone
// flag on the module config means context cancellation aborts the call
// from another goroutine; we also pass the deadline into wazero.
// Returns the raw function result registers so callers can inspect the
// plugin-decoded status code directly instead of writing it via memory.
func callWithDeadline(ctx context.Context, fn api.Function, deadlineMs int64, args ...uint64) ([]uint64, error) {
	if deadlineMs <= 0 {
		return fn.Call(ctx, args...)
	}
	dctx, cancel := context.WithTimeout(ctx, millisToDuration(deadlineMs))
	defer cancel()
	res, err := fn.Call(dctx, args...)
	if errors.Is(err, context.DeadlineExceeded) {
		return res, errDeadline
	}
	return res, err
}

// readU32 reads 4 bytes little-endian.
func readU32(mod api.Module, ptr uint32) (uint32, error) {
	b, ok := mod.Memory().Read(ptr, 4)
	if !ok {
		return 0, errors.New("u32 out of range")
	}
	return binary.LittleEndian.Uint32(b), nil
}

// errDeadline is what callWithDeadline returns when the per-call budget
// is exceeded. Manager compares with errors.Is for disable accounting.
var errDeadline = errors.New("plugin call exceeded time budget")
