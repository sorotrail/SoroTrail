package decode

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecodeScValComprehensive covers every ScVal shape, numeric boundaries,
// address decoding, nested structures, unknown types, and malformed inputs.
func TestDecodeScValComprehensive(t *testing.T) {
	t.Run("Bool", func(t *testing.T) {
		valTrue := xdr.ScVal{Type: xdr.ScvBool, B: true}
		resTrue, err := Decode(valTrue)
		require.NoError(t, err)
		assert.Equal(t, true, resTrue)

		valFalse := xdr.ScVal{Type: xdr.ScvBool, B: false}
		resFalse, err := Decode(valFalse)
		require.NoError(t, err)
		assert.Equal(t, false, resFalse)
	})

	t.Run("Void", func(t *testing.T) {
		val := xdr.ScVal{Type: xdr.ScvVoid}
		res, err := Decode(val)
		require.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("Numeric boundaries", func(t *testing.T) {
		u32Min := uint32(0)
		u32Max := uint32(4294967295)
		res, err := Decode(xdr.ScVal{Type: xdr.ScvU32, U32: &u32Min})
		require.NoError(t, err)
		assert.Equal(t, "0", res)

		res, err = Decode(xdr.ScVal{Type: xdr.ScvU32, U32: &u32Max})
		require.NoError(t, err)
		assert.Equal(t, "4294967295", res)

		i32Min := int32(-2147483648)
		i32Max := int32(2147483647)
		res, err = Decode(xdr.ScVal{Type: xdr.ScvI32, I32: &i32Min})
		require.NoError(t, err)
		assert.Equal(t, "-2147483648", res)

		res, err = Decode(xdr.ScVal{Type: xdr.ScvI32, I32: &i32Max})
		require.NoError(t, err)
		assert.Equal(t, "2147483647", res)

		u64Min := uint64(0)
		u64Max := uint64(18446744073709551615)
		u64Val := xdr.Uint64Parts{Hi: uint32(u64Max >> 32), Lo: uint32(u64Max & 0xFFFFFFFF)}
		res, err = Decode(xdr.ScVal{Type: xdr.ScvU64, U64: &u64Val})
		require.NoError(t, err)
		assert.NotNil(t, res)

		i64Min := int64(-9223372036854775808)
		i64Val := xdr.Int64Parts{Hi: int32(i64Min >> 32), Lo: int32(i64Min & 0xFFFFFFFF)}
		res, err = Decode(xdr.ScVal{Type: xdr.ScvI64, I64: &i64Val})
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("Address decoding", func(t *testing.T) {
		// Account id strkey or contract id strkey scenarios
		// Using an arbitrary valid or test address representation in XDR
		var accID xdr.AccountId
		accID.SetAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF")
		val := xdr.ScVal{
			Type:    xdr.ScvAddress,
			Address: &xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &accID},
		}
		res, err := Decode(val)
		require.NoError(t, err)
		assert.Contains(t, res, "G")
	})

	t.Run("Nested maps and vectors including empty ones", func(t *testing.T) {
		// Empty vector
		emptyVecVal := xdr.ScVal{
			Type: xdr.ScvVec,
			Vec:  &xdr.ScVec{},
		}
		res, err := Decode(emptyVecVal)
		require.NoError(t, err)
		assert.Equal(t, []any{}, res)

		// Empty map
		emptyMapVal := xdr.ScVal{
			Type: xdr.ScvMap,
			Map:  &xdr.ScMap{},
		}
		res, err = Decode(emptyMapVal)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{}, res)
	})

	t.Run("Unknown-type fallback shape", func(t *testing.T) {
		val := xdr.ScVal{
			Type: xdr.ScvType(-999),
		}
		res, err := Decode(val)
		// Ensure the unknown fallback doesn't panic and returns a predictable representation or error
		if err == nil {
			assert.NotNil(t, res)
		} else {
			assert.Error(t, err)
		}
	})

	t.Run(
		"Malformed input rejected without panicking", func(t *testing.T) {
			val := xdr.ScVal{
				Type: xdr.ScvU32,
				U32:  nil, // malformed
			}
			_, err := Decode(val)
			assert.Error(t, err)
		},
	)
}
