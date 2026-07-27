package decode

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
)

func TestEventTopicsValue_JSONPassthrough(t *testing.T) {
	e := rpc.Event{
		TopicJSON: []json.RawMessage{
			json.RawMessage(`{"symbol":"transfer"}`),
			json.RawMessage(`{"address":"CAAAA"}`),
		},
		ValueJSON: json.RawMessage(`{"i128":"1000"}`),
	}

	// nil decoder proves the XDR path is never touched.
	topics, value, err := EventTopicsValue(nil, e)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"transfer"},{"address":"CAAAA"}]`, string(topics))
	assert.JSONEq(t, `{"i128":"1000"}`, string(value))
}

func TestEventTopicsValue_XDRFallback(t *testing.T) {
	e := rpc.Event{
		Topic: []string{mustBase64(t, scSymbol("transfer"))},
		Value: mustBase64(t, scU64(500)),
	}

	topics, value, err := EventTopicsValue(XDRDecoder{}, e)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"transfer"}]`, string(topics))
	assert.JSONEq(t, `{"u64":500}`, string(value))
}

func TestEventTopicsValue_EmptyEvent(t *testing.T) {
	topics, value, err := EventTopicsValue(XDRDecoder{}, rpc.Event{})
	require.NoError(t, err)
	assert.JSONEq(t, `[]`, string(topics))
	assert.JSONEq(t, `null`, string(value))
}
