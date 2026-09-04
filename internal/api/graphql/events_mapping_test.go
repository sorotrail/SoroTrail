package graphql

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestEventToResult_MappingAndFields(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	e := store.Event{
		ID:               "0000000042-0000000001",
		ContractID:       "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:           42,
		Type:             "contract",
		TxHash:           "deadbeef12345678",
		TxIndex:          1,
		OpIndex:          2,
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:            json.RawMessage(`{"u64":100}`),
		CreatedAt:        now,
	}

	res := eventToResult(e)

	assert.Equal(t, e.ID, res.ID)
	assert.Equal(t, e.ContractID, res.ContractID)
	assert.Equal(t, e.Ledger, res.Ledger)
	assert.Equal(t, e.Type, res.Type)
	assert.Equal(t, e.TxHash, res.TxHash)
	assert.Equal(t, e.TxIndex, res.TxIndex)
	assert.Equal(t, e.OpIndex, res.OpIndex)
	assert.Equal(t, e.InSuccessfulCall, res.InSuccessfulCall)
	assert.JSONEq(t, string(e.Topics), string(res.Topics))
	assert.JSONEq(t, string(e.Value), string(res.Value))
	assert.Equal(t, e.CreatedAt, res.CreatedAt)
}

func TestEventToResult_OptionalFieldsNilWhenAbsent(t *testing.T) {
	e := store.Event{
		ID:               "0000000042-0000000002",
		ContractID:       "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:           42,
		Type:             "contract",
		TxHash:           "deadbeef12345678",
		TxIndex:          0,
		OpIndex:          0,
		InSuccessfulCall: true,
		Topics:           nil,
		Value:            nil,
		CreatedAt:        time.Now(),
	}

	res := eventToResult(e)

	assert.Nil(t, res.Topics)
	assert.Nil(t, res.Value)
}

func TestConnectionsAndEdges_ShapesAndEmptySets(t *testing.T) {
	// Test empty result set yielding empty connection slices rather than nil
	conn := EventConnection{
		Edges:      []EventEdge{},
		Nodes:      []EventResult{},
		PageInfo:   PageInfo{HasNextPage: false, EndCursor: ""},
		TotalCount: 0,
	}

	assert.NotNil(t, conn.Edges)
	assert.NotNil(t, conn.Nodes)
	assert.Empty(t, conn.Edges)
	assert.Empty(t, conn.Nodes)
	assert.False(t, conn.PageInfo.HasNextPage)
	assert.Empty(t, conn.PageInfo.EndCursor)
	assert.Equal(t, int32(0), conn.TotalCount)

	// Test populated connection shape with pageInfo and cursors
	node := EventResult{
		ID:         "0000000042-0000000001",
		ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:     42,
	}
	edge := EventEdge{
		Cursor: "cursor-abc",
		Node:   node,
	}

	popConn := EventConnection{
		Edges:      []EventEdge{edge},
		Nodes:      []EventResult{node},
		PageInfo:   PageInfo{HasNextPage: true, EndCursor: "cursor-abc"},
		TotalCount: 1,
	}

	assert.Len(t, popConn.Edges, 1)
	assert.Equal(t, "cursor-abc", popConn.Edges[0].Cursor)
	assert.Equal(t, node.ID, popConn.Edges[0].Node.ID)
	assert.Len(t, popConn.Nodes, 1)
	assert.Equal(t, node.ID, popConn.Nodes[0].ID)
	assert.True(t, popConn.PageInfo.HasNextPage)
	assert.Equal(t, "cursor-abc", popConn.PageInfo.EndCursor)
	assert.Equal(t, int32(1), popConn.TotalCount)
}

func TestTokenEventConnectionAndContractConnection_Shapes(t *testing.T) {
	tokenConn := TokenEventConnection{
		Edges:      []TokenEventEdge{},
		Nodes:      []TokenEventResult{},
		PageInfo:   PageInfo{HasNextPage: false},
		TotalCount: 0,
	}
	assert.NotNil(t, tokenConn.Edges)
	assert.NotNil(t, tokenConn.Nodes)

	contractConn := ContractConnection{
		Edges:      []ContractEdge{},
		Nodes:      []ContractResult{},
		PageInfo:   PageInfo{HasNextPage: false},
		TotalCount: 0,
	}
	assert.NotNil(t, contractConn.Edges)
	assert.NotNil(t, contractConn.Nodes)
}
