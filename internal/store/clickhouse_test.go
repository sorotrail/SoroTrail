package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseClickHouseURL(t *testing.T) {
	cfg, err := parseClickHouseConfig("clickhouse://user:pass@localhost:9000/sorotrail?sslmode=disable")
	require.NoError(t, err)
	assert.Equal(t, "localhost", cfg.host)
	assert.Equal(t, 9000, cfg.port)
	assert.Equal(t, "user", cfg.username)
	assert.Equal(t, "pass", cfg.password)
	assert.Equal(t, "sorotrail", cfg.database)
	assert.False(t, cfg.ssl)
}

func TestNewStoreFromURL(t *testing.T) {
	postgresStore, err := NewStoreFromURL("postgres://user:pass@localhost:5432/sorotrail")
	require.NoError(t, err)
	assert.IsType(t, &Postgres{}, postgresStore)

	clickhouseStore, err := NewStoreFromURL("clickhouse://user:pass@localhost:9000/sorotrail")
	require.NoError(t, err)
	assert.IsType(t, &ClickHouse{}, clickhouseStore)
}
