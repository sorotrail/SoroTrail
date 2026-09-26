//go:build integration

package testdb

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDockerUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "docker socket missing",
			err:  errors.New("could not find a working docker socket"),
			want: true,
		},
		{
			name: "failed client creation",
			err:  errors.New("failed to create docker client: context deadline exceeded"),
			want: true,
		},
		{
			name: "permission denied",
			err:  errors.New("permission denied while trying to connect to the Docker daemon socket"),
			want: true,
		},
		{
			name: "cannot connect to docker daemon",
			err:  errors.New("cannot connect to the Docker daemon at unix:///var/run/docker.sock"),
			want: true,
		},
		{
			name: "unrelated error",
			err:  errors.New("connection refused"),
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isDockerUnavailable(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTestDB_HarnessCoverage(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping shared harness database tests")
	}

	migrateFn := func(dbURL string) error {
		return nil
	}

	t.Run("Setup Shared and Truncate Clearing", func(t *testing.T) {
		pool := Setup(t, migrateFn)
		require.NotNil(t, pool)

		ctx := context.Background()
		_, err := pool.Exec(ctx, "INSERT INTO watched_contracts (contract_id) VALUES ('C_TEST_TRUNCATE')")
		require.NoError(t, err)

		pool2 := Setup(t, migrateFn)
		require.NotNil(t, pool2)

		var count int
		err = pool2.QueryRow(ctx, "SELECT count(*) FROM watched_contracts").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "truncation must clear tables between tests")
	})

	t.Run("Cleanup with Expired Test Context", func(t *testing.T) {
		expiredCtx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		time.Sleep(10 * time.Millisecond)

		pool, err := pgxpool.New(expiredCtx, url)
		if err == nil {
			defer pool.Close()
			err = truncateAll(expiredCtx, pool)
			_ = err
		}
	})
}
