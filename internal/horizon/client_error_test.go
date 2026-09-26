package horizon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHorizonClientErrorPaths(t *testing.T) {
	t.Run("canceled context", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Never respond
		}))
		defer srv.Close()

		client := NewClient(srv.URL)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.GetTransactions(ctx, 1)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("bad status code", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		client := NewClient(srv.URL)
		_, err := client.GetTransactions(context.Background(), 1)
		require.Error(t, err)
	})
}
