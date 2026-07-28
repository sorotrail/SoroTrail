package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverer_Passthrough(t *testing.T) {
	rcv := NewRecoverer(slog.New(slog.NewTextHandler(io.Discard, nil)))
	before := rcv.PanicsRecovered()

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/ok", nil)
	rcv.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, before, rcv.PanicsRecovered(), "counter must not change on normal requests")
}

func TestRecoverer_Panics(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name:    "string panic",
			handler: func(w http.ResponseWriter, r *http.Request) { panic("boom") },
		},
		{
			name:    "error panic",
			handler: func(w http.ResponseWriter, r *http.Request) { panic(assert.AnError) },
		},
		{
			name:    "int panic",
			handler: func(w http.ResponseWriter, r *http.Request) { panic(42) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			rcv := NewRecoverer(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelError,
			})))
			before := rcv.PanicsRecovered()

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/test-path", nil)
			rcv.Middleware(tt.handler).ServeHTTP(w, r)

			assert.Equal(t, http.StatusInternalServerError, w.Code)
			assert.Equal(t, before+1, rcv.PanicsRecovered(), "counter must increment")

			logged := buf.String()
			assert.Contains(t, logged, "http panic recovered")
			assert.Contains(t, logged, "/test-path")
			assert.Contains(t, logged, "GET")
		})
	}
}

func TestRecoverer_MultiplePanics(t *testing.T) {
	rcv := NewRecoverer(slog.New(slog.NewTextHandler(io.Discard, nil)))

	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("repeated")
	})
	mw := rcv.Middleware(panicHandler)

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/multi", nil)
		mw.ServeHTTP(w, r)
		require.Equal(t, http.StatusInternalServerError, w.Code, "iteration %d", i)
	}

	assert.Equal(t, uint64(5), rcv.PanicsRecovered())
}

func TestRecoverer_WithServerRouter(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)

	// Normal request — no panic, counter stays 0.
	resp, _ := doGet(t, s, "/health")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, uint64(0), s.recoverer.PanicsRecovered(),
		"healthy request must not increment panic counter")
}
