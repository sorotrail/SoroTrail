package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNetworkError_Table(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "net.Error is detected",
			err:  &net.OpError{Op: "dial", Err: errors.New("connection refused")},
			want: true,
		},
		{
			name: "connection refused string",
			err:  errors.New("connection refused"),
			want: true,
		},
		{
			name: "no such host string",
			err:  errors.New("no such host"),
			want: true,
		},
		{
			name: "i/o timeout string",
			err:  errors.New("i/o timeout"),
			want: true,
		},
		{
			name: "deadline exceeded string",
			err:  errors.New("context deadline exceeded"),
			want: true,
		},
		{
			name: "EOF string",
			err:  errors.New("EOF"),
			want: true,
		},
		{
			name: "connection reset string",
			err:  errors.New("connection reset by peer"),
			want: true,
		},
		{
			name: "TLS handshake timeout",
			err:  errors.New("TLS handshake timeout"),
			want: true,
		},
		{
			name: "connect: connection refused",
			err:  errors.New("connect: connection refused"),
			want: true,
		},
		{
			name: "wrapped network error via errors.As",
			err:  fmt.Errorf("outer: %w", &net.OpError{Op: "read", Err: errors.New("broken pipe")}),
			want: true,
		},
		{
			name: "non-network error returns false",
			err:  errors.New("something went wrong"),
			want: false,
		},
		{
			name: "context.Canceled is not a network error",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "plain timeout error without network keywords",
			err:  errors.New("operation timed out"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNetworkError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
