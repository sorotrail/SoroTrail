// Package client is the versioned Go client for the SoroTrail HTTP API.
//
// The typed surface — request/response types and one method per
// operation — is generated from api/openapi.yaml by cmd/clientgen
// (`make client`), and pkg/client/drift_test.go fails the build if the
// committed client drifts from the spec. This file holds the hand-written
// core the generated code calls: the HTTP transport, error mapping, and
// the small helpers the generated methods are compiled against.
//
// The client is versioned in lockstep with the spec: SpecVersion (in
// client.gen.go) carries the api/openapi.yaml info.version it was
// generated from. See RELEASING.md for how releases publish it.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultTimeout bounds every request; callers with long-running queries
// (large exports, slow pagination) can supply their own http.Client via
// WithHTTPClient.
const defaultTimeout = 30 * time.Second

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sets the tenant API key. It is sent as
// `Authorization: Bearer <key>` on every request, matching the spec's
// TenantAuth scheme. Single-tenant instances serve anonymous requests,
// so the option is optional.
func WithAPIKey(key string) Option {
	return func(c *Client) {
		c.apiKey = key
	}
}

// WithHTTPClient replaces the default HTTP client (30s timeout). Use it
// to control transport, timeouts, or retries.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		c.http = hc
	}
}

// Client talks to one SoroTrail API instance. Construct with New; all
// generated methods are on this type.
type Client struct {
	baseURL string
	http    *http.Client
	apiKey  string
}

// New returns a Client for the given base URL (e.g.
// "https://sorotrail.example.com"). The URL is used verbatim; no path
// is appended, so point it at the API root.
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http:    &http.Client{Timeout: defaultTimeout},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// APIError is returned for every non-2xx response. Message is the
// server's standard error envelope when the body carried one.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sorotrail: %s (HTTP %d)", e.Message, e.StatusCode)
	}
	return fmt.Sprintf("sorotrail: HTTP %d", e.StatusCode)
}

// do is the typed helper the generated object-returning methods call.
// It decodes the success body into a fresh T and returns a pointer to it.
func do[T any](c *Client, ctx context.Context, method, path string, query url.Values, body any) (*T, error) {
	var out T
	if err := c.roundTrip(ctx, method, path, query, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// doSlice is the typed helper for operations whose success body is a
// JSON array (e.g. listSubscriptions). It returns the slice by value.
func doSlice[T any](c *Client, ctx context.Context, method, path string, query url.Values, body any) ([]T, error) {
	var out []T
	if err := c.roundTrip(ctx, method, path, query, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// doRaw is the helper for operations whose success body is not JSON
// (the CSV export, raw XDR). The raw bytes are returned untouched.
func doRaw(c *Client, ctx context.Context, method, path string, query url.Values, body any) ([]byte, error) {
	resp, err := c.send(ctx, method, path, query, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading %s %s response: %w", method, path, err)
	}
	return data, nil
}

// doNoContent is the helper for operations with no success body (204
// or empty success responses): only the error is interesting.
func doNoContent(c *Client, ctx context.Context, method, path string, query url.Values, body any) error {
	return c.roundTrip(ctx, method, path, query, body, nil)
}

// roundTrip sends the request and JSON-decodes the success body into
// out (when out is non-nil).
func (c *Client) roundTrip(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	resp, err := c.send(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding %s %s response: %w", method, path, err)
	}
	return nil
}

// send builds and executes one request, returning a non-nil error for
// transport failures and an *APIError for non-2xx responses. The caller
// owns the returned response body.
func (c *Client) send(ctx context.Context, method, path string, query url.Values, body any) (*http.Response, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encoding %s %s request body: %w", method, path, err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return nil, fmt.Errorf("building %s %s request: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sorotrail: %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var errResp struct {
			Error string `json:"error"`
		}
		// The envelope is small; limit the read so a misbehaving server
		// cannot balloon the error path.
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&errResp)
		return nil, &APIError{StatusCode: resp.StatusCode, Message: errResp.Error}
	}
	return resp, nil
}

// urlEscapePath substitutes the {param} placeholders of a template path
// (in declaration order) with percent-escaped values. The generated
// methods pass path arguments positionally, mirroring the spec's
// parameter order.
func urlEscapePath(tmpl string, values ...string) string {
	for _, v := range values {
		start := strings.IndexByte(tmpl, '{')
		end := strings.IndexByte(tmpl, '}')
		if start < 0 || end < 0 || end < start {
			break
		}
		tmpl = tmpl[:start] + url.PathEscape(v) + tmpl[end+1:]
	}
	return tmpl
}
