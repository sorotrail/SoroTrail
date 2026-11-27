package api

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// routeMux is a chi.Router that rejects duplicate method/pattern
// registrations. chi silently replaces the earlier handler, so a duplicated
// block looks healthy while the first registration (and any middleware on it)
// is dead. Panicking at construction turns that into an immediate failure
// instead of a route that only appears to exist.
//
// With and Group return routeMux values that share the registration set, so
// routes mounted inside middleware groups are covered as well. The key is the
// method plus pattern, which keeps GET /events and DELETE /events distinct.
type routeMux struct {
	chi.Router
	registered map[string]struct{}
}

func newRouteMux(r chi.Router) *routeMux {
	return &routeMux{Router: r, registered: make(map[string]struct{})}
}

func (m *routeMux) add(method, pattern string) {
	key := method + " " + pattern
	if _, exists := m.registered[key]; exists {
		panic(fmt.Sprintf("api: duplicate route registration: %s %s", method, pattern))
	}
	m.registered[key] = struct{}{}
}

func (m *routeMux) register(method, pattern string, handler http.Handler) {
	m.add(method, pattern)
	m.Router.Method(method, pattern, handler)
}

// Get registers a GET route pattern with its handler function.
func (m *routeMux) Get(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodGet, pattern, handler)
}

// Post registers a handler for HTTP POST requests at pattern and
// rejects a duplicate POST registration for the same pattern.
func (m *routeMux) Post(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodPost, pattern, handler)
}

// Put registers a PUT route pattern with its handler function.
func (m *routeMux) Put(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodPut, pattern, handler)
}

// Patch registers a handler for HTTP PATCH requests at pattern.
func (m *routeMux) Patch(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodPatch, pattern, handler)
}

// Delete registers a handler for HTTP DELETE requests at pattern and
// rejects a duplicate DELETE registration for the same pattern.
func (m *routeMux) Delete(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodDelete, pattern, handler)
}

// Head registers a handler for HTTP HEAD requests at pattern.
func (m *routeMux) Head(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodHead, pattern, handler)
}

// Options registers a handler for HTTP OPTIONS requests matching pattern.
func (m *routeMux) Options(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodOptions, pattern, handler)
}

// Connect registers handler for HTTP CONNECT requests on pattern and rejects
// a duplicate CONNECT registration for the same pattern.
func (m *routeMux) Connect(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodConnect, pattern, handler)
}

// Trace registers a handler for HTTP TRACE requests at pattern.
func (m *routeMux) Trace(pattern string, handler http.HandlerFunc) {
	m.register(http.MethodTrace, pattern, handler)
}

// Method registers a handler for an arbitrary HTTP method at pattern and rejects
// a duplicate registration for the same method and pattern.
func (m *routeMux) Method(method, pattern string, handler http.Handler) {
	m.register(method, pattern, handler)
}

// Handle mounts a handler for every HTTP method. It uses "*" as the
// registration key because chi expands the registration internally; a second
// Handle for the same pattern is still rejected.
func (m *routeMux) Handle(pattern string, handler http.Handler) {
	m.add("*", pattern)
	m.Router.Handle(pattern, handler)
}

// With returns a new router with the given middlewares wired in, sharing the same registration set.
func (m *routeMux) With(middlewares ...func(http.Handler) http.Handler) chi.Router {
	return &routeMux{
		Router:     m.Router.With(middlewares...),
		registered: m.registered,
	}
}

// Group creates a new inline router group sharing the routeMux registration set.
func (m *routeMux) Group(fn func(r chi.Router)) chi.Router {
	im := m.With()
	if fn != nil {
		fn(im)
	}
	return im
}
