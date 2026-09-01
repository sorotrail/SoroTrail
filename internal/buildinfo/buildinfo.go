// Package buildinfo holds variables that are populated at link time via
// go build -ldflags -X, so the binary can report its own version, commit,
// and build date at runtime.
//
// When built with a plain "go build" (no ldflags), every variable keeps
// the fallback value "unknown" — this is intentional and keeps local
// development friction-free.
//
// Production builds override these through the Makefile's LDFLAGS variable
// (see the root Makefile) or the Dockerfile's build stage; both pass
// -ldflags="-X .../internal/buildinfo.Version=..." for each symbol.
package buildinfo

// Build-time values injected via ldflags. Defaults are suitable for
// development; production builds override them in the Makefile or Dockerfile.
var (
	Version   = "unknown"
	Commit    = "unknown"
	BuildDate = "unknown"
)
