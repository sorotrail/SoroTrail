package buildinfo

// Build-time values injected via ldflags. Defaults are suitable for
// development; production builds override them in the Makefile or Dockerfile.
var (
	Version   = "unknown"
	Commit    = "unknown"
	BuildDate = "unknown"
)
