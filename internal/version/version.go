package version

var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// GetVersion returns the Version variable or "dev" if empty.
func GetVersion() string {
	if Version == "" {
		return "dev"
	}
	return Version
}

// GetCommit returns the Commit variable or "unknown" if empty.
func GetCommit() string {
	if Commit == "" {
		return "unknown"
	}
	return Commit
}

// GetDate returns the Date variable or "unknown" if empty.
func GetDate() string {
	if Date == "" {
		return "unknown"
	}
	return Date
}
